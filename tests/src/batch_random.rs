// Randomized end-to-end checks of the batch executor against a live cluster.
//
// The executor splits a batch per node, sends one command per node, then puts
// every row back at the index it had on input; on a Sequence/PreferRack retry
// it re-splits the batch onto the next replica. These tests draw random batch
// sizes, op mixes, bin lists (so header repeats land at random positions), key
// routing, and unroutable rows, and check the properties a caller depends on:
// every row comes back, at its own index, with the right outcome — under every
// replica policy, and across a retry that has to re-route onto another node.
//
// Each test prints its seed on failure so a case reproduces exactly. They need
// a multi-node cluster to say anything about routing; the 4-node 3100 cluster
// (services-alternate + admin/admin) is what they were written against.

use std::collections::HashSet;
use std::sync::Arc;

use aerospike::*;
use aerospike::policy::Replica;
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

use crate::common;

/// Deterministic xorshift so a failing seed reproduces exactly.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, one_in: usize) -> bool {
        self.below(one_in) == 0
    }
}

const V: &str = "v";

/// The bin lists a read row can carry. Consecutive rows with the same list (and
/// the same set) repeat the previous header on the wire; a change breaks it.
fn random_bins(r: &mut Rng) -> Bins {
    match r.below(3) {
        0 => Bins::from([V]),
        1 => Bins::from([V, "absent"]),
        _ => Bins::All,
    }
}

async fn write_values(client: &Client, keys: &[Key], value_of: impl Fn(usize) -> i64) {
    let bwp = BatchWritePolicy::default();
    let writes: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let bin = as_bin!(V, value_of(i));
            BatchOperation::write(&bwp, k.clone(), vec![operations::put(&bin)])
        })
        .collect();
    for chunk in writes.chunks(500) {
        let recs = client
            .batch(&BatchPolicy::default(), chunk)
            .await
            .expect("seed write");
        assert!(recs.iter().all(|r| r.result_code == Some(ResultCode::Ok)));
    }
}

fn bin_value(rec: &BatchRecord) -> Option<Value> {
    rec.record.as_ref().and_then(|r| r.bins.get(V).cloned())
}

async fn breaker_client(max_error_rate: usize) -> Client {
    let mut policy = common::client_policy().clone();
    policy.max_error_rate = max_error_rate;
    // Far longer than any test here, so the breaker never resets under us.
    policy.error_rate_window = 10_000;
    Client::new(&policy, &common::hosts().to_string())
        .await
        .expect("connect for breaker test")
}

async fn rack_client(rack: usize) -> Client {
    let mut policy = common::client_policy().clone();
    policy.rack_ids = Some(HashSet::from([rack]));
    Client::new(&policy, &common::hosts().to_string())
        .await
        .expect("connect for rack test")
}

/// Node count and the namespace's replication factor, printed so a run's
/// output says what topology it actually exercised.
async fn cluster_facts(client: &Client, namespace: &str) -> (usize, Option<usize>) {
    let nodes = client.cluster.nodes();
    let key = format!("namespace/{namespace}");
    let rf = match nodes.first() {
        Some(node) => node
            .info(&AdminPolicy::default(), &[&key])
            .await
            .ok()
            .and_then(|m| m.get(&key).cloned())
            .and_then(|s| {
                s.split(';')
                    .find_map(|kv| kv.strip_prefix("replication-factor="))
                    .and_then(|v| v.parse().ok())
            }),
        None => None,
    };
    println!(
        "cluster: {} node(s), {namespace} replication-factor={rf:?}",
        nodes.len()
    );
    (nodes.len(), rf)
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    ReadSome,
    ReadMore,
    ReadAll,
    Write,
    Delete,
}

/// Random mixes of reads (three bin shapes), writes and deletes over random
/// key sizes, half the keys pre-written. Checks every row is present, at its
/// input index, with the outcome the pre-state dictates; then reads everything
/// back to confirm the writes and deletes actually took effect.
#[aerospike_macro::test]
async fn random_mixed_batches_return_every_row_in_input_order() {
    let client = common::client().await;
    let namespace = common::namespace();
    cluster_facts(&client, namespace).await;

    for seed in 1..=6u64 {
        let mut r = Rng::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let set = common::rand_str(10);
        let n = 1 + r.below(300);
        let keys: Vec<Key> = (0..n).map(|i| as_key!(namespace, &set, i as i64)).collect();

        let written: Vec<bool> = (0..n).map(|_| r.chance(2)).collect();
        let prewritten: Vec<Key> = keys
            .iter()
            .zip(&written)
            .filter(|(_, w)| **w)
            .map(|(k, _)| k.clone())
            .collect();
        // Value = the key's index, so a wrong-slot row is detectable by value too.
        let index_of = |k: &Key| keys.iter().position(|x| x == k).unwrap() as i64;
        write_values(&client, &prewritten, |j| index_of(&prewritten[j])).await;

        let kinds: Vec<Kind> = (0..n)
            .map(|_| match r.below(5) {
                0 => Kind::ReadSome,
                1 => Kind::ReadMore,
                2 => Kind::ReadAll,
                3 => Kind::Write,
                _ => Kind::Delete,
            })
            .collect();

        let brp = BatchReadPolicy::default();
        let bwp = BatchWritePolicy::default();
        let bdp = BatchDeletePolicy::default();
        let ops: Vec<BatchOperation> = kinds
            .iter()
            .zip(&keys)
            .enumerate()
            .map(|(i, (kind, key))| match kind {
                Kind::ReadSome => BatchOperation::read(&brp, key.clone(), Bins::from([V])),
                Kind::ReadMore => BatchOperation::read(&brp, key.clone(), Bins::from([V, "absent"])),
                Kind::ReadAll => BatchOperation::read(&brp, key.clone(), Bins::All),
                Kind::Write => BatchOperation::write(
                    &bwp,
                    key.clone(),
                    vec![operations::put(&as_bin!(V, 1_000 + i as i64))],
                ),
                Kind::Delete => BatchOperation::delete(&bdp, key.clone()),
            })
            .collect();

        let recs = client
            .batch(&BatchPolicy::default(), &ops)
            .await
            .unwrap_or_else(|e| panic!("{}", format!("seed {seed}: batch failed: {e}")));

        let ctx = format!("seed {seed} n={n}");
        assert_eq!(recs.len(), n, "{ctx}: row count");
        for i in 0..n {
            let row = format!("{ctx} row {i} kind {:?} written={}", kinds[i], written[i]);
            assert_eq!(recs[i].key, keys[i], "{row}: key not at its input index");
            let expected = match kinds[i] {
                Kind::Write => ResultCode::Ok,
                _ if written[i] => ResultCode::Ok,
                _ => ResultCode::KeyNotFoundError,
            };
            assert_eq!(recs[i].result_code, Some(expected), "{row}: result code");
            if matches!(kinds[i], Kind::ReadSome | Kind::ReadMore | Kind::ReadAll) && written[i] {
                assert_eq!(
                    bin_value(&recs[i]),
                    Some(Value::from(i as i64)),
                    "{row}: read returned another row's value"
                );
            }
        }

        // The writes and deletes must have landed on the right keys.
        let verify: Vec<_> = keys
            .iter()
            .map(|k| BatchOperation::read(&brp, k.clone(), Bins::All))
            .collect();
        let after = client.batch(&BatchPolicy::default(), &verify).await.unwrap();
        for i in 0..n {
            let row = format!("{ctx} verify row {i} kind {:?} written={}", kinds[i], written[i]);
            match kinds[i] {
                Kind::Write => assert_eq!(bin_value(&after[i]), Some(Value::from(1_000 + i as i64)), "{row}"),
                Kind::Delete => assert_eq!(after[i].result_code, Some(ResultCode::KeyNotFoundError), "{row}"),
                _ if written[i] => assert_eq!(bin_value(&after[i]), Some(Value::from(i as i64)), "{row}"),
                _ => assert_eq!(after[i].result_code, Some(ResultCode::KeyNotFoundError), "{row}"),
            }
        }
    }
}

/// Rows the cluster cannot route are per-row outcomes. They must come back as
/// `PartitionUnavailable` at exactly their own index, with every routable row
/// unaffected — and a batch with nothing routable at all fails outright.
#[aerospike_macro::test]
async fn random_batches_with_unroutable_rows_land_at_their_index() {
    let client = common::client().await;
    let namespace = common::namespace();
    let brp = BatchReadPolicy::default();

    for seed in 1..=6u64 {
        let mut r = Rng::new(seed.wrapping_mul(0xD1B5_4A32_D192_ED03));
        let set = common::rand_str(10);
        let bogus_ns = format!("nx_{}", common::rand_str(6));
        let n = 1 + r.below(200);

        // Row 0 always routable so the batch as a whole is valid.
        let unroutable: Vec<bool> = (0..n).map(|i| i > 0 && r.chance(5)).collect();
        let keys: Vec<Key> = (0..n)
            .map(|i| {
                let ns = if unroutable[i] { bogus_ns.as_str() } else { namespace };
                as_key!(ns, &set, i as i64)
            })
            .collect();
        let routable: Vec<Key> = keys
            .iter()
            .zip(&unroutable)
            .filter(|(_, u)| !**u)
            .map(|(k, _)| k.clone())
            .collect();
        let index_of = |k: &Key| keys.iter().position(|x| x == k).unwrap() as i64;
        write_values(&client, &routable, |j| index_of(&routable[j])).await;

        let ops: Vec<_> = keys
            .iter()
            .map(|k| BatchOperation::read(&brp, k.clone(), random_bins(&mut r)))
            .collect();
        let recs = client
            .batch(&BatchPolicy::default(), &ops)
            .await
            .unwrap_or_else(|e| panic!("{}", format!("seed {seed}: batch failed: {e}")));

        let ctx = format!("seed {seed} n={n}");
        assert_eq!(recs.len(), n, "{ctx}: row count");
        for i in 0..n {
            let row = format!("{ctx} row {i} unroutable={}", unroutable[i]);
            assert_eq!(recs[i].key, keys[i], "{row}: key not at its input index");
            if unroutable[i] {
                assert_eq!(recs[i].result_code, Some(ResultCode::PartitionUnavailable), "{row}");
                assert!(recs[i].record.is_none(), "{}", format!("{row}: unroutable row carried a record"));
            } else {
                assert_eq!(recs[i].result_code, Some(ResultCode::Ok), "{row}");
                assert_eq!(bin_value(&recs[i]), Some(Value::from(i as i64)), "{row}: value");
            }
        }
    }

    // Nothing routable: the routing error is the result, not an empty batch.
    let all_bogus: Vec<_> = (0..5)
        .map(|i| {
            BatchOperation::read(
                &brp,
                as_key!(format!("nx_{}", common::rand_str(6)).as_str(), "s", i as i64),
                Bins::All,
            )
        })
        .collect();
    assert!(
        client.batch(&BatchPolicy::default(), &all_bogus).await.is_err(),
        "a batch with no routable key must fail"
    );
}

/// The same random read batch under Master, Sequence and PreferRack must
/// produce identical rows. The policies route keys to different nodes, so the
/// per-node split and the merge back into input order differ each time; the
/// caller must not be able to tell.
#[aerospike_macro::test]
async fn same_random_read_batch_agrees_across_replica_policies() {
    let master = common::client().await;
    let rack = rack_client(0).await;
    let namespace = common::namespace();
    let brp = BatchReadPolicy::default();

    for seed in 1..=4u64 {
        let mut r = Rng::new(seed.wrapping_mul(0xA24B_AED4_963E_E407));
        let set = common::rand_str(10);
        let n = 1 + r.below(400);
        let keys: Vec<Key> = (0..n).map(|i| as_key!(namespace, &set, i as i64)).collect();
        write_values(&master, &keys, |i| i as i64).await;

        let ops: Vec<_> = keys
            .iter()
            .map(|k| BatchOperation::read(&brp, k.clone(), random_bins(&mut r)))
            .collect();

        type Row = (Key, Option<ResultCode>, Vec<(String, Value)>);
        async fn run(client: &Client, replica: Replica, ops: &[BatchOperation], seed: u64) -> Vec<Row> {
            let mut policy = BatchPolicy::default();
            policy.replica = replica;
            client
                .batch(&policy, ops)
                .await
                .unwrap_or_else(|e| panic!("{}", format!("seed {seed} {replica:?}: {e}")))
                .into_iter()
                .map(|rec| {
                    let mut bins: Vec<(String, Value)> = rec
                        .record
                        .map(|rr| rr.bins.into_iter().collect())
                        .unwrap_or_default();
                    bins.sort_by(|a, b| a.0.cmp(&b.0));
                    (rec.key, rec.result_code, bins)
                })
                .collect()
        }

        let by_master = run(&master, Replica::Master, &ops, seed).await;
        let by_sequence = run(&master, Replica::Sequence, &ops, seed).await;
        let by_rack = run(&rack, Replica::PreferRack, &ops, seed).await;

        assert_eq!(by_master.len(), n, "seed {seed}: row count");
        assert_eq!(by_master, by_sequence, "seed {seed}: Master vs Sequence differ");
        assert_eq!(by_master, by_rack, "seed {seed}: Master vs PreferRack differ");
        for (i, (key, rc, bins)) in by_master.iter().enumerate() {
            assert_eq!(*key, keys[i], "seed {seed} row {i}: order");
            assert_eq!(*rc, Some(ResultCode::Ok), "seed {seed} row {i}: code");
            assert!(bins.iter().any(|(b, v)| b == V && *v == Value::from(i as i64)), "{}", format!("seed {seed} row {i}: value"));
        }
    }
    rack.close().await.unwrap();
}

/// The retry path end to end. With the circuit breaker tripped on exactly one
/// node, every key whose master is that node fails its first attempt. Under
/// `Replica::Sequence` the retry must re-split the batch and route those keys
/// to their next replica — a different, healthy node — and the whole batch must
/// then succeed with every row correct and in order. `max_retries = 0` on the
/// same setup must fail, which is what proves the keys really did land on the
/// tripped node and that the retry, not luck, is what rescued them.
#[aerospike_macro::test]
async fn sequence_retry_resplits_onto_the_next_replica_and_succeeds() {
    let client = breaker_client(1).await;
    let namespace = common::namespace();
    let (node_count, rf) = cluster_facts(&client, namespace).await;
    if node_count < 2 || rf.map_or(false, |rf| rf < 2) {
        println!("SKIP: needs >= 2 nodes and replication-factor >= 2 (have {node_count}, rf {rf:?})");
        client.close().await.unwrap();
        return;
    }

    let brp = BatchReadPolicy::default();
    let mut policy = BatchPolicy::default();
    policy.replica = Replica::Sequence;
    policy.base_policy.sleep_between_retries = 0;
    policy.base_policy.total_timeout = 10_000;

    // Seed before tripping anything.
    let set = common::rand_str(10);
    let keys: Vec<Key> = (0..200).map(|i| as_key!(namespace, &set, i as i64)).collect();
    write_values(&client, &keys, |i| i as i64).await;

    let tripped = client.cluster.nodes()[0].clone();
    for _ in 0..16 {
        tripped.incr_error_rate();
    }
    let trips_before = tripped.error_rate_count();

    let mut r = Rng::new(0x5EED);
    let ops: Vec<_> = keys
        .iter()
        .map(|k| BatchOperation::read(&brp, k.clone(), random_bins(&mut r)))
        .collect();

    // No retry budget: the group on the tripped node is refused and the batch fails.
    policy.base_policy.max_retries = 0;
    let err = client
        .batch(&policy, &ops)
        .await
        .expect_err("with max_retries = 0 the keys on the tripped node cannot succeed");
    println!("max_retries=0 failed as expected: {err}");

    // With retries the re-split routes those keys to the next replica.
    policy.base_policy.max_retries = 3;
    let recs = client
        .batch(&policy, &ops)
        .await
        .expect("the retry must re-route onto the healthy replica");
    assert_eq!(recs.len(), keys.len());
    for (i, rec) in recs.iter().enumerate() {
        assert_eq!(rec.key, keys[i], "row {i}: order after re-split");
        assert_eq!(rec.result_code, Some(ResultCode::Ok), "row {i}: code");
        assert_eq!(bin_value(rec), Some(Value::from(i as i64)), "row {i}: value");
    }

    // Breaker refusals are not node errors; neither attempt may add to the count.
    assert_eq!(
        tripped.error_rate_count(),
        trips_before,
        "retry attempts must not feed the breaker"
    );

    // Random sizes and bin shapes through the same re-split.
    for seed in 1..=3u64 {
        let mut r = Rng::new(seed.wrapping_mul(0x2545_F491_4F6C_DD1D));
        let n = 1 + r.below(150);
        let sub: Vec<_> = keys[..n]
            .iter()
            .map(|k| BatchOperation::read(&brp, k.clone(), random_bins(&mut r)))
            .collect();
        let recs = client.batch(&policy, &sub).await.unwrap_or_else(|e| panic!("{}", format!("seed {seed}: {e}")));
        assert_eq!(recs.len(), n, "seed {seed}");
        for (i, rec) in recs.iter().enumerate() {
            assert_eq!(rec.key, keys[i], "seed {seed} row {i}: order");
            assert_eq!(bin_value(rec), Some(Value::from(i as i64)), "seed {seed} row {i}: value");
        }
    }
    client.close().await.unwrap();
}

/// Every `Arc<Node>` the executor clones while splitting, grouping and
/// re-splitting must be dropped by the time the batch returns. After many
/// random batches — Master and Sequence, some with unroutable rows, some
/// issued concurrently — the nodes' strong counts must be back at baseline.
#[aerospike_macro::test]
async fn arc_node_refcounts_return_to_baseline_after_many_random_batches() {
    let client = common::client().await;
    let namespace = common::namespace();
    let brp = BatchReadPolicy::default();

    let set = common::rand_str(10);
    let keys: Vec<Key> = (0..300).map(|i| as_key!(namespace, &set, i as i64)).collect();
    write_values(&client, &keys, |i| i as i64).await;

    // Background work — tend, pool fill, a just-finished batch's tasks — can
    // hold a node briefly, so both readings are the minimum over a settle window.
    async fn settled(client: &Client) -> Vec<usize> {
        let snap = |c: &Client| -> Vec<usize> {
            let nodes = c.cluster.nodes();
            nodes.iter().map(Arc::strong_count).collect()
        };
        let mut best = snap(client);
        for _ in 0..6 {
            sleep(Duration::from_millis(300)).await;
            for (b, v) in best.iter_mut().zip(snap(client)) {
                *b = (*b).min(v);
            }
        }
        best
    }
    let baseline = settled(&client).await;

    let mut r = Rng::new(0xC0FFEE);
    let make = |r: &mut Rng| -> (BatchPolicy, Vec<BatchOperation>) {
        let n = 1 + r.below(300);
        let bogus = format!("nx_{}", common::rand_str(4));
        let ops = (0..n)
            .map(|i| {
                let key = if i > 0 && r.chance(6) {
                    as_key!(bogus.as_str(), &set, i as i64)
                } else {
                    keys[i].clone()
                };
                BatchOperation::read(&brp, key, random_bins(r))
            })
            .collect();
        let mut policy = BatchPolicy::default();
        policy.replica = if r.chance(2) { Replica::Master } else { Replica::Sequence };
        (policy, ops)
    };

    for _ in 0..100 {
        let (policy, ops) = make(&mut r);
        client.batch(&policy, &ops).await.expect("batch");
    }
    // A burst in flight together, so per-node groups overlap in time.
    let batches: Vec<_> = (0..8).map(|_| make(&mut r)).collect();
    futures::future::join_all(batches.iter().map(|(p, o)| client.batch(p, o)))
        .await
        .into_iter()
        .for_each(|res| {
            res.expect("concurrent batch");
        });

    let after = settled(&client).await;
    println!("node Arc strong counts: baseline {baseline:?} after {after:?}");
    assert_eq!(after, baseline, "Arc<Node> references leaked out of the batch path");
}
