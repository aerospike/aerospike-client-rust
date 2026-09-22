// End-to-end timing of `client.batch()` against a live server. Every test here
// is `#[ignore]`d: they need a server, print numbers rather than assert them,
// and seed a fresh set per run. Run them on purpose:
//
//   AEROSPIKE_HOSTS=... cargo test --release --no-default-features \
//     --features async,serialization,rt-tokio,tls perf_e2e -- --ignored --nocapture --test-threads=1
//
// Read the throughput tests, not the latency one. Single-batch latency is set
// by the server's response time, which drifted by ~3 ms between identical runs
// on a local node — 6–30x the client-side work under test — so it can only ever
// show noise for a client change. Client per-key CPU is a per-core cost shared
// by every batch in flight, and it surfaces as batches/second under
// concurrency: on v2 that separated a 1.76 ms -> 1.45 ms per-batch change
// cleanly (5 runs each, no overlap) where latency could not.

use std::sync::Arc;

use aerospike::*;
use aerospike_rt::time::Instant;

use crate::common;

const ITERS: usize = 25;

fn read_ops(keys: &[Key]) -> Vec<BatchOperation> {
    let brp = BatchReadPolicy::default();
    keys.iter().map(|k| BatchOperation::read(&brp, k.clone(), Bins::from(["a", "b"]))).collect()
}

async fn seed(client: &Client, namespace: &str, set_name: &str, n: usize) -> Vec<Key> {
    let bwp = BatchWritePolicy::default();
    let bins = [as_bin!("a", 1), as_bin!("b", "some value"), as_bin!("c", 42)];
    let wops: Vec<_> = bins.iter().map(operations::put).collect();
    let keys: Vec<Key> = (0..n as i64).map(|i| as_key!(namespace, set_name, i)).collect();
    for chunk in keys.chunks(1000) {
        let mut writes: Vec<_> = chunk.iter().map(|k| BatchOperation::write(&bwp, k.clone(), wops.clone())).collect();
        client.batch(&BatchPolicy::default(), &mut writes).await.expect("seed batch write");
    }
    keys
}

fn report(label: &str, n: usize, mut samples: Vec<f64>) {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let (min, median) = (samples[0], samples[samples.len() / 2]);
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    println!(
        "PERF e2e {label:<10} {n:>5} keys x{ITERS}: min {min:8.3}ms  median {median:8.3}ms  mean {mean:8.3}ms  ({:.1}µs/key at median)",
        median * 1000.0 / n as f64
    );
}

async fn time_reads(client: &Client, keys: &[Key], concurrency: Concurrency, label: &str) {
    let mut policy = BatchPolicy::default();
    policy.concurrency = concurrency;
    for _ in 0..3 {
        let mut ops = read_ops(keys);
        client.batch(&policy, &mut ops).await.expect("warm-up");
    }
    let mut samples = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let mut ops = read_ops(keys);
        let start = Instant::now();
        client.batch(&policy, &mut ops).await.expect("batch read");
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        assert!(ops.iter().all(|o| o.batch_record().result_code == Some(ResultCode::Ok)));
    }
    report(label, keys.len(), samples);
}

#[aerospike_macro::test]
#[ignore = "perf harness, not a correctness test: needs a live server and prints timings; run with --ignored"]
async fn perf_e2e_batch_read() {
    let client = common::client().await;
    let namespace = common::namespace();
    for n in [1_000usize, 5_000] {
        let set_name = common::rand_str(10);
        let keys = seed(&client, namespace, &set_name, n).await;
        time_reads(&client, &keys, Concurrency::Parallel, "parallel").await;
        time_reads(&client, &keys, Concurrency::Sequential, "sequential").await;
    }
}

/// Throughput under concurrency: `IN_FLIGHT` batches issued together, repeated
/// for `ROUNDS`. This is the metric that resolves a client-side change.
#[aerospike_macro::test]
#[ignore = "perf harness, not a correctness test: needs a live server and prints timings; run with --ignored"]
async fn perf_e2e_batch_throughput() {
    const IN_FLIGHT: usize = 16;
    const ROUNDS: usize = 12;
    const N: usize = 1_000;

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let keys = seed(&client, namespace, &set_name, N).await;
    let policy = BatchPolicy::default();

    let fire = |keys: &[Key]| {
        let mut ops = read_ops(keys);
        let client = &client;
        let policy = &policy;
        async move { client.batch(policy, &mut ops).await.map(|()| ops.len()) }
    };
    futures::future::join_all((0..IN_FLIGHT).map(|_| fire(&keys))).await;

    let start = Instant::now();
    for _ in 0..ROUNDS {
        for r in futures::future::join_all((0..IN_FLIGHT).map(|_| fire(&keys))).await {
            assert_eq!(r.expect("batch read"), N);
        }
    }
    let el = start.elapsed().as_secs_f64();
    let batches = (IN_FLIGHT * ROUNDS) as f64;
    println!(
        "PERF thru {N} keys, {IN_FLIGHT} in flight x{ROUNDS} rounds: {:.1} batches/s, {:.0} keys/s, {:.3}ms amortised per batch",
        batches / el, batches * N as f64 / el, el * 1000.0 / batches
    );
}

/// Like `perf_e2e_batch_throughput`, but each in-flight batch runs on its own
/// task, as it would with one request handler per caller.
#[cfg(feature = "rt-tokio")]
#[aerospike_macro::test]
#[ignore = "perf harness, not a correctness test: needs a live server and prints timings; run with --ignored"]
async fn perf_e2e_batch_throughput_spawned() {
    const IN_FLIGHT: usize = 16;
    const ROUNDS: usize = 12;
    const N: usize = 1_000;

    let client = Arc::new(common::client().await);
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let keys: Arc<Vec<Key>> = Arc::new(seed(&client, namespace, &set_name, N).await);
    let policy = Arc::new(BatchPolicy::default());

    let fire = |client: &Arc<Client>, keys: &Arc<Vec<Key>>, policy: &Arc<BatchPolicy>| {
        let (c, k, p) = (client.clone(), keys.clone(), policy.clone());
        tokio::spawn(async move {
            let mut ops = read_ops(&k);
            c.batch(&p, &mut ops).await.map(|()| ops.len())
        })
    };
    for h in (0..IN_FLIGHT).map(|_| fire(&client, &keys, &policy)) {
        h.await.expect("join").expect("warm-up");
    }
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let handles: Vec<_> = (0..IN_FLIGHT).map(|_| fire(&client, &keys, &policy)).collect();
        for h in handles {
            assert_eq!(h.await.expect("join").expect("batch read"), N);
        }
    }
    let el = start.elapsed().as_secs_f64();
    let batches = (IN_FLIGHT * ROUNDS) as f64;
    println!(
        "PERF thru-spawned {N} keys, {IN_FLIGHT} in flight x{ROUNDS} rounds: {:.1} batches/s, {:.0} keys/s, {:.3}ms amortised per batch",
        batches / el, batches * N as f64 / el, el * 1000.0 / batches
    );
}
