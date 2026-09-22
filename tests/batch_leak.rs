// Memory-leak checks for the batch path, in a binary of their own so the
// counting allocator below does not sit under every other test.
//
// A "leak" here means memory that is never freed (live bytes climb with every
// batch), an `Arc` cycle, or allocation counts that grow per iteration. Many
// random batches are run; the live total must come back to where it started
// and stay flat across the run. Needs a live cluster.

extern crate env_logger;
#[macro_use]
extern crate lazy_static;
extern crate rand;
#[cfg(feature = "tls")]
extern crate tokio_rustls;

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering::Relaxed};

use aerospike::policy::Replica;
use aerospike::*;
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as isize, Relaxed);
            ALLOCATIONS.fetch_add(1, Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE_BYTES.fetch_sub(layout.size() as isize, Relaxed);
    }
}
#[global_allocator]
static GLOBAL: Counting = Counting;

fn live() -> isize {
    LIVE_BYTES.load(Relaxed)
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

const V: &str = "v";

fn random_bins(r: &mut Rng) -> Bins {
    match r.below(3) {
        0 => Bins::from([V]),
        1 => Bins::from([V, "absent"]),
        _ => Bins::All,
    }
}

async fn seed_keys(client: &Client, namespace: &str, n: usize) -> Vec<Key> {
    let set = common::rand_str(10);
    let keys: Vec<Key> = (0..n).map(|i| as_key!(namespace, &set, i as i64)).collect();
    let bwp = BatchWritePolicy::default();
    for chunk in keys.chunks(500) {
        let mut writes: Vec<_> = chunk
            .iter()
            .enumerate()
            .map(|(i, k)| BatchOperation::write(&bwp, k.clone(), vec![operations::put(&as_bin!(V, i as i64))]))
            .collect();
        client.batch(&BatchPolicy::default(), &mut writes).await.expect("seed write");
    }
    keys
}

/// A random read batch: random size, random bin list per row, one row in six
/// unroutable, Master or Sequence at random.
fn random_batch(r: &mut Rng, keys: &[Key]) -> (BatchPolicy, Vec<BatchOperation>) {
    let brp = BatchReadPolicy::default();
    let n = 1 + r.below(keys.len());
    let bogus = format!("nx_{}", common::rand_str(4));
    let ops = (0..n)
        .map(|i| {
            let key = if i > 0 && r.below(6) == 0 { as_key!(bogus.as_str(), &keys[i].set_name, i as i64) } else { keys[i].clone() };
            BatchOperation::read(&brp, key, random_bins(r))
        })
        .collect();
    let mut policy = BatchPolicy::default();
    policy.replica = if r.below(2) == 0 { Replica::Master } else { Replica::Sequence };
    (policy, ops)
}

/// Minimum live-byte reading over a short window, so background allocation
/// caught mid-flight does not read as a leak.
async fn settled_live() -> isize {
    let mut best = live();
    for _ in 0..5 {
        sleep(Duration::from_millis(200)).await;
        best = best.min(live());
    }
    best
}

const KIB: isize = 1024;

#[derive(Default)]
struct Run {
    live_samples: Vec<isize>,
    allocs_per_row: Vec<f64>,
}
impl Run {
    fn record(&mut self, allocs_before: usize, rows: usize) {
        let allocs = ALLOCATIONS.load(Relaxed) - allocs_before;
        self.live_samples.push(live());
        self.allocs_per_row.push(allocs as f64 / rows as f64);
    }
}

fn assert_flat(label: &str, live_before: isize, live_after: isize, run: &Run) {
    let n = run.live_samples.len();
    let window = 15.min(n / 2).max(1);
    let mean = |s: &[isize]| s.iter().map(|&x| x as f64).sum::<f64>() / s.len() as f64;
    let growth = (mean(&run.live_samples[n - window..]) - mean(&run.live_samples[..window])) as isize;
    let spread = run.live_samples.iter().max().unwrap() - run.live_samples.iter().min().unwrap();
    let mut apr = run.allocs_per_row.clone();
    apr.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let (median_apr, max_apr) = (apr[apr.len() / 2], *apr.last().unwrap());
    println!(
        "LEAK {label}: live before {} KiB, after {} KiB (delta {} KiB); trend first{window}->last{window} {} KiB; spread {} KiB; allocs/row median {median_apr:.1} max {max_apr:.1}",
        live_before / KIB, live_after / KIB, (live_after - live_before) / KIB, growth / KIB, spread / KIB
    );
    // A leaked row set (one 300-row op vector is ~170 KiB) blows through any
    // of these within a handful of batches.
    assert!(live_after - live_before <= 256 * KIB, "{label}: live bytes did not return to baseline (+{} KiB)", (live_after - live_before) / KIB);
    assert!(growth <= 256 * KIB, "{label}: live bytes trend upward across the run (+{} KiB)", growth / KIB);
    assert!(max_apr <= 400.0, "{label}: allocations per row exploded ({max_apr:.0}/row)");
}

#[aerospike_macro::test]
async fn batch_does_not_leak_memory_or_grow_allocations() {
    let client = common::client().await;
    let namespace = common::namespace();
    let keys = seed_keys(&client, namespace, 300).await;
    let mut r = Rng(0xBEEF_F00D);

    for _ in 0..30 {
        let (p, mut ops) = random_batch(&mut r, &keys);
        client.batch(&p, &mut ops).await.expect("warm-up");
    }
    let live_before = settled_live().await;
    let mut run = Run::default();
    for _ in 0..60 {
        let (p, mut ops) = random_batch(&mut r, &keys);
        let rows = ops.len();
        let allocs_before = ALLOCATIONS.load(Relaxed);
        client.batch(&p, &mut ops).await.expect("batch");
        drop(ops);
        run.record(allocs_before, rows);
    }
    let live_after = settled_live().await;
    assert_flat("mixed batches", live_before, live_after, &run);
}

/// The retry re-split allocates its own structures on every retry. Trip the
/// breaker on one node so every batch takes that path.
#[aerospike_macro::test]
async fn retry_resplit_path_does_not_leak() {
    let mut policy = common::client_policy().clone();
    policy.max_error_rate = 1;
    policy.error_rate_window = 10_000;
    let client = Client::new(&policy, &common::hosts()).await.expect("connect");
    let namespace = common::namespace();
    if client.cluster.nodes().len() < 2 {
        println!("SKIP: retry re-split needs >= 2 nodes");
        client.close().await.unwrap();
        return;
    }
    let keys = seed_keys(&client, namespace, 300).await;
    let tripped = client.cluster.nodes()[0].clone();
    for _ in 0..16 {
        tripped.incr_error_rate();
    }
    let mut bpolicy = BatchPolicy::default();
    bpolicy.replica = Replica::Sequence;
    bpolicy.base_policy.max_retries = 3;
    bpolicy.base_policy.sleep_between_retries = 0;
    bpolicy.base_policy.total_timeout = 10_000;

    let brp = BatchReadPolicy::default();
    let mut r = Rng(0x5EED_5EED);
    let make = |r: &mut Rng| -> Vec<BatchOperation> {
        let n = 1 + r.below(keys.len());
        keys[..n].iter().map(|k| BatchOperation::read(&brp, k.clone(), random_bins(r))).collect()
    };
    for _ in 0..10 {
        let mut ops = make(&mut r);
        client.batch(&bpolicy, &mut ops).await.expect("warm-up through retry");
    }
    let live_before = settled_live().await;
    let mut run = Run::default();
    for _ in 0..40 {
        let mut ops = make(&mut r);
        let rows = ops.len();
        let allocs_before = ALLOCATIONS.load(Relaxed);
        client.batch(&bpolicy, &mut ops).await.expect("retry batch");
        assert!(ops.iter().all(|o| o.batch_record().result_code == Some(ResultCode::Ok)));
        drop(ops);
        run.record(allocs_before, rows);
    }
    let live_after = settled_live().await;
    assert_flat("retry re-split", live_before, live_after, &run);
    client.close().await.unwrap();
}
