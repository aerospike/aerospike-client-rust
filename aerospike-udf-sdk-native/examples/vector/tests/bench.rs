// Copyright 2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! What an exact top-k costs, and what it saves.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… AEROSPIKE_USE_SERVICES_ALTERNATE=true cargo test --test bench -- --ignored --nocapture
//! ```
//!
//! Two comparisons, and they answer different questions.
//!
//! **Server-side against client-side** is the one that matters: the same top-k, computed in the server versus
//! computed by fetching every vector and sorting locally. Both are exact; only one of them ships 150 MB.
//!
//! **Lane-parallel against one accumulator** prices the unaligned kernels. Same records, same accumulator, same
//! server — the only difference is which kernel `Metric::measure` calls. It used to price `simsimd` against a hand
//! loop, and did say the gap was small (1.2×); the packed-blob path no longer reaches `simsimd` at all, because a
//! borrowed particle cannot be cast to `&[f32]`, so the comparison moved to the one that replaced it.

use std::env;
use std::time::Instant;

use aerospike_core::{
    as_bin, as_key, as_val, AdminPolicy, Bins, Client, ClientPolicy, PartitionFilter, QueryPolicy,
    Statement, Value, WritePolicy,
};
use aerospike_udf_example_vector::heap::{BestK, Hit};
use aerospike_udf_example_vector::vec::{AlignedBuf, DType, Measurer, Metric};
use aerospike_udf_sdk_native::Blob;
use futures::StreamExt;

/// 768 dimensions, which is what a sentence embedding usually is — and the size at which the arithmetic starts
/// to matter more than the record read.
const DIM: usize = 768;

/// Fewer records than the other benches, because each one is 3 KB rather than 30 bytes.
///
/// Override with `AEROSPIKE_BENCH_RECORDS` to check that the per-record cost is flat — which is the only thing a
/// bigger run can tell you that this one cannot. Note the arithmetic: at 768 dimensions every record is 3 KB, so a
/// million of them is **3 GB** in the namespace *and* another 3 GB in this process for the in-memory floor.
fn records() -> i64 {
    std::env::var("AEROSPIKE_BENCH_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000)
}

const LOAD_CONCURRENCY: usize = 64;

/// Set once at the start of the run so every row reports against the same count.
static RECORD_COUNT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(50_000);

static PATHS: [std::sync::atomic::AtomicU64; 2] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

fn n() -> i64 {
    RECORD_COUNT.load(std::sync::atomic::Ordering::Relaxed)
}

fn module() -> String {
    env::var("AEROSPIKE_VECTOR_MODULE").unwrap_or_else(|_| "vector".to_string())
}

async fn connect() -> Option<Client> {
    let mut policy = ClientPolicy::default();

    policy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
        .unwrap_or(false);

    Client::new(&policy, &env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".into()))
        .await
        .ok()
}

fn vector_for(i: i64) -> Vec<f32> {
    (0..DIM)
        .map(|d| ((i as f32) * 0.37 + (d as f32) * 1.11).sin())
        .collect()
}

fn pack(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Time a server-side top-k. Returns milliseconds and how many hits crossed the wire.
async fn server_topk(
    client: &Client,
    namespace: &str,
    set: &str,
    query: &[f32],
    metric: &str,
    k: i64,
) -> (f64, usize) {
    let args = [
        as_val!("v"),
        Value::Blob(pack(query)),
        as_val!(metric),
        as_val!(k),
    ];

    let mut statement = Statement::new(namespace, set, Bins::All);

    statement.set_aggregate_function(&module(), "topk", Some(&args));

    let started = Instant::now();

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut hits = 0usize;
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        for (_, value) in record.unwrap().bins.iter() {
            let map: std::collections::HashMap<Value, Value> = match value {
                Value::HashMap(m) => m.clone(),
                Value::OrderedMap(m) => m.clone().into_iter().collect(),
                Value::SortedMap(m) => m.clone().into_iter().collect(),
                other => panic!("wanted a map partial, got {other:?}"),
            };

            if let Some(Value::List(list)) = map.get(&as_val!("hits")) {
                hits += list.len();
            }

            // Whether the zero-copy path is being taken at all - the claim the whole blob layout rests on.
            for (key, slot) in [("zero_copy", 0usize), ("copied", 1usize)] {
                if let Some(Value::Int(v)) = map.get(&as_val!(key)) {
                    PATHS[slot].fetch_add(*v as u64, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    (started.elapsed().as_secs_f64() * 1000.0, hits)
}

async fn best_of(
    rounds: usize,
    client: &Client,
    namespace: &str,
    set: &str,
    query: &[f32],
    metric: &str,
    k: i64,
) -> (f64, usize) {
    let mut best = (f64::MAX, 0);

    for _ in 0..rounds {
        let r = server_topk(client, namespace, set, query, metric, k).await;

        if r.0 < best.0 {
            best = r;
        }
    }

    best
}

#[tokio::test]
#[ignore]
async fn topk_bench() {
    // **Refuse to report debug-mode numbers**, because this file already produced a wrong headline that way.
    //
    // The client-side comparison row is a distance loop written in this test. Compiled without optimisation it
    // measured 2,182 ms; with `--release` it measures 127 ms. The server-side row barely moved, so the reported
    // "96x faster than sorting on the client" was almost entirely the harness handicapping its own control. The
    // real figure is around 7x.
    //
    // A benchmark that silently reports that is worse than no benchmark, so this one stops.
    assert!(
        !cfg!(debug_assertions),
        "run this with --release: an unoptimised comparison loop overstates the server by an order of magnitude"
    );

    RECORD_COUNT.store(records(), std::sync::atomic::Ordering::Relaxed);

    let Some(client) = connect().await else {
        eprintln!("skipping topk_bench: no server");
        return;
    };

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("vec_bench_{}", std::process::id());
    let write = WritePolicy::default();
    let probe = as_key!(namespace.as_str(), set.as_str(), 1i64);

    client
        .put(&write, &probe, &[as_bin!("v", Value::Blob(pack(&vector_for(1))))])
        .await
        .unwrap();

    if client
        .execute_udf(&write, &probe, &module(), "vec_dim", Some(&[as_val!("v")]))
        .await
        .is_err()
    {
        eprintln!("skipping topk_bench: no usable module '{}'", module());
        return;
    }

    eprintln!(
        "loading {} records of {DIM} dimensions ({:.0} MB of vectors)...",
        n(),
        (n() as f64 * DIM as f64 * 4.0) / 1_048_576.0
    );

    let started = Instant::now();

    futures::stream::iter(1..=n())
        .for_each_concurrent(LOAD_CONCURRENCY, |i| {
            let client = &client;
            let write = &write;
            let namespace = namespace.as_str();
            let set = set.as_str();

            async move {
                let key = as_key!(namespace, set, i);

                client
                    .put(
                        write,
                        &key,
                        &[as_bin!("v", Value::Blob(pack(&vector_for(i))))],
                    )
                    .await
                    .unwrap();
            }
        })
        .await;

    eprintln!("loaded in {:.1} s\n", started.elapsed().as_secs_f64());

    let query = vector_for(7);

    //------------------------------------------------
    // Server-side, and what the kernel is worth.
    //

    eprintln!("=== exact top-k over {} records of {DIM} dimensions ===\n", n());
    eprintln!(
        "  {:<44} {:>8}  {:>13}  {:>10}",
        "how", "ms", "records/sec", "hits on wire"
    );

    let mut simd_ms = f64::NAN;

    // The vendor kernel against a hand-written scalar loop, on the same records through the same accumulator - the
    // only difference being which function `Measurer` resolved to. Both read the same aligned bytes.
    for (label, metric, k) in [
        ("server, vendor cosine, k=10", "cosine", 10i64),
        ("server, vendor cosine, k=100", "cosine", 100),
        ("server, scalar cosine, k=10", "cosine_naive", 10),
    ] {
        let (ms, hits) = best_of(3, &client, &namespace, &set, &query, metric, k).await;

        if metric == "cosine" && k == 10 {
            simd_ms = ms;
        }

        eprintln!(
            "  {:<44} {:>8.0}  {:>13.0}  {:>10}",
            label,
            ms,
            n() as f64 / (ms / 1000.0),
            hits
        );
    }

    //------------------------------------------------
    // Where the per-record time goes.
    //
    // Each probe does strictly less than the next, so the differences attribute the cost. Nothing here is a
    // guess about what the server is doing - it is the same accumulator machinery with one stage removed at a
    // time.
    //

    {
        let zero = PATHS[0].load(std::sync::atomic::Ordering::Relaxed);
        let copied = PATHS[1].load(std::sync::atomic::Ordering::Relaxed);
        let total = (zero + copied).max(1);

        eprintln!(
            "\n  read in place for {:.1}% of records ({zero} zero-copy, {copied} copied into a scratch buffer)",
            100.0 * zero as f64 / total as f64
        );
    }

    eprintln!("\n=== attributing the per-record cost ===\n");
    eprintln!("  {:<44} {:>8}  {:>12}  {:>10}", "stage", "ms", "ns/record", "delta ns");

    let stage_args = [
        as_val!("v"),
        Value::Blob(pack(&query)),
        as_val!("cosine"),
        as_val!("f32"),
    ];

    let mut previous_ns = 0.0f64;
    let mut stage_ns: Vec<(String, f64)> = Vec::new();

    for (label, function) in [
        ("query + record pipeline", "probe_count"),
        ("+ read the blob bin", "probe_read"),
        ("+ align or copy, then measure", "probe_dist"),
    ] {
        let mut best = f64::MAX;

        for _ in 0..3 {
            let mut statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

            statement.set_aggregate_function(&module(), function, Some(&stage_args));

            let started = Instant::now();
            let records = client
                .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
                .await
                .unwrap();

            let mut stream = records.into_stream();

            while let Some(r) = stream.next().await {
                r.unwrap();
            }

            let ms = started.elapsed().as_secs_f64() * 1000.0;

            if ms < best {
                best = ms;
            }
        }

        let ns = best * 1_000_000.0 / n() as f64;

        eprintln!(
            "  {:<44} {:>8.0}  {:>12.0}  {:>10}",
            label,
            best,
            ns,
            if previous_ns == 0.0 {
                "-".to_string()
            } else {
                format!("{:+.0}", ns - previous_ns)
            }
        );

        previous_ns = ns;
        stage_ns.push((label.to_string(), ns));
    }

    // And the unconditional-copy probe, which prices the fallback whether or not it is taken.
    {
        let mut best = f64::MAX;

        for _ in 0..3 {
            let mut statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

            statement.set_aggregate_function(&module(), "probe_copy", Some(&stage_args));

            let started = Instant::now();
            let records = client
                .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
                .await
                .unwrap();

            let mut stream = records.into_stream();

            while let Some(r) = stream.next().await {
                r.unwrap();
            }

            let ms = started.elapsed().as_secs_f64() * 1000.0;

            if ms < best {
                best = ms;
            }
        }

        let ns = best * 1_000_000.0 / n() as f64;
        let read_ns = stage_ns.get(1).map(|(_, v)| *v).unwrap_or(0.0);

        eprintln!(
            "  {:<44} {:>8.0}  {:>12.0}  {:>10}",
            "read + an unconditional memcpy",
            best,
            ns,
            format!("{:+.0}", ns - read_ns)
        );
        eprintln!(
            "  {:<44} {:>8}  {:>12}  {:>10}",
            "  (that delta is what the fallback costs)", "", "", ""
        );
    }

    let full_ns = simd_ms * 1_000_000.0 / n() as f64;
    let dist_ns = stage_ns.last().map(|(_, v)| *v).unwrap_or(0.0);

    eprintln!(
        "  {:<44} {:>8.0}  {:>12.0}  {:>10}",
        "+ the bounded heap (= topk)",
        simd_ms,
        full_ns,
        format!("{:+.0}", full_ns - dist_ns)
    );

    //------------------------------------------------
    // The floor: the same arithmetic with no database under it.
    //
    // **The same code, not an equivalent of it.** This crate builds as both a `cdylib` and a `lib`, so the loop
    // below goes through the plugin's own `Measurer` and `BestK` - the same kernel, the same bounded heap, the
    // same digest tiebreak, the same cheap rejection. Only the source of the bytes changes. Whatever this costs
    // is arithmetic no amount of server work could avoid.
    //
    // Two layouts, because the difference between them is itself an answer:
    //
    //   - **contiguous**: one allocation, stride `DIM * 4`. The best case a CPU can be given.
    //   - **scattered**: one allocation per vector, closer to how records actually sit in memory.
    //

    struct Row {
        digest: [u8; 20],
        vector: Vec<u8>,
    }

    let measurer = Measurer::new(Metric::Cosine, DType::F32, &pack(&query)).unwrap();

    let scattered: Vec<Row> = (1..=n())
        .map(|i| Row {
            digest: {
                let mut d = [0u8; 20];

                d[..8].copy_from_slice(&i.to_le_bytes());
                d
            },
            vector: pack(&vector_for(i)),
        })
        .collect();

    // The contiguous copy is a second full copy of the data, and it measured 1.01x against scattered - so past a
    // few hundred thousand records it costs gigabytes to re-learn nothing. Skipped, and said so.
    let stride = DIM * core::mem::size_of::<f32>();
    let want_contiguous = n() <= 200_000;
    let mut flat: Vec<u8> = Vec::new();

    if want_contiguous {
        flat.reserve(n() as usize * stride);

        for row in &scattered {
            flat.extend_from_slice(&row.vector);
        }
    } else {
        eprintln!(
            "  (skipping the contiguous layout row: a second {:.1} GB copy to re-measure a 1.01x difference)",
            (n() as f64 * stride as f64) / 1_073_741_824.0
        );
    }

    let mut floor_rows: Vec<(String, f64)> = Vec::new();

    let layouts: &[(&str, bool)] = if want_contiguous {
        &[("in memory, contiguous", true), ("in memory, scattered", false)]
    } else {
        &[("in memory, scattered", false)]
    };

    for &(label, contiguous) in layouts {
        let mut best_ms = f64::MAX;
        let mut kept = 0usize;

        for _ in 0..3 {
            let started = Instant::now();

            let mut best = BestK::new(10);
            let mut scratch = AlignedBuf::default();

            for (i, row) in scattered.iter().enumerate() {
                let bytes = if contiguous {
                    &flat[i * stride..(i + 1) * stride]
                } else {
                    row.vector.as_slice()
                };

                let d = measurer.measure_bytes(bytes, &mut scratch).unwrap();
                let score = Metric::Cosine.score(d);

                // The same rejection `accumulate` does: a candidate that cannot beat the worst kept hit never
                // has its identity built.
                if let Some(worst) = best.threshold() {
                    if score >= worst {
                        continue;
                    }
                }

                best.offer(Hit {
                    score,
                    digest: Blob(row.digest.to_vec()),
                    key: String::new(),
                });
            }

            let ms = started.elapsed().as_secs_f64() * 1000.0;

            if ms < best_ms {
                best_ms = ms;
                kept = best.len();
            }
        }

        assert_eq!(kept, 10, "{label}: the in-memory heap must keep k");

        eprintln!(
            "  {:<44} {:>8.1}  {:>13.0}  {:>10}",
            label,
            best_ms,
            n() as f64 / (best_ms / 1000.0),
            10
        );

        floor_rows.push((label.to_string(), best_ms));
    }

    let floor_ms = floor_rows.iter().map(|(_, ms)| *ms).fold(f64::MAX, f64::min);

    //------------------------------------------------
    // The alternative: fetch everything and sort here.
    //

    let started = Instant::now();
    let mut scanned = 0usize;
    let mut best: Vec<(f64, usize)> = Vec::new();

    // A plain query with no aggregation is a scan of the set - which is exactly the alternative being priced.
    let statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.unwrap();

        let Some(Value::Blob(bytes)) = record.bins.get("v") else {
            continue;
        };

        let v: Vec<f32> = bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();

        // The same arithmetic, on this side of the wire.
        let mut dot = 0.0f64;
        let mut na = 0.0f64;
        let mut nb = 0.0f64;

        for (x, y) in query.iter().zip(v.iter()) {
            dot += (*x as f64) * (*y as f64);
            na += (*x as f64) * (*x as f64);
            nb += (*y as f64) * (*y as f64);
        }

        best.push((1.0 - dot / (na.sqrt() * nb.sqrt()), scanned));
        scanned += 1;
    }

    best.sort_by(|a, b| a.0.total_cmp(&b.0));
    best.truncate(10);

    let client_ms = started.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(scanned as i64, n(), "the scan lost records");

    eprintln!(
        "  {:<44} {:>8.0}  {:>13.0}  {:>10}",
        "client, fetch every vector and sort",
        client_ms,
        n() as f64 / (client_ms / 1000.0),
        n()
    );

    eprintln!(
        "\n  server-side is {:.0}x faster than fetching and sorting, shipping 10 hits instead of {} \
         vectors ({:.0} MB).",
        client_ms / simd_ms,
        n(),
        (n() as f64 * DIM as f64 * 4.0) / 1_048_576.0
    );

    eprintln!(
        "\n  the same kernel and heap over a Vec of structs costs {floor_ms:.1} ms - so the entire database,\n\
         \x20 reading {} records off storage plus query threads, partials and the wire, costs {:.1}x the\n\
         \x20 arithmetic ({:.0} ns per record against {:.0} ns).",
        n(),
        simd_ms / floor_ms,
        simd_ms * 1_000_000.0 / n() as f64,
        floor_ms * 1_000_000.0 / n() as f64
    );

    if let (Some((_, a)), Some((_, b))) = (floor_rows.first(), floor_rows.get(1)) {
        eprintln!(
            "  scattering the vectors rather than packing them costs {:.2}x on its own.",
            b / a
        );
    }

    // Truncated for the same reason the other benches truncate: a set-scoped query with no set index walks the
    // namespace's whole primary index, so records left behind make every later run slower.
    if let Err(e) = client
        .truncate(&AdminPolicy::default(), &namespace, &set, 0)
        .await
    {
        eprintln!("\nnote: could not truncate {set} ({e})");
    }
}
