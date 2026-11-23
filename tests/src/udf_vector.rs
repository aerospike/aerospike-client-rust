// Copyright 2015-2026 Aerospike, Inc.
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

//! The vector top-k as a WASM UDF in three languages, against each other, Lua and (when configured) the
//! native module.
//!
//! The three modules - Rust (`aerospike-udf-sdk/wasm-rust/examples/vector`), C
//! (`aerospike-udf-sdk/wasm-c/examples/vector`) and Go (`aerospike-udf-sdk/wasm-go/examples/vector`) - are
//! written to one algorithm, step for step, so that a difference between their rows is the language and its
//! toolchain and nothing else. The checked-in binaries under `tests/fixtures/wasm/` are built from them; the
//! READMEs beside each say how.
//!
//! Two tests. The first runs in the suite and asserts **exactness**: every engine's top-k equals the one
//! computed here from the same vectors, digests in order, for three metrics. The second is `#[ignore]`d and
//! measures, over 768-dimension vectors:
//!
//! ```text
//! AEROSPIKE_BENCH_RECORDS=50000 cargo test --features serialization,rt-tokio,async vector_wasm_bench -- --ignored --nocapture
//! ```
//!
//! Both self-skip on a server without WASM. The native row needs `mod-native { module vector { ... } }` in
//! the server's configuration and is named by `AEROSPIKE_VECTOR_MODULE` (default `vector`).

use std::collections::HashMap;
use std::time::Instant;

use crate::common;

use aerospike::*;
use futures::StreamExt;

const RUST: &[u8] = include_bytes!("../fixtures/wasm/vector_rust.wasm");
const C: &[u8] = include_bytes!("../fixtures/wasm/vector_c.wasm");
const GO: &[u8] = include_bytes!("../fixtures/wasm/vector_go.wasm");
const JAVA: &[u8] = include_bytes!("../fixtures/wasm/vector_java.wasm");
const CPP: &[u8] = include_bytes!("../fixtures/wasm/vector_cpp.wasm");
const LUA: &str = include_str!("../../aerospike-udf-sdk-native/examples/vector/lua/vector.lua");

/// The three WASM modules and the Lua script, in the order the tables print them.
const ENGINES: [(&str, Option<&[u8]>); 6] = [
    ("lua", None),
    ("wasm rust", Some(RUST)),
    ("wasm c", Some(C)),
    ("wasm go", Some(GO)),
    ("wasm java", Some(JAVA)),
    ("wasm c++", Some(CPP)),
];

const LOAD_CONCURRENCY: usize = 64;

fn bench_records() -> i64 {
    std::env::var("AEROSPIKE_BENCH_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000)
}

fn native_module() -> String {
    std::env::var("AEROSPIKE_VECTOR_MODULE").unwrap_or_else(|_| "vector".to_string())
}

fn skipped(test: &str, reason: &str) {
    eprintln!("skipping {test}: {reason}");
}

/// The same generator the native example's benches use, so a run here is comparable with a run there.
fn vector_for(i: i64, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|d| ((i as f32) * 0.37 + (d as f32) * 1.11).sin())
        .collect()
}

fn pack(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// A sum of `f(i)` over the elements the way the three WASM modules sum: four `f64` accumulators, lane `k`
/// holding the elements whose index is `k` mod 4, folded in one fixed order. The modules write their loops
/// that way so a SIMD build can keep the sums in vector registers; this mirrors it so the expected scores are
/// bit-identical to theirs.
fn lane_sum(n: usize, f: impl Fn(usize) -> f64) -> f64 {
    let mut lanes = [0.0f64; 4];

    for i in 0..n {
        lanes[i % 4] += f(i);
    }

    ((lanes[0] + lanes[1]) + lanes[2]) + lanes[3]
}

/// The score every engine ranks by, computed the way the three WASM modules do it: larger is worse (`dot`
/// negated), and summed as [`lane_sum`] does.
fn score(metric: &str, v: &[f32], q: &[f32]) -> f64 {
    let n = v.len();

    match metric {
        "cosine" => {
            let dot = lane_sum(n, |i| f64::from(v[i]) * f64::from(q[i]));
            let na = lane_sum(n, |i| f64::from(v[i]) * f64::from(v[i]));
            let nb: f64 = q.iter().map(|b| f64::from(*b) * f64::from(*b)).sum();

            if na == 0.0 || nb == 0.0 {
                1.0
            } else {
                1.0 - dot / (na.sqrt() * nb.sqrt())
            }
        }
        "dot" => -lane_sum(n, |i| f64::from(v[i]) * f64::from(q[i])),
        "sqeuclidean" => lane_sum(n, |i| {
            let d = f64::from(v[i]) - f64::from(q[i]);
            d * d
        }),
        other => panic!("no local kernel for {other}"),
    }
}

/// The expected top-k: (score, digest), best first, ties by digest - the ordering every engine uses.
fn expected_topk(namespace: &str, set: &str, records: i64, dim: usize, metric: &str, q: &[f32], k: usize) -> Vec<(f64, Vec<u8>)> {
    let mut all: Vec<(f64, Vec<u8>)> = (1..=records)
        .map(|i| {
            let key = as_key!(namespace, set, i);

            (score(metric, &vector_for(i, dim), q), key.digest().to_vec())
        })
        .collect();

    all.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    all.truncate(k);

    all
}

async fn load(client: &Client, namespace: &str, set: &str, records: i64, dim: usize) {
    let mut write = WritePolicy::default();

    write.base_policy.total_timeout = 20_000;
    write.base_policy.socket_timeout = 1_000;
    write.base_policy.max_retries = 10;

    futures::stream::iter(1..=records)
        .for_each_concurrent(LOAD_CONCURRENCY, |i| {
            let client = &client;
            let write = &write;

            async move {
                let key = as_key!(namespace, set, i);

                client
                    .put(write, &key, &[as_bin!("v", Value::Blob(pack(&vector_for(i, dim))))])
                    .await
                    .unwrap();
            }
        })
        .await;
}

/// Register one engine's module under `package`, and prove it runs by calling `vec_distance` once. `None`
/// when the server cannot run it.
async fn register(client: &Client, package: &str, module: Option<&[u8]>, probe: &Key, dim: usize) -> Option<()> {
    let policy = AdminPolicy::default();
    let registered = match module {
        None => {
            client
                .register_udf(&policy, LUA.as_bytes(), &format!("{package}.lua"), UdfLang::Lua)
                .await
                .ok()?
                .wait_till_complete(None)
                .await
                .ok()
        }
        Some(bytes) => {
            client
                .register_udf(&policy, bytes, &format!("{package}.wasm"), UdfLang::Wasm)
                .await
                .ok()?
                .wait_till_complete(None)
                .await
                .ok()
        }
    };

    registered?;

    let args = [Value::from("v"), Value::Blob(pack(&vector_for(1, dim))), Value::from("cosine")];

    client
        .execute_udf(&WritePolicy::default(), probe, package, "vec_distance", &args)
        .await
        .ok()
        .map(|_| ())
}

fn as_map_of(value: &Value) -> Option<HashMap<Value, Value>> {
    match value {
        Value::HashMap(m) => Some(m.clone()),
        Value::OrderedMap(m) => Some(m.clone().into_iter().collect()),
        Value::SortedMap(m) => Some(m.clone().into_iter().collect()),
        _ => None,
    }
}

fn as_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Float(f)) => Some(f64::from(f.clone())),
        Some(Value::Int(i)) => Some(*i as f64),
        _ => None,
    }
}

/// What one aggregation cost and answered.
#[derive(Default)]
struct Agg {
    ms: f64,
    partials: usize,
    hits_on_wire: usize,
    /// (score, digest), best first, merged and truncated here - the same merge for every engine.
    top: Vec<(f64, Vec<u8>)>,
    skipped: i64,
}

async fn run_agg(client: &Client, namespace: &str, set: &str, module: &str, args: &[Value], k: usize) -> Agg {
    let mut statement = Statement::new(namespace, set, Bins::All);

    statement.set_aggregate_function(module, "topk", args);

    let started = Instant::now();
    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut out = Agg::default();
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        for (_, value) in record.unwrap().bins.iter() {
            let Some(map) = as_map_of(value) else { continue };

            out.partials += 1;

            for field in ["skipped_dim", "skipped_none"] {
                if let Some(Value::Int(n)) = map.get(&Value::from(field)) {
                    out.skipped += n;
                }
            }

            let Some(Value::List(entries)) = map.get(&Value::from("hits")) else {
                continue;
            };

            out.hits_on_wire += entries.len();

            for entry in entries {
                let Some(entry) = as_map_of(entry) else { continue };
                let Some(s) = as_f64(entry.get(&Value::from("score")).or_else(|| entry.get(&Value::from("distance")))) else {
                    continue;
                };
                let digest = match entry.get(&Value::from("digest")) {
                    Some(Value::Blob(b)) => b.clone(),
                    _ => Vec::new(),
                };

                out.top.push((s, digest));
            }
        }
    }

    out.ms = started.elapsed().as_secs_f64() * 1000.0;
    out.top.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    out.top.truncate(k);

    out
}

fn topk_args(metric: &str, k: i64, query: &[f32]) -> Vec<Value> {
    vec![Value::from("v"), Value::Blob(pack(query)), Value::from(metric), Value::Int(k)]
}

fn hex(digest: &[u8]) -> String {
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// **The three WASM vector modules, Lua and the local computation all agree**, digest for digest, for every
/// metric. Exactness is the whole claim of a brute-force top-k; a module that disagreed at any rank would be
/// an approximate index with extra steps.
#[aerospike_macro::test]
async fn wasm_vector_modules_agree() {
    const DIM: usize = 8;
    const RECORDS: i64 = 500;
    const K: usize = 7;

    let client = common::client().await;
    let namespace = common::namespace();
    let set = &common::rand_str(10);
    let suffix = common::rand_str(6);

    load(&client, namespace, set, RECORDS, DIM).await;

    let probe = as_key!(namespace, set, 1i64);
    let mut packages: Vec<(&str, String)> = Vec::new();

    for (name, module) in ENGINES {
        let package = format!("vec_{}_{suffix}", name.replace(' ', "_"));

        if register(&client, &package, module, &probe, DIM).await.is_none() {
            if module.is_some() {
                skipped("wasm_vector_modules_agree", &format!("{name} does not run on this server"));
                return;
            }

            panic!("the Lua vector module did not register");
        }

        packages.push((name, package));
    }

    let query = vector_for(RECORDS + 7, DIM);

    for metric in ["cosine", "dot", "sqeuclidean"] {
        let want = expected_topk(namespace, set, RECORDS, DIM, metric, &query, K);
        let args = topk_args(metric, K as i64, &query);

        for (name, package) in &packages {
            let got = run_agg(&client, namespace, set, package, &args, K).await;

            assert_eq!(got.skipped, 0, "{name}/{metric}: nothing should be skipped");
            assert_eq!(got.top.len(), K, "{name}/{metric}: {K} hits");

            for (rank, (g, w)) in got.top.iter().zip(&want).enumerate() {
                assert_eq!(hex(&g.1), hex(&w.1), "{name}/{metric}: digest at rank {rank}");
                assert!(
                    (g.0 - w.0).abs() < 1e-9,
                    "{name}/{metric}: score at rank {rank}: {} vs {}",
                    g.0,
                    w.0
                );
            }
        }
    }

    let _ = client.truncate(&AdminPolicy::default(), namespace, set, 0).await;
}

/// **The same top-k, timed**: 768 dimensions, cosine, k = 10, best of three per engine.
#[aerospike_macro::test]
#[ignore]
async fn vector_wasm_bench() {
    const DIM: usize = 768;
    const K: usize = 10;
    const ROUNDS: usize = 3;

    let records = bench_records();
    let client = common::client().await;
    let namespace = common::namespace();
    let set = &common::rand_str(10);
    let suffix = common::rand_str(6);
    let probe = as_key!(namespace, set, 1i64);

    eprintln!("loading {records} records of {DIM} f32 ({} MB)...", records as usize * DIM * 4 / 1_000_000);

    let started = Instant::now();

    load(&client, namespace, set, records, DIM).await;
    eprintln!("loaded in {:.1} s\n", started.elapsed().as_secs_f64());

    let mut engines: Vec<(String, String)> = Vec::new();

    for (name, module) in ENGINES {
        let package = format!("vec_{}_{suffix}", name.replace(' ', "_"));

        match register(&client, &package, module, &probe, DIM).await {
            Some(()) => engines.push((name.to_string(), package)),
            None => eprintln!("note: {name} does not run here - no row"),
        }
    }

    // The native module, if the server has it configured.
    let native = native_module();
    let native_args = [Value::from("v")];

    if client
        .execute_udf(&WritePolicy::default(), &probe, &native, "vec_dim", &native_args)
        .await
        .is_ok()
    {
        engines.push((format!("native ({native})"), native.clone()));
    }

    let query = vector_for(records + 7, DIM);
    let want = expected_topk(namespace, set, records, DIM, "cosine", &query, K);
    let args = topk_args("cosine", K as i64, &query);

    eprintln!("=== topk cosine k={K} over {records} records of {DIM} dimensions, best of {ROUNDS} ===\n");

    let mut rows: Vec<(String, Agg)> = Vec::new();

    for (name, package) in &engines {
        let mut best: Option<Agg> = None;

        for _ in 0..ROUNDS {
            let agg = run_agg(&client, namespace, set, package, &args, K).await;

            if best.as_ref().is_none_or(|b| agg.ms < b.ms) {
                best = Some(agg);
            }
        }

        let best = best.unwrap();

        for (rank, (g, w)) in best.top.iter().zip(&want).enumerate() {
            assert_eq!(hex(&g.1), hex(&w.1), "{name}: digest at rank {rank}");
        }

        rows.push((name.clone(), best));
    }

    let fastest = rows.iter().map(|(_, r)| r.ms).fold(f64::MAX, f64::min);

    eprintln!(
        "  {:<18} {:>9}  {:>12}  {:>10}  {:>8}  {:>9}  {:>8}",
        "engine", "ms", "records/sec", "ns/record", "vs best", "partials", "hits"
    );

    for (name, r) in &rows {
        eprintln!(
            "  {:<18} {:>9.1}  {:>12.0}  {:>10.0}  {:>7.2}x  {:>9}  {:>8}",
            name,
            r.ms,
            records as f64 / (r.ms / 1000.0),
            r.ms * 1_000_000.0 / records as f64,
            r.ms / fastest,
            r.partials,
            r.hits_on_wire
        );
    }

    eprintln!("\n  every engine answered the same {K} digests; the rows differ only in time");

    // The per-call section: one `vec_distance` is one instance, one configuration and one record, so it
    // prices what every one of the 4096 streams above pays before its first record. The round trip is in
    // every row; the differences between rows are what matter.
    const CALLS: usize = 300;

    eprintln!("\n=== vec_distance, one record per call, {CALLS} calls per round, best of {ROUNDS} ===\n");

    let dist_args = [Value::from("v"), Value::Blob(pack(&query)), Value::from("cosine")];
    let mut per_call: Vec<(String, f64)> = Vec::new();

    for (name, package) in &engines {
        let mut best = f64::MAX;

        for _ in 0..ROUNDS {
            let started = Instant::now();

            for _ in 0..CALLS {
                client
                    .execute_udf(&WritePolicy::default(), &probe, package, "vec_distance", &dist_args)
                    .await
                    .unwrap();
            }

            best = best.min(started.elapsed().as_secs_f64() * 1_000_000.0 / CALLS as f64);
        }

        per_call.push((name.clone(), best));
    }

    let fastest = per_call.iter().map(|(_, us)| *us).fold(f64::MAX, f64::min);

    eprintln!("  {:<18} {:>9}  {:>8}", "engine", "us/call", "vs best");

    for (name, us) in &per_call {
        eprintln!("  {:<18} {:>9.1}  {:>7.2}x", name, us, us / fastest);
    }

    if let Err(e) = client.truncate(&AdminPolicy::default(), namespace, set, 0).await {
        eprintln!("note: could not truncate {set} ({e}) - a later run will be slower for it");
    }
}
