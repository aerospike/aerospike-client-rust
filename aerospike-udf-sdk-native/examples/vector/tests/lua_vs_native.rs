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

//! The same exact top-k in Lua and in the native module, on the same records.
//!
//! ```sh
//! # they agree
//! AEROSPIKE_HOSTS=… cargo test --test lua_vs_native -- --nocapture
//!
//! # what each of them costs
//! AEROSPIKE_HOSTS=… cargo test --release --test lua_vs_native -- --ignored --nocapture
//! ```
//!
//! # Why both halves are here
//!
//! A speed comparison between two implementations is worth nothing until they are shown to compute the same
//! answer, and "vector search in Lua" has an obvious way to be quietly wrong: Lua's `bytes` has no float getter,
//! so the packed vector has to be decoded by hand, and a decode that is subtly off produces a plausible-looking
//! top-k of the wrong records. So [`lua_and_native_agree`] runs first, over a small set, and requires the two
//! engines to return the *same digests in the same order* — and it is a normal test, not an `#[ignore]`d one.
//!
//! [`lua_vs_native_bench`] is the measurement, and it is `#[ignore]`d for the usual reason: it takes minutes and
//! it reports numbers rather than passing or failing.
//!
//! # What the rows separate
//!
//! The interesting question is not "how much slower is Lua" — it is *which part* is slower, because the answers
//! have different consequences:
//!
//! | row | isolates |
//! |---|---|
//! | `orderby` | one bin read and one comparison per record: engine overhead with no decode in it |
//! | `probe_read` − `probe_count` | `bytes.get_string`, the copy the native module doesn't make |
//! | `probe_unpack` − `probe_read` | `string.unpack` of 768 floats, which has no native counterpart at all |
//! | `topk` vs `topk_scan` | the same arithmetic, one call per vector against one call per element |
//!
//! Every row runs against the same records in the same server in the same run, so nothing here depends on the
//! two engines having been measured under comparable conditions — they were measured under identical ones.

use std::collections::HashMap;
use std::env;
use std::time::Instant;

use aerospike_core::{
    as_bin, as_key, as_val, AdminPolicy, Bins, Client, ClientPolicy, PartitionFilter, QueryPolicy,
    Statement, Task, UDFLang, Value, WritePolicy,
};
use futures::StreamExt;

/// The Lua module, registered from this file so the two are never out of step.
const LUA: &str = include_str!("../lua/vector.lua");

/// The name the Lua module is registered under. The native one is `vector`.
const LUA_MODULE: &str = "vector_lua";

/// 768 dimensions, as in the native bench — a sentence embedding, and 3 KB a record.
const DIM: usize = 768;

/// Sequential record-UDF calls for the per-call section.
const CALLS: usize = 200;

/// Every timed row is the best of this many, because the first round pays for whatever the server had not cached.
const ROUNDS: usize = 3;

fn records() -> i64 {
    env::var("AEROSPIKE_BENCH_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000)
}

const LOAD_CONCURRENCY: usize = 64;

fn native_module() -> String {
    env::var("AEROSPIKE_VECTOR_MODULE").unwrap_or_else(|_| "vector".to_string())
}

fn namespace() -> String {
    env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

async fn connect() -> Option<Client> {
    let policy = ClientPolicy {
        use_services_alternate: env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
            .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
            .unwrap_or(false),
        ..ClientPolicy::default()
    };

    Client::new(
        &policy,
        &env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".into()),
    )
    .await
    .ok()
}

/// The same generator the native bench uses, so a run here is comparable with a run there.
fn vector_for(i: i64) -> Vec<f32> {
    (0..DIM)
        .map(|d| ((i as f32) * 0.37 + (d as f32) * 1.11).sin())
        .collect()
}

fn pack(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Also stored, for `orderby`: a scalar whose ordering has nothing to do with the vectors.
fn price_for(i: i64) -> i64 {
    (i * 7919) % 100_003
}

async fn register_lua(client: &Client) -> bool {
    let Ok(task) = client
        .register_udf(
            &AdminPolicy::default(),
            LUA.as_bytes(),
            &format!("{LUA_MODULE}.lua"),
            UDFLang::Lua,
        )
        .await
    else {
        return false;
    };

    task.wait_till_complete(None).await.is_ok()
}

/// Whether the native module answers at all — it is loaded from the server's config, so it either exists or the
/// whole comparison is meaningless.
async fn native_usable(client: &Client, namespace: &str, set: &str) -> bool {
    let key = as_key!(namespace, set, 1i64);

    client
        .execute_udf(
            &WritePolicy::default(),
            &key,
            &native_module(),
            "vec_dim",
            Some(&[as_val!("v")]),
        )
        .await
        .is_ok()
}

async fn load(client: &Client, namespace: &str, set: &str, n: i64, with_list: bool) {
    let mut write = WritePolicy::default();

    // The default one-second total is too tight for a burst of 10 KB records, and the default 30-second socket
    // timeout is far too loose: on this client a few of the *first* concurrent writes - the ones that race to open
    // connections on an empty pool - never see a reply at all, and with a 30-second socket timeout they burn the
    // whole deadline waiting for it. A short socket timeout and room for retries makes the loader ride over that;
    // it is a client-side problem worth chasing on its own, and it is not what this file measures.
    write.base_policy.total_timeout = 20_000;
    write.base_policy.socket_timeout = 1_000;
    write.base_policy.max_retries = 10;

    // One write on its own before the burst. Without it, sixty-four concurrent writes against a cold connection
    // pool stall for tens of seconds and then time out - reproducibly, on this client and this server, and only
    // when the pool starts empty. Worth chasing separately; here it is one line and not what is being measured.
    {
        let key = as_key!(namespace, set, 0i64);

        client
            .put(&write, &key, &[as_bin!("warm", 1i64)])
            .await
            .unwrap();

        let _ = client.delete(&write, &key).await;
    }

    futures::stream::iter(1..=n)
        .for_each_concurrent(LOAD_CONCURRENCY, |i| {
            let client = &client;
            let write = &write;

            async move {
                let key = as_key!(namespace, set, i);
                let vector = vector_for(i);

                if with_list {
                    // The layout a Lua implementation would reach for first: a list of numbers, which needs no
                    // decoding and costs 8 bytes an element instead of 4.
                    let list = Value::List(vector.iter().map(|f| as_val!(*f as f64)).collect());

                    client
                        .put(
                            write,
                            &key,
                            &[
                                as_bin!("v", Value::Blob(pack(&vector))),
                                as_bin!("p", price_for(i)),
                                as_bin!("l", list),
                            ],
                        )
                        .await
                        .unwrap();
                } else {
                    client
                        .put(
                            write,
                            &key,
                            &[
                                as_bin!("v", Value::Blob(pack(&vector))),
                                as_bin!("p", price_for(i)),
                            ],
                        )
                        .await
                        .unwrap();
                }
            }
        })
        .await;
}

/// A partial as any of the map flavours, as one map.
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

fn as_i64(v: Option<&Value>) -> i64 {
    match v {
        Some(Value::Int(i)) => *i,
        _ => 0,
    }
}

fn as_bytes(v: Option<&Value>) -> Vec<u8> {
    match v {
        Some(Value::Blob(b)) => b.clone(),
        Some(Value::String(s)) => s.as_bytes().to_vec(),
        _ => Vec::new(),
    }
}

/// What one aggregation query cost and what it answered.
#[derive(Debug, Default)]
struct Agg {
    ms: f64,
    /// How many values the server sent — one per node for the native module, one per stream for Lua.
    partials: usize,
    /// How many hits those values carried in total, which is what the caller pays for on the wire.
    hits_on_wire: usize,
    /// The merged answer: (value, digest), best first.
    top: Vec<(f64, Vec<u8>)>,
    skipped_dim: i64,
    skipped_none: i64,
}

/// Run one aggregation and merge whatever it sends back.
///
/// The merge is done here rather than in either engine on purpose: the native module merges per node in its own
/// `merge` and Lua cannot (its stream ends at `reduce`), so doing it identically on this side keeps the timed
/// region the same for both — the server work plus the wire.
async fn run_agg(
    client: &Client,
    namespace: &str,
    set: &str,
    module: &str,
    function: &str,
    args: &[Value],
    k: usize,
) -> Agg {
    let mut statement = Statement::new(namespace, set, Bins::All);

    statement.set_aggregate_function(module, function, Some(args));

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
            out.skipped_dim += as_i64(map.get(&as_val!("skipped_dim")));
            out.skipped_none += as_i64(map.get(&as_val!("skipped_none")));

            // `hits` from a top-k, `rows` from an orderby.
            let list = map
                .get(&as_val!("hits"))
                .or_else(|| map.get(&as_val!("rows")));

            let Some(Value::List(entries)) = list else {
                continue;
            };

            out.hits_on_wire += entries.len();

            for entry in entries {
                let Some(entry) = as_map_of(entry) else { continue };

                // `score` in a Lua partial, `distance`/`value` in a finalized native answer. For every metric
                // used here they are the same number; `dot` is the one where they are not, and it is not used
                // for a comparison row.
                let Some(value) = as_f64(
                    entry
                        .get(&as_val!("score"))
                        .or_else(|| entry.get(&as_val!("distance")))
                        .or_else(|| entry.get(&as_val!("value"))),
                ) else {
                    continue;
                };

                out.top.push((value, as_bytes(entry.get(&as_val!("digest")))));
            }
        }
    }

    out.ms = started.elapsed().as_secs_f64() * 1000.0;

    // The same ordering both engines use internally: by value, ties by digest.
    out.top
        .sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    out.top.truncate(k);

    out
}

/// The best of [`ROUNDS`] rounds.
async fn best_of(
    client: &Client,
    namespace: &str,
    set: &str,
    module: &str,
    function: &str,
    args: &[Value],
    k: usize,
) -> Agg {
    let mut best: Option<Agg> = None;

    for _ in 0..ROUNDS {
        let agg = run_agg(client, namespace, set, module, function, args, k).await;

        if best.as_ref().is_none_or(|b| agg.ms < b.ms) {
            best = Some(agg);
        }
    }

    best.unwrap()
}

fn topk_args(metric: &str, k: i64, query: &[f32]) -> Vec<Value> {
    vec![
        as_val!("v"),
        Value::Blob(pack(query)),
        as_val!(metric),
        as_val!(k),
    ]
}

fn hex(digest: &[u8]) -> String {
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

//==========================================================
// Do they agree?
//

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lua_and_native_agree() {
    let Some(client) = connect().await else {
        eprintln!("skipping lua_and_native_agree: no server");
        return;
    };

    let namespace = namespace();
    let set = format!("vec_agree_{}", std::process::id());

    // Small enough to be a test rather than a benchmark, big enough that the top-10 is not the whole set and the
    // per-stream partials really do have to be merged.
    let n = 2_000i64;

    load(&client, &namespace, &set, n, true).await;

    if !native_usable(&client, &namespace, &set).await {
        eprintln!(
            "skipping lua_and_native_agree: the server has no native module '{}'",
            native_module()
        );

        return;
    }

    assert!(register_lua(&client).await, "could not register {LUA_MODULE}");

    let query = vector_for(7);
    let k = 10usize;

    for metric in ["cosine", "sqeuclidean", "euclidean"] {
        let args = topk_args(metric, k as i64, &query);

        let native = run_agg(
            &client,
            &namespace,
            &set,
            &native_module(),
            "topk",
            &args,
            k,
        )
        .await;

        let lua = run_agg(&client, &namespace, &set, LUA_MODULE, "topk", &args, k).await;

        assert_eq!(native.top.len(), k, "native {metric}: wrong number of hits");
        assert_eq!(lua.top.len(), k, "lua {metric}: wrong number of hits");

        for (rank, (want, got)) in native.top.iter().zip(lua.top.iter()).enumerate() {
            assert_eq!(
                hex(&want.1),
                hex(&got.1),
                "{metric} rank {rank}: native picked a different record than Lua \
                 (native {:.9}, lua {:.9})",
                want.0,
                got.0
            );

            // Both sum f32 values into f64, so they should agree to well within f32's own precision. The vendor
            // kernel reassociates, which is why this is a tolerance and not an equality.
            let tolerance = want.0.abs().max(1.0) * 1e-6;

            assert!(
                (want.0 - got.0).abs() <= tolerance,
                "{metric} rank {rank}: native says {:.9}, lua says {:.9}",
                want.0,
                got.0
            );
        }

        // Nothing in this set should be skipped by either engine.
        assert_eq!((native.skipped_dim, native.skipped_none), (0, 0));
        assert_eq!((lua.skipped_dim, lua.skipped_none), (0, 0));
    }

    // The element-at-a-time decoder must land on the same answer as the whole-vector one, or the cheaper row in
    // the benchmark is cheaper because it is wrong.
    {
        let args = topk_args("cosine", k as i64, &query);
        let batch = run_agg(&client, &namespace, &set, LUA_MODULE, "topk", &args, k).await;
        let scan = run_agg(&client, &namespace, &set, LUA_MODULE, "topk_scan", &args, k).await;

        for (rank, (a, b)) in batch.top.iter().zip(scan.top.iter()).enumerate() {
            assert_eq!(hex(&a.1), hex(&b.1), "topk_scan differs at rank {rank}");
            assert_eq!(a.0, b.0, "topk_scan differs at rank {rank}");
        }
    }

    // Vectors held as a list of doubles rank the same way. Not the same numbers — the list stores what the f32
    // blob rounded away — so only the ordering is compared.
    {
        let args = vec![
            as_val!("l"),
            Value::Blob(pack(&query)),
            as_val!("cosine"),
            as_val!(k as i64),
        ];

        let from_list = run_agg(&client, &namespace, &set, LUA_MODULE, "topk_list", &args, k).await;
        let from_blob = run_agg(
            &client,
            &namespace,
            &set,
            LUA_MODULE,
            "topk",
            &topk_args("cosine", k as i64, &query),
            k,
        )
        .await;

        for (rank, (a, b)) in from_blob.top.iter().zip(from_list.top.iter()).enumerate() {
            assert_eq!(
                hex(&a.1),
                hex(&b.1),
                "topk_list ranked differently at rank {rank}"
            );
        }
    }

    // orderby, where neither engine has any decoding to do.
    {
        let args = vec![as_val!("p"), as_val!("asc"), as_val!(10i64)];

        let native = run_agg(
            &client,
            &namespace,
            &set,
            &native_module(),
            "orderby",
            &args,
            10,
        )
        .await;

        let lua = run_agg(&client, &namespace, &set, LUA_MODULE, "orderby", &args, 10).await;

        assert_eq!(native.top.len(), 10);
        assert_eq!(lua.top.len(), 10);

        for (rank, (want, got)) in native.top.iter().zip(lua.top.iter()).enumerate() {
            assert_eq!(hex(&want.1), hex(&got.1), "orderby differs at rank {rank}");
            assert_eq!(want.0, got.0, "orderby differs at rank {rank}");
        }
    }

    // The record functions, on one record.
    {
        let key = as_key!(namespace.as_str(), set.as_str(), 11i64);
        let write = WritePolicy::default();

        for (function, args) in [
            (
                "vec_distance",
                vec![as_val!("v"), Value::Blob(pack(&query)), as_val!("cosine")],
            ),
            ("vec_dim", vec![as_val!("v")]),
            ("vec_norm", vec![as_val!("v")]),
        ] {
            let native = client
                .execute_udf(&write, &key, &native_module(), function, Some(&args))
                .await
                .unwrap();

            let lua = client
                .execute_udf(&write, &key, LUA_MODULE, function, Some(&args))
                .await
                .unwrap();

            let a = match native {
                Some(Value::Float(f)) => f64::from(f),
                Some(Value::Int(i)) => i as f64,
                other => panic!("native {function} answered {other:?}"),
            };

            let b = match lua {
                Some(Value::Float(f)) => f64::from(f),
                Some(Value::Int(i)) => i as f64,
                other => panic!("lua {function} answered {other:?}"),
            };

            assert!(
                (a - b).abs() <= a.abs().max(1.0) * 1e-6,
                "{function}: native {a}, lua {b}"
            );
        }
    }

    eprintln!("Lua and the native module agree on the top-10, the ordering and the record functions.");

    let _ = client
        .truncate(&AdminPolicy::default(), &namespace, &set, 0)
        .await;
}

//==========================================================
// What does each cost?
//

/// Time an aggregation that answers a counter rather than hits, for the probe rows.
async fn probe_ms(
    client: &Client,
    namespace: &str,
    set: &str,
    module: &str,
    function: &str,
    args: &[Value],
) -> f64 {
    let mut best = f64::MAX;

    for _ in 0..ROUNDS {
        let agg = run_agg(client, namespace, set, module, function, args, 0).await;

        if agg.ms < best {
            best = agg.ms;
        }
    }

    best
}

#[tokio::test]
#[ignore]
async fn lua_vs_native_bench() {
    // The native bench refuses to run in debug mode because its in-memory control loop is compiled by this
    // crate. Nothing here is, so a debug build would still measure the two engines fairly — but the record
    // loader is this crate's code, and an unoptimised one takes long enough to be worth the same warning.
    if cfg!(debug_assertions) {
        eprintln!("note: --release is much faster to load, though the measured work is all server-side");
    }

    let Some(client) = connect().await else {
        eprintln!("skipping lua_vs_native_bench: no server");
        return;
    };

    let n = records();
    let namespace = namespace();
    let set = format!("vec_lua_{}", std::process::id());

    // The list bin doubles the load time and adds ~6 KB a record to the namespace, so it is opt-in.
    let with_list = env::var("AEROSPIKE_BENCH_LIST")
        .map(|v| v.trim() == "1" || v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    eprintln!(
        "loading {n} records of {DIM} dimensions ({:.0} MB of vectors{})...",
        (n as f64 * DIM as f64 * 4.0) / 1_048_576.0,
        if with_list { ", plus a list copy" } else { "" }
    );

    let started = Instant::now();

    load(&client, &namespace, &set, n, with_list).await;

    eprintln!("loaded in {:.1} s", started.elapsed().as_secs_f64());

    if !native_usable(&client, &namespace, &set).await {
        eprintln!(
            "skipping lua_vs_native_bench: the server has no native module '{}'",
            native_module()
        );

        return;
    }

    assert!(register_lua(&client).await, "could not register {LUA_MODULE}");

    let query = vector_for(7);
    let k = 10usize;
    let native = native_module();

    //------------------------------------------------
    // The headline: the same exact top-k, twice.
    //

    eprintln!("\n=== exact top-{k} over {n} records of {DIM} dimensions, cosine ===\n");
    eprintln!(
        "  {:<40} {:>9}  {:>12}  {:>9}  {:>8}  {:>7}",
        "engine", "ms", "records/sec", "partials", "hits", "vs native"
    );

    let args = topk_args("cosine", k as i64, &query);

    let baseline = best_of(&client, &namespace, &set, &native, "topk", &args, k).await;

    let mut rows: Vec<(String, Agg)> = vec![("native, one vector per call".into(), baseline)];

    for (label, function) in [
        ("lua, string.unpack per vector", "topk"),
        ("lua, string.unpack per element", "topk_scan"),
    ] {
        let agg = best_of(&client, &namespace, &set, LUA_MODULE, function, &args, k).await;

        rows.push((label.into(), agg));
    }

    if with_list {
        let list_args = vec![
            as_val!("l"),
            Value::Blob(pack(&query)),
            as_val!("cosine"),
            as_val!(k as i64),
        ];

        let agg = best_of(
            &client,
            &namespace,
            &set,
            LUA_MODULE,
            "topk_list",
            &list_args,
            k,
        )
        .await;

        rows.push(("lua, vectors stored as a list".into(), agg));
    }

    let native_ms = rows[0].1.ms;

    for (label, agg) in &rows {
        eprintln!(
            "  {:<40} {:>9.0}  {:>12.0}  {:>9}  {:>8}  {:>7}",
            label,
            agg.ms,
            n as f64 / (agg.ms / 1000.0),
            agg.partials,
            agg.hits_on_wire,
            if agg.ms == native_ms {
                "-".to_string()
            } else {
                format!("{:.1}x", agg.ms / native_ms)
            }
        );
    }

    // Both engines must still be answering the same question at this size.
    for (label, agg) in rows.iter().skip(1) {
        if agg.top.first().map(|h| &h.1) != rows[0].1.top.first().map(|h| &h.1) {
            eprintln!("  !! {label} disagreed with native on the nearest record");
        }
    }

    //------------------------------------------------
    // Where the difference is.
    //

    eprintln!("\n=== attributing the per-record cost, both engines ===\n");
    eprintln!(
        "  {:<40} {:>11}  {:>11}  {:>9}",
        "stage", "native ns", "lua ns", "lua/native"
    );

    let stage_args = [
        as_val!("v"),
        Value::Blob(pack(&query)),
        as_val!("cosine"),
        as_val!("f32"),
    ];

    let per_record = |ms: f64| ms * 1_000_000.0 / n as f64;

    // `probe_unpack` has no native counterpart: there is nothing to unpack when the vector is read in place.
    //
    // Cumulative, each row doing strictly more than the one above it, so the deltas attribute the cost.
    let stages: &[(&str, Option<&str>, &str)] = &[
        ("the query and the record pipeline", Some("probe_count"), "probe_count"),
        ("+ read the vector bin", Some("probe_read"), "probe_read"),
        ("+ decode 768 floats", None, "probe_unpack"),
        ("+ measure the distance", Some("probe_dist"), "probe_dist"),
    ];

    // Each column's delta is against the previous row that *has* a value in that column: native has no decode
    // stage, so its chain is count -> read -> measure while Lua's has one more link.
    let mut previous_native: Option<f64> = None;
    let mut previous_lua: Option<f64> = None;

    for (label, native_fn, lua_fn) in stages {
        let native_ns = match native_fn {
            Some(f) => Some(per_record(
                probe_ms(&client, &namespace, &set, &native, f, &stage_args).await,
            )),
            None => None,
        };

        let lua_ns = per_record(
            probe_ms(&client, &namespace, &set, LUA_MODULE, lua_fn, &stage_args).await,
        );

        eprintln!(
            "  {:<40} {:>11}  {:>11.0}  {:>9}",
            label,
            native_ns
                .map(|v| format!("{v:.0}"))
                .unwrap_or_else(|| "n/a".into()),
            lua_ns,
            match native_ns {
                Some(v) if v > 0.0 => format!("{:.1}x", lua_ns / v),
                _ => "-".to_string(),
            }
        );

        // The deltas are the point of the table.
        if previous_native.is_some() || previous_lua.is_some() {
            eprintln!(
                "  {:<40} {:>11}  {:>11}  {:>9}",
                "  delta",
                match (previous_native, native_ns) {
                    (Some(p), Some(n)) => format!("{:+.0}", n - p),
                    _ => String::new(),
                },
                previous_lua
                    .map(|p| format!("{:+.0}", lua_ns - p))
                    .unwrap_or_default(),
                ""
            );
        }

        if native_ns.is_some() {
            previous_native = native_ns;
        }

        previous_lua = Some(lua_ns);
    }

    // And the whole query, which is those stages plus the bounded structure and shipping the hits.
    let lua_topk_ms = rows[1].1.ms;

    eprintln!(
        "  {:<40} {:>11.0}  {:>11.0}  {:>9}",
        "+ the bounded top-k (the whole query)",
        per_record(native_ms),
        per_record(lua_topk_ms),
        format!("{:.1}x", lua_topk_ms / native_ms)
    );

    eprintln!(
        "  {:<40} {:>11}  {:>11}  {:>9}",
        "  delta",
        previous_native
            .map(|p| format!("{:+.0}", per_record(native_ms) - p))
            .unwrap_or_default(),
        previous_lua
            .map(|p| format!("{:+.0}", per_record(lua_topk_ms) - p))
            .unwrap_or_default(),
        ""
    );

    // One cost the table cannot show as a stage, because it is not per record: Lua resolves the query - unpacking
    // 768 floats and building a format string - once per *stream*, and the server ran one stream per partition
    // slice. `probe_configure` is `probe_count` plus exactly that, so the difference is what those 4096 setups
    // cost, spread over the records they served. The native module resolves it once per query thread.
    {
        let configured = per_record(
            probe_ms(
                &client,
                &namespace,
                &set,
                LUA_MODULE,
                "probe_configure",
                &stage_args,
            )
            .await,
        );

        let counted = per_record(
            probe_ms(&client, &namespace, &set, LUA_MODULE, "probe_count", &stage_args).await,
        );

        eprintln!(
            "\n  of which per-stream setup: {:+.0} ns a record ({} streams, each unpacking the query again).",
            configured - counted,
            rows[1].1.partials
        );
    }

    //------------------------------------------------
    // The case with no decoding in it.
    //

    eprintln!("\n=== orderby: one bin read and one comparison per record ===\n");
    eprintln!(
        "  {:<40} {:>9}  {:>12}  {:>9}  {:>8}",
        "engine", "ms", "records/sec", "ns/record", "vs native"
    );

    let order_args = [as_val!("p"), as_val!("asc"), as_val!(20i64)];

    let native_order = best_of(
        &client,
        &namespace,
        &set,
        &native,
        "orderby",
        &order_args,
        20,
    )
    .await;

    let lua_order = best_of(
        &client,
        &namespace,
        &set,
        LUA_MODULE,
        "orderby",
        &order_args,
        20,
    )
    .await;

    for (label, agg) in [("native", &native_order), ("lua", &lua_order)] {
        eprintln!(
            "  {:<40} {:>9.0}  {:>12.0}  {:>9.0}  {:>8}",
            label,
            agg.ms,
            n as f64 / (agg.ms / 1000.0),
            per_record(agg.ms),
            if label == "native" {
                "-".to_string()
            } else {
                format!("{:.1}x", agg.ms / native_order.ms)
            }
        );
    }

    //------------------------------------------------
    // One record at a time, where nothing amortises.
    //
    // Read the per-call numbers carefully: the round trip is hundreds of microseconds and the engine difference
    // is a few, so this section bounds the answer rather than measuring it. What makes it worth printing at all
    // is that the Lua function has to unpack the *query* on every call, which is work the aggregation paid once.
    //

    eprintln!("\n=== one record at a time ({CALLS} sequential calls) ===\n");
    eprintln!("  {:<40} {:>9}  {:>13}", "call", "µs/call", "vs native");

    let write = WritePolicy::default();
    let call_args = [as_val!("v"), Value::Blob(pack(&query)), as_val!("cosine")];

    let mut native_us = 0.0;

    for (label, module) in [("native vec_distance", native.as_str()), ("lua vec_distance", LUA_MODULE)] {
        let started = Instant::now();

        for i in 1..=CALLS as i64 {
            let key = as_key!(namespace.as_str(), set.as_str(), i);

            client
                .execute_udf(&write, &key, module, "vec_distance", Some(&call_args))
                .await
                .unwrap();
        }

        let us = started.elapsed().as_secs_f64() * 1_000_000.0 / CALLS as f64;

        if native_us == 0.0 {
            native_us = us;
        }

        eprintln!(
            "  {:<40} {:>9.0}  {:>13}",
            label,
            us,
            if module == native.as_str() {
                "-".to_string()
            } else {
                format!("{:.2}x", us / native_us)
            }
        );
    }

    // A plain read of the same records, as the floor under both.
    {
        let started = Instant::now();

        for i in 1..=CALLS as i64 {
            let key = as_key!(namespace.as_str(), set.as_str(), i);

            client
                .get(&aerospike_core::ReadPolicy::default(), &key, Bins::None)
                .await
                .unwrap();
        }

        eprintln!(
            "  {:<40} {:>9.0}  {:>13}",
            "(a plain read, no UDF)",
            started.elapsed().as_secs_f64() * 1_000_000.0 / CALLS as f64,
            ""
        );
    }

    // Truncated for the same reason the other benches truncate: records left behind make every later run slower.
    if let Err(e) = client
        .truncate(&AdminPolicy::default(), &namespace, &set, 0)
        .await
    {
        eprintln!("\nnote: could not truncate {set} ({e})");
    }
}

