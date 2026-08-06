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

//! Aggregation across all three UDF engines, at a scale where the per-record cost is what you are measuring.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test agg_bench --no-default-features \
//!     --features serialization,async,rt-tokio,tls -- --ignored --nocapture
//! ```
//!
//! `#[ignore]`d: it loads 200,000 records, which is not something a test run should do by accident.
//!
//! # Two shapes, and they measure different things
//!
//! **The accumulator** — count and sum of one integer bin — is the shape where the engine is one call per
//! record and the output is a handful of partials. Almost all of the wall clock is the server reading rows, so
//! this measures how much of a query the engine actually is.
//!
//! **The emitter** — return the records matching a threshold — is the shape where output scales with input. It
//! exists in this file because it is the case a native module can express and the others largely cannot, and
//! because it moves the bottleneck from the engine to the response path, which is worth seeing next to the
//! first number rather than instead of it.
//!
//! # The engines are given the same work, not the same code
//!
//! Each record carries `v` and `n` with the **same value**, because the WASM fixture reads `n` and the Lua and
//! native functions read `v`. Rebuilding the checked-in fixture to rename a bin would have meant a WASM
//! toolchain on the machine running this, for no gain: reading `n` and reading `v` are the same host call.
//!
//! Every run asserts its own answer. A benchmark that is not also a correctness check measures the wrong thing
//! sooner or later — and in this file it caught a Lua aggregation returning one partial per slice while the
//! native one returned one per thread, which is a difference in what is being timed, not just in speed.
//!
//! # The set is truncated at the end
//!
//! Not tidiness. A set-scoped query with no set index reduces over the namespace's whole primary index, so
//! records another run left behind are records this one walks for nothing: four consecutive runs drifted the
//! native row from 32 ms to 47 ms, and compressed every ratio with it. That drift is indistinguishable from a
//! real effect by eye, which is why the cleanup is part of the measurement rather than an afterthought.
//!
//! It also means **the numbers are only comparable against a namespace this bench had to itself.** A namespace
//! holding a few hundred thousand records from another suite will read slower for every engine.

use crate::common;

use aerospike::*;

use std::time::Instant;

/// Enough records that the per-record cost dominates the fixed cost of starting a query.
const RECORDS: i64 = 200_000;

/// How many concurrent writes to keep in flight while loading. Sequential puts at ~200 µs would take 40
/// seconds; this brings it under five.
const LOAD_CONCURRENCY: usize = 256;

/// The threshold the emitter filters on, as a fraction of `RECORDS`.
///
/// Ten percent rather than everything: a filter that matches every record measures the response path and
/// almost nothing else, and one that matches nothing measures only the bin read. Ten percent leaves the
/// per-record call visible while still shipping a realistic number of records back.
const EMIT_FLOOR: i64 = RECORDS - RECORDS / 10 + 1;

/// The WASM fixture, the same one the WASM suite registers.
const WASM_MODULE: &[u8] = include_bytes!("../fixtures/wasm/udf_test.wasm");

/// The Lua counterparts, written to do the same work through the server's own object model.
const LUA: &[u8] = br#"
-- The accumulator. A map so that count and sum travel together, which is what makes a partial combinable.
local function acc(m, rec)
    local v = rec['v']
    if v ~= nil then
        m['count'] = (m['count'] or 0) + 1
        m['sum'] = (m['sum'] or 0) + v
    end
    return m
end

local function comb(a, b)
    a['count'] = (a['count'] or 0) + (b['count'] or 0)
    a['sum'] = (a['sum'] or 0) + (b['sum'] or 0)
    return a
end

function stats(stream, bin)
    return stream : aggregate(map{count = 0, sum = 0}, acc) : reduce(comb)
end

-- The same aggregation with a **scalar** accumulator, which is not the same amount of Lua work.
--
-- `stats` above has to index a map twice per record to keep count and sum together - the idiomatic way to carry
-- two numbers through `aggregate`, and measurably more expensive in Lua than adding to a number. Without this
-- row the engine comparison would be crediting native for Lua's map indexing.
local function acc_sum(total, rec)
    local v = rec['v']
    if v ~= nil then
        total = total + v
    end
    return total
end

local function comb_sum(a, b)
    return a + b
end

function sum(stream, bin)
    return stream : aggregate(0, acc_sum) : reduce(comb_sum)
end

-- The emitter. `filter` leaves records on the stream rather than values, so what reaches the client is
-- records - the same thing the native module's `emit_record` produces.
local function matches(rec)
    local v = rec['v']
    local floor = rec['floor']

    return v ~= nil and floor ~= nil and v >= floor
end

function over(stream)
    return stream : filter(matches)
end
"#;

fn native_module() -> String {
    std::env::var("AEROSPIKE_NATIVE_MODULE").unwrap_or_else(|_| "stats".to_string())
}

/// The second native module, if one is configured — the check that the ABI is language-neutral in cost as well
/// as in shape.
fn native_module_2() -> Option<String> {
    std::env::var("AEROSPIKE_NATIVE_MODULE_2").ok()
}

async fn native_usable(client: &Client, module: &str, key: &Key) -> bool {
    client
        .execute_udf(
            &WritePolicy::default(),
            key,
            module,
            "echo",
            Some(&[as_val!(1)]),
        )
        .await
        .is_ok()
}

/// A map value, whichever flavour the client decoded it into.
fn as_map_of(value: &Value) -> Option<std::collections::HashMap<Value, Value>> {
    match value {
        Value::HashMap(m) => Some(m.clone()),
        Value::OrderedMap(m) => Some(m.clone().into_iter().collect()),
        Value::SortedMap(m) => Some(m.clone().into_iter().collect()),
        _ => None,
    }
}

/// What one aggregation run produced.
struct Run {
    ms: f64,
    /// Values on the result stream — partials, for the accumulator shape.
    partials: usize,
    /// Records on the result stream, i.e. things carrying bins.
    records: usize,
    count: i64,
    sum: i64,
}

impl Run {
    fn per_sec(&self) -> f64 {
        RECORDS as f64 / (self.ms / 1000.0)
    }
}

/// Run one aggregation and take it apart.
///
/// Deliberately one function for both shapes: a partial and an emitted record arrive on the *same* stream, and
/// telling them apart is the reader's job — a record carries the bins it was written with, a partial is a
/// single value under the server's `SUCCESS` bin. Doing that here rather than in two functions is what makes
/// the "emit values or records, not both" rule visible.
async fn run(
    client: &Client,
    namespace: &str,
    set_name: &str,
    module: &str,
    function: &str,
    args: Option<&[Value]>,
) -> Run {
    use futures::StreamExt;

    let mut statement = Statement::new(namespace, set_name, Bins::All);

    statement.set_aggregate_function(module, function, args);

    let started = Instant::now();

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut out = Run {
        ms: 0.0,
        partials: 0,
        records: 0,
        count: 0,
        sum: 0,
    };

    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.unwrap();

        // A record the UDF passed through: it has the bin it was written with.
        if let Some(Value::Int(v)) = record.bins.get("v") {
            out.records += 1;
            out.count += 1;
            out.sum += v;

            continue;
        }

        for (_, value) in record.bins.iter() {
            out.partials += 1;

            // A scalar partial: what a plain sum produces from Lua, from WASM, and from a C plugin that
            // hand-writes `to_partial` as an integer.
            if let Value::Int(n) = value {
                out.sum += n;
                continue;
            }

            if let Some(map) = as_map_of(value) {
                // `total` is what `partial!` names the field in the Rust plugin's `Sum`, where the C plugin
                // writes a bare integer. Reading both is the reader acknowledging that a partial's format
                // belongs to the plugin.
                if let Some(Value::Int(n)) = map.get(&as_val!("total")) {
                    out.sum += n;
                }

                if let Some(Value::Int(n)) = map.get(&as_val!("count")) {
                    out.count += n;
                }

                if let Some(Value::Int(n)) = map.get(&as_val!("sum")) {
                    out.sum += n;
                }
            }
        }
    }

    out.ms = started.elapsed().as_secs_f64() * 1000.0;

    out
}

/// Best of `rounds`, by wall clock. The first round of anything on this path pays for a cold cache somewhere,
/// so a single measurement is not one.
async fn best_of(
    rounds: usize,
    client: &Client,
    namespace: &str,
    set_name: &str,
    module: &str,
    function: &str,
    args: Option<&[Value]>,
) -> Run {
    let mut best: Option<Run> = None;

    for _ in 0..rounds {
        let r = run(client, namespace, set_name, module, function, args).await;

        if best.as_ref().is_none_or(|b| r.ms < b.ms) {
            best = Some(r);
        }
    }

    best.unwrap()
}

#[aerospike_macro::test]
#[ignore]
async fn agg_bench() {
    use futures::stream::StreamExt;

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let lua_package = &format!("bench_lua_{}", common::rand_str(6));
    let wasm_package = &format!("bench_wasm_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let probe = as_key!(namespace, set_name, 1i64);

    client
        .put(&write, &probe, &[as_bin!("v", 1)])
        .await
        .unwrap();

    let native = native_module();
    let have_native = native_usable(&client, &native, &probe).await;

    if !have_native {
        eprintln!("note: no usable native module '{native}' - the native rows will be missing");
    }

    let native_2 = match native_module_2() {
        Some(m) if native_usable(&client, &m, &probe).await => Some(m),
        _ => None,
    };

    // --- register the two engines that need registering ---

    let policy = AdminPolicy::default();

    client
        .register_udf(&policy, LUA, &format!("{lua_package}.lua"), UDFLang::Lua)
        .await
        .unwrap()
        .wait_till_complete(None)
        .await
        .unwrap();

    let have_wasm = match client
        .register_udf(
            &policy,
            WASM_MODULE,
            &format!("{wasm_package}.wasm"),
            UDFLang::Wasm,
        )
        .await
    {
        Ok(task) => task.wait_till_complete(None).await.is_ok(),
        Err(e) => {
            eprintln!("note: WASM unavailable ({e}) - the WASM row will be missing");
            false
        }
    };

    // --- load ---

    eprintln!("loading {RECORDS} records ({LOAD_CONCURRENCY} concurrent)...");

    let started = Instant::now();

    // `n` alongside `v` because the checked-in WASM fixture reads `n`; `floor` so the emitter has a threshold
    // to read without needing an argument it has nowhere to keep.
    futures::stream::iter(1..=RECORDS)
        .for_each_concurrent(LOAD_CONCURRENCY, |i| {
            let client = &client;
            let write = &write;

            async move {
                let key = as_key!(namespace, set_name, i);

                client
                    .put(
                        write,
                        &key,
                        &[
                            as_bin!("v", i),
                            as_bin!("n", i),
                            as_bin!("floor", EMIT_FLOOR),
                        ],
                    )
                    .await
                    .unwrap();
            }
        })
        .await;

    let load_ms = started.elapsed().as_secs_f64() * 1000.0;

    eprintln!(
        "loaded in {:.1} s ({:.0} records/sec)\n",
        load_ms / 1000.0,
        RECORDS as f64 / (load_ms / 1000.0)
    );

    let expected_sum: i64 = (1 + RECORDS) * RECORDS / 2;
    let expected_emitted = RECORDS - EMIT_FLOOR + 1;

    //------------------------------------------------
    // Shape one: count and sum.
    //

    eprintln!("=== count + sum over {RECORDS} records ===\n");

    let bin_arg = [as_val!("v")];

    let mut rows: Vec<(String, Run)> = Vec::new();

    let lua = best_of(3, &client, namespace, set_name, lua_package, "stats", Some(&bin_arg)).await;

    assert_eq!(lua.count, RECORDS, "lua counted wrong");
    assert_eq!(lua.sum, expected_sum, "lua summed wrong");
    rows.push(("lua".into(), lua));

    if have_wasm {
        // The fixture's `stats` takes no arguments and reads bin `n`.
        let wasm = best_of(3, &client, namespace, set_name, wasm_package, "stats", None).await;

        assert_eq!(wasm.count, RECORDS, "wasm counted wrong");
        assert_eq!(wasm.sum, expected_sum, "wasm summed wrong");
        rows.push(("wasm (aot)".into(), wasm));
    }

    if have_native {
        let n = best_of(3, &client, namespace, set_name, &native, "stats", Some(&bin_arg)).await;

        assert_eq!(n.count, RECORDS, "native counted wrong");
        assert_eq!(n.sum, expected_sum, "native summed wrong");
        rows.push((format!("native ({native})"), n));

        if let Some(m) = &native_2 {
            let n2 = best_of(3, &client, namespace, set_name, m, "stats", Some(&bin_arg)).await;

            assert_eq!(n2.count, RECORDS, "second native module counted wrong");
            rows.push((format!("native ({m})"), n2));
        }
    }

    report(&rows, "partials");

    //------------------------------------------------
    // The same aggregation with a scalar accumulator, because `stats` is not a fair engine comparison on its
    // own: Lua's version indexes a map twice per record to carry count and sum together, and native's writes
    // two struct fields. Summing one bin removes that difference and leaves the per-record call.
    //

    eprintln!("\n=== sum only, scalar accumulator ===\n");

    let mut scalar_rows: Vec<(String, Run)> = Vec::new();

    // A scalar partial, so `count` stays zero here - only the sum is checked.
    let lua_sum = best_of(3, &client, namespace, set_name, lua_package, "sum", Some(&bin_arg)).await;

    assert_eq!(lua_sum.sum, expected_sum, "lua summed wrong");
    scalar_rows.push(("lua".into(), lua_sum));

    if have_wasm {
        let wasm_sum = best_of(3, &client, namespace, set_name, wasm_package, "sum", None).await;

        assert_eq!(wasm_sum.sum, expected_sum, "wasm summed wrong");
        scalar_rows.push(("wasm (aot)".into(), wasm_sum));
    }

    if have_native {
        let n = best_of(3, &client, namespace, set_name, &native, "sum", Some(&bin_arg)).await;

        assert_eq!(n.sum, expected_sum, "native summed wrong");
        scalar_rows.push((format!("native ({native})"), n));

        if let Some(m) = &native_2 {
            let n2 = best_of(3, &client, namespace, set_name, m, "sum", Some(&bin_arg)).await;

            assert_eq!(n2.sum, expected_sum, "second native module summed wrong");
            scalar_rows.push((format!("native ({m})"), n2));
        }
    }

    report(&scalar_rows, "partials");

    //------------------------------------------------
    // Which of the two savings is which.
    //
    // The gap above has two causes and the table cannot separate them: a native aggregation has no engine, and
    // it keeps one accumulator per query *thread* where Lua and WASM finalize per partition slice — the
    // `partials` column is 4 against 4096. Running the same engine against itself at both partial counts
    // isolates the second.
    //

    if let Some(m) = native_2.as_ref().or(if have_native { Some(&native) } else { None }) {
        // `stats_chunked` against *itself*, not against `stats`: chunk 0 never dumps and chunk 49 dumps at
        // roughly Lua's rate, so the two runs differ in the partial count and in nothing else at all. Comparing
        // against `stats` would have left min/max/seen in one arm and not the other — a smaller difference than
        // the one being measured, but not zero, and there is no reason to accept it.
        let per_partition = (RECORDS / 4096).max(1);

        let control_args = [as_val!("v"), as_val!(0)];
        let chunk_args = [as_val!("v"), as_val!(per_partition)];

        let control = best_of(3, &client, namespace, set_name, m, "stats_chunked", Some(&control_args)).await;

        if control.partials == 0 {
            eprintln!("\n  (this module has no `stats_chunked`, so the split is not available)");
        } else {
            let chunked =
                best_of(3, &client, namespace, set_name, m, "stats_chunked", Some(&chunk_args)).await;

            for (what, r) in [("control", &control), ("chunked", &chunked)] {
                assert_eq!(r.count, RECORDS, "the {what} run counted wrong");
                assert_eq!(r.sum, expected_sum, "the {what} run summed wrong");
            }

            let lua_ms = rows[0].1.ms;

            eprintln!(
                "\n  the same aggregation, dumping or not:  {} partials {:.0} ms   |   {} partials {:.0} ms",
                control.partials, control.ms, chunked.partials, chunked.ms
            );
            eprintln!(
                "  so of the {:.1}x the count+sum table shows over lua: {:.1}x is shipping {} partials \
                 instead of {},",
                lua_ms / control.ms,
                chunked.ms / control.ms,
                chunked.partials,
                control.partials
            );
            eprintln!(
                "  and {:.1}x is the engine - the two multiply, and neither alone is the headline number.",
                lua_ms / chunked.ms
            );
        }
    }

    //------------------------------------------------
    // Shape two: return the matching records.
    //

    eprintln!("\n=== emit {expected_emitted} matching records of {RECORDS} ===\n");

    let mut emit_rows: Vec<(String, Run)> = Vec::new();

    let lua = best_of(3, &client, namespace, set_name, lua_package, "over", None).await;

    if lua.records == 0 {
        eprintln!(
            "  lua              emitted no records - `stream:filter` did not put records on the stream \
             here, so this engine has no row"
        );
    } else {
        assert_eq!(
            lua.records as i64, expected_emitted,
            "lua emitted the wrong number of records"
        );
        emit_rows.push(("lua".into(), lua));
    }

    if have_native {
        let n = best_of(3, &client, namespace, set_name, &native, "over", None).await;

        assert_eq!(
            n.records as i64, expected_emitted,
            "native emitted the wrong number of records"
        );
        emit_rows.push((format!("native ({native})"), n));

        if let Some(m) = &native_2 {
            let n2 = best_of(3, &client, namespace, set_name, m, "over", None).await;

            assert_eq!(n2.records as i64, expected_emitted, "second module emitted wrong");
            emit_rows.push((format!("native ({m})"), n2));
        }
    }

    // Not an omission: a WASM stream UDF's `end` writes exactly one value to the output stream and its
    // `record` hook returns only a status, so there is nowhere for a per-record emission to go. Saying so is
    // worth more than leaving a blank row.
    eprintln!("  wasm             not expressible - a WASM stream UDF writes one value at `end`, so it has");
    eprintln!("                   no way to emit per record. It would need an emit call in the WASM ABI.\n");

    report(&emit_rows, "records");

    //------------------------------------------------
    // For scale.
    //

    let started = Instant::now();
    let mut scanned = 0usize;

    let statement = Statement::new(namespace, set_name, Bins::All);
    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut stream = records.into_stream();

    while let Some(r) = stream.next().await {
        r.unwrap();
        scanned += 1;
    }

    let scan_ms = started.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(scanned as i64, RECORDS, "the scan lost records");

    eprintln!(
        "\nfor scale: a plain scan of the same {RECORDS} records costs {scan_ms:.0} ms \
         ({:.0} records/sec)",
        RECORDS as f64 / (scan_ms / 1000.0)
    );
    eprintln!(
        "           it ships every record, so it is not a floor an aggregation builds on - it is the cost \
         of the thing an aggregation exists to avoid."
    );

    // **Truncate, or the next run is slower than this one.**
    //
    // A set-scoped query without a set index reduces over the namespace's whole primary index and filters, so
    // 200,000 records left behind are 200,000 records the next run walks for nothing. Four consecutive runs
    // drifted from 32 ms to 47 ms on the native row before this was here, and the ratios compressed with them -
    // which looks exactly like a real effect and is not one.
    if let Err(e) = client
        .truncate(&AdminPolicy::default(), namespace, set_name, 0)
        .await
    {
        eprintln!("\nnote: could not truncate {set_name} ({e}) - a later run will be slower for it");
    }
}

/// One table, with the fastest engine as the baseline.
fn report(rows: &[(String, Run)], stream_units: &str) {
    if rows.is_empty() {
        eprintln!("  (nothing to report)");
        return;
    }

    let fastest = rows.iter().map(|(_, r)| r.ms).fold(f64::MAX, f64::min);

    eprintln!(
        "  {:<22} {:>10}  {:>14}  {:>8}  {:>10}",
        "engine", "ms", "records/sec", "vs best", stream_units
    );

    for (name, r) in rows {
        let n = if stream_units == "records" {
            r.records
        } else {
            r.partials
        };

        eprintln!(
            "  {:<22} {:>10.0}  {:>14.0}  {:>7.2}x  {:>10}",
            name,
            r.ms,
            r.per_sec(),
            r.ms / fastest,
            n
        );
    }
}
