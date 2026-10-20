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

//! Native UDFs against Lua, measured rather than asserted.
//!
//! `#[ignore]`d, because it is a measurement and not a test — it takes minutes and it reports numbers rather
//! than passing or failing:
//!
//! ```text
//! cargo test --features serialization,rt-tokio,async native_bench -- --ignored --nocapture
//! ```
//!
//! # Read the aggregation numbers, be careful with the per-call ones
//!
//! An aggregation over tens of thousands of records amortises the network away: what is being timed is the
//! server doing per-record work, so the comparison is real.
//!
//! A single UDF call does not. The round trip from here to the server is hundreds of microseconds and the
//! engine difference is a handful, so the per-call section subtracts a plain-write baseline and *still*
//! reports noise as large as the thing it is measuring. It is here because it bounds the answer — "the
//! difference is under the noise floor" is worth knowing — not because the number is precise.
//!
//! This project has already produced two measurements that contradicted a prediction: a "4.5 µs cheaper data
//! path" that turned out to be a write that never committed, and a 15× compute gap that was 4× once both
//! sides used the same optimization level. So the rule here is that nothing is a claim until it is printed by
//! this file.

use crate::common;

use aerospike::*;
use std::time::Instant;

/// Enough records that per-record cost dominates, and few enough to load in a reasonable time.
const RECORDS: i64 = 50_000;

/// Sequential UDF calls for the per-call section.
const CALLS: usize = 300;

/// The Lua equivalents of the native example's functions.
///
/// Deliberately the *same shape* — read a bin, add, commit; and accumulate one bin across a stream — so the
/// comparison is between engines rather than between two different pieces of work.
const LUA: &[u8] = br#"
function increment(rec, by)
    local n = rec['n']
    if n == nil then n = 0 end
    rec['n'] = n + (by or 1)
    if aerospike:exists(rec) then
        aerospike:update(rec)
    else
        aerospike:create(rec)
    end
    return rec['n']
end

function sum_list(rec)
    local total = 0
    local l = rec['l']
    if l ~= nil then
        for v in list.iterator(l) do
            total = total + v
        end
    end
    return total
end

local function accumulate(acc, rec)
    local v = rec['v']
    if v ~= nil then
        acc = acc + v
    end
    return acc
end

local function combine(a, b)
    return a + b
end

function sum(stream, bin)
    return stream : aggregate(0, accumulate) : reduce(combine)
end
"#;

fn native_module() -> String {
    std::env::var("AEROSPIKE_NATIVE_MODULE").unwrap_or_else(|_| "stats".to_string())
}

/// The second native module to compare against, if one is configured.
fn native_module_2() -> Option<String> {
    std::env::var("AEROSPIKE_NATIVE_MODULE_2").ok()
}

async fn usable(client: &Client, module: &str, key: &Key) -> bool {
    client
        .execute_udf(&WritePolicy::default(), key, module, "echo", &[as_val!(1)])
        .await
        .is_ok()
}

/// A partial that arrived as any of the map flavours, as one map.
fn as_map_of(value: &Value) -> Option<std::collections::HashMap<Value, Value>> {
    match value {
        Value::HashMap(m) => Some(m.clone()),
        Value::OrderedMap(m) => Some(m.clone().into_iter().collect()),
        Value::SortedMap(m) => Some(m.clone().into_iter().collect()),
        _ => None,
    }
}

/// Time an aggregation, returning milliseconds and the total it computed.
async fn time_aggregation(
    client: &Client,
    namespace: &str,
    set_name: &str,
    module: &str,
    function: &str,
) -> (f64, i64) {
    use futures::StreamExt;

    let args = [as_val!("v")];
    let mut statement = Statement::new(namespace, set_name, Bins::All);

    statement.set_aggregate_function(module, function, &args);

    let started = Instant::now();

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut total = 0i64;
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.unwrap();

        for (_, value) in record.bins.iter() {
            match value {
                Value::Int(n) => total += n,
                Value::HashMap(m) => {
                    if let Some(Value::Int(n)) = m.get(&as_val!("sum")) {
                        total += n;
                    }
                }
                other => {
                    // An ordered or sorted map partial - the shape `stats` returns. Collected through the
                    // generic path so this arm does not have to know which map flavour arrived.
                    if let Some(map) = as_map_of(other) {
                        if let Some(Value::Int(n)) = map.get(&as_val!("sum")) {
                            total += n;
                        }
                    }
                }
            }
        }
    }

    (started.elapsed().as_secs_f64() * 1000.0, total)
}

/// Time `CALLS` sequential calls, returning microseconds per call.
async fn time_calls(client: &Client, key: &Key, module: &str, function: &str) -> f64 {
    let write = WritePolicy::default();
    let started = Instant::now();

    for _ in 0..CALLS {
        client
            .execute_udf(&write, key, module, function, &[])
            .await
            .unwrap();
    }

    started.elapsed().as_secs_f64() * 1_000_000.0 / CALLS as f64
}

#[aerospike_macro::test]
#[ignore]
async fn native_bench() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let lua_package = &format!("bench_lua_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let probe_key = as_key!(namespace, set_name, 1);

    client
        .put(&write, &probe_key, &[as_bin!("v", 1), as_bin!("n", 0)])
        .await
        .unwrap();

    let native = native_module();

    if !usable(&client, &native, &probe_key).await {
        eprintln!("skipping native_bench: no usable native module '{native}'");
        return;
    }

    let native_2 = match native_module_2() {
        Some(m) if usable(&client, &m, &probe_key).await => Some(m),
        _ => None,
    };

    // --- Lua, for comparison ---

    let task = client
        .register_udf(
            &AdminPolicy::default(),
            LUA,
            &format!("{lua_package}.lua"),
            UdfLang::Lua,
        )
        .await
        .unwrap();

    task.wait_till_complete(None).await.unwrap();

    // --- load ---

    eprintln!("loading {RECORDS} records...");

    let load_started = Instant::now();

    for i in 2..=RECORDS {
        let key = as_key!(namespace, set_name, i);

        client
            .put(&write, &key, &[as_bin!("v", i), as_bin!("n", 0)])
            .await
            .unwrap();
    }

    eprintln!("loaded in {:.1}s", load_started.elapsed().as_secs_f64());

    let expected: i64 = (1..=RECORDS).sum();

    // --- a plain scan of the same records, for scale ---
    //
    // This was added to separate the server's record pipeline from the UDF's share of an aggregation, on the
    // assumption that a scan is the floor an aggregation builds on. **It is not**, and the number says so: a
    // scan of these 50,000 records costs several times what aggregating them does, because it ships every
    // record to the client while an aggregation ships one partial per slice.
    //
    // So it cannot be subtracted, and the code below does not. It is kept because it puts the aggregation
    // numbers in scale - the whole aggregation costs a fraction of merely reading the same rows out - and
    // because the assumption was worth disproving in the file rather than in someone's head.
    let scan_started = Instant::now();
    {
        use futures::StreamExt;

        let statement = Statement::new(namespace, set_name, Bins::All);
        let records = client
            .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
            .await
            .unwrap();
        let mut stream = records.into_stream();
        let mut seen = 0;

        while let Some(r) = stream.next().await {
            r.unwrap();
            seen += 1;
        }

        assert_eq!(seen, RECORDS, "the scan baseline must see every record");
    }
    let scan_ms = scan_started.elapsed().as_secs_f64() * 1000.0;

    eprintln!(
        "
  plain scan of the same {RECORDS} records: {scan_ms:.1} ms (no UDF at all)"
    );

    // --- aggregation: the measurement that means something ---

    eprintln!("\n=== aggregation over {RECORDS} records (sum of one integer bin) ===");

    let mut results: Vec<(String, f64)> = Vec::new();

    // Warm both engines once, so neither pays for a first-call cost the other does not.
    let _ = time_aggregation(&client, namespace, set_name, lua_package, "sum").await;
    let _ = time_aggregation(&client, namespace, set_name, &native, "sum").await;

    for round in 0..3 {
        let (lua_ms, lua_total) =
            time_aggregation(&client, namespace, set_name, lua_package, "sum").await;
        let (nat_ms, nat_total) =
            time_aggregation(&client, namespace, set_name, &native, "sum").await;

        assert_eq!(lua_total, expected, "Lua computed the wrong total");
        assert_eq!(
            nat_total, expected,
            "the native module computed the wrong total"
        );

        eprintln!(
            "  round {round}:  lua {lua_ms:8.1} ms   native/{native} {nat_ms:8.1} ms   \
             ({:.2}x)",
            lua_ms / nat_ms
        );

        results.push(("lua".into(), lua_ms));
        results.push((native.clone(), nat_ms));

        if let Some(m) = &native_2 {
            let (ms2, total2) = time_aggregation(&client, namespace, set_name, m, "sum").await;

            assert_eq!(
                total2, expected,
                "the second native module computed the wrong total"
            );
            eprintln!(
                "            native/{m} {ms2:8.1} ms   ({:.2}x lua)",
                lua_ms / ms2
            );
            results.push((m.clone(), ms2));
        }
    }

    let best = |name: &str| -> f64 {
        results
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, ms)| *ms)
            .fold(f64::INFINITY, f64::min)
    };

    eprintln!(
        "\n  best of three:  lua {:.1} ms, native {:.1} ms  ->  {:.2}x",
        best("lua"),
        best(&native),
        best("lua") / best(&native)
    );

    // Deliberately no subtraction of the scan: see the comment where it is measured. Reporting the ratio
    // instead, which is what this setup can actually establish.
    eprintln!(
        "  for scale, a plain scan of the same rows costs {scan_ms:.1} ms - so both aggregations cost a \
         fraction of reading the rows out, and neither engine is the bottleneck of a real query."
    );

    if let Some(m) = &native_2 {
        eprintln!(
            "                  native/{m} {:.1} ms  ->  {:.2}x lua",
            best(m),
            best("lua") / best(m)
        );
    }

    // --- per call: bounded, not precise ---

    eprintln!("\n=== one record UDF call, {CALLS} sequential ===");
    eprintln!("  (the round trip dominates; a plain write is the baseline to subtract)");

    let bare_started = Instant::now();

    for _ in 0..CALLS {
        client
            .put(&write, &probe_key, &[as_bin!("n", 0)])
            .await
            .unwrap();
    }

    let baseline_us = bare_started.elapsed().as_secs_f64() * 1_000_000.0 / CALLS as f64;

    let lua_us = time_calls(&client, &probe_key, lua_package, "increment").await;
    let nat_us = time_calls(&client, &probe_key, &native, "increment").await;

    eprintln!("  plain write        {baseline_us:8.1} us  (baseline)");
    eprintln!(
        "  lua increment      {lua_us:8.1} us  ->  {:+.1} us over baseline",
        lua_us - baseline_us
    );
    eprintln!(
        "  native increment   {nat_us:8.1} us  ->  {:+.1} us over baseline",
        nat_us - baseline_us
    );

    if let Some(m) = &native_2 {
        let us2 = time_calls(&client, &probe_key, m, "increment").await;

        eprintln!(
            "  native/{m} increment {us2:8.1} us  ->  {:+.1} us over baseline",
            us2 - baseline_us
        );
    }

    eprintln!(
        "\n  Treat any per-call gap smaller than {:.0} us as unmeasured: that is the baseline itself, and \
         run-to-run variation on this link is a large fraction of it.",
        baseline_us * 0.1
    );
}
