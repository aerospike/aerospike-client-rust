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

//! Every worked example in `TUTORIAL.md`, run for real so the document can quote instead of imagine.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test --test tutorial -- --ignored --nocapture
//! ```
//!
//! There are no assertions here on purpose - this produces output. `against_a_server` asserts the same paths.

use std::collections::HashMap;
use std::env;

use aerospike_core::{
    as_key, as_val, Bins, Client, ClientPolicy, Key, PartitionFilter, QueryPolicy, ReadPolicy,
    Statement, Value, WritePolicy,
};
use futures::StreamExt;

/// 2026-01-01T00:00:00Z. Bucket 29453760 at a one-minute step, so every number below is reproducible.
const T0: i64 = 1_767_225_600_000;
const MIN: i64 = 60_000;

fn module() -> String {
    env::var("AEROSPIKE_TIMESERIES_MODULE").unwrap_or_else(|_| "timeseries".to_string())
}

fn ns() -> String {
    env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

async fn connect() -> Option<Client> {
    let asked = env::var("AEROSPIKE_HOSTS").is_ok();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    match Client::new(&ClientPolicy::default(), &hosts).await {
        Ok(c) => Some(c),
        Err(e) if asked => panic!("AEROSPIKE_HOSTS is set but unreachable: {e}"),
        Err(_) => None,
    }
}

fn render(v: &Value) -> String {
    match v {
        Value::Nil => "nil".into(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            let n = f64::from(f.clone());

            if n.fract() == 0.0 && n.abs() < 1e15 {
                format!("{n:.1}")
            } else {
                format!("{n}")
            }
        }
        Value::String(s) => s.clone(),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(render).collect();

            format!("[{}]", inner.join(", "))
        }
        // `per_archive` and `per_metric` are maps, and without this they print as `SortedMap({String("day"): …})`.
        Value::HashMap(m) => render_map(m.iter()),
        Value::SortedMap(m) => render_map(m.iter()),
        Value::OrderedMap(m) => render_map(m.iter()),
        other => format!("{other:?}"),
    }
}

fn render_map<'a>(entries: impl Iterator<Item = (&'a Value, &'a Value)>) -> String {
    let inner: Vec<String> = entries
        .map(|(k, v)| format!("{}: {}", render(k), render(v)))
        .collect();

    format!("{{{}}}", inner.join(", "))
}

fn as_map(v: &Value) -> Option<HashMap<String, Value>> {
    let pairs: Vec<(Value, Value)> = match v {
        Value::HashMap(m) => m.iter().map(|(k, x)| (k.clone(), x.clone())).collect(),
        Value::SortedMap(m) => m.iter().map(|(k, x)| (k.clone(), x.clone())).collect(),
        Value::OrderedMap(m) => m.iter().map(|(k, x)| (k.clone(), x.clone())).collect(),
        _ => return None,
    };

    Some(
        pairs
            .into_iter()
            .filter_map(|(k, x)| match k {
                Value::String(s) => Some((s, x)),
                _ => None,
            })
            .collect(),
    )
}

async fn udf(
    client: &Client,
    key: &Key,
    function: &str,
    args: &[Value],
) -> Result<HashMap<String, Value>, String> {
    match client
        .execute_udf(&WritePolicy::default(), key, &module(), function, Some(args))
        .await
    {
        Ok(Some(v)) => as_map(&v).ok_or_else(|| format!("not a map: {v:?}")),
        Ok(None) => Err("returned nothing".into()),
        Err(e) => Err(format!("{e}")),
    }
}

/// Run a call and print the keys a reader cares about, on one line.
async fn show(client: &Client, key: &Key, call: &str, args: &[Value], keys: &[&str]) {
    let function = call.split('(').next().unwrap_or(call);

    println!("  {call}");

    match udf(client, key, function, args).await {
        Ok(map) => {
            let shown: Vec<String> = keys
                .iter()
                .filter_map(|k| map.get(*k).map(|v| format!("{k}={}", render(v))))
                .collect();

            println!("    -> {}", shown.join("  "));
        }
        // Truncated: the plugin's messages are long by design, and the tutorial wants the first clause.
        Err(e) => {
            let msg = e.replace('\n', " ");
            let brief = msg.split(" - ").next().unwrap_or(&msg);

            println!("    -> ERROR: {}", brief.trim());
        }
    }
}

async fn init(client: &Client, key: &Key, bin: &str, step: i64, slots: i64, opts: Option<&str>) {
    let args = vec![
        as_val!(bin),
        as_val!(step),
        as_val!(slots),
        opts.map_or(Value::Nil, |o| as_val!(o)),
        Value::Nil,
        as_val!(T0),
    ];

    let _ = udf(client, key, "ts_init", &args).await;
}

async fn append(client: &Client, key: &Key, bin: &str, at: i64, v: Value) {
    let _ = udf(client, key, "ts_append", &[as_val!(bin), as_val!(at), v]).await;
}

fn head(n: u32, title: &str) {
    println!("\n\n=== {n}. {title} ===\n");
}

#[tokio::test]
#[ignore = "produces the tutorial's output; run it deliberately"]
async fn tutorial() {
    let Some(client) = connect().await else {
        eprintln!("skipping tutorial: no server");
        return;
    };

    let ns = ns();
    let set = format!("tut_{}", std::process::id());
    let k = |id: &str| as_key!(ns.as_str(), set.as_str(), id);
    let wipe = |key: Key| async move { key };

    let _ = wipe;

    //==========================================================
    head(1, "Creating a series");

    let host = k("host-a");
    let _ = client.delete(&WritePolicy::default(), &host).await;

    show(
        &client,
        &host,
        "ts_init(\"latency\", 60000, 1440)",
        &[as_val!("latency"), as_val!(MIN), as_val!(1440), Value::Nil, Value::Nil, as_val!(T0)],
        &["result", "bin", "config_bin", "ring_bytes", "step_ms", "slots", "agg", "kind", "mode"],
    )
    .await;

    show(
        &client,
        &host,
        "ts_init(\"latency\", 60000, 1440)   -- again, same shape",
        &[as_val!("latency"), as_val!(MIN), as_val!(1440), Value::Nil, Value::Nil, as_val!(T0)],
        &["result", "slots"],
    )
    .await;

    show(
        &client,
        &host,
        "ts_init(\"latency\", 1000, 1440)    -- a different step",
        &[as_val!("latency"), as_val!(1000), as_val!(1440), Value::Nil, Value::Nil, as_val!(T0)],
        &["result"],
    )
    .await;

    show(
        &client,
        &host,
        "ts_init(\"cpu\", 60000, 1440)       -- a second ring, same config bin",
        &[as_val!("cpu"), as_val!(MIN), as_val!(1440), Value::Nil, Value::Nil, as_val!(T0)],
        &["result", "bin", "config_bin", "ring_bytes"],
    )
    .await;

    show(
        &client,
        &host,
        "ts_init(\"latency\", 60000, 1440, \"agg=maxx\")",
        &[as_val!("latency"), as_val!(MIN), as_val!(1440), as_val!("agg=maxx"), Value::Nil, as_val!(T0)],
        &["result"],
    )
    .await;

    //==========================================================
    head(2, "Appending samples");

    show(
        &client,
        &host,
        "ts_append(\"latency\", T0, 12.4)",
        &[as_val!("latency"), as_val!(T0), Value::from(12.4)],
        &["result", "accepted", "bucket", "bucket_ms", "stored", "cleared", "last_bucket"],
    )
    .await;

    show(
        &client,
        &host,
        "ts_append(\"latency\", T0 + 30s, 19.9)   -- same minute",
        &[as_val!("latency"), as_val!(T0 + 30_000), Value::from(19.9)],
        &["result", "accepted", "bucket", "stored"],
    )
    .await;

    show(
        &client,
        &host,
        "ts_append(\"latency\", T0 + 5min, 13.1)  -- four minutes skipped",
        &[as_val!("latency"), as_val!(T0 + 5 * MIN), Value::from(13.1)],
        &["result", "accepted", "bucket", "cleared", "last_bucket"],
    )
    .await;

    show(
        &client,
        &host,
        "ts_append(\"latency\", T0 + 2min, 11.0)  -- a late sample",
        &[as_val!("latency"), as_val!(T0 + 2 * MIN), Value::from(11.0)],
        &["result", "accepted", "bucket", "stored", "last_bucket"],
    )
    .await;

    //==========================================================
    head(3, "Appending a batch");

    let batch = k("host-b");
    let _ = client.delete(&WritePolicy::default(), &batch).await;

    init(&client, &batch, "latency", MIN, 60, None).await;

    let times: Vec<Value> = (0..60).map(|b| as_val!(T0 + b * MIN)).collect();
    let values: Vec<Value> = (0..60).map(|b| Value::from(10.0 + (b % 7) as f64)).collect();

    show(
        &client,
        &batch,
        "ts_append_many(\"latency\", [60 timestamps], [60 values])",
        &[as_val!("latency"), Value::from(times.clone()), Value::from(values.clone())],
        &["offered", "accepted", "placed", "combined", "backfilled", "too_old", "cleared", "last_bucket"],
    )
    .await;

    show(
        &client,
        &batch,
        "ts_append_many(\"latency\", [the same 60], [the same 60])  -- replayed",
        &[as_val!("latency"), Value::from(times), Value::from(values)],
        &["offered", "accepted", "placed", "combined", "backfilled", "too_old"],
    )
    .await;

    //==========================================================
    head(4, "Refusals: nothing is changed, and you are told why");

    let guard = k("host-c");
    let _ = client.delete(&WritePolicy::default(), &guard).await;

    init(&client, &guard, "v", MIN, 10, None).await;

    for b in 0..10 {
        append(&client, &guard, "v", T0 + b * MIN, Value::from(b as f64)).await;
    }

    show(
        &client,
        &guard,
        "ts_append(\"v\", T0 - 1h, 5.0)          -- older than the window",
        &[as_val!("v"), as_val!(T0 - 60 * MIN), Value::from(5.0)],
        &["result", "accepted", "bucket"],
    )
    .await;

    show(
        &client,
        &guard,
        "ts_append(\"v\", T0_in_seconds, 5.0)    -- seconds, not milliseconds",
        &[as_val!("v"), as_val!(T0 / 1000), Value::from(5.0)],
        &["result", "accepted"],
    )
    .await;

    show(
        &client,
        &guard,
        "ts_append(\"v\", T0 * 1000, 5.0)        -- microseconds, not milliseconds",
        &[as_val!("v"), as_val!(T0.saturating_mul(1000)), Value::from(5.0)],
        &["result", "accepted"],
    )
    .await;

    let strict = k("host-d");
    let _ = client.delete(&WritePolicy::default(), &strict).await;

    init(&client, &strict, "v", MIN, 10, Some("backfill=0")).await;
    append(&client, &strict, "v", T0 + 5 * MIN, Value::from(1.0)).await;

    show(
        &client,
        &strict,
        "ts_append(\"v\", T0 + 1min, 2.0)        -- backfill=0",
        &[as_val!("v"), as_val!(T0 + MIN), Value::from(2.0)],
        &["result", "accepted"],
    )
    .await;

    //==========================================================
    head(5, "Aggregation modes: three samples in one minute");

    for mode in ["last", "first", "min", "max", "sum", "count"] {
        let key = k(&format!("agg-{mode}"));
        let _ = client.delete(&WritePolicy::default(), &key).await;

        init(&client, &key, "v", MIN, 4, Some(&format!("agg={mode}"))).await;

        for v in [5.0f64, 1.0, 3.0] {
            append(&client, &key, "v", T0, Value::from(v)).await;
        }

        println!("  agg={mode:<6}  -- append 5.0, 1.0, 3.0 to the same minute, then:");

        show(
            &client,
            &key,
            "ts_report(\"v\")",
            &[as_val!("v")],
            &["values", "known"],
        )
        .await;
    }

    //==========================================================
    head(6, "Integer series stay exact");

    let ints = k("host-e");
    let _ = client.delete(&WritePolicy::default(), &ints).await;

    init(&client, &ints, "bytes", MIN, 10, Some("kind=i64,agg=sum")).await;

    let big = (1i64 << 53) + 1;

    show(
        &client,
        &ints,
        "ts_append(\"bytes\", T0, 9007199254740993)   -- 2^53 + 1",
        &[as_val!("bytes"), as_val!(T0), as_val!(big)],
        &["result", "stored"],
    )
    .await;

    show(
        &client,
        &ints,
        "ts_append(\"bytes\", T0, 9007199254740993)   -- again, agg=sum",
        &[as_val!("bytes"), as_val!(T0), as_val!(big)],
        &["result", "stored"],
    )
    .await;

    show(
        &client,
        &ints,
        "ts_stats(\"bytes\")",
        &[as_val!("bytes")],
        &["count", "sum", "min", "max"],
    )
    .await;

    show(
        &client,
        &ints,
        "ts_append(\"bytes\", T0 + 1min, 1.5)         -- a double into an i64 ring",
        &[as_val!("bytes"), as_val!(T0 + MIN), Value::from(1.5)],
        &["result"],
    )
    .await;

    //==========================================================
    head(7, "Counters: rates from a running total");

    let ctr = k("iface-0");
    let _ = client.delete(&WritePolicy::default(), &ctr).await;

    init(&client, &ctr, "rx", MIN, 8, Some("kind=i64,mode=counter,agg=sum")).await;

    show(
        &client,
        &ctr,
        "ts_append(\"rx\", T0, 1000000)          -- the first reading",
        &[as_val!("rx"), as_val!(T0), as_val!(1_000_000)],
        &["result", "counter", "stored"],
    )
    .await;

    show(
        &client,
        &ctr,
        "ts_append(\"rx\", T0 + 1min, 1240000)",
        &[as_val!("rx"), as_val!(T0 + MIN), as_val!(1_240_000)],
        &["result", "counter", "stored"],
    )
    .await;

    show(
        &client,
        &ctr,
        "ts_append(\"rx\", T0 + 2min, 4000)      -- the process restarted",
        &[as_val!("rx"), as_val!(T0 + 2 * MIN), as_val!(4_000)],
        &["result", "counter", "stored", "counter_resets"],
    )
    .await;

    show(
        &client,
        &ctr,
        "ts_append(\"rx\", T0 + 3min, 260000)    -- measured from the new baseline",
        &[as_val!("rx"), as_val!(T0 + 3 * MIN), as_val!(260_000)],
        &["result", "counter", "stored"],
    )
    .await;

    show(
        &client,
        &ctr,
        "ts_report(\"rx\")",
        &[as_val!("rx")],
        &["values", "known", "unknown", "counter_resets"],
    )
    .await;

    let wrap = k("iface-1");
    let _ = client.delete(&WritePolicy::default(), &wrap).await;

    init(&client, &wrap, "rx", MIN, 8, Some("kind=i64,mode=counter,agg=sum,wrap=32")).await;

    let ceiling = 1i64 << 32;

    append(&client, &wrap, "rx", T0, as_val!(ceiling - 100)).await;

    println!("  wrap=32, previous reading 4294967196, next reading 50:");

    show(
        &client,
        &wrap,
        "ts_append(\"rx\", T0 + 1min, 50)",
        &[as_val!("rx"), as_val!(T0 + MIN), as_val!(50)],
        &["result", "counter", "stored", "counter_resets"],
    )
    .await;

    //==========================================================
    head(8, "Reading a series");

    let read = k("host-f");
    let _ = client.delete(&WritePolicy::default(), &read).await;

    init(&client, &read, "latency", MIN, 12, None).await;

    show(
        &client,
        &read,
        "ts_report(\"latency\")                  -- before any sample",
        &[as_val!("latency")],
        &["result", "points", "known"],
    )
    .await;

    for (b, v) in [
        (0i64, 12.4f64),
        (1, 15.1),
        (2, 11.8),
        (3, 45.2),
        (6, 98.6),
        (7, 14.0),
        (8, 13.2),
        (9, 12.9),
        (10, 16.4),
        (11, 12.1),
    ] {
        append(&client, &read, "latency", T0 + b * MIN, Value::from(v)).await;
    }

    show(
        &client,
        &read,
        "ts_report(\"latency\")",
        &[as_val!("latency")],
        &["result", "points", "known", "unknown", "bucket_ms", "from_ms", "to_ms", "values"],
    )
    .await;

    show(
        &client,
        &read,
        "ts_report(\"latency\", nil, nil, 4)     -- downsample to 4 points",
        &[as_val!("latency"), Value::Nil, Value::Nil, as_val!(4)],
        &["points", "bucket_ms", "downsampled_with", "values"],
    )
    .await;

    show(
        &client,
        &read,
        "ts_report(\"latency\", T0+6min, T0+8min)",
        &[as_val!("latency"), as_val!(T0 + 6 * MIN), as_val!(T0 + 8 * MIN)],
        &["points", "from_ms", "to_ms", "values"],
    )
    .await;

    show(
        &client,
        &read,
        "ts_stats(\"latency\")",
        &[as_val!("latency")],
        &["count", "unknown", "sum", "mean", "min", "max", "first", "first_ms", "last", "last_ms"],
    )
    .await;

    show(
        &client,
        &read,
        "ts_stats(\"latency\", T0+6min, T0+8min)",
        &[as_val!("latency"), as_val!(T0 + 6 * MIN), as_val!(T0 + 8 * MIN)],
        &["count", "sum", "mean", "min", "max"],
    )
    .await;

    show(
        &client,
        &read,
        "ts_report(\"latency\", T0+1h, T0+2h)    -- a window past the data",
        &[as_val!("latency"), as_val!(T0 + 60 * MIN), as_val!(T0 + 120 * MIN)],
        &["result", "points"],
    )
    .await;

    //==========================================================
    head(9, "Several series in one record");

    let multi = k("host-g");
    let _ = client.delete(&WritePolicy::default(), &multi).await;

    for bin in ["cpu", "mem", "net"] {
        init(&client, &multi, bin, MIN, 6, None).await;
        append(&client, &multi, bin, T0, Value::from(1.0)).await;
    }

    // A different shape needs its own config bin.
    let _ = udf(
        &client,
        &multi,
        "ts_init",
        &[
            as_val!("errors"),
            as_val!(1_000),
            as_val!(60),
            as_val!("agg=sum"),
            as_val!("errcfg"),
            as_val!(T0),
        ],
    )
    .await;

    let _ = udf(
        &client,
        &multi,
        "ts_append",
        &[as_val!("errors"), as_val!(T0), as_val!(1), as_val!("errcfg")],
    )
    .await;

    let record = client
        .get(&ReadPolicy::default(), &multi, Bins::All)
        .await
        .expect("get");

    let mut names: Vec<String> = record.bins.keys().cloned().collect();

    names.sort();

    println!("  bins in the record after four ts_init calls");
    println!("    -> {}", names.join(", "));

    show(
        &client,
        &multi,
        "ts_report(\"errors\", nil, nil, nil, \"errcfg\")",
        &[as_val!("errors"), Value::Nil, Value::Nil, Value::Nil, as_val!("errcfg")],
        &["step_ms", "slots", "agg", "known", "values"],
    )
    .await;

    //==========================================================
    head(10, "Rolling up a fleet");

    let fleet = format!("{set}_fleet");

    for (d, base) in [(0i64, 10.0f64), (1, 20.0), (2, 30.0)] {
        let key = as_key!(ns.as_str(), fleet.as_str(), d);
        let _ = client.delete(&WritePolicy::default(), &key).await;

        init(&client, &key, "rps", MIN, 6, None).await;

        let t: Vec<Value> = (0..6).map(|b| as_val!(T0 + b * MIN)).collect();
        let v: Vec<Value> = (0..6).map(|b| Value::from(base + b as f64)).collect();

        let _ = udf(
            &client,
            &key,
            "ts_append_many",
            &[as_val!("rps"), Value::from(t), Value::from(v)],
        )
        .await;
    }

    // One host reports a different shape, to show it is refused rather than resampled.
    let odd = as_key!(ns.as_str(), fleet.as_str(), 99);
    let _ = client.delete(&WritePolicy::default(), &odd).await;

    init(&client, &odd, "rps", 1_000, 6, None).await;
    append(&client, &odd, "rps", T0, Value::from(999.0)).await;

    use aerospike_udf_example_timeseries::Rollup;
    use aerospike_udf_sdk_native::client::Combiner;

    for (call, args) in [
        ("ts_rollup(\"rps\")", vec![as_val!("rps")]),
        ("ts_rollup(\"rps\", \"max\")", vec![as_val!("rps"), as_val!("max")]),
        ("ts_rollup(\"rps\", \"min\")", vec![as_val!("rps"), as_val!("min")]),
        (
            "ts_rollup(\"rps\", \"sum\", 3)",
            vec![as_val!("rps"), as_val!("sum"), as_val!(3)],
        ),
        (
            "ts_rollup(\"rps\", \"count\")",
            vec![as_val!("rps"), as_val!("count")],
        ),
        (
            "ts_rollup(\"rps\", \"sum\", nil, nil, 60000, 6)   -- shape pinned",
            vec![as_val!("rps"), as_val!("sum"), Value::Nil, Value::Nil, as_val!(MIN), as_val!(6)],
        ),
        (
            "ts_rollup(\"rps\", \"max\", nil, nil, 60000, 6)   -- shape pinned",
            vec![as_val!("rps"), as_val!("max"), Value::Nil, Value::Nil, as_val!(MIN), as_val!(6)],
        ),
        (
            "ts_rollup(\"rps\", \"min\", nil, nil, 60000, 6)   -- shape pinned",
            vec![as_val!("rps"), as_val!("min"), Value::Nil, Value::Nil, as_val!(MIN), as_val!(6)],
        ),
        (
            "ts_rollup(\"rps\", \"sum\", 3, nil, 60000, 6)     -- pinned and downsampled",
            vec![as_val!("rps"), as_val!("sum"), as_val!(3), Value::Nil, as_val!(MIN), as_val!(6)],
        ),
    ] {
        let mut statement = Statement::new(&ns, &fleet, Bins::All);

        statement.set_aggregate_function(&module(), "ts_rollup", Some(&args));

        println!("  {call}");

        let combiner = Combiner::<Rollup>::new(&args);

        let Ok(mut combiner) = combiner else {
            println!("    -> ERROR: {}", combiner.err().unwrap());
            continue;
        };

        let records = client
            .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
            .await;

        let Ok(records) = records else {
            println!("    -> ERROR: {}", records.err().unwrap());
            continue;
        };

        let mut stream = records.into_stream();
        let mut failed = None;

        while let Some(record) = stream.next().await {
            match record {
                Ok(r) => {
                    for value in r.bins.values() {
                        if let Err(e) = combiner.absorb(value) {
                            failed = Some(format!("{e}"));
                        }
                    }
                }
                Err(e) => failed = Some(format!("{e}")),
            }
        }

        if let Some(e) = failed {
            println!("    -> ERROR: {}", e.replace('\n', " "));
            continue;
        }

        match combiner.finish() {
            Ok(v) => {
                let map = as_map(&v).unwrap_or_default();
                let keys = [
                    "result", "op", "records", "samples", "skipped_shape", "shape_declared",
                    "step_ms", "points", "bucket_ms", "values", "counts",
                ];
                let shown: Vec<String> = keys
                    .iter()
                    .filter_map(|key| map.get(*key).map(|x| format!("{key}={}", render(x))))
                    .collect();

                println!("    -> {}", shown.join("  "));
            }
            Err(e) => println!("    -> ERROR: {}", format!("{e}").replace('\n', " ")),
        }
    }

    //==========================================================
    head(13, "Long retention with archives");

    let arch = k("archived");
    let _ = client.delete(&WritePolicy::default(), &arch).await;

    show(
        &client,
        &arch,
        "ts_init(\"cpu\", 0, 0, \"archives=default\")",
        &[
            as_val!("cpu"),
            as_val!(1_000),
            as_val!(3_600),
            as_val!("archives=default"),
            Value::Nil,
            as_val!(T0),
        ],
        &["result", "archives", "rings_created", "rings_existing", "ring_bytes", "step_ms", "slots"],
    )
    .await;

    // Nine samples, one per second; the tenth is the one shown.
    for i in 1..=9i64 {
        append(&client, &arch, "cpu", T0 + (i - 1) * 1_000, Value::from(i as f64)).await;
    }

    show(
        &client,
        &arch,
        "ts_append(\"cpu\", T0 + 9000, 10.0)",
        &[as_val!("cpu"), as_val!(T0 + 9_000), Value::from(10.0)],
        &["result", "accepted", "archives", "archives_accepted", "archives_refused", "per_archive"],
    )
    .await;

    for res in ["s", "m", "h", "y"] {
        show(
            &client,
            &arch,
            &format!("ts_report(\"cpu\", T0, T0 + 9000, nil, nil, \"{res}\")"),
            &[
                as_val!("cpu"),
                as_val!(T0),
                as_val!(T0 + 9_000),
                Value::Nil,
                Value::Nil,
                as_val!(res),
            ],
            &["result", "resolution", "archive_bin", "step_ms", "points", "known", "values"],
        )
        .await;
    }

    // Archive selection, from the descriptors alone - so `now_ms` is pinned and the choice is reproducible.
    let day = 86_400_000i64;

    for (label, back) in [
        ("ten minutes", 10 * MIN),
        ("two hours", 2 * 60 * MIN),
        ("three days", 3 * day),
        ("thirty days", 30 * day),
        ("two hundred days", 200 * day),
        ("three years", 3 * 365 * day),
        ("fifty years", 50 * 365 * day),
    ] {
        show(
            &client,
            &arch,
            &format!("ts_report(\"cpu\", T0 - {label}, nil, 4, nil, nil, T0)"),
            &[
                as_val!("cpu"),
                as_val!(T0 - back),
                Value::Nil,
                as_val!(4),
                Value::Nil,
                Value::Nil,
                as_val!(T0),
            ],
            &["result", "resolution", "archive_bin", "points", "known", "values", "archive_chosen_because"],
        )
        .await;
    }

    show(
        &client,
        &arch,
        "ts_report(\"cpu\", nil, nil, nil, nil, \"nanosecond\")",
        &[as_val!("cpu"), Value::Nil, Value::Nil, Value::Nil, Value::Nil, as_val!("nanosecond")],
        &["result"],
    )
    .await;

    show(
        &client,
        &arch,
        "ts_info(\"cpu\")",
        &[as_val!("cpu")],
        &["result", "archives", "rings_present", "ring_bytes", "created_ms"],
    )
    .await;

    // xff: an hourly archive that wants half of its sixty possible minute-buckets.
    let sparse = k("sparse");
    let _ = client.delete(&WritePolicy::default(), &sparse).await;

    show(
        &client,
        &sparse,
        "ts_init(\"s\", 0, 0, \"archives=m:60;h:24:mean:50\")",
        &[
            as_val!("s"),
            as_val!(MIN),
            as_val!(60),
            as_val!("archives=m:60;h:24:mean:50"),
            Value::Nil,
            as_val!(T0),
        ],
        &["result", "archives", "ring_bytes"],
    )
    .await;

    for i in 0..5i64 {
        append(&client, &sparse, "s", T0 + i * MIN, Value::from(7.0)).await;
    }

    for res in ["m", "h"] {
        show(
            &client,
            &sparse,
            &format!("ts_report(\"s\", T0, T0 + 4 * MIN, nil, nil, \"{res}\")   -- 5 samples"),
            &[
                as_val!("s"),
                as_val!(T0),
                as_val!(T0 + 4 * MIN),
                Value::Nil,
                Value::Nil,
                as_val!(res),
            ],
            &["result", "resolution", "points", "known", "unknown", "xff_suppressed", "values"],
        )
        .await;
    }

    // The heartbeat: a counter that went quiet for longer than it is allowed to.
    let beat = k("heartbeat");
    let _ = client.delete(&WritePolicy::default(), &beat).await;

    show(
        &client,
        &beat,
        "ts_init(\"b\", 0, 0, \"kind=i64,mode=counter,archives=m:60:sum,heartbeat=120000\")",
        &[
            as_val!("b"),
            as_val!(MIN),
            as_val!(60),
            as_val!("kind=i64,mode=counter,archives=m:60:sum,heartbeat=120000"),
            Value::Nil,
            as_val!(T0),
        ],
        &["result", "archives", "heartbeat_ms"],
    )
    .await;

    for (label, at, reading) in [
        ("T0", T0, 1_000i64),
        ("T0 + 1 min", T0 + MIN, 1_100),
        ("T0 + 6 min", T0 + 6 * MIN, 5_000),
        ("T0 + 7 min", T0 + 7 * MIN, 5_050),
    ] {
        show(
            &client,
            &beat,
            &format!("ts_append(\"b\", {label}, {reading})"),
            &[as_val!("b"), as_val!(at), as_val!(reading)],
            &["result", "counter", "stored"],
        )
        .await;
    }

    // ts_append_all: three metrics under one config, one commit.
    let together = k("together");
    let _ = client.delete(&WritePolicy::default(), &together).await;

    for bin in ["cpu", "mem", "net"] {
        init(&client, &together, bin, MIN, 60, Some("archives=m:60;h:24")).await;
    }

    show(
        &client,
        &together,
        "ts_append_all([\"cpu\", \"mem\", \"net\"], T0, [1.0, 2.0, 3.0])",
        &[
            Value::from(vec![as_val!("cpu"), as_val!("mem"), as_val!("net")]),
            as_val!(T0),
            Value::from(vec![Value::from(1.0), Value::from(2.0), Value::from(3.0)]),
        ],
        &["result", "metrics", "rings_accepted", "rings_refused", "per_metric"],
    )
    .await;

    println!();

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &fleet, 0)
        .await;
}
