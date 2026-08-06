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

//! Prints what the verbs actually return, so the README can quote it rather than describe it.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test --test demo -- --ignored --nocapture
//! ```
//!
//! Not an assertion in sight: this exists to produce output. The correctness of the same paths is asserted in
//! `against_a_server`.

use std::env;

use aerospike_core::{
    as_key, as_val, Bins, Client, ClientPolicy, Key, PartitionFilter, QueryPolicy, Statement, Value,
    WritePolicy,
};
use futures::StreamExt;

/// 2026-01-01T00:00:00Z, so the bucket indices in the output are stable and quotable.
const BASE_MS: i64 = 1_767_225_600_000;
const STEP: i64 = 60_000;

fn module() -> String {
    env::var("AEROSPIKE_TIMESERIES_MODULE").unwrap_or_else(|_| "timeseries".to_string())
}

fn namespace() -> String {
    env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

async fn connect() -> Option<Client> {
    let asked = env::var("AEROSPIKE_HOSTS").is_ok();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    match Client::new(&ClientPolicy::default(), &hosts).await {
        Ok(client) => Some(client),
        Err(e) if asked => panic!("AEROSPIKE_HOSTS is set but unreachable: {e}"),
        Err(_) => None,
    }
}

/// One value, rendered the way a reader wants to see it.
fn render(v: &Value) -> String {
    match v {
        Value::Nil => "nil".to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            let n = f64::from(f.clone());

            if n.fract() == 0.0 && n.abs() < 1e15 {
                format!("{n:.1}")
            } else {
                format!("{n}")
            }
        }
        Value::String(s) => format!("\"{s}\""),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(render).collect();

            format!("[{}]", inner.join(", "))
        }
        // Nested maps, which `ts_init`'s archive table and `ts_append`'s per-archive outcomes both are. Without this
        // they printed as `SortedMap({String("agg"): String("last"), …})` - accurate, and unreadable enough that the
        // README could not quote it.
        Value::HashMap(m) => render_map(m.iter().map(|(k, v)| (k, v))),
        Value::SortedMap(m) => render_map(m.iter().map(|(k, v)| (k, v))),
        Value::OrderedMap(m) => render_map(m.iter().map(|(k, v)| (k, v))),
        other => format!("{other:?}"),
    }
}

fn render_map<'a>(entries: impl Iterator<Item = (&'a Value, &'a Value)>) -> String {
    let inner: Vec<String> = entries
        .map(|(k, v)| format!("{}: {}", render(k).trim_matches('"'), render(v)))
        .collect();

    format!("{{{}}}", inner.join(", "))
}

/// Print a returned map with its keys in a fixed, readable order rather than whatever the map yields.
fn show(title: &str, call: &str, value: &Value) {
    let pairs: Vec<(String, Value)> = match value {
        Value::HashMap(m) => m.iter().map(|(k, v)| (render(k), v.clone())).collect(),
        Value::SortedMap(m) => m.iter().map(|(k, v)| (render(k), v.clone())).collect(),
        Value::OrderedMap(m) => m.iter().map(|(k, v)| (render(k), v.clone())).collect(),
        other => {
            println!("{title}: not a map: {other:?}");
            return;
        }
    };

    // Shape first, then the window, then the answer, then the caveats. Anything unlisted goes last, so a key added
    // later still shows up instead of vanishing.
    const ORDER: &[&str] = &[
        "bin", "op", "step_ms", "slots", "kind", "mode", "agg", "config_bin", "ring_bytes",
        "result", "accepted", "bucket", "bucket_ms", "cleared", "counter", "stored", "last_bucket",
        "from_ms", "to_ms", "last_ms", "buckets", "points", "known", "unknown",
        "count", "sum", "mean", "min", "max", "first", "first_ms", "last", "last_ms",
        "downsampled_with", "values", "counts",
        "records", "samples", "appends", "offered", "placed", "combined", "backfilled",
        "too_old", "backfill_refused", "would_wipe",
        "skipped_absent", "skipped_shape", "skipped_unreadable", "dropped_old",
        "counter_resets", "saturated",
        "resolution", "archive_bin", "archive_chosen_because", "xff_suppressed",
        "archives", "archives_accepted", "archives_refused", "per_archive",
        "rings_created", "rings_existing", "rings_present", "metrics", "per_metric",
        "created_ms", "allow_backfill", "allow_wipe", "wrap_bits", "heartbeat_ms", "table",
    ];

    let rank = |k: &str| ORDER.iter().position(|o| *o == k).unwrap_or(ORDER.len());
    let mut sorted = pairs;

    sorted.sort_by(|a, b| {
        let (ka, kb) = (a.0.trim_matches('"'), b.0.trim_matches('"'));

        rank(ka).cmp(&rank(kb)).then(ka.cmp(kb))
    });

    println!("\n{title}");
    println!("  {call}");
    println!();

    for (k, v) in sorted {
        println!("    {:<18} {}", k.trim_matches('"'), render(&v));
    }
}

async fn udf(client: &Client, key: &Key, function: &str, args: &[Value]) -> Value {
    client
        .execute_udf(&WritePolicy::default(), key, &module(), function, Some(args))
        .await
        .unwrap_or_else(|e| panic!("{function}: {e}"))
        .unwrap_or(Value::Nil)
}

async fn init(client: &Client, key: &Key, bin: &str, slots: i64, opts: Option<&str>) {
    udf(
        client,
        key,
        "ts_init",
        &[
            as_val!(bin),
            as_val!(STEP),
            as_val!(slots),
            opts.map_or(Value::Nil, |o| as_val!(o)),
            Value::Nil,
            as_val!(BASE_MS),
        ],
    )
    .await;
}

#[tokio::test]
#[ignore = "prints output for the README; run it deliberately"]
async fn demo() {
    let Some(client) = connect().await else {
        eprintln!("skipping demo: no server");
        return;
    };

    let ns = namespace();
    let set = format!("ts_demo_{}", std::process::id());

    //==========================================================
    // One series: twelve minutes of request latency, with a two-minute gap where nothing reported.
    //

    let key = as_key!(ns.as_str(), set.as_str(), "web-01");

    init(&client, &key, "latency", 12, None).await;

    let readings: [(i64, f64); 10] = [
        (0, 12.4),
        (1, 15.1),
        (2, 11.8),
        (3, 45.2),
        // buckets 4 and 5: the host was down and reported nothing.
        (6, 98.6),
        (7, 14.0),
        (8, 13.2),
        (9, 12.9),
        (10, 16.4),
        (11, 12.1),
    ];

    for (bucket, value) in readings {
        udf(
            &client,
            &key,
            "ts_append",
            &[as_val!("latency"), as_val!(BASE_MS + bucket * STEP), Value::from(value)],
        )
        .await;
    }

    println!("\n================================================================");
    println!(" one series: 12 one-minute buckets, two of them never reported");
    println!("================================================================");

    let full = udf(&client, &key, "ts_report", &[as_val!("latency")]).await;

    show("ts_report - every bucket", "ts_report(\"latency\")", &full);

    let down = udf(
        &client,
        &key,
        "ts_report",
        &[as_val!("latency"), Value::Nil, Value::Nil, as_val!(4)],
    )
    .await;

    show(
        "ts_report - downsampled on the server",
        "ts_report(\"latency\", nil, nil, 4)",
        &down,
    );

    let window = udf(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("latency"),
            as_val!(BASE_MS + 6 * STEP),
            as_val!(BASE_MS + 8 * STEP),
        ],
    )
    .await;

    show(
        "ts_report - a window",
        "ts_report(\"latency\", 1767225960000, 1767226080000)",
        &window,
    );

    let stats = udf(&client, &key, "ts_stats", &[as_val!("latency")]).await;

    show("ts_stats - no list at all", "ts_stats(\"latency\")", &stats);

    //==========================================================
    // A counter series, so the reset shows up.
    //

    let counter_key = as_key!(ns.as_str(), set.as_str(), "iface-0");

    init(
        &client,
        &counter_key,
        "rx_bytes",
        8,
        Some("kind=i64,mode=counter,agg=sum"),
    )
    .await;

    // A monotonic byte counter that restarts at bucket 4 when the process was bounced.
    for (bucket, raw) in [
        (0i64, 1_000_000i64),
        (1, 1_240_000),
        (2, 1_505_000),
        (3, 1_720_000),
        (4, 4_000),
        (5, 260_000),
        (6, 505_000),
        (7, 740_000),
    ] {
        udf(
            &client,
            &counter_key,
            "ts_append",
            &[as_val!("rx_bytes"), as_val!(BASE_MS + bucket * STEP), as_val!(raw)],
        )
        .await;
    }

    println!("\n================================================================");
    println!(" a counter series: deltas, and a restart at bucket 4");
    println!("================================================================");

    let counter_report = udf(&client, &counter_key, "ts_report", &[as_val!("rx_bytes")]).await;

    show("ts_report - a counter", "ts_report(\"rx_bytes\")", &counter_report);

    let counter_stats = udf(&client, &counter_key, "ts_stats", &[as_val!("rx_bytes")]).await;

    show("ts_stats - a counter", "ts_stats(\"rx_bytes\")", &counter_stats);

    //==========================================================
    // Three hosts, rolled up server-side.
    //

    let fleet_set = format!("{set}_fleet");

    for (d, base) in [(0i64, 10.0f64), (1, 20.0), (2, 30.0)] {
        let fkey = as_key!(ns.as_str(), fleet_set.as_str(), d);

        init(&client, &fkey, "rps", 6, None).await;

        let times: Vec<Value> = (0..6).map(|b| as_val!(BASE_MS + b * STEP)).collect();
        let values: Vec<Value> = (0..6).map(|b| Value::from(base + b as f64)).collect();

        udf(
            &client,
            &fkey,
            "ts_append_many",
            &[as_val!("rps"), Value::from(times), Value::from(values)],
        )
        .await;
    }

    println!("\n================================================================");
    println!(" three hosts rolled up: 10+b, 20+b and 30+b requests per second");
    println!("================================================================");

    use aerospike_udf_example_timeseries::Rollup;
    use aerospike_udf_sdk_native::client::Combiner;

    for (label, args) in [
        ("ts_rollup - sum across records", vec![as_val!("rps")]),
        (
            "ts_rollup - the worst host per bucket",
            vec![as_val!("rps"), as_val!("max")],
        ),
        (
            "ts_rollup - summed and downsampled to 3 points",
            vec![as_val!("rps"), as_val!("sum"), as_val!(3)],
        ),
    ] {
        let mut statement = Statement::new(&ns, &fleet_set, Bins::All);

        statement.set_aggregate_function(&module(), "ts_rollup", Some(&args));

        let records = client
            .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
            .await
            .expect("query");

        let mut combiner = Combiner::<Rollup>::new(&args).expect("combiner");
        let mut stream = records.into_stream();

        while let Some(record) = stream.next().await {
            for value in record.unwrap().bins.values() {
                combiner.absorb(value).expect("absorb");
            }
        }

        let rendered: Vec<String> = args.iter().map(render).collect();

        show(
            label,
            &format!("ts_rollup({})", rendered.join(", ")),
            &combiner.finish().expect("finalize"),
        );
    }

    println!("\n================================================================");
    println!(" an archive hierarchy: one sample, seven resolutions");
    println!("================================================================");

    let arch_set = format!("tsdemo_arch_{}", std::process::id());
    let akey = as_key!(ns.as_str(), arch_set.as_str(), 1);

    let _ = client.delete(&WritePolicy::default(), &akey).await;

    show(
        "ts_init - the whole default set",
        r#"ts_init("cpu", 0, 0, "archives=default")"#,
        &udf(
            &client,
            &akey,
            "ts_init",
            &[
                as_val!("cpu"),
                as_val!(1_000),
                as_val!(3_600),
                as_val!("archives=default"),
                Value::Nil,
                as_val!(BASE_MS),
            ],
        )
        .await,
    );

    // Nine samples, one per second, values 1 through 9; the shown call below is the tenth. Appending ten here and then
    // showing an eleventh made every coarse mean 65/11, which is right and reads like a mistake.
    for i in 1..=9i64 {
        udf(
            &client,
            &akey,
            "ts_append",
            &[as_val!("cpu"), as_val!(BASE_MS + (i - 1) * 1_000), Value::from(i as f64)],
        )
        .await;
    }

    show(
        "ts_append - the tenth sample, and where it landed",
        r#"ts_append("cpu", base + 9000, 10.0)"#,
        &udf(
            &client,
            &akey,
            "ts_append",
            &[as_val!("cpu"), as_val!(BASE_MS + 9_000), Value::from(10.0)],
        )
        .await,
    );

    for (label, res) in [("the per-second archive", "s"), ("the hourly archive", "h")] {
        show(
            &format!("ts_report - {label}"),
            &format!(r#"ts_report("cpu", base, base + 9000, nil, nil, "{res}")"#),
            &udf(
                &client,
                &akey,
                "ts_report",
                &[
                    as_val!("cpu"),
                    as_val!(BASE_MS),
                    as_val!(BASE_MS + 9_000),
                    Value::Nil,
                    Value::Nil,
                    as_val!(res),
                ],
            )
            .await,
        );
    }

    show(
        "ts_report - three years back, so it chooses for itself",
        r#"ts_report("cpu", base - 3y, nil, 4, nil, nil, base)"#,
        &udf(
            &client,
            &akey,
            "ts_report",
            &[
                as_val!("cpu"),
                as_val!(BASE_MS - 3 * 365 * 86_400_000),
                Value::Nil,
                as_val!(4),
                Value::Nil,
                Value::Nil,
                as_val!(BASE_MS),
            ],
        )
        .await,
    );

    show(
        "ts_info - which bins exist and how far back each reaches",
        r#"ts_info("cpu")"#,
        &udf(&client, &akey, "ts_info", &[as_val!("cpu")]).await,
    );

    println!();

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &fleet_set, 0)
        .await;
    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &arch_set, 0)
        .await;
}
