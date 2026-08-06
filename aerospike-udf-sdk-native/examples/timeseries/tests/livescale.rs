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

//! Millions of samples through a **real particle**, and the record-size claims checked rather than computed.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test --release --test livescale -- --ignored --nocapture --test-threads=1
//! ```
//!
//! # What this reaches that `scale.rs` cannot
//!
//! `scale.rs` drives `RingMut` directly, so it proves the arithmetic and nothing else. Everything below has to survive
//! being written into a blob particle, read back out of one, and — for the sixteen-metric case — fit inside a write
//! block. Those are the parts that cannot be reasoned about from the Rust side:
//!
//! - a 512 KB particle rewritten in place a hundred times over
//! - **whether sixteen archived metrics actually fit one record**, which the README claims from arithmetic
//! - a report over a window that has lapped, built by the host and shipped over the wire

use std::collections::HashMap;
use std::env;
use std::time::Instant;

use aerospike_core::{
    as_key, as_val, Bins, Client, ClientPolicy, Key, ReadPolicy, Value, WritePolicy,
};

/// 2026-01-01T00:00:00Z.
const T0: i64 = 1_767_225_600_000;
const SEC: i64 = 1_000;

/// How many samples the heavy tests push, and how many go in one call.
fn scale() -> i64 {
    env::var("TS_SCALE")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1_000_000)
}

fn batch() -> i64 {
    env::var("TS_BATCH")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(20_000)
}

/// The sample at index `i` — integers, so every mean below is exact and a mismatch is a logic error.
fn value_at(i: i64) -> f64 {
    (i % 997) as f64
}

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
        Ok(c) => Some(c),
        Err(e) if asked => panic!("AEROSPIKE_HOSTS is set but unreachable: {e}"),
        Err(_) => None,
    }
}

fn as_map(value: &Value) -> HashMap<String, Value> {
    let pairs: Vec<(Value, Value)> = match value {
        Value::HashMap(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        Value::SortedMap(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        Value::OrderedMap(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        other => panic!("not a map: {other:?}"),
    };

    pairs
        .into_iter()
        .filter_map(|(k, v)| match k {
            Value::String(s) => Some((s, v)),
            _ => None,
        })
        .collect()
}

async fn must(client: &Client, key: &Key, function: &str, args: &[Value]) -> HashMap<String, Value> {
    match client
        .execute_udf(&WritePolicy::default(), key, &module(), function, Some(args))
        .await
    {
        Ok(Some(v)) => as_map(&v),
        Ok(None) => panic!("{function} returned nothing"),
        Err(e) => panic!("{function}: {e}"),
    }
}

fn i64_of(map: &HashMap<String, Value>, key: &str) -> Option<i64> {
    match map.get(key) {
        Some(Value::Int(i)) => Some(*i),
        _ => None,
    }
}

fn f64_of(map: &HashMap<String, Value>, key: &str) -> Option<f64> {
    match map.get(key) {
        Some(Value::Float(f)) => Some(f64::from(f.clone())),
        Some(Value::Int(i)) => Some(*i as f64),
        _ => None,
    }
}

fn str_of(map: &HashMap<String, Value>, key: &str) -> Option<String> {
    match map.get(key) {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn values_of(map: &HashMap<String, Value>) -> Vec<Option<f64>> {
    match map.get("values") {
        Some(Value::List(items)) => items
            .iter()
            .map(|v| match v {
                Value::Nil => None,
                Value::Float(f) => Some(f64::from(f.clone())),
                Value::Int(i) => Some(*i as f64),
                other => panic!("a series value should be a number or nil, got {other:?}"),
            })
            .collect(),
        other => panic!("no values list: {other:?}"),
    }
}

/// Push `n` one-second samples in batches, and return the wall clock and the total accepted.
async fn push(client: &Client, key: &Key, bin: &str, n: i64, per_call: i64) -> (f64, i64) {
    let started = Instant::now();
    let mut accepted = 0i64;
    let mut i = 0i64;

    while i < n {
        let count = per_call.min(n - i);
        let times: Vec<Value> = (i..i + count).map(|k| as_val!(T0 + k * SEC)).collect();
        let values: Vec<Value> = (i..i + count).map(|k| Value::from(value_at(k))).collect();

        let result = must(
            client,
            key,
            "ts_append_many",
            &[as_val!(bin), Value::from(times), Value::from(values)],
        )
        .await;

        let took = i64_of(&result, "accepted").unwrap_or(0);

        assert_eq!(took, count, "batch at {i} was partly refused: {result:?}");

        accepted += took;
        i += count;
    }

    (started.elapsed().as_secs_f64(), accepted)
}

/// The total bytes of every bin in a record, so a size claim can be checked instead of computed.
async fn record_bytes(client: &Client, key: &Key) -> (usize, usize) {
    let record = client
        .get(&ReadPolicy::default(), key, Bins::All)
        .await
        .expect("the record exists");

    let mut bytes = 0usize;

    for (name, value) in &record.bins {
        bytes += name.len();
        bytes += match value {
            Value::Blob(b) => b.len(),
            other => format!("{other:?}").len(),
        };
    }

    (record.bins.len(), bytes)
}

//==========================================================

/// Millions of samples into an archived series, read back at every resolution.
#[tokio::test]
#[ignore = "pushes millions of samples at a server; run it deliberately"]
async fn millions_of_samples_survive_the_particle_round_trip() {
    assert!(!cfg!(debug_assertions), "run this with --release");

    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = format!("tsls_{}", std::process::id());
    let key = as_key!(ns.as_str(), set.as_str(), 1);
    let n = scale();

    let _ = client.delete(&WritePolicy::default(), &key).await;

    let created = must(
        &client,
        &key,
        "ts_init",
        &[as_val!("v"), as_val!(SEC), as_val!(3_600), as_val!("archives=default"), Value::Nil, as_val!(T0)],
    )
    .await;

    assert_eq!(str_of(&created, "result").as_deref(), Some("created"));

    let (secs, accepted) = push(&client, &key, "v", n, batch()).await;

    println!(
        "\n  {n} samples in batches of {}: {secs:.2}s, {:.0} samples/s, {:.1} us/sample\n",
        batch(),
        n as f64 / secs,
        secs * 1e6 / n as f64
    );

    assert_eq!(accepted, n);

    // --- the per-second archive: the last 3,600 samples, each in its own bucket ---

    let last_ts = T0 + (n - 1) * SEC;
    let report = must(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("v"),
            as_val!(last_ts - 9 * SEC),
            as_val!(last_ts),
            Value::Nil,
            Value::Nil,
            as_val!("s"),
        ],
    )
    .await;

    let expected: Vec<Option<f64>> = ((n - 10)..n).map(|i| Some(value_at(i))).collect();

    assert_eq!(values_of(&report), expected, "the ten newest seconds");
    assert_eq!(i64_of(&report, "appends"), Some(n), "every append counted");

    // The oldest live second, and the one below it.
    let oldest = n - 3_600;
    let edge = must(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("v"),
            as_val!(T0 + (oldest - 1) * SEC),
            as_val!(T0 + oldest * SEC),
            Value::Nil,
            Value::Nil,
            as_val!("s"),
        ],
    )
    .await;

    assert_eq!(
        values_of(&edge),
        vec![Some(value_at(oldest))],
        "the window opens exactly 3,600 buckets back, so the one before it is gone"
    );

    // --- the minute archive: each bucket is the mean of its sixty seconds ---

    let minute_of = |i: i64| (T0 + i * SEC) / 60_000 * 60_000;
    // A minute well inside the window: the newest complete one.
    let target = (n - 1) / 60 - 1;
    let from = target * 60;
    let mean: f64 = (from..from + 60).map(value_at).sum::<f64>() / 60.0;

    let minutes = must(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("v"),
            as_val!(minute_of(from)),
            as_val!(minute_of(from)),
            Value::Nil,
            Value::Nil,
            as_val!("m"),
        ],
    )
    .await;

    assert_eq!(
        values_of(&minutes),
        vec![Some(mean)],
        "minute {target} is the exact mean of its sixty samples"
    );

    // --- the hour archive: 3,600 samples per bucket, and the mean is still exact ---

    let hour = (n - 1) / 3_600 - 1;
    let hour_from = hour * 3_600;
    let hour_mean: f64 = (hour_from..hour_from + 3_600).map(value_at).sum::<f64>() / 3_600.0;
    let hours = must(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("v"),
            as_val!(T0 + hour_from * SEC),
            as_val!(T0 + hour_from * SEC),
            Value::Nil,
            Value::Nil,
            as_val!("h"),
        ],
    )
    .await;

    assert_eq!(values_of(&hours), vec![Some(hour_mean)], "hour {hour}");

    // --- ts_stats over the whole per-second window ---

    let stats = must(
        &client,
        &key,
        "ts_stats",
        &[as_val!("v"), Value::Nil, Value::Nil, Value::Nil, as_val!("s")],
    )
    .await;

    let live: Vec<f64> = (n - 3_600..n).map(value_at).collect();

    assert_eq!(i64_of(&stats, "count"), Some(3_600));
    assert_eq!(f64_of(&stats, "sum"), Some(live.iter().sum::<f64>()));
    assert_eq!(f64_of(&stats, "min"), Some(live.iter().copied().fold(f64::MAX, f64::min)));
    assert_eq!(f64_of(&stats, "max"), Some(live.iter().copied().fold(f64::MIN, f64::max)));
    assert_eq!(f64_of(&stats, "last"), Some(value_at(n - 1)));

    // --- and the whole hierarchy is still the size it claims ---

    let (bins, bytes) = record_bytes(&client, &key).await;

    println!("  the record holds {bins} bins and {bytes} bytes after {n} samples\n");

    assert_eq!(bins, 8, "seven rings and a config");

    let info = must(&client, &key, "ts_info", &[as_val!("v")]).await;

    assert_eq!(i64_of(&info, "ring_bytes"), Some(62_232), "no growth: a ring is fixed size");
    assert_eq!(i64_of(&info, "rings_present"), Some(7));

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// **The README's sixteen-metrics-per-record claim, checked against a real write block.**
///
/// 16 × 62,232 is 995,712 bytes, which is arithmetic. Whether it fits alongside a config bin, sixteen bin names and
/// whatever the storage engine adds is not arithmetic, and the honest way to find out is to try.
#[tokio::test]
#[ignore = "builds a near-1 MB record; run it deliberately"]
async fn sixteen_archived_metrics_fit_one_record() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = format!("tsfit_{}", std::process::id());
    let key = as_key!(ns.as_str(), set.as_str(), 1);

    let _ = client.delete(&WritePolicy::default(), &key).await;

    // Two characters, so `m15~mo` is six and well inside the fifteen a bin name may be.
    let names: Vec<String> = (0..16).map(|i| format!("m{i}")).collect();

    for (i, name) in names.iter().enumerate() {
        let created = must(
            &client,
            &key,
            "ts_init",
            &[
                as_val!(name.as_str()),
                as_val!(SEC),
                as_val!(3_600),
                as_val!("archives=default"),
                Value::Nil,
                as_val!(T0),
            ],
        )
        .await;

        assert_eq!(
            str_of(&created, "result").as_deref(),
            Some("created"),
            "metric {i} ({name}) could not be created: {created:?}"
        );
    }

    let (bins, bytes) = record_bytes(&client, &key).await;

    println!("\n  sixteen archived metrics: {bins} bins, {bytes} bytes");
    println!("  a 1 MB write block leaves {} bytes of headroom\n", 1_048_576i64 - bytes as i64);

    assert_eq!(bins, 16 * 7 + 1, "seven rings per metric and one shared config");
    assert!(bytes < 1_048_576, "the whole point of the 60.8 KiB figure");

    // And it is still writable, which is the part a size calculation cannot tell you: a record that fits may still fail
    // to be *rewritten* if the write path needs headroom.
    let bin_list = Value::from(names.iter().map(|n| as_val!(n.as_str())).collect::<Vec<_>>());

    for round in 0..5i64 {
        let values = Value::from(
            (0..16)
                .map(|m| Value::from((round * 16 + m) as f64))
                .collect::<Vec<_>>(),
        );
        let applied = must(
            &client,
            &key,
            "ts_append_all",
            &[bin_list.clone(), as_val!(T0 + round * SEC), values],
        )
        .await;

        assert_eq!(str_of(&applied, "result").as_deref(), Some("ok"), "round {round}");
        assert_eq!(i64_of(&applied, "rings_accepted"), Some(16 * 7), "round {round}");
    }

    // Every metric kept its own values rather than another's.
    for (m, name) in names.iter().enumerate() {
        let report = must(
            &client,
            &key,
            "ts_report",
            &[
                as_val!(name.as_str()),
                as_val!(T0),
                as_val!(T0 + 4 * SEC),
                Value::Nil,
                Value::Nil,
                as_val!("s"),
            ],
        )
        .await;

        let expected: Vec<Option<f64>> = (0..5).map(|r| Some((r * 16 + m as i64) as f64)).collect();

        assert_eq!(values_of(&report), expected, "metric {name}");
    }

    // Seventeen must not fit, or the figure is not a limit.
    let seventeenth = client
        .execute_udf(
            &WritePolicy::default(),
            &key,
            &module(),
            "ts_init",
            Some(&[
                as_val!("m16"),
                as_val!(SEC),
                as_val!(3_600),
                as_val!("archives=default"),
                Value::Nil,
                as_val!(T0),
            ]),
        )
        .await;

    match seventeenth {
        Err(e) => println!("  a seventeenth metric is refused, as expected: {e}\n"),
        Ok(_) => {
            let (bins, bytes) = record_bytes(&client, &key).await;

            println!(
                "  a seventeenth metric *did* fit: {bins} bins, {bytes} bytes - the write block is larger than 1 MB \
                 on this cluster\n"
            );
        }
    }

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// The largest single ring there is — 512 KB — rewritten in place a hundred times over.
#[tokio::test]
#[ignore = "rewrites a 512 KB particle many times; run it deliberately"]
async fn the_largest_ring_survives_being_rewritten_in_place() {
    assert!(!cfg!(debug_assertions), "run this with --release");

    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = format!("tsbig_{}", std::process::id());
    let key = as_key!(ns.as_str(), set.as_str(), 1);
    let slots = 65_536i64;
    let n = scale().max(2 * slots);

    let _ = client.delete(&WritePolicy::default(), &key).await;

    // A single ring at the ceiling: 65,536 slots of eight bytes, plus the header.
    let created = must(
        &client,
        &key,
        "ts_init",
        &[as_val!("big"), as_val!(SEC), as_val!(slots), Value::Nil, Value::Nil, as_val!(T0)],
    )
    .await;

    assert_eq!(str_of(&created, "result").as_deref(), Some("created"));
    assert_eq!(i64_of(&created, "ring_bytes"), Some(56 + slots * 8));

    let (secs, accepted) = push(&client, &key, "big", n, batch()).await;

    println!(
        "\n  {n} samples into a {} KB ring: {secs:.2}s, {:.0} samples/s ({} laps)\n",
        (56 + slots * 8) / 1024,
        n as f64 / secs,
        n / slots
    );

    assert_eq!(accepted, n);

    // The whole window, downsampled by the server so the answer is a few hundred numbers rather than 65,536.
    let report = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("big"), Value::Nil, Value::Nil, as_val!(256)],
    )
    .await;

    assert_eq!(str_of(&report, "result").as_deref(), Some("ok"));
    assert_eq!(i64_of(&report, "points"), Some(256));
    assert_eq!(i64_of(&report, "known"), Some(256), "a full ring has no gaps");
    assert_eq!(i64_of(&report, "unknown"), Some(0));
    assert_eq!(i64_of(&report, "bucket_ms"), Some(256 * SEC), "65536 / 256");

    // The newest ten buckets at full resolution, which is the part that has to be exactly right after 15 laps.
    let last_ts = T0 + (n - 1) * SEC;
    let newest = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("big"), as_val!(last_ts - 9 * SEC), as_val!(last_ts)],
    )
    .await;

    assert_eq!(
        values_of(&newest),
        ((n - 10)..n).map(|i| Some(value_at(i))).collect::<Vec<_>>(),
        "the ten newest buckets after {} laps",
        n / slots
    );

    // And the oldest live bucket is exactly `slots` back, with nothing beyond it.
    let oldest = n - slots;
    let edge = must(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("big"),
            as_val!(T0 + (oldest - 1) * SEC),
            as_val!(T0 + (oldest + 1) * SEC),
        ],
    )
    .await;

    assert_eq!(
        values_of(&edge),
        vec![Some(value_at(oldest)), Some(value_at(oldest + 1))],
        "the window is exactly {slots} buckets and starts where it should"
    );

    let stats = must(&client, &key, "ts_stats", &[as_val!("big")]).await;

    assert_eq!(i64_of(&stats, "count"), Some(slots), "every slot holds a sample");
    assert_eq!(
        f64_of(&stats, "sum"),
        Some((n - slots..n).map(value_at).sum::<f64>()),
        "the live window's sum, exactly"
    );

    // --- downsampling, at factors that do not divide evenly ---
    //
    // `factor = ceil(span / max_points)` and `points = ceil(span / factor)`, so the last group is usually short. Each
    // point is checked against the aggregate of its own group, computed here - which is the arithmetic that decides
    // whether a 200-point chart of a 65,536-slot ring is telling the truth.
    let oldest = n - slots;

    println!("  {:>11} {:>8} {:>10} {:>8}", "max_points", "points", "bucket_ms", "factor");

    for max_points in [1i64, 7, 100, 256, 1_000, 4_095, slots - 1, slots, slots + 1] {
        let report = must(
            &client,
            &key,
            "ts_report",
            &[as_val!("big"), Value::Nil, Value::Nil, as_val!(max_points)],
        )
        .await;

        let points = i64_of(&report, "points").expect("points");
        let bucket_ms = i64_of(&report, "bucket_ms").expect("bucket_ms");
        let factor = bucket_ms / SEC;
        let values = values_of(&report);

        println!("  {max_points:>11} {points:>8} {bucket_ms:>10} {factor:>8}");

        assert_eq!(values.len() as i64, points, "the list length must match `points`");
        assert!(points <= max_points.max(1), "max_points={max_points} but {points} came back");
        assert_eq!(factor, (slots + max_points - 1) / max_points, "factor = ceil(span / max_points)");
        assert_eq!(points, (slots + factor - 1) / factor, "points = ceil(span / factor)");

        // Every point is the `last` of its group, since this ring aggregates with `last`. The final group is short
        // whenever the factor does not divide the span, and that is the case most likely to be off by one.
        for (p, got) in values.iter().enumerate() {
            let group_start = p as i64 * factor;
            let group_end = ((p as i64 + 1) * factor - 1).min(slots - 1);
            let want = value_at(oldest + group_end);

            assert_eq!(
                *got,
                Some(want),
                "max_points={max_points}, point {p} covers buckets {group_start}..={group_end}"
            );
        }

        // And the groups have to tile the window exactly: no bucket in two points, none in none.
        assert_eq!(
            (points - 1) * factor,
            (slots - 1) - ((slots - 1) % factor),
            "max_points={max_points}: the last group must contain the newest bucket"
        );
    }

    println!();

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// An archive set far too large for a record is refused **without leaving a half-built series behind**.
#[tokio::test]
#[ignore = "asks the server to write megabytes; run it deliberately"]
async fn an_impossible_archive_set_is_refused_atomically() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = format!("tsimp_{}", std::process::id());
    let key = as_key!(ns.as_str(), set.as_str(), 1);

    let _ = client.delete(&WritePolicy::default(), &key).await;

    // Seven archives at the slot ceiling with 16-byte mean slots: about 7.3 MB, which no write block will take.
    let spec = "archives=s:65536:mean;m:65536:mean;h:65536:mean;d:65536:mean;w:65536:mean;mo:65536:mean;y:65536:mean";
    let result = client
        .execute_udf(
            &WritePolicy::default(),
            &key,
            &module(),
            "ts_init",
            Some(&[as_val!("v"), as_val!(SEC), as_val!(3_600), as_val!(spec), Value::Nil, as_val!(T0)]),
        )
        .await;

    match &result {
        Err(e) => println!("\n  7.3 MB of archives refused: {e}\n"),
        Ok(v) => panic!("a 7.3 MB record should not have been written: {v:?}"),
    }

    // **Nothing may be left behind.** `ts_init` stages every ring and commits once, so a rejected commit has to leave the
    // record as it was — not six rings and a config describing seven.
    let record = client.get(&ReadPolicy::default(), &key, Bins::All).await;

    match record {
        Err(_) => println!("  and the record does not exist at all\n"),
        Ok(r) => assert!(
            r.bins.is_empty(),
            "a refused init left {} bins behind: {:?}",
            r.bins.len(),
            r.bins.keys().collect::<Vec<_>>()
        ),
    }

    // The same series at a workable size then initialises cleanly, so the refusal did not poison the key.
    let created = must(
        &client,
        &key,
        "ts_init",
        &[as_val!("v"), as_val!(SEC), as_val!(3_600), as_val!("archives=default"), Value::Nil, as_val!(T0)],
    )
    .await;

    assert_eq!(str_of(&created, "result").as_deref(), Some("created"));

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}
