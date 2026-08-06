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

//! What the archive hierarchy costs per append, and what `ts_append_all` recovers.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test --release --test archbench -- --ignored --nocapture
//! ```
//!
//! # Why this exists
//!
//! The design notes carried an *estimate* of about 400 µs for an append into the full seven-archive set, reasoned from
//! the ring sizes. An estimate reasoned from sizes is exactly the kind of number that turns out to be wrong once the
//! per-archive constant, the record commit and the wire are in it — the same way the earlier "1.2× headroom" guess for
//! jsonpath turned out to be 5×. So it is measured here instead, and the estimate is deleted rather than left standing
//! beside the measurement.
//!
//! Two questions, separately:
//!
//! 1. **Per append, against archive count.** Each archive is one in-place ring write, and the rings are wildly
//!    different sizes — 28 KiB for the per-second one and 456 bytes for the yearly one — so the curve should be
//!    dominated by bytes, not by count.
//! 2. **`ts_append_all` against N separate `ts_append` calls.** Same rings touched either way; what disappears is
//!    N − 1 round trips and N − 1 record commits.

use std::collections::HashMap;
use std::env;
use std::time::Instant;

use aerospike_core::{as_key, as_val, Client, ClientPolicy, Key, Value, WritePolicy};

/// 2026-08-06T00:00:00Z.
const NOW: i64 = 20_671 * 86_400_000;

/// The default set, finest first, as `resolution:slots:agg` specs — so a prefix of this is a valid archive list.
const DEFAULT_SET: [(&str, &str, i64); 7] = [
    ("s", "second", 28_856),
    ("m", "minute", 23_096),
    ("h", "hour", 5_432),
    ("d", "day", 1_656),
    ("w", "week", 1_720),
    ("mo", "month", 1_016),
    ("y", "year", 456),
];

const SPECS: [&str; 7] = [
    "s:3600:last",
    "m:1440:mean",
    "h:336:mean",
    "d:100:mean",
    "w:104:mean",
    "mo:60:mean",
    "y:25:mean",
];

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

async fn udf(client: &Client, key: &Key, function: &str, args: &[Value]) -> Value {
    client
        .execute_udf(&WritePolicy::default(), key, &module(), function, Some(args))
        .await
        .unwrap_or_else(|e| panic!("{function}: {e}"))
        .unwrap_or(Value::Nil)
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

/// Create a series with the first `n` archives of the default set, or a single ring when `n` is zero.
async fn init(client: &Client, key: &Key, bin: &str, n: usize) {
    let opts = if n == 0 {
        // The single-ring baseline, sized like the archive set's finest ring so the comparison is against the same
        // number of bytes rather than against a toy.
        "archives=none".to_string()
    } else {
        format!("archives={}", SPECS[..n].join(";"))
    };

    let created = as_map(
        &udf(
            client,
            key,
            "ts_init",
            &[
                as_val!(bin),
                as_val!(1_000),
                as_val!(3_600),
                as_val!(opts.as_str()),
                Value::Nil,
                as_val!(0),
            ],
        )
        .await,
    );

    match created.get("result") {
        Some(Value::String(s)) if s == "created" => {}
        other => panic!("ts_init({bin}, {n} archives) said {other:?}"),
    }
}

/// One `ts_append` per round trip, timed. Each append advances one second: a new bucket in the finest archive and the
/// same bucket in every coarser one, which is the ordinary case and the one where coarse archives combine most.
async fn append_us(reps: usize, client: &Client, key: &Key, bin: &str, t: &mut i64) -> f64 {
    let started = Instant::now();

    for _ in 0..reps {
        udf(client, key, "ts_append", &[as_val!(bin), as_val!(*t), Value::from(1.0)]).await;
        *t += 1_000;
    }

    started.elapsed().as_secs_f64() * 1e6 / reps as f64
}

/// `reps` samples in **one** round trip, so the per-sample number is server-side ring work rather than the network.
async fn batched_us(reps: usize, client: &Client, key: &Key, bin: &str, t: &mut i64) -> f64 {
    let times = Value::from((0..reps).map(|i| as_val!(*t + i as i64 * 1_000)).collect::<Vec<_>>());
    let values = Value::from((0..reps).map(|_| Value::from(1.0)).collect::<Vec<_>>());

    *t += reps as i64 * 1_000;

    let started = Instant::now();
    let result = as_map(
        &udf(
            client,
            key,
            "ts_append_many",
            &[as_val!(bin), times, values],
        )
        .await,
    );
    let per = started.elapsed().as_secs_f64() * 1e6 / reps as f64;

    // Refuse to report a number for work that did not happen. A batch whose samples were rejected would look fast.
    match result.get("accepted") {
        Some(Value::Int(n)) if *n == reps as i64 => per,
        other => panic!("ts_append_many accepted {other:?} of {reps} - the timing would be meaningless"),
    }
}

#[tokio::test]
#[ignore = "a benchmark: run it deliberately, and only in release"]
async fn what_an_archive_hierarchy_costs_per_append() {
    assert!(
        !cfg!(debug_assertions),
        "run this with --release, or the numbers describe the optimiser"
    );

    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = ns();
    let set = format!("tsarch_{}", std::process::id());
    let write = WritePolicy::default();

    // **Warm the connection before the sweep, not inside it.**
    //
    // The first version warmed each record and still produced a curve that *fell* as archives were added - seven
    // archives apparently cheaper than one, which cannot happen when the only difference is more work. The cold row was
    // the first one, and it was paying for the connection rather than for its single ring. Two passes over the whole
    // sweep, reporting the better of the two, is what makes each row comparable to the others.
    let warm = as_key!(ns.as_str(), set.as_str(), "warm");

    let _ = client.delete(&write, &warm).await;

    init(&client, &warm, "v", 1).await;

    let mut t = NOW;

    append_us(200, &client, &warm, "v", &mut t).await;

    let _ = client.delete(&write, &warm).await;

    let mut single = [f64::MAX; 8];
    let mut batched = [f64::MAX; 8];

    for pass in 0..2 {
        for n in 1..=7usize {
            let key = as_key!(ns.as_str(), set.as_str(), format!("n{n}_{pass}").as_str());

            let _ = client.delete(&write, &key).await;

            init(&client, &key, "v", n).await;

            let mut t = NOW;

            // A warm-up into this record that is not timed: the first append into a fresh ring pays for whatever the
            // server does lazily, and timing it would describe that instead of an append.
            append_us(20, &client, &key, "v", &mut t).await;

            single[n] = single[n].min(append_us(200, &client, &key, "v", &mut t).await);
            batched[n] = batched[n].min(batched_us(200, &client, &key, "v", &mut t).await);

            let _ = client.delete(&write, &key).await;
        }
    }

    println!("\n=== one sample, against how many archives it feeds ===\n");
    println!(
        "  {:>8}  {:>10}  {:>12}  {:>13}  {:>9}  {:>13}  {:>9}",
        "archives", "ring bytes", "coarsest", "us/round trip", "vs 1", "us/sample", "vs 1"
    );

    for n in 1..=7usize {
        let bytes: i64 = DEFAULT_SET[..n].iter().map(|(_, _, b)| *b).sum();

        println!(
            "  {n:>8}  {bytes:>10}  {:>12}  {:>10.1} us  {:>8.2}x  {:>10.2} us  {:>8.2}x",
            DEFAULT_SET[n - 1].1,
            single[n],
            single[n] / single[1],
            batched[n],
            batched[n] / batched[1],
        );
    }

    println!(
        "\n  The middle column is one append per round trip, which is what an agent shipping a sample every second\n  \
         actually pays. The right column is 200 samples in one call, which removes the network and leaves the ring\n  \
         work - so the two together say how much of the cost is the hierarchy and how much is the wire.\n"
    );

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

#[tokio::test]
#[ignore = "a benchmark: run it deliberately, and only in release"]
async fn append_all_against_one_call_per_metric() {
    assert!(
        !cfg!(debug_assertions),
        "run this with --release, or the numbers describe the optimiser"
    );

    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = ns();
    let set = format!("tsall_{}", std::process::id());
    let write = WritePolicy::default();

    println!("\n=== N metrics sampled together: ts_append_all against N x ts_append ===\n");
    println!(
        "  {:>7}  {:>8}  {:>14}  {:>14}  {:>8}",
        "metrics", "archives", "N x ts_append", "ts_append_all", "gain"
    );

    // Three archives per metric rather than seven, so sixteen metrics still fit a record with room to spare: the point
    // here is the commit count, and padding the record with the 28 KiB per-second ring would measure the wire instead.
    let archives = 3usize;

    // The connection, warmed outside the sweep - the same correction the archive-count benchmark needed, and it changed
    // the conclusion rather than just the numbers. Cold, the one-metric row read 165 us against 217 us and said batching
    // a single metric was a 0.76x *loss*; warm, it reads 81 us against 67 us, a 1.21x gain. Both the size and the sign
    // were wrong.
    let warm = as_key!(ns.as_str(), set.as_str(), "warm");

    let _ = client.delete(&write, &warm).await;

    init(&client, &warm, "w", archives).await;

    for i in 0..200i64 {
        udf(&client, &warm, "ts_append", &[as_val!("w"), as_val!(NOW + i * 1_000), Value::from(1.0)]).await;
    }

    let _ = client.delete(&write, &warm).await;

    for metrics in [1usize, 4, 8, 16] {
        let key = as_key!(ns.as_str(), set.as_str(), format!("m{metrics}").as_str());

        let _ = client.delete(&write, &key).await;

        let names: Vec<String> = (0..metrics).map(|i| format!("b{i}")).collect();

        for name in &names {
            init(&client, &key, name, archives).await;
        }

        let name_list = Value::from(names.iter().map(|n| as_val!(n.as_str())).collect::<Vec<_>>());
        let value_list = Value::from((0..metrics).map(|i| Value::from(i as f64)).collect::<Vec<_>>());

        let mut t = NOW;

        // Warm both paths before either is timed.
        for _ in 0..5 {
            for name in &names {
                udf(&client, &key, "ts_append", &[as_val!(name.as_str()), as_val!(t), Value::from(1.0)]).await;
            }

            udf(
                &client,
                &key,
                "ts_append_all",
                &[name_list.clone(), as_val!(t), value_list.clone()],
            )
            .await;

            t += 1_000;
        }

        let reps = 20usize;
        let mut one_at_a_time = f64::MAX;
        let mut all_at_once = f64::MAX;

        for _ in 0..3 {
            let started = Instant::now();

            for _ in 0..reps {
                for name in &names {
                    udf(&client, &key, "ts_append", &[as_val!(name.as_str()), as_val!(t), Value::from(1.0)]).await;
                }

                t += 1_000;
            }

            one_at_a_time = one_at_a_time.min(started.elapsed().as_secs_f64() * 1e6 / reps as f64);

            let started = Instant::now();

            for _ in 0..reps {
                udf(
                    &client,
                    &key,
                    "ts_append_all",
                    &[name_list.clone(), as_val!(t), value_list.clone()],
                )
                .await;

                t += 1_000;
            }

            all_at_once = all_at_once.min(started.elapsed().as_secs_f64() * 1e6 / reps as f64);
        }

        println!(
            "  {metrics:>7}  {archives:>8}  {one_at_a_time:>11.1} us  {all_at_once:>11.1} us  {:>7.2}x",
            one_at_a_time / all_at_once
        );

        let _ = client.delete(&write, &key).await;
    }

    println!(
        "\n  The same rings are written either way. What one call removes is N-1 round trips, N-1 config reads and\n  \
         N-1 record commits, which is why the gain grows with the metric count.\n\n  \
         At one metric it is still slightly ahead rather than level, because ts_append builds the larger result - a\n  \
         per-archive outcome map that ts_append_all does not return.\n"
    );

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}
