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

//! What a ring costs against the alternative anyone actually has: a CDT list.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test --release --test bench -- --ignored --nocapture
//! ```
//!
//! # What it compares
//!
//! **Appending.** `ts_append` against `operate()` doing a list append plus a trim to bound the list — which is what a
//! round-robin series in a CDT has to do. Both rewrite the bin's particle; only one of them re-packs msgpack.
//!
//! **Batching.** `ts_append_many` against the same number of single appends, and against one `append_items`. The ring
//! is copied out of its particle and back on every write, so this is where that cost is either paid N times or once.
//!
//! **Reading.** `ts_report` with `max_points` against fetching the whole series and downsampling on the client. This is
//! the comparison the design exists for: one ships the answer, the other ships the data.
//!
//! # No claim is made that is not measured here
//!
//! Notably absent: any statement that appending to a blob is O(1). It is not — see `ring`. The numbers below are what
//! the two representations actually cost against each other on one machine, and the ring-size sweep is there precisely
//! to show the part that grows.

use std::env;
use std::time::Instant;

use aerospike_core::operations::lists;
use futures::StreamExt;
use aerospike_core::{
    as_bin, as_key, as_val, Bins, Client, ClientPolicy, Key, ListPolicy, ReadPolicy, Value,
    WritePolicy,
};

const STEP: i64 = 60_000;

fn module() -> String {
    env::var("AEROSPIKE_TIMESERIES_MODULE").unwrap_or_else(|_| "timeseries".to_string())
}

fn namespace() -> String {
    env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

fn samples() -> i64 {
    env::var("AEROSPIKE_BENCH_SAMPLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000)
}

/// **If `AEROSPIKE_HOSTS` is set, failing to reach it is a failure.**
async fn connect() -> Option<Client> {
    let asked = env::var("AEROSPIKE_HOSTS").is_ok();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    match Client::new(&ClientPolicy::default(), &hosts).await {
        Ok(client) => Some(client),
        Err(e) if asked => {
            panic!("AEROSPIKE_HOSTS is set to {hosts:?} but the client could not reach it: {e}")
        }
        Err(_) => None,
    }
}

async fn init(client: &Client, key: &Key, slots: i64) {
    let _ = client.delete(&WritePolicy::default(), key).await;

    client
        .execute_udf(
            &WritePolicy::default(),
            key,
            &module(),
            "ts_init",
            Some(&[
                as_val!("v"),
                as_val!(STEP),
                as_val!(slots),
                Value::Nil,
                Value::Nil,
                as_val!(0),
            ]),
        )
        .await
        .expect("ts_init");
}

fn row(label: &str, total_ms: f64, n: i64) {
    eprintln!("  {label:<44} {total_ms:>9.1} ms {:>12.1} us/op", total_ms * 1000.0 / n as f64);
}

#[tokio::test]
#[ignore = "a benchmark: run it deliberately, and only in release"]
async fn timeseries_bench() {
    assert!(
        !cfg!(debug_assertions),
        "run this with --release, or the numbers describe the optimiser rather than the work"
    );

    let Some(client) = connect().await else {
        eprintln!("skipping timeseries_bench: no server");
        return;
    };

    let ns = namespace();
    let set = format!("ts_bench_{}", std::process::id());
    let n = samples();
    let slots = 4_096i64;
    let write = WritePolicy::default();
    let list_policy = ListPolicy::default();

    eprintln!("\n=== appending {n} samples, one at a time ===\n");
    eprintln!("  {:<44} {:>9} {:>15}", "how", "total", "per sample");

    // --- the ring ---

    let ring_key = as_key!(ns.as_str(), set.as_str(), "ring");

    init(&client, &ring_key, slots).await;

    let started = Instant::now();

    for i in 0..n {
        client
            .execute_udf(
                &write,
                &ring_key,
                &module(),
                "ts_append",
                Some(&[as_val!("v"), as_val!(i * STEP), Value::from(i as f64)]),
            )
            .await
            .expect("ts_append");
    }

    let ring_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("ts_append (blob ring, 4096 slots)", ring_ms, n);

    // --- the CDT list a round-robin series needs ---
    //
    // Append plus a trim, because an unbounded list is not the same thing: a ring holds the last `slots` samples and a
    // list has to be told to. Both operations are in one `operate()`, so this is one round trip, like the UDF.

    let list_key = as_key!(ns.as_str(), set.as_str(), "list");

    let _ = client.delete(&write, &list_key).await;

    let started = Instant::now();

    for i in 0..n {
        client
            .operate(
                &write,
                &list_key,
                &[
                    lists::append(&list_policy, "v", Value::from(i as f64)),
                    lists::trim("v", -slots, slots),
                ],
            )
            .await
            .expect("list append + trim");
    }

    let list_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("operate: list append + trim", list_ms, n);

    eprintln!(
        "\n  the ring is {:.2}x the CDT list per sample.\n",
        ring_ms / list_ms
    );

    // --- batching ---

    eprintln!("=== appending {n} samples in one call ===\n");
    eprintln!("  {:<44} {:>9} {:>15}", "how", "total", "per sample");

    let batch_key = as_key!(ns.as_str(), set.as_str(), "batch");

    init(&client, &batch_key, slots).await;

    let times: Vec<Value> = (0..n).map(|i| as_val!(i * STEP)).collect();
    let values: Vec<Value> = (0..n).map(|i| Value::from(i as f64)).collect();

    let started = Instant::now();

    client
        .execute_udf(
            &write,
            &batch_key,
            &module(),
            "ts_append_many",
            Some(&[as_val!("v"), Value::from(times), Value::from(values.clone())]),
        )
        .await
        .expect("ts_append_many");

    let batch_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("ts_append_many (one call, one ring copy)", batch_ms, n);

    let items_key = as_key!(ns.as_str(), set.as_str(), "items");

    let _ = client.delete(&write, &items_key).await;

    let started = Instant::now();

    client
        .operate(
            &write,
            &items_key,
            &[
                lists::append_items(&list_policy, "v", values.clone()),
                lists::trim("v", -slots, slots),
            ],
        )
        .await
        .expect("append_items");

    let items_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("operate: list append_items + trim", items_ms, n);

    eprintln!(
        "\n  batching the ring is {:.0}x faster than one call per sample, and {:.2}x the CDT bulk form.\n",
        ring_ms / batch_ms.max(f64::MIN_POSITIVE),
        batch_ms / items_ms.max(f64::MIN_POSITIVE)
    );

    // --- reading ---

    eprintln!("=== reading a {slots}-slot series ===\n");
    eprintln!("  {:<44} {:>9} {:>15}", "how", "total", "per read");

    let reads = 50i64;

    let started = Instant::now();

    for _ in 0..reads {
        client
            .execute_udf(
                &write,
                &batch_key,
                &module(),
                "ts_report",
                Some(&[as_val!("v"), Value::Nil, Value::Nil, as_val!(200)]),
            )
            .await
            .expect("ts_report downsampled");
    }

    let down_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("ts_report, downsampled to 200 points", down_ms, reads);

    let started = Instant::now();

    for _ in 0..reads {
        client
            .execute_udf(
                &write,
                &batch_key,
                &module(),
                "ts_report",
                Some(&[as_val!("v")]),
            )
            .await
            .expect("ts_report full");
    }

    let full_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("ts_report, every slot", full_ms, reads);

    let started = Instant::now();

    for _ in 0..reads {
        client
            .execute_udf(
                &write,
                &batch_key,
                &module(),
                "ts_stats",
                Some(&[as_val!("v")]),
            )
            .await
            .expect("ts_stats");
    }

    let stats_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("ts_stats, no list at all", stats_ms, reads);

    // The client-side alternative: fetch the blob and do the work here.
    let started = Instant::now();
    let mut downsampled_here = 0usize;

    for _ in 0..reads {
        let record = client
            .get(&ReadPolicy::default(), &batch_key, Bins::from(["v"]))
            .await
            .expect("get the ring");

        if let Some(Value::Blob(bytes)) = record.bins.get("v") {
            // Same arithmetic the plugin does, on the client: 4096 slots into 200 points.
            let per = (slots as usize + 199) / 200;
            let mut points = Vec::with_capacity(200);

            for group in bytes[40..].chunks(8 * per) {
                let mut best = f64::NEG_INFINITY;

                for slot in group.chunks_exact(8) {
                    let mut buf = [0u8; 8];

                    buf.copy_from_slice(slot);

                    let v = f64::from_le_bytes(buf);

                    if !v.is_nan() && v > best {
                        best = v;
                    }
                }

                points.push(best);
            }

            downsampled_here = points.len();
        }
    }

    let client_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("client: fetch the blob, downsample here", client_ms, reads);

    // And the CDT equivalent, which has to unpack the whole list to get anything.
    let started = Instant::now();

    for _ in 0..reads {
        let record = client
            .get(&ReadPolicy::default(), &items_key, Bins::from(["v"]))
            .await
            .expect("get the list");

        if let Some(Value::List(items)) = record.bins.get("v") {
            std::hint::black_box(items.len());
        }
    }

    let cdt_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("client: fetch the CDT list", cdt_ms, reads);

    eprintln!(
        "\n  downsampling on the server is {:.2}x fetching the blob and {:.2}x fetching the list.",
        client_ms / down_ms.max(f64::MIN_POSITIVE),
        cdt_ms / down_ms.max(f64::MIN_POSITIVE)
    );
    eprintln!("  ({downsampled_here} points computed on the client, for comparison)\n");

    // --- reading, at a size where the wire actually costs something ---
    //
    // **The 4,096-slot comparison above does not test the claim.** 32 KB crosses a LAN for less than the UDF dispatch
    // costs, so shipping the whole ring wins and downsampling on the server cannot pay for itself. If server-side
    // downsampling is ever worth it, it is when the series is large enough that transferring it dominates - so that is
    // measured too, rather than concluded.

    let big = 65_536i64;
    let big_key = as_key!(ns.as_str(), set.as_str(), "big");

    init(&client, &big_key, big).await;

    let big_list_key = as_key!(ns.as_str(), set.as_str(), "biglist");

    let _ = client.delete(&write, &big_list_key).await;

    // Filled in batches, because one call carrying 65,536 pairs is a megabyte of arguments.
    for chunk in 0..(big / 8_192) {
        let base = chunk * 8_192;
        let times: Vec<Value> = (0..8_192).map(|i| as_val!((base + i) * STEP)).collect();
        let values: Vec<Value> = (0..8_192).map(|i| Value::from((base + i) as f64)).collect();

        client
            .execute_udf(
                &write,
                &big_key,
                &module(),
                "ts_append_many",
                Some(&[as_val!("v"), Value::from(times), Value::from(values.clone())]),
            )
            .await
            .expect("ts_append_many");

        client
            .operate(
                &write,
                &big_list_key,
                &[lists::append_items(&list_policy, "v", values)],
            )
            .await
            .expect("append_items");
    }

    eprintln!("=== reading a {big}-slot series ({} KB of samples) ===\n", big * 8 / 1024);
    eprintln!("  {:<44} {:>9} {:>15}", "how", "total", "per read");

    let reads = 20i64;
    let started = Instant::now();

    for _ in 0..reads {
        client
            .execute_udf(
                &write,
                &big_key,
                &module(),
                "ts_report",
                Some(&[as_val!("v"), Value::Nil, Value::Nil, as_val!(200)]),
            )
            .await
            .expect("ts_report downsampled");
    }

    let big_down_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("ts_report, downsampled to 200 points", big_down_ms, reads);

    let started = Instant::now();

    for _ in 0..reads {
        client
            .execute_udf(&write, &big_key, &module(), "ts_stats", Some(&[as_val!("v")]))
            .await
            .expect("ts_stats");
    }

    let big_stats_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("ts_stats, no list at all", big_stats_ms, reads);

    let started = Instant::now();

    for _ in 0..reads {
        let record = client
            .get(&ReadPolicy::default(), &big_key, Bins::from(["v"]))
            .await
            .expect("get the ring");

        if let Some(Value::Blob(bytes)) = record.bins.get("v") {
            let per = (big as usize + 199) / 200;
            let mut points = 0usize;

            for group in bytes[40..].chunks(8 * per) {
                let mut best = f64::NEG_INFINITY;

                for slot in group.chunks_exact(8) {
                    let mut buf = [0u8; 8];

                    buf.copy_from_slice(slot);

                    let v = f64::from_le_bytes(buf);

                    if !v.is_nan() && v > best {
                        best = v;
                    }
                }

                points += 1;
                std::hint::black_box(best);
            }

            std::hint::black_box(points);
        }
    }

    let big_client_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("client: fetch the blob, downsample here", big_client_ms, reads);

    let started = Instant::now();

    for _ in 0..reads {
        let record = client
            .get(&ReadPolicy::default(), &big_list_key, Bins::from(["v"]))
            .await
            .expect("get the list");

        if let Some(Value::List(items)) = record.bins.get("v") {
            std::hint::black_box(items.len());
        }
    }

    let big_cdt_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("client: fetch the CDT list", big_cdt_ms, reads);

    eprintln!(
        "\n  downsampling on the server is {:.2}x fetching the blob and {:.2}x fetching the list.\n",
        big_client_ms / big_down_ms.max(f64::MIN_POSITIVE),
        big_cdt_ms / big_down_ms.max(f64::MIN_POSITIVE)
    );

    let _ = client.delete(&write, &big_key).await;
    let _ = client.delete(&write, &big_list_key).await;

    // --- the rollup: the one place the alternative is O(records) on the wire ---
    //
    // Everything above is one record, and for one record the client can always just fetch the bytes. A fleet is
    // different: rolling up N series client-side means transferring all N rings. This is the comparison that decides
    // whether the aggregation earns its place, so it is measured rather than assumed.

    let fleet = 200i64;
    let fleet_slots = 4_096i64;
    let fleet_set = format!("{set}_fleet");

    for d in 0..fleet {
        let key = as_key!(ns.as_str(), fleet_set.as_str(), d);

        init(&client, &key, fleet_slots).await;

        let times: Vec<Value> = (0..fleet_slots).map(|i| as_val!(i * STEP)).collect();
        let values: Vec<Value> = (0..fleet_slots).map(|i| Value::from(i as f64)).collect();

        client
            .execute_udf(
                &write,
                &key,
                &module(),
                "ts_append_many",
                Some(&[as_val!("v"), Value::from(times), Value::from(values)]),
            )
            .await
            .expect("fill the fleet");
    }

    eprintln!(
        "=== rolling up {fleet} series of {fleet_slots} slots ({} MB of rings) ===\n",
        fleet * fleet_slots * 8 / 1_048_576
    );
    eprintln!("  {:<44} {:>9} {:>15}", "how", "total", "per rollup");

    let rounds = 5i64;
    let args = [as_val!("v")];
    let started = Instant::now();

    for _ in 0..rounds {
        let mut statement = aerospike_core::Statement::new(&ns, &fleet_set, Bins::All);

        statement.set_aggregate_function(&module(), "ts_rollup", Some(&args));

        let records = client
            .query(
                &aerospike_core::QueryPolicy::default(),
                aerospike_core::PartitionFilter::all(),
                statement,
            )
            .await
            .expect("query");

        let mut combiner =
            aerospike_udf_sdk_native::client::Combiner::<aerospike_udf_example_timeseries::Rollup>::new(&args)
                .expect("combiner");
        let mut stream = records.into_stream();

        while let Some(record) = stream.next().await {
            for value in record.unwrap().bins.values() {
                combiner.absorb(value).expect("absorb");
            }
        }

        std::hint::black_box(combiner.finish().expect("finalize"));
    }

    let rollup_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("ts_rollup, server-side", rollup_ms, rounds);

    // The client-side equivalent: scan every record, pull every ring, combine here.
    let started = Instant::now();

    for _ in 0..rounds {
        let statement = aerospike_core::Statement::new(&ns, &fleet_set, Bins::from(["v"]));

        let records = client
            .query(
                &aerospike_core::QueryPolicy::default(),
                aerospike_core::PartitionFilter::all(),
                statement,
            )
            .await
            .expect("query");

        let mut totals = vec![0.0f64; fleet_slots as usize];
        let mut stream = records.into_stream();

        while let Some(record) = stream.next().await {
            for value in record.unwrap().bins.values() {
                if let Value::Blob(bytes) = value {
                    for (i, slot) in bytes[40..].chunks_exact(8).enumerate() {
                        let mut buf = [0u8; 8];

                        buf.copy_from_slice(slot);

                        let v = f64::from_le_bytes(buf);

                        if !v.is_nan() && i < totals.len() {
                            totals[i] += v;
                        }
                    }
                }
            }
        }

        std::hint::black_box(totals.len());
    }

    let fetch_all_ms = started.elapsed().as_secs_f64() * 1000.0;

    row("client: scan every ring, combine here", fetch_all_ms, rounds);

    eprintln!(
        "\n  the rollup is {:.2}x fetching every ring, and ships one map instead of {} MB.\n",
        fetch_all_ms / rollup_ms.max(f64::MIN_POSITIVE),
        fleet * fleet_slots * 8 / 1_048_576
    );

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &fleet_set, 0)
        .await;

    // --- the cost that grows with the ring ---
    //
    // The claim under test: an append moves O(slots) bytes even though it touches one slot. If that is true, the
    // per-append cost rises with the ring size; if it is not, this table is flat and the note in `ring` is wrong.

    eprintln!("=== one append against ring size ===\n");
    eprintln!("  {:>8}  {:>10}  {:>12}", "slots", "ring bytes", "us/append");

    for size in [64i64, 1_024, 8_192, 65_536] {
        let key = as_key!(ns.as_str(), set.as_str(), format!("sz{size}").as_str());

        init(&client, &key, size).await;

        let rounds = 200i64;
        let started = Instant::now();

        for i in 0..rounds {
            client
                .execute_udf(
                    &write,
                    &key,
                    &module(),
                    "ts_append",
                    Some(&[as_val!("v"), as_val!(i * STEP), Value::from(i as f64)]),
                )
                .await
                .expect("ts_append");
        }

        let per = started.elapsed().as_secs_f64() * 1e6 / rounds as f64;

        eprintln!("  {size:>8}  {:>10}  {per:>12.1}", 40 + size * 8);

        let _ = client.delete(&write, &key).await;
    }

    eprintln!();

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}
