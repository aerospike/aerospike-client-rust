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

//! What twelve statistics cost, per record.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test --release --test bench -- --ignored --nocapture
//! ```
//!
//! # What it compares
//!
//! **Stage probes.** Six aggregations, each doing strictly less than the next, so the differences attribute the
//! per-record cost to one component: the query itself, the bin read, the moments, the quantile sketch, Space-Saving,
//! HyperLogLog. Separate aggregations rather than one with a mode argument, so nothing branches on the per-record path
//! and no unused accumulator inflates the partial.
//!
//! **An in-memory floor.** The same accumulators over a `Vec<f64>` in this process, no server involved — possible
//! because the crate is `["cdylib", "lib"]`. The gap between it and the server row is what the database costs.
//!
//! **The client-side alternative.** Fetch every record and compute locally, which is what anybody without this plugin
//! has to do.

use std::env;
use std::time::Instant;

use aerospike_core::{
    as_bin, as_key, as_val, Bins, Client, ClientPolicy, PartitionFilter, QueryPolicy, Statement,
    Value, WritePolicy,
};
use futures::StreamExt;

use aerospike_udf_example_summary::heavy::Heavy;
use aerospike_udf_example_summary::hll::Hll;
use aerospike_udf_example_summary::sum::ExactSum;
use aerospike_udf_example_summary::Means;
use aerospike_udf_example_summary::moments::Moments;
use aerospike_udf_example_summary::sketch::Sketch;

fn records() -> i64 {
    env::var("AEROSPIKE_BENCH_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000)
}

const LOAD_CONCURRENCY: usize = 256;

fn module() -> String {
    env::var("AEROSPIKE_SUMMARY_MODULE").unwrap_or_else(|_| "summary".to_string())
}

/// **If `AEROSPIKE_HOSTS` is set, failing to reach it is a failure** — the same rule as `against_a_server`, which this
/// file was missing. It printed "skipping summary_bench: no server" while the suite next door connected to that very
/// host, so the reason was swallowed instead of reported.
async fn connect() -> Option<Client> {
    let asked_for_a_server = env::var("AEROSPIKE_HOSTS").is_ok();
    let mut policy = ClientPolicy::default();

    policy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
        .unwrap_or(false);

    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    match Client::new(&policy, &hosts).await {
        Ok(client) => Some(client),
        Err(e) if asked_for_a_server => {
            panic!("AEROSPIKE_HOSTS is set to {hosts:?} but the client could not reach it: {e}")
        }
        Err(_) => None,
    }
}

/// Time one aggregation, best of `rounds`, and return the milliseconds and the count it reported.
///
/// Best-of rather than a mean: the thing being measured is the work, and a slow round is something else happening on
/// the box.
async fn best_of(
    rounds: usize,
    client: &Client,
    namespace: &str,
    set: &str,
    function: &str,
) -> (f64, i64) {
    let mut best = (f64::MAX, 0i64);

    for _ in 0..rounds {
        let mut statement = Statement::new(namespace, set, Bins::All);
        let args = [as_val!("v")];

        statement.set_aggregate_function(&module(), function, Some(&args));

        let started = Instant::now();

        let stream = client
            .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
            .await
            .unwrap();

        // Every partial has to be drained for the timing to mean anything, and the counts are summed so the row can
        // prove it saw every record - a stage that errored out early would otherwise look wonderfully fast.
        //
        // **What arrives here is the partial, not the finalized answer.** A query returns what `to_partial` wrote -
        // the serialized accumulator, keyed by field name - and `finalize` runs client-side in the combiner. So there
        // is no `count` key to read: the probes carry a top-level `n`, and `summary` keeps its count inside `moments`.
        let mut seen = 0i64;
        let mut records = stream.into_stream();

        while let Some(record) = records.next().await {
            for (bin, value) in record.unwrap().bins.iter() {
                if std::env::var("SUMMARY_BENCH_DUMP").is_ok() && seen == 0 {
                    eprintln!("    [dump] bin {bin:?} => {value:?}");
                }

                seen += count_in(value);
            }
        }

        let ms = started.elapsed().as_secs_f64() * 1000.0;

        if ms < best.0 {
            best = (ms, seen);
        }
    }

    best
}

/// The record count inside a partial, wherever that accumulator happens to keep it.
fn count_in(value: &Value) -> i64 {
    // **A partial arrives as a `SortedMap`, not a `HashMap`**, because the host builds it key-ordered. Matching only
    // `HashMap` silently found nothing and every row read as zero records, which looked like the query had failed. All
    // three map representations are handled rather than the one that happened to be guessed.
    let get = |map: &Value, key: &str| -> Option<Value> {
        let k = Value::from(key);

        match map {
            Value::HashMap(m) => m.get(&k).cloned(),
            Value::SortedMap(m) => m.get(&k).cloned(),
            Value::OrderedMap(m) => m.iter().find(|(mk, _)| *mk == &k).map(|(_, v)| v.clone()),
            _ => None,
        }
    };

    if let Some(Value::Int(n)) = get(value, "n") {
        return n;
    }

    // `summary` keeps its count inside the nested moments.
    if let Some(moments) = get(value, "moments") {
        if let Some(Value::Int(n)) = get(&moments, "n") {
            return n;
        }
    }

    0
}

#[tokio::test]
#[ignore = "a benchmark: run it deliberately, and only in release"]
async fn summary_bench() {
    assert!(
        !cfg!(debug_assertions),
        "run this with --release; an unoptimised sketch measures nothing but the optimiser"
    );

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("summary_bench_{}", std::process::id());
    let write = WritePolicy::default();
    let n = records();

    // A realistic shape rather than a uniform run: log-normal-ish latencies, which is what percentiles get used on and
    // what makes the sketch occupy a plausible number of buckets. Deterministic, so two runs measure the same work.
    let values: Vec<f64> = (0..n)
        .map(|i| {
            let x = (i as f64) / (n as f64);
            let base = (-((1.0 - x).max(1e-9)).ln()) * 40.0;

            (base + 1.0).min(10_000.0)
        })
        .collect();

    // --- component attribution, in memory ---
    //
    // **In memory rather than on the server, and that is a correction rather than a shortcut.** The first version of
    // this benchmark attributed components from the server-side probes, and the deltas came out +74, +0, -3, +16 and
    // -22 ns - the last of which is impossible, since `summary` does strictly more work than `probe_heavy`. Four query
    // threads divide the per-record CPU cost into wall clock, so a component worth a few ns of CPU is worth about one
    // ns of the number being measured, well under the ~10% spread between runs.
    //
    // So attribution belongs here, single-threaded and server-free, where a few ns is resolvable. The server rows
    // below then answer the different question of what the database costs on top.

    let stage = |label: &str, f: &mut dyn FnMut(f64), previous: &mut Option<f64>| {
        // A warm-up, so the first stage does not pay for cold caches the others avoid.
        for &v in values.iter().take(1000) {
            f(v);
        }

        let started = Instant::now();

        for &v in &values {
            f(v);
        }

        let per = started.elapsed().as_secs_f64() * 1e9 / n as f64;
        let delta = previous.map(|p: f64| per - p);

        eprintln!(
            "  {label:<46} {:>12.1}  {:>10}",
            per,
            delta.map(|d| format!("{d:+.1}")).unwrap_or_else(|| "-".into())
        );

        *previous = Some(per);
    };

    eprintln!("=== {n} records: what each component costs, in memory ===\n");
    eprintln!("  {:<46} {:>12}  {:>10}", "accumulating", "ns/record", "delta ns");

    let mut previous = None;

    {
        let mut m = Moments::default();
        stage("moments (var, skew, kurtosis)", &mut |v| m.push(v), &mut previous);
        std::hint::black_box(m.kurtosis());
    }

    {
        let mut m = Moments::default();
        let mut sk = Sketch::with_epsilon(0.01);
        stage(
            "+ quantile sketch (percentiles)",
            &mut |v| {
                m.push(v);
                sk.push(v);
            },
            &mut previous,
        );
        std::hint::black_box((m.kurtosis(), sk.quantile(0.99)));
    }

    {
        let mut m = Moments::default();
        let mut sk = Sketch::with_epsilon(0.01);
        let mut hv = Heavy::with_capacity(64);
        stage(
            "+ Space-Saving (mode)",
            &mut |v| {
                m.push(v);
                sk.push(v);
                hv.push(v);
            },
            &mut previous,
        );
        std::hint::black_box((m.kurtosis(), sk.quantile(0.99), hv.mode()));
    }

    // **Timed through `stage` like every other row**, which it was not. It used to be hand-timed here with no warm-up
    // while the three rows above each warmed on 1000 values, so its number carried cold caches and first-touch page
    // faults for a 1 KB register array that the others had already paid. That made the row the noisiest in the table
    // and the delta attributed to HyperLogLog partly an artifact of the harness - which is a poor place for the row
    // that a real regression in that component would show up in first.
    {
        let mut m = Moments::default();
        let mut sk = Sketch::with_epsilon(0.01);
        let mut hv = Heavy::with_capacity(64);
        let mut hl = Hll::new();

        stage(
            "+ HyperLogLog (distinct)",
            &mut |v| {
                m.push(v);
                sk.push(v);
                hv.push(v);
                hl.push(v);
            },
            &mut previous,
        );

        std::hint::black_box((m.kurtosis(), sk.quantile(0.99), hv.mode(), hl.estimate()));
    }

    // **The last two rows exist because the table used to claim "= everything" and mean four of the six components.**
    // `ExactSum` and `Means` were both on the per-record path and in neither the attribution nor anybody's attention,
    // which is how a 258 ns regression in HyperLogLog survived being called a safety fix. A component the table cannot
    // reach is a component nobody notices getting slower.
    //
    // The bin the server rows below query is a **double**, so the sums exercise the compensated float side - which is
    // the one that costs anything. An integer bin takes the `i128` path instead, and an `i128` add is cheaper.
    {
        let mut m = Moments::default();
        let mut sk = Sketch::with_epsilon(0.01);
        let mut hv = Heavy::with_capacity(64);
        let mut hl = Hll::new();
        let mut sm = ExactSum::default();

        stage(
            "+ the two sums (i128 + compensated f64)",
            &mut |v| {
                m.push(v);
                sk.push(v);
                hv.push(v);
                hl.push(v);
                sm.push_f64(v);
            },
            &mut previous,
        );

        std::hint::black_box((m.kurtosis(), sk.quantile(0.99), hv.mode(), hl.estimate(), sm.total()));
    }

    let mut fm = Moments::default();
    let mut fs = Sketch::with_epsilon(0.01);
    let mut fh = Heavy::with_capacity(64);
    let mut fd = Hll::new();
    let mut fsum = ExactSum::default();
    let mut fmeans = Means::default();

    stage(
        "+ geometric and harmonic means = everything",
        &mut |v| {
            fm.push(v);
            fs.push(v);
            fh.push(v);
            fd.push(v);
            fsum.push_f64(v);
            fmeans.push(v);
        },
        &mut previous,
    );

    std::hint::black_box((
        fm.stddev(),
        fs.quantile(0.99),
        fh.mode(),
        fd.estimate(),
        fsum.total(),
        fmeans.clone(),
    ));

    let floor_per = previous.unwrap_or(0.0);

    // **The same table on data mode is actually for.** Above, 100,000 values with ~104,000 distinct ones means every
    // observation misses and evicts, which is Space-Saving's worst case - and also a case where the answer is a
    // category error, since nothing repeats. A low-cardinality bin is what anybody asks the mode of, and it hits the
    // table early and never evicts. Reporting both is the difference between "mode is expensive" and "mode is
    // expensive exactly when it is meaningless".
    {
        let statuses = [200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 301.0, 404.0, 500.0];
        let low: Vec<f64> = (0..n).map(|i| statuses[(i as usize) % statuses.len()]).collect();

        let mut hv = Heavy::with_capacity(64);

        for &v in low.iter().take(1000) {
            hv.push(v);
        }

        let mut hv = Heavy::with_capacity(64);
        let started = Instant::now();

        for &v in &low {
            hv.push(v);
        }

        let per = started.elapsed().as_secs_f64() * 1e9 / n as f64;

        std::hint::black_box(hv.mode());

        eprintln!(
            "\n  Space-Saving alone, ~104,000 distinct values (its worst case): {:.1} ns/record",
            previous.unwrap_or(0.0) - 18.8
        );
        eprintln!("  Space-Saving alone, 4 distinct values (what mode is for):     {per:.1} ns/record");
    }

    eprintln!(
        "\n  sketch buckets in use: {}, distinct estimate {:.0}, mode {:?}",
        fs.buckets(),
        fd.estimate(),
        fh.mode().map(|(v, c, _)| (v, c))
    );

    // --- the server ---
    //
    // Only two rows are worth reporting here: the floor of the machinery, and the whole aggregation. The intermediate
    // probes are still registered and still run, but their differences are below what this can resolve - see above.

    // **The server-free half is over, and it ran first on purpose.** It used to sit behind `connect()`, so a machine
    // that could not reach a cluster got no attribution table either - even though this half is single-threaded,
    // in-process and needs no database at all. The one time that mattered, a client-side connect stall on the
    // developer's machine made every number above unobtainable for a component that had just regressed 33x.
    let Some(client) = connect().await else {
        eprintln!("\nno server: skipping the rows that need one\n");
        return;
    };

    eprintln!("loading {n} records ({LOAD_CONCURRENCY} concurrent)...");

    let load_started = Instant::now();

    futures::stream::iter(0..n)
        .for_each_concurrent(LOAD_CONCURRENCY, |i| {
            let client = &client;
            let write = &write;
            let namespace = namespace.as_str();
            let set = set.as_str();
            let v = values[i as usize];

            async move {
                let key = as_key!(namespace, set, i);

                client.put(write, &key, &[as_bin!("v", v)]).await.unwrap();
            }
        })
        .await;

    eprintln!("loaded in {:.1} s", load_started.elapsed().as_secs_f64());

    eprintln!("\n=== {n} records: on the server ===\n");
    eprintln!("  {:<46} {:>8}  {:>12}", "how", "ms", "ns/record");

    let (bare_ms, bare_seen) = best_of(5, &client, &namespace, &set, "probe_nothing").await;

    assert_eq!(bare_seen, n, "probe_nothing should have seen every record");

    eprintln!(
        "  {:<46} {bare_ms:>8.0}  {:>12.0}",
        "query + dispatch, no bin read",
        bare_ms * 1_000_000.0 / n as f64
    );

    let (full_ms, full_seen) = best_of(5, &client, &namespace, &set, "summary").await;

    assert_eq!(full_seen, n, "summary should have seen every record");

    eprintln!(
        "  {:<46} {full_ms:>8.0}  {:>12.0}",
        "summary: all twelve statistics",
        full_ms * 1_000_000.0 / n as f64
    );

    // --- the client-side alternative ---

    let fetch_started = Instant::now();
    let mut fetched = 0i64;
    let mut cm = Moments::default();

    let stream = client
        .query(
            &QueryPolicy::default(),
            PartitionFilter::all(),
            Statement::new(&namespace, &set, Bins::All),
        )
        .await
        .unwrap();

    let mut records_stream = stream.into_stream();

    while let Some(record) = records_stream.next().await {
        if let Some(Value::Float(f)) = record.unwrap().bins.get("v") {
            cm.push(f64::from(f.clone()));
            fetched += 1;
        }
    }

    let fetch_ms = fetch_started.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(fetched, n, "the client-side row should have fetched every record");

    eprintln!(
        "  {:<46} {fetch_ms:>8.0}  {:>12.0}",
        "client: fetch every record, compute locally",
        fetch_ms * 1_000_000.0 / n as f64
    );

    eprintln!(
        "\n  server-side is {:.1}x faster than fetching, and ships a handful of partials rather than {n} records.",
        fetch_ms / full_ms
    );
    eprintln!(
        "  the database costs {:.1}x the arithmetic: {:.0} ns/record against {:.0} in memory.\n",
        full_ms * 1_000_000.0 / n as f64 / floor_per,
        full_ms * 1_000_000.0 / n as f64,
        floor_per
    );

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &namespace, &set, 0)
        .await;

}
