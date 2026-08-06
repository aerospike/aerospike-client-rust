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

//! What a JSONPath engine costs, per record, over 200,000 of them.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… AEROSPIKE_USE_SERVICES_ALTERNATE=true cargo test --test bench -- --ignored --nocapture
//! ```
//!
//! # What it compares, and why that is the right comparison
//!
//! Three filters over **the same records**, selecting **the same ones**:
//!
//! | filter | how |
//! |---|---|
//! | `stats_rs`'s `over` | two bin reads and an integer compare, hand-written |
//! | `jsonpath_filter` `$.orders[?@.total >= …]` | expand a list bin, expand a map, read a key, compare |
//! | `jsonpath_filter` `$.v[?…]`-free path | a single bin read through the adapter, no collection touched |
//!
//! The first is the floor: it is what the work costs with no query language at all. The second is the honest
//! cost of the feature on nested data. The third isolates **the adapter** from **the collection walk**, which is
//! the number that says whether laziness is doing anything.
//!
//! Both modules have to be configured, which is the only reason this is a separate file from the correctness
//! tests: it reports a skip rather than a failure when either is missing.

use std::env;
use std::time::Instant;

use aerospike_core::{
    as_bin, as_key, as_list, as_map, as_val, Bins, Client, ClientPolicy, PartitionFilter,
    QueryPolicy, Statement, Value, WritePolicy,
};

use futures::StreamExt;

const RECORDS: i64 = 200_000;
const LOAD_CONCURRENCY: usize = 256;

/// Ten percent selected, as in the aggregation bench — enough response work to be realistic, little enough that
/// the per-record call is still visible.
const FLOOR: i64 = RECORDS - RECORDS / 10 + 1;

fn jsonpath_module() -> String {
    env::var("AEROSPIKE_JSONPATH_MODULE").unwrap_or_else(|_| "jsonpath".to_string())
}

fn native_module() -> String {
    env::var("AEROSPIKE_NATIVE_MODULE").unwrap_or_else(|_| "stats_rs".to_string())
}

async fn connect() -> Option<Client> {
    let mut policy = ClientPolicy::default();

    policy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
        .unwrap_or(false);

    Client::new(
        &policy,
        &env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string()),
    )
    .await
    .ok()
}

/// Run a filtering aggregation and count the records it returned.
async fn time_filter(
    client: &Client,
    namespace: &str,
    set: &str,
    module: &str,
    function: &str,
    args: Option<&[Value]>,
) -> (f64, usize) {
    let mut statement = Statement::new(namespace, set, Bins::All);

    statement.set_aggregate_function(module, function, args);

    let started = Instant::now();

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut count = 0usize;
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        // A record carries the bins it was written with; a partial would be one unnamed value.
        if record.unwrap().bins.contains_key("v") {
            count += 1;
        }
    }

    (started.elapsed().as_secs_f64() * 1000.0, count)
}

async fn best_of(
    rounds: usize,
    client: &Client,
    namespace: &str,
    set: &str,
    module: &str,
    function: &str,
    args: Option<&[Value]>,
) -> (f64, usize) {
    let mut best = (f64::MAX, 0);

    for _ in 0..rounds {
        let r = time_filter(client, namespace, set, module, function, args).await;

        if r.0 < best.0 {
            best = r;
        }
    }

    best
}

/// One table, fastest row as the baseline.
fn report(rows: &[(String, f64)]) {
    let fastest = rows.iter().map(|(_, ms)| *ms).fold(f64::MAX, f64::min);

    eprintln!(
        "  {:<48} {:>8}  {:>13}  {:>8}",
        "filter", "ms", "records/sec", "vs best"
    );

    for (name, ms) in rows {
        eprintln!(
            "  {:<48} {:>8.0}  {:>13.0}  {:>7.2}x",
            name,
            ms,
            RECORDS as f64 / (ms / 1000.0),
            ms / fastest
        );
    }
}

#[tokio::test]
#[ignore]
async fn jsonpath_filter_bench() {
    let Some(client) = connect().await else {
        eprintln!("skipping jsonpath_filter_bench: no server");
        return;
    };

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("jp_bench_{}", std::process::id());
    let write = WritePolicy::default();

    let probe = as_key!(namespace.as_str(), set.as_str(), 1i64);

    client
        .put(&write, &probe, &[as_bin!("v", 1i64)])
        .await
        .unwrap();

    let jp = jsonpath_module();
    let native = native_module();

    if client
        .execute_udf(&write, &probe, &jp, "jsonpath_exists", Some(&[as_val!("$.v")]))
        .await
        .is_err()
    {
        eprintln!("skipping jsonpath_filter_bench: no usable module '{jp}'");
        return;
    }

    let have_native = client
        .execute_udf(&write, &probe, &native, "echo", Some(&[as_val!(1)]))
        .await
        .is_ok();

    // --- load ---

    eprintln!("loading {RECORDS} records ({LOAD_CONCURRENCY} concurrent)...");

    let started = Instant::now();

    // `v` and `floor` for the hand-written filter; `orders`, a list of one map, for the JSONPath one. Both
    // describe the same threshold, so the two filters select exactly the same records — which is what makes the
    // times comparable.
    futures::stream::iter(1..=RECORDS)
        .for_each_concurrent(LOAD_CONCURRENCY, |i| {
            let client = &client;
            let write = &write;
            let namespace = namespace.as_str();
            let set = set.as_str();

            async move {
                let key = as_key!(namespace, set, i);

                client
                    .put(
                        write,
                        &key,
                        &[
                            as_bin!("v", i),
                            as_bin!("floor", FLOOR),
                            as_bin!("orders", as_list!(as_map!("total" => i))),
                        ],
                    )
                    .await
                    .unwrap();
            }
        })
        .await;

    eprintln!(
        "loaded in {:.1} s\n",
        started.elapsed().as_secs_f64()
    );

    let expected = (RECORDS - FLOOR + 1) as usize;

    eprintln!("=== filtering {RECORDS} records down to {expected} ===\n");

    let mut rows: Vec<(String, f64)> = Vec::new();

    // The floor: no query language, two bin reads and a compare.
    if have_native {
        let (ms, n) = best_of(3, &client, &namespace, &set, &native, "over", None).await;

        assert_eq!(n, expected, "the hand-written filter selected the wrong records");
        rows.push(("hand-written: two bin reads, a compare".into(), ms));
    }

    // The same records, selected by walking a list bin, the map inside it, and a key.
    let path = format!("$.orders[?@.total >= {FLOOR}]");
    let (ms, n) = best_of(
        3,
        &client,
        &namespace,
        &set,
        &jp,
        "jsonpath_filter",
        Some(&[as_val!(path.as_str())]),
    )
    .await;

    assert_eq!(n, expected, "the JSONPath filter selected the wrong records");
    rows.push(("jsonpath: $.orders[?@.total >= N]".into(), ms));

    report(&rows);

    //------------------------------------------------
    // The same work with **nothing selected**, which is the only way to see the engine.
    //
    // Above, both filters ship 20,000 records back, and that response cost is large enough to hide whatever the
    // engine costs. Selecting nothing removes it: what is left is the per-record work and only that.
    //

    eprintln!("\n=== the same paths, selecting nothing - no response cost to hide behind ===\n");

    let mut engine: Vec<(String, f64)> = Vec::new();

    // Root expanded, one bin looked up, no match. The adapter and nothing else.
    let (ms, n) = best_of(
        3,
        &client,
        &namespace,
        &set,
        &jp,
        "jsonpath_filter",
        Some(&[as_val!("$.nosuchbin")]),
    )
    .await;

    assert_eq!(n, 0, "$.nosuchbin must match nothing");
    engine.push(("jsonpath: $.nosuchbin (root expansion only)".into(), ms));

    // Root expanded, plus the list bin and the map inside it, plus a compare that never holds.
    let miss = format!("$.orders[?@.total > {}]", RECORDS * 2);
    let (ms, n) = best_of(
        3,
        &client,
        &namespace,
        &set,
        &jp,
        "jsonpath_filter",
        Some(&[as_val!(miss.as_str())]),
    )
    .await;

    assert_eq!(n, 0, "an impossible threshold must match nothing");
    engine.push(("jsonpath: $.orders[?@.total > 2N] (walks the list)".into(), ms));

    report(&engine);

    eprintln!(
        "\n  the gap between those two rows is what expanding a list bin and the map inside it costs,\n  \
         per record, with the response path taken out of the picture."
    );

    // Truncated for the same reason the aggregation bench truncates: a set-scoped query with no set index walks
    // the namespace's whole primary index, so records left behind make the next run slower and every ratio
    // smaller.
    if let Err(e) = client
        .truncate(&aerospike_core::AdminPolicy::default(), &namespace, &set, 0)
        .await
    {
        eprintln!("\nnote: could not truncate {set} ({e}) - a later run will be slower for it");
    }
}
