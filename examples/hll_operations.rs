// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
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

//! HyperLogLog (HLL) operations: cardinality estimates on the server.
//!
//! An HLL bin holds a sketch of a set. Adding elements costs a few bytes
//! however many there are, and the server answers "how many distinct values"
//! and "how many in the union of these sketches" with a small, bounded error.
//! This example builds two sketches, estimates each, unions them, measures
//! their similarity, folds one to a coarser precision, and reads the same
//! figures through HLL expressions.
//!
//! ```bash
//! cargo run --example hll_operations
//! ```

use std::env;

use aerospike::expressions::{self as exp, hll as hll_exp};
use aerospike::operations::exp::{read_exp, ExpReadFlags};
use aerospike::operations::hll::{self, HllPolicy, HllWriteFlags};
use aerospike::{as_key, as_val, Bins, Client, ClientPolicy, ReadPolicy, Value, WritePolicy};

const SET: &str = "hll_demo";
const BIN: &str = "visitors";

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    let hll_policy = HllPolicy::new(HllWriteFlags::DEFAULT);

    // Two pages, each with its own sketch of the visitors it saw.
    let home = as_key!("test", SET, "page:home");
    let pricing = as_key!("test", SET, "page:pricing");
    let _ = client.delete(&wpolicy, &home).await;
    let _ = client.delete(&wpolicy, &pricing).await;

    // ---- init + add: an index bit count of 12 gives ~1.6% error at 4 KB ----
    let home_visitors: Vec<Value> = (0..5000).map(|i| as_val!(format!("user-{i}"))).collect();
    let pricing_visitors: Vec<Value> = (4000..6000).map(|i| as_val!(format!("user-{i}"))).collect();

    let ops = [
        hll::init(&hll_policy, BIN, 12),
        hll::add(&hll_policy, BIN, home_visitors),
    ];
    let rec = client.operate(&wpolicy, &home, &ops).await.unwrap();
    println!("home: add reported {:?} new entries", rec.bins.get(BIN));

    // `add_with_index` creates the sketch when the bin is missing, so a
    // separate `init` is optional.
    let ops = [hll::add_with_index(&hll_policy, BIN, pricing_visitors, 12)];
    client.operate(&wpolicy, &pricing, &ops).await.unwrap();

    // ---- get_count: the cardinality estimate ----
    let rec = client.operate(&wpolicy, &home, &[hll::get_count(BIN)]).await.unwrap();
    println!("home distinct visitors ≈ {:?} (exact: 5000)", rec.bins.get(BIN));

    // ---- describe: the sketch's precision ----
    let rec = client.operate(&wpolicy, &home, &[hll::describe(BIN)]).await.unwrap();
    println!("home sketch [index bits, min-hash bits] = {:?}", rec.bins.get(BIN));

    // ---- union: combine with another sketch without touching it ----
    // Read the pricing sketch as a value and hand it to the home record.
    let pricing_rec = client.get(&rpolicy, &pricing, Bins::from([BIN])).await.unwrap();
    let pricing_sketch = pricing_rec.bins.get(BIN).cloned().expect("pricing sketch");
    assert!(matches!(pricing_sketch, Value::Hll(_)));

    let ops = [
        hll::get_union_count(BIN, vec![pricing_sketch.clone()]),
        hll::get_intersect_count(BIN, vec![pricing_sketch.clone()]),
        hll::get_similarity(BIN, vec![pricing_sketch.clone()]),
    ];
    let rec = client.operate(&wpolicy, &home, &ops).await.unwrap();
    let results = rec.results.as_deref().unwrap_or(&[]);
    println!(
        "home ∪ pricing ≈ {:?} (exact 6000), home ∩ pricing ≈ {:?} (exact 1000), similarity {:?}",
        results.first(),
        results.get(1),
        results.get(2)
    );

    // `set_union` folds the other sketch into this bin permanently.
    let ops = [
        hll::set_union(&hll_policy, BIN, vec![pricing_sketch]),
        hll::get_count(BIN),
    ];
    let rec = client.operate(&wpolicy, &home, &ops).await.unwrap();
    println!("home after set_union ≈ {:?} distinct", rec.results.as_deref().and_then(|r| r.get(1)));

    // ---- fold: shrink the sketch to fewer index bits (less memory, more error) ----
    let ops = [hll::fold(BIN, 10), hll::describe(BIN)];
    let rec = client.operate(&wpolicy, &home, &ops).await.unwrap();
    println!("home after fold to 10 bits: {:?}", rec.results.as_deref().and_then(|r| r.get(1)));

    // `refresh_count` recomputes the cached count after folds and unions.
    client.operate(&wpolicy, &home, &[hll::refresh_count(BIN)]).await.unwrap();

    // ---- The same figures as expressions, usable in filters and read_exp ----
    // Count the home sketch and check whether a value may be in it, in one
    // read_exp each. Expressions run on the server and never write.
    let ops = [
        read_exp("count", hll_exp::get_count(exp::hll_bin(BIN)), ExpReadFlags::DEFAULT),
        read_exp(
            "maybe_member",
            hll_exp::may_contain(exp::list_val(vec![as_val!("user-42")]), exp::hll_bin(BIN)),
            ExpReadFlags::DEFAULT,
        ),
    ];
    let rec = client.operate(&wpolicy, &home, &ops).await.unwrap();
    println!(
        "expression count ≈ {:?}, user-42 may be a member: {:?}",
        rec.bins.get("count"),
        rec.bins.get("maybe_member")
    );

    // A filter expression on the estimate: only records with over 5500 visitors.
    let mut big = ReadPolicy::default();
    big.base_policy.filter_expression = Some(exp::gt(
        hll_exp::get_count(exp::hll_bin(BIN)),
        exp::int_val(5500),
    ));
    let filtered = client.get(&big, &home, Bins::None).await;
    println!("home passes the >5500 filter: {}", filtered.is_ok());

    let _ = client.delete(&wpolicy, &home).await;
    let _ = client.delete(&wpolicy, &pricing).await;
    client.close().await.unwrap();
}
