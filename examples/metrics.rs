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

//! Client metrics: what the client measures about itself and how to read it.
//!
//! Metrics have two tiers. The base tier counts connections opened and closed,
//! tend cycles and node changes, and reports the pool gauges. The operational
//! tier adds per-command latency histograms, bytes sent and received, and
//! error counts, sampled per call. This example turns both on, runs some
//! traffic, reads the snapshot, prints a few figures, and dumps the whole
//! snapshot as JSON — the form a monitoring agent would ship.
//!
//! ```bash
//! cargo run --example metrics
//! ```

use std::env;

use aerospike::metrics::MetricsPolicy;
use aerospike::{as_bin, as_key, Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};

const SET: &str = "metrics_demo";

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

    // ---- Turn metrics on: millisecond histograms, operational tier included ----
    // `millis()` is the default layout (seven columns, doubling from 1 ms);
    // `with_operational(true)` enables the per-command instruments.
    let policy = MetricsPolicy::millis().with_operational(true);
    client.enable_metrics(policy);
    println!("metrics enabled: {}", client.metrics_enabled());

    // ---- Generate some traffic to measure ----
    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    for i in 0..200 {
        let key = as_key!("test", SET, i);
        client
            .put(&wpolicy, &key, &[as_bin!("n", i), as_bin!("s", "metrics")])
            .await
            .unwrap();
        client.get(&rpolicy, &key, Bins::All).await.unwrap();
    }
    // A miss counts too: the result code histogram records KEY_NOT_FOUND.
    let _ = client.get(&rpolicy, &as_key!("test", SET, "missing"), Bins::All).await;

    // ---- Read the snapshot ----
    // `metrics()` merges every node's figures into `cluster_aggregated` and
    // keeps the per-node views in `nodes`. Pool gauges are live values.
    let snapshot = client.metrics();
    println!("nodes measured: {}", snapshot.total_nodes);
    println!("open connections: {}", snapshot.open_connections);
    println!("in use / in pool: {} / {}", snapshot.connections_in_use, snapshot.connections_in_pool);
    let counters = &snapshot.cluster_aggregated.counters;
    println!(
        "connections attempted / successful / failed: {} / {} / {}",
        counters.connections_attempts, counters.connections_successful, counters.connections_failed
    );
    println!("tend cycles: {} ({} failed)", counters.tends_total, counters.tends_failed);

    // The JSON form carries everything: per-command latency columns, bytes,
    // result-code counts, labels and the pool gauges, with snake_case keys.
    let json = serde_json::to_string_pretty(&snapshot).expect("snapshot serializes");
    let preview: String = json.lines().take(24).collect::<Vec<_>>().join("\n");
    println!("snapshot (first lines):\n{preview}\n…");
    println!("snapshot size: {} bytes of JSON", json.len());

    // ---- Off again: collection stops, the gauges stay readable ----
    client.disable_metrics();
    println!("metrics enabled: {}", client.metrics_enabled());

    for i in 0..200 {
        let _ = client.delete(&wpolicy, &as_key!("test", SET, i)).await;
    }
    client.close().await.unwrap();
}
