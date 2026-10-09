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

//! Query server/node information via the info protocol.
//!
//! Port of the Java client's `ServerInfo` example.

use std::env;

use aerospike::{AdminPolicy, Client, ClientPolicy};

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = std::env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let apolicy = AdminPolicy::default();

    // ---- What the client knows about the cluster ----
    println!("connected:            {}", client.is_connected());
    println!("cluster nodes:        {:?}", client.node_names());
    // `cluster_name` is the name the ClientPolicy asked for (validation only);
    // `server_cluster_name` is what the nodes report, whether or not it was
    // asked for.
    println!("expected name:        {:?}", client.cluster_name());
    println!("server-reported name: {:?}", client.server_cluster_name());
    // `partition_map_ready` says every namespace has a map; `_complete` says
    // every partition in it has an owner.
    println!(
        "partition map:        ready = {}, complete = {}",
        client.partition_map_ready(),
        client.partition_map_complete()
    );
    println!(
        "namespace `test`:     strong consistency = {:?}",
        client.is_strong_consistency("test")
    );
    if let Some(name) = client.node_names().first() {
        let node = client.get_node(name).expect("node by name");
        println!("node {name}: address {}, active = {}", node.address(), node.is_active());
    }

    for node in client.nodes() {
        println!("--- node {} (server {:?}) ---", node.name(), node.version());

        let info = node
            .info(&apolicy, &["build", "edition", "namespaces", "statistics"])
            .await
            .unwrap();

        println!("build:      {:?}", info.get("build"));
        println!("edition:    {:?}", info.get("edition"));
        println!("namespaces: {:?}", info.get("namespaces"));

        // `statistics` is a large ';'-separated list — show a taste.
        if let Some(stats) = info.get("statistics") {
            for stat in stats.split(';').take(5) {
                println!("stat:       {stat}");
            }
            println!(
                "…           ({} statistics total)",
                stats.split(';').count()
            );
        }
    }

    client.close().await.unwrap();
}
