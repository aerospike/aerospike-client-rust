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

//! Dynamic configuration from a YAML file.
//!
//! With the `dynamic-config` feature, a client built through
//! `Client::new_with_config` takes its tunables from a `ConfigProvider`
//! instead of (or on top of) the policies in code. `YamlFileProvider` reads
//! the cross-client YAML format: a `static` section applied once at start,
//! and a `dynamic` section the client re-reads on a timer, so operators can
//! change timeouts, retries or metrics on a running process by editing the
//! file.
//!
//! The example writes a config file, starts a client on it, flips a setting
//! in the file and watches the client pick it up.
//!
//! ```bash
//! cargo run --example config_file --features dynamic-config
//! ```

use std::env;
use std::sync::Arc;
use std::time::Duration;

use aerospike::config::YamlFileProvider;
use aerospike::{as_bin, as_key, Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};

const SET: &str = "config_demo";

/// The YAML document. `config_interval` is in seconds (minimum 1); the
/// `dynamic` section can change anything the client resolves per call.
fn config_yaml(metrics_enabled: bool, total_timeout_ms: u32) -> String {
    format!(
        "version: \"1.0.0\"\n\
         static:\n\
         \x20 client:\n\
         \x20   config_interval: 1\n\
         dynamic:\n\
         \x20 read:\n\
         \x20   total_timeout: {total_timeout_ms}\n\
         \x20   max_retries: 3\n\
         \x20 metrics:\n\
         \x20   enabled: {metrics_enabled}\n\
         \x20   extended:\n\
         \x20     operational:\n\
         \x20       enabled: true\n"
    )
}

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

    // ---- Write the file and start a client on it ----
    let path = env::temp_dir().join(format!("aerospike-config-example-{}.yaml", std::process::id()));
    std::fs::write(&path, config_yaml(true, 2_000)).expect("write the config file");
    println!("config file: {}", path.display());

    let provider = Arc::new(YamlFileProvider::new(path.clone()));
    let client = Client::new_with_config(&cpolicy, &hosts, provider)
        .await
        .expect("Failed to connect to cluster");

    // The static and dynamic sections were applied during construction.
    println!("metrics enabled after start: {}", client.metrics_enabled());

    // Policies in code still work; the file's `dynamic` values override the
    // matching fields at call time.
    let wpolicy = WritePolicy::default();
    let key = as_key!("test", SET, "k");
    client.put(&wpolicy, &key, &[as_bin!("v", 1)]).await.unwrap();
    let rec = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();
    println!("read under the file's read policy: {:?}", rec.bins.get("v"));

    // ---- Change the file; the watcher reloads it ----
    // The provider reloads only when the file's mtime moves, so leave a
    // second between writes.
    aerospike_rt::sleep(Duration::from_millis(1_100)).await;
    std::fs::write(&path, config_yaml(false, 5_000)).expect("rewrite the config file");
    println!("rewrote the file with metrics disabled; waiting for the watcher…");

    let mut reloaded = false;
    for _ in 0..40 {
        aerospike_rt::sleep(Duration::from_millis(250)).await;
        if !client.metrics_enabled() {
            reloaded = true;
            break;
        }
    }
    println!("metrics enabled after reload: {} (reloaded: {reloaded})", client.metrics_enabled());

    let _ = client.delete(&wpolicy, &key).await;
    client.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}
