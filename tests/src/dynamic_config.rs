// Copyright 2015-2024 Aerospike, Inc.
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

//! Integration tests for dynamic configuration (the `dynamic-config` feature).
//! Require a running Aerospike server, like every test in this crate.

use std::path::PathBuf;
use std::sync::Arc;

use aerospike::config::YamlFileProvider;
use aerospike::Client;
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

use crate::common;

fn temp_config_path(tag: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "aerospike-dynconfig-{}-{tag}.yaml",
        std::process::id()
    ));
    path
}

// `config_interval: 1` (seconds) makes the watcher reload roughly once a second
// (clamped to a 1s minimum), keeping the reload assertion quick.
fn config_yaml(metrics_enabled: bool) -> String {
    format!(
        "version: \"1.0.0\"\n\
         static:\n  client:\n    config_interval: 1\n\
         dynamic:\n  metrics:\n    enable: {metrics_enabled}\n    latency_columns: 9\n"
    )
}

/// Construction applies the initial config (metrics on), and the background
/// watcher picks up a later file change (metrics off).
#[aerospike_macro::test]
async fn dynamic_config_applies_and_reloads() {
    let path = temp_config_path("metrics");
    std::fs::write(&path, config_yaml(true)).expect("write initial config");

    let provider = Arc::new(YamlFileProvider::new(path.clone()));
    let client = Client::new_with_config(common::client_policy(), &common::hosts(), provider)
        .await
        .expect("client with dynamic config");

    // Initial load was applied synchronously during construction.
    assert!(
        client.metrics_enabled(),
        "metrics should be enabled by the initial dynamic config"
    );

    // Rewrite the file to disable metrics. Sleep first so the new mtime is
    // strictly greater (the provider skips reloads when mtime is unchanged).
    sleep(Duration::from_millis(1100)).await;
    std::fs::write(&path, config_yaml(false)).expect("rewrite config");

    // Wait out at least one watch interval (min 1s) plus slack.
    sleep(Duration::from_millis(2500)).await;
    assert!(
        !client.metrics_enabled(),
        "metrics should be disabled after the watcher reloads the config"
    );

    client.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}

/// `dynamic.metrics.latency_unit` picks the resolution of the latency
/// histograms, and a later reload can switch it - which discards the samples
/// collected in the previous unit, since they cannot share buckets.
#[aerospike_macro::test]
async fn dynamic_metrics_latency_unit_applies_and_switches() {
    use aerospike::{CommandType, LatencyUnit};

    let unit_yaml = |unit: &str| {
        format!(
            "version: \"1.0.0\"\n\
             static:\n  client:\n    config_interval: 1\n\
             dynamic:\n  metrics:\n    enable: true\n    latency_unit: {unit}\n"
        )
    };

    let path = temp_config_path("latency-unit");
    std::fs::write(&path, unit_yaml("ms")).expect("write initial config");

    let provider = Arc::new(YamlFileProvider::new(path.clone()));
    let client = Client::new_with_config(common::client_policy(), &common::hosts(), provider)
        .await
        .expect("client with dynamic config");

    // The initial load enabled metrics with the file's unit.
    assert!(client.metrics_enabled());
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = aerospike::as_key!(namespace, &set_name, "latency-unit");
    let bins = [aerospike::as_bin!("bin", "value")];
    client
        .put(&aerospike::WritePolicy::default(), &key, &bins)
        .await
        .expect("put");

    let agg = client.metrics().cluster_aggregated;
    assert_eq!(agg.latency_unit, LatencyUnit::Milliseconds);
    assert!(
        agg.command_histogram(CommandType::Put).unwrap().count() >= 1,
        "expected a Put sample in the millisecond histogram"
    );

    // Switch the unit in the file. Sleep first so the mtime is strictly greater
    // (the provider skips reloads when it is unchanged).
    sleep(Duration::from_millis(1100)).await;
    std::fs::write(&path, unit_yaml("us")).expect("rewrite config");
    sleep(Duration::from_millis(2500)).await;

    let agg = client.metrics().cluster_aggregated;
    assert_eq!(
        agg.latency_unit,
        LatencyUnit::Microseconds,
        "the watcher should have applied the new unit"
    );
    assert_eq!(
        agg.command_histogram(CommandType::Put).unwrap().count(),
        0,
        "millisecond samples must not survive the switch to microseconds"
    );

    client.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}

/// A client built with a dynamic read/write override still performs operations
/// correctly — the overrides layer onto the per-call policies transparently.
#[aerospike_macro::test]
async fn dynamic_config_overrides_do_not_break_operations() {
    let path = temp_config_path("ops");
    std::fs::write(
        &path,
        "version: \"1.0.0\"\n\
         dynamic:\n\
         \x20 read:\n    socket_timeout: 2000\n    total_timeout: 3000\n\
         \x20 write:\n    socket_timeout: 2000\n    durable_delete: false\n",
    )
    .expect("write config");

    let provider = Arc::new(YamlFileProvider::new(path.clone()));
    let client = Client::new_with_config(common::client_policy(), &common::hosts(), provider)
        .await
        .expect("client with dynamic config");

    let key = aerospike::as_key!(common::namespace(), common::prop_setname(), "dynconfig-ops");
    let bins = [aerospike::as_bin!("bin", "value")];
    client
        .put(&aerospike::WritePolicy::default(), &key, &bins)
        .await
        .expect("put with dynamic write override");
    let record = client
        .get(&aerospike::ReadPolicy::default(), &key, aerospike::Bins::All)
        .await
        .expect("get with dynamic read override");
    assert_eq!(record.bins.get("bin").unwrap().to_string(), "value");

    client
        .delete(&aerospike::WritePolicy::default(), &key)
        .await
        .unwrap();
    client.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}

/// A multi-key batch with `batch_read`/`batch_write`/`batch_delete` sub-sections
/// configured executes correctly through the per-record overlay and the
/// multi-key wire path (`patch_batch_wire`).
#[aerospike_macro::test]
async fn dynamic_config_batch_sub_policies_apply() {
    use aerospike::{
        BatchDeletePolicy, BatchOperation, BatchPolicy, BatchReadPolicy, BatchWritePolicy, Bins,
    };

    let path = temp_config_path("batch");
    std::fs::write(
        &path,
        "version: \"1.0.0\"\n\
         dynamic:\n\
         \x20 batch_read:\n    read_mode_ap: ALL\n    socket_timeout: 2000\n\
         \x20 batch_write:\n    durable_delete: false\n    send_key: true\n    socket_timeout: 2000\n\
         \x20 batch_delete:\n    durable_delete: false\n    send_key: true\n",
    )
    .expect("write config");

    let provider = Arc::new(YamlFileProvider::new(path.clone()));
    let client = Client::new_with_config(common::client_policy(), &common::hosts(), provider)
        .await
        .expect("client with dynamic config");

    let ns = common::namespace();
    let set = common::prop_setname();
    // Multiple keys → on a single-node test cluster they group onto one node,
    // exercising the multi-key wire batch path (`patch_batch_wire`).
    let keys: Vec<_> = (0..3)
        .map(|i| aerospike::as_key!(ns, set, format!("dynconfig-batch-{i}")))
        .collect();
    let wops = vec![aerospike::operations::put(&aerospike::as_bin!("v", 1))];

    let bwp = BatchWritePolicy::default();
    let writes: Vec<BatchOperation> = keys
        .iter()
        .map(|k| BatchOperation::write(&bwp, k.clone(), wops.clone()))
        .collect();
    let write_results = client
        .batch(&BatchPolicy::default(), &writes)
        .await
        .expect("batch write with batch_write sub-section config");
    assert_eq!(write_results.len(), 3);

    let brp = BatchReadPolicy::default();
    let reads: Vec<BatchOperation> = keys
        .iter()
        .map(|k| BatchOperation::read(&brp, k.clone(), Bins::All))
        .collect();
    let read_results = client
        .batch(&BatchPolicy::default(), &reads)
        .await
        .expect("batch read with batch_read sub-section config");
    assert_eq!(read_results.len(), 3);
    for br in &read_results {
        assert_eq!(
            br.record.as_ref().and_then(|r| r.bins.get("v")).map(ToString::to_string),
            Some("1".to_string())
        );
    }

    let bdp = BatchDeletePolicy::default();
    let deletes: Vec<BatchOperation> = keys
        .iter()
        .map(|k| BatchOperation::delete(&bdp, k.clone()))
        .collect();
    client
        .batch(&BatchPolicy::default(), &deletes)
        .await
        .expect("batch delete with batch_delete sub-section config");

    client.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}
