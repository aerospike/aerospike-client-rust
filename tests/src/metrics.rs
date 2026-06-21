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

//! Integration tests for the client metrics subsystem. These require a running
//! Aerospike server (like every test in this crate).

use std::collections::HashMap;

use aerospike::{
    as_bin, as_key, operations, BatchOperation, BatchPolicy, BatchReadPolicy, BatchWritePolicy,
    Bins, Client, CommandType, MetricsPolicy, ReadPolicy, WritePolicy,
};
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

use crate::common::{self};

// Runs put/get/get-header/exists/operate/delete against one key so several
// per-command-type histograms accumulate data.
async fn exercise_single_key(client: &Client, namespace: &str, set_name: &str) {
    let key = as_key!(namespace, set_name, "metrics-key");
    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    let bins = [as_bin!("bin", "value")];

    client.put(&wpolicy, &key, &bins).await.unwrap();
    let _ = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    let _ = client.get(&rpolicy, &key, Bins::None).await.unwrap(); // header-only
    let _ = client.exists(&rpolicy, &key).await.unwrap();
    let _ = client
        .operate(&wpolicy, &key, &[operations::put(&bins[0])])
        .await
        .unwrap();
    let _ = client.delete(&wpolicy, &key).await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_disabled_by_default() {
    let client = common::client().await;
    assert!(!client.metrics_enabled());

    // Running commands while disabled records nothing.
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;
    for ct in [CommandType::Put, CommandType::Get, CommandType::Delete] {
        assert_eq!(
            agg.command_histogram(ct).unwrap().count(),
            0,
            "no samples expected while metrics disabled for {ct:?}"
        );
    }
    assert_eq!(agg.counters.transaction_error_count, 0);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_enable_disable_toggle() {
    let client = common::client().await;
    assert!(!client.metrics_enabled());
    client.enable_metrics(MetricsPolicy::default());
    assert!(client.metrics_enabled());
    client.disable_metrics();
    assert!(!client.metrics_enabled());
    client.enable_metrics(MetricsPolicy::default());
    assert!(client.metrics_enabled());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_single_key_command_histograms() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;

    assert!(metrics.total_nodes >= 1);
    assert!(
        metrics.open_connections >= 1,
        "expected open connections > 0"
    );
    assert!(
        !metrics.nodes.is_empty(),
        "expected per-node metrics entries"
    );

    // Every command type we issued recorded at least one latency sample.
    for ct in [
        CommandType::Put,
        CommandType::Get,
        CommandType::GetHeader,
        CommandType::Exists,
        CommandType::Operate,
        CommandType::Delete,
    ] {
        let h = agg
            .command_histogram(ct)
            .unwrap_or_else(|| panic!("missing histogram for {ct:?}"));
        assert!(
            h.count() >= 1,
            "expected >=1 sample for {ct:?}, got {}",
            h.count()
        );
        // A recorded latency implies non-zero sum bookkeeping is consistent.
        assert!(h.sum() >= 0.0);
    }
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_detailed_and_result_codes() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "detailed-key");
    let missing = as_key!(namespace, &set_name, "does-not-exist");
    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    let bins = [as_bin!("bin", "value")];

    client.put(&wpolicy, &key, &bins).await.unwrap();
    let _ = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    // Read a missing key: server returns KEY_NOT_FOUND, recorded as a Get
    // result code (the command itself returns an error).
    let missing_res = client.get(&rpolicy, &missing, Bins::All).await;
    assert!(missing_res.is_err(), "expected error reading missing key");

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;

    // Detailed per-namespace metrics populated for this namespace.
    let put_metric = agg
        .detailed_metric(namespace, CommandType::Put)
        .expect("expected detailed Put metrics for namespace");
    assert!(put_metric.bytes_sent.count() >= 1);
    assert!(put_metric.latency.count() >= 1);

    let get_metric = agg
        .detailed_metric(namespace, CommandType::Get)
        .expect("expected detailed Get metrics for namespace");
    assert!(get_metric.parsing.count() >= 1);
    assert!(get_metric.bytes_received.count() >= 1);

    // Result codes recorded per (namespace, command, code): a successful Get
    // (OK) and the missing-key Get (KEY_NOT_FOUND_ERROR).
    assert!(
        agg.result_code_count(namespace, CommandType::Get, aerospike::ResultCode::Ok) >= 1,
        "expected at least one OK Get result code"
    );
    assert!(
        agg.result_code_count(
            namespace,
            CommandType::Get,
            aerospike::ResultCode::KeyNotFoundError
        ) >= 1,
        "expected a KEY_NOT_FOUND_ERROR Get result code"
    );
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_connection_and_tend_counters() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    // Allow at least one tend cycle to run (default tend interval is ~1s).
    sleep(Duration::from_millis(1500)).await;

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;
    assert!(
        metrics.open_connections >= 1,
        "expected open connections > 0"
    );
    assert!(
        agg.counters.tends_total >= 1,
        "expected tends-total to advance after a tend cycle, got {}",
        agg.counters.tends_total
    );
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_labels_include_reserved_and_custom() {
    let client = common::client().await;

    let mut custom = HashMap::new();
    custom.insert("env".to_string(), "test".to_string());
    custom.insert("team".to_string(), "client".to_string());
    client.enable_metrics(MetricsPolicy::default_with_labels(vec![custom]));

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let metrics = client.metrics();
    let labels = metrics.cluster_aggregated.labels.entries();
    // One label set per node.
    assert_eq!(labels.len(), metrics.total_nodes);
    assert!(!labels.is_empty());

    for entry in labels {
        // Reserved labels are always present.
        assert!(entry.contains_key("node"), "missing reserved 'node' label");
        assert!(entry.contains_key("host"), "missing reserved 'host' label");
        assert!(entry.contains_key("cluster"));
        assert!(entry.contains_key("app-id"));
        // Custom labels are merged in.
        assert_eq!(entry.get("env").map(String::as_str), Some("test"));
        assert_eq!(entry.get("team").map(String::as_str), Some("client"));
    }
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_batch_histograms() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let bin = as_bin!("bin", 42);

    // The batch executor has a single-key fast path: a node that receives only
    // one key is served by a regular single-key command (recorded as Put/Get),
    // bypassing the batch protocol. To deterministically exercise the
    // BatchWrite/BatchRead path we need at least one node to receive >= 2 keys.
    // With `nodes + 1` distinct keys, the pigeonhole principle guarantees that
    // regardless of how keys distribute across the cluster.
    let key_count = client.nodes().len() + 1;
    let keys: Vec<_> = (0..key_count)
        .map(|i| as_key!(namespace, &set_name, i as i64))
        .collect();

    let mut bpolicy = BatchPolicy::default();
    // A generous timeout so the batch isn't aborted under heavy parallel load.
    bpolicy.base_policy.total_timeout = 5000;
    let bpw = BatchWritePolicy::default();
    let bpr = BatchReadPolicy::default();

    // Batch containing writes -> BatchWrite.
    let writes: Vec<_> = keys
        .iter()
        .map(|k| BatchOperation::write(&bpw, k.clone(), vec![operations::put(&bin)]))
        .collect();
    client.batch(&bpolicy, &writes).await.unwrap();

    // Read-only batch -> BatchRead.
    let reads: Vec<_> = keys
        .iter()
        .map(|k| BatchOperation::read(&bpr, k.clone(), Bins::All))
        .collect();
    client.batch(&bpolicy, &reads).await.unwrap();

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;

    assert!(
        agg.command_histogram(CommandType::BatchWrite)
            .unwrap()
            .count()
            >= 1,
        "expected batch-write samples"
    );
    assert!(
        agg.command_histogram(CommandType::BatchRead)
            .unwrap()
            .count()
            >= 1,
        "expected batch-read samples"
    );

    // Detailed metrics recorded per namespace for the batch command types.
    assert!(agg
        .detailed_metric(namespace, CommandType::BatchWrite)
        .is_some());
    assert!(agg
        .detailed_metric(namespace, CommandType::BatchRead)
        .is_some());
    client.close().await.unwrap();
}

#[cfg(feature = "serialization")]
#[aerospike_macro::test]
async fn metrics_json_serialization_layout() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let metrics = client.metrics();
    let v = serde_json::to_value(&metrics).expect("serialize ClusterMetrics");

    // Synthetic top-level keys are present in the serialized map.
    assert!(v.get("cluster-aggregated-metrics").is_some());
    assert!(v.get("total-nodes").is_some());
    assert!(v.get("open-connections").is_some());
    assert!(v.get("exceeded-max-retries").is_some());
    assert!(v.get("exceeded-total-timeout").is_some());

    let agg = &v["cluster-aggregated-metrics"];
    // Stable counter and histogram field names.
    assert!(agg.get("connections-attempts").is_some());
    assert!(agg.get("put-metrics").is_some());
    assert!(agg["put-metrics"].get("buckets").unwrap().is_array());
    assert!(agg.get("detailed-metrics").is_some());
    assert!(agg.get("detailed-resultcode-counts").is_some());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_never_sampler_records_no_commands() {
    let client = common::client().await;

    // Metrics enabled, but a `None` sampler means no command is ever
    // recorded even though collection is "on".
    let policy = MetricsPolicy {
        sampler: None,
        ..MetricsPolicy::default()
    };
    client.enable_metrics(policy);
    assert!(client.metrics_enabled());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;
    for ct in [
        CommandType::Put,
        CommandType::Get,
        CommandType::GetHeader,
        CommandType::Exists,
        CommandType::Operate,
        CommandType::Delete,
    ] {
        assert_eq!(
            agg.command_histogram(ct).unwrap().count(),
            0,
            "None sampler must record no samples for {ct:?}"
        );
    }
    // No detailed per-namespace metrics either.
    assert!(agg.detailed_metric(namespace, CommandType::Put).is_none());
    client.close().await.unwrap();
}
