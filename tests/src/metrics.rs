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

use aerospike::metrics::CommandMetric;
use aerospike::query::{Filter, PartitionFilter};
use aerospike::{
    as_bin, as_key, as_val, operations, AdminPolicy, BatchDeletePolicy, BatchOperation,
    BatchPolicy, BatchReadPolicy, BatchUDFPolicy, BatchWritePolicy, Bins, Client,
    CollectionIndexType, CommandType, IndexType, LatencyUnit, MetricsPolicy, QueryPolicy,
    ReadPolicy, Statement, Task, UDFLang, WritePolicy,
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
    client.enable_metrics(MetricsPolicy::default().with_operational(true));
    assert!(client.metrics_enabled());
    client.disable_metrics();
    assert!(!client.metrics_enabled());
    client.enable_metrics(MetricsPolicy::default().with_operational(true));
    assert!(client.metrics_enabled());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_single_key_command_histograms() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

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

// A local command usually takes well under a millisecond, so the two units see
// the same work very differently: microseconds resolve it, milliseconds round it
// to 0. This is the observable point of `MetricsPolicy::latency_unit`.
#[aerospike_macro::test]
async fn metrics_latency_unit_changes_resolution() {
    let namespace = common::namespace();

    // Microseconds: sub-millisecond latency is measurable, so the recorded
    // maximum is in the hundreds-or-more range rather than 0.
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::micros().with_operational(true));
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let agg = client.metrics().cluster_aggregated;
    assert_eq!(agg.latency_unit, LatencyUnit::Microseconds);
    let us = agg.command_histogram(CommandType::Put).unwrap();
    assert!(us.count() >= 1, "expected a Put sample");
    assert!(
        us.max() > 0,
        "a Put took {}µs - microsecond metrics should not round a real command to 0",
        us.max()
    );
    // The per-RPC detailed latency spans connection acquire → response
    // parsed, so it contains both the acquire and the parse phases and can
    // never be shorter than either; and the whole-call latency contains the
    // RPC. Microseconds make the ordering observable.
    let put = agg
        .detailed_metric(namespace, CommandType::Put)
        .expect("detailed Put metrics");
    assert!(put.latency.count() >= 1);
    assert!(
        put.latency.max() >= put.parsing.max(),
        "rpc latency {}µs must cover parsing {}µs",
        put.latency.max(),
        put.parsing.max()
    );
    assert!(
        put.latency.max() >= put.connection_aq.max(),
        "rpc latency {}µs must cover connection acquire {}µs",
        put.latency.max(),
        put.connection_aq.max()
    );
    assert!(
        us.max() >= put.latency.max(),
        "whole-call latency {}µs must cover the rpc {}µs",
        us.max(),
        put.latency.max()
    );
    client.close().await.unwrap();

    // Milliseconds (the default): the same work, coarser buckets. The unit
    // travels with the snapshot so a consumer can tell the two apart.
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::millis().with_operational(true));
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let agg = client.metrics().cluster_aggregated;
    assert_eq!(agg.latency_unit, LatencyUnit::Milliseconds);
    let ms = agg.command_histogram(CommandType::Put).unwrap();
    assert!(ms.count() >= 1, "expected a Put sample");
    assert!(
        ms.max() < us.max(),
        "millisecond max ({}) should be far below the microsecond max ({})",
        ms.max(),
        us.max()
    );
    assert_eq!(
        ms.buckets().len(),
        7,
        "the millis preset keeps the Java-parity 7 columns"
    );
    client.close().await.unwrap();
}

// Switching unit while collecting must not blend the two resolutions in one
// histogram: the samples recorded before the switch are dropped.
#[aerospike_macro::test]
async fn metrics_unit_switch_discards_earlier_samples() {
    let client = common::client().await;
    let namespace = common::namespace();

    client.enable_metrics(MetricsPolicy::micros().with_operational(true));
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;
    assert!(
        client
            .metrics()
            .cluster_aggregated
            .command_histogram(CommandType::Put)
            .unwrap()
            .count()
            >= 1
    );

    // Re-enable with the other unit; the accumulated microsecond samples go.
    client.enable_metrics(MetricsPolicy::millis().with_operational(true));
    let agg = client.metrics().cluster_aggregated;
    assert_eq!(agg.latency_unit, LatencyUnit::Milliseconds);
    assert_eq!(
        agg.command_histogram(CommandType::Put).unwrap().count(),
        0,
        "microsecond samples must not survive a switch to milliseconds"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_detailed_and_result_codes() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

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

    // Byte accounting is exact and outcome-independent: one Put and two
    // Gets went on the wire (the second Get failed with KEY_NOT_FOUND, but
    // its request was sent and its error reply was read), so each side
    // holds exactly that many samples, every one at least a wire header.
    assert_eq!(put_metric.bytes_sent.count(), 1);
    assert_eq!(put_metric.bytes_received.count(), 1);
    assert_eq!(
        get_metric.bytes_sent.count(),
        2,
        "the failed Get's request bytes must be counted"
    );
    assert_eq!(
        get_metric.bytes_received.count(),
        2,
        "the failed Get's error reply bytes must be counted"
    );
    for (label, h) in [
        ("put sent", &put_metric.bytes_sent),
        ("put received", &put_metric.bytes_received),
        ("get sent", &get_metric.bytes_sent),
        ("get received", &get_metric.bytes_received),
    ] {
        assert!(
            h.min() >= MSG_HEADER,
            "{label}: smallest sample {} is below a wire header",
            h.min()
        );
    }
    // Only the successful attempts carry a latency / parse sample.
    assert_eq!(get_metric.latency.count(), 1);
    assert_eq!(get_metric.parsing.count(), 1);

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
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

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
        assert!(entry.contains_key("app_id"));
        // Custom labels are merged in.
        assert_eq!(entry.get("env").map(String::as_str), Some("test"));
        assert_eq!(entry.get("team").map(String::as_str), Some("client"));
    }
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_batch_histograms() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

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
    let mut writes: Vec<_> = keys
        .iter()
        .map(|k| BatchOperation::write(&bpw, k.clone(), vec![operations::put(&bin)]))
        .collect();
    client.batch(&bpolicy, &mut writes).await.unwrap();

    // Read-only batch -> BatchRead.
    let mut reads: Vec<_> = keys
        .iter()
        .map(|k| BatchOperation::read(&bpr, k.clone(), Bins::All))
        .collect();
    client.batch(&bpolicy, &mut reads).await.unwrap();

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
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let metrics = client.metrics();
    let v = serde_json::to_value(&metrics).expect("serialize ClusterMetrics");

    // Synthetic top-level keys are present in the serialized map.
    assert!(v.get("cluster_aggregated_metrics").is_some());
    assert!(v.get("total_nodes").is_some());
    assert!(v.get("open_connections").is_some());
    assert!(v.get("connections_in_use").is_some());
    assert!(v.get("connections_in_pool").is_some());
    assert!(v.get("recover_queue_size").is_some());
    assert!(v.get("nodes_invalid").is_some());
    assert!(v.get("exceeded_max_retries").is_some());
    assert!(v.get("exceeded_total_timeout").is_some());

    let agg = &v["cluster_aggregated_metrics"];
    // Stable counter and histogram field names.
    assert!(agg.get("connections_attempts").is_some());
    assert!(agg.get("connections_error_tls").is_some());
    assert!(agg.get("connections_error_auth").is_some());
    assert!(agg.get("connections_closed_error").is_some());
    assert!(agg.get("connections_closed_node_removed").is_some());
    assert!(agg.get("connections_recovering").is_some());
    assert!(agg.get("error_rate").is_some());
    assert!(agg.get("put_metrics").is_some());
    assert!(agg["put_metrics"].get("buckets").unwrap().is_array());
    assert!(agg.get("detailed_metrics").is_some());
    assert!(agg.get("detailed_resultcode_counts").is_some());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_scan_histogram_records_filterless_query() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let wpolicy = WritePolicy::default();
    for i in 0..5i64 {
        let key = as_key!(namespace, &set_name, i);
        client
            .put(&wpolicy, &key, &[as_bin!("bin", i)])
            .await
            .unwrap();
    }

    // A filter-less statement is a scan; it must land in the scan histogram.
    // (A statement with a secondary-index filter takes the identical code path
    // and is attributed to CommandType::Query instead.)
    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let rs = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
        .unwrap();
    use futures::StreamExt;
    let count = rs.into_stream().count().await;
    assert!(count >= 5, "scan should return the seeded records");

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;
    assert!(
        agg.command_histogram(CommandType::Scan).unwrap().count() > 0,
        "scan executions must be recorded in the scan-metrics histogram"
    );
    // Detailed per-namespace metrics are attributed too.
    assert!(
        agg.detailed_metric(namespace, CommandType::Scan).is_some(),
        "scan must appear in detailed per-namespace metrics"
    );
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_never_sampler_records_no_commands() {
    let client = common::client().await;

    // Metrics and the operational tier enabled, but `Sampler::never()` means
    // no command is ever recorded even though collection is "on".
    let policy = MetricsPolicy {
        sampler: aerospike::Sampler::never(),
        ..MetricsPolicy::default().with_operational(true)
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
            "never() sampler must record no samples for {ct:?}"
        );
    }
    // No detailed per-namespace metrics either.
    assert!(agg.detailed_metric(namespace, CommandType::Put).is_none());
    client.close().await.unwrap();
}

/// Tier 0 only (metrics.md §3): enabling metrics with the default policy —
/// operational group off — records the lifecycle instruments (pool gauges,
/// opened connections, tends) but nothing on the command path: no latency
/// samples, no detailed per-namespace metrics, no result codes.
#[aerospike_macro::test]
async fn metrics_tier0_only_records_lifecycle_not_commands() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default());
    assert!(client.metrics_enabled());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;
    // Client start-up already opened one pooled connection per node (before
    // metrics were on), and sequential commands reuse it. Run a burst of
    // concurrent reads so the pool has to open fresh sockets under metrics:
    // those opens are Tier 0 events, the pool-empty waits that trigger them
    // are Tier 1 and must not be counted.
    let key = as_key!(namespace, &set_name, "tier0-burst");
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", 1)])
        .await
        .unwrap();
    let rpolicy = ReadPolicy::default();
    let burst = (0..16).map(|_| client.get(&rpolicy, &key, Bins::All));
    for res in futures::future::join_all(burst).await {
        res.unwrap();
    }
    // A missing-key read fails the command; its error/result code is Tier 1
    // and must not be counted either.
    let missing = as_key!(namespace, &set_name, "tier0-missing");
    assert!(client
        .get(&ReadPolicy::default(), &missing, Bins::All)
        .await
        .is_err());

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;
    assert!(metrics.open_connections >= 2, "the burst must have grown the pool");
    assert!(
        agg.counters.connections_successful >= 1,
        "Tier 0 opened-connections counter must move"
    );
    assert_eq!(
        agg.counters.connections_pool_empty, 0,
        "pool-empty is an operational counter and must stay 0 under Tier 0"
    );
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
            "operational off: no latency samples for {ct:?}"
        );
    }
    assert!(agg.detailed_metric(namespace, CommandType::Put).is_none());
    assert_eq!(
        agg.result_code_count(
            namespace,
            CommandType::Get,
            aerospike::ResultCode::KeyNotFoundError
        ),
        0
    );
    assert_eq!(agg.counters.transaction_error_count, 0);

    // Turning the group on live (re-enable with the flag) unlocks the command
    // path without touching the Tier 0 counters already accumulated.
    client.enable_metrics(MetricsPolicy::default().with_operational(true));
    exercise_single_key(&client, namespace, &set_name).await;
    let agg = client.metrics().cluster_aggregated;
    assert!(agg.command_histogram(CommandType::Put).unwrap().count() >= 1);
    assert!(agg.counters.connections_successful >= 1);
    client.close().await.unwrap();
}

/// The pool gauges are a live pool walk (metrics.md §4.3 / §5.5.3): in-use
/// plus in-pool is the open total, an idle client has its connections in the
/// pool, and the walk still runs while collection is disabled.
#[aerospike_macro::test]
async fn metrics_pool_gauges_are_a_live_pool_walk() {
    let client = common::client().await;

    // Disabled: counters are frozen but the gauges are still read live.
    assert!(!client.metrics_enabled());
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;
    let disabled = client.metrics();
    assert!(
        disabled.open_connections >= 1,
        "pool gauges must be readable while metrics are disabled"
    );
    assert_eq!(
        disabled.connections_in_use + disabled.connections_in_pool,
        disabled.open_connections,
        "in_use + in_pool must equal the open total"
    );
    assert_eq!(disabled.cluster_aggregated.counters.connections_successful, 0);

    client.enable_metrics(MetricsPolicy::default());
    exercise_single_key(&client, namespace, &set_name).await;
    let metrics = client.metrics();
    assert_eq!(
        metrics.connections_in_use + metrics.connections_in_pool,
        metrics.open_connections
    );
    // Nothing is in flight now, so every connection is back in the pool.
    assert!(
        metrics.connections_in_pool >= 1,
        "an idle client keeps its connections in the pool"
    );
    assert_eq!(metrics.recover_queue_size, 0, "no timeouts, nothing recovering");
    // Per-node and cluster views agree.
    let mut in_use = 0;
    let mut in_pool = 0;
    for node in metrics.nodes.values() {
        let g = node.pool_gauges();
        assert_eq!(g.in_use(), node.connections_in_use());
        in_use += node.connections_in_use();
        in_pool += node.connections_in_pool();
    }
    assert_eq!(in_use, metrics.connections_in_use);
    assert_eq!(in_pool, metrics.connections_in_pool);
    assert_eq!(metrics.cluster_aggregated.connections_in_pool(), in_pool);
    client.close().await.unwrap();
}

/// The circuit-breaker gauge (`error-rate`) is stamped per node from the
/// live window count, the cluster view is its sum, and a healthy run counts
/// no failed peer validations.
#[aerospike_macro::test]
async fn metrics_error_rate_and_nodes_invalid_gauges() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let metrics = client.metrics();
    // Every node reports the same value the breaker itself holds right now.
    let mut sum = 0;
    for node in client.nodes() {
        let snapshot = metrics
            .nodes
            .get(&node.host().to_string())
            .expect("every active node has a snapshot");
        assert_eq!(snapshot.error_rate(), node.error_rate_count() as u64);
        sum += snapshot.error_rate();
    }
    assert_eq!(metrics.cluster_aggregated.error_rate(), sum);
    // Only successful commands were issued, so the window holds nothing.
    assert_eq!(sum, 0, "no command failed, the breaker window must be empty");
    assert_eq!(
        metrics.nodes_invalid, 0,
        "a healthy cluster has no failed peer validations"
    );
    client.close().await.unwrap();
}

/// Regression test for connection churn when `min_conns_per_node` is not a
/// multiple of `conn_pools_per_node`.
///
/// The idle-connection reaper used to keep a per-queue floor of
/// `min / conn_pools_per_node` (here `2 / 4 == 0`), so every tend it reaped the
/// minimum connections as "idle" and `fill_min_conns` recreated them — an
/// open/close cycle forever. With the global-budget reaper the pool stays at the
/// minimum and no idle connections are dropped while the node sits idle.
///
/// Requires a running server. Runs for a few tend cycles with no traffic so the
/// min connections cross their idle deadline and the reaper processes them.
#[aerospike_macro::test]
async fn min_conns_no_churn_across_tends() {
    let mut policy = common::client_policy().clone();
    policy.min_conns_per_node = 2;
    policy.max_conns_per_node = 8;
    policy.conn_pools_per_node = 4; // 2 / 4 = 0 per-queue floor — the churny case
    policy.idle_timeout = 2_000; // ms — min conns become reap-eligible after 2s idle
    policy.tend_interval = 1_000; // ms — reap/fill run ~every second

    let hosts = common::hosts().to_string();
    let client = Client::new(&policy, &hosts)
        .await
        .expect("connect with min/max conns configured");
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

    // No traffic: let the minimum connections go idle and several tend cycles
    // run. A churning pool accumulates idle-drops here; a healthy pool does not.
    sleep(Duration::from_secs(6)).await;

    let metrics = client.metrics();
    let idle_dropped = metrics
        .cluster_aggregated
        .counters
        .connections_idle_dropped;
    assert_eq!(
        idle_dropped, 0,
        "min connections were reaped and recreated across tends (churn); \
         connections-idle-dropped={idle_dropped}"
    );

    client.close().await.unwrap();
}

// --- bytes-received accounting -------------------------------------------
//
// Regression coverage for the detailed `bytes_received` histogram, whose sum
// used to stay at zero: the connection's read counter was reset by the
// header→body→ready state transitions before the metrics code read it, so
// every sample recorded 0 (the count advanced, the sum did not). Each test
// below reads back a payload large enough that a correct sum is
// unmistakable and asserts against it — not merely against `count`.

/// Payload large enough that any response carrying it dwarfs the protocol
/// header, so `sum >= PAYLOAD` cannot be satisfied by header bytes alone.
const PAYLOAD: usize = 4096;

/// Every wire response carries at least the 30-byte proto + message header.
const MSG_HEADER: u64 = 30;

/// Asserts that `cm.bytes_received` has samples and that their sum is at
/// least `min_sum` bytes — the invariant the sum-stays-zero bug violated.
fn assert_bytes_received(cm: &CommandMetric, label: &str, min_sum: u64) {
    let count = cm.bytes_received.count();
    let sum = cm.bytes_received.sum();
    assert!(count >= 1, "{label}: no bytes-received samples");
    assert!(
        sum >= min_sum as f64,
        "{label}: bytes-received sum {sum} < {min_sum} over {count} samples"
    );
    // Every response is at least a header; the sum must be consistent with
    // the count, not a lone stray value.
    assert!(
        sum >= (count * MSG_HEADER) as f64,
        "{label}: bytes-received sum {sum} smaller than {count} headers"
    );
    // Sanity: the sent side, which never regressed, must also be populated.
    assert!(
        cm.bytes_sent.sum() > 0.0,
        "{label}: bytes-sent sum unexpectedly zero"
    );
}

fn payload() -> String {
    "x".repeat(PAYLOAD)
}

/// Registers a Lua UDF that echoes its argument, so a UDF response can be
/// made as large as the caller wants.
async fn register_echo_udf(client: &Client) -> &'static str {
    const NAME: &str = "metrics_echo";
    let body = r#"
function echo(rec, val)
  return val
end
"#;
    let task = client
        .register_udf(
            &AdminPolicy::default(),
            body.as_bytes(),
            &format!("{NAME}.lua"),
            UDFLang::Lua,
        )
        .await
        .expect("register udf");
    task.wait_till_complete(None).await.unwrap();
    NAME
}

/// get / put / delete / udf: single-key commands each record a per-command
/// received-byte total, and a read of a 4 KiB record sums to at least that.
#[aerospike_macro::test]
async fn metrics_bytes_received_single_key_commands() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "bytes-received");
    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    let blob = payload();
    let bins = [as_bin!("blob", blob.as_str())];

    client.put(&wpolicy, &key, &bins).await.unwrap();
    const READS: u64 = 3;
    for _ in 0..READS {
        let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
        assert_eq!(rec.bins.len(), 1);
    }
    let udf = register_echo_udf(&client).await;
    let echoed = client
        .execute_udf(&wpolicy, &key, udf, "echo", Some(&[as_val!(blob.as_str())]))
        .await
        .unwrap();
    assert_eq!(echoed, Some(as_val!(blob.as_str())));
    client.delete(&wpolicy, &key).await.unwrap();

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;
    let detailed = |ct: CommandType| {
        agg.detailed_metric(namespace, ct)
            .unwrap_or_else(|| panic!("no detailed {ct:?} metrics for {namespace}"))
    };

    // Each get returns the whole 4 KiB record.
    assert_bytes_received(detailed(CommandType::Get), "Get", READS * PAYLOAD as u64);
    // The UDF echoes the 4 KiB argument back in its response.
    assert_bytes_received(detailed(CommandType::Udf), "Udf", PAYLOAD as u64);
    // Put and delete responses are header-only; they must still count.
    assert_bytes_received(detailed(CommandType::Put), "Put", MSG_HEADER);
    assert_bytes_received(detailed(CommandType::Delete), "Delete", MSG_HEADER);
    client.close().await.unwrap();
}

/// Batch read / write / delete / udf: the batch parser drives the connection
/// through many header/body segments per response and bookmarks between
/// records; the received total must survive all of that.
#[aerospike_macro::test]
async fn metrics_bytes_received_batch_commands() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let blob = payload();
    let bin = as_bin!("blob", blob.as_str());

    // See `metrics_batch_histograms`: a node holding a single key takes the
    // single-key fast path and is recorded as Get/Put, not BatchRead/Write.
    // With 2 * nodes keys at least one node holds >= 2 keys, so at least two
    // 4 KiB records flow through the batch protocol proper.
    let key_count = client.nodes().len() * 2;
    let keys: Vec<_> = (0..key_count)
        .map(|i| as_key!(namespace, &set_name, i as i64))
        .collect();

    let mut bpolicy = BatchPolicy::default();
    bpolicy.base_policy.total_timeout = 5000;
    let bpw = BatchWritePolicy::default();
    let bpr = BatchReadPolicy::default();
    let bpu = BatchUDFPolicy::default();
    let bpd = BatchDeletePolicy::default();

    let mut writes: Vec<_> = keys
        .iter()
        .map(|k| BatchOperation::write(&bpw, k.clone(), vec![operations::put(&bin)]))
        .collect();
    client.batch(&bpolicy, &mut writes).await.unwrap();

    let mut reads: Vec<_> = keys
        .iter()
        .map(|k| BatchOperation::read(&bpr, k.clone(), Bins::All))
        .collect();
    client.batch(&bpolicy, &mut reads).await.unwrap();
    for op in &reads {
        assert!(op.record().is_some(), "batch read returned no record");
    }

    let udf = register_echo_udf(&client).await;
    let mut udfs: Vec<_> = keys
        .iter()
        .map(|k| {
            BatchOperation::udf(&bpu, k.clone(), udf, "echo", Some(vec![as_val!(blob.as_str())]))
        })
        .collect();
    client.batch(&bpolicy, &mut udfs).await.unwrap();

    let mut deletes: Vec<_> = keys
        .iter()
        .map(|k| BatchOperation::delete(&bpd, k.clone()))
        .collect();
    client.batch(&bpolicy, &mut deletes).await.unwrap();

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;
    let read_metric = agg
        .detailed_metric(namespace, CommandType::BatchRead)
        .expect("no detailed BatchRead metrics");
    let write_metric = agg
        .detailed_metric(namespace, CommandType::BatchWrite)
        .expect("no detailed BatchWrite metrics");

    // At least two 4 KiB records came back over the batch protocol.
    assert_bytes_received(read_metric, "BatchRead", 2 * PAYLOAD as u64);
    // Writes, deletes and UDFs all land in BatchWrite; the UDF echo alone
    // returns at least two 4 KiB values.
    assert_bytes_received(write_metric, "BatchWrite", 2 * PAYLOAD as u64);
    client.close().await.unwrap();
}

/// Scan and secondary-index query: a stream response is parsed through the
/// buffered reader across many segments and bookmarks; the received total
/// for the command must cover every record it delivered.
#[aerospike_macro::test]
async fn metrics_bytes_received_query_commands() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default().with_operational(true));

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let wpolicy = WritePolicy::default();
    let blob = payload();
    const RECORDS: usize = 5;
    for i in 0..RECORDS as i64 {
        let key = as_key!(namespace, &set_name, i);
        client
            .put(&wpolicy, &key, &[as_bin!("bin", i), as_bin!("blob", blob.as_str())])
            .await
            .unwrap();
    }

    let index_name = format!("{}_{}_{}", namespace, set_name, "bin");
    let _index_guard = common::lock_index_ops().await;
    let task = client
        .create_index_on_bin(
            &AdminPolicy::default(),
            namespace,
            &set_name,
            "bin",
            &index_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("create index");
    task.wait_till_complete(None).await.unwrap();

    use futures::StreamExt;

    // Filter-less statement: a scan.
    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let rs = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
        .unwrap();
    let scanned = rs.into_stream().count().await;
    assert_eq!(scanned, RECORDS);

    // Secondary-index filter: a query.
    let mut stmt = Statement::new(namespace, &set_name, Bins::All);
    stmt.add_filter(Filter::range("bin", 0, RECORDS as i64));
    let rs = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
        .unwrap();
    let queried = rs.into_stream().count().await;
    assert_eq!(queried, RECORDS);

    let metrics = client.metrics();
    let agg = &metrics.cluster_aggregated;
    let scan_metric = agg
        .detailed_metric(namespace, CommandType::Scan)
        .expect("no detailed Scan metrics");
    let query_metric = agg
        .detailed_metric(namespace, CommandType::Query)
        .expect("no detailed Query metrics");

    // Every record carries a 4 KiB bin; the per-node command totals are
    // aggregated across the cluster, so the sum covers all of them.
    assert_bytes_received(scan_metric, "Scan", (RECORDS * PAYLOAD) as u64);
    assert_bytes_received(query_metric, "Query", (RECORDS * PAYLOAD) as u64);
    client.close().await.unwrap();
}
