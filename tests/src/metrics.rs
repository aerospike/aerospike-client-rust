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

use aerospike::query::PartitionFilter;
use aerospike::{
    as_bin, as_key, operations, BatchOperation, BatchPolicy, BatchReadPolicy, BatchWritePolicy,
    Bins, Client, CommandType, LatencyUnit, MetricsPolicy, QueryPolicy, ReadPolicy, Statement,
    WritePolicy,
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

// Operational metrics are off in the shipped default, so every test that
// expects per-command latency has to opt in explicitly.
fn operational_millis() -> MetricsPolicy {
    let mut policy = MetricsPolicy::millis();
    policy.operational_enabled = true;
    policy
}

// Microsecond counterpart of `operational_millis`.
fn operational_micros() -> MetricsPolicy {
    let mut policy = MetricsPolicy::micros();
    policy.operational_enabled = true;
    policy
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
    client.enable_metrics(operational_millis());

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

    // Microseconds: sub-millisecond latency is measurable, so the
    // recorded maximum is in the hundreds-or-more range rather than 0.
    let client = common::client().await;
    client.enable_metrics(operational_micros());
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
    client.close().await.unwrap();

    // Milliseconds: the same work, coarser buckets. The unit travels with the
    // snapshot so a consumer can tell the two apart.
    let client = common::client().await;
    client.enable_metrics(operational_millis());
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

    client.enable_metrics(operational_micros());
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
    client.enable_metrics(operational_millis());
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
    client.enable_metrics(operational_millis());

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
    client.enable_metrics(operational_millis());

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
    assert!(v.get("usage").is_some());
    assert_eq!(v["usage"], serde_json::json!({}));

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
async fn metrics_scan_histogram_records_filterless_query() {
    let client = common::client().await;
    client.enable_metrics(operational_millis());

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

    // Metrics enabled, but `Sampler::never()` means no command is ever
    // recorded even though collection is "on".
    let mut policy = operational_millis();
    policy.sampler = aerospike::Sampler::never();
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

#[aerospike_macro::test]
async fn metrics_one_latency_sample_per_successful_call() {
    let client = common::client().await;
    client.enable_metrics(operational_millis());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "one-sample");
    let mut wpolicy = WritePolicy::default();
    // A generous retry budget must not multiply samples on a single success.
    wpolicy.base_policy.max_retries = 5;
    client
        .put(&wpolicy, &key, &[as_bin!("bin", "value")])
        .await
        .unwrap();

    let count = client
        .metrics()
        .cluster_aggregated
        .command_histogram(CommandType::Put)
        .unwrap()
        .count();
    assert_eq!(
        count, 1,
        "one user call records one latency sample (not per retry slot)"
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
    client.enable_metrics(MetricsPolicy::default());

    // No traffic: let the minimum connections go idle and several tend cycles
    // run. A churning pool accumulates idle-drops here; a healthy pool does not.
    sleep(Duration::from_secs(6)).await;

    let metrics = client.metrics();
    let idle_dropped = metrics.cluster_aggregated.counters.connections_idle_dropped;
    assert_eq!(
        idle_dropped, 0,
        "min connections were reaped and recreated across tends (churn); \
         connections-idle-dropped={idle_dropped}"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_operational_disabled_records_no_latency() {
    let client = common::client().await;
    client.enable_metrics(MetricsPolicy::default());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    exercise_single_key(&client, namespace, &set_name).await;

    let agg = client.metrics().cluster_aggregated;
    for ct in [CommandType::Put, CommandType::Get, CommandType::Delete] {
        assert_eq!(
            agg.command_histogram(ct).unwrap().count(),
            0,
            "operational_enabled=false must not record latency for {ct:?}"
        );
    }
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_usage_counters() {
    let client = common::client().await;

    // Default policy leaves usage off: the hook is a no-op.
    client.enable_metrics(MetricsPolicy::default());
    client.record_usage("feature.api");
    assert!(
        client.metrics().usage.is_empty(),
        "usage_enabled=false must not record"
    );

    // Usage on, operational off: counters still increment (independent flags).
    let mut usage_only = MetricsPolicy::default();
    usage_only.usage_enabled = true;
    client.enable_metrics(usage_only);
    client.record_usage("feature.api");
    client.record_usage("feature.api");
    client.record_usage("feature.shape.batch");
    let usage = client.metrics().usage;
    assert_eq!(usage.get("feature.api").copied(), Some(2));
    assert_eq!(usage.get("feature.shape.batch").copied(), Some(1));

    client.disable_metrics();
    assert!(
        client.metrics().usage.is_empty(),
        "disabled metrics hide usage counters"
    );
    client.close().await.unwrap();
}

// Marker-bin sleep UDF for the retry tests below: the first execution on a
// missing record busy-waits, then creates the record; any later execution
// returns immediately. This turns "fail attempt 1, succeed attempt 2" into a
// deterministic sequence: the client's socket timer abandons the first
// attempt mid-sleep, the server still completes it and writes the marker,
// and the retry (scheduled after the server is done) finds the marker.
const SLEEP_ONCE_UDF: &str = r#"
function sleep_once(rec, ms)
    if aerospike:exists(rec) then
        return 0
    end
    local clock = os.clock
    local t0 = clock()
    while (clock() - t0) * 1000 < ms do end
    rec['done'] = 1
    aerospike:create(rec)
    return 1
end
"#;

async fn register_sleep_once_udf(client: &Client) {
    use aerospike::Task;
    let task = client
        .register_udf(
            &aerospike::AdminPolicy::default(),
            SLEEP_ONCE_UDF.as_bytes(),
            "metrics_sleep_once.lua",
            aerospike::UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();
}

// Write policy for a deterministic retried-then-successful UDF call.
// total_timeout must stay 0: a nonzero total makes the client send
// min(socket, total) as the server-side deadline, and the server's own abort
// would beat the client's socket timer. sleep_between_retries outlasts the
// server-side sleep so the retry starts after the marker record exists and
// is itself fast.
fn retried_udf_policy() -> WritePolicy {
    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.socket_timeout = 200;
    wpolicy.base_policy.total_timeout = 0;
    wpolicy.base_policy.max_retries = 1;
    wpolicy.base_policy.sleep_between_retries = 600;
    wpolicy
}

#[aerospike_macro::test]
async fn metrics_retries_inflate_recorded_latency() {
    let client = common::client().await;
    register_sleep_once_udf(&client).await;
    client.enable_metrics(operational_millis());

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "retry-inflates");

    // Attempt 1 abandons at ~200ms (server sleeps 400ms, completes, writes
    // the marker); the retry fires at ~800ms and returns in a few ms.
    let res = client
        .execute_udf(
            &retried_udf_policy(),
            &key,
            "metrics_sleep_once",
            "sleep_once",
            Some(&[aerospike::as_val!(400)]),
        )
        .await;
    res.unwrap();

    let snapshot = client.metrics();
    let hist = snapshot
        .cluster_aggregated
        .command_histogram(CommandType::Udf)
        .unwrap();
    // One user call = one sample, even though two attempts ran.
    assert_eq!(hist.count(), 1, "retried call must record a single sample");
    // The sample spans the whole call (attempt 1 + backoff + attempt 2),
    // not just the successful attempt: the retry's own latency is a few
    // milliseconds, while the call took >= socket_timeout + backoff.
    assert!(
        hist.max() >= 700,
        "latency sample must span retries: recorded {}ms, expected >= 700ms",
        hist.max()
    );
    // With the millisecond range layout (7 columns, shift 1) that lands in
    // the last bucket (>32ms); a timer restarted per attempt would land in
    // the first ones.
    assert_eq!(hist.buckets().last().copied(), Some(1));
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn metrics_sampler_decision_survives_retries() {
    // The complementary half of the retry contract: the per-call sample
    // decision is made once at API entry and never re-rolled on retries.
    // With Sampler::never a retried-then-successful call must record
    // nothing — a per-attempt re-roll could only be observed through a
    // probabilistic sampler, but a decision flipping to "sampled" mid-call
    // would record a partial latency, which the inflation test's whole-call
    // assertion also guards against.
    let client = common::client().await;
    register_sleep_once_udf(&client).await;
    let mut policy = operational_millis();
    policy.sampler = aerospike::Sampler::never();
    client.enable_metrics(policy);

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "never-sampled-retry");

    client
        .execute_udf(
            &retried_udf_policy(),
            &key,
            "metrics_sleep_once",
            "sleep_once",
            Some(&[aerospike::as_val!(400)]),
        )
        .await
        .unwrap();

    let snapshot = client.metrics();
    let no_samples = snapshot
        .cluster_aggregated
        .command_histogram(CommandType::Udf)
        .map_or(true, |h| h.count() == 0);
    assert!(no_samples, "Sampler::never must hold across retries");
    client.close().await.unwrap();
}
