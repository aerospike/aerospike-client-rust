// Copyright 2014-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Per-node statistics gathered by the metrics subsystem.
//!
//! [`NodeMetrics`] is the live, concurrently-updated structure held by each
//! [`crate::cluster::Node`]. It is periodically drained into a
//! [`NodeMetricsSnapshot`] (an owned, serializable copy) which is aggregated into
//! the cluster-wide view.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use super::histogram::SyncHistogram;
use super::policy::MetricsPolicy;
use crate::sampler::Sampler;
use crate::xor_shift::XorShift;
use crate::ResultCode;

/// Encodes the policy's optional sampler into the `(range, threshold)` pair
/// stored in two atomics. `range == 0` is the sentinel for "no sampling"
/// (`None`, or a degenerate zero-range sampler).
const fn encode_sampler(sampler: Option<Sampler>) -> (u64, u64) {
    match sampler {
        Some(s) => (s.range, s.threshold),
        None => (0, 0),
    }
}

#[cfg(feature = "serialization")]
use serde::Serialize;
#[cfg(feature = "serialization")]
use serde::Serializer;

use super::policy::Labels;

/// Number of distinct command types tracked.
pub const COMMAND_TYPE_COUNT: usize = 12;

/// Logical command category a statistic is attributed to.
///
/// The discriminants are stable (`None` = 0 .. `BatchWrite` = 11) so exported
/// metrics line up across clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    /// Uncategorized.
    None = 0,
    /// Single-record read.
    Get = 1,
    /// Single-record header-only read.
    GetHeader = 2,
    /// Existence check.
    Exists = 3,
    /// Single-record write.
    Put = 4,
    /// Single-record delete.
    Delete = 5,
    /// Operate (multi-op) command.
    Operate = 6,
    /// Query.
    Query = 7,
    /// Scan.
    Scan = 8,
    /// UDF execution.
    Udf = 9,
    /// Read-only batch command.
    BatchRead = 10,
    /// Batch command containing writes.
    BatchWrite = 11,
}

impl CommandType {
    /// Array slot index for this command type.
    #[must_use]
    pub const fn index(self) -> usize {
        self as usize
    }

    /// Builds a command type from its array slot index.
    #[must_use]
    pub const fn from_index(i: usize) -> CommandType {
        match i {
            1 => CommandType::Get,
            2 => CommandType::GetHeader,
            3 => CommandType::Exists,
            4 => CommandType::Put,
            5 => CommandType::Delete,
            6 => CommandType::Operate,
            7 => CommandType::Query,
            8 => CommandType::Scan,
            9 => CommandType::Udf,
            10 => CommandType::BatchRead,
            11 => CommandType::BatchWrite,
            _ => CommandType::None,
        }
    }

    /// Display name for this command type.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            CommandType::None => "None",
            CommandType::Get => "Get",
            CommandType::GetHeader => "GetHeader",
            CommandType::Exists => "Exists",
            CommandType::Put => "Put",
            CommandType::Delete => "Delete",
            CommandType::Operate => "Operate",
            CommandType::Query => "Query",
            CommandType::Scan => "Scan",
            CommandType::Udf => "UDF",
            CommandType::BatchRead => "BatchRead",
            CommandType::BatchWrite => "BatchWrite",
        }
    }
}

/// Detailed per-namespace, per-command-type histograms.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct CommandMetric {
    /// Time spent acquiring a connection from the pool.
    #[cfg_attr(feature = "serialization", serde(rename = "connection-aq"))]
    pub connection_aq: SyncHistogram,
    /// Round-trip command latency.
    pub latency: SyncHistogram,
    /// Time spent parsing the response.
    pub parsing: SyncHistogram,
    /// Bytes written to the wire.
    #[cfg_attr(feature = "serialization", serde(rename = "bytes-sent"))]
    pub bytes_sent: SyncHistogram,
    /// Bytes read from the wire.
    #[cfg_attr(feature = "serialization", serde(rename = "bytes-received"))]
    pub bytes_received: SyncHistogram,
}

impl CommandMetric {
    fn new(policy: &MetricsPolicy) -> Self {
        let mk =
            || SyncHistogram::new(policy.histogram_type, policy.base(), policy.latency_columns);
        CommandMetric {
            connection_aq: mk(),
            latency: mk(),
            parsing: mk(),
            bytes_sent: mk(),
            bytes_received: mk(),
        }
    }
}

type MetricSlots = Box<[Option<CommandMetric>; COMMAND_TYPE_COUNT]>;
type LiveResultCodeSlots = Box<[Option<Mutex<HashMap<ResultCode, u64>>>; COMMAND_TYPE_COUNT]>;
type ResultCodeSlots = Box<[Option<HashMap<ResultCode, u64>>; COMMAND_TYPE_COUNT]>;

fn empty_metric_slots() -> MetricSlots {
    Box::new(std::array::from_fn(|_| None))
}

fn empty_live_rc_slots() -> LiveResultCodeSlots {
    Box::new(std::array::from_fn(|_| None))
}

fn empty_rc_slots() -> ResultCodeSlots {
    Box::new(std::array::from_fn(|_| None))
}

macro_rules! define_counters {
    ($($field:ident => $json:literal),+ $(,)?) => {
        /// Live, atomic counters.
        #[derive(Debug, Default)]
        pub struct LiveCounters {
            $(pub(crate) $field: AtomicU64,)+
        }

        impl LiveCounters {
            fn snapshot_and_reset(&self) -> Counters {
                Counters {
                    $($field: self.$field.swap(0, Ordering::Relaxed),)+
                }
            }
        }

        /// Owned snapshot of the counters.
        #[derive(Debug, Clone, Default)]
        #[cfg_attr(feature = "serialization", derive(Serialize))]
        pub struct Counters {
            $(
                #[doc = concat!("Counter exported as `", $json, "`.")]
                #[cfg_attr(feature = "serialization", serde(rename = $json))]
                pub $field: u64,
            )+
        }

        impl Counters {
            fn add(&mut self, other: &Counters) {
                $(self.$field += other.$field;)+
            }
        }
    };
}

define_counters! {
    connections_attempts => "connections-attempts",
    connections_successful => "connections-successful",
    connections_failed => "connections-failed",
    connections_timeout_errors => "connections-error-timeout",
    connections_other_errors => "connections-error-other",
    circuit_breaker_hits => "circuit-breaker-hits",
    connections_pool_empty => "connections-pool-empty",
    connections_pool_overflow => "connections-pool-overflow",
    connections_idle_dropped => "connections-idle-dropped",
    connections_open => "open-connections",
    connections_closed => "closed-connections",
    connections_recovered => "connections-recovered",
    tends_total => "tends-total",
    tends_successful => "tends-successful",
    tends_failed => "tends-failed",
    partition_map_updates => "partition-map-updates",
    node_added => "node-added-count",
    node_removed => "node-removed-count",
    transaction_retry_count => "transaction-retry-count",
    transaction_error_count => "transaction-error-count",
}

/// The 11 per-command-type latency histograms, in declaration order.
macro_rules! command_histograms {
    () => {
        [
            ("get-metrics", CommandType::Get),
            ("get-header-metrics", CommandType::GetHeader),
            ("exists-metrics", CommandType::Exists),
            ("put-metrics", CommandType::Put),
            ("delete-metrics", CommandType::Delete),
            ("operate-metrics", CommandType::Operate),
            ("query-metrics", CommandType::Query),
            ("scan-metrics", CommandType::Scan),
            ("udf-metrics", CommandType::Udf),
            ("batch-read-metrics", CommandType::BatchRead),
            ("batch-write-metrics", CommandType::BatchWrite),
        ]
    };
}

/// Live, concurrently-updated per-node statistics.
#[derive(Debug)]
pub struct NodeMetrics {
    policy: MetricsPolicy,
    /// Live sampler, stored lock-free as a `(range, threshold)` pair and
    /// refreshed by [`NodeMetrics::reshape`] whenever the policy changes (e.g.
    /// `enable_metrics`) so a node created before metrics were enabled still
    /// picks up the configured sampler. `range == 0` means no sampling.
    ///
    /// The two values are written/read independently with relaxed ordering; a
    /// reader racing a `reshape` may briefly observe a mismatched pair and
    /// mis-sample a single command, which is harmless given how rarely the
    /// policy changes.
    sampler_range: AtomicU64,
    sampler_threshold: AtomicU64,
    /// Whether collection is currently enabled. Gates the connection-lifecycle
    /// counters that are recorded outside the command hot-path.
    enabled: AtomicBool,
    /// Atomic scalar counters.
    pub(crate) counters: LiveCounters,
    /// Per-command-type latency histograms, indexed by [`CommandType::index`].
    /// Slot 0 (`None`) is unused but kept for index alignment.
    command_metrics: [Option<SyncHistogram>; COMMAND_TYPE_COUNT],
    detailed_metrics: RwLock<HashMap<String, MetricSlots>>,
    result_code_counts: RwLock<HashMap<String, LiveResultCodeSlots>>,
}

impl NodeMetrics {
    /// Creates a fresh set of node statistics shaped by `policy`.
    #[must_use]
    pub fn new(policy: MetricsPolicy) -> Self {
        let mut command_metrics: [Option<SyncHistogram>; COMMAND_TYPE_COUNT] =
            std::array::from_fn(|_| None);
        for (_, ct) in command_histograms!() {
            command_metrics[ct.index()] = Some(SyncHistogram::new(
                policy.histogram_type,
                policy.base(),
                policy.latency_columns,
            ));
        }
        let (range, threshold) = encode_sampler(policy.sampler);
        NodeMetrics {
            policy,
            sampler_range: AtomicU64::new(range),
            sampler_threshold: AtomicU64::new(threshold),
            enabled: AtomicBool::new(false),
            counters: LiveCounters::default(),
            command_metrics,
            detailed_metrics: RwLock::new(HashMap::new()),
            result_code_counts: RwLock::new(HashMap::new()),
        }
    }

    /// Returns whether collection is enabled for this node.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Enables or disables collection for this node.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Returns whether the next command should be recorded: collection must be
    /// enabled **and** the configured sampler must select it (drawing from
    /// `rand`, typically the serving connection's generator). A `range` of 0
    /// (no sampler / `None`) records nothing.
    pub fn should_sample(&self, rand: &mut XorShift) -> bool {
        if !self.is_enabled() {
            return false;
        }
        let range = self.sampler_range.load(Ordering::Relaxed);
        if range == 0 {
            return false;
        }
        let threshold = self.sampler_threshold.load(Ordering::Relaxed);
        rand.next_u64() % range < threshold
    }

    /// Records a connection-open attempt.
    pub fn incr_connections_attempt(&self) {
        if self.is_enabled() {
            self.counters
                .connections_attempts
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a successfully-opened connection.
    pub fn incr_connections_successful(&self) {
        if self.is_enabled() {
            self.counters
                .connections_successful
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a failed connection attempt, classifying timeouts separately.
    pub fn incr_connections_failed(&self, timeout: bool) {
        if self.is_enabled() {
            self.counters
                .connections_failed
                .fetch_add(1, Ordering::Relaxed);
            if timeout {
                self.counters
                    .connections_timeout_errors
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.counters
                    .connections_other_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Records a closed connection.
    pub fn incr_connections_closed(&self) {
        if self.is_enabled() {
            self.counters
                .connections_closed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a poll against an empty connection pool.
    pub fn incr_connections_pool_empty(&self) {
        if self.is_enabled() {
            self.counters
                .connections_pool_empty
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a connection offered to a full pool and closed.
    pub fn incr_connections_pool_overflow(&self) {
        if self.is_enabled() {
            self.counters
                .connections_pool_overflow
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records an idle connection that was dropped.
    pub fn incr_connections_idle_dropped(&self) {
        if self.is_enabled() {
            self.counters
                .connections_idle_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a command retry.
    pub fn incr_transaction_retry(&self) {
        if self.is_enabled() {
            self.counters
                .transaction_retry_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a command that ultimately failed.
    pub fn incr_transaction_error(&self) {
        if self.is_enabled() {
            self.counters
                .transaction_error_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a tend attempt against this node, classifying success/failure.
    pub fn incr_tend(&self, success: bool) {
        if self.is_enabled() {
            self.counters.tends_total.fetch_add(1, Ordering::Relaxed);
            if success {
                self.counters
                    .tends_successful
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.counters.tends_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Records a partition-map update applied from this node.
    pub fn incr_partition_map_update(&self) {
        if self.is_enabled() {
            self.counters
                .partition_map_updates
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records that this node was added to the cluster.
    pub fn incr_node_added(&self) {
        if self.is_enabled() {
            self.counters.node_added.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records that this node was removed from the cluster.
    pub fn incr_node_removed(&self) {
        if self.is_enabled() {
            self.counters.node_removed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records the elapsed time (microseconds) of a completed command against
    /// its per-command-type histogram.
    pub fn record_command(&self, ct: CommandType, micros: u64) {
        if let Some(h) = &self.command_metrics[ct.index()] {
            h.add(micros);
        }
    }

    /// Records connection-acquire time for the detailed per-namespace metrics.
    pub fn record_connection_aq(&self, namespace: &str, ct: CommandType, micros: u64) {
        self.with_command_metric(namespace, ct, |cm| cm.connection_aq.add(micros));
    }

    /// Records write latency and bytes-sent for the detailed metrics.
    pub fn record_write(&self, namespace: &str, ct: CommandType, bytes_sent: u64, latency: u64) {
        self.with_command_metric(namespace, ct, |cm| {
            cm.bytes_sent.add(bytes_sent);
            cm.latency.add(latency);
        });
    }

    /// Records parse time and bytes-received for the detailed metrics.
    pub fn record_parse(
        &self,
        namespace: &str,
        ct: CommandType,
        parsing: u64,
        bytes_received: u64,
    ) {
        self.with_command_metric(namespace, ct, |cm| {
            cm.parsing.add(parsing);
            cm.bytes_received.add(bytes_received);
        });
    }

    /// Increments the count for a `(namespace, command type, result code)`
    /// triple.
    pub fn record_result_code(&self, namespace: &str, ct: CommandType, rc: ResultCode) {
        let idx = ct.index();
        // Fast path: namespace + slot already present.
        {
            let map = self.result_code_counts.read().unwrap();
            if let Some(slots) = map.get(namespace) {
                if let Some(counts) = &slots[idx] {
                    *counts.lock().unwrap().entry(rc).or_insert(0) += 1;
                    return;
                }
            }
        }
        // Slow path: insert namespace/slot.
        let mut map = self.result_code_counts.write().unwrap();
        let slots = map
            .entry(namespace.to_string())
            .or_insert_with(empty_live_rc_slots);
        if slots[idx].is_none() {
            slots[idx] = Some(Mutex::new(HashMap::new()));
        }
        *slots[idx]
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .entry(rc)
            .or_insert(0) += 1;
    }

    fn with_command_metric<F: FnOnce(&CommandMetric)>(
        &self,
        namespace: &str,
        ct: CommandType,
        f: F,
    ) {
        if namespace.is_empty() {
            return;
        }
        let idx = ct.index();
        {
            let map = self.detailed_metrics.read().unwrap();
            if let Some(slots) = map.get(namespace) {
                if let Some(cm) = &slots[idx] {
                    f(cm);
                    return;
                }
            }
        }
        let mut map = self.detailed_metrics.write().unwrap();
        let slots = map
            .entry(namespace.to_string())
            .or_insert_with(empty_metric_slots);
        if slots[idx].is_none() {
            slots[idx] = Some(CommandMetric::new(&self.policy));
        }
        f(slots[idx].as_ref().unwrap());
    }

    /// Re-applies a (possibly changed) policy, resetting histograms whose shape
    /// changed.
    pub fn reshape(&self, policy: &MetricsPolicy) {
        // Pick up the (possibly new) sampler from the applied policy.
        let (range, threshold) = encode_sampler(policy.sampler);
        self.sampler_threshold.store(threshold, Ordering::Relaxed);
        self.sampler_range.store(range, Ordering::Relaxed);
        for h in self.command_metrics.iter().flatten() {
            h.reshape(policy.histogram_type, policy.base(), policy.latency_columns);
        }
        for slots in self.detailed_metrics.read().unwrap().values() {
            for cm in slots.iter().flatten() {
                for h in [
                    &cm.connection_aq,
                    &cm.latency,
                    &cm.parsing,
                    &cm.bytes_sent,
                    &cm.bytes_received,
                ] {
                    h.reshape(policy.histogram_type, policy.base(), policy.latency_columns);
                }
            }
        }
    }

    /// Drains the live counters and histograms into an owned snapshot, resetting
    /// the live values.
    #[must_use]
    pub fn get_and_reset(&self) -> NodeMetricsSnapshot {
        let mut snapshot = NodeMetricsSnapshot::new(self.policy.clone());
        snapshot.counters = self.counters.snapshot_and_reset();

        for (name, ct) in command_histograms!() {
            if let Some(h) = &self.command_metrics[ct.index()] {
                snapshot.set_command_histogram(name, h.clone_and_reset());
            }
        }

        // Drain detailed metrics.
        for (ns, slots) in self.detailed_metrics.read().unwrap().iter() {
            let mut tgt = empty_metric_slots();
            for (i, slot) in slots.iter().enumerate() {
                if let Some(src) = slot {
                    tgt[i] = Some(CommandMetric {
                        connection_aq: src.connection_aq.clone_and_reset(),
                        latency: src.latency.clone_and_reset(),
                        parsing: src.parsing.clone_and_reset(),
                        bytes_sent: src.bytes_sent.clone_and_reset(),
                        bytes_received: src.bytes_received.clone_and_reset(),
                    });
                }
            }
            snapshot.detailed_metrics.insert(ns.clone(), tgt);
        }

        // Drain result-code counts (clone then zero the live entries).
        for (ns, slots) in self.result_code_counts.read().unwrap().iter() {
            let mut tgt = empty_rc_slots();
            for (i, slot) in slots.iter().enumerate() {
                if let Some(counts) = slot {
                    let mut guard = counts.lock().unwrap();
                    tgt[i] = Some(guard.clone());
                    for v in guard.values_mut() {
                        *v = 0;
                    }
                }
            }
            snapshot.result_code_counts.insert(ns.clone(), tgt);
        }

        snapshot
    }
}

/// Owned, serializable snapshot of a node's statistics. Used for aggregation
/// across tends and for the cluster-wide view returned to the user.
///
/// JSON field names are stable and shared across Aerospike clients.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct NodeMetricsSnapshot {
    #[cfg_attr(feature = "serialization", serde(skip))]
    policy: MetricsPolicy,

    #[cfg_attr(
        feature = "serialization",
        serde(rename = "labels", skip_serializing_if = "labels_empty")
    )]
    /// Labels attached to this snapshot (populated for the cluster-aggregated view).
    pub labels: Labels,

    #[cfg_attr(feature = "serialization", serde(flatten))]
    /// Scalar counters.
    pub counters: Counters,

    #[cfg_attr(feature = "serialization", serde(rename = "get-metrics"))]
    get_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "get-header-metrics"))]
    get_header_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "exists-metrics"))]
    exists_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "put-metrics"))]
    put_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "delete-metrics"))]
    delete_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "operate-metrics"))]
    operate_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "query-metrics"))]
    query_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "scan-metrics"))]
    scan_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "udf-metrics"))]
    udf_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "batch-read-metrics"))]
    batch_read_metrics: SyncHistogram,
    #[cfg_attr(feature = "serialization", serde(rename = "batch-write-metrics"))]
    batch_write_metrics: SyncHistogram,

    #[cfg_attr(
        feature = "serialization",
        serde(
            rename = "detailed-resultcode-counts",
            serialize_with = "serialize_result_codes"
        )
    )]
    result_code_counts: HashMap<String, ResultCodeSlots>,

    #[cfg_attr(
        feature = "serialization",
        serde(rename = "detailed-metrics", serialize_with = "serialize_detailed")
    )]
    detailed_metrics: HashMap<String, MetricSlots>,
}

impl NodeMetricsSnapshot {
    /// Creates an empty snapshot shaped by `policy`.
    #[must_use]
    pub fn new(policy: MetricsPolicy) -> Self {
        let mk =
            || SyncHistogram::new(policy.histogram_type, policy.base(), policy.latency_columns);
        NodeMetricsSnapshot {
            labels: Labels::new(),
            counters: Counters::default(),
            get_metrics: mk(),
            get_header_metrics: mk(),
            exists_metrics: mk(),
            put_metrics: mk(),
            delete_metrics: mk(),
            operate_metrics: mk(),
            query_metrics: mk(),
            scan_metrics: mk(),
            udf_metrics: mk(),
            batch_read_metrics: mk(),
            batch_write_metrics: mk(),
            result_code_counts: HashMap::new(),
            detailed_metrics: HashMap::new(),
            policy,
        }
    }

    fn command_histogram_mut(&mut self, name: &str) -> &mut SyncHistogram {
        match name {
            "get-metrics" => &mut self.get_metrics,
            "get-header-metrics" => &mut self.get_header_metrics,
            "exists-metrics" => &mut self.exists_metrics,
            "put-metrics" => &mut self.put_metrics,
            "delete-metrics" => &mut self.delete_metrics,
            "operate-metrics" => &mut self.operate_metrics,
            "query-metrics" => &mut self.query_metrics,
            "scan-metrics" => &mut self.scan_metrics,
            "udf-metrics" => &mut self.udf_metrics,
            "batch-read-metrics" => &mut self.batch_read_metrics,
            "batch-write-metrics" => &mut self.batch_write_metrics,
            other => unreachable!("unknown command histogram: {other}"),
        }
    }

    fn set_command_histogram(&mut self, name: &str, h: SyncHistogram) {
        *self.command_histogram_mut(name) = h;
    }

    fn command_histograms(&self) -> [(&'static str, &SyncHistogram); 11] {
        [
            ("get-metrics", &self.get_metrics),
            ("get-header-metrics", &self.get_header_metrics),
            ("exists-metrics", &self.exists_metrics),
            ("put-metrics", &self.put_metrics),
            ("delete-metrics", &self.delete_metrics),
            ("operate-metrics", &self.operate_metrics),
            ("query-metrics", &self.query_metrics),
            ("scan-metrics", &self.scan_metrics),
            ("udf-metrics", &self.udf_metrics),
            ("batch-read-metrics", &self.batch_read_metrics),
            ("batch-write-metrics", &self.batch_write_metrics),
        ]
    }

    /// Returns the per-command-type latency histogram for `ct`, or `None` for
    /// command types without a dedicated histogram ([`CommandType::None`]).
    #[must_use]
    pub fn command_histogram(&self, ct: CommandType) -> Option<&SyncHistogram> {
        Some(match ct {
            CommandType::Get => &self.get_metrics,
            CommandType::GetHeader => &self.get_header_metrics,
            CommandType::Exists => &self.exists_metrics,
            CommandType::Put => &self.put_metrics,
            CommandType::Delete => &self.delete_metrics,
            CommandType::Operate => &self.operate_metrics,
            CommandType::Query => &self.query_metrics,
            CommandType::Scan => &self.scan_metrics,
            CommandType::Udf => &self.udf_metrics,
            CommandType::BatchRead => &self.batch_read_metrics,
            CommandType::BatchWrite => &self.batch_write_metrics,
            CommandType::None => return None,
        })
    }

    /// Returns the detailed per-namespace, per-command-type metric, if any data
    /// was recorded for that `(namespace, command type)` pair.
    #[must_use]
    pub fn detailed_metric(&self, namespace: &str, ct: CommandType) -> Option<&CommandMetric> {
        self.detailed_metrics
            .get(namespace)
            .and_then(|slots| slots[ct.index()].as_ref())
    }

    /// Returns the recorded count for a `(namespace, command type, result code)`
    /// triple (0 if none recorded).
    #[must_use]
    pub fn result_code_count(&self, namespace: &str, ct: CommandType, rc: ResultCode) -> u64 {
        self.result_code_counts
            .get(namespace)
            .and_then(|slots| slots[ct.index()].as_ref())
            .and_then(|counts| counts.get(&rc).copied())
            .unwrap_or(0)
    }

    /// Namespaces that have detailed metrics recorded.
    #[must_use]
    pub fn detailed_namespaces(&self) -> Vec<&str> {
        self.detailed_metrics.keys().map(String::as_str).collect()
    }

    /// Sets the labels attached to this snapshot.
    pub fn set_labels(&mut self, labels: Labels) {
        self.labels = labels;
    }

    /// Overrides the open-connections gauge (set from the node's live count).
    pub fn set_open_connections(&mut self, open: u64) {
        self.counters.connections_open = open;
    }

    /// Open-connections gauge value.
    #[must_use]
    pub fn open_connections(&self) -> u64 {
        self.counters.connections_open
    }

    /// Merges another snapshot into this one.
    pub fn aggregate(&mut self, other: &NodeMetricsSnapshot) {
        self.counters.add(&other.counters);

        // Merge per-command-type histograms by name (shape is identical).
        let names: [&str; 11] = [
            "get-metrics",
            "get-header-metrics",
            "exists-metrics",
            "put-metrics",
            "delete-metrics",
            "operate-metrics",
            "query-metrics",
            "scan-metrics",
            "udf-metrics",
            "batch-read-metrics",
            "batch-write-metrics",
        ];
        for name in names {
            let src = other.command_histogram_ref(name).clone();
            self.command_histogram_mut(name).merge(&src);
        }

        // Merge detailed metrics.
        for (ns, slots) in &other.detailed_metrics {
            let tgt = self
                .detailed_metrics
                .entry(ns.clone())
                .or_insert_with(empty_metric_slots);
            for (i, slot) in slots.iter().enumerate() {
                if let Some(src) = slot {
                    let dst = tgt[i].get_or_insert_with(|| CommandMetric::new(&self.policy));
                    dst.connection_aq.merge(&src.connection_aq);
                    dst.latency.merge(&src.latency);
                    dst.parsing.merge(&src.parsing);
                    dst.bytes_sent.merge(&src.bytes_sent);
                    dst.bytes_received.merge(&src.bytes_received);
                }
            }
        }

        // Merge result-code counts.
        for (ns, slots) in &other.result_code_counts {
            let tgt = self
                .result_code_counts
                .entry(ns.clone())
                .or_insert_with(empty_rc_slots);
            for (i, slot) in slots.iter().enumerate() {
                if let Some(src) = slot {
                    let dst = tgt[i].get_or_insert_with(HashMap::new);
                    for (rc, count) in src {
                        *dst.entry(*rc).or_insert(0) += count;
                    }
                }
            }
        }
    }

    fn command_histogram_ref(&self, name: &str) -> &SyncHistogram {
        self.command_histograms()
            .into_iter()
            .find(|(n, _)| *n == name)
            .map(|(_, h)| h)
            .expect("unknown command histogram")
    }
}

#[cfg(feature = "serialization")]
fn labels_empty(labels: &Labels) -> bool {
    labels.0.is_empty()
}

#[cfg(feature = "serialization")]
fn serialize_detailed<S>(
    map: &HashMap<String, MetricSlots>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;
    let mut out = serializer.serialize_map(None)?;
    for (ns, slots) in map {
        let inner: HashMap<&'static str, &CommandMetric> = slots
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| {
                slot.as_ref()
                    .map(|cm| (CommandType::from_index(i).as_str(), cm))
            })
            .collect();
        if !inner.is_empty() {
            out.serialize_entry(ns, &inner)?;
        }
    }
    out.end()
}

#[cfg(feature = "serialization")]
fn serialize_result_codes<S>(
    map: &HashMap<String, ResultCodeSlots>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;
    let mut out = serializer.serialize_map(None)?;
    for (ns, slots) in map {
        let mut inner: HashMap<&'static str, HashMap<String, u64>> = HashMap::new();
        for (i, slot) in slots.iter().enumerate() {
            if let Some(counts) = slot {
                if counts.is_empty() {
                    continue;
                }
                let rc_map: HashMap<String, u64> = counts
                    .iter()
                    .map(|(rc, c)| (rc.into_string(), *c))
                    .collect();
                inner.insert(CommandType::from_index(i).as_str(), rc_map);
            }
        }
        if !inner.is_empty() {
            out.serialize_entry(ns, &inner)?;
        }
    }
    out.end()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_type_names_and_indices() {
        assert_eq!(CommandType::None.index(), 0);
        assert_eq!(CommandType::BatchWrite.index(), 11);
        assert_eq!(CommandType::Udf.as_str(), "UDF");
        assert_eq!(CommandType::from_index(7), CommandType::Query);
        assert_eq!(CommandType::from_index(99), CommandType::None);
    }

    #[test]
    fn record_command_targets_right_histogram() {
        let metrics = NodeMetrics::new(MetricsPolicy::default());
        metrics.record_command(CommandType::Put, 100);
        metrics.record_command(CommandType::Put, 200);
        metrics.record_command(CommandType::Get, 50);
        let snap = metrics.get_and_reset();
        assert_eq!(snap.put_metrics.count(), 2);
        assert_eq!(snap.get_metrics.count(), 1);
        assert_eq!(snap.exists_metrics.count(), 0);
        // live values were reset
        let snap2 = metrics.get_and_reset();
        assert_eq!(snap2.put_metrics.count(), 0);
    }

    #[test]
    fn should_sample_respects_enabled_and_sampler() {
        use crate::sampler::Sampler;
        let mut rng = XorShift::with_seed(1, 2);

        // Default policy samples always, but collection is off by default.
        let m = NodeMetrics::new(MetricsPolicy::default());
        assert!(!m.should_sample(&mut rng), "disabled must never sample");
        m.set_enabled(true);
        assert!(m.should_sample(&mut rng), "enabled + Always must sample");

        // `None` (no sampling) records nothing even while enabled.
        let policy = MetricsPolicy {
            sampler: None,
            ..MetricsPolicy::default()
        };
        let m2 = NodeMetrics::new(policy);
        m2.set_enabled(true);
        assert!(!m2.should_sample(&mut rng), "None must not sample");

        // A reshape to a sampling policy is picked up live (lock-free).
        m2.reshape(&MetricsPolicy {
            sampler: Some(Sampler::all()),
            ..MetricsPolicy::default()
        });
        assert!(
            m2.should_sample(&mut rng),
            "reshape must update the sampler"
        );
    }

    #[test]
    fn counters_reset_on_drain_and_aggregate_sums() {
        let metrics = NodeMetrics::new(MetricsPolicy::default());
        metrics
            .counters
            .connections_attempts
            .fetch_add(3, Ordering::Relaxed);
        metrics.counters.tends_total.fetch_add(1, Ordering::Relaxed);
        let a = metrics.get_and_reset();
        assert_eq!(a.counters.connections_attempts, 3);
        // drained
        let b = metrics.get_and_reset();
        assert_eq!(b.counters.connections_attempts, 0);

        let mut agg = NodeMetricsSnapshot::new(MetricsPolicy::default());
        agg.aggregate(&a);
        agg.aggregate(&a);
        assert_eq!(agg.counters.connections_attempts, 6);
        assert_eq!(agg.counters.tends_total, 2);
    }

    #[test]
    fn detailed_and_result_codes_roundtrip() {
        let metrics = NodeMetrics::new(MetricsPolicy::default());
        metrics.record_write("test", CommandType::Put, 128, 250);
        metrics.record_parse("test", CommandType::Put, 30, 64);
        metrics.record_result_code("test", CommandType::Put, ResultCode::KeyNotFoundError);
        metrics.record_result_code("test", CommandType::Put, ResultCode::KeyNotFoundError);

        let snap = metrics.get_and_reset();
        let slots = snap.detailed_metrics.get("test").unwrap();
        let cm = slots[CommandType::Put.index()].as_ref().unwrap();
        assert_eq!(cm.bytes_sent.count(), 1);
        assert_eq!(cm.latency.count(), 1);
        assert_eq!(cm.parsing.count(), 1);

        let rc_slots = snap.result_code_counts.get("test").unwrap();
        let counts = rc_slots[CommandType::Put.index()].as_ref().unwrap();
        assert_eq!(counts.get(&ResultCode::KeyNotFoundError), Some(&2));
    }

    #[test]
    fn command_type_all_variants_round_trip() {
        let all = [
            CommandType::None,
            CommandType::Get,
            CommandType::GetHeader,
            CommandType::Exists,
            CommandType::Put,
            CommandType::Delete,
            CommandType::Operate,
            CommandType::Query,
            CommandType::Scan,
            CommandType::Udf,
            CommandType::BatchRead,
            CommandType::BatchWrite,
        ];
        assert_eq!(all.len(), COMMAND_TYPE_COUNT);
        for (i, ct) in all.into_iter().enumerate() {
            assert_eq!(ct.index(), i);
            assert_eq!(CommandType::from_index(i), ct);
            assert!(!ct.as_str().is_empty());
        }
        // Stable display names shared across Aerospike clients.
        assert_eq!(CommandType::None.as_str(), "None");
        assert_eq!(CommandType::GetHeader.as_str(), "GetHeader");
        assert_eq!(CommandType::BatchRead.as_str(), "BatchRead");
        assert_eq!(CommandType::BatchWrite.as_str(), "BatchWrite");
    }

    #[test]
    fn disabled_node_does_not_record_gated_counters() {
        let metrics = NodeMetrics::new(MetricsPolicy::default());
        // disabled by default
        assert!(!metrics.is_enabled());
        metrics.incr_connections_attempt();
        metrics.incr_tend(true);
        metrics.incr_transaction_retry();
        let snap = metrics.get_and_reset();
        assert_eq!(snap.counters.connections_attempts, 0);
        assert_eq!(snap.counters.tends_total, 0);
        assert_eq!(snap.counters.transaction_retry_count, 0);

        metrics.set_enabled(true);
        metrics.incr_connections_attempt();
        metrics.incr_connections_successful();
        metrics.incr_tend(false);
        let snap = metrics.get_and_reset();
        assert_eq!(snap.counters.connections_attempts, 1);
        assert_eq!(snap.counters.connections_successful, 1);
        assert_eq!(snap.counters.tends_total, 1);
        assert_eq!(snap.counters.tends_failed, 1);
        assert_eq!(snap.counters.tends_successful, 0);
    }

    #[test]
    fn aggregate_merges_detailed_metrics_and_result_codes() {
        let policy = MetricsPolicy::default();
        let metrics = NodeMetrics::new(policy.clone());
        metrics.set_enabled(true);

        // Two namespaces, two command types, repeated result codes.
        metrics.record_write("ns1", CommandType::Put, 100, 200);
        metrics.record_parse("ns1", CommandType::Put, 10, 50);
        metrics.record_result_code("ns1", CommandType::Put, ResultCode::Ok);
        metrics.record_result_code("ns2", CommandType::Get, ResultCode::KeyNotFoundError);
        let a = metrics.get_and_reset();

        metrics.record_write("ns1", CommandType::Put, 300, 400);
        metrics.record_result_code("ns1", CommandType::Put, ResultCode::Ok);
        let b = metrics.get_and_reset();

        let mut agg = NodeMetricsSnapshot::new(policy);
        agg.aggregate(&a);
        agg.aggregate(&b);

        // Detailed metric merges counts across both snapshots.
        let cm = agg.detailed_metric("ns1", CommandType::Put).unwrap();
        assert_eq!(cm.bytes_sent.count(), 2);
        assert_eq!(cm.latency.count(), 2);
        assert_eq!(cm.bytes_sent.max(), 300);

        // Result codes merge per (namespace, command, code).
        assert_eq!(
            agg.result_code_count("ns1", CommandType::Put, ResultCode::Ok),
            2
        );
        assert_eq!(
            agg.result_code_count("ns2", CommandType::Get, ResultCode::KeyNotFoundError),
            1
        );
        // Missing entries report zero, not panic.
        assert_eq!(
            agg.result_code_count("ns3", CommandType::Put, ResultCode::Ok),
            0
        );
        assert!(agg.detailed_metric("ns2", CommandType::Put).is_none());

        // Only ns1 recorded detailed histograms; ns2 only recorded a result
        // code, which lives in a separate map.
        assert_eq!(agg.detailed_namespaces(), vec!["ns1"]);
    }

    #[test]
    fn reshape_changes_histogram_layout() {
        let metrics = NodeMetrics::new(MetricsPolicy::default());
        metrics.set_enabled(true);
        metrics.record_command(CommandType::Get, 10);
        // Switch to a linear histogram with a different column count.
        let mut new_policy = MetricsPolicy::default();
        new_policy.histogram_type = crate::metrics::HistogramType::Linear;
        new_policy.latency_columns = 7;
        new_policy.latency_base = 5;
        metrics.reshape(&new_policy);
        // Reshape resets the histogram and changes its bucket count.
        let snap = metrics.get_and_reset();
        assert_eq!(snap.get_metrics.count(), 0);
        assert_eq!(snap.get_metrics.buckets().len(), 7);
    }

    #[cfg(feature = "serialization")]
    #[test]
    fn snapshot_serializes_with_stable_field_names() {
        let metrics = NodeMetrics::new(MetricsPolicy::default());
        metrics.set_enabled(true);
        metrics
            .counters
            .connections_open
            .store(4, Ordering::Relaxed);
        metrics.counters.tends_total.fetch_add(2, Ordering::Relaxed);
        metrics.record_command(CommandType::Put, 123);
        metrics.record_write("test", CommandType::Put, 64, 90);
        metrics.record_result_code("test", CommandType::Put, ResultCode::KeyNotFoundError);

        let snap = metrics.get_and_reset();
        let v = serde_json::to_value(&snap).unwrap();

        // Counter field names (JSON tags).
        assert_eq!(v["open-connections"], 4);
        assert_eq!(v["tends-total"], 2);
        assert!(v.get("transaction-retry-count").is_some());

        // Histogram object shape.
        let put = &v["put-metrics"];
        assert_eq!(put["count"], 1);
        assert!(put.get("buckets").unwrap().is_array());
        assert!(put.get("min").is_some() && put.get("max").is_some() && put.get("sum").is_some());

        // Detailed metrics nested by namespace -> command -> histograms.
        let detailed = &v["detailed-metrics"]["test"]["Put"];
        assert_eq!(detailed["bytes-sent"]["count"], 1);
        assert!(detailed.get("connection-aq").is_some());
        assert!(detailed.get("bytes-received").is_some());

        // Result-code counts nested by namespace -> command -> code string.
        let rc = &v["detailed-resultcode-counts"]["test"]["Put"];
        assert_eq!(rc[ResultCode::KeyNotFoundError.into_string()], 1);
    }
}
