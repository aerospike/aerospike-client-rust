// Copyright 2015-2018 Aerospike, Inc.
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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(feature = "serialization")]
use serde::{ser::Error, Serialize, Serializer};

use crate::cluster::histogram::Histogram;
use crate::commands::{CommandType, NamespaceProvider};
use crate::policy::MetricsPolicy;
use crate::ResultCode;

#[derive(Debug, Clone)]
pub struct ResultCodeArray {
    values: [u64; 256],
}

impl Default for ResultCodeArray {
    fn default() -> Self {
        Self { values: [0; 256] }
    }
}

#[cfg(feature = "serialization")]
impl Serialize for ResultCodeArray {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values[..].serialize(serializer).map_err(|e| {
            S::Error::custom(crate::errors::Error::ClientError(format!(
                "Serialization error: {e}"
            )))
        })
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct CommandResultCodeMetric {
    /// Contains the resultcodes per count
    #[cfg_attr(feature = "serialization", serde(flatten))]
    pub result_code_counts: ResultCodeArray,
}

#[derive(Debug)]
pub struct SingleCommandMetric {
    pub connection_acq: u128,
    pub command_transmission: u128,
    pub command_encoding: u128,
    pub command_parsing: u128,
    pub command_latency: u128,
    pub bytes_sent: u128,
    pub bytes_received: u128,
    pub result_codes: ResultCode,
}

impl SingleCommandMetric {
    pub fn new() -> Self {
        Self {
            connection_acq: 0,
            command_encoding: 0,
            command_transmission: 0,
            command_parsing: 0,
            command_latency: 0,
            bytes_sent: 0,
            bytes_received: 0,
            result_codes: ResultCode::Ok,
        }
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct CommandMetric {
    pub connection_acq: Histogram<u128>,
    pub command_transmission: Histogram<u128>,
    pub command_encoding: Histogram<u128>,
    pub command_parsing: Histogram<u128>,
    pub command_latency: Histogram<u128>,
    pub bytes_sent: Histogram<u128>,
    pub bytes_received: Histogram<u128>,
}

impl CommandMetric {
    pub fn new(policy: &MetricsPolicy) -> Self {
        CommandMetric {
            connection_acq: Histogram::new(
                policy.histogram_type,
                policy.latency_base as u128,
                policy.latency_columns,
            ),
            command_transmission: Histogram::new(
                policy.histogram_type,
                policy.latency_base as u128,
                policy.latency_columns,
            ),
            command_encoding: Histogram::new(
                policy.histogram_type,
                policy.latency_base as u128,
                policy.latency_columns,
            ),
            command_parsing: Histogram::new(
                policy.histogram_type,
                policy.latency_base as u128,
                policy.latency_columns,
            ),
            command_latency: Histogram::new(
                policy.histogram_type,
                policy.latency_base as u128,
                policy.latency_columns,
            ),
            bytes_sent: Histogram::new(
                policy.histogram_type,
                policy.latency_base as u128,
                policy.latency_columns,
            ),
            bytes_received: Histogram::new(
                policy.histogram_type,
                policy.latency_base as u128,
                policy.latency_columns,
            ),
        }
    }

    pub fn apply(&mut self, other: &SingleCommandMetric) {
        self.connection_acq.put(other.connection_acq);
        self.command_encoding.put(other.command_encoding);
        self.command_transmission.put(other.command_transmission);
        self.command_parsing.put(other.command_parsing);
        self.command_latency.put(other.command_latency);
        self.bytes_sent.put(other.bytes_sent);
        self.bytes_received.put(other.bytes_received);
    }
}

//////////////////////////////////////////////////////////////////
// Metrics (Mutex<u64> counters)
//////////////////////////////////////////////////////////////////
#[derive(Debug)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
/// Contains the client performance metrics.
pub struct Metrics {
    /// MetricsPolicy contains policy, default values for histograms, which are used on node_stats init time
    pub metric_policy: MetricsPolicy,

    /// Attempts to open a connection (failed + successful)
    pub connections_attempts: Mutex<u64>,
    /// Successful attempts to open a connection
    pub connections_successful: Mutex<u64>,
    /// Failed attempts to use a connection (includes all errors)
    pub connections_failed: Mutex<u64>,
    /// Connection Timeout errors
    pub connections_timeout_errors: Mutex<u64>,
    /// Connection errors other than timeouts
    pub connections_other_errors: Mutex<u64>,
    /// Number of times circuit breaker was hit
    // TODO: After implementation
    pub circuit_breaker_hits: Mutex<u64>,
    /// The command polled the connection pool, but no connections were in the pool
    pub connections_pool_empty: Mutex<u64>,
    /// The command offered the connection to the pool, but the pool was full and the connection was closed
    pub connections_pool_overflow: Mutex<u64>,
    /// The connection was idle and was dropped
    pub connections_idle_dropped: Mutex<u64>,
    /// Number of open connections at a given time. Keep in mind that this number is not
    /// mathematically calculated from attempted, failed, successful and closed values
    /// since it is tracked over time and not just in this metric gathering period.
    pub connections_open: Mutex<u64>,
    /// Number of connections that were closed, for any reason (idled out, errored out, etc)
    pub connections_closed: Mutex<u64>,
    /// Total number of connections recovered after a command failure.
    pub connections_recovered: Mutex<u64>,
    /// Total number of attempted tends (failed + success)
    pub tends_total: Mutex<u64>,
    /// Total number of successful tends
    pub tends_successful: Mutex<u64>,
    /// Total number of failed tends
    pub tends_failed: Mutex<u64>,
    /// Total number of partition map updates
    pub partition_map_updates: Mutex<u64>,
    /// Total number of times nodes were added to the client (not the same as actual nodes added. Network disruptions between client and server may cause a node being dropped and re-added client-side)
    pub node_added: Mutex<u64>,
    /// Total number of times nodes were removed from the client (not the same as actual nodes removed. Network disruptions between client and server may cause a node being dropped client-side)
    pub node_removed: Mutex<u64>,
    /// Total number of command retries
    // TODO: After MRT implementations
    pub transaction_retry_count: Mutex<u64>,
    /// Total number of command errors
    pub transaction_error_count: Mutex<u64>,

    /// Latency metrics for Get commands
    pub get_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for GetHeader commands
    pub get_header_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for Exists commands
    pub exists_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for Put commands
    pub put_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for Delete commands
    pub delete_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for Operate commands
    pub operate_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for Query commands
    pub query_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for Scan commands
    pub scan_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for UDFMetrics commands
    pub udf_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for Read only Batch commands
    pub batch_read_metrics: Mutex<Histogram<u64>>,
    /// Latency metrics for Batch commands containing writes
    pub batch_write_metrics: Mutex<Histogram<u64>>,

    /// Error counts for each command per namespace
    pub result_code_counts_per_namespace:
        Mutex<HashMap<String, HashMap<CommandType, CommandResultCodeMetric>>>,

    /// Detailed metrics per namespace and per command type
    pub metrics_per_namespace: Mutex<HashMap<String, HashMap<CommandType, CommandMetric>>>,
}

impl Metrics {
    /// Creates new metrics
    pub(crate) fn new(policy: MetricsPolicy) -> Self {
        Self {
            connections_attempts: Mutex::new(0),
            connections_successful: Mutex::new(0),
            connections_failed: Mutex::new(0),
            connections_timeout_errors: Mutex::new(0),
            connections_other_errors: Mutex::new(0),
            circuit_breaker_hits: Mutex::new(0),
            connections_pool_empty: Mutex::new(0),
            connections_pool_overflow: Mutex::new(0),
            connections_idle_dropped: Mutex::new(0),
            connections_open: Mutex::new(0),
            connections_closed: Mutex::new(0),
            connections_recovered: Mutex::new(0),
            tends_total: Mutex::new(0),
            tends_successful: Mutex::new(0),
            tends_failed: Mutex::new(0),
            partition_map_updates: Mutex::new(0),
            node_added: Mutex::new(0),
            node_removed: Mutex::new(0),
            transaction_retry_count: Mutex::new(0),
            transaction_error_count: Mutex::new(0),

            get_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            get_header_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            exists_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            put_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            delete_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            operate_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            query_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            scan_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            udf_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            batch_read_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),
            batch_write_metrics: Mutex::new(Histogram::new(
                policy.histogram_type,
                policy.latency_base as u64,
                policy.latency_columns,
            )),

            result_code_counts_per_namespace: Mutex::new(HashMap::new()),
            metrics_per_namespace: Mutex::new(HashMap::new()),

            metric_policy: policy,
        }
    }
}

impl Metrics {
    pub(crate) fn apply_latency(&self, command_type: CommandType, micros: u64) {
        if !self.metric_policy.latency_metrics {
            return;
        };

        match command_type {
            CommandType::Get => self
                .get_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::GetHeader => self
                .get_header_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::Exists => self
                .exists_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::Put => self
                .put_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::Delete => self
                .delete_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::Operate => self
                .operate_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::Query => self
                .query_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::Scan => self
                .scan_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::Udf => self
                .udf_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::BatchRead => self
                .batch_read_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            CommandType::BatchWrite => self
                .batch_write_metrics
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .put(micros),
            _ => (),
        }
    }

    pub(crate) fn update_result_code_for_namespace<C: NamespaceProvider>(
        &self,
        cmd: &C,
        result_code: ResultCode,
    ) {
        if !self.metric_policy.error_metrics_per_latency {
            return;
        };

        let mut rc_counts = self
            .result_code_counts_per_namespace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (namespace, command_type) in cmd.get_namespaces() {
            // Try to get a mutable reference to the inner HashMap without allocating
            let inner: &mut HashMap<CommandType, CommandResultCodeMetric>;
            if let Some(value_ref) = rc_counts.get_mut(namespace) {
                // Key exists, use the existing value
                inner = value_ref;
            } else {
                // Key does not exist, insert a new HashMap
                inner = rc_counts
                    .entry(namespace.to_string())
                    .or_insert_with(HashMap::new);
            }

            let metric = inner
                .entry(command_type)
                .or_insert_with(|| CommandResultCodeMetric {
                    result_code_counts: ResultCodeArray { values: [0; 256] },
                });

            metric.result_code_counts.values[u8::from(result_code) as usize] += 1;
        }
    }

    pub(crate) fn update_metrics_for_namespace<C: NamespaceProvider>(
        &self,
        cmd: &C,
        single_metric: &SingleCommandMetric,
    ) {
        if !self.metric_policy.metrics_per_namespace {
            return;
        };

        let mut metrics = self
            .metrics_per_namespace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (namespace, command_type) in cmd.get_namespaces() {
            // Try to get a mutable reference to the inner HashMap without allocating
            let inner: &mut HashMap<CommandType, CommandMetric>;
            if let Some(value_ref) = metrics.get_mut(namespace) {
                // Key exists, use the existing value
                inner = value_ref;
            } else {
                // Key does not exist, insert a new HashMap
                inner = metrics
                    .entry(namespace.to_string())
                    .or_insert_with(HashMap::new);
            }

            let metric = inner
                .entry(command_type)
                .or_insert_with(|| CommandMetric::new(&self.metric_policy));

            metric.apply(single_metric);
        }
    }
}
