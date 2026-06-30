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

//! Client-side periodic metrics.
//!
//! This subsystem collects per-node and cluster-wide operational statistics —
//! connection lifecycle counts, tend counts, node add/remove counts,
//! transaction retry/error counts, per-command-type latency histograms, and
//! detailed per-namespace/per-command-type histograms and result-code counts.
//!
//! Metrics are **polled**: enable collection with
//! [`crate::Client::enable_metrics`] and read a snapshot with
//! [`crate::Client::metrics`]. Collection is off by default and the recording
//! hot-path is gated by a cheap atomic flag, so there is no overhead when
//! disabled.
//!
//! Metrics are controlled only through the explicit [`crate::Client`] methods.

pub mod cluster;
pub mod histogram;
pub mod node_metrics;
pub mod policy;

pub use cluster::ClusterMetrics;
pub use histogram::{HistogramType, SyncHistogram};
pub use node_metrics::{
    CommandMetric, CommandType, NodeMetrics, NodeMetricsSnapshot, COMMAND_TYPE_COUNT,
};
pub use policy::{Labels, MetricsPolicy};
#[cfg(feature = "dynamic-config")]
pub(crate) use policy::MetricsPolicyConfig;
