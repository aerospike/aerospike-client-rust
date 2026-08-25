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
//!
//! # Time resolution
//!
//! Every elapsed time — command latency, connection-acquire time, parse time —
//! is bucketed in the [`MetricsPolicy::latency_unit`] resolution using the
//! logarithmic range layout (`<= 1`, `> 1`, then boundaries multiplied by
//! `2^latency_shift`). The default is [`LatencyUnit::Milliseconds`] with 7
//! columns ([`MetricsPolicy::millis`]); [`MetricsPolicy::micros`] selects
//! microseconds with 24 columns. Unit and column count belong together — 7
//! columns of microseconds top out at `>32µs`.
//!
//! The unit is reported by every snapshot ([`NodeMetricsSnapshot::latency_unit`],
//! serialized as `latency-unit`), because bucket counts cannot be read without
//! it. Changing it while collecting discards the samples already recorded, which
//! were measured in the other unit — exactly as a `latency_columns` change does.
//! With the `dynamic-config` feature the histogram keys live under
//! `dynamic.metrics.extended.operational` (`latency_unit`, `latency_columns`,
//! `latency_shift`, `sampler`) with a `usage` sibling. Flat `latency_unit` /
//! `latency_columns` / `latency_base` / `latency_shift` aliases at the
//! `dynamic.metrics` root remain valid.

pub mod cluster;
pub mod histogram;
pub mod node_metrics;
pub mod policy;

pub use cluster::ClusterMetrics;
pub use histogram::SyncHistogram;
pub use node_metrics::{
    CommandMetric, CommandType, NodeMetrics, NodeMetricsSnapshot, COMMAND_TYPE_COUNT,
};
#[cfg(feature = "dynamic-config")]
pub(crate) use policy::MetricsPolicyConfig;
pub use policy::{
    Labels, LatencyUnit, MetricsPolicy, DEFAULT_LATENCY_BASE, DEFAULT_LATENCY_COLUMNS,
    DEFAULT_LATENCY_SHIFT, MICROS_LATENCY_COLUMNS, MILLIS_LATENCY_COLUMNS,
};
