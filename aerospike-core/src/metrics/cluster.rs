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

//! Cluster-wide metrics snapshot returned to the user.

use std::collections::HashMap;

use super::node_metrics::NodeMetricsSnapshot;

#[cfg(feature = "serialization")]
use serde::Serialize;

/// Aggregated statistics for the whole cluster, returned by
/// [`crate::Client::metrics`]. The per-host snapshots are flattened in alongside
/// the synthetic `cluster-aggregated-metrics`, `total-nodes` and
/// `open-connections` keys.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct ClusterMetrics {
    /// Per-node statistics keyed by host address.
    #[cfg_attr(feature = "serialization", serde(flatten))]
    pub nodes: HashMap<String, NodeMetricsSnapshot>,

    /// Statistics aggregated across every node (carries the cluster labels).
    #[cfg_attr(
        feature = "serialization",
        serde(rename = "cluster-aggregated-metrics")
    )]
    pub cluster_aggregated: NodeMetricsSnapshot,

    /// Number of active nodes in the cluster.
    #[cfg_attr(feature = "serialization", serde(rename = "total-nodes"))]
    pub total_nodes: usize,

    /// Total number of open connections across all nodes.
    #[cfg_attr(feature = "serialization", serde(rename = "open-connections"))]
    pub open_connections: u64,

    /// Number of commands that exhausted their retry budget.
    #[cfg_attr(feature = "serialization", serde(rename = "exceeded-max-retries"))]
    pub exceeded_max_retries: u64,

    /// Number of commands that exceeded their total timeout.
    #[cfg_attr(feature = "serialization", serde(rename = "exceeded-total-timeout"))]
    pub exceeded_total_timeout: u64,

    /// Feature/API usage counters. Empty unless `usage_enabled` was set on
    /// the metrics policy.
    pub usage: HashMap<String, u64>,
}

#[cfg(all(test, feature = "serialization"))]
mod tests {
    use super::*;
    use crate::metrics::MetricsPolicy;
    use std::collections::HashMap;

    #[test]
    fn cluster_metrics_serializes_with_expected_layout() {
        let policy = MetricsPolicy::default();
        let mut node = NodeMetricsSnapshot::new(policy.clone());
        node.set_open_connections(2);
        let mut nodes = HashMap::new();
        nodes.insert("127.0.0.1:3000".to_string(), node);

        let metrics = ClusterMetrics {
            nodes,
            cluster_aggregated: NodeMetricsSnapshot::new(policy),
            total_nodes: 1,
            open_connections: 2,
            exceeded_max_retries: 0,
            exceeded_total_timeout: 0,
            usage: HashMap::new(),
        };

        let v = serde_json::to_value(&metrics).unwrap();
        // Per-node entries are flattened at the top level alongside the
        // synthetic keys.
        assert!(v.get("127.0.0.1:3000").is_some());
        assert!(v.get("cluster-aggregated-metrics").is_some());
        assert_eq!(v["total-nodes"], 1);
        assert_eq!(v["open-connections"], 2);
        assert_eq!(v["exceeded-max-retries"], 0);
        assert_eq!(v["exceeded-total-timeout"], 0);
        assert!(v.get("usage").is_some());
        assert_eq!(v["usage"], serde_json::json!({}));
        // The per-node entry carries the open-connections gauge.
        assert_eq!(v["127.0.0.1:3000"]["open-connections"], 2);
    }
}
