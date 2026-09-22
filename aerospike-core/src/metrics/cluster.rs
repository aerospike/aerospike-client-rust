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

    /// Total number of open connections across all nodes (idle, checked out,
    /// opening or recovering).
    #[cfg_attr(feature = "serialization", serde(rename = "open-connections"))]
    pub open_connections: u64,

    /// Connections checked out, opening or recovering across all nodes
    /// (`open_connections - connections_in_pool`). Read live from the pools
    /// when the snapshot is taken, so it moves in either direction.
    #[cfg_attr(feature = "serialization", serde(rename = "connections-in-use"))]
    pub connections_in_use: u64,

    /// Idle connections sitting in the pools across all nodes, read live when
    /// the snapshot is taken.
    #[cfg_attr(feature = "serialization", serde(rename = "connections-in-pool"))]
    pub connections_in_pool: u64,

    /// Connections currently handed to background timeout-recovery tasks
    /// across all nodes (the `timeout_delay` back-pressure gauge).
    #[cfg_attr(feature = "serialization", serde(rename = "recover-queue-size"))]
    pub recover_queue_size: u64,

    /// Peer hosts that failed node validation during tend, accumulated over
    /// the client's lifetime while metrics are enabled. A host that stays
    /// unreachable adds one per tend it is tried in.
    #[cfg_attr(feature = "serialization", serde(rename = "nodes-invalid"))]
    pub nodes_invalid: u64,

    /// Number of commands that exhausted their retry budget.
    #[cfg_attr(feature = "serialization", serde(rename = "exceeded-max-retries"))]
    pub exceeded_max_retries: u64,

    /// Number of commands that exceeded their total timeout.
    #[cfg_attr(feature = "serialization", serde(rename = "exceeded-total-timeout"))]
    pub exceeded_total_timeout: u64,
}

#[cfg(all(test, feature = "serialization"))]
mod tests {
    use super::*;
    use crate::metrics::MetricsPolicy;
    use std::collections::HashMap;

    #[test]
    fn cluster_metrics_serializes_with_expected_layout() {
        use crate::metrics::PoolGauges;
        let policy = MetricsPolicy::default();
        let mut node = NodeMetricsSnapshot::new(policy.clone());
        node.set_pool_gauges(PoolGauges {
            total: 2,
            in_pool: 1,
            recovering: 0,
        });
        node.set_error_rate(5);
        let mut nodes = HashMap::new();
        nodes.insert("127.0.0.1:3000".to_string(), node);

        let metrics = ClusterMetrics {
            nodes,
            cluster_aggregated: NodeMetricsSnapshot::new(policy),
            total_nodes: 1,
            open_connections: 2,
            connections_in_use: 1,
            connections_in_pool: 1,
            recover_queue_size: 0,
            nodes_invalid: 3,
            exceeded_max_retries: 0,
            exceeded_total_timeout: 0,
        };

        let v = serde_json::to_value(&metrics).unwrap();
        // Per-node entries are flattened at the top level alongside the
        // synthetic keys.
        assert!(v.get("127.0.0.1:3000").is_some());
        assert!(v.get("cluster-aggregated-metrics").is_some());
        assert_eq!(v["total-nodes"], 1);
        assert_eq!(v["open-connections"], 2);
        assert_eq!(v["connections-in-use"], 1);
        assert_eq!(v["connections-in-pool"], 1);
        assert_eq!(v["recover-queue-size"], 0);
        assert_eq!(v["nodes-invalid"], 3);
        assert_eq!(v["exceeded-max-retries"], 0);
        assert_eq!(v["exceeded-total-timeout"], 0);
        // The per-node entry carries the pool and breaker gauges.
        let node = &v["127.0.0.1:3000"];
        assert_eq!(node["open-connections"], 2);
        assert_eq!(node["connections-in-use"], 1);
        assert_eq!(node["connections-in-pool"], 1);
        assert_eq!(node["connections-recovering"], 0);
        assert_eq!(node["error-rate"], 5);
    }
}
