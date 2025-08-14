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

use crate::expressions::FilterExpression;
use crate::policy::{BasePolicy, PolicyLike, QueryDuration, Replica, StreamPolicy};
use aerospike_rt::time::Duration;

/// `QueryPolicy` encapsulates parameters for query operations.
#[derive(Debug, Clone)]
pub struct QueryPolicy {
    /// Base policy instance
    pub base_policy: BasePolicy,

    /// Maximum number of concurrent requests to server nodes at any point in time. If there are 16
    /// nodes in the cluster and `max_concurrent_nodes` is 8, then queries will be made to 8 nodes
    /// in parallel. When a query completes, a new query will be issued until all 16 nodes have
    /// been queried. Default (0) is to issue requests to all server nodes in parallel.
    pub max_concurrent_nodes: usize,

    /// Number of records to return to the client. This number is divided by the
    /// number of nodes involved in the query. The actual number of records returned
    /// may be less than max_records if node record counts are small and unbalanced across
    /// nodes.
    ///
    /// This field is supported on server versions >= 4.9.
    ///
    /// Default: 0 (do not limit record count)
    pub max_records: u64,

    /// Number of records to place in queue before blocking. Records received from multiple server
    /// nodes will be placed in a queue. A separate thread consumes these records in parallel. If
    /// the queue is full, the producer threads will block until records are consumed.
    pub record_queue_size: usize,

    /// Limits returned records per second (rps) rate for each server node.
    /// It does not apply rps limit if `records_per_second` is zero (default).
    //
    /// `records_per_second` is supported in all primary and secondary index
    /// queries in server versions 6.0+. For background queries, `records_per_second`
    /// is bounded by the server config `background-query-max-rps`.
    pub records_per_second: u32,

    /// Terminate query if cluster is in fluctuating state.
    pub fail_on_cluster_change: bool,

    /// Maximum time in milliseconds to wait when polling socket for availability prior to
    /// performing an operation on the socket on the server side. Zero means there is no socket
    /// timeout. Default: 10,000 ms.
    pub socket_timeout: u32,

    /// Expected query duration. The server treats the query in different ways depending on the expected duration.
    /// This field is ignored for aggregation queries, background queries and server versions < 6.0.
    ///
    /// Default: QueryDuration::Long
    pub expected_duration: QueryDuration,

    /// Optional Filter Expression
    pub filter_expression: Option<FilterExpression>,

    /// Defines algorithm used to determine the target node for a command. The replica algorithm only affects single record and batch commands.
    pub replica: Replica,
}

impl QueryPolicy {
    /// Create a new query policy instance with default parameters.
    pub fn new() -> Self {
        QueryPolicy::default()
    }

    /// Get the current Filter Expression
    pub const fn filter_expression(&self) -> &Option<FilterExpression> {
        &self.filter_expression
    }
}

impl Default for QueryPolicy {
    fn default() -> Self {
        QueryPolicy {
            base_policy: BasePolicy::default(),
            max_concurrent_nodes: 0,
            max_records: 0,
            records_per_second: 0,
            record_queue_size: 1024,
            fail_on_cluster_change: true,
            socket_timeout: 10000,
            expected_duration: QueryDuration::Long,
            filter_expression: None,
            replica: Replica::default(),
        }
    }
}

impl PolicyLike for QueryPolicy {
    fn base(&self) -> &BasePolicy {
        &self.base_policy
    }
}

impl StreamPolicy for &QueryPolicy {
    fn max_records(&self) -> Option<u64> {
        if self.max_records > 0 {
            Some(self.max_records)
        } else {
            None
        }
    }
    fn sleep_between_retries(&self) -> Option<Duration> {
        self.base_policy.sleep_between_retries
    }
    fn socket_timeout(&self) -> Option<Duration> {
        self.base_policy.total_timeout //self.base_policy.socket_timeout
    }
    fn total_timeout(&self) -> Option<Duration> {
        self.base_policy.total_timeout
    }
    fn replica(&self) -> crate::policy::Replica {
        self.replica
    }
    fn max_retries(&self) -> Option<usize> {
        self.base_policy.max_retries
    }
}
