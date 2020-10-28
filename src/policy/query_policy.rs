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
use crate::policy::{BasePolicy, PolicyLike};

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

    /// Number of records to place in queue before blocking. Records received from multiple server
    /// nodes will be placed in a queue. A separate thread consumes these records in parallel. If
    /// the queue is full, the producer threads will block until records are consumed.
    pub record_queue_size: usize,

    /// Terminate query if cluster is in fluctuating state.
    pub fail_on_cluster_change: bool,

    /// Optional Filter Expression
    pub filter_expression: Option<FilterExpression>,
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
            record_queue_size: 1024,
            fail_on_cluster_change: true,
            filter_expression: None,
        }
    }
}

impl PolicyLike for QueryPolicy {
    fn base(&self) -> &BasePolicy {
        &self.base_policy
    }
}
