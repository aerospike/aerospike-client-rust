// Copyright 2015-2017 Aerospike, Inc.
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

use policy::{BasePolicy, PolicyLike};

pub enum BatchConcurrency {
    /// Issue batch requests sequentially. This mode has a performance advantage for small to
    /// medium sized batch sizes because requests can be issued in the main transaction thread.
    /// This is the default.
    Sequential,

    /// Issue all batch requests in parallel threads. This mode has a performance advantage for
    /// extremely large batch sizes because each node can process the request immediately. The
    /// downside is extra threads will need to be created (or takedn from a thread pool).
    Parallel,

    /// Issue up to N batch requests in parallel threads. When a request completes, a new request
    /// will be issued until all threads are complete. This mode prevents too many parallel threads
    /// being created for large cluster implementations. The downside is extra threads will still
    /// need to be created (or taken from a thread pool).
    ///
    /// E.g. if there are 16 nodes/namespace combinations requested and concurrency is set to
    /// `MaxThreads(8)`, then batch requests will be made for 8 node/namespace combinations in
    /// parallel threads. When a request completes, a new request will be issued until all 16
    /// requests are complete.
    MaxThreads(usize),
}

/// `BatchPolicy` encapsulates parameters for all batch operations.
pub struct BatchPolicy {
    /// Base policy instance
    pub base_policy: BasePolicy,

    /// Concurrency mode for batch requests: Sequential or Parallel (with optional max. no of
    /// parallel threads).
    pub concurrency: BatchConcurrency,

    /// Allow batch to be processed immediately in the server's receiving thread when the server
    /// deems it to be appropriate. If false, the batch will always be processed in separate
    /// transaction threads.
    ///
    /// For batch exists or batch reads of smaller sized records (<= 1K per record), inline
    /// processing will be significantly faster on "in memory" namespaces. The server disables
    /// inline processing on disk based namespaces regardless of this policy field.
    ///
    /// Inline processing can introduce the possibility of unfairness because the server can
    /// process the entire batch before moving onto the next command.
    ///
    /// Default: true
    pub allow_inline: bool,

    /// Send set name field to server for every key in the batch. This is only necessary when
    /// authentication is enabled and security roles are defined on a per-set basis.
    ///
    /// Default: false
    pub send_set_name: bool,
}

impl BatchPolicy {
    /// Create a new batch policy instance.
    pub fn new() -> Self {
        BatchPolicy::default()
    }
}

impl Default for BatchPolicy {
    fn default() -> Self {
        BatchPolicy {
            base_policy: BasePolicy::default(),
            concurrency: BatchConcurrency::Sequential,
            allow_inline: true,
            send_set_name: false,
        }
    }
}

impl PolicyLike for BatchPolicy {
    fn base(&self) -> &BasePolicy {
        &self.base_policy
    }
}
