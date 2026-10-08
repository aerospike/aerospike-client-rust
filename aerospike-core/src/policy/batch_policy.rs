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

use crate::expressions::Expression;
#[cfg(feature = "dynamic-config")]
use crate::policy::BasePolicyConfig;
use crate::policy::{BasePolicy, Concurrency, PolicyLike};


/// `BatchPolicy` encapsulates parameters for all batch operations.
///
/// [`default()`](Self::default) is the read default (`max_retries` 2).
/// A batch that contains writes, deletes or UDF calls should use
/// [`write_default()`](Self::write_default), whose `max_retries` is 0.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
pub struct BatchPolicy {
    /// Base policy instance
    #[cfg_attr(feature = "dynamic-config", config(flatten))]
    pub base_policy: BasePolicy,

    /// Concurrency mode for batch requests: one node after another, or all
    /// nodes at once. Default: `Parallel`.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub concurrency: Concurrency,

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

    /// Allow batch to be processed immediately in the server's receiving thread for SSD
    /// namespaces. If false, the batch will always be processed in separate service threads.
    /// Server versions before 6.0 ignore this field.
    ///
    /// Inline processing can introduce the possibility of unfairness because the server
    /// can process the entire batch before moving onto the next command.
    ///
    /// Default: false
    pub allow_inline_ssd: bool, // = false

    /// Should all batch keys be attempted regardless of errors. This field is used on both
    /// the client and server. The client handles node specific errors and the server handles
    /// key specific errors.
    ///
    /// If true, every batch key is attempted regardless of previous key specific errors.
    /// Node specific errors such as timeouts stop keys to that node, but keys directed at
    /// other nodes will continue to be processed.
    ///
    /// If false, the server will stop the batch to its node on most key specific errors.
    /// The exceptions are `ResultCode::KeyNotFoundError` and `ResultCode::FilteredOut` which never stop the batch.
    /// The client will stop the entire batch on node specific errors for sync commands
    /// that are run in sequence ([`Concurrency::Sequential`]). The client will not stop
    /// the entire batch for async commands or sync commands run in parallel.
    ///
    /// Server versions < 6.0 do not support this field and treat this value as false
    /// for key specific errors.
    ///
    /// Default: true
    pub respond_all_keys: bool, //= true;
}

impl BatchPolicy {
    /// Creates a new batch policy instance.
    pub fn new() -> Self {
        BatchPolicy::default()
    }

    /// The default policy for a batch that contains writes: `max_retries` is
    /// 0, as in the Java and Go clients, because a retried write can be
    /// applied twice when the first attempt did reach the server. Every other
    /// field matches [`default()`](Self::default). Pass it to
    /// [`Client::batch`](crate::Client::batch) when any row is a write, delete
    /// or UDF call.
    #[must_use]
    pub fn write_default() -> Self {
        let mut policy = Self::default();
        policy.base_policy.max_retries = 0;
        policy
    }

    /// The filter expression every row of the batch is evaluated against
    /// (`base_policy.filter_expression`).
    pub const fn filter_expression(&self) -> Option<&Expression> {
        self.base_policy.filter_expression.as_ref()
    }
}

impl Default for BatchPolicy {
    fn default() -> Self {
        BatchPolicy {
            base_policy: BasePolicy::default(),
            concurrency: Concurrency::Parallel,
            allow_inline: true,
            allow_inline_ssd: false,
            respond_all_keys: true,
        }
    }
}

impl PolicyLike for BatchPolicy {
    fn base(&self) -> &BasePolicy {
        &self.base_policy
    }
}
