// Copyright 2015-2020 Aerospike, Inc.
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
use crate::{CommitLevel, Expiration, GenerationPolicy, RecordExistsAction};

/// `WritePolicy` encapsulates parameters for all write operations.
#[derive(Debug, Clone)]
pub struct WritePolicy {
    /// Base policy instance
    pub base_policy: BasePolicy,

    /// RecordExistsAction qualifies how to handle writes where the record already exists.
    pub record_exists_action: RecordExistsAction,

    /// GenerationPolicy qualifies how to handle record writes based on record generation.
    /// The default (NONE) indicates that the generation is not used to restrict writes.
    pub generation_policy: GenerationPolicy,

    /// Desired consistency guarantee when committing a transaction on the server. The default
    /// (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
    /// be successful before returning success to the client.
    pub commit_level: CommitLevel,

    /// Generation determines expected generation.
    /// Generation is the number of times a record has been
    /// modified (including creation) on the server.
    /// If a write operation is creating a record, the expected generation would be 0.
    pub generation: u32,

    /// Expiration determimes record expiration in seconds. Also known as TTL (Time-To-Live).
    /// Seconds record will live before being removed by the server.
    pub expiration: Expiration,

    /// Send user defined key in addition to hash digest on a record put.
    /// The default is to not send the user defined key.
    pub send_key: bool,

    /// For Client::operate() method, return a result for every operation.
    /// Some list operations do not return results by default (`operations::list::clear()` for
    /// example). This can sometimes make it difficult to determine the desired result offset in
    /// the returned bin's result list.
    ///
    /// Setting RespondPerEachOp to true makes it easier to identify the desired result offset
    /// (result offset equals bin's operate sequence). This only makes sense when multiple list
    /// operations are used in one operate call and some of those operations do not return results
    /// by default.
    pub respond_per_each_op: bool,

    /// If the transaction results in a record deletion, leave a tombstone for the record. This
    /// prevents deleted records from reappearing after node failures.  Valid for Aerospike Server
    /// Enterprise Edition 3.10+ only.
    pub durable_delete: bool,

    /// Optional Filter Expression
    pub filter_expression: Option<FilterExpression>,
}

impl WritePolicy {
    /// Create a new write policy instance with the specified generation and expiration parameters.
    pub fn new(gen: u32, exp: Expiration) -> Self {
        Self {
            generation: gen,
            expiration: exp,
            ..WritePolicy::default()
        }
    }

    /// Get the current Filter expression
    pub const fn filter_expression(&self) -> &Option<FilterExpression> {
        &self.filter_expression
    }
}

impl Default for WritePolicy {
    fn default() -> Self {
        WritePolicy {
            base_policy: BasePolicy::default(),
            record_exists_action: RecordExistsAction::Update,
            generation_policy: GenerationPolicy::None,
            commit_level: CommitLevel::CommitAll,
            generation: 0,
            expiration: Expiration::NamespaceDefault,
            send_key: false,
            respond_per_each_op: false,
            durable_delete: false,
            filter_expression: None,
        }
    }
}

impl PolicyLike for WritePolicy {
    fn base(&self) -> &BasePolicy {
        &self.base_policy
    }
}
