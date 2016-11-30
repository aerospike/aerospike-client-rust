// Copyright 2015-2016 Aerospike, Inc.
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

const NAMESPACE_DEFAULT: u32 = 0x00000000;
const NEVER_EXPIRE: u32      = 0xFFFFFFFF; // -1 as i32
const DONT_UPDATE: u32       = 0xFFFFFFFE; // -2 as i32

use std::u32;

use policy::{BasePolicy, PolicyLike};
use RecordExistsAction;
use GenerationPolicy;
use CommitLevel;

#[derive(Debug, Clone, Copy)]
pub enum Expiration {
    /// Set the record to expire X seconds from now
    Seconds(u32),

    /// Set the record's expiry time using the default time-to-live (TTL) value for the namespace
    NamespaceDefault,

    /// Set the record to never expire
    Never,

    /// Do not change the record's expiry time when updating the record
    DontUpdate,
}

impl From<Expiration> for u32 {
    fn from(exp: Expiration) -> u32 {
        match exp {
            Expiration::Seconds(secs)    => secs,
            Expiration::NamespaceDefault => NAMESPACE_DEFAULT,
            Expiration::Never            => NEVER_EXPIRE,
            Expiration::DontUpdate       => DONT_UPDATE,
        }
    }
}

pub struct WritePolicy {
    pub base_policy: BasePolicy,

    // RecordExistsAction qualifies how to handle writes where the record already exists.
    pub record_exists_action: RecordExistsAction, // = RecordExistsAction.UPDATE;

    // GenerationPolicy qualifies how to handle record writes based on record generation.
    // The default (NONE) indicates that the generation is not used to restrict writes.
    pub generation_policy: GenerationPolicy, // = GenerationPolicy.NONE;

    // Desired consistency guarantee when committing a transaction on the server. The default
    // (COMMIT_ALL) indicates that the server should wait for master and all replica commits to
    // be successful before returning success to the client.
    pub commit_level: CommitLevel, // = COMMIT_ALL

    // Generation determines expected generation.
    // Generation is the number of times a record has been
    // modified (including creation) on the server.
    // If a write operation is creating a record, the expected generation would be 0.
    pub generation: u32,

    // Expiration determimes record expiration in seconds. Also known as TTL (Time-To-Live).
    // Seconds record will live before being removed by the server.
    // Expiration values:
    // MaxUint32: Never expire for Aerospike 2 server versions >= 2.7.2 and Aerospike 3 server
    // versions >= 3.1.4.  Do not use -1 for older servers.
    // 0: Default to namespace configuration variable "default-ttl" on the server.
    // > 0: Actual expiration in seconds.
    pub expiration: Expiration,

    // Send user defined key in addition to hash digest on a record put.
    // The default is to not send the user defined key.
    pub send_key: bool,

    // For client.Operate() method, return a result for every operation.
    // Some list operations do not return results by default (ListClearOp() for example).
    // This can sometimes make it difficult to determine the desired result offset in the returned
    // bin's result list.
    //
    // Setting RespondPerEachOp to true makes it easier to identify the desired result offset
    // (result offset equals bin's operate sequence). This only makes sense when multiple list
    // operations are used in one operate call and some of those operations do not return results
    // by default.
    pub respond_per_each_op: bool,
}


impl WritePolicy {
    pub fn new(gen: u32, exp: Expiration) -> Self {
        let mut wp = WritePolicy::default();
        wp.generation = gen;
        wp.expiration = exp;

        wp
    }
}

impl Default for WritePolicy {
    fn default() -> WritePolicy {
        WritePolicy {
            base_policy: BasePolicy::default(),

            record_exists_action: RecordExistsAction::Update,
            generation_policy: GenerationPolicy::None,
            commit_level: CommitLevel::CommitAll,
            generation: 0,
            expiration: Expiration::NamespaceDefault,
            send_key: false,
            respond_per_each_op: false,
        }
    }
}

impl PolicyLike for WritePolicy {
    fn base(&self) -> &BasePolicy {
        &self.base_policy
    }
}
