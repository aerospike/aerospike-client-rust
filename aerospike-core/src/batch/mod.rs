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

pub mod batch_executor;
pub mod batch_record;

use crate::commands::buffer::{FIELD_HEADER_SIZE, OPERATION_HEADER_SIZE};
use crate::expressions::Expression;
use crate::msgpack::encoder;
use crate::operations::Operation;
use crate::Bins;
use crate::CommitLevel;
use crate::Expiration;
use crate::GenerationPolicy;
use crate::Key;
use crate::ReadTouchTTL;
use crate::Record;
use crate::RecordExistsAction;
use crate::ResultCode;
use crate::Value;

pub use self::batch_executor::BatchExecutor;
pub use self::batch_record::BatchRecord;

use crate::errors::{Error, Result};

pub struct BatchRecordIndex {
    pub batch_index: usize,
    pub record: Option<crate::Record>,
    pub result_code: ResultCode,
}

/// Policy for a single batch read operation.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchReadPolicy {
    /// `read_touch_ttl` determines how record TTL (time to live) is affected on reads. When enabled, the server can
    /// efficiently operate as a read-based LRU cache where the least recently used records are expired.
    /// The value is expressed as a percentage of the TTL sent on the most recent write such that a read
    /// within this interval of the record’s end of life will generate a touch.
    ///
    /// For example, if the most recent write had a TTL of 10 hours and `read_touch_ttl` is set to
    /// 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
    /// recent write) will result in a touch, resetting the TTL to another 10 hours.
    ///
    /// Supported in server v8+.
    ///
    /// Default: `ReadTouchTTL::ServerDefault`
    pub read_touch_ttl: ReadTouchTTL,

    /// Filter Expression is the optional expression filter. If filter Expression exists and evaluates to false, the specific batch key
    /// request is not performed and BatchRecord.ResultCode is set to `ResultCode::FILTERED_OUT`.
    ///
    /// Default: None
    pub filter_expression: Option<Expression>,
}

impl Default for BatchReadPolicy {
    fn default() -> Self {
        Self {
            read_touch_ttl: ReadTouchTTL::ServerDefault,
            filter_expression: None,
        }
    }
}

/// Policy for a single batch write operation.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchWritePolicy {
    /// `RecordExistsAction` qualifies how to handle writes where the record already exists.
    pub record_exists_action: RecordExistsAction,

    /// `GenerationPolicy` qualifies how to handle record writes based on record generation.
    /// The default (NONE) indicates that the generation is not used to restrict writes.
    pub generation_policy: GenerationPolicy,

    /// Desired consistency guarantee when committing a transaction on the server. The default
    /// (`COMMIT_ALL`) indicates that the server should wait for master and all replica commits to
    /// be successful before returning success to the client.
    pub commit_level: CommitLevel,

    /// Generation determines expected generation.
    /// Generation is the number of times a record has been
    /// modified (including creation) on the server.
    /// If a write operation is creating a record, the expected generation would be 0.
    pub generation: u32,

    /// Expiration determines record expiration in seconds. Also known as TTL (Time-To-Live).
    /// Seconds record will live before being removed by the server.
    pub expiration: Expiration,

    /// Send user defined key in addition to hash digest on a record put.
    /// The default is to not send the user defined key.
    pub send_key: bool,

    /// If the transaction results in a record deletion, leave a tombstone for the record. This
    /// prevents deleted records from reappearing after node failures. Valid for Aerospike Server
    /// Enterprise Edition 3.10+ only.
    pub durable_delete: bool,

    /// Optional Filter Expression
    pub filter_expression: Option<Expression>,
}

impl Default for BatchWritePolicy {
    fn default() -> Self {
        Self {
            record_exists_action: RecordExistsAction::Update,
            generation_policy: GenerationPolicy::None,
            commit_level: CommitLevel::CommitAll,
            generation: 0,
            expiration: Expiration::NamespaceDefault,
            send_key: false,
            durable_delete: false,
            filter_expression: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Policy for a single batch delete operation.
pub struct BatchDeletePolicy {
    /// `GenerationPolicy` qualifies how to handle record writes based on record generation.
    /// The default (NONE) indicates that the generation is not used to restrict writes.
    pub generation_policy: GenerationPolicy,

    /// Desired consistency guarantee when committing a transaction on the server. The default
    /// (`COMMIT_ALL`) indicates that the server should wait for master and all replica commits to
    /// be successful before returning success to the client.
    pub commit_level: CommitLevel,

    /// Generation determines expected generation.
    /// Generation is the number of times a record has been
    /// modified (including creation) on the server.
    /// If a write operation is creating a record, the expected generation would be 0.
    pub generation: u32,

    /// Send user defined key in addition to hash digest on a record put.
    /// The default is to not send the user defined key.
    pub send_key: bool,

    /// If the transaction results in a record deletion, leave a tombstone for the record. This
    /// prevents deleted records from reappearing after node failures. Valid for Aerospike Server
    /// Enterprise Edition 3.10+ only.
    pub durable_delete: bool,

    /// Optional Filter Expression
    pub filter_expression: Option<Expression>,
}

impl Default for BatchDeletePolicy {
    fn default() -> Self {
        Self {
            generation_policy: GenerationPolicy::None,
            commit_level: CommitLevel::CommitAll,
            generation: 0,
            send_key: false,
            durable_delete: false,
            filter_expression: None,
        }
    }
}

/// Policy for a single batch udf operation.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchUDFPolicy {
    /// Desired consistency guarantee when committing a transaction on the server. The default
    /// (`CommitAll`) indicates that the server should wait for master and all replica commits to
    /// be successful before returning success to the client.
    pub commit_level: CommitLevel,

    /// Expiration determines record expiration in seconds. Also known as TTL (Time-To-Live).
    /// Seconds record will live before being removed by the server.
    pub expiration: Expiration,

    /// Send user defined key in addition to hash digest on a record put.
    /// The default is to not send the user defined key.
    pub send_key: bool,

    /// If the transaction results in a record deletion, leave a tombstone for the record. This
    /// prevents deleted records from reappearing after node failures. Valid for Aerospike Server
    /// Enterprise Edition 3.10+ only.
    pub durable_delete: bool,

    /// Optional Filter Expression
    pub filter_expression: Option<Expression>,
}

impl Default for BatchUDFPolicy {
    fn default() -> Self {
        Self {
            commit_level: CommitLevel::CommitAll,
            expiration: Expiration::NamespaceDefault,
            send_key: false,
            durable_delete: false,
            filter_expression: None,
        }
    }
}

/// Represents a batch operation.
/// Do not directly create the batch operations. Use the helper methods instead.
#[derive(Clone, Debug)]
pub enum BatchOperation {
    #[doc(hidden)]
    Read {
        br: BatchRecord,
        policy: BatchReadPolicy,
        bins: Bins,
        ops: Option<Vec<Operation>>,
    },
    #[doc(hidden)]
    Write {
        br: BatchRecord,
        policy: BatchWritePolicy,
        ops: Vec<Operation>,
    },
    #[doc(hidden)]
    Delete {
        br: BatchRecord,
        policy: BatchDeletePolicy,
    },
    #[doc(hidden)]
    UDF {
        br: BatchRecord,
        policy: BatchUDFPolicy,
        udf_name: String,
        function_name: String,
        args: Option<Vec<Value>>,
    },
}

impl BatchOperation {
    /// Create a batch read operation.
    pub fn read(policy: &BatchReadPolicy, key: Key, bins: Bins) -> Self {
        BatchOperation::Read {
            br: BatchRecord::new(key, false),
            policy: policy.clone(),
            bins,
            ops: None,
        }
    }

    /// Create a batch read with multiple operations.
    pub fn read_ops(policy: &BatchReadPolicy, key: Key, ops: Vec<Operation>) -> Self {
        BatchOperation::Read {
            br: BatchRecord::new(key, false),
            policy: policy.clone(),
            bins: Bins::None,
            ops: Some(ops),
        }
    }

    /// Create a batch write with multiple operations.
    pub fn write(policy: &BatchWritePolicy, key: Key, ops: Vec<Operation>) -> Self {
        BatchOperation::Write {
            br: BatchRecord::new(key, true),
            policy: policy.clone(),
            ops,
        }
    }

    /// Create a batch delete operation.
    pub fn delete(policy: &BatchDeletePolicy, key: Key) -> Self {
        BatchOperation::Delete {
            br: BatchRecord::new(key, true),
            policy: policy.clone(),
        }
    }

    /// Create a batch UDF operation.
    pub fn udf(
        policy: &BatchUDFPolicy,
        key: Key,
        udf_name: &str,
        function_name: &str,
        args: Option<Vec<Value>>,
    ) -> Self {
        BatchOperation::UDF {
            br: BatchRecord::new(key, true),
            policy: policy.clone(),
            udf_name: udf_name.into(),
            function_name: function_name.into(),
            args,
        }
    }

    pub(crate) fn size(&self, parent_fe: &Option<Expression>) -> Result<usize> {
        match self {
            Self::Read {
                policy, bins, ops, ..
            } => {
                let mut size: usize = 0;

                match (&policy.filter_expression, &parent_fe) {
                    (Some(fe), _) => {
                        size += fe.size()? + FIELD_HEADER_SIZE as usize;
                    }
                    (_, Some(pfe)) => {
                        size += pfe.size()? + FIELD_HEADER_SIZE as usize;
                    }
                    _ => (),
                }

                if let Bins::Some(bin_names) = bins {
                    for bin in bin_names {
                        size += bin.len() + OPERATION_HEADER_SIZE as usize;
                    }
                }

                if let Some(ops) = ops {
                    for op in ops {
                        if op.is_write() {
                            return Err(Error::ClientError(
                                "Write operations not allowed in batch read".into(),
                            ));
                        }
                        size += op.estimate_size()? + 8;
                    }
                }

                Ok(size)
            }
            Self::Write {
                br, policy, ops, ..
            } => {
                let mut size: usize = 2; // gen(2) = 2

                match (&policy.filter_expression, &parent_fe) {
                    (Some(fe), _) => {
                        size += fe.size()? + FIELD_HEADER_SIZE as usize;
                    }
                    (_, Some(pfe)) => {
                        size += pfe.size()? + FIELD_HEADER_SIZE as usize;
                    }
                    _ => (),
                }

                if policy.send_key && br.key.has_value_to_send() {
                    if let Some(ref user_key) = br.key.user_key {
                        // field header size + key size
                        size += user_key.estimate_size()? + FIELD_HEADER_SIZE as usize + 1;
                    }
                }

                let mut has_write = false;

                for op in ops {
                    if op.is_write() {
                        has_write = true;
                    }
                    size += op.estimate_size()? + 8;
                }

                if !has_write {
                    return Err(Error::ClientError(
                        "Batch write operations do not contain a write".into(),
                    ));
                }
                Ok(size)
            }
            Self::Delete { br, policy } => {
                let mut size: usize = 2; // gen(2) = 2

                match (&policy.filter_expression, &parent_fe) {
                    (Some(fe), _) => {
                        size += fe.size()? + FIELD_HEADER_SIZE as usize;
                    }
                    (_, Some(pfe)) => {
                        size += pfe.size()? + FIELD_HEADER_SIZE as usize;
                    }
                    _ => (),
                }

                if policy.send_key && br.key.has_value_to_send() {
                    if let Some(ref user_key) = br.key.user_key {
                        // field header size + key size
                        size += user_key.estimate_size()? + FIELD_HEADER_SIZE as usize + 1;
                    }
                }

                Ok(size)
            }
            Self::UDF {
                br,
                policy,
                udf_name,
                function_name,
                args,
            } => {
                let mut size: usize = 2; // gen(2) = 2

                match (&policy.filter_expression, &parent_fe) {
                    (Some(fe), _) => {
                        size += fe.size()? + FIELD_HEADER_SIZE as usize;
                    }
                    (_, Some(pfe)) => {
                        size += pfe.size()? + FIELD_HEADER_SIZE as usize;
                    }
                    _ => (),
                }

                if policy.send_key && br.key.has_value_to_send() {
                    if let Some(ref user_key) = br.key.user_key {
                        // field header size + key size
                        size += user_key.estimate_size()? + FIELD_HEADER_SIZE as usize + 1;
                    }
                }

                size += udf_name.len() + FIELD_HEADER_SIZE as usize;
                size += function_name.len() + FIELD_HEADER_SIZE as usize;
                if let Some(args) = args {
                    size += encoder::pack_array(&mut None, args)? + FIELD_HEADER_SIZE as usize;
                } else {
                    size += encoder::pack_empty_args_array(&mut None) + FIELD_HEADER_SIZE as usize;
                }

                Ok(size)
            }
        }
    }

    pub(crate) const fn match_header(&self, _prev: Option<&BatchOperation>) -> bool {
        false
    }

    pub(crate) fn key(&self) -> Key {
        match self {
            Self::Read { br, .. }
            | Self::Write { br, .. }
            | Self::Delete { br, .. }
            | Self::UDF { br, .. } => br.key.clone(),
        }
    }

    /// Return the resulting batch record.
    pub fn batch_record(&self) -> BatchRecord {
        match self {
            Self::Read { br, .. }
            | Self::Write { br, .. }
            | Self::Delete { br, .. }
            | Self::UDF { br, .. } => br.clone(),
        }
    }

    pub(crate) fn set_record(&mut self, record: Option<Record>) {
        match self {
            Self::Read { br, .. }
            | Self::Write { br, .. }
            | Self::Delete { br, .. }
            | Self::UDF { br, .. } => {
                br.record = record;
                br.result_code = Some(ResultCode::Ok);
            }
        }
    }

    pub(crate) const fn set_result_code(&mut self, rc: ResultCode, in_doubt: bool) {
        match self {
            Self::Read { br, .. } => {
                br.result_code = Some(rc);
                br.in_doubt = false;
            }
            Self::Write { br, .. } => {
                br.result_code = Some(rc);
                br.in_doubt = in_doubt;
            }
            Self::Delete { br, .. } => {
                br.result_code = Some(rc);
                br.in_doubt = in_doubt;
            }
            Self::UDF { br, .. } => {
                br.result_code = Some(rc);
                br.in_doubt = in_doubt;
            }
        }
    }
}
