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
#[cfg(test)]
mod encode_tests;
pub mod hook;
pub use hook::BatchHook;
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
use std::sync::Arc;

pub use self::batch_executor::BatchExecutor;
pub use self::batch_record::BatchRecord;

use crate::errors::{Error, Result};

pub struct BatchRecordIndex {
    pub batch_index: usize,
    pub record: Option<crate::Record>,
    pub result_code: ResultCode,
    pub version: Option<u64>,
    /// The server's extended error detail for the row, when the row carries
    /// a non-error outcome that still has one (`KeyNotFound`, `FilteredOut`).
    pub error_detail: Option<Box<crate::ServerErrorDetail>>,
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

impl BatchReadPolicy {
    /// Project this per-record policy onto a `ReadPolicy` derived from
    /// the parent `BatchPolicy`. Per-record fields override; everything
    /// else (timeouts, retries, replica, txn, etc.) inherits.
    pub(crate) fn to_read_policy(
        &self,
        parent: &crate::policy::BatchPolicy,
    ) -> crate::policy::ReadPolicy {
        let mut rp = crate::policy::ReadPolicy::default();
        rp.base_policy = parent.base_policy.clone();
        rp.replica = parent.replica;
        rp.base_policy.read_touch_ttl = self.read_touch_ttl;
        if self.filter_expression.is_some() {
            rp.base_policy
                .filter_expression
                .clone_from(&self.filter_expression);
        }
        rp
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

    /// If true, the MRT monitor record will only be written if the record is locked.
    /// Default: false
    pub on_locking_only: bool,

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
            on_locking_only: false,
            filter_expression: None,
        }
    }
}

impl BatchWritePolicy {
    pub(crate) fn to_write_policy(
        &self,
        parent: &crate::policy::BatchPolicy,
    ) -> crate::policy::WritePolicy {
        let mut wp = crate::policy::WritePolicy::default();
        wp.base_policy = parent.base_policy.clone();
        wp.record_exists_action = self.record_exists_action.clone();
        wp.generation_policy = self.generation_policy.clone();
        wp.commit_level = self.commit_level.clone();
        wp.generation = self.generation;
        wp.expiration = self.expiration;
        wp.send_key = self.send_key;
        wp.durable_delete = self.durable_delete;
        wp.on_locking_only = self.on_locking_only;
        if self.filter_expression.is_some() {
            wp.base_policy
                .filter_expression
                .clone_from(&self.filter_expression);
        }
        // Match Java's batch->single conversion: single-op writes that
        // come from a batch with multi-op shape return per-op results.
        wp.respond_per_each_op = true;
        wp
    }
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
/// Policy for a single batch delete operation.
pub struct BatchDeletePolicy {
    /// `GenerationPolicy` qualifies how to handle record writes based on record generation.
    /// The default (NONE) indicates that the generation is not used to restrict writes.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub generation_policy: GenerationPolicy,

    /// Desired consistency guarantee when committing a transaction on the server. The default
    /// (`COMMIT_ALL`) indicates that the server should wait for master and all replica commits to
    /// be successful before returning success to the client.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub commit_level: CommitLevel,

    /// Generation determines expected generation.
    /// Generation is the number of times a record has been
    /// modified (including creation) on the server.
    /// If a write operation is creating a record, the expected generation would be 0.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub generation: u32,

    /// Send user defined key in addition to hash digest on a record put.
    /// The default is to not send the user defined key.
    pub send_key: bool,

    /// If the transaction results in a record deletion, leave a tombstone for the record. This
    /// prevents deleted records from reappearing after node failures. Valid for Aerospike Server
    /// Enterprise Edition 3.10+ only.
    pub durable_delete: bool,

    /// Optional Filter Expression
    #[cfg_attr(feature = "dynamic-config", config(skip))]
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

impl BatchDeletePolicy {
    pub(crate) fn to_write_policy(
        &self,
        parent: &crate::policy::BatchPolicy,
    ) -> crate::policy::WritePolicy {
        let mut wp = crate::policy::WritePolicy::default();
        wp.base_policy = parent.base_policy.clone();
        wp.generation_policy = self.generation_policy.clone();
        wp.commit_level = self.commit_level.clone();
        wp.generation = self.generation;
        wp.send_key = self.send_key;
        wp.durable_delete = self.durable_delete;
        if self.filter_expression.is_some() {
            wp.base_policy
                .filter_expression
                .clone_from(&self.filter_expression);
        }
        wp
    }
}

/// Policy for a single batch udf operation.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
pub struct BatchUDFPolicy {
    /// Desired consistency guarantee when committing a transaction on the server. The default
    /// (`CommitAll`) indicates that the server should wait for master and all replica commits to
    /// be successful before returning success to the client.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub commit_level: CommitLevel,

    /// Expiration determines record expiration in seconds. Also known as TTL (Time-To-Live).
    /// Seconds record will live before being removed by the server.
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub expiration: Expiration,

    /// Send user defined key in addition to hash digest on a record put.
    /// The default is to not send the user defined key.
    pub send_key: bool,

    /// If the transaction results in a record deletion, leave a tombstone for the record. This
    /// prevents deleted records from reappearing after node failures. Valid for Aerospike Server
    /// Enterprise Edition 3.10+ only.
    pub durable_delete: bool,

    /// If true, the MRT monitor record will only be written if the record is locked.
    /// Default: false
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub on_locking_only: bool,

    /// Optional Filter Expression
    #[cfg_attr(feature = "dynamic-config", config(skip))]
    pub filter_expression: Option<Expression>,
}

impl Default for BatchUDFPolicy {
    fn default() -> Self {
        Self {
            commit_level: CommitLevel::CommitAll,
            expiration: Expiration::NamespaceDefault,
            send_key: false,
            durable_delete: false,
            on_locking_only: false,
            filter_expression: None,
        }
    }
}

impl BatchUDFPolicy {
    pub(crate) fn to_write_policy(
        &self,
        parent: &crate::policy::BatchPolicy,
    ) -> crate::policy::WritePolicy {
        let mut wp = crate::policy::WritePolicy::default();
        wp.base_policy = parent.base_policy.clone();
        wp.commit_level = self.commit_level.clone();
        wp.expiration = self.expiration;
        wp.send_key = self.send_key;
        wp.durable_delete = self.durable_delete;
        wp.on_locking_only = self.on_locking_only;
        if self.filter_expression.is_some() {
            wp.base_policy
                .filter_expression
                .clone_from(&self.filter_expression);
        }
        wp.respond_per_each_op = true;
        wp
    }
}

/// One row of a batch call: a key, the operation to run at that key, and
/// after the call its outcome.
///
/// Build rows with [`BatchOperation::read`], [`read_ops`](Self::read_ops),
/// [`write`](Self::write), [`delete`](Self::delete) and [`udf`](Self::udf),
/// pass them to [`Client::batch`](crate::Client::batch), then read the
/// outcome through [`record`](Self::record), [`take_record`](Self::take_record),
/// [`result_code`](Self::result_code), [`in_doubt`](Self::in_doubt) or the
/// whole [`batch_record`](Self::batch_record). What the row does, and the
/// policy it does it with, are fixed at construction and not inspectable.
#[derive(Clone, Debug)]
pub struct BatchOperation {
    pub(crate) br: BatchRecord,
    pub(crate) kind: BatchOp,
}

/// What a [`BatchOperation`] does at its key. Not exported: the encoders,
/// the single-key fallback and the dynamic-config patcher match on it; users
/// only ever see the opaque row.
#[derive(Clone, Debug)]
pub enum BatchOp {
    Read {
        policy: BatchReadPolicy,
        bins: Bins,
        ops: Option<Vec<Operation>>,
    },
    Write {
        policy: BatchWritePolicy,
        ops: Vec<Operation>,
    },
    Delete {
        policy: BatchDeletePolicy,
    },
    Udf {
        policy: BatchUDFPolicy,
        udf_name: String,
        function_name: String,
        args: Option<Vec<Value>>,
    },
    /// Multi-record-transaction *verify*: check a record's version. Built by
    /// the transaction roll/verify path, never by users.
    TxnVerify {
        version: Option<u64>,
    },
    /// Multi-record-transaction *roll* forward/back. `roll_attr` is one of
    /// `INFO4_MRT_ROLL_FORWARD` / `INFO4_MRT_ROLL_BACK`.
    TxnRoll {
        txn: Arc<crate::txn::Txn>,
        roll_attr: u8,
    },
}

impl BatchOperation {
    /// Creates a batch read operation.
    pub fn read(policy: &BatchReadPolicy, key: Key, bins: Bins) -> Self {
        Self {
            br: BatchRecord::new(key, false),
            kind: BatchOp::Read {
                policy: policy.clone(),
                bins,
                ops: None,
            },
        }
    }

    /// Creates a batch read with multiple operations.
    pub fn read_ops(policy: &BatchReadPolicy, key: Key, ops: Vec<Operation>) -> Self {
        Self {
            br: BatchRecord::new(key, false),
            kind: BatchOp::Read {
                policy: policy.clone(),
                bins: Bins::None,
                ops: Some(ops),
            },
        }
    }

    /// Creates a batch write with multiple operations.
    pub fn write(policy: &BatchWritePolicy, key: Key, ops: Vec<Operation>) -> Self {
        Self {
            br: BatchRecord::new(key, true),
            kind: BatchOp::Write {
                policy: policy.clone(),
                ops,
            },
        }
    }

    /// Creates a batch delete operation.
    pub fn delete(policy: &BatchDeletePolicy, key: Key) -> Self {
        Self {
            br: BatchRecord::new(key, true),
            kind: BatchOp::Delete {
                policy: policy.clone(),
            },
        }
    }

    /// Creates a batch UDF operation.
    pub fn udf(
        policy: &BatchUDFPolicy,
        key: Key,
        udf_name: &str,
        function_name: &str,
        args: Option<Vec<Value>>,
    ) -> Self {
        Self {
            br: BatchRecord::new(key, true),
            kind: BatchOp::Udf {
                policy: policy.clone(),
                udf_name: udf_name.into(),
                function_name: function_name.into(),
                args,
            },
        }
    }

    /// A transaction *verify* row: compare the record at `key` against the
    /// `version` the transaction read. Encoded by the homogeneous
    /// `set_batch_txn_verify` encoder only.
    pub(crate) const fn txn_verify(key: Key, version: Option<u64>) -> Self {
        Self {
            br: BatchRecord::new(key, false),
            kind: BatchOp::TxnVerify { version },
        }
    }

    /// A transaction *roll* row for `key`. Encoded by the homogeneous
    /// `set_batch_txn_roll` encoder only.
    pub(crate) const fn txn_roll(key: Key, txn: Arc<crate::txn::Txn>, roll_attr: u8) -> Self {
        Self {
            br: BatchRecord::new(key, true),
            kind: BatchOp::TxnRoll { txn, roll_attr },
        }
    }

    /// Returns true if this batch operation contains a write.
    pub(crate) const fn has_write(&self) -> bool {
        match self.kind {
            BatchOp::Read { .. } | BatchOp::TxnVerify { .. } => false,
            BatchOp::Write { .. }
            | BatchOp::Delete { .. }
            | BatchOp::Udf { .. }
            | BatchOp::TxnRoll { .. } => true,
        }
    }

    pub(crate) fn size(&self, parent_fe: &Option<Expression>) -> Result<usize> {
        let br = &self.br;
        match &self.kind {
            BatchOp::Read { policy, bins, ops } => {
                let mut size: usize = 0;

                match (&policy.filter_expression, parent_fe) {
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
                            return Err(Error::client_error(
                                "Write operations not allowed in batch read",
                            ));
                        }
                        size += op.estimate_size()? + 8;
                    }
                }

                Ok(size)
            }
            BatchOp::Write { policy, ops } => {
                let mut size: usize = 2; // gen(2) = 2

                match (&policy.filter_expression, parent_fe) {
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
                    return Err(Error::client_error(
                        "Batch write operations do not contain a write",
                    ));
                }
                Ok(size)
            }
            BatchOp::Delete { policy } => {
                let mut size: usize = 2; // gen(2) = 2

                match (&policy.filter_expression, parent_fe) {
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
            BatchOp::Udf {
                policy,
                udf_name,
                function_name,
                args,
            } => {
                let mut size: usize = 2; // gen(2) = 2

                match (&policy.filter_expression, parent_fe) {
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
            // Txn verify/roll are encoded by dedicated homogeneous encoders
            // (`set_batch_txn_verify` / `set_batch_txn_roll`), not the generic
            // per-op sizing loop, so this is never consulted for them.
            BatchOp::TxnVerify { .. } | BatchOp::TxnRoll { .. } => Ok(0),
        }
    }

    /// Returns true if this op's header can be encoded as a `BATCH_MSG_REPEAT` of `prev`
    /// (i.e. identical namespace/set/policy/payload). When true, the wire protocol writes
    /// only the repeat flag and digest, saving the full namespace/set/bin payload.
    ///
    /// Safe conservative fallback: returns false whenever we can't cheaply prove equality.
    /// Correctness-critical: a false-positive here produces a wrong request.
    pub(crate) fn match_header(
        &self,
        prev: Option<&BatchOperation>,
        ver: Option<u64>,
        ver_prev: Option<u64>,
    ) -> bool {
        let Some(prev) = prev else { return false };

        // Same txn read version.
        if ver != ver_prev {
            return false;
        }

        // Same namespace & set.
        let key = &self.br.key;
        let key_prev = &prev.br.key;
        if key.namespace != key_prev.namespace || key.set_name != key_prev.set_name {
            return false;
        }

        // Variant-specific payload match. send_key=true forces non-repeat because
        // the user_key field in the per-record header differs across records.
        // Operation lists compare by content, with opaque encoder closures
        // compared by Arc identity — so cloned op lists (the natural
        // "build once, apply to N keys" pattern) repeat, and anything not
        // provably identical conservatively writes a full header.
        match (&self.kind, &prev.kind) {
            (
                BatchOp::Read {
                    policy: p,
                    bins: b,
                    ops: o,
                },
                BatchOp::Read {
                    policy: pp,
                    bins: bp,
                    ops: op,
                },
            ) => p == pp && b == bp && o == op,
            (BatchOp::Delete { policy: p }, BatchOp::Delete { policy: pp }) => {
                !p.send_key && !pp.send_key && p == pp
            }
            (
                BatchOp::Write { policy: p, ops: o },
                BatchOp::Write {
                    policy: pp,
                    ops: op,
                },
            ) => !p.send_key && !pp.send_key && p == pp && o == op,
            (
                BatchOp::Udf {
                    policy: p,
                    udf_name: n,
                    function_name: f,
                    args: a,
                },
                BatchOp::Udf {
                    policy: pp,
                    udf_name: np,
                    function_name: fp,
                    args: ap,
                },
            ) => !p.send_key && !pp.send_key && p == pp && n == np && f == fp && a == ap,
            _ => false,
        }
    }

    pub(crate) fn key(&self) -> Key {
        self.br.key.clone()
    }

    /// The parsed record for this operation, if the call found one.
    pub const fn record(&self) -> Option<&Record> {
        self.batch_record().record.as_ref()
    }

    /// Moves the parsed record out of this operation, leaving `None`.
    pub const fn take_record(&mut self) -> Option<Record> {
        self.record_mut().record.take()
    }

    /// The per-key result code, `None` if the operation was never executed.
    /// See [`BatchRecord::result_code`].
    pub fn result_code(&self) -> Option<ResultCode> {
        self.batch_record().result_code()
    }

    /// Whether a write may have been applied despite an error.
    pub fn in_doubt(&self) -> bool {
        self.batch_record().in_doubt()
    }

    /// The failure behind this row, see [`BatchRecord::error`].
    #[must_use]
    pub const fn error(&self) -> Option<&Error> {
        self.batch_record().error()
    }

    /// The node behind this row's outcome, see [`BatchRecord::node`].
    #[must_use]
    pub fn node(&self) -> Option<&str> {
        self.batch_record().node()
    }

    /// Clears any result from a previous execution, so a reused operation
    /// starts a call with a clean row.
    pub(crate) fn clear_result(&mut self) {
        self.record_mut().clear();
    }

    /// A cheap, allocation-free stand-in swapped into a caller's slice while
    /// its row is travelling through the batch engine.
    pub(crate) fn placeholder() -> Self {
        let key = Key {
            namespace: String::new(),
            set_name: String::new(),
            user_key: None,
            digest: [0; 20],
        };
        Self {
            br: BatchRecord::new(key, false),
            kind: BatchOp::Read {
                policy: BatchReadPolicy::default(),
                bins: Bins::None,
                ops: None,
            },
        }
    }

    /// The operation's batch record: its key, and after execution its
    /// result. Borrowed — the record lives inside the operation.
    pub const fn batch_record(&self) -> &BatchRecord {
        &self.br
    }

    pub(crate) fn set_record(&mut self, record: Option<Record>) {
        self.record_mut().set_ok(record);
    }

    pub(crate) const fn record_mut(&mut self) -> &mut BatchRecord {
        &mut self.br
    }

    /// Record this row's failure, see [`BatchRecord::set_error`].
    pub(crate) fn set_error(&mut self, error: Error) {
        self.record_mut().set_error(error);
    }

    /// Stamp this row with a command-level failure if the server never
    /// answered it, see [`BatchRecord::stamp_unanswered`].
    pub(crate) fn stamp_unanswered(&mut self, cause: &Error, txn: Option<&Arc<crate::txn::Txn>>) {
        self.record_mut().stamp_unanswered(cause, txn);
    }
}

#[cfg(test)]
mod repeat_tests {
    use super::*;
    use crate::operations::{self, lists};
    use crate::Bins;

    fn key(n: i64) -> Key {
        Key::new("ns", "set", crate::Value::from(n)).unwrap()
    }

    // Identical writes over the same op list (built once, cloned per key)
    // repeat — the Java `record.equals(prev)` batch compression.
    #[test]
    fn write_repeats_for_shared_op_list() {
        let policy = BatchWritePolicy::default();
        let ops = vec![
            operations::put(&as_bin!("a", 1)),
            lists::append(&lists::ListPolicy::default(), "l", crate::Value::from(1)),
        ];
        let w1 = BatchOperation::write(&policy, key(1), ops.clone());
        let w2 = BatchOperation::write(&policy, key(2), ops);
        assert!(w2.match_header(Some(&w1), None, None));
    }

    // Scalar-only op lists compare fully by content, so even separately
    // constructed identical lists repeat.
    #[test]
    fn write_repeats_for_equal_scalar_ops() {
        let policy = BatchWritePolicy::default();
        let w1 = BatchOperation::write(&policy, key(1), vec![operations::put(&as_bin!("a", 1))]);
        let w2 = BatchOperation::write(&policy, key(2), vec![operations::put(&as_bin!("a", 1))]);
        assert!(w2.match_header(Some(&w1), None, None));
    }

    // Independently constructed CDT ops carry distinct encoder Arcs, so
    // equality cannot be proven — conservatively no repeat.
    #[test]
    fn write_does_not_repeat_for_separately_built_cdt_ops() {
        let policy = BatchWritePolicy::default();
        let cdt = |v: i64| {
            vec![lists::append(
                &lists::ListPolicy::default(),
                "l",
                crate::Value::from(v),
            )]
        };
        let w1 = BatchOperation::write(&policy, key(1), cdt(1));
        let w2 = BatchOperation::write(&policy, key(2), cdt(1));
        assert!(!w2.match_header(Some(&w1), None, None));
    }

    // send_key forces a full header (the user_key field differs per record).
    #[test]
    fn write_does_not_repeat_with_send_key() {
        let mut policy = BatchWritePolicy::default();
        policy.send_key = true;
        let ops = vec![operations::put(&as_bin!("a", 1))];
        let w1 = BatchOperation::write(&policy, key(1), ops.clone());
        let w2 = BatchOperation::write(&policy, key(2), ops);
        assert!(!w2.match_header(Some(&w1), None, None));
    }

    // Different write payloads and different namespaces never repeat.
    #[test]
    fn write_does_not_repeat_across_payload_or_namespace() {
        let policy = BatchWritePolicy::default();
        let w1 = BatchOperation::write(&policy, key(1), vec![operations::put(&as_bin!("a", 1))]);
        let w2 = BatchOperation::write(&policy, key(2), vec![operations::put(&as_bin!("a", 2))]);
        assert!(!w2.match_header(Some(&w1), None, None));

        let other_ns = Key::new("other", "set", crate::Value::from(3)).unwrap();
        let ops = vec![operations::put(&as_bin!("a", 1))];
        let w3 = BatchOperation::write(&policy, other_ns, ops.clone());
        let w4 = BatchOperation::write(&policy, key(4), ops);
        assert!(!w4.match_header(Some(&w3), None, None));
    }

    // Identical UDF invocations repeat; different args do not.
    #[test]
    fn udf_repeats_for_equal_invocations() {
        let policy = BatchUDFPolicy::default();
        let args = Some(vec![crate::Value::from(1)]);
        let u1 = BatchOperation::udf(&policy, key(1), "pkg", "fun", args.clone());
        let u2 = BatchOperation::udf(&policy, key(2), "pkg", "fun", args);
        assert!(u2.match_header(Some(&u1), None, None));

        let u3 = BatchOperation::udf(
            &policy,
            key(3),
            "pkg",
            "fun",
            Some(vec![crate::Value::from(2)]),
        );
        assert!(!u3.match_header(Some(&u2), None, None));
    }

    // Reads with op lists (not just bins) now repeat too.
    #[test]
    fn read_ops_repeat_for_shared_op_list() {
        let policy = BatchReadPolicy::default();
        let ops = vec![operations::get_bin("a")];
        let r1 = BatchOperation::read_ops(&policy, key(1), ops.clone());
        let r2 = BatchOperation::read_ops(&policy, key(2), ops);
        assert!(r2.match_header(Some(&r1), None, None));

        // Mixed shapes never repeat.
        let r3 = BatchOperation::read(&policy, key(3), Bins::All);
        assert!(!r3.match_header(Some(&r2), None, None));
    }

    // Differing txn read versions force full headers.
    #[test]
    fn txn_version_mismatch_prevents_repeat() {
        let policy = BatchWritePolicy::default();
        let ops = vec![operations::put(&as_bin!("a", 1))];
        let w1 = BatchOperation::write(&policy, key(1), ops.clone());
        let w2 = BatchOperation::write(&policy, key(2), ops);
        assert!(!w2.match_header(Some(&w1), Some(7), None));
        assert!(w2.match_header(Some(&w1), Some(7), Some(7)));
    }
}

#[cfg(test)]
mod in_doubt_tests {
    use super::*;
    use crate::txn::Txn;

    fn key(n: &str) -> Key {
        Key::new("ns", "set", crate::Value::from(n)).unwrap()
    }

    fn write_op(k: &str) -> BatchOperation {
        BatchOperation::write(&crate::BatchWritePolicy::default(), key(k), vec![])
    }

    fn read_op(k: &str) -> BatchOperation {
        BatchOperation::read(&crate::BatchReadPolicy::default(), key(k), Bins::All)
    }

    #[test]
    fn batch_record_exposes_has_write() {
        assert!(write_op("w").batch_record().has_write());
        assert!(!read_op("r").batch_record().has_write());
    }

    #[test]
    fn error_detail_defaults_to_absent_and_is_readable_once_set() {
        let mut w = write_op("w");
        let br = w.batch_record();
        assert_eq!(br.sub_code(), crate::server_error::sub_code::NONE);
        assert!(br.server_message().is_none());
        assert!(br.error_detail().is_none());
        assert!(br.error().is_none());
        assert!(br.node().is_none());

        let detail = crate::ServerErrorDetail {
            sub_code: 7,
            message: "count op on non-hll bin".to_string(),
            exp_trace: None,
        };
        w.set_error(Error::server_error(
            ResultCode::BinNotFound,
            "A1: 10.0.0.1:3000",
            Some(Box::new(detail)),
        ));

        let br = w.batch_record();
        assert_eq!(br.result_code(), Some(ResultCode::BinNotFound));
        assert_eq!(br.sub_code(), 7);
        assert_eq!(br.server_message(), Some("count op on non-hll bin"));
        assert_eq!(br.node(), Some("A1: 10.0.0.1:3000"));
        assert!(br.error().unwrap().matches(&[ResultCode::BinNotFound]));
    }

    #[test]
    fn empty_server_message_reads_as_absent() {
        // Verbosity 1 can carry a subcode with no text; `server_message` must
        // not hand back an empty string, matching `Error::server_message`.
        let mut w = write_op("w");
        let detail = crate::ServerErrorDetail {
            sub_code: 3,
            message: String::new(),
            exp_trace: None,
        };
        w.set_error(Error::server_error(
            ResultCode::BinNotFound,
            "A1",
            Some(Box::new(detail)),
        ));

        let br = w.batch_record();
        assert_eq!(br.sub_code(), 3);
        assert!(br.server_message().is_none());
    }

    #[test]
    fn in_doubt_is_read_off_the_error_and_only_for_writes() {
        let in_doubt_timeout = || Error::timeout("Timeout").set_in_doubt(true, 1);

        let mut w = write_op("w");
        w.set_error(in_doubt_timeout());
        assert!(w.batch_record().in_doubt());
        assert_eq!(
            w.result_code(),
            Some(ResultCode::Timeout),
            "a client timeout reads as TIMEOUT like the other clients"
        );

        let mut r = read_op("r");
        r.set_error(in_doubt_timeout());
        assert!(
            !r.batch_record().in_doubt(),
            "a read is never in doubt, whatever its error says"
        );
    }

    #[test]
    fn a_client_failure_without_a_server_code_reads_as_none() {
        let mut w = write_op("w");
        w.set_error(Error::connection("reset by peer"));
        assert_eq!(w.result_code(), None);
        assert!(w.error().is_some(), "but the failure itself is there");
    }

    #[test]
    fn no_response_write_becomes_in_doubt_and_notifies_txn() {
        let txn = Arc::new(Txn::new());
        let cause = Error::timeout("Command timed out").set_in_doubt(true, 1);
        let mut w = write_op("w");
        w.stamp_unanswered(&cause, Some(&txn));

        let br = w.batch_record();
        assert!(br.in_doubt());
        assert_eq!(br.result_code(), Some(ResultCode::Timeout));
        assert!(txn.write_in_doubt());
    }

    #[test]
    fn no_response_read_is_never_in_doubt() {
        let txn = Arc::new(Txn::new());
        let cause = Error::timeout("Command timed out").set_in_doubt(true, 1);
        let mut r = read_op("r");
        r.stamp_unanswered(&cause, Some(&txn));

        let br = r.batch_record();
        assert!(!br.in_doubt());
        assert!(
            !br.error().unwrap().in_doubt(),
            "the read row's copy of the cause is cleared, not just hidden"
        );
        assert!(!txn.write_in_doubt());
    }

    #[test]
    #[cfg(feature = "serialization")]
    fn a_row_serializes_field_for_field_with_a_structured_error() {
        let mut w = write_op("w");
        let detail = crate::ServerErrorDetail {
            sub_code: 2,
            message: "filtered".into(),
            exp_trace: None,
        };
        w.set_record(None);
        w.set_error(
            Error::server_error(
                ResultCode::FilteredOut,
                "A1: h:3000",
                Some(Box::new(detail)),
            )
            .with_retry_context(1, None, vec![Error::timeout("first try")]),
        );
        let json: serde_json::Value = serde_json::to_value(w.batch_record()).unwrap();

        assert!(
            json["record"].is_null(),
            "a failure drops the earlier record"
        );
        assert_eq!(json["has_write"], true);
        let e = &json["error"];
        assert_eq!(e["kind"], "Server");
        assert_eq!(e["result_code"], 27);
        assert_eq!(e["message"], "Command filtered out, Detail: filtered");
        assert_eq!(e["node"], "A1: h:3000");
        assert_eq!(e["iteration"], 1);
        assert_eq!(e["in_doubt"], false);
        assert_eq!(e["server_error_detail"]["sub_code"], 2);
        assert_eq!(e["sub_errors"][0]["kind"], "Timeout");
        assert!(e["source"].is_null());
        assert_eq!(
            json.as_object().unwrap().len(),
            4,
            "key, record, error, has_write — nothing derived"
        );

        let mut ok = write_op("ok");
        ok.set_record(None);
        let json: serde_json::Value = serde_json::to_value(ok.batch_record()).unwrap();
        assert!(json["error"].is_null());
        assert!(json["record"].is_object(), "an answered row has a record");
    }

    #[test]
    fn an_answered_row_is_not_restamped_and_clear_resets_everything() {
        let mut w = write_op("w");
        w.set_record(None);
        w.stamp_unanswered(&Error::timeout("late").set_in_doubt(true, 1), None);
        assert_eq!(
            w.result_code(),
            Some(ResultCode::Ok),
            "an answered row keeps its answer"
        );

        w.set_error(Error::server_error(ResultCode::KeyBusy, "A1", None));
        w.clear_result();
        let br = w.batch_record();
        assert_eq!(br.result_code(), None);
        assert!(br.error().is_none() && br.node().is_none() && !br.in_doubt());
    }

    #[test]
    fn responded_write_is_not_marked_by_the_no_response_walk() {
        // A record that already carries a result code got a definitive answer
        // for the final attempt; the terminal walk must not touch it.
        let txn = Arc::new(Txn::new());
        let mut w = write_op("w");
        w.set_error(Error::server_error_bare(ResultCode::KeyNotFoundError));
        w.stamp_unanswered(&Error::timeout("late").set_in_doubt(true, 1), Some(&txn));

        let br = w.batch_record();
        assert!(!br.in_doubt());
        assert!(!txn.write_in_doubt());
    }
}
