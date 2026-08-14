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
    /// Creates a batch read operation.
    pub fn read(policy: &BatchReadPolicy, key: Key, bins: Bins) -> Self {
        BatchOperation::Read {
            br: BatchRecord::new(key, false),
            policy: policy.clone(),
            bins,
            ops: None,
        }
    }

    /// Creates a batch read with multiple operations.
    pub fn read_ops(policy: &BatchReadPolicy, key: Key, ops: Vec<Operation>) -> Self {
        BatchOperation::Read {
            br: BatchRecord::new(key, false),
            policy: policy.clone(),
            bins: Bins::None,
            ops: Some(ops),
        }
    }

    /// Creates a batch write with multiple operations.
    pub fn write(policy: &BatchWritePolicy, key: Key, ops: Vec<Operation>) -> Self {
        BatchOperation::Write {
            br: BatchRecord::new(key, true),
            policy: policy.clone(),
            ops,
        }
    }

    /// Creates a batch delete operation.
    pub fn delete(policy: &BatchDeletePolicy, key: Key) -> Self {
        BatchOperation::Delete {
            br: BatchRecord::new(key, true),
            policy: policy.clone(),
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
        BatchOperation::UDF {
            br: BatchRecord::new(key, true),
            policy: policy.clone(),
            udf_name: udf_name.into(),
            function_name: function_name.into(),
            args,
        }
    }

    pub(crate) fn size(&self, parent_fe: Option<&Expression>) -> Result<usize> {
        match self {
            Self::Read {
                policy, bins, ops, ..
            } => {
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
                    return Err(Error::ClientError(
                        "Batch write operations do not contain a write".into(),
                    ));
                }
                Ok(size)
            }
            Self::Delete { br, policy } => {
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
            Self::UDF {
                br,
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
        }
    }

    /// `true` when this record's per-record header would be byte-identical to
    /// `prev`'s, so the wire can carry the `BATCH_MSG_REPEAT` flag — offset and
    /// digest only — instead of repeating the header.
    ///
    /// This is where the batch encoder's `REPEAT` support gets its answer; while
    /// it returned a constant `false`, every record in every batch re-sent a
    /// full header: the 12-byte header plus namespace, set and the whole bin/op
    /// payload, against one byte.
    ///
    /// Conservative by construction. `send_key` forces a full header because the
    /// user-key field differs per record, namespace/set must match because those
    /// fields are what the repeat omits, and operation lists compare by content
    /// with their opaque encoder closures compared by `Arc` identity — so the
    /// natural "build the ops once, apply to N keys" pattern repeats, while
    /// anything not provably identical writes a full header.
    pub(crate) fn match_header(&self, prev: Option<&BatchOperation>) -> bool {
        let Some(prev) = prev else { return false };

        // Same namespace and set: a repeat omits both fields.
        if self.key_ref().namespace != prev.key_ref().namespace
            || self.key_ref().set_name != prev.key_ref().set_name
        {
            return false;
        }

        match (self, prev) {
            (
                Self::Read {
                    policy: p,
                    bins: b,
                    ops: o,
                    ..
                },
                Self::Read {
                    policy: pp,
                    bins: bp,
                    ops: op,
                    ..
                },
            ) => p == pp && b == bp && o == op,
            (Self::Delete { policy: p, .. }, Self::Delete { policy: pp, .. }) => {
                !p.send_key && !pp.send_key && p == pp
            }
            (
                Self::Write {
                    policy: p, ops: o, ..
                },
                Self::Write {
                    policy: pp,
                    ops: op,
                    ..
                },
            ) => !p.send_key && !pp.send_key && p == pp && o == op,
            (
                Self::UDF {
                    policy: p,
                    udf_name: n,
                    function_name: f,
                    args: a,
                    ..
                },
                Self::UDF {
                    policy: pp,
                    udf_name: np,
                    function_name: fp,
                    args: ap,
                    ..
                },
            ) => !p.send_key && !pp.send_key && p == pp && n == np && f == fp && a == ap,
            _ => false,
        }
    }

    /// Borrow this record's key. [`Self::key`] clones it, which the encoder
    /// cannot afford to do per record.
    pub(crate) const fn key_ref(&self) -> &Key {
        match self {
            Self::Read { br, .. }
            | Self::Write { br, .. }
            | Self::Delete { br, .. }
            | Self::UDF { br, .. } => &br.key,
        }
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
            Self::Write { br, .. } | Self::Delete { br, .. } | Self::UDF { br, .. } => {
                br.result_code = Some(rc);
                br.in_doubt = in_doubt;
            }
        }
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

    // Identical writes over one op list (built once, cloned per key) repeat —
    // the Java client's `record.equals(prev)` batch compression.
    #[test]
    fn write_repeats_for_shared_op_list() {
        let policy = BatchWritePolicy::default();
        let ops = vec![
            operations::put(&as_bin!("a", 1)),
            lists::append(&lists::ListPolicy::default(), "l", crate::Value::from(1)),
        ];
        let w1 = BatchOperation::write(&policy, key(1), ops.clone());
        let w2 = BatchOperation::write(&policy, key(2), ops);
        assert!(w2.match_header(Some(&w1)));
    }

    // Scalar-only op lists compare fully by content, so even separately built
    // identical lists repeat.
    #[test]
    fn write_repeats_for_equal_scalar_ops() {
        let policy = BatchWritePolicy::default();
        let w1 = BatchOperation::write(&policy, key(1), vec![operations::put(&as_bin!("a", 1))]);
        let w2 = BatchOperation::write(&policy, key(2), vec![operations::put(&as_bin!("a", 1))]);
        assert!(w2.match_header(Some(&w1)));
    }

    // Independently constructed CDT ops carry distinct encoder Arcs, so equality
    // cannot be proven — conservatively no repeat.
    #[test]
    fn write_does_not_repeat_for_separately_built_cdt_ops() {
        let policy = BatchWritePolicy::default();
        let cdt = |v: i64| vec![lists::append(&lists::ListPolicy::default(), "l", crate::Value::from(v))];
        let w1 = BatchOperation::write(&policy, key(1), cdt(1));
        let w2 = BatchOperation::write(&policy, key(2), cdt(1));
        assert!(!w2.match_header(Some(&w1)));
    }

    // send_key forces a full header: the user-key field differs per record.
    #[test]
    fn write_does_not_repeat_with_send_key() {
        let policy = BatchWritePolicy {
            send_key: true,
            ..BatchWritePolicy::default()
        };
        let ops = vec![operations::put(&as_bin!("a", 1))];
        let w1 = BatchOperation::write(&policy, key(1), ops.clone());
        let w2 = BatchOperation::write(&policy, key(2), ops);
        assert!(!w2.match_header(Some(&w1)));
    }

    // Different payloads, and different namespaces, never repeat.
    #[test]
    fn write_does_not_repeat_across_payload_or_namespace() {
        let policy = BatchWritePolicy::default();
        let w1 = BatchOperation::write(&policy, key(1), vec![operations::put(&as_bin!("a", 1))]);
        let w2 = BatchOperation::write(&policy, key(2), vec![operations::put(&as_bin!("a", 2))]);
        assert!(!w2.match_header(Some(&w1)));

        let other_ns = Key::new("other", "set", crate::Value::from(3)).unwrap();
        let ops = vec![operations::put(&as_bin!("a", 1))];
        let w3 = BatchOperation::write(&policy, other_ns, ops.clone());
        let w4 = BatchOperation::write(&policy, key(4), ops);
        assert!(!w4.match_header(Some(&w3)));
    }

    // The first record of a batch has nothing to repeat.
    #[test]
    fn first_record_never_repeats() {
        let policy = BatchReadPolicy::default();
        let r1 = BatchOperation::read(&policy, key(1), Bins::All);
        assert!(!r1.match_header(None));
    }

    // Plain reads over the same bins repeat.
    #[test]
    fn read_repeats_for_equal_bins() {
        let policy = BatchReadPolicy::default();
        let r1 = BatchOperation::read(&policy, key(1), Bins::All);
        let r2 = BatchOperation::read(&policy, key(2), Bins::All);
        assert!(r2.match_header(Some(&r1)));

        let r3 = BatchOperation::read(&policy, key(3), Bins::from(["a"]));
        assert!(!r3.match_header(Some(&r2)));
    }

    // Reads carrying op lists repeat too, and never against a different shape.
    #[test]
    fn read_ops_repeat_for_shared_op_list() {
        let policy = BatchReadPolicy::default();
        let ops = vec![operations::get_bin("a")];
        let r1 = BatchOperation::read_ops(&policy, key(1), ops.clone());
        let r2 = BatchOperation::read_ops(&policy, key(2), ops);
        assert!(r2.match_header(Some(&r1)));

        let r3 = BatchOperation::read(&policy, key(3), Bins::All);
        assert!(!r3.match_header(Some(&r2)));
    }

    // Identical UDF invocations repeat; different args do not.
    #[test]
    fn udf_repeats_for_equal_invocations() {
        let policy = BatchUDFPolicy::default();
        let args = Some(vec![crate::Value::from(1)]);
        let u1 = BatchOperation::udf(&policy, key(1), "pkg", "fun", args.clone());
        let u2 = BatchOperation::udf(&policy, key(2), "pkg", "fun", args);
        assert!(u2.match_header(Some(&u1)));

        let u3 = BatchOperation::udf(
            &policy,
            key(3),
            "pkg",
            "fun",
            Some(vec![crate::Value::from(2)]),
        );
        assert!(!u3.match_header(Some(&u2)));
    }

    // Deletes repeat unless send_key is set.
    #[test]
    fn delete_repeats_unless_send_key() {
        let policy = BatchDeletePolicy::default();
        let d1 = BatchOperation::delete(&policy, key(1));
        let d2 = BatchOperation::delete(&policy, key(2));
        assert!(d2.match_header(Some(&d1)));

        let keyed = BatchDeletePolicy {
            send_key: true,
            ..BatchDeletePolicy::default()
        };
        let d3 = BatchOperation::delete(&keyed, key(3));
        let d4 = BatchOperation::delete(&keyed, key(4));
        assert!(!d4.match_header(Some(&d3)));
    }

    // Variants never repeat across each other, whatever their policies say.
    #[test]
    fn different_variants_never_repeat() {
        let r = BatchOperation::read(&BatchReadPolicy::default(), key(1), Bins::All);
        let d = BatchOperation::delete(&BatchDeletePolicy::default(), key(2));
        let w = BatchOperation::write(
            &BatchWritePolicy::default(),
            key(3),
            vec![operations::put(&as_bin!("a", 1))],
        );
        assert!(!d.match_header(Some(&r)));
        assert!(!w.match_header(Some(&d)));
        assert!(!r.match_header(Some(&w)));
    }
}
