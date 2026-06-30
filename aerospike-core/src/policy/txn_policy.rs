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

use super::{BatchPolicy, BasePolicy, PolicyLike, ReadModeSC, Replica};
#[cfg(feature = "dynamic-config")]
use crate::policy::BatchPolicyConfig;

/// Policy for the *verify* phase of a multi-record transaction — reading and
/// checking the versions of the records that took part in the transaction
/// before it is committed.
///
/// Like the Aerospike Go client's `TxnVerifyPolicy`, this wraps a
/// [`BatchPolicy`]: verification is sent to the server as one batch command per
/// node, so the batch knobs (`concurrency`, `allow_inline`, `respond_all_keys`,
/// `replica`, read modes) all apply.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
pub struct TxnVerifyPolicy {
    /// Batch policy instance.
    #[cfg_attr(feature = "dynamic-config", config(flatten))]
    pub batch_policy: BatchPolicy,
}

impl Default for TxnVerifyPolicy {
    fn default() -> Self {
        // Matches Go's `NewTxnVerifyPolicy`: linearized SC reads, master
        // replica, 5 retries, 3s socket / 10s total timeout, 1s sleep.
        let mut bp = BatchPolicy::default();
        bp.base_policy.read_mode_sc = ReadModeSC::Linearize;
        bp.base_policy.max_retries = 5;
        bp.base_policy.socket_timeout = 3_000;
        bp.base_policy.total_timeout = 10_000;
        bp.base_policy.sleep_between_retries = 1_000;
        bp.replica = Replica::Master;
        Self { batch_policy: bp }
    }
}

impl PolicyLike for TxnVerifyPolicy {
    fn base(&self) -> &BasePolicy {
        &self.batch_policy.base_policy
    }
}

/// Policy for the *roll* phase of a multi-record transaction — rolling records
/// forward on commit or back on abort.
///
/// Like the Aerospike Go client's `TxnRollPolicy`, this wraps a [`BatchPolicy`]:
/// rolling is sent as one batch command per node.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "dynamic-config", derive(aerospike_macro::Config))]
pub struct TxnRollPolicy {
    /// Batch policy instance.
    #[cfg_attr(feature = "dynamic-config", config(flatten))]
    pub batch_policy: BatchPolicy,
}

impl Default for TxnRollPolicy {
    fn default() -> Self {
        // Matches Go's `NewTxnRollPolicy`: master replica, 5 retries,
        // 3s socket / 10s total timeout, 1s sleep between retries.
        let mut bp = BatchPolicy::default();
        bp.base_policy.max_retries = 5;
        bp.base_policy.socket_timeout = 3_000;
        bp.base_policy.total_timeout = 10_000;
        bp.base_policy.sleep_between_retries = 1_000;
        bp.replica = Replica::Master;
        Self { batch_policy: bp }
    }
}

impl PolicyLike for TxnRollPolicy {
    fn base(&self) -> &BasePolicy {
        &self.batch_policy.base_policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_verify_defaults_match_go() {
        let p = TxnVerifyPolicy::default();
        assert_eq!(p.batch_policy.base_policy.read_mode_sc, ReadModeSC::Linearize);
        assert_eq!(p.batch_policy.base_policy.max_retries, 5);
        assert_eq!(p.batch_policy.base_policy.socket_timeout, 3_000);
        assert_eq!(p.batch_policy.base_policy.total_timeout, 10_000);
        assert_eq!(p.batch_policy.base_policy.sleep_between_retries, 1_000);
        assert_eq!(p.batch_policy.replica, Replica::Master);
    }

    #[test]
    fn txn_roll_defaults_match_go() {
        let p = TxnRollPolicy::default();
        assert_eq!(p.batch_policy.base_policy.max_retries, 5);
        assert_eq!(p.batch_policy.base_policy.socket_timeout, 3_000);
        assert_eq!(p.batch_policy.base_policy.total_timeout, 10_000);
        assert_eq!(p.batch_policy.base_policy.sleep_between_retries, 1_000);
        assert_eq!(p.batch_policy.replica, Replica::Master);
        // Roll does not force a read mode — stays at the BasePolicy default.
        assert_eq!(p.batch_policy.base_policy.read_mode_sc, ReadModeSC::Session);
    }
}
