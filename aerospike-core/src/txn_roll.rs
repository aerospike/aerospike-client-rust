// Copyright 2015-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Transaction roll: orchestrates verify, commit, and abort for MRT.

use std::sync::Arc;

use futures::future::join_all;

use crate::batch::BatchRecord;
use crate::cluster::Cluster;
use crate::commands::buffer::{INFO4_MRT_ROLL_BACK, INFO4_MRT_ROLL_FORWARD};
use crate::commands::txn_close_command::TxnCloseCommand;
use crate::commands::txn_mark_roll_forward_command::TxnMarkRollForwardCommand;
use crate::commands::txn_roll_command::TxnRollCommand;
use crate::commands::txn_verify_command::TxnVerifyCommand;
use crate::errors::{Error, Result};
use crate::policy::{BasePolicy, WritePolicy};
use crate::txn::{
    get_txn_monitor_key, AbortStatus, CommitErrorType, CommitStatus, Txn, TxnState,
};
use crate::{Key, ResultCode};

pub(crate) struct TxnRoll {
    cluster: Arc<Cluster>,
    txn: Arc<Txn>,
    verify_records: Vec<BatchRecord>,
    roll_records: Vec<BatchRecord>,
}

impl TxnRoll {
    pub fn new(cluster: Arc<Cluster>, txn: Arc<Txn>) -> Self {
        TxnRoll {
            cluster,
            txn,
            verify_records: Vec::new(),
            roll_records: Vec::new(),
        }
    }

    /// Verify record versions, then set state to Verified.
    /// If verification fails, abort the transaction before returning an error.
    pub async fn verify(
        &mut self,
        verify_policy: &BasePolicy,
        roll_policy: &BasePolicy,
    ) -> Result<()> {
        if let Err(err) = self.verify_record_versions(verify_policy).await {
            // Verification failed — roll back the transaction.
            self.txn.set_state(TxnState::Aborted);

            if let Err(_roll_err) = self.roll(roll_policy, INFO4_MRT_ROLL_BACK).await {
                return Err(self.make_commit_error(
                    CommitErrorType::VerifyFailAbortAbandoned,
                    self.txn.in_doubt(),
                    Some(err),
                ));
            }

            if self.txn.close_monitor() {
                let wp = write_policy_from_base(roll_policy);
                if let Some(txn_key) = get_txn_monitor_key(&self.txn) {
                    if let Err(_close_err) = self.close(&wp, &txn_key).await {
                        return Err(self.make_commit_error(
                            CommitErrorType::VerifyFailCloseAbandoned,
                            self.txn.in_doubt(),
                            Some(err),
                        ));
                    }
                }
            }

            return Err(self.make_commit_error(
                CommitErrorType::VerifyFail,
                self.txn.in_doubt(),
                Some(err),
            ));
        }

        self.txn.set_state(TxnState::Verified);
        Ok(())
    }

    /// Commit the transaction: mark roll forward, roll all writes, close monitor.
    pub async fn commit(&mut self, roll_policy: &BasePolicy) -> Result<CommitStatus> {
        let wp = write_policy_from_base(roll_policy);

        if self.txn.monitor_exists() {
            if let Some(txn_key) = get_txn_monitor_key(&self.txn) {
                if let Err(err) = self.mark_roll_forward(&wp, &txn_key).await {
                    // MRT_ABORTED from the server means it already aborted
                    // this transaction. Flip the client state to Aborted and
                    // clear the in-doubt flag so callers don't treat the
                    // failure as ambiguous.
                    let is_aborted = matches!(
                        &err,
                        Error::ServerError(ResultCode::MrtAborted, _, _)
                    );

                    if is_aborted {
                        self.txn.set_in_doubt(false);
                        self.txn.set_state(TxnState::Aborted);
                        return Err(self.make_commit_error(
                            CommitErrorType::MarkRollForwardAbandoned,
                            false,
                            Some(err),
                        ));
                    }

                    // Propagate in-doubt: if the txn or this attempt was
                    // already in doubt, keep that flag on the commit failure.
                    let in_doubt = if self.txn.in_doubt() {
                        true
                    } else if matches!(&err, Error::Timeout(_)) {
                        self.txn.set_in_doubt(true);
                        true
                    } else {
                        false
                    };

                    return Err(self.make_commit_error(
                        CommitErrorType::MarkRollForwardAbandoned,
                        in_doubt,
                        Some(err),
                    ));
                }
            }
        }

        self.txn.set_state(TxnState::Committed);
        self.txn.set_in_doubt(false);

        if self.roll(roll_policy, INFO4_MRT_ROLL_FORWARD).await.is_err() {
            return Ok(CommitStatus::RollForwardAbandoned);
        }

        if self.txn.close_monitor() {
            if let Some(txn_key) = get_txn_monitor_key(&self.txn) {
                if let Err(_err) = self.close(&wp, &txn_key).await {
                    return Ok(CommitStatus::CloseAbandoned);
                }
            }
        }

        Ok(CommitStatus::Ok)
    }

    /// Abort the transaction: roll back all writes, close monitor.
    pub async fn abort(&mut self, roll_policy: &BasePolicy) -> Result<AbortStatus> {
        self.txn.set_state(TxnState::Aborted);

        if self.roll(roll_policy, INFO4_MRT_ROLL_BACK).await.is_err() {
            return Ok(AbortStatus::RollBackAbandoned);
        }

        if self.txn.close_monitor() {
            let wp = write_policy_from_base(roll_policy);
            if let Some(txn_key) = get_txn_monitor_key(&self.txn) {
                if let Err(_err) = self.close(&wp, &txn_key).await {
                    return Ok(AbortStatus::CloseAbandoned);
                }
            }
        }

        Ok(AbortStatus::Ok)
    }

    /// Verify all record versions concurrently. Populates `self.verify_records`
    /// with per-key outcomes even on failure so callers can inspect.
    async fn verify_record_versions(&mut self, policy: &BasePolicy) -> Result<()> {
        let reads = self.txn.get_reads();
        if reads.is_empty() {
            return Ok(());
        }

        // Collect the verifiable reads (those that recorded a version).
        let to_verify: Vec<(Key, u64)> = reads
            .iter()
            .filter_map(|(k, v)| v.map(|ver| (k.clone(), ver)))
            .collect();

        if to_verify.is_empty() {
            return Ok(());
        }

        let mut records: Vec<BatchRecord> = to_verify
            .iter()
            .map(|(k, _)| BatchRecord::new(k.clone(), false))
            .collect();

        let futures = to_verify.iter().map(|(key, version)| {
            let cluster = self.cluster.clone();
            async move {
                let mut cmd = TxnVerifyCommand::new(policy, cluster, key, *version);
                let res = cmd.execute().await;
                (cmd.result_code, res)
            }
        });
        let outcomes: Vec<(Option<ResultCode>, Result<()>)> = join_all(futures).await;

        let mut last_err: Option<Error> = None;
        for (i, (rc, res)) in outcomes.into_iter().enumerate() {
            match res {
                Ok(()) => {
                    records[i].result_code = rc.or(Some(ResultCode::Ok));
                }
                Err(e) => {
                    records[i].result_code = rc;
                    last_err = Some(e);
                }
            }
        }

        let any_failed = records
            .iter()
            .any(|r| r.result_code != Some(ResultCode::Ok));
        self.verify_records = records;

        if any_failed {
            return Err(last_err.unwrap_or_else(|| {
                Error::ClientError("Failed to verify one or more record versions".to_string())
            }));
        }

        Ok(())
    }

    /// Mark the transaction monitor record as roll-forward.
    async fn mark_roll_forward(&self, policy: &WritePolicy, txn_key: &Key) -> Result<()> {
        let mut cmd = TxnMarkRollForwardCommand::new(policy, self.cluster.clone(), txn_key);
        cmd.execute().await
    }

    /// Roll forward or back all written keys concurrently. Populates
    /// `self.roll_records` with per-key outcomes and `in_doubt` flags.
    async fn roll(&mut self, policy: &BasePolicy, txn_attr: u8) -> Result<()> {
        let writes = self.txn.get_writes();
        if writes.is_empty() {
            return Ok(());
        }

        let mut records: Vec<BatchRecord> = writes
            .iter()
            .map(|k| BatchRecord::new(k.clone(), true))
            .collect();

        let futures = writes.iter().map(|key| {
            let cluster = self.cluster.clone();
            let txn = self.txn.clone();
            async move {
                let mut cmd = TxnRollCommand::new(policy, cluster, key, txn, txn_attr);
                let res = cmd.execute().await;
                (cmd.result_code, res)
            }
        });
        let outcomes: Vec<(Option<ResultCode>, Result<()>)> = join_all(futures).await;

        let mut last_err: Option<Error> = None;
        for (i, (rc, res)) in outcomes.into_iter().enumerate() {
            match res {
                Ok(()) => {
                    records[i].result_code = rc.or(Some(ResultCode::Ok));
                }
                Err(e) => {
                    records[i].result_code = rc;
                    // Timeouts mean the write may or may not have been applied
                    // by the server — mark this record as in_doubt so the
                    // caller can request recovery.
                    if matches!(&e, Error::Timeout(_)) {
                        records[i].in_doubt = true;
                        self.txn.on_write_in_doubt(&records[i].key);
                    }
                    last_err = Some(e);
                }
            }
        }

        let any_failed = records
            .iter()
            .any(|r| r.result_code != Some(ResultCode::Ok));
        self.roll_records = records;

        if any_failed {
            let action = if txn_attr == INFO4_MRT_ROLL_FORWARD {
                "commit"
            } else {
                "abort"
            };
            return Err(last_err.unwrap_or_else(|| {
                Error::ClientError(format!(
                    "Failed to {action} one or more records"
                ))
            }));
        }

        Ok(())
    }

    /// Close (delete) the transaction monitor record on the server and clear
    /// the client-side transaction state.
    async fn close(&self, policy: &WritePolicy, txn_key: &Key) -> Result<()> {
        let mut cmd = TxnCloseCommand::new(policy, self.cluster.clone(), txn_key);
        cmd.execute().await?;

        // Mirror the Java client: after successful close, wipe client-side
        // state so the Txn can't be accidentally reused with stale data.
        self.txn.clear();
        Ok(())
    }

    fn make_commit_error(
        &self,
        error_type: CommitErrorType,
        in_doubt: bool,
        source: Option<Error>,
    ) -> Error {
        Error::CommitFailed {
            error_type,
            verify_records: self.verify_records.clone(),
            roll_records: self.roll_records.clone(),
            in_doubt,
            source: source.map(Box::new),
        }
    }
}

fn write_policy_from_base(base: &BasePolicy) -> WritePolicy {
    let mut wp = WritePolicy::default();
    wp.base_policy.socket_timeout = base.socket_timeout;
    wp.base_policy.total_timeout = base.total_timeout;
    wp.base_policy.timeout_delay = base.timeout_delay;
    wp.base_policy.max_retries = base.max_retries;
    wp.base_policy.sleep_between_retries = base.sleep_between_retries;
    wp.base_policy.use_compression = base.use_compression;
    wp
}
