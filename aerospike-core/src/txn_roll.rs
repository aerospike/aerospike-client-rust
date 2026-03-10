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

use crate::cluster::Cluster;
use crate::commands::buffer::{INFO4_MRT_ROLL_BACK, INFO4_MRT_ROLL_FORWARD};
use crate::commands::txn_close_command::TxnCloseCommand;
use crate::commands::txn_mark_roll_forward_command::TxnMarkRollForwardCommand;
use crate::commands::txn_roll_command::TxnRollCommand;
use crate::commands::txn_verify_command::TxnVerifyCommand;
use crate::errors::{Error, Result};
use crate::policy::{BasePolicy, WritePolicy};
use crate::txn::{get_txn_monitor_key, CommitErrorType, CommitStatus, AbortStatus, Txn, TxnState};
use crate::Key;

pub(crate) struct TxnRoll {
    cluster: Arc<Cluster>,
    txn: Arc<Txn>,
}

impl TxnRoll {
    pub fn new(cluster: Arc<Cluster>, txn: Arc<Txn>) -> Self {
        TxnRoll { cluster, txn }
    }

    /// Verify record versions, then set state to Verified.
    /// If verification fails, abort and return an error.
    pub async fn verify(&self, policy: &BasePolicy) -> Result<()> {
        if let Err(err) = self.verify_record_versions(policy).await {
            // Verification failed — abort the transaction.
            // Use unsafe mutable access via Arc since TxnState transitions are single-threaded
            // in the commit/abort path.
            self.set_txn_state(TxnState::Aborted);

            if let Err(_roll_err) = self.roll(policy, INFO4_MRT_ROLL_BACK).await {
                return Err(Error::ClientError(format!(
                    "{}: {}",
                    CommitErrorType::VerifyFailAbortAbandoned,
                    err
                )));
            }

            if self.txn.close_monitor() {
                let wp = self.make_write_policy(policy);
                if let Some(txn_key) = get_txn_monitor_key(&self.txn) {
                    if let Err(_close_err) = self.close(&wp, &txn_key).await {
                        return Err(Error::ClientError(format!(
                            "{}: {}",
                            CommitErrorType::VerifyFailCloseAbandoned,
                            err
                        )));
                    }
                }
            }

            return Err(Error::ClientError(format!(
                "{}: {}",
                CommitErrorType::VerifyFail,
                err
            )));
        }

        self.set_txn_state(TxnState::Verified);
        Ok(())
    }

    /// Commit the transaction: mark roll forward, roll all writes, close monitor.
    pub async fn commit(&self, policy: &BasePolicy) -> Result<CommitStatus> {
        let wp = self.make_write_policy(policy);

        if self.txn.monitor_exists() {
            if let Some(txn_key) = get_txn_monitor_key(&self.txn) {
                if let Err(err) = self.mark_roll_forward(&wp, &txn_key).await {
                    return Err(Error::ClientError(format!(
                        "{}: {}",
                        CommitErrorType::MarkRollForwardAbandoned,
                        err
                    )));
                }
            }
        }

        self.set_txn_state(TxnState::Committed);

        if self.roll(policy, INFO4_MRT_ROLL_FORWARD).await.is_err() {
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
    pub async fn abort(&self, policy: &BasePolicy) -> Result<AbortStatus> {
        self.set_txn_state(TxnState::Aborted);

        if let Err(_err) = self.roll(policy, INFO4_MRT_ROLL_BACK).await {
            return Ok(AbortStatus::RollBackAbandoned);
        }

        if self.txn.close_monitor() {
            let wp = self.make_write_policy(policy);
            if let Some(txn_key) = get_txn_monitor_key(&self.txn) {
                if let Err(_err) = self.close(&wp, &txn_key).await {
                    return Ok(AbortStatus::CloseAbandoned);
                }
            }
        }

        Ok(AbortStatus::Ok)
    }

    /// Verify all record versions by issuing individual verify commands.
    async fn verify_record_versions(&self, policy: &BasePolicy) -> Result<()> {
        let reads = self.txn.get_reads();

        if reads.is_empty() {
            return Ok(());
        }

        let mut last_err: Option<Error> = None;

        for (key, version) in &reads {
            if let Some(ver) = version {
                let mut cmd = TxnVerifyCommand::new(
                    policy,
                    self.cluster.clone(),
                    key,
                    *ver,
                );
                if let Err(err) = cmd.execute().await {
                    last_err = Some(err);
                }
            }
        }

        if let Some(err) = last_err {
            return Err(Error::ClientError(format!(
                "Failed to verify one or more record versions: {err}"
            )));
        }

        Ok(())
    }

    /// Mark the transaction monitor record as roll-forward.
    async fn mark_roll_forward(&self, policy: &WritePolicy, txn_key: &Key) -> Result<()> {
        let mut cmd = TxnMarkRollForwardCommand::new(
            policy,
            self.cluster.clone(),
            txn_key,
        );
        cmd.execute().await
    }

    /// Roll forward or back all written keys.
    async fn roll(&self, policy: &BasePolicy, txn_attr: u8) -> Result<()> {
        let writes = self.txn.get_writes();

        if writes.is_empty() {
            return Ok(());
        }

        let mut last_err: Option<Error> = None;

        for key in &writes {
            let mut cmd = TxnRollCommand::new(
                policy,
                self.cluster.clone(),
                key,
                self.txn.clone(),
                txn_attr,
            );
            if let Err(err) = cmd.execute().await {
                last_err = Some(err);
            }
        }

        if let Some(err) = last_err {
            let action = if txn_attr == INFO4_MRT_ROLL_FORWARD {
                "commit"
            } else {
                "abort"
            };
            return Err(Error::ClientError(format!(
                "Failed to {action} one or more records: {err}"
            )));
        }

        Ok(())
    }

    /// Close (delete) the transaction monitor record and clear the transaction.
    async fn close(&self, policy: &WritePolicy, txn_key: &Key) -> Result<()> {
        let mut cmd = TxnCloseCommand::new(
            policy,
            self.cluster.clone(),
            txn_key,
        );
        cmd.execute().await
    }

    fn make_write_policy(&self, policy: &BasePolicy) -> WritePolicy {
        let mut wp = WritePolicy::default();
        wp.base_policy.socket_timeout = policy.socket_timeout;
        wp.base_policy.total_timeout = policy.total_timeout;
        wp.base_policy.timeout_delay = policy.timeout_delay;
        wp.base_policy.max_retries = policy.max_retries;
        wp.base_policy.sleep_between_retries = policy.sleep_between_retries;
        wp
    }

    fn set_txn_state(&self, state: TxnState) {
        self.txn.set_state(state);
    }
}
