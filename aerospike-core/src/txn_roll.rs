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

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::join_all;

use crate::batch::{BatchOperation, BatchRecord};
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::batch_operate_command::BatchOperateCommand;
use crate::commands::buffer::{INFO4_MRT_ROLL_BACK, INFO4_MRT_ROLL_FORWARD};
use crate::commands::txn_close_command::TxnCloseCommand;
use crate::commands::txn_mark_roll_forward_command::TxnMarkRollForwardCommand;
use crate::commands::txn_roll_command::TxnRollCommand;
use crate::commands::txn_verify_command::TxnVerifyCommand;
use crate::errors::{Error, Result};
use crate::policy::{BatchPolicy, TxnRollPolicy, TxnVerifyPolicy, WritePolicy};
use crate::txn::{get_txn_monitor_key, AbortStatus, CommitErrorType, CommitStatus, Txn, TxnState};
use crate::{Key, ResultCode};

pub struct TxnRoll {
    cluster: Arc<Cluster>,
    txn: Arc<Txn>,
    verify_records: Vec<BatchRecord>,
    roll_records: Vec<BatchRecord>,
}

impl TxnRoll {
    pub const fn new(cluster: Arc<Cluster>, txn: Arc<Txn>) -> Self {
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
        verify_policy: &TxnVerifyPolicy,
        roll_policy: &TxnRollPolicy,
    ) -> Result<()> {
        // Verify/roll run as batch commands (one per node) using the batch policy.
        let verify_policy = &verify_policy.batch_policy;
        let roll_policy = &roll_policy.batch_policy;
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
    pub async fn commit(&mut self, roll_policy: &TxnRollPolicy) -> Result<CommitStatus> {
        let roll_policy = &roll_policy.batch_policy;
        let wp = write_policy_from_base(roll_policy);

        if self.txn.monitor_exists() {
            if let Some(txn_key) = get_txn_monitor_key(&self.txn) {
                if let Err(err) = self.mark_roll_forward(&wp, &txn_key).await {
                    // MRT_ABORTED from the server means it already aborted
                    // this transaction. Flip the client state to Aborted and
                    // clear the in-doubt flag so callers don't treat the
                    // failure as ambiguous.
                    let is_aborted =
                        matches!(&err, Error::ServerError(ResultCode::MrtAborted, _, _));

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

        if self
            .roll(roll_policy, INFO4_MRT_ROLL_FORWARD)
            .await
            .is_err()
        {
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
    pub async fn abort(&mut self, roll_policy: &TxnRollPolicy) -> Result<AbortStatus> {
        let roll_policy = &roll_policy.batch_policy;
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

    /// Verify all record versions using one batch command per node (a single
    /// per-record command for one-key nodes), mirroring the Go client.
    /// Populates `self.verify_records` with per-key outcomes.
    async fn verify_record_versions(&mut self, policy: &BatchPolicy) -> Result<()> {
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

        let keys: Vec<Key> = to_verify.iter().map(|(k, _)| k.clone()).collect();
        let mut records: Vec<BatchRecord> = keys
            .iter()
            .map(|k| BatchRecord::new(k.clone(), false))
            .collect();

        let groups = self.group_by_node(&keys)?;
        let futures = groups.into_iter().map(|(node, idxs)| {
            let cluster = self.cluster.clone();
            let policy = policy.clone();
            let group: Vec<(usize, Key, u64)> = idxs
                .iter()
                .map(|&i| (i, to_verify[i].0.clone(), to_verify[i].1))
                .collect();
            async move { Self::run_verify_group(cluster, policy, node, group).await }
        });

        for group_result in join_all(futures).await {
            for (i, rc) in group_result {
                records[i].result_code = rc;
            }
        }
        self.verify_records = records;

        // Verification passes when every record's version matched (`Ok`) or was
        // simply not verifiable (`KeyNotFound` / `FilteredOut`) — matching Go's
        // batch verify tolerance. Any other code (e.g. version mismatch) or a
        // missing response fails verification.
        let failure = self
            .verify_records
            .iter()
            .find(|r| {
                !matches!(
                    r.result_code,
                    Some(ResultCode::Ok | ResultCode::KeyNotFoundError | ResultCode::FilteredOut)
                )
            })
            .map(|r| r.result_code);
        if let Some(code) = failure {
            return Err(match code {
                Some(rc) => Error::ServerError(rc, false, String::new()),
                None => {
                    Error::Timeout("Verify: no response for one or more records".to_string())
                }
            });
        }
        Ok(())
    }

    /// Groups keys by the node owning their (master) partition, returning
    /// node -> indices into `keys`.
    #[allow(clippy::mutable_key_type)]
    fn group_by_node(&self, keys: &[Key]) -> Result<HashMap<Arc<Node>, Vec<usize>>> {
        let mut groups: HashMap<Arc<Node>, Vec<usize>> = HashMap::new();
        for (i, key) in keys.iter().enumerate() {
            let mut partition = Partition::for_write(key);
            let node = partition.get_node(&self.cluster)?;
            groups.entry(node).or_default().push(i);
        }
        Ok(groups)
    }

    /// Runs verify for one node's key group: a single-record command for a
    /// one-key group, otherwise a batch verify. Returns per-index result codes.
    async fn run_verify_group(
        cluster: Arc<Cluster>,
        policy: BatchPolicy,
        node: Arc<Node>,
        group: Vec<(usize, Key, u64)>,
    ) -> Vec<(usize, Option<ResultCode>)> {
        if group.len() == 1 {
            let (i, key, ver) = &group[0];
            let mut cmd = TxnVerifyCommand::new(&policy.base_policy, cluster, key, *ver);
            let rc = match cmd.execute().await {
                Ok(()) => cmd.result_code.or(Some(ResultCode::Ok)),
                Err(_) => cmd.result_code,
            };
            return vec![(*i, rc)];
        }

        let ops: Vec<(BatchOperation, usize)> = group
            .iter()
            .map(|(i, key, ver)| {
                (
                    BatchOperation::TxnVerify {
                        br: BatchRecord::new(key.clone(), false),
                        version: Some(*ver),
                    },
                    *i,
                )
            })
            .collect();
        let cmd = BatchOperateCommand::new(policy, node, ops);
        match cmd.execute(cluster).await {
            Ok(done) => done
                .batch_ops
                .into_iter()
                .map(|(op, i)| (i, op.batch_record().result_code))
                .collect(),
            Err(_) => group.iter().map(|(i, _, _)| (*i, None)).collect(),
        }
    }

    /// Mark the transaction monitor record as roll-forward.
    async fn mark_roll_forward(&self, policy: &WritePolicy, txn_key: &Key) -> Result<()> {
        let mut cmd = TxnMarkRollForwardCommand::new(policy, self.cluster.clone(), txn_key);
        cmd.execute().await
    }

    /// Roll forward or back all written keys concurrently. Populates
    /// `self.roll_records` with per-key outcomes and `in_doubt` flags.
    async fn roll(&mut self, policy: &BatchPolicy, txn_attr: u8) -> Result<()> {
        let keys = self.txn.get_writes();
        if keys.is_empty() {
            return Ok(());
        }

        let mut records: Vec<BatchRecord> = keys
            .iter()
            .map(|k| BatchRecord::new(k.clone(), true))
            .collect();

        let groups = self.group_by_node(&keys)?;
        let futures = groups.into_iter().map(|(node, idxs)| {
            let cluster = self.cluster.clone();
            let policy = policy.clone();
            let txn = self.txn.clone();
            let group: Vec<(usize, Key)> = idxs.iter().map(|&i| (i, keys[i].clone())).collect();
            async move { Self::run_roll_group(cluster, policy, node, txn, txn_attr, group).await }
        });

        for group_result in join_all(futures).await {
            for (i, rc, in_doubt) in group_result {
                records[i].result_code = rc;
                records[i].in_doubt = in_doubt;
                if in_doubt {
                    self.txn.on_write_in_doubt(&records[i].key);
                }
            }
        }
        self.roll_records = records;

        let failure = self
            .roll_records
            .iter()
            .find(|r| r.result_code != Some(ResultCode::Ok))
            .map(|r| r.result_code);
        if let Some(code) = failure {
            let action = if txn_attr == INFO4_MRT_ROLL_FORWARD {
                "commit"
            } else {
                "abort"
            };
            return Err(match code {
                Some(rc) => {
                    Error::ServerError(rc, false, format!("Failed to {action} one or more records"))
                }
                None => Error::Timeout(format!(
                    "Failed to {action}: no response for one or more records"
                )),
            });
        }
        Ok(())
    }

    /// Runs roll for one node's key group: single-record command for a one-key
    /// group, otherwise a batch roll. Returns per-index (result code, in_doubt).
    async fn run_roll_group(
        cluster: Arc<Cluster>,
        policy: BatchPolicy,
        node: Arc<Node>,
        txn: Arc<Txn>,
        roll_attr: u8,
        group: Vec<(usize, Key)>,
    ) -> Vec<(usize, Option<ResultCode>, bool)> {
        if group.len() == 1 {
            let (i, key) = &group[0];
            let mut cmd = TxnRollCommand::new(&policy.base_policy, cluster, key, txn, roll_attr);
            return match cmd.execute().await {
                Ok(()) => vec![(*i, cmd.result_code.or(Some(ResultCode::Ok)), false)],
                Err(Error::Timeout(_)) => vec![(*i, cmd.result_code, true)],
                Err(_) => vec![(*i, cmd.result_code, false)],
            };
        }

        let ops: Vec<(BatchOperation, usize)> = group
            .iter()
            .map(|(i, key)| {
                (
                    BatchOperation::TxnRoll {
                        br: BatchRecord::new(key.clone(), true),
                        txn: txn.clone(),
                        roll_attr,
                    },
                    *i,
                )
            })
            .collect();
        let cmd = BatchOperateCommand::new(policy, node, ops);
        match cmd.execute(cluster).await {
            Ok(done) => done
                .batch_ops
                .into_iter()
                .map(|(op, i)| {
                    let br = op.batch_record();
                    (i, br.result_code, br.in_doubt)
                })
                .collect(),
            // Whole node-group failed (e.g. timeout after retries): treat every
            // write in the group as in-doubt, matching Go's no-response rule.
            Err(_) => group.iter().map(|(i, _)| (*i, None, true)).collect(),
        }
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

fn write_policy_from_base(policy: &BatchPolicy) -> WritePolicy {
    let base = &policy.base_policy;
    let mut wp = WritePolicy::default();
    wp.base_policy.socket_timeout = base.socket_timeout;
    wp.base_policy.total_timeout = base.total_timeout;
    wp.base_policy.timeout_delay = base.timeout_delay;
    wp.base_policy.max_retries = base.max_retries;
    wp.base_policy.sleep_between_retries = base.sleep_between_retries;
    wp.base_policy.sleep_multiplier = base.sleep_multiplier;
    wp.base_policy.use_compression = base.use_compression;
    wp
}
