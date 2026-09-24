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

use crate::batch::{BatchHook, BatchOperation};
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::{
    BatchOperateCommand, DeleteCommand, ExecuteUDFCommand, OperateCommand, ReadCommand,
};
use crate::errors::Result;
use crate::policy::{BatchPolicy, Concurrency};
use crate::{Error, Key, ResultCode};
use std::sync::Arc;

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

impl BatchExecutor {
    pub const fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor { cluster }
    }

    fn node_for_key(
        &self,
        key: &Key,
        has_write: bool,
        replica: crate::policy::Replica,
        read_mode_sc: crate::policy::ReadModeSC,
    ) -> Result<Arc<Node>> {
        // Java BatchNodeList parity: write records route via the
        // write-side replica logic (master, or the sequence walk for
        // Sequence/PreferRack — never rack-preferred, which could pick a
        // replica), read records via the read-side logic including
        // PreferRack and the SC read-mode overrides.
        let mut partition = if has_write {
            let mut partition = Partition::for_write(key);
            partition.replica = replica;
            partition
        } else {
            Partition::for_read(&self.cluster, key, replica, read_mode_sc)
        };
        partition.get_node(&self.cluster)
    }

    /// Batch execution: results are written into the caller's operations —
    /// no result vector, no clone in either direction. Prior results are
    /// cleared on entry. Returns the first per-node failure, if any; per-key
    /// outcomes (not-found, filtered-out, routing failures) live on the rows.
    ///
    /// Rows are moved out of the slice and back, so timeouts must not cancel
    /// the in-flight future — the per-command policy deadlines already bound
    /// the wait and stamp unanswered rows, which also means rows answered
    /// before a timeout keep their real results.
    #[allow(clippy::mutable_key_type)]
    pub async fn execute(
        &self,
        policy: &BatchPolicy,
        ops: &mut [BatchOperation],
    ) -> Result<()> {
        let rows: Vec<(BatchOperation, usize)> = ops
            .iter_mut()
            .enumerate()
            .map(|(i, op)| {
                op.clear_result();
                (std::mem::replace(op, BatchOperation::placeholder()), i)
            })
            .collect();
        let (rows, first_err) = self.run_rows(policy, rows, None).await?;
        for (op, idx) in rows {
            ops[idx] = op;
        }
        match first_err {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }

    /// Hook-driven execution: rows are owned by the call and every row's
    /// outcome reaches `hook` exactly once — answered rows as they are
    /// parsed, everything else in a final sweep. `false` from the hook, or
    /// the caller dropping the future, stops the batch.
    #[allow(clippy::mutable_key_type)]
    pub async fn execute_foreach(
        &self,
        policy: &BatchPolicy,
        ops: Vec<BatchOperation>,
        hook: Arc<BatchHook>,
    ) -> Result<()> {
        let rows: Vec<(BatchOperation, usize)> = ops
            .into_iter()
            .enumerate()
            .map(|(i, op)| (op, i))
            .collect();
        let (rows, first_err) = self.run_rows(policy, rows, Some(hook.clone())).await?;
        // Whatever never fired — unanswered, unroutable, or abandoned by an
        // abort — fires now with the outcome it carries, so the hook is the
        // complete record of the batch.
        for (op, idx) in &rows {
            if hook.is_cancelled() {
                break;
            }
            hook.fire(*idx, op.batch_record()).await;
        }
        match first_err {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }

    /// The shared batch engine: split by node, run the per-node commands and
    /// the single-key fast paths, and hand every row back — in arbitrary
    /// order, each tagged with its original input index and carrying its
    /// result — together with the first per-node failure. Callers place rows
    /// by index, which is O(n); sorting here would be needless work.
    #[allow(clippy::mutable_key_type)]
    async fn run_rows(
        &self,
        policy: &BatchPolicy,
        rows: Vec<(BatchOperation, usize)>,
        hook: Option<Arc<BatchHook>>,
    ) -> Result<(Vec<(BatchOperation, usize)>, Option<Error>)> {
        let row_count = rows.len();
        let BatchSplit {
            groups: batch_nodes,
            unroutable,
        } = self.get_batch_operate_nodes(rows, policy.replica, policy.base_policy.read_mode_sc)?;

        // Unroutable keys are decided already: report them now rather than
        // making the hook wait on the nodes that can answer.
        if let Some(hook) = &hook {
            for (op, idx) in &unroutable {
                if hook.is_cancelled() {
                    break;
                }
                hook.fire(*idx, op.batch_record()).await;
            }
        }
        let active = || hook.as_ref().is_none_or(|h| h.is_active());

        // Per-node fast path: when a node has only one key, route it
        // through a regular single-key command instead of the batch
        // protocol. The server processes single-key commands on the
        // generic transaction queue, bypassing the (more contended)
        // batch queue. Mirrors Go's `executeSingle` and Java's
        // `BatchSingle*` family. The selection is per-node — a batch
        // can mix singleton groups (fast path) with multi-key groups
        // (regular batch) in the same call.
        let mut single_groups: Vec<(Arc<Node>, BatchOperation, usize)> = Vec::new();
        let mut multi_jobs: Vec<BatchOperateCommand> = Vec::new();
        for (node, ops) in batch_nodes {
            if ops.len() == 1 {
                let (op, idx) = ops.into_iter().next().expect("one element");
                single_groups.push((node, op, idx));
            } else {
                // Apply per-record batch sub-policy config to the wire path.
                let mut parent = policy.clone();
                let mut ops = ops;
                self.cluster.patch_batch_wire(&mut parent, &mut ops);
                multi_jobs.push(BatchOperateCommand::new(parent, node, ops).with_hook(hook.clone()));
            }
        }

        let mut all_results: Vec<(BatchOperation, usize)> = Vec::with_capacity(row_count);
        // Rows that never left the client are results too, and they are
        // already marked. They do not feed `first_err`: a key the cluster
        // could not route is a per-key outcome, exactly like a server
        // answering INVALID_NAMESPACE for it, and per-key outcomes do not
        // fail the call.
        all_results.extend(unroutable);
        // First failure across all per-node groups. The batch keeps running so
        // every per-key outcome is collected; on failure the full record set is
        // surfaced via `ErrorKind::BatchFailed` (Java `BatchRecordArray`
        // parity) instead of being dropped.
        let mut first_err: Option<Error> = None;

        if !active() {
            // Aborted before the groups ran: their rows come back untouched.
            for cmd in multi_jobs.drain(..) {
                all_results.extend(cmd.batch_ops);
            }
        }
        if !multi_jobs.is_empty() {
            let cmds = self
                .execute_batch_operate_jobs(multi_jobs, policy.concurrency)
                .await?;
            for mut cmd in cmds {
                if let Some(e) = cmd.terminal_error.take() {
                    // A hook abort tears a group down through this path; it
                    // is the caller's decision, not a failure.
                    if matches!(e.kind(), crate::ErrorKind::StreamTerminated) {
                        all_results.extend(cmd.batch_ops);
                        continue;
                    }
                    // Mark this node's unanswered rows with the failure
                    // (Java parity): a client timeout stamps TIMEOUT and
                    // makes writes in-doubt — they may have been applied;
                    // other terminal errors stamp their server code when
                    // one exists. Rows answered before the failure keep
                    // their real results.
                    let rc = if e.is_client_timeout() {
                        Some(ResultCode::Timeout)
                    } else {
                        e.server_result_code()
                    };
                    let in_doubt = e.in_doubt() || e.is_client_timeout();
                    for (op, _) in &mut cmd.batch_ops {
                        if op.record_mut().result_code.is_none() {
                            if in_doubt {
                                op.set_in_doubt_on_no_response(policy.base_policy.txn.as_ref());
                            }
                            if let Some(rc) = rc {
                                op.set_result_code(rc, in_doubt);
                            }
                        }
                    }
                    first_err.get_or_insert(e);
                }
                all_results.extend(cmd.batch_ops);
            }
        }

        // The single-op commands re-resolve the node via partition
        // lookup; we keep the `node` from the per-node split only
        // to gate the fast path. Re-resolving lets the command
        // pick up partition migrations that happened between the
        // BatchExecutor split and the single-op dispatch.
        //
        // Singles honor the batch concurrency policy just like the
        // multi-key groups: running N per-node singles sequentially would
        // stack their latencies against the shared total_timeout budget.
        let hook_ref = hook.clone();
        let single_futures = single_groups.into_iter().map(|(_node, mut op, idx)| {
            let cluster = self.cluster.clone();
            let hook = hook_ref.clone();
            async move {
                if hook.as_ref().is_some_and(|h| !h.is_active()) {
                    return (Ok(()), op, idx);
                }
                let res = Self::execute_single_op(cluster, policy, &mut op).await;
                if let Some(hook) = &hook {
                    hook.fire(idx, op.batch_record()).await;
                }
                (res, op, idx)
            }
        });
        let single_results = match policy.concurrency {
            Concurrency::Sequential => {
                let mut results = Vec::new();
                for fut in single_futures {
                    results.push(fut.await);
                }
                results
            }
            Concurrency::Parallel => futures::future::join_all(single_futures).await,
        };
        for (res, op, idx) in single_results {
            if let Err(e) = res {
                first_err.get_or_insert(e);
            }
            all_results.push((op, idx));
        }

        Ok((all_results, first_err))
    }

   async fn execute_single_op(
        cluster: Arc<Cluster>,
        parent: &BatchPolicy,
        batch_op: &mut BatchOperation,
    ) -> Result<()> {
        let key = batch_op.key();

        // Build the right command for the variant, run it, and
        // capture the resulting record (or per-key error).
        let result: std::result::Result<Option<crate::Record>, Error> = match batch_op {
            BatchOperation::Read {
                policy, bins, ops, ..
            } => {
                if let Some(op_list) = ops.as_ref() {
                    // Read-with-ops takes the operate path on a write
                    // policy because that's how single-record `operate`
                    // is invoked even for read-only ops.
                    let bw_policy_proxy = crate::batch::BatchWritePolicy {
                        filter_expression: policy.filter_expression.clone(),
                        ..Default::default()
                    };
                    let mut wp = bw_policy_proxy.to_write_policy(parent);
                    wp.base_policy.read_touch_ttl = policy.read_touch_ttl;
                    let mut cmd =
                        OperateCommand::new(&wp, cluster.clone(), &key, op_list.as_slice());
                    cmd.execute().await.map(|()| cmd.read_command.record.take())
                } else {
                    let mut rp = policy.to_read_policy(parent);
                    cluster.apply_batch_read(&mut rp);
                    let mut cmd = ReadCommand::new(&rp, cluster.clone(), &key, bins.clone());
                    cmd.execute().await.map(|()| cmd.record.take())
                }
            }
            BatchOperation::Write { policy, ops, .. } => {
                let mut wp = policy.to_write_policy(parent);
                cluster.apply_batch_write(&mut wp);
                let mut cmd = OperateCommand::new(&wp, cluster.clone(), &key, ops.as_slice());
                cmd.execute().await.map(|()| cmd.read_command.record.take())
            }
            BatchOperation::Delete { policy, .. } => {
                let mut wp = policy.to_write_policy(parent);
                cluster.apply_batch_delete(&mut wp);
                let mut cmd = DeleteCommand::new(&wp, cluster.clone(), &key);
                // DeleteCommand reports missing-key via `cmd.existed`, not Result::Err —
                // re-inject KEY_NOT_FOUND so batch sees it per-record.
                match cmd.execute().await {
                    Ok(()) if !cmd.existed => Err(Error::server_error(
                        ResultCode::KeyNotFoundError,
                        String::new(),
                        None,
                    )),
                    // Same row shape as a delete that went through the
                    // multi-record wire path (and as Java's
                    // `BatchSingle.Delete`): an Ok row carries a bin-less
                    // record with the deleted record's generation and
                    // expiration, not `None`. `None` is reserved for rows
                    // that found no record or failed.
                    Ok(()) => Ok(Some(crate::Record::new(
                        None,
                        crate::IndexMap::new(),
                        None,
                        cmd.generation,
                        cmd.expiration,
                    ))),
                    Err(e) => Err(e),
                }
            }
            BatchOperation::UDF {
                policy,
                udf_name,
                function_name,
                args,
                ..
            } => {
                let mut wp = policy.to_write_policy(parent);
                cluster.apply_batch_udf(&mut wp);
                let mut cmd = ExecuteUDFCommand::new(
                    &wp,
                    cluster.clone(),
                    &key,
                    udf_name,
                    function_name,
                    args.as_deref(),
                );
                cmd.execute().await.map(|()| cmd.read_command.record.take())
            }
            // Txn verify/roll never flow through the public batch executor; the
            // transaction roll path groups and dispatches them itself.
            BatchOperation::TxnVerify { .. } | BatchOperation::TxnRoll { .. } => {
                unreachable!("txn verify/roll are dispatched by the transaction roll path")
            }
        };

        // Every server error from a single-key command is per-key; absorb onto
        // the BatchRecord, only other kinds propagate.
        match result {
            Ok(record) => {
                batch_op.set_record(record);
            }
            Err(err) if matches!(err.kind(), crate::ErrorKind::Server { .. }) => {
                let rc = err.server_result_code().expect("server error has rc");
                batch_op.set_result_code(rc, err.in_doubt());
            }
            Err(err) if matches!(err.kind(), crate::ErrorKind::UdfBadResponse) => {
                // A UDF execution failure is a per-key batch outcome. The
                // single-key command surfaces it as ErrorKind::UdfBadResponse
                // carrying the FAILURE reason; rebuild the row shape the
                // multi-record wire path produces — result code
                // UDF_BAD_RESPONSE plus a record with the FAILURE bin.
                let reason = err.message().unwrap_or("UDF Error").to_string();
                let mut bins = crate::IndexMap::new();
                bins.insert("FAILURE".to_string(), crate::Value::from(reason));
                let in_doubt = err.in_doubt();
                batch_op.set_record(Some(crate::Record::new(None, bins, None, 0, 0)));
                batch_op.set_result_code(ResultCode::UdfBadResponse, in_doubt);
            }
            Err(err) => {
                // Mirrors Java's `BatchSingle.setInDoubt()` gated on
                // `ae.getInDoubt()`: the single-command retry loop marked the
                // error in-doubt iff this was a write that reached the wire.
                // Propagate that onto the record and notify any transaction
                // before the error bubbles up.
                if err.in_doubt() {
                    batch_op.set_in_doubt_on_no_response(parent.base_policy.txn.as_ref());
                }
                return Err(err);
            }
        }
        Ok(())
    }

    async fn execute_batch_operate_jobs(
        &self,
        jobs: Vec<BatchOperateCommand>,
        concurrency: Concurrency,
    ) -> Result<Vec<BatchOperateCommand>> {
        let handles = jobs
            .into_iter()
            .map(|job| job.execute(self.cluster.clone()));
        match concurrency {
            Concurrency::Sequential => futures::future::join_all(handles)
                .await
                .into_iter()
                .collect(),
            #[cfg(feature = "rt-async-std")]
            Concurrency::Parallel => futures::future::join_all(handles)
                .await
                .into_iter()
                .map(|value| value.map_err(|e| Error::client_error(e.to_string())))
                .collect(),
            #[cfg(feature = "rt-tokio")]
            Concurrency::Parallel => futures::future::join_all(handles.map(aerospike_rt::spawn))
                .await
                .into_iter()
                .map(|value| value.map_err(|e| Error::client_error(e.to_string()))?)
                .collect(),
        }
    }

    /// Split the batch by owning node.
    ///
    /// Returns the per-node groups plus the rows whose key could not be routed
    /// at all, each already carrying its own result code.
    ///
    /// Java `BatchNodeList.generate` parity: a key that cannot be assigned to a
    /// node has the error written onto *its* record (`record.setError(...)`,
    /// then `continue`) and the rest of the batch still runs. One key naming a
    /// namespace this cluster does not have is that key's problem, not the
    /// other ninety-nine's. The call fails outright only when no key could be
    /// routed, because then there is no batch left to send — and the error
    /// returned is the first routing failure, which is more specific than
    /// "empty batch".
    fn get_batch_operate_nodes(
        &self,
        rows: Vec<(BatchOperation, usize)>,
        replica: crate::policy::Replica,
        read_mode_sc: crate::policy::ReadModeSC,
    ) -> Result<BatchSplit> {
        #![allow(clippy::type_complexity)]
        // Grouped by node in first-seen order. A `HashMap<Arc<Node>, _>` did
        // this before, hashing the node's ~40-character name string once per
        // row; a batch spans a handful of nodes, so a short vector probed with
        // `Arc::ptr_eq` — a pointer compare, and usually a hit on the entry
        // just used, since consecutive rows often share a node — is cheaper.
        let mut groups: Vec<(Arc<Node>, Vec<(BatchOperation, usize)>)> = Vec::new();
        let mut unroutable: Vec<(BatchOperation, usize)> = Vec::new();
        let mut first_err: Option<Error> = None;

        for (mut batch_op, index) in rows {
            // Route by borrowing the key: `key()` clones its two Strings, and
            // a routing lookup has no business allocating per row.
            let routed = self.node_for_key(
                &batch_op.batch_record().key,
                batch_op.has_write(),
                replica,
                read_mode_sc,
            );
            match routed {
                Ok(node) => {
                    match groups.iter_mut().find(|(n, _)| Arc::ptr_eq(n, &node)) {
                        Some((_, bucket)) => bucket.push((batch_op, index)),
                        None => groups.push((node, vec![(batch_op, index)])),
                    }
                }
                Err(err) => {
                    // Never in-doubt: nothing was sent for this key.
                    batch_op.set_result_code(routing_result_code(&err), false);
                    unroutable.push((batch_op, index));
                    first_err.get_or_insert(err);
                }
            }
        }

        if groups.is_empty() {
            if let Some(err) = first_err {
                return Err(err);
            }
        }

        Ok(BatchSplit { groups, unroutable })
    }
}

/// The outcome of splitting a batch across nodes.
struct BatchSplit {
    /// Keys that resolved to a node, grouped by that node in first-seen order.
    groups: Vec<(Arc<Node>, Vec<(BatchOperation, usize)>)>,
    /// Keys that resolved to nothing, each already marked with its result code
    /// and carrying its original input index.
    unroutable: Vec<(BatchOperation, usize)>,
}

/// The per-key result code for a key the client could not route.
///
/// Java stores the raw exception code here, including its negative client-side
/// codes; [`BatchRecord::result_code`](crate::BatchRecord) holds a
/// server-space [`ResultCode`], so the client-side codes are mapped onto the
/// server code that describes the same condition:
///
/// - an unknown namespace is `INVALID_NAMESPACE` on both sides, so it carries
///   through unchanged;
/// - anything else means the partition has no node the client can reach, which
///   is what `PARTITION_UNAVAILABLE` says.
fn routing_result_code(err: &Error) -> ResultCode {
    err.server_result_code().unwrap_or_else(|| {
        if matches!(err.kind(), crate::ErrorKind::InvalidNamespace) {
            ResultCode::InvalidNamespace
        } else {
            ResultCode::PartitionUnavailable
        }
    })
}
