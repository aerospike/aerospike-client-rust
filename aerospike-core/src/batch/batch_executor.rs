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

use crate::batch::BatchOperation;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::{
    BatchOperateCommand, DeleteCommand, ExecuteUDFCommand, OperateCommand, ReadCommand,
};
use crate::errors::Result;
use crate::policy::{BatchPolicy, Concurrency};
use crate::{BatchRecord, Error, Key, Policy, ResultCode};
use aerospike_rt::time::Duration;
use futures::channel::mpsc;
use futures::Stream;
use std::collections::HashMap;
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
        replica: crate::policy::Replica,
        read_mode_sc: crate::policy::ReadModeSC,
    ) -> Result<Arc<Node>> {
        let mut partition = Partition::for_read(&self.cluster, key, replica, read_mode_sc);
        partition.get_node(&self.cluster)
    }

    #[allow(clippy::option_if_let_else)]
    pub async fn execute(
        &self,
        policy: &BatchPolicy,
        batch_ops: &[BatchOperation],
    ) -> Result<Vec<BatchRecord>> {
        if policy.total_timeout() > 0 {
            match aerospike_rt::timeout(
                Duration::from_millis(u64::from(policy.total_timeout())),
                self.execute_batch_operate(policy, batch_ops),
            )
            .await
            {
                Ok(res) => res,
                Err(_) => Err(Error::Timeout("Timeout".to_string())),
            }
        } else {
            self.execute_batch_operate(policy, batch_ops).await
        }
    }

    #[allow(clippy::mutable_key_type)]
    pub async fn execute_batch_operate(
        &self,
        policy: &BatchPolicy,
        batch_ops: &[BatchOperation],
    ) -> Result<Vec<BatchRecord>> {
        let batch_nodes = self.get_batch_operate_nodes(
            batch_ops,
            policy.replica,
            policy.base_policy.read_mode_sc,
        )?;

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
                multi_jobs.push(BatchOperateCommand::new(policy.clone(), node, ops));
            }
        }

        let mut all_results: Vec<(BatchOperation, usize)> = Vec::with_capacity(batch_ops.len());

        if !multi_jobs.is_empty() {
            let ops = self
                .execute_batch_operate_jobs(multi_jobs, policy.concurrency)
                .await?;
            all_results.extend(ops.into_iter().flat_map(|cmd| cmd.batch_ops));
        }

        for (_node, mut op, idx) in single_groups {
            // The single-op commands re-resolve the node via partition
            // lookup; we keep the `node` from the per-node split only
            // to gate the fast path. Re-resolving lets the command
            // pick up partition migrations that happened between the
            // BatchExecutor split and the single-op dispatch.
            Self::execute_single_op(self.cluster.clone(), policy, &mut op).await?;
            all_results.push((op, idx));
        }

        all_results.sort_by_key(|(_, i)| *i);
        Ok(all_results
            .into_iter()
            .map(|(b, _)| b.batch_record())
            .collect())
    }

    /// Streaming variant of `execute_batch_operate`.
    ///
    /// Splits per node (same as the buffered path) and applies the
    /// same single-key fast path, but instead of waiting for every
    /// per-node command to finish and returning a sorted vector, each
    /// per-node group is spawned on its own task and pushes
    /// `(original_index, BatchRecord)` tuples onto an mpsc channel as
    /// soon as it completes. The returned receiver is an
    /// `impl Stream<Item = (usize, BatchRecord)>` — items arrive
    /// interleaved by node-completion timing, **not** sorted by input
    /// index, and each carries the original input index so callers can
    /// match results back to their `ops` slice.
    ///
    /// Whole-per-node-group failures (e.g. socket error after retries)
    /// do not abort the stream — they simply omit the affected keys.
    /// Per-key results (KEY_NOT_FOUND, FILTERED_OUT, etc.) ride on each
    /// emitted `BatchRecord` just as they do for `batch`.
    #[allow(clippy::mutable_key_type)]
    pub async fn execute_stream(
        &self,
        policy: &BatchPolicy,
        batch_ops: Vec<BatchOperation>,
    ) -> Result<impl Stream<Item = (usize, BatchRecord)>> {
        let batch_nodes = self.get_batch_operate_nodes(
            &batch_ops,
            policy.replica,
            policy.base_policy.read_mode_sc,
        )?;
        // The splitter already cloned each op into the per-node map;
        // the original `batch_ops` is no longer needed.
        drop(batch_ops);

        let (tx, rx) = mpsc::unbounded::<(usize, BatchRecord)>();

        for (node, ops) in batch_nodes {
            let cluster = self.cluster.clone();
            let policy = policy.clone();
            let tx = tx.clone();

            if ops.len() == 1 {
                // Single-key fast path: route through a non-batch
                // single-record command and emit the resulting record
                // when it returns.
                aerospike_rt::spawn(async move {
                    let (mut op, idx) =
                        ops.into_iter().next().expect("one element in fast-path group");
                    // Errors are either captured on the BatchRecord
                    // (KEY_NOT_FOUND / FILTERED_OUT) or signal a real
                    // failure for this key; either way we still emit
                    // the BatchRecord so the consumer sees the
                    // outcome.
                    let _ = Self::execute_single_op(cluster, &policy, &mut op).await;
                    let _ = tx.unbounded_send((idx, op.batch_record()));
                });
            } else {
                // Regular per-node batch path.
                let cmd = BatchOperateCommand::new(policy, node, ops);
                aerospike_rt::spawn(async move {
                    match cmd.execute(cluster).await {
                        Ok(done) => {
                            for (op, idx) in done.batch_ops {
                                let _ = tx.unbounded_send((idx, op.batch_record()));
                            }
                        }
                        // Group-level failure (e.g. socket error after
                        // retries). The command's parse path may have
                        // recorded per-key codes on some ops, but
                        // `execute` consumes `cmd` so we don't have a
                        // handle to those here. Drop silently — the
                        // stream just yields fewer items than the
                        // input had keys, per the documented contract.
                        Err(_) => {}
                    }
                });
            }
        }

        // Drop the original sender so the stream ends as soon as the
        // last spawned task completes (and drops its `Sender` clone).
        drop(tx);

        Ok(rx)
    }

    /// Single-key fast path. Translates the per-record policy onto a
    /// `ReadPolicy` / `WritePolicy`, dispatches the matching non-batch
    /// command, and writes the outcome back into the
    /// `BatchOperation`'s `BatchRecord`. Per-key errors that are
    /// expected in batch results (`KeyNotFoundError`, `FilteredOut`)
    /// are recorded on the `BatchRecord` and don't bubble up; other
    /// server errors propagate so the caller sees them.
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
                    let rp = policy.to_read_policy(parent);
                    let mut cmd = ReadCommand::new(&rp, cluster.clone(), &key, bins.clone());
                    cmd.execute().await.map(|()| cmd.record.take())
                }
            }
            BatchOperation::Write { policy, ops, .. } => {
                let wp = policy.to_write_policy(parent);
                let mut cmd = OperateCommand::new(&wp, cluster.clone(), &key, ops.as_slice());
                cmd.execute().await.map(|()| cmd.read_command.record.take())
            }
            BatchOperation::Delete { policy, .. } => {
                let wp = policy.to_write_policy(parent);
                let mut cmd = DeleteCommand::new(&wp, cluster.clone(), &key);
                cmd.execute().await.map(|()| None)
            }
            BatchOperation::UDF {
                policy,
                udf_name,
                function_name,
                args,
                ..
            } => {
                let wp = policy.to_write_policy(parent);
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
        };

        match result {
            Ok(record) => {
                batch_op.set_record(record);
            }
            Err(Error::ServerError(rc, in_doubt, _)) => {
                // Per-key errors that are normal in a batch context are
                // recorded but not propagated. Anything else bubbles.
                match rc {
                    ResultCode::KeyNotFoundError | ResultCode::FilteredOut => {
                        batch_op.set_result_code(rc, in_doubt);
                    }
                    other => {
                        batch_op.set_result_code(other, in_doubt);
                        return Err(Error::ServerError(other, in_doubt, String::new()));
                    }
                }
            }
            Err(err) => return Err(err),
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
                .map(|value| value.map_err(|e| Error::ClientError(e.to_string())))
                .collect(),
            #[cfg(feature = "rt-tokio")]
            Concurrency::Parallel => futures::future::join_all(handles.map(aerospike_rt::spawn))
                .await
                .into_iter()
                .map(|value| value.map_err(|e| Error::ClientError(e.to_string()))?)
                .collect(),
        }
    }

    fn get_batch_operate_nodes(
        &self,
        batch_ops: &[BatchOperation],
        replica: crate::policy::Replica,
        read_mode_sc: crate::policy::ReadModeSC,
    ) -> Result<HashMap<Arc<Node>, Vec<(BatchOperation, usize)>>> {
        #![allow(clippy::type_complexity)]
        let mut map = HashMap::new();
        for (index, batch_op) in batch_ops.iter().enumerate() {
            let node = self.node_for_key(&batch_op.key(), replica, read_mode_sc)?;
            map.entry(node)
                .or_insert_with(Vec::new)
                .push((batch_op.clone(), index));
        }
        Ok(map)
    }
}
