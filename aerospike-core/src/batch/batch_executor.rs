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
use crate::cluster::{Cluster, Node};
use crate::commands::BatchOperateCommand;
use crate::errors::Result;
use crate::policy::{BatchPolicy, Concurrency};
use crate::Error;
use crate::{BatchRecord, Policy, ResultCode};
use aerospike_rt::time::Duration;
use std::sync::Arc;

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

/// A batch operation paired with the index it had in the caller's input.
type IndexedOp = (BatchOperation, usize);

/// One node's share of a batch.
type NodeGroup = (Arc<Node>, Vec<IndexedOp>);

/// The per-node split of a batch, plus the keys the cluster could not route.
///
/// Unroutable keys come back already marked so the executor can merge them into
/// the results at their original index: a key that cannot be routed is a per-key
/// outcome, not a reason to discard the whole batch.
struct BatchSplit {
    /// One entry per node that has work, in first-seen order.
    ///
    /// A `HashMap<Arc<Node>, _>` keyed this split before, which hashed the
    /// node's ~40-character name string once per key and cloned an `Arc` for
    /// every lookup. A batch spans a handful of nodes at most, so a short
    /// vector probed with `Arc::ptr_eq` — a pointer compare — beats hashing,
    /// and it makes the grouping order deterministic as a side effect.
    groups: Vec<NodeGroup>,
    unroutable: Vec<IndexedOp>,
}

impl BatchExecutor {
    pub const fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor { cluster }
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

    pub async fn execute_batch_operate(
        &self,
        policy: &BatchPolicy,
        batch_ops: &[BatchOperation],
    ) -> Result<Vec<BatchRecord>> {
        let BatchSplit {
            groups: batch_nodes,
            unroutable,
        } = self.get_batch_operate_nodes(batch_ops, policy.replica)?;
        let jobs = batch_nodes
            .into_iter()
            .map(|(node, ops)| BatchOperateCommand::new(policy.clone(), node, ops))
            .collect();
        let ops = self
            .execute_batch_operate_jobs(jobs, policy.concurrency)
            .await?;

        // Restore the caller's order by writing each row straight into its own
        // slot. Every operation carries the index it had on input, so the order
        // is already known: the previous code instead gathered all the rows into
        // one vector and `sort_by_key`'d it, an O(N log N) comparison sort whose
        // every swap moved a 568-byte `BatchOperation`, and then cloned each
        // result out with `batch_record()` — a second deep copy of the key, the
        // record and its bin map, taken from rows that were about to be dropped.
        // Placing by index is O(N), touches each row once, and moves the record
        // rather than cloning it.
        let mut slots: Vec<Option<BatchRecord>> = (0..batch_ops.len()).map(|_| None).collect();
        let mut place = |(op, index): (BatchOperation, usize)| {
            if let Some(slot) = slots.get_mut(index) {
                *slot = Some(op.into_batch_record());
            }
        };
        for cmd in ops {
            cmd.batch_ops.into_iter().for_each(&mut place);
        }
        // Rows that never left the client are results too, and they are already
        // marked.
        unroutable.into_iter().for_each(&mut place);

        // Routable and unroutable rows together cover every input index, so no
        // slot is left empty; `flatten` keeps the old behaviour of returning
        // whatever rows exist rather than panicking if that ever stops holding.
        debug_assert!(
            slots.iter().all(Option::is_some),
            "every batch index must be filled exactly once"
        );
        Ok(slots.into_iter().flatten().collect())
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
    ) -> Result<BatchSplit> {
        // Route the whole batch against one snapshot of the partition table
        // instead of reloading it and re-hashing the namespace per key.
        let mut routed: Vec<Result<Arc<Node>>> = Vec::with_capacity(batch_ops.len());
        self.cluster.route_keys(
            batch_ops.iter().map(|op| (op.key(), None)),
            replica,
            |node| routed.push(node),
        );

        // Count each node's share first so every bucket can be allocated at its
        // exact size. Guessing instead either regrows the bucket repeatedly —
        // each regrowth copying 568-byte operations — or over-allocates the
        // whole batch to the first node of a wide fan-out. Both passes are
        // pointer compares over a handful of nodes.
        let mut counts: Vec<(Arc<Node>, usize)> = Vec::new();
        let mut unroutable_count = 0;
        for node in &routed {
            match node {
                Ok(node) => match counts
                    .iter_mut()
                    .find(|(existing, _)| Arc::ptr_eq(existing, node))
                {
                    Some((_, count)) => *count += 1,
                    None => counts.push((node.clone(), 1)),
                },
                Err(_) => unroutable_count += 1,
            }
        }

        let mut groups: Vec<NodeGroup> = counts
            .into_iter()
            .map(|(node, count)| (node, Vec::with_capacity(count)))
            .collect();
        let mut unroutable: Vec<IndexedOp> = Vec::with_capacity(unroutable_count);
        let mut first_err: Option<Error> = None;

        for (index, (batch_op, node)) in batch_ops.iter().zip(routed).enumerate() {
            match node {
                Ok(node) => {
                    // Linear probe by pointer. A batch touches few nodes, and
                    // consecutive keys often repeat one, so the match is
                    // usually the first entry examined.
                    let bucket = groups
                        .iter_mut()
                        .find(|(existing, _)| Arc::ptr_eq(existing, &node))
                        .map(|(_, bucket)| bucket)
                        .expect("counting pass registered every routable node");
                    bucket.push((batch_op.clone(), index));
                }
                Err(err) => {
                    // A key the cluster cannot route is a per-key outcome, like
                    // the server answering an error for it: record it on that
                    // key's own row and carry on with the rest. Propagating here
                    // discarded every other key's result before anything was
                    // even sent.
                    //
                    // The code is always `PartitionUnavailable`: v2's routing
                    // failure is `Error::InvalidNode` for both an unknown
                    // namespace and an unreachable replica, distinguishable only
                    // by message text, and adding an `InvalidNamespace` variant
                    // would break the public `Error` enum.
                    let mut op = batch_op.clone();
                    op.set_result_code(ResultCode::PartitionUnavailable, false);
                    unroutable.push((op, index));
                    first_err.get_or_insert(err);
                }
            }
        }

        // Only a batch with nothing routable at all fails outright; the routing
        // error is more specific than "empty batch".
        if groups.is_empty() {
            if let Some(err) = first_err {
                return Err(err);
            }
        }

        Ok(BatchSplit { groups, unroutable })
    }
}
