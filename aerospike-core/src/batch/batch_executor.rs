// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::batch::{BatchOperation, BatchRecord};
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::{BatchOperateCommand, BatchStreamCommand};
use crate::errors::Result;
use crate::policy::{BatchPolicy, Concurrency};
use crate::query::SemanticSync;
use crate::Error;
use crate::Key;
use crate::Policy;
use aerospike_rt::time::Duration;
use futures::{Stream, StreamExt};

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

impl BatchExecutor {
    pub const fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor { cluster }
    }

    async fn node_for_key(&self, key: &Key, replica: crate::policy::Replica) -> Result<Arc<Node>> {
        let partition = Partition::new_by_key(key);
        let node = self.cluster.get_node(&partition, replica, Weak::new())?;
        Ok(node)
    }

    /// Execute a batch of operations in-place.
    ///
    /// Results are written directly into the `BatchRecord` embedded in each `BatchOperation`.
    /// On success **or** error (including timeout), any operations that completed before
    /// the failure are already populated in `ops` and remain accessible to the caller.
    pub async fn execute(&self, policy: &BatchPolicy, ops: &mut Vec<BatchOperation>) -> Result<()> {
        // Move the vec into a SemanticSync so it can be shared across concurrent futures
        // without synchronization. Each per-node command only accesses its own indices.
        let sem = SemanticSync::new(std::mem::take(ops));

        let result = if policy.total_timeout() > 0 {
            match aerospike_rt::timeout(
                Duration::from_millis(u64::from(policy.total_timeout())),
                self.execute_batch_operate(policy, &sem),
            )
            .await
            {
                Ok(res) => res,
                Err(_) => Err(Error::Timeout("Timeout".to_string())),
            }
        } else {
            self.execute_batch_operate(policy, &sem).await
        };

        // Whether success or error, restore the vec (with in-place results) to the caller.
        // Safety: all futures from execute_batch_operate are completed or dropped by this point
        // (join_all without spawn guarantees this), so we are the sole Arc holder.
        *ops = sem.into_inner().unwrap();

        result
    }

    async fn execute_batch_operate(
        &self,
        policy: &BatchPolicy,
        sem: &SemanticSync<Vec<BatchOperation>>,
    ) -> Result<()> {
        let node_indices = self
            .get_batch_operate_nodes(sem.as_ref(), policy.replica)
            .await?;
        let jobs = node_indices
            .into_iter()
            .map(|(node, owned_indices)| {
                BatchOperateCommand::new(policy.clone(), node, owned_indices, sem.clone())
            })
            .collect();
        self.execute_batch_operate_jobs(jobs, policy.concurrency)
            .await
    }

    async fn execute_batch_operate_jobs(
        &self,
        jobs: Vec<BatchOperateCommand>,
        concurrency: Concurrency,
    ) -> Result<()> {
        let handles = jobs
            .into_iter()
            .map(|job| job.execute(self.cluster.clone()));
        match concurrency {
            Concurrency::Sequential => {
                for handle in handles {
                    handle.await?;
                }
                Ok(())
            }
            // FuturesUnordered polls only the future that was actually woken (O(1) per
            // completion), unlike join_all which re-polls every future on each wakeup (O(n)).
            // This matters with large node counts (e.g. 256 nodes).
            //
            // Safety: no spawn — futures are dropped when this stream is dropped (e.g. on
            // timeout), so the SemanticSync Arc count returns to 1 and into_inner() is safe.
            Concurrency::Parallel => {
                let mut unordered: futures::stream::FuturesUnordered<_> = handles.collect();
                while let Some(result) = unordered.next().await {
                    result?;
                }
                Ok(())
            }
        }
    }

    async fn get_batch_operate_nodes(
        &self,
        batch_ops: &[BatchOperation],
        replica: crate::policy::Replica,
    ) -> Result<HashMap<Arc<Node>, Vec<usize>>> {
        let mut map = HashMap::new();
        for (index, batch_op) in batch_ops.iter().enumerate() {
            let node = self.node_for_key(&batch_op.key(), replica).await?;
            map.entry(node).or_insert_with(Vec::new).push(index);
        }
        Ok(map)
    }

    /// Execute a batch and stream results as they arrive from each node.
    ///
    /// The returned stream yields `(original_index, BatchRecord)` pairs in the order they are
    /// received from the server — which is **not** necessarily the order of `ops`.  Once all
    /// per-node tasks have completed (or failed) the stream ends automatically.
    ///
    /// Ownership of `ops` is taken so that it can be shared cheaply (via `Arc`) across the
    /// spawned per-node tasks without any cloning.
    pub async fn execute_stream(
        &self,
        policy: &BatchPolicy,
        ops: Vec<BatchOperation>,
    ) -> Result<impl Stream<Item = (usize, BatchRecord)>> {
        // Bounded channel: if the consumer stalls the per-node tasks apply backpressure instead
        // of buffering an unbounded number of records in memory.
        let (sender, receiver) = futures::channel::mpsc::channel(256);
        let ops = Arc::new(ops);

        let node_indices = self
            .get_batch_operate_nodes(ops.as_slice(), policy.replica)
            .await?;

        for (node, owned_indices) in node_indices {
            let cmd = BatchStreamCommand::new(
                policy.clone(),
                node,
                owned_indices,
                ops.clone(),
                sender.clone(),
            );
            let cluster = self.cluster.clone();
            // Spawn each per-node command so that results from different nodes are interleaved
            // as they arrive.  The sender clone is dropped when the task completes, and when
            // all tasks are done the channel closes, ending the stream.
            aerospike_rt::spawn(cmd.execute(cluster));
        }

        // Drop the executor's copy of the sender.  The channel now closes once every spawned
        // task has also dropped its clone.
        drop(sender);

        Ok(receiver)
    }
}
