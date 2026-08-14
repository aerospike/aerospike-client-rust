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
use crate::commands::BatchOperateCommand;
use crate::errors::Result;
use crate::policy::{BatchPolicy, Concurrency};
use crate::Error;
use crate::Key;
use crate::{BatchRecord, Policy, ResultCode};
use aerospike_rt::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

/// The per-node split of a batch, plus the keys the cluster could not route.
///
/// Unroutable keys come back already marked so the executor can merge them into
/// the results at their original index: a key that cannot be routed is a per-key
/// outcome, not a reason to discard the whole batch.
struct BatchSplit {
    map: HashMap<Arc<Node>, Vec<(BatchOperation, usize)>>,
    unroutable: Vec<(BatchOperation, usize)>,
}

impl BatchExecutor {
    pub const fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor { cluster }
    }

    fn node_for_key(&self, key: &Key, replica: crate::policy::Replica) -> Result<Arc<Node>> {
        let partition = Partition::new_by_key(key);
        let node = self.cluster.get_node(&partition, replica, None)?;
        Ok(node)
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
        let BatchSplit {
            map: batch_nodes,
            unroutable,
        } = self.get_batch_operate_nodes(batch_ops, policy.replica)?;
        let jobs = batch_nodes
            .into_iter()
            .map(|(node, ops)| BatchOperateCommand::new(policy.clone(), node, ops))
            .collect();
        let ops = self
            .execute_batch_operate_jobs(jobs, policy.concurrency)
            .await?;
        let mut all_results: Vec<_> = ops.into_iter().flat_map(|cmd| cmd.batch_ops).collect();
        // Rows that never left the client are results too, and they are already
        // marked.
        all_results.extend(unroutable);
        all_results.sort_by_key(|(_, i)| *i);
        Ok(all_results
            .into_iter()
            .map(|(b, _)| b.batch_record())
            .collect())
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

    #[allow(clippy::mutable_key_type)]
    fn get_batch_operate_nodes(
        &self,
        batch_ops: &[BatchOperation],
        replica: crate::policy::Replica,
    ) -> Result<BatchSplit> {
        #![allow(clippy::type_complexity)]
        let mut map: HashMap<Arc<Node>, Vec<(BatchOperation, usize)>> = HashMap::new();
        let mut unroutable: Vec<(BatchOperation, usize)> = Vec::new();
        let mut first_err: Option<Error> = None;

        for (index, batch_op) in batch_ops.iter().enumerate() {
            match self.node_for_key(&batch_op.key(), replica) {
                Ok(node) => {
                    map.entry(node).or_default().push((batch_op.clone(), index));
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
        if map.is_empty() {
            if let Some(err) = first_err {
                return Err(err);
            }
        }

        Ok(BatchSplit { map, unroutable })
    }
}
