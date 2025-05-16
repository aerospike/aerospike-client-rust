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

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::batch::BatchOperation;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::BatchOperateCommand;
use crate::errors::Result;
use crate::policy::{BatchPolicy, Concurrency};
use crate::BatchRecord;
use crate::Error;
use crate::Key;

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

impl BatchExecutor {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor { cluster }
    }

    async fn node_for_key(&self, key: &Key, replica: crate::policy::Replica) -> Result<Arc<Node>> {
        let partition = Partition::new_by_key(key);
        let node = self
            .cluster
            .get_node(&partition, replica, Weak::new())
            .await?;
        Ok(node)
    }

    pub async fn execute_batch_operate<'a>(
        &self,
        policy: &'a BatchPolicy,
        batch_ops: &[BatchOperation<'a>],
    ) -> Result<Vec<BatchRecord>> {
        let batch_nodes = self
            .get_batch_operate_nodes(batch_ops, policy.replica)
            .await?;
        let jobs = batch_nodes
            .into_iter()
            .map(|(node, ops)| BatchOperateCommand::new(policy, node, ops))
            .collect();
        let ops = self
            .execute_batch_operate_jobs(jobs, policy.concurrency)
            .await?;
        let mut all_results: Vec<_> = ops.into_iter().flat_map(|cmd| cmd.batch_ops).collect();
        all_results.sort_by_key(|(_, i)| *i);
        Ok(all_results
            .into_iter()
            .map(|(b, _)| b.batch_record())
            .collect())
    }

    async fn execute_batch_operate_jobs<'a>(
        &self,
        jobs: Vec<BatchOperateCommand<'a>>,
        concurrency: Concurrency,
    ) -> Result<Vec<BatchOperateCommand<'a>>> {
        let handles = jobs.into_iter().map(|job| {
            // SAFETY: this variable needs to be static, because it is passed into a green-thread,
            // but the green-thread does not live longer than this function, because we wait for
            // it to finish by calling `join` and `await` on it.
            let job: BatchOperateCommand<'static> = unsafe { std::mem::transmute(job) };
            job.execute(self.cluster.clone())
        });
        match concurrency {
            Concurrency::Sequential => futures::future::join_all(handles)
                .await
                .into_iter()
                .collect(),
            Concurrency::Parallel => futures::future::join_all(handles.map(aerospike_rt::spawn))
                .await
                .into_iter()
                .map(|value| value.map_err(|e| Error::ClientError(e.to_string()))?)
                .collect(),
        }
    }

    async fn get_batch_operate_nodes<'a>(
        &self,
        batch_ops: &[BatchOperation<'a>],
        replica: crate::policy::Replica,
    ) -> Result<HashMap<Arc<Node>, Vec<(BatchOperation<'a>, usize)>>> {
        let mut map = HashMap::new();
        for (index, batch_op) in batch_ops.iter().enumerate() {
            let node = self.node_for_key(&batch_op.key(), replica).await?;
            map.entry(node)
                .or_insert_with(Vec::new)
                .push((batch_op.clone(), index));
        }
        Ok(map)
    }
}
