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

use crate::batch::BatchRead;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::BatchReadCommand;
use crate::errors::Result;
use crate::policy::{BatchPolicy, Concurrency};
use crate::Key;

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

const MAX_BATCH_REQUEST_SIZE : usize = 5000;

impl BatchExecutor {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor { cluster }
    }


    pub async fn execute_batch_read(
        &self,
        policy: &BatchPolicy,
        batch_reads: Vec<BatchRead>,
    ) -> Result<Vec<BatchRead>> {
        let batch_nodes = self.get_batch_nodes(&batch_reads, policy.replica)?;
        let mut jobs = Vec::<BatchReadCommand>::new();
        for (node, node_jobs) in batch_nodes {
            for node_chunk in node_jobs.chunks(MAX_BATCH_REQUEST_SIZE) {
                jobs.push( BatchReadCommand::new(policy, node.clone(), node_chunk.to_vec()) );
            }
        }
        let reads = self.execute_batch_jobs(jobs, policy.concurrency).await?;
        let mut all_results: Vec<_> = reads.into_iter().flat_map(|cmd|cmd.batch_reads).collect();
        all_results.sort_by_key(|(_, i)|*i);
        Ok(all_results.into_iter().map(|(b, _)|b).collect())
    }

    async fn execute_batch_jobs(
        &self,
        jobs: Vec<BatchReadCommand>,
        concurrency: Concurrency,
    ) -> Result<Vec<BatchReadCommand>> {
        let handles = jobs.into_iter().map(|job|job.execute(self.cluster.clone()));
        match concurrency {
            Concurrency::Sequential => futures::future::join_all(handles).await.into_iter().collect(),
            Concurrency::Parallel => futures::future::join_all(handles.map(aerospike_rt::spawn)).await.into_iter().map(|value|value.map_err(|e|e.to_string())?).collect(),
        }
    }

    fn get_batch_nodes(
        &self,
        batch_reads: &[BatchRead],
        replica: crate::policy::Replica,
    ) -> Result<HashMap<Arc<Node>, Vec<(BatchRead, usize)>>> {
        let mut map = HashMap::new();
        for (index, batch_read) in batch_reads.iter().enumerate() {
            let node = self.node_for_key(&batch_read.key, replica)?;
            map.entry(node)
                .or_insert_with(Vec::new)
                .push((batch_read.clone(), index));
        }
        Ok(map)
    }

    fn node_for_key(&self, key: &Key, replica: crate::policy::Replica) -> Result<Arc<Node>> {
        let partition = Partition::new_by_key(key);
        let node = self.cluster.get_node(&partition, replica, Weak::new())?;
        Ok(node)
    }
}
