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

use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;

use crate::batch::BatchRead;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::BatchReadCommand;
use crate::errors::{Error, Result};
use crate::policy::{BatchPolicy, Concurrency};
use crate::Key;
use futures::lock::Mutex;

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

impl BatchExecutor {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor { cluster }
    }

    pub async fn execute_batch_read(
        &self,
        policy: &BatchPolicy,
        batch_reads: Vec<BatchRead>,
    ) -> Result<Vec<BatchRead>> {
        let batch_nodes = self.get_batch_nodes(&batch_reads).await?;
        let jobs = batch_nodes
            .into_iter()
            .map(|(node, reads)| BatchReadCommand::new(policy, node, reads))
            .collect();
        let reads = self.execute_batch_jobs(jobs).await?;
        Ok(reads.into_iter().flat_map(|cmd|cmd.batch_reads).collect())
    }

    async fn execute_batch_jobs(
        &self,
        jobs: Vec<BatchReadCommand>,
    ) -> Result<Vec<BatchReadCommand>> {
        let handles = jobs.into_iter().map(|mut cmd|async move {
            //let next_job = async { jobs.lock().await.next().await};
            if let Err(err) = cmd.execute().await {
                Err(err)
            } else {
                Ok(cmd)
            }
        });
        let responses = futures::future::join_all(handles).await;
        responses.into_iter().collect()
    }

    async fn get_batch_nodes(
        &self,
        batch_reads: &[BatchRead],
    ) -> Result<HashMap<Arc<Node>, Vec<BatchRead>>> {
        let mut map = HashMap::new();
        for batch_read in batch_reads.iter() {
            let node = self.node_for_key(&batch_read.key).await?;
            map.entry(node)
                .or_insert_with(Vec::new)
                .push(batch_read.clone());
        }
        Ok(map)
    }

    async fn node_for_key(&self, key: &Key) -> Result<Arc<Node>> {
        let partition = Partition::new_by_key(key);
        let node = self.cluster.get_node(&partition).await?;
        Ok(node)
    }
}
