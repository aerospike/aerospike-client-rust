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
        let mut batch_nodes = self.get_batch_nodes(&batch_reads).await?;
        let jobs = batch_nodes
            .drain()
            .map(|(node, reads)| BatchReadCommand::new(policy, node, reads))
            .collect();
        let reads = self.execute_batch_jobs(jobs, &policy.concurrency).await?;
        let mut res: Vec<BatchRead> = vec![];
        for mut read in reads {
            res.append(&mut read.batch_reads);
        }
        Ok(res)
    }

    async fn execute_batch_jobs(
        &self,
        jobs: Vec<BatchReadCommand>,
        concurrency: &Concurrency,
    ) -> Result<Vec<BatchReadCommand>> {
        let threads = match *concurrency {
            Concurrency::Sequential => 1,
            Concurrency::Parallel => jobs.len(),
            Concurrency::MaxThreads(max) => cmp::min(max, jobs.len()),
        };
        let size = jobs.len() / threads;
        let mut overhead = jobs.len() % threads;
        let last_err: Arc<Mutex<Option<Error>>> = Arc::default();
        let mut slice_index = 0;
        let mut handles = vec![];
        let res = Arc::new(Mutex::new(vec![]));
        for _ in 0..threads {
            let mut thread_size = size;
            if overhead >= 1 {
                thread_size += 1;
                overhead -= 1;
            }
            let slice = Vec::from(&jobs[slice_index..slice_index + thread_size]);
            slice_index = thread_size + 1;
            let last_err = last_err.clone();
            let res = res.clone();
            let handle = aerospike_rt::spawn(async move {
                //let next_job = async { jobs.lock().await.next().await};
                for mut cmd in slice {
                    if let Err(err) = cmd.execute().await {
                        *last_err.lock().await = Some(err);
                    };
                    res.lock().await.push(cmd);
                }
            });
            handles.push(handle);
        }
        futures::future::join_all(handles).await;
        match Arc::try_unwrap(last_err).unwrap().into_inner() {
            None => Ok(res.lock().await.to_vec()),
            Some(err) => Err(err),
        }
    }

    async fn get_batch_nodes(
        &self,
        batch_reads: &[BatchRead],
    ) -> Result<HashMap<Arc<Node>, Vec<BatchRead>>> {
        let mut map = HashMap::new();
        for (_, batch_read) in batch_reads.iter().enumerate() {
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
