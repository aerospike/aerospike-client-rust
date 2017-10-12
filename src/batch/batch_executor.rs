// Copyright 2015-2017 Aerospike, Inc.
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

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::mem::transmute;
use std::cmp;

use scoped_pool::Pool;
use parking_lot::Mutex;

use errors::*;
use Key;
use batch::BatchRead;
use cluster::partition::Partition;
use cluster::{Cluster, Node};
use commands::BatchReadCommand;
use policy::{BatchPolicy, Concurrency};

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
    thread_pool: Pool,
}

impl BatchExecutor {
    pub fn new(cluster: Arc<Cluster>, thread_pool: Pool) -> Self {
        BatchExecutor {
            cluster: cluster,
            thread_pool: thread_pool,
        }
    }

    pub fn execute_batch_read<'a>(&self,
                                  policy: &BatchPolicy,
                                  batch_reads: Vec<BatchRead<'a>>)
                                  -> Result<Vec<BatchRead<'a>>> {
        let mut batch_nodes = self.get_batch_nodes(&batch_reads)?;
        let batch_reads = SharedSlice::new(batch_reads);
        let jobs = batch_nodes
            .drain()
            .map(|(node, offsets)| {
                     BatchReadCommand::new(policy, node, batch_reads.clone(), offsets)
                 })
            .collect();
        self.execute_batch_jobs(jobs, &policy.concurrency)?;
        batch_reads.into_inner()
    }

    fn execute_batch_jobs(&self,
                          mut jobs: Vec<BatchReadCommand>,
                          concurrency: &Concurrency)
                          -> Result<()> {
        let threads = match *concurrency {
            Concurrency::Sequential => 1,
            Concurrency::Parallel => jobs.len(),
            Concurrency::MaxThreads(max) => cmp::min(max, jobs.len()),
        };
        let jobs = Arc::new(Mutex::new(jobs.iter_mut()));
        let last_err: Arc<Mutex<Option<Error>>> = Arc::default();
        self.thread_pool
            .scoped(|scope| {
                for _ in 0..threads {
                    let last_err = last_err.clone();
                    let jobs = jobs.clone();
                    scope.execute(move || {
                        let next_job = || jobs.lock().next();
                        while let Some(cmd) = next_job() {
                            if let Err(err) = cmd.execute() {
                                *last_err.lock() = Some(err);
                                jobs.lock().all(|_| true); // consume the remaining jobs
                            };
                        }
                    });
                }
            });
        match Arc::try_unwrap(last_err).unwrap().into_inner() {
            None => Ok(()),
            Some(err) => Err(err),
        }
    }

    fn get_batch_nodes<'a>(&self,
                           batch_reads: &Vec<BatchRead<'a>>)
                           -> Result<HashMap<Arc<Node>, Vec<usize>>> {
        let mut map = HashMap::new();
        for (idx, batch_read) in batch_reads.iter().enumerate() {
            let node = self.node_for_key(&batch_read.key)?;
            map.entry(node).or_insert_with(|| vec![]).push(idx);
        }
        Ok(map)
    }

    fn node_for_key(&self, key: &Key) -> Result<Arc<Node>> {
        let partition = Partition::new_by_key(key);
        let node = self.cluster.get_node(&partition)?;
        Ok(node)
    }
}

// A slice with interior mutability, that can be shared across threads. The threads are required to
// ensure that no member of the slice is accessed by more than one thread. No runtime checks are
// performed by the slice to guarantee this.
pub struct SharedSlice<T> {
    value: Arc<UnsafeCell<Vec<T>>>,
}

unsafe impl<T> Send for SharedSlice<T> {}

unsafe impl<T> Sync for SharedSlice<T> {}

impl<T> Clone for SharedSlice<T> {
    fn clone(&self) -> Self {
        SharedSlice { value: self.value.clone() }
    }
}

impl<T> SharedSlice<T> {
    pub fn new(value: Vec<T>) -> Self {
        SharedSlice { value: Arc::new(UnsafeCell::new(value)) }
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        unsafe { (*self.value.get()).get(idx) }
    }

    // Like slice.get_mut but does not require a mutable reference!
    pub fn get_mut(&self, idx: usize) -> Option<&mut T> {
        unsafe { transmute::<*mut Vec<T>, &mut Vec<T>>(self.value.get()).get_mut(idx) }
    }

    pub fn len(&self) -> usize {
        unsafe { (*self.value.get()).len() }
    }

    pub fn into_inner(self) -> Result<Vec<T>> {
        match Arc::try_unwrap(self.value) {
            Ok(cell) => Ok(unsafe { cell.into_inner() }),
            Err(_) => Err("Unable to process batch request".into()),
        }
    }
}
