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

use scoped_pool::Pool;
use parking_lot::Mutex;

use errors::*;
use Key;
use batch::BatchRead;
use cluster::partition::Partition;
use cluster::{Cluster, Node};
use commands::BatchReadCommand;
use policy::BatchPolicy;

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
                                  batch_reads: &'a mut [BatchRead<'a>])
                                  -> Result<()> {
        let mut batch_nodes = self.get_batch_nodes(batch_reads)?;
        let batch_reads = SharedSlice::new(batch_reads);
        let last_err: Arc<Mutex<Option<Error>>> = Arc::default();
        self.thread_pool.scoped(|scope| {
            for (node, offsets) in batch_nodes.drain() {
                let batch_reads = batch_reads.clone();
                let last_err = last_err.clone();
                scope.execute(move || {
                    let mut cmd =
                        BatchReadCommand::new(policy, node, batch_reads, offsets);
                    if let Err(err) = cmd.execute() {
                        *last_err.lock() = Some(err);
                    };
                });
            }
            () // to keep rustfmt from horribly mangling the closure...
        });
        match Arc::try_unwrap(last_err).unwrap().into_inner() {
            None => Ok(()),
            Some(err) => Err(err),
        }
    }

    fn get_batch_nodes<'a>(&self,
                           batch_reads: &'a [BatchRead<'a>])
                           -> Result<HashMap<Arc<Node>, Vec<usize>>> {
        let mut map = HashMap::new();
        for (idx, batch_read) in batch_reads.iter().enumerate() {
            let node = self.node_for_key(&batch_read.key)?;
            map.entry(node).or_insert(vec![]).push(idx);
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
pub struct SharedSlice<'a, T: 'a> {
    value: Arc<UnsafeCell<&'a mut [T]>>,
}

unsafe impl<'a, T> Send for SharedSlice<'a, T> {}

unsafe impl<'a, T> Sync for SharedSlice<'a, T> {}

impl<'a, T: 'a> Clone for SharedSlice<'a, T> {
    fn clone(&self) -> Self {
        SharedSlice { value: self.value.clone() }
    }
}

impl<'a, T> SharedSlice<'a, T> {
    pub fn new(value: &'a mut [T]) -> Self {
        SharedSlice { value: Arc::new(UnsafeCell::new(value)) }
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        unsafe { (*self.value.get()).get(idx) }
    }

    // Like slice.get_mut but does not require a mutable reference!
    pub fn get_mut(&self, idx: usize) -> Option<&mut T> {
        unsafe { transmute::<*mut &mut [T], &mut &mut [T]>(self.value.get()).get_mut(idx) }
    }

    pub fn len(&self) -> usize {
        unsafe { (*self.value.get()).len() }
    }
}
