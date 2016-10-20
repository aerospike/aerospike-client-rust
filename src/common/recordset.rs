// Copyright 2015-2016 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache Licenseersion 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

extern crate core;
extern crate rand;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use crossbeam::sync::MsQueue;
use rand::Rng;

use common::record::Record;
use error::AerospikeResult;

// #[derive(Debug)]
pub struct Recordset {
    instances: AtomicUsize,
    record_queue_count: AtomicUsize,
    record_queue_size: AtomicUsize,
    record_queue: MsQueue<AerospikeResult<Record>>,
    active: AtomicBool,

    task_id: AtomicUsize,
}

impl Recordset {
    pub fn new(rec_queue_size: usize, nodes: usize) -> AerospikeResult<Self> {

        let mut rng = rand::thread_rng();

        Ok(Recordset {
            instances: AtomicUsize::new(nodes),
            record_queue_size: AtomicUsize::new(rec_queue_size),
            record_queue_count: AtomicUsize::new(0),
            record_queue: MsQueue::new(),
            active: AtomicBool::new(true),

            task_id: AtomicUsize::new(rng.gen::<usize>()),
        })
    }

    pub fn close(&self) {
        self.active.store(false, Ordering::Relaxed)
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn push(&self, record: AerospikeResult<Record>) -> Option<AerospikeResult<Record>> {
        if self.record_queue_count.fetch_add(1, Ordering::Relaxed) <
           self.record_queue_size.load(Ordering::Relaxed) {
            self.record_queue.push(record);
            return None;
        }
        self.record_queue_count.fetch_sub(1, Ordering::Relaxed);
        Some(record)
    }

    pub fn task_id(&self) -> u64 {
        self.task_id.load(Ordering::Relaxed) as u64
    }

    pub fn signal_end(&self) {
        if self.instances.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.close()
        };
    }

    pub fn iter(&self) -> &Self {
        self
    }
}

impl<'a> Iterator for &'a Recordset {
    type Item = AerospikeResult<Record>;

    fn next(&mut self) -> Option<AerospikeResult<Record>> {
        loop {
            if self.is_active() || !self.record_queue.is_empty() {
                let result = self.record_queue.try_pop();
                if result.is_some() {
                    self.record_queue_count.fetch_sub(1, Ordering::Relaxed);
                    return result;
                }
                thread::yield_now();
                continue;
            } else {
                return None;
            }
        }
    }
}
