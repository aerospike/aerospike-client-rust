// Copyright 2015-2018 Aerospike, Inc.
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

extern crate rand;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use crossbeam_queue::SegQueue;
use rand::Rng;

use crate::errors::Result;
use crate::Record;

/// Virtual collection of records retrieved through queries and scans. During a query/scan,
/// multiple threads will retrieve records from the server nodes and put these records on an
/// internal queue managed by the recordset. The single user thread consumes these records from the
/// queue.
pub struct Recordset {
    instances: AtomicUsize,
    record_queue_count: AtomicUsize,
    record_queue_size: AtomicUsize,
    record_queue: SegQueue<Result<Record>>,
    active: AtomicBool,
    task_id: AtomicUsize,
}

impl Recordset {
    #[doc(hidden)]
    pub fn new(rec_queue_size: usize, nodes: usize) -> Self {
        let mut rng = rand::thread_rng();
        let task_id = rng.gen::<usize>();

        Recordset {
            instances: AtomicUsize::new(nodes),
            record_queue_size: AtomicUsize::new(rec_queue_size),
            record_queue_count: AtomicUsize::new(0),
            record_queue: SegQueue::new(),
            active: AtomicBool::new(true),
            task_id: AtomicUsize::new(task_id),
        }
    }

    /// Close the query.
    pub fn close(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    /// Check whether the query is still active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    #[doc(hidden)]
    pub fn push(&self, record: Result<Record>) -> Option<Result<Record>> {
        if self.record_queue_count.fetch_add(1, Ordering::Relaxed)
            < self.record_queue_size.load(Ordering::Relaxed)
        {
            self.record_queue.push(record);
            return None;
        }
        self.record_queue_count.fetch_sub(1, Ordering::Relaxed);
        Some(record)
    }

    /// Returns the task ID for the scan/query.
    pub fn task_id(&self) -> u64 {
        self.task_id.load(Ordering::Relaxed) as u64
    }

    #[doc(hidden)]
    pub fn signal_end(&self) {
        if self.instances.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.close();
        };
    }
}

impl<'a> Iterator for &'a Recordset {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Result<Record>> {
        loop {
            if self.is_active() || !self.record_queue.is_empty() {
                let result = self.record_queue.pop().ok();
                if result.is_some() {
                    self.record_queue_count.fetch_sub(1, Ordering::Relaxed);
                    return result;
                }
                thread::yield_now();
                continue;
            }
            return None;
        }
    }
}
