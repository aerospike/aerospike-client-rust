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
use std::sync::Arc;
use std::thread;

use aerospike_rt::Mutex;

use crossbeam_queue::SegQueue;
use rand::Rng;

use crate::errors::Result;
use crate::query::{PartitionFilter, PartitionTracker};
use crate::Record;

/// Virtual collection of records retrieved through queries and scans. During a query/scan,
/// multiple threads will retrieve records from the server nodes and put these records on an
/// internal queue managed by the recordset. The single user thread consumes these records from the
/// queue.
#[derive(Debug)]
pub struct Recordset {
    instances: AtomicUsize,
    record_queue_count: AtomicUsize,
    record_queue_size: AtomicUsize,
    record_queue: SegQueue<Result<Record>>,
    active: AtomicBool,
    task_id: AtomicUsize,
    pub(crate) tracker: Arc<Mutex<PartitionTracker>>,
}

impl Drop for Recordset {
    fn drop(&mut self) {
        // close the recordset to finish all the commands sending data
        self.close();
    }
}

impl Recordset {
    pub(crate) fn new(
        rec_queue_size: usize,
        nodes: usize,
        tracker: Arc<Mutex<PartitionTracker>>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let task_id = rng.gen::<usize>();

        Recordset {
            instances: AtomicUsize::new(nodes),
            record_queue_size: AtomicUsize::new(rec_queue_size),
            record_queue_count: AtomicUsize::new(0),
            record_queue: SegQueue::new(),
            active: AtomicBool::new(true),
            task_id: AtomicUsize::new(task_id),
            tracker: tracker,
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

    pub(crate) fn reset_task_id(&self) {
        let mut rng = rand::thread_rng();
        let task_id = rng.gen::<usize>();
        self.task_id.store(task_id, Ordering::Relaxed);
    }

    pub(crate) async fn busy_push(&self, mut record: Result<Record>) {
        loop {
            let result = self.push(record);
            match result {
                None => break,
                Some(returned) => {
                    record = returned;
                    // thread::yield_now();
                    aerospike_rt::task::yield_now().await;
                }
            }
        }
    }

    fn push(&self, record: Result<Record>) -> Option<Result<Record>> {
        if !self.is_active() {
            return None; // accepted. That allows commands waiting to continue and exit
        }

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
    pub(crate) fn task_id(&self) -> u64 {
        self.task_id.load(Ordering::Relaxed) as u64
    }

    pub(crate) fn signal_end(&self) {
        if self.instances.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.close();
        };
    }

    /// If the recordset is inactive, it will extract the PartitionFilter cursor to use in a future scan/query.
    pub async fn partition_filter(&self) -> Option<PartitionFilter> {
        if !self.is_active() {
            return self.tracker.lock().await.extract_partition_filter();
        }
        None
    }
}

impl<'a> Iterator for &'a Recordset {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Result<Record>> {
        loop {
            if self.is_active() || !self.record_queue.is_empty() {
                let result = self.record_queue.pop();
                if result.is_some() {
                    self.record_queue_count.fetch_sub(1, Ordering::Relaxed);
                    return result;
                }
                // aerospike_rt::task::yield_now().await;
                thread::yield_now();
                continue;
            }
            return None;
        }
    }
}
