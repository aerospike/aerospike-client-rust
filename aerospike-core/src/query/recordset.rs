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

use async_channel::{bounded, Receiver, Sender};
use rand::Rng;

use crate::errors::Result;
use crate::Record;

/// Virtual collection of records retrieved through queries and scans. During a query/scan,
/// multiple threads will retrieve records from the server nodes and put these records on an
/// internal queue managed by the recordset. The single user thread consumes these records from the
/// queue.
pub struct Recordset {
    instances: AtomicUsize,
    record_recv: Receiver<Result<Record>>,
    pub(crate) record_sendr: Sender<Result<Record>>,
    active: AtomicBool,
    task_id: AtomicUsize,
}

impl Recordset {
    #[doc(hidden)]
    pub fn new(rec_queue_size: usize, nodes: usize) -> Self {
        let mut rng = rand::thread_rng();
        let task_id = rng.gen::<usize>();

        let (sender, receiver) = bounded::<Result<Record>>(rec_queue_size);

        Recordset {
            instances: AtomicUsize::new(nodes),
            record_recv: receiver,
            record_sendr: sender,
            active: AtomicBool::new(true),
            task_id: AtomicUsize::new(task_id),
        }
    }

    /// Close the query.
    pub fn close(&self) {
        self.record_sendr.close();
        self.active.store(false, Ordering::Relaxed);
    }

    /// Check whether the query is still active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    // #[doc(hidden)]
    // pub async fn push(&mut self, record: Result<Record>) -> Result<()> {
    //     match self.record_sendr.send(record).await {
    //         Ok(()) => Ok(()),
    //         Err(e) => Err(e),
    //     }
    // }

    /// Returns the task ID for the scan/query.
    pub fn task_id(&self) -> u64 {
        self.task_id.load(Ordering::Relaxed) as u64
    }

    /// Returns the next record asynchronously.
    pub async fn next(&self) -> Option<Result<Record>> {
        match self.record_recv.recv().await {
            Ok(r) => Some(r),
            Err(_) => None,
        }
    }

    /// Returns the next record synchronously.
    pub fn next_record(&self) -> Option<Result<Record>> {
        match self.record_recv.recv_blocking() {
            Ok(r) => Some(r),
            Err(_) => None,
        }
    }

    /// Returns a stream for the recordset.
    pub fn to_stream<'a>(&'a self) -> impl futures::Stream<Item = Result<Record>> + Unpin + 'a {
        Box::pin(futures::stream::unfold(
            self.record_recv.clone(),
            |recv| async {
                match recv.recv().await {
                    Ok(r) => Some((r, recv)),
                    Err(_) => None,
                }
            },
        ))
    }

    #[doc(hidden)]
    pub(crate) fn signal_end(&self) {
        if self.instances.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.close();
        };
    }
}

impl<'a> Iterator for &'a Recordset {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Result<Record>> {
        match self.record_recv.recv_blocking() {
            Ok(r) => Some(r),
            Err(_) => None,
        }
    }
}
