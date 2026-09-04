// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use aerospike_rt::Mutex;

use async_channel::{Receiver, Sender};

use crate::errors::Result;
use crate::query::{PartitionFilter, PartitionTracker};
use crate::Record;

/// A stream over incoming records for a [`Recordset`] that can be iterated over either synchronously or asynchronously.
pub struct RecordStream(Arc<Recordset>);

/// Virtual collection of records retrieved through queries and scans.
///
/// During a query/scan, multiple threads will retrieve records from the server nodes and put
/// these records on an internal queue managed by the recordset. The single user thread consumes
/// these records from the queue.
#[derive(Debug)]
pub struct Recordset {
    instances: AtomicUsize,
    rx: Receiver<Result<Record>>,
    tx: Sender<Result<Record>>,
    active: AtomicBool,
    task_id: AtomicU64,
    pub(crate) tracker: Arc<Mutex<PartitionTracker>>,
}

impl Drop for Recordset {
    fn drop(&mut self) {
        // close the recordset to finish all the commands sending data
        self.close();
    }
}

impl Recordset {
    /// `rec_queue_size` bounds the buffer between the per-node reader tasks and
    /// the consumer. `max_records`, when the caller specified one, caps it: the
    /// channel preallocates its whole slot array at
    /// `size_of::<Result<Record>>()` (248 bytes) plus a stamp per slot, so a
    /// query that can return at most 10 records has no use for a 1024-slot,
    /// 262 KB array. Zero means "no limit" and leaves the queue at full size.
    pub(crate) fn new(
        rec_queue_size: usize,
        max_records: u64,
        nodes: usize,
        tracker: Arc<Mutex<PartitionTracker>>,
    ) -> Self {
        let task_id = rand::random::<u64>();

        let capacity = if max_records > 0 {
            rec_queue_size.min(max_records as usize)
        } else {
            rec_queue_size
        };
        let (tx, rx) = async_channel::bounded(capacity.max(1));
        Recordset {
            instances: AtomicUsize::new(nodes),
            rx,
            tx,
            active: AtomicBool::new(true),
            task_id: AtomicU64::new(task_id),
            tracker,
        }
    }

    /// Close the query.
    pub fn close(&self) {
        self.active.store(false, Ordering::Relaxed);
        // Close the channel so consumers observe the end of the stream:
        // buffered records can still be drained, then receives report
        // `Closed`. This is what lets the blocking iterator park in
        // `recv_blocking` instead of spinning on `try_recv`.
        self.rx.close();
    }

    /// Check whether the query is still active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) fn set_instances(&self, count: usize) {
        self.instances.store(count, Ordering::Relaxed);
    }

    pub(crate) fn reset_task_id(&self) {
        let task_id = rand::random::<u64>();
        self.task_id.store(task_id, Ordering::Relaxed);
    }

    pub(crate) async fn err(&self, e: crate::Error) {
        let _ = self.tx.clone().send(Err(e)).await;
    }

    pub(crate) async fn push(&self, record: Result<Record>) -> Result<()> {
        match record {
            // Do not emit stream termination errors; they are used as signals only.
            Err(e) if matches!(e.kind(), crate::ErrorKind::StreamTerminated) => Ok(()),
            _ => match self.tx.send(record).await {
                Ok(()) => Ok(()),
                Err(_) => Err(crate::Error::stream_terminated(None)),
            },
        }
    }

    /// Returns the task ID for the scan/query.
    pub(crate) fn task_id(&self) -> u64 {
        self.task_id.load(Ordering::Relaxed)
    }

    pub(crate) fn signal_end(&self) {
        if self.instances.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.close();
        }
    }

    /// If the recordset is inactive, it will extract the `PartitionFilter` cursor to use in a future scan/query.
    /// It will still return nil if the `PartitionFilter` is already extracted.
    pub async fn partition_filter(&self) -> Option<PartitionFilter> {
        if !self.is_active() {
            return self.tracker.lock().await.extract_partition_filter();
        }
        None
    }

    #[cfg(feature = "sync")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
    /// Returns a result from the queue if it exists. Otherwise, returns None.
    pub fn next_record(&self) -> Option<Result<Record>> {
        self.rx.try_recv().ok()
    }

    /// Converts a reference to a [`Recordset`] into a [`RecordStream`] that can be used
    /// to iterate over records.
    pub const fn into_stream(self: Arc<Self>) -> RecordStream {
        RecordStream(self)
    }
}

#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
impl Iterator for &Recordset {
    type Item = Result<Record>;

    /// Blocking iterator: parks the calling thread until the next record
    /// arrives; ends once the recordset is closed and drained. No
    /// spinning — the channel wakes the thread exactly when there is
    /// something to do.
    fn next(&mut self) -> Option<Result<Record>> {
        self.rx.recv_blocking().ok()
    }
}

impl futures::Stream for RecordStream {
    type Item = Result<Record>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.0.rx.try_recv() {
            Ok(r) => std::task::Poll::Ready(Some(r)),
            // Channel closed and drained: the stream has ended. (`close()`
            // closes the channel; `is_empty()` alone would spin forever on
            // the `Closed` error.)
            Err(e) if e.is_closed() => std::task::Poll::Ready(None),
            Err(e) => {
                if !self.0.is_active() && e.is_empty() {
                    std::task::Poll::Ready(None)
                } else {
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            }
        }
    }
}

impl AsRef<Recordset> for RecordStream {
    fn as_ref(&self) -> &Recordset {
        &self.0
    }
}

/// If the record stream is inactive, it will extract the `PartitionFilter` cursor to use in a future scan/query.
/// It will still return nil if the `PartitionFilter` is already extracted.
impl RecordStream {
    /// Returns the partition filter from the recordset.
    pub async fn partition_filter(&self) -> Option<PartitionFilter> {
        self.0.partition_filter().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IndexMap;
    use std::time::Duration;

    use futures::executor::block_on;
    use futures::StreamExt;

    use crate::policy::QueryPolicy;

    /// A recordset with an empty tracker (no cluster needed): pure
    /// channel-lifecycle testing.
    fn recordset(queue_size: usize) -> Arc<Recordset> {
        let tracker = block_on(PartitionTracker::new(
            &QueryPolicy::default(),
            Arc::new(Mutex::new(PartitionFilter::all())),
            Vec::new(),
        ))
        .expect("tracker");
        Arc::new(Recordset::new(queue_size, 0, 1, Arc::new(Mutex::new(tracker))))
    }

    fn record() -> Record {
        Record::new(None, IndexMap::new(), None, 0, 0)
    }

    #[cfg(feature = "sync")]
    #[test]
    fn blocking_iterator_drains_buffered_records_after_close() {
        let rs = recordset(8);
        for _ in 0..3 {
            block_on(rs.push(Ok(record()))).unwrap();
        }
        rs.close();

        // Buffered records survive the close; then the iterator ends —
        // and stays ended.
        let mut iter = &*rs;
        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[cfg(feature = "sync")]
    #[test]
    fn blocking_iterator_ends_immediately_on_closed_empty_set() {
        let rs = recordset(8);
        rs.close();
        assert!((&*rs).next().is_none());
    }

    #[cfg(feature = "sync")]
    #[test]
    fn parked_iterator_wakes_on_close() {
        // A consumer parked in `recv_blocking` must be woken by `close()`
        // — the old spin loop version got this via polling; the parked
        // version must get an actual wakeup.
        let rs = recordset(8);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let consumer_rs = rs.clone();
        std::thread::spawn(move || {
            let item = (&*consumer_rs).next(); // parks: queue empty, not closed
            let _ = done_tx.send(item.is_none());
        });

        std::thread::sleep(Duration::from_millis(100));
        rs.close();
        let ended_clean = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("parked iterator was not woken by close()");
        assert!(ended_clean, "expected None after close on empty set");
    }

    #[test]
    fn push_fails_fast_after_close() {
        let rs = recordset(8);
        rs.close();
        let err = block_on(rs.push(Ok(record()))).unwrap_err();
        assert!(
            matches!(err.kind(), crate::ErrorKind::StreamTerminated),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn producer_blocked_on_full_queue_unblocks_on_close() {
        // Regression for the latent leak: a worker awaiting `push` into a
        // full queue whose consumer went away used to wait forever. With
        // the channel closed, the pending send must fail promptly.
        let rs = recordset(1);
        block_on(rs.push(Ok(record()))).unwrap(); // fill the queue

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let producer_rs = rs.clone();
        std::thread::spawn(move || {
            // Blocks: queue is full and nobody is consuming.
            let result = block_on(producer_rs.push(Ok(record())));
            let _ = done_tx.send(result.is_err());
        });

        std::thread::sleep(Duration::from_millis(100));
        rs.close();
        let send_failed = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("blocked producer was not unblocked by close()");
        assert!(send_failed, "push into a closed recordset must fail");
    }

    #[test]
    fn async_stream_ends_after_close_and_drain() {
        // The poll_next `Closed` arm: after close, the stream yields the
        // buffered records and then terminates instead of staying
        // Pending forever.
        let rs = recordset(8);
        for _ in 0..2 {
            block_on(rs.push(Ok(record()))).unwrap();
        }
        rs.close();

        let mut stream = rs.into_stream();
        assert!(block_on(stream.next()).is_some());
        assert!(block_on(stream.next()).is_some());
        assert!(block_on(stream.next()).is_none());
    }
}
