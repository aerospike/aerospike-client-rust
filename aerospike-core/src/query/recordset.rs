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
pub struct RecordStream {
    rs: Arc<Recordset>,
    /// The stream's own handle on the record queue. `async_channel::Receiver`
    /// implements `Stream` with real waker registration, so polling it parks
    /// the task until a record is pushed or the channel is closed. Boxed and
    /// pinned because the receiver pins its event listener (`!Unpin`); this
    /// also keeps `RecordStream` itself `Unpin`.
    rx: std::pin::Pin<Box<Receiver<Result<Record>>>>,
}

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
    /// the consumer. `max_records`, when the caller set one, caps it: the channel
    /// preallocates its whole slot array at `size_of::<Result<Record>>()` — 248
    /// bytes — plus a stamp per slot, so the default 1024-slot queue is a ~262 KB
    /// allocation rather than merely a bound, and a query that can return at most
    /// ten records has no use for it. Zero means "no limit" and leaves the queue
    /// at full size.
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
        // `bounded(0)` panics, and a policy may legitimately ask for either zero.
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
        // `recv_blocking` instead of spinning on `try_recv`, and it unblocks
        // any worker parked in `push` on a queue nobody is draining.
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
            Err(crate::Error::StreamTerminatedError()) => Ok(()),
            _ => match self.tx.send(record).await {
                Ok(()) => Ok(()),
                Err(_) => Err(crate::Error::StreamTerminatedError()),
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
    /// Returns a result from the queue if it exists. Otherwise, returns None.
    pub fn next_record(&self) -> Option<Result<Record>> {
        self.rx.try_recv().ok()
    }

    /// Converts a reference to a [`Recordset`] into a [`RecordStream`] that can be used
    /// to iterate over records.
    pub fn into_stream(self: Arc<Self>) -> RecordStream {
        let rx = Box::pin(self.rx.clone());
        RecordStream { rs: self, rx }
    }
}

#[cfg(feature = "sync")]
impl Iterator for &Recordset {
    type Item = Result<Record>;

    /// Blocking iterator: parks the calling thread until the next record
    /// arrives; ends once the recordset is closed and drained. No spinning —
    /// the channel wakes the thread exactly when there is something to do.
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
        // Delegate to the channel's own `Stream`. On an empty, open queue it
        // registers the waker and returns `Pending` — the task sleeps until a
        // `push` or `close()` notifies it. The previous `try_recv` +
        // `wake_by_ref()` returned `Pending` while waking itself, so the
        // executor re-polled in a hot loop for as long as the servers were
        // slower than the consumer. The end-of-stream rule is unchanged and
        // now enforced by the library: `Ready(None)` only once the channel is
        // closed *and* drained, so a record pushed just before `close()` is
        // still delivered.
        self.get_mut().rx.as_mut().poll_next(cx)
    }
}

impl AsRef<Recordset> for RecordStream {
    fn as_ref(&self) -> &Recordset {
        &self.rs
    }
}

/// If the record stream is inactive, it will extract the `PartitionFilter` cursor to use in a future scan/query.
/// It will still return nil if the `PartitionFilter` is already extracted.
impl RecordStream {
    /// Returns the partition filter from the recordset.
    pub async fn partition_filter(&self) -> Option<PartitionFilter> {
        self.rs.partition_filter().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
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
        Arc::new(Recordset::new(
            queue_size,
            0,
            1,
            Arc::new(Mutex::new(tracker)),
        ))
    }

    /// A recordset built with an explicit `max_records`, for the queue-sizing
    /// tests.
    fn recordset_with_max(queue_size: usize, max_records: u64) -> Arc<Recordset> {
        let tracker = block_on(PartitionTracker::new(
            &QueryPolicy::default(),
            Arc::new(Mutex::new(PartitionFilter::all())),
            Vec::new(),
        ))
        .expect("tracker");
        Arc::new(Recordset::new(
            queue_size,
            max_records,
            1,
            Arc::new(Mutex::new(tracker)),
        ))
    }

    fn record() -> Record {
        Record::new(None, HashMap::new(), 0, 0)
    }

    /// `bounded` allocates its slot array up front, so a query that cannot
    /// return more than `max_records` rows must not pay for a full-size queue.
    #[test]
    fn queue_is_capped_by_max_records() {
        let rs = recordset_with_max(1024, 10);
        assert_eq!(rs.rx.capacity(), Some(10));
    }

    /// `max_records` above the queue size changes nothing: the queue is a
    /// buffer, not a result limit.
    #[test]
    fn queue_keeps_its_size_when_max_records_is_larger() {
        let rs = recordset_with_max(64, 10_000);
        assert_eq!(rs.rx.capacity(), Some(64));
    }

    /// Zero means "no limit" — the default for an unbounded query.
    #[test]
    fn queue_keeps_its_size_when_max_records_is_zero() {
        let rs = recordset_with_max(64, 0);
        assert_eq!(rs.rx.capacity(), Some(64));
    }

    /// Either zero must not reach `bounded`, which panics on a zero capacity.
    #[test]
    fn queue_never_degenerates_to_zero() {
        assert_eq!(recordset_with_max(0, 0).rx.capacity(), Some(1));
        assert_eq!(recordset_with_max(1024, 0).rx.capacity(), Some(1024));
        assert_eq!(recordset_with_max(0, 10).rx.capacity(), Some(1));
    }

    #[cfg(feature = "sync")]
    #[test]
    fn blocking_iterator_drains_buffered_records_after_close() {
        let rs = recordset(8);
        for _ in 0..3 {
            block_on(rs.push(Ok(record()))).unwrap();
        }
        rs.close();

        // Buffered records survive the close; then the iterator ends — and
        // stays ended.
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
        // A consumer parked in `recv_blocking` must be woken by `close()` —
        // the old spin loop version got this via polling; the parked version
        // must get an actual wakeup.
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

    /// The async stream must park, not spin. Polling an empty, open queue has
    /// to return `Pending` *without* waking its own waker — the old
    /// `try_recv` + `wake_by_ref()` did exactly that and re-polled in a hot
    /// loop for as long as the servers were slower than the consumer. A push
    /// must be what wakes it, and `close()` must end it only after draining.
    #[test]
    fn stream_parks_on_an_empty_queue_instead_of_waking_itself() {
        use futures::Stream;
        use std::task::{Context, Poll, Wake, Waker};

        struct Counter(AtomicUsize);
        impl Wake for Counter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);

        let rs = recordset(4);
        let mut stream = rs.clone().into_stream();
        let mut stream = std::pin::Pin::new(&mut stream);

        // Empty and active: Pending, and the waker has not fired.
        assert!(matches!(stream.as_mut().poll_next(&mut cx), Poll::Pending));
        assert!(matches!(stream.as_mut().poll_next(&mut cx), Poll::Pending));
        assert_eq!(counter.0.load(Ordering::SeqCst), 0, "a parked stream must not wake itself");

        // A push is what wakes the parked consumer, and the record arrives.
        block_on(rs.push(Ok(record()))).expect("push");
        assert!(counter.0.load(Ordering::SeqCst) >= 1, "push must wake the parked stream");
        assert!(matches!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(Ok(_)))));

        // A record buffered before close() still drains; only then does the stream end.
        block_on(rs.push(Ok(record()))).expect("push before close");
        rs.close();
        assert!(matches!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(Ok(_)))));
        assert!(matches!(stream.as_mut().poll_next(&mut cx), Poll::Ready(None)));
    }

    #[test]
    fn push_fails_fast_after_close() {
        let rs = recordset(8);
        rs.close();
        let err = block_on(rs.push(Ok(record()))).unwrap_err();
        assert!(
            matches!(err, crate::Error::StreamTerminatedError()),
            "unexpected error: {0}",
            err
        );
    }

    #[test]
    fn producer_blocked_on_full_queue_unblocks_on_close() {
        // Regression for the latent leak: a worker awaiting `push` into a full
        // queue whose consumer went away used to wait forever. With the
        // channel closed, the pending send must fail promptly.
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
        // buffered records and then terminates instead of staying Pending
        // forever.
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

    /// A record delivered in the instant before `close()` must still reach the
    /// consumer.
    ///
    /// Each trial parks the consumer in the empty-and-polling state, then
    /// pushes one last record and closes immediately behind it. A consumer that
    /// pairs the emptiness it observed *before* that push with an `is_active()`
    /// read from *after* the close ends the stream with the record still in the
    /// queue — no error, just a short result.
    ///
    /// Probabilistic: the window is a couple of instructions wide, so losses
    /// only appear when the consumer is descheduled inside it. `RACE_TRIALS`
    /// sets the attempt count and `RACE_BURNERS` adds spinning threads to
    /// create the CPU contention that widens the window.
    #[test]
    #[ignore = "stress test; run explicitly with --ignored"]
    fn final_record_survives_a_close_racing_the_poll() {
        let trials: usize = std::env::var("RACE_TRIALS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000);
        let burners: usize = std::env::var("RACE_BURNERS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        let stop = Arc::new(AtomicBool::new(false));
        let burner_handles: Vec<_> = (0..burners)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::hint::spin_loop();
                    }
                })
            })
            .collect();

        let mut lost = 0usize;
        for _ in 0..trials {
            let rs = recordset(8);

            let consumer_rs = rs.clone();
            let polling = Arc::new(AtomicBool::new(false));
            let polling_signal = polling.clone();
            let consumer = std::thread::spawn(move || {
                let mut stream = consumer_rs.into_stream();
                block_on(async move {
                    let mut seen = 0usize;
                    while stream.next().await.is_some() {
                        seen += 1;
                        // The warm-up record is in hand, so from here the
                        // consumer is polling an empty, still-active channel.
                        if seen == 1 {
                            polling_signal.store(true, Ordering::Release);
                        }
                    }
                    seen
                })
            });

            block_on(rs.push(Ok(record()))).expect("warm-up push");
            while !polling.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            // Land the final push at a random phase of the consumer's poll
            // loop rather than always the same one.
            for _ in 0..(rand::random::<u32>() % 4096) {
                std::hint::spin_loop();
            }
            block_on(rs.push(Ok(record()))).expect("final push before close");
            rs.close();

            if consumer.join().expect("consumer thread") != 2 {
                lost += 1;
            }
        }

        stop.store(true, Ordering::Relaxed);
        for handle in burner_handles {
            let _ = handle.join();
        }

        assert_eq!(
            lost, 0,
            "{lost} of {trials} trials ended the stream with a record still queued",
        );
    }
}
