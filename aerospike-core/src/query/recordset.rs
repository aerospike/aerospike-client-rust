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

use async_channel::{Receiver, Sender};

use crate::errors::Result;
use crate::query::{PartitionFilter, TrackerShared};
use crate::Record;

/// How many records a node stream accumulates before handing them to the
/// consumer in one channel send. The per-record async-channel hop (queue slot
/// plus wakeup) was the scan path's single largest cost once the tracker went
/// lock-free; batching amortizes it 64-fold for +39%/+53% measured scan
/// throughput (with/without bin data), while a batch of small records stays
/// well under a memory page.
pub(crate) const RECORD_BATCH: usize = 64;

/// One stream element as it travels from a node reader to the consumer: the
/// record (or error), plus what the consumer edge needs to commit the resume
/// cursor for records the user actually takes out — the secondary-index
/// cursor value (`bval`), and the per-partition delivery stamp that keeps the
/// cursor on the contiguously consumed prefix when several consumers drain
/// the same recordset.
#[derive(Debug)]
pub(crate) struct StreamEntry {
    pub(crate) result: Result<Record>,
    pub(crate) bval: Option<u64>,
    /// `(epoch, seq)` from [`TrackerShared::stamp_delivery`]; `None` on the
    /// cold paths (errors, timeouts), which never move the cursor.
    pub(crate) stamp: Option<(u32, u32)>,
}

/// A stream over incoming records for a [`Recordset`] that can be iterated over either synchronously or asynchronously.
///
/// The channel carries *batches*; the `VecDeque` drains the current batch
/// between polls.
pub struct RecordStream(
    Arc<Recordset>,
    std::pin::Pin<Box<Receiver<Vec<StreamEntry>>>>,
    std::collections::VecDeque<StreamEntry>,
);

/// Virtual collection of records retrieved through queries and scans.
///
/// During a query/scan, multiple threads will retrieve records from the server nodes and put
/// these records on an internal queue managed by the recordset. The single user thread consumes
/// these records from the queue.
///
/// Delivery is **at-least-once and loss-free**: the resume cursor tracks what
/// the consumer has taken out, not what was fetched, so an early
/// [`close`](Self::close) plus a resume from
/// [`partition_filter`](Self::partition_filter) re-fetches every undelivered
/// record. Duplicates can appear — bounded by the in-flight window — when a
/// retry overlaps buffered records, or when several concurrent consumers
/// leave a gap that a resume re-fetches. Exactly-once delivery is the
/// callback API's contract
/// ([`Client::query_foreach`](crate::Client::query_foreach)).
#[derive(Debug)]
pub struct Recordset {
    instances: AtomicUsize,
    rx: Receiver<Vec<StreamEntry>>,
    tx: Sender<Vec<StreamEntry>>,
    /// Records already received from the channel but not yet handed out by
    /// one of the synchronous consumers (`next_record`, the blocking
    /// iterator). Cold path; the async `RecordStream` drains into its own
    /// buffer instead.
    #[cfg(feature = "sync")]
    sync_buf: parking_lot::Mutex<std::collections::VecDeque<StreamEntry>>,
    active: AtomicBool,
    task_id: AtomicU64,
    /// The lock-free slice of tracker state every node stream reads on the
    /// record path — and the shared resume cursor.
    pub(crate) tracker: Arc<TrackerShared>,
}

impl Drop for Recordset {
    fn drop(&mut self) {
        // close the recordset to finish all the commands sending data
        self.close();
    }
}

impl Recordset {
    /// `rec_queue_size` bounds, in *records*, the buffer between the per-node
    /// reader tasks and the consumer; the channel itself holds batches of up
    /// to [`RECORD_BATCH`], so the slot count is that budget divided by the
    /// batch size. `max_records`, when the caller specified one, caps the
    /// budget first — a query that can return at most 10 records has no use
    /// for a deeper queue. Zero means "no limit" and leaves it at full size.
    pub(crate) fn new(
        rec_queue_size: usize,
        max_records: u64,
        nodes: usize,
        tracker: Arc<TrackerShared>,
    ) -> Self {
        let task_id = rand::random::<u64>();

        let capacity = if max_records > 0 {
            rec_queue_size.min(max_records as usize)
        } else {
            rec_queue_size
        };
        // The channel now carries batches, so its slot count is the record
        // budget divided by the batch size — the same number of records
        // buffered as before, in 64x fewer (and larger) slots.
        let (tx, rx) = async_channel::bounded((capacity / RECORD_BATCH).max(1));
        Recordset {
            instances: AtomicUsize::new(nodes),
            rx,
            tx,
            #[cfg(feature = "sync")]
            sync_buf: parking_lot::Mutex::new(std::collections::VecDeque::new()),
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
        let entry = StreamEntry {
            result: Err(e),
            bval: None,
            stamp: None,
        };
        let _ = self.tx.clone().send(vec![entry]).await;
    }

    /// Sends one record (or error) as a batch of one. The record hot path
    /// goes through [`push_batch`](Self::push_batch) instead; this remains
    /// for the cold paths — timeouts and errors.
    pub(crate) async fn push(&self, record: Result<Record>) -> Result<()> {
        match record {
            // Do not emit stream termination errors; they are used as signals only.
            Err(e) if matches!(e.kind(), crate::ErrorKind::StreamTerminated) => Ok(()),
            _ => {
                self.push_batch(vec![StreamEntry {
                    result: record,
                    bval: None,
                    stamp: None,
                }])
                .await
            }
        }
    }

    /// Hands a batch of records to the consumer in one channel send.
    pub(crate) async fn push_batch(&self, batch: Vec<StreamEntry>) -> Result<()> {
        match self.tx.send(batch).await {
            Ok(()) => Ok(()),
            Err(_) => Err(crate::Error::stream_terminated(None)),
        }
    }

    /// Hands one stream entry out to the user, committing the resume cursor
    /// as it crosses the boundary. This is the moment a record counts as
    /// *seen*: anything still buffered when the stream is closed stays in
    /// front of the cursor and is re-fetched by a resume, never lost.
    fn deliver(&self, entry: StreamEntry) -> Result<Record> {
        let StreamEntry {
            result,
            bval,
            stamp,
        } = entry;
        if let Ok(rec) = &result {
            if let Some(key) = &rec.key {
                self.tracker
                    .commit_cursor(key.partition_id(), key.digest, bval, stamp);
            }
        }
        result
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

    /// If the recordset is inactive, it will return the `PartitionFilter` cursor to use in a future scan/query.
    /// It will still return nil while the scan/query is still running.
    ///
    /// The cursor is shared with the executor rather than handed over when the
    /// query ends, so a consumer that cancels mid-scan — closing the recordset
    /// while node streams are still winding down — reads it straight away.
    /// It reflects the records the consumer has actually taken from the
    /// stream, not those merely fetched: a resume from it re-fetches anything
    /// that was buffered but never consumed, so cancelling early never loses
    /// records — at-least-once, with duplicates possible only for records a
    /// concurrent consumer delivered ahead of a sibling's unclosed gap.
    pub async fn partition_filter(&self) -> Option<PartitionFilter> {
        if !self.is_active() {
            return Some(self.tracker.partition_filter());
        }
        None
    }

    #[cfg(feature = "sync")]
    /// Returns a result from the queue if it exists. Otherwise, returns None.
    pub fn next_record(&self) -> Option<Result<Record>> {
        let entry = {
            let mut buf = self.sync_buf.lock();
            if let Some(entry) = buf.pop_front() {
                entry
            } else {
                let batch = self.rx.try_recv().ok()?;
                buf.extend(batch);
                buf.pop_front()?
            }
        };
        Some(self.deliver(entry))
    }

    /// Converts a reference to a [`Recordset`] into a [`RecordStream`] that can be used
    /// to iterate over records.
    pub fn into_stream(self: Arc<Self>) -> RecordStream {
        let rx = Box::pin(self.rx.clone());
        RecordStream(self, rx, std::collections::VecDeque::new())
    }
}

#[cfg(feature = "sync")]
impl Iterator for &Recordset {
    type Item = Result<Record>;

    /// Blocking iterator: parks the calling thread until the next record
    /// arrives; ends once the recordset is closed and drained. No
    /// spinning — the channel wakes the thread exactly when there is
    /// something to do.
    fn next(&mut self) -> Option<Result<Record>> {
        loop {
            // Never hold the buffer lock across the blocking receive.
            if let Some(entry) = self.sync_buf.lock().pop_front() {
                return Some(self.deliver(entry));
            }
            let batch = self.rx.recv_blocking().ok()?;
            self.sync_buf.lock().extend(batch);
        }
    }
}

impl futures::Stream for RecordStream {
    type Item = Result<Record>;

    /// Delegates to the channel's own stream, which parks the task and is
    /// woken by the sender. The previous `try_recv` + self-wake loop was a
    /// busy-poll: an empty moment re-polled the task immediately, burning
    /// the very CPU the producer needed to fill the channel.
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(entry) = this.2.pop_front() {
                return std::task::Poll::Ready(Some(this.0.deliver(entry)));
            }
            match this.1.as_mut().poll_next(cx) {
                std::task::Poll::Ready(Some(batch)) => this.2.extend(batch),
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

impl AsRef<Recordset> for RecordStream {
    fn as_ref(&self) -> &Recordset {
        &self.0
    }
}

/// If the record stream is inactive, it will return the `PartitionFilter` cursor to use in a future scan/query.
impl RecordStream {
    /// Returns the partition filter from the recordset.
    pub async fn partition_filter(&self) -> Option<PartitionFilter> {
        self.0.partition_filter().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::PartitionTracker;
    use crate::IndexMap;
    use std::time::Duration;

    use futures::executor::block_on;
    use futures::StreamExt;

    use crate::policy::QueryPolicy;

    /// A recordset with an empty tracker (no cluster needed): pure
    /// channel-lifecycle testing.
    ///
    /// `slots` is the number of channel slots. The channel carries batches,
    /// so the record queue size is scaled by `RECORD_BATCH` to land on
    /// exactly that many — these tests push single records and reason about
    /// slot-level blocking.
    fn recordset(slots: usize) -> Arc<Recordset> {
        let tracker =
            PartitionTracker::new(&QueryPolicy::default(), PartitionFilter::all(), &[])
                .expect("tracker");
        Arc::new(Recordset::new(slots * RECORD_BATCH, 0, 1, tracker.shared()))
    }

    fn record() -> Record {
        Record::new(None, IndexMap::new(), None, 0, 0)
    }

    /// Keys with pairwise-distinct partition ids, so per-partition cursor
    /// assertions cannot collide.
    fn distinct_partition_keys(n: usize) -> Vec<crate::Key> {
        let mut keys: Vec<crate::Key> = Vec::new();
        let mut i: i64 = 0;
        while keys.len() < n {
            let key = crate::Key::new("ns", "set", crate::Value::from(i)).unwrap();
            if !keys.iter().any(|k| k.partition_id() == key.partition_id()) {
                keys.push(key);
            }
            i += 1;
        }
        keys
    }

    /// The resume cursor (digest, bval) for the partition holding `key`.
    fn cursor_of(
        tracker: &TrackerShared,
        key: &crate::Key,
    ) -> (Option<[u8; 20]>, Option<u64>) {
        let pf = tracker.partition_filter();
        let parts = pf.partitions.as_ref().unwrap();
        let ps = parts[key.partition_id()].lock();
        (ps.digest, ps.bval)
    }

    /// The no-loss contract: the resume cursor moves only when the consumer
    /// takes a record out of the stream. Records pushed and buffered — even
    /// after the recordset is closed — must stay in front of the cursor so a
    /// resume re-fetches them.
    #[test]
    fn cursor_commits_only_on_consumption() {
        let tracker = PartitionTracker::new(&QueryPolicy::default(), PartitionFilter::all(), &[])
            .expect("tracker");
        let shared = tracker.shared();
        let rs = Arc::new(Recordset::new(RECORD_BATCH, 0, 1, shared.clone()));

        let keys = distinct_partition_keys(3);
        let entries: Vec<StreamEntry> = keys
            .iter()
            .map(|k| StreamEntry {
                result: Ok(Record::new(Some(k.clone()), IndexMap::new(), None, 0, 0)),
                bval: None,
                stamp: shared.stamp_delivery(k.partition_id()),
            })
            .collect();
        block_on(rs.clone().push_batch(entries)).unwrap();

        // Pushed but unconsumed: no cursor movement.
        for k in &keys {
            assert_eq!(cursor_of(&shared, k).0, None, "cursor moved before consumption");
        }

        // Consume exactly one record; only its partition's cursor advances.
        let mut stream = rs.clone().into_stream();
        let first = block_on(stream.next()).unwrap().unwrap();
        let first_key = first.key.as_ref().unwrap();
        assert_eq!(cursor_of(&shared, first_key).0, Some(first_key.digest));
        for k in keys.iter().filter(|k| k.digest != first_key.digest) {
            assert_eq!(cursor_of(&shared, k).0, None, "unconsumed record was committed");
        }

        // Close with the rest still buffered: cursors must not move.
        rs.close();
        for k in keys.iter().filter(|k| k.digest != first_key.digest) {
            assert_eq!(
                cursor_of(&shared, k).0,
                None,
                "buffered record was committed by close()"
            );
        }
    }

    /// The multi-consumer contract: out-of-order consumption parks until the
    /// gap beneath it closes, so the cursor only ever rests on the longest
    /// contiguously consumed prefix — never past a record a sibling consumer
    /// still holds.
    #[test]
    fn out_of_order_consumption_commits_only_the_contiguous_prefix() {
        let tracker = PartitionTracker::new(&QueryPolicy::default(), PartitionFilter::all(), &[])
            .expect("tracker");
        let shared = tracker.shared();
        let rs = Recordset::new(RECORD_BATCH, 0, 1, shared.clone());

        // Three records of ONE partition, delivered (stamped) in order.
        let key = distinct_partition_keys(1).remove(0);
        let mut entry = |bval: u64| StreamEntry {
            result: Ok(Record::new(Some(key.clone()), IndexMap::new(), None, 0, 0)),
            bval: Some(bval),
            stamp: shared.stamp_delivery(key.partition_id()),
        };
        let (e1, e2, e3) = (entry(1), entry(2), entry(3));

        // A sibling consumer takes the LAST record first: the cursor must
        // not move — records 1 and 2 are still unconsumed somewhere.
        rs.deliver(e3).unwrap();
        assert_eq!(cursor_of(&shared, &key), (None, None), "cursor jumped a gap");

        // The first record closes nothing but its own slot.
        rs.deliver(e1).unwrap();
        assert_eq!(cursor_of(&shared, &key).1, Some(1));

        // Consuming the middle record closes the gap; the parked third
        // commits with it, leaving the cursor at the true prefix end.
        rs.deliver(e2).unwrap();
        assert_eq!(cursor_of(&shared, &key), (Some(key.digest), Some(3)));
    }

    /// Entries left over from an earlier round must not move the cursor once
    /// the partition has been reassigned: their range is being re-queried.
    #[test]
    fn stale_round_entries_do_not_move_the_cursor() {
        let tracker = PartitionTracker::new(&QueryPolicy::default(), PartitionFilter::all(), &[])
            .expect("tracker");
        let shared = tracker.shared();
        let rs = Recordset::new(RECORD_BATCH, 0, 1, shared.clone());

        let key = distinct_partition_keys(1).remove(0);
        let stale = StreamEntry {
            result: Ok(Record::new(Some(key.clone()), IndexMap::new(), None, 0, 0)),
            bval: None,
            stamp: shared.stamp_delivery(key.partition_id()),
        };

        // A new round begins for this partition before the entry is consumed.
        {
            let pf = shared.partition_filter();
            pf.partitions.as_ref().unwrap()[key.partition_id()]
                .lock()
                .begin_delivery_round();
        }

        rs.deliver(stale).unwrap();
        assert_eq!(
            cursor_of(&shared, &key).0,
            None,
            "stale-round entry moved the cursor"
        );
    }

    /// A query entry's bval reaches the cursor with its digest, and only on
    /// consumption — the secondary-index resume needs both.
    #[test]
    fn query_bval_rides_to_the_cursor() {
        let tracker = PartitionTracker::new(&QueryPolicy::default(), PartitionFilter::all(), &[])
            .expect("tracker");
        let shared = tracker.shared();
        let rs = Arc::new(Recordset::new(RECORD_BATCH, 0, 1, shared.clone()));

        let key = distinct_partition_keys(1).remove(0);
        let rec = Record::new(Some(key.clone()), IndexMap::new(), None, 0, 0);
        let entry = StreamEntry {
            result: Ok(rec),
            bval: Some(42),
            stamp: shared.stamp_delivery(key.partition_id()),
        };
        block_on(rs.clone().push_batch(vec![entry])).unwrap();
        assert_eq!(cursor_of(&shared, &key), (None, None));

        let mut stream = rs.into_stream();
        block_on(stream.next()).unwrap().unwrap();
        assert_eq!(cursor_of(&shared, &key), (Some(key.digest), Some(42)));
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
