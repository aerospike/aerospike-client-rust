// Copyright 2015-2026 Aerospike, Inc.
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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_channel::{Receiver, Sender};

use crate::errors::Result;
use crate::Value;

/// A stream over the values of a [`ResultSet`] that can be iterated over
/// asynchronously.
pub struct ResultStream(Arc<ResultSet>);

/// Virtual collection of the values produced by an aggregation query
/// ([`Client::query_aggregate`](crate::Client::query_aggregate)).
///
/// The counterpart of [`Recordset`](crate::Recordset) for queries that
/// return computed values instead of records: the client-side Lua stream
/// pipeline pushes its output values onto an internal queue, and the user
/// consumes them through [`ResultSet::into_stream`]. Most aggregations
/// produce a single final value; a UDF whose last operation is a `map` or
/// `filter` can produce many.
#[derive(Debug)]
pub struct ResultSet {
    rx: Receiver<Result<Value>>,
    tx: Sender<Result<Value>>,
    active: AtomicBool,
}

impl ResultSet {
    pub(crate) fn new(queue_size: usize) -> Self {
        let (tx, rx) = async_channel::bounded(queue_size.max(1));
        ResultSet {
            rx,
            tx,
            active: AtomicBool::new(true),
        }
    }

    /// Close the result set. Further values are discarded.
    pub fn close(&self) {
        self.active.store(false, Ordering::Relaxed);
        // Close the channel so consumers observe the end of the stream;
        // buffered values can still be drained.
        self.rx.close();
    }

    /// Check whether the aggregation is still producing values.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) async fn push(&self, value: Result<Value>) -> Result<()> {
        match self.tx.send(value).await {
            Ok(()) => Ok(()),
            Err(_) => Err(crate::Error::stream_terminated(None)),
        }
    }

    #[cfg(feature = "sync")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
    /// Returns a result from the queue if one is available. Otherwise,
    /// returns None.
    pub fn next_value(&self) -> Option<Result<Value>> {
        self.rx.try_recv().ok()
    }

    /// Converts a reference to a [`ResultSet`] into a [`ResultStream`] that
    /// can be used to iterate over the aggregation values.
    pub const fn into_stream(self: Arc<Self>) -> ResultStream {
        ResultStream(self)
    }
}

#[cfg(feature = "sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
impl Iterator for &ResultSet {
    type Item = Result<Value>;

    /// Blocking iterator: parks the calling thread until the next value
    /// arrives; ends once the result set is closed and drained.
    fn next(&mut self) -> Option<Result<Value>> {
        self.rx.recv_blocking().ok()
    }
}

impl futures::Stream for ResultStream {
    type Item = Result<Value>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.0.rx.try_recv() {
            Ok(r) => std::task::Poll::Ready(Some(r)),
            // Channel closed and drained: the stream has ended.
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

impl AsRef<ResultSet> for ResultStream {
    fn as_ref(&self) -> &ResultSet {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use futures::executor::block_on;
    use futures::StreamExt;

    #[cfg(feature = "sync")]
    #[test]
    fn blocking_iterator_drains_then_ends_after_close() {
        let rs = Arc::new(ResultSet::new(8));
        block_on(rs.push(Ok(Value::Int(1)))).unwrap();
        block_on(rs.push(Ok(Value::Int(2)))).unwrap();
        rs.close();

        let mut iter = &*rs;
        assert_eq!(iter.next().map(|r| r.unwrap()), Some(Value::Int(1)));
        assert_eq!(iter.next().map(|r| r.unwrap()), Some(Value::Int(2)));
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[cfg(feature = "sync")]
    #[test]
    fn parked_iterator_wakes_on_close() {
        let rs = Arc::new(ResultSet::new(8));
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let consumer_rs = rs.clone();
        std::thread::spawn(move || {
            let item = (&*consumer_rs).next(); // parks
            let _ = done_tx.send(item.is_none());
        });

        std::thread::sleep(Duration::from_millis(100));
        rs.close();
        assert!(done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("parked iterator was not woken by close()"));
    }

    #[test]
    fn push_fails_fast_after_close() {
        let rs = ResultSet::new(8);
        rs.close();
        assert!(block_on(rs.push(Ok(Value::Int(1)))).is_err());
    }

    #[test]
    fn async_stream_ends_after_close_and_drain() {
        let rs = Arc::new(ResultSet::new(8));
        block_on(rs.push(Ok(Value::Int(7)))).unwrap();
        rs.close();

        let mut stream = rs.into_stream();
        assert_eq!(
            block_on(stream.next()).map(|r| r.unwrap()),
            Some(Value::Int(7))
        );
        assert!(block_on(stream.next()).is_none());
    }
}
