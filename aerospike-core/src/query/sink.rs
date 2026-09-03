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

use crate::errors::{Error, Result};
use crate::query::{PartitionFilter, Recordset, TrackerShared};
use crate::Record;

/// The user closure of [`Client::query_foreach`](crate::Client::query_foreach).
///
/// Invoked inline from the node streams — concurrently, one invocation per
/// node at a time — hence `Send + Sync`. Returning `false` aborts the query,
/// like the C client's callback contract.
pub(crate) type QueryCallback = Box<dyn Fn(Result<Record>) -> bool + Send + Sync>;

/// Where a query's records go: the channel behind a [`Recordset`], or a
/// user callback invoked inline from the node streams.
///
/// This is the seam that lets both delivery styles share the whole query
/// engine — tracker, rounds, retries, parsing — with the mode decided once,
/// at the `Client` entry point.
#[derive(Clone)]
pub(crate) enum QuerySink {
    /// Buffered delivery: records flow through the recordset's channel and
    /// the resume cursor is committed at the consumer edge.
    Channel(Arc<Recordset>),
    /// Inline delivery: the callback runs on the node task and the resume
    /// cursor commits the moment it returns — delivery and commit are
    /// atomic, so this mode is exactly-once by construction.
    Callback(Arc<CallbackCtx>),
}

impl QuerySink {
    pub(crate) fn tracker(&self) -> &TrackerShared {
        match self {
            QuerySink::Channel(rs) => &rs.tracker,
            QuerySink::Callback(ctx) => &ctx.tracker,
        }
    }

    pub(crate) fn task_id(&self) -> u64 {
        match self {
            QuerySink::Channel(rs) => rs.task_id(),
            QuerySink::Callback(ctx) => ctx.task_id.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn reset_task_id(&self) {
        match self {
            QuerySink::Channel(rs) => rs.reset_task_id(),
            QuerySink::Callback(ctx) => ctx
                .task_id
                .store(rand::random::<u64>(), Ordering::Relaxed),
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        match self {
            QuerySink::Channel(rs) => rs.is_active(),
            QuerySink::Callback(ctx) => ctx.active.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn close(&self) {
        match self {
            QuerySink::Channel(rs) => rs.close(),
            QuerySink::Callback(ctx) => ctx.active.store(false, Ordering::Relaxed),
        }
    }

    pub(crate) fn set_instances(&self, count: usize) {
        match self {
            QuerySink::Channel(rs) => rs.set_instances(count),
            QuerySink::Callback(ctx) => ctx.instances.store(count, Ordering::Relaxed),
        }
    }

    pub(crate) fn signal_end(&self) {
        match self {
            QuerySink::Channel(rs) => rs.signal_end(),
            QuerySink::Callback(ctx) => {
                if ctx.instances.fetch_sub(1, Ordering::Relaxed) == 1 {
                    ctx.active.store(false, Ordering::Relaxed);
                }
            }
        }
    }

    /// Delivers a stream error. Channel mode enqueues it; callback mode
    /// invokes the callback inline (a `false` return aborts, as for records).
    /// Stream-termination "errors" are internal signals — a cancelled or
    /// aborted stream reporting its own teardown — and are not delivered.
    pub(crate) async fn err(&self, e: Error) {
        if matches!(e.kind(), crate::ErrorKind::StreamTerminated) {
            return;
        }
        match self {
            QuerySink::Channel(rs) => rs.err(e).await,
            QuerySink::Callback(ctx) => {
                if !(ctx.callback)(Err(e)) {
                    ctx.active.store(false, Ordering::Relaxed);
                }
            }
        }
    }

    /// Delivers an error that terminates the query. In callback mode it is
    /// also recorded as the terminal status that `QueryHandle::wait` reports;
    /// [`Error`] is not `Clone`, so the handle's copy keeps the message while
    /// the callback receives the original.
    pub(crate) async fn fatal(&self, e: Error) {
        if let QuerySink::Callback(ctx) = self {
            let copy = Error::client_error(e.to_string());
            ctx.terminal.lock().get_or_insert(copy);
        }
        self.err(e).await;
    }
}

/// The engine state of a callback-mode query: what [`Recordset`] is to the
/// channel mode, minus the channel.
pub(crate) struct CallbackCtx {
    pub(crate) callback: QueryCallback,
    pub(crate) tracker: Arc<TrackerShared>,
    active: AtomicBool,
    instances: AtomicUsize,
    task_id: AtomicU64,
    terminal: parking_lot::Mutex<Option<Error>>,
}

impl CallbackCtx {
    pub(crate) fn new(callback: QueryCallback, tracker: Arc<TrackerShared>) -> Self {
        CallbackCtx {
            callback,
            tracker,
            active: AtomicBool::new(true),
            instances: AtomicUsize::new(0),
            task_id: AtomicU64::new(rand::random::<u64>()),
            terminal: parking_lot::Mutex::new(None),
        }
    }
}

impl std::fmt::Debug for CallbackCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallbackCtx")
            .field("active", &self.active)
            .field("task_id", &self.task_id)
            .finish_non_exhaustive()
    }
}

/// Handle to a running [`Client::query_foreach`](crate::Client::query_foreach).
///
/// Dropping the handle **detaches**: the query keeps running and the callback
/// keeps being invoked until the query completes on its own — the handle is a
/// remote control, not the query's lifetime. Use [`cancel`](Self::cancel) to
/// stop it, [`wait`](Self::wait) for the C-style blocking behavior.
#[derive(Debug)]
pub struct QueryHandle {
    pub(crate) ctx: Arc<CallbackCtx>,
    /// Taken by the first `wait`; later waits report the stored terminal
    /// status directly.
    pub(crate) task: Option<aerospike_rt::task::JoinHandle<()>>,
}

impl QueryHandle {
    /// Stops the query: node streams tear down after their in-flight callback
    /// invocation returns, and their connections are discarded. Cancellation
    /// granularity is one record.
    pub fn cancel(&self) {
        self.ctx.active.store(false, Ordering::Relaxed);
    }

    /// Whether the query is still running.
    pub fn is_active(&self) -> bool {
        self.ctx.active.load(Ordering::Relaxed)
    }

    /// Waits for the query to finish (complete, fail, or honor a cancel) and
    /// returns its terminal status. Stream errors were already delivered to
    /// the callback; the error returned here is the one that ended the query,
    /// if any. The handle stays usable afterwards — for
    /// [`partition_filter`](Self::partition_filter) in particular.
    pub async fn wait(&mut self) -> Result<()> {
        if let Some(task) = self.task.take() {
            #[cfg(feature = "rt-tokio")]
            task.await
                .map_err(|e| Error::client_error(format!("query task failed: {e}")))?;
            #[cfg(feature = "rt-async-std")]
            task.await;
        }

        match self.ctx.terminal.lock().take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Once the query is no longer active, returns the resume cursor — the
    /// same [`PartitionFilter`] contract as
    /// [`Recordset::partition_filter`](crate::Recordset::partition_filter).
    /// Because callback delivery commits the cursor as each invocation
    /// returns, a resume after [`cancel`](Self::cancel) is exactly-once: no
    /// record is lost, none is re-delivered.
    pub async fn partition_filter(&self) -> Option<PartitionFilter> {
        if !self.is_active() {
            return Some(self.ctx.tracker.partition_filter());
        }
        None
    }
}
