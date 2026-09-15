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

use std::sync::atomic::{AtomicBool, Ordering};

use futures::future::BoxFuture;

use crate::BatchRecord;

/// The per-row hook of [`Client::batch_foreach`](crate::Client::batch_foreach),
/// type-erased so the batch engine stays non-generic: one small future
/// allocation per row, against the microseconds each row already costs.
pub(crate) type RowHook = Box<dyn Fn(usize, &BatchRecord) -> BoxFuture<'static, bool> + Send + Sync>;

/// A batch's row hook plus the bookkeeping that makes it fire **exactly once
/// per row** and stop the moment the caller has lost interest.
///
/// Rows fire at parse time on the node task as their result lands; rows that
/// never get a server answer (unroutable keys, a node that failed, rows an
/// abort left behind) fire once at the end with whatever outcome they carry.
/// `active` is cleared by an abort (`false` from the hook) or by the caller
/// dropping the `batch_foreach` future — after which nothing fires again and
/// running groups tear down.
pub(crate) struct BatchHook {
    hook: RowHook,
    /// Work should continue. Cleared by an abort (`false` from the hook) and
    /// by cancellation. Groups stop reading once it is false.
    active: AtomicBool,
    /// The caller dropped the future: nothing may fire again. An abort alone
    /// leaves this false so the final sweep can still report the rows the
    /// abort left behind — the hook stays the complete record of the batch.
    cancelled: AtomicBool,
    fired: Box<[AtomicBool]>,
}

impl BatchHook {
    pub(crate) fn new(hook: RowHook, rows: usize) -> Self {
        BatchHook {
            hook,
            active: AtomicBool::new(true),
            cancelled: AtomicBool::new(false),
            fired: (0..rows).map(|_| AtomicBool::new(false)).collect(),
        }
    }

    /// Fires the hook for row `idx` unless it already fired or the batch was
    /// cancelled. Returns whether the batch should keep working.
    pub(crate) async fn fire(&self, idx: usize, row: &BatchRecord) -> bool {
        if self.is_cancelled() {
            return false;
        }
        if self.fired[idx].swap(true, Ordering::AcqRel) {
            return self.is_active();
        }
        if !(self.hook)(idx, row).await {
            self.active.store(false, Ordering::Relaxed);
        }
        self.is_active()
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// The caller lost interest: stop the work and never fire again.
    pub(crate) fn cancel(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for BatchHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchHook")
            .field("active", &self.active)
            .field("rows", &self.fired.len())
            .finish_non_exhaustive()
    }
}
