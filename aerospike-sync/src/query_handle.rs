// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Blocking control of a callback query.

use aerospike_core::query::PartitionFilter;
use aerospike_core::Result;

use crate::client::block_on;

/// A running callback query started by
/// [`Client::query_foreach`](crate::Client::query_foreach). It wraps the
/// asynchronous handle: [`wait`](Self::wait) blocks until the query ends,
/// [`cancel`](Self::cancel) stops it, and dropping the handle detaches,
/// leaving the query running.
#[derive(Debug)]
pub struct QueryHandle(aerospike_core::QueryHandle);

impl QueryHandle {
    pub(crate) const fn new(inner: aerospike_core::QueryHandle) -> Self {
        QueryHandle(inner)
    }

    /// Stops the query. Records already handed to the callback are not
    /// affected; the callback is not invoked again.
    pub fn cancel(&self) {
        self.0.cancel();
    }

    /// Whether the query is still running.
    pub fn is_active(&self) -> bool {
        self.0.is_active()
    }

    /// Blocks until the query ends and reports its terminal error, if any.
    /// The handle stays usable afterwards, for
    /// [`partition_filter`](Self::partition_filter) in particular.
    pub fn wait(&mut self) -> Result<()> {
        block_on(self.0.wait())
    }

    /// The cursor to resume the query from once it has stopped, or `None`
    /// while it is still running. See the asynchronous
    /// [`QueryHandle::partition_filter`](aerospike_core::QueryHandle::partition_filter)
    /// for the exactly-once resume semantics.
    pub fn partition_filter(&self) -> Option<PartitionFilter> {
        self.0.partition_filter()
    }

    /// The asynchronous handle, for use with your own runtime.
    pub fn into_inner(self) -> aerospike_core::QueryHandle {
        self.0
    }
}
