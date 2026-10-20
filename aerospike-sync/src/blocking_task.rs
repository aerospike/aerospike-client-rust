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

//! Blocking waits for the server-side tasks the client starts.

use std::time::Duration;

use aerospike_core::task::{Status, Task as AsyncTask};
use aerospike_core::Result;

use crate::client::block_on;

/// A server-side task started by the blocking [`Client`](crate::Client): an
/// index build or drop, a UDF registration or removal, or a background query
/// job. It wraps the asynchronous task type and waits on it with blocking
/// calls; [`into_inner`](Self::into_inner) hands the asynchronous task back
/// for use on your own runtime.
#[derive(Debug, Clone)]
pub struct Task<T: AsyncTask>(T);

impl<T: AsyncTask + Send + Sync> Task<T> {
    pub(crate) const fn new(inner: T) -> Self {
        Task(inner)
    }

    /// The task's current status on the server.
    pub fn query_status(&self) -> Result<Status> {
        block_on(self.0.query_status())
    }

    /// Polls the server until the task completes or fails, or until `timeout`
    /// elapses; `None` waits indefinitely.
    pub fn wait_till_complete(&self, timeout: Option<Duration>) -> Result<Status> {
        block_on(self.0.wait_till_complete(timeout))
    }

    /// Borrows the asynchronous task.
    pub const fn inner(&self) -> &T {
        &self.0
    }

    /// The asynchronous task, for use with your own runtime.
    pub fn into_inner(self) -> T {
        self.0
    }
}
