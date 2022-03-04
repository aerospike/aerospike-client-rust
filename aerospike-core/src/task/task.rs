// Copyright 2015-2020 Aerospike, Inc.
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

use crate::errors::{ErrorKind, Result};
use aerospike_rt::sleep;
use aerospike_rt::time::{Duration, Instant};

/// Status of task
#[derive(Debug, Clone, Copy)]
pub enum Status {
    /// long running task not found
    NotFound,
    /// long running task in progress
    InProgress,
    /// long running task completed
    Complete,
}

static POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Base task interface
#[async_trait::async_trait]
pub trait Task {
    /// interface for query specific task status
    async fn query_status(&self) -> Result<Status>;

    /// Wait until query status is complete, an error occurs, or the timeout has elapsed.
    async fn wait_till_complete(&self, timeout: Option<Duration>) -> Result<Status> {
        let now = Instant::now();
        let timeout_elapsed = |deadline| now.elapsed() + POLL_INTERVAL > deadline;

        loop {
            // Sleep first to give task a chance to complete and help avoid case where task hasn't
            // started yet.
            sleep(POLL_INTERVAL).await;

            match self.query_status().await {
                Ok(Status::NotFound) => {
                    bail!(ErrorKind::BadResponse("task status not found".to_string()))
                }
                Ok(Status::InProgress) => {} // do nothing and wait
                error_or_complete => return error_or_complete,
            }

            if timeout.map_or(false, timeout_elapsed) {
                bail!(ErrorKind::Timeout("Task timeout reached".to_string()))
            }
        }
    }
}
