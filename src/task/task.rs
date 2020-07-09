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
use std::thread;
use std::time::{Duration, Instant};

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

static POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Base task interface
pub trait Task {
    /// interface for query specific task status
    fn query_status(&self) -> Result<Status>;

    /// interface for retrieving client timeout
    fn get_timeout(&self) -> Result<Duration>;

    // TODO: consider adding async and only support rust 1.39.0+
    /// logic to wait until status query is completed
    fn wait_till_complete(&self) -> Result<Status> {
        let now = Instant::now();

        while now.elapsed().as_secs() < self.get_timeout()?.as_secs() {
            match self.query_status() {
                Ok(Status::NotFound) => {
                    bail!(ErrorKind::BadResponse("task status not found".to_string()))
                }
                Ok(Status::InProgress) => {
                    // do nothing and wait
                }
                error_or_complete => return error_or_complete,
            }
            thread::sleep(POLL_INTERVAL);
        }

        // Timeout
        bail!(ErrorKind::Connection("query status timeout".to_string()))
    }
}
