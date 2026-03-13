// Copyright 2015-2020 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use crate::cluster::version_parser::Version;
use crate::cluster::Cluster;
use crate::errors::{Error, Result};
use crate::task::{Status, Task};
use crate::AdminPolicy;

/// Task used to poll for long running query execute job completion.
#[derive(Debug, Clone)]
pub struct ExecuteTask {
    cluster: Arc<Cluster>,
    task_id: u64,
    /// true if the statement had no filter (scan mode)
    scan: bool,
}

impl ExecuteTask {
    /// Creates a new `ExecuteTask`.
    pub fn new(cluster: Arc<Cluster>, task_id: u64, scan: bool) -> Self {
        ExecuteTask {
            cluster,
            task_id,
            scan,
        }
    }

    /// Returns the task ID.
    pub const fn task_id(&self) -> u64 {
        self.task_id
    }

    fn build_command(version: &Version, scan: bool, task_id: u64) -> String {
        let id_key = if version >= &Version::new(8, 1, 0, 0) {
            "id"
        } else {
            "trid"
        };

        if version.supports_partition_query() {
            // query-show works for both scan and query on server >= 6.0
            format!("query-show:{id_key}={task_id}")
        } else if version.supports_query_show() {
            let module = if scan { "scan" } else { "query" };
            format!("{module}-show:{id_key}={task_id}")
        } else {
            let module = if scan { "scan" } else { "query" };
            format!("jobs:module={module};cmd=get-job;{id_key}={task_id}")
        }
    }

    fn parse_status(response: &str) -> Result<Status> {
        // "ERROR:2" means job not found — treat as complete for modern servers
        if response.contains("ERROR:2") {
            return Ok(Status::Complete);
        }

        if response.starts_with("ERROR") {
            return Err(Error::BadResponse(format!(
                "Query execute failed: {response}"
            )));
        }

        // Look for "status=" in the response
        if let Some(status_idx) = response.find("status=") {
            let status_start = status_idx + "status=".len();
            let status_str = &response[status_start..];
            // Status value ends at the next ':' or end of string
            let status_end = status_str.find(':').unwrap_or(status_str.len());
            let status_val = &status_str[..status_end];

            if status_val.to_lowercase().starts_with("done") {
                return Ok(Status::Complete);
            }
            return Ok(Status::InProgress);
        }

        // If we can't parse the response, treat as in progress
        Ok(Status::InProgress)
    }
}

#[async_trait::async_trait]
impl Task for ExecuteTask {
    async fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes();

        if nodes.is_empty() {
            return Err(Error::Connection("No connected node".to_string()));
        }

        let admin_policy = AdminPolicy { timeout: 3_000 };
        for node in &nodes {
            let command = Self::build_command(node.version(), self.scan, self.task_id);
            let response = node.info(&admin_policy, &[&command[..]]).await?;

            if let Some(resp) = response.get(&command) {
                match Self::parse_status(resp)? {
                    Status::Complete => {}
                    other => return Ok(other),
                }
            } else {
                // No response for command — task may not have started yet
                return Ok(Status::InProgress);
            }
        }

        Ok(Status::Complete)
    }
}
