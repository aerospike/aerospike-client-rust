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

/// What a background job has done so far.
///
/// The server reports these in its `query-show` reply and they are the only way to learn how many
/// records a background write actually changed — a `query_operate` sends the work and returns
/// nothing, so without these there is no affected-row count.
///
/// `succeeded + filtered_bins + filtered_meta` accounts for the records the job examined.
// No `Eq`: `percent` is a float.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct JobProgress {
    /// Whether the job has finished.
    pub complete: bool,
    /// Records written. **This is the affected-row count.**
    pub succeeded: u64,
    /// Records the job tried to write and could not. A non-zero value means the job partly failed,
    /// which a caller must not read as success.
    pub failed: u64,
    /// Records a filter expression excluded after reading their bins.
    pub filtered_bins: u64,
    /// Records a filter expression excluded on metadata alone, without reading bins.
    pub filtered_meta: u64,
    /// Records delayed by the records-per-second limit. A large figure means the job was throttled
    /// rather than slow.
    pub throttled: u64,
    /// Percent complete, as the server reports it.
    pub percent: f64,
}

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
    pub const fn new(cluster: Arc<Cluster>, task_id: u64, scan: bool) -> Self {
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

    /// What the job has done so far, summed over every node.
    ///
    /// A background job runs on each node independently and each reports its own figures, so the
    /// counts add and the job is complete only when every node says so.
    ///
    /// # Errors
    /// When no node is reachable, or a node refuses the status command.
    pub async fn progress(&self) -> Result<JobProgress> {
        let nodes = self.cluster.nodes();
        if nodes.is_empty() {
            return Err(Error::connection("No connected node".to_string()));
        }

        let admin_policy = AdminPolicy { timeout: 3_000 };
        let mut total = JobProgress {
            complete: true,
            ..JobProgress::default()
        };
        let mut reporting = 0u64;

        for node in &nodes {
            let command = Self::build_command(node.version(), self.scan, self.task_id);
            let response = node.info(&admin_policy, &[&command[..]]).await?;
            let Some(reply) = response.get(&command) else {
                // The job may not have reached this node yet, which is not completion.
                total.complete = false;
                continue;
            };
            let node_progress = Self::parse_progress(reply)?;
            total.succeeded += node_progress.succeeded;
            total.failed += node_progress.failed;
            total.filtered_bins += node_progress.filtered_bins;
            total.filtered_meta += node_progress.filtered_meta;
            total.throttled += node_progress.throttled;
            total.percent += node_progress.percent;
            total.complete &= node_progress.complete;
            reporting += 1;
        }

        if reporting > 0 {
            // The mean, so a two-node job at 50% each reads as 50% rather than 100%.
            total.percent /= reporting as f64;
        }
        Ok(total)
    }

    /// Wait for the job to finish, and report what it did.
    ///
    /// # Errors
    /// As [`ExecuteTask::progress`], or when the job does not finish within `timeout`.
    pub async fn wait_for_progress(
        &self,
        timeout: Option<std::time::Duration>,
        interval: std::time::Duration,
    ) -> Result<JobProgress> {
        let deadline = timeout.map(|t| std::time::Instant::now() + t);
        loop {
            let progress = self.progress().await?;
            if progress.complete {
                return Ok(progress);
            }
            if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                return Err(Error::timeout(format!(
                    "background job {} did not finish in time; it had written {} record(s)",
                    self.task_id, progress.succeeded
                )));
            }
            aerospike_rt::sleep(interval).await;
        }
    }

    /// The figures out of one node's reply.
    ///
    /// # Errors
    /// When the reply is an error rather than a status.
    fn parse_progress(reply: &str) -> Result<JobProgress> {
        // A job the server has forgotten is a job that finished — the same reading as
        // `parse_status`, and for the same reason: a modern server drops completed jobs.
        if reply.contains("ERROR:2") {
            return Ok(JobProgress {
                complete: true,
                ..JobProgress::default()
            });
        }
        if reply.starts_with("ERROR") {
            return Err(Error::bad_response(format!(
                "Query execute failed: {reply}"
            )));
        }

        // **Anchored on the field name, not searched for as a substring.** The reply carries
        // `time-since-done`, so testing whether it *contains* "done" is true from the first poll
        // while the job is still at 2% progress — and a caller would then read records mid-write.
        let field = |name: &str| -> Option<&str> {
            reply.split(':').find_map(|part| {
                part.split_once('=')
                    .filter(|(key, _)| *key == name)
                    .map(|(_, value)| value)
            })
        };
        let number = |name: &str| -> u64 { field(name).and_then(|v| v.parse().ok()).unwrap_or(0) };

        Ok(JobProgress {
            complete: field("status").is_some_and(|s| s.to_lowercase().starts_with("done")),
            succeeded: number("recs-succeeded"),
            failed: number("recs-failed"),
            filtered_bins: number("recs-filtered-bins"),
            filtered_meta: number("recs-filtered-meta"),
            throttled: number("recs-throttled"),
            percent: field("job-progress")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
        })
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
            return Err(Error::bad_response(format!(
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
            return Err(Error::connection("No connected node".to_string()));
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
