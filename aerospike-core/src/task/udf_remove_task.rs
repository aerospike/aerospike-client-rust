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

use crate::cluster::Cluster;
use crate::errors::{Error, Result};
use crate::task::{Status, Task};
use crate::AdminPolicy;
use std::sync::Arc;

/// Struct for querying index creation status
#[derive(Debug, Clone)]
pub struct UdfRemoveTask {
    cluster: Arc<Cluster>,
    package_name: String,
}

impl UdfRemoveTask {
    /// Initializes `UdfRemoveTask` from client, creation should only be expose to Client
    pub fn new(cluster: Arc<Cluster>, package_name: String) -> Self {
        UdfRemoveTask {
            cluster,
            package_name,
        }
    }

    fn build_command() -> String {
        String::from("udf-list")
    }

    fn parse_response(response: &str, package_name: &str) -> Result<Status> {
        let find = format!("filename={}", package_name);
        if !response.contains(&find) {
            Ok(Status::Complete)
        } else {
            Ok(Status::InProgress)
        }
    }
}

#[async_trait::async_trait]
impl Task for UdfRemoveTask {
    /// Query the status of execution across all nodes
    async fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes().await;

        if nodes.is_empty() {
            return Err(Error::Connection("No connected node".to_string()));
        }

        let admin_policy = AdminPolicy { timeout: 3_000 };
        for node in &nodes {
            let command = &UdfRemoveTask::build_command();
            let response = node.info(&admin_policy, &[&command[..]]).await?;

            if !response.contains_key(command) {
                return Ok(Status::NotFound);
            }

            match UdfRemoveTask::parse_response(&response[command], &self.package_name) {
                Ok(Status::Complete) => {}
                in_progress_or_error => return in_progress_or_error,
            }
        }
        Ok(Status::Complete)
    }
}
