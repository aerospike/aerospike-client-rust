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

use crate::cluster::{Cluster, Node};
use crate::errors::{Error, Result};
use crate::task::{Status, Task};
use crate::{AdminPolicy, Version};
use std::sync::Arc;

/// Struct for querying index creation status
#[derive(Debug, Clone)]
pub struct DropIndexTask {
    cluster: Arc<Cluster>,
    namespace: String,
    index_name: String,
}

static SUCCESS_PATTERN: &str = "false";

impl DropIndexTask {
    /// Initializes `DropIndexTask` from client, creation should only be expose to Client
    pub fn new(cluster: Arc<Cluster>, namespace: String, index_name: String) -> Self {
        DropIndexTask {
            cluster,
            namespace,
            index_name,
        }
    }

    fn build_command(node: &Arc<Node>, namespace: &str, index_name: &str) -> String {
        let node_version = node.version();

        if node_version >= &Version::new(8, 1, 0, 0) {
            format!(
                "sindex-exists:namespace={};indexname={}",
                namespace, index_name,
            )
        } else {
            format!("sindex-exists:ns={};indexname={}", namespace, index_name)
        }
    }

    fn parse_response(response: &str) -> Result<Status> {
        if response.to_lowercase() == SUCCESS_PATTERN {
            Ok(Status::Complete)
        } else {
            Ok(Status::InProgress)
        }
    }
}

#[async_trait::async_trait]
impl Task for DropIndexTask {
    /// Query the status of execution across all nodes
    async fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes().await;

        if nodes.is_empty() {
            return Err(Error::Connection("No connected node".to_string()));
        }

        let admin_policy = AdminPolicy { timeout: 3_000 };
        for node in &nodes {
            let command = &DropIndexTask::build_command(node, &self.namespace, &self.index_name);
            let response = node.info(&admin_policy, &[&command[..]]).await?;

            if !response.contains_key(command) {
                return Ok(Status::NotFound);
            }

            match DropIndexTask::parse_response(&response[command]) {
                Ok(Status::Complete) => {}
                in_progress_or_error => return in_progress_or_error,
            }
        }
        Ok(Status::Complete)
    }
}
