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
pub struct IndexTask {
    cluster: Arc<Cluster>,
    namespace: String,
    index_name: String,
}

static SUCCESS_PATTERN: &str = "load_pct=";
static FAIL_PATTERN_201: &str = "FAIL:201";
static FAIL_PATTERN_203: &str = "FAIL:203";
static DELMITER: &str = ";";

impl IndexTask {
    /// Initializes `IndexTask` from client, creation should only be expose to Client
    pub fn new(cluster: Arc<Cluster>, namespace: String, index_name: String) -> Self {
        IndexTask {
            cluster,
            namespace,
            index_name,
        }
    }

    fn build_command(node: &Arc<Node>, namespace: String, index_name: String) -> String {
        if node.version() >= &Version::new(8, 1, 0, 0) {
            format!(
                "sindex-stat:namespace={};indexname={}",
                namespace, index_name
            )
        } else {
            format!("sindex/{}/{}", namespace, index_name)
        }
    }

    fn parse_response(response: &str) -> Result<Status> {
        match response.find(SUCCESS_PATTERN) {
            None => {
                if response.contains(FAIL_PATTERN_201) || response.contains(FAIL_PATTERN_203) {
                    Ok(Status::NotFound)
                } else {
                    return Err(Error::BadResponse(format!(
                        "Code 201 and 203 missing. Response: {}",
                        response
                    )));
                }
            }
            Some(pattern_index) => {
                let percent_begin = pattern_index + SUCCESS_PATTERN.len();

                let percent_end = match response[percent_begin..].find(DELMITER) {
                    None => {
                        return Err(Error::BadResponse(format!(
                            "delimiter missing in response. Response: {}",
                            response
                        )))
                    }
                    Some(percent_end) => percent_end,
                };
                let percent_str = &response[percent_begin..percent_begin + percent_end];
                match percent_str.parse::<isize>() {
                    Ok(100) => Ok(Status::Complete),
                    Ok(_) => Ok(Status::InProgress),
                    Err(_) => {
                        return Err(Error::BadResponse(
                            "Unexpected load_pct value from server".to_string(),
                        ))
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Task for IndexTask {
    /// Query the status of index creation across all nodes
    async fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes().await;

        if nodes.is_empty() {
            return Err(Error::Connection("No connected node".to_string()));
        }

        let admin_policy = AdminPolicy { timeout: 3_000 };
        for node in &nodes {
            let command =
                &IndexTask::build_command(node, self.namespace.clone(), self.index_name.clone());
            let response = node.info(&admin_policy, &[&command[..]]).await?;

            if !response.contains_key(command) {
                return Ok(Status::NotFound);
            }

            match IndexTask::parse_response(&response[command]) {
                Ok(Status::Complete) => {}
                in_progress_or_error => return in_progress_or_error,
            }
        }
        Ok(Status::Complete)
    }
}
