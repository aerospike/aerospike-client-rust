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

impl IndexTask {
    /// Initializes `IndexTask` from a client, creation should only be exposed to Client
    pub const fn new(cluster: Arc<Cluster>, namespace: String, index_name: String) -> Self {
        IndexTask {
            cluster,
            namespace,
            index_name,
        }
    }

    fn build_command(node: &Arc<Node>, namespace: &str, index_name: &str) -> String {
        if node.version() >= &Version::new(8, 1, 0, 0) {
            format!("sindex-stat:namespace={namespace};indexname={index_name}")
        } else {
            format!("sindex/{namespace}/{index_name}")
        }
    }

    fn parse_response(response: &str) -> Result<Status> {
        if response.is_empty() {
            return Err(Error::BadResponse(
                "sindex-stat failed: empty response".to_string(),
            ));
        }

        let find = "load_pct=";
        match response.find(find) {
            None => {
                // Check if it's an error response
                if response.contains("FAIL") || response.contains("ERROR") {
                    Err(Error::BadResponse(format!(
                        "sindex-stat failed: {response}"
                    )))
                } else {
                    // Index not readable yet, continue polling
                    Ok(Status::InProgress)
                }
            }
            Some(index) => {
                let start = index + find.len();
                let pct_str: String = response[start..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();

                if pct_str.is_empty() {
                    return Err(Error::BadResponse(format!(
                        "sindex-stat failed: could not parse load_pct from response '{response}'"
                    )));
                }

                match pct_str.parse::<usize>() {
                    Ok(pct) if pct >= 100 => Ok(Status::Complete),
                    Ok(_) => Ok(Status::InProgress),
                    Err(_) => Err(Error::BadResponse(format!(
                        "sindex-stat failed: invalid load_pct value '{pct_str}'"
                    ))),
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Task for IndexTask {
    /// Query the status of index creation across all nodes
    async fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes();

        if nodes.is_empty() {
            return Err(Error::Connection("No connected node".to_string()));
        }

        let admin_policy = AdminPolicy { timeout: 3_000 };
        for node in &nodes {
            let command =
                IndexTask::build_command(node, &self.namespace, &self.index_name);
            let response = node.info(&admin_policy, &[&command[..]]).await?;

            match response.get(&command) {
                None => {
                    return Err(Error::BadResponse(
                        "sindex-stat failed: missing response".to_string(),
                    ));
                }
                Some(resp) => match IndexTask::parse_response(resp) {
                    Ok(Status::Complete) => {}
                    in_progress_or_error => return in_progress_or_error,
                },
            }
        }
        Ok(Status::Complete)
    }
}
