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
use crate::errors::{ErrorKind, Result};
use crate::task::{Status, Task};
use std::sync::Arc;

/// Struct for querying udf register status
#[derive(Debug, Clone)]
pub struct RegisterTask {
    cluster: Arc<Cluster>,
    package_name: String,
}

static COMMAND: &str = "udf-list";
static RESPONSE_PATTERN: &str = "filename=";

impl RegisterTask {
    /// Initializes `RegisterTask` from client, creation should only be expose to Client
    pub fn new(cluster: Arc<Cluster>, package_name: String) -> Self {
        RegisterTask {
            cluster,
            package_name,
        }
    }
}

#[async_trait::async_trait]
impl Task for RegisterTask {
    /// Query the status of index creation across all nodes
    async fn query_status(&self) -> Result<Status> {
        let nodes = self.cluster.nodes().await;

        if nodes.is_empty() {
            bail!(ErrorKind::Connection("No connected node".to_string()))
        }

        for node in &nodes {
            let response = node.info(&[COMMAND]).await?;

            if !response.contains_key(COMMAND) {
                return Ok(Status::NotFound);
            }

            let response_find = format!("{}{}", RESPONSE_PATTERN, self.package_name);
            if !response[COMMAND].contains(&response_find) {
                return Ok(Status::InProgress);
            }
        }
        Ok(Status::Complete)
    }
}
