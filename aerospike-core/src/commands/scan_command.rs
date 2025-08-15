// Copyright 2015-2018 Aerospike, Inc.
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

use std::str;
use std::sync::Arc;

use aerospike_rt::time::Duration;
use aerospike_rt::Mutex;

use crate::cluster::Node;
use crate::commands::{Command, SingleCommand, StreamCommand};
use crate::errors::Result;
use crate::net::Connection;
use crate::policy::ScanPolicy;
use crate::query::NodePartitions;
use crate::{Bins, Recordset};

pub(crate) struct ScanCommand<'a> {
    stream_command: StreamCommand,
    policy: &'a ScanPolicy,
    namespace: &'a str,
    set_name: &'a str,
    bins: Bins,
}

impl<'a> ScanCommand<'a> {
    pub async fn new(
        policy: &'a ScanPolicy,
        namespace: &'a str,
        set_name: &'a str,
        bins: Bins,
        recordset: Arc<Recordset>,
        node_partitions: Arc<Mutex<NodePartitions>>,
    ) -> Self {
        let node = {
            let node_partitions = node_partitions.lock().await;
            node_partitions.node.clone()
        };

        ScanCommand {
            stream_command: StreamCommand::new(node, recordset, node_partitions, true),
            policy,
            namespace,
            set_name,
            bins,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl<'a> Command for ScanCommand<'a> {
    async fn write_timeout(
        &mut self,
        conn: &mut Connection,
        timeout: Option<Duration>,
    ) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        let node_partitions = self.stream_command.node_partitions.lock().await;
        conn.buffer
            .set_scan(
                self.policy,
                self.namespace,
                self.set_name,
                &self.bins,
                self.stream_command.recordset.task_id(),
                &node_partitions,
            )
            .await
    }

    async fn get_node(&mut self) -> Result<Arc<Node>> {
        self.stream_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        StreamCommand::parse_result(&mut self.stream_command, conn).await
    }
}
