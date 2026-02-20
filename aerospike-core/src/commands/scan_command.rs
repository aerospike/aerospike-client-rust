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

use aerospike_rt::Mutex;

use crate::cluster::Node;
use crate::commands::{Command, CommandType, NamespaceProvider, SingleCommand, StreamCommand};
use crate::errors::Result;
use crate::net::Connection;
use crate::policy::QueryPolicy;
use crate::query::NodePartitions;
use crate::{Bins, Recordset};

pub struct ScanCommand<'a> {
    stream_command: StreamCommand,
    policy: &'a QueryPolicy,
    namespace: &'a str,
    set_name: &'a str,
    bins: Bins,
}

impl<'a> ScanCommand<'a> {
    pub async fn new(
        policy: &'a QueryPolicy,
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
        crate::report_latency!(self, Arc::downgrade(&self.stream_command.node))
    }
}

#[async_trait::async_trait]
impl Command for ScanCommand<'_> {
    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        let server_timeout = self
            .stream_command
            .recordset
            .tracker
            .lock()
            .await
            .server_timeout();

        conn.buffer.write_timeout(server_timeout);
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

    fn command_type(&self) -> CommandType {
        CommandType::Scan
    }

    fn hint(&self) -> u8 {
        0
    }

    fn can_retry(&mut self) -> bool {
        false
    }

    fn can_recover_connection(&mut self) -> bool {
        false
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        StreamCommand::parse_result(&mut self.stream_command, conn).await
    }
}

impl NamespaceProvider for ScanCommand<'_> {
    fn get_namespaces(&self) -> impl Iterator<Item = (&str, CommandType)> {
        std::iter::once((self.namespace.as_ref(), self.command_type()))
    }
}
