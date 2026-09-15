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

use std::sync::Arc;

use crate::cluster::{Cluster, Node};
use crate::commands::buffer::QueryDirection;
use crate::commands::{Command, SingleCommand, StreamCommand};
use crate::errors::Result;
use crate::net::Connection;
use crate::policy::QueryPolicy;
use crate::query::{NodePartitions, QuerySink};
use crate::Statement;

pub struct QueryCommand<'a> {
    stream_command: StreamCommand,
    policy: &'a QueryPolicy,
    statement: Arc<Statement>,
    execute_where: Option<Arc<[u8]>>,
}

impl<'a> QueryCommand<'a> {
    pub fn new(
        policy: &'a QueryPolicy,
        statement: Arc<Statement>,
        sink: QuerySink,
        node_partitions: NodePartitions,
        cluster: Arc<Cluster>,
        execute_where: Option<Arc<[u8]>>,
    ) -> Self {
        let node = node_partitions.node.clone();

        QueryCommand {
            stream_command: StreamCommand::new(node, sink, node_partitions, cluster),
            policy,
            statement,
            execute_where,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }

    /// Returns this node's partition set to the executor once the command has
    /// run, error or not.
    pub(crate) fn into_node_partitions(self) -> NodePartitions {
        self.stream_command.into_node_partitions()
    }
}

#[async_trait::async_trait]
impl Command for QueryCommand<'_> {
    fn cluster(&self) -> Option<&Cluster> {
        self.stream_command.cluster()
    }

    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        self.stream_command.write_timeout(conn).await
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        let node_partitions = self.stream_command.node_partitions();
        let execute_where = self.execute_where.as_deref();
        conn.buffer.set_query(
            QueryDirection::Foreground(self.policy),
            &self.statement,
            self.stream_command.sink.task_id(),
            &node_partitions.node,
            Some(node_partitions),
            execute_where,
        )
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        self.stream_command.get_node()
    }

    fn hint(&self) -> u8 {
        0
    }

    fn command_type(&self) -> crate::metrics::CommandType {
        // A statement with secondary-index filters is a query; otherwise a scan.
        if self.statement.filters.is_some() {
            crate::metrics::CommandType::Query
        } else {
            crate::metrics::CommandType::Scan
        }
    }

    fn namespace(&self) -> Option<&str> {
        Some(&self.statement.namespace)
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
