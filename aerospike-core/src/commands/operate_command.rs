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
use std::time::Duration;

use crate::cluster::{Cluster, Node};
use crate::commands::{Command, ReadCommand, SingleCommand};
use crate::errors::Result;
use crate::net::Connection;
use crate::operations::Operation;
use crate::policy::WritePolicy;
use crate::{Bins, Key};

pub struct OperateCommand<'a> {
    pub read_command: ReadCommand<'a>,
    policy: &'a WritePolicy,
    operations: &'a [Operation<'a>],
}

impl<'a> OperateCommand<'a> {
    pub fn new(
        policy: &'a WritePolicy,
        cluster: Arc<Cluster>,
        key: &'a Key,
        operations: &'a [Operation<'a>],
    ) -> Self {
        OperateCommand {
            read_command: ReadCommand::new(&policy.base_policy, cluster, key, Bins::All),
            policy,
            operations,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl<'a> Command for OperateCommand<'a> {
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

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_operate(
            self.policy,
            self.read_command.single_command.key,
            self.operations,
        )
    }

    async fn get_node(&self) -> Result<Arc<Node>> {
        self.read_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        self.read_command.parse_result(conn).await
    }
}
