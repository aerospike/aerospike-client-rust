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
use crate::commands::buffer;
use crate::commands::{Command, SingleCommand};
use crate::errors::{ErrorKind, Result};
use crate::net::Connection;
use crate::operations::OperationType;
use crate::policy::WritePolicy;
use crate::{Bin, Key, ResultCode};

pub struct WriteCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
    bins: &'a [Bin],
    operation: OperationType,
}

impl<'a> WriteCommand<'a> {
    pub fn new(
        policy: &'a WritePolicy,
        cluster: Arc<Cluster>,
        key: &'a Key,
        bins: &'a [Bin],
        operation: OperationType,
    ) -> Self {
        WriteCommand {
            single_command: SingleCommand::new(cluster, key),
            bins,
            policy,
            operation,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl<'a> Command for WriteCommand<'a> {
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
        conn.buffer.set_write(
            self.policy,
            self.operation,
            self.single_command.key,
            self.bins,
        )
    }

    async fn get_node(&self) -> Result<Arc<Node>> {
        self.single_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        // Read header.
        if let Err(err) = conn
            .read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize)
            .await
        {
            warn!("Parse result error: {}", err);
            return Err(err);
        }

        conn.buffer.reset_offset();
        let result_code = ResultCode::from(conn.buffer.read_u8(Some(13)));
        if result_code != ResultCode::Ok {
            bail!(ErrorKind::ServerError(result_code));
        }
        let res = SingleCommand::empty_socket(conn).await;
        res
    }
}
