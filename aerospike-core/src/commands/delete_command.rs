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
use crate::commands::{Command, SingleCommand};
use crate::errors::{Error, Result};
use crate::net::Connection;
use crate::policy::{Policy, WritePolicy};
use crate::{Key, ResultCode};

pub struct DeleteCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
    pub existed: bool,
}

impl<'a> DeleteCommand<'a> {
    pub fn new(policy: &'a WritePolicy, cluster: Arc<Cluster>, key: &'a Key) -> Self {
        let partition = crate::cluster::partition::Partition::for_write(key);
        DeleteCommand {
            single_command: SingleCommand::new(cluster, key, partition),
            policy,
            existed: false,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl Command for DeleteCommand<'_> {
    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.write_timeout(self.policy.server_timeout());
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_delete(self.policy, self.single_command.key)
    }

    async fn get_node(&mut self) -> Result<Arc<Node>> {
        self.single_command.get_node().await
    }

    fn hint(&self) -> u8 {
        self.single_command.hint()
    }

    fn can_recover_connection(&mut self) -> bool {
        true
    }

    fn can_retry(&mut self) -> bool {
        true
    }

    fn prepare_retry(&mut self, is_client_timeout: bool) {
        self.single_command.prepare_retry(is_client_timeout);
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        // Read header.
        if let Err(err) = conn.read_header().await {
            warn!("Parse result error: {err}");
            return Err(err);
        }

        conn.buffer.reset_offset();

        let result_code = ResultCode::from(conn.buffer.read_u8(Some(13)));

        if result_code != ResultCode::Ok && result_code != ResultCode::KeyNotFoundError {
            return Err(Error::ServerError(result_code, false, conn.addr.clone()));
        }

        self.existed = result_code == ResultCode::Ok;

        SingleCommand::empty_socket(conn).await
    }
}
