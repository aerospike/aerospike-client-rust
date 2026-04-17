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

pub struct TouchCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
}

impl<'a> TouchCommand<'a> {
    pub fn new(policy: &'a WritePolicy, cluster: Arc<Cluster>, key: &'a Key) -> Self {
        let partition = crate::cluster::partition::Partition::for_write(key);
        TouchCommand {
            single_command: SingleCommand::new(cluster, key, partition),
            policy,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl Command for TouchCommand<'_> {
    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.write_timeout(self.policy.server_timeout());
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_touch(self.policy, self.single_command.key)
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        self.single_command.get_node()
    }

    fn hint(&self) -> u8 {
        self.single_command.hint()
    }

    fn can_retry(&mut self) -> bool {
        true
    }

    fn can_recover_connection(&mut self) -> bool {
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
        let sz = conn.buffer.read_u64(Some(0));
        let header_length = conn.buffer.read_u8(Some(8));
        let result_code = ResultCode::from(conn.buffer.read_u8(Some(13)));
        let field_count = conn.buffer.read_u16(Some(26)) as usize;
        let receive_size = ((sz & 0xFFFF_FFFF_FFFF) - u64::from(header_length)) as usize;

        if receive_size > 0 {
            conn.buffer.resize_buffer(receive_size)?;
            conn.read_body(receive_size).await?;
            conn.buffer.reset_offset();
        }

        let version = if field_count > 0 {
            conn.buffer.parse_fields_for_version(field_count)
        } else {
            None
        };

        if result_code != ResultCode::Ok {
            return Err(Error::ServerError(result_code, false, conn.addr.clone()));
        }

        if let Some(txn) = &self.policy.base_policy.txn {
            txn.on_write(self.single_command.key, version, result_code);
        }

        Ok(())
    }
}
