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
use crate::policy::ReadPolicy;
use crate::{Key, Policy, ResultCode};

pub struct ExistsCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a ReadPolicy,
    pub exists: bool,
}

impl<'a> ExistsCommand<'a> {
    pub fn new(policy: &'a ReadPolicy, cluster: Arc<Cluster>, key: &'a Key) -> Self {
        let partition = crate::cluster::partition::Partition::for_read(
            &cluster,
            key,
            policy.replica,
            policy.base_policy.read_mode_sc,
        );
        ExistsCommand {
            single_command: SingleCommand::new(cluster, key, partition),
            policy,
            exists: false,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl Command for ExistsCommand<'_> {
    fn cluster(&self) -> Option<&Cluster> {
        Some(self.single_command.cluster())
    }

    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.write_timeout(self.policy.server_timeout());
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_exists(self.policy, self.single_command.key)
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        self.single_command.get_node()
    }

    fn hint(&self) -> u8 {
        self.single_command.hint()
    }

    fn command_type(&self) -> crate::metrics::CommandType {
        crate::metrics::CommandType::Exists
    }

    fn namespace(&self) -> Option<&str> {
        Some(&self.single_command.key.namespace)
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

        // Drain the response body. With error-detail verbosity enabled the
        // server attaches an ERROR_MESSAGE field to the body (this command is
        // NOBINDATA, so on success there is nothing else to read).
        let mut error_detail = None;
        if receive_size > 0 {
            conn.buffer.resize_buffer(receive_size)?;
            conn.read_body(receive_size).await?;
            conn.buffer.reset_offset();
            if field_count > 0 {
                error_detail = conn.buffer.parse_response_fields(field_count).error_detail;
            }
        }

        if result_code != ResultCode::Ok && result_code != ResultCode::KeyNotFoundError {
            return Err(Error::ServerError(
                result_code,
                false,
                conn.addr.clone(),
                error_detail,
            ));
        }

        self.exists = result_code == ResultCode::Ok;

        Ok(())
    }
}
