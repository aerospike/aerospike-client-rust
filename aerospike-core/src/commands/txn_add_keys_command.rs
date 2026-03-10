// Copyright 2015-2024 Aerospike, Inc.
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
use crate::operations::Operation;
use crate::policy::{Policy, WritePolicy};
use crate::txn::Txn;
use crate::{Key, ResultCode};

pub struct TxnAddKeysCommand<'a> {
    single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
    operations: Vec<Operation>,
    txn: Arc<Txn>,
}

impl<'a> TxnAddKeysCommand<'a> {
    pub fn new(
        policy: &'a WritePolicy,
        cluster: Arc<Cluster>,
        key: &'a Key,
        operations: Vec<Operation>,
        txn: Arc<Txn>,
    ) -> Self {
        let partition = crate::cluster::partition::Partition::for_write(key);
        TxnAddKeysCommand {
            single_command: SingleCommand::new(cluster, key, partition),
            policy,
            operations,
            txn,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl Command for TxnAddKeysCommand<'_> {
    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.write_timeout(self.policy.server_timeout());
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_txn_add_keys(
            self.policy,
            self.single_command.key,
            &self.operations,
        )
    }

    async fn get_node(&mut self) -> Result<Arc<Node>> {
        self.single_command.get_node().await
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
        if let Err(err) = conn.read_header().await {
            warn!("Parse result error: {err}");
            return Err(err);
        }

        conn.buffer.reset_offset();
        let result_code = ResultCode::from(conn.buffer.read_u8(Some(13)));

        if result_code != ResultCode::Ok {
            return Err(Error::ServerError(result_code, false, conn.addr.clone()));
        }

        // Parse the deadline from the response fields.
        let sz = conn.buffer.read_i64(None);
        let header_length = i64::from(conn.buffer.read_u8(None));
        let receive_size = ((sz & 0xFFFF_FFFF_FFFF) - header_length) as usize;

        conn.buffer.reset_offset();
        let field_count = conn.buffer.read_u16(Some(26)) as usize;

        if receive_size > 0 {
            conn.buffer.resize_buffer(receive_size)?;
            conn.read_body(receive_size).await?;
            conn.buffer.reset_offset();

            // Parse MRT_DEADLINE from fields.
            for _ in 0..field_count {
                let field_len = conn.buffer.read_i32(None) as usize;
                let field_type = conn.buffer.read_u8(None);
                let data_size = field_len - 1;

                if field_type == crate::commands::field_type::FieldType::MrtDeadline as u8
                    && data_size == 4
                {
                    let deadline = conn.buffer.read_u32_little_endian(None) as i32;
                    self.txn.set_deadline(deadline);
                } else {
                    conn.buffer.data_offset += data_size;
                }
            }
        }

        Ok(())
    }
}
