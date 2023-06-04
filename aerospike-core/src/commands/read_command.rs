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
use crate::policy::ReadPolicy;
use crate::{Bins, Key, ReadableBins, Record, ResultCode};

pub struct ReadCommand<'a, T: ReadableBins> {
    pub single_command: SingleCommand<'a>,
    pub record: Option<Record<T>>,
    policy: &'a ReadPolicy,
    bins: Bins,
}

impl<'a, T: ReadableBins> ReadCommand<'a, T> {
    pub fn new(policy: &'a ReadPolicy, cluster: Arc<Cluster>, key: &'a Key, bins: Bins) -> Self {
        ReadCommand {
            single_command: SingleCommand::new(cluster, key),
            bins,
            policy,
            record: None,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.policy, self).await
    }

    async fn parse_record(
        &mut self,
        conn: &mut Connection,
        op_count: usize,
        field_count: usize,
        generation: u32,
        expiration: u32,
    ) -> Result<Record<T>> {
        // There can be fields in the response (setname etc). For now, ignore them. Expose them to
        // the API if needed in the future.
        for _ in 0..field_count {
            conn.read_buffer(4).await?;
            let field_size = conn.buffer.read_u32(None) as usize;
            conn.read_buffer(field_size).await?;
            conn.buffer.skip(field_size);
        }

        let pre_data = conn.pre_parse_stream_bins(op_count).await?;
        let bins = T::read_bins_from_bytes(pre_data)?;
        Ok(Record::new(None, bins, generation, expiration))
    }

    async fn parse_udf_error(
        &mut self,
        conn: &mut Connection,
        op_count: usize,
        field_count: usize,
        generation: u32,
        expiration: u32,
    ) -> Result<String> {
        // There can be fields in the response (setname etc). For now, ignore them. Expose them to
        // the API if needed in the future.
        for _ in 0..field_count {
            conn.read_buffer(4).await?;
            let field_size = conn.buffer.read_u32(None) as usize;
            conn.read_buffer(field_size).await?;
            conn.buffer.skip(field_size);
        }

        let pre_data = conn.pre_parse_stream_bins(op_count).await?;
        let fail = pre_data.get("FAILURE");
        if let Some(fail) = fail {
            return fail.value.buffer.clone().read_str(fail.value.byte_length);
        }
        Ok(String::from("UDF Error"))
    }
}

#[async_trait::async_trait]
impl<'a, T: ReadableBins> Command for ReadCommand<'a, T> {
    async fn write_timeout(
        &mut self,
        conn: &mut Connection,
        timeout: Option<Duration>,
    ) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer
            .set_read(self.policy, self.single_command.key, &self.bins)
    }

    async fn get_node(&self) -> Result<Arc<Node>> {
        self.single_command.get_node().await
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        if let Err(err) = conn
            .read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize)
            .await
        {
            warn!("Parse result error: {}", err);
            bail!(err);
        }

        conn.buffer.reset_offset();
        conn.buffer.skip(9);
        let result_code = conn.buffer.read_u8(Some(13));
        let generation = conn.buffer.read_u32(Some(14));
        let expiration = conn.buffer.read_u32(Some(18));
        let field_count = conn.buffer.read_u16(Some(26)) as usize; // almost certainly 0
        let op_count = conn.buffer.read_u16(Some(28)) as usize;

        match ResultCode::from(result_code) {
            ResultCode::Ok => {
                let record = if self.bins.is_none() {
                    Record::new(None, T::new_empty()?, generation, expiration)
                } else {
                    self.parse_record(conn, op_count, field_count, generation, expiration)
                        .await?
                };
                self.record = Some(record);
                Ok(())
            }
            ResultCode::UdfBadResponse => {
                // record bin "FAILURE" contains details about the UDF error
                let reason = self
                    .parse_udf_error(conn, op_count, field_count, generation, expiration)
                    .await?;
                Err(ErrorKind::UdfBadResponse(reason).into())
            }
            rc => Err(ErrorKind::ServerError(rc).into()),
        }
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }
}
