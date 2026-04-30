// Copyright 2015-2020 Aerospike, Inc.
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

use std::io::Read;
use std::sync::Arc;

use flate2::read::ZlibDecoder;

use crate::cluster::Node;
use crate::commands::buffer;
use crate::commands::{Command, SingleCommand};
use crate::errors::{Error, Result};
use crate::net::{BufferedConn, Connection};
use crate::operations::Operation;
use crate::policy::{Policy, WritePolicy};
use crate::{ResultCode, Statement};

/// Payload type for background server commands.
pub enum ServerCommandPayload<'a> {
    /// Apply write operations to matching records.
    Operations(&'a [Operation]),
    /// Apply a UDF to matching records.
    Udf,
}

/// Command that executes a background query/scan on a single node,
/// applying write operations or a UDF to matching records without returning data.
pub struct ServerCommand<'a> {
    node: Arc<Node>,
    write_policy: &'a WritePolicy,
    statement: &'a Statement,
    task_id: u64,
    payload: ServerCommandPayload<'a>,
}

impl<'a> ServerCommand<'a> {
    pub const fn new(
        node: Arc<Node>,
        write_policy: &'a WritePolicy,
        statement: &'a Statement,
        task_id: u64,
        operations: &'a [Operation],
    ) -> Self {
        ServerCommand {
            node,
            write_policy,
            statement,
            task_id,
            payload: ServerCommandPayload::Operations(operations),
        }
    }

    pub const fn new_udf(
        node: Arc<Node>,
        write_policy: &'a WritePolicy,
        statement: &'a Statement,
        task_id: u64,
    ) -> Self {
        ServerCommand {
            node,
            write_policy,
            statement,
            task_id,
            payload: ServerCommandPayload::Udf,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        SingleCommand::execute(self.write_policy, self).await
    }
}

#[async_trait::async_trait]
impl Command for ServerCommand<'_> {
    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer
            .write_timeout(self.write_policy.socket_timeout());
        Ok(())
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        match &self.payload {
            ServerCommandPayload::Operations(operations) => conn.buffer.set_query_operate(
                self.write_policy,
                self.statement,
                self.task_id,
                operations,
            ),
            ServerCommandPayload::Udf => {
                conn.buffer
                    .set_query_udf_execute(self.write_policy, self.statement, self.task_id)
            }
        }
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        Ok(self.node.clone())
    }

    fn hint(&self) -> u8 {
        0
    }

    fn can_retry(&mut self) -> bool {
        true
    }

    fn can_recover_connection(&mut self) -> bool {
        false
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        // Server commands should only send back a return code.
        // Still parse the response to drain the socket.
        let mut status = true;
        while status {
            let mut conn = BufferedConn::new(conn);

            conn.set_limit_header(8)?;
            conn.read_buffer(8).await?;

            let proto = conn.buffer().read_u64(Some(0));
            let msg_type = ((proto >> 48) & 0xFF) as u8;
            let size = (proto & 0x0000_FFFF_FFFF_FFFF) as usize;

            if msg_type == buffer::AS_MSG_TYPE_COMPRESSED {
                // Compressed server-command response: read compressed payload
                // from the network, then stream-decompress on demand.
                conn.conn.compressed_stream_body = true;
                conn.bookmark();
                conn.set_limit_body(size)?;

                // Read the 8-byte uncompressed size
                conn.read_buffer(8).await?;
                let uncompressed_size = conn.buffer().read_u64(Some(0)) as usize;

                // Read all remaining compressed data
                let compressed_len = size - 8;
                conn.read_buffer(compressed_len).await?;
                let compressed_data = conn.buffer().data_buffer[..compressed_len].to_vec();

                // All compressed data read from network; clear the flag.
                conn.conn.compressed_stream_body = false;

                // Read only the 8-byte inner proto header to get the message size.
                let mut decoder = ZlibDecoder::new(std::io::Cursor::new(compressed_data));
                let mut proto_buf = [0u8; 8];
                decoder.read_exact(&mut proto_buf).map_err(|e| {
                    Error::ClientError(format!("Server command decompression error: {e}"))
                })?;
                let inner_proto = u64::from_be_bytes(proto_buf);
                let inner_size = (inner_proto & 0x0000_FFFF_FFFF_FFFF) as usize;

                status = false;
                if inner_size > 0 {
                    // Stream-decompress the rest on demand (body after the
                    // 8-byte proto header we already consumed).
                    let body_decompressed_size = uncompressed_size - 8;
                    let mut inner_conn =
                        BufferedConn::new_with_decoder(conn.conn, decoder, body_decompressed_size);

                    match self.parse_record_results(&mut inner_conn).await {
                        Ok(stat) => status = stat,
                        Err(e @ Error::ServerError(_, _, _)) => {
                            inner_conn.drain(inner_conn.conn.deadline()).await?;
                            return Err(e);
                        }
                        Err(e) => return Err(e),
                    }
                    inner_conn.drain(inner_conn.conn.deadline()).await?;
                }
            } else {
                conn.bookmark();

                status = false;
                if size > 0 {
                    conn.set_limit_body(size)?;
                    match self.parse_record_results(&mut conn).await {
                        Ok(stat) => status = stat,
                        Err(e @ Error::ServerError(_, _, _)) => {
                            conn.drain(conn.conn.deadline()).await?;
                            return Err(e);
                        }
                        Err(e) => return Err(e),
                    }
                }
                conn.drain(conn.conn.deadline()).await?;
            }
        }
        Ok(())
    }
}

impl ServerCommand<'_> {
    async fn parse_record_results(&self, conn: &mut BufferedConn<'_>) -> Result<bool> {
        while !conn.exhausted() {
            conn.read_buffer(buffer::MSG_REMAINING_HEADER_SIZE as usize)
                .await?;

            let result_code = ResultCode::from(conn.buffer().read_u8(Some(5)));

            // Check for end of response
            let info3 = conn.buffer().read_u8(Some(3));
            if info3 & buffer::INFO3_LAST == buffer::INFO3_LAST {
                if result_code != ResultCode::Ok {
                    return Err(Error::ServerError(
                        result_code,
                        false,
                        conn.conn.addr.clone(),
                    ));
                }
                return Ok(false);
            }

            if result_code != ResultCode::Ok {
                return Err(Error::ServerError(
                    result_code,
                    false,
                    conn.conn.addr.clone(),
                ));
            }

            // Skip past remaining header fields
            conn.buffer().skip(6);
            conn.buffer().read_u32(None); // generation
            conn.buffer().read_u32(None); // expiration
            conn.buffer().skip(4);
            let field_count = conn.buffer().read_u16(None) as usize;
            let op_count = conn.buffer().read_u16(None) as usize;

            // Skip fields
            for _ in 0..field_count {
                conn.read_buffer(4).await?;
                let field_len = conn.buffer().read_u32(None) as usize;
                conn.read_buffer(field_len).await?;
                conn.buffer().skip(field_len);
            }

            // Skip operations
            for _ in 0..op_count {
                conn.read_buffer(8).await?;
                let op_size = conn.buffer().read_u32(None) as usize;
                conn.buffer().skip(1); // op type
                conn.buffer().skip(1); // particle type
                conn.buffer().skip(1); // version
                let name_size = conn.buffer().read_u8(None) as usize;
                conn.read_buffer(name_size).await?;
                conn.buffer().skip(name_size);
                let particle_bytes_size = op_size - (4 + name_size);
                if particle_bytes_size > 0 {
                    conn.read_buffer(particle_bytes_size).await?;
                    conn.buffer().skip(particle_bytes_size);
                }
            }
        }
        Ok(true)
    }
}
