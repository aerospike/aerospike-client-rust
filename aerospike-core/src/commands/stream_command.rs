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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aerospike_rt::Mutex;

use crate::cluster::Node;
use crate::commands::buffer;
use crate::commands::field_type::FieldType;
use crate::commands::Command;
use crate::errors::{Error, Result};
use crate::net::{BufferedConn, Connection};
use crate::query::{NodePartitions, Recordset};
use crate::value::bytes_to_particle;
use crate::{Key, Record, ResultCode, Value};

pub(crate) struct StreamCommand {
    is_scan: bool,
    node: Arc<Node>,
    pub(crate) recordset: Arc<Recordset>,
    pub(crate) node_partitions: Arc<Mutex<NodePartitions>>,
}

impl Drop for StreamCommand {
    fn drop(&mut self) {
        // signal_end
        self.recordset.signal_end();
    }
}

impl StreamCommand {
    pub fn new(
        node: Arc<Node>,
        recordset: Arc<Recordset>,
        node_partitions: Arc<Mutex<NodePartitions>>,
        is_scan: bool,
    ) -> Self {
        StreamCommand {
            is_scan,
            node,
            recordset,
            node_partitions,
        }
    }

    async fn parse_record(
        &self,
        conn: &mut BufferedConn<'_>,
        size: usize,
    ) -> Result<(Option<Record>, Option<u64>, bool)> {
        let result_code = ResultCode::from(conn.buffer().read_u8(Some(5)));
        if result_code != ResultCode::Ok {
            if conn.bytes_read() < size {
                let remaining = size - conn.bytes_read();
                conn.read_buffer(remaining).await?;
            }

            match result_code {
                ResultCode::KeyNotFoundError | ResultCode::FilteredOut => {
                    return Ok((None, None, false))
                }
                ResultCode::PartitionUnavailable => (),
                _ => {
                    return Err(Error::ServerError(
                        result_code,
                        false,
                        conn.conn.addr.clone(),
                    ))
                }
            }
        }

        // if cmd is the end marker of the response, do not proceed further
        let info3 = conn.buffer().read_u8(Some(3));
        if info3 & buffer::INFO3_LAST == buffer::INFO3_LAST {
            return Ok((None, None, false));
        }

        conn.buffer().skip(6);
        let generation = conn.buffer().read_u32(None);
        let expiration = conn.buffer().read_u32(None);
        conn.buffer().skip(4);
        let field_count = conn.buffer().read_u16(None) as usize; // almost certainly 0
        let op_count = conn.buffer().read_u16(None) as usize;

        let (key, bval) = StreamCommand::parse_key(conn, field_count).await?;

        // Partition is done, don't go further
        if info3 & buffer::INFO3_PARTITION_DONE != 0 {
            // return Ok((None, true));
            if result_code != ResultCode::Ok {
                let mut tracker = self.recordset.tracker.lock().await;
                let mut node_partitions = self.node_partitions.lock().await;
                tracker
                    .partition_unavailable(&mut node_partitions, generation as u16)
                    .await;
            }
            return Ok((None, None, true));
        }

        let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

        for _ in 0..op_count {
            conn.read_buffer(8).await?;
            let op_size = conn.buffer().read_u32(None) as usize;
            conn.buffer().skip(1);
            let particle_type = conn.buffer().read_u8(None);
            conn.buffer().skip(1);
            let name_size = conn.buffer().read_u8(None) as usize;
            conn.read_buffer(name_size).await?;
            let name: String = conn.buffer().read_str(name_size)?;

            let particle_bytes_size = op_size - (4 + name_size);
            conn.read_buffer(particle_bytes_size).await?;
            let value = bytes_to_particle(particle_type, &mut conn.buffer(), particle_bytes_size)?;

            bins.insert(name, value);
        }

        let record = Record::new(Some(key), bins, generation, expiration);
        Ok((Some(record), bval, true))
    }

    async fn parse_stream<'a>(
        &mut self,
        conn: &'a mut BufferedConn<'a>,
        size: usize,
    ) -> Result<bool> {
        'outer: while self.recordset.is_active() && !conn.exhausted() {
            // Read header.
            if let Err(err) = conn
                .read_buffer(buffer::MSG_REMAINING_HEADER_SIZE as usize)
                .await
            {
                warn!("Parse result error: {}", err);
                return Err(err);
            }

            let res = self.parse_record(conn, size).await;
            match res {
                Ok((Some(rec), bval, _)) => {
                    let mut tracker = self.recordset.tracker.lock().await;
                    let mut node_partitions = self.node_partitions.lock().await;
                    if !tracker.allow_record(&mut node_partitions) {
                        continue 'outer;
                    }
                    let key = &rec.key.clone().unwrap();
                    self.recordset.busy_push(Ok(rec)).await;

                    if self.is_scan {
                        tracker.set_digest(&mut node_partitions, key).await?;
                    } else {
                        tracker.set_last(&mut node_partitions, key, bval).await?;
                    }
                }
                Ok((None, _, false)) => return Ok(false),
                Ok((None, _, true)) => continue, // handle partition done
                Err(err) => {
                    self.recordset.busy_push(Err(err)).await;
                    return Ok(false);
                }
            };
        }

        Ok(true)
    }

    pub async fn parse_key(
        conn: &mut BufferedConn<'_>,
        field_count: usize,
    ) -> Result<(Key, Option<u64>)> {
        let mut digest: [u8; 20] = [0; 20];
        let mut namespace: String = "".to_string();
        let mut set_name: String = "".to_string();
        let mut orig_key: Option<Value> = None;
        let mut bval = None;

        for _ in 0..field_count {
            conn.read_buffer(4).await?;
            let field_len = conn.buffer().read_u32(None) as usize;
            conn.read_buffer(field_len).await?;
            let field_type = conn.buffer().read_u8(None);

            match field_type {
                x if x == FieldType::DigestRipe as u8 => {
                    digest.copy_from_slice(conn.buffer().read_slice(field_len - 1));
                }
                x if x == FieldType::Namespace as u8 => {
                    namespace = conn.buffer().read_str(field_len - 1)?;
                }
                x if x == FieldType::Table as u8 => {
                    set_name = conn.buffer().read_str(field_len - 1)?;
                }
                x if x == FieldType::Key as u8 => {
                    let particle_type = conn.buffer().read_u8(None);
                    let particle_bytes_size = field_len - 2;
                    orig_key = Some(bytes_to_particle(
                        particle_type,
                        &mut conn.buffer(),
                        particle_bytes_size,
                    )?);
                }
                x if x == FieldType::BValArray as u8 => {
                    bval = Some(conn.buffer().read_le_u64(None));
                }
                _ => unreachable!(),
            }
        }

        Ok((
            Key {
                namespace,
                set_name,
                user_key: orig_key,
                digest,
            },
            bval,
        ))
    }
}

#[async_trait::async_trait]
impl Command for StreamCommand {
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

    #[allow(unused_variables)]
    async fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        // should be implemented downstream
        unreachable!()
    }

    async fn get_node(&mut self) -> Result<Arc<Node>> {
        Ok(self.node.clone())
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        let mut status = true;

        while status {
            let mut conn = BufferedConn::new(conn);

            conn.set_limit(8);
            conn.read_buffer(8).await?;
            let size = conn.buffer().read_msg_size(None);
            conn.bookmark();

            status = false;
            if size > 0 {
                conn.set_limit(size);
                // status = self.parse_stream(&mut conn, size as usize).await?;
                status = self.parse_stream(&mut conn, size as usize).await?
            }
        }

        Ok(())
    }
}
