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

use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use aerospike_rt::Mutex;
use flate2::read::ZlibDecoder;

use crate::cluster::Node;
use crate::commands::buffer::{self, Buffer};
use crate::commands::field_type::FieldType;
use crate::commands::Command;
use crate::errors::{Error, Result};
use crate::net::{BufferedConn, Connection};
use crate::query::{NodePartitions, Recordset};
use crate::value::bytes_to_particle;
use crate::{Key, Record, ResultCode, Value};

pub struct StreamCommand {
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
    pub const fn new(
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
        _size: usize,
    ) -> Result<(Option<Record>, Option<u64>, bool)> {
        let result_code = ResultCode::from(conn.buffer().read_u8(Some(5)));
        if result_code != ResultCode::Ok {
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
                    ));
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
                let tracker = self.recordset.tracker.lock().await;
                let mut node_partitions = self.node_partitions.lock().await;
                tracker
                    .partition_unavailable(&mut node_partitions, generation as u16)
                    .await;
                drop(tracker);
                drop(node_partitions);
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
            let value = bytes_to_particle(particle_type, conn.buffer(), particle_bytes_size)?;

            if !value.is_nil() {
                // list/map operations may return multiple values for the same bin.
                match bins.entry(name) {
                    Vacant(entry) => {
                        entry.insert(value);
                    }
                    Occupied(entry) => match *entry.into_mut() {
                        Value::MultiResult(ref mut list) => list.push(value),
                        ref mut prev => {
                            *prev = Value::MultiResult(vec![prev.clone(), value]);
                        }
                    },
                }
            }
        }

        let record = Record::new(Some(key), bins, generation, expiration);
        Ok((Some(record), bval, true))
    }

    async fn parse_stream(&self, conn: &mut BufferedConn<'_>, size: usize) -> Result<bool> {
        'outer: while !conn.exhausted() {
            // Read header.
            if let Err(err) = conn
                .read_buffer(buffer::MSG_REMAINING_HEADER_SIZE as usize)
                .await
            {
                warn!("Parse result error: {err}");
                return Err(err);
            }

            let res = self.parse_record(conn, size).await;
            match res {
                Ok((Some(rec), bval, _)) => {
                    let tracker = self.recordset.tracker.lock().await;
                    let mut node_partitions = self.node_partitions.lock().await;
                    if !tracker.allow_record(&mut node_partitions) {
                        continue 'outer;
                    }
                    let key = &rec.key.clone().unwrap();
                    self.recordset.push(Ok(rec)).await?;

                    if self.is_scan {
                        tracker.set_digest(&mut node_partitions, key).await?;
                    } else {
                        tracker.set_last(&mut node_partitions, key, bval).await?;
                    }
                    drop(tracker);
                    drop(node_partitions);
                }
                Ok((None, _, false)) => return Ok(false),
                Ok((None, _, true)) => {} // handle partition done
                Err(err) => {
                    // let _ = self.recordset.push(Err(err)).await;
                    return Err(err);
                }
            }
        }

        Ok(true)
    }

    pub async fn parse_key(
        conn: &mut BufferedConn<'_>,
        field_count: usize,
    ) -> Result<(Key, Option<u64>)> {
        Self::parse_key_and_version(conn, field_count)
            .await
            .map(|(key, bval, _version)| (key, bval))
    }

    /// Parse key fields from batch/stream response, also extracting record version if present.
    pub async fn parse_key_and_version(
        conn: &mut BufferedConn<'_>,
        field_count: usize,
    ) -> Result<(Key, Option<u64>, Option<u64>)> {
        let mut digest: [u8; 20] = [0; 20];
        let mut namespace: String = String::new();
        let mut set_name: String = String::new();
        let mut orig_key: Option<Value> = None;
        let mut bval = None;
        let mut version = None;

        for _ in 0..field_count {
            conn.read_buffer(4).await?;
            let field_len = conn.buffer().read_u32(None) as usize;
            conn.read_buffer(field_len).await?;
            let field_type = conn.buffer().read_u8(None);
            let data_size = field_len - 1;

            match field_type {
                x if x == FieldType::DigestRipe as u8 => {
                    digest.copy_from_slice(conn.buffer().read_slice(data_size));
                }
                x if x == FieldType::Namespace as u8 => {
                    namespace = conn.buffer().read_str(data_size)?;
                }
                x if x == FieldType::Table as u8 => {
                    set_name = conn.buffer().read_str(data_size)?;
                }
                x if x == FieldType::Key as u8 => {
                    let particle_type = conn.buffer().read_u8(None);
                    let particle_bytes_size = data_size - 1;
                    orig_key = Some(bytes_to_particle(
                        particle_type,
                        conn.buffer(),
                        particle_bytes_size,
                    )?);
                }
                x if x == FieldType::BValArray as u8 => {
                    bval = Some(conn.buffer().read_le_u64(None));
                }
                x if x == FieldType::RecordVersion as u8 && data_size == 7 => {
                    let buf = conn.buffer();
                    version = Some(Buffer::version_bytes_to_u64(
                        &buf.data_buffer,
                        buf.data_offset(),
                    ));
                    buf.skip(data_size);
                }
                _ => {
                    // Skip unknown field types
                    conn.buffer().skip(data_size);
                }
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
            version,
        ))
    }
}

#[async_trait::async_trait]
impl Command for StreamCommand {
    async fn write_timeout(&mut self, _conn: &mut Connection) -> Result<()> {
        // should be implemented downstream
        unreachable!()
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    #[allow(unused_variables)]
    async fn prepare_buffer(&mut self, _conn: &mut Connection) -> Result<()> {
        // should be implemented downstream
        unreachable!()
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        Ok(self.node.clone())
    }

    fn hint(&self) -> u8 {
        unreachable!()
    }

    fn can_retry(&mut self) -> bool {
        unreachable!()
    }

    fn can_recover_connection(&mut self) -> bool {
        unreachable!()
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        let mut status = true;

        while status {
            let mut conn = BufferedConn::new(conn);

            conn.set_limit_header(8)?;
            conn.read_buffer(8).await?;

            let proto = conn.buffer().read_u64(Some(0));
            let msg_type = ((proto >> 48) & 0xFF) as u8;
            let size = (proto & 0x0000_FFFF_FFFF_FFFF) as usize;

            if msg_type == buffer::AS_MSG_TYPE_COMPRESSED {
                // Compressed stream response: read compressed payload from the
                // network, then stream-decompress records on demand.
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

                // Drain any remaining bytes from the network (should be 0)
                conn.drain(conn.conn.deadline()).await?;

                // All compressed data read from network; clear the flag.
                conn.conn.compressed_stream_body = false;

                // Read only the 8-byte inner proto header to get the message size.
                let mut decoder = ZlibDecoder::new(std::io::Cursor::new(compressed_data));
                let mut proto_buf = [0u8; 8];
                decoder
                    .read_exact(&mut proto_buf)
                    .map_err(|e| Error::ClientError(format!("Stream decompression error: {e}")))?;
                let inner_proto = u64::from_be_bytes(proto_buf);
                let inner_size = (inner_proto & 0x0000_FFFF_FFFF_FFFF) as usize;

                status = false;
                if inner_size > 0 {
                    // Stream-decompress the rest on demand (body after the
                    // 8-byte proto header we already consumed).
                    let body_decompressed_size = uncompressed_size - 8;
                    let mut inner_conn =
                        BufferedConn::new_with_decoder(conn.conn, decoder, body_decompressed_size);

                    match self.parse_stream(&mut inner_conn, inner_size).await {
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
                    match self.parse_stream(&mut conn, size).await {
                        Ok(stat) => status = stat,
                        Err(e @ Error::ServerError(_, _, _)) => {
                            conn.drain(conn.conn.deadline()).await?;
                            return Err(e);
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
                conn.drain(conn.conn.deadline()).await?;
            }
        }

        Ok(())
    }
}
