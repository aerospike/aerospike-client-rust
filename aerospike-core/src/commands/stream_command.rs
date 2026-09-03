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

use crate::IndexMap;
use std::io::Read;
use std::sync::Arc;

use flate2::read::ZlibDecoder;

use crate::cluster::{Cluster, Node};
use crate::commands::buffer::{self, Buffer};
use crate::commands::field_type::FieldType;
use crate::commands::Command;
use crate::errors::{Error, ErrorKind, Result};
use crate::net::{BufferedConn, Connection};
use crate::query::{NodePartitions, QuerySink, Recordset, StreamEntry, RECORD_BATCH};
use crate::value::bytes_to_particle;
use crate::{Key, Record, ResultCode, Value};

pub struct StreamCommand {
    node: Arc<Node>,
    /// The owning cluster, surfaced via [`Command::cluster`] so the retry loop
    /// can record cluster-wide `exceeded-*` counters for scans/queries.
    cluster: Arc<Cluster>,
    pub(crate) sink: QuerySink,
    /// This node's share of the query, owned outright for the life of the
    /// command. The executor moves it in and takes it back from the join
    /// value, so the single-writer story is checked by the compiler rather
    /// than promised by the protocol.
    node_partitions: NodePartitions,
    /// Announces this stream's end to the sink when the command goes away,
    /// however it goes away. A guard field rather than `impl Drop` on the
    /// command itself, which would forbid moving `node_partitions` back out
    /// at the end.
    _end: EndSignal,
}

/// Signals end-of-stream to the sink on drop.
struct EndSignal(QuerySink);

impl Drop for EndSignal {
    fn drop(&mut self) {
        self.0.signal_end();
    }
}

impl StreamCommand {
    pub fn new(
        node: Arc<Node>,
        sink: QuerySink,
        node_partitions: NodePartitions,
        cluster: Arc<Cluster>,
    ) -> Self {
        StreamCommand {
            node,
            cluster,
            _end: EndSignal(sink.clone()),
            sink,
            node_partitions,
        }
    }

    /// This node's partition set, for the request buffer.
    pub(crate) const fn node_partitions(&self) -> &NodePartitions {
        &self.node_partitions
    }

    /// Hands the partition set back to the executor once the command is done,
    /// so completion and retry planning can read what this node did. Consumes
    /// the command; the end-of-stream signal fires here.
    pub(crate) fn into_node_partitions(self) -> NodePartitions {
        self.node_partitions
    }

    async fn parse_record(
        &mut self,
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
                    return Err(Error::server_error(
                        result_code,
                        conn.conn.addr.clone(),
                        None,
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
                self.sink
                    .tracker()
                    .partition_unavailable(&mut self.node_partitions, generation as u16);
            }
            return Ok((None, None, true));
        }

        let mut bins: IndexMap<String, Value> = IndexMap::with_capacity(op_count);

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
                // For query/scan read with duplicate bin projections will overwrite.
                bins.insert(name, value);
            }
        }

        let record = Record::new(Some(key), bins, None, generation, expiration);
        Ok((Some(record), bval, true))
    }

    /// Hands the accumulated batch to the consumer and counts it against
    /// this node's round.
    ///
    /// Only the *count* is recorded here — it drives the executor's
    /// max-records budget and page-done planning, which reason about what the
    /// server sent this round. The resume *cursor* is deliberately not
    /// touched: it is committed at the consumer edge, record by record, as
    /// the user takes them out — so a stream closed with records still
    /// buffered resumes from the last record the user saw, not the last one
    /// parsed. A failed push counts nothing: those records are re-fetched by
    /// the next round.
    async fn flush_batch(
        recordset: &Recordset,
        node_partitions: &mut NodePartitions,
        batch: &mut Vec<StreamEntry>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let count = batch.len() as u64;
        recordset.push_batch(std::mem::take(batch)).await?;
        node_partitions.record_count += count;
        Ok(())
    }

    async fn parse_stream(&mut self, conn: &mut BufferedConn<'_>, size: usize) -> Result<bool> {
        // One channel send per RECORD_BATCH records, not per record: the
        // queue-slot handoff and consumer wakeup were the hot path's largest
        // single cost.
        let mut batch: Vec<StreamEntry> = Vec::with_capacity(RECORD_BATCH);

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
                    // No lock on this path: the tracker's record-facing state
                    // is atomics, and the partition set has a single writer —
                    // this command.
                    if !self
                        .sink
                        .tracker()
                        .allow_record(&mut self.node_partitions)
                    {
                        continue 'outer;
                    }

                    match &self.sink {
                        QuerySink::Channel(rs) => {
                            // Stamp after the allow gate: rejected records
                            // are neither delivered nor sequenced.
                            let key = rec.key.as_ref().unwrap();
                            let stamp = rs.tracker.stamp_delivery(key.partition_id());
                            batch.push(StreamEntry {
                                result: Ok(rec),
                                bval,
                                stamp,
                            });
                            if batch.len() >= RECORD_BATCH {
                                Self::flush_batch(rs, &mut self.node_partitions, &mut batch)
                                    .await?;
                            }
                        }
                        QuerySink::Callback(ctx) => {
                            // Inline, C-style: the callback runs here on the
                            // node task and the cursor commits the moment it
                            // returns — delivery and commit are atomic, which
                            // is what makes this mode exactly-once, cancel and
                            // resume included.
                            let key = rec.key.as_ref().unwrap();
                            let (pid, digest) = (key.partition_id(), key.digest);
                            let keep_going = (ctx.callback)(Ok(rec));
                            ctx.tracker.commit_direct(pid, digest, bval);
                            self.node_partitions.record_count += 1;
                            if !keep_going {
                                self.sink.close();
                            }
                            if !self.sink.is_active() {
                                return Err(Error::stream_terminated(None));
                            }
                        }
                    }
                }
                Ok((None, _, false)) => {
                    if let QuerySink::Channel(rs) = &self.sink {
                        Self::flush_batch(rs, &mut self.node_partitions, &mut batch).await?;
                    }
                    return Ok(false);
                }
                Ok((None, _, true)) => {} // handle partition done
                Err(err) => {
                    // Not flushed: the un-pushed tail is dropped with its
                    // progress uncommitted, so a retry round re-fetches it.
                    // let _ = self.recordset.push(Err(err)).await;
                    return Err(err);
                }
            }
        }

        if let QuerySink::Channel(rs) = &self.sink {
            Self::flush_batch(rs, &mut self.node_partitions, &mut batch).await?;
        }
        Ok(true)
    }

    pub async fn parse_key(
        conn: &mut BufferedConn<'_>,
        field_count: usize,
    ) -> Result<(Key, Option<u64>)> {
        Self::parse_key_and_version(conn, field_count)
            .await
            .map(|(key, bval, _version, _detail)| (key, bval))
    }

    /// Parse key fields from batch/stream response, also extracting record version if present.
    #[allow(clippy::type_complexity)]
    pub async fn parse_key_and_version(
        conn: &mut BufferedConn<'_>,
        field_count: usize,
    ) -> Result<(
        Key,
        Option<u64>,
        Option<u64>,
        Option<Box<crate::ServerErrorDetail>>,
    )> {
        let mut digest: [u8; 20] = [0; 20];
        let mut namespace: String = String::new();
        let mut set_name: String = String::new();
        let mut orig_key: Option<Value> = None;
        let mut bval = None;
        let mut version = None;
        // A failing row carries the server's explanation as a field; without
        // capturing it here the unknown-field arm below would skip it.
        let mut error_detail: Option<Box<crate::ServerErrorDetail>> = None;

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
                x if x == FieldType::ErrorMessage as u8 && data_size > 0 => {
                    let buf = conn.buffer();
                    let start = buf.data_offset();
                    if let Some(slice) = buf.data_buffer.get(start..start + data_size) {
                        error_detail =
                            crate::server_error::parse_error_detail(slice).map(Box::new);
                    }
                    conn.buffer().skip(data_size);
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
            error_detail,
        ))
    }
}

#[async_trait::async_trait]
impl Command for StreamCommand {
    fn cluster(&self) -> Option<&Cluster> {
        Some(&self.cluster)
    }

    async fn write_timeout(&mut self, conn: &mut Connection) -> Result<()> {
        // Fill the header timeout bytes (22..26) from the partition
        // tracker, exactly like the query wrapper — previously this was
        // `unreachable!()` on the assumption that only `QueryCommand`
        // drives a `StreamCommand`; implementing it here keeps any direct
        // driver from panicking and from sending a zero timeout.
        let server_timeout = self.sink.tracker().server_timeout();
        conn.buffer.write_timeout(server_timeout);
        Ok(())
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
                    .map_err(|e| Error::client_error(format!("Stream decompression error: {e}")))?;
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
                        Err(e) if matches!(e.kind(), ErrorKind::Server { .. }) => {
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
                        Err(e) if matches!(e.kind(), ErrorKind::Server { .. }) => {
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
