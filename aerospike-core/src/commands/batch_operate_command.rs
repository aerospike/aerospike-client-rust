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

use aerospike_rt::time::Instant;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use flate2::read::ZlibDecoder;

use crate::batch::BatchOperation;
use crate::batch::BatchRecordIndex;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::StreamCommand;
use crate::commands::{self, buffer};
use crate::errors::{Error, Result};
use crate::net::{BufferedConn, Connection};
use crate::policy::{BatchPolicy, Policy, Replica};
use crate::{value, Record, ResultCode, Value};
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

#[derive(Clone)]
pub struct BatchOperateCommand {
    policy: BatchPolicy,
    pub node: Arc<Node>,
    pub batch_ops: Vec<(BatchOperation, usize)>,
}

impl BatchOperateCommand {
    pub const fn new(
        policy: BatchPolicy,
        node: Arc<Node>,
        batch_ops: Vec<(BatchOperation, usize)>,
    ) -> BatchOperateCommand {
        BatchOperateCommand {
            policy,
            node,
            batch_ops,
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub async fn execute(self, cluster: Arc<Cluster>) -> Result<Self> {
        if self.policy.total_timeout() > 0 {
            let res = aerospike_rt::timeout(
                Duration::from_millis(u64::from(self.policy.total_timeout())),
                self.execute_command(cluster),
            )
            .await;
            match res {
                Ok(res) => res,
                Err(_) => Err(Error::Timeout("Timeout".to_string())),
            }
        } else {
            self.execute_command(cluster).await
        }
    }

    pub async fn execute_command(mut self, cluster: Arc<Cluster>) -> Result<Self> {
        let mut iterations = 0;
        let mut last_err: Option<Error> = None;

        // set timeout outside the loop
        let deadline = self.policy.deadline();

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            let retry_err = if iterations & 1 == 0 || matches!(self.policy.replica, Replica::Master)
            {
                // For even iterations, we request all keys from the same node for efficiency.
                Self::request_group(
                    &mut self.batch_ops,
                    &self.policy,
                    deadline,
                    self.node.clone(),
                )
                .await?
            } else {
                // However, for odd iterations try the second choice for each. Instead of re-sharding the batch (as the second choice may not correspond to the first), just try each by itself.
                let mut group_err = None;
                for individual_op in self.batch_ops.chunks_mut(1) {
                    let key = individual_op[0].0.key();
                    // Find somewhere else to try.
                    let mut partition = Partition::for_read(
                        &cluster,
                        &key,
                        self.policy.replica,
                        self.policy.base_policy.read_mode_sc,
                    );
                    // Advance sequence to try a different node than the primary
                    partition.sequence = 1;
                    let node = partition.get_node(&cluster)?;

                    if let Some(e) =
                        Self::request_group(individual_op, &self.policy, deadline, node).await?
                    {
                        group_err = Some(e);
                        break;
                    }
                }
                group_err
            };

            if let Some(e) = retry_err {
                last_err = Some(e.chain_cause(last_err));
            } else {
                // command has completed successfully. Exit method.
                return Ok(self);
            }

            iterations += 1;

            // too many retries
            if self.policy.max_retries() > 0 && iterations > self.policy.max_retries() + 1 {
                return Err(Error::Timeout(format!("Timeout after {iterations} tries"))
                    .chain_cause(last_err));
            }

            // Sleep before trying again, after the first iteration
            if let Some(sleep_between_retries) = self.policy.sleep_between_retries() {
                sleep(sleep_between_retries).await;
            }

            // check for command timeout
            if let Some(deadline) = deadline {
                if Instant::now() > deadline {
                    return Err(Error::Timeout(format!(
                        "Command timed out after {iterations} tries"
                    ))
                    .chain_cause(last_err));
                }
            }
        }
    }

    async fn request_group(
        batch_ops: &mut [(BatchOperation, usize)],
        policy: &BatchPolicy,
        deadline: Option<Instant>,
        node: Arc<Node>,
    ) -> Result<Option<Error>> {
        let mut conn = match node.get_connection(0).await {
            Ok(conn) => conn,
            Err(err) => {
                warn!("Node {node}: {err}");
                return Ok(Some(err));
            }
        };

        conn.buffer.set_compress(policy.use_compression());
        conn.buffer
            .set_batch_operate(policy, batch_ops)
            .map_err(|_| Error::ClientError("Failed to prepare send buffer".into()))?;

        conn.buffer.write_timeout(policy.server_timeout());

        if policy.use_compression() {
            conn.buffer
                .compress()
                .map_err(|_| Error::ClientError("Failed to compress send buffer".into()))?;
        }

        conn.set_socket_timeout(deadline, policy.socket_timeout());
        conn.set_timeout_delay(true, policy.timeout_delay());

        // Send command.
        if let Err(err) = conn.flush().await {
            // IO errors are considered temporary anomalies. Retry.
            // Close socket to flush out possible garbage. Do not put back in pool.
            conn.invalidate();
            warn!("Node {node}: {err}");
            return Ok(Some(err));
        }

        // Parse results.
        if let Err(err) =
            Self::parse_result(batch_ops, &mut conn, policy.base_policy.txn.as_ref()).await
        {
            // close the connection
            // cancelling/closing the batch/multi commands will return an error, which will
            // close the connection to throw away its data and signal the server about the
            // situation. We will not put back the connection in the buffer.
            if !Self::keep_connection(&err) {
                conn.invalidate();
            }
            Err(err)
        } else {
            Ok(None)
        }
    }

    async fn parse_group(
        batch_ops: &mut [(BatchOperation, usize)],
        conn: &mut BufferedConn<'_>,
        size: usize,
        txn: Option<&Arc<crate::txn::Txn>>,
    ) -> Result<bool> {
        while conn.bytes_read() < size {
            conn.read_buffer(commands::buffer::MSG_REMAINING_HEADER_SIZE as usize)
                .await?;
            match Self::parse_record(conn).await {
                Ok(None) => return Ok(false),
                Ok(Some(batch_record)) => {
                    let batch_op = batch_ops
                        .get_mut(batch_record.batch_index)
                        .expect("Invalid batch index");

                    // Update transaction state with version info
                    if let Some(txn) = txn {
                        let key = &batch_op.0.key();
                        if batch_op.0.has_write() {
                            txn.on_write(key, batch_record.version, batch_record.result_code);
                        } else {
                            txn.on_read(key, batch_record.version);
                        }
                    }

                    batch_op.0.set_record(batch_record.record);
                    batch_op.0.set_result_code(batch_record.result_code, false);
                }
                Err(Error::BatchLastError(batch_index, rc, in_doubt, ..)) => {
                    // Per-key error on the final record of the response. Record the
                    // error on the individual BatchRecord and treat the stream as
                    // ended — do not propagate as a batch-level failure, matching
                    // Java's behavior (BatchStatus.setRowError keeps other records).
                    let batch_op = batch_ops
                        .get_mut(batch_index as usize)
                        .expect("Invalid batch index");
                    batch_op.0.set_result_code(rc, in_doubt);
                    return Ok(false);
                }
                Err(Error::BatchError(batch_index, rc, in_doubt, ..)) => {
                    // Per-key error mid-stream. Record on the individual BatchRecord
                    // and continue parsing remaining records in the response.
                    let batch_op = batch_ops
                        .get_mut(batch_index as usize)
                        .expect("Invalid batch index");
                    batch_op.0.set_result_code(rc, in_doubt);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    async fn parse_record(conn: &mut BufferedConn<'_>) -> Result<Option<BatchRecordIndex>> {
        // if cmd is the end marker of the response, do not proceed further
        let info3 = conn.buffer().read_u8(Some(3));
        let last_record = info3 & commands::buffer::INFO3_LAST == commands::buffer::INFO3_LAST;

        let batch_index = conn.buffer().read_u32(Some(14));
        let result_code = ResultCode::from(conn.buffer().read_u8(Some(5)));

        match result_code {
            ResultCode::Ok
            | ResultCode::UdfBadResponse // UDF errors will have a body that needs to be parsed
            | ResultCode::KeyNotFoundError
            | ResultCode::FilteredOut => (),
            rc => {
                if last_record {
                    return Err(Error::BatchLastError(
                        batch_index,
                        rc,
                        false,
                        conn.conn.addr.clone(),
                    ));
                }

                return Err(Error::BatchError(
                    batch_index,
                    rc,
                    false,
                    conn.conn.addr.clone(),
                ));
            }
        }

        // if cmd is the end marker of the response, do not proceed further
        if last_record {
            return Ok(None);
        }

        let found_key = match result_code {
            ResultCode::Ok | ResultCode::UdfBadResponse => true,
            ResultCode::KeyNotFoundError | ResultCode::FilteredOut => false,
            _ => unreachable!(),
        };

        conn.buffer().skip(6);
        let generation = conn.buffer().read_u32(None);
        let expiration = conn.buffer().read_u32(None);
        let batch_index = conn.buffer().read_u32(None);
        let field_count = conn.buffer().read_u16(None) as usize; // almost certainly 0
        let op_count = conn.buffer().read_u16(None) as usize;

        let (key, _, version) = StreamCommand::parse_key_and_version(conn, field_count).await?;

        let record = if found_key {
            let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

            for _ in 0..op_count {
                conn.read_buffer(8).await?;
                let op_size = conn.buffer().read_u32(None) as usize;
                conn.buffer().skip(1);
                let particle_type = conn.buffer().read_u8(None);
                conn.buffer().skip(1);
                let name_size = conn.buffer().read_u8(None) as usize;
                conn.read_buffer(name_size).await?;
                let name = conn.buffer().read_str(name_size)?;
                let particle_bytes_size = op_size - (4 + name_size);
                conn.read_buffer(particle_bytes_size).await?;
                let value =
                    value::bytes_to_particle(particle_type, conn.buffer(), particle_bytes_size)?;

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

            Some(Record::new(Some(key), bins, generation, expiration))
        } else {
            None
        };
        Ok(Some(BatchRecordIndex {
            batch_index: batch_index as usize,
            record,
            result_code,
            version,
        }))
    }

    const fn keep_connection(err: &Error) -> bool {
        matches!(err, Error::ServerError(_, _, _) | Error::Timeout(_))
    }

    async fn parse_result(
        batch_ops: &mut [(BatchOperation, usize)],
        conn: &mut Connection,
        txn: Option<&Arc<crate::txn::Txn>>,
    ) -> Result<()> {
        let mut status = true;

        while status {
            let mut conn = BufferedConn::new(conn);

            conn.set_limit_header(8)?;
            conn.read_buffer(8).await?;

            let proto = conn.buffer().read_u64(Some(0));
            let msg_type = ((proto >> 48) & 0xFF) as u8;
            let size = (proto & 0x0000_FFFF_FFFF_FFFF) as usize;

            if msg_type == buffer::AS_MSG_TYPE_COMPRESSED {
                // Compressed batch response
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

                // Drain any remaining bytes from the network
                // conn.drain(conn.conn.deadline()).await?;

                // All compressed data read from network; clear the flag.
                conn.conn.compressed_stream_body = false;

                // Read only the 8-byte inner proto header to get the message size.
                let mut decoder = ZlibDecoder::new(std::io::Cursor::new(compressed_data));
                let mut proto_buf = [0u8; 8];
                decoder
                    .read_exact(&mut proto_buf)
                    .map_err(|e| Error::ClientError(format!("Batch decompression error: {e}")))?;
                let inner_proto = u64::from_be_bytes(proto_buf);
                let inner_size = (inner_proto & 0x0000_FFFF_FFFF_FFFF) as usize;

                status = false;
                if inner_size > 0 {
                    // Stream-decompress the rest on demand.
                    let body_decompressed_size = uncompressed_size - 8;
                    let mut inner_conn =
                        BufferedConn::new_with_decoder(conn.conn, decoder, body_decompressed_size);

                    match Self::parse_group(batch_ops, &mut inner_conn, inner_size, txn).await {
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
                    match Self::parse_group(batch_ops, &mut conn, size, txn).await {
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

        conn.reset_state();
        Ok(())
    }
}
