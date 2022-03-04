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

use aerospike_rt::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::Arc;

use crate::cluster::Node;
use crate::commands::{self, Command};
use crate::errors::{ErrorKind, Result, ResultExt};
use crate::net::Connection;
use crate::policy::{BatchPolicy, Policy, PolicyLike};
use crate::{value, BatchRead, Record, ResultCode, Value};
use aerospike_rt::sleep;

struct BatchRecord {
    batch_index: usize,
    record: Option<Record>,
}

#[derive(Clone, Debug)]
pub struct BatchReadCommand {
    policy: BatchPolicy,
    pub node: Arc<Node>,
    pub batch_reads: Vec<BatchRead>,
}

impl BatchReadCommand {
    pub fn new(policy: &BatchPolicy, node: Arc<Node>, batch_reads: Vec<BatchRead>) -> Self {
        BatchReadCommand {
            policy: policy.clone(),
            node,
            batch_reads,
        }
    }

    pub async fn execute(&mut self) -> Result<()> {
        let mut iterations = 0;
        let base_policy = self.policy.base().clone();

        // set timeout outside the loop
        let deadline = base_policy.deadline();

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            iterations += 1;

            // too many retries
            if let Some(max_retries) = base_policy.max_retries() {
                if iterations > max_retries + 1 {
                    bail!(ErrorKind::Connection(format!(
                        "Timeout after {} tries",
                        iterations
                    )));
                }
            }

            // Sleep before trying again, after the first iteration
            if iterations > 1 {
                if let Some(sleep_between_retries) = base_policy.sleep_between_retries() {
                    sleep(sleep_between_retries).await;
                }
            }

            // check for command timeout
            if let Some(deadline) = deadline {
                if Instant::now() > deadline {
                    break;
                }
            }

            // set command node, so when you return a record it has the node
            let node = match self.get_node().await {
                Ok(node) => node,
                Err(_) => continue, // Node is currently inactive. Retry.
            };

            let mut conn = match node.get_connection().await {
                Ok(conn) => conn,
                Err(err) => {
                    warn!("Node {}: {}", node, err);
                    continue;
                }
            };

            self.prepare_buffer(&mut conn)
                .chain_err(|| "Failed to prepare send buffer")?;
            self.write_timeout(&mut conn, base_policy.timeout())
                .await
                .chain_err(|| "Failed to set timeout for send buffer")?;

            // Send command.
            if let Err(err) = self.write_buffer(&mut conn).await {
                // IO errors are considered temporary anomalies. Retry.
                // Close socket to flush out possible garbage. Do not put back in pool.
                conn.invalidate();
                warn!("Node {}: {}", node, err);
                continue;
            }

            // Parse results.
            if let Err(err) = self.parse_result(&mut conn).await {
                // close the connection
                // cancelling/closing the batch/multi commands will return an error, which will
                // close the connection to throw away its data and signal the server about the
                // situation. We will not put back the connection in the buffer.
                if !commands::keep_connection(&err) {
                    conn.invalidate();
                }
                return Err(err);
            }

            // command has completed successfully.  Exit method.
            return Ok(());
        }

        bail!(ErrorKind::Connection("Timeout".to_string()))
    }

    async fn parse_group(&mut self, conn: &mut Connection, size: usize) -> Result<bool> {
        while conn.bytes_read() < size {
            conn.read_buffer(commands::buffer::MSG_REMAINING_HEADER_SIZE as usize)
                .await?;
            match self.parse_record(conn).await? {
                None => return Ok(false),
                Some(batch_record) => {
                    let batch_read = self
                        .batch_reads
                        .get_mut(batch_record.batch_index)
                        .expect("Invalid batch index");
                    batch_read.record = batch_record.record;
                }
            }
        }
        Ok(true)
    }

    async fn parse_record(&mut self, conn: &mut Connection) -> Result<Option<BatchRecord>> {
        let found_key = match ResultCode::from(conn.buffer.read_u8(Some(5))) {
            ResultCode::Ok => true,
            ResultCode::KeyNotFoundError => false,
            rc => bail!(ErrorKind::ServerError(rc)),
        };

        // if cmd is the end marker of the response, do not proceed further
        let info3 = conn.buffer.read_u8(Some(3));
        if info3 & commands::buffer::INFO3_LAST == commands::buffer::INFO3_LAST {
            return Ok(None);
        }

        conn.buffer.skip(6);
        let generation = conn.buffer.read_u32(None);
        let expiration = conn.buffer.read_u32(None);
        let batch_index = conn.buffer.read_u32(None);
        let field_count = conn.buffer.read_u16(None) as usize; // almost certainly 0
        let op_count = conn.buffer.read_u16(None) as usize;

        let key = commands::StreamCommand::parse_key(conn, field_count).await?;

        let record = if found_key {
            let mut bins: HashMap<String, Value> = HashMap::with_capacity(op_count);

            for _ in 0..op_count {
                conn.read_buffer(8).await?;
                let op_size = conn.buffer.read_u32(None) as usize;
                conn.buffer.skip(1);
                let particle_type = conn.buffer.read_u8(None);
                conn.buffer.skip(1);
                let name_size = conn.buffer.read_u8(None) as usize;
                conn.read_buffer(name_size).await?;
                let name = conn.buffer.read_str(name_size)?;
                let particle_bytes_size = op_size - (4 + name_size);
                conn.read_buffer(particle_bytes_size).await?;
                let value =
                    value::bytes_to_particle(particle_type, &mut conn.buffer, particle_bytes_size)?;
                bins.insert(name, value);
            }

            Some(Record::new(Some(key), bins, generation, expiration))
        } else {
            None
        };
        Ok(Some(BatchRecord {
            batch_index: batch_index as usize,
            record,
        }))
    }
}

#[async_trait::async_trait]
impl commands::Command for BatchReadCommand {
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

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer
            .set_batch_read(&self.policy, self.batch_reads.clone())
    }

    async fn get_node(&self) -> Result<Arc<Node>> {
        Ok(self.node.clone())
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<()> {
        loop {
            conn.read_buffer(8).await?;
            let size = conn.buffer.read_msg_size(None);
            conn.bookmark();
            if size > 0 && !self.parse_group(conn, size as usize).await? {
                break;
            }
        }
        Ok(())
    }
}
