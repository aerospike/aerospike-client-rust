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
use std::collections::HashMap;
use std::sync::Arc;

use crate::batch::BatchOperation;
use crate::batch::BatchRecordIndex;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::Duration;
use crate::commands::{self};
use crate::errors::{Error, Result};
use crate::net::Connection;
use crate::policy::{BatchPolicy, Policy, PolicyLike, Replica};
use crate::{value, Record, ResultCode, Value};
use aerospike_rt::sleep;

#[derive(Clone)]
pub(crate) struct BatchOperateCommand<'a> {
    policy: BatchPolicy,
    pub node: Arc<Node>,
    pub batch_ops: Vec<(BatchOperation<'a>, usize)>,
}

impl<'a> BatchOperateCommand<'a> {
    pub fn new(
        policy: &'a BatchPolicy,
        node: Arc<Node>,
        batch_ops: Vec<(BatchOperation<'a>, usize)>,
    ) -> BatchOperateCommand<'a> {
        BatchOperateCommand {
            policy: policy.clone(),
            node,
            batch_ops,
        }
    }

    pub async fn execute(mut self, cluster: Arc<Cluster>) -> Result<Self> {
        let mut iterations = 0;
        let base_policy = self.policy.base().clone();

        // set timeout outside the loop
        let deadline = base_policy.deadline();

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            let success = if iterations & 1 == 0 || matches!(self.policy.replica, Replica::Master) {
                // For even iterations, we request all keys from the same node for efficiency.
                Self::request_group(&mut self.batch_ops, &self.policy, self.node.clone()).await?
            } else {
                // However, for odd iterations try the second choice for each. Instead of re-sharding the batch (as the second choice may not correspond to the first), just try each by itself.
                let mut all_successful = true;
                for individual_op in self.batch_ops.chunks_mut(1) {
                    let key = individual_op[0].0.key();
                    // Find somewhere else to try.
                    let partition = Partition::new_by_key(&key);
                    let node = cluster
                        .get_node(&partition, self.policy.replica, Arc::downgrade(&self.node))
                        .await?;

                    if !Self::request_group(individual_op, &self.policy, node).await? {
                        all_successful = false;
                        break;
                    }
                }
                all_successful
            };

            if success {
                // command has completed successfully.  Exit method.
                return Ok(self);
            }

            iterations += 1;

            // too many retries
            if let Some(max_retries) = base_policy.max_retries() {
                if iterations > max_retries + 1 {
                    return Err(Error::Connection(format!(
                        "Timeout after {} tries",
                        iterations
                    )));
                }
            }

            // Sleep before trying again, after the first iteration
            if let Some(sleep_between_retries) = base_policy.sleep_between_retries() {
                sleep(sleep_between_retries).await;
            }

            // check for command timeout
            if let Some(deadline) = deadline {
                if Instant::now() > deadline {
                    return Err(Error::Connection("Timeout".to_string()));
                }
            }

            // set command node, so when you return a record it has the node
            let mut conn = match self.node.get_connection().await {
                Ok(conn) => conn,
                Err(err) => {
                    warn!("Node {}: {}", self.node, err);
                    continue;
                }
            };

            self.prepare_buffer(&mut conn)
                .map_err(|e| e.chain_error("Failed to prepare send buffer"))?;
            self.write_timeout(&mut conn, base_policy.timeout())
                .await
                .map_err(|e| e.chain_error("Failed to set timeout for send buffer"))?;

            // Send command.
            if let Err(err) = self.write_buffer(&mut conn).await {
                // IO errors are considered temporary anomalies. Retry.
                // Close socket to flush out possible garbage. Do not put back in pool.
                conn.invalidate();
                warn!("Node {}: {}", self.node, err);
                continue;
            }

            // Parse results.
            if let Err(err) = Self::parse_result(&mut self.batch_ops, &mut conn).await {
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
            return Ok(self);
        }
    }

    async fn request_group(
        batch_ops: &mut [(BatchOperation<'a>, usize)],
        policy: &BatchPolicy,
        node: Arc<Node>,
    ) -> Result<bool> {
        let mut conn = match node.get_connection().await {
            Ok(conn) => conn,
            Err(err) => {
                warn!("Node {}: {}", node, err);
                return Ok(false);
            }
        };

        conn.buffer
            .set_batch_operate(policy, batch_ops)
            .map_err(|_| Error::ClientError("Failed to prepare send buffer".into()))?;

        conn.buffer.write_timeout(policy.base().timeout());

        // Send command.
        if let Err(err) = conn.flush().await {
            // IO errors are considered temporary anomalies. Retry.
            // Close socket to flush out possible garbage. Do not put back in pool.
            conn.invalidate();
            warn!("Node {}: {}", node, err);
            return Ok(false);
        }

        // Parse results.
        if let Err(err) = Self::parse_result(batch_ops, &mut conn).await {
            // close the connection
            // cancelling/closing the batch/multi commands will return an error, which will
            // close the connection to throw away its data and signal the server about the
            // situation. We will not put back the connection in the buffer.
            if !commands::keep_connection(&err) {
                conn.invalidate();
            }
            Err(err)
        } else {
            Ok(true)
        }
    }

    async fn parse_group(
        batch_ops: &mut [(BatchOperation<'a>, usize)],
        conn: &mut Connection,
        size: usize,
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
                    batch_op.0.set_record(batch_record.record);
                    batch_op.0.set_result_code(batch_record.result_code, false);
                }
                Err(Error::BatchError(batch_index, rc, in_doubt, ..)) => {
                    let batch_op = batch_ops
                        .get_mut(batch_index as usize)
                        .expect("Invalid batch index");
                    batch_op.0.set_result_code(rc, in_doubt);
                }
                Err(err @ _) => return Err(err),
            }
        }
        Ok(true)
    }

    async fn parse_record(conn: &mut Connection) -> Result<Option<BatchRecordIndex>> {
        let batch_index = conn.buffer.read_u32(Some(14));
        let result_code = ResultCode::from(conn.buffer.read_u8(Some(5)));
        match result_code {
            ResultCode::Ok => (),
            ResultCode::KeyNotFoundError => (),
            rc => {
                return Err(Error::BatchError(batch_index, rc, false, conn.addr.clone()));
            }
        };

        // if cmd is the end marker of the response, do not proceed further
        let info3 = conn.buffer.read_u8(Some(3));
        if info3 & commands::buffer::INFO3_LAST == commands::buffer::INFO3_LAST {
            return Ok(None);
        }

        let found_key = match result_code {
            ResultCode::Ok => true,
            ResultCode::KeyNotFoundError => false,
            rc => {
                return Err(Error::BatchError(batch_index, rc, false, conn.addr.clone()));
            }
        };

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
        Ok(Some(BatchRecordIndex {
            batch_index: batch_index as usize,
            record,
            result_code: result_code,
        }))
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_batch_operate(&self.policy, &self.batch_ops)
    }

    async fn parse_result(
        batch_ops: &mut [(BatchOperation<'a>, usize)],
        conn: &mut Connection,
    ) -> Result<()> {
        loop {
            conn.read_buffer(8).await?;
            let size = conn.buffer.read_msg_size(None);
            conn.bookmark();
            if size > 0 && !Self::parse_group(batch_ops, conn, size as usize).await? {
                break;
            }
        }
        Ok(())
    }

    async fn write_timeout(
        &mut self,
        conn: &mut Connection,
        timeout: Option<Duration>,
    ) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }
}
