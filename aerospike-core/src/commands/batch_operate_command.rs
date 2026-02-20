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
use std::sync::Arc;

use crate::batch::BatchOperation;
use crate::batch::BatchRecordIndex;
use crate::cluster::metrics::SingleCommandMetric;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::{self, BatchAttr};
use crate::commands::{CommandType, NamespaceProvider, StreamCommand};
use crate::errors::{Error, Result};
use crate::net::{BufferedConn, Connection};
use crate::policy::{BatchPolicy, Policy, Replica};
use crate::{record_bytes, record_latency};
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

    pub async fn execute(mut self, cluster: Arc<Cluster>) -> Result<Self> {
        let mut attr = BatchAttr::default();

        use aerospike_rt::time::Instant;
        let mut single_metric = SingleCommandMetric::new();
        let mut start = Instant::now();
        let node = self.node.clone();
        let res = self
            .execute_timed(cluster, &mut attr, &mut single_metric)
            .await;

        record_latency!(node, start, single_metric.command_latency);

        let cmd_type = if attr.has_write {
            CommandType::BatchWrite
        } else {
            CommandType::BatchRead
        };

        node.metrics
            .load()
            .apply_latency(cmd_type, start.elapsed().as_micros() as u64);

        node.metrics
            .load()
            .update_metrics_for_namespace(&self, &single_metric);

        match res {
            Ok(_) => Ok(self),
            Err(e @ Error::ServerError(rc, _, _)) => {
                node.metrics
                    .load()
                    .update_result_code_for_namespace(&self, rc);
                Err(e)
            }
            Err(e) => {
                node.metrics
                    .load()
                    .update_result_code_for_namespace(&self, ResultCode::Ok);
                Err(e)
            }
        }
    }

    pub async fn execute_timed(
        &mut self,
        cluster: Arc<Cluster>,
        attr: &mut BatchAttr,
        metric: &mut SingleCommandMetric,
    ) -> Result<()> {
        if self.policy.total_timeout() > 0 {
            let res = aerospike_rt::timeout(
                Duration::from_millis(u64::from(self.policy.total_timeout())),
                self.execute_command(cluster, attr, metric),
            )
            .await;
            match res {
                Ok(res) => res,
                Err(_) => Err(Error::Timeout("Timeout".to_string())),
            }
        } else {
            self.execute_command(cluster, attr, metric).await
        }
    }

    pub async fn execute_command(
        &mut self,
        cluster: Arc<Cluster>,
        attr: &mut BatchAttr,
        metric: &mut SingleCommandMetric,
    ) -> Result<()> {
        let mut iterations = 0;

        // set timeout outside the loop
        let deadline = self.policy.deadline();

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            let success = if iterations & 1 == 0 || matches!(self.policy.replica, Replica::Master) {
                // For even iterations, we request all keys from the same node for efficiency.
                Self::request_group(
                    &mut self.batch_ops,
                    &self.policy,
                    deadline,
                    self.node.clone(),
                    attr,
                    metric,
                )
                .await?
            } else {
                // However, for odd iterations try the second choice for each. Instead of re-sharding the batch (as the second choice may not correspond to the first), just try each by itself.
                let mut all_successful = true;
                for individual_op in self.batch_ops.chunks_mut(1) {
                    let key = individual_op[0].0.key();
                    // Find somewhere else to try.
                    let partition = Partition::new_by_key(&key);
                    let node = cluster.get_node(
                        &partition,
                        self.policy.replica,
                        Arc::downgrade(&self.node),
                    )?;

                    if !Self::request_group(
                        individual_op,
                        &self.policy,
                        deadline,
                        node,
                        attr,
                        metric,
                    )
                    .await?
                    {
                        all_successful = false;
                        break;
                    }
                }
                all_successful
            };

            if success {
                // command has completed successfully. Exit method.
                return Ok(());
            }

            iterations += 1;

            // too many retries
            if self.policy.max_retries() > 0 && iterations > self.policy.max_retries() + 1 {
                return Err(Error::Timeout(format!("Timeout after {iterations} tries")));
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
                    )));
                }
            }
        }
    }

    async fn request_group(
        batch_ops: &mut [(BatchOperation, usize)],
        policy: &BatchPolicy,
        deadline: Option<Instant>,
        node: Arc<Node>,
        attr: &mut BatchAttr,
        metric: &mut SingleCommandMetric,
    ) -> Result<bool> {
        let mut start = Instant::now();
        let hint = batch_ops
            .first()
            .map(|bop| bop.0.key().digest[0])
            .or(Some(0))
            .unwrap();
        let mut conn = match node.get_connection(hint).await {
            Ok(conn) => conn,
            Err(err) => {
                warn!("Node {node}: {err}");
                return Ok(false);
            }
        };

        record_latency!(node, start, metric.connection_acq);

        *attr = conn
            .buffer
            .set_batch_operate(policy, batch_ops)
            .map_err(|_| Error::ClientError("Failed to prepare send buffer".into()))?;

        record_latency!(node, start, metric.command_encoding);

        conn.buffer.write_timeout(policy.server_timeout());

        conn.set_socket_timeout(deadline, policy.socket_timeout());
        conn.set_timeout_delay(true, policy.timeout_delay());

        // Send command.
        if let Err(err) = conn.flush().await {
            record_latency!(node, start, metric.command_transmission);
            record_bytes!(node, conn, metric);

            // IO errors are considered temporary anomalies. Retry.
            // Close socket to flush out possible garbage. Do not put back in pool.
            conn.invalidate();
            warn!("Node {node}: {err}");
            return Ok(false);
        }

        record_latency!(node, start, metric.command_transmission);

        // Parse results.
        if let Err(err) = Self::parse_result(batch_ops, &mut conn).await {
            record_latency!(node, start, metric.command_parsing);
            record_bytes!(node, conn, metric);

            // close the connection
            // cancelling/closing the batch/multi commands will return an error, which will
            // close the connection to throw away its data and signal the server about the
            // situation. We will not put back the connection in the buffer.
            if !Self::keep_connection(&err) {
                conn.invalidate();
            }
            Err(err)
        } else {
            record_latency!(node, start, metric.command_parsing);
            record_bytes!(node, conn, metric);

            conn.reset_state();
            Ok(true)
        }
    }

    async fn parse_group(
        batch_ops: &mut [(BatchOperation, usize)],
        conn: &mut BufferedConn<'_>,
        size: usize,
    ) -> Result<bool> {
        while conn.bytes_received() < size {
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
                Err(Error::BatchLastError(batch_index, rc, in_doubt, ref msg)) => {
                    let batch_op = batch_ops
                        .get_mut(batch_index as usize)
                        .expect("Invalid batch index");
                    batch_op.0.set_result_code(rc, in_doubt);
                    return Err(Error::BatchError(batch_index, rc, in_doubt, msg.clone()));
                }
                Err(Error::BatchError(batch_index, rc, in_doubt, ..)) => {
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
            ResultCode::Ok => (),
            ResultCode::UdfBadResponse => (), // UDF errors will have a body that needs to be parsed
            ResultCode::KeyNotFoundError | ResultCode::FilteredOut => (),
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
            ResultCode::Ok => true,
            ResultCode::UdfBadResponse => true,
            ResultCode::KeyNotFoundError | ResultCode::FilteredOut => false,
            _ => unreachable!(),
        };

        conn.buffer().skip(6);
        let generation = conn.buffer().read_u32(None);
        let expiration = conn.buffer().read_u32(None);
        let batch_index = conn.buffer().read_u32(None);
        let field_count = conn.buffer().read_u16(None) as usize; // almost certainly 0
        let op_count = conn.buffer().read_u16(None) as usize;

        let (key, _) = StreamCommand::parse_key(conn, field_count).await?;

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
        }))
    }

    const fn keep_connection(err: &Error) -> bool {
        matches!(err, Error::ServerError(_, _, _) | Error::Timeout(_))
    }

    async fn parse_result(
        batch_ops: &mut [(BatchOperation, usize)],
        conn: &mut Connection,
    ) -> Result<()> {
        let mut status = true;

        while status {
            let mut conn = BufferedConn::new(conn);

            conn.set_limit_header(8)?;
            conn.read_buffer(8).await?;
            let size = conn.buffer().read_msg_size(None);
            conn.bookmark();

            status = false;
            if size > 0 {
                conn.set_limit_body(size)?;
                match Self::parse_group(batch_ops, &mut conn, size).await {
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

        Ok(())
    }
}

impl NamespaceProvider for BatchOperateCommand {
    fn get_namespaces(&self) -> impl Iterator<Item = (&str, CommandType)> {
        self.batch_ops
            .iter()
            .map(|op| (op.0.namespace(), op.0.command_type()))
    }
}
