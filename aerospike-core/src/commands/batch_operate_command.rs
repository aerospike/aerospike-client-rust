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
use indexmap::map::Entry::{Occupied, Vacant};
use crate::IndexMap;
use std::io::Read;
use std::sync::Arc;

use flate2::read::ZlibDecoder;

use crate::batch::BatchOperation;
use crate::batch::BatchRecordIndex;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::StreamCommand;
use crate::commands::{self, buffer};
use crate::errors::{Error, ErrorKind, Result};
use crate::net::{BufferedConn, Connection};
use crate::policy::{next_retry_interval, BatchPolicy, Policy, Replica};
use crate::{value, Record, ResultCode, Value};
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

pub struct BatchOperateCommand {
    policy: BatchPolicy,
    pub node: Arc<Node>,
    pub batch_ops: Vec<(BatchOperation, usize)>,
    /// Set when the command failed after per-key processing began (retries
    /// exhausted, deadline elapsed, unrecoverable request error). The command
    /// still returns `Ok(self)` so `batch_ops` — carrying every per-key
    /// outcome and in-doubt mark — survives for the executor to surface via
    /// [`ErrorKind::BatchFailed`](crate::ErrorKind::BatchFailed) (Java
    /// `AerospikeException.BatchRecordArray` parity).
    pub(crate) terminal_error: Option<Error>,
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
            terminal_error: None,
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub async fn execute(self, cluster: Arc<Cluster>) -> Result<Self> {
        if self.policy.total_timeout() > 0 {
            let res = aerospike_rt::timeout(
                Duration::from_millis(u64::from(self.policy.total_timeout())),
                self.execute_command(cluster.clone()),
            )
            .await;
            match res {
                Ok(res) => res,
                Err(_) => {
                    // The whole-command deadline elapsed before the inner loop
                    // returned. The in-loop deadline check is mutually
                    // exclusive with this path, so there's no double count.
                    cluster.incr_total_timeout_exceeded();
                    Err(Error::timeout("Timeout".to_string()))
                }
            }
        } else {
            self.execute_command(cluster).await
        }
    }

    pub async fn execute_command(mut self, cluster: Arc<Cluster>) -> Result<Self> {
        let mut iterations: usize = 0;
        let mut last_err: Option<Error> = None;
        let node_addr = self.node.to_string();
        // Number of times a request buffer actually reached the wire. Drives
        // per-row in-doubt (a row error after a retry may mask an applied
        // earlier attempt) and the terminal no-response in-doubt walk.
        let mut commands_sent: u32 = 0;

        // set timeout outside the loop
        let deadline = self.policy.deadline();
        // Retry backoff: sleep interval grows by `sleep_multiplier` after each
        // retry sleep (matching Go). A multiplier <= 1.0 keeps it constant.
        let sleep_multiplier = self.policy.sleep_multiplier();
        let mut sleep_interval = self.policy.sleep_between_retries();
        // Consecutive waits spent on an empty connection pool while a
        // background task opens a connection (not part of the retry budget).
        let mut pool_empty_waits: usize = 0;

        // Whether this batch carries any write. Drives both the metrics command
        // type and the in-doubt rule for a terminal failure (only writes can be
        // in doubt). The op set does not change across retries, so it is
        // computed once.
        let is_write = self.batch_ops.iter().any(|op| op.0.has_write());

        // Metrics: a batch containing any write op is a BatchWrite, otherwise
        // a BatchRead. `trans_start` measures the overall command latency.
        let cmd_type = if is_write {
            crate::metrics::CommandType::BatchWrite
        } else {
            crate::metrics::CommandType::BatchRead
        };
        let trans_start = Instant::now();
        // One sample decision per node sub-batch command, reused across retries.
        let mut sampled = Some(crate::sampler::with_thread_rng(|rng| {
            cluster.should_record_operational(rng)
        }));

        // Replica sequence offsets, advanced on every scheduled retry (Java
        // BatchCommand.prepareRetry). AP and SC namespaces track separate
        // counters because SC does not advance on client timeout under
        // Linearize.
        let mut sequence_ap: usize = 0;
        let mut sequence_sc: usize = 0;
        // Retries return to the originally selected node unless the replica
        // policy walks a sequence (Java prepareRetry returns true for
        // Master/MasterProles/Random).
        let same_node_retry =
            !matches!(self.policy.replica, Replica::Sequence | Replica::PreferRack);

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            let retry_err = if iterations == 0 || same_node_retry {
                // First attempt, and every retry for non-sequence replicas:
                // the whole group goes to the originally selected node.
                match Self::request_group(
                    &mut self.batch_ops,
                    &self.policy,
                    deadline,
                    self.node.clone(),
                    cmd_type,
                    &mut sampled,
                    &mut commands_sent,
                )
                .await
                {
                    Ok(res) => res,
                    Err(err) => {
                        self.set_terminal_error(err, is_write, commands_sent);
                        return Ok(self);
                    }
                }
            } else {
                // Sequence/PreferRack retry (Java BatchCommand.retryBatch):
                // the advanced replica sequence re-maps every key, and the
                // keys are re-split into per-node batch groups. Java runs
                // the sub-batches in parallel, each retrying recursively;
                // here the groups run sequentially inside the shared retry
                // loop — identical routing and retry budget, simpler
                // control flow.
                let mut nodes: Vec<Arc<Node>> = Vec::with_capacity(self.batch_ops.len());
                let mut hard_err: Option<Error> = None;
                for (op, _) in &self.batch_ops {
                    let key = op.key();
                    let mut partition = if op.has_write() {
                        let mut partition = Partition::for_write(&key);
                        partition.replica = self.policy.replica;
                        partition
                    } else {
                        Partition::for_read(
                            &cluster,
                            &key,
                            self.policy.replica,
                            self.policy.base_policy.read_mode_sc,
                        )
                    };
                    partition.sequence = if cluster
                        .is_strong_consistency(&key.namespace)
                        .unwrap_or(false)
                    {
                        sequence_sc
                    } else {
                        sequence_ap
                    };
                    match partition.get_node(&cluster) {
                        Ok(node) => nodes.push(node),
                        Err(err) => {
                            hard_err = Some(err);
                            break;
                        }
                    }
                }
                if let Some(err) = hard_err {
                    self.set_terminal_error(err, is_write, commands_sent);
                    return Ok(self);
                }

                // Regroup the ops contiguously per node so each group can be
                // requested as one batch command.
                let pairs: Vec<(BatchOperation, usize)> = self.batch_ops.drain(..).collect();
                let mut routed: Vec<(Arc<Node>, (BatchOperation, usize))> =
                    nodes.into_iter().zip(pairs).collect();
                routed.sort_by(|a, b| a.0.name().cmp(b.0.name()));

                let mut ranges: Vec<(Arc<Node>, std::ops::Range<usize>)> = Vec::new();
                for (node, pair) in routed {
                    let pos = self.batch_ops.len();
                    match ranges.last_mut() {
                        Some((last, range)) if Arc::ptr_eq(last, &node) => range.end = pos + 1,
                        _ => ranges.push((node, pos..pos + 1)),
                    }
                    self.batch_ops.push(pair);
                }

                // Run every group this round even if one fails (Java's
                // sub-batches are independent); keep the first retriable
                // error to drive the next iteration.
                let mut group_err: Option<Error> = None;
                for (node, range) in ranges {
                    match Self::request_group(
                        &mut self.batch_ops[range],
                        &self.policy,
                        deadline,
                        node,
                        cmd_type,
                        &mut sampled,
                        &mut commands_sent,
                    )
                    .await
                    {
                        Ok(Some(e)) => {
                            group_err.get_or_insert(e);
                        }
                        Ok(None) => (),
                        Err(err) => {
                            self.set_terminal_error(err, is_write, commands_sent);
                            return Ok(self);
                        }
                    }
                }
                group_err
            };

            if let Some(e) = retry_err {
                // Pool-empty is a pacing wait while a background task opens a
                // connection: it consumes neither the retry budget nor the
                // retry metrics, and is not chained into the error history
                // (thousands of waits must not build a thousand-deep chain).
                // Bounded by the outer total-timeout wrapper and the wait cap.
                if e.is_pool_empty()
                    && pool_empty_waits < commands::POOL_EMPTY_MAX_WAITS
                {
                    pool_empty_waits += 1;
                    sleep(commands::POOL_EMPTY_WAIT).await;
                    continue;
                }
                // Java BatchCommand.prepareRetry: the AP sequence advances on
                // every scheduled retry; SC advances too unless the policy is
                // Linearize and the failure was NOT a connection-level error
                // (a client timeout under Linearize must re-read the same
                // replica).
                sequence_ap += 1;
                if !matches!(
                    self.policy.base_policy.read_mode_sc,
                    crate::policy::ReadModeSC::Linearize
                ) || e.client_result_code()
                    == Some(crate::ClientResultCode::ServerNotAvailable)
                {
                    sequence_sc += 1;
                }
                last_err = Some(e.chain_cause(last_err));
                if sampled.unwrap_or(false) {
                    self.node.metrics().incr_transaction_retry();
                }
            } else {
                // command has completed successfully. Record per-command-type
                // latency and the final per-record result codes, then exit.
                if sampled.unwrap_or(false) {
                    self.node
                        .metrics()
                        .record_command(cmd_type, trans_start.elapsed());
                    for (op, _) in &self.batch_ops {
                        if let Some(rc) = op.batch_record().result_code {
                            self.node.metrics().record_result_code(
                                &op.key().namespace,
                                cmd_type,
                                rc,
                            );
                        }
                    }
                }
                return Ok(self);
            }

            iterations += 1;

            // Retry budget exhausted: max_retries + 1 total attempts, like
            // Java and the single-command path (max_retries == 0 means a
            // single attempt, not unbounded retries).
            if iterations > self.policy.max_retries() {
                if sampled.unwrap_or(false) {
                    self.node.metrics().incr_transaction_error();
                }
                cluster.incr_max_retries_exceeded();
                let u32_iters = if iterations > u32::MAX as usize {
                    u32::MAX
                } else {
                    iterations as u32
                };
                self.set_terminal_error(
                    Error::max_retries_exceeded(format!("Timeout after {iterations} tries"))
                        .chain_cause(last_err)
                        .with_retry_context(u32_iters, Some(&node_addr), Vec::new()),
                    is_write,
                    commands_sent,
                );
                return Ok(self);
            }

            // Sleep before trying again, after the first iteration
            if let Some(interval) = sleep_interval {
                sleep(interval).await;
                sleep_interval = Some(next_retry_interval(interval, sleep_multiplier));
            }

            // check for command timeout
            if let Some(deadline) = deadline {
                if Instant::now() > deadline {
                    if sampled.unwrap_or(false) {
                        self.node.metrics().incr_transaction_error();
                    }
                    cluster.incr_total_timeout_exceeded();
                    let u32_iters = if iterations > u32::MAX as usize {
                        u32::MAX
                    } else {
                        iterations as u32
                    };
                    self.set_terminal_error(
                        Error::timeout(format!("Command timed out after {iterations} tries"))
                            .chain_cause(last_err)
                            .with_retry_context(u32_iters, Some(&node_addr), Vec::new()),
                        is_write,
                        commands_sent,
                    );
                    return Ok(self);
                }
            }
        }
    }

    /// Records the failure that ends this command, marking both the per-row
    /// outcomes **and the error itself** in-doubt.
    ///
    /// The error mark is what [`SingleCommand::execute_command`]'s `finalize`
    /// does for single-key commands: a write that reached the wire and never
    /// answered may have been applied, so the error has to say so. Without it
    /// the rows were marked and the error was not, so
    /// [`Error::in_doubt`](crate::Error::in_doubt) on the aggregate
    /// [`ErrorKind::BatchFailed`](crate::ErrorKind::BatchFailed) — which
    /// inherits the cause's mark — reported `false` for an in-doubt batch write.
    fn set_terminal_error(&mut self, err: Error, is_write: bool, commands_sent: u32) {
        self.mark_rows_in_doubt(commands_sent);
        self.terminal_error = Some(err.set_in_doubt(is_write, commands_sent));
    }

    /// After a command-level failure with at least one attempt on the wire,
    /// mark every record that never received a response: an unanswered write
    /// may have been applied by the server, so it becomes in-doubt and an
    /// attached transaction is notified. Reads are unaffected. Mirrors
    /// Java's `Batch.inDoubt()` walk over `BatchRecord.hasWrite`.
    ///
    /// Marks *rows*; the command's own error is marked by
    /// [`set_terminal_error`](Self::set_terminal_error).
    fn mark_rows_in_doubt(&mut self, commands_sent: u32) {
        if commands_sent == 0 {
            return;
        }
        let txn = self.policy.base_policy.txn.clone();
        for (op, _) in &mut self.batch_ops {
            op.set_in_doubt_on_no_response(txn.as_ref());
        }
    }

    async fn request_group(
        batch_ops: &mut [(BatchOperation, usize)],
        policy: &BatchPolicy,
        deadline: Option<Instant>,
        node: Arc<Node>,
        cmd_type: crate::metrics::CommandType,
        sampled: &mut Option<bool>,
        commands_sent: &mut u32,
    ) -> Result<Option<Error>> {
        // Per-node circuit breaker: don't even open a socket if the node
        // is currently outside its error-rate window. Mirrors Java's
        // `node.validateErrorCount()` call site at the top of every
        // command attempt.
        if let Err(err) = node.validate_error_count() {
            node.metrics().incr_circuit_breaker_hits();
            return Ok(Some(err));
        }

        // Metrics: detailed per-namespace metrics are attributed to every
        // distinct namespace in this request group. Build the namespace set
        // when collection is enabled. The sample decision was made at command
        // entry and is reused here (filled in only if the caller passed None).
        let namespaces: Vec<String> = if node.metrics().is_enabled() {
            let mut v: Vec<String> = batch_ops
                .iter()
                .map(|op| op.0.key().namespace.clone())
                .collect();
            v.sort();
            v.dedup();
            v
        } else {
            Vec::new()
        };

        let aq_start = Instant::now();
        let mut conn = match node.get_connection(0).await {
            Ok(conn) => conn,
            // Pool-empty is a pacing signal (a background task is opening a
            // connection), not node ill-health — don't trip the breaker.
            Err(err) if err.is_pool_empty() => return Ok(Some(err)),
            Err(err) => {
                warn!("Node {node}: {err}");
                node.incr_error_rate();
                return Ok(Some(err));
            }
        };
        // Reuse the call-entry sample decision. The Option exists so a
        // connection-time fallback can still fill it if the caller passed None.
        if sampled.is_none() {
            *sampled = Some(crate::sampler::with_thread_rng(|rng| {
                node.metrics().should_sample(rng)
            }));
        }
        let metrics_on = sampled.unwrap_or(false);
        if metrics_on {
            let aq_elapsed = aq_start.elapsed();
            for ns in &namespaces {
                node.metrics()
                    .record_connection_aq(ns, cmd_type, aq_elapsed);
            }
        }

        conn.buffer
            .set_compress(policy.use_compression(), policy.compression_threshold());
        conn.buffer
            .set_batch_operate(policy, batch_ops)
            .map_err(|e| {
                // Same as the single-key path: keep a caller's argument error
                // (and its PARAMETER_ERROR code) instead of replacing it with a
                // generic client error. This previously discarded the cause
                // entirely, so a value the client cannot encode surfaced as a
                // bare "Failed to prepare send buffer" with nothing to explain
                // it.
                if matches!(e.kind(), crate::ErrorKind::InvalidArgument) {
                    e
                } else {
                    e.chain_error("Failed to prepare send buffer")
                }
            })?;

        conn.buffer.write_timeout(policy.server_timeout());

        if policy.use_compression() {
            conn.buffer
                .compress()
                .map_err(|_| Error::client_error("Failed to compress send buffer"))?;
        }

        conn.set_socket_timeout(deadline, policy.socket_timeout());
        conn.set_timeout_delay(true, policy.timeout_delay());

        // Send command.
        let bytes_sent = conn.buffer.data_buffer.len() as u64;
        let write_start = Instant::now();
        if let Err(err) = conn.flush().await {
            // IO errors are considered temporary anomalies. Retry.
            // Close socket to flush out possible garbage. Do not put back in pool.
            conn.invalidate();
            warn!("Node {node}: {err}");
            node.incr_error_rate();
            return Ok(Some(err));
        }
        *commands_sent += 1;
        if metrics_on {
            let write_elapsed = write_start.elapsed();
            for ns in &namespaces {
                node.metrics()
                    .record_write(ns, cmd_type, bytes_sent, write_elapsed);
            }
        }

        // Parse results.
        let parse_start = Instant::now();
        let parse_outcome = Self::parse_result(
            batch_ops,
            &mut conn,
            policy.base_policy.txn.as_ref(),
            *commands_sent,
        )
        .await;
        if metrics_on && parse_outcome.is_ok() {
            let parse_elapsed = parse_start.elapsed();
            let received = conn.bytes_read() as u64;
            for ns in &namespaces {
                node.metrics()
                    .record_parse(ns, cmd_type, parse_elapsed, received);
            }
        }
        if let Err(err) = parse_outcome {
            // close the connection
            // cancelling/closing the batch/multi commands will return an error, which will
            // close the connection to throw away its data and signal the server about the
            // situation. We will not put back the connection in the buffer.
            if !Self::keep_connection(&err) {
                conn.invalidate();
            }
            // Retriable server errors (TIMEOUT / DEVICE_OVERLOAD / KEY_BUSY /
            // PARTITION_UNAVAILABLE) should drive another retry iteration, not
            // abort the whole batch. Return them as recoverable so the outer
            // loop can loop again.
            if commands::should_retry(&err) {
                if commands::is_network_error(&err) || commands::is_retriable_server_error(&err) {
                    node.incr_error_rate();
                }
                Ok(Some(err))
            } else {
                Err(err)
            }
        } else {
            Ok(None)
        }
    }

    async fn parse_group(
        batch_ops: &mut [(BatchOperation, usize)],
        conn: &mut BufferedConn<'_>,
        size: usize,
        txn: Option<&Arc<crate::txn::Txn>>,
        commands_sent: u32,
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
                Err(err) => match *err.kind() {
                    // Per-key row error. Record it on the individual
                    // BatchRecord — do not propagate as a batch-level
                    // failure, matching Java's behavior
                    // (BatchStatus.setRowError keeps other records).
                    // In-doubt mirrors Java's `Command.batchInDoubt`: a row
                    // error in this response is definitive for this attempt,
                    // so a write is only in doubt when an earlier attempt was
                    // also sent. A `last` row additionally ends the stream.
                    ErrorKind::BatchRow {
                        index,
                        rc,
                        last,
                        ref detail,
                    } => {
                        let batch_op = batch_ops
                            .get_mut(index as usize)
                            .expect("Invalid batch index");
                        batch_op.0.set_result_code(rc, commands_sent > 1);
                        batch_op.0.set_error_detail(detail.clone());
                        if last {
                            return Ok(false);
                        }
                    }
                    _ => return Err(err),
                },
            }
        }
        Ok(true)
    }

    async fn parse_record(conn: &mut BufferedConn<'_>) -> Result<Option<BatchRecordIndex>> {
        // if cmd is the end marker of the response, do not proceed further
        let info3 = conn.buffer().read_u8(Some(3));
        let last_record = info3 & commands::buffer::INFO3_LAST == commands::buffer::INFO3_LAST;

        // Read at offset 14 (the batch response reuses the transaction-ttl
        // slot for the row index). The success path re-reads an index from the
        // sequential header below; keep them separate so the row-error path
        // reports exactly the index it always has.
        let row_index = conn.buffer().read_u32(Some(14));
        let result_code = ResultCode::from(conn.buffer().read_u8(Some(5)));

        // A row error still has a body, and that body is where the server puts
        // its explanation (subcode/message, as a field). Parsing it is what
        // lets the detail reach the BatchRecord — returning here, as this used
        // to, discarded it and left those bytes for the next header read.
        let row_error = match result_code {
            ResultCode::Ok
            | ResultCode::UdfBadResponse // UDF errors will have a body that needs to be parsed
            | ResultCode::KeyNotFoundError
            | ResultCode::FilteredOut => None,
            rc => Some(rc),
        };

        // The end marker carries no body, so nothing to parse past the header.
        if last_record && row_error.is_none() {
            return Ok(None);
        }

        let found_key = matches!(
            result_code,
            ResultCode::Ok | ResultCode::UdfBadResponse
        );

        conn.buffer().skip(6);
        let generation = conn.buffer().read_u32(None);
        let expiration = conn.buffer().read_u32(None);
        let batch_index = conn.buffer().read_u32(None);
        let field_count = conn.buffer().read_u16(None) as usize; // almost certainly 0
        let op_count = conn.buffer().read_u16(None) as usize;

        let (key, _, version, error_detail) =
            StreamCommand::parse_key_and_version(conn, field_count).await?;

        if let Some(rc) = row_error {
            // Consume this row's op payloads so the next header starts where
            // the stream expects it to.
            for _ in 0..op_count {
                conn.read_buffer(8).await?;
                let op_size = conn.buffer().read_u32(None) as usize;
                conn.buffer().skip(4);
                let remaining = op_size.saturating_sub(4);
                conn.read_buffer(remaining).await?;
                conn.buffer().skip(remaining);
            }
            return Err(Error::batch_row(
                row_index,
                rc,
                last_record,
                conn.conn.addr.clone(),
                error_detail,
            ));
        }

        let record = if found_key {
            let mut bins: IndexMap<String, Value> = IndexMap::with_capacity(op_count);
            let mut results: Vec<Value> = Vec::with_capacity(op_count);

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

                results.push(value.clone());

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

            Some(Record::new(Some(key), bins, Some(results), generation, expiration))
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

    fn keep_connection(err: &Error) -> bool {
        commands::keep_connection(err)
    }

    async fn parse_result(
        batch_ops: &mut [(BatchOperation, usize)],
        conn: &mut Connection,
        txn: Option<&Arc<crate::txn::Txn>>,
        commands_sent: u32,
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
                    .map_err(|e| Error::client_error(format!("Batch decompression error: {e}")))?;
                let inner_proto = u64::from_be_bytes(proto_buf);
                let inner_size = (inner_proto & 0x0000_FFFF_FFFF_FFFF) as usize;

                status = false;
                if inner_size > 0 {
                    // Stream-decompress the rest on demand.
                    let body_decompressed_size = uncompressed_size - 8;
                    let mut inner_conn =
                        BufferedConn::new_with_decoder(conn.conn, decoder, body_decompressed_size);

                    match Self::parse_group(batch_ops, &mut inner_conn, inner_size, txn, commands_sent)
                        .await
                    {
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
                    match Self::parse_group(batch_ops, &mut conn, size, txn, commands_sent).await {
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

        conn.reset_state();
        Ok(())
    }
}
