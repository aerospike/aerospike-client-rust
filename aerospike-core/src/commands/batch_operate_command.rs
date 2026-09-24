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

/// A batch operation paired with the index it had in the caller's input.
type IndexedOp = (BatchOperation, usize);

/// A batch split into contiguous per-node slices: the reordered `(op, index)`
/// pairs, plus one `(node, range)` for every command that has to be sent.
type NodeGroups = (Vec<IndexedOp>, Vec<(Arc<Node>, std::ops::Range<usize>)>);

pub struct BatchOperateCommand {
    policy: BatchPolicy,
    pub node: Arc<Node>,
    pub batch_ops: Vec<(BatchOperation, usize)>,
    /// Set when the command failed after per-key processing began (retries
    /// exhausted, deadline elapsed, unrecoverable request error). The command
    /// still returns `Ok(self)` so `batch_ops` — carrying every per-key
    /// outcome and in-doubt mark — survives for the executor to stamp and
    /// hand back to the caller's rows.
    pub(crate) terminal_error: Option<Error>,
    /// `batch_foreach`'s per-row hook, fired as each row's result lands.
    hook: Option<Arc<crate::batch::BatchHook>>,
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
            hook: None,
        }
    }

    pub(crate) fn with_hook(mut self, hook: Option<Arc<crate::batch::BatchHook>>) -> Self {
        self.hook = hook;
        self
    }

    #[allow(clippy::option_if_let_else)]
    pub async fn execute(self, cluster: Arc<Cluster>) -> Result<Self> {
        // An aborted or cancelled batch_foreach: don't start work nobody
        // wants; the rows come back untouched for the final sweep.
        if self.hook.as_ref().is_some_and(|h| !h.is_active()) {
            return Ok(self);
        }
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
        // The per-command sample decision (operational tier on AND
        // sampler-selected). One random draw per sub-batch command, taken here
        // before the first attempt (metrics.md §3.1.1); the decision is made
        // from it the first time a node is tried and reused for the whole
        // command so all of its metrics are recorded together or not at all.
        let sample_draw: u64 = rand::random();
        let mut sampled: Option<bool> = None;

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
            // A hook abort or a dropped batch_foreach: no further attempts.
            if self.hook.as_ref().is_some_and(|h| !h.is_active()) {
                return Ok(self);
            }
            let retry_err = if iterations == 0 || same_node_retry {
                // First attempt, and every retry for non-sequence replicas:
                // the whole group goes to the originally selected node.
                match Self::request_group(
                    &mut self.batch_ops,
                    &self.policy,
                    deadline,
                    self.node.clone(),
                    cmd_type,
                    sample_draw,
                    &mut sampled,
                    &mut commands_sent,
                self.hook.as_deref(),
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
                // Route every row this round. A row that already carries a
                // result is settled — answered on an earlier attempt, or
                // stranded on an earlier retry — and is not routed again.
                let mut routed: Vec<Option<Arc<Node>>> = Vec::with_capacity(self.batch_ops.len());
                let mut route_err: Option<Error> = None;
                for (op, _) in &self.batch_ops {
                    if op.batch_record().result_code.is_some() {
                        routed.push(None);
                        continue;
                    }
                    // Borrow the key: `key()` clones two Strings per call.
                    let key = &op.batch_record().key;
                    let mut partition = if op.has_write() {
                        let mut partition = Partition::for_write(key);
                        partition.replica = self.policy.replica;
                        partition
                    } else {
                        Partition::for_read(
                            &cluster,
                            key,
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
                        Ok(node) => routed.push(Some(node)),
                        Err(err) => {
                            routed.push(None);
                            route_err.get_or_insert(err);
                        }
                    }
                }

                // A key with no reachable replica is a per-key outcome, as it
                // is on the first-attempt split (CLIENT-5172): it is stamped and
                // the rest of the group carries on. Failing the command here
                // instead made the executor discard every other node's
                // completed results — the whole batch lost, one level down,
                // in precisely the disruption that caused the retry. Only a
                // round with nothing left to send fails outright, and only
                // when routing (not settlement) is why.
                let (regrouped, ranges) =
                    Self::regroup_for_retry(std::mem::take(&mut self.batch_ops), routed);
                self.batch_ops = regrouped;
                if ranges.is_empty() {
                    if let Some(err) = route_err {
                        self.set_terminal_error(err, is_write, commands_sent);
                    }
                    return Ok(self);
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
                        sample_draw,
                        &mut sampled,
                        &mut commands_sent,
                    self.hook.as_deref(),
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
    /// Re-split a batch for a Sequence/PreferRack retry.
    ///
    /// `routed[i]` is where `ops[i]` goes this round, or `None` when it must
    /// not be sent: it already carries a result, or no replica could be
    /// reached for it. Rows to send are grouped per node by pointer, in
    /// first-seen order; rows not sent are placed after the last range, so no
    /// group includes them but they stay in the command for the executor to
    /// return at their input index. An unrouted row with no result yet is the
    /// unreachable-replica case and is stamped `PARTITION_UNAVAILABLE` — never
    /// in doubt, since nothing was sent for it.
    ///
    /// Grouping by pointer visits each row twice. Sorting the routed rows by
    /// node name instead — as this did — compared two ~40-character strings
    /// per comparison and moved a 568-byte tuple per swap, O(N log N) of each,
    /// to reach the same contiguity. Order within a node is preserved, which
    /// is what lets identical consecutive rows still compress into repeats.
    fn regroup_for_retry(ops: Vec<IndexedOp>, routed: Vec<Option<Arc<Node>>>) -> NodeGroups {
        debug_assert_eq!(ops.len(), routed.len());

        // Distinct nodes in first-seen order, each with its share counted, so
        // every bucket is allocated at its exact size.
        let mut counts: Vec<(Arc<Node>, usize)> = Vec::new();
        let mut bucket_of: Vec<Option<usize>> = Vec::with_capacity(ops.len());
        for node in &routed {
            bucket_of.push(node.as_ref().map(|node| {
                if let Some(pos) = counts.iter().position(|(e, _)| Arc::ptr_eq(e, node)) {
                    counts[pos].1 += 1;
                    pos
                } else {
                    counts.push((node.clone(), 1));
                    counts.len() - 1
                }
            }));
        }

        let mut buckets: Vec<(Arc<Node>, Vec<IndexedOp>)> = counts
            .into_iter()
            .map(|(node, count)| (node, Vec::with_capacity(count)))
            .collect();
        let mut held: Vec<IndexedOp> = Vec::new();
        for (mut pair, bucket) in ops.into_iter().zip(bucket_of) {
            match bucket {
                Some(b) => buckets[b].1.push(pair),
                None => {
                    if pair.0.batch_record().result_code.is_none() {
                        pair.0.set_result_code(ResultCode::PartitionUnavailable, false);
                    }
                    held.push(pair);
                }
            }
        }

        let total = buckets.iter().map(|(_, b)| b.len()).sum::<usize>() + held.len();
        let mut regrouped: Vec<IndexedOp> = Vec::with_capacity(total);
        let mut ranges: Vec<(Arc<Node>, std::ops::Range<usize>)> = Vec::with_capacity(buckets.len());
        for (node, bucket) in buckets {
            let start = regrouped.len();
            regrouped.extend(bucket);
            ranges.push((node, start..regrouped.len()));
        }
        regrouped.extend(held);
        (regrouped, ranges)
    }

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

    /// Connection-queue hint for a request group, derived from the group's
    /// first digest exactly as `SingleCommand::hint` does for one key.
    ///
    /// The hint picks which of the node's `conn_pools_per_node` queues a
    /// checkout starts on, and which one a new connection is opened into. A
    /// constant sent every batch sub-request to queue 0 — every checkout
    /// contending on the same lock, and every new connection filling queue 0
    /// before any other queue was touched, which is the opposite of what
    /// sharding the pool is for.
    fn queue_hint(batch_ops: &[(BatchOperation, usize)]) -> u8 {
        batch_ops.first().map_or(0, |(op, _)| op.key().digest[0])
    }

    async fn request_group(
        batch_ops: &mut [(BatchOperation, usize)],
        policy: &BatchPolicy,
        deadline: Option<Instant>,
        node: Arc<Node>,
        cmd_type: crate::metrics::CommandType,
        sample_draw: u64,
        sampled: &mut Option<bool>,
        commands_sent: &mut u32,
        hook: Option<&crate::batch::BatchHook>,
    ) -> Result<Option<Error>> {
        // Per-node circuit breaker: don't even open a socket if the node
        // is currently outside its error-rate window. Mirrors Java's
        // `node.validateErrorCount()` call site at the top of every
        // command attempt.
        // Metrics: one sample decision per command, made from the call-level
        // draw the first time a node is tried and reused across retries and
        // per-op groups (never re-rolled — metrics.md §3.1.1).
        if sampled.is_none() {
            *sampled = Some(node.metrics().should_sample_draw(sample_draw));
        }
        let metrics_on = sampled.unwrap_or(false);

        if let Err(err) = node.validate_error_count() {
            if metrics_on {
                node.metrics().incr_circuit_breaker_hits();
            }
            return Ok(Some(err));
        }

        // Detailed per-namespace metrics are attributed to every distinct
        // namespace in this request group; only worth building when this
        // command is being recorded.
        let namespaces: Vec<String> = if metrics_on {
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
        let mut conn = match node.get_connection(Self::queue_hint(batch_ops)).await {
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
        let write_result = conn.flush().await;
        // Bytes are accounted for whatever the outcome: the socket layer
        // counted exactly what left the client, including a partial write.
        if metrics_on {
            let sent = conn.bytes_sent() as u64;
            for ns in &namespaces {
                node.metrics().record_bytes_sent(ns, cmd_type, sent);
            }
        }
        if let Err(err) = write_result {
            // IO errors are considered temporary anomalies. Retry.
            // Close socket to flush out possible garbage. Do not put back in pool.
            conn.invalidate();
            warn!("Node {node}: {err}");
            node.incr_error_rate();
            return Ok(Some(err));
        }
        *commands_sent += 1;

        // Parse results.
        let parse_start = Instant::now();
        let parse_outcome = Self::parse_result(
            batch_ops,
            &mut conn,
            policy.base_policy.txn.as_ref(),
            *commands_sent,
        hook,
                )
        .await;
        if metrics_on {
            // Read side, same rule: exact bytes whatever the outcome.
            let received = conn.bytes_received() as u64;
            for ns in &namespaces {
                node.metrics().record_bytes_received(ns, cmd_type, received);
            }
            if parse_outcome.is_ok() {
                // One sample per successful node sub-batch RPC: latency spans
                // connection acquire → response parsed (metrics.md §4.6).
                let rpc_elapsed = aq_start.elapsed();
                let parse_elapsed = parse_start.elapsed();
                for ns in &namespaces {
                    node.metrics().record_latency(ns, cmd_type, rpc_elapsed);
                    node.metrics().record_parse(ns, cmd_type, parse_elapsed);
                }
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
        hook: Option<&crate::batch::BatchHook>,
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
                    // Fire as the row lands; `false` (or a cancelled batch)
                    // tears this group down — the connection is mid-stream
                    // and is invalidated, not pooled.
                    if let Some(hook) = hook {
                        if !hook.fire(batch_op.1, batch_op.0.batch_record()).await {
                            return Err(Error::stream_terminated(None));
                        }
                    }
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
                        if let Some(hook) = hook {
                            if !hook.fire(batch_op.1, batch_op.0.batch_record()).await {
                                return Err(Error::stream_terminated(None));
                            }
                        }
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
        hook: Option<&crate::batch::BatchHook>,
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

                    match Self::parse_group(batch_ops, &mut inner_conn, inner_size, txn, commands_sent, hook)
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
                    match Self::parse_group(batch_ops, &mut conn, size, txn, commands_sent, hook).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node_validator::NodeValidator;
    use crate::net::Host;
    use crate::policy::ClientPolicy;
    use crate::{BatchReadPolicy, Bins, Key, Version};

    fn node(name: &str) -> Arc<Node> {
        let policy = ClientPolicy::default();
        let nv = Arc::new(NodeValidator {
            name: name.to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
            cluster_name: None,
            detect_load_balancer: false,
        });
        let metrics = Arc::new(crate::metrics::NodeMetrics::new(
            crate::metrics::MetricsPolicy::default(),
        ));
        Arc::new(Node::new(
            policy,
            nv,
            metrics,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            None,
        ))
    }

    /// Deterministic xorshift so a failing seed reproduces exactly.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn indices(v: &[IndexedOp]) -> Vec<usize> {
        v.iter().map(|(_, i)| *i).collect()
    }

    /// Every key shares a node: the retry must be one group covering all rows.
    #[test]
    fn regroup_sends_one_group_when_every_key_shares_a_node() {
        let only = node("A");
        let ops: Vec<_> = (0..5).map(pair).collect();
        let routed = vec![Some(only.clone()); 5];
        let (regrouped, ranges) = BatchOperateCommand::regroup_for_retry(ops, routed);
        assert_eq!(ranges.len(), 1);
        assert!(Arc::ptr_eq(&ranges[0].0, &only));
        assert_eq!(ranges[0].1, 0..5);
        assert_eq!(indices(&regrouped), vec![0, 1, 2, 3, 4]);
    }

    /// Interleaved keys come back contiguous per node, relative order kept.
    /// Grouping is first-seen (B leads because key 0 routed to it); which node
    /// comes first is not a guarantee, contiguity and relative order are.
    #[test]
    fn regroup_makes_each_nodes_keys_contiguous() {
        let (a, b) = (node("A"), node("B"));
        let ops: Vec<_> = (0..4).map(pair).collect();
        let routed = vec![Some(b.clone()), Some(a.clone()), Some(b.clone()), Some(a.clone())];
        let (regrouped, ranges) = BatchOperateCommand::regroup_for_retry(ops, routed);
        assert_eq!(ranges.len(), 2);
        assert!(Arc::ptr_eq(&ranges[0].0, &b));
        assert_eq!(ranges[0].1, 0..2);
        assert!(Arc::ptr_eq(&ranges[1].0, &a));
        assert_eq!(ranges[1].1, 2..4);
        assert_eq!(indices(&regrouped), vec![0, 2, 1, 3]);
    }

    /// A key with no reachable replica is stamped and set aside after the
    /// last range; routable keys are grouped as usual.
    #[test]
    fn regroup_strands_unroutable_keys_and_keeps_the_rest() {
        let (a, b) = (node("A"), node("B"));
        let ops: Vec<_> = (0..5).map(pair).collect();
        // 0 -> A, 1 -> unroutable, 2 -> B, 3 -> A, 4 -> unroutable
        let routed = vec![Some(a.clone()), None, Some(b.clone()), Some(a.clone()), None];
        let (regrouped, ranges) = BatchOperateCommand::regroup_for_retry(ops, routed);

        assert_eq!(regrouped.len(), 5, "every row is kept for the executor");
        let covered: usize = ranges.iter().map(|(_, r)| r.len()).sum();
        assert_eq!(covered, 3, "ranges cover exactly the routable rows");
        assert_eq!(ranges.last().unwrap().1.end, 3, "ranges stop before the stranded rows");
        for (node, range) in &ranges {
            for i in range.clone() {
                let original = regrouped[i].1;
                let expected = if original == 2 { &b } else { &a };
                assert!(Arc::ptr_eq(node, expected), "row {original} in the wrong group");
                assert!(regrouped[i].0.batch_record().result_code.is_none());
            }
        }
        let mut stranded = indices(&regrouped[3..]);
        stranded.sort_unstable();
        assert_eq!(stranded, vec![1, 4]);
        for (op, _) in &regrouped[3..] {
            let br = op.batch_record();
            assert_eq!(br.result_code, Some(ResultCode::PartitionUnavailable));
            assert!(!br.in_doubt, "an unsent row is never in doubt");
        }
    }

    /// A row that already carries a result — answered on an earlier attempt,
    /// or stranded on an earlier retry — is held out of every group and its
    /// result is left exactly as it was.
    #[test]
    fn regroup_holds_settled_rows_without_restamping_them() {
        let a = node("A");
        let mut ops: Vec<_> = (0..3).map(pair).collect();
        ops[1].0.set_result_code(ResultCode::KeyNotFoundError, false);
        let routed = vec![Some(a.clone()), None, Some(a.clone())];
        let (regrouped, ranges) = BatchOperateCommand::regroup_for_retry(ops, routed);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].1, 0..2);
        assert_eq!(indices(&regrouped), vec![0, 2, 1]);
        assert_eq!(
            regrouped[2].0.batch_record().result_code,
            Some(ResultCode::KeyNotFoundError),
            "a settled row keeps its own result, it is not stamped PartitionUnavailable"
        );
    }

    #[test]
    fn regroup_with_nothing_routable_strands_every_row_and_sends_nothing() {
        let ops: Vec<_> = (0..3).map(pair).collect();
        let (regrouped, ranges) =
            BatchOperateCommand::regroup_for_retry(ops, vec![None, None, None]);
        assert!(ranges.is_empty());
        assert_eq!(regrouped.len(), 3);
        assert!(regrouped
            .iter()
            .all(|(op, _)| op.batch_record().result_code == Some(ResultCode::PartitionUnavailable)));
    }

    #[test]
    fn regroup_handles_an_empty_batch() {
        let (regrouped, ranges) = BatchOperateCommand::regroup_for_retry(Vec::new(), Vec::new());
        assert!(regrouped.is_empty());
        assert!(ranges.is_empty());
    }

    /// Random sizes (incl. 0 and 1), random node assignments from a random
    /// node count, random unroutable rows. Every property a caller relies on,
    /// checked independently of the implementation: ranges tile the routable
    /// prefix exactly (no gap, overlap or empty range); every row in a range
    /// routed to that range's node; a node's rows keep their input order;
    /// every input index appears exactly once; one range per node used; all
    /// unrouted rows sit after the last range, stamped.
    #[test]
    fn regroup_random_assignments_keep_every_invariant() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for round in 0..250 {
            let seed = rng.next();
            let mut r = Rng(seed | 1);
            let n = r.below(48);
            let k = 1 + r.below(5);
            let nodes: Vec<Arc<Node>> = (0..k).map(|i| node(&format!("N{i}"))).collect();
            // usize::MAX marks "unroutable".
            let assign: Vec<usize> = (0..n)
                .map(|_| if r.below(6) == 0 { usize::MAX } else { r.below(k) })
                .collect();
            let ops: Vec<_> = (0..n).map(pair).collect();
            let routed: Vec<Option<Arc<Node>>> = assign
                .iter()
                .map(|&a| (a != usize::MAX).then(|| nodes[a].clone()))
                .collect();

            let (regrouped, ranges) = BatchOperateCommand::regroup_for_retry(ops, routed);
            let ctx = format!("round {round} seed {seed:#x} n={n} k={k} assign={assign:?}");
            let routable = assign.iter().filter(|&&a| a != usize::MAX).count();

            assert_eq!(regrouped.len(), n, "lost or duplicated pairs: {ctx}");
            let mut next_start = 0;
            for (_, range) in &ranges {
                assert_eq!(range.start, next_start, "gap or overlap: {ctx}");
                assert!(!range.is_empty(), "empty range: {ctx}");
                next_start = range.end;
            }
            assert_eq!(next_start, routable, "ranges must cover exactly the routable rows: {ctx}");

            for (node, range) in &ranges {
                let mut prev: Option<usize> = None;
                for i in range.clone() {
                    let original = regrouped[i].1;
                    assert!(
                        Arc::ptr_eq(&nodes[assign[original]], node),
                        "pair {original} sits in another node's range: {ctx}"
                    );
                    if let Some(p) = prev {
                        assert!(p < original, "relative order not preserved: {ctx}");
                    }
                    prev = Some(original);
                    assert!(regrouped[i].0.batch_record().result_code.is_none(), "{ctx}");
                }
            }
            for (op, original) in &regrouped[routable..] {
                assert_eq!(assign[*original], usize::MAX, "routable row held out: {ctx}");
                assert_eq!(
                    op.batch_record().result_code,
                    Some(ResultCode::PartitionUnavailable),
                    "{ctx}"
                );
            }

            let mut all = indices(&regrouped);
            all.sort_unstable();
            assert_eq!(all, (0..n).collect::<Vec<_>>(), "index multiset wrong: {ctx}");

            let mut used: Vec<usize> = assign.iter().copied().filter(|&a| a != usize::MAX).collect();
            used.sort_unstable();
            used.dedup();
            assert_eq!(ranges.len(), used.len(), "range count != nodes used: {ctx}");
            for (i, (na, _)) in ranges.iter().enumerate() {
                for (nb, _) in &ranges[i + 1..] {
                    assert!(!Arc::ptr_eq(na, nb), "node appears in two ranges: {ctx}");
                }
            }
        }
    }

    /// `index` doubles as the key, so each pair has a distinct digest.
    fn pair(index: usize) -> (BatchOperation, usize) {
        let key = Key::new("test", "test", Value::from(index as i64)).unwrap();
        (
            BatchOperation::read(&BatchReadPolicy::default(), key, Bins::All),
            index,
        )
    }

    #[test]
    fn queue_hint_comes_from_the_first_digest() {
        let ops = [pair(1), pair(2)];
        let expected = ops[0].0.key().digest[0];
        assert_eq!(BatchOperateCommand::queue_hint(&ops), expected);
    }

    /// The hint must vary with the group, or every batch sub-request starts on
    /// the same queue — the defect this replaces. Distinct keys give distinct
    /// digests, so a fixed hint would show up as every group hashing alike.
    #[test]
    fn queue_hint_varies_across_groups() {
        let hints: std::collections::HashSet<u8> = (1..64)
            .map(|k| BatchOperateCommand::queue_hint(&[pair(k)]))
            .collect();
        assert!(
            hints.len() > 1,
            "hint is constant across 63 distinct first keys: {hints:?}"
        );
    }

    #[test]
    fn queue_hint_of_an_empty_group_does_not_panic() {
        assert_eq!(BatchOperateCommand::queue_hint(&[]), 0);
    }
}
