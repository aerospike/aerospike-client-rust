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
use crate::cluster::{Cluster, Node};
use crate::commands::StreamCommand;
use crate::commands::{self};
use crate::errors::{Error, Result};
use crate::net::{BufferedConn, Connection};
use crate::policy::{BatchPolicy, Policy, Replica};
use crate::{value, Record, ResultCode, Value};
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

/// A batch operation paired with the index it had in the caller's input.
type IndexedOp = (BatchOperation, usize);

/// One node's operations, gathered while regrouping.
type NodeBucket = (Arc<Node>, Vec<IndexedOp>);

/// A batch split into contiguous per-node slices: the reordered `(op, index)`
/// pairs, plus one `(node, range)` for every command that has to be sent.
type NodeGroups = (Vec<IndexedOp>, Vec<(Arc<Node>, std::ops::Range<usize>)>);

/// `NodeGroups` for a retry, plus the node each row (in the new order) was
/// last sent to.
type RetryGroups = (
    Vec<IndexedOp>,
    Vec<(Arc<Node>, std::ops::Range<usize>)>,
    Vec<Arc<Node>>,
);

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
        let mut pool_empty_waits = 0;
        // Remember the most recent per-attempt error so retry exhaustion can
        // return a timeout that still displays the last failure.
        let mut last_err: Option<Error> = None;

        // set timeout outside the loop
        let deadline = self.policy.deadline();

        // A retry goes back to the originally selected node only when the
        // replica policy pins routing to the master: Java's
        // `BatchCommand.prepareRetry` returns true for MASTER/MASTER_PROLES/
        // RANDOM, and of those v2 has only `Master`. Sequence and PreferRack
        // must advance the replica instead.
        let same_node_retry = matches!(self.policy.replica, Replica::Master);
        // The node each key was last tried on. v2 routes by "the replica after
        // `last_tried`" rather than by an explicit sequence index, so advancing
        // the sequence means remembering where every key has just been. All
        // keys start on the node the executor picked for this command.
        //
        // Built on first use, not up front: only a Sequence/PreferRack re-split
        // ever reads it, so filling it eagerly charged every batch — including
        // the `Replica::Master` policy that never re-splits, and the common case
        // where the first attempt succeeds — one `Arc` clone, and so one atomic
        // increment, per key in the batch.
        let mut last_tried: Option<Vec<Arc<Node>>> = None;

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            let error = if iterations == 0 || same_node_retry {
                // First attempt, and every retry under `Replica::Master`: the
                // whole group goes to the originally selected node.
                Self::request_group(
                    &mut self.batch_ops,
                    &self.policy,
                    deadline,
                    self.node.clone(),
                )
                .await?
            } else {
                // Sequence/PreferRack retry (Java `BatchCommand.retryBatch`):
                // advancing the replica re-maps every key, so re-split the
                // batch into per-node groups and send one command per node.
                //
                // The previous scheme instead alternated: even attempts went
                // back to the node that had just failed, and odd attempts sent
                // **one wire command per key** — a 1000-key batch became 1000
                // round trips — always to the replica after the *original*
                // node, so a third attempt never advanced past the second
                // replica.
                let previous =
                    last_tried.get_or_insert_with(|| vec![self.node.clone(); self.batch_ops.len()]);

                // One snapshot of the partition table for the whole re-split.
                // Routing each key through `cluster.get_node` instead repeated
                // a hazard-pointer load and a namespace hash per key, every
                // retry.
                let mut routed: Vec<Option<Arc<Node>>> = Vec::with_capacity(self.batch_ops.len());
                let mut route_err: Option<Error> = None;
                cluster.route_keys(
                    self.batch_ops
                        .iter()
                        .zip(previous.iter())
                        .map(|((op, _), prev)| (op.key(), Some(prev.clone()))),
                    self.policy.replica,
                    |node| match node {
                        Ok(node) => routed.push(Some(node)),
                        Err(err) => {
                            routed.push(None);
                            route_err.get_or_insert(err);
                        }
                    },
                );
                // A key with no reachable replica is a per-key outcome, exactly
                // as on the first-attempt split: it is stamped
                // PARTITION_UNAVAILABLE and the rest of the group carries on.
                // Failing the command here instead made the executor's `?`
                // discard every other node's already-completed results — the
                // whole batch lost, one level down and after the work was
                // done, in precisely the disruption that caused the retry.
                // Only a round with nothing routable at all fails outright.
                if routed.iter().all(Option::is_none) {
                    if let Some(err) = route_err {
                        return Err(err);
                    }
                }

                // `mem::take` hands the vector over by pointer; `drain(..)
                // .collect()` built a second vector and moved every 568-byte
                // operation into it first.
                let (regrouped, ranges, tried) =
                    Self::regroup_for_retry(std::mem::take(&mut self.batch_ops), routed, previous);
                self.batch_ops = regrouped;
                last_tried = Some(tried);

                // Run every group this round even if one fails — Java's
                // sub-batches are independent — and keep the first retriable
                // error to drive the next iteration.
                let mut group_err: Option<Error> = None;
                for (node, range) in ranges {
                    if let Some(e) = Self::request_group(
                        &mut self.batch_ops[range],
                        &self.policy,
                        deadline,
                        node,
                    )
                    .await?
                    {
                        group_err.get_or_insert(e);
                    }
                }
                group_err
            };

            if let Some(err) = error {
                // Pool exhaustion is a pacing wait, not a failure: another
                // task returning its connection clears it. It consumes
                // neither the retry budget (`iterations` is not bumped) nor
                // `last_err` (thousands of waits must not bury the terminal
                // error), and `request_group` did not count it against the
                // node's error-rate breaker. The loop's deadline check sits
                // below `iterations += 1`, which this arm skips, so the wait
                // is deadline-checked here; `POOL_EMPTY_MAX_WAITS` bounds
                // deadline-less commands.
                if err.is_pool_empty() && pool_empty_waits < commands::POOL_EMPTY_MAX_WAITS {
                    if let Some(deadline) = deadline {
                        if Instant::now() + commands::POOL_EMPTY_WAIT > deadline {
                            return Err(Self::wrap_last_error(
                                last_err,
                                Error::Timeout(format!(
                                    "Command timed out after {iterations} tries"
                                )),
                            ));
                        }
                    }
                    pool_empty_waits += 1;
                    sleep(commands::POOL_EMPTY_WAIT).await;
                    continue;
                }
                warn!("Node {}: {err}", self.node);
                last_err = Some(err);
            } else {
                // command has completed successfully. Exit method.
                return Ok(self);
            }

            iterations += 1;

            // Retry budget exhausted: `max_retries + 1` attempts in total, as
            // in Java and in v2's own single-command path. The old condition
            // allowed `max_retries + 2` attempts, and treated
            // `max_retries == 0` — which means "do not retry" — as unbounded
            // retries until the deadline expired.
            if iterations > self.policy.max_retries() {
                return Err(Self::wrap_last_error(
                    last_err,
                    Error::Timeout(format!("Timeout after {iterations} tries")),
                ));
            }

            // Sleep before trying again, after the first iteration
            if let Some(sleep_between_retries) = self.policy.sleep_between_retries() {
                sleep(sleep_between_retries).await;
            }

            // check for command timeout
            if let Some(deadline) = deadline {
                if Instant::now() > deadline {
                    return Err(Self::wrap_last_error(
                        last_err,
                        Error::Timeout(format!("Command timed out after {iterations} tries")),
                    ));
                }
            }
        }
    }

    /// Reorder `pairs` so that every node's keys sit contiguously, and return
    /// the per-node ranges over the reordered vector.
    ///
    /// `nodes[i]` is where `pairs[i]` must go. One batch command covers one
    /// contiguous slice, so the keys have to be moved next to their peers
    /// before they can be sent as a group. Each pair carries its original
    /// index, so the executor can still restore the caller's order afterwards.
    ///
    /// The grouping is by pointer into per-node buckets, which visits each pair
    /// twice. Sorting the routed pairs by node name instead — as this did —
    /// compared two ~40-character strings per comparison and moved a 576-byte
    /// tuple per swap, O(N log N) of each, to arrive at the same contiguity.
    ///
    /// Nodes come out in first-seen order. The order is an implementation
    /// detail either way: the `batch_index` on the wire is relative to the
    /// slice handed to the encoder, and the caller's ordering is restored from
    /// the index each pair carries.
    fn group_by_node(pairs: Vec<IndexedOp>, nodes: Vec<Arc<Node>>) -> NodeGroups {
        // Distinct nodes in first-seen order, each with its share counted, and
        // the bucket every pair belongs to.
        let mut counts: Vec<(Arc<Node>, usize)> = Vec::new();
        let mut bucket_of: Vec<usize> = Vec::with_capacity(nodes.len());
        for node in &nodes {
            if let Some(pos) = counts.iter().position(|(e, _)| Arc::ptr_eq(e, node)) {
                counts[pos].1 += 1;
                bucket_of.push(pos);
            } else {
                bucket_of.push(counts.len());
                counts.push((node.clone(), 1));
            }
        }

        let mut buckets: Vec<NodeBucket> = counts
            .into_iter()
            .map(|(node, count)| (node, Vec::with_capacity(count)))
            .collect();
        for (pair, bucket) in pairs.into_iter().zip(&bucket_of) {
            buckets[*bucket].1.push(pair);
        }

        let total: usize = buckets.iter().map(|(_, b)| b.len()).sum();
        let mut regrouped: Vec<IndexedOp> = Vec::with_capacity(total);
        let mut ranges: Vec<(Arc<Node>, std::ops::Range<usize>)> =
            Vec::with_capacity(buckets.len());
        for (node, bucket) in buckets {
            let start = regrouped.len();
            regrouped.extend(bucket);
            ranges.push((node, start..regrouped.len()));
        }
        (regrouped, ranges)
    }

    /// Re-split a batch for a Sequence/PreferRack retry.
    ///
    /// `routed[i]` is where `ops[i]` goes this round, or `None` when no
    /// replica could be reached for it; `previous[i]` is where it was last
    /// sent. Routable rows are grouped per node exactly as on the first
    /// attempt; unroutable rows are stamped `PARTITION_UNAVAILABLE` and placed
    /// after the last range, so no group sends them, but they stay in the
    /// command so the executor returns them at their input index.
    ///
    /// The returned `last_tried` is aligned to the new order. A stranded row
    /// keeps its previous node, so if a later retry within the budget can
    /// route it again the replica sequence resumes where it left off — and
    /// the answer then overwrites the stamp, as any re-sent row's does.
    fn regroup_for_retry(
        ops: Vec<IndexedOp>,
        routed: Vec<Option<Arc<Node>>>,
        previous: &[Arc<Node>],
    ) -> RetryGroups {
        debug_assert_eq!(ops.len(), routed.len());
        debug_assert_eq!(ops.len(), previous.len());

        let mut routable: Vec<IndexedOp> = Vec::with_capacity(ops.len());
        let mut nodes: Vec<Arc<Node>> = Vec::with_capacity(ops.len());
        let mut stranded: Vec<(IndexedOp, Arc<Node>)> = Vec::new();
        for (i, (mut pair, node)) in ops.into_iter().zip(routed).enumerate() {
            if let Some(node) = node {
                routable.push(pair);
                nodes.push(node);
            } else {
                pair.0
                    .set_result_code(ResultCode::PartitionUnavailable, false);
                stranded.push((pair, previous[i].clone()));
            }
        }

        let (mut regrouped, ranges) = Self::group_by_node(routable, nodes);
        let mut tried: Vec<Arc<Node>> = ranges
            .iter()
            .flat_map(|(node, range)| range.clone().map(move |_| node.clone()))
            .collect();
        for (pair, prev) in stranded {
            regrouped.push(pair);
            tried.push(prev);
        }
        (regrouped, ranges, tried)
    }

    fn wrap_last_error(last_err: Option<Error>, timeout_err: Error) -> Error {
        match last_err {
            Some(err) => err.wrap(timeout_err),
            None => timeout_err,
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
    ) -> Result<Option<Error>> {
        // Per-node circuit breaker: don't even open a socket if the node
        // is currently outside its error-rate window. Mirrors Java's
        // `node.validateErrorCount()` call site at the top of every
        // command attempt.
        if let Err(err) = node.validate_error_count() {
            return Ok(Some(err));
        }

        let mut conn = match node.get_connection(Self::queue_hint(batch_ops), None).await {
            Ok(conn) => conn,
            Err(err) => {
                // An exhausted pool is a transient shortage the retry loop
                // waits out, not a node failure: it must not warn (a paced
                // wait would flood the log) and must not trip the error-rate
                // breaker, which would turn the shortage into instant
                // `MaxErrorRate` refusals.
                if !err.is_pool_empty() {
                    warn!("Node {node}: {err}");
                    node.incr_error_rate();
                }
                return Ok(Some(err));
            }
        };

        conn.buffer
            .set_batch_operate(policy, batch_ops)
            .map_err(|e| {
                // Same as the single-key path: keep a caller's argument error
                // instead of replacing it with a generic client error. This
                // previously discarded the cause entirely, so a value the client
                // cannot encode surfaced as a bare "Failed to prepare send
                // buffer" with nothing to explain it.
                if matches!(e, Error::InvalidArgument(_)) {
                    e
                } else {
                    e.chain_error("Failed to prepare send buffer")
                }
            })?;

        conn.buffer.write_timeout(policy.server_timeout());

        conn.set_socket_timeout(deadline, policy.socket_timeout());
        conn.set_timeout_delay(true, policy.timeout_delay());

        // Send command.
        if let Err(err) = conn.flush().await {
            // IO errors are considered temporary anomalies. Retry.
            // Close socket to flush out possible garbage. Do not put back in pool.
            conn.invalidate();
            warn!("Node {node}: {err}");
            node.incr_error_rate();
            return Ok(Some(err));
        }

        // Parse results.
        if let Err(err) = Self::parse_result(batch_ops, &mut conn).await {
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
                Err(Error::BatchLastError(batch_index, rc, in_doubt, ..)) => {
                    // A per-key server error that happens to arrive on the LAST
                    // record of the response. It is the same kind of per-key
                    // outcome as the `BatchError` arm below — the only
                    // difference is where it landed in the stream — so stamp
                    // the row and finish the group instead of failing the whole
                    // call and discarding every other row (Java records the
                    // code on the BatchRecord and returns the record set).
                    let batch_op = batch_ops
                        .get_mut(batch_index as usize)
                        .expect("Invalid batch index");
                    batch_op.0.set_result_code(rc, in_doubt);
                    return Ok(false);
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
        commands::keep_connection(err)
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
            services: vec![],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
        });
        Arc::new(Node::new(policy, nv))
    }

    /// `index` doubles as the key, so a regrouped pair can be traced back to
    /// the node it was routed to.
    fn pair(index: usize) -> (BatchOperation, usize) {
        let key = Key::new("test", "test", Value::from(index as i64)).unwrap();
        (
            BatchOperation::read(&BatchReadPolicy::default(), key, Bins::All),
            index,
        )
    }

    /// The single-node case, and the one the old code got most expensive: every
    /// key shares a node, so the retry must be *one* batch command. The
    /// previous per-key retry sent as many commands as there were keys.
    #[test]
    fn group_by_node_sends_one_group_when_every_key_shares_a_node() {
        let only = node("A");
        let pairs: Vec<_> = (0..5).map(pair).collect();
        let nodes = vec![only.clone(); 5];

        let (regrouped, ranges) = BatchOperateCommand::group_by_node(pairs, nodes);

        assert_eq!(ranges.len(), 1, "expected a single batch command");
        assert!(Arc::ptr_eq(&ranges[0].0, &only));
        assert_eq!(ranges[0].1, 0..5);
        let indices: Vec<usize> = regrouped.iter().map(|(_, i)| *i).collect();
        assert_eq!(indices, vec![0, 1, 2, 3, 4]);
    }

    /// Keys interleaved across two nodes must come back gathered into one
    /// contiguous range per node, because a batch command can only cover a
    /// slice.
    #[test]
    fn group_by_node_makes_each_nodes_keys_contiguous() {
        let a = node("A");
        let b = node("B");
        let pairs: Vec<_> = (0..4).map(pair).collect();
        // Interleaved on purpose: 0 -> B, 1 -> A, 2 -> B, 3 -> A.
        let nodes = vec![b.clone(), a.clone(), b.clone(), a.clone()];

        let (regrouped, ranges) = BatchOperateCommand::group_by_node(pairs, nodes);

        assert_eq!(ranges.len(), 2, "one group per node");
        // Grouped in first-seen order, so B — which key 0 routed to — leads.
        // Which node comes first is not a guarantee; that each node's keys are
        // contiguous, and that their relative order survives, is.
        assert!(Arc::ptr_eq(&ranges[0].0, &b));
        assert_eq!(ranges[0].1, 0..2);
        assert!(Arc::ptr_eq(&ranges[1].0, &a));
        assert_eq!(ranges[1].1, 2..4);

        // Nothing lost, and every key sits in its own node's range.
        let indices: Vec<usize> = regrouped.iter().map(|(_, i)| *i).collect();
        assert_eq!(indices, vec![0, 2, 1, 3]);
    }

    /// The ranges must tile the whole vector: a gap or an overlap would drop or
    /// double-send keys.
    #[test]
    fn group_by_node_ranges_tile_every_pair_exactly_once() {
        let (a, b, c) = (node("A"), node("B"), node("C"));
        let pairs: Vec<_> = (0..6).map(pair).collect();
        let nodes = vec![c.clone(), a.clone(), c.clone(), b, a, c];

        let (regrouped, ranges) = BatchOperateCommand::group_by_node(pairs, nodes);

        let mut covered = 0;
        let mut next_start = 0;
        for (_, range) in &ranges {
            assert_eq!(range.start, next_start, "ranges must be contiguous");
            covered += range.len();
            next_start = range.end;
        }
        assert_eq!(covered, regrouped.len());
        assert_eq!(next_start, regrouped.len());

        let mut indices: Vec<usize> = regrouped.iter().map(|(_, i)| *i).collect();
        indices.sort_unstable();
        assert_eq!(indices, vec![0, 1, 2, 3, 4, 5]);
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

    /// Random batch sizes (including 0 and 1) over random node assignments
    /// drawn from a random number of nodes. Every property a caller relies on
    /// is checked independently of how the grouping is implemented:
    /// - the ranges tile `0..n` exactly, with no gap, overlap or empty range;
    /// - every pair inside a range routed to that range's node;
    /// - a node's pairs keep the relative order they had on input, which is
    ///   what lets consecutive identical rows still compress into repeats;
    /// - every original index appears exactly once;
    /// - one range per node that actually received a key, and no node twice.
    #[test]
    fn group_by_node_random_assignments_keep_every_invariant() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for round in 0..250 {
            let seed = rng.next();
            let mut r = Rng(seed | 1);
            let n = r.below(48);
            let k = 1 + r.below(5);
            let nodes: Vec<Arc<Node>> = (0..k).map(|i| node(&format!("N{i}"))).collect();
            let assign: Vec<usize> = (0..n).map(|_| r.below(k)).collect();

            let pairs: Vec<_> = (0..n).map(pair).collect();
            let routed: Vec<Arc<Node>> = assign.iter().map(|&a| nodes[a].clone()).collect();
            let (regrouped, ranges) = BatchOperateCommand::group_by_node(pairs, routed);

            let ctx = format!("round {round} seed {seed:#x} n={n} k={k} assign={assign:?}");
            assert_eq!(regrouped.len(), n, "lost or duplicated pairs: {ctx}");

            // Tiling.
            let mut next_start = 0;
            for (_, range) in &ranges {
                assert_eq!(range.start, next_start, "gap or overlap: {ctx}");
                assert!(!range.is_empty(), "{}", format!("empty range: {ctx}"));
                next_start = range.end;
            }
            assert_eq!(next_start, n, "ranges do not cover the batch: {ctx}");

            // Membership and relative order, per range.
            for (node, range) in &ranges {
                let mut prev_index: Option<usize> = None;
                for i in range.clone() {
                    let original = regrouped[i].1;
                    assert!(
                        Arc::ptr_eq(&nodes[assign[original]], node),
                        "{}",
                        format!("pair {original} sits in another node's range: {ctx}")
                    );
                    if let Some(p) = prev_index {
                        assert!(
                            p < original,
                            "{}",
                            format!("relative order not preserved: {ctx}")
                        );
                    }
                    prev_index = Some(original);
                }
            }

            // Completeness.
            let mut indices: Vec<usize> = regrouped.iter().map(|(_, i)| *i).collect();
            indices.sort_unstable();
            assert_eq!(
                indices,
                (0..n).collect::<Vec<_>>(),
                "index multiset wrong: {ctx}"
            );

            // One range per node used, each node at most once.
            let mut used: Vec<usize> = assign.clone();
            used.sort_unstable();
            used.dedup();
            assert_eq!(ranges.len(), used.len(), "range count != nodes used: {ctx}");
            for (a, (na, _)) in ranges.iter().enumerate() {
                for (nb, _) in &ranges[a + 1..] {
                    assert!(
                        !Arc::ptr_eq(na, nb),
                        "{}",
                        format!("node appears in two ranges: {ctx}")
                    );
                }
            }
        }
    }

    #[test]
    fn group_by_node_handles_an_empty_batch() {
        let (regrouped, ranges) = BatchOperateCommand::group_by_node(Vec::new(), Vec::new());
        assert!(regrouped.is_empty());
        assert!(ranges.is_empty());
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
            "{}",
            format!("hint is constant across 63 distinct first keys: {hints:?}")
        );
    }

    /// On a retry re-split, a key with no reachable replica is stamped and set
    /// aside instead of failing the command; the routable keys are grouped as
    /// usual, and `last_tried` lines up with the new order.
    #[test]
    fn regroup_for_retry_strands_unroutable_keys_and_keeps_the_rest() {
        let (a, b, old) = (node("A"), node("B"), node("OLD"));
        let ops: Vec<_> = (0..5).map(pair).collect();
        // 0 -> A, 1 -> unroutable, 2 -> B, 3 -> A, 4 -> unroutable
        let routed = vec![
            Some(a.clone()),
            None,
            Some(b.clone()),
            Some(a.clone()),
            None,
        ];
        let previous = vec![old.clone(); 5];

        let (regrouped, ranges, tried) =
            BatchOperateCommand::regroup_for_retry(ops, routed, &previous);

        assert_eq!(regrouped.len(), 5, "every row is kept for the executor");
        assert_eq!(tried.len(), 5, "last_tried aligned with the rows");

        // The routable prefix is grouped per node and covered by the ranges.
        let covered: usize = ranges.iter().map(|(_, r)| r.len()).sum();
        assert_eq!(covered, 3, "ranges cover exactly the routable rows");
        assert_eq!(
            ranges.last().unwrap().1.end,
            3,
            "ranges stop before the stranded rows"
        );
        for (node, range) in &ranges {
            for i in range.clone() {
                let original = regrouped[i].1;
                let expected = if original == 2 { &b } else { &a };
                assert!(
                    Arc::ptr_eq(node, expected),
                    "row {original} in the wrong group"
                );
                assert!(
                    Arc::ptr_eq(&tried[i], node),
                    "last_tried must be the node the row went to"
                );
                assert!(
                    regrouped[i].0.batch_record().result_code.is_none(),
                    "routable rows are untouched"
                );
            }
        }

        // The stranded rows sit after every range, stamped, with their previous
        // node carried so a later retry resumes the sequence.
        let mut stranded: Vec<usize> = regrouped[3..].iter().map(|(_, i)| *i).collect();
        stranded.sort_unstable();
        assert_eq!(stranded, vec![1, 4]);
        for i in 3..5 {
            let br = regrouped[i].0.batch_record();
            assert_eq!(br.result_code, Some(ResultCode::PartitionUnavailable));
            assert!(!br.in_doubt, "an unsent row is never in doubt");
            assert!(Arc::ptr_eq(&tried[i], &old));
        }
    }

    #[test]
    fn regroup_for_retry_with_everything_routable_matches_group_by_node() {
        let (a, b) = (node("A"), node("B"));
        let ops: Vec<_> = (0..4).map(pair).collect();
        let nodes = vec![b.clone(), a.clone(), b.clone(), a.clone()];
        let routed: Vec<_> = nodes.iter().cloned().map(Some).collect();
        let previous = vec![node("OLD"); 4];

        let (r1, ranges1, tried) = BatchOperateCommand::regroup_for_retry(ops, routed, &previous);
        let (r2, ranges2) = BatchOperateCommand::group_by_node((0..4).map(pair).collect(), nodes);

        let idx = |v: &Vec<(BatchOperation, usize)>| v.iter().map(|(_, i)| *i).collect::<Vec<_>>();
        assert_eq!(idx(&r1), idx(&r2));
        assert_eq!(ranges1.len(), ranges2.len());
        assert_eq!(tried.len(), 4);
        assert!(r1
            .iter()
            .all(|(op, _)| op.batch_record().result_code.is_none()));
    }

    #[test]
    fn regroup_for_retry_with_nothing_routable_strands_every_row() {
        let old = node("OLD");
        let ops: Vec<_> = (0..3).map(pair).collect();
        let (regrouped, ranges, tried) = BatchOperateCommand::regroup_for_retry(
            ops,
            vec![None, None, None],
            &vec![old.clone(); 3],
        );
        assert!(ranges.is_empty(), "nothing to send");
        assert_eq!(regrouped.len(), 3);
        assert!(
            regrouped
                .iter()
                .all(|(op, _)| op.batch_record().result_code
                    == Some(ResultCode::PartitionUnavailable))
        );
        assert!(tried.iter().all(|n| Arc::ptr_eq(n, &old)));
    }

    #[test]
    fn queue_hint_of_an_empty_group_does_not_panic() {
        assert_eq!(BatchOperateCommand::queue_hint(&[]), 0);
    }
}

#[cfg(test)]
mod pool_wait_tests {
    use super::*;
    use crate::batch::BatchOperation;
    use crate::cluster::node_validator::NodeValidator;
    use crate::net::Host;
    use crate::policy::ClientPolicy;
    use crate::{BatchReadPolicy, Bins, Key, Version};

    /// A batch that cannot get a connection waits for its deadline instead of
    /// failing instantly, and the wait feeds neither the retry budget nor the
    /// node's error-rate breaker.
    #[aerospike_macro::test]
    async fn batch_pool_empty_wait_is_deadline_bounded_and_not_a_node_error() {
        // max_error_rate 1: a single stray incr_error_rate trips the breaker
        // and changes the outcome, failing this test.
        let client_policy = ClientPolicy {
            max_conns_per_node: 1,
            conn_pools_per_node: 1,
            max_error_rate: 1,
            ..ClientPolicy::default()
        };
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            services: vec![],
            address: "127.0.0.1:3000".to_string(),
            client_policy: client_policy.clone(),
            use_new_info: true,
            version: Version::default(),
        });
        let node = Arc::new(Node::new(client_policy, nv));
        let _held = node
            .get_connection(0, None)
            .await
            .expect("first borrow saturates the pool");

        // The retry loop only consults the cluster when re-splitting the
        // batch, which a Replica::Master policy never does; an empty cluster
        // satisfies the signature.
        let cluster = Cluster::new(
            ClientPolicy {
                timeout: 100,
                fail_if_not_connected: false,
                ..ClientPolicy::default()
            },
            &[],
        )
        .await
        .expect("empty cluster");

        let mut policy = BatchPolicy::default();
        policy.base_policy.total_timeout = 200;
        policy.base_policy.max_retries = 0;
        policy.replica = Replica::Master;

        let key = Key::new("test", "set", 1.into()).expect("key");
        let ops = vec![(
            BatchOperation::read(&BatchReadPolicy::default(), key, Bins::All),
            0,
        )];
        let cmd = BatchOperateCommand::new(policy, node.clone(), ops);

        let start = Instant::now();
        let err = cmd
            .execute_command(cluster)
            .await
            .err()
            .expect("nothing ever returns a connection");
        let elapsed = start.elapsed();

        assert!(
            matches!(err, Error::Timeout(_)),
            "the wait must end in a clean timeout, got: {:?}",
            err
        );
        assert!(
            elapsed >= Duration::from_millis(150),
            "must wait for the deadline, not fail instantly: {:?}",
            elapsed
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must not overshoot the deadline: {:?}",
            elapsed
        );
        assert_eq!(
            node.error_rate_count(),
            0,
            "a pool wait must not count toward the node error rate"
        );
    }
}
