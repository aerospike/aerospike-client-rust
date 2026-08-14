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
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::StreamCommand;
use crate::commands::{self};
use crate::errors::{Error, Result};
use crate::net::{BufferedConn, Connection};
use crate::policy::{BatchPolicy, Policy, Replica};
use crate::{value, Record, ResultCode, Value};
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

/// A batch split into contiguous per-node slices: the reordered `(op, index)`
/// pairs, plus one `(node, range)` for every command that has to be sent.
type NodeGroups = (
    Vec<(BatchOperation, usize)>,
    Vec<(Arc<Node>, std::ops::Range<usize>)>,
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
        // Remember the most recent per-attempt error so retry exhaustion can
        // return a timeout that still displays the last failure.
        let mut last_err: Option<Error>;

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
        let mut last_tried: Vec<Arc<Node>> = vec![self.node.clone(); self.batch_ops.len()];

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
                let mut nodes: Vec<Arc<Node>> = Vec::with_capacity(self.batch_ops.len());
                for ((op, _), previous) in self.batch_ops.iter().zip(&last_tried) {
                    let key = op.key();
                    let partition = Partition::new_by_key(&key);
                    nodes.push(cluster.get_node(
                        &partition,
                        self.policy.replica,
                        Some(previous.clone()),
                    )?);
                }

                let (regrouped, ranges) =
                    Self::group_by_node(self.batch_ops.drain(..).collect(), nodes);
                self.batch_ops = regrouped;
                last_tried = ranges
                    .iter()
                    .flat_map(|(node, range)| range.clone().map(move |_| node.clone()))
                    .collect();

                // Run every group this round even if one fails — Java's
                // sub-batches are independent — and keep the first retriable
                // error to drive the next iteration.
                let mut group_err: Option<Error> = None;
                for (node, range) in ranges {
                    if let Some(e) =
                        Self::request_group(&mut self.batch_ops[range], &self.policy, deadline, node)
                            .await?
                    {
                        group_err.get_or_insert(e);
                    }
                }
                group_err
            };

            if let Some(err) = error {
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
    /// before they can be sent as a group; sorting by node name is what
    /// achieves that, and it also keeps the grouping deterministic. Each pair
    /// carries its original index, so the executor can still restore the
    /// caller's order afterwards.
    fn group_by_node(pairs: Vec<(BatchOperation, usize)>, nodes: Vec<Arc<Node>>) -> NodeGroups {
        let mut routed: Vec<(Arc<Node>, (BatchOperation, usize))> =
            nodes.into_iter().zip(pairs).collect();
        routed.sort_by(|a, b| a.0.name().cmp(b.0.name()));

        let mut regrouped: Vec<(BatchOperation, usize)> = Vec::with_capacity(routed.len());
        let mut ranges: Vec<(Arc<Node>, std::ops::Range<usize>)> = Vec::new();
        for (node, pair) in routed {
            let pos = regrouped.len();
            match ranges.last_mut() {
                Some((last, range)) if Arc::ptr_eq(last, &node) => range.end = pos + 1,
                _ => ranges.push((node, pos..pos + 1)),
            }
            regrouped.push(pair);
        }
        (regrouped, ranges)
    }

    fn wrap_last_error(last_err: Option<Error>, timeout_err: Error) -> Error {
        match last_err {
            Some(err) => err.wrap(timeout_err),
            None => timeout_err,
        }
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

        let mut conn = match node.get_connection(0).await {
            Ok(conn) => conn,
            Err(err) => {
                warn!("Node {node}: {err}");
                node.incr_error_rate();
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
        // Sorted by node name, so A's keys come first.
        assert!(Arc::ptr_eq(&ranges[0].0, &a));
        assert_eq!(ranges[0].1, 0..2);
        assert!(Arc::ptr_eq(&ranges[1].0, &b));
        assert_eq!(ranges[1].1, 2..4);

        // Nothing lost, and every key sits in its own node's range.
        let indices: Vec<usize> = regrouped.iter().map(|(_, i)| *i).collect();
        assert_eq!(indices, vec![1, 3, 0, 2]);
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

    #[test]
    fn group_by_node_handles_an_empty_batch() {
        let (regrouped, ranges) = BatchOperateCommand::group_by_node(Vec::new(), Vec::new());
        assert!(regrouped.is_empty());
        assert!(ranges.is_empty());
    }
}