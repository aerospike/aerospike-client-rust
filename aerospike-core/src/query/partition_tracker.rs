// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use crate::cluster::node;
use crate::cluster::partition::Partition;
use crate::cluster::Cluster;
use crate::errors::{Error, Result};
use crate::policy::Replica;
use crate::policy::StreamPolicy;
use crate::query::NodePartitions;
use crate::query::PartitionFilter;
use crate::query::PartitionStatus;
use crate::Node;
use parking_lot::Mutex as PartMutex;

use aerospike_rt::time::{Duration, Instant};

use std::cmp::max;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// The slice of tracker state a node's record stream touches.
///
/// Everything here is either fixed for the life of the query or an atomic, so
/// the per-record path takes **no** lock: the partition statuses carry their
/// own `parking_lot` locks, and those are the ones that actually protect the
/// data. The coordinator's planning state lives in [`PartitionTracker`],
/// which owns this by `Arc` and hands clones to the recordset.
#[derive(Debug)]
pub struct TrackerShared {
    /// Status of every partition in the query's range, indexed by offset from
    /// `partition_begin`. The same allocation the `PartitionFilter` and each
    /// `NodePartitions` refer to.
    partitions: Arc<Vec<PartMutex<PartitionStatus>>>,
    /// The resume cursor, shared rather than handed over at the end: a
    /// consumer that cancels mid-scan asks for it the moment it closes the
    /// recordset, long before the coordinator has wound down. Everything
    /// either side does to it after construction goes through `&self` —
    /// two atomics and the per-partition locks — so sharing costs no lock.
    partition_filter: Arc<PartitionFilter>,
    partition_begin: usize,
    /// Records still allowed. The coordinator lowers it between rounds; the
    /// streams only read it.
    max_records: AtomicU64,
    /// Records handed out this round, across all nodes.
    record_count: AtomicUsize,
    socket_timeout: AtomicU32,
    total_timeout: AtomicU32,
}

impl TrackerShared {
    /// Whether this record fits under `max_records`, counting it if so.
    ///
    /// The budget is global to the query, so the counter is an atomic rather
    /// than per-node state; `np` only records the rejections.
    pub(crate) fn allow_record(&self, np: &mut NodePartitions) -> bool {
        let max_records = self.max_records.load(Ordering::Relaxed);
        if max_records == 0 {
            return true;
        }

        let record_count = self.record_count.fetch_add(1, Ordering::SeqCst) + 1;
        if record_count as u64 <= max_records {
            return true;
        }

        // Record was returned, but would exceed max_records.
        // Discard record and increment disallowed_count.
        np.disallowed_count += 1;
        false
    }

    /// Marks a partition the server could not serve, so the next round asks
    /// another node for it.
    pub(crate) fn partition_unavailable(
        &self,
        node_partitions: &mut NodePartitions,
        partition_id: u16,
    ) {
        if let Some(ps) = self
            .partitions
            .get(partition_id as usize - self.partition_begin)
        {
            let mut ps = ps.lock();
            ps.retry = true;
            if let Some(ref mut seq) = ps.sequence {
                *seq += 1;
            }
        }
        node_partitions.parts_unavailable += 1;
    }

    /// Advances the resume cursor for a record whose delivery is inherently
    /// in order: callback mode invokes the user inline on the node task, so
    /// delivery and commit are atomic and per-partition order is total — no
    /// stamp, no watermark. The channel path must not use this; it goes
    /// through [`commit_cursor`](Self::commit_cursor).
    pub(crate) fn commit_direct(&self, partition_id: usize, digest: [u8; 20], bval: Option<u64>) {
        let Some(offset) = partition_id.checked_sub(self.partition_begin) else {
            debug_assert!(false, "record partition {partition_id} below tracker range");
            return;
        };
        if let Some(ps) = self.partitions.get(offset) {
            let mut ps = ps.lock();
            ps.digest = Some(digest);
            if bval.is_some() {
                ps.bval = bval;
            }
        } else {
            debug_assert!(false, "record partition {partition_id} beyond tracker range");
        }
    }

    /// Stamps one record of `partition_id` as delivered to the record
    /// channel, returning its `(epoch, seq)`. A node stream delivers each
    /// partition's records in digest order, so the sequence numbers order
    /// them; the consumer edge uses the stamp to advance the resume cursor
    /// only along the contiguously consumed prefix.
    pub(crate) fn stamp_delivery(&self, partition_id: usize) -> Option<(u32, u32)> {
        let offset = partition_id.checked_sub(self.partition_begin)?;
        let ps = self.partitions.get(offset)?;
        let mut ps = ps.lock();
        ps.delivered += 1;
        Some((ps.epoch, ps.delivered))
    }

    /// Advances the resume cursor for a record the consumer has actually
    /// taken out of the stream.
    ///
    /// Called from the consumer edge — not the node stream — so the cursor
    /// never runs ahead of what the user has seen. Records that were parsed
    /// and buffered but never consumed stay in front of the cursor, and a
    /// resume re-fetches them: this is what makes an early `close()` lose
    /// nothing. (The C client gets the same guarantee by committing after
    /// the user callback returns.) `bval` accompanies the digest for
    /// secondary-index queries, whose cursor needs both to resume in order.
    ///
    /// Concurrent consumers take a partition's records out of order, and a
    /// cursor must never advance past a record a sibling consumer still
    /// holds unconsumed. The delivery stamp makes this safe: the cursor
    /// moves only along the longest *contiguously* consumed prefix of the
    /// delivery sequence — an out-of-order consume parks in `pending` until
    /// the gap beneath it closes. A stamp from an earlier round is stale
    /// and ignored; an unstamped entry (error and timeout paths) never
    /// moves the cursor.
    pub(crate) fn commit_cursor(
        &self,
        partition_id: usize,
        digest: [u8; 20],
        bval: Option<u64>,
        stamp: Option<(u32, u32)>,
    ) {
        let Some((epoch, seq)) = stamp else {
            return;
        };
        let Some(offset) = partition_id.checked_sub(self.partition_begin) else {
            debug_assert!(false, "record partition {partition_id} below tracker range");
            return;
        };
        let Some(ps) = self.partitions.get(offset) else {
            debug_assert!(false, "record partition {partition_id} beyond tracker range");
            return;
        };

        let mut ps = ps.lock();
        if epoch != ps.epoch {
            return; // a leftover from an earlier round; its range was re-queried
        }

        if seq == ps.consumed + 1 {
            ps.consumed = seq;
            ps.digest = Some(digest);
            if bval.is_some() {
                ps.bval = bval;
            }
            // Anything parked contiguously above this seq commits with it.
            while ps.pending.first().is_some_and(|&(s, ..)| s == ps.consumed + 1) {
                let (s, d, b) = ps.pending.remove(0);
                ps.consumed = s;
                ps.digest = Some(d);
                if b.is_some() {
                    ps.bval = b;
                }
            }
        } else if seq > ps.consumed {
            // Out of order: park until the gap beneath it closes.
            let at = ps.pending.partition_point(|&(s, ..)| s < seq);
            ps.pending.insert(at, (seq, digest, bval));
        }
        // seq <= consumed cannot happen: each entry is delivered exactly once.
    }

    /// The cursor as it stands, for a caller resuming a later scan/query.
    ///
    /// A stream closed early can leave records delivered but never consumed;
    /// their partitions are marked for retry here and the returned cursor is
    /// not `done`, so a resume re-queries them from the consumed watermark.
    /// Without this, the `done` flag — computed from what the server
    /// *delivered* — could claim completion while a closing consumer dropped
    /// a buffered tail.
    pub(crate) fn partition_filter(&self) -> PartitionFilter {
        let pf = (*self.partition_filter).clone();
        let mut lagging = false;
        for ps in self.partitions.iter() {
            let mut ps = ps.lock();
            if ps.delivered > ps.consumed {
                ps.retry = true;
                lagging = true;
            }
        }
        if lagging {
            pf.done.store(false, Ordering::Relaxed);
        }
        pf
    }

    /// The timeout written into a stream request's header.
    pub(crate) fn server_timeout(&self) -> u32 {
        if self.total_timeout.load(Ordering::Relaxed) > 0 {
            self.socket_timeout.load(Ordering::Relaxed)
        } else {
            0
        }
    }
}

/// The coordinator's half of the tracker: partition assignment, retry
/// planning and completion.
///
/// Owned outright by the executor future — never shared, never locked. Each
/// round it *moves* the per-node partition sets into their node tasks and
/// takes them back from the join values, so single-writer access to
/// [`NodePartitions`] is a property the compiler checks rather than one the
/// protocol merely promises.
#[derive(Debug)]
pub struct PartitionTracker {
    shared: Arc<TrackerShared>,
    partitions_capacity: usize,
    node_capacity: usize,
    node_filter: Option<Arc<Node>>,
    replica: Replica,
    node_partitions_list: Vec<NodePartitions>,
    sleep_between_retries: Option<Duration>,
    iteration: usize,
    deadline: Option<Instant>,
}

impl PartitionTracker {
    pub(crate) fn new(
        policy: impl StreamPolicy,
        mut partition_filter: PartitionFilter,
        nodes: &[Arc<Node>],
    ) -> Result<Self> {
        // Validate here instead of initial PartitionFilter constructor because total number of
        // cluster partitions may change on the server and PartitionFilter will never have access
        // to Cluster instance. Use fixed number of partitions for now.
        if partition_filter.begin >= node::PARTITIONS {
            return Err(Error::invalid_argument(format!(
                "Invalid partition begin {} . Valid range: 0-{}",
                partition_filter.begin,
                node::PARTITIONS - 1
            )));
        }

        if partition_filter.count == 0 {
            return Err(Error::invalid_argument(format!(
                "Invalid partition count {}",
                partition_filter.count
            )));
        }

        if (partition_filter.begin + partition_filter.count) > node::PARTITIONS {
            return Err(Error::invalid_argument(format!(
                "Invalid partition range ({},{})",
                partition_filter.begin,
                partition_filter.begin + partition_filter.count
            )));
        }

        if partition_filter.partitions.is_none() {
            let begin = partition_filter.begin;
            let count = partition_filter.count;
            let digest = partition_filter.digest;
            partition_filter.set_partitions(Self::init_partitions(begin, count, digest));
            partition_filter.retry.store(true, Ordering::Relaxed);
        } else {
            // retry all partitions when max_records not specified.
            if policy.max_records().is_none() {
                partition_filter.retry.store(true, Ordering::Relaxed);
            }

            partition_filter.reset_partition_status();
        }

        let partitions = Arc::clone(
            partition_filter
                .partitions
                .as_ref()
                .expect("partitions were just initialised"),
        );

        let partitions_capacity = partition_filter.count;
        let partition_begin = partition_filter.begin;
        let shared = Arc::new(TrackerShared {
            partitions,
            partition_begin,
            partition_filter: Arc::new(partition_filter),
            max_records: AtomicU64::new(policy.max_records().unwrap_or(0)),
            record_count: AtomicUsize::new(0),
            socket_timeout: AtomicU32::new(policy.socket_timeout()),
            total_timeout: AtomicU32::new(policy.total_timeout()),
        });

        // This is required for proxy server since there are no nodes represented there
        let node_capacity = max(1, nodes.len());

        Ok(PartitionTracker {
            shared,
            partitions_capacity,
            node_capacity,
            node_filter: None,
            replica: policy.replica(),
            node_partitions_list: vec![],
            sleep_between_retries: policy.sleep_between_retries(),
            iteration: 1,
            deadline: policy.deadline(),
        })
    }

    /// The handle the recordset and every node stream share.
    pub(crate) fn shared(&self) -> Arc<TrackerShared> {
        Arc::clone(&self.shared)
    }

    /// Hands this round's per-node sets to the caller, which moves each into
    /// its node's task. They come back through
    /// [`restore_node_partitions`](Self::restore_node_partitions).
    pub(crate) fn take_node_partitions(&mut self) -> Vec<NodePartitions> {
        std::mem::take(&mut self.node_partitions_list)
    }

    /// Takes back the sets the node tasks returned, so completion and retry
    /// planning can read what each node actually did.
    pub(crate) fn restore_node_partitions(&mut self, list: Vec<NodePartitions>) {
        self.node_partitions_list = list;
    }

    pub(crate) fn assign_partitions_to_nodes(
        &mut self,
        cluster: &Arc<Cluster>,
        namespace: &str,
    ) -> Result<()> {
        let mut list = Vec::<NodePartitions>::with_capacity(self.node_capacity);
        let partitions = Arc::clone(&self.shared.partitions);

        let retry = self.shared.partition_filter.retry.load(Ordering::Relaxed)
            && self.iteration == 1;

        for (offset, part) in partitions.iter().enumerate() {
            let (part_retry, part_id) = {
                let part = part.lock();
                (part.retry, part.id)
            };
            if retry || part_retry {
                let mut partition = Partition::new(namespace, part_id as usize);
                partition.replica = self.replica;
                let node = cluster.get_node(&mut partition)?;

                // Use node name to check for single node equality because
                // partition map may be in transitional state between
                // the old and new node with the same name.
                if let Some(node_filter) = self.node_filter.as_ref() {
                    if node_filter.name() != node.name() {
                        continue;
                    }
                }

                // The partition is being (re)queried: open a new delivery
                // round so stale entries stop moving its cursor.
                part.lock().begin_delivery_round();

                let index = offset as u16;
                if let Some(np) = Self::find_node(&mut list, &node) {
                    np.add_partition(index);
                } else {
                    // If the partition map is in a transitional state, multiple
                    // nodePartitions instances (each with different partitions)
                    // may be created for a single node.
                    let mut np = NodePartitions::new(
                        node.clone(),
                        self.partitions_capacity,
                        Arc::clone(&partitions),
                    );
                    np.add_partition(index);
                    list.push(np);
                }
            }
        }

        let node_size = list.len();
        if node_size == 0 {
            return Err(Error::client_error("No nodes were assigned"));
        }

        // Set global retry to true because scan/query may terminate early and all partitions
        // will need to be retried if the PartitionFilter instance is reused in a new scan/query.
        // Global retry will be set to false if the scan/query completes normally and max_records
        // is specified.
        self.shared
            .partition_filter
            .retry
            .store(true, Ordering::Relaxed);

        self.shared.record_count.store(0, Ordering::Relaxed);

        let max_records = self.shared.max_records.load(Ordering::Relaxed);
        if max_records > 0 {
            if max_records >= node_size as u64 {
                // Distribute max_records across nodes.
                let max = max_records / node_size as u64;
                let rem = max_records - (max * node_size as u64);

                for (i, np) in list.iter_mut().enumerate() {
                    if (i as u64) < rem {
                        np.record_max = max + 1;
                    } else {
                        np.record_max = max;
                    }
                }
            } else {
                // If max_records < nodeSize, the retry = true, ensure each node receives at least one max record
                // allocation and filter out excess records when receiving records from the server.
                for np in &mut list {
                    np.record_max = 1;
                }

                // Track records returned for this iteration.
                self.shared.record_count.store(0, Ordering::Relaxed);
            }
        }

        self.node_partitions_list = list;
        Ok(())
    }

    fn find_node<'a>(
        list: &'a mut Vec<NodePartitions>,
        node: &Arc<Node>,
    ) -> Option<&'a mut NodePartitions> {
        list.iter_mut().find(|np| np.node == *node)
    }

    pub(crate) fn is_complete(
        &mut self,
        policy: impl StreamPolicy,
        timed_out: bool,
        faulted: bool,
    ) -> Result<bool> {
        let mut record_count: u64 = 0;
        let mut parts_unavailable = 0;

        for np in &self.node_partitions_list {
            record_count += np.record_count;
            parts_unavailable += np.parts_unavailable;
        }

        let max_records = self.shared.max_records.load(Ordering::Relaxed);

        // A round where a node stream failed or was cancelled must not
        // declare the scan done: its partitions still hold undelivered
        // records, and the cursor's `done` flag is a promise that a resume
        // has nothing left to fetch.
        if !timed_out && !faulted && parts_unavailable == 0 {
            let pf = &self.shared.partition_filter;
            if max_records == 0 {
                pf.retry.store(false, Ordering::Relaxed);
                pf.done.store(true, Ordering::Relaxed);
            } else if self.iteration > 1 {
                // If errors occurred on a node, only that node's partitions are retried in the
                // next iteration. If that node finally succeeds, the other original nodes still
                // need to be retried if partition state is reused in the next scan/query command.
                // Force retry on all node partitions.
                pf.retry.store(true, Ordering::Relaxed);
                pf.done.store(false, Ordering::Relaxed);
            } else {
                // Server version >= 6.0 will return all records for each node up to
                // that node's max. If node's record count reached max, there still
                // may be records available for that node.
                let mut done = true;

                for np in &self.node_partitions_list {
                    if np.record_count + np.disallowed_count >= np.record_max {
                        Self::mark_retry(np);
                        done = false;
                    }
                }

                pf.retry.store(false, Ordering::Relaxed);
                pf.done.store(done, Ordering::Relaxed);
            }
            return Ok(true);
        }

        if max_records > 0 && record_count >= max_records {
            return Ok(true);
        }

        // Check if limits have been reached.
        if policy.max_retries() > 0 && self.iteration > policy.max_retries() {
            return Err(Error::client_error(format!(
                "Max retries exceeded: {}",
                policy.max_retries()
            )));
        }

        if let Some(deadline) = self.deadline {
            // Check for total timeout.
            if Instant::now()
                + self
                    .sleep_between_retries
                    .unwrap_or(Duration::from_millis(0))
                > deadline
            {
                return Err(Error::timeout("Scan/Query timed out"));
            }

            let total_timeout = u64::from(policy.total_timeout());
            if deadline < Instant::now() + Duration::from_millis(total_timeout) {
                let remaining = (deadline - Instant::now()).as_millis() as u32;
                self.shared.total_timeout.store(remaining, Ordering::Relaxed);

                if self.shared.socket_timeout.load(Ordering::Relaxed) > remaining {
                    self.shared.socket_timeout.store(remaining, Ordering::Relaxed);
                }
            }
        }

        // Prepare for next iteration.
        if max_records > 0 {
            self.shared
                .max_records
                .store(max_records - record_count, Ordering::Relaxed);
        }

        self.iteration += 1;
        Ok(false)
    }

    fn mark_retry(node_partitions: &NodePartitions) {
        // Mark retry for same replica.
        for &index in &node_partitions.parts_full {
            node_partitions.status(index).retry = true;
        }

        for &index in &node_partitions.parts_partial {
            node_partitions.status(index).retry = true;
        }
    }

    pub(crate) fn partition_error(&self) {
        // Mark all partitions for retry on fatal errors.
        self.shared
            .partition_filter
            .retry
            .store(true, Ordering::Relaxed);
    }

    /// Builds the partition status array: one allocation for the whole range,
    /// shared by `Arc` and indexed by offset from `partition_begin`.
    pub(crate) fn init_partitions(
        partition_begin: usize,
        partition_count: usize,
        digest: Option<[u8; 20]>,
    ) -> Arc<Vec<PartMutex<PartitionStatus>>> {
        let mut parts_all = Vec::with_capacity(partition_count);

        for i in 0..partition_count {
            let mut part = PartitionStatus::new(partition_begin + i);
            if i == 0 && digest.is_some() {
                part.set_digest(digest);
            }
            parts_all.push(PartMutex::new(part));
        }

        Arc::new(parts_all)
    }
}
