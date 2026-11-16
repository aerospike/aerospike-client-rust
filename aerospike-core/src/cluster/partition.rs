// Copyright 2015-2026 Aerospike, Inc.
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

use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};

use crate::cluster::node;
use crate::cluster::{Cluster, Node, PartitionTable, Partitions};
use crate::errors::{Error, Result};
use crate::policy::{ReadModeSc, Replica};
use crate::Key;

/// Partition encapsulates partition information used for node selection.
/// Carries `replica`, `sequence`, and `linearize` fields for managing
/// retry behavior and strong-consistency mode.
#[derive(Debug, Clone)]
pub struct Partition<'a> {
    pub namespace: &'a str,
    pub partition_id: usize,
    pub replica: Replica,
    pub sequence: usize,
    pub linearize: bool,
    pub is_write: bool,
    /// Node the previous attempt of this command selected. `PreferRack`
    /// routing deprioritizes it on retry — a same-rack node that just
    /// failed is worse than a different-rack node (Java `Partition.prevNode`).
    pub prev_node: Option<Arc<Node>>,
}

impl<'a> Partition<'a> {
    pub const fn new(namespace: &'a str, partition_id: usize) -> Self {
        Partition {
            namespace,
            partition_id,
            replica: Replica::Master,
            sequence: 0,
            linearize: false,
            is_write: false,
            prev_node: None,
        }
    }

    fn new_by_key(key: &'a Key) -> Self {
        let mut rdr = Cursor::new(&key.digest[0..2]);
        Partition {
            namespace: &key.namespace,
            // CAN'T USE MOD directly - mod will give negative numbers.
            // First AND makes positive and negative correctly, then mod.
            // For any x, y : x % 2^y = x & (2^y - 1); the second method is twice as fast
            partition_id: rdr.read_u16::<LittleEndian>().unwrap() as usize & (node::PARTITIONS - 1),
            replica: Replica::Master,
            sequence: 0,
            linearize: false,
            is_write: false,
            prev_node: None,
        }
    }

    /// Create a partition for write operations. `Sequence` and `PreferRack`
    /// walk the replicas on retry; the other modes always target the master.
    pub fn for_write(key: &'a Key, replica: Replica) -> Self {
        let mut p = Self::new_by_key(key);
        p.replica = replica;
        p.is_write = true;
        p
    }

    /// Create a partition for read operations, applying SC mode overrides
    /// based on the namespace's SC mode and the policy's `ReadModeSc` setting.
    pub fn for_read(
        cluster: &Cluster,
        key: &'a Key,
        replica: Replica,
        read_mode_sc: ReadModeSc,
    ) -> Self {
        let mut p = Self::new_by_key(key);
        p.replica = replica;

        // Check SC mode and apply overrides
        let pmap = cluster.partition_map.load();
        if let Some(partitions) = pmap.get(&key.namespace) {
            if partitions.sc_mode {
                match read_mode_sc {
                    ReadModeSc::Session => {
                        p.replica = Replica::Master;
                        p.linearize = false;
                    }
                    ReadModeSc::Linearize => {
                        p.replica = if replica == Replica::PreferRack {
                            Replica::Sequence
                        } else {
                            replica
                        };
                        p.linearize = true;
                    }
                    _ => {
                        p.replica = replica;
                        p.linearize = false;
                    }
                }
            }
        }

        p
    }

    /// Get the appropriate node for this partition, dispatching to
    /// read or write node selection based on `is_write`.
    pub fn get_node(&mut self, cluster: &Cluster) -> Result<Arc<Node>> {
        if self.is_write {
            self.get_node_write(cluster)
        } else {
            self.get_node_read(cluster)
        }
    }

    /// The node for this partition, selected from an already looked-up
    /// namespace table. For a scan or query that assigns all 4096 partitions
    /// in one loop, this is what makes the loop cheap: the map is loaded and
    /// the namespace hashed once by the caller, and the node comes back
    /// borrowed from the table rather than cloned, so the per-partition cost
    /// is an index and an `is_active` load, with no atomic on a counter every
    /// in-flight query shares. `Replica::Random` has no table to borrow from
    /// and returns an owned node.
    pub(crate) fn get_node_in<'t>(
        &mut self,
        cluster: &Cluster,
        table: &'t Partitions,
    ) -> Result<NodeRef<'t>> {
        let node = if self.is_write {
            match self.replica {
                Replica::Sequence | Replica::PreferRack => self.sequence_node_in(table)?,
                Replica::Master | Replica::MasterProles | Replica::Random => {
                    self.master_node_in(table)?
                }
            }
        } else {
            match self.replica {
                Replica::Sequence => self.sequence_node_in(table)?,
                Replica::PreferRack => self.rack_node_in(cluster, table)?,
                Replica::Master => self.master_node_in(table)?,
                Replica::MasterProles => self.master_proles_node_in(cluster, table)?,
                Replica::Random => return cluster.get_random_node().map(NodeRef::Owned),
            }
        };

        Ok(NodeRef::Borrowed(node))
    }

    /// This partition's namespace in a partition-map snapshot.
    pub(crate) fn table<'p>(&self, pmap: &'p PartitionTable) -> Result<&'p Partitions> {
        pmap.get(self.namespace)
            .ok_or_else(|| invalid_namespace_error(self.namespace, pmap.len()))
    }

    /// Prepare the partition for a retry attempt by advancing the sequence number.
    /// Dispatches to read or write retry logic based on `is_write`.
    pub const fn prepare_retry(&mut self, is_client_timeout: bool) {
        if self.is_write {
            // Write retries: advance sequence unless client timeout
            if !is_client_timeout {
                self.sequence += 1;
            }
        } else {
            // Read retries: advance sequence unless client timeout AND linearize
            if !is_client_timeout || !self.linearize {
                self.sequence += 1;
            }
        }
    }

    fn get_node_read(&mut self, cluster: &Cluster) -> Result<Arc<Node>> {
        match self.replica {
            Replica::Sequence => self.get_sequence_node(cluster),
            Replica::PreferRack => self.get_rack_node(cluster),
            Replica::Master => self.get_master_node(cluster),
            Replica::MasterProles => self.get_master_proles_node(cluster),
            Replica::Random => cluster.get_random_node(),
        }
    }

    fn get_node_write(&mut self, cluster: &Cluster) -> Result<Arc<Node>> {
        match self.replica {
            Replica::Sequence | Replica::PreferRack => self.get_sequence_node(cluster),
            Replica::Master | Replica::MasterProles | Replica::Random => {
                self.get_master_node(cluster)
            }
        }
    }

    fn get_sequence_node(&mut self, cluster: &Cluster) -> Result<Arc<Node>> {
        let pmap = cluster.partition_map.load();
        let table = self.table(&pmap)?;
        self.sequence_node_in(table).map(Arc::clone)
    }

    fn sequence_node_in<'t>(&mut self, partitions: &'t Partitions) -> Result<&'t Arc<Node>> {
        let replica_count = partitions.replicas;
        if replica_count == 0 {
            return Err(invalid_node_error(self));
        }

        for _ in 0..replica_count {
            let index = self.sequence % replica_count;

            if let Some((_, Some(node))) =
                partitions.nodes.get(index * node::PARTITIONS + self.partition_id)
            {
                if node.is_active() {
                    return Ok(node);
                }
            }
            self.sequence += 1;
        }

        Err(invalid_node_error(self))
    }

    fn get_rack_node(&mut self, cluster: &Cluster) -> Result<Arc<Node>> {
        let pmap = cluster.partition_map.load();
        let table = self.table(&pmap)?;
        self.rack_node_in(cluster, table).map(Arc::clone)
    }

    fn rack_node_in<'t>(
        &mut self,
        cluster: &Cluster,
        partitions: &'t Partitions,
    ) -> Result<&'t Arc<Node>> {
        let replica_count = partitions.replicas;
        if replica_count == 0 {
            return Err(invalid_node_error(self));
        }

        // An empty list is treated as "not configured" rather than as a rack
        // set that matches nothing: the loop below would run zero times, record
        // no fallback, and fail node selection with an error the retry loop
        // reports as a timeout. `ClientPolicy::validate` refuses `Some([])`, so
        // this only catches a live policy that a dynamic-config reload emptied.
        let policy = cluster.client_policy.load();
        let rack_ids = if policy.rack_aware() {
            policy.rack_ids.as_deref()
        } else {
            None
        };
        let rack_ids = rack_ids.ok_or_else(|| {
            Error::invalid_argument(
                "Attempted to use Replica::PreferRack without configuring racks in client policy"
                    .to_string(),
            )
        })?;

        // Java Partition.getRackNode: racks are tried in preference order,
        // and the node the previous attempt failed on is deprioritized —
        // preferred-rack match first, then any other active node
        // (fallback1), and the previous node only as a last resort
        // (fallback2).
        let mut fallback1: Option<(&'t Arc<Node>, usize)> = None;
        let mut fallback2: Option<(&'t Arc<Node>, usize)> = None;

        for rack_id in rack_ids {
            for seq in self.sequence..self.sequence + replica_count {
                let index = seq % replica_count;

                if let Some((_, Some(node))) =
                    partitions.nodes.get(index * node::PARTITIONS + self.partition_id)
                {
                    // Avoid retrying on the node where the command failed,
                    // even if it is the only one on the preferred rack.
                    let is_prev = self
                        .prev_node
                        .as_ref()
                        .is_some_and(|prev| Arc::ptr_eq(prev, node));

                    if !is_prev {
                        if node.has_rack(self.namespace, *rack_id) {
                            if node.is_active() {
                                self.prev_node = Some(Arc::clone(node));
                                self.sequence = seq;
                                return Ok(node);
                            }
                        } else if fallback1.is_none() && node.is_active() {
                            // Meets all criteria except not on the same rack.
                            fallback1 = Some((node, seq));
                        }
                    } else if fallback2.is_none() && node.is_active() {
                        // The previous node is the least desirable fallback.
                        fallback2 = Some((node, seq));
                    }
                }
            }
        }

        // Prefer a node on a different rack over the previously failed node.
        if let Some((node, seq)) = fallback1.or(fallback2) {
            self.prev_node = Some(Arc::clone(node));
            self.sequence = seq;
            return Ok(node);
        }

        Err(invalid_node_error(self))
    }

    fn get_master_proles_node(&self, cluster: &Cluster) -> Result<Arc<Node>> {
        let pmap = cluster.partition_map.load();
        let table = self.table(&pmap)?;
        self.master_proles_node_in(cluster, table).map(Arc::clone)
    }

    fn master_proles_node_in<'t>(
        &self,
        cluster: &Cluster,
        partitions: &'t Partitions,
    ) -> Result<&'t Arc<Node>> {
        let replica_count = partitions.replicas;
        if replica_count == 0 {
            return Err(invalid_node_error(self));
        }

        for _ in 0..replica_count {
            let index = (cluster
                .replica_index
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1)
            .unsigned_abs()
                % replica_count;

            if let Some((_, Some(node))) =
                partitions.nodes.get(index * node::PARTITIONS + self.partition_id)
            {
                if node.is_active() {
                    return Ok(node);
                }
            }
        }

        Err(invalid_node_error(self))
    }

    pub fn get_master_node(&self, cluster: &Cluster) -> Result<Arc<Node>> {
        let pmap = cluster.partition_map.load();
        let table = self.table(&pmap)?;
        self.master_node_in(table).map(Arc::clone)
    }

    fn master_node_in<'t>(&self, partitions: &'t Partitions) -> Result<&'t Arc<Node>> {
        if let Some((_, Some(node))) = partitions.nodes.get(self.partition_id) {
            if node.is_active() {
                return Ok(node);
            }
        }

        Err(invalid_node_error(self))
    }
}

/// A selected node: borrowed from the partition table the caller holds, or
/// owned where there is no table to borrow from (`Replica::Random`).
pub enum NodeRef<'t> {
    Borrowed(&'t Arc<Node>),
    Owned(Arc<Node>),
}

impl NodeRef<'_> {
    pub const fn as_arc(&self) -> &Arc<Node> {
        match self {
            NodeRef::Borrowed(node) => node,
            NodeRef::Owned(node) => node,
        }
    }
}

impl PartialEq for Partition<'_> {
    fn eq(&self, other: &Partition) -> bool {
        self.namespace == other.namespace && self.partition_id == other.partition_id
    }
}

impl fmt::Display for Partition<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        write!(f, "Partition ({}: {})", self.namespace, self.partition_id)
    }
}

fn invalid_namespace_error(namespace: &str, map_size: usize) -> Error {
    if map_size == 0 {
        Error::invalid_namespace("Partition map empty".to_string())
    } else {
        Error::invalid_namespace(format!("Namespace not found in partition map: {namespace}"))
    }
}

fn invalid_node_error(partition: &Partition) -> Error {
    Error::invalid_node(format!(
        "Cannot get appropriate node for namespace: {} partition: {}",
        partition.namespace, partition.partition_id
    ))
}
