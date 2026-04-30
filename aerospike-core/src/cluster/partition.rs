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

use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};

use crate::cluster::node;
use crate::cluster::{Cluster, Node};
use crate::errors::{Error, Result};
use crate::policy::{ReadModeSC, Replica};
use crate::Key;

/// Partition encapsulates partition information used for node selection.
/// Equivalent to the Go client's `Partition` struct with `replica`, `sequence`,
/// and `linearize` fields for managing retry behavior and SC mode.
#[derive(Debug, Clone)]
pub struct Partition<'a> {
    pub namespace: &'a str,
    pub partition_id: usize,
    pub replica: Replica,
    pub sequence: usize,
    pub linearize: bool,
    pub is_write: bool,
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
        }
    }

    /// Create a partition for write operations.
    pub fn for_write(key: &'a Key) -> Self {
        let mut p = Self::new_by_key(key);
        p.replica = Replica::Master;
        p.is_write = true;
        p
    }

    /// Create a partition for read operations, applying SC mode overrides
    /// based on the namespace's SC mode and the policy's `ReadModeSC` setting.
    pub fn for_read(
        cluster: &Cluster,
        key: &'a Key,
        replica: Replica,
        read_mode_sc: ReadModeSC,
    ) -> Self {
        let mut p = Self::new_by_key(key);
        p.replica = replica;

        // Check SC mode and apply overrides
        let pmap = cluster.partition_map.load();
        if let Some(partitions) = pmap.get(&key.namespace) {
            if partitions.sc_mode {
                match read_mode_sc {
                    ReadModeSC::Session => {
                        p.replica = Replica::Master;
                        p.linearize = false;
                    }
                    ReadModeSC::Linearize => {
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

    /// Get the replica policy for SC (strong consistency) mode.
    pub fn get_replica_policy_sc(replica: Replica, read_mode_sc: ReadModeSC) -> Replica {
        match read_mode_sc {
            ReadModeSC::Session => Replica::Master,
            ReadModeSC::Linearize => {
                if replica == Replica::PreferRack {
                    Replica::Sequence
                } else {
                    replica
                }
            }
            _ => replica,
        }
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
        let partitions = pmap
            .get(self.namespace)
            .ok_or_else(|| invalid_namespace_error(self.namespace, pmap.len()))?;

        let replica_count = partitions.replicas;
        if replica_count == 0 {
            return Err(invalid_node_error(self));
        }

        for _ in 0..replica_count {
            let index = self.sequence % replica_count;
            let node = partitions
                .nodes
                .get(index * node::PARTITIONS + self.partition_id)
                .and_then(|(_, n)| n.clone());

            if let Some(ref node) = node {
                if node.is_active() {
                    return Ok(node.clone());
                }
            }
            self.sequence += 1;
        }

        Err(invalid_node_error(self))
    }

    fn get_rack_node(&mut self, cluster: &Cluster) -> Result<Arc<Node>> {
        let pmap = cluster.partition_map.load();
        let partitions = pmap
            .get(self.namespace)
            .ok_or_else(|| invalid_namespace_error(self.namespace, pmap.len()))?;

        let replica_count = partitions.replicas;
        if replica_count == 0 {
            return Err(invalid_node_error(self));
        }

        let rack_ids = cluster.client_policy.load().rack_ids.clone();
        let rack_ids = rack_ids.as_ref().ok_or_else(|| {
            Error::InvalidArgument(
                "Attempted to use Replica::PreferRack without configuring racks in client policy"
                    .to_string(),
            )
        })?;

        let mut fallback = None;

        for _ in 0..replica_count {
            let index = self.sequence % replica_count;
            let node = partitions
                .nodes
                .get(index * node::PARTITIONS + self.partition_id)
                .and_then(|(_, n)| n.clone());

            if let Some(ref node) = node {
                if node.is_active() {
                    if node.is_in_rack(self.namespace, rack_ids) {
                        return Ok(node.clone());
                    }
                    if fallback.is_none() {
                        fallback = Some(node.clone());
                    }
                }
            }
            self.sequence += 1;
        }

        if let Some(fallback) = fallback {
            return Ok(fallback);
        }

        Err(invalid_node_error(self))
    }

    fn get_master_proles_node(&self, cluster: &Cluster) -> Result<Arc<Node>> {
        let pmap = cluster.partition_map.load();
        let partitions = pmap
            .get(self.namespace)
            .ok_or_else(|| invalid_namespace_error(self.namespace, pmap.len()))?;

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
            let node = partitions
                .nodes
                .get(index * node::PARTITIONS + self.partition_id)
                .and_then(|(_, n)| n.clone());

            if let Some(ref node) = node {
                if node.is_active() {
                    return Ok(node.clone());
                }
            }
        }

        Err(invalid_node_error(self))
    }

    pub fn get_master_node(&self, cluster: &Cluster) -> Result<Arc<Node>> {
        let pmap = cluster.partition_map.load();
        let partitions = pmap
            .get(self.namespace)
            .ok_or_else(|| invalid_namespace_error(self.namespace, pmap.len()))?;

        let node = partitions
            .nodes
            .get(self.partition_id)
            .and_then(|(_, n)| n.clone());

        if let Some(ref node) = node {
            if node.is_active() {
                return Ok(node.clone());
            }
        }

        Err(invalid_node_error(self))
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
        Error::InvalidNamespace("Partition map empty".to_string())
    } else {
        Error::InvalidNamespace(format!("Namespace not found in partition map: {namespace}"))
    }
}

fn invalid_node_error(partition: &Partition) -> Error {
    Error::InvalidNode(format!(
        "Cannot get appropriate node for namespace: {} partition: {}",
        partition.namespace, partition.partition_id
    ))
}
