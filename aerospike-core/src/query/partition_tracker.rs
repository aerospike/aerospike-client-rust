// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache Licenseersion 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use crate::cluster::node;
use crate::cluster::Cluster;
use crate::errors::{Error, Result};
use crate::policy::Replica;
use crate::policy::StreamPolicy;
use crate::query::NodePartitions;
use crate::query::PartitionFilter;
use crate::query::PartitionStatus;
use crate::Key;
use crate::Node;

use aerospike_rt::{
    time::{Duration, Instant},
    Mutex,
};

use std::cmp::max;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[derive(Debug)]
pub(crate) struct PartitionTracker {
    partitions_capacity: usize,
    partition_begin: usize,
    node_capacity: usize,
    node_filter: Option<Arc<Node>>,
    partition_filter: Option<Arc<Mutex<PartitionFilter>>>,
    #[allow(dead_code)]
    replica: Replica,
    node_partitions_list: Vec<Arc<Mutex<NodePartitions>>>,
    record_count: AtomicUsize,
    max_records: u64,
    sleep_between_retries: Option<Duration>,
    socket_timeout: Option<Duration>,
    total_timeout: Option<Duration>,
    iteration: AtomicUsize, //= 1
    deadline: Instant,
}

impl PartitionTracker {
    pub(crate) async fn new(
        policy: impl StreamPolicy,
        partition_filter: Arc<Mutex<PartitionFilter>>,
        nodes: Vec<Arc<Node>>,
    ) -> Result<Self> {
        let mut pt = {
            let mut partition_filter = partition_filter.lock().await;

            // Validate here instead of initial PartitionFilter constructor because total number of
            // cluster partitions may change on the server and PartitionFilter will never have access
            // to Cluster instance.  Use fixed number of partitions for now.
            if !(usize::from(partition_filter.begin) < node::PARTITIONS) {
                return Err(Error::InvalidArgument(format!(
                    "Invalid partition begin {} . Valid range: 0-{}",
                    partition_filter.begin,
                    node::PARTITIONS - 1
                )));
            }

            if partition_filter.count <= 0 {
                return Err(Error::InvalidArgument(format!(
                    "Invalid partition count {}",
                    partition_filter.count
                )));
            }

            if usize::from(partition_filter.begin + partition_filter.count) > node::PARTITIONS {
                return Err(Error::InvalidArgument(format!(
                    "Invalid partition range ({},{})",
                    partition_filter.begin,
                    partition_filter.begin + partition_filter.count
                )));
            }

            // This is required for proxy server since there are no nodes represented there
            let node_capacity = max(1, nodes.len());
            let pt = PartitionTracker {
                // partitions: None,
                partitions_capacity: partition_filter.count,
                partition_begin: partition_filter.begin,
                node_capacity: node_capacity,
                node_filter: None,
                partition_filter: None,
                replica: policy.replica(),
                node_partitions_list: vec![],
                record_count: AtomicUsize::new(0),
                max_records: policy.max_records().unwrap_or(0),
                sleep_between_retries: None,
                socket_timeout: None,
                total_timeout: None,
                iteration: AtomicUsize::new(1),
                deadline: Instant::now(),
            };

            if partition_filter.partitions.is_none() {
                let begin = partition_filter.begin;
                let count = partition_filter.count;
                let digest = partition_filter.digest;
                partition_filter
                    .set_partitions(PartitionTracker::init_partitions(begin, count, digest));
                partition_filter.retry.store(true, Ordering::Relaxed);
            } else {
                // retry all partitions when max_records not specified.
                if policy.max_records().is_none() {
                    partition_filter.retry.store(true, Ordering::Relaxed);
                }

                partition_filter.reset_partition_status().await;
            }
            pt
        };

        pt.partition_filter = Some(partition_filter);
        pt.init(policy);
        Ok(pt)
    }

    // pub(crate) fn set_sleep_between_retries(&mut self, duration: Option<Duration>) {
    //     self.sleep_between_retries = duration;
    // }

    pub(crate) async fn assign_partitions_to_nodes(
        &mut self,
        cluster: Arc<Cluster>,
        namespace: &str,
    ) -> Result<Vec<Arc<Mutex<NodePartitions>>>> {
        let mut list = Vec::<Arc<Mutex<NodePartitions>>>::with_capacity(self.node_capacity);

        let retry = self.partition_filter.is_none()
            || self
                .partition_filter
                .as_ref()
                .unwrap()
                .lock()
                .await
                .retry
                .load(Ordering::Relaxed)
                && (*self.iteration.get_mut() == 1);

        let partition_filter = self.partition_filter.as_mut().unwrap().lock().await;
        let partitions = partition_filter.partitions.as_ref().unwrap();
        for part in partitions.iter() {
            let (part_retry, part_id) = {
                let part = part.lock().await;
                (part.retry, part.id)
            };
            if retry || part_retry {
                let node = cluster.get_master_node(namespace, part_id as usize).await?;

                // Use node name to check for single node equality because
                // partition map may be in transitional state between
                // the old and new node with the same name.
                if let Some(node_filter) = self.node_filter.as_ref() {
                    if node_filter.name() != node.name() {
                        continue;
                    }
                }

                let np = Self::find_node(&list, node.clone()).await;
                match np {
                    Some(np) => {
                        let mut np = np.lock().await;
                        np.add_partition(part.clone()).await;
                    }
                    None => {
                        // If the partition map is in a transitional state, multiple
                        // nodePartitions instances (each with different partitions)
                        // may be created for a single node.
                        let mut np = NodePartitions::new(node.clone(), self.partitions_capacity);
                        np.add_partition(part.clone()).await;
                        list.push(Arc::new(Mutex::new(np)));
                    }
                }
            }
        }

        let node_size = list.len();
        if node_size <= 0 {
            return Err(Error::ClientError("No nodes were assigned".into()));
        }

        // Set global retry to true because scan/query may terminate early and all partitions
        // will need to be retried if the PartitionFilter instance is reused in a new scan/query.
        // Global retry will be set to false if the scan/query completes normally and max_records
        // is specified.
        partition_filter.retry.store(true, Ordering::Relaxed);

        self.record_count.store(0, Ordering::Relaxed);

        if self.max_records > 0 {
            if self.max_records >= node_size as u64 {
                // Distribute max_records across nodes.
                let max = self.max_records / node_size as u64;
                let rem = self.max_records - (max * node_size as u64);

                for (i, np) in list.iter().enumerate() {
                    let mut np = np.lock().await;
                    if (i as u64) < rem {
                        np.record_max = max + 1;
                    } else {
                        np.record_max = max;
                    }
                }
            } else {
                // If max_records < nodeSize, the retry = true, ensure each node receives at least one max record
                // allocation and filter out excess records when receiving records from the server.
                for np in &list {
                    let mut np = np.lock().await;
                    np.record_max = 1;
                }

                // Track records returned for this iteration.
                self.record_count.store(0, Ordering::Relaxed);
            }
        }

        // let list = Arc::new(Mutex::new(list));
        self.node_partitions_list = list.clone();
        return Ok(list);
    }

    fn init(&mut self, policy: impl StreamPolicy) {
        self.sleep_between_retries = policy.sleep_between_retries();
        self.socket_timeout = policy.socket_timeout();
        self.total_timeout = policy.total_timeout();

        if let Some(total_timeout) = self.total_timeout {
            if !total_timeout.is_zero() {
                self.deadline = Instant::now() + total_timeout;
                if let Some(socket_timeout) = self.socket_timeout {
                    if socket_timeout.is_zero() || socket_timeout > total_timeout {
                        self.socket_timeout = Some(total_timeout);
                    }
                }
            }
        }

        // if self.replica == RANDOM {
        //     panic(newError(types.PARAMETER_ERROR, "Invalid replica: RANDOM"))
        // }
    }

    pub(crate) async fn find_node(
        list: &Vec<Arc<Mutex<NodePartitions>>>,
        node: Arc<Node>,
    ) -> Option<Arc<Mutex<NodePartitions>>> {
        for node_partition in list {
            // Use pointer equality for performance.
            if node_partition.lock().await.node == node {
                return Some(node_partition.clone());
            }
        }
        return None;
    }

    pub(crate) async fn partition_unavailable(
        &mut self,
        node_partitions: &mut NodePartitions,
        partition_id: u16,
    ) {
        let mut pf = self.partition_filter.as_mut().unwrap().lock().await;
        let partitions = pf.partitions.as_mut().unwrap();

        // let mut partitions = partitions.write();
        if let Some(ps) = partitions.get_mut(partition_id as usize - self.partition_begin) {
            let mut ps = ps.lock().await;
            ps.retry = true;
            if let Some(ref mut seq) = ps.sequence {
                *seq += 1;
            };
        }
        node_partitions.parts_unavailable += 1;
    }

    pub(crate) async fn set_digest(
        &mut self,
        node_partitions: &mut NodePartitions,
        key: &Key,
    ) -> Result<()> {
        let mut pf = self.partition_filter.as_mut().unwrap().lock().await;
        let partitions = pf.partitions.as_mut();

        let partition_id = key.partition_id();
        if let Some(partitions) = partitions {
            // let mut partitions = partitions.write();
            if let Some(ps) = partitions.get_mut(partition_id as usize - self.partition_begin) {
                let mut ps = ps.lock().await;
                ps.digest = Some(key.digest);
            } else {
                return Err(Error::ClientError(format!(
                    "Partition mismatch: key.partition_id: {}, partition_begin: {}",
                    partition_id, self.partition_begin
                )));
            }
        }

        node_partitions.record_count += 1;
        Ok(())
    }

    pub(crate) async fn set_last(
        &mut self,
        node_partitions: &mut NodePartitions,
        key: &Key,
        bval: Option<u64>,
    ) -> Result<()> {
        let partition_id = key.partition_id();
        if (partition_id as i64) - (self.partition_begin as i64) < 0 {
            return Err(Error::ClientError(format!(
                "Partition mismatch: key.partition_id: {}, partition_begin: {}",
                partition_id, self.partition_begin
            )));
        }

        let mut pf = self.partition_filter.as_mut().unwrap().lock().await;
        let partitions = pf.partitions.as_mut();

        if let Some(partitions) = partitions {
            if let Some(ps) = partitions.get_mut(partition_id as usize - self.partition_begin) {
                let mut ps = ps.lock().await;
                ps.digest = Some(key.digest);
                if bval.is_some() {
                    ps.bval = bval;
                }
            }
        }

        node_partitions.record_count += 1;
        Ok(())
    }

    pub(crate) fn allow_record(&self, np: &mut NodePartitions) -> bool {
        if self.max_records == 0 {
            return true;
        };

        let record_count = self.record_count.fetch_add(1, Ordering::SeqCst) + 1;
        if self.max_records > 0 {
            if record_count as u64 <= self.max_records {
                return true;
            }
        }

        // Record was returned, but would exceed max_records.
        // Discard record and increment disallowed_count.
        np.disallowed_count += 1;
        return false;
    }

    pub(crate) async fn is_cluster_complete(&mut self, policy: impl StreamPolicy) -> Result<bool> {
        return self
            .is_complete(policy, self.node_partitions_list.clone())
            .await;
    }

    pub(crate) async fn is_complete(
        &mut self,
        policy: impl StreamPolicy,
        mut node_partitions_list: Vec<Arc<Mutex<NodePartitions>>>,
    ) -> Result<bool> {
        let mut record_count: u64 = 0;
        let mut parts_unavailable = 0;

        for np in node_partitions_list.iter() {
            let np = np.lock().await;
            record_count += np.record_count;
            parts_unavailable += np.parts_unavailable;
        }

        if parts_unavailable == 0 {
            if self.max_records <= 0 {
                if let Some(pf) = &self.partition_filter {
                    let pf = pf.lock().await;
                    pf.retry.store(false, Ordering::Relaxed);
                    pf.done.store(true, Ordering::Relaxed);
                }
            } else if self.iteration.load(Ordering::Relaxed) > 1 {
                if let Some(pf) = &self.partition_filter {
                    // If errors occurred on a node, only that node's partitions are retried in the
                    // next iteration. If that node finally succeeds, the other original nodes still
                    // need to be retried if partition state is reused in the next scan/query command.
                    // Force retry on all node partitions.
                    let pf = pf.lock().await;
                    pf.retry.store(true, Ordering::Relaxed);
                    pf.done.store(false, Ordering::Relaxed);
                }
            } else {
                // Server version >= 6.0 will return all records for each node up to
                // that node's max. If node's record count reached max, there still
                // may be records available for that node.
                let mut done = true;

                for np in node_partitions_list.iter_mut() {
                    let mut np = np.lock().await;
                    if np.record_count + np.disallowed_count >= np.record_max {
                        self.mark_retry(&mut np).await;
                        done = false;
                    }
                }

                if let Some(pf) = &self.partition_filter {
                    let pf = pf.lock().await;
                    pf.retry.store(false, Ordering::Relaxed);
                    pf.done.store(done, Ordering::Relaxed);
                }
            }
            return Ok(true);
        }

        if self.max_records > 0 && record_count >= self.max_records {
            return Ok(true);
        }

        // Check if limits have been reached.
        if let Some(max_retries) = policy.max_retries() {
            if *self.iteration.get_mut() > max_retries {
                return Err(Error::ClientError(format!(
                    "Max retries exceeded: {}",
                    max_retries
                )));
            }
        }

        if let Some(total_timeout) = policy.total_timeout() {
            // Check for total timeout.
            let remaining = self.deadline
                - Instant::now()
                - self
                    .sleep_between_retries
                    .unwrap_or(Duration::from_micros(0));

            if remaining.is_zero() {
                return Err(Error::Timeout("Scan/Query timed out".into(), true));
            }

            if remaining < total_timeout {
                self.total_timeout = Some(remaining);

                if self.socket_timeout > self.total_timeout {
                    self.socket_timeout = self.total_timeout
                }
            }
        }

        // Prepare for next iteration.
        if self.max_records > 0 {
            self.max_records -= record_count
        }

        self.iteration.fetch_add(1, Ordering::Relaxed);
        return Ok(false);
    }

    pub(crate) async fn mark_retry(&mut self, node_partitions: &mut NodePartitions) {
        // Mark retry for same replica.
        for ps in &node_partitions.parts_full {
            let mut ps = ps.lock().await;
            ps.retry = true;
        }

        for ps in &node_partitions.parts_partial {
            let mut ps = ps.lock().await;
            ps.retry = true;
        }
    }

    pub(crate) async fn partition_error(&mut self) {
        // Mark all partitions for retry on fatal errors.
        match self.partition_filter {
            Some(ref mut pf) => {
                let pf = pf.lock().await;
                pf.retry.store(true, Ordering::Relaxed);
            }
            None => (),
        };
    }

    pub(crate) fn init_partitions(
        partition_begin: usize,
        partition_count: usize,
        digest: Option<[u8; 20]>,
    ) -> Vec<Arc<Mutex<PartitionStatus>>> {
        let mut parts_all = Vec::<Arc<Mutex<PartitionStatus>>>::with_capacity(partition_count);

        for i in 0..partition_count {
            let mut part = PartitionStatus::new(partition_begin + i);
            if i == 0 && digest.is_some() {
                part.set_digest(digest);
            }
            parts_all.push(Arc::new(Mutex::new(part)));
        }

        parts_all
    }

    pub(crate) fn extract_partition_filter(&mut self) -> Option<PartitionFilter> {
        let pf = self.partition_filter.take();
        match pf {
            Some(pf) => match Arc::try_unwrap(pf) {
                Ok(pf) => Some(pf.into_inner()),
                Err(_) => None,
            },
            None => None,
        }
    }
}
