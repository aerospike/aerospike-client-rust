// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

pub mod node;
pub mod node_validator;
pub mod partition;
pub mod partition_tokenizer;
pub mod peers_parser;
pub mod version_parser;

use aerospike_rt::time::{Duration, Instant};
use std::cell::OnceCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Arc;
use std::vec::Vec;

pub use self::node::Node;
pub use self::partition::Partition;

use self::node_validator::NodeValidator;
use self::partition_tokenizer::PartitionTokenizer;

use crate::commands::admin_command::AdminCommand;
use crate::commands::Message;
use crate::errors::{Error, Result};
use crate::net::Host;
use crate::policy::ClientPolicy;
use crate::AdminPolicy;
use aerospike_rt::Mutex;
use futures::channel::mpsc;
use futures::channel::mpsc::{Receiver, Sender};
use hazarc::AtomicArc;

static CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Per-namespace partition data, equivalent to Go's `Partitions` struct.
/// Contains replicated node arrays, SC mode flag, and regime tracking.
#[derive(Debug, Default, Clone)]
pub struct Partitions {
    pub(crate) nodes: Vec<(u32, Option<Arc<Node>>)>,
    pub(crate) replicas: usize,
    pub(crate) sc_mode: bool,
}

pub type PartitionTable = HashMap<String, Partitions>;

impl Partitions {}

// Cluster encapsulates the aerospike cluster nodes and manages
// them.
#[derive(Debug)]
pub struct Cluster {
    // Initial host nodes specified by user.
    seeds: AtomicArc<Vec<Host>>,

    // All aliases for all nodes in cluster.
    aliases: AtomicArc<HashMap<Host, Arc<Node>>>,

    // Active nodes in cluster.
    nodes: AtomicArc<Vec<Arc<Node>>>,

    // Which partition contains the key.
    pub(crate) partition_map: AtomicArc<PartitionTable>,

    // Random node index.
    node_index: AtomicIsize,

    // Round-robin replica index for MasterProles policy.
    pub(crate) replica_index: AtomicIsize,

    pub(crate) client_policy: AtomicArc<ClientPolicy>,
    hashed_pass: AtomicArc<Option<String>>,

    tend_channel: Mutex<Sender<()>>,
    closed: AtomicBool,
}

impl Cluster {
    pub async fn new(mut policy: ClientPolicy, hosts: &[Host]) -> Result<Arc<Self>> {
        // updated the hashed password
        let _ = policy.set_auth_mode(policy.auth_mode.clone());

        let (tx, rx) = mpsc::channel(100);
        let cluster = Arc::new(Cluster {
            hashed_pass: AtomicArc::from(policy.hashed_pass()),
            client_policy: AtomicArc::from(policy),

            seeds: AtomicArc::from(hosts.to_vec()),
            aliases: AtomicArc::from(HashMap::new()),
            nodes: AtomicArc::from(vec![]),

            partition_map: AtomicArc::from(HashMap::default()),
            node_index: AtomicIsize::new(0),
            replica_index: AtomicIsize::new(0),

            tend_channel: Mutex::new(tx),
            closed: AtomicBool::new(false),
        });
        // try to seed connections for first use
        Cluster::wait_till_stabilized(cluster.clone()).await?;

        // apply policy rules
        if cluster.client_policy.load().fail_if_not_connected && !cluster.is_connected() {
            return Err(Error::Connection(
                "Failed to connect to host(s). The network \
                 connection(s) to cluster nodes may have timed out, or \
                 the cluster may be in a state of flux."
                    .to_string(),
            ));
        }

        // Expand the seed list with every discovered node's primary host so
        // recovery still has reachable addresses if the originally-configured
        // seeds go offline. Mirrors Java `Cluster.initTendThread`:
        // iterate nodes, add any whose host isn't already in the seed list.
        let discovered: Vec<Host> = cluster.nodes().iter().map(|n| n.host()).collect();
        cluster.merge_seeds(&discovered);

        let cluster_for_tend = cluster.clone();
        let _res = aerospike_rt::spawn(Cluster::tend_thread(cluster_for_tend, rx));
        debug!("New cluster initialized and ready to be used...");
        Ok(cluster)
    }

    async fn tend_thread(cluster: Arc<Cluster>, mut rx: Receiver<()>) {
        let tend_interval = cluster.client_policy.load().tend_interval;

        loop {
            if rx.try_next().is_ok() {
                unreachable!();
            } else if let Err(err) = cluster.tend().await {
                log_error_chain!(err, "Error tending cluster");
            }
            aerospike_rt::sleep(Duration::from_millis(u64::from(tend_interval))).await;
        }

        // close all nodes
        //let nodes = cluster.nodes().await;
        //for mut node in nodes {
        //    if let Some(node) = Arc::get_mut(&mut node) {
        //        node.close().await;
        //    }
        //}
        //cluster.set_nodes(vec![]).await;
    }

    async fn tend(&self) -> Result<()> {
        let mut nodes = self.nodes();

        // All node additions/deletions are performed in tend thread.
        // If active nodes don't exist, seed cluster.
        if nodes.is_empty() {
            debug!("No connections available; seeding...");
            self.seed_nodes().await;
            nodes = self.nodes();
        }

        // Snapshot the cluster size for this tend. Used by the split-cluster
        // guard below — the guard only makes sense when the cluster already
        // believes it has multiple nodes.
        let cluster_size = nodes.len();

        let mut friend_list: Vec<Host> = vec![];
        let mut refresh_count = 0;

        let mut partition_map = OnceCell::new();

        // Refresh all known nodes.
        for node in nodes {
            // Reap idle connections, but keep enough of them alive via a
            // cheap info probe to stay at or above `min_conns_per_node` —
            // avoids the full TCP-connect round-trip that `fill_min_conns`
            // would otherwise pay to replace them. Uses `try_lock` per
            // queue so this never blocks in-flight callers; contended
            // pools are simply retried on the next tend.
            let processed = node.reap_and_refresh_idle_connections().await;
            if processed > 0 {
                debug!("Reap/refresh processed {processed} idle connections on {node}");
            }

            let old_gen = node.partition_generation();
            let old_rebalance_gen = node.rebalance_generation();
            if node.is_active() {
                match node.refresh(self.aliases()).await {
                    Ok(friends) => {
                        refresh_count += 1;

                        if !friends.is_empty() {
                            friend_list.extend_from_slice(&friends);
                        }

                        if old_gen != node.partition_generation() {
                            // Split-cluster guard: if the cluster currently
                            // believes it has multiple nodes but this node
                            // advertises zero peers after at least one prior
                            // refresh, it's likely isolated — don't let its
                            // partition view overwrite the shared map.
                            // A single-node cluster has cluster_size == 1 and
                            // is expected to report 0 peers, so the guard
                            // does not fire. Matches Java
                            // `Node.refreshPartitions`'s intent (peersCount == 0
                            // && refreshCount > 1) where refreshCount is per-tend
                            // and > 1 implies multiple peer-refreshes this tend.
                            let suspect_split = cluster_size > 1
                                && node.peers_count() == 0
                                && node.refresh_count() > 1;
                            if suspect_split {
                                debug!(
                                    "Skipping partition update for node {node}: reports 0 peers in {cluster_size}-node cluster (likely split)"
                                );
                            } else {
                                partition_map.get_or_init(|| {
                                    // this will clone the inner value
                                    (*self.partition_map.load().clone()).clone()
                                });
                                self.update_partitions(partition_map.get_mut().unwrap(), &node)
                                    .await?;
                            }
                        }

                        if old_rebalance_gen != node.rebalance_generation() {
                            self.update_rack_ids(&node).await?;
                        }
                    }
                    Err(err) => {
                        node.increase_failures();
                        warn!("Node `{node}` refresh failed: {err}");
                    }
                }
            }
        }

        // if partition map has changed, store the new updated one
        if let Some(partition_map) = partition_map.take() {
            self.partition_map.store(Arc::new(partition_map));
        }

        // Add nodes in a batch.
        let add_list = self.find_new_nodes_to_add(friend_list).await;
        self.add_nodes_and_aliases(&add_list);

        // IMPORTANT: Remove must come after add to remove aliases
        // Handle nodes changes determined from refreshes.
        // Remove nodes in a batch.
        let remove_list = self.find_nodes_to_remove(refresh_count).await;
        self.remove_nodes_and_aliases(remove_list);

        let aliases: Vec<String> = self
            .aliases
            .load()
            .values()
            .map(std::string::ToString::to_string)
            .collect();

        debug!("Nodes {aliases:?}");

        Ok(())
    }

    async fn wait_till_stabilized(cluster: Arc<Cluster>) -> Result<()> {
        let timeout = {
            let timeout = cluster.client_policy.load().timeout;
            if timeout > 0 {
                Duration::from_millis(u64::from(timeout))
            } else {
                Duration::from_secs(3)
            }
        };
        let deadline = Instant::now() + timeout;
        let sleep_between_tend = Duration::from_millis(1);

        let handle = aerospike_rt::spawn(async move {
            let mut count: isize = -1;
            loop {
                if Instant::now() > deadline {
                    break;
                }

                if let Err(err) = cluster.tend().await {
                    log_error_chain!(err, "Error during initial cluster tend");
                }

                let old_count = count;
                count = cluster.nodes().len() as isize;
                if count == old_count {
                    break;
                }

                aerospike_rt::sleep(sleep_between_tend).await;
            }
        });

        #[cfg(all(feature = "rt-tokio", not(feature = "rt-async-std")))]
        return handle.await.map_err(|err| {
            Error::InvalidArgument(format!("Error during initial cluster tend: {err:?}"))
        });
        #[cfg(all(feature = "rt-async-std", not(feature = "rt-tokio")))]
        return {
            handle.await;
            Ok(())
        };
    }

    pub fn cluster_name(&self) -> Option<String> {
        self.client_policy().cluster_name
    }

    pub fn client_policy(&self) -> ClientPolicy {
        (*self.client_policy.load().clone()).clone()
    }

    pub fn add_seeds(&self, new_seeds: &[Host]) {
        let mut seeds = self.seeds.load().to_vec();
        seeds.extend_from_slice(new_seeds);
        self.seeds.store(Arc::new(seeds));
    }

    /// Append only those hosts that aren't already in the seed list.
    /// Used after cluster stabilization to promote discovered nodes to
    /// fallback seeds without creating duplicates on repeated calls.
    pub fn merge_seeds(&self, new_seeds: &[Host]) {
        let mut seeds = self.seeds.load().to_vec();
        let mut changed = false;
        for host in new_seeds {
            if !seeds.iter().any(|s| s == host) {
                seeds.push(host.clone());
                changed = true;
            }
        }
        if changed {
            self.seeds.store(Arc::new(seeds));
        }
    }

    pub fn alias_exists(&self, host: &Host) -> bool {
        let aliases = self.aliases.load();
        aliases.contains_key(host)
    }

    pub fn node_partitions(&self, node: &Node, namespace: &str) -> Vec<u16> {
        let mut res: Vec<u16> = vec![];
        let partitions = self.partition_map.load();

        if let Some(node_array) = partitions.get(namespace) {
            for (i, (_, tnode)) in node_array.nodes.iter().enumerate().take(node::PARTITIONS) {
                if tnode.as_ref().is_some_and(|tnode| tnode.as_ref() == node) {
                    res.push(i as u16);
                }
            }
        }

        res
    }

    pub async fn update_partitions(
        &self,
        partition_map: &mut PartitionTable,
        node: &Arc<Node>,
    ) -> Result<()> {
        let mut conn = node.get_connection(0).await?;

        let admin_policy = AdminPolicy {
            timeout: self.client_policy.load().timeout,
        };
        let tokens = PartitionTokenizer::new(&admin_policy, &mut conn, node).await;
        if let Err(e) = tokens {
            conn.invalidate();
            return Err(e);
        }

        let tokens = tokens.unwrap();
        tokens.update_partition(partition_map, node)?;

        Ok(())
    }

    pub async fn update_rack_ids(&self, node: &Arc<Node>) -> Result<()> {
        const RACK_IDS: &str = "rack-ids";
        let mut conn = node.get_connection(0).await?;
        let admin_policy = AdminPolicy {
            timeout: self.client_policy.load().timeout,
        };
        let info_map = Message::info(
            &admin_policy,
            &mut conn,
            &[RACK_IDS, node::REBALANCE_GENERATION],
        )
        .await?;
        if let Some(buf) = info_map.get(RACK_IDS) {
            node.parse_rack(buf.as_str())?;
        }

        // We re-update the rebalance generation right now (in case its changed since it was last polled)
        node.update_rebalance_generation(&info_map)?;

        Ok(())
    }

    pub async fn seed_nodes(&self) -> bool {
        let seed_array = self.seeds.load();

        info!("Seeding the cluster. Seeds count: {}", seed_array.len());

        let mut list: Vec<Arc<Node>> = vec![];
        // Fallback retention: if a seed validates but advertises no peers, it
        // may be an isolated node or a restarting one. Retain the first such
        // validator as a fallback and only use it if no other seed yields a
        // populated peer list. Mirrors Java `NodeValidator.seedNode` logic.
        let mut fallback: Option<NodeValidator> = None;

        for seed in seed_array.iter() {
            let mut seed_node_validator = NodeValidator::new(self.client_policy());
            if let Err(err) = seed_node_validator.validate_node(self, seed).await {
                log_error_chain!(err, "Failed to validate seed host: {}", seed);
                continue;
            }

            if seed_node_validator.services().is_empty() {
                // Seed reports no peers — defer it. If a later seed returns
                // a real peer list we'll drop this candidate.
                if fallback.is_none() {
                    debug!("Seed {seed} has no peers; retaining as fallback");
                    fallback = Some(seed_node_validator);
                } else {
                    debug!("Discarding additional peerless seed {seed}");
                }
                continue;
            }

            // A previously-retained fallback is orthogonal once a real peer
            // list shows up — drop it.
            if fallback.is_some() {
                debug!("Dropping fallback seed in favor of {seed}");
                fallback = None;
            }

            for alias in seed_node_validator.services() {
                let mut nv = NodeValidator::new(self.client_policy());
                if let Err(err) = nv.validate_node(self, &alias).await {
                    log_error_chain!(err, "Seeding host {} failed with error", alias);
                    continue;
                }

                if self.find_node_name(&list, &nv.name) {
                    continue;
                }

                let node = self.create_node(nv).await;
                let node = Arc::new(node);
                self.add_aliases(node.clone());
                list.push(node);
            }
        }

        // No seed yielded peers. Use the fallback (if any) as the single
        // cluster node so the client can still make progress.
        if list.is_empty() {
            if let Some(nv) = fallback {
                info!("Using fallback seed node: {}", nv.name);
                for alias in nv.aliases() {
                    let mut peer_nv = NodeValidator::new(self.client_policy());
                    if let Err(err) = peer_nv.validate_node(self, &alias).await {
                        log_error_chain!(err, "Fallback seed {} failed", alias);
                        continue;
                    }
                    if self.find_node_name(&list, &peer_nv.name) {
                        continue;
                    }
                    let node = self.create_node(peer_nv).await;
                    let node = Arc::new(node);
                    self.add_aliases(node.clone());
                    list.push(node);
                    break; // one fallback node is enough
                }
            }
        }

        self.add_nodes_and_aliases(&list);
        !list.is_empty()
    }

    fn find_node_name(&self, list: &[Arc<Node>], name: &str) -> bool {
        list.iter().any(|node| node.name() == name)
    }

    async fn find_new_nodes_to_add(&self, hosts: Vec<Host>) -> Vec<Arc<Node>> {
        let mut list: Vec<Arc<Node>> = vec![];

        for host in hosts {
            let mut nv = NodeValidator::new(self.client_policy());
            if let Err(err) = nv.validate_node(self, &host).await {
                log_error_chain!(err, "Adding node {} failed with error: {}", host, err);
                continue;
            }

            // Duplicate node name found. This usually occurs when the server
            // services list contains both internal and external IP addresses
            // for the same node. Add new host to list of alias filters
            // and do not add new node.
            let mut dup = false;
            match self.get_node_by_name(&nv.name) {
                Ok(node) => {
                    self.add_alias(host, node.clone());
                    dup = true;
                }
                Err(_) => {
                    if let Some(node) = list.iter().find(|n| n.name() == nv.name) {
                        self.add_alias(host, node.clone());
                        dup = true;
                    }
                }
            }

            if !dup {
                let node = self.create_node(nv).await;
                list.push(Arc::new(node));
            }
        }

        list
    }

    async fn create_node(&self, nv: NodeValidator) -> Node {
        let res = Node::new(self.client_policy(), Arc::new(nv));
        res.send_user_agent_id().await;
        res
    }

    async fn find_nodes_to_remove(&self, refresh_count: usize) -> Vec<Arc<Node>> {
        let nodes = self.nodes();
        let mut remove_list: Vec<Arc<Node>> = vec![];
        let cluster_size = nodes.len();
        for node in nodes {
            let tnode = node.clone();

            if !node.is_active() {
                remove_list.push(tnode);
                continue;
            }

            match cluster_size {
                // Single node clusters rely on whether it responded to info requests.
                1 if node.failures() >= 5 => {
                    // 5 consecutive info requests failed. Try seeds.
                    if self.seed_nodes().await {
                        debug!(
                            "Removing nodes in single node cluster after waiting for some failures"
                        );
                        remove_list.push(tnode);
                    }
                }

                // Either all nodes are down then wait for some min threshold failures
                // or one node refreshes and other failed durign refresh
                // For small size cluster, don't be aggressive could be transient failures
                2 if refresh_count <= 1 && node.failures() >= 5 => {
                    debug!("no node refreshed or one refreshed and other had failures in cluster size eq 2");
                    remove_list.push(tnode);
                }

                _ => {
                    // Multi-node: prefer a live node's view; drop non-responders or repeatedly failing peers.
                    // Another node's refresh can bump reference_count via add_friends while this node's refresh failed.
                    let failures = node.failures();
                    if refresh_count == 0 && failures > 0 {
                        debug!(
                            "no node refreshed in cluster size gte 2 and failures above min threshold"
                        );
                        remove_list.push(tnode);
                    } else if refresh_count >= 1 && node.reference_count() == 0 && failures == 0 {
                        if node.has_responded() && !self.find_node_in_partition_map(node) {
                            remove_list.push(tnode);
                            debug!(
                                    "some node refreshes but the current node is active but not referenced in partition-map"
                                );
                        }
                    } else if refresh_count >= 1 && failures > 2 {
                        remove_list.push(tnode);
                        debug!(
                            "multi-node: refresh failed repeatedly, removing node despite peer reference_count"
                        );
                    }
                }
            }
        }

        remove_list
    }

    fn add_nodes_and_aliases(&self, friend_list: &[Arc<Node>]) {
        for node in friend_list {
            self.add_aliases(node.clone());
        }
        self.add_nodes(friend_list);
    }

    fn remove_nodes_and_aliases(&self, mut nodes_to_remove: Vec<Arc<Node>>) {
        for node in &mut nodes_to_remove {
            for alias in node.aliases() {
                self.remove_alias(&alias);
            }
            if let Some(node) = Arc::get_mut(node) {
                node.close();
            }
        }
        self.remove_nodes(&nodes_to_remove);
    }

    fn add_alias(&self, host: Host, node: Arc<Node>) {
        let mut aliases = self.aliases();
        node.add_alias(host.clone());
        aliases.insert(host, node);
        self.aliases.store(Arc::new(aliases));
    }

    fn remove_alias(&self, host: &Host) {
        let mut aliases = self.aliases();
        aliases.remove(host);
        self.aliases.store(Arc::new(aliases));
    }

    fn add_aliases(&self, node: Arc<Node>) {
        let mut aliases = self.aliases();
        for alias in node.aliases() {
            aliases.insert(alias, node.clone());
        }
        self.aliases.store(Arc::new(aliases));
    }

    fn find_node_in_partition_map(&self, filter: Arc<Node>) -> bool {
        let filter = Some(filter);
        let partitions = self.partition_map.load();
        (*partitions)
            .values()
            .any(|map| map.nodes.iter().any(|(_, node)| *node == filter))
    }

    fn add_nodes(&self, friend_list: &[Arc<Node>]) {
        if friend_list.is_empty() {
            return;
        }

        let mut nodes = self.nodes();
        nodes.extend(friend_list.iter().cloned());
        self.set_nodes(nodes);
    }

    fn remove_nodes(&self, nodes_to_remove: &[Arc<Node>]) {
        if nodes_to_remove.is_empty() {
            return;
        }

        let nodes = self.nodes();
        let mut node_array: Vec<Arc<Node>> = vec![];

        for node in &nodes {
            if !nodes_to_remove.contains(node) {
                node_array.push(node.clone());
            }
        }

        self.set_nodes(node_array);
    }

    pub fn is_connected(&self) -> bool {
        let nodes = self.nodes();
        let closed = self.closed.load(Ordering::Relaxed);
        !nodes.is_empty() && !closed
    }

    pub fn aliases(&self) -> HashMap<Host, Arc<Node>> {
        (*self.aliases.load().clone()).clone()
    }

    pub fn nodes(&self) -> Vec<Arc<Node>> {
        (*self.nodes.load().clone()).clone()
    }

    fn set_nodes(&self, new_nodes: Vec<Arc<Node>>) {
        self.nodes.store(Arc::new(new_nodes));
    }

    pub fn get_node(&self, partition: &mut Partition<'_>) -> Result<Arc<Node>> {
        partition.get_node(self)
    }

    pub fn get_master_node(&self, namespace: &str, partition_id: usize) -> Result<Arc<Node>> {
        let partition = Partition::new(namespace, partition_id);
        partition.get_master_node(self)
    }

    pub fn get_random_node(&self) -> Result<Arc<Node>> {
        let node_array = self.nodes();
        let length = node_array.len() as isize;

        for _ in 0..length {
            let index = ((self.node_index.fetch_add(1, Ordering::Relaxed) + 1) % length).abs();
            if let Some(node) = node_array.get(index as usize) {
                if node.is_active() {
                    return Ok(node.clone());
                }
            }
        }

        Err(Error::Connection("No active node".into()))
    }

    pub fn get_node_by_name(&self, node_name: &str) -> Result<Arc<Node>> {
        let node_array = self.nodes();

        for node in &node_array {
            if node.name() == node_name {
                return Ok(node.clone());
            }
        }

        Err(Error::InvalidNode(format!(
            "Requested node `{node_name}` not found."
        )))
    }

    // Returns the hashed password for the cluster.
    // Hashing passwords is an expensive operation, se we ony do it once
    // and then cache it.
    pub(crate) fn hashed_pass(&self) -> Option<String> {
        (*self.hashed_pass.load().clone()).clone()
    }

    // Will update the cluster password if the password change was for the current user.
    pub(crate) fn update_password(&self, user: &str, password: &str) -> Result<()> {
        let auth_mode = { &self.client_policy.load().auth_mode };
        match auth_mode {
            crate::AuthMode::Internal(u, _) | crate::AuthMode::External(u, _) if u == user => {
                self.hashed_pass
                    .store(Arc::new(Some(AdminCommand::hash_password(password)?)));
            }
            _ => (),
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if !self.closed.load(Ordering::Relaxed) {
            // close tend by closing the channel
            let tx = self.tend_channel.lock().await;
            drop(tx);
            self.closed.store(true, Ordering::Relaxed);
        }

        Ok(())
    }
}
