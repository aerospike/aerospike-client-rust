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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;

use hazarc::AtomicArc;

use crate::cluster::node_validator::NodeValidator;
use crate::cluster::peers_parser::PeersParser;
use crate::cluster::CLIENT_VERSION;
use crate::commands::Message;
use crate::errors::{Error, Result};
use crate::net::{Connection, ConnectionPool, Host, PooledConnection};
use crate::policy::{AdminPolicy, ClientPolicy};
use crate::Version;

pub const PARTITIONS: usize = 4096;
pub const PARTITION_GENERATION: &str = "partition-generation";
pub const PEERS_GENERATION: &str = "peers-generation";
pub const REBALANCE_GENERATION: &str = "rebalance-generation";

/// The node instance holding connections and node settings.
/// Exposed for usage in the sync client interface.
#[derive(Debug)]
pub struct Node {
    client_policy: ClientPolicy,
    name: String,
    host: Host,
    aliases: AtomicArc<Vec<Host>>,
    address: String,

    connection_pool: ConnectionPool,
    failures: AtomicUsize,

    partition_generation: AtomicIsize,
    rebalance_generation: AtomicIsize,
    /// Peers-generation reported by this node on the last successful refresh.
    /// `-1` means "never refreshed" so the first successful refresh always
    /// triggers peer parsing. Matches Java `Node.peersGeneration`.
    peers_generation: AtomicIsize,
    /// Number of peers this node advertised in its last successful peer-list
    /// parse. Used as a split-cluster guard during partition refresh so a node
    /// that reports no peers after its first tend cannot dominate the map.
    peers_count: AtomicUsize,
    // Which racks are these things part of
    rack_ids: AtomicArc<HashMap<String, usize>>,
    refresh_count: AtomicUsize,
    reference_count: AtomicUsize,
    responded: AtomicBool,
    active: AtomicBool,
    version: Version,
}

impl Node {
    #![allow(missing_docs)]
    pub fn new(client_policy: ClientPolicy, nv: Arc<NodeValidator>) -> Self {
        Node {
            client_policy: client_policy.clone(),
            name: nv.name.clone(),
            aliases: AtomicArc::from(nv.aliases.clone()),
            address: nv.address.clone(),

            host: nv.aliases[0].clone(),
            rebalance_generation: AtomicIsize::new(if client_policy.rack_ids.is_some() {
                -1
            } else {
                0
            }),
            connection_pool: ConnectionPool::new(nv.aliases[0].clone(), client_policy),
            failures: AtomicUsize::new(0),
            partition_generation: AtomicIsize::new(-1),
            peers_generation: AtomicIsize::new(-1),
            peers_count: AtomicUsize::new(0),
            refresh_count: AtomicUsize::new(0),
            reference_count: AtomicUsize::new(0),
            responded: AtomicBool::new(false),
            active: AtomicBool::new(true),
            version: nv.version.clone(),
            rack_ids: AtomicArc::from(HashMap::new()),
        }
    }

    /// Returns the Node address
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Returns the Node name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Node name
    pub const fn version(&self) -> &Version {
        &self.version
    }

    // Returns the active client policy
    pub const fn client_policy(&self) -> &ClientPolicy {
        &self.client_policy
    }

    pub fn host(&self) -> Host {
        self.host.clone()
    }

    // Returns the reference count
    pub fn reference_count(&self) -> usize {
        self.reference_count.load(Ordering::Relaxed)
    }

    // `true` after this node has completed at least one successful [`Node::refresh`].
    // Used by cluster tending to avoid treating newly discovered nodes (not yet refreshed
    // this cycle) as stale ghosts.
    pub(crate) fn has_responded(&self) -> bool {
        self.responded.load(Ordering::Relaxed)
    }

    // Refresh the node
    pub async fn refresh(&self, current_aliases: HashMap<Host, Arc<Node>>) -> Result<Vec<Host>> {
        self.reference_count.store(0, Ordering::Relaxed);
        self.responded.store(false, Ordering::Relaxed);
        self.refresh_count.fetch_add(1, Ordering::Relaxed);
        let mut commands = vec![
            "node",
            "cluster-name",
            PARTITION_GENERATION,
            PEERS_GENERATION,
            self.client_policy.peers_string(),
        ];

        if self.client_policy.rack_ids.is_some() {
            commands.push(REBALANCE_GENERATION);
        }

        let admin_policy = AdminPolicy {
            timeout: self.client_policy.timeout,
        };

        let info_map = self
            .info(&admin_policy, &commands)
            .await
            .map_err(|e| e.chain_error("Info command failed"))?;
        self.validate_node(&info_map)
            .map_err(|e| e.chain_error("Failed to validate node"))?;
        self.responded.store(true, Ordering::Relaxed);

        // Only walk the peer list when the server reports a different
        // peers-generation than last time. Matches Java's
        // `verifyPeersGeneration` + `refreshPeers` split. When the generation
        // hasn't changed we return an empty friends list without parsing the
        // service string — callers rely on `peers_count` / partition map for
        // removal decisions in that case.
        let peers_gen_changed = self
            .verify_peers_generation(&info_map)
            .map_err(|e| e.chain_error("Failed to verify peers generation"))?;

        let friends = if peers_gen_changed {
            let friends = self
                .add_friends(current_aliases, &info_map)
                .map_err(|e| e.chain_error("Failed to add friends"))?;
            Ok::<_, Error>(friends)
        } else {
            // Unchanged generation: still bump reference_count for currently
            // known aliases so the removal logic doesn't treat them as orphans.
            self.count_existing_peers(&current_aliases, &info_map)?;
            Ok(Vec::new())
        }?;

        self.update_partitions(&info_map)
            .map_err(|e| e.chain_error("Failed to update partitions"))?;
        self.update_rebalance_generation(&info_map)
            .map_err(|e| e.chain_error("Failed to update rebalance generation"))?;
        self.reset_failures();
        let _ = self.fill_min_conns().await;
        Ok(friends)
    }

    /// Parses `peers-generation` from `info_map` and compares with the stored
    /// value. Returns `true` when the generation has changed (i.e. caller must
    /// re-parse the peer list). Quick-restart (gen reset to a lower number) is
    /// treated as a change.
    fn verify_peers_generation(&self, info_map: &HashMap<String, String>) -> Result<bool> {
        let gen_str = info_map
            .get(PEERS_GENERATION)
            .ok_or_else(|| Error::BadResponse("Missing peers-generation".to_string()))?;
        let gen = gen_str.parse::<isize>()?;

        let stored = self.peers_generation.load(Ordering::Relaxed);
        Ok(stored != gen)
    }

    /// When peers-generation did not change, still scan the advertised peers
    /// to bump `reference_count` for aliases we already know. This mirrors
    /// the reference-counting that `add_friends` does so cluster-level
    /// removal decisions stay accurate between generation bumps.
    fn count_existing_peers(
        &self,
        current_aliases: &HashMap<Host, Arc<Node>>,
        info_map: &HashMap<String, String>,
    ) -> Result<()> {
        let friend_string = match info_map.get(self.client_policy.peers_string()) {
            Some(s) if !s.is_empty() => s,
            _ => return Ok(()),
        };

        let (_, hosts) = PeersParser::new(friend_string).parse()?;
        for mut alias in hosts {
            if let Some(ref ip_map) = self.client_policy.ip_map {
                if let Some(mapped) = ip_map.get(&alias.name) {
                    alias.name.clone_from(mapped);
                }
            }
            if current_aliases.contains_key(&alias) {
                self.reference_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn validate_node(&self, info_map: &HashMap<String, String>) -> Result<()> {
        self.verify_node_name(info_map)?;
        self.verify_cluster_name(info_map)?;
        Ok(())
    }

    fn verify_node_name(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match info_map.get("node") {
            None => Err(Error::InvalidNode("Missing node name".to_string())),
            Some(info_name) if info_name == &self.name => Ok(()),
            Some(info_name) => {
                self.inactivate();
                Err(Error::InvalidNode(format!(
                    "Node name has changed: '{}' => '{}'",
                    self.name, info_name
                )))
            }
        }
    }

    #[allow(clippy::option_if_let_else)]
    fn verify_cluster_name(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match self.client_policy.cluster_name {
            None => Ok(()),
            Some(ref expected) => match info_map.get("cluster-name") {
                None => Err(Error::InvalidNode("Missing cluster name".to_string())),
                Some(info_name) if info_name == expected => Ok(()),
                Some(info_name) => {
                    self.inactivate();
                    Err(Error::InvalidNode(format!(
                        "Cluster name mismatch: expected={expected},
                                                           got={info_name}"
                    )))
                }
            },
        }
    }

    fn add_friends(
        &self,
        current_aliases: HashMap<Host, Arc<Node>>,
        info_map: &HashMap<String, String>,
    ) -> Result<Vec<Host>> {
        let mut friends: Vec<Host> = vec![];

        // Record the latest peers-generation so subsequent refreshes can
        // short-circuit when unchanged. Parsed again here because the caller
        // only did a comparison.
        let gen_str = info_map
            .get(PEERS_GENERATION)
            .ok_or_else(|| Error::BadResponse("Missing peers-generation".to_string()))?;
        let new_gen = gen_str.parse::<isize>()?;

        let friend_string = match info_map.get(self.client_policy.peers_string()) {
            None => return Err(Error::BadResponse("Missing services list".to_string())),
            Some(friend_string) if friend_string.is_empty() => {
                // Empty peer list is a valid state (e.g. single-node cluster).
                // Still update bookkeeping so we don't re-parse on every tend.
                self.peers_count.store(0, Ordering::Relaxed);
                self.peers_generation.store(new_gen, Ordering::Relaxed);
                return Ok(friends);
            }
            Some(friend_string) => friend_string,
        };

        let (_, hosts) = PeersParser::new(friend_string).parse()?;
        let peers_total = hosts.len();
        for mut alias in hosts {
            if let Some(ref ip_map) = self.client_policy.ip_map {
                if let Some(mapped) = ip_map.get(&alias.name) {
                    alias.name.clone_from(mapped);
                }
            }

            if current_aliases.contains_key(&alias) {
                self.reference_count.fetch_add(1, Ordering::Relaxed);
            } else if !friends.contains(&alias) {
                friends.push(alias);
            }
        }

        self.peers_count.store(peers_total, Ordering::Relaxed);
        self.peers_generation.store(new_gen, Ordering::Relaxed);
        Ok(friends)
    }

    pub fn update_partitions(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match info_map.get(PARTITION_GENERATION) {
            None => {
                return Err(Error::BadResponse(
                    "Missing partition generation".to_string(),
                ))
            }
            Some(gen_string) => {
                let gen = gen_string.parse::<isize>()?;
                self.partition_generation.store(gen, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    pub fn update_rebalance_generation(&self, info_map: &HashMap<String, String>) -> Result<()> {
        if let Some(gen_string) = info_map.get(REBALANCE_GENERATION) {
            let gen = gen_string.parse::<isize>()?;
            self.rebalance_generation.store(gen, Ordering::Relaxed);
        }

        Ok(())
    }

    pub fn is_in_rack(&self, namespace: &str, rack_ids: &HashSet<usize>) -> bool {
        self.rack_ids
            .load()
            .get(namespace)
            .is_some_and(|r| rack_ids.contains(r))
    }

    pub fn parse_rack(&self, buf: &str) -> Result<()> {
        let new_table = buf
            .split(';')
            .map(|entry| {
                let (key, val) = entry
                    .split_once(':')
                    .ok_or(Error::BadResponse("Invalid rack entry".into()))?;
                Ok((key.to_string(), val.parse::<usize>()?))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        self.rack_ids.store(Arc::new(new_table));
        Ok(())
    }

    // Get a connection to the node from the connection pool
    pub async fn get_connection(&self, hint: u8) -> Result<PooledConnection> {
        if let Ok(conn) = self.connection_pool.get(hint) {
            return Ok(conn);
        }

        self.connection_pool.make_conn(0).await
    }

    // Put a connection to the node back in the connection pool
    pub fn put_connection(&self, mut pconn: PooledConnection) {
        if let Some(conn) = pconn.conn.take() {
            pconn.queue.put_back(conn);
        }
    }

    // Amount of failures
    pub fn failures(&self) -> usize {
        self.failures.load(Ordering::Relaxed)
    }

    fn reset_failures(&self) {
        self.failures.store(0, Ordering::Relaxed);
    }

    // Adds a failure to the failure count
    pub fn increase_failures(&self) -> usize {
        self.failures.fetch_add(1, Ordering::Relaxed)
    }

    fn inactivate(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    // Returns true if the node is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    // Get a list of aliases to the node
    pub fn aliases(&self) -> Vec<Host> {
        self.aliases.load().to_vec()
    }

    // Add an alias to the node
    pub fn add_alias(&self, alias: Host) {
        let mut aliases = self.aliases();
        aliases.push(alias);
        self.aliases.store(Arc::new(aliases));
        self.reference_count.fetch_add(1, Ordering::Relaxed);
    }

    // Set the node inactive and close all connections in the pool
    pub fn close(&mut self) {
        self.inactivate();
        self.connection_pool.close();
    }

    // Send info commands to this node
    pub async fn info(
        &self,
        policy: &AdminPolicy,
        commands: &[&str],
    ) -> Result<HashMap<String, String>> {
        let mut conn = self.get_connection(0).await?;
        let res = Message::info(policy, &mut conn, commands).await;

        if let Err(e) = res {
            conn.invalidate();
            return Err(e);
        }
        self.put_connection(conn);
        res
    }

    // Get the partition generation
    pub fn partition_generation(&self) -> isize {
        self.partition_generation.load(Ordering::Relaxed)
    }

    // Get the rebalance generation
    pub fn rebalance_generation(&self) -> isize {
        self.rebalance_generation.load(Ordering::Relaxed)
    }

    /// Peers-generation reported on the last successful refresh, or `-1` if
    /// this node has never been refreshed.
    pub fn peers_generation(&self) -> isize {
        self.peers_generation.load(Ordering::Relaxed)
    }

    /// Number of peers this node advertised on its last successful refresh.
    /// `0` indicates either a single-node cluster or a split-cluster view.
    pub fn peers_count(&self) -> usize {
        self.peers_count.load(Ordering::Relaxed)
    }

    /// Total number of times [`refresh`](Self::refresh) has been called
    /// (whether or not it succeeded). Used as a split-cluster guard.
    pub fn refresh_count(&self) -> usize {
        self.refresh_count.load(Ordering::Relaxed)
    }

    pub(crate) async fn send_user_agent_id(&self) {
        if !self.version().supports_app_id() {
            return;
        }

        let app_id = self.client_policy().application_id();

        // Source user-agent payload
        // Format: "1,rust-<version>,<application-id>"
        let user_agent_id = format!("1,rust-{CLIENT_VERSION},{app_id}");
        let user_agent_id = base64::encode(&user_agent_id);
        let user_agent_command = format!("user-agent-set:value={user_agent_id}");

        let policy = AdminPolicy {
            timeout: self.client_policy().timeout,
        };
        let _ = self.info(&policy, &[&user_agent_command]).await;
    }

    /// Reap idle connections, but for any idle connection that would take
    /// the pool below `min_conns_per_node`, send a cheap info probe to keep
    /// it alive instead of dropping it. A successful probe reads a response
    /// which in turn resets the connection's idle deadline (`Message::info`
    /// calls `conn.refresh()` internally), so the probed connection goes
    /// back into the pool as fresh.
    ///
    /// **Non-blocking**: uses `try_lock` on each queue so a contended pool
    /// is skipped for this tend iteration — tend retries next time. Probes
    /// run concurrently via `join_all` so total wall-clock is bounded by
    /// the single slowest probe, not `N × per-probe latency`.
    ///
    /// Returns the number of connections processed (reaped + refreshed).
    pub async fn reap_and_refresh_idle_connections(&self) -> usize {
        let policy = &self.client_policy;
        let num_queues = policy.conn_pools_per_node as usize;
        if num_queues == 0 {
            return 0;
        }
        // Distribute `min_conns_per_node` evenly across internal queues.
        // When it doesn't divide evenly, each queue's share is floor(); the
        // one-off connection is tolerated (Java does the same).
        let per_queue_min = policy.min_conns_per_node / num_queues.max(1);

        let probe_policy = AdminPolicy {
            // Tight timeout — this is a keep-alive probe, not a command.
            timeout: policy.timeout.min(2000),
        };

        let mut total_processed = 0usize;
        for queue in self.connection_pool.queues() {
            let Some(idle) = queue.try_extract_idle() else {
                // Queue is contended by a live caller — skip this tend.
                continue;
            };
            if idle.is_empty() {
                continue;
            }

            // After extraction, `reserved` still counts the idle conns we
            // pulled out. `effective_live` is what the pool would have if
            // every extracted conn were dropped right now (non-idle queued
            // + in-flight). The shortfall vs `per_queue_min` tells us how
            // many idle conns to keep alive via probe.
            let reserved = queue.reserved_count();
            let effective_live = reserved.saturating_sub(idle.len());
            let to_keep = per_queue_min.saturating_sub(effective_live).min(idle.len());

            let mut idle_iter = idle.into_iter();
            let keepers: Vec<Connection> = idle_iter.by_ref().take(to_keep).collect();
            // Remaining iterator entries are surplus idles → drop + free slot.
            for conn in idle_iter {
                drop(conn);
                queue.reduce_capacity();
                total_processed += 1;
            }

            if keepers.is_empty() {
                continue;
            }

            // Probe keepers concurrently. Successful probe → surviving conn
            // goes straight back in the queue (its idle deadline was reset
            // as a side effect of reading the info response). Failed probe
            // → the connection is dead; free its slot.
            let probes = keepers.into_iter().map(|mut conn| {
                let pp = probe_policy;
                async move {
                    match Message::info(&pp, &mut conn, &["node"]).await {
                        Ok(_) => Some(conn),
                        Err(_) => None,
                    }
                }
            });
            let results = futures::future::join_all(probes).await;

            for result in results {
                if let Some(conn) = result {
                    // `put_back` uses a blocking `lock()` but only
                    // briefly (push one element) — no async I/O is
                    // held under it. Keeps the API simple and the
                    // probed conns go back in order.
                    queue.put_back(conn);
                    total_processed += 1;
                } else {
                    queue.reduce_capacity();
                    total_processed += 1;
                }
            }
        }

        total_processed
    }

    /// Fills the connection pool to the minimum required
    /// by the [`ClientPolicy.min_conns_per_node`]
    pub(crate) async fn fill_min_conns(&self) -> Result<usize> {
        let mut count = 0;

        let client_policy = self.client_policy();
        if client_policy.min_conns_per_node > 0 {
            let to_fill = client_policy.min_conns_per_node - self.connection_pool.num_conns();
            for _ in 0..to_fill {
                self.connection_pool.make_conn(count).await?;
                count += 1;
            }
        }

        Ok(count)
    }
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.name == other.name
    }
}

impl Eq for Node {}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        format!("{}: {}", self.name, self.host).fmt(f)
    }
}
