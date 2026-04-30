// Copyright 2015-2024 Aerospike, Inc.
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

use aerospike_rt::Mutex as AsyncMutex;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use hazarc::AtomicArc;

use crate::cluster::node_validator::NodeValidator;
use crate::cluster::peers::Peers;
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
    /// Long-lived dedicated socket used by `tend()` and friends:
    /// `refresh`, `refresh_peers`, partition map fetch, rack-ids fetch.
    /// Lazy-opened on first use and reused on subsequent tends so we don't
    /// pay LOGIN + TCP-handshake every cycle. On any error it's torn down
    /// so the next tend will reopen. Mirrors Java `Node.tendConnection`.
    tend_connection: AsyncMutex<Option<Connection>>,
    failures: AtomicUsize,

    partition_generation: AtomicIsize,
    rebalance_generation: AtomicIsize,
    peers_generation: AtomicIsize,
    peers_count: AtomicUsize,
    partition_changed: AtomicBool,
    rebalance_changed: AtomicBool,
    // Which racks are these things part of
    rack_ids: AtomicArc<HashMap<String, usize>>,
    reference_count: AtomicUsize,
    refresh_count: AtomicUsize,
    responded: AtomicBool,
    active: AtomicBool,
    version: Version,
    /// Per-`error_rate_window` circuit breaker state. `error_rate_count`
    /// is bumped on every retriable failure (network error, server
    /// `TIMEOUT` / `DEVICE_OVERLOAD` / `KEY_BUSY`, connection-close-on-error)
    /// and reset every `error_rate_window` tend iterations.
    /// `node_max_error_rate` is the per-node ceiling — it adapts each
    /// reset, doubling on a clean window (capped at the cluster setting)
    /// or halving when the previous window tripped. Mirrors Java's
    /// `Node.errorRateCount` + `Node.maxErrorRate`.
    error_rate_count: AtomicUsize,
    node_max_error_rate: AtomicUsize,
    /// Cached hostname that resolves to this node's IP. Populated by
    /// `Cluster::peer_exists` on the first successful DNS-aware match, so
    /// subsequent tends can short-circuit the lookup. Mirrors Java's
    /// `node.hostname` field.
    hostname: std::sync::OnceLock<String>,
}

impl Drop for Node {
    fn drop(&mut self) {
        debug!("Node closed {self}");
        self.close();
        self.connection_pool.close();
        // The tend socket (if any) is held inside `tend_connection` and
        // will be torn down by `Connection`'s own `Drop` impl when this
        // `Node` drops — no manual close needed here.
    }
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
            connection_pool: ConnectionPool::new(nv.aliases[0].clone(), client_policy.clone()),
            tend_connection: AsyncMutex::new(None),
            failures: AtomicUsize::new(0),
            error_rate_count: AtomicUsize::new(0),
            node_max_error_rate: AtomicUsize::new(client_policy.max_error_rate),
            partition_generation: AtomicIsize::new(-1),
            peers_generation: AtomicIsize::new(-1),
            peers_count: AtomicUsize::new(0),
            partition_changed: AtomicBool::new(false),
            rebalance_changed: AtomicBool::new(false),
            refresh_count: AtomicUsize::new(0),
            reference_count: AtomicUsize::new(0),
            responded: AtomicBool::new(false),
            active: AtomicBool::new(true),
            version: nv.version.clone(),
            rack_ids: AtomicArc::from(HashMap::new()),
            hostname: std::sync::OnceLock::new(),
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

    /// Returns the hostname resolved to this node's IP, if cached.
    pub fn cached_hostname(&self) -> Option<&str> {
        self.hostname.get().map(String::as_str)
    }

    /// Cache the hostname that resolved to this node's IP. No-op on the
    /// second call — first writer wins, matching Java's behavior.
    pub fn cache_hostname(&self, name: String) {
        let _ = self.hostname.set(name);
    }

    // Returns the reference count
    pub fn reference_count(&self) -> usize {
        self.reference_count.load(Ordering::Relaxed)
    }

    /// Increments the reference count by 1.
    pub fn increment_reference_count(&self) {
        self.reference_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Resets the reference count to 0. Called at the start of each tend
    /// cycle, before peer refresh (mirrors Java's `referenceCount = 0`).
    pub fn reset_reference_count(&self) {
        self.reference_count.store(0, Ordering::Relaxed);
    }

    /// Resets the per-tend `partition_changed` flag.
    pub fn set_partition_changed(&self, changed: bool) {
        self.partition_changed.store(changed, Ordering::Relaxed);
    }

    /// Resets the per-tend `rebalance_changed` flag.
    pub fn set_rebalance_changed(&self, changed: bool) {
        self.rebalance_changed.store(changed, Ordering::Relaxed);
    }

    // Returns whether partition changed during this tend cycle
    pub fn partition_changed(&self) -> bool {
        self.partition_changed.load(Ordering::Relaxed)
    }

    // Returns whether rebalance generation changed during this tend cycle
    pub fn rebalance_changed(&self) -> bool {
        self.rebalance_changed.load(Ordering::Relaxed)
    }

    // Refresh the node
    /// Phase 1 of the tend cycle: Refresh node metadata and check generation numbers.
    ///
    /// Mirrors Java `Node.refresh(Peers peers)`. Sends lightweight info
    /// commands to verify node identity and check peers / partition /
    /// rebalance generations. Does NOT fetch the full peer list — that
    /// happens in [`refresh_peers`](Self::refresh_peers) only when
    /// `peers.gen_changed` ends up true.
    pub async fn refresh(&self, peers: &Peers) -> Result<()> {
        if !self.is_active() {
            return Ok(());
        }

        let rack_aware = self.client_policy.rack_ids.is_some();

        self.responded.store(false, Ordering::Relaxed);

        let mut commands = vec![
            "node",
            "cluster-name",
            PEERS_GENERATION,
            PARTITION_GENERATION,
        ];
        if rack_aware {
            commands.push(REBALANCE_GENERATION);
        }

        let admin_policy = AdminPolicy {
            timeout: self.client_policy.timeout,
        };

        let info_result = self.tend_info(&admin_policy, &commands).await;
        let info_map = match info_result {
            Ok(map) => map,
            Err(e) => {
                // On failure, force re-discovery on the next tend cycle.
                peers.set_gen_changed(true);
                self.refresh_failed();
                return Err(e.chain_error("Info command failed"));
            }
        };

        if let Err(e) = self.validate_node(&info_map) {
            peers.set_gen_changed(true);
            self.refresh_failed();
            return Err(e.chain_error("Failed to validate node"));
        }

        if let Err(e) = self.verify_peers_generation(&info_map, peers) {
            peers.set_gen_changed(true);
            self.refresh_failed();
            return Err(e.chain_error("Failed to verify peers generation"));
        }

        if let Err(e) = self.verify_partition_generation(&info_map) {
            peers.set_gen_changed(true);
            self.refresh_failed();
            return Err(e.chain_error("Failed to verify partition generation"));
        }

        if rack_aware {
            if let Err(e) = self.verify_rebalance_generation(&info_map) {
                peers.set_gen_changed(true);
                self.refresh_failed();
                return Err(e.chain_error("Failed to verify rebalance generation"));
            }
        }

        peers.increment_refresh_count();

        // Reload peers, partitions and racks if there were failures on the
        // previous tend (mirror Java's behavior).
        if self.failures() > 0 {
            peers.set_gen_changed(true);
            self.partition_changed.store(true, Ordering::Relaxed);
            if rack_aware {
                self.rebalance_changed.store(true, Ordering::Relaxed);
            }
        }

        self.reset_failures();
        self.responded.store(true, Ordering::Relaxed);
        self.refresh_count.fetch_add(1, Ordering::Relaxed);

        let _ = self.fill_min_conns().await;
        Ok(())
    }

    /// Phase 2: Fetch and parse the full peer list from the server.
    ///
    /// Only called when `peers.gen_changed` is true (i.e., when any node's
    /// peers generation changed during phase 1).
    pub async fn refresh_peers(&self, peers: &mut Peers) -> Result<()> {
        // Don't refresh peers when node connection has already failed during this tend.
        if self.failures() > 0 || !self.is_active() {
            return Ok(());
        }

        let admin_policy = AdminPolicy {
            timeout: self.client_policy.timeout,
        };

        let peers_cmd = self.client_policy.peers_string();
        let info_map = self
            .tend_info(&admin_policy, &[peers_cmd])
            .await
            .map_err(|e| {
                self.refresh_failed();
                e.chain_error("Failed to fetch peers info")
            })?;

        let peer_string = match info_map.get(peers_cmd) {
            None => {
                self.refresh_failed();
                return Err(Error::BadResponse("Missing peers list".to_string()));
            }
            Some(s) if s.is_empty() => return Ok(()),
            Some(s) => s,
        };

        let result = PeersParser::new(peer_string)
            .with_ip_map(self.client_policy.ip_map.as_ref())
            .parse()
            .map_err(|e| {
                self.refresh_failed();
                e
            })?;

        // Tag each peer with the node that parsed it so a later
        // materialization failure invalidates only that node's pending
        // peers-generation, not all of them. Each parsing node replaces
        // the working peer set; we materialize its peers immediately
        // afterward (Java-style per-node validation).
        let tagged: Vec<crate::cluster::peers::Peer> = result
            .peers
            .into_iter()
            .map(|mut p| {
                p.from_node_name = Some(self.name.clone());
                p
            })
            .collect();

        let parsed_count = tagged.len();
        peers.append_peers(tagged);
        peers.increment_refresh_count();

        // Stage the new generation; commit only after every parsed peer has
        // been materialized into the cluster (Java's `peersValidated`).
        peers.set_pending_generation(self.name.clone(), result.generation as isize);

        // `peers_count` is what split-cluster checks consult later in the
        // tend, so it should reflect what *this* node advertised.
        self.peers_count.store(parsed_count, Ordering::Relaxed);

        Ok(())
    }

    /// Commit a previously-staged peers-generation. Called by the cluster
    /// after `materialize_peers` confirms that every peer parsed by this
    /// node was reachable — mirrors Java's `peersGeneration = parser.generation`
    /// inside `Node.refreshPeers`.
    pub fn commit_peers_generation(&self, generation: isize) {
        self.peers_generation.store(generation, Ordering::Relaxed);
    }

    /// Called when a refresh step fails. Resets generation numbers to force
    /// re-discovery on the next tend cycle.
    fn refresh_failed(&self) {
        self.peers_generation.store(-1, Ordering::Relaxed);
        self.partition_generation.store(-1, Ordering::Relaxed);

        if self.client_policy.rack_ids.is_some() {
            self.rebalance_generation.store(-1, Ordering::Relaxed);
        }

        self.increase_failures();
    }

    /// Parses `peers-generation` from `info_map` and compares with the
    /// stored value. Sets `peers.gen_changed = true` if they differ. Mirrors
    /// Java's `Node.verifyPeersGeneration`.
    ///
    /// When the server's reported generation goes *backward* (`stored > gen`)
    /// the node almost certainly quick-restarted: it forgot us and reset
    /// its peers list. Surface that in the log and reset our retry-error
    /// rate so the recovered node isn't immediately punished for the
    /// pre-restart failure history.
    fn verify_peers_generation(
        &self,
        info_map: &HashMap<String, String>,
        peers: &Peers,
    ) -> Result<()> {
        let gen_str = info_map
            .get(PEERS_GENERATION)
            .ok_or_else(|| Error::BadResponse("Missing peers-generation".to_string()))?;
        let gen = gen_str.parse::<isize>()?;

        let stored = self.peers_generation.load(Ordering::Relaxed);
        if stored != gen {
            peers.set_gen_changed(true);

            if stored > gen && stored != -1 {
                info!(
                    "Quick node restart detected: node={self} oldgen={stored} newgen={gen}"
                );
                // Drop accumulated failure count so the freshly-restarted
                // node is treated like a healthy peer until proven
                // otherwise this tend.
                self.reset_failures();
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

    /// Compares the server's partition-generation with the node's last known value.
    /// Sets `partition_changed` flag if they differ.
    fn verify_partition_generation(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match info_map.get(PARTITION_GENERATION) {
            None => Err(Error::BadResponse(
                "Missing partition generation".to_string(),
            )),
            Some(gen_string) => {
                let gen = gen_string.parse::<isize>()?;
                if self.partition_generation.load(Ordering::Relaxed) != gen {
                    self.partition_changed.store(true, Ordering::Relaxed);
                }
                Ok(())
            }
        }
    }

    /// Compares the server's rebalance-generation with the node's last known
    /// value. Sets `rebalance_changed` flag if they differ. Only called when
    /// the cluster is rack-aware.
    fn verify_rebalance_generation(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match info_map.get(REBALANCE_GENERATION) {
            None => Err(Error::BadResponse(
                "Missing rebalance-generation".to_string(),
            )),
            Some(gen_string) => {
                let gen = gen_string.parse::<isize>()?;
                if self.rebalance_generation.load(Ordering::Relaxed) != gen {
                    self.rebalance_changed.store(true, Ordering::Relaxed);
                }
                Ok(())
            }
        }
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

    pub fn set_partition_generation(&self, gen: isize) {
        self.partition_generation.store(gen, Ordering::Relaxed);
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
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                let (key, val) = entry
                    .split_once(':')
                    .ok_or(Error::BadResponse("Invalid rack entry".into()))?;
                let ns = key.trim();
                // Aerospike server enforces 1..=31 for namespace names.
                // Reject anything outside that to avoid populating the rack
                // table with poisoned entries (mirrors Java's RackParser).
                if ns.is_empty() || ns.len() >= 32 {
                    return Err(Error::BadResponse(format!(
                        "Invalid racks namespace `{ns}`"
                    )));
                }
                Ok((ns.to_string(), val.parse::<usize>()?))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        self.rack_ids.store(Arc::new(new_table));
        Ok(())
    }

    // Get a connection to the node from the connection pool
    pub async fn get_connection(&self, hint: u8) -> Result<PooledConnection> {
        if !self.is_active() {
            return Err(Error::InvalidNode(format!(
                "Cannot get a connection for node. The node `{self}` is inactive"
            )));
        }

        if let Ok(conn) = self.connection_pool.get(hint) {
            return Ok(conn);
        }

        self.connection_pool.make_conn(0).await
    }

    // Put a connection to the node back in the connection pool
    pub fn put_connection(&self, mut pconn: PooledConnection) {
        if self.is_active() {
            if let Some(conn) = pconn.conn.take() {
                pconn.queue.put_back(conn);
            }
        } else {
            // Inactive: do not return a Ready connection to the pool — `PooledConnection`'s
            // `Drop` would otherwise `put_back` it.
            pconn.invalidate();
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
    pub fn close(&self) {
        self.inactivate();
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

    /// Run an info command over this node's long-lived tend socket. Lazily
    /// opens it on first use; on any error tears the socket down so the
    /// next call reopens. Mirrors Java's `Node.refresh` reuse of
    /// `tendConnection`. Use this for tend-time traffic only — operational
    /// commands should go through the pool via [`info`](Self::info).
    pub async fn tend_info(
        &self,
        policy: &AdminPolicy,
        commands: &[&str],
    ) -> Result<HashMap<String, String>> {
        let mut guard = self.tend_connection.lock().await;
        if guard.is_none() {
            // Open lazily. The first call after Node::new pays the
            // TCP-handshake + LOGIN here; subsequent calls reuse the
            // already-authenticated socket.
            let conn = Connection::new(
                &self.host,
                &self.client_policy,
                self.client_policy.hashed_pass().as_ref(),
            )
            .await
            .map_err(|e| e.chain_error("Failed to open tend connection"))?;
            *guard = Some(conn);
        }

        // SAFETY: we just ensured `guard` is `Some`.
        let conn = guard.as_mut().expect("tend connection just opened");
        match Message::info(policy, conn, commands).await {
            Ok(map) => Ok(map),
            Err(e) => {
                // Drop the socket so the next call reopens — the error
                // could leave the read buffer mid-frame.
                if let Some(mut bad) = guard.take() {
                    bad.close();
                }
                Err(e)
            }
        }
    }

    /// Tear down the tend connection if open. Called from `close()` and on
    /// quick-restart so the next call opens a fresh socket.
    pub async fn close_tend_connection(&self) {
        let mut guard = self.tend_connection.lock().await;
        if let Some(mut c) = guard.take() {
            c.close();
        }
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

    // ---- Per-node circuit breaker ---------------------------------------
    //
    // Mirrors Java's `Node.{incrErrorRate, resetErrorRate, errorRateWithinLimit,
    // validateErrorCount}`. The breaker is a soft fence: it rejects the
    // *next* command at this node, not in-flight ones, and resets every
    // `error_rate_window` tend iterations.

    /// Increment the per-node error counter. No-op when the cluster
    /// breaker is disabled (`max_error_rate == 0`).
    pub fn incr_error_rate(&self) {
        if self.client_policy.max_error_rate > 0 {
            self.error_rate_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `true` when the breaker is disabled or the count is still under
    /// the cluster-level threshold.
    pub fn error_rate_within_limit(&self) -> bool {
        let cluster_max = self.client_policy.max_error_rate;
        cluster_max == 0 || self.error_rate_count.load(Ordering::Relaxed) <= cluster_max
    }

    /// Returns `Err(MaxErrorRate(addr))` when the breaker has tripped.
    /// Use before sending a command at this node.
    pub fn validate_error_count(&self) -> Result<()> {
        if self.error_rate_within_limit() {
            Ok(())
        } else {
            Err(Error::MaxErrorRate(self.address.clone()))
        }
    }

    /// Called once per `error_rate_window` tend iterations to roll the
    /// counter forward. Adapts the per-node ceiling exactly like Java's
    /// `Node.resetErrorRate`: the previous-window's `count` is compared
    /// against the *per-node* ceiling (not the cluster cap). Previous
    /// window clean → next ceiling doubles (capped at cluster max);
    /// previous window tripped → next ceiling halves with a floor of 1.
    pub fn reset_error_rate(&self) {
        let cluster_max = self.client_policy.max_error_rate;
        if cluster_max == 0 {
            return;
        }
        let count = self.error_rate_count.swap(0, Ordering::Relaxed);
        let prev_ceiling = self.node_max_error_rate.load(Ordering::Relaxed);
        let next_ceiling = if count <= prev_ceiling {
            prev_ceiling.saturating_mul(2).min(cluster_max)
        } else if prev_ceiling >= 2 {
            prev_ceiling / 2
        } else {
            1
        };
        self.node_max_error_rate
            .store(next_ceiling, Ordering::Relaxed);
    }

    /// Current error-rate sample, exposed for diagnostics / metrics.
    pub fn error_rate_count(&self) -> usize {
        self.error_rate_count.load(Ordering::Relaxed)
    }

    /// Current per-node error-rate ceiling. Adapts on every call to
    /// [`reset_error_rate`](Self::reset_error_rate); converges back to the
    /// cluster setting when windows stay clean. Mostly useful for
    /// diagnostics / tests — production code paths consult the cluster
    /// cap, not this value.
    pub fn node_max_error_rate(&self) -> usize {
        self.node_max_error_rate.load(Ordering::Relaxed)
    }

    pub(crate) async fn send_user_agent_id(&self) {
        if !self.version().supports_app_id() {
            return;
        }

        let app_id = self.client_policy().application_id();

        // Source user-agent payload
        // Format: "1,rust-<version>,<application-id>"
        let user_agent_id = format!("1,rust-{CLIENT_VERSION},{app_id}");
        let user_agent_id = BASE64.encode(&user_agent_id);
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
        if self.is_active() {
            let mut count = 0;

            let client_policy = self.client_policy();
            if client_policy.min_conns_per_node > 0 {
                // `saturating_sub` so a burst that puts the pool above the
                // configured min doesn't underflow (was a panic before).
                let to_fill = client_policy
                    .min_conns_per_node
                    .saturating_sub(self.connection_pool.num_conns());
                for _ in 0..to_fill {
                    self.connection_pool.make_conn(count).await?;
                    count += 1;
                }
            }

            Ok(count)
        } else {
            Err(Error::InvalidNode(format!(
                "Cannot fill the connection pool to 'policy.min_conns_per_node'. The node `{self}` is inactive"
            )))
        }
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

#[cfg(test)]
mod node_tests {
    use std::sync::Arc;

    use crate::cluster::node_validator::NodeValidator;
    use crate::errors::Error;
    use crate::net::Host;
    use crate::policy::ClientPolicy;
    use crate::Version;

    use super::Node;

    fn test_node() -> Node {
        let policy = ClientPolicy::default();
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
            detect_load_balancer: false,
        });
        Node::new(policy, nv)
    }

    /// One idle connection in the pool, using the test [`crate::net::Connection`] (no real socket).
    async fn create_node_with_connection() -> Node {
        let node = test_node();
        let pconn = node
            .connection_pool
            .make_conn(0)
            .await
            .expect("make_conn uses test Connection");
        node.put_connection(pconn);
        assert_eq!(node.connection_pool.num_conns(), 1);
        node
    }

    #[aerospike_macro::test]
    async fn get_connection_returns_invalid_node_when_inactive() {
        let node = create_node_with_connection().await;
        let before = node.connection_pool.num_conns();
        node.close();
        assert!(!node.is_active());

        let err = node.get_connection(0).await.unwrap_err();
        match err {
            Error::InvalidNode(msg) => assert!(msg.contains("inactive"), "unexpected: {}", msg),
            other => panic!("expected InvalidNode, got {:?}", other),
        }
        assert_eq!(
            node.connection_pool.num_conns(),
            before,
            "inactive node must not open or hand out pool connections"
        );
    }

    #[aerospike_macro::test]
    async fn put_connection_does_not_return_conn_to_pool_when_inactive() {
        let node = create_node_with_connection().await;
        let pconn = node
            .get_connection(0)
            .await
            .expect("active node with one mock conn in pool");
        assert_eq!(node.connection_pool.num_conns(), 0);

        node.close();
        assert!(!node.is_active());

        node.put_connection(pconn);
        assert_eq!(
            node.connection_pool.num_conns(),
            0,
            "inactive node must not return connections to the pool"
        );
    }

    #[aerospike_macro::test]
    async fn node_drop_inactivates_and_closes_pool_when_last_arc_dropped() {
        let arc = Arc::new(create_node_with_connection().await);
        let queue_witness = {
            let pconn = arc
                .get_connection(0)
                .await
                .expect("pool should have one connection");
            let q = pconn.queue.clone();
            arc.put_connection(pconn);
            q
        };
        let weak = Arc::downgrade(&arc);
        assert_eq!(Arc::strong_count(&arc), 1);

        assert!(arc.is_active());
        assert_eq!(arc.connection_pool.num_conns(), 1);

        drop(arc);
        assert!(
            weak.upgrade().is_none(),
            "expected Node to be dropped after the last Arc was released"
        );
        assert_eq!(
            queue_witness.num_conns(),
            0,
            "Node::drop should clear pooled connections"
        );
    }
}
