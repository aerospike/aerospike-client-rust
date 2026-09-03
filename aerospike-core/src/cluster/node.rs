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

use std::collections::HashMap;
use std::time::Duration;
use indexmap::IndexMap;
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
use crate::metrics::NodeMetrics;
use crate::net::{Connection, ConnectionPool, Host, PooledConnection, TailVerdict};
use crate::policy::{AdminPolicy, ClientPolicy};
use crate::Version;

pub const PARTITIONS: usize = 4096;
pub const PARTITION_GENERATION: &str = "partition-generation";
pub const PEERS_GENERATION: &str = "peers-generation";
pub const REBALANCE_GENERATION: &str = "rebalance-generation";

/// Safety margin added to the keep-alive look-ahead: a floor connection is
/// healed once its idle deadline is within `tend_interval + this`, so a late
/// or skipped tend pass cannot let it expire in the pool.
const KEEPALIVE_MARGIN: Duration = Duration::from_secs(1);

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
    /// so the next tend will reopen.
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
    /// or halving when the previous window tripped.
    error_rate_count: AtomicUsize,
    node_max_error_rate: AtomicUsize,
    /// Cached hostname that resolves to this node's IP. Populated by
    /// `Cluster::peer_exists` on the first successful DNS-aware match, so
    /// subsequent tends can short-circuit the lookup.
    hostname: std::sync::OnceLock<String>,
    /// Per-node metrics. Shared with the connection pool so connection
    /// lifecycle events are recorded against the same sink.
    metrics: Arc<NodeMetrics>,
    /// Cluster-wide count of connections currently being opened by background
    /// fill tasks. Shared across every node of the cluster and checked against
    /// `ClientPolicy::opening_connection_threshold` before spawning a fill —
    /// mirrors the Go client's `Cluster.connectionThreshold`.
    opening_connections: Arc<AtomicUsize>,
}

/// Await a spawned probe task. A panicked task degrades to a failed probe
/// (`None`) on both runtimes, so the tend task survives.
async fn await_spawned_task<T>(handle: aerospike_rt::task::JoinHandle<Option<T>>) -> Option<T> {
    use futures::FutureExt;
    std::panic::AssertUnwindSafe(async move {
        #[cfg(feature = "rt-tokio")]
        return handle.await.ok().flatten(); 

        #[cfg(feature = "rt-async-std")]
        return handle.await; // task panic re-raises here → caught below
    })
    .catch_unwind()
    .await
    .ok() // Err(payload) = a panic was caught → None
    .flatten()
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
    pub fn new(
        client_policy: ClientPolicy,
        nv: Arc<NodeValidator>,
        metrics: Arc<NodeMetrics>,
        opening_connections: Arc<AtomicUsize>,
        buffer_pool: Option<Arc<crate::net::buffer_pool::TieredBufferPool>>,
    ) -> Self {
        Node {
            opening_connections,
            client_policy: client_policy.clone(),
            name: nv.name.clone(),
            aliases: AtomicArc::from(nv.aliases.clone()),
            address: nv.address.clone(),

            host: nv.aliases[0].clone(),
            rebalance_generation: AtomicIsize::new(if client_policy.rack_aware() {
                -1
            } else {
                0
            }),
            connection_pool: ConnectionPool::new(
                nv.aliases[0].clone(),
                client_policy.clone(),
                Some(metrics.clone()),
                buffer_pool,
            ),
            metrics,
            tend_connection: AsyncMutex::new(None),
            failures: AtomicUsize::new(0),
            error_rate_count: AtomicUsize::new(0),
            node_max_error_rate: AtomicUsize::new(client_policy.max_error_rate),
            partition_generation: AtomicIsize::new(-1),
            peers_generation: AtomicIsize::new(-1),
            peers_count: AtomicUsize::new(0),
            // `partition_changed` means "this node's parsed partition
            // generation differs from the server's", which for a brand-new
            // node is true by definition: `partition_generation` starts at
            // -1, and every real generation is >= 0. Starting it `true` lets
            // the tend cycle that *creates* the node also fetch its partition
            // map, instead of leaving the work to the next cycle: the seeding
            // cycle takes its node snapshot before seeding, so phase 1 — the
            // only other place this flag is raised — never sees freshly
            // seeded nodes, and phase 4 would otherwise find nothing to do.
            partition_changed: AtomicBool::new(true),
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
    /// second call — first writer wins.
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
    /// cycle, before peer refresh.
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
    /// Sends lightweight info commands to verify node identity and check
    /// peers / partition / rebalance generations. Does NOT fetch the full
    /// peer list — that happens in [`refresh_peers`](Self::refresh_peers)
    /// only when `peers.gen_changed` ends up true.
    pub async fn refresh(&self, peers: &Peers) -> Result<()> {
        if !self.is_active() {
            return Ok(());
        }

        let rack_aware = self.client_policy.rack_aware();

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
    pub async fn refresh_peers(&self, peers: &Peers) -> Result<()> {
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
                return Err(Error::bad_response("Missing peers list".to_string()));
            }
            Some(s) if s.is_empty() => return Ok(()),
            Some(s) => s,
        };

        let result = PeersParser::new(peer_string)
            .with_ip_map(self.client_policy.ip_map.as_ref())
            .parse()
            .inspect_err(|_e| {
                self.refresh_failed();
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
    /// node was reachable.
    pub fn commit_peers_generation(&self, generation: isize) {
        self.peers_generation.store(generation, Ordering::Relaxed);
    }

    /// Called when a refresh step fails. Resets generation numbers to force
    /// re-discovery on the next tend cycle.
    fn refresh_failed(&self) {
        self.peers_generation.store(-1, Ordering::Relaxed);
        self.partition_generation.store(-1, Ordering::Relaxed);

        if self.client_policy.rack_aware() {
            self.rebalance_generation.store(-1, Ordering::Relaxed);
        }

        self.increase_failures();
    }

    /// Parses `peers-generation` from `info_map` and compares with the
    /// stored value. Sets `peers.gen_changed = true` if they differ.
    ///
    /// When the server's reported generation goes *backward* (`stored > gen`)
    /// the node almost certainly quick-restarted: it forgot us and reset
    /// its peers list. Surface that in the log and reset our retry-error
    /// rate so the recovered node isn't immediately punished for the
    /// pre-restart failure history.
    fn verify_peers_generation(
        &self,
        info_map: &IndexMap<String, String>,
        peers: &Peers,
    ) -> Result<()> {
        let gen_str = info_map
            .get(PEERS_GENERATION)
            .ok_or_else(|| Error::bad_response("Missing peers-generation".to_string()))?;
        let gen = gen_str.parse::<isize>()?;

        let stored = self.peers_generation.load(Ordering::Relaxed);
        if stored != gen {
            peers.set_gen_changed(true);

            if stored > gen && stored != -1 {
                info!("Quick node restart detected: node={self} oldgen={stored} newgen={gen}");
                // Drop accumulated failure count so the freshly-restarted
                // node is treated like a healthy peer until proven
                // otherwise this tend.
                self.reset_failures();
            }
        }
        Ok(())
    }

    fn validate_node(&self, info_map: &IndexMap<String, String>) -> Result<()> {
        self.verify_node_name(info_map)?;
        self.verify_cluster_name(info_map)?;
        Ok(())
    }

    fn verify_node_name(&self, info_map: &IndexMap<String, String>) -> Result<()> {
        match info_map.get("node") {
            None => Err(Error::invalid_node("Missing node name".to_string())),
            Some(info_name) if info_name == &self.name => Ok(()),
            Some(info_name) => {
                self.inactivate();
                Err(Error::invalid_node(format!(
                    "Node name has changed: '{}' => '{}'",
                    self.name, info_name
                )))
            }
        }
    }

    #[allow(clippy::option_if_let_else)]
    fn verify_cluster_name(&self, info_map: &IndexMap<String, String>) -> Result<()> {
        match self.client_policy.cluster_name {
            None => Ok(()),
            Some(ref expected) => match info_map.get("cluster-name") {
                None => Err(Error::invalid_node("Missing cluster name".to_string())),
                Some(info_name) if info_name == expected => Ok(()),
                Some(info_name) => {
                    self.inactivate();
                    Err(Error::invalid_node(format!(
                        "Cluster name mismatch: expected={expected},
                                                           got={info_name}"
                    )))
                }
            },
        }
    }

    /// Compares the server's partition-generation with the node's last known value.
    /// Sets `partition_changed` flag if they differ.
    fn verify_partition_generation(&self, info_map: &IndexMap<String, String>) -> Result<()> {
        match info_map.get(PARTITION_GENERATION) {
            None => Err(Error::bad_response(
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
    fn verify_rebalance_generation(&self, info_map: &IndexMap<String, String>) -> Result<()> {
        match info_map.get(REBALANCE_GENERATION) {
            None => Err(Error::bad_response(
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

    pub fn update_partitions(&self, info_map: &IndexMap<String, String>) -> Result<()> {
        match info_map.get(PARTITION_GENERATION) {
            None => {
                return Err(Error::bad_response(
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

    pub fn update_rebalance_generation(&self, info_map: &IndexMap<String, String>) -> Result<()> {
        if let Some(gen_string) = info_map.get(REBALANCE_GENERATION) {
            let gen = gen_string.parse::<isize>()?;
            self.rebalance_generation.store(gen, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Returns true if this node hosts the given namespace on exactly the
    /// given rack (Java `Node.hasRack`). Rack preference ordering is the
    /// caller's concern — see `Partition::get_rack_node`.
    pub fn has_rack(&self, namespace: &str, rack_id: usize) -> bool {
        self.rack_ids
            .load()
            .get(namespace)
            .is_some_and(|r| *r == rack_id)
    }

    pub fn parse_rack(&self, buf: &str) -> Result<()> {
        let new_table = buf
            .split(';')
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                let (key, val) = entry
                    .split_once(':')
                    .ok_or(Error::bad_response("Invalid rack entry"))?;
                let ns = key.trim();
                // Aerospike server enforces 1..=31 for namespace names.
                // Reject anything outside that to avoid populating the rack
                // table with poisoned entries (mirrors Java's RackParser).
                if ns.is_empty() || ns.len() >= 32 {
                    return Err(Error::bad_response(format!(
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
            return Err(Error::invalid_node(format!(
                "Cannot get a connection for node. The node `{self}` is inactive"
            )));
        }

        if let Ok(conn) = self.connection_pool.get(hint) {
            return Ok(conn);
        }

        // Pool had no ready connection. Hand the expensive open (TCP connect +
        // TLS + login) to a detached background task and report
        // `ConnectionPoolEmpty` so the caller's retry loop paces itself while
        // the connection is prepared — the handshake never runs inside a
        // command's latency. Mirrors the Go client's `makeConnectionForPool`.
        self.metrics.incr_connections_pool_empty();
        self.spawn_background_conn_fill(hint);
        Err(Error::pool_empty())
    }

    /// Spawns a detached task that opens one connection and parks it in the
    /// pool. No-op when the cluster-wide
    /// [`ClientPolicy::opening_connection_threshold`] is reached (another
    /// in-flight open will refill the pool) or when every queue is already at
    /// capacity (all slots hold live or in-flight connections — the retrying
    /// caller will pick one up as it is returned).
    fn spawn_background_conn_fill(&self, hint: u8) {
        let threshold = self.client_policy.opening_connection_threshold;
        let opening = self.opening_connections.clone();
        if threshold > 0 && opening.fetch_add(1, Ordering::Relaxed) + 1 > threshold {
            opening.fetch_sub(1, Ordering::Relaxed);
            return;
        }

        let Some(queue) = self.connection_pool.reserve_queue(usize::from(hint)) else {
            if threshold > 0 {
                opening.fetch_sub(1, Ordering::Relaxed);
            }
            return;
        };

        aerospike_rt::spawn(async move {
            // The slot was reserved above; settle it either way. `make_conn`
            // records the attempt/success/failure connection metrics itself.
            match queue.make_conn().await {
                Ok(conn) => queue.put_back(conn),
                Err(_) => queue.reduce_capacity(),
            }
            if threshold > 0 {
                opening.fetch_sub(1, Ordering::Relaxed);
            }
        });
    }

    /// Returns the per-node metrics sink.
    pub fn metrics(&self) -> &Arc<NodeMetrics> {
        &self.metrics
    }

    /// Number of connections currently owned by this node (open-connections
    /// gauge for metrics).
    pub fn open_connections(&self) -> u64 {
        self.connection_pool.reserved_conns() as u64
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

    /// Send info commands to this node and return the parsed key/value
    /// response. The map preserves the server's response order — one entry
    /// per command, in request order. For an arbitrary-node request use
    /// [`Client::info`](crate::Client::info).
    pub async fn info(
        &self,
        policy: &AdminPolicy,
        commands: &[&str],
    ) -> Result<IndexMap<String, String>> {
        // `get_connection` reports `ConnectionPoolEmpty` while a background
        // task opens the connection; this method has no retry loop of its
        // own, so poll briefly until the connection lands or the admin
        // timeout elapses (mirrors the Go client's public `GetConnection`).
        let deadline = aerospike_rt::time::Instant::now()
            + std::time::Duration::from_millis(u64::from(policy.timeout()).max(1000));
        let mut conn = loop {
            match self.get_connection(0).await {
                Ok(conn) => break conn,
                Err(err)
                    if err.is_pool_empty() && aerospike_rt::time::Instant::now() < deadline =>
                {
                    aerospike_rt::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(e) => return Err(e),
            }
        };
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
    /// next call reopens. Use this for tend-time traffic only —
    /// operational commands should go through the pool via
    /// [`info`](Self::info).
    pub async fn tend_info(
        &self,
        policy: &AdminPolicy,
        commands: &[&str],
    ) -> Result<IndexMap<String, String>> {
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
            Err(Error::max_error_rate(self.address.clone()))
        }
    }

    /// Called once per `error_rate_window` tend iterations to roll the
    /// counter forward. Adapts the per-node ceiling: the previous
    /// window's `count` is compared against the *per-node* ceiling (not
    /// the cluster cap). Previous window clean → next ceiling doubles
    /// (capped at cluster max); previous window tripped → next ceiling
    /// halves with a floor of 1.
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

        let policy = self.client_policy();

        // The client policy may override the
        // assembled payload via `custom_client_id`; otherwise uses
        // the default `"1,rust-<version>,<application-id>"` format.
        let client_id = match &policy.custom_client_id {
            Some(custom) => custom.to_owned(),
            None => format!("rust-{CLIENT_VERSION}"),
        };
        let user_agent_id = format!("1,{},{}", client_id, policy.application_id());
        let user_agent_id = BASE64.encode(&user_agent_id);
        let user_agent_command = format!("user-agent-set:value={user_agent_id}");

        let admin_policy = AdminPolicy {
            timeout: policy.timeout,
        };
        let _ = self.info(&admin_policy, &[&user_agent_command]).await;
    }

    /// Reap idle connections, but for any idle connection that would take
    /// the pool below `min_conns_per_node`, send a cheap info probe to keep
    /// it alive instead of dropping it. A successful probe reads a response
    /// which in turn resets the connection's idle deadline (`Message::info`
    /// calls `conn.refresh()` internally), so the probed connection goes
    /// back into the pool as fresh.
    ///
    /// Queues are swept round-robin, one verdict per queue per turn, so the
    /// retire budget is spent evenly across queues instead of draining them
    /// in order. All keep-alive probes complete before this function returns,
    /// so the pool is settled and the returned count is final.
    ///
    /// Returns the number of connections processed (reaped + refreshed).
    pub async fn reap_and_refresh_idle_connections(&self) -> usize {
        let policy = &self.client_policy;
        if policy.conn_pools_per_node == 0 {
            return 0;
        }

        // Global budget shared across queues: how many may be closed without
        // taking the node below `min_conns_per_node`.
        let mut droppable = self
            .connection_pool
            .total_reserved()
            .saturating_sub(policy.min_conns_per_node);

        // A floor connection whose idle deadline falls inside this horizon
        // would expire before tend can look again — heal it now.
        let expiry_horizon =
            std::time::Duration::from_millis(u64::from(policy.tend_interval)) + KEEPALIVE_MARGIN;

        let probe_policy = AdminPolicy {
            // Tight timeout — this is a keep-alive probe, not a command.
            timeout: policy.timeout.min(2000),
        };

        // Cycle through queues one verdict at a time so the retire budget is spread
        // evenly instead of draining queues in order. A full lap of quiet verdicts
        // (fresh, empty, or contended) ends the sweep. With no queues, cycle() is empty.
        //
        // Probed connections are put back only after this loop. Putting one back here
        // could make it immediately eligible again—especially when
        // idle_timeout <= tend_interval + KEEPALIVE_MARGIN—and cause an infinite sweep.
        // `take` is the safety bound: capacity-bounded pops should terminate normally;
        // it only protects against an accidental non-terminating sweep.
        let mut probe_handles = Vec::new();
        let mut total_processed = 0usize;
        let queues = self.connection_pool.queues();
        let max_probe_attempts = queues.len() * (policy.max_conns_per_node + 1);
        let mut queues_without_work = 0;
        for queue in queues.iter().cycle().take(max_probe_attempts) {
            match queue.inspect_tail(droppable > 0, expiry_horizon) {
                Some(TailVerdict::Retire(conn)) => {
                    drop(conn);
                    queue.reduce_capacity();
                    droppable = droppable.saturating_sub(1);
                    self.metrics.incr_connections_idle_dropped();
                    self.metrics.incr_connections_closed();
                    total_processed += 1;
                    queues_without_work = 0;
                }
                Some(TailVerdict::KeepAlive(conn)) => {
                    // Probe runs concurrently from here; pool mutation waits below.
                    // The queue stays outside the task so its slot can be freed
                    // even if the task dies.
                    let pp = probe_policy;
                    let handle = aerospike_rt::spawn(async move {
                        let mut conn = conn;
                        match Message::info(&pp, &mut conn, &["node"]).await {
                            Ok(_) => Some(conn),
                            Err(_) => None,
                        }
                    });
                    probe_handles.push((queue.clone(), handle));
                    queues_without_work = 0;
                }
                // Settled (front is fresh, so everything behind it is fresher) or
                // contended (leave it for traffic and re-check it on the next lap).
                None | Some(TailVerdict::Settled) => {
                    queues_without_work += 1;
                    if queues_without_work == queues.len() {
                        break;
                    }
                }
            }
        }

        // Each handle is paired with the queue its connection came from, and
        // the task returns the connection itself on success. The probes have
        // been running since spawn, so awaiting them in order costs only the
        // slowest one. A healed connection re-pools into its own queue, and a
        // failed one frees that queue's reserved slot.
        for (queue, handle) in probe_handles {
            match await_spawned_task(handle).await {
                Some(conn) => queue.put_back(conn),
                None => {
                    queue.reduce_capacity();
                    self.metrics.incr_connections_closed();
                }
            }
            total_processed += 1;
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
                // Compare against total owned connections (idle + in-flight),
                // not just idle connections sitting in the queue. Using
                // `num_conns()` here would ignore checked-out connections and
                // create unnecessary replacements every tend cycle.
                let to_fill = client_policy
                    .min_conns_per_node
                    .saturating_sub(self.connection_pool.total_reserved());
                for _ in 0..to_fill {
                    self.connection_pool.make_conn(count).await?;
                    count += 1;
                }
            }

            Ok(count)
        } else {
            Err(Error::invalid_node(format!(
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

/// Builds a minimal, connection-less `Node` pinned to `version`, for unit
/// tests elsewhere in the crate that need to exercise version-gated
/// wire-encode paths (e.g. `commands::buffer::tests`) without a live server.
#[cfg(test)]
pub(crate) fn test_node_with_version(version: crate::Version) -> Node {
    let policy = ClientPolicy::default();
    let nv = Arc::new(NodeValidator {
        name: "test-node".to_string(),
        aliases: vec![Host::new("127.0.0.1", 3000)],
        address: "127.0.0.1:3000".to_string(),
        client_policy: policy.clone(),
        use_new_info: true,
        version,
        detect_load_balancer: false,
    });
    let metrics = Arc::new(crate::metrics::NodeMetrics::new(
        crate::metrics::MetricsPolicy::default(),
    ));
    Node::new(policy, nv, metrics, Arc::new(AtomicUsize::new(0)), None)
}

#[cfg(test)]
mod node_tests {
    use std::sync::Arc;

    use crate::cluster::node_validator::NodeValidator;
    use crate::net::Host;
    use crate::policy::ClientPolicy;
    use crate::Version;

    use super::{test_node_with_version, Node};

    fn test_node() -> Node {
        test_node_with_version(Version::default())
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
        assert!(
            matches!(err.kind(), crate::ErrorKind::InvalidNode),
            "expected InvalidNode, got {err:?}"
        );
        assert!(
            err.to_string().contains("inactive"),
            "unexpected: {err}"
        );
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

    #[aerospike_macro::test]
    async fn get_connection_miss_uses_hint_to_select_queue() {
        // A pool miss must report `ConnectionPoolEmpty` and hand the open to a
        // background task, honoring the hint for queue selection.
        let policy = ClientPolicy {
            conn_pools_per_node: 4,
            max_conns_per_node: 8, // 2 per queue — room to observe distribution
            ..ClientPolicy::default()
        };
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
            detect_load_balancer: false,
        });
        let metrics = Arc::new(crate::metrics::NodeMetrics::new(
            crate::metrics::MetricsPolicy::default(),
        ));
        let node = Node::new(policy, nv, metrics, Arc::new(std::sync::atomic::AtomicUsize::new(0)), None);

        // Trigger 4 pool misses with distinct hints — each reports pool-empty
        // and spawns a background fill on its own queue.
        for hint in 0..4u8 {
            let err = node.get_connection(hint).await.unwrap_err();
            assert!(
                err.is_pool_empty(),
                "pool miss must report ConnectionPoolEmpty, got {err:?}"
            );
        }

        // Let the spawned fill tasks run (test connections are dummies, so
        // they complete on the next scheduler passes).
        for _ in 0..20 {
            aerospike_rt::sleep(aerospike_rt::time::Duration::from_millis(1)).await;
            if node.connection_pool.total_reserved() == 4 {
                break;
            }
        }

        let queues = node.connection_pool.queues();
        for i in 0..4 {
            assert_eq!(
                queues[i].reserved_count(),
                1,
                "queue[{i}] must have exactly 1 reserved connection"
            );
        }
    }

    /// A pool miss spawns a background fill; a subsequent `get_connection`
    /// (after the fill lands) succeeds from the pool — the retry pattern the
    /// command loops rely on.
    #[cfg(feature = "rt-tokio")]
    #[tokio::test(flavor = "current_thread")]
    async fn get_connection_retry_picks_up_background_fill() {
        let node = test_node();

        let err = node.get_connection(0).await.unwrap_err();
        assert!(err.is_pool_empty());

        // Let the spawned fill task run (dummy connection, completes fast).
        for _ in 0..20 {
            aerospike_rt::sleep(aerospike_rt::time::Duration::from_millis(1)).await;
            if node.connection_pool.num_conns() == 1 {
                break;
            }
        }

        let conn = node
            .get_connection(0)
            .await
            .expect("retry must pick up the background-filled connection");
        drop(conn);
    }

    /// `opening_connection_threshold` caps concurrent background opens
    /// cluster-wide: with a threshold of 1, a second miss while the first
    /// open is still pending must not spawn another fill.
    #[cfg(feature = "rt-tokio")]
    #[tokio::test(flavor = "current_thread")]
    async fn opening_connection_threshold_caps_background_fills() {
        let policy = ClientPolicy {
            opening_connection_threshold: 1,
            ..ClientPolicy::default()
        };
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
            detect_load_balancer: false,
        });
        let metrics = Arc::new(crate::metrics::NodeMetrics::new(
            crate::metrics::MetricsPolicy::default(),
        ));
        let node = Node::new(
            policy,
            nv,
            metrics,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            None,
        );

        // On a current-thread runtime no spawned task runs until we await, so
        // both calls observe the first fill still "in flight".
        let _ = node.get_connection(0).await.unwrap_err(); // spawns (1 <= threshold)
        let _ = node.get_connection(0).await.unwrap_err(); // capped (2 > threshold)

        for _ in 0..20 {
            aerospike_rt::sleep(aerospike_rt::time::Duration::from_millis(1)).await;
        }
        assert_eq!(
            node.connection_pool.total_reserved(),
            1,
            "only one background fill may run under threshold 1"
        );
    }

    #[aerospike_macro::test]
    async fn fill_min_conns_does_not_overshoot_when_connections_in_flight() {
        let policy = ClientPolicy {
            min_conns_per_node: 5,
            max_conns_per_node: 10,
            ..ClientPolicy::default()
        };
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
            detect_load_balancer: false,
        });
        let metrics = Arc::new(crate::metrics::NodeMetrics::new(
            crate::metrics::MetricsPolicy::default(),
        ));
        let node = Node::new(policy, nv, metrics, Arc::new(std::sync::atomic::AtomicUsize::new(0)), None);

        let mut in_flight = Vec::new();
        for i in 0..5 {
            let pconn = node
                .connection_pool
                .make_conn(i)
                .await
                .expect("make_conn failed");
            in_flight.push(pconn);
        }
        assert_eq!(node.connection_pool.total_reserved(), 5);
        assert_eq!(
            node.connection_pool.num_conns(),
            0,
            "all connections should be in-flight"
        );

        let created = node.fill_min_conns().await.expect("fill_min_conns failed");
        assert_eq!(
            created, 0,
            "fill_min_conns must not create connections when total_reserved already meets min"
        );
        assert_eq!(node.connection_pool.total_reserved(), 5);

        drop(in_flight);
    }
}

/// LIFO pool-health tests. A loopback fake node stands in for `asd`: dead
/// mode FINs every accepted socket, live mode answers info probes. Each test
/// drives one or more reaper passes directly — the same method tend calls.
#[cfg(all(test, feature = "rt-tokio"))]
mod pool_health_tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use aerospike_rt::net::TcpListener;

    use crate::cluster::node_validator::NodeValidator;
    use crate::net::Host;
    use crate::policy::ClientPolicy;
    use crate::Version;

    use super::Node;

    /// Fake node; the flag toggles dead (FIN on accept) vs live (answers info).
    async fn spawn_fake_node(dead: bool) -> (SocketAddr, Arc<AtomicBool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dead_flag = Arc::new(AtomicBool::new(dead));
        let flag = dead_flag.clone();
        aerospike_rt::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                if flag.load(Ordering::SeqCst) {
                    let _ = sock.shutdown().await;
                    drop(sock);
                } else {
                    aerospike_rt::spawn(async move {
                        loop {
                            // Info framing: 8-byte header + payload.
                            let mut header = [0u8; 8];
                            if sock.read_exact(&mut header).await.is_err() {
                                return;
                            }
                            let mut len8 = [0u8; 8];
                            len8[2..8].copy_from_slice(&header[2..8]);
                            let len = u64::from_be_bytes(len8) as usize;
                            let mut payload = vec![0u8; len];
                            if sock.read_exact(&mut payload).await.is_err() {
                                return;
                            }
                            let body: &[u8] = b"node\tFAKEPEER\n";
                            let mut resp = Vec::with_capacity(8 + body.len());
                            resp.push(2);
                            resp.push(1);
                            resp.extend_from_slice(&(body.len() as u64).to_be_bytes()[2..8]);
                            resp.extend_from_slice(body);
                            if sock.write_all(&resp).await.is_err() {
                                return;
                            }
                        }
                    });
                }
            }
        });
        (addr, dead_flag)
    }

    fn node_against(addr: SocketAddr, policy: ClientPolicy) -> Node {
        let nv = Arc::new(NodeValidator {
            name: "fake-node".to_string(),
            aliases: vec![Host::new(&addr.ip().to_string(), addr.port())],
            address: addr.to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
            detect_load_balancer: false,
        });
        let metrics = Arc::new(crate::metrics::NodeMetrics::new(
            crate::metrics::MetricsPolicy::default(),
        ));
        Node::new(policy, nv, metrics, Arc::new(AtomicUsize::new(0)), None)
    }

    /// Park `n` real TCP conns (make_conn's test shim has no socket).
    async fn park_conns(node: &Node, addr: SocketAddr, policy: &ClientPolicy, n: usize) {
        let queue = &node.connection_pool.queues()[0];
        for _ in 0..n {
            let stream = aerospike_rt::net::TcpStream::connect(addr)
                .await
                .expect("connect to fake node");
            let conn = crate::net::Connection::test_from_tcp_stream(stream, policy);
            assert!(queue.reserve_capacity());
            queue.put_back(conn);
        }
        // Let the peer's accept/FIN land.
        aerospike_rt::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(node.connection_pool.num_conns(), n);
    }

    fn test_policy(idle_timeout: u32, min_conns: usize) -> ClientPolicy {
        ClientPolicy {
            idle_timeout,
            min_conns_per_node: min_conns,
            tend_interval: 250,
            // Generous, so a probe cannot time out on a loaded machine.
            timeout: 5_000,
            ..ClientPolicy::default()
        }
    }

    // ─── LIFO ordering ────────────────────────────────────────────────────

    /// Checkout returns the most recently returned connection; a reused
    /// connection stays on top while the rest of the pool ages untouched.
    #[aerospike_macro::test]
    async fn checkout_is_lifo() {
        let (addr, _dead) = spawn_fake_node(false).await;
        let policy = test_policy(0, 0);
        let node = node_against(addr, policy.clone());
        park_conns(&node, addr, &policy, 3).await;

        let queue = &node.connection_pool.queues()[0];
        let first = queue.get().expect("conn");
        drop(first); // returns to the top
        let again = queue.get().expect("conn");
        // With three parked conns and LIFO order, num_conns dropped by one and
        // the same top slot cycles; the two older conns were never touched.
        assert_eq!(node.connection_pool.num_conns(), 2);
        drop(again);
    }

    // ─── retention: surplus retires, floor survives ───────────────────────

    /// The regression gate: surplus retires across repeated tends even while
    /// tend keeps running. Under FIFO+probe designs the maintenance itself
    /// kept renewing the surplus deadlines and the pool never shrank.
    #[aerospike_macro::test]
    async fn surplus_retires_across_repeated_tends() {
        let (addr, _dead) = spawn_fake_node(false).await;
        let policy = test_policy(600, 1); // retire after 600ms, keep 1 warm
        let node = node_against(addr, policy.clone());
        park_conns(&node, addr, &policy, 4).await;

        for _ in 0..4 {
            aerospike_rt::sleep(std::time::Duration::from_millis(300)).await;
            node.reap_and_refresh_idle_connections().await;
        }

        assert_eq!(
            node.connection_pool.total_reserved(),
            1,
            "surplus must retire down to min_conns_per_node across repeated tends"
        );
    }

    /// A connection short of its idle deadline is left alone — no drop, no
    /// probe. Touching it would renew the deadline and keep surplus alive.
    #[aerospike_macro::test]
    async fn unexpired_conns_are_left_alone() {
        let (addr, _dead) = spawn_fake_node(false).await;
        let policy = test_policy(60_000, 0); // one minute: nothing expires here
        let node = node_against(addr, policy.clone());
        park_conns(&node, addr, &policy, 2).await;

        let processed = node.reap_and_refresh_idle_connections().await;
        assert_eq!(processed, 0, "nothing is expired, so tend must touch nothing");
        assert_eq!(node.connection_pool.num_conns(), 2);
    }

    /// A floor connection past its deadline (e.g. after a missed pass) is
    /// healed — probed and re-pooled — not stranded or dropped.
    #[aerospike_macro::test]
    async fn expired_floor_conn_is_healed() {
        let (addr, _dead) = spawn_fake_node(false).await;
        let policy = test_policy(300, 1); // floor of 1
        let node = node_against(addr, policy.clone());
        park_conns(&node, addr, &policy, 1).await;

        aerospike_rt::sleep(std::time::Duration::from_millis(400)).await; // expired

        let processed = node.reap_and_refresh_idle_connections().await;
        assert_eq!(processed, 1, "the floor conn must be probed");
        assert_eq!(
            node.connection_pool.num_conns(),
            1,
            "a live floor conn survives its probe and returns to the pool"
        );
        // And checkout can use it immediately (no expired-discard at checkout).
        assert!(node.connection_pool.queues()[0].get().is_ok());
    }

    /// A dead floor connection fails its probe and is evicted, freeing the
    /// slot for fill_min_conns to replace.
    #[aerospike_macro::test]
    async fn dead_floor_conn_is_evicted_by_probe() {
        let (addr, _dead) = spawn_fake_node(true).await; // peer FINs everything
        let policy = test_policy(300, 1);
        let node = node_against(addr, policy.clone());
        park_conns(&node, addr, &policy, 1).await;

        aerospike_rt::sleep(std::time::Duration::from_millis(400)).await;

        node.reap_and_refresh_idle_connections().await;
        assert_eq!(
            node.connection_pool.total_reserved(),
            0,
            "a dead floor conn must be evicted so the fill can replace it"
        );
    }

    /// idle_timeout = 0: no deadline exists, so tend must touch nothing —
    /// no drops, no probes. Dead sockets are the checkout peek's job.
    #[aerospike_macro::test]
    async fn idle_timeout_zero_tend_is_noop() {
        let (addr, _dead) = spawn_fake_node(true).await; // even with DEAD conns
        let policy = test_policy(0, 0);
        let node = node_against(addr, policy.clone());
        park_conns(&node, addr, &policy, 3).await;

        aerospike_rt::sleep(std::time::Duration::from_millis(400)).await;

        let processed = node.reap_and_refresh_idle_connections().await;
        assert_eq!(processed, 0, "no deadline armed: tend has nothing to do");
        assert_eq!(node.connection_pool.num_conns(), 3);
    }

    /// One pass over a fully-expired pool retires exactly the surplus and
    /// keep-alives exactly the floor — the probe batch is bounded by
    /// `min_conns_per_node`, and the loop terminates with the queue drained.
    #[aerospike_macro::test]
    async fn keepalive_batch_is_bounded_by_the_floor() {
        let (addr, _dead) = spawn_fake_node(false).await;
        let policy = test_policy(300, 2); // floor of 2
        let node = node_against(addr, policy.clone());
        park_conns(&node, addr, &policy, 5).await;

        aerospike_rt::sleep(std::time::Duration::from_millis(400)).await; // all expired

        let processed = node.reap_and_refresh_idle_connections().await;

        assert_eq!(processed, 5, "3 retired + 2 probed: every conn accounted for");
        assert_eq!(
            node.connection_pool.total_reserved(),
            2,
            "exactly the floor survives: surplus retired, keepers healed"
        );
        assert_eq!(
            node.connection_pool.num_conns(),
            2,
            "both floor conns are back in the pool after their probes"
        );
    }

    /// The retire budget is global across queues: the pool settles at the
    /// floor and the budget cannot underflow.
    #[aerospike_macro::test]
    async fn retire_budget_is_shared_across_queues() {
        let (addr, _dead) = spawn_fake_node(false).await;
        let policy = ClientPolicy {
            idle_timeout: 300,
            min_conns_per_node: 1,
            tend_interval: 250,
            conn_pools_per_node: 4,
            timeout: 5_000,
            ..ClientPolicy::default()
        };
        let node = node_against(addr, policy.clone());
        for qi in 0..4 {
            let queue = &node.connection_pool.queues()[qi];
            for _ in 0..2 {
                let stream = aerospike_rt::net::TcpStream::connect(addr)
                    .await
                    .expect("connect");
                let conn = crate::net::Connection::test_from_tcp_stream(stream, &policy);
                assert!(queue.reserve_capacity());
                queue.put_back(conn);
            }
        }
        assert_eq!(node.connection_pool.total_reserved(), 8);
        aerospike_rt::sleep(std::time::Duration::from_millis(600)).await;

        node.reap_and_refresh_idle_connections().await;

        assert_eq!(
            node.connection_pool.total_reserved(),
            1,
            "7 of 8 must retire across the four queues, stopping at the floor"
        );
    }

    /// The retire budget is spent round-robin, one connection per queue per
    /// turn — the old queue-by-queue sweep left (0, 0, 2, 2) here.
    #[aerospike_macro::test]
    async fn retire_budget_spreads_across_queues_fairly() {
        let (addr, _dead) = spawn_fake_node(false).await;
        let policy = ClientPolicy {
            idle_timeout: 300,
            min_conns_per_node: 4,
            tend_interval: 250,
            conn_pools_per_node: 4,
            timeout: 5_000,
            ..ClientPolicy::default()
        };
        let node = node_against(addr, policy.clone());
        for qi in 0..4 {
            let queue = &node.connection_pool.queues()[qi];
            for _ in 0..2 {
                let stream = aerospike_rt::net::TcpStream::connect(addr)
                    .await
                    .expect("connect");
                let conn = crate::net::Connection::test_from_tcp_stream(stream, &policy);
                assert!(queue.reserve_capacity());
                queue.put_back(conn);
            }
        }
        assert_eq!(node.connection_pool.total_reserved(), 8);
        aerospike_rt::sleep(std::time::Duration::from_millis(600)).await;

        // Budget = 8 - 4 = 4: one retirement per queue, then the floor
        // survivors (one per queue) are probed and re-pooled.
        node.reap_and_refresh_idle_connections().await;

        for qi in 0..4 {
            assert_eq!(
                node.connection_pool.queues()[qi].reserved_count(),
                1,
                "queue {qi} must keep exactly one conn: retirement is spread \
                 one-per-queue, not drained queue-by-queue"
            );
        }
    }
}
