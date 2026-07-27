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

pub mod node;
pub mod node_validator;
pub mod partition;
pub mod partition_tokenizer;
pub mod peers;
pub mod peers_parser;
pub mod version_parser;

use aerospike_rt::time::{Duration, Instant};
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::vec::Vec;

pub use self::node::Node;
pub use self::partition::Partition;

use self::node_validator::NodeValidator;
use self::partition_tokenizer::PartitionTokenizer;
use self::peers::{Peer, Peers};

use crate::commands::admin_command::AdminCommand;
#[cfg(feature = "dynamic-config")]
use crate::config::{ConfigDocument, ConfigProvider, DynConfig, DynamicConfig};
use crate::errors::{Error, Result};
use crate::metrics::{Labels, MetricsPolicy, NodeMetrics, NodeMetricsSnapshot};
use crate::net::Host;
use crate::policy::{
    BatchPolicy, ClientPolicy, QueryPolicy, ReadPolicy, TxnRollPolicy, TxnVerifyPolicy, WritePolicy,
};
use crate::AdminPolicy;
use aerospike_rt::Mutex;
use futures::channel::mpsc;
use futures::channel::mpsc::{Receiver, Sender, TryRecvError};
use hazarc::AtomicArc;
use std::borrow::Cow;

static CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Per-namespace partition data.
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

    /// This cluster's tiered buffer pool, shared by all of its
    /// connections; `None` when `ClientPolicy::use_buffer_pool` is off.
    /// Aged by the tend loop and freed when the cluster drops.
    buffer_pool: Option<Arc<crate::net::buffer_pool::TieredBufferPool>>,

    // Number of completed tend cycles. Drives the per-node circuit-breaker
    // window: every `error_rate_window` tends we walk the node list and
    // call `node.reset_error_rate()`, mirroring Java's
    // `if (tendCount % errorRateWindow == 0) … resetErrorRate()`.
    tend_count: std::sync::atomic::AtomicUsize,

    tend_channel: Mutex<Sender<()>>,
    closed: AtomicBool,

    // Per-host seed validation errors recorded during the most recent
    // `seed_nodes` call. Used by `Cluster::new` to build a Java-style
    // `clusterInitError` aggregated message when `fail_if_not_connected`
    // is set and no node validates. Only meaningful during init — cleared
    // at the start of each `seed_nodes` invocation.
    last_seed_errors: std::sync::Mutex<Vec<(Host, String)>>,

    // ---- Metrics ----
    // Whether periodic metrics collection is enabled.
    metrics_enabled: AtomicBool,
    // Active metrics policy (histogram shape + labels).
    metrics_policy: AtomicArc<MetricsPolicy>,
    // Per-host accumulated statistics, aggregated once per tend. Retains
    // entries for removed hosts (reported with zero open connections).
    metrics: std::sync::Mutex<HashMap<String, NodeMetricsSnapshot>>,
    // Commands that exhausted their retry budget / total timeout. Surfaced in
    // the cluster-aggregated metrics.
    max_retries_exceeded_count: AtomicU64,
    total_timeout_exceeded_count: AtomicU64,

    // Cluster-wide count of connections currently being opened by background
    // fill tasks; shared into every node and checked against
    // `ClientPolicy::opening_connection_threshold`.
    opening_connections: Arc<std::sync::atomic::AtomicUsize>,

    // ---- Dynamic configuration ----
    // Present only when a config provider is attached (env var or explicit
    // injection). Holds the live dynamic config; the watcher task refreshes it.
    // Set once during client construction, before the client is handed out.
    #[cfg(feature = "dynamic-config")]
    dyn_config: std::sync::OnceLock<Arc<DynConfig>>,
}

/// `true` when `node.host().name` parses to a loopback address, or is the
/// `localhost` / `::1` literal. Used by `peer_exists` as a shortcut for
/// loopback-host comparisons.
fn node_address_is_loopback(node: &Node) -> bool {
    let name = node.host().name;
    if let Ok(ip) = name.parse::<std::net::IpAddr>() {
        return ip.is_loopback();
    }
    matches!(name.as_str(), "localhost" | "::1")
}

impl Cluster {
    pub async fn new(mut policy: ClientPolicy, hosts: &[Host]) -> Result<Arc<Self>> {
        // updated the hashed password
        let _ = policy.set_auth_mode(policy.auth_mode.clone());

        let (tx, rx) = mpsc::channel(100);
        let buffer_pool = crate::net::buffer_pool::TieredBufferPool::from_policy(&policy);
        let cluster = Arc::new(Cluster {
            hashed_pass: AtomicArc::from(policy.hashed_pass()),
            client_policy: AtomicArc::from(policy),
            buffer_pool,

            seeds: AtomicArc::from(hosts.to_vec()),
            aliases: AtomicArc::from(HashMap::new()),
            nodes: AtomicArc::from(vec![]),

            partition_map: AtomicArc::from(HashMap::default()),
            node_index: AtomicIsize::new(0),
            replica_index: AtomicIsize::new(0),
            tend_count: std::sync::atomic::AtomicUsize::new(0),

            tend_channel: Mutex::new(tx),
            closed: AtomicBool::new(false),
            last_seed_errors: std::sync::Mutex::new(Vec::new()),

            metrics_enabled: AtomicBool::new(false),
            metrics_policy: AtomicArc::from(MetricsPolicy::default()),
            metrics: std::sync::Mutex::new(HashMap::new()),
            max_retries_exceeded_count: AtomicU64::new(0),
            total_timeout_exceeded_count: AtomicU64::new(0),
            opening_connections: Arc::new(std::sync::atomic::AtomicUsize::new(0)),

            #[cfg(feature = "dynamic-config")]
            dyn_config: std::sync::OnceLock::new(),
        });
        // try to seed connections for first use
        Cluster::wait_till_stabilized(cluster.clone()).await?;

        // apply policy rules
        if cluster.client_policy.load().fail_if_not_connected && !cluster.is_connected() {
            // Mirrors Java's `Peers.clusterInitError`: surface every per-seed
            // error from the most recent seed pass so callers know *why*
            // each host failed, not just that "host(s) failed".
            return Err(Error::connection(cluster.format_init_error()));
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
        use futures::{FutureExt, StreamExt};

        let tend_interval = cluster.client_policy.load().tend_interval;

        loop {
            match rx.try_recv() {
                Ok(()) => unreachable!(),
                Err(TryRecvError::Closed) => break,
                Err(TryRecvError::Empty) => {
                    if let Err(err) = cluster.tend().await {
                        log_error_chain!(err, "Error tending cluster");
                    }
                    // Sleep until the next cycle, but wake immediately when
                    // `close()` closes the channel — otherwise shutdown
                    // cleanup (clearing nodes/aliases below) would lag up to
                    // a full tend interval behind close().
                    let sleep =
                        aerospike_rt::sleep(Duration::from_millis(u64::from(tend_interval)));
                    futures::select! {
                        _ = rx.next().fuse() => break,
                        () = sleep.fuse() => {}
                    }
                }
            }
        }

        // Cleanup is performed here — as the last act of the tend thread —
        // rather than in `close()`, so the "all node additions/deletions are
        // performed in tend thread" invariant (see `tend()`) is preserved.
        // This makes the cleanup race-free without any cross-thread sync.
        cluster.partition_map.store(Arc::new(HashMap::default()));
        cluster.hashed_pass.store(Arc::new(None));
        cluster.set_nodes(vec![]);
        cluster.aliases.store(Arc::new(HashMap::new()));
        cluster.seeds.store(Arc::new(vec![]));
    }

    async fn tend(&self) -> Result<()> {
        // If close() has been called, bail before any work — otherwise an
        // in-flight cycle would repopulate nodes after close() cleared them.
        if self.closed.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Age this cluster's buffer pool. This plays the role the garbage
        // collector plays for Go's sync.Pool: idle oversized buffers are
        // dropped after two aging intervals (the call self-throttles, so
        // the cadence is independent of the tend frequency).
        if let Some(pool) = &self.buffer_pool {
            pool.age_if_due();
        }

        let seed_only = self.client_policy().seed_only_cluster;
        let metrics_enabled = self.metrics_enabled();

        // Per-tend peer state. `gen_changed` is initialized to false (Java's
        // default) and set to true if any node's peers-generation differs.
        let peers = Peers::new(16, 16);
        peers.set_gen_changed(false);

        let nodes = self.nodes();

        // Mirror Java: clear per-tend node flags before refreshing.
        for node in &nodes {
            node.reset_reference_count();
            node.set_partition_changed(false);
            node.set_rebalance_changed(false);
        }

        // Re-seed when we have no nodes, or — under `seed_only_cluster`
        // — whenever the live node count drops below the seed count.
        // The latter is what gives `seed_only_cluster` its
        // "retain seeds despite connection failures" semantics.
        let seed_count = self.seeds.load().len();
        let need_seed = nodes.is_empty() || (seed_only && nodes.len() < seed_count);

        if need_seed {
            debug!(
                "Seeding cluster (live={} seeds={} seed_only={})",
                nodes.len(),
                seed_count,
                seed_only
            );
            self.seed_nodes().await;
        }
        if nodes.is_empty() {
            // Fall through to the non-refresh suffix (partition update).
        } else {
            // Phase 1: refresh all known nodes concurrently (light info
            // commands only) — mirrors the Go client's `ParDo` so tend
            // latency is bounded by the slowest node, not the sum of all
            // nodes. Safe to share `&peers` across the tasks: the phase-1
            // `Peers` methods (`set_gen_changed`, `increment_refresh_count`)
            // are atomic, and each task touches only its own node.
            let refresh_tasks = nodes.iter().map(|node| {
                let peers = &peers;
                async move {
                    // Reap idle connections, but keep enough of them alive via a
                    // cheap info probe to stay at or above `min_conns_per_node` —
                    // avoids the full TCP-connect round-trip that `fill_min_conns`
                    // would otherwise pay to replace them.
                    let processed = node.reap_and_refresh_idle_connections().await;
                    if processed > 0 {
                        debug!("Reap/refresh processed {processed} idle connections on {node}");
                    }

                    let refresh_result = node.refresh(peers).await;
                    if metrics_enabled {
                        node.metrics().incr_tend(refresh_result.is_ok());
                    }
                    if let Err(err) = refresh_result {
                        warn!("Node `{node}` refresh failed: {err}");
                    }
                }
            });
            futures::future::join_all(refresh_tasks).await;

            // Phases 2 + 3 + commit are skipped under `seed_only_cluster`:
            // peer discovery is the very thing the option disables. We
            // also skip removal so a transient seed failure doesn't
            // evict the seed.
            if !seed_only {
                // Phase 2: when peers-generation changed on any node, refresh
                // the full peer list and reconcile add/remove decisions. The
                // per-node peer fetches run concurrently (Go's `ParDo`); each
                // parsed peer is tagged with its parsing node's name, so a
                // materialization failure still invalidates only that node's
                // pending generation (Java's `peersValidated`).
                if peers.gen_changed() {
                    peers.reset_refresh_count();

                    let peer_fetches = nodes.iter().map(|node| {
                        let peers = &peers;
                        async move {
                            if let Err(err) = node.refresh_peers(peers).await {
                                warn!("Node `{node}` peer refresh failed: {err}");
                            }
                        }
                    });
                    futures::future::join_all(peer_fetches).await;
                    // Validate/connect the accumulated peers and create nodes.
                    self.materialize_peers(&peers).await;

                    // Decide which existing nodes can be dropped.
                    self.find_nodes_to_remove(&peers).await;

                    let nodes_to_remove = peers.get_nodes_to_remove();
                    if !nodes_to_remove.is_empty() {
                        self.remove_nodes_and_aliases(nodes_to_remove);
                    }
                }

                // Phase 3: add any newly-discovered peer nodes, then iterate
                // refresh-peers-of-peers until no further peers turn up. This
                // mirrors Java's `Cluster.refreshPeers` loop and lets multi-hop
                // discovery converge in a single tend cycle (seed → A → B → C).
                loop {
                    let drained = peers.drain_nodes();
                    if drained.is_empty() {
                        break;
                    }
                    self.add_nodes_and_aliases(&drained);

                    let peer_fetches = drained.iter().map(|node| {
                        let peers = &peers;
                        async move {
                            if let Err(err) = node.refresh_peers(peers).await {
                                warn!("Node `{node}` peer refresh failed: {err}");
                            }
                        }
                    });
                    futures::future::join_all(peer_fetches).await;
                    self.materialize_peers(&peers).await;
                }

                // Commit pending peers-generations: each parsing node's
                // generation only advances if every peer it reported was
                // materialized into the cluster. Otherwise we re-parse next
                // tend and retry the unreachable hosts.
                let pending = peers.take_pending_generations();
                for (name, generation) in pending {
                    if let Ok(node) = self.get_node_by_name(&name) {
                        node.commit_peers_generation(generation);
                    }
                }
            }

            // If any seed-host failed during this tend, surface a warning so
            // operators have visibility into init-time connection errors.
            if peers.invalid_count() > 0 {
                debug!(
                    "Tend cycle saw {} invalid peer host(s): {:?}",
                    peers.invalid_count(),
                    peers.invalid_hosts()
                );
            }
        }

        // Phase 4: refresh partition map / rack info for any node whose
        // generation flag flipped during phase 1. The per-node fetches run
        // concurrently (each node uses its own tend connection); the results
        // are merged into ONE shared partition map guarded by a synchronous
        // mutex. The lock is only held for the in-memory merge — never across
        // an await — so contention is negligible. (The Go client does the
        // equivalent with per-goroutine work behind an `iatomic.Guard`;
        // sharing a single synced map avoids its extra clones.)
        let active_nodes = self.nodes();
        let peers_refresh_count = peers.refresh_count();

        // (node, refresh_partitions, refresh_racks) work list.
        let mut refresh_work: Vec<(&Arc<Node>, bool, bool)> = Vec::new();
        for node in &active_nodes {
            let mut partitions = node.partition_changed();
            // Split-cluster guard: skip a node that thinks it's the only
            // one in the cluster (peers_count == 0) when we've already
            // refreshed peers from at least two nodes this tend
            // (`peers.refresh_count > 1`). Lets the rest of the cluster's
            // map win when an isolated node has stale or zero-peer view.
            // Mirrors Java `Node.refreshPartitions`.
            if partitions && !seed_only && node.peers_count() == 0 && peers_refresh_count > 1 {
                debug!(
                    "Skipping partition update for node {node}: reports 0 peers in {}-node cluster (likely split)",
                    active_nodes.len()
                );
                partitions = false;
            }
            let racks = node.rebalance_changed();
            if partitions || racks {
                refresh_work.push((node, partitions, racks));
            }
        }

        // Clone the current map once, only when some node needs a partition
        // refresh. All merges land in this single shared copy.
        let shared_map: Option<std::sync::Mutex<PartitionTable>> = refresh_work
            .iter()
            .any(|(_, partitions, _)| *partitions)
            .then(|| std::sync::Mutex::new((*self.partition_map.load().clone()).clone()));

        let admin_policy = AdminPolicy {
            timeout: self.client_policy.load().timeout,
        };
        let refresh_tasks = refresh_work.iter().map(|(node, partitions, racks)| {
            let shared_map = shared_map.as_ref();
            let admin_policy = &admin_policy;
            async move {
                // Partition fetch + rack fetch stay sequential *within* a node
                // (they share the node's tend connection); nodes run in parallel.
                if *partitions {
                    match PartitionTokenizer::from_node(node, admin_policy).await {
                        Ok(tokens) => {
                            let mut map = shared_map
                                .expect("shared map initialized when any node refreshes partitions")
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            match tokens.update_partition(&mut map, node) {
                                Ok(()) => {
                                    drop(map);
                                    if metrics_enabled {
                                        node.metrics().incr_partition_map_update();
                                    }
                                }
                                Err(err) => {
                                    warn!("Node `{node}` partition update failed: {err}");
                                }
                            }
                        }
                        Err(err) => warn!("Node `{node}` partition update failed: {err}"),
                    }
                }

                if *racks {
                    if let Err(err) = self.update_rack_ids(node).await {
                        warn!("Node `{node}` rack update failed: {err}");
                    }
                }
            }
        });
        futures::future::join_all(refresh_tasks).await;

        if let Some(shared_map) = shared_map {
            let partition_map = shared_map
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.partition_map.store(Arc::new(partition_map));
        }

        // Drain per-node metrics into the cluster accumulator once per tend.
        if self.metrics_enabled() {
            self.aggregate_node_metrics(&active_nodes);
        }

        // Bump the tend counter and, if the configured `error_rate_window`
        // boundary lands here, roll every node's per-window error counter
        // forward. Mirrors Java's
        // `if (tendCount % errorRateWindow == 0) … resetErrorRate()`.
        let tend_count = self
            .tend_count
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let policy = self.client_policy.load();
        let window = policy.error_rate_window;
        if window > 0 && tend_count.is_multiple_of(window) {
            for node in &active_nodes {
                node.reset_error_rate();
            }
        }

        let aliases: Vec<String> = self
            .aliases
            .load()
            .values()
            .map(std::string::ToString::to_string)
            .collect();

        debug!("Nodes {aliases:?}");

        Ok(())
    }

    /// Walks the discovered peer list and validates each peer host until one
    /// connects, creating a new `Node` for the first successful host. Split
    /// out from the parser so the network-bound validation step can stay
    /// async.
    ///
    /// Tracks per-source-node "every peer materialized" so the caller can
    /// commit each parsing node's `peers-generation`. If any peer parsed by
    /// node X is unreachable, X's pending generation is invalidated and the
    /// next tend will re-parse and retry.
    async fn materialize_peers(&self, peers: &Peers) {
        let peers_list = peers.peers_list();
        // Reset the working set; we've taken a copy of what was in there.
        peers.clear_peers();

        // Group duplicate mentions of the same peer node — every existing
        // node typically advertises a newly added one — so each distinct
        // peer is validated exactly once (the sequential loop achieved this
        // via the "already added this tend" check). Host lists are unioned
        // across mentions, and every mention's parsing-node name is kept: if
        // the peer turns out unreachable, each of those nodes' pending
        // peers-generations must be invalidated.
        let mut groups: Vec<(Peer, Vec<String>)> = Vec::new();
        let mut index_by_name: HashMap<String, usize> = HashMap::new();
        for peer in peers_list {
            if let Some(&i) = index_by_name.get(&peer.node_name) {
                let (rep, sources) = &mut groups[i];
                if let Some(src) = peer.from_node_name {
                    if !sources.contains(&src) {
                        sources.push(src);
                    }
                }
                for host in peer.hosts {
                    if !rep.hosts.contains(&host) {
                        rep.hosts.push(host);
                    }
                }
            } else {
                index_by_name.insert(peer.node_name.clone(), groups.len());
                let sources = peer.from_node_name.clone().into_iter().collect();
                groups.push((peer, sources));
            }
        }

        // Validate/connect the distinct peers concurrently (Go's `ParDo`
        // over `peers.peers()`). Tasks only append to the internally-synced
        // `peers` accumulator; actual cluster membership is still committed
        // afterwards by the tend task (add/remove stays single-flow).
        let materialize_tasks = groups.into_iter().map(|(mut peer, sources)| async move {
            if self.peer_exists(peers, &mut peer).await {
                return;
            }

            let mut materialized = false;
            for host in &peer.hosts {
                if peers.has_failed(host) {
                    continue;
                }

                let mut nv = NodeValidator::new(self.client_policy());
                if let Err(err) = nv.validate_node(self, host).await {
                    peers.fail(host.clone());
                    warn!("Add peer node `{host}` failed: `{err}`");
                    continue;
                }

                if peer.node_name != nv.name {
                    warn!(
                        "Peer node `{}` is different than actual node `{}` for host `{}`",
                        peer.node_name, nv.name, host
                    );
                }

                let node_name = nv.name.clone();
                let node = Arc::new(self.create_node(nv).await);
                peers.add_node(node_name, node);

                if let Some(ref replace_node) = peer.replace_node {
                    if !peers.contains_node_to_remove(replace_node) {
                        peers.add_node_to_remove(replace_node.clone());
                    }
                }
                materialized = true;
                break;
            }

            // If none of the peer's hosts validated, invalidate every
            // parsing node's pending generation so none of them commit at
            // the end of the tend. The peer will be re-parsed next tend.
            if !materialized {
                for source in &sources {
                    peers.invalidate_pending_generation(source);
                }
            }
        });
        futures::future::join_all(materialize_tasks).await;
    }

    /// Checks if a peer represents an already-known node.
    ///
    /// Following logic:
    /// - If node found by name and healthy (or localhost), increment reference count.
    /// - If node has failures, verify host addresses match before reusing.
    /// - If a peer host is a hostname, resolve it via DNS and compare each
    ///   resolved IP against the existing node's address. Cache the
    ///   hostname on the node on success.
    /// - If host mismatch on a failing node, mark as `replace_node`.
    /// - Also check if already added during this tend cycle.
    async fn peer_exists(&self, peers: &Peers, peer: &mut Peer) -> bool {
        // Check 1: Find by node name in current cluster nodes.
        if let Ok(node) = self.get_node_by_name(&peer.node_name) {
            // Mirrors Java's `findPeerNode`:
            //   `node.failures <= 0 || node.address.isLoopbackAddress()`.
            // A node bound to localhost is never going to be replaced by a
            // peer reachable via a different address, so even when it has
            // stale failures we treat it as "still us" and skip the
            // host-match scan.
            if node.failures() == 0 || node_address_is_loopback(&node) {
                // Node is healthy (or localhost) — no need to update IP.
                node.increment_reference_count();
                return true;
            }

            // Node has failures — check if any peer host points at the
            // same address. Direct name match is cheap and covers the
            // common case; cached-hostname match handles the previous
            // tend's resolution; DNS resolution is the fallback.
            let node_host = node.host();
            for host in &peer.hosts {
                if host.port != node_host.port {
                    continue;
                }

                if host.name == node_host.name
                    || node
                        .cached_hostname()
                        .is_some_and(|cached| cached == host.name)
                {
                    node.increment_reference_count();
                    return true;
                }

                if let Ok(addrs) = (host.name.as_str(), host.port).to_socket_addrs() {
                    for addr in addrs {
                        let ip_str = addr.ip().to_string();
                        if ip_str == node_host.name || addr.ip().is_loopback() {
                            // Cache for next tend so we don't pay the
                            // resolution cost again.
                            node.cache_hostname(host.name.clone());
                            node.increment_reference_count();
                            return true;
                        }
                    }
                }
            }

            // Host mismatch on a failing node — this peer should replace it.
            peer.replace_node = Some(node);
        }

        // Check 2: Already added during this tend cycle.
        if let Some(node) = peers.node_by_name(&peer.node_name) {
            node.increment_reference_count();
            peer.replace_node = None;
            return true;
        }

        false
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
                    if count == 0 {
                        // No reachable nodes: nothing further to wait for —
                        // `fail_if_not_connected` decides what happens next.
                        break;
                    }
                    // Node-count stability alone is not enough: on a
                    // multi-node cluster the nodes materialize in the seed
                    // pass but their partition maps land one tend later, so
                    // breaking here would let commands race the first
                    // partition fetch and die on "partition map empty"
                    // before the tend thread fills it in. Java's tend parses
                    // partitions synchronously for fresh nodes, so its
                    // count-only check is safe; ours must wait until every
                    // node has parsed a partition map at least once.
                    let nodes = cluster.nodes();
                    let partitions_ready = !cluster.partition_map.load().is_empty()
                        && nodes.iter().all(|n| n.partition_generation() != -1);
                    if partitions_ready {
                        break;
                    }
                }

                aerospike_rt::sleep(sleep_between_tend).await;
            }
        });

        #[cfg(all(feature = "rt-tokio", not(feature = "rt-async-std")))]
        return handle.await.map_err(|err| {
            Error::invalid_argument(format!("Error during initial cluster tend: {err:?}"))
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
        // Issue `replicas` + `partition-generation` over the node's
        // long-lived tend connection (Java's `tendConnection` reuse).
        let admin_policy = AdminPolicy {
            timeout: self.client_policy.load().timeout,
        };
        let tokens = PartitionTokenizer::from_node(node, &admin_policy).await?;
        tokens.update_partition(partition_map, node)?;
        Ok(())
    }

    pub async fn update_rack_ids(&self, node: &Arc<Node>) -> Result<()> {
        const RACK_IDS: &str = "rack-ids";
        let admin_policy = AdminPolicy {
            timeout: self.client_policy.load().timeout,
        };
        // Same tend-connection reuse as `update_partitions`.
        let info_map = node
            .tend_info(&admin_policy, &[RACK_IDS, node::REBALANCE_GENERATION])
            .await?;

        // Reject explicit "rack-ids not supported" replies. The server
        // returns the literal string "ERROR..." (or an empty value) when the
        // feature is disabled, even though `rack_ids` is configured on the
        // client policy. Letting it pass would silently strip the rack table.
        match info_map.get(RACK_IDS) {
            Some(buf) if !buf.is_empty() && !buf.to_uppercase().starts_with("ERROR") => {
                node.parse_rack(buf.as_str())?;
            }
            _ => {
                return Err(Error::bad_response(
                    "ClientPolicy.rack_ids is set, but the server does not support this feature."
                        .to_string(),
                ));
            }
        }

        // We re-update the rebalance generation right now (in case its changed since it was last polled)
        node.update_rebalance_generation(&info_map)?;

        Ok(())
    }

    fn record_seed_error(&self, host: Host, err: &Error) {
        if let Ok(mut errs) = self.last_seed_errors.lock() {
            errs.push((host, err.to_string()));
        }
    }

    /// Format the per-seed errors recorded during the most recent
    /// `seed_nodes` call into a single connection-error message. Falls back
    /// to a generic message when nothing was recorded (e.g. seed list was
    /// empty).
    fn format_init_error(&self) -> String {
        let errs = self
            .last_seed_errors
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();

        if errs.is_empty() {
            return "Failed to connect to host(s). The network connection(s) \
                 to cluster nodes may have timed out, or the cluster may \
                 be in a state of flux."
                .to_string();
        }

        let mut sb = String::with_capacity(64 + errs.len() * 80);
        sb.push_str(&format!("Failed to connect to [{}] host(s):\n", errs.len()));
        for (host, err) in &errs {
            sb.push_str(&format!("  {host} {err}\n"));
        }
        sb
    }

    pub async fn seed_nodes(&self) -> bool {
        let seed_array = self.seeds.load();

        info!("Seeding the cluster. Seeds count: {}", seed_array.len());

        // Mirrors Java `addSeedAndPeers`: on a full reseed we must drop any
        // alias rows that survived from the previous, now-empty cluster
        // view. Otherwise an old IP can keep mapping to a stale node and
        // distort `peer_exists` lookups in this and future tends.
        self.aliases.store(Arc::new(HashMap::new()));

        // Reset the error log for this attempt so `Cluster::new` only sees
        // failures from the most recent seed pass.
        if let Ok(mut errs) = self.last_seed_errors.lock() {
            errs.clear();
        }

        let mut list: Vec<Arc<Node>> = vec![];
        // Fallback retention: a seed whose `peers-…` response is empty
        // might be a new or recovering single-node cluster; keep the
        // already-validated `Node` aside in case no other seed yields a
        // populated peer list. Mirrors Java `NodeValidator.fallback` plus
        // `Cluster.seedNode`'s post-loop `addSeedAndPeers(nv.fallback, …)`.
        let mut fallback: Option<Arc<Node>> = None;

        let seed_only = self.client_policy().seed_only_cluster;

        for seed in seed_array.iter() {
            let mut seed_node_validator = NodeValidator::new_for_seed(self.client_policy());
            if let Err(err) = seed_node_validator.validate_node(self, seed).await {
                self.record_seed_error(seed.clone(), &err);
                log_error_chain!(err, "Failed to validate seed host: {}", seed);
                continue;
            }

            // Construct the seed Node up front — Java's
            // `validatePeers → Node.refreshPeers` flow needs a Node so it
            // can issue `peers-…` over its own pool.
            let seed_node = Arc::new(self.create_node(seed_node_validator).await);

            // Under `seed_only_cluster` peer discovery is the very
            // thing the option disables. Add the seed Node and move on
            // without ever calling `refresh_peers` on it — the seed is
            // also not eligible to be used as a "fallback" since the
            // notion of fallback only matters when peer harvesting was
            // attempted but came back empty.
            if seed_only {
                if !self.find_node_name(&list, seed_node.name()) {
                    self.add_aliases(seed_node.clone());
                    list.push(seed_node);
                }
                continue;
            }

            // Pull the rich peer list (`peers-{tls,clear}-{std,alt}`):
            // node names + per-peer multi-host fallback. Use a throwaway
            // `Peers` so any pending generation produced here doesn't bleed
            // into the next tend cycle's accounting.
            let mut harvest = Peers::new(16, 16);
            harvest.set_gen_changed(false);
            harvest.reset_refresh_count();
            if let Err(err) = seed_node.refresh_peers(&mut harvest).await {
                self.record_seed_error(seed.clone(), &err);
                log_error_chain!(err, "Seed peer fetch failed: {}", seed);
                seed_node.close();
                continue;
            }

            // Empty peer list → single-node cluster or recovering node;
            // hold the seed aside as a fallback.
            if harvest.peer_count() == 0 {
                if fallback.is_none() {
                    debug!("Seed {seed} has no peers; retaining as fallback");
                    fallback = Some(seed_node);
                } else {
                    debug!("Discarding additional peerless seed {seed}");
                    seed_node.close();
                }
                continue;
            }

            // Real peer list arrived — abandon the previously retained
            // fallback (only one needs to be alive at a time).
            if let Some(prev) = fallback.take() {
                debug!("Dropping fallback seed in favor of {seed}");
                prev.close();
            }

            // Add the seed itself as a real cluster node.
            if !self.find_node_name(&list, seed_node.name()) {
                self.add_aliases(seed_node.clone());
                list.push(seed_node);
            }

            // Materialize each peer once. Java's `Node.refreshPeers` tries
            // every `peer.hosts` entry until one connects — we do the same,
            // skipping hosts already proven unreachable in this seed pass
            // (Java's `peers.hasFailed(host)` short-circuit).
            for peer in harvest.peers_list() {
                if self.find_node_name(&list, &peer.node_name) {
                    continue;
                }

                let mut peer_validated = false;
                for host in &peer.hosts {
                    if harvest.has_failed(host) {
                        continue;
                    }

                    let mut peer_nv = NodeValidator::new(self.client_policy());
                    if let Err(err) = peer_nv.validate_node(self, host).await {
                        self.record_seed_error(host.clone(), &err);
                        harvest.fail(host.clone());
                        log_error_chain!(err, "Seeding peer host {} failed", host);
                        continue;
                    }

                    if peer.node_name != peer_nv.name {
                        warn!(
                            "Peer node `{}` is different than actual node `{}` for host `{}`",
                            peer.node_name, peer_nv.name, host
                        );
                    }

                    if self.find_node_name(&list, &peer_nv.name) {
                        peer_validated = true;
                        break;
                    }

                    let node = Arc::new(self.create_node(peer_nv).await);
                    self.add_aliases(node.clone());
                    list.push(node);
                    peer_validated = true;
                    break; // first reachable host wins
                }

                if !peer_validated {
                    // A peer that fails on every host invalidates its
                    // parsing source's pending generation, mirroring Java's
                    // `peersValidated = false` rule. We'll re-parse this
                    // node's peers next tend and retry the unreachable host.
                    if let Some(ref source) = peer.from_node_name {
                        harvest.invalidate_pending_generation(source);
                    }
                    debug!(
                        "Peer {} unreachable on every advertised host",
                        peer.node_name
                    );
                }
            }

            // Commit the seed's `peers-generation` if every peer it parsed
            // was successfully materialized. Mirrors Java's
            // `peersGeneration = parser.generation` inside `Node.refreshPeers`
            // — without this the next tend would re-fetch every seed's
            // peer list because the stored generation would still be `-1`.
            // The new nodes only live in `list` at this point (they haven't
            // been published to `self.nodes()` yet), so look them up there.
            for (name, generation) in harvest.take_pending_generations() {
                if let Some(node) = list.iter().find(|n| n.name() == name) {
                    node.commit_peers_generation(generation);
                }
            }
        }

        // No seed yielded peers; install the fallback as the cluster's
        // single node so the client can still make progress.
        if list.is_empty() {
            if let Some(node) = fallback.take() {
                info!("Using fallback seed node: {}", node.name());
                self.add_aliases(node.clone());
                list.push(node);
            }
        } else if let Some(prev) = fallback.take() {
            // We accumulated a real peer list along the way; the retained
            // fallback is no longer needed.
            prev.close();
        }

        self.add_nodes_and_aliases(&list);
        !list.is_empty()
    }

    fn find_node_name(&self, list: &[Arc<Node>], name: &str) -> bool {
        list.iter().any(|node| node.name() == name)
    }

    async fn create_node(&self, nv: NodeValidator) -> Node {
        // Shape the node's metrics with the current metrics policy and enable
        // collection immediately if the cluster already has metrics on (so
        // nodes discovered after `enable_metrics` still record).
        let metrics = Arc::new(NodeMetrics::new((*self.metrics_policy()).clone()));
        if self.metrics_enabled() {
            metrics.set_enabled(true);
        }
        let res = Node::new(
            self.client_policy(),
            Arc::new(nv),
            metrics,
            self.opening_connections.clone(),
            self.buffer_pool.clone(),
        );
        res.send_user_agent_id().await;
        res
    }

    // ---- Metrics API ----

    /// Returns the active metrics policy.
    pub fn metrics_policy(&self) -> Arc<MetricsPolicy> {
        self.metrics_policy.load().clone()
    }

    /// Returns whether periodic metrics collection is enabled.
    pub fn metrics_enabled(&self) -> bool {
        self.metrics_enabled.load(Ordering::Relaxed)
    }

    /// Enables metrics collection, (re)shaping every node's histograms to the
    /// given policy.
    pub fn enable_metrics(&self, policy: MetricsPolicy) {
        self.metrics_policy.store(Arc::new(policy.clone()));
        self.metrics_enabled.store(true, Ordering::Relaxed);

        // Reshape retained per-host snapshots.
        {
            let mut metrics = self.metrics.lock().unwrap();
            for snapshot in metrics.values_mut() {
                let mut reshaped = NodeMetricsSnapshot::new(policy.clone());
                reshaped.aggregate(snapshot);
                *snapshot = reshaped;
            }
        }

        for node in self.nodes().iter() {
            node.metrics().reshape(&policy);
            node.metrics().set_enabled(true);
        }
    }

    /// Disables metrics collection.
    pub fn disable_metrics(&self) {
        self.metrics_enabled.store(false, Ordering::Relaxed);
        for node in self.nodes().iter() {
            node.metrics().set_enabled(false);
        }
    }

    // ---- Dynamic configuration ----

    /// Overlays the cluster's current dynamic `read`-section config (if any) onto a
    /// user-supplied policy. Returns the original borrowed when dynamic config is
    /// off or absent (zero cost); an owned, merged copy otherwise.
    pub(crate) fn resolve_read<'a>(&self, policy: &'a ReadPolicy) -> Cow<'a, ReadPolicy> {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = dc.dynamic().read.clone() {
                let mut owned = policy.clone();
                cfg.merge_into(&mut owned);
                return Cow::Owned(owned);
            }
        }
        Cow::Borrowed(policy)
    }

    /// As [`resolve_read`](Self::resolve_read) for the `write` section.
    pub(crate) fn resolve_write<'a>(&self, policy: &'a WritePolicy) -> Cow<'a, WritePolicy> {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = dc.dynamic().write.clone() {
                let mut owned = policy.clone();
                cfg.merge_into(&mut owned);
                return Cow::Owned(owned);
            }
        }
        Cow::Borrowed(policy)
    }

    /// As [`resolve_read`](Self::resolve_read) for the `query` section.
    pub(crate) fn resolve_query<'a>(&self, policy: &'a QueryPolicy) -> Cow<'a, QueryPolicy> {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = dc.dynamic().query.clone() {
                let mut owned = policy.clone();
                cfg.merge_into(&mut owned);
                return Cow::Owned(owned);
            }
        }
        Cow::Borrowed(policy)
    }

    /// As [`resolve_read`](Self::resolve_read) for the `batch` section.
    pub(crate) fn resolve_batch<'a>(&self, policy: &'a BatchPolicy) -> Cow<'a, BatchPolicy> {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = dc.dynamic().batch.clone() {
                let mut owned = policy.clone();
                cfg.merge_into(&mut owned);
                return Cow::Owned(owned);
            }
        }
        Cow::Borrowed(policy)
    }

    /// As [`resolve_read`](Self::resolve_read) for the `txn_verify` section.
    pub(crate) fn resolve_txn_verify<'a>(
        &self,
        policy: &'a TxnVerifyPolicy,
    ) -> Cow<'a, TxnVerifyPolicy> {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = dc.dynamic().txn_verify.clone() {
                let mut owned = policy.clone();
                cfg.merge_into(&mut owned);
                return Cow::Owned(owned);
            }
        }
        Cow::Borrowed(policy)
    }

    /// As [`resolve_read`](Self::resolve_read) for the `txn_roll` section.
    pub(crate) fn resolve_txn_roll<'a>(&self, policy: &'a TxnRollPolicy) -> Cow<'a, TxnRollPolicy> {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = dc.dynamic().txn_roll.clone() {
                let mut owned = policy.clone();
                cfg.merge_into(&mut owned);
                return Cow::Owned(owned);
            }
        }
        Cow::Borrowed(policy)
    }

    // ---- Per-record batch sub-policy overlays (mirrors Go's batch_read /
    // batch_write / batch_delete / batch_udf sections) ----
    //
    // These overlay the matching dynamic section onto the *effective* per-record
    // policy used by the single-key batch path; the wire (multi-key) path is
    // handled by `patch_batch_wire`. No-ops when the feature/config is absent.

    /// Overlays the `batch_read` section onto a batch read's effective read policy.
    #[cfg_attr(not(feature = "dynamic-config"), allow(unused_variables))]
    pub(crate) fn apply_batch_read(&self, policy: &mut ReadPolicy) {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = dc.dynamic().batch_read.clone() {
                // Only the per-record read fields apply to a single read; the
                // batch wire flags (allow_inline/…) are parent-batch concerns.
                cfg.read.merge_into(policy);
            }
        }
    }

    /// Overlays the `batch_write` section onto a batch write's effective write policy.
    #[cfg_attr(not(feature = "dynamic-config"), allow(unused_variables))]
    pub(crate) fn apply_batch_write(&self, policy: &mut WritePolicy) {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = dc.dynamic().batch_write.clone() {
                cfg.merge_into(policy);
            }
        }
    }

    /// Overlays the `batch_delete` section's `send_key`/`durable_delete` onto a
    /// batch delete's effective write policy.
    #[cfg_attr(not(feature = "dynamic-config"), allow(unused_variables))]
    pub(crate) fn apply_batch_delete(&self, policy: &mut WritePolicy) {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = &dc.dynamic().batch_delete {
                if let Some(send_key) = cfg.send_key {
                    policy.send_key = send_key;
                }
                if let Some(durable) = cfg.durable_delete {
                    policy.durable_delete = durable;
                }
            }
        }
    }

    /// Overlays the `batch_udf` section's `send_key`/`durable_delete` onto a
    /// batch UDF's effective write policy.
    #[cfg_attr(not(feature = "dynamic-config"), allow(unused_variables))]
    pub(crate) fn apply_batch_udf(&self, policy: &mut WritePolicy) {
        #[cfg(feature = "dynamic-config")]
        if let Some(dc) = self.dyn_config.get() {
            if let Some(cfg) = &dc.dynamic().batch_udf {
                if let Some(send_key) = cfg.send_key {
                    policy.send_key = send_key;
                }
                if let Some(durable) = cfg.durable_delete {
                    policy.durable_delete = durable;
                }
            }
        }
    }

    /// Patches the multi-key (wire) batch path in place: per-record `send_key`/
    /// `durable_delete` flags onto each write/delete/UDF op's sub-policy, and the
    /// `batch_read` read modes onto the shared parent base policy (writes ignore
    /// read modes, so this only affects reads). Per-section timeouts are
    /// command-level here and come from the already-resolved parent `batch`
    /// section. No-op when the feature/config is absent.
    #[cfg_attr(not(feature = "dynamic-config"), allow(unused_variables))]
    pub(crate) fn patch_batch_wire(
        &self,
        parent: &mut BatchPolicy,
        ops: &mut [(crate::batch::BatchOperation, usize)],
    ) {
        #[cfg(feature = "dynamic-config")]
        {
            use crate::batch::BatchOperation;
            let Some(dc) = self.dyn_config.get() else {
                return;
            };
            let dynamic = dc.dynamic();

            if let Some(read_cfg) = &dynamic.batch_read {
                if let Some(base) = &read_cfg.read.base_policy {
                    if let Some(mode) = base.read_mode_ap {
                        parent.base_policy.read_mode_ap = mode;
                    }
                    if let Some(mode) = base.read_mode_sc {
                        parent.base_policy.read_mode_sc = mode;
                    }
                }
                // Batch-command wire flags live on the parent policy; the Go
                // client applies these from the `batch_read` section.
                if let Some(v) = read_cfg.allow_inline {
                    parent.allow_inline = v;
                }
                if let Some(v) = read_cfg.allow_inline_ssd {
                    parent.allow_inline_ssd = v;
                }
                if let Some(v) = read_cfg.respond_all_keys {
                    parent.respond_all_keys = v;
                }
            }

            for (op, _) in ops.iter_mut() {
                match op {
                    BatchOperation::Write { policy, .. } => {
                        if let Some(cfg) = &dynamic.batch_write {
                            if let Some(send_key) = cfg.send_key {
                                policy.send_key = send_key;
                            }
                            if let Some(durable) = cfg.durable_delete {
                                policy.durable_delete = durable;
                            }
                        }
                    }
                    BatchOperation::Delete { policy, .. } => {
                        if let Some(cfg) = dynamic.batch_delete.clone() {
                            cfg.merge_into(policy);
                        }
                    }
                    BatchOperation::UDF { policy, .. } => {
                        if let Some(cfg) = dynamic.batch_udf.clone() {
                            cfg.merge_into(policy);
                        }
                    }
                    // Reads carry no wire-patchable sub-policy here; txn
                    // verify/roll never flow through the public batch wire path.
                    BatchOperation::Read { .. }
                    | BatchOperation::TxnVerify { .. }
                    | BatchOperation::TxnRoll { .. } => {}
                }
            }
        }
    }

    /// Creates a [`DynConfig`] from `provider`, performs the initial load + apply
    /// (static section once, dynamic section), stores it, and spawns the watcher
    /// task. Called during client construction, before the client is returned.
    #[cfg(feature = "dynamic-config")]
    pub(crate) async fn attach_dyn_config(self: &Arc<Self>, provider: Arc<dyn ConfigProvider>) {
        let dyn_config = Arc::new(DynConfig::new(provider));
        match dyn_config.provider().load().await {
            Ok(Some(doc)) => self.apply_config_doc(&dyn_config, doc),
            Ok(None) => {}
            Err(err) => {
                log_error_chain!(err, "Error loading initial dynamic configuration");
            }
        }
        // OnceLock: set once at construction; ignore the (impossible) re-set.
        let _ = self.dyn_config.set(dyn_config);
        let cluster = self.clone();
        let _res = aerospike_rt::spawn(Cluster::config_watch_thread(cluster));
    }

    /// Applies a freshly-loaded config document: the `static` section once (on the
    /// first apply), the dynamic `client`/`metrics` sections every time, and stores
    /// the whole dynamic section for per-command [`resolve_read`](Self::resolve_read)
    /// & friends.
    #[cfg(feature = "dynamic-config")]
    fn apply_config_doc(&self, dyn_config: &DynConfig, doc: ConfigDocument) {
        let first_apply = !dyn_config.mark_initialized();

        if first_apply {
            if let Some(client_cfg) = doc.static_config.as_ref().and_then(|s| s.client.clone()) {
                let mut cp = (*self.client_policy.load().clone()).clone();
                client_cfg.merge_static_into(&mut cp);
                self.client_policy.store(Arc::new(cp));
            }
        }

        match doc.dynamic {
            Some(dynamic) => {
                if let Some(client_cfg) = dynamic.client.clone() {
                    let mut cp = (*self.client_policy.load().clone()).clone();
                    client_cfg.merge_into(&mut cp);
                    self.client_policy.store(Arc::new(cp));
                }
                if let Some(metrics) = dynamic.metrics.clone() {
                    self.apply_metrics_config(&metrics);
                }
                dyn_config.store_dynamic(dynamic);
            }
            None => dyn_config.store_dynamic(DynamicConfig::default()),
        }
    }

    /// Applies the `dynamic.metrics` section: toggles collection via the existing
    /// metrics API and folds any latency-histogram overrides into the policy.
    #[cfg(feature = "dynamic-config")]
    fn apply_metrics_config(&self, metrics: &crate::config::MetricsConfig) {
        // Builds the next metrics policy from the current one, folding in the
        // latency-histogram overrides and any custom labels.
        let next_policy = || {
            let mut policy = (*self.metrics_policy()).clone();
            metrics.policy.clone().merge_into(&mut policy);
            if let Some(labels) = &metrics.labels {
                // The cross-client schema models labels as a single flat map;
                // `Labels` holds a list of entries (empty maps are dropped).
                policy.labels = crate::metrics::Labels::with_pairs(vec![labels.clone()]);
            }
            policy
        };
        match metrics.enable {
            Some(false) => self.disable_metrics(),
            Some(true) => self.enable_metrics(next_policy()),
            // No explicit toggle: refine the policy only if already enabled.
            None if self.metrics_enabled() => self.enable_metrics(next_policy()),
            None => {}
        }
    }

    /// Background task: periodically reloads config from the provider and applies
    /// changes until the cluster is closed. Interval comes from
    /// `static.client.config_interval` (minimum 1s).
    #[cfg(feature = "dynamic-config")]
    async fn config_watch_thread(cluster: Arc<Cluster>) {
        let Some(dyn_config) = cluster.dyn_config.get().cloned() else {
            return;
        };
        debug!("Starting dynamic-config watch task...");
        loop {
            if cluster.closed.load(Ordering::Relaxed) {
                break;
            }
            let interval_ms = u64::from(cluster.client_policy.load().config_interval.max(1000));
            aerospike_rt::sleep(Duration::from_millis(interval_ms)).await;
            if cluster.closed.load(Ordering::Relaxed) {
                break;
            }
            match dyn_config.provider().load().await {
                Ok(Some(doc)) => cluster.apply_config_doc(&dyn_config, doc),
                Ok(None) => {}
                Err(err) => {
                    log_error_chain!(err, "Error reloading dynamic configuration");
                }
            }
        }
        debug!("Stopping dynamic-config watch task.");
    }

    /// Records a command that exhausted its retry budget.
    pub fn incr_max_retries_exceeded(&self) {
        if self.metrics_enabled() {
            self.max_retries_exceeded_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a command that exceeded its total timeout.
    pub fn incr_total_timeout_exceeded(&self) {
        if self.metrics_enabled() {
            self.total_timeout_exceeded_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Drains each node's live metrics and merges them into the per-host
    /// accumulator.
    fn aggregate_node_metrics(&self, nodes: &[Arc<Node>]) {
        let mut metrics = self.metrics.lock().unwrap();
        for node in nodes {
            let host = node.host().to_string();
            let drained = node.metrics().get_and_reset();
            match metrics.get_mut(&host) {
                Some(existing) => existing.aggregate(&drained),
                None => {
                    metrics.insert(host, drained);
                }
            }
        }
    }

    /// Aggregates and returns a clone of the per-host statistics, stamping each
    /// active node's open-connection gauge.
    pub fn metrics_copy(&self) -> HashMap<String, NodeMetricsSnapshot> {
        let nodes = self.nodes();
        self.aggregate_node_metrics(&nodes);

        let mut open_by_host: HashMap<String, u64> = HashMap::new();
        for node in nodes.iter() {
            open_by_host.insert(node.host().to_string(), node.open_connections());
        }

        let metrics = self.metrics.lock().unwrap();
        let mut res = HashMap::with_capacity(metrics.len());
        for (host, snapshot) in metrics.iter() {
            let mut copy = snapshot.clone();
            // Active nodes report their current open count; removed hosts 0.
            copy.set_open_connections(open_by_host.get(host).copied().unwrap_or(0));
            res.insert(host.clone(), copy);
        }
        res
    }

    /// Value of the max-retries-exceeded counter.
    pub fn max_retries_exceeded_count(&self) -> u64 {
        self.max_retries_exceeded_count.load(Ordering::Relaxed)
    }

    /// Value of the total-timeout-exceeded counter.
    pub fn total_timeout_exceeded_count(&self) -> u64 {
        self.total_timeout_exceeded_count.load(Ordering::Relaxed)
    }

    /// Builds the per-node reserved labels (node/host/cluster/app-id) merged
    /// with the user-provided labels.
    pub fn node_labels(&self) -> Labels {
        let policy = self.metrics_policy();
        let user_labels = &policy.labels;
        let client_policy = self.client_policy();
        let cluster_name = client_policy.cluster_name.clone().unwrap_or_default();
        let app_id = client_policy.application_id.clone().unwrap_or_default();

        let mut labels = Labels::new();
        for node in self.nodes().iter() {
            let mut entries: HashMap<String, String> = HashMap::new();
            for label_map in user_labels.entries() {
                for (k, v) in label_map {
                    entries.insert(k.clone(), v.clone());
                }
            }
            entries.insert("node".to_string(), node.name().to_string());
            entries.insert("host".to_string(), node.host().to_string());
            entries.insert("cluster".to_string(), cluster_name.clone());
            entries.insert("app-id".to_string(), app_id.clone());
            labels.push(entries);
        }
        labels
    }

    /// Identifies nodes that should be removed from the cluster.
    ///
    /// Following logic:
    /// - Inactive nodes are always removed.
    /// - Single-node clusters: remove after 5 consecutive failures if all peer
    ///   refreshes also failed (refreshCount == 0).
    /// - Multi-node clusters: remove if referenceCount == 0 (not referenced by
    ///   any peer) AND either failing or not in partition map.
    async fn find_nodes_to_remove(&self, peers: &Peers) {
        let refresh_count = peers.refresh_count();
        let nodes = self.nodes();

        for node in &nodes {
            // Inactive nodes must be removed.
            if !node.is_active() {
                if !peers.contains_node_to_remove(node) {
                    peers.add_node_to_remove(node.clone());
                }
                continue;
            }

            // All node info requests failed and this node had 5 consecutive
            // failures. Remove it. If no nodes are left, seeds will be tried
            // in the next cluster tend iteration. Mirrors Java's
            // `findNodesToRemove`.
            if refresh_count == 0 && node.failures() >= 5 {
                if !peers.contains_node_to_remove(node) {
                    peers.add_node_to_remove(node.clone());
                }
                continue;
            }

            // Multi-node cluster: remove if not referenced by any other node.
            if nodes.len() > 1 && refresh_count >= 1 && node.reference_count() == 0 {
                if node.failures() == 0 {
                    // Node is alive but not referenced. Drop only if it's
                    // also not mapped to any partition.
                    if !self.find_node_in_partition_map(node.clone())
                        && !peers.contains_node_to_remove(node)
                    {
                        peers.add_node_to_remove(node.clone());
                    }
                } else if !peers.contains_node_to_remove(node) {
                    // Node not responding. Remove it.
                    peers.add_node_to_remove(node.clone());
                }
            }
        }
    }

    fn add_nodes_and_aliases(&self, friend_list: &[Arc<Node>]) {
        for node in friend_list {
            self.add_aliases(node.clone());
        }
        if self.metrics_enabled() {
            for node in friend_list {
                node.metrics().incr_node_added();
            }
        }
        self.add_nodes(friend_list);
    }

    fn remove_nodes_and_aliases(&self, mut nodes_to_remove: Vec<Arc<Node>>) {
        for node in &nodes_to_remove {
            debug!("Removing alias for node {node}");
            for alias in node.aliases() {
                self.remove_alias(&alias);
            }
        }
        // Record the removal and drain the node's final metrics into the
        // per-host accumulator so they survive the node going away (the map
        // keeps removed-host entries, reported with zero open connections).
        if self.metrics_enabled() {
            for node in &nodes_to_remove {
                node.metrics().incr_node_removed();
            }
            self.aggregate_node_metrics(&nodes_to_remove);
        }
        for node in &mut nodes_to_remove {
            debug!("Closing node {node}");
            node.close();
        }
        self.remove_nodes(&nodes_to_remove);
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

        // `seed_only_cluster` cap: once all seeds have been validated
        // and added, refuse to grow the cluster view further. Mirrors
        // Go's `addNodes` short-circuit `SeedOnlyCluster && GetSeedCount() == len(nodes)`.
        if self.client_policy().seed_only_cluster {
            let seed_count = self.seeds.load().len();
            if nodes.len() >= seed_count {
                return;
            }
        }

        // Dedup by name — `add_nodes` runs twice in normal flow (init
        // seed pass and tend's add-nodes-and-aliases) so a same-name
        // append must be a no-op. Mirrors Go's `findNodeName` guard
        // inside `Cluster.addNodes`.
        for node in friend_list {
            if !nodes.iter().any(|n| n.name() == node.name()) {
                nodes.push(node.clone());
            }
        }
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

    /// Returns whether `namespace` is configured for strong consistency on the
    /// cluster.
    ///
    /// Reads from the in-memory partition map, which the tend loop refreshes
    /// from the `replicas` info command. The map records SC mode per namespace
    /// as `regime != 0`, so this is a synchronous lookup with no network I/O.
    ///
    /// - `Some(true)` — the namespace is an SC namespace.
    /// - `Some(false)` — the namespace is AP.
    /// - `None` — the namespace is not present in the partition map (unknown
    ///   namespace, or partition map not yet populated for this cluster).
    pub fn is_strong_consistency(&self, namespace: &str) -> Option<bool> {
        let map = self.partition_map.load();
        map.get(namespace).map(|p| p.sc_mode)
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

        Err(Error::connection("No active node"))
    }

    pub fn get_node_by_name(&self, node_name: &str) -> Result<Arc<Node>> {
        let node_array = self.nodes();

        for node in &node_array {
            if node.name() == node_name {
                return Ok(node.clone());
            }
        }

        Err(Error::invalid_node(format!(
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
        // Mark closed first so any in-flight tend cycle bails early.
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Actually close the tend channel: locking the Mutex and dropping
        // the *guard* only releases the lock — it doesn't drop the Sender,
        // which is what tend_thread's `try_recv` watches via TryRecvError::
        // Closed. Use Sender::close_channel() to signal closure. The tend
        // thread itself clears `nodes` and `aliases` as its last act before
        // exiting (see `tend_thread`), preserving the single-writer
        // invariant on those fields.
        self.tend_channel.lock().await.close_channel();
        Ok(())
    }
}
