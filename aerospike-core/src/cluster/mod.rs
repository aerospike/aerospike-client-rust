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
use std::cell::OnceCell;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Arc;
use std::vec::Vec;

pub use self::node::Node;
pub use self::partition::Partition;

use self::node_validator::NodeValidator;
use self::partition_tokenizer::PartitionTokenizer;
use self::peers::{Peer, Peers};

use crate::commands::admin_command::AdminCommand;
use crate::errors::{Error, Result};
use crate::net::Host;
use crate::policy::ClientPolicy;
use crate::AdminPolicy;
use aerospike_rt::Mutex;
use futures::channel::mpsc;
use futures::channel::mpsc::{Receiver, Sender, TryRecvError};
use hazarc::AtomicArc;

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
        let cluster = Arc::new(Cluster {
            hashed_pass: AtomicArc::from(policy.hashed_pass()),
            client_policy: AtomicArc::from(policy),

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
        });
        // try to seed connections for first use
        Cluster::wait_till_stabilized(cluster.clone()).await?;

        // apply policy rules
        if cluster.client_policy.load().fail_if_not_connected && !cluster.is_connected() {
            // Mirrors Java's `Peers.clusterInitError`: surface every per-seed
            // error from the most recent seed pass so callers know *why*
            // each host failed, not just that "host(s) failed".
            return Err(Error::Connection(cluster.format_init_error()));
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
            match rx.try_recv() {
                Ok(()) => unreachable!(),
                Err(TryRecvError::Closed) => break,
                Err(TryRecvError::Empty) => {
                    if let Err(err) = cluster.tend().await {
                        log_error_chain!(err, "Error tending cluster");
                    }
                    aerospike_rt::sleep(Duration::from_millis(u64::from(tend_interval))).await;
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

        let seed_only = self.client_policy().seed_only_cluster;

        // Per-tend peer state. `gen_changed` is initialized to false (Java's
        // default) and set to true if any node's peers-generation differs.
        let mut peers = Peers::new(16, 16);
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
            // Phase 1: refresh all known nodes (light info commands only).
            for node in &nodes {
                // Reap idle connections, but keep enough of them alive via a
                // cheap info probe to stay at or above `min_conns_per_node` —
                // avoids the full TCP-connect round-trip that `fill_min_conns`
                // would otherwise pay to replace them.
                let processed = node.reap_and_refresh_idle_connections().await;
                if processed > 0 {
                    debug!("Reap/refresh processed {processed} idle connections on {node}");
                }

                if let Err(err) = node.refresh(&peers).await {
                    warn!("Node `{node}` refresh failed: {err}");
                }
            }

            // Phases 2 + 3 + commit are skipped under `seed_only_cluster`:
            // peer discovery is the very thing the option disables. We
            // also skip removal so a transient seed failure doesn't
            // evict the seed.
            if !seed_only {
                // Phase 2: when peers-generation changed on any node, refresh
                // the full peer list and reconcile add/remove decisions. Each
                // node's peer list is parsed and materialized in isolation so
                // a single unreachable peer doesn't prevent the others from
                // committing their generation (Java's `peersValidated`).
                if peers.gen_changed() {
                    peers.reset_refresh_count();

                    for node in &nodes {
                        if let Err(err) = node.refresh_peers(&mut peers).await {
                            warn!("Node `{node}` peer refresh failed: {err}");
                        }
                        self.materialize_peers(&mut peers).await;
                    }

                    // Decide which existing nodes can be dropped.
                    self.find_nodes_to_remove(&mut peers).await;

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

                    for node in &drained {
                        if let Err(err) = node.refresh_peers(&mut peers).await {
                            warn!("Node `{node}` peer refresh failed: {err}");
                        }
                        self.materialize_peers(&mut peers).await;
                    }
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
        // generation flag flipped during phase 1.
        let active_nodes = self.nodes();
        let peers_refresh_count = peers.refresh_count();
        let mut partition_map = OnceCell::new();
        for node in &active_nodes {
            if node.partition_changed() {
                // Split-cluster guard: skip a node that thinks it's the only
                // one in the cluster (peers_count == 0) when we've already
                // refreshed peers from at least two nodes this tend
                // (`peers.refresh_count > 1`). Lets the rest of the cluster's
                // map win when an isolated node has stale or zero-peer view.
                // Mirrors Java `Node.refreshPartitions`.
                if node.peers_count() == 0 && peers_refresh_count > 1 {
                    debug!(
                        "Skipping partition update for node {node}: reports 0 peers in {}-node cluster (likely split)",
                        active_nodes.len()
                    );
                } else {
                    partition_map.get_or_init(|| (*self.partition_map.load().clone()).clone());
                    if let Err(err) = self
                        .update_partitions(partition_map.get_mut().unwrap(), node)
                        .await
                    {
                        warn!("Node `{node}` partition update failed: {err}");
                    }
                }
            }

            if node.rebalance_changed() {
                if let Err(err) = self.update_rack_ids(node).await {
                    warn!("Node `{node}` rack update failed: {err}");
                }
            }
        }

        if let Some(partition_map) = partition_map.take() {
            self.partition_map.store(Arc::new(partition_map));
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
    async fn materialize_peers(&self, peers: &mut Peers) {
        let peers_list = peers.peers_list();
        // Reset the working set for the next parsing-node iteration; we've
        // taken a copy of what was in there.
        peers.clear_peers();

        for mut peer in peers_list {
            if self.peer_exists(peers, &mut peer).await {
                continue;
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

            // If none of the peer's hosts validated, invalidate the parsing
            // node's pending generation so it won't be committed at the end
            // of the tend. The peer (and any siblings from the same node)
            // will be re-parsed next tend.
            if !materialized {
                if let Some(ref source) = peer.from_node_name {
                    peers.invalidate_pending_generation(source);
                }
            }
        }
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
    async fn peer_exists(&self, peers: &mut Peers, peer: &mut Peer) -> bool {
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
                return Err(Error::BadResponse(
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
        let res = Node::new(self.client_policy(), Arc::new(nv));
        res.send_user_agent_id().await;
        res
    }

    /// Identifies nodes that should be removed from the cluster.
    ///
    /// Following logic:
    /// - Inactive nodes are always removed.
    /// - Single-node clusters: remove after 5 consecutive failures if all peer
    ///   refreshes also failed (refreshCount == 0).
    /// - Multi-node clusters: remove if referenceCount == 0 (not referenced by
    ///   any peer) AND either failing or not in partition map.
    async fn find_nodes_to_remove(&self, peers: &mut Peers) {
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
        self.add_nodes(friend_list);
    }

    fn remove_nodes_and_aliases(&self, mut nodes_to_remove: Vec<Arc<Node>>) {
        for node in &nodes_to_remove {
            debug!("Removing alias for node {node}");
            for alias in node.aliases() {
                self.remove_alias(&alias);
            }
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
