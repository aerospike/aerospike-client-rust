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

use aerospike_rt::time::Instant;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use hazarc::AtomicArc;

use crate::cluster::node_validator::NodeValidator;
use crate::cluster::peers_parser::PeersParser;
use crate::cluster::CLIENT_VERSION;
use crate::commands::Message;
use crate::errors::{Error, Result};
use crate::net::{ConnectionPool, Host, PooledConnection};
use crate::policy::{AdminPolicy, ClientPolicy};
use crate::Version;

pub const PARTITIONS: usize = 4096;
pub const PARTITION_GENERATION: &str = "partition-generation";
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
    // Which racks are these things part of
    rack_ids: AtomicArc<HashMap<String, usize>>,
    refresh_count: AtomicUsize,
    reference_count: AtomicUsize,
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
}

impl Drop for Node {
    fn drop(&mut self) {
        debug!("Node closed {self}");
        self.close();
        self.connection_pool.close();
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
            failures: AtomicUsize::new(0),
            error_rate_count: AtomicUsize::new(0),
            node_max_error_rate: AtomicUsize::new(client_policy.max_error_rate),
            partition_generation: AtomicIsize::new(-1),
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
        let friends = self
            .add_friends(current_aliases, &info_map)
            .map_err(|e| e.chain_error("Failed to add friends"))?;
        self.update_partitions(&info_map)
            .map_err(|e| e.chain_error("Failed to update partitions"))?;
        self.update_rebalance_generation(&info_map)
            .map_err(|e| e.chain_error("Failed to update rebalance generation"))?;
        self.reset_failures();
        let _ = self.fill_min_conns().await;
        Ok(friends)
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

        let friend_string = match info_map.get(self.client_policy.peers_string()) {
            None => return Err(Error::BadResponse("Missing services list".to_string())),
            Some(friend_string) if friend_string.is_empty() => return Ok(friends),
            Some(friend_string) => friend_string,
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
            } else if !friends.contains(&alias) {
                friends.push(alias);
            }
        }

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

    // Get a connection to the node from the connection pool. When the pool
    // has to open a new connection, the connect is bounded by `deadline`
    // (the command's total-timeout deadline); pass `None` on paths without
    // one — admin, info and tend traffic — to keep today's fail-fast
    // semantics with the `ClientPolicy::timeout()` connect bound.
    pub async fn get_connection(
        &self,
        hint: u8,
        deadline: Option<Instant>,
    ) -> Result<PooledConnection> {
        if !self.is_active() {
            return Err(Error::InvalidNode(format!(
                "Cannot get a connection for node. The node `{self}` is inactive"
            )));
        }

        if let Ok(conn) = self.connection_pool.get(hint) {
            return Ok(conn);
        }

        // Honour the caller's hint on a pool miss too. Passing 0 sent every
        // miss to queue 0, which had to fill up before any other queue was
        // touched — the opposite of what `conn_pools_per_node` is for.
        self.connection_pool
            .make_conn(usize::from(hint), deadline)
            .await
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
        let mut conn = self.get_connection(0, None).await?;
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

    /// Fills the connection pool to the minimum required
    /// by the [`ClientPolicy.min_conns_per_node`]
    pub(crate) async fn fill_min_conns(&self) -> Result<usize> {
        if self.is_active() {
            let mut count = 0;

            let client_policy = self.client_policy();
            if client_policy.min_conns_per_node > 0 {
                // Measure the pool by every slot it holds, not by the idle ones
                // alone: `num_conns()` does not see in-flight connections, so a
                // busy pool looked empty and this kept opening more.
                //
                // The subtraction has to saturate, too. Once the pool has grown
                // past `min` — normal after any burst of load — `min -
                // num_conns()` underflowed: a panic in debug builds, and in
                // release a `to_fill` near `usize::MAX`, which refilled the
                // pool all the way to `max_conns_per_node` on every tend. That
                // is the churn this policy was supposed to prevent.
                let to_fill = client_policy
                    .min_conns_per_node
                    .saturating_sub(self.connection_pool.total_reserved());
                for _ in 0..to_fill {
                    self.connection_pool.make_conn(count, None).await?;
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

    use aerospike_rt::time::Instant;

    use crate::cluster::node_validator::NodeValidator;
    use crate::errors::Error;
    use crate::net::Host;
    use crate::policy::{AdminPolicy, ClientPolicy};
    use crate::Version;

    use super::Node;

    fn test_node() -> Node {
        let policy = ClientPolicy::default();
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            services: vec![],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
        });
        Node::new(policy, nv)
    }

    /// One idle connection in the pool, using the test [`crate::net::Connection`] (no real socket).
    async fn create_node_with_connection() -> Node {
        let node = test_node();
        let pconn = node
            .connection_pool
            .make_conn(0, None)
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

        let err = node.get_connection(0, None).await.unwrap_err();
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
            .get_connection(0, None)
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

    /// A node with several internal queues: a pool miss must open its
    /// connection on the queue the caller asked for, not always on queue 0.
    #[aerospike_macro::test]
    async fn get_connection_miss_uses_hint_to_select_queue() {
        let policy = ClientPolicy {
            conn_pools_per_node: 4,
            max_conns_per_node: 8, // 2 per queue — room to observe distribution
            ..ClientPolicy::default()
        };
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            services: vec![],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
        });
        let node = Node::new(policy, nv);

        // Four misses with distinct hints; each should land on its own queue.
        let _c0 = node.get_connection(0, None).await.expect("hint=0");
        let _c1 = node.get_connection(1, None).await.expect("hint=1");
        let _c2 = node.get_connection(2, None).await.expect("hint=2");
        let _c3 = node.get_connection(3, None).await.expect("hint=3");

        for (i, queue) in node.connection_pool.queues().iter().enumerate() {
            assert_eq!(
                queue.reserved(),
                1,
                "queue[{i}] must hold exactly one connection"
            );
        }
    }

    /// A pool that has grown past `min_conns_per_node` must simply have nothing
    /// to fill. The old `min - num_conns()` subtraction underflowed here.
    #[aerospike_macro::test]
    async fn fill_min_conns_does_nothing_once_the_pool_is_above_min() {
        let policy = ClientPolicy {
            min_conns_per_node: 2,
            max_conns_per_node: 16,
            conn_pools_per_node: 1,
            ..ClientPolicy::default()
        };
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            services: vec![],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
        });
        let node = Node::new(policy, nv);

        // Grow the pool well past `min`, as a burst of load would.
        for i in 0..6 {
            drop(
                node.connection_pool
                    .make_conn(i, None)
                    .await
                    .expect("make_conn failed"),
            );
        }
        assert_eq!(node.connection_pool.total_reserved(), 6);

        let created = node.fill_min_conns().await.expect("fill_min_conns failed");
        assert_eq!(created, 0, "nothing to fill when the pool is above min");
        assert_eq!(
            node.connection_pool.total_reserved(),
            6,
            "fill_min_conns must not grow a pool that is already above min"
        );
    }

    /// In-flight connections count towards `min_conns_per_node`: they are real
    /// connections, just busy. Counting only the idle ones made a loaded pool
    /// look empty, so every tend opened more.
    #[aerospike_macro::test]
    async fn fill_min_conns_counts_in_flight_connections() {
        let policy = ClientPolicy {
            min_conns_per_node: 2,
            max_conns_per_node: 16,
            conn_pools_per_node: 1,
            ..ClientPolicy::default()
        };
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            services: vec![],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
        });
        let node = Node::new(policy, nv);

        // Hold both connections, so the queue itself is empty.
        let _held = [
            node.connection_pool.make_conn(0, None).await.expect("conn 1"),
            node.connection_pool.make_conn(0, None).await.expect("conn 2"),
        ];
        assert_eq!(node.connection_pool.num_conns(), 0, "both are in flight");
        assert_eq!(node.connection_pool.total_reserved(), 2);

        let created = node.fill_min_conns().await.expect("fill_min_conns failed");
        assert_eq!(
            created, 0,
            "min is already met by the in-flight connections"
        );
    }

    #[aerospike_macro::test]
    async fn node_drop_inactivates_and_closes_pool_when_last_arc_dropped() {
        let arc = Arc::new(create_node_with_connection().await);
        let queue_witness = {
            let pconn = arc
                .get_connection(0, None)
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

    /// Admin/info traffic must keep today's fail-fast semantics on an
    /// exhausted pool: the tend thread can never wait behind user commands.
    #[aerospike_macro::test]
    async fn info_fails_fast_when_pool_is_exhausted() {
        let policy = ClientPolicy {
            max_conns_per_node: 1,
            conn_pools_per_node: 1,
            ..ClientPolicy::default()
        };
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            services: vec![],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
        });
        let node = Node::new(policy, nv);
        let _held = node
            .get_connection(0, None)
            .await
            .expect("first borrow saturates the pool");

        let admin_policy = AdminPolicy { timeout: 30_000 };
        let start = Instant::now();
        let err = node
            .info(&admin_policy, &["node"])
            .await
            .expect_err("exhausted pool must fail the info call");
        let elapsed = start.elapsed();

        assert!(
            err.is_pool_empty(),
            "info should surface the pool state, got: {:?}", err
        );
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "info must not wait behind user traffic: {:?}", elapsed
        );
    }
}
