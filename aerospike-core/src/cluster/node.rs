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

use hazarc::AtomicArc;

use crate::cluster::node_validator::NodeValidator;
use crate::cluster::peers::Peers;
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
pub const PEERS_GENERATION: &str = "peers-generation";

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
    peers_generation: AtomicIsize,
    peers_count: AtomicUsize,
    partition_changed: AtomicBool,
    // Which racks are these things part of
    rack_ids: AtomicArc<HashMap<String, usize>>,
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
            partition_changed: AtomicBool::new(false),
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

    /// Increments the reference count by 1.
    pub fn increment_reference_count(&self) {
        self.reference_count.fetch_add(1, Ordering::Relaxed);
    }

    // Returns the peers count
    pub fn peers_count(&self) -> usize {
        self.peers_count.load(Ordering::Relaxed)
    }

    // Returns whether partition changed during this tend cycle
    pub fn partition_changed(&self) -> bool {
        self.partition_changed.load(Ordering::Relaxed)
    }

    /// Phase 1 of the tend cycle: Refresh node metadata and check generation numbers.
    ///
    /// Sends lightweight info commands to verify node identity and check
    /// peers-generation and partition-generation. Does NOT fetch the full peer list.
    /// Sets `peers.gen_changed` if the peers generation differs from the last known value.
    pub async fn refresh(&self, peers: &Peers) -> Result<()> {
        if !self.is_active() {
            return Ok(());
        }

        self.reference_count.store(0, Ordering::Relaxed);
        self.responded.store(false, Ordering::Relaxed);
        self.partition_changed.store(false, Ordering::Relaxed);

        let mut commands = vec![
            "node",
            "cluster-name",
            PEERS_GENERATION,
            PARTITION_GENERATION,
        ];

        if self.client_policy.rack_ids.is_some() {
            commands.push("rack-ids");
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

        self.verify_peers_generation(&info_map, peers)
            .map_err(|e| e.chain_error("Failed to verify peers generation"))?;

        self.verify_partition_generation(&info_map)
            .map_err(|e| e.chain_error("Failed to verify partition generation"))?;

        if let Err(err) = self.update_rack_info(&info_map) {
            warn!("Updating node rack info failed: {err}");
        }

        self.responded.store(true, Ordering::Relaxed);
        self.reset_failures();
        peers.increment_refresh_count();
        self.reference_count.fetch_add(1, Ordering::Relaxed);

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
        let info_map = self.info(&admin_policy, &[peers_cmd]).await.map_err(|e| {
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

        let result = PeersParser::new(peer_string).parse().map_err(|e| {
            self.refresh_failed();
            e
        })?;

        if !result.peers.is_empty() {
            peers.increment_refresh_count();
            peers.append_peers(result.peers);
        }

        self.peers_generation
            .store(result.generation as isize, Ordering::Relaxed);
        self.peers_count
            .store(peers.peer_count(), Ordering::Relaxed);

        Ok(())
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

    /// Compares the server's peers-generation with the node's last known value.
    /// Sets `peers.gen_changed` to true if they differ.
    fn verify_peers_generation(
        &self,
        info_map: &HashMap<String, String>,
        peers: &Peers,
    ) -> Result<()> {
        match info_map.get(PEERS_GENERATION) {
            None => Err(Error::BadResponse("Missing peers-generation".to_string())),
            Some(gen_string) => {
                let gen = gen_string.parse::<isize>()?;
                let changed = self.peers_generation.load(Ordering::Relaxed) != gen;
                peers.set_gen_changed(changed);
                Ok(())
            }
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

    fn update_rack_info(&self, info_map: &HashMap<String, String>) -> Result<()> {
        if self.client_policy.rack_ids.is_none() {
            return Ok(());
        }

        // Receive format: <ns1>:<rack1>;<ns2>:<rack2>...
        let rack_ids = info_map.get("rack-ids").map(String::as_str).unwrap_or("");

        // Server does not support rack-aware
        if rack_ids.is_empty() || rack_ids.to_uppercase().starts_with("ERROR") {
            return Err(Error::BadResponse(
                "ClientPolicy.rack_ids is set, but the server does not support this feature."
                    .to_string(),
            ));
        }

        self.parse_rack(rack_ids)
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
