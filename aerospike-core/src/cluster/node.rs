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
use std::sync::{Arc, RwLock};

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
    aliases: RwLock<Vec<Host>>,
    address: String,

    connection_pool: ConnectionPool,
    failures: AtomicUsize,

    partition_generation: AtomicIsize,
    rebalance_generation: AtomicIsize,
    // Which racks are these things part of
    rack_ids: std::sync::Mutex<HashMap<String, usize>>,
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
            aliases: RwLock::new(nv.aliases.clone()),
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
            refresh_count: AtomicUsize::new(0),
            reference_count: AtomicUsize::new(0),
            responded: AtomicBool::new(false),
            active: AtomicBool::new(true),
            version: nv.version.clone(),
            rack_ids: std::sync::Mutex::new(HashMap::new()),
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
    pub fn version(&self) -> &Version {
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
            None => Err(Error::InvalidNode("Missing node name".to_string()).into()),
            Some(info_name) if info_name == &self.name => Ok(()),
            Some(info_name) => {
                self.inactivate();
                Err(Error::InvalidNode(format!(
                    "Node name has changed: '{}' => '{}'",
                    self.name, info_name
                ))
                .into())
            }
        }
    }

    fn verify_cluster_name(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match self.client_policy.cluster_name {
            None => Ok(()),
            Some(ref expected) => match info_map.get("cluster-name") {
                None => Err(Error::InvalidNode("Missing cluster name".to_string()).into()),
                Some(info_name) if info_name == expected => Ok(()),
                Some(info_name) => {
                    self.inactivate();
                    Err(Error::InvalidNode(format!(
                        "Cluster name mismatch: expected={},
                                                           got={}",
                        expected, info_name
                    ))
                    .into())
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
        for alias in hosts {
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
        self.rack_ids.lock().map_or(false, |locked| {
            locked
                .get(namespace)
                .map_or(false, |r| rack_ids.contains(r))
        })
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

        *self
            .rack_ids
            .lock()
            .map_err(|err| Error::ClientError(err.to_string()))? = new_table;
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
        self.aliases
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .to_vec()
    }

    // Add an alias to the node
    pub fn add_alias(&self, alias: Host) {
        let mut aliases = self.aliases.write().unwrap_or_else(|e| e.into_inner());
        aliases.push(alias);
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
        // Format: "1,go-<version>,<application-id>"
        let user_agent_id = format!("1,rust-{},{}", CLIENT_VERSION, app_id);
        let user_agent_id = base64::encode(&user_agent_id);
        let user_agent_command = format!("user-agent-set:value={}", user_agent_id);

        let policy = AdminPolicy {
            timeout: self.client_policy().timeout,
        };
        let _ = self.info(&policy, &[&user_agent_command]).await;
    }

    /// Fills the connection pool to the minimum required
    /// by the [ClientPolicy.min_conns_per_node]
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

        return Ok(count);
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
