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
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cluster::node_validator::{NodeValidator, NodeFeatures};
use crate::commands::Message;
use crate::errors::{ErrorKind, Result, ResultExt};
use crate::net::{ConnectionPool, Host, PooledConnection};
use crate::policy::ClientPolicy;

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
    aliases: std::sync::Mutex<Vec<Host>>,
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

    features: NodeFeatures,
}

impl Node {
    #![allow(missing_docs)]
    pub fn new(client_policy: ClientPolicy, nv: Arc<NodeValidator>) -> Self {
        Node {
            client_policy: client_policy.clone(),
            name: nv.name.clone(),
            aliases: std::sync::Mutex::new(nv.aliases.clone()),
            address: nv.address.clone(),

            host: nv.aliases[0].clone(),
            rebalance_generation: AtomicIsize::new(if client_policy.rack_ids.is_some() {-1} else {0}),
            connection_pool: ConnectionPool::new(nv.aliases[0].clone(), client_policy),
            failures: AtomicUsize::new(0),
            partition_generation: AtomicIsize::new(-1),
            refresh_count: AtomicUsize::new(0),
            reference_count: AtomicUsize::new(0),
            responded: AtomicBool::new(false),
            active: AtomicBool::new(true),
            features: nv.features,
            rack_ids: std::sync::Mutex::new(HashMap::new()),
        }
    }
    // Returns the Node address
    pub fn address(&self) -> &str {
        &self.address
    }

    // Returns the Node name
    pub fn name(&self) -> &str {
        &self.name
    }

    // Returns the active client policy
    pub const fn client_policy(&self) -> &ClientPolicy {
        &self.client_policy
    }

    pub fn host(&self) -> Host {
        self.host.clone()
    }

    // Returns true if the Node supports floats
    pub const fn features(&self) -> &NodeFeatures {
        &self.features
    }

    // Returns true if the Node supports geo
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
            self.services_name(),
        ];

        if self.client_policy.rack_ids.is_some() {
            commands.push(REBALANCE_GENERATION);
        }

        let info_map = self
            .info(&commands)
            .await
            .chain_err(|| "Info command failed")?;
        self.validate_node(&info_map)
            .chain_err(|| "Failed to validate node")?;
        self.responded.store(true, Ordering::Relaxed);
        let friends = self
            .add_friends(current_aliases, &info_map)
            .chain_err(|| "Failed to add friends")?;
        self.update_partitions(&info_map)
            .chain_err(|| "Failed to update partition generation")?;
        self.update_rebalance_generation(&info_map)
            .chain_err(|| "Failed to update rebalance generation")?;
        self.reset_failures();
        Ok(friends)
    }

    // Returns the services that the client should use for the cluster tend
    const fn services_name(&self) -> &'static str {
        if self.client_policy.use_services_alternate {
            "services-alternate"
        } else {
            "services"
        }
    }

    fn validate_node(&self, info_map: &HashMap<String, String>) -> Result<()> {
        self.verify_node_name(info_map)?;
        self.verify_cluster_name(info_map)?;
        Ok(())
    }

    fn verify_node_name(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match info_map.get("node") {
            None => Err(ErrorKind::InvalidNode("Missing node name".to_string()).into()),
            Some(info_name) if info_name == &self.name => Ok(()),
            Some(info_name) => {
                self.inactivate();
                Err(ErrorKind::InvalidNode(format!(
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
                None => Err(ErrorKind::InvalidNode("Missing cluster name".to_string()).into()),
                Some(info_name) if info_name == expected => Ok(()),
                Some(info_name) => {
                    self.inactivate();
                    Err(ErrorKind::InvalidNode(format!(
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

        let friend_string = match info_map.get(self.services_name()) {
            None => bail!(ErrorKind::BadResponse("Missing services list".to_string())),
            Some(friend_string) if friend_string.is_empty() => return Ok(friends),
            Some(friend_string) => friend_string,
        };

        let friend_names = friend_string.split(';');
        for friend in friend_names {
            let mut friend_info = friend.split(':');
            if friend_info.clone().count() != 2 {
                error!(
                    "Node info from asinfo:services is malformed. Expected HOST:PORT, but got \
                     '{}'",
                    friend
                );
                continue;
            }

            let host = friend_info.next().unwrap();
            let port = u16::from_str(friend_info.next().unwrap())?;
            let alias = match self.client_policy.ip_map {
                Some(ref ip_map) if ip_map.contains_key(host) => {
                    Host::new(ip_map.get(host).unwrap(), port)
                }
                _ => Host::new(host, port),
            };

            if current_aliases.contains_key(&alias) {
                self.reference_count.fetch_add(1, Ordering::Relaxed);
            } else if !friends.contains(&alias) {
                friends.push(alias);
            }
        }

        Ok(friends)
    }

    pub(crate) fn update_partitions(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match info_map.get(PARTITION_GENERATION) {
            None => bail!(ErrorKind::BadResponse(
                "Missing partition generation".to_string()
            )),
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
        if let Ok(locked) = self.rack_ids.lock() {
            locked.get(namespace).map_or(false, |r|rack_ids.contains(r))
        } else {
            false
        }
    }

    pub fn parse_rack(&self, buf: &str) -> Result<()> {
        let new_table = buf.split(';').map(|entry|{
            let (key, val) = entry.split_once(':').ok_or("Invalid rack entry")?;
            Ok((key.to_string(), val.parse::<usize>()?))
        }).collect::<Result<HashMap<_, _>>>()?;

        *self.rack_ids.lock().map_err(|err|err.to_string())? = new_table;
        Ok(())
    }

    // Get a connection to the node from the connection pool
    pub async fn get_connection(&self) -> Result<PooledConnection> {
        self.connection_pool.get().await
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
        self.aliases.lock().unwrap().to_vec()
    }

    // Add an alias to the node
    pub fn add_alias(&self, alias: Host) {
        let mut aliases = self.aliases.lock().unwrap();
        aliases.push(alias);
        self.reference_count.fetch_add(1, Ordering::Relaxed);
    }

    // Set the node inactive and close all connections in the pool
    pub async fn close(&mut self) {
        self.inactivate();
        self.connection_pool.close().await;
    }

    // Send info commands to this node
    pub async fn info(&self, commands: &[&str]) -> Result<HashMap<String, String>> {
        let mut conn = self.get_connection().await?;
        Message::info(&mut conn, commands).await.map_err(|e| {
            conn.invalidate();
            e
        })
    }

    // Get the partition generation
    pub fn partition_generation(&self) -> isize {
        self.partition_generation.load(Ordering::Relaxed)
    }

    // Get the rebalance generation
    pub fn rebalance_generation(&self) -> isize {
        self.rebalance_generation.load(Ordering::Relaxed)
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
