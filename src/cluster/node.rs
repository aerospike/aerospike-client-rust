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

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::result::Result as StdResult;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use cluster::node_validator::NodeValidator;
use commands::Message;
use errors::*;
use net::{ConnectionPool, Host, PooledConnection};
use policy::ClientPolicy;

pub const PARTITIONS: usize = 4096;

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
    refresh_count: AtomicUsize,
    reference_count: AtomicUsize,
    responded: AtomicBool,
    use_new_info: bool,
    active: AtomicBool,

    supports_float: AtomicBool,
    supports_batch_index: AtomicBool,
    supports_replicas_all: AtomicBool,
    supports_geo: AtomicBool,
}

impl Node {
    pub fn new(client_policy: ClientPolicy, nv: Arc<NodeValidator>) -> Self {
        Node {
            client_policy: client_policy.clone(),
            name: nv.name.clone(),
            aliases: RwLock::new(nv.aliases.to_vec()),
            address: nv.address.to_owned(),
            use_new_info: nv.use_new_info,

            host: nv.aliases[0].clone(),
            connection_pool: ConnectionPool::new(nv.aliases[0].clone(), client_policy),
            failures: AtomicUsize::new(0),
            partition_generation: AtomicIsize::new(-1),
            refresh_count: AtomicUsize::new(0),
            reference_count: AtomicUsize::new(0),
            responded: AtomicBool::new(false),
            active: AtomicBool::new(true),

            supports_float: AtomicBool::new(nv.supports_float),
            supports_batch_index: AtomicBool::new(nv.supports_batch_index),
            supports_replicas_all: AtomicBool::new(nv.supports_replicas_all),
            supports_geo: AtomicBool::new(nv.supports_geo),
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn client_policy(&self) -> &ClientPolicy {
        &self.client_policy
    }

    pub fn host(&self) -> Host {
        self.host.clone()
    }

    pub fn supports_float(&self) -> bool {
        self.supports_float.load(Ordering::Relaxed)
    }

    pub fn supports_geo(&self) -> bool {
        self.supports_geo.load(Ordering::Relaxed)
    }

    pub fn reference_count(&self) -> usize {
        self.reference_count.load(Ordering::Relaxed)
    }

    pub fn refresh(&self, current_aliases: HashMap<Host, Arc<Node>>) -> Result<Vec<Host>> {
        self.reference_count.store(0, Ordering::Relaxed);
        self.responded.store(false, Ordering::Relaxed);
        self.refresh_count.fetch_add(1, Ordering::Relaxed);

        let commands = vec![
            "node",
            "cluster-name",
            "partition-generation",
            self.services_name(),
        ];
        let info_map = self
            .info(None, &commands)
            .chain_err(|| "Info command failed")?;
        self.validate_node(&info_map)
            .chain_err(|| "Failed to validate node")?;
        self.responded.store(true, Ordering::Relaxed);
        let friends = self
            .add_friends(current_aliases, &info_map)
            .chain_err(|| "Failed to add friends")?;
        self.update_partitions(&info_map)
            .chain_err(|| "Failed to update partitions")?;
        self.reset_failures();

        Ok(friends)
    }

    fn services_name(&self) -> &'static str {
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
            Some(friend_string) if friend_string == "" => return Ok(friends),
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

    fn update_partitions(&self, info_map: &HashMap<String, String>) -> Result<()> {
        match info_map.get("partition-generation") {
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

    pub fn get_connection(&self, timeout: Option<Duration>) -> Result<PooledConnection> {
        self.connection_pool.get(timeout)
    }

    pub fn failures(&self) -> usize {
        self.failures.load(Ordering::Relaxed)
    }

    fn reset_failures(&self) {
        self.failures.store(0, Ordering::Relaxed)
    }

    pub fn increase_failures(&self) -> usize {
        self.failures.fetch_add(1, Ordering::Relaxed)
    }

    fn inactivate(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn aliases(&self) -> Vec<Host> {
        self.aliases.read().to_vec()
    }

    pub fn add_alias(&self, alias: Host) {
        let mut aliases = self.aliases.write();
        aliases.push(alias);
        self.reference_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn close(&mut self) {
        self.inactivate();
        self.connection_pool.close();
    }

    pub fn info(
        &self,
        timeout: Option<Duration>,
        commands: &[&str],
    ) -> Result<HashMap<String, String>> {
        let mut conn = self.get_connection(timeout)?;
        Message::info(&mut conn, commands).or_else(|e| {
            conn.invalidate();
            Err(e)
        })
    }

    pub fn partition_generation(&self) -> isize {
        self.partition_generation.load(Ordering::Relaxed)
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
