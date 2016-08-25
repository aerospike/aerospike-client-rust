// Copyright 2015-2016 Aerospike, Inc.
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

use std::rc::Rc;
use std::str::FromStr;
use std::collections::{HashMap, VecDeque};
use std::sync::{RwLock, MutexGuard, Arc, Mutex};
use std::error::Error;
use std::num::ParseIntError;
use std::cell::RefCell;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::fmt;

use internal::wait_group::WaitGroup;
use net::{Host, Connection};
use command::info_command::Message;
use error::{AerospikeError, ResultCode, AerospikeResult};
use policy::ClientPolicy;
use Cluster;
use cluster::node_validator::NodeValidator;

pub const PARTITIONS: usize = 4096;

// Node represents an Aerospike Database Server Node
#[derive(Debug)]
pub struct Node {
    client_policy: ClientPolicy,
    name: String,
    host: Host,
    aliases: RwLock<Vec<Host>>,
    address: String,

    connections: RwLock<VecDeque<Connection>>,
    connection_count: AtomicUsize,
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
        let pool_size = client_policy.connection_pool_size;
        Node {
            client_policy: client_policy,
            name: nv.name.clone(),
            aliases: RwLock::new(nv.aliases.to_vec()),
            address: nv.address.to_owned(),
            use_new_info: nv.use_new_info,

            host: nv.aliases[0].clone(),
            connections: RwLock::new(VecDeque::with_capacity(pool_size)),
            connection_count: AtomicUsize::new(0),
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

    pub fn client_policy(&self) -> &ClientPolicy {
        &self.client_policy
    }

    // pub fn cluster(&self) -> Arc<RwLock<Cluster>> {
    //     self.cluster.clone()
    // }

    pub fn host(&self) -> Host {
        self.host.clone()
    }

    pub fn supports_float(&self) -> bool {
        self.supports_float.load(Ordering::Relaxed)
    }

    pub fn supports_geo(&self) -> bool {
        self.supports_geo.load(Ordering::Relaxed)
    }

    pub fn dec_connections(&self) -> usize {
        self.connection_count.fetch_sub(1, Ordering::Relaxed)
    }

    pub fn inc_connections(&self) -> usize {
        self.connection_count.fetch_add(1, Ordering::Relaxed)
    }

    pub fn reference_count(&self) -> usize {
        self.reference_count.load(Ordering::Relaxed)
    }

    pub fn refresh(&self, current_aliases: HashMap<Host, Arc<Node>>) -> AerospikeResult<Vec<Host>> {
        self.reference_count.store(0, Ordering::Relaxed);
        self.responded.store(false, Ordering::Relaxed);

        self.refresh_count.fetch_add(1, Ordering::Relaxed);

        let mut commands: Vec<&str> = vec![];
        if !self.client_policy.use_services_alternate {
            commands.extend(&["node", "partition-generation", "services"]);
        } else {
            commands.extend(&["node", "partition-generation", "servicges-alternate"]);
        }

        let info_map = try!(self.info(&commands));
        try!(self.verify_node_name(&info_map));

        self.responded.store(true, Ordering::Relaxed);

        let friends = try!(self.add_friends(current_aliases, &info_map));
        try!(self.update_partitions(&info_map));

        self.reset_failures();

        Ok(friends)
    }

    fn verify_node_name(&self, info_map: &HashMap<String, String>) -> AerospikeResult<()> {
        match info_map.get("node") {
            None => {
                return Err(AerospikeError::new(ResultCode::INVALID_NODE_ERROR,
                                               Some("Node name is empty.".to_string())))
            }
            Some(info_name) => {
                if !(&self.name == info_name) {
                    // Set node to inactive immediately.
                    self.active.store(false, Ordering::Relaxed);
                    return Err(AerospikeError::new(ResultCode::INVALID_NODE_ERROR,
                                                   Some(format!("Node name has changed. \
                                                                 Old='{}' New={}",
                                                                self.name,
                                                                info_name))));
                }
            }
        }

        Ok(())
    }

    fn add_friends(&self,
                   current_aliases: HashMap<Host, Arc<Node>>,
                   info_map: &HashMap<String, String>)
                   -> AerospikeResult<Vec<Host>> {
        let mut friends: Vec<Host> = vec![];

        let friend_string = match info_map.get("services") {
            None => {
                return Err(AerospikeError::new(ResultCode::INVALID_NODE_ERROR,
                                               Some("Node name is empty.".to_string())))
            }

            Some(friend_string) if friend_string == "" => return Ok(friends),
            Some(friend_string) => friend_string,
        };

        let friend_names = friend_string.split(";");
        for friend in friend_names {
            let mut friend_info = friend.split(":");
            if friend_info.clone().count() != 2 {
                error!("Node info from asinfo:services is malformed. Expected HOST:PORT, but got \
                        '{}'",
                       friend);
                continue;
            }

            let host = friend_info.nth(0).unwrap();
            let port = try!(u16::from_str(friend_info.nth(0).unwrap()));
            // let port = friend_info.nth(0).unwrap().parse::<u16>().unwrap_or({
            //     return Err(AerospikeError::new(ResultCode::PARSE_ERROR,
            //                                    Some("Node port wasn't a number.".to_string())));
            // });

            let alias = match self.client_policy.ip_map {
                Some(ref ip_map) if ip_map.contains_key(host) => {
                    Host::new(ip_map.get(host).unwrap(), port)
                }
                _ => Host::new(host, port),
            };

            if current_aliases.contains_key(&alias) {
                self.reference_count.fetch_add(1, Ordering::Relaxed);
            } else {
                if !self.find_alias(friends.as_slice(), &alias) {
                    friends.push(alias);
                }
            }
        }

        Ok(friends)
    }

    fn find_alias(&self, friends: &[Host], alias: &Host) -> bool {
        for h in friends {
            if h == alias {
                return true;
            }
        }

        return false;
    }

    fn update_partitions(&self, info_map: &HashMap<String, String>) -> AerospikeResult<()> {
        match info_map.get("partition-generation") {
            None => {
                return Err(AerospikeError::new(ResultCode::PARSE_ERROR,
                                               Some("partition-generation is empty.".to_string())))
            }
            Some(gen_string) => {
                let gen = try!(gen_string.parse::<isize>());
                self.partition_generation.store(gen, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    pub fn get_connection(&self,
                          timeout: Option<Duration>)
                          -> AerospikeResult<Connection> {
        let mut connections = self.connections.write().unwrap();
        loop {
            match connections.pop_front() {
                Some(mut conn) => {
                    {
                        if conn.is_idle() {
                            self.invalidate_connection(&mut conn);
                            continue;
                        }
                        try!(conn.set_timeout(timeout));
                    }
                    return Ok(conn);
                }
                None => {
                    if self.inc_connections() > self.client_policy.connection_pool_size {
                        // too many connections, undo
                        self.dec_connections();
                        return Err(AerospikeError::ErrConnectionPoolEmpty());
                    }

                    let conn = match Connection::new(self) {
                        Ok(c) => c,
                        Err(e) => {
                            self.dec_connections();
                            return Err(e);
                        }
                    };

                    match conn.set_timeout(timeout) {
                        Err(e) => {
                            self.dec_connections();
                            return Err(e);
                        },
                        _ => (),
                    }

                    return Ok(conn);

                }
            }
        }
    }

    pub fn put_connection(&self, mut conn: Connection) {
        if self.active.load(Ordering::Relaxed) {
            let mut connections = self.connections.write().unwrap();
            if connections.len() < self.client_policy.connection_pool_size {
                connections.push_back(conn);
            } else {
                self.invalidate_connection(&mut conn);
            }
        }
    }

    pub fn invalidate_connection(&self, conn: &mut Connection) {
        self.dec_connections();
        conn.close();
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

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn aliases(&self) -> Vec<Host> {
        let aliases = self.aliases.read().unwrap();
        aliases.to_vec()
    }

    fn set_aliases(&mut self, new_aliases: &[Host]) {
        let mut aliases = self.aliases.write().unwrap();
        *aliases = new_aliases.to_vec();
    }

    pub fn add_alias(&self, alias: Host) {
        let mut aliases = self.aliases.write().unwrap();
        aliases.push(alias);
        self.reference_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn close(&self) {
        self.active.store(false, Ordering::Relaxed);
        let mut connections = self.connections.write().unwrap();
        loop {
            if connections.pop_front().is_none() {
                break;
            }
        }
    }

    pub fn info(&self, commands: &[&str]) -> AerospikeResult<HashMap<String, String>> {
        let mut conn = try!(self.get_connection(self.client_policy.timeout));
        match Message::info(&mut conn, commands) {
            Ok(res) => Ok(res),
            Err(e) => {
                self.invalidate_connection(&mut conn);
                Err(e)
            }
        }
    }

    pub fn partition_generation(&self) -> AerospikeResult<isize> {
        Ok(self.partition_generation.load(Ordering::Relaxed))
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.name == other.name
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        format!("{}: ({})", self.name, self.host).fmt(f)
    }
}
