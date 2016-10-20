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

pub mod node_validator;
pub mod node;
pub mod partition;
pub mod partition_tokenizer;

use self::node_validator::NodeValidator;
pub use self::node::Node;
use self::partition::Partition;
use self::partition_tokenizer::PartitionTokenizer;

use std::collections::HashMap;
use std::vec::Vec;
use std::sync::{RwLock, Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::thread;
use std::sync::mpsc::{Sender, Receiver, TryRecvError};
use std::sync::mpsc;
use std::time::{Instant, Duration};

use net::Host;

use policy::ClientPolicy;
use error::{AerospikeError, ResultCode, AerospikeResult};

// Cluster encapsulates the aerospike cluster nodes and manages
// them.
#[derive(Debug)]
pub struct Cluster {
    // Initial host nodes specified by user.
    seeds: Arc<RwLock<Vec<Host>>>,

    // All aliases for all nodes in cluster.
    aliases: Arc<RwLock<HashMap<Host, Arc<Node>>>>,

    // Active nodes in cluster.
    nodes: Arc<RwLock<Vec<Arc<Node>>>>,

    // Hints for best node for a partition
    partition_write_map: Arc<RwLock<HashMap<String, Vec<Arc<Node>>>>>,

    // Random node index.
    node_index: AtomicIsize,

    client_policy: ClientPolicy,

    // mutex:       RWLock,
    tend_channel: Mutex<Sender<()>>,
    closed: AtomicBool,
}

impl<'a> Cluster {
    pub fn new(policy: ClientPolicy, hosts: &[Host]) -> AerospikeResult<Arc<Self>> {

        let (tx, rx): (Sender<()>, Receiver<()>) = mpsc::channel();
        let cluster = Arc::new(Cluster {
            client_policy: policy,

            seeds: Arc::new(RwLock::new(hosts.to_vec())),
            aliases: Arc::new(RwLock::new(HashMap::new())),
            nodes: Arc::new(RwLock::new(vec![])),

            partition_write_map: Arc::new(RwLock::new(HashMap::new())),
            node_index: AtomicIsize::new(0),

            tend_channel: Mutex::new(tx),
            closed: AtomicBool::new(false),
        });

        // try to seed connections for first use
        try!(Cluster::wait_till_stabilized(cluster.clone()));

        // apply policy rules
        if cluster.client_policy.fail_if_not_connected && !cluster.is_connected() {
            return Err(AerospikeError::new(ResultCode::INVALID_NODE_ERROR,
                                           Some(format!("Failed to connect to host(s): . The \
                                                         network connection(s) to cluster \
                                                         nodes may have timed out, or the \
                                                         cluster may be in a state of flux."))));
        }

        let cluster_for_tend = cluster.clone();
        thread::spawn(move || Cluster::tend_thread(cluster_for_tend, rx));


        debug!("New cluster initialized and ready to be used...");
        Ok(cluster)
    }


    fn tend_thread(cluster: Arc<Cluster>, rx: Receiver<()>) {
        let tend_interval = cluster.client_policy.tend_interval;

        loop {
            // try to read from the receive channel to see if it hung up
            match rx.try_recv() {
                Ok(_) => unreachable!(),
                // signaled to end
                Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => {
                    if let Err(err) = cluster.tend() {
                        error!("{}", err);
                    }

                    thread::sleep(tend_interval);
                }
            }
        }

        // close all nodes
        let nodes = cluster.nodes();
        for node in nodes {
            node.close();
        }

        if let Err(err) = cluster.set_nodes(vec![]) { 
            error!("{}", err);
        }
    }

    fn tend(&'a self) -> AerospikeResult<()> {
        let mut nodes = self.nodes();

        // All node additions/deletions are performed in tend thread.
        // If active nodes don't exist, seed cluster.
        if nodes.len() == 0 {
            debug!("No connections available; seeding...");
            try!(self.seed_nodes());

            nodes = self.nodes();
        }

        let mut friend_list: Vec<Host> = vec![];
        let mut refresh_count = 0;

        // Refresh all known nodes.
        for node in nodes.iter() {
            let old_gen = node.partition_generation();
            if node.is_active() {
                match node.refresh(self.aliases()) {
                    Ok(friends) => {
                        refresh_count += 1;

                        if friends.len() > 0 {
                            friend_list.extend_from_slice(&friends);
                        }

                        if old_gen != node.partition_generation() {
                            try!(self.update_partitions(node.clone()));
                        }
                    }
                    Err(err) => {
                        node.increase_failures();
                        warn!("Node `{}` refresh failed: {}", node, err);
                    }
                }
            }
        }

        // Add nodes in a batch.
        let add_list = try!(self.find_new_nodes_to_add(friend_list));
        if add_list.len() > 0 {
            try!(self.add_nodes(&add_list));
        }

        // IMPORTANT: Remove must come after add to remove aliases
        // Handle nodes changes determined from refreshes.
        // Remove nodes in a batch.
        let remove_list = try!(self.find_nodes_to_remove(refresh_count));
        if remove_list.len() > 0 {
            try!(self.remove_nodes(remove_list));
        }

        Ok(())
    }

    fn wait_till_stabilized(cluster: Arc<Cluster>) -> AerospikeResult<()> {
        let timeout = cluster.client_policy().timeout;
        let mut deadline = Instant::now();
        match timeout {
            Some(timeout) => deadline = deadline + timeout,
            None => deadline = deadline + Duration::from_millis(3_000),
        }

        let (tx, rx): (Sender<()>, Receiver<()>) = mpsc::channel();
        let cluster_for_tend = cluster.clone();
        let snd = tx.clone();
        thread::spawn(move || {
            let mut count: isize = -1;
            loop {
                if Instant::now() > deadline {
                    break;
                }

                if let Err(err) = cluster_for_tend.tend() {
                    error!("{}", err);
                }

                if (cluster_for_tend.nodes().len() as isize) == count {
                    break;
                }

                thread::sleep(Duration::from_millis(1));
                count = cluster_for_tend.nodes().len() as isize;
            }

            if let Err(err) = snd.send(()) {
                error!("{}", err);
            }
        });

        try!(rx.recv());
        Ok(())
    }

    pub fn client_policy(&self) -> &ClientPolicy {
        &self.client_policy
    }

    pub fn add_seeds(&self, new_seeds: &[Host]) -> AerospikeResult<()> {
        let mut seeds = self.seeds.write().unwrap();
        seeds.extend_from_slice(new_seeds);

        Ok(())
    }

    pub fn alias_exists(&self, host: &Host) -> AerospikeResult<bool> {
        let aliases = self.aliases.read().unwrap();
        Ok(aliases.contains_key(host))
    }

    fn set_partitions(&self, partitions: HashMap<String, Vec<Arc<Node>>>) -> AerospikeResult<()> {
        let mut partition_map = self.partition_write_map.write().unwrap();
        *partition_map = partitions;

        Ok(())
    }

    fn partitions(&self) -> Arc<RwLock<HashMap<String, Vec<Arc<Node>>>>> {
        self.partition_write_map.clone()
    }

    pub fn update_partitions(&self, node: Arc<Node>) -> AerospikeResult<()> {
        let mut conn = try!(node.get_connection(self.client_policy.timeout));
        let tokens = match PartitionTokenizer::new(&mut conn) {
            Ok(res) => res,
            Err(e) => {
                node.invalidate_connection(&mut conn);
                return Err(e);
            }
        };

        node.put_connection(conn);

        let nmap = try!(tokens.update_partition(self.partitions(), node));

        try!(self.set_partitions(nmap));

        Ok(())
    }

    pub fn seed_nodes(&'a self) -> AerospikeResult<bool> {
        let seed_array = self.seeds.read().unwrap();

        info!("Seeding the cluster. Seeds count: {}", seed_array.len());

        let mut list: Vec<Arc<Node>> = vec![];
        for seed in seed_array.iter() {
            let seed_node_validator = match NodeValidator::new(self, seed) {
                Err(err) => {
                    println!("Seed {} failed with error: {}", seed, err);
                    error!("Seed {} failed with error: {}", seed, err);
                    continue;
                }
                Ok(snv) => snv,
            };

            let al = seed_node_validator.aliases();
            for alias in al.iter() {
                let nv = Arc::new(if *seed == *alias {
                    seed_node_validator.clone()
                } else {
                    match NodeValidator::new(self, seed) {
                        Err(err) => {
                            error!("Seed {} failed with error: {}", alias, err);
                            continue;
                        }
                        Ok(snv) => snv,
                    }
                });

                if !try!(self.find_node_name(&list, &nv.name)) {
                    let node = Arc::new(try!(self.create_node(nv.clone())));
                    try!(self.add_aliases(node.clone()));
                    list.push(node);
                }
            }
        }

        if list.len() > 0 {
            try!(self.add_nodes(&list));
            return Ok(true);
        }

        Ok(false)
    }

    fn find_node_name(&self, list: &[Arc<Node>], name: &str) -> AerospikeResult<bool> {
        for node in list {
            if node.name() == name {
                return Ok((true));
            }
        }
        Ok(false)
    }

    fn find_new_nodes_to_add(&'a self, hosts: Vec<Host>) -> AerospikeResult<Vec<Arc<Node>>> {
        let mut list: Vec<Arc<Node>> = vec![];

        for host in hosts {
            let nv = match NodeValidator::new(self, &host) {
                Err(err) => {
                    error!("Add node {} failed with error: {}", host.name, err);
                    continue;
                }
                Ok(nv) => nv,
            };

            // Duplicate node name found.  This usually occurs when the server
            // services list contains both internal and external IP addresses
            // for the same node.  Add new host to list of alias filters
            // and do not add new node.
            match self.get_node_by_name(&nv.name) {
                Ok(node) => {
                    try!(self.add_alias(host, node.clone()));
                }
                _ => {
                    if let Some(node) = list.iter().find(|n| n.name() == nv.name) {
                        try!(self.add_alias(host, node.clone()));
                    }
                }
            };

            let node = try!(self.create_node(Arc::new(nv)));
            list.push(Arc::new(node));
        }

        Ok(list)
    }

    fn create_node(&self, nv: Arc<NodeValidator>) -> AerospikeResult<Node> {
        Ok(Node::new(self.client_policy.clone(), nv))
    }

    fn find_nodes_to_remove(&'a self, refresh_count: usize) -> AerospikeResult<Vec<Arc<Node>>> {
        let nodes = self.nodes.read().unwrap().to_vec();

        let mut remove_list: Vec<Arc<Node>> = vec![];
        let cluster_size = nodes.len();
        for node in nodes {
            let tnode = node.clone();

            if !node.is_active() {
                remove_list.push(tnode);
                continue;
            }

            match cluster_size {
                // Single node clusters rely on whether it responded to info requests.
                1 if node.failures() > 5 => {
                    if try!(self.seed_nodes()) {
                        // 5 consecutive info requests failed. Try seeds.
                        remove_list.push(tnode);
                    }
                }

                // Two node clusters require at least one successful refresh before removing.
                2 if refresh_count == 1 && node.reference_count() == 0 && node.failures() > 0 => {
                    remove_list.push(node)
                }
                _ => {
                    // Multi-node clusters require two successful node refreshes before removing.
                    if refresh_count >= 2 && node.reference_count() == 0 {
                        // Node is not referenced by other nodes.
                        // Check if node responded to info request.
                        if node.failures() == 0 {
                            // Node is alive, but not referenced by other nodes.  Check if mapped.
                            if !try!(self.find_node_in_partition_map(node)) {
                                remove_list.push(tnode);
                            }
                        } else {
                            // Node not responding. Remove it.
                            remove_list.push(tnode);
                        }
                    }
                }
            }
        }

        Ok(remove_list)
    }

    fn add_alias(&self, host: Host, node: Arc<Node>) -> AerospikeResult<()> {
        let mut aliases = self.aliases.write().unwrap();
        aliases.insert(host, node);
        Ok(())
    }

    fn remove_alias(&self, host: &Host) -> AerospikeResult<()> {
        let mut aliases = self.aliases.write().unwrap();
        aliases.remove(host);
        Ok(())
    }

    fn add_aliases(&self, node: Arc<Node>) -> AerospikeResult<()> {
        let mut aliases = self.aliases.write().unwrap();
        for alias in node.aliases() {
            aliases.insert(alias, node.clone());
        }

        Ok(())
    }

    fn find_node_in_partition_map(&self, filter: Arc<Node>) -> AerospikeResult<bool> {
        // let partitions1 = self.partitions;
        let partitions = self.partition_write_map.read().unwrap();

        for (_, map) in partitions.iter() {
            for node in map {
                if *node == filter {
                    return Ok(true);
                }
            }
        }

        return Ok(false);
    }

    fn add_nodes(&self, friend_list: &Vec<Arc<Node>>) -> AerospikeResult<()> {
        for node in friend_list {
            try!(self.add_aliases(node.clone()))
        }

        self.add_nodes_copy(&friend_list)
    }

    fn add_nodes_copy(&self, friend_list: &Vec<Arc<Node>>) -> AerospikeResult<()> {
        let mut nodes = self.nodes();
        nodes.extend(friend_list.iter().cloned());
        self.set_nodes(nodes)
    }

    fn remove_nodes(&self, nodes_to_remove: Vec<Arc<Node>>) -> AerospikeResult<()> {
        for node in nodes_to_remove.iter() {
            for alias in node.aliases() {
                // debug!("Removing alias {:?}", alias)
                try!(self.remove_alias(&alias));
            }
            node.close();
        }

        self.remove_nodes_copy(&nodes_to_remove)
    }

    fn set_nodes(&self, new_nodes: Vec<Arc<Node>>) -> AerospikeResult<()> {
        let mut nodes = self.nodes.write().unwrap();

        *nodes = new_nodes;

        Ok(())
    }

    fn remove_nodes_copy(&self, nodes_to_remove: &Vec<Arc<Node>>) -> AerospikeResult<()> {
        let nodes = self.nodes();
        let mut node_array: Vec<Arc<Node>> = vec![];

        for node in nodes.iter() {
            if !nodes_to_remove.contains(node) {
                node_array.push(node.clone());
            }
        }

        self.set_nodes(node_array)
    }

    pub fn is_connected(&self) -> bool {
        let nodes = self.nodes();
        let closed = self.closed.load(Ordering::Relaxed);
        return nodes.len() > 0 && !closed;
    }

    pub fn aliases(&self) -> HashMap<Host, Arc<Node>> {
        let aliases = self.aliases.read().unwrap();
        aliases.to_owned()
    }

    pub fn nodes(&self) -> Vec<Arc<Node>> {
        let nodes = self.nodes.read().unwrap();
        nodes.to_vec()
    }

    pub fn get_node(&self, partition: &Partition) -> AerospikeResult<Arc<Node>> {
        let partitions = self.partitions();
        let partitions = partitions.read();

        if let Ok(partitions1) = partitions {
            if let Some(node_array) = partitions1.get(partition.namespace) {
                if let Some(node) = node_array.get(partition.partition_id) {
                    return Ok(node.clone());
                }
            }
        }

        self.get_random_node()
    }

    pub fn get_random_node(&self) -> AerospikeResult<Arc<Node>> {
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

        Err(AerospikeError::new(ResultCode::INVALID_NODE_ERROR, None))
    }

    fn get_node_by_name(&self, node_name: &str) -> AerospikeResult<Arc<Node>> {
        let node_array = self.nodes();

        for node in node_array.iter() {
            if node.name() == node_name {
                return Ok(node.clone());
            }
        }

        Err(AerospikeError::new(ResultCode::INVALID_NODE_ERROR,
                                Some(format!("Requested node `{}` not found.", node_name))))
    }

    pub fn close(&self) -> AerospikeResult<()> {
        if !self.closed.load(Ordering::Relaxed) {
            // close tend by closing the channel
            let tx = self.tend_channel.lock().unwrap();
            drop(tx);
        }

        Ok(())
    }
}
