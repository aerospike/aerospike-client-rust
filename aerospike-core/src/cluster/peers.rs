// Copyright 2015-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::cluster::Node;
use crate::net::Host;

/// Represents a peer node discovered from the server's peers list.
#[derive(Debug, Clone)]
pub struct Peer {
    pub node_name: String,
    pub tls_name: String,
    pub hosts: Vec<Host>,
    /// If set, this peer should replace the given existing node
    /// (e.g. when a node's IP address has changed).
    pub replace_node: Option<Arc<Node>>,
}

/// Tracks peer state during a single cluster tend cycle.
///
/// This struct is created fresh for each tend iteration and accumulates
/// information about discovered peers, new nodes, and nodes to remove.
#[derive(Debug)]
pub struct Peers {
    /// Discovered peers keyed by node name.
    peers: HashMap<String, Peer>,
    /// New nodes to add to the cluster, keyed by node name.
    nodes: HashMap<String, Arc<Node>>,
    /// Nodes marked for removal, keyed by node string representation.
    nodes_to_remove: HashMap<String, Arc<Node>>,
    /// Number of nodes that successfully refreshed peers.
    refresh_count: AtomicUsize,
    /// Whether any node's peers generation changed during this tend cycle.
    gen_changed: AtomicBool,
}

impl Peers {
    /// Creates a new Peers instance for a tend cycle.
    /// `gen_changed` starts as `true` to force initial peer discovery.
    pub fn new(peer_capacity: usize, add_capacity: usize) -> Self {
        Peers {
            peers: HashMap::with_capacity(peer_capacity),
            nodes: HashMap::with_capacity(add_capacity),
            nodes_to_remove: HashMap::with_capacity(add_capacity),
            refresh_count: AtomicUsize::new(0),
            gen_changed: AtomicBool::new(true),
        }
    }

    /// Adds a newly created node.
    pub fn add_node(&mut self, name: String, node: Arc<Node>) {
        self.nodes.insert(name, node);
    }

    /// Looks up a new node by name.
    pub fn node_by_name(&self, name: &str) -> Option<&Arc<Node>> {
        self.nodes.get(name)
    }

    /// Returns a clone of the new nodes map.
    pub fn nodes(&self) -> HashMap<String, Arc<Node>> {
        self.nodes.clone()
    }

    /// Appends parsed peers into the peers map.
    pub fn append_peers(&mut self, new_peers: Vec<Peer>) {
        for peer in new_peers {
            self.peers.insert(peer.node_name.clone(), peer);
        }
    }

    /// Returns a snapshot of all discovered peers.
    pub fn peers_list(&self) -> Vec<Peer> {
        self.peers.values().cloned().collect()
    }

    /// Returns the number of discovered peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Marks a node for removal.
    pub fn add_node_to_remove(&mut self, node: Arc<Node>) {
        self.nodes_to_remove
            .insert(node.to_string(), node);
    }

    /// Checks if a node is already marked for removal.
    pub fn contains_node_to_remove(&self, node: &Node) -> bool {
        self.nodes_to_remove.contains_key(&node.to_string())
    }

    /// Returns all nodes marked for removal.
    pub fn get_nodes_to_remove(&self) -> Vec<Arc<Node>> {
        self.nodes_to_remove.values().cloned().collect()
    }

    /// Returns the current refresh count.
    pub fn refresh_count(&self) -> usize {
        self.refresh_count.load(Ordering::Relaxed)
    }

    /// Increments the refresh count by 1.
    pub fn increment_refresh_count(&self) {
        self.refresh_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Resets the refresh count to 0.
    pub fn reset_refresh_count(&self) {
        self.refresh_count.store(0, Ordering::Relaxed);
    }

    /// Returns whether peers generation changed.
    pub fn gen_changed(&self) -> bool {
        self.gen_changed.load(Ordering::Relaxed)
    }

    /// Sets gen_changed to true if `changed` is true (OR semantics).
    pub fn set_gen_changed(&self, changed: bool) {
        if changed {
            self.gen_changed.store(true, Ordering::Relaxed);
        }
    }
}
