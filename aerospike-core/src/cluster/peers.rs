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
    /// Name of the node that *parsed* this peer. Mirrors Java's
    /// peer-source bookkeeping so we can advance only that node's
    /// `peers_generation` after every peer of its set has materialized.
    pub from_node_name: Option<String>,
}

/// Tracks peer state during a single cluster tend cycle.
///
/// This struct is created fresh for each tend iteration and accumulates
/// information about discovered peers, new nodes, and nodes to remove.
#[derive(Debug)]
pub struct Peers {
    /// Discovered peers in parse order. Mirrors Java's `ArrayList<Peer>`:
    /// duplicates from the same parsing node are preserved (let the
    /// materialization step decide), and order is meaningful for debug
    /// logs. Each parsing node clears this before populating.
    peers: Vec<Peer>,
    /// New nodes to add to the cluster, keyed by node name.
    nodes: HashMap<String, Arc<Node>>,
    /// Nodes marked for removal, keyed by node string representation.
    nodes_to_remove: HashMap<String, Arc<Node>>,
    /// Per-tend list of hosts that already failed to validate. Mirrors
    /// Java's `Peers.invalidHosts`. Used to short-circuit retries against
    /// hosts whose connection just failed within this same tend.
    invalid_hosts: HashMap<Host, ()>,
    /// Per-source-node `peers-generation` values waiting to be committed
    /// once every peer parsed by that node has successfully materialized.
    /// Mirrors Java's `peersValidated` rule: advance the node's generation
    /// only when its full peer set is reachable; otherwise let the next
    /// tend re-parse and retry. Keyed by parsing-node name.
    pending_generations: HashMap<String, isize>,
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
            peers: Vec::with_capacity(peer_capacity),
            nodes: HashMap::with_capacity(add_capacity),
            nodes_to_remove: HashMap::with_capacity(add_capacity),
            invalid_hosts: HashMap::with_capacity(8),
            pending_generations: HashMap::with_capacity(add_capacity),
            refresh_count: AtomicUsize::new(0),
            gen_changed: AtomicBool::new(true),
        }
    }

    /// Mark a host as already-failed for the rest of this tend cycle. Mirrors
    /// Java's `Peers.fail(Host)`.
    pub fn fail(&mut self, host: Host) {
        self.invalid_hosts.insert(host, ());
    }

    /// `true` when [`fail`](Self::fail) was called on this host earlier in
    /// the same tend cycle.
    pub fn has_failed(&self, host: &Host) -> bool {
        self.invalid_hosts.contains_key(host)
    }

    /// Number of hosts that failed to validate during this tend.
    pub fn invalid_count(&self) -> usize {
        self.invalid_hosts.len()
    }

    /// Snapshot of the failed-host set, for diagnostic messages.
    pub fn invalid_hosts(&self) -> Vec<Host> {
        self.invalid_hosts.keys().cloned().collect()
    }

    /// Stage a `peers-generation` reported by `parsing_node` so it can be
    /// committed later, once `materialize_peers` confirms every peer that
    /// node parsed has been added to the cluster (Java's `peersValidated`).
    pub fn set_pending_generation(&mut self, parsing_node: String, generation: isize) {
        self.pending_generations.insert(parsing_node, generation);
    }

    /// Drop one pending generation. Called by `materialize_peers` when one
    /// of the parsing node's peers couldn't be reached on any host.
    pub fn invalidate_pending_generation(&mut self, parsing_node: &str) {
        self.pending_generations.remove(parsing_node);
    }

    /// Return and clear all pending generations that survived materialization.
    pub fn take_pending_generations(&mut self) -> HashMap<String, isize> {
        std::mem::take(&mut self.pending_generations)
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

    /// Removes and returns all newly created nodes. Used by the tend loop
    /// to drain `peers.nodes` between iterations of the peers-of-peers
    /// refresh, mirroring Java's `Cluster.refreshPeers` clear-then-iterate.
    pub fn drain_nodes(&mut self) -> Vec<Arc<Node>> {
        std::mem::take(&mut self.nodes).into_values().collect()
    }

    /// Appends parsed peers into the peers list, preserving order and
    /// duplicates exactly as Java's `ArrayList<Peer>` would.
    pub fn append_peers(&mut self, new_peers: Vec<Peer>) {
        self.peers.extend(new_peers);
    }

    /// Drop the current peer list. Mirrors Java's `peers.clear()` at the
    /// start of `PeerParser`: each parsing node replaces the working set so
    /// it can be materialized in isolation.
    pub fn clear_peers(&mut self) {
        self.peers.clear();
    }

    /// Returns a snapshot of all discovered peers.
    pub fn peers_list(&self) -> Vec<Peer> {
        self.peers.clone()
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
