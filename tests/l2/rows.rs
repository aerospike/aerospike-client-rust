// Copyright 2015-2026 Aerospike, Inc.
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

//! Testing strategy section 4.3 rows N-1 to N-14 against the topology
//! (Java `TestL2Topology`). N-3 is retired, N-10 is in `reachable` and N-13
//! is not implemented. Every case seeds the first listener and builds its
//! own policy.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use aerospike::discovery::{
    AddressTranslator, Endpoint, PortMapAddressTranslator, SeedCandidateContext,
    SeedCandidateProvider, TranslationMissMode,
};
use aerospike::{
    Bin, Bins, Client, ClientPolicy, ErrorKind, Host, Key, ReadPolicy, Replica, Value, WritePolicy,
};

use super::topology::{self as topo, L2Node, Restore};
use super::{capture, run, skip_unless};

pub const SET: &str = "l2";
const KEY_COUNT: usize = 300;
pub const MISS_RETRIES: usize = 3;
const DNS_BLOCKER: &str = "N-4 needs a DNS server on the L2 client network serving SRV records for \
                           the endpoint; the L2 topology has none yet";
const NODE_CONTROL_BLOCKER: &str = "N-11 needs DISCOVERY_CONTROL_DIR so the host can start a fourth \
                                    node (run_l2.sh L2_NODE_CONTROL=1, or the discovery-l2 workflow)";
const REFRESH_INTERVAL: u32 = 600_000;
const MISS_KEY_COUNT: usize = 200;

/// A translator that maps nothing.
pub struct EmptyTranslator;

impl AddressTranslator for EmptyTranslator {
    fn translate(&self, _advertised: &Endpoint) -> Option<Host> {
        None
    }
}

/// A port-map the row can replace while the client runs.
pub struct SwappablePortMap(pub RwLock<PortMapAddressTranslator>);

impl AddressTranslator for SwappablePortMap {
    fn translate(&self, advertised: &Endpoint) -> Option<Host> {
        self.0.read().unwrap().translate(advertised)
    }

    fn name(&self) -> String {
        "port-map".to_string()
    }
}

/// Seed provider that returns fixed seeds and records the time of every call.
pub struct CountingSeedProvider {
    pub calls: Mutex<Vec<Instant>>,
    seeds: Vec<Host>,
}

impl CountingSeedProvider {
    pub fn new(seeds: Vec<Host>) -> Arc<Self> {
        Arc::new(CountingSeedProvider {
            calls: Mutex::new(Vec::new()),
            seeds,
        })
    }

    pub fn calls(&self) -> usize {
        self.calls.lock().unwrap().len()
    }

    pub fn calls_between(&self, begin: Instant, end: Instant) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|t| **t >= begin && **t <= end)
            .count()
    }
}

#[async_trait::async_trait]
impl SeedCandidateProvider for CountingSeedProvider {
    async fn refresh_seed_candidates(&self, _ctx: &SeedCandidateContext) -> aerospike::Result<Vec<Host>> {
        self.calls.lock().unwrap().push(Instant::now());
        Ok(self.seeds.clone())
    }

    fn needs_periodic_refresh(&self) -> bool {
        true
    }

    fn supports_deadline(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

pub fn key(set: &str, name: String) -> Key {
    Key::new(topo::namespace(), set, Value::from(name)).expect("key")
}

pub fn port_map(map: Vec<(String, String)>) -> Arc<PortMapAddressTranslator> {
    Arc::new(PortMapAddressTranslator::new(map).expect("valid port map"))
}

pub async fn connect(policy: &ClientPolicy, seeds: &[Host]) -> Client {
    let client = Client::new(policy, &seeds.to_vec())
        .await
        .unwrap_or_else(|e| panic!("client did not connect: {}", topo::messages(&e)));
    topo::register_client(&client);
    client
}

/// The warnings that a translation of `victim` missed.
pub fn misses(victim: &str) -> Vec<String> {
    capture::messages_containing(&[&format!("has no mapping for node {victim}")])
}

/// The warnings that a certificate matched the tls-name only by a legacy CN
/// rule. The Rust client has no such rule, so this is always empty.
pub fn cn_fallback_warnings() -> Vec<String> {
    capture::messages_containing(&["legacy"])
}

pub async fn writes(client: &Client, policy: &WritePolicy, key: &Key) -> bool {
    client.put(policy, key, &[Bin::new("v", Value::from(0_i64))]).await.is_ok()
}

/// Writes `i` and reads it back.
pub async fn put_get(client: &Client, wp: &WritePolicy, rp: &ReadPolicy, key: &Key, i: i64) {
    client
        .put(wp, key, &[Bin::new("v", Value::from(i))])
        .await
        .unwrap_or_else(|e| panic!("put {key}: {}", topo::messages(&e)));
    let record = client
        .get(rp, key, Bins::All)
        .await
        .unwrap_or_else(|e| panic!("get {key}: {}", topo::messages(&e)));
    assert_eq!(Some(&Value::from(i)), record.bins.get("v"), "{key}");
}

pub fn node_of(nodes: &[L2Node], name: &str) -> L2Node {
    nodes
        .iter()
        .find(|n| n.name == name)
        .cloned()
        .unwrap_or_else(|| panic!("Unknown node {name}"))
}

async fn assert_port_map_joins(row: &str, map: Vec<(String, String)>) {
    let mut policy = topo::policy();
    println!("L2-PORT-MAP {row} {map:?}");
    policy.address_translator = Some(port_map(map));
    let client = connect(&policy, &[topo::seed().await]).await;
    let names = topo::node_names().await;
    topo::wait_nodes(&client, &names).await;
    println!("L2-JOIN {row} nodes={:?}", topo::client_node_names(&client));
    let owners = topo::owners(&topo::assert_all_partitions_owned(row, &client).await);
    assert_eq!(names, owners.keys().cloned().collect::<Vec<_>>());
    client.close().await.expect("close");
}

/// The seed serves the keys it holds and the keys nobody holds are refused.
pub async fn assert_seed_keys_work(row: &str, client: &Client, seed: &L2Node) {
    let wp = WritePolicy::default();
    let rp = ReadPolicy::default();
    let mut on_seed = 0;
    let mut not_owned = 0;
    for i in 0..MISS_KEY_COUNT {
        let key = key(SET, format!("miss-{i}"));
        let node = match client.write_node(&key, wp.base_policy.replica) {
            Ok(node) => node,
            Err(e) if matches!(e.kind(), ErrorKind::InvalidNode) => {
                not_owned += 1;
                continue;
            }
            Err(e) => panic!("{key}: {e}"),
        };
        assert_eq!(seed.name, node.name());
        put_get(client, &wp, &rp, &key, i as i64).await;
        on_seed += 1;
    }
    println!(
        "L2-MISS-KEYS {row} seed={} keysOnSeed={on_seed} keysNotOwned={not_owned}",
        seed.name
    );
    assert!(on_seed > 0);
    assert!(not_owned > 0);
}

/// Every partition with a master is owned by one of `mapped_names`, and
/// every key is served, through the owner or a replica.
async fn assert_owned_keys_work(client: &Client, mapped_names: &[String], victim: &L2Node) {
    let masters = topo::masters(client);
    let wp = WritePolicy::default();
    let rp = ReadPolicy::default();
    let mut no_master = 0;
    let mut owned = 0;
    let mut missed_keys = 0;
    for node in &masters {
        match node {
            None => no_master += 1,
            Some(node) => assert!(mapped_names.contains(&node.name().to_string()), "{}", node.name()),
        }
    }
    for i in 0..KEY_COUNT {
        let key = key(SET, format!("n8-{i}"));
        if masters[key.partition_id()].is_none() {
            missed_keys += 1;
        } else {
            owned += 1;
        }
        put_get(client, &wp, &rp, &key, i as i64).await;
    }
    println!(
        "L2-N8-KEYS victim={} partitionsWithoutMaster={no_master} keysOnMappedMasters={owned} \
         keysOnMissedPartitions={missed_keys}",
        victim.name
    );
    assert!(no_master > 0);
    assert!(owned > 0);
    assert!(missed_keys > 0);
}

async fn assert_strict_miss_drops_peers(row: &str, policy: ClientPolicy, translator: &str) {
    let nodes = topo::nodes().await;
    let seed = nodes[0].clone();
    let peers = &nodes[1..];
    capture::clear();

    let client = connect(&policy, &[topo::seed().await]).await;
    topo::wait_for(|| {
        assert_eq!(vec![seed.name.clone()], topo::client_node_names(&client));
        peers.iter().all(|peer| misses(&peer.name).len() >= MISS_RETRIES)
    })
    .await;

    for peer in peers {
        let misses = misses(&peer.name);
        println!(
            "L2-MISS-RESULT {row} peer={} nodes={:?} missWarnings={}",
            peer.name,
            topo::client_node_names(&client),
            misses.len()
        );
        assert!(
            misses.len() >= MISS_RETRIES,
            "Missed peer {} not retried every tend: {:?}",
            peer.name,
            capture::warnings()
        );
        let message = &misses[0];
        println!("L2-MISS-LOG {row} {message}");
        for part in [peer.name.as_str(), peer.advertised[0].as_str(), "standard", translator, "pass_through"] {
            assert!(message.contains(part), "{part} not in: {message}");
        }
    }
    assert_eq!(vec![seed.name.clone()], topo::client_node_names(&client));
    assert_seed_keys_work(row, &client, &seed).await;
    client.close().await.expect("close");
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// N-1: baseline, no translator. The client reaches the seed through the
/// endpoint and then fails on the peers, whose advertised addresses it has
/// no route to.
#[test]
fn n1_baseline_without_translator_fails_on_peers() {
    run("N-1", || async {
        let t = topo::t();
        let seed = Host::new(&t.host, t.ports[0]);
        let info = topo::try_info(&seed.name, seed.port, &["node", "peers-clear-std"])
            .await
            .unwrap_or_else(|e| panic!("N-1 guard: seed {seed} is not reachable through the endpoint: {e}"));
        let node = info.get("node").cloned().unwrap_or_default();
        let reply = info.get("peers-clear-std").cloned().unwrap_or_default();
        let peers = topo::parse_peers(Some(&reply));
        println!("L2-N1-GUARD seed={seed} node={node} peers-clear-std={reply}");
        assert!(!node.is_empty(), "N-1 guard: seed {seed} returned no node name");
        assert!(!peers.is_empty(), "N-1 guard: seed {node} lists no peers: {reply}");
        for peer in &peers {
            for endpoint in &peer.endpoints {
                topo::assert_unreachable(endpoint).await;
            }
        }

        match Client::new(&topo::policy(), &vec![seed.clone()]).await {
            Ok(client) => {
                let names = topo::client_node_names(&client);
                client.close().await.expect("close");
                panic!("N-1: client joined {names:?} without a translator");
            }
            Err(e) => {
                let message = topo::messages(&e);
                println!("L2-N1-ERROR {message}");
                assert!(message.contains("Peers not reachable: "), "{message}");
                for peer in &peers {
                    for endpoint in &peer.endpoints {
                        let unreachable = format!("{} {}", topo::host_of(endpoint), topo::port_of(endpoint));
                        assert!(message.contains(&unreachable), "{unreachable} not named in: {message}");
                    }
                }
            }
        }
    });
}

/// N-2: port-map keyed by advertised `(host, port)`. All nodes join and the
/// partition map is complete.
#[test]
fn n2_port_map_by_host_port_joins_all_nodes() {
    run("N-2", || async {
        let nodes = topo::nodes().await;
        assert_port_map_joins("N-2-host-port", topo::port_map_by_endpoint(&nodes)).await;
    });
}

/// N-2: port-map keyed by node name. All nodes join and the partition map is
/// complete.
#[test]
fn n2_port_map_by_node_name_joins_all_nodes() {
    run("N-2", || async {
        let nodes = topo::nodes().await;
        assert_port_map_joins("N-2-node-name", topo::port_map_by_node_name(&nodes)).await;
    });
}

/// N-4: dns provider end to end. The seed comes from SRV records for the
/// endpoint, one per listener, and the peers through port-map. The topology
/// has no DNS server yet, so the row skips.
#[test]
fn n4_dns_provider_seeds_through_endpoint() {
    run("N-4", || async {
        skip_unless(false, DNS_BLOCKER);
    });
}

/// N-5: no seed-only cluster. Every node owns partitions as its own node,
/// and writes and reads for keys spread across partitions are served by the
/// owner without a server proxy hop.
#[test]
fn n5_every_partition_owner_is_a_distinct_node() {
    run("N-5", || async {
        let nodes = topo::nodes().await;
        let mut policy = topo::policy();
        policy.address_translator = Some(port_map(topo::port_map_by_endpoint(&nodes)));
        let client = connect(&policy, &[topo::seed().await]).await;
        topo::wait_nodes(&client, &topo::node_names().await).await;
        let masters = topo::assert_all_partitions_owned("N-5", &client).await;
        let distinct = topo::distinct_masters(&masters);
        let owners = topo::owners(&masters);
        println!("L2-N5-OWNERS nodes={} partitionsPerNode={owners:?}", distinct.len());
        assert_eq!(nodes.len(), distinct.len());
        assert_eq!(topo::node_names().await, owners.keys().cloned().collect::<Vec<_>>());

        for node in client.nodes() {
            let by_name = client.get_node(node.name()).expect("node by name");
            assert!(Arc::ptr_eq(&node, &by_name), "{}", node.name());
            assert!(distinct.iter().any(|d| Arc::ptr_eq(d, &node)), "{}", node.name());
        }

        let stats = [
            "client_write_success",
            "client_read_success",
            "client_proxy_complete",
            "client_proxy_error",
            "client_proxy_timeout",
        ];
        let mut before = BTreeMap::new();
        for node in &nodes {
            before.insert(node.name.clone(), topo::namespace_stats(node, &stats).await);
        }

        let mut wp = WritePolicy::default();
        wp.base_policy.replica = Replica::Master;
        let mut rp = ReadPolicy::default();
        rp.base_policy.replica = Replica::Master;
        let mut served: BTreeMap<String, i64> = BTreeMap::new();
        let mut partitions = HashSet::new();
        for i in 0..KEY_COUNT {
            let key = key(SET, format!("n5-{i}"));
            let owner = masters[key.partition_id()].clone().expect("owned");
            let write_node = client.write_node(&key, Replica::Master).expect("write node");
            let read_node = client.read_node(&key, Replica::Master).expect("read node");
            assert!(Arc::ptr_eq(&owner, &write_node), "{key}: write routed to {}", write_node.name());
            assert!(Arc::ptr_eq(&owner, &read_node), "{key}: read routed to {}", read_node.name());
            put_get(&client, &wp, &rp, &key, i as i64).await;
            *served.entry(owner.name().to_string()).or_insert(0) += 1;
            partitions.insert(key.partition_id());
        }

        for node in &nodes {
            let after = topo::namespace_stats(node, &stats).await;
            let start = &before[&node.name];
            let delta = |name: &str| after[name] - start[name];
            let writes = delta("client_write_success");
            let reads = delta("client_read_success");
            let proxied =
                delta("client_proxy_complete") + delta("client_proxy_error") + delta("client_proxy_timeout");
            let expected = served.get(&node.name).copied().unwrap_or(0);
            println!(
                "L2-N5-SERVED node={} partitions={} keys={expected} writes={writes} reads={reads} proxied={proxied}",
                node.name,
                owners.get(&node.name).copied().unwrap_or(0)
            );
            assert!(expected > 0, "{} served no keys", node.name);
            assert_eq!(expected, writes, "{} writes", node.name);
            assert_eq!(expected, reads, "{} reads", node.name);
            assert_eq!(0, proxied, "{} proxied", node.name);
        }
        println!("L2-N5-KEYS keys={KEY_COUNT} partitions={}", partitions.len());
        client.close().await.expect("close");
    });
}

/// N-6: TLS through the proxy, `ee` only. The handshake validates the
/// certificate against the node's tls-name, not the endpoint hostname the
/// client connects to. The server certificate carries the tls-name only as a
/// dNSName SAN; the Rust client has no CN fallback to warn about.
#[cfg(feature = "tls")]
#[test]
fn n6_tls_validates_against_node_tls_name() {
    run("N-6", || async {
        let t = topo::t();
        if !skip_unless(
            t.subset == "ee",
            &format!("N-6 needs DISCOVERY_SUBSET=ee for server TLS and TLS listeners; subset is {}", t.subset),
        ) {
            return;
        }
        let nodes = topo::nodes().await;
        for node in &nodes {
            assert!(node.tls_port > 0, "No TLS listener for {node}");
            assert!(!node.tls_advertised.is_empty(), "No advertised TLS address for {node}");
            assert!(node.tls_name.is_some(), "No tls-name for {node}");
        }
        let first = &nodes[0];
        let tls_name = first.tls_name.clone().expect("tls-name");
        assert_ne!(tls_name, t.host, "tls-name {tls_name} is the endpoint hostname");

        let mut policy = topo::policy();
        policy.tls_policy = Some(super::cn_only::tls_policy());
        policy.address_translator = Some(port_map(topo::tls_port_map(&nodes)));
        capture::clear();

        let client = connect(&policy, &[Host::new_tls(&t.host, &tls_name, first.tls_port)]).await;
        topo::wait_nodes(&client, &topo::node_names().await).await;
        topo::assert_all_partitions_owned("N-6", &client).await;
        let endpoint_addresses = topo::resolve(&t.host);
        for node in client.nodes() {
            let h = node.host();
            let expected = node_of(&nodes, node.name());
            println!(
                "L2-N6-NODE {} host={}:{} tlsName={:?} endpoint={}{endpoint_addresses:?}",
                node.name(),
                h.name,
                h.port,
                h.tls_name,
                t.host
            );
            assert!(
                h.name == t.host || endpoint_addresses.contains(&h.name),
                "{} is not {}{endpoint_addresses:?}",
                h.name,
                t.host
            );
            assert_eq!(expected.tls_port, h.port);
            assert_eq!(expected.tls_name, h.tls_name);
        }
        let wp = WritePolicy::default();
        let rp = ReadPolicy::default();
        for i in 0..20 {
            put_get(&client, &wp, &rp, &key(SET, format!("n6-{i}")), i).await;
        }
        client.close().await.expect("close");

        match Client::new(&policy, &vec![Host::new_tls(&t.host, &t.host, first.tls_port)]).await {
            Ok(client) => {
                client.close().await.expect("close");
                panic!("N-6: TLS seed validated against endpoint hostname {}", t.host);
            }
            Err(e) => {
                let message = topo::messages(&e);
                println!("L2-N6-ERROR {message}");
                assert!(
                    message.contains("NotValidForName") || message.to_lowercase().contains("certificate"),
                    "{message}"
                );
            }
        }
        println!(
            "L2-N6-CN-FALLBACK certShape={} warnings={:?}",
            t.tls_cert_shape,
            cn_fallback_warnings()
        );
        assert_eq!(Vec::<String>::new(), cn_fallback_warnings());
    });
}

/// N-7: node removed from the endpoint. With one node's listener disabled
/// the client serves the other nodes' partitions; once it is enabled again
/// the node returns and no node is stuck.
#[test]
fn n7_node_removed_from_endpoint_degrades_and_returns() {
    run("N-7", || async {
        let nodes = topo::nodes().await;
        let victim = nodes[nodes.len() - 1].clone();
        let mut policy = topo::policy();
        policy.address_translator = Some(port_map(topo::port_map_by_endpoint(&nodes)));

        let client = connect(&policy, &[topo::seed().await]).await;
        let names = topo::node_names().await;
        topo::wait_nodes(&client, &names).await;
        let masters = topo::assert_all_partitions_owned("N-7-before", &client).await;
        let mut wp = WritePolicy::default();
        wp.base_policy.max_retries = 0;
        wp.base_policy.total_timeout = 2000;
        let rp = ReadPolicy::default();
        let mut victim_keys = Vec::new();
        let mut other_keys = Vec::new();
        for i in 0..KEY_COUNT {
            let key = key(SET, format!("n7-{i}"));
            let owner = masters[key.partition_id()].clone().expect("owned");
            if owner.name() == victim.name {
                victim_keys.push(key);
            } else {
                other_keys.push(key);
            }
        }
        assert!(!victim_keys.is_empty());
        assert!(!other_keys.is_empty());

        topo::restore_later(Restore::EnableListener(victim.proxy.clone()));
        topo::set_listener(&victim.proxy, false).await;

        let deadline = Instant::now() + std::time::Duration::from_millis(topo::WAIT_MS);
        while writes(&client, &wp, &victim_keys[0]).await {
            assert!(
                Instant::now() < deadline,
                "Writes to {} still succeed with its listener disabled",
                victim.name
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let mut victim_failed = 0;
        for key in &victim_keys {
            if !writes(&client, &wp, key).await {
                victim_failed += 1;
            }
        }
        for key in &other_keys {
            put_get(&client, &wp, &rp, key, 1).await;
        }

        let mut active: Vec<String> = client
            .nodes()
            .iter()
            .filter(|n| n.is_active() && n.name() != victim.name)
            .map(|n| n.name().to_string())
            .collect();
        active.sort();
        let others: Vec<String> = names.iter().filter(|n| **n != victim.name).cloned().collect();
        println!(
            "L2-N7-DEGRADED victim={} nodes={:?} activeOthers={active:?} otherKeysServed={} victimKeysFailed={victim_failed}/{}",
            victim.name,
            topo::client_node_names(&client),
            other_keys.len(),
            victim_keys.len()
        );
        assert_eq!(others, active);
        assert_eq!(victim_keys.len(), victim_failed);

        topo::set_listener(&victim.proxy, true).await;
        topo::restored(|r| matches!(r, Restore::EnableListener(_)));

        let deadline = Instant::now() + std::time::Duration::from_millis(topo::WAIT_MS);
        while !writes(&client, &wp, &victim_keys[0]).await {
            assert!(Instant::now() < deadline, "Writes to {} did not recover", victim.name);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        topo::wait_nodes(&client, &names).await;
        topo::assert_all_partitions_owned("N-7-after", &client).await;
        for node in client.nodes() {
            assert!(node.is_active(), "{} is not active", node.name());
        }
        for key in victim_keys.iter().chain(other_keys.iter()) {
            put_get(&client, &wp, &rp, key, 2).await;
        }
        println!(
            "L2-N7-RECOVERED nodes={:?} keys={KEY_COUNT}",
            topo::client_node_names(&client)
        );
        client.close().await.expect("close");
    });
}

/// N-8: on_miss strict. A peer with no mapping produces the error naming the
/// node, the advertised endpoint and the translator, and creates no node:
/// its partitions have no master in the client's map. The other nodes keep
/// serving every key and the missed peer is retried every tend.
#[test]
fn n8_strict_miss_creates_no_node() {
    run("N-8", || async {
        let nodes = topo::nodes().await;
        let victim = nodes[nodes.len() - 1].clone();
        let mapped: Vec<L2Node> = nodes.iter().filter(|n| n.name != victim.name).cloned().collect();
        let mut mapped_names: Vec<String> = mapped.iter().map(|n| n.name.clone()).collect();
        mapped_names.sort();
        capture::clear();

        let mut policy = topo::policy();
        policy.address_translator = Some(port_map(topo::port_map_by_endpoint(&mapped)));
        policy.translation_on_miss = TranslationMissMode::Strict;

        let client = connect(&policy, &[topo::seed().await]).await;
        topo::wait_nodes(&client, &mapped_names).await;
        topo::wait_for(|| {
            assert_eq!(mapped_names, topo::client_node_names(&client));
            misses(&victim.name).len() >= MISS_RETRIES
        })
        .await;

        let misses = misses(&victim.name);
        println!(
            "L2-N8-RESULT victim={} nodes={:?} missWarnings={}",
            victim.name,
            topo::client_node_names(&client),
            misses.len()
        );
        assert!(
            misses.len() >= MISS_RETRIES,
            "Missed peer {} not retried every tend: {:?}",
            victim.name,
            capture::warnings()
        );
        assert_eq!(mapped_names, topo::client_node_names(&client));

        let message = &misses[0];
        println!("L2-N8-LOG {message}");
        assert!(
            victim.advertised.iter().any(|a| message.contains(a)),
            "{message} names none of {:?}",
            victim.advertised
        );
        for part in [victim.name.as_str(), "port-map", "pass_through", "standard"] {
            assert!(message.contains(part), "{part} not in: {message}");
        }
        assert_owned_keys_work(&client, &mapped_names, &victim).await;
        client.close().await.expect("close");
    });
}

/// N-11: a node started mid-test is an active node in the client within
/// 2 x tend_interval of the peers-generation bump that announces it, and
/// eventually owns partitions. The host starts the node through
/// DISCOVERY_CONTROL_DIR and the test gives it a listener and a port-map
/// entry before it joins.
#[test]
fn n11_new_node_active_within_two_tend_intervals() {
    run("N-11", || async {
        if !skip_unless(!topo::t().control_dir.is_empty(), NODE_CONTROL_BLOCKER) {
            return;
        }
        let nodes = topo::nodes().await;
        let first = nodes[0].clone();
        let names = topo::node_names().await;
        let swappable = Arc::new(SwappablePortMap(RwLock::new(
            PortMapAddressTranslator::new(topo::port_map_by_endpoint(&nodes)).expect("port map"),
        )));
        let mut policy = topo::policy();
        policy.address_translator = Some(swappable.clone());

        let client = connect(&policy, &[topo::seed().await]).await;
        topo::wait_nodes(&client, &names).await;
        topo::assert_all_partitions_owned("N-11-before", &client).await;

        let generation = topo::peers_generation(&first).await;
        let ack = topo::control("start").await;
        let added_node = ack["container"].clone();
        topo::restore_later(Restore::StopNode(added_node.clone()));
        let upstream_port = topo::upstream_port(&first.proxy).await;
        let port = topo::add_listener(&added_node, &format!("{added_node}:{upstream_port}")).await;
        topo::restore_later(Restore::RemoveListener(added_node.clone()));

        let advertised = format!("{}:{upstream_port}", ack["address"]);
        let mut map = topo::port_map_by_endpoint(&nodes);
        map.push((advertised.clone(), topo::listener(port)));
        *swappable.0.write().unwrap() = PortMapAddressTranslator::new(map).expect("port map");
        println!("L2-N11-MAPPED {advertised}={}", topo::listener(port));

        assert_eq!(
            generation,
            topo::peers_generation(&first).await,
            "peers-generation bumped before {added_node} had a listener and a mapping"
        );

        let joined = topo::wait_joined(&client, &first, generation, &names).await;
        let node = joined.node;
        let budget = 2 * u64::from(policy.tend_interval);
        let info = topo::info(&topo::t().host, port, &["node", "service-clear-std"]).await;
        println!(
            "L2-N11-TIMING node={} host={} container={added_node} generation={generation}->{} bumpToActiveMs={} tendInterval={} budgetMs={budget}",
            node.name(),
            node.host(),
            joined.generation,
            joined.bump_to_active_ms,
            policy.tend_interval
        );
        assert_eq!(info["node"], node.name());
        assert!(
            info["service-clear-std"].contains(&advertised),
            "{advertised} not in {info:?}"
        );
        assert_eq!(port, node.host().port);
        assert!(
            joined.bump_to_active_ms <= budget,
            "{} active {}ms after the bump, budget {budget}ms",
            node.name(),
            joined.bump_to_active_ms
        );

        let owns = topo::wait_for(|| topo::owners_of(&client).contains_key(node.name())).await;
        println!("L2-N11-OWNERS {:?}", topo::owners_of(&client));
        assert!(owns, "{} owns no partitions", node.name());
        client.close().await.expect("close");
    });
}

/// N-12: re-seed on zero active nodes. With every listener disabled the
/// client drops to zero nodes; once they are enabled again it re-seeds from
/// the seeds it holds within 2 x tend_interval plus the connect timeout,
/// without a seed provider refresh, and the partition map is complete again.
#[test]
fn n12_reseed_on_zero_active_nodes() {
    run("N-12", || async {
        let nodes = topo::nodes().await;
        let provider = CountingSeedProvider::new(vec![topo::seed().await]);
        let mut policy = topo::policy();
        policy.seed_candidate_provider = Some(provider.clone());
        policy.discovery_refresh_interval = REFRESH_INTERVAL;
        policy.address_translator = Some(port_map(topo::port_map_by_endpoint(&nodes)));

        let client = connect(&policy, &[]).await;
        let names = topo::node_names().await;
        topo::wait_nodes(&client, &names).await;
        topo::assert_all_partitions_owned("N-12-before", &client).await;

        let proxies: Vec<String> = topo::t().proxy_ports.iter().map(|(p, _)| p.clone()).collect();
        for proxy in &proxies {
            topo::restore_later(Restore::EnableListener(proxy.clone()));
            topo::set_listener(proxy, false).await;
        }
        assert!(
            topo::wait_for(|| client.nodes().is_empty()).await,
            "Client kept nodes {:?} with every listener disabled",
            topo::client_node_names(&client)
        );

        let calls_before = provider.calls();
        let reenable = Instant::now();
        for proxy in &proxies {
            topo::set_listener(proxy, true).await;
        }
        topo::restored(|r| matches!(r, Restore::EnableListener(_)));

        assert!(
            topo::wait_for_ms(&mut || !client.nodes().is_empty(), 5).await,
            "Client did not re-seed"
        );
        let reseeded = Instant::now();
        let elapsed = reseeded.duration_since(reenable).as_millis() as u64;
        let budget = 2 * u64::from(policy.tend_interval) + u64::from(policy.timeout);
        let refreshes = provider.calls_between(reenable, reseeded);
        println!(
            "L2-N12-RESEED reenableToFirstNodeMs={elapsed} tendInterval={} connectTimeout={} budgetMs={budget} refreshesInWindow={refreshes} refreshCallsBeforeReenable={calls_before} refreshCalls={} refreshInterval={} nodes={:?}",
            policy.tend_interval,
            policy.timeout,
            provider.calls(),
            policy.discovery_refresh_interval,
            topo::client_node_names(&client)
        );
        assert_eq!(0, refreshes);
        assert_eq!(1, provider.calls());
        assert!(elapsed <= budget, "Re-seeded {elapsed}ms after re-enable, budget {budget}ms");

        topo::wait_nodes(&client, &names).await;
        topo::assert_all_partitions_owned("N-12-after", &client).await;
        client.close().await.expect("close");
    });
}

/// N-14: use_services_alternate against a cluster with no alternate access
/// address. The seed's service-clear-alt is empty and client initialization
/// fails with an error that names the flag instead of presenting an empty
/// cluster.
#[test]
fn n14_use_services_alternate_without_alternate_fails_init() {
    run("N-14", || async {
        let first = topo::nodes().await[0].clone();
        let alternate = topo::info_value(&first, "service-clear-alt").await;
        println!("L2-N14-SEED {} service-clear-alt=[{alternate}]", first.name);
        assert!(alternate.is_empty(), "{alternate}");

        let mut policy = topo::policy();
        policy.use_services_alternate = true;
        match Client::new(&policy, &vec![topo::seed().await]).await {
            Ok(client) => {
                client.close().await.expect("close");
                panic!("N-14: client constructed with use_services_alternate and no alternate address");
            }
            Err(e) => {
                println!("L2-N14-ERROR {}: {e}", e.kind().name());
                assert!(matches!(e.kind(), ErrorKind::AlternateAddressMissing), "{e}");
                let message = e.to_string();
                assert!(message.contains("use_services_alternate"), "{message}");
                assert!(message.contains(&first.name), "{message}");
            }
        }
    });
}

/// on_miss strict with an empty static-map (ip_map): every peer misses, the
/// error names the peer, its advertised endpoint, the standard list and the
/// translator, and is repeated every tend. Only the seed is a node and
/// serves the keys it holds.
#[test]
fn strict_empty_ip_map_drops_peers() {
    run("ipmap-empty-strict", || async {
        let mut policy = topo::policy();
        policy.ip_map = Some(Default::default());
        policy.translation_on_miss = TranslationMissMode::Strict;
        assert_strict_miss_drops_peers("ipmap-empty-strict", policy, "static-map").await;
    });
}

/// on_miss strict with a custom translator that maps nothing: as with the
/// empty ip_map, named by the translator.
#[test]
fn strict_empty_custom_translator_drops_peers() {
    run("custom-empty-strict", || async {
        let mut policy = topo::policy();
        policy.address_translator = Some(Arc::new(EmptyTranslator));
        policy.translation_on_miss = TranslationMissMode::Strict;
        let name = EmptyTranslator.name();
        assert_strict_miss_drops_peers("custom-empty-strict", policy, &name).await;
    });
}
