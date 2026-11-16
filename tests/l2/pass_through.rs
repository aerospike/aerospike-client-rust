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

//! Testing strategy rows N-15, N-16 and N-18 against the pass-through
//! profiles (Java `TestL2PassThrough`). In pass-through-names (Branch A)
//! each node advertises its own alternate-access-address hostname with its
//! own listener port; in pass-through-ports (Branch B) every node advertises
//! one hostname with its own listener port. Every hostname resolves to the
//! one Toxiproxy container in the client container, so every listener shares
//! one IP address and the nodes differ only by port.
//! DISCOVERY_PASS_THROUGH_BRANCH names the branch; without it every case is
//! skipped.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use aerospike::discovery::{
    AddressTranslator, Endpoint, PassThroughAddressTranslator, TranslationMissMode,
};
use aerospike::{Client, ClientPolicy, Host, ReadPolicy, Replica, WritePolicy};

use super::rows::{self, put_get};
use super::topology::{self as topo, L2Node, Restore};
use super::{capture, run, skip_unless};

const SET: &str = "l2-pass-through";
const KEY_COUNT: usize = 300;
const COLLISION_RETRIES: usize = 3;
const BRANCH_A: &str = " needs DISCOVERY_PASS_THROUGH_BRANCH=A, each node advertising its own \
                        alternate-access-address hostname (run_l2.sh L2_PROFILE=pass-through-names)";
const BRANCH_B: &str = " needs DISCOVERY_PASS_THROUGH_BRANCH=B and DISCOVERY_CONTROL_DIR, every node \
                        advertising one alternate-access-address hostname with its own port \
                        (run_l2.sh L2_PROFILE=pass-through-ports)";

/// A translator that maps every endpoint to one address.
pub struct SingleAddressTranslator(pub Host);

impl AddressTranslator for SingleAddressTranslator {
    fn translate(&self, _advertised: &Endpoint) -> Option<Host> {
        Some(self.0.clone())
    }
}

fn require_branch_b(row: &str) -> bool {
    let t = topo::t();
    skip_unless(
        t.pass_through_branch == "B" && !t.control_dir.is_empty(),
        &format!("{row}{BRANCH_B}"),
    )
}

fn pass_through_policy() -> ClientPolicy {
    let mut policy = topo::policy();
    policy.use_services_alternate = true;
    policy.address_translator = Some(Arc::new(PassThroughAddressTranslator));
    policy
}

/// The one alternate address a node advertises.
async fn alternate(node: &L2Node) -> String {
    let list = topo::alternates(node).await;
    assert_eq!(1, list.len(), "{} alternate list {list:?}", node.name);
    list[0].clone()
}

async fn seed(node: &L2Node) -> Host {
    let alternate = alternate(node).await;
    Host::new(&topo::host_of(&alternate), topo::port_of(&alternate))
}

/// Starts a node through the node server and registers its removal.
async fn start(options: &str) -> String {
    let container = topo::control(&format!("start {options}")).await["container"].clone();
    topo::restore_later(Restore::StopNode(container.clone()));
    container
}

/// The warnings that `node_name` collided with another node.
fn collisions(node_name: &str) -> Vec<String> {
    capture::messages_containing(&["to the same address", node_name])
}

fn assert_collision(message: &str, node_a: &str, node_b: &str, host: &Host, translator: &str) {
    for part in [node_a, node_b, &format!("{}:{}", host.name, host.port), translator, "to the same address"] {
        assert!(message.contains(part), "{part} not in: {message}");
    }
}

/// N-15: pass-through, Branch A. Each node advertises its own
/// alternate-access-address hostname and its own listener port, and the
/// client reads the alternate list through the pass-through translator with
/// no map. Every partition owner is a distinct node at the address its node
/// advertises, and writes and reads are served by the owner without a server
/// proxy hop.
#[test]
fn n15_distinct_names_route_to_every_owner() {
    run("N-15", || async {
        if !skip_unless(topo::t().pass_through_branch == "A", &format!("N-15{BRANCH_A}")) {
            return;
        }
        let nodes = topo::nodes().await;
        let mut hostnames = HashSet::new();
        for node in &nodes {
            let alternate = alternate(node).await;
            println!(
                "L2-N15-ADVERTISED {} standard={:?} alternate={alternate}",
                node.name, node.advertised
            );
            hostnames.insert(topo::host_of(&alternate));
            assert_eq!(node.port, topo::port_of(&alternate));
        }
        assert_eq!(nodes.len(), hostnames.len(), "Hostnames {hostnames:?}");
        assert_routes_to_owners("N-15", pass_through_policy(), &nodes).await;
    });
}

/// N-16: pass-through, Branch B. Every node advertises the same
/// alternate-access-address hostname and its own alternate-access-port, the
/// client reads the alternate list through the pass-through translator with
/// no map, and the partition map is complete with every owner a distinct
/// node served without a server proxy hop.
#[test]
fn n16_shared_name_routes_to_every_owner() {
    run("N-16", || async {
        if !require_branch_b("N-16") {
            return;
        }
        let nodes = topo::nodes().await;
        let mut hostnames = HashSet::new();
        for node in &nodes {
            let alternate = alternate(node).await;
            println!(
                "L2-N16-ADVERTISED {} standard={:?} alternate={alternate}",
                node.name, node.advertised
            );
            hostnames.insert(topo::host_of(&alternate));
            assert_eq!(node.port, topo::port_of(&alternate));
        }
        assert_eq!(1, hostnames.len(), "Hostnames {hostnames:?}");
        assert_routes_to_owners("N-16", pass_through_policy(), &nodes).await;
    });
}

/// N-16 with on_miss strict: the pass-through translator never misses, so
/// routing is the same as with the default.
#[test]
fn n16_shared_name_strict_routes_to_every_owner() {
    run("N-16 strict", || async {
        if !require_branch_b("N-16 strict") {
            return;
        }
        let mut policy = pass_through_policy();
        policy.translation_on_miss = TranslationMissMode::Strict;
        assert_routes_to_owners("N-16-strict", policy, &topo::nodes().await).await;
    });
}

/// N-16 scale-out: a node started mid-test that advertises the same hostname
/// and its own port is an active node within 2 x tend_interval of the
/// peers-generation bump that announces it, with no client change. The test
/// gives it a listener on that port before it joins.
#[test]
fn n16_new_node_on_shared_name_active_within_two_tend_intervals() {
    run("N-16 scale-out", || async {
        if !require_branch_b("N-16 scale-out") {
            return;
        }
        let nodes = topo::nodes().await;
        let first = nodes[0].clone();
        let names = topo::node_names().await;
        let hostname = topo::host_of(&alternate(&first).await);
        let policy = pass_through_policy();

        let client = rows::connect(&policy, &[seed(&first).await]).await;
        topo::wait_nodes(&client, &names).await;
        topo::assert_all_partitions_owned("N-16-before", &client).await;

        let generation = topo::peers_generation(&first).await;
        let port = topo::next_listener_port().await;
        let container = start(&format!("alternate-port={port}")).await;
        let upstream = topo::upstream_port(&first.proxy).await;
        topo::add_listener_on(&container, port, &format!("{container}:{upstream}")).await;
        topo::restore_later(Restore::RemoveListener(container.clone()));
        assert_eq!(
            generation,
            topo::peers_generation(&first).await,
            "peers-generation bumped before {container} had a listener"
        );

        let joined = topo::wait_joined(&client, &first, generation, &names).await;
        let node = joined.node;
        let budget = 2 * u64::from(policy.tend_interval);
        let info = topo::info(&topo::t().host, port, &["node", "service-clear-alt"]).await;
        println!(
            "L2-N16-TIMING node={} host={} container={container} advertised={} generation={generation}->{} bumpToActiveMs={} tendInterval={} budgetMs={budget}",
            node.name(),
            node.host(),
            info["service-clear-alt"],
            joined.generation,
            joined.bump_to_active_ms,
            policy.tend_interval
        );
        assert_eq!(info["node"], node.name());
        assert!(
            info["service-clear-alt"].contains(&format!("{hostname}:{port}")),
            "{hostname}:{port} not in {info:?}"
        );
        assert_eq!(port, node.host().port);
        assert!(
            joined.bump_to_active_ms <= budget,
            "{} active {}ms after the bump, budget {budget}ms",
            node.name(),
            joined.bump_to_active_ms
        );

        let owns = topo::wait_for(|| topo::owners_of(&client).contains_key(node.name())).await;
        println!("L2-N16-OWNERS {:?}", topo::owners_of(&client));
        assert!(owns, "{} owns no partitions", node.name());
        client.close().await.expect("close");
    });
}

/// N-18 on pass-through: two nodes advertise the same alternate-access-address
/// and alternate-access-port. Seeded through a node that does not collide,
/// neither colliding node becomes a node, the collision message names both
/// node ids, the `host:port` and the translator on every tend, and the other
/// nodes keep serving their partitions.
#[test]
fn n18_pass_through_colliding_nodes_create_no_node() {
    run("N-18 pass-through", || async {
        if !require_branch_b("N-18 pass-through") {
            return;
        }
        assert_colliding_nodes_create_no_node("N-18-pass-through", pass_through_policy(), "pass-through").await;
    });
}

/// N-18 with the default translator: as on pass-through, named static-map.
#[test]
fn n18_static_map_colliding_nodes_create_no_node() {
    run("N-18 static-map", || async {
        if !require_branch_b("N-18 static-map") {
            return;
        }
        let mut policy = topo::policy();
        policy.use_services_alternate = true;
        assert_colliding_nodes_create_no_node("N-18-static-map", policy, "static-map").await;
    });
}

/// N-18 with a custom translator that maps every endpoint to the seed's
/// address: no peer becomes a node, the collision message names the peer,
/// the seed, the address and the translator on every tend, and the seed
/// keeps serving its partitions.
#[test]
fn n18_custom_translator_colliding_nodes_create_no_node() {
    run("N-18 custom", || async {
        if !require_branch_b("N-18 custom") {
            return;
        }
        let nodes = topo::nodes().await;
        let first = nodes[0].clone();
        let seed = seed(&first).await;
        let address = topo::resolve(&seed.name)
            .into_iter()
            .next()
            .unwrap_or_else(|| seed.name.clone());
        let target = Host::new(&address, seed.port);
        let translator = Arc::new(SingleAddressTranslator(target.clone()));
        let translator_name = translator.name();
        let mut policy = topo::policy();
        policy.use_services_alternate = true;
        policy.address_translator = Some(translator);
        capture::clear();

        let client = rows::connect(&policy, &[seed]).await;
        topo::wait_for(|| {
            assert_eq!(vec![first.name.clone()], topo::client_node_names(&client));
            nodes[1..].iter().all(|peer| collisions(&peer.name).len() >= COLLISION_RETRIES)
        })
        .await;

        for peer in &nodes[1..] {
            let collisions = collisions(&peer.name);
            println!(
                "L2-N18-CUSTOM-RESULT peer={} nodes={:?} target={target} collisionWarnings={}",
                peer.name,
                topo::client_node_names(&client),
                collisions.len()
            );
            assert!(
                collisions.len() >= COLLISION_RETRIES,
                "Colliding peer {} not retried every tend: {:?}",
                peer.name,
                capture::warnings()
            );
            println!("L2-N18-CUSTOM-LOG {}", collisions[0]);
            // The other node named is whichever the client met first at the
            // address: the seed, or another peer translated there too.
            let message = &collisions[0];
            let others: Vec<&str> = nodes.iter().filter(|n| n.name != peer.name).map(|n| n.name.as_str()).collect();
            assert!(
                others.iter().any(|o| message.contains(o)),
                "none of {others:?} in: {message}"
            );
            for part in [peer.name.as_str(), &format!("{}:{}", target.name, target.port), &translator_name, "to the same address"] {
                assert!(message.contains(part), "{part} not in: {message}");
            }
        }
        assert_eq!(vec![first.name.clone()], topo::client_node_names(&client));
        rows::assert_seed_keys_work("N-18-custom", &client, &first).await;
        client.close().await.expect("close");
    });
}

/// N-18 on port-map: two nodes are mapped to the same `host:port`. Seeded
/// through the node that does not collide, neither colliding node becomes a
/// node, the collision message names both node ids, the `host:port` and the
/// translator on every tend, and the seed keeps serving its partitions.
#[test]
fn n18_port_map_colliding_nodes_create_no_node() {
    run("N-18 port-map", || async {
        if !require_branch_b("N-18 port-map") {
            return;
        }
        let nodes = topo::nodes().await;
        let first = nodes[0].clone();
        let a = nodes[1].clone();
        let b = nodes[2].clone();
        let mut port_map = topo::port_map_by_endpoint(&nodes);
        for advertised in &b.advertised {
            port_map.retain(|(k, _)| k != advertised);
            port_map.push((advertised.clone(), topo::listener(a.port)));
        }
        println!("L2-N18-PORT-MAP {port_map:?}");

        let mut policy = topo::policy();
        policy.address_translator = Some(rows::port_map(port_map));
        capture::clear();

        let client = rows::connect(&policy, &[topo::seed().await]).await;
        topo::wait_for(|| {
            assert_eq!(vec![first.name.clone()], topo::client_node_names(&client));
            collisions(&a.name).len() >= COLLISION_RETRIES && collisions(&b.name).len() >= COLLISION_RETRIES
        })
        .await;

        let message = collisions(&a.name)[0].clone();
        println!(
            "L2-N18-PORT-MAP-RESULT nodes={:?} colliding=[{}, {}] collisionWarnings={}/{}",
            topo::client_node_names(&client),
            a.name,
            b.name,
            collisions(&a.name).len(),
            collisions(&b.name).len()
        );
        println!("L2-N18-PORT-MAP-LOG {message}");
        assert_eq!(vec![first.name.clone()], topo::client_node_names(&client));
        assert_collision(&message, &a.name, &b.name, &Host::new(&topo::t().host, a.port), "port-map");
        rows::assert_seed_keys_work("N-18-port-map", &client, &first).await;
        client.close().await.expect("close");
    });
}

/// N-18 newcomer: a node started mid-test advertises the alternate-access-
/// address and port of a node that is already a node. No node is created for
/// the newcomer's endpoint, under the newcomer's name or under the name of
/// the node that answers on it, and the collision is logged on every tend.
/// What happens to the existing node is reported, not asserted. The wait
/// stops at the first extra node, since each holds connections to the
/// existing node.
#[test]
fn n18_pass_through_newcomer_colliding_with_node_is_not_created() {
    run("N-18 newcomer", || async {
        if !require_branch_b("N-18 newcomer") {
            return;
        }
        let nodes = topo::nodes().await;
        let first = nodes[0].clone();
        let existing = nodes[nodes.len() - 1].clone();
        let names = topo::node_names().await;
        let endpoint = alternate(&existing).await;

        let client = rows::connect(&pass_through_policy(), &[seed(&first).await]).await;
        topo::wait_nodes(&client, &names).await;
        topo::assert_all_partitions_owned("N-18-newcomer-before", &client).await;
        let before = client.get_node(&existing.name).expect("existing");
        capture::clear();

        start(&format!("alternate-port={}", topo::port_of(&endpoint))).await;
        topo::wait_cluster_stable(nodes.len() + 1).await;

        let mut advertising = topo::peer_names(&first, "peers-clear-alt", &endpoint).await;
        advertising.retain(|n| *n != existing.name);
        println!(
            "L2-N18-NEWCOMER-ADVERTISED endpoint={endpoint} existing={} newcomer={advertising:?} peers-clear-alt={}",
            existing.name,
            topo::info_value(&first, "peers-clear-alt").await
        );
        assert_eq!(1, advertising.len(), "Newcomers advertising {endpoint}");

        let newcomer = advertising[0].clone();
        topo::wait_for(|| collisions(&newcomer).len() >= COLLISION_RETRIES || client.nodes().len() > names.len())
            .await;

        let collisions = collisions(&newcomer);
        let node_names = topo::client_node_names(&client);
        let after = client.get_node(&existing.name).ok();
        let instances = node_names.iter().filter(|n| **n == existing.name).count();
        println!(
            "L2-N18-NEWCOMER-RESULT newcomer={newcomer} nodes={node_names:?} collisionWarnings={}",
            collisions.len()
        );
        println!(
            "L2-N18-EXISTING node={} kept={} sameInstance={} instances={instances} active={} host={:?} owners={:?}",
            existing.name,
            node_names.contains(&existing.name),
            after.as_ref().is_some_and(|a| Arc::ptr_eq(a, &before)),
            after.as_ref().is_some_and(|a| a.is_active()),
            after.as_ref().map(|a| a.host().to_string()),
            topo::owners_of(&client)
        );
        assert!(!node_names.contains(&newcomer), "{newcomer} is a Node: {node_names:?}");
        assert_eq!(
            names, node_names,
            "Node created for {newcomer}'s endpoint {endpoint}, answered by {}",
            existing.name
        );
        assert!(
            collisions.len() >= COLLISION_RETRIES,
            "No collision logged for {newcomer}: {:?}",
            capture::warnings()
        );
        println!("L2-N18-NEWCOMER-LOG {}", collisions[0]);
        assert_collision(
            &collisions[0],
            &newcomer,
            &existing.name,
            &Host::new(&topo::host_of(&endpoint), topo::port_of(&endpoint)),
            "pass-through",
        );
        client.close().await.expect("close");
    });
}

async fn assert_colliding_nodes_create_no_node(row: &str, policy: ClientPolicy, translator: &str) {
    let nodes = topo::nodes().await;
    let first = nodes[0].clone();
    let names = topo::node_names().await;
    let hostname = topo::host_of(&alternate(&first).await);
    let port = topo::next_listener_port().await;
    let endpoint = format!("{hostname}:{port}");
    let container = start(&format!("alternate-port={port}")).await;
    start(&format!("alternate-port={port}")).await;
    topo::wait_cluster_stable(nodes.len() + 2).await;
    let upstream = topo::upstream_port(&first.proxy).await;
    topo::add_listener_on(&container, port, &format!("{container}:{upstream}")).await;
    topo::restore_later(Restore::RemoveListener(container.clone()));

    let colliding = topo::peer_names(&first, "peers-clear-alt", &endpoint).await;
    println!(
        "L2-{row}-ADVERTISED endpoint={endpoint} nodes={colliding:?} peers-clear-alt={}",
        topo::info_value(&first, "peers-clear-alt").await
    );
    assert_eq!(2, colliding.len(), "Nodes advertising {endpoint}");
    capture::clear();

    let client = rows::connect(&policy, &[seed(&first).await]).await;
    topo::wait_nodes(&client, &names).await;
    topo::wait_for(|| {
        assert_eq!(names, topo::client_node_names(&client));
        collisions(&colliding[0]).len() >= COLLISION_RETRIES && collisions(&colliding[1]).len() >= COLLISION_RETRIES
    })
    .await;

    let message = collisions(&colliding[0])[0].clone();
    println!(
        "L2-{row}-RESULT nodes={:?} colliding={colliding:?} collisionWarnings={}/{}",
        topo::client_node_names(&client),
        collisions(&colliding[0]).len(),
        collisions(&colliding[1]).len()
    );
    println!("L2-{row}-LOG {message}");
    assert_eq!(names, topo::client_node_names(&client));
    assert!(
        collisions(&colliding[1]).len() >= COLLISION_RETRIES,
        "Colliding node {} not retried every tend: {:?}",
        colliding[1],
        capture::warnings()
    );
    assert_collision(&message, &colliding[0], &colliding[1], &Host::new(&hostname, port), translator);
    assert_others_serve(row, &client, &names).await;
    client.close().await.expect("close");
}

async fn assert_routes_to_owners(row: &str, policy: ClientPolicy, nodes: &[L2Node]) {
    capture::clear();
    let client = rows::connect(&policy, &[seed(&nodes[0]).await]).await;
    topo::wait_nodes(&client, &topo::node_names().await).await;
    let masters = topo::assert_all_partitions_owned(row, &client).await;
    let distinct = topo::distinct_masters(&masters);
    let owners = topo::owners(&masters);
    println!("L2-{row}-OWNERS nodes={} partitionsPerNode={owners:?}", distinct.len());
    assert_eq!(nodes.len(), distinct.len());
    assert_eq!(topo::node_names().await, owners.keys().cloned().collect::<Vec<_>>());

    for node in client.nodes() {
        let h = node.host();
        let alternate = alternate(&rows::node_of(nodes, node.name())).await;
        println!("L2-{row}-NODE {} host={}:{} alternate={alternate}", node.name(), h.name, h.port);
        let by_name = client.get_node(node.name()).expect("node by name");
        assert!(Arc::ptr_eq(&node, &by_name), "{}", node.name());
        assert!(distinct.iter().any(|d| Arc::ptr_eq(d, &node)), "{}", node.name());
        topo::assert_address(&alternate, &h);
    }
    assert_served_by_owners(row, &client, &masters, nodes).await;
    assert_eq!(Vec::<String>::new(), capture::warnings());
    client.close().await.expect("close");
}

/// Writes and reads keys spread across partitions with replica MASTER and
/// asserts, from each node's namespace statistics, that each node served
/// exactly the keys it owns with no server proxy hop.
async fn assert_served_by_owners(
    row: &str,
    client: &Client,
    masters: &[Option<Arc<aerospike::Node>>],
    nodes: &[L2Node],
) {
    let stats = [
        "client_write_success",
        "client_read_success",
        "client_proxy_complete",
        "client_proxy_error",
        "client_proxy_timeout",
    ];
    let mut before = BTreeMap::new();
    for node in nodes {
        before.insert(node.name.clone(), topo::namespace_stats(node, &stats).await);
    }

    let mut wp = WritePolicy::default();
    wp.base_policy.replica = Replica::Master;
    let mut rp = ReadPolicy::default();
    rp.base_policy.replica = Replica::Master;
    let mut served: BTreeMap<String, i64> = BTreeMap::new();
    for i in 0..KEY_COUNT {
        let key = rows::key(SET, format!("{row}-{i}"));
        let owner = masters[key.partition_id()].clone().expect("owned");
        let write_node = client.write_node(&key, Replica::Master).expect("write node");
        let read_node = client.read_node(&key, Replica::Master).expect("read node");
        assert!(Arc::ptr_eq(&owner, &write_node), "{key}: write routed to {}", write_node.name());
        assert!(Arc::ptr_eq(&owner, &read_node), "{key}: read routed to {}", read_node.name());
        put_get(client, &wp, &rp, &key, i as i64).await;
        *served.entry(owner.name().to_string()).or_insert(0) += 1;
    }

    for node in nodes {
        let after = topo::namespace_stats(node, &stats).await;
        let start = &before[&node.name];
        let delta = |name: &str| after[name] - start[name];
        let writes = delta("client_write_success");
        let reads = delta("client_read_success");
        let proxied = delta("client_proxy_complete") + delta("client_proxy_error") + delta("client_proxy_timeout");
        let expected = served.get(&node.name).copied().unwrap_or(0);
        println!(
            "L2-{row}-SERVED node={} keys={expected} writes={writes} reads={reads} proxied={proxied}",
            node.name
        );
        assert!(expected > 0, "{} served no keys", node.name);
        assert_eq!(expected, writes, "{} writes", node.name);
        assert_eq!(expected, reads, "{} reads", node.name);
        assert_eq!(0, proxied, "{} proxied", node.name);
    }
}

/// Every partition with a master in the client's map is owned by one of
/// `names` and serves writes and reads, and the partitions of the colliding
/// nodes have no master.
async fn assert_others_serve(row: &str, client: &Client, names: &[String]) {
    let masters = topo::masters(client);
    let mut wp = WritePolicy::default();
    wp.base_policy.replica = Replica::Master;
    let mut rp = ReadPolicy::default();
    rp.base_policy.replica = Replica::Master;
    let mut served = 0;
    let mut no_master = 0;
    for i in 0..KEY_COUNT {
        let key = rows::key(SET, format!("{row}-{i}"));
        let Some(node) = &masters[key.partition_id()] else {
            no_master += 1;
            continue;
        };
        assert!(names.contains(&node.name().to_string()), "{}", node.name());
        put_get(client, &wp, &rp, &key, i as i64).await;
        served += 1;
    }
    println!("L2-{row}-KEYS served={served} keysWithoutMaster={no_master}");
    assert!(served > 0);
    assert!(no_master > 0);
}
