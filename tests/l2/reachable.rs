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

//! Testing strategy row N-10 and the alternate list cases against the
//! `reachable` profile (Java `TestL2ReachableNode`). The node named by
//! DISCOVERY_REACHABLE_NODE advertises a hostname and its own listener port
//! as its access address; the hostname resolves to the proxy in the client
//! and server containers. Every other node advertises its private address in
//! the standard list and the same hostname with its own listener port in the
//! alternate list. Without the profile every case is skipped.

use std::sync::Arc;

use aerospike::discovery::{
    AddressTranslator, Endpoint, PortMapAddressTranslator, SourceList, TranslationMissMode,
};
use aerospike::{Client, ClientPolicy, ErrorKind, Host};

use super::rows::{self, EmptyTranslator, MISS_RETRIES};
use super::topology::{self as topo, L2Node};
use super::{capture, run, skip_unless};

const BLOCKER: &str = " needs DISCOVERY_REACHABLE_NODE, a node advertising an address the client can \
                       reach (run_l2.sh L2_PROFILE=reachable, or the discovery-l2 workflow's reachable profile)";

fn require_alternates() -> bool {
    skip_unless(
        !topo::t().reachable_node.is_empty(),
        &format!("Alternate list cases{BLOCKER}"),
    )
}

fn alternate_policy() -> ClientPolicy {
    let mut policy = topo::policy();
    policy.use_services_alternate = true;
    policy
}

fn reachable(nodes: &[L2Node]) -> L2Node {
    let node = nodes
        .iter()
        .find(|n| n.proxy == topo::t().reachable_node)
        .cloned()
        .unwrap_or_else(|| {
            panic!(
                "DISCOVERY_REACHABLE_NODE {} has no listener: {nodes:?}",
                topo::t().reachable_node
            )
        });
    assert!(
        node.name != nodes[0].name,
        "Reachable node {node} must not be the first listener"
    );
    node
}

async fn with_alternate(nodes: &[L2Node]) -> Vec<L2Node> {
    let mut list = Vec::new();
    for node in nodes {
        if !topo::alternates(node).await.is_empty() {
            list.push(node.clone());
        }
    }
    assert!(list.len() >= 2, "Fewer than two nodes with an alternate address: {nodes:?}");
    list
}

fn names(nodes: &[L2Node]) -> Vec<String> {
    let mut names: Vec<String> = nodes.iter().map(|n| n.name.clone()).collect();
    names.sort();
    names
}

/// The errors that `node` advertises no alternate address.
fn alternate_missing(node: &L2Node) -> Vec<String> {
    capture::errors()
        .into_iter()
        .filter(|e| e.contains(&format!("Node {}", node.name)) && e.contains("use_services_alternate"))
        .collect()
}

fn translation_misses() -> Vec<String> {
    capture::messages_containing(&["has no mapping"])
}

/// N-10: on_miss unset (pass_through). port-map covers every node but the
/// reachable one, whose advertised address is used unchanged for its node.
/// All nodes join, no translation miss is logged and the partition map is
/// complete.
#[test]
fn n10_unmapped_peer_passes_through_unchanged() {
    run("N-10", || async {
        if !skip_unless(!topo::t().reachable_node.is_empty(), &format!("N-10{BLOCKER}")) {
            return;
        }
        let nodes = topo::nodes().await;
        let reachable = reachable(&nodes);
        let mapped: Vec<L2Node> = nodes.iter().filter(|n| n.name != reachable.name).cloned().collect();
        let advertised = reachable.advertised[0].clone();
        let port_map = topo::port_map_by_endpoint(&mapped);

        let endpoint = Endpoint::new(
            reachable.name.clone(),
            None,
            topo::host_of(&advertised),
            topo::port_of(&advertised),
            SourceList::Standard,
        );
        assert!(
            PortMapAddressTranslator::new(port_map.clone())
                .expect("port map")
                .translate(&endpoint)
                .is_none(),
            "{advertised} is mapped"
        );
        assert_eq!(TranslationMissMode::PassThrough, ClientPolicy::default().translation_on_miss);
        println!(
            "L2-N10-ADVERTISED node={} container={} advertised={:?} portMap={port_map:?}",
            reachable.name, reachable.proxy, reachable.advertised
        );

        let mut policy = topo::policy();
        policy.address_translator = Some(rows::port_map(port_map));
        capture::clear();

        let client = rows::connect(&policy, &[Host::new(&topo::t().host, mapped[0].port)]).await;
        topo::wait_nodes(&client, &topo::node_names().await).await;

        let node = client.get_node(&reachable.name).expect("reachable node");
        let h = node.host();
        println!(
            "L2-N10-NODE {} host={}:{} active={} advertised={advertised} nodes={:?}",
            node.name(),
            h.name,
            h.port,
            node.is_active(),
            topo::client_node_names(&client)
        );
        assert!(node.is_active());
        topo::assert_address(&advertised, &h);

        let owners = topo::owners(&topo::assert_all_partitions_owned("N-10", &client).await);
        assert_eq!(topo::node_names().await, owners.keys().cloned().collect::<Vec<_>>());
        assert_eq!(Vec::<String>::new(), translation_misses());
        client.close().await.expect("close");
    });
}

/// use_services_alternate with the default translator: the nodes with an
/// alternate address join on it with no translation miss, and the reachable
/// node, which has none, is not added and is reported on every tend.
#[test]
fn alternate_default_joins_nodes_with_alternate() {
    run("alternate-default", || async {
        if !require_alternates() {
            return;
        }
        assert_alternate_joins("alternate-default", alternate_policy()).await;
    });
}

/// use_services_alternate, on_miss strict and an ip_map that maps the
/// alternate hostname to itself: every alternate address is mapped, so the
/// nodes with one join.
#[test]
fn alternate_identity_ip_map_strict_joins() {
    run("alternate-ipmap-identity-strict", || async {
        if !require_alternates() {
            return;
        }
        let first = topo::nodes().await[0].clone();
        let hostname = topo::host_of(&topo::alternates(&first).await[0]);
        let mut policy = alternate_policy();
        policy.ip_map = Some([(hostname.clone(), hostname)].into_iter().collect());
        policy.translation_on_miss = TranslationMissMode::Strict;
        assert_alternate_joins("alternate-ipmap-identity-strict", policy).await;
    });
}

/// use_services_alternate, on_miss pass_through and a custom translator that
/// maps nothing: every alternate address is used unchanged.
#[test]
fn alternate_empty_custom_pass_through_joins() {
    run("alternate-custom-empty-pass-through", || async {
        if !require_alternates() {
            return;
        }
        let mut policy = alternate_policy();
        policy.address_translator = Some(Arc::new(EmptyTranslator));
        policy.translation_on_miss = TranslationMissMode::PassThrough;
        assert_alternate_joins("alternate-custom-empty-pass-through", policy).await;
    });
}

/// use_services_alternate, on_miss strict and an empty ip_map: the peer with
/// an alternate address misses on the alternate list on every tend and only
/// the seed is a node.
#[test]
fn alternate_empty_ip_map_strict_drops_peer() {
    run("alternate-ipmap-empty-strict", || async {
        if !require_alternates() {
            return;
        }
        let mut policy = alternate_policy();
        policy.ip_map = Some(Default::default());
        policy.translation_on_miss = TranslationMissMode::Strict;
        assert_alternate_misses("alternate-ipmap-empty-strict", policy, "static-map").await;
    });
}

/// use_services_alternate, on_miss strict and a custom translator that maps
/// nothing: as with the empty ip_map, named by the translator.
#[test]
fn alternate_empty_custom_strict_drops_peer() {
    run("alternate-custom-empty-strict", || async {
        if !require_alternates() {
            return;
        }
        let mut policy = alternate_policy();
        policy.address_translator = Some(Arc::new(EmptyTranslator));
        policy.translation_on_miss = TranslationMissMode::Strict;
        let name = EmptyTranslator.name();
        assert_alternate_misses("alternate-custom-empty-strict", policy, &name).await;
    });
}

/// use_services_alternate seeded at the node without an alternate address:
/// initialization fails with an error that names the node and the flag.
#[test]
fn alternate_seed_without_alternate_fails_init() {
    run("alternate-seed-missing", || async {
        if !require_alternates() {
            return;
        }
        let reachable = reachable(&topo::nodes().await);
        match Client::new(&alternate_policy(), &vec![Host::new(&topo::t().host, reachable.port)]).await {
            Ok(client) => {
                client.close().await.expect("close");
                panic!("client constructed at a seed without an alternate address");
            }
            Err(e) => {
                println!("L2-ALT-SEED-MISSING {}: {e}", e.kind().name());
                assert!(matches!(e.kind(), ErrorKind::AlternateAddressMissing), "{e}");
                let message = e.to_string();
                assert!(message.contains(&reachable.name), "{message}");
                assert!(message.contains("use_services_alternate"), "{message}");
            }
        }
    });
}

async fn assert_alternate_joins(row: &str, policy: ClientPolicy) {
    let nodes = topo::nodes().await;
    let reachable = reachable(&nodes);
    let with_alternate = with_alternate(&nodes).await;
    let names = names(&with_alternate);
    capture::clear();

    let client = rows::connect(&policy, &[Host::new(&topo::t().host, with_alternate[0].port)]).await;
    topo::wait_nodes(&client, &names).await;
    topo::wait_for(|| {
        assert_eq!(names, topo::client_node_names(&client));
        alternate_missing(&reachable).len() >= MISS_RETRIES
    })
    .await;

    let missing = alternate_missing(&reachable);
    println!(
        "L2-ALT-JOIN {row} nodes={:?} withoutAlternate={} missingErrors={} translationMisses={}",
        topo::client_node_names(&client),
        reachable.name,
        missing.len(),
        translation_misses().len()
    );
    for expected in &with_alternate {
        let h = client.get_node(&expected.name).expect("node").host();
        let alternates = topo::alternates(expected).await;
        println!(
            "L2-ALT-NODE {row} {} host={}:{} alternate={alternates:?}",
            expected.name, h.name, h.port
        );
        topo::assert_address(&alternates[0], &h);
    }
    assert_eq!(names, topo::client_node_names(&client));
    assert!(
        missing.len() >= MISS_RETRIES,
        "Node {} without alternate not reported every tend: {:?}",
        reachable.name,
        capture::errors()
    );
    println!("L2-ALT-LOG {row} {}", missing[0]);
    assert_eq!(Vec::<String>::new(), translation_misses());
    client.close().await.expect("close");
}

async fn assert_alternate_misses(row: &str, policy: ClientPolicy, translator: &str) {
    let with_alternate = with_alternate(&topo::nodes().await).await;
    let seed = with_alternate[0].clone();
    let peer = with_alternate[1].clone();
    let endpoint = topo::alternates(&peer).await[0].clone();
    capture::clear();

    let client = rows::connect(&policy, &[Host::new(&topo::t().host, seed.port)]).await;
    topo::wait_for(|| {
        assert_eq!(vec![seed.name.clone()], topo::client_node_names(&client));
        rows::misses(&peer.name).len() >= MISS_RETRIES
    })
    .await;

    let misses = rows::misses(&peer.name);
    println!(
        "L2-ALT-MISS {row} peer={} nodes={:?} missWarnings={}",
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
    println!("L2-ALT-MISS-LOG {row} {message}");
    for part in [peer.name.as_str(), endpoint.as_str(), "alternate", translator, "pass_through"] {
        assert!(message.contains(part), "{part} not in: {message}");
    }
    assert_eq!(vec![seed.name.clone()], topo::client_node_names(&client));
    rows::assert_seed_keys_work(row, &client, &seed).await;
    client.close().await.expect("close");
}
