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

//! Testing strategy row N-19 against the `port-map-range` profile (Java
//! `TestL2PortMapRange`). The nodes sit on a fixed subnet and
//! DISCOVERY_PORT_MAP_RANGE names a contiguous range of node addresses,
//! including addresses no node has yet. Toxiproxy has a listener for every
//! address in the range on DISCOVERY_PORT_MAP_PORT_BASE plus the address's
//! last octet, upstream by IP. One port-map built from that rule before the
//! cluster scales out covers every node started inside the range. Without
//! the profile every case is skipped.

use std::collections::HashMap;

use aerospike::discovery::TranslationMissMode;

use super::rows::{self, MISS_RETRIES};
use super::topology::{self as topo, L2Node, Restore};
use super::{capture, run, skip_unless};

const BLOCKER: &str = " needs DISCOVERY_PORT_MAP_RANGE, DISCOVERY_PORT_MAP_PORT_BASE and \
                       DISCOVERY_CONTROL_DIR, nodes on a fixed subnet with a listener per address \
                       of a range (run_l2.sh L2_PROFILE=port-map-range)";

fn require_range() -> bool {
    let t = topo::t();
    skip_unless(
        !t.port_map_range.is_empty() && !t.port_map_port_base.is_empty() && !t.control_dir.is_empty(),
        &format!("N-19{BLOCKER}"),
    )
}

fn range() -> (String, String) {
    let range = &topo::t().port_map_range;
    let (first, last) = range.split_once('-').unwrap_or_else(|| panic!("{range}"));
    assert_eq!(prefix(first), prefix(last), "{range}");
    (first.to_string(), last.to_string())
}

fn prefix(address: &str) -> String {
    address[..=address.rfind('.').expect("dotted address")].to_string()
}

fn octet(address: &str) -> u16 {
    address[address.rfind('.').expect("dotted address") + 1..]
        .parse()
        .expect("last octet")
}

fn listener_port(address: &str) -> u16 {
    topo::t().port_map_port_base.parse::<u16>().expect("port base") + octet(address)
}

fn service_port(nodes: &[L2Node]) -> u16 {
    topo::port_of(&nodes[0].advertised[0])
}

/// Builds the port-map from the rule, for every address of the range, and
/// checks that the cluster's nodes are inside the range and every listener
/// of the range exists.
async fn range_port_map(nodes: &[L2Node]) -> Vec<(String, String)> {
    let (first, last) = range();
    let prefix = prefix(&first);
    let port = service_port(nodes);
    let listeners = topo::listener_ports().await;
    let mut map = Vec::new();
    for octet in octet(&first)..=octet(&last) {
        let address = format!("{prefix}{octet}");
        let listener = listener_port(&address);
        assert!(
            listeners.contains(&listener),
            "No listener on {listener} for {address}: {listeners:?}"
        );
        map.push((format!("{address}:{port}"), topo::listener(listener)));
    }
    for node in nodes {
        let mapped = map.iter().find(|(k, _)| *k == node.advertised[0]).map(|(_, v)| v.clone());
        println!("L2-N19-NODE {} advertised={:?} mapped={mapped:?}", node.name, node.advertised);
        assert!(mapped.is_some(), "{:?} outside {}", node.advertised, topo::t().port_map_range);
    }
    println!(
        "L2-N19-PORT-MAP range={} portBase={} entries={} {map:?}",
        topo::t().port_map_range,
        topo::t().port_map_port_base,
        map.len()
    );
    map
}

fn free_address(nodes: &[L2Node]) -> String {
    let (first, last) = range();
    let prefix = prefix(&first);
    let port = service_port(nodes);
    let used: Vec<&String> = nodes.iter().flat_map(|n| n.advertised.iter()).collect();
    for octet in octet(&first)..=octet(&last) {
        let address = format!("{prefix}{octet}");
        if !used.contains(&&format!("{address}:{port}")) {
            return address;
        }
    }
    panic!("No free address in {}: {used:?}", topo::t().port_map_range);
}

async fn start(address: &str) -> HashMap<String, String> {
    let ack = topo::control(&format!("start address={address}")).await;
    topo::restore_later(Restore::StopNode(ack["container"].clone()));
    ack
}

/// The messages that the unchanged, unreachable address of a node was tried.
fn attempts(endpoint: &str) -> Vec<String> {
    capture::messages_containing(&[endpoint, "failed"])
}

/// N-19: a port-map pre-filled from a fixed rule (node address to listener
/// port base plus the address's last octet) for every address of the range
/// joins the cluster with no warning. A node started on a free address of
/// the range is an active node within 2 x tend_interval of the
/// peers-generation bump that announces it, with no client configuration
/// change, and the partition map is then complete with the new node among
/// the owners.
#[test]
fn n19_prefilled_port_map_joins_node_in_range() {
    run("N-19", || async {
        if !require_range() {
            return;
        }
        let nodes = topo::nodes().await;
        let first = nodes[0].clone();
        let names = topo::node_names().await;
        let port_map = range_port_map(&nodes).await;
        let mut policy = topo::policy();
        policy.address_translator = Some(rows::port_map(port_map.clone()));
        capture::clear();

        let client = rows::connect(&policy, &[topo::seed().await]).await;
        topo::wait_nodes(&client, &names).await;
        topo::assert_all_partitions_owned("N-19-before", &client).await;
        assert_eq!(Vec::<String>::new(), capture::warnings());

        let address = free_address(&nodes);
        let generation = topo::peers_generation(&first).await;
        let ack = start(&address).await;
        assert_eq!(address, ack["address"]);

        let joined = topo::wait_joined(&client, &first, generation, &names).await;
        let node = joined.node;
        let budget = 2 * u64::from(policy.tend_interval);
        let port = listener_port(&address);
        let info = topo::info(&topo::t().host, port, &["node", "service-clear-std"]).await;
        let mapped = port_map
            .iter()
            .find(|(k, _)| *k == format!("{address}:{}", service_port(&nodes)))
            .map(|(_, v)| v.clone());
        println!(
            "L2-N19-TIMING node={} host={} container={} address={address} advertised={} mapped={mapped:?} generation={generation}->{} bumpToActiveMs={} tendInterval={} budgetMs={budget}",
            node.name(),
            node.host(),
            ack["container"],
            info["service-clear-std"],
            joined.generation,
            joined.bump_to_active_ms,
            policy.tend_interval
        );
        assert_eq!(info["node"], node.name());
        assert_eq!(port, node.host().port);
        assert!(
            joined.bump_to_active_ms <= budget,
            "{} active {}ms after the bump, budget {budget}ms",
            node.name(),
            joined.bump_to_active_ms
        );

        let owns = topo::wait_for(|| topo::owners_of(&client).contains_key(node.name())).await;
        println!("L2-N19-OWNERS {:?}", topo::owners_of(&client));
        assert!(owns, "{} owns no partitions", node.name());
        assert_eq!(
            names.len() + 1,
            topo::owners(&topo::assert_all_partitions_owned("N-19-after", &client).await).len()
        );
        client.close().await.expect("close");
    });
}

/// N-19 outside the range: a node started on an address the pre-filled
/// port-map does not cover is decided by on_miss. With strict the error
/// names the node, its advertised endpoint and the translator on every tend
/// and no node is created. With the default the unchanged address, which the
/// client cannot reach, is tried on every tend and no node is created.
#[test]
fn n19_node_outside_range_decided_by_on_miss() {
    run("N-19 outside", || async {
        if !require_range() {
            return;
        }
        let nodes = topo::nodes().await;
        let first = nodes[0].clone();
        let names = topo::node_names().await;
        let port_map = range_port_map(&nodes).await;
        let (_, last) = range();
        let address = format!("{}{}", prefix(&last), octet(&last) + 1);
        let advertised = format!("{address}:{}", service_port(&nodes));

        assert_eq!(address, start(&address).await["address"]);
        topo::wait_cluster_stable(nodes.len() + 1).await;

        let name = topo::peer_name(&first, "peers-clear-std", &advertised).await;
        println!(
            "L2-N19-OUTSIDE node={name:?} advertised={advertised} range={}",
            topo::t().port_map_range
        );
        let name = name.unwrap_or_else(|| panic!("No peer advertises {advertised}"));
        assert!(!port_map.iter().any(|(k, _)| *k == advertised));

        let mut strict = topo::policy();
        strict.address_translator = Some(rows::port_map(port_map.clone()));
        strict.translation_on_miss = TranslationMissMode::Strict;
        capture::clear();

        let client = rows::connect(&strict, &[topo::seed().await]).await;
        topo::wait_for(|| {
            assert_eq!(names, topo::client_node_names(&client));
            rows::misses(&name).len() >= MISS_RETRIES
        })
        .await;
        let misses = rows::misses(&name);
        println!(
            "L2-N19-STRICT node={name} nodes={:?} missWarnings={}",
            topo::client_node_names(&client),
            misses.len()
        );
        assert!(
            misses.len() >= MISS_RETRIES,
            "Missed node {name} not retried every tend: {:?}",
            capture::warnings()
        );
        assert_eq!(names, topo::client_node_names(&client));
        let message = &misses[0];
        println!("L2-N19-STRICT-LOG {message}");
        for part in [name.as_str(), advertised.as_str(), "port-map", "standard"] {
            assert!(message.contains(part), "{part} not in: {message}");
        }
        client.close().await.expect("close");

        let mut pass_through = topo::policy();
        pass_through.address_translator = Some(rows::port_map(port_map));
        assert_eq!(TranslationMissMode::PassThrough, pass_through.translation_on_miss);
        capture::clear();

        let client = rows::connect(&pass_through, &[topo::seed().await]).await;
        topo::wait_for(|| {
            assert_eq!(names, topo::client_node_names(&client));
            attempts(&advertised).len() >= MISS_RETRIES
        })
        .await;
        let attempts = attempts(&advertised);
        println!(
            "L2-N19-PASS-THROUGH node={name} nodes={:?} attempts={} missWarnings={}",
            topo::client_node_names(&client),
            attempts.len(),
            rows::misses(&name).len()
        );
        assert!(
            attempts.len() >= MISS_RETRIES,
            "Unchanged address {advertised} not tried every tend: {:?}",
            capture::warnings()
        );
        println!("L2-N19-PASS-THROUGH-LOG {}", attempts[0]);
        assert_eq!(names, topo::client_node_names(&client));
        assert_eq!(Vec::<String>::new(), rows::misses(&name));
        client.close().await.expect("close");
    });
}
