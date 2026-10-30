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

//! The L2 topology as the environment describes it: a three node cluster on
//! a private network with no published ports, reachable from the client only
//! through one Toxiproxy listener per node behind one endpoint hostname. The
//! client has no route to the node addresses.
//!
//! - `DISCOVERY_ENDPOINT_HOSTNAME`: hostname every listener is behind
//! - `DISCOVERY_ENDPOINT_PORTS`: clear listener ports, one per node
//! - `DISCOVERY_TLS_ENDPOINT_PORTS`: TLS listener ports; empty on `ce`
//! - `DISCOVERY_PROXY_MAP`: Toxiproxy proxy name to listener port
//! - `DISCOVERY_SUBSET`: `ce` or `ee`
//! - `TOXIPROXY_URL`: Toxiproxy control API
//! - `AEROSPIKE_CONTAINER_NAMES`: server containers, one per node
//! - `DISCOVERY_CONTROL_DIR`: directory shared with the host, which starts
//!   and removes nodes on request; empty when the host offers no node control
//! - `DISCOVERY_REACHABLE_NODE`: `reachable` profile, the node advertising an
//!   address the client can reach
//! - `DISCOVERY_PASS_THROUGH_BRANCH`: `A` when each node advertises its own
//!   alternate-access-address hostname, `B` when every node advertises one
//!   hostname with its own listener port
//! - `DISCOVERY_PORT_MAP_RANGE`, `DISCOVERY_PORT_MAP_PORT_BASE`: `port-map-range`
//!   profile, a `first-last` range of node addresses with a listener each
//! - `DISCOVERY_TLS_CERT_SHAPE`: `san-only` or `cn-only` on `ee`
//! - `DISCOVERY_CLIENT_NETWORK_MTU`: `low-mtu` profile
//! - `DISCOVERY_PROXY_PORT_RANGE`: `port-range` profile, `first last`
//! - `DISCOVERY_IDLE_RESET`: `idle-reset` profile
//! - `AEROSPIKE_CA_CERT`: the server CA on `ee`
//!
//! Each node's name and advertised addresses are read through its listener
//! at runtime.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use aerospike::{Client, ClientPolicy, Error, Host, Node};

pub const TEND_INTERVAL: u32 = 250;
pub const CONNECT_TIMEOUT: u32 = 1000;
pub const WAIT_MS: u64 = 30_000;
pub const TLS_PROXY_SUFFIX: &str = "-tls";
pub const PARTITIONS: usize = 4096;
const INFO_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Topology {
    pub host: String,
    pub ports: Vec<u16>,
    pub tls_ports: Vec<u16>,
    pub proxy_ports: Vec<(String, u16)>,
    pub subset: String,
    pub toxiproxy_url: String,
    pub containers: Vec<String>,
    pub control_dir: String,
    pub reachable_node: String,
    pub pass_through_branch: String,
    pub port_map_range: String,
    pub port_map_port_base: String,
    pub tls_cert_shape: String,
    pub client_network_mtu: String,
    pub proxy_port_range: String,
    pub idle_reset: String,
    pub ca_cert: String,
    pub namespace: String,
}

fn optional(name: &str) -> String {
    env::var(name).unwrap_or_default()
}

fn ports(csv: &str) -> Vec<u16> {
    csv.split(',')
        .filter(|p| !p.is_empty())
        .map(|p| p.parse().unwrap_or_else(|_| panic!("port {p:?}")))
        .collect()
}

/// The topology, `None` when `DISCOVERY_ENDPOINT_HOSTNAME` is not set.
pub fn topology() -> Option<&'static Topology> {
    static TOPOLOGY: OnceLock<Option<Topology>> = OnceLock::new();
    TOPOLOGY
        .get_or_init(|| {
            let host = optional("DISCOVERY_ENDPOINT_HOSTNAME");
            if host.is_empty() {
                return None;
            }
            let required = |name: &str| {
                let value = optional(name);
                assert!(
                    !value.is_empty(),
                    "Environment variable {name} is not set. Run tests/discovery/run_l2.sh or the \
                     discovery-l2 workflow"
                );
                value
            };
            let proxy_ports = required("DISCOVERY_PROXY_MAP")
                .split(',')
                .map(|entry| {
                    let (name, port) = entry.rsplit_once(':').expect("proxy:port");
                    (name.to_string(), port.parse().expect("proxy port"))
                })
                .collect();
            Some(Topology {
                ports: ports(&required("DISCOVERY_ENDPOINT_PORTS")),
                tls_ports: ports(&optional("DISCOVERY_TLS_ENDPOINT_PORTS")),
                proxy_ports,
                subset: required("DISCOVERY_SUBSET"),
                toxiproxy_url: required("TOXIPROXY_URL"),
                containers: required("AEROSPIKE_CONTAINER_NAMES")
                    .split(',')
                    .map(str::to_string)
                    .collect(),
                control_dir: optional("DISCOVERY_CONTROL_DIR"),
                reachable_node: optional("DISCOVERY_REACHABLE_NODE"),
                pass_through_branch: optional("DISCOVERY_PASS_THROUGH_BRANCH"),
                port_map_range: optional("DISCOVERY_PORT_MAP_RANGE"),
                port_map_port_base: optional("DISCOVERY_PORT_MAP_PORT_BASE"),
                tls_cert_shape: optional("DISCOVERY_TLS_CERT_SHAPE"),
                client_network_mtu: optional("DISCOVERY_CLIENT_NETWORK_MTU"),
                proxy_port_range: optional("DISCOVERY_PROXY_PORT_RANGE"),
                idle_reset: optional("DISCOVERY_IDLE_RESET"),
                ca_cert: optional("AEROSPIKE_CA_CERT"),
                namespace: env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string()),
                host,
            })
        })
        .as_ref()
}

/// The topology; the row has checked it exists.
pub fn t() -> &'static Topology {
    topology().expect("the L2 topology")
}

pub fn namespace() -> &'static str {
    &t().namespace
}

/// Node behind one listener, as it advertises itself.
#[derive(Clone, Debug)]
pub struct L2Node {
    pub proxy: String,
    pub port: u16,
    pub name: String,
    pub advertised: Vec<String>,
    pub tls_port: u16,
    pub tls_advertised: Vec<String>,
    pub tls_name: Option<String>,
}

impl std::fmt::Display for L2Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} proxy={} listener={} advertised={:?}",
            self.name, self.proxy, self.port, self.advertised
        )?;
        if self.tls_port > 0 {
            write!(
                f,
                " tlsListener={} tlsAdvertised={:?} tlsName={:?}",
                self.tls_port, self.tls_advertised, self.tls_name
            )?;
        }
        Ok(())
    }
}

/// Peer as listed by a node's peers reply.
#[derive(Clone, Debug)]
pub struct Peer {
    pub name: String,
    pub tls_name: String,
    pub endpoints: Vec<String>,
}

static NODES: Mutex<Option<Vec<L2Node>>> = Mutex::new(None);

/// Queries every listener for its node's name and advertised addresses.
/// Read once per run.
pub async fn nodes() -> Vec<L2Node> {
    if let Some(nodes) = NODES.lock().unwrap().clone() {
        return nodes;
    }
    let t = t();
    assert_eq!(
        t.containers.len(),
        t.ports.len(),
        "DISCOVERY_ENDPOINT_PORTS {:?} vs AEROSPIKE_CONTAINER_NAMES {:?}",
        t.ports,
        t.containers
    );
    let commands: &[&str] = if t.tls_ports.is_empty() {
        &["node", "service-clear-std"]
    } else {
        &["node", "service-clear-std", "service-tls-std", "peers-tls-std"]
    };

    let mut list = Vec::new();
    let mut tls_names: HashMap<String, String> = HashMap::new();
    for &port in &t.ports {
        let proxy = proxy_name(port);
        let info = info(&t.host, port, commands).await;
        let name = info
            .get("node")
            .cloned()
            .unwrap_or_else(|| panic!("No node name from listener {port}"));
        let tls_port = t
            .proxy_ports
            .iter()
            .find(|(p, _)| *p == format!("{proxy}{TLS_PROXY_SUFFIX}"))
            .map_or(0, |(_, port)| *port);
        let node = L2Node {
            proxy,
            port,
            name: name.clone(),
            advertised: endpoints(info.get("service-clear-std").map(String::as_str)),
            tls_port,
            tls_advertised: endpoints(info.get("service-tls-std").map(String::as_str)),
            tls_name: None,
        };
        assert!(
            !node.advertised.is_empty(),
            "Node {name} advertises no clear address"
        );
        list.push(node);
        for peer in parse_peers(info.get("peers-tls-std").map(String::as_str)) {
            if !peer.tls_name.is_empty() {
                tls_names.insert(peer.name, peer.tls_name);
            }
        }
    }
    for node in &mut list {
        node.tls_name = tls_names.get(&node.name).cloned();
        println!("L2-NODE {node}");
    }
    *NODES.lock().unwrap() = Some(list.clone());
    list
}

/// The cluster's node names, sorted.
pub async fn node_names() -> Vec<String> {
    let mut names: Vec<String> = nodes().await.into_iter().map(|n| n.name).collect();
    names.sort();
    names
}

/// The first listener as a seed.
pub async fn seed() -> Host {
    Host::new(&t().host, nodes().await[0].port)
}

pub fn listener(port: u16) -> String {
    format!("{}:{port}", t().host)
}

/// The policy every row starts from: a fast tend and a short connect timeout.
pub fn policy() -> ClientPolicy {
    ClientPolicy {
        tend_interval: TEND_INTERVAL,
        timeout: CONNECT_TIMEOUT,
        fail_if_not_connected: true,
        ..ClientPolicy::default()
    }
}

/// port-map from each node's advertised clear `host:port` to its listener.
pub fn port_map_by_endpoint(list: &[L2Node]) -> Vec<(String, String)> {
    list.iter()
        .flat_map(|node| {
            node.advertised
                .iter()
                .map(move |a| (a.clone(), listener(node.port)))
        })
        .collect()
}

/// port-map from each node's name to its listener.
pub fn port_map_by_node_name(list: &[L2Node]) -> Vec<(String, String)> {
    list.iter()
        .map(|node| (node.name.clone(), listener(node.port)))
        .collect()
}

/// port-map from each node's advertised TLS `host:port` to its TLS listener.
pub fn tls_port_map(list: &[L2Node]) -> Vec<(String, String)> {
    list.iter()
        .flat_map(|node| {
            node.tls_advertised
                .iter()
                .map(move |a| (a.clone(), listener(node.tls_port)))
        })
        .collect()
}

/// The client's node names, sorted.
pub fn client_node_names(client: &Client) -> Vec<String> {
    let mut names: Vec<String> = client.nodes().iter().map(|n| n.name().to_string()).collect();
    names.sort();
    names
}

pub async fn wait_for(mut condition: impl FnMut() -> bool) -> bool {
    wait_for_ms(&mut condition, 100).await
}

/// Polls `condition` every `poll_ms` for up to `WAIT_MS`.
pub async fn wait_for_ms(condition: &mut dyn FnMut() -> bool, poll_ms: u64) -> bool {
    let deadline = Instant::now() + Duration::from_millis(WAIT_MS);
    while !condition() {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(poll_ms)).await;
    }
    true
}

/// Waits until the client's node names are exactly `names`.
pub async fn wait_nodes(client: &Client, names: &[String]) {
    wait_for(|| client_node_names(client) == names).await;
    assert_eq!(names, client_node_names(client).as_slice());
}

/// The master of every partition of the namespace in the client's map, an
/// inactive node counting as none.
pub fn masters(client: &Client) -> Vec<Option<Arc<Node>>> {
    client
        .partition_masters(namespace())
        .map(|masters| {
            let mut masters: Vec<Option<Arc<Node>>> = masters
                .into_iter()
                .map(|n| n.filter(|n| n.is_active()))
                .collect();
            masters.resize(PARTITIONS, None);
            masters
        })
        .unwrap_or_else(|| vec![None; PARTITIONS])
}

pub fn unowned(masters: &[Option<Arc<Node>>]) -> usize {
    masters.iter().filter(|m| m.is_none()).count()
}

/// Waits until every partition of the namespace has an active master in the
/// client's partition map and returns the master of each partition.
pub async fn assert_all_partitions_owned(row: &str, client: &Client) -> Vec<Option<Arc<Node>>> {
    wait_for(|| unowned(&masters(client)) == 0).await;
    let masters = masters(client);
    let unowned = unowned(&masters);
    println!(
        "L2-PARTITIONS {row} partitions={PARTITIONS} unowned={unowned} owners={:?}",
        owners(&masters)
    );
    assert_eq!(0, unowned);
    masters
}

/// The number of partitions each node is master of.
pub fn owners(masters: &[Option<Arc<Node>>]) -> BTreeMap<String, usize> {
    let mut owners = BTreeMap::new();
    for node in masters.iter().flatten() {
        *owners.entry(node.name().to_string()).or_insert(0) += 1;
    }
    owners
}

pub fn owners_of(client: &Client) -> BTreeMap<String, usize> {
    owners(&masters(client))
}

/// The nodes `masters` names, each once.
pub fn distinct_masters(masters: &[Option<Arc<Node>>]) -> Vec<Arc<Node>> {
    let mut distinct: Vec<Arc<Node>> = Vec::new();
    for node in masters.iter().flatten() {
        if !distinct.iter().any(|d| Arc::ptr_eq(d, node)) {
            distinct.push(node.clone());
        }
    }
    distinct
}

/// Asserts the client cannot open a TCP connection to `endpoint`.
pub async fn assert_unreachable(endpoint: &str) {
    let host = host_of(endpoint);
    let port = port_of(endpoint);
    match tokio::time::timeout(Duration::from_secs(2), TcpStream::connect((host.as_str(), port))).await {
        Ok(Ok(_)) => panic!("Advertised address {endpoint} is reachable from the client"),
        Ok(Err(e)) => println!("L2-UNREACHABLE {endpoint} {e}"),
        Err(_) => println!("L2-UNREACHABLE {endpoint} connect timed out"),
    }
}

// ---------------------------------------------------------------------------
// Info protocol
// ---------------------------------------------------------------------------

/// One info request to `host:port`, as a client would send it.
pub async fn try_info(host: &str, port: u16, commands: &[&str]) -> Result<HashMap<String, String>, String> {
    let request = async {
        let mut stream = TcpStream::connect((host, port))
            .await
            .map_err(|e| format!("connect {host}:{port}: {e}"))?;
        let mut payload = String::new();
        for command in commands {
            payload.push_str(command);
            payload.push('\n');
        }
        let mut out = Vec::with_capacity(8 + payload.len());
        out.push(2);
        out.push(1);
        out.extend_from_slice(&(payload.len() as u64).to_be_bytes()[2..8]);
        out.extend_from_slice(payload.as_bytes());
        stream.write_all(&out).await.map_err(|e| e.to_string())?;

        let mut header = [0u8; 8];
        stream.read_exact(&mut header).await.map_err(|e| format!("read header: {e}"))?;
        let mut len = [0u8; 8];
        len[2..].copy_from_slice(&header[2..]);
        let mut body = vec![0u8; u64::from_be_bytes(len) as usize];
        stream.read_exact(&mut body).await.map_err(|e| format!("read body: {e}"))?;
        let text = String::from_utf8_lossy(&body).into_owned();
        let mut map = HashMap::new();
        for line in text.lines() {
            let (key, value) = line.split_once('\t').unwrap_or((line, ""));
            map.insert(key.to_string(), value.to_string());
        }
        Ok(map)
    };
    tokio::time::timeout(INFO_TIMEOUT, request)
        .await
        .map_err(|_| format!("info {host}:{port} {commands:?}: timed out"))?
}

pub async fn info(host: &str, port: u16, commands: &[&str]) -> HashMap<String, String> {
    try_info(host, port, commands)
        .await
        .unwrap_or_else(|e| panic!("{e}"))
}

/// One info value read through a node's listener.
pub async fn info_value(node: &L2Node, command: &str) -> String {
    info(&t().host, node.port, &[command])
        .await
        .remove(command)
        .unwrap_or_default()
}

/// The namespace statistics of a node, read through its listener.
pub async fn namespace_stats(node: &L2Node, names: &[&str]) -> HashMap<String, i64> {
    let command = format!("namespace/{}", namespace());
    let reply = info_value(node, &command).await;
    let mut stats = HashMap::new();
    for token in reply.split(';') {
        if let Some((name, value)) = token.split_once('=') {
            if names.contains(&name) {
                stats.insert(name.to_string(), value.parse().unwrap_or(0));
            }
        }
    }
    for wanted in names {
        assert!(
            stats.contains_key(*wanted),
            "Node {} has no namespace statistic {wanted}",
            node.name
        );
    }
    stats
}

pub async fn peers_generation(node: &L2Node) -> i64 {
    info_value(node, "peers-generation")
        .await
        .parse()
        .expect("peers-generation")
}

/// The alternate `host:port` list a node advertises in service-clear-alt.
pub async fn alternates(node: &L2Node) -> Vec<String> {
    endpoints(Some(&info_value(node, "service-clear-alt").await))
}

/// The name of the peer `observer` lists with `endpoint` in a peers reply.
pub async fn peer_name(observer: &L2Node, command: &str, endpoint: &str) -> Option<String> {
    parse_peers(Some(&info_value(observer, command).await))
        .into_iter()
        .find(|p| p.endpoints.iter().any(|e| e == endpoint))
        .map(|p| p.name)
}

/// The names of the peers `observer` lists with `endpoint` in `command`.
pub async fn peer_names(observer: &L2Node, command: &str, endpoint: &str) -> Vec<String> {
    let mut names: Vec<String> = parse_peers(Some(&info_value(observer, command).await))
        .into_iter()
        .filter(|p| p.endpoints.iter().any(|e| e == endpoint))
        .map(|p| p.name)
        .collect();
    names.sort();
    names
}

/// Waits until every node reports the same cluster key for a cluster of
/// `size` nodes with no migrations left.
pub async fn wait_cluster_stable(size: usize) {
    let command = format!("cluster-stable:size={size};ignore-migrations=false");
    let list = nodes().await;
    let deadline = Instant::now() + Duration::from_millis(WAIT_MS);
    let mut keys = Vec::new();
    loop {
        keys.clear();
        for node in &list {
            match try_info(&t().host, node.port, &[&command]).await {
                Ok(map) => keys.push(map.get(&command).cloned().unwrap_or_default()),
                Err(e) => keys.push(e),
            }
        }
        let distinct: HashSet<&String> = keys.iter().collect();
        let stable = distinct.len() == 1 && !keys[0].starts_with("ERROR") && !keys[0].is_empty();
        if stable || Instant::now() >= deadline {
            println!("L2-CLUSTER-STABLE size={size} {keys:?}");
            assert!(stable, "Cluster not stable at size {size}: {keys:?}");
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Parses a peers-clear-std or peers-tls-std reply:
/// `gen,port,[[node,tls-name,[address[:port],...]],...]`.
pub fn parse_peers(reply: Option<&str>) -> Vec<Peer> {
    let Some(reply) = reply else {
        return Vec::new();
    };
    let Some((_, rest)) = reply.split_once(',') else {
        return Vec::new();
    };
    let Some((port, list)) = rest.split_once(',') else {
        return Vec::new();
    };
    let default_port = port.parse::<u16>().unwrap_or(0);
    let list = list.trim();
    let list = list
        .strip_prefix('[')
        .and_then(|l| l.strip_suffix(']'))
        .unwrap_or(list);
    split_top_level(list)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.trim().strip_prefix('[')?.strip_suffix(']')?;
            let (name, rest) = entry.split_once(',')?;
            let (tls_name, addresses) = rest.split_once(',')?;
            let addresses = addresses.trim().strip_prefix('[')?.strip_suffix(']')?;
            let endpoints = split_top_level(addresses)
                .into_iter()
                .filter(|a| !a.is_empty())
                .map(|address| {
                    let has_port = if address.starts_with('[') {
                        address.contains("]:")
                    } else {
                        address.contains(':')
                    };
                    if has_port {
                        address
                    } else {
                        format!("{address}:{default_port}")
                    }
                })
                .collect();
            Some(Peer {
                name: name.to_string(),
                tls_name: tls_name.to_string(),
                endpoints,
            })
        })
        .collect()
}

/// Splits `text` at the commas outside brackets.
fn split_top_level(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut current = String::new();
    for c in text.chars() {
        match c {
            '[' => {
                depth += 1;
                current.push(c);
            }
            ']' => {
                depth = depth.saturating_sub(1);
                current.push(c);
            }
            ',' if depth == 0 => parts.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

/// A `service-*` reply as `host:port` strings, an IPv6 host in brackets.
pub fn endpoints(reply: Option<&str>) -> Vec<String> {
    let Some(reply) = reply else {
        return Vec::new();
    };
    reply
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|entry| {
            let (host, port) = split_host_port(entry);
            if host.contains(':') {
                format!("[{host}]:{port}")
            } else {
                format!("{host}:{port}")
            }
        })
        .collect()
}

/// `host:port`, `[ipv6]:port`, a bare host (port 3000) or a bare IPv6 address.
fn split_host_port(entry: &str) -> (String, u16) {
    if let Some(rest) = entry.strip_prefix('[') {
        if let Some((host, after)) = rest.split_once(']') {
            let port = after.strip_prefix(':').and_then(|p| p.parse().ok()).unwrap_or(3000);
            return (host.to_string(), port);
        }
    }
    if entry.matches(':').count() > 1 {
        return (entry.to_string(), 3000);
    }
    match entry.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().unwrap_or(3000)),
        None => (entry.to_string(), 3000),
    }
}

/// The host of a `host:port` endpoint, brackets removed.
pub fn host_of(endpoint: &str) -> String {
    split_host_port(endpoint).0
}

pub fn port_of(endpoint: &str) -> u16 {
    split_host_port(endpoint).1
}

/// The addresses `name` resolves to.
pub fn resolve(name: &str) -> Vec<String> {
    (name, 0)
        .to_socket_addrs()
        .map(|addrs| addrs.map(|a| a.ip().to_string()).collect())
        .unwrap_or_default()
}

/// Asserts the node's host is the advertised `host:port`, by name or by one
/// of the addresses the name resolves to.
pub fn assert_address(advertised: &str, h: &Host) {
    let name = host_of(advertised);
    let addresses = resolve(&name);
    assert!(
        h.name == name || addresses.contains(&h.name),
        "{} is not {name}{addresses:?}",
        h.name
    );
    assert_eq!(port_of(advertised), h.port);
}

/// The error and its cause chain, as the Java rows print them.
pub fn messages(error: &Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.cause();
    while let Some(e) = cause {
        text.push_str(" <- ");
        text.push_str(&e.to_string());
        cause = e.cause();
    }
    text
}

fn proxy_name(port: u16) -> String {
    t().proxy_ports
        .iter()
        .find(|(_, p)| *p == port)
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| {
            panic!(
                "DISCOVERY_PROXY_MAP has no proxy for listener {port}: {:?}",
                t().proxy_ports
            )
        })
}

// ---------------------------------------------------------------------------
// Toxiproxy
// ---------------------------------------------------------------------------

/// One request to the Toxiproxy control API: status and body.
async fn toxiproxy(method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let url = &t().toxiproxy_url;
    let authority = url
        .strip_prefix("http://")
        .unwrap_or(url)
        .trim_end_matches('/');
    let (host, port) = split_host_port(authority);
    let request = async {
        let mut stream = TcpStream::connect((host.as_str(), port)).await?;
        let body = body.unwrap_or("");
        let text = format!(
            "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(text.as_bytes()).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok::<_, std::io::Error>(parse_http(&response))
    };
    match tokio::time::timeout(HTTP_TIMEOUT, request).await {
        Ok(Ok(result)) => result,
        Ok(Err(e)) => panic!("Toxiproxy {method} {path}: {e}"),
        Err(_) => panic!("Toxiproxy {method} {path}: timed out"),
    }
}

/// Status and body of an HTTP/1.1 response read to the end of the stream:
/// a content-length body, a chunked body, or whatever followed the headers.
fn parse_http(response: &[u8]) -> (u16, String) {
    let text = String::from_utf8_lossy(response).into_owned();
    let Some((head, body)) = text.split_once("\r\n\r\n") else {
        return (0, text);
    };
    let status = head.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let chunked = head
        .lines()
        .any(|l| l.to_ascii_lowercase().starts_with("transfer-encoding:") && l.contains("chunked"));
    if !chunked {
        return (status, body.to_string());
    }
    let mut decoded = String::new();
    let mut rest = body;
    loop {
        let Some((size, after)) = rest.split_once("\r\n") else {
            break;
        };
        let size = usize::from_str_radix(size.trim().split(';').next().unwrap_or("0"), 16).unwrap_or(0);
        if size == 0 || after.len() < size {
            break;
        }
        decoded.push_str(&after[..size]);
        rest = after[size..].strip_prefix("\r\n").unwrap_or("");
    }
    (status, decoded)
}

/// Enables or disables a Toxiproxy listener.
pub async fn set_listener(proxy: &str, enabled: bool) {
    let (status, body) = toxiproxy(
        "POST",
        &format!("/proxies/{proxy}"),
        Some(&format!("{{\"enabled\":{enabled}}}")),
    )
    .await;
    println!("L2-TOXIPROXY {proxy} enabled={enabled} status={status} {body}");
    assert_eq!(200, status, "{proxy}: {body}");
}

/// Creates a Toxiproxy listener on the first free port above every listener
/// in use and returns that port.
pub async fn add_listener(proxy: &str, upstream: &str) -> u16 {
    let port = next_listener_port().await;
    add_listener_on(proxy, port, upstream).await;
    port
}

/// The first free port above every Toxiproxy listener in use.
pub async fn next_listener_port() -> u16 {
    listener_ports().await.into_iter().max().unwrap_or(0) + 1
}

/// Creates a Toxiproxy listener on `port`.
pub async fn add_listener_on(proxy: &str, port: u16, upstream: &str) {
    let (status, body) = toxiproxy(
        "POST",
        "/proxies",
        Some(&format!(
            "{{\"name\":\"{proxy}\",\"listen\":\"0.0.0.0:{port}\",\"upstream\":\"{upstream}\",\"enabled\":true}}"
        )),
    )
    .await;
    println!("L2-TOXIPROXY add {proxy} status={status} {body}");
    assert_eq!(201, status, "{proxy}: {body}");
}

pub async fn remove_listener(proxy: &str) {
    let (status, body) = toxiproxy("DELETE", &format!("/proxies/{proxy}"), None).await;
    println!("L2-TOXIPROXY remove {proxy} status={status}");
    assert_eq!(204, status, "{proxy}: {body}");
}

/// Adds a toxic to a Toxiproxy listener. `attributes` is the JSON object of
/// the toxic's attributes.
pub async fn add_toxic(proxy: &str, name: &str, kind: &str, stream: &str, toxicity: f64, attributes: &str) {
    let (status, body) = toxiproxy(
        "POST",
        &format!("/proxies/{proxy}/toxics"),
        Some(&format!(
            "{{\"name\":\"{name}\",\"type\":\"{kind}\",\"stream\":\"{stream}\",\"toxicity\":{toxicity},\"attributes\":{attributes}}}"
        )),
    )
    .await;
    println!("L2-TOXIPROXY toxic add {proxy} {name} status={status} {body}");
    assert_eq!(200, status, "{proxy}: {body}");
}

pub async fn remove_toxic(proxy: &str, name: &str) {
    let (status, body) = toxiproxy("DELETE", &format!("/proxies/{proxy}/toxics/{name}"), None).await;
    println!("L2-TOXIPROXY toxic remove {proxy} {name} status={status}");
    assert_eq!(204, status, "{proxy}: {body}");
}

/// The toxics of a Toxiproxy listener, as the control API returns them.
pub async fn toxics(proxy: &str) -> String {
    let (status, body) = toxiproxy("GET", &format!("/proxies/{proxy}/toxics"), None).await;
    assert_eq!(200, status, "{proxy}: {body}");
    body
}

/// The port a Toxiproxy listener forwards to.
pub async fn upstream_port(proxy: &str) -> u16 {
    let (status, body) = toxiproxy("GET", &format!("/proxies/{proxy}"), None).await;
    assert_eq!(200, status, "{proxy}: {body}");
    json_ports(&body, "\"upstream\":\"")
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("No upstream port for {proxy}: {body}"))
}

/// Every Toxiproxy listener port in use.
pub async fn listener_ports() -> HashSet<u16> {
    let (status, body) = toxiproxy("GET", "/proxies", None).await;
    assert_eq!(200, status, "{body}");
    let mut set: HashSet<u16> = t().proxy_ports.iter().map(|(_, p)| *p).collect();
    set.extend(json_ports(&body, "\"listen\":\""));
    set
}

/// The ports of every `"<key>":"host:port"` value in a JSON text.
fn json_ports(json: &str, key: &str) -> Vec<u16> {
    json.match_indices(key)
        .filter_map(|(i, _)| {
            let rest = &json[i + key.len()..];
            let value = rest.split('"').next()?;
            value.rsplit_once(':')?.1.parse().ok()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Node control
// ---------------------------------------------------------------------------

/// Asks the host to act on the server cluster through `DISCOVERY_CONTROL_DIR`
/// and returns its acknowledgement. The request is written as `<id>.request`
/// and the host answers with `<id>.ack`, one `key=value` per line.
pub async fn control(command: &str) -> HashMap<String, String> {
    let dir = PathBuf::from(&t().control_dir);
    let id = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    );
    let tmp = dir.join(format!("{id}.tmp"));
    let ack = dir.join(format!("{id}.ack"));
    std::fs::write(&tmp, format!("{command}\n")).expect("control request written");
    std::fs::rename(&tmp, dir.join(format!("{id}.request"))).expect("control request renamed");
    assert!(
        wait_for(|| ack.exists()).await,
        "No acknowledgement for '{command}' in {}",
        dir.display()
    );
    let mut map = HashMap::new();
    for line in std::fs::read_to_string(&ack).expect("ack readable").lines() {
        if let Some((key, value)) = line.split_once('=') {
            map.insert(key.to_string(), value.to_string());
        }
    }
    let _ = std::fs::remove_file(&ack);
    println!("L2-CONTROL {command} {map:?}");
    assert!(
        !map.contains_key("error"),
        "'{command}' failed: {}",
        map.get("error").cloned().unwrap_or_default()
    );
    map
}

/// Node that joined the client and the time from the peers-generation bump
/// that announced it to the node being active.
pub struct Joined {
    pub node: Arc<Node>,
    pub bump_to_active_ms: u64,
    pub generation: i64,
}

/// Waits for the peers-generation of `observer` to move from `generation`
/// and for an active node not in `names` to appear in the client.
pub async fn wait_joined(client: &Client, observer: &L2Node, generation: i64, names: &[String]) -> Joined {
    let deadline = Instant::now() + Duration::from_millis(WAIT_MS);
    let mut bump: Option<Instant> = None;
    let mut active: Option<(Instant, Arc<Node>)> = None;
    loop {
        let now = Instant::now();
        if bump.is_none() && peers_generation(observer).await != generation {
            bump = Some(now);
        }
        if active.is_none() {
            if let Some(node) = client
                .nodes()
                .into_iter()
                .find(|n| n.is_active() && !names.contains(&n.name().to_string()))
            {
                active = Some((now, node));
            }
        }
        if let (Some(bump), Some((at, node))) = (bump, active.clone()) {
            return Joined {
                node,
                bump_to_active_ms: at.saturating_duration_since(bump).as_millis() as u64,
                generation: peers_generation(observer).await,
            };
        }
        assert!(
            Instant::now() < deadline,
            "No peers-generation bump or no active node after generation {generation}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ---------------------------------------------------------------------------
// Connection statistics
// ---------------------------------------------------------------------------

/// The connection and error counters of one node.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConnStats {
    pub in_use: u64,
    pub in_pool: u64,
    pub opened: u64,
    pub closed: u64,
    pub errors: u64,
    pub timeouts: u64,
}

/// Operational metrics, so command errors are counted.
pub fn metrics_policy() -> aerospike::metrics::MetricsPolicy {
    aerospike::metrics::MetricsPolicy::default().with_operational(true)
}

/// The connection and error counters of every node, by node name. Counters
/// accumulate while metrics are enabled; the pool gauges are live.
pub fn connection_stats(client: &Client) -> BTreeMap<String, ConnStats> {
    let metrics = client.metrics();
    client
        .nodes()
        .iter()
        .map(|node| {
            let gauges = node.pool_gauges();
            let counters = metrics
                .nodes
                .get(&node.host().to_string())
                .map(|s| s.counters.clone())
                .unwrap_or_default();
            (
                node.name().to_string(),
                ConnStats {
                    in_use: gauges.total.saturating_sub(gauges.in_pool),
                    in_pool: gauges.in_pool,
                    opened: counters.connections_successful,
                    closed: counters.connections_closed,
                    errors: counters.transaction_error_count,
                    timeouts: counters.connections_timeout_errors,
                },
            )
        })
        .collect()
}

pub fn format_stats(stats: &BTreeMap<String, ConnStats>) -> String {
    stats
        .iter()
        .map(|(name, s)| {
            format!(
                "{name}{{inUse={},inPool={},opened={},closed={},errors={},timeouts={}}}",
                s.in_use, s.in_pool, s.opened, s.closed, s.errors, s.timeouts
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

// ---------------------------------------------------------------------------
// Restoring the topology after a row
// ---------------------------------------------------------------------------

/// What a row changed and `restore_all` undoes, in order, after the row.
pub enum Restore {
    EnableListener(String),
    RemoveListener(String),
    StopNode(String),
    RemoveToxic(String, String),
}

static RESTORE: Mutex<Vec<Restore>> = Mutex::new(Vec::new());
static CLIENTS: Mutex<Vec<Client>> = Mutex::new(Vec::new());

/// Remembers a client the row opened, so it is closed after the row even
/// when the row panics before closing it; a leaked client keeps tending and
/// logging into the next row's capture.
pub fn register_client(client: &Client) {
    CLIENTS.lock().unwrap().push(client.clone());
}

/// Registers something to undo after the row, whether it passes or panics.
pub fn restore_later(restore: Restore) {
    RESTORE.lock().unwrap().push(restore);
}

/// Drops a registered restoration the row has already undone itself.
pub fn restored(matches: impl Fn(&Restore) -> bool) {
    RESTORE.lock().unwrap().retain(|r| !matches(r));
}

pub async fn restore_all() {
    let clients: Vec<Client> = std::mem::take(&mut *CLIENTS.lock().unwrap());
    for client in clients {
        if client.is_connected() {
            let _ = client.close().await;
        }
    }
    let pending: Vec<Restore> = std::mem::take(&mut *RESTORE.lock().unwrap());
    let mut stopped = false;
    for restore in pending {
        match restore {
            Restore::EnableListener(proxy) => set_listener(&proxy, true).await,
            Restore::RemoveListener(proxy) => remove_listener(&proxy).await,
            Restore::StopNode(container) => {
                control(&format!("stop {container}")).await;
                stopped = true;
            }
            Restore::RemoveToxic(proxy, name) => remove_toxic(&proxy, &name).await,
        }
    }
    if stopped {
        wait_cluster_stable(nodes().await.len()).await;
    }
}
