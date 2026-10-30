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

//! Seed discovery and address translation, driven against fake nodes that
//! speak just enough of the info protocol for the client to seed, tend and
//! discover peers. No Aerospike server is needed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use aerospike::discovery::{
    AddressTranslator, Endpoint, PassThroughAddressTranslator, PortMapAddressTranslator,
    SeedCandidateContext, SeedCandidateProvider, SeedMergePolicy, TranslationMissMode,
};
use aerospike::{Client, ClientPolicy, ClientResultCode, ErrorKind, Host, Result};

// ---------------------------------------------------------------------------
// A fake node
// ---------------------------------------------------------------------------

/// A TCP server answering info requests from a table of responses.
struct FakeNode {
    addr: SocketAddr,
    responses: Arc<Mutex<HashMap<String, String>>>,
    accepted: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<Vec<String>>>>,
}

fn bitmap(all: bool) -> String {
    BASE64.encode(vec![if all { 0xFFu8 } else { 0 }; 512])
}

impl FakeNode {
    async fn start(name: &str) -> FakeNode {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut table = HashMap::new();
        table.insert("node".to_string(), name.to_string());
        table.insert("build".to_string(), "8.1.0.0".to_string());
        table.insert("cluster-name".to_string(), "null".to_string());
        table.insert("partition-generation".to_string(), "1".to_string());
        table.insert("peers-generation".to_string(), "5".to_string());
        table.insert("rebalance-generation".to_string(), "0".to_string());
        table.insert("peers-clear-std".to_string(), "5,3000,[]".to_string());
        table.insert("peers-clear-alt".to_string(), "5,3000,[]".to_string());
        table.insert("service-clear-std".to_string(), format!("127.0.0.1:{}", addr.port()));
        table.insert("service-clear-alt".to_string(), String::new());
        table.insert("replicas".to_string(), format!("test:0,1,{}", bitmap(true)));
        let responses = Arc::new(Mutex::new(table));
        let accepted = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));

        let (resp, acc, reqs) = (responses.clone(), accepted.clone(), requests.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                acc.fetch_add(1, Ordering::SeqCst);
                let (resp, reqs) = (resp.clone(), reqs.clone());
                tokio::spawn(async move {
                    loop {
                        let mut header = [0u8; 8];
                        if sock.read_exact(&mut header).await.is_err() {
                            return;
                        }
                        let mut len8 = [0u8; 8];
                        len8[2..8].copy_from_slice(&header[2..8]);
                        let len = u64::from_be_bytes(len8) as usize;
                        let mut payload = vec![0u8; len];
                        if sock.read_exact(&mut payload).await.is_err() {
                            return;
                        }
                        let text = String::from_utf8_lossy(&payload).into_owned();
                        let commands: Vec<String> =
                            text.lines().filter(|l| !l.is_empty()).map(str::to_string).collect();
                        reqs.lock().unwrap().push(commands.clone());
                        let mut body = String::new();
                        for command in &commands {
                            let key = command.split(':').next().unwrap_or_default();
                            if let Some(value) = resp.lock().unwrap().get(key) {
                                body.push_str(command);
                                body.push('\t');
                                body.push_str(value);
                                body.push('\n');
                            }
                        }
                        let mut out = Vec::with_capacity(8 + body.len());
                        out.push(2);
                        out.push(1);
                        out.extend_from_slice(&(body.len() as u64).to_be_bytes()[2..8]);
                        out.extend_from_slice(body.as_bytes());
                        if sock.write_all(&out).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        FakeNode {
            addr,
            responses,
            accepted,
            requests,
        }
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }

    fn host(&self) -> Host {
        Host::new("127.0.0.1", self.port())
    }

    fn set(&self, command: &str, value: &str) -> &Self {
        self.responses
            .lock()
            .unwrap()
            .insert(command.to_string(), value.to_string());
        self
    }

    /// Advertise `peers` (the bracketed list) in the standard peers list.
    fn peers(&self, peers: &str) -> &Self {
        self.set("peers-clear-std", &format!("5,3000,{peers}"))
    }

    /// Advertise `peers` in the alternate peers list.
    fn peers_alt(&self, peers: &str) -> &Self {
        self.set("peers-clear-alt", &format!("5,3000,{peers}"))
    }

    /// Give the node its own alternate address, so it is usable under
    /// `use_services_alternate`.
    fn with_alternate(&self) -> &Self {
        self.set("service-clear-alt", &format!("127.0.0.1:{}", self.port()))
    }

    /// Claim no partition, so the seed keeps every master.
    fn claims_no_partitions(&self) -> &Self {
        self.set("replicas", &format!("test:0,1,{}", bitmap(false)))
    }

    /// Change the advertised peers after construction, bumping the peers
    /// generation so the next tend re-reads the list.
    fn change_peers(&self, peers: &str, generation: u32) -> &Self {
        self.peers(peers);
        self.set("peers-generation", &generation.to_string())
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<Vec<String>> {
        self.requests.lock().unwrap().clone()
    }

    fn received(&self, command: &str) -> bool {
        self.requests()
            .iter()
            .any(|r| r.iter().any(|c| c == command))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn policy() -> ClientPolicy {
    ClientPolicy {
        fail_if_not_connected: false,
        timeout: 1000,
        tend_interval: 250,
        ..ClientPolicy::default()
    }
}

async fn connect(policy: &ClientPolicy, seeds: &[&FakeNode]) -> Client {
    let hosts: Vec<Host> = seeds.iter().map(|n| n.host()).collect();
    Client::new(policy, &hosts).await.expect("client connects")
}

fn expect_err(result: Result<Client>) -> aerospike::Error {
    match result {
        Ok(_) => panic!("construction should have failed"),
        Err(e) => e,
    }
}

fn node_names(client: &Client) -> Vec<String> {
    let mut names: Vec<String> = client.nodes().iter().map(|n| n.name().to_string()).collect();
    names.sort();
    names
}

async fn wait_for(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A provider whose answers are scripted per call, recording every context.
struct Scripted {
    answers: Mutex<Vec<Result<Vec<Host>>>>,
    last: Mutex<Option<Result<Vec<Host>>>>,
    contexts: Mutex<Vec<SeedCandidateContext>>,
    calls: AtomicUsize,
    closed: AtomicUsize,
    periodic: bool,
    deadline: bool,
    delay: Duration,
}

impl Scripted {
    fn new(answers: Vec<Result<Vec<Host>>>, periodic: bool) -> Arc<Self> {
        Arc::new(Scripted {
            answers: Mutex::new(answers),
            last: Mutex::new(None),
            contexts: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            closed: AtomicUsize::new(0),
            periodic,
            deadline: true,
            delay: Duration::ZERO,
        })
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

fn clone_result(r: &Result<Vec<Host>>) -> Result<Vec<Host>> {
    match r {
        Ok(hosts) => Ok(hosts.clone()),
        Err(e) => Err(e.clone()),
    }
}

#[async_trait::async_trait]
impl SeedCandidateProvider for Scripted {
    async fn refresh_seed_candidates(&self, ctx: &SeedCandidateContext) -> Result<Vec<Host>> {
        self.contexts.lock().unwrap().push(ctx.clone());
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let mut answers = self.answers.lock().unwrap();
        let answer = if answers.is_empty() {
            self.last.lock().unwrap().as_ref().map_or(Ok(Vec::new()), clone_result)
        } else {
            answers.remove(0)
        };
        *self.last.lock().unwrap() = Some(clone_result(&answer));
        answer
    }
    fn needs_periodic_refresh(&self) -> bool {
        self.periodic
    }
    fn supports_deadline(&self) -> bool {
        self.deadline
    }
    async fn close(&self) -> Result<()> {
        self.closed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A translator that records the view changes it is told about.
struct Recording {
    inner: PassThroughAddressTranslator,
    changes: Mutex<Vec<u32>>,
}

impl AddressTranslator for Recording {
    fn translate(&self, advertised: &Endpoint) -> Option<Host> {
        self.inner.translate(advertised)
    }
    fn on_cluster_view_changed(&self, view_change: u32) {
        self.changes.lock().unwrap().push(view_change);
    }
}

// ---------------------------------------------------------------------------
// Seed candidate providers
// ---------------------------------------------------------------------------

#[aerospike_macro::test]
async fn default_seeds_are_the_configured_hosts() {
    let a = FakeNode::start("BB9A").await;
    let client = connect(&policy(), &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A"]);
    assert_eq!(client.seeds(), vec![a.host()]);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn merge_policies_order_static_and_discovered_seeds_at_init() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    for (merge_policy, expected_seeds, expected_nodes) in [
        (SeedMergePolicy::Merge, vec![a.host(), b.host()], vec!["BB9A"]),
        (SeedMergePolicy::Replace, vec![a.host(), b.host()], vec!["BB9A"]),
        (SeedMergePolicy::DiscoveryOnly, vec![b.host()], vec!["BB9B"]),
    ] {
        let provider = Scripted::new(vec![Ok(vec![b.host()])], false);
        let p = ClientPolicy {
            seed_candidate_provider: Some(provider.clone()),
            discovery_merge_policy: merge_policy,
            ..policy()
        };
        let client = connect(&p, &[&a]).await;
        assert_eq!(client.seeds(), expected_seeds, "{merge_policy:?}");
        assert_eq!(node_names(&client), expected_nodes, "{merge_policy:?}");
        assert_eq!(provider.calls(), 1, "a non-periodic provider is called once");
        let ctx = provider.contexts.lock().unwrap()[0].clone();
        assert_eq!(ctx.tend_iteration, 0);
        assert_eq!(ctx.configured_seeds, vec![a.host()]);
        assert_eq!(ctx.cluster_name, None);
        client.close().await.unwrap();
        wait_for("provider close", || provider.closed.load(Ordering::SeqCst) == 1).await;
    }
}

#[aerospike_macro::test]
async fn provider_failure_at_init_keeps_the_static_seeds_or_fails_without_them() {
    let a = FakeNode::start("BB9A").await;
    let failing = || Scripted::new(vec![Err(aerospike::Error::client_error("registry down"))], false);
    let p = ClientPolicy {
        seed_candidate_provider: Some(failing()),
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A"]);
    client.close().await.unwrap();

    let p = ClientPolicy {
        seed_candidate_provider: Some(failing()),
        ..policy()
    };
    let err = expect_err(Client::new(&p, &Vec::<Host>::new()).await);
    assert!(matches!(err.kind(), ErrorKind::SeedDiscoveryFailed), "{err}");
    assert_eq!(err.result_code(), -27);
    assert_eq!(err.client_result_code(), Some(ClientResultCode::SeedDiscoveryFailed));
    assert!(err.to_string().contains("registry down"), "{err}");

    // An empty result with no static seed is not a discovery failure.
    let p = ClientPolicy {
        seed_candidate_provider: Some(Scripted::new(vec![Ok(vec![])], false)),
        ..policy()
    };
    let client = Client::new(&p, &Vec::<Host>::new()).await.expect("no seeds is not a failure");
    assert!(client.nodes().is_empty());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn periodic_refresh_runs_on_the_tend_and_applies_the_merge_policy() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    let c = Host::new("192.0.2.9", 3000);
    for (merge_policy, expected) in [
        (SeedMergePolicy::Merge, vec![a.host(), c.clone()]),
        (SeedMergePolicy::Replace, vec![c.clone()]),
    ] {
        let provider = Scripted::new(vec![Ok(vec![b.host()]), Ok(vec![c.clone()])], true);
        let p = ClientPolicy {
            seed_candidate_provider: Some(provider.clone()),
            discovery_merge_policy: merge_policy,
            discovery_refresh_interval: 250,
            discovery_tend_refresh_deadline: 100,
            ..policy()
        };
        let client = connect(&p, &[&a]).await;
        assert_eq!(client.seeds(), vec![a.host(), b.host()]);
        wait_for("seed refresh", || client.seeds() == expected).await;
        let contexts = provider.contexts.lock().unwrap().clone();
        assert!(contexts.len() >= 2);
        assert!(contexts[1].tend_iteration > 0);
        client.close().await.unwrap();
    }
}

#[aerospike_macro::test]
async fn three_consecutive_overruns_disable_the_provider_and_restore_init_seeds() {
    let a = FakeNode::start("BB9A").await;
    let b = Host::new("192.0.2.2", 3000);
    let c = Host::new("192.0.2.3", 3000);
    let mut scripted = Scripted::new(vec![Ok(vec![b.clone()]), Ok(vec![c.clone()])], true);
    Arc::get_mut(&mut scripted).unwrap().delay = Duration::from_millis(200);
    let provider = scripted;
    let p = ClientPolicy {
        seed_candidate_provider: Some(provider.clone()),
        discovery_merge_policy: SeedMergePolicy::Replace,
        discovery_refresh_interval: 250,
        discovery_tend_refresh_deadline: 100,
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    let init = client.seeds();
    assert_eq!(init, vec![a.host(), b.clone()]);
    // Three tend refreshes overrun, each cancelled at the deadline, then the
    // provider is disabled: no further call, and the init list is back.
    wait_for("provider tripped", || provider.calls() == 4).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(provider.calls(), 4, "disabled after three overruns");
    assert_eq!(client.seeds(), init);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn discovery_settings_are_validated_at_construction() {
    let a = FakeNode::start("BB9A").await;
    let periodic = || Scripted::new(vec![Ok(vec![])], true);
    let cases: Vec<(ClientPolicy, &str)> = vec![
        (
            ClientPolicy {
                seed_candidate_provider: Some(periodic()),
                discovery_refresh_interval: 100,
                ..policy()
            },
            "Discovery refresh interval 100 must be greater or equal to the tend interval 250",
        ),
        (
            ClientPolicy {
                seed_candidate_provider: Some(periodic()),
                discovery_tend_refresh_deadline: 0,
                ..policy()
            },
            "Invalid discovery tend refresh deadline: 0. Must be > 0 and <= tend interval / 2 (125)",
        ),
        (
            ClientPolicy {
                seed_candidate_provider: Some(periodic()),
                discovery_tend_refresh_deadline: 126,
                ..policy()
            },
            "Invalid discovery tend refresh deadline: 126",
        ),
        (
            ClientPolicy {
                seed_candidate_provider: Some({
                    let mut s = Scripted::new(vec![Ok(vec![])], true);
                    Arc::get_mut(&mut s).unwrap().deadline = false;
                    s
                }),
                ..policy()
            },
            "A seed candidate provider that needs periodic refresh must support a deadline",
        ),
        (
            ClientPolicy {
                seed_candidate_provider: Some(Arc::new(
                    aerospike::discovery::dns::DnsSeedCandidateProvider::builder()
                        .names(["seeds.example.com"])
                        .servers(["127.0.0.1:1"])
                        .timeout(150)
                        .build()
                        .unwrap(),
                )),
                discovery_tend_refresh_deadline: 100,
                ..policy()
            },
            "DNS seed provider timeout 150 must be less or equal to the discovery tend refresh deadline 100",
        ),
    ];
    for (p, expected) in cases {
        let err = expect_err(Client::new(&p, &vec![a.host()]).await);
        assert!(err.to_string().contains(expected), "{err} should contain {expected}");
    }
    // The checks apply only to a provider that needs periodic refresh.
    let p = ClientPolicy {
        seed_candidate_provider: Some(Scripted::new(vec![Ok(vec![])], false)),
        discovery_refresh_interval: 100,
        discovery_tend_refresh_deadline: 0,
        ..policy()
    };
    connect(&p, &[&a]).await.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Address translation
// ---------------------------------------------------------------------------

#[aerospike_macro::test]
async fn strict_miss_drops_the_missed_peer_and_pass_through_keeps_it() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    let c = FakeNode::start("BB9C").await;
    b.claims_no_partitions();
    c.claims_no_partitions();
    a.peers(&format!(
        "[[BB9B,,[10.0.0.2:{}]],[BB9C,,[localhost:{}]]]",
        b.port(),
        c.port()
    ));
    for (on_miss, expected) in [
        (TranslationMissMode::Strict, vec!["BB9A", "BB9B"]),
        (TranslationMissMode::PassThrough, vec!["BB9A", "BB9B", "BB9C"]),
    ] {
        let p = ClientPolicy {
            ip_map: Some(HashMap::from([("10.0.0.2".to_string(), "127.0.0.1".to_string())])),
            translation_on_miss: on_miss,
            ..policy()
        };
        let client = connect(&p, &[&a]).await;
        assert_eq!(node_names(&client), expected, "{on_miss:?}");
        client.close().await.unwrap();
    }
}

#[aerospike_macro::test]
async fn port_map_translates_by_endpoint_and_by_node_name() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    let c = FakeNode::start("BB9C").await;
    b.claims_no_partitions();
    c.claims_no_partitions();
    a.peers("[[BB9B,,[10.0.0.2:3000]],[BB9C,,[10.0.0.3:3000]]]");
    let translator = PortMapAddressTranslator::new([
        ("10.0.0.2:3000".to_string(), format!("127.0.0.1:{}", b.port())),
        ("BB9C".to_string(), format!("127.0.0.1:{}", c.port())),
    ])
    .unwrap();
    let p = ClientPolicy {
        address_translator: Some(Arc::new(translator)),
        translation_on_miss: TranslationMissMode::Strict,
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A", "BB9B", "BB9C"]);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn static_map_collision_in_one_reply_creates_neither() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    a.peers(&format!(
        "[[BB9B,,[10.0.0.2:{0}]],[BB9C,,[10.0.0.3:{0}]]]",
        b.port()
    ));
    let p = ClientPolicy {
        ip_map: Some(HashMap::from([
            ("10.0.0.2".to_string(), "127.0.0.1".to_string()),
            ("10.0.0.3".to_string(), "127.0.0.1".to_string()),
        ])),
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A"]);
    assert_eq!(b.accepted(), 0, "neither colliding peer is connected");
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn distinct_ports_on_a_shared_host_create_every_peer() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    let c = FakeNode::start("BB9C").await;
    b.claims_no_partitions();
    c.claims_no_partitions();
    a.peers(&format!(
        "[[BB9B,,[10.0.0.2:{}]],[BB9C,,[10.0.0.3:{}]]]",
        b.port(),
        c.port()
    ));
    let p = ClientPolicy {
        ip_map: Some(HashMap::from([
            ("10.0.0.2".to_string(), "127.0.0.1".to_string()),
            ("10.0.0.3".to_string(), "127.0.0.1".to_string()),
        ])),
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A", "BB9B", "BB9C"]);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn pass_through_peers_reaching_one_node_create_neither() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    b.claims_no_partitions();
    // Two advertised addresses, one node behind them: neither becomes a node.
    a.peers(&format!(
        "[[BB9B,,[127.0.0.1:{0}]],[BB9N,,[0.0.0.0:{0}]]]",
        b.port()
    ));
    let p = ClientPolicy {
        address_translator: Some(Arc::new(PassThroughAddressTranslator)),
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A"]);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn pass_through_peer_on_an_unadvertised_node_is_created_under_its_name() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    b.claims_no_partitions();
    a.peers(&format!("[[BB9N,,[localhost:{}]]]", b.port()));
    let p = ClientPolicy {
        address_translator: Some(Arc::new(PassThroughAddressTranslator)),
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A", "BB9B"]);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn a_newcomer_colliding_with_a_live_node_is_refused_on_tend() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    b.claims_no_partitions();
    a.peers(&format!("[[BB9B,,[127.0.0.1:{}]]]", b.port()));
    b.peers(&format!("[[BB9A,,[127.0.0.1:{}]]]", a.port()));
    let p = ClientPolicy {
        address_translator: Some(Arc::new(PassThroughAddressTranslator)),
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A", "BB9B"]);
    let accepted = b.accepted();

    // A later reply names a newcomer at B's address. B is live, so the
    // newcomer never becomes a node and B stays.
    a.change_peers(
        &format!("[[BB9B,,[127.0.0.1:{0}]],[BB9P,,[127.0.0.1:{0}]]]", b.port()),
        6,
    );
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(node_names(&client), vec!["BB9A", "BB9B"]);
    assert!(client.nodes().iter().all(|n| n.is_active()));
    assert_eq!(b.accepted(), accepted, "the collision is found before connecting");
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn translator_is_told_about_node_additions_and_address_changes() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    b.claims_no_partitions();
    let translator = Arc::new(Recording {
        inner: PassThroughAddressTranslator,
        changes: Mutex::new(Vec::new()),
    });
    let p = ClientPolicy {
        address_translator: Some(translator.clone()),
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A"]);
    // The view established at construction is not a change.
    assert!(translator.changes.lock().unwrap().is_empty());

    a.change_peers(&format!("[[BB9B,,[127.0.0.1:{}]]]", b.port()), 6);
    wait_for("node B added", || node_names(&client) == vec!["BB9A", "BB9B"]).await;
    wait_for("first view change", || !translator.changes.lock().unwrap().is_empty()).await;
    assert_eq!(translator.changes.lock().unwrap().clone(), vec![1]);

    // The same node advertised at another address is a change too.
    a.change_peers(&format!("[[BB9B,,[0.0.0.0:{}]]]", b.port()), 7);
    wait_for("second view change", || translator.changes.lock().unwrap().len() == 2).await;
    assert_eq!(translator.changes.lock().unwrap().clone(), vec![1, 2]);
    assert_eq!(node_names(&client), vec!["BB9A", "BB9B"]);
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Alternate addresses
// ---------------------------------------------------------------------------

// A seed with no alternate address that names peers in its standard list
// would leave a seed-only cluster that looks healthy: refused.
#[aerospike_macro::test]
async fn a_seed_without_an_alternate_address_fails_construction_when_it_has_peers() {
    let a = FakeNode::start("BB9A").await;
    a.peers("[[BB9B,,[127.0.0.1:1]]]");
    let p = ClientPolicy {
        use_services_alternate: true,
        fail_if_not_connected: true,
        ..policy()
    };
    let err = expect_err(Client::new(&p, &vec![a.host()]).await);
    assert!(matches!(err.kind(), ErrorKind::AlternateAddressMissing), "{err}");
    assert_eq!(err.result_code(), -1);
    let msg = err.to_string();
    for part in ["BB9A", "use_services_alternate", "alternate-access-address", "set use_services_alternate to false"] {
        assert!(msg.contains(part), "{msg} should contain {part}");
    }
    assert!(a.received("service-clear-alt"));
    assert!(a.received("peers-clear-std"));
    assert!(!a.received("service-clear-std"));

    // Without fail_if_not_connected the client starts with no node.
    let p = ClientPolicy {
        use_services_alternate: true,
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert!(client.nodes().is_empty());
    client.close().await.unwrap();
}

// A lone node with no alternate address loses nothing: the client reaches
// it by its seed address and re-seeds when it is gone. It is connected, and
// a peer that appears later with an alternate address is still added.
#[aerospike_macro::test]
async fn a_lone_seed_without_an_alternate_address_is_connected() {
    let a = FakeNode::start("BB9A").await;
    let p = ClientPolicy {
        use_services_alternate: true,
        fail_if_not_connected: true,
        ..policy()
    };
    let client = Client::new(&p, &vec![a.host()]).await.expect("a lone node connects");
    assert_eq!(node_names(&client), vec!["BB9A"]);
    assert!(a.received("service-clear-alt"));
    assert!(a.received("peers-clear-std"));

    let b = FakeNode::start("BB9B").await;
    b.with_alternate().claims_no_partitions();
    a.peers_alt(&format!("[[BB9B,,[127.0.0.1:{}]]]", b.port()));
    a.change_peers(&format!("[[BB9B,,[127.0.0.1:{}]]]", b.port()), 6);
    wait_for("the new peer", || node_names(&client) == ["BB9A", "BB9B"]).await;
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn a_seed_with_an_alternate_address_passes_and_the_flag_off_sends_no_service_command() {
    let a = FakeNode::start("BB9A").await;
    a.with_alternate();
    let p = ClientPolicy {
        use_services_alternate: true,
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A"]);
    client.close().await.unwrap();

    let b = FakeNode::start("BB9B").await;
    let client = connect(&policy(), &[&b]).await;
    assert_eq!(node_names(&client), vec!["BB9B"]);
    // A loopback seed is never a load balancer, so no service command at all.
    assert!(!b.received("service-clear-std"));
    assert!(!b.received("service-clear-alt"));
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn a_peer_omitted_from_the_alternate_list_is_not_added() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    a.with_alternate();
    b.with_alternate().claims_no_partitions();
    a.peers(&format!("[[BB9B,,[127.0.0.1:{}]]]", b.port()));
    a.peers_alt("[]");
    let p = ClientPolicy {
        use_services_alternate: true,
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A"]);
    assert_eq!(b.accepted(), 0);
    assert!(a.received("peers-clear-std"));
    assert!(a.received("peers-clear-alt"));
    client.close().await.unwrap();

    // Listed in the alternate list with no hosts: the same.
    a.peers_alt("[[BB9B,,[]]]");
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A"]);
    assert_eq!(b.accepted(), 0);
    client.close().await.unwrap();

    // Listed with its address: added.
    a.peers_alt(&format!("[[BB9B,,[127.0.0.1:{}]]]", b.port()));
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A", "BB9B"]);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn a_peer_without_its_own_alternate_address_is_not_added() {
    let a = FakeNode::start("BB9A").await;
    let b = FakeNode::start("BB9B").await;
    a.with_alternate();
    b.claims_no_partitions();
    a.peers_alt(&format!("[[BB9B,,[127.0.0.1:{}]]]", b.port()));
    a.peers(&format!("[[BB9B,,[127.0.0.1:{}]]]", b.port()));
    let p = ClientPolicy {
        use_services_alternate: true,
        ..policy()
    };
    let client = connect(&p, &[&a]).await;
    assert_eq!(node_names(&client), vec!["BB9A"]);
    assert!(b.accepted() > 0, "the peer is connected and then refused");
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// The seed's primary host
// ---------------------------------------------------------------------------

// A seed missing from the node's own service list is a load balancer: the
// node is built on the real backend the list names. The seed address that
// answered validation must not be put back in front of it; it was, and the
// node then dialled the load balancer (and, under `for_login_only`, the TLS
// address instead of the cleartext one the same way).
#[aerospike_macro::test]
async fn a_load_balancer_seed_is_replaced_by_the_real_backend() {
    let a = FakeNode::start("BB9A").await;
    // Detection is off for a loopback seed; the IPv4-mapped form reaches the
    // same listener and is not loopback to the check.
    let lb = Host::new("::ffff:127.0.0.1", a.port());
    let client = Client::new(&policy(), &vec![lb]).await.expect("client connects");
    assert_eq!(node_names(&client), vec!["BB9A"]);
    assert!(a.received("service-clear-std"));
    let node = client.nodes().into_iter().next().unwrap();
    assert_eq!(node.host(), a.host());
    assert_eq!(node.address(), a.host().address());
    assert_eq!(node.aliases(), vec![a.host()]);
    client.close().await.unwrap();
}

// The reordering exists for a name that resolves to several addresses: the
// one that answered leads, so the node never dials an unreachable one.
#[aerospike_macro::test]
async fn the_alias_that_answered_leads_a_name_with_several_addresses() {
    let a = FakeNode::start("BB9A").await;
    let client = Client::new(&policy(), &vec![Host::new("localhost", a.port())])
        .await
        .expect("client connects");
    assert_eq!(node_names(&client), vec!["BB9A"]);
    let node = client.nodes().into_iter().next().unwrap();
    assert_eq!(node.host(), a.host());
    let aliases = node.aliases();
    assert_eq!(aliases[0], a.host());
    assert!(aliases.iter().all(|h| h.port == a.port()));
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// YAML configuration
// ---------------------------------------------------------------------------

#[cfg(feature = "dynamic-config")]
mod yaml {
    use super::*;
    use aerospike::config::{ConfigMap, YamlFileProvider};
    use aerospike::discovery::StaticSeedCandidateProvider;
    use std::path::PathBuf;

    fn temp_file(tag: &str, contents: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("aerospike-discovery-{}-{tag}.yaml", std::process::id()));
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[aerospike_macro::test]
    async fn registered_provider_and_translator_are_built_from_the_file_with_options() {
        let a = FakeNode::start("BB9A").await;
        let b = FakeNode::start("BB9B").await;
        b.claims_no_partitions();
        a.peers(&format!("[[BB9B,,[10.0.0.2:{}]]]", b.port()));
        let path = temp_file(
            "registered",
            "version: 1.2.0\nstatic:\n  client:\n    discovery:\n      provider: consul\n      merge_policy: merge\n      service: aerospike\n    translation:\n      provider: nat-table\n      on_miss: strict\n      table:\n        \"10.0.0.2\": \"127.0.0.1\"\n",
        );
        let options: Arc<Mutex<Vec<ConfigMap>>> = Arc::new(Mutex::new(Vec::new()));
        let mut p = policy();
        let seed = a.host();
        let o = options.clone();
        p.register_seed_candidate_provider(
            "consul",
            Arc::new(move |opts| {
                o.lock().unwrap().push(opts.clone());
                Ok(Arc::new(StaticSeedCandidateProvider::new(vec![seed.clone()])))
            }),
        )
        .unwrap();
        let o = options.clone();
        p.register_address_translator(
            "nat-table",
            Arc::new(move |opts| {
                o.lock().unwrap().push(opts.clone());
                let table: HashMap<String, String> = opts
                    .get("table")
                    .and_then(|t| t.as_map())
                    .map(|m| {
                        m.iter()
                            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                            .collect()
                    })
                    .unwrap();
                Ok(Arc::new(aerospike::discovery::StaticMapAddressTranslator::new(Some(table))))
            }),
        )
        .unwrap();
        let provider = Arc::new(YamlFileProvider::new(path.clone()));
        // No seed in code: the registered provider supplies it.
        let client = Client::new_with_config(&p, &Vec::<Host>::new(), provider)
            .await
            .expect("client from yaml");
        assert_eq!(node_names(&client), vec!["BB9A", "BB9B"]);
        {
            let options = options.lock().unwrap();
            assert_eq!(options.len(), 2);
            assert_eq!(options[0].get("service").and_then(|v| v.as_str()), Some("aerospike"));
            assert_eq!(options[1].get("on_miss").and_then(|v| v.as_str()), Some("strict"));
        }
        client.close().await.unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[aerospike_macro::test]
    async fn an_invalid_discovery_setting_ignores_the_whole_file() {
        let a = FakeNode::start("BB9A").await;
        let path = temp_file(
            "invalid",
            "version: 1.2.0\nstatic:\n  client:\n    discovery:\n      provider: consul\n    translation:\n      on_miss: drop\n",
        );
        let called = Arc::new(AtomicUsize::new(0));
        let mut p = policy();
        let c = called.clone();
        p.register_seed_candidate_provider(
            "consul",
            Arc::new(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(StaticSeedCandidateProvider::new(vec![])))
            }),
        )
        .unwrap();
        let provider = Arc::new(YamlFileProvider::new(path.clone()));
        let client = Client::new_with_config(&p, &vec![a.host()], provider)
            .await
            .expect("client starts on the code policy");
        assert_eq!(node_names(&client), vec!["BB9A"]);
        assert_eq!(called.load(Ordering::SeqCst), 0, "nothing from an invalid file is applied");
        client.close().await.unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[aerospike_macro::test]
    async fn a_port_map_from_the_file_shapes_the_first_connection() {
        let a = FakeNode::start("BB9A").await;
        let b = FakeNode::start("BB9B").await;
        b.claims_no_partitions();
        a.peers("[[BB9B,,[10.0.0.2:3000]]]");
        let path = temp_file(
            "port-map",
            &format!(
                "version: 1.2.0\nstatic:\n  client:\n    translation:\n      provider: port-map\n      on_miss: strict\n      port_map:\n        BB9B: \"127.0.0.1:{}\"\n",
                b.port()
            ),
        );
        let provider = Arc::new(YamlFileProvider::new(path.clone()));
        let client = Client::new_with_config(&policy(), &vec![a.host()], provider)
            .await
            .expect("client from yaml");
        assert_eq!(node_names(&client), vec!["BB9A", "BB9B"]);
        client.close().await.unwrap();
        let _ = std::fs::remove_file(path);
    }
}
