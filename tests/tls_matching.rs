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

//! Server certificate name matching against a TLS-enabled server whose
//! certificate shape is chosen per run by `tests/tls/run_tls_matching.sh`.
//! The case arrives as environment variables:
//!
//! - `TLS_CASE`: case name
//! - `TLS_NAME`: tls name used as the reference identifier
//! - `TLS_EXPECT`: `accept` or `reject`
//! - `TLS_CA`, `TLS_HOST`, `TLS_PORT`, `TLS_NAMESPACE`: CA file and server
//!   address
//! - `TLS_NODES`: the `n6_proxy` case, node name to published TLS port
//!   (`A1=4433,B2=4434`); the first is the seed
//!
//! Without `TLS_CASE` or `TLS_NODES` the tests skip. The Rust client has one
//! connection path, so each case runs once, and it has no legacy CN rule, so
//! a certificate carrying the name only as its CN is rejected.

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use tokio::net::{TcpListener, TcpStream};

use aerospike::discovery::{AddressTranslator, Endpoint};
use aerospike::policy::AdminPolicy;
use aerospike::{Bin, Bins, Client, ClientPolicy, Error, Host, Key, ReadPolicy, TlsPolicy, Value, WritePolicy};

const TIMEOUT_MS: u32 = 5000;
const KEY_COUNT: usize = 50;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn optional(name: &str) -> String {
    env::var(name).unwrap_or_default()
}

fn required(name: &str) -> String {
    let value = optional(name);
    assert!(
        !value.is_empty(),
        "Environment variable {name} is required. Run tests/tls/run_tls_matching.sh"
    );
    value
}

/// A TLS policy trusting the CA file `TLS_CA` names.
fn tls_policy() -> TlsPolicy {
    let ca = required("TLS_CA");
    let mut roots = rustls::RootCertStore::empty();
    roots.add_parsable_certificates(
        CertificateDer::pem_file_iter(&ca)
            .unwrap_or_else(|e| panic!("Cannot open CA file {ca}: {e}"))
            .map(|c| c.expect("CA certificate")),
    );
    TlsPolicy::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

fn client_policy() -> ClientPolicy {
    ClientPolicy {
        tls_policy: Some(tls_policy()),
        timeout: TIMEOUT_MS,
        fail_if_not_connected: true,
        ..ClientPolicy::default()
    }
}

/// The error and its cause chain.
fn messages(error: &Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.cause();
    while let Some(e) = cause {
        text.push_str(" <- ");
        text.push_str(&e.to_string());
        cause = e.cause();
    }
    text
}

fn key(namespace: &str, name: &str) -> Key {
    Key::new(namespace, "tls", Value::from(name)).expect("key")
}

/// Writes the case name under the case key and reads it back.
async fn round_trip(client: &Client, namespace: &str, case: &str) -> Result<bool, Error> {
    let key = key(namespace, case);
    client.put(&WritePolicy::default(), &key, &[Bin::new("v", Value::from(case))]).await?;
    let record = client.get(&ReadPolicy::default(), &key, Bins::All).await?;
    Ok(record.bins.get("v") == Some(&Value::from(case)))
}

#[test]
fn matching() {
    let case = optional("TLS_CASE");
    if case.is_empty() {
        println!("TLS-SKIP: TLS_CASE is not set. Run tests/tls/run_tls_matching.sh");
        return;
    }
    runtime().block_on(async {
        let tls_name = required("TLS_NAME");
        let expect_accept = required("TLS_EXPECT") == "accept";
        let host = env::var("TLS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port: u16 = required("TLS_PORT").parse().expect("TLS_PORT");
        let namespace = env::var("TLS_NAMESPACE").unwrap_or_else(|_| "test".to_string());

        let policy = client_policy();
        let outcome = match Client::new(&policy, &vec![Host::new_tls(&host, &tls_name, port)]).await {
            Ok(client) => {
                let accepted = round_trip(&client, &namespace, &case).await;
                client.close().await.expect("close");
                match accepted {
                    Ok(true) => Ok(()),
                    Ok(false) => Err("record did not round-trip".to_string()),
                    Err(e) => Err(messages(&e)),
                }
            }
            Err(e) => Err(messages(&e)),
        };
        println!(
            "TLS-RESULT case={case} tlsName={tls_name} expected={} outcome={}",
            if expect_accept { "accept" } else { "reject" },
            match &outcome {
                Ok(()) => "accept".to_string(),
                Err(e) => format!("reject[{}]", e.replace(' ', "_")),
            }
        );
        match (expect_accept, outcome) {
            (true, Ok(())) | (false, Err(_)) => {}
            (true, Err(e)) => panic!("expected accept: {e}"),
            (false, Ok(())) => panic!("expected reject, the client connected and wrote a record"),
        }
    });
}

/// TCP pass-through listener on an ephemeral loopback port. Bytes are copied
/// unchanged in both directions, so the node terminates TLS.
struct Forwarder {
    port: u16,
    accepted: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Forwarder {
    const ADDRESS: &'static str = "127.0.0.1";

    async fn start(target_host: String, target_port: u16) -> Forwarder {
        let listener = TcpListener::bind((Self::ADDRESS, 0)).await.expect("forwarder bind");
        let port = listener.local_addr().expect("local addr").port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut client, _)) = listener.accept().await else {
                    break;
                };
                let Ok(mut target) = TcpStream::connect((target_host.as_str(), target_port)).await else {
                    continue;
                };
                count.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut target).await;
                });
            }
        });
        Forwarder { port, accepted, task }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Maps each node, by name, to its forwarder, and tries to set the TLS name
/// to the endpoint address; the client keeps the advertised tls-name.
struct ForwarderTranslator {
    ports: HashMap<String, u16>,
}

impl AddressTranslator for ForwarderTranslator {
    fn translate(&self, advertised: &Endpoint) -> Option<Host> {
        let (name, port) = match self.ports.get(&advertised.node_name) {
            Some(port) => (Forwarder::ADDRESS.to_string(), *port),
            None => (advertised.host.clone(), advertised.port),
        };
        Some(Host::new_tls(&name, Forwarder::ADDRESS, port))
    }
}

/// N-6 through a TCP pass-through listener. A two node cluster is reached
/// only through one local forwarder per node. The second node is never
/// seeded: the client learns it, and its tls-name, from peers-* and reaches
/// it through an address translator that also tries to set the TLS name to
/// the endpoint address. The certificate carries the tls-name only as a
/// dNSName SAN with a non-matching CN, and no iPAddress SAN, so the handshake
/// succeeds only when it validates against the node's tls-name and not the
/// endpoint or the CN.
#[test]
fn through_pass_through_listener() {
    let nodes = optional("TLS_NODES");
    if nodes.is_empty() {
        println!("TLS-SKIP: TLS_NODES is not set. Run tests/tls/run_tls_matching.sh n6_proxy");
        return;
    }
    runtime().block_on(async {
        let tls_name = required("TLS_NAME");
        let host = env::var("TLS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let namespace = env::var("TLS_NAMESPACE").unwrap_or_else(|_| "test".to_string());

        let mut forwarders: HashMap<String, Forwarder> = HashMap::new();
        let mut seed_node = None;
        for entry in nodes.split(',') {
            let (name, port) = entry.split_once('=').expect("name=port");
            if seed_node.is_none() {
                seed_node = Some(name.to_string());
            }
            forwarders.insert(
                name.to_string(),
                Forwarder::start(host.clone(), port.parse().expect("port")).await,
            );
        }
        let seed_node = seed_node.expect("a seed node");

        let mut policy = client_policy();
        policy.address_translator = Some(Arc::new(ForwarderTranslator {
            ports: forwarders.iter().map(|(name, f)| (name.clone(), f.port)).collect(),
        }));
        let seed = Host::new_tls(Forwarder::ADDRESS, &tls_name, forwarders[&seed_node].port);

        let client = Client::new(&policy, &vec![seed])
            .await
            .unwrap_or_else(|e| panic!("client did not connect: {}", messages(&e)));
        let deadline = Instant::now() + Duration::from_millis(u64::from(TIMEOUT_MS));
        while client.nodes().len() != forwarders.len() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(forwarders.len(), client.nodes().len(), "cluster did not discover both nodes");

        for node in client.nodes() {
            let h = node.host();
            let forwarder = forwarders.get(node.name());
            println!(
                "TLS-PROXY node={} host={h} tlsName={:?} forwarderConnections={}",
                node.name(),
                h.tls_name,
                forwarder.map_or(-1, |f| f.accepted() as i64)
            );
            let forwarder = forwarder.unwrap_or_else(|| panic!("unexpected node {}", node.name()));
            assert_eq!(Forwarder::ADDRESS, h.name, "node {} endpoint", node.name());
            assert_eq!(forwarder.port, h.port, "node {} port", node.name());
            assert_eq!(Some(tls_name.clone()), h.tls_name, "node {} tlsName", node.name());
            let info = node.info(&AdminPolicy::default(), &["node"]).await.expect("info");
            assert_eq!(Some(&node.name().to_string()), info.get("node"), "node {} info", node.name());
            assert!(forwarder.accepted() > 0, "no connection through forwarder for {}", node.name());
        }

        for i in 0..KEY_COUNT {
            let key = key(&namespace, &format!("proxy-{i}"));
            client
                .put(&WritePolicy::default(), &key, &[Bin::new("v", Value::from(i as i64))])
                .await
                .unwrap_or_else(|e| panic!("put {i}: {}", messages(&e)));
            let record = client
                .get(&ReadPolicy::default(), &key, Bins::All)
                .await
                .unwrap_or_else(|e| panic!("get {i}: {}", messages(&e)));
            assert_eq!(Some(&Value::from(i as i64)), record.bins.get("v"), "record {i}");
        }
        client.close().await.expect("close");
    });
}
