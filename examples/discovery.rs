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

//! Pluggable seed discovery and address translation.
//!
//! Three things the client can be told about the network between it and the
//! cluster:
//!
//! 1. Where the seed hosts come from: a `SeedCandidateProvider`. Here one
//!    that returns a host list the application keeps, standing in for a
//!    service registry; the built-in `dns` and `aws-tags` providers are set
//!    up the same way.
//! 2. How a server-advertised address becomes the address the client
//!    connects to: an `AddressTranslator`. Here one that logs every
//!    endpoint and passes it through unchanged, then a `port-map` built from
//!    the node names the first client discovered.
//! 3. What a translation miss does: `translation_on_miss`.

use std::env;
use std::sync::Arc;

use aerospike::discovery::{
    AddressTranslator, Endpoint, PortMapAddressTranslator, SeedCandidateContext,
    SeedCandidateProvider, SeedMergePolicy, TranslationMissMode,
};
use aerospike::{Client, ClientPolicy, Host, Result, ToHosts};

#[tokio::main]
async fn main() {
    run().await;
}

/// A seed candidate provider over a host list the application maintains,
/// for example from a service registry callback it already runs. The list
/// can change, so the client re-reads it on the tend task; that is allowed
/// because reading it does no I/O.
struct HostListProvider {
    hosts: std::sync::RwLock<Vec<Host>>,
}

#[async_trait::async_trait]
impl SeedCandidateProvider for HostListProvider {
    async fn refresh_seed_candidates(&self, ctx: &SeedCandidateContext) -> Result<Vec<Host>> {
        println!(
            "provider called at tend iteration {} (configured seeds: {:?})",
            ctx.tend_iteration, ctx.configured_seeds
        );
        Ok(self.hosts.read().unwrap().clone())
    }

    fn needs_periodic_refresh(&self) -> bool {
        true
    }

    fn supports_deadline(&self) -> bool {
        true
    }
}

/// An address translator that logs what the servers advertise and connects
/// to it unchanged. `translate` runs on the tend task, in memory only.
struct LoggingTranslator;

impl AddressTranslator for LoggingTranslator {
    fn translate(&self, advertised: &Endpoint) -> Option<Host> {
        println!(
            "node {} advertises {}:{} in the {:?} list",
            advertised.node_name, advertised.host, advertised.port, advertised.source_list
        );
        Some(advertised.to_host())
    }

    fn on_cluster_view_changed(&self, view_change: u32) {
        println!("cluster view change #{view_change}");
    }
}

/// `host:port`, with an IPv6 host in brackets, as a port-map value.
fn host_port(host: &Host) -> String {
    if host.name.contains(':') {
        format!("[{}]:{}", host.name, host.port)
    } else {
        format!("{}:{}", host.name, host.port)
    }
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let seeds = hosts.to_hosts().expect("AEROSPIKE_HOSTS parses");
    let use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);

    // ---- 1. A seed candidate provider, merged with the static seeds ----
    let provider = Arc::new(HostListProvider {
        hosts: std::sync::RwLock::new(seeds.clone()),
    });
    let mut policy = ClientPolicy {
        use_services_alternate,
        seed_candidate_provider: Some(provider.clone()),
        // Static seeds first, then the discovered ones; duplicates dropped.
        discovery_merge_policy: SeedMergePolicy::Merge,
        // The provider is re-read every 30 s (the default) and must answer
        // within 100 ms (the default) when called from the tend task.
        discovery_refresh_interval: 30_000,
        discovery_tend_refresh_deadline: 100,
        address_translator: Some(Arc::new(LoggingTranslator)),
        translation_on_miss: TranslationMissMode::Strict,
        ..ClientPolicy::default()
    };

    let client = Client::new(&policy, &hosts)
        .await
        .expect("Failed to connect to cluster");
    println!("seed list: {:?}", client.seeds());
    println!("nodes:     {:?}", client.node_names());

    // ---- 2. A port map keyed on node names ----
    // Every node maps to the address it answered at, so the second client
    // connects through the map alone; a node missing from it would be a
    // strict miss and not connected.
    let port_map: Vec<(String, String)> = client
        .nodes()
        .iter()
        .map(|node| (node.name().to_string(), host_port(&node.host())))
        .collect();
    client.close().await.expect("close");
    println!("port map:  {port_map:?}");

    let translator = PortMapAddressTranslator::new(port_map).expect("valid port map");
    policy.seed_candidate_provider = None;
    policy.address_translator = Some(Arc::new(translator));
    policy.translation_on_miss = TranslationMissMode::Strict;

    let client = Client::new(&policy, &hosts)
        .await
        .expect("Failed to connect through the port map");
    println!("nodes through the port map: {:?}", client.node_names());
    client.close().await.expect("close");
}
