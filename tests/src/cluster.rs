// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

//! Integration coverage for the cluster-management changes:
//! - long-lived `tendConnection` per node and its lazy-open / reuse
//! - `Node::cached_hostname` / `cache_hostname` first-writer-wins cache
//! - per-tend flag round-trips (`partition_changed`, `rebalance_changed`,
//!   `reset_reference_count`)
//! - peers / partition generation commit accessors
//! - rack-ids parser (Java parity: `<= 0 || >= 32` chars rejected)

use std::collections::HashSet;

use aerospike::policy::AdminPolicy;
use aerospike::{Client, Error};

use crate::common;

/// A fresh `Client` with each test's own cluster state. Avoids interference
/// from the shared singleton when we mutate per-node breaker / cache state.
async fn fresh_client() -> Client {
    let policy = common::client_policy().clone();
    Client::new(&policy, &common::hosts().to_string())
        .await
        .expect("failed to connect cluster for cluster test")
}

// ---- tend connection -----------------------------------------------------

#[aerospike_macro::test]
async fn tend_info_two_calls_succeed() {
    // Sanity: the long-lived tend socket is reusable. Two consecutive
    // `tend_info` calls must both succeed against the same node.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes
        .first()
        .expect("cluster should have at least one node");
    let policy = AdminPolicy::default();

    let m1 = node.tend_info(&policy, &["node"]).await.unwrap();
    let m2 = node.tend_info(&policy, &["node"]).await.unwrap();

    let n1 = m1
        .get("node")
        .expect("first response must include node name");
    let n2 = m2
        .get("node")
        .expect("second response must include node name");
    // Same socket → same logical node identity on both responses.
    assert_eq!(n1, n2);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn close_tend_connection_reopens_on_next_call() {
    // Tearing the tend socket down forces the next call to reopen and
    // re-authenticate. The second call must still succeed.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();
    let policy = AdminPolicy::default();

    let _first = node.tend_info(&policy, &["node"]).await.unwrap();
    node.close_tend_connection().await;

    let second = node.tend_info(&policy, &["node"]).await.unwrap();
    assert!(second.contains_key("node"));

    client.close().await.unwrap();
}

// ---- hostname cache (peer_exists fast path) ------------------------------

#[aerospike_macro::test]
async fn hostname_cache_first_writer_wins() {
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    // Fresh node: cache empty.
    assert!(node.cached_hostname().is_none());

    node.cache_hostname("primary.example.com".to_string());
    assert_eq!(node.cached_hostname(), Some("primary.example.com"));

    // Second writer is silently ignored — Java's `node.hostname = h.name`
    // is also effectively first-writer-wins because tend serialization
    // never overwrites a hit.
    node.cache_hostname("other.example.com".to_string());
    assert_eq!(node.cached_hostname(), Some("primary.example.com"));

    client.close().await.unwrap();
}

// ---- per-tend node flags -------------------------------------------------

#[aerospike_macro::test]
async fn partition_changed_flag_round_trip() {
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    // Tend may or may not have flipped `partition_changed` already; force
    // it false and exercise the round-trip from there.
    node.set_partition_changed(false);
    assert!(!node.partition_changed());

    node.set_partition_changed(true);
    assert!(node.partition_changed());

    node.set_partition_changed(false);
    assert!(!node.partition_changed());

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn rebalance_changed_flag_round_trip() {
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    node.set_rebalance_changed(false);
    assert!(!node.rebalance_changed());

    node.set_rebalance_changed(true);
    assert!(node.rebalance_changed());

    node.set_rebalance_changed(false);
    assert!(!node.rebalance_changed());

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn reference_count_increment_and_reset() {
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    node.reset_reference_count();
    assert_eq!(node.reference_count(), 0);

    node.increment_reference_count();
    node.increment_reference_count();
    node.increment_reference_count();
    assert_eq!(node.reference_count(), 3);

    node.reset_reference_count();
    assert_eq!(node.reference_count(), 0);

    client.close().await.unwrap();
}

// ---- generation commits --------------------------------------------------

#[aerospike_macro::test]
async fn commit_peers_generation_updates_state() {
    // Mirrors the seed-flow `peersValidated → peersGeneration = parser.generation`
    // commit point: writing through `commit_peers_generation` must be
    // observable via the `peers_generation()` getter.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    node.commit_peers_generation(42);
    assert_eq!(node.peers_generation(), 42);

    node.commit_peers_generation(7);
    assert_eq!(node.peers_generation(), 7);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn set_partition_generation_updates_state() {
    // Same property for partition generation. The partition tokenizer
    // commits via this setter once the bitmap parses successfully —
    // without this commit the next tend would re-fetch the map needlessly.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    node.set_partition_generation(123);
    assert_eq!(node.partition_generation(), 123);

    // The ownership-transfer path resets to -1 on a previous owner so
    // it picks the new map up next tend; the setter must accept that too.
    node.set_partition_generation(-1);
    assert_eq!(node.partition_generation(), -1);

    client.close().await.unwrap();
}

// ---- rack parsing --------------------------------------------------------

#[aerospike_macro::test]
async fn rack_parse_valid_format() {
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    node.parse_rack("ns_a:1;ns_b:2").unwrap();

    let mut want_rack_1 = HashSet::new();
    want_rack_1.insert(1usize);
    let mut want_rack_2 = HashSet::new();
    want_rack_2.insert(2usize);

    assert!(node.is_in_rack("ns_a", &want_rack_1));
    assert!(!node.is_in_rack("ns_a", &want_rack_2));
    assert!(node.is_in_rack("ns_b", &want_rack_2));
    assert!(!node.is_in_rack("ns_b", &want_rack_1));
    // Unknown namespace → false.
    assert!(!node.is_in_rack("unknown", &want_rack_1));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn rack_parse_trailing_semicolon_is_ignored() {
    // `<ns>:<rack>;` with the trailing `;` is what the server emits;
    // the empty fragment after the final `;` must be filtered out, not
    // rejected as a parse error.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    node.parse_rack("ns_a:1;").unwrap();

    let mut want = HashSet::new();
    want.insert(1usize);
    assert!(node.is_in_rack("ns_a", &want));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn rack_parse_empty_namespace_rejected() {
    // Java's RackParser rejects namespace length `<= 0`. Ours mirrors that.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    let err = node.parse_rack(":3").unwrap_err();
    assert!(
        matches!(err, Error::BadResponse(_)),
        "expected BadResponse, got: {err:?}"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn rack_parse_namespace_length_boundary() {
    // Java's boundary: `>= 32` chars rejected, `< 32` accepted.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    // 31 chars → accepted.
    let ns_31 = "a".repeat(31);
    node.parse_rack(&format!("{ns_31}:1")).unwrap();

    // 32 chars → rejected.
    let ns_32 = "a".repeat(32);
    let err = node.parse_rack(&format!("{ns_32}:1")).unwrap_err();
    assert!(matches!(err, Error::BadResponse(_)));

    // 64 chars → rejected.
    let ns_64 = "a".repeat(64);
    let err = node.parse_rack(&format!("{ns_64}:1")).unwrap_err();
    assert!(matches!(err, Error::BadResponse(_)));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn rack_parse_invalid_entry_rejected() {
    // No `:` at all → Invalid rack entry.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    let err = node.parse_rack("ns_no_colon").unwrap_err();
    assert!(matches!(err, Error::BadResponse(_)));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn rack_parse_replace_table() {
    // Re-parsing replaces the rack table wholesale — old entries don't
    // bleed through.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    node.parse_rack("ns_a:1;ns_b:2").unwrap();
    let mut want_1 = HashSet::new();
    want_1.insert(1usize);
    assert!(node.is_in_rack("ns_a", &want_1));

    // Second parse: only `ns_c` survives.
    node.parse_rack("ns_c:9").unwrap();
    let mut want_9 = HashSet::new();
    want_9.insert(9usize);
    assert!(node.is_in_rack("ns_c", &want_9));
    assert!(!node.is_in_rack("ns_a", &want_1));
    assert!(!node.is_in_rack("ns_b", &want_1));

    client.close().await.unwrap();
}

// ---- cluster topology smoke test -----------------------------------------

#[aerospike_macro::test]
async fn cluster_has_at_least_one_node_after_seed() {
    // Sanity: the seed flow rewritten today (`Cluster::seed_nodes` now
    // builds the seed Node, fetches `peers-…`, materializes peers with
    // multi-host fallback) must still reach steady state with at least
    // one live node.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    assert!(
        !nodes.is_empty(),
        "expected at least one node after seed; got {}",
        nodes.len()
    );
    for node in &nodes {
        assert!(node.is_active(), "node {node} should be active after seed");
        // Per #12: the seed flow now commits each parsed peer-generation,
        // so a freshly-seeded node should have a non-`-1` generation
        // (the server's reported value).
        assert_ne!(
            node.peers_generation(),
            -1,
            "seed node {node} should have a committed peers_generation"
        );
    }

    client.close().await.unwrap();
}
