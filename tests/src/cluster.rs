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

use aerospike::policy::AdminPolicy;
use aerospike::Client;

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

    assert!(node.has_rack("ns_a", 1));
    assert!(!node.has_rack("ns_a", 2));
    assert!(node.has_rack("ns_b", 2));
    assert!(!node.has_rack("ns_b", 1));
    // Unknown namespace → false.
    assert!(!node.has_rack("unknown", 1));

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

    assert!(node.has_rack("ns_a", 1));

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
        matches!(err.kind(), aerospike::ErrorKind::BadResponse),
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
    assert!(matches!(err.kind(), aerospike::ErrorKind::BadResponse));

    // 64 chars → rejected.
    let ns_64 = "a".repeat(64);
    let err = node.parse_rack(&format!("{ns_64}:1")).unwrap_err();
    assert!(matches!(err.kind(), aerospike::ErrorKind::BadResponse));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn rack_parse_invalid_entry_rejected() {
    // No `:` at all → Invalid rack entry.
    let client = fresh_client().await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    let err = node.parse_rack("ns_no_colon").unwrap_err();
    assert!(matches!(err.kind(), aerospike::ErrorKind::BadResponse));

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
    assert!(node.has_rack("ns_a", 1));

    // Second parse: only `ns_c` survives.
    node.parse_rack("ns_c:9").unwrap();
    assert!(node.has_rack("ns_c", 9));
    assert!(!node.has_rack("ns_a", 1));
    assert!(!node.has_rack("ns_b", 1));

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

// ---- seed-only cluster --------------------------------------------------

#[aerospike_macro::test]
async fn seed_only_cluster_pins_to_seed_addresses() {
    // With `seed_only_cluster=true` the cluster view is restricted to
    // the seeds the client was started with — peer discovery is
    // disabled, so a one-seed start against a multi-node cluster
    // ends up with exactly one node, and a tend cycle later still
    // shows exactly one node.
    let mut policy = common::client_policy().clone();
    policy.seed_only_cluster = true;

    // Use only the first seed from the configured list (drop everything
    // after a comma if present). A single seed is enough to prove that
    // peer discovery is suppressed.
    let hosts_string = common::hosts().to_string();
    let single_seed = hosts_string
        .split(',')
        .next()
        .unwrap_or(&hosts_string)
        .to_string();

    let client = Client::new(&policy, &single_seed)
        .await
        .expect("connect with seed_only_cluster");

    let initial = client.cluster.nodes().len();
    assert_eq!(
        initial, 1,
        "seed_only_cluster init should add exactly the seed, got {}",
        initial
    );

    // Give a tend cycle a chance to run; without seed_only_cluster
    // peers discovery would have enrolled additional nodes by now.
    aerospike_rt::sleep(std::time::Duration::from_millis(2_500)).await;

    let after_tend = client.cluster.nodes();
    assert_eq!(
        after_tend.len(),
        1,
        "seed_only_cluster post-tend node count drifted to {} ({:?})",
        after_tend.len(),
        after_tend
            .iter()
            .map(|n| format!("{n}"))
            .collect::<Vec<_>>()
    );

    client.close().await.unwrap();
}

// ---- rack-aware read routing ----------------------------------------------

#[aerospike_macro::test]
async fn prefer_rack_read_routing() {
    use aerospike::policy::Replica;
    use aerospike::{as_bin, as_key, as_val, Bins, ReadPolicy, WritePolicy};

    // Ordered rack preference list: rack 7 (nowhere) is preferred over the
    // server's actual rack. Whatever racks the server reports, a
    // PreferRack read must route successfully — preferred-rack match or
    // the different-rack/fallback tiers.
    let mut policy = common::client_policy().clone();
    policy.rack_ids = Some(vec![7, 0]);
    let client = Client::new(&policy, &common::hosts().to_string())
        .await
        .expect("connect with rack_ids");

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "rack_read");

    client
        .put(&WritePolicy::default(), &key, &[as_bin!("a", 1)])
        .await
        .unwrap();

    let mut rpolicy = ReadPolicy::default();
    rpolicy.replica = Replica::PreferRack;
    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    assert_eq!(rec.bins.get("a"), Some(&as_val!(1)));

    // Batch under PreferRack: reads route rack-preferred, writes must
    // route via the write-side logic (never to a rack replica). Mixed
    // batch with >1 key per node exercises the grouped (non-fast-path)
    // node split.
    use aerospike::{BatchOperation, BatchPolicy, BatchReadPolicy, BatchWritePolicy, Bins as B};
    let mut bpolicy = BatchPolicy::default();
    bpolicy.replica = Replica::PreferRack;
    let wkey = as_key!(namespace, &set_name, "rack_batch_write");
    let batch = vec![
        BatchOperation::write(
            &BatchWritePolicy::default(),
            wkey.clone(),
            vec![aerospike::operations::put(&as_bin!("b", 2))],
        ),
        BatchOperation::read(&BatchReadPolicy::default(), key.clone(), B::All),
        BatchOperation::read(&BatchReadPolicy::default(), wkey.clone(), B::All),
    ];
    let results = client.batch(&bpolicy, &batch).await.unwrap();
    assert_eq!(results.len(), 3);
    let rec = client.get(&rpolicy, &wkey, Bins::All).await.unwrap();
    assert_eq!(rec.bins.get("b"), Some(&as_val!(2)));

    client.close().await.unwrap();
}

// ---- client version --------------------------------------------------------

#[aerospike_macro::test]
async fn client_version_reports_crate_version() {
    let version = Client::client_version();
    assert!(!version.is_empty());
    // Semver-ish: starts with a digit and contains a dot.
    assert!(version.as_bytes()[0].is_ascii_digit() && version.contains('.'));
}
