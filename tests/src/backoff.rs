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

//! Tests for the per-node circuit breaker introduced via `max_error_rate` /
//! `error_rate_window` on `ClientPolicy`. The breaker logic itself is pure
//! state on `Node`, so most coverage comes from driving `Node`'s public
//! methods directly. The final test exercises the integration with the
//! single-command pipeline.

use aerospike::{as_bin, as_key, Client, Error, WritePolicy};

use crate::common;

/// Build a client whose breaker is configured per the test's needs and
/// whose error-rate window is large enough that no automatic reset can
/// fire mid-test.
async fn breaker_client(max_error_rate: usize) -> Client {
    let mut policy = common::client_policy().clone();
    policy.max_error_rate = max_error_rate;
    // Default tend interval is 1s; pick a window so large no reset fires
    // for the lifetime of any test in this module.
    policy.error_rate_window = 10_000;
    Client::new(&policy, &common::hosts().to_string())
        .await
        .expect("failed to connect cluster for breaker test")
}

#[aerospike_macro::test]
async fn breaker_disabled_passes_through() {
    let client = breaker_client(0).await;
    let nodes = client.cluster.nodes();
    let node = nodes
        .first()
        .expect("cluster should have at least one node");

    // With the breaker disabled (max_error_rate == 0) increments are
    // explicitly no-ops — nothing else in the system can cause the
    // counter to advance.
    for _ in 0..1000 {
        node.incr_error_rate();
    }
    assert_eq!(node.error_rate_count(), 0);
    assert!(node.error_rate_within_limit());
    assert!(node.validate_error_count().is_ok());

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn breaker_within_limit_at_threshold() {
    let client = breaker_client(5).await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    // Up to and *including* the cluster threshold is still valid.
    // Mirrors Java's `errorRateWithinLimit`: `count <= cluster.maxErrorRate`.
    for _ in 0..5 {
        node.incr_error_rate();
    }
    assert_eq!(node.error_rate_count(), 5);
    assert!(node.error_rate_within_limit());
    assert!(node.validate_error_count().is_ok());

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn breaker_trips_when_exceeded() {
    let client = breaker_client(3).await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    // 4 > 3 → breaker open.
    for _ in 0..4 {
        node.incr_error_rate();
    }
    assert!(!node.error_rate_within_limit());

    let err = node.validate_error_count().unwrap_err();
    assert!(
        matches!(err, Error::MaxErrorRate(_)),
        "expected Error::MaxErrorRate, got: {err:?}"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn reset_clean_window_doubles_ceiling() {
    let client = breaker_client(8).await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    // Fresh node: per-node ceiling matches the cluster setting.
    assert_eq!(node.node_max_error_rate(), 8);

    // Burn the ceiling down by tripping a window.
    for _ in 0..9 {
        node.incr_error_rate();
    }
    node.reset_error_rate();
    assert_eq!(node.node_max_error_rate(), 4);

    // Clean window → ceiling doubles back toward the cap.
    node.reset_error_rate();
    assert_eq!(node.node_max_error_rate(), 8);

    // Already at cluster cap → stays put (no overshoot).
    node.reset_error_rate();
    assert_eq!(node.node_max_error_rate(), 8);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn reset_breached_window_halves_ceiling() {
    let client = breaker_client(8).await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    assert_eq!(node.node_max_error_rate(), 8);

    // 9 > 8 → halves to 4.
    for _ in 0..9 {
        node.incr_error_rate();
    }
    node.reset_error_rate();
    assert_eq!(node.node_max_error_rate(), 4);

    // 5 > 4 → halves to 2.
    for _ in 0..5 {
        node.incr_error_rate();
    }
    node.reset_error_rate();
    assert_eq!(node.node_max_error_rate(), 2);

    // 3 > 2 → halves to 1.
    for _ in 0..3 {
        node.incr_error_rate();
    }
    node.reset_error_rate();
    assert_eq!(node.node_max_error_rate(), 1);

    // 2 > 1 → floor at 1 (Java's `else { maxErrorRate = 1; }`).
    for _ in 0..2 {
        node.incr_error_rate();
    }
    node.reset_error_rate();
    assert_eq!(node.node_max_error_rate(), 1);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn reset_clears_count() {
    let client = breaker_client(5).await;
    let nodes = client.cluster.nodes();
    let node = nodes.first().unwrap();

    for _ in 0..3 {
        node.incr_error_rate();
    }
    assert_eq!(node.error_rate_count(), 3);

    node.reset_error_rate();
    assert_eq!(node.error_rate_count(), 0);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn pipeline_returns_max_error_rate_when_breaker_open() {
    // End-to-end: trip the breaker on every node, then issue a real
    // command and confirm the error chain surfaces `Error::MaxErrorRate`.
    let client = breaker_client(1).await;

    for node in client.cluster.nodes() {
        // Push well above the cluster cap so even if the partition
        // tracker rotates between nodes (multi-node setup) every
        // candidate is tripped.
        for _ in 0..16 {
            node.incr_error_rate();
        }
    }

    // Bound the retry loop so the test doesn't depend on `total_timeout`
    // expiring while the breaker keeps re-tripping.
    let mut wp = WritePolicy::default();
    wp.base_policy.max_retries = 2;
    wp.base_policy.total_timeout = 500;
    wp.base_policy.socket_timeout = 500;

    let key = as_key!(common::namespace(), "breaker_test", 1_i64);
    let bin = as_bin!("a", 1);
    let result = client.put(&wp, &key, &[bin]).await;

    let err = result.expect_err("put should fail with the breaker open");
    let display = format!("{err}");
    assert!(
        display.contains("Max error rate exceeded"),
        "expected MaxErrorRate in error chain, got: {display}"
    );

    client.close().await.unwrap();
}
