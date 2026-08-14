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

//! Cluster initialization behaviour.

use crate::common;

use aerospike::{as_bin, as_key, ClientPolicy, WritePolicy};

/// `Client::new` must not return before the initial partition map is routable.
///
/// Node-count stability alone races the first partition fetch: on a multi-node
/// cluster the nodes materialize in the seed pass but their partition maps land
/// a tend later, so a write issued immediately after construction could die on
/// "partition map empty" before the tend thread filled it in.
#[aerospike_macro::test]
async fn partition_map_ready_when_new_returns() {
    let client = common::client().await;
    let namespace = common::namespace();

    // Every node must have parsed a partition map at least once...
    let nodes = client.cluster.nodes();
    assert!(!nodes.is_empty(), "expected at least one node");
    for node in &nodes {
        assert_ne!(
            node.partition_generation(),
            -1,
            "node {} returned from Client::new without partition data",
            node
        );
    }

    // ...and the map must already route this namespace.
    let owned: usize = nodes
        .iter()
        .map(|n| client.cluster.node_partitions(n, namespace).len())
        .sum();
    assert!(
        owned > 0,
        "partition map must cover `{}` before Client::new returns",
        namespace
    );

    // An immediate write with NO retry budget must succeed — no transient
    // "partition map empty" window is allowed to exist after new().
    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.max_retries = 0;
    let key = as_key!(namespace, "stabilize", "first_write");
    client
        .put(&wpolicy, &key, &[as_bin!("a", 1)])
        .await
        .expect("first write immediately after Client::new must not race the partition map");

    client.close().await.unwrap();
}

/// A seed that nobody answers must still fail fast rather than spinning out the
/// stabilization deadline: with no nodes there is nothing to wait for, and
/// `fail_if_not_connected` decides the outcome.
#[aerospike_macro::test]
async fn unreachable_seed_fails_without_waiting_out_the_deadline() {
    let mut policy = ClientPolicy::default();
    policy.fail_if_not_connected = true;
    // Well above the time an unreachable seed should take, so a regression that
    // waits out the deadline is visible as a slow test rather than a pass.
    policy.timeout = 10_000;

    let started = std::time::Instant::now();
    let outcome = aerospike::Client::new(&policy, &"127.0.0.1:3999".to_string()).await;
    let elapsed = started.elapsed();

    assert!(outcome.is_err(), "an unreachable seed must not connect");
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "gave up after {:?}; a node count stuck at zero should break immediately",
        elapsed
    );
}
