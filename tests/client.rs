// Copyright 2015-2018 Aerospike, Inc.
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

extern crate env_logger;
#[macro_use]
extern crate lazy_static;
extern crate rand;
#[cfg(feature = "tls")]
extern crate webpki_roots;

use aerospike::Bins;
use aerospike::Client;
use aerospike_rt::time::Duration;

mod common;

#[aerospike_macro::test]
async fn cluster_name() {
    let policy = &mut common::client_policy().clone();
    policy.cluster_name = Some(String::from("notTheRealClusterName"));
    let Err(err) = Client::new(policy, &common::hosts()).await else {
        panic!("expected cluster name mismatch");
    };
    let msg = err.to_string();
    assert!(
        msg.contains("Cluster name mismatch"),
        "expected cluster name mismatch error, got: {msg}"
    );
    assert!(
        msg.contains("expected=notTheRealClusterName"),
        "expected wrong cluster name in error, got: {msg}"
    );
}

#[aerospike_macro::test]
async fn node_names() {
    let client = common::client().await;
    let names = client.node_names();
    assert!(!names.is_empty());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn nodes() {
    let client = common::client().await;
    let nodes = client.nodes();
    assert!(!nodes.is_empty());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn get_node() {
    let client = common::client().await;
    for name in client.node_names() {
        let node = client.get_node(&name);
        assert!(node.is_ok());
    }
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn close() {
    let client = Client::new(common::client_policy(), &common::hosts())
        .await
        .unwrap();
    assert_eq!(client.is_connected(), true, "The client is not connected");

    if let Ok(()) = client.close().await {
        assert_eq!(
            client.is_connected(),
            false,
            "The client did not disconnect"
        );
    } else {
        assert!(false, "Failed to close client");
    }
}

#[cfg(feature = "tls")]
#[aerospike_macro::test]
async fn tls_client_no_auth() {
    if common::no_tls() {
        return;
    }

    let policy = &mut common::client_policy().clone();
    policy.tls_config = Some(common::tls_config_no_client_auth());
    let client = Client::new(policy, &common::hosts()).await.unwrap();
    let names = client.node_names();
    assert!(!names.is_empty());
    client.close().await.unwrap();
}

#[cfg(feature = "tls")]
#[aerospike_macro::test]
async fn tls_client_auth() {
    if common::no_tls() {
        return;
    }

    let policy = &mut common::client_policy().clone();
    policy.tls_config = Some(common::tls_config());
    let client = Client::new(policy, &common::hosts()).await.unwrap();
    let names = client.node_names();
    assert!(!names.is_empty());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn test_close_does_not_stop_tend_thread() {
    let client = common::client().await;
    let namespace = common::namespace();

    let key = aerospike::as_key!(namespace, "close_bug_test", "key1");
    let bin = aerospike::as_bin!("val", 42);
    client
        .put(&aerospike::WritePolicy::default(), &key, &[bin])
        .await
        .unwrap();

    let nodes_before = client.node_names();
    assert!(!nodes_before.is_empty(), "Should have nodes before close");

    client.close().await.unwrap();
    assert!(
        !client.is_connected(),
        "Expected is_connected()=false after close"
    );

    // After close(), the tend thread performs the node/alias cleanup as its
    // last act. With the close-signal select it normally lands within
    // milliseconds, but an in-flight tend cycle (slow under parallel suite
    // load on a large cluster) may delay it — so poll for the eventual
    // cleanup instead of assuming a fixed window.
    let deadline = aerospike_rt::time::Instant::now() + Duration::from_secs(15);
    loop {
        if client.node_names().is_empty() {
            break;
        }
        assert!(
            aerospike_rt::time::Instant::now() < deadline,
            "Nodes were NOT cleared within 15s of close()"
        );
        aerospike_rt::sleep(Duration::from_millis(50)).await;
    }

    let get_result = client
        .get(&aerospike::ReadPolicy::default(), &key, Bins::All)
        .await;
    assert!(get_result.is_err(), "get should have failed",);

    let key2 = aerospike::as_key!(namespace, "close_bug_test", "key2");
    let bin2 = aerospike::as_bin!("val", 99);
    let put_result = client
        .put(&aerospike::WritePolicy::default(), &key2, &[bin2])
        .await;
    assert!(put_result.is_err(), "put should have failed",);

    let delete_result = client
        .delete(&aerospike::WritePolicy::default(), &key2)
        .await;
    assert!(delete_result.is_err(), "delete should have failed",);
}
