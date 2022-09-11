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

use aerospike::Client;

mod common;

#[aerospike_macro::test]
#[should_panic(expected = "Failed to connect to host(s).")]
async fn cluster_name() {
    let policy = &mut common::client_policy().clone();
    policy.cluster_name = Some(String::from("notTheRealClusterName"));
    Client::new(policy, &common::hosts()).await.unwrap();
}

#[aerospike_macro::test]
async fn node_names() {
    let client = common::client().await;
    let names = client.node_names().await;
    assert!(!names.is_empty());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn nodes() {
    let client = common::client().await;
    let nodes = client.nodes().await;
    assert!(!nodes.is_empty());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn get_node() {
    let client = common::client().await;
    for name in client.node_names().await {
        let node = client.get_node(&name).await;
        assert!(node.is_ok());
    }
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn close() {
    let client = Client::new(common::client_policy(), &common::hosts())
        .await
        .unwrap();
    assert_eq!(
        client.is_connected().await,
        true,
        "The client is not connected"
    );

    if let Ok(()) = client.close().await {
        assert_eq!(
            client.is_connected().await,
            false,
            "The client did not disconnect"
        );
    } else {
        assert!(false, "Failed to close client");
    }
}
