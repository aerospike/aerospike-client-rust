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

#[test]
#[should_panic(expected = "Failed to connect to host(s).")]
fn cluster_name() {
    let policy = &mut common::client_policy().clone();
    policy.cluster_name = Some(String::from("notTheRealClusterName"));
    Client::new(policy, &common::hosts()).unwrap();
}

#[test]
fn node_names() {
    let client = common::client();
    let names = client.node_names();
    assert!(!names.is_empty());
}

#[test]
fn nodes() {
    let client = common::client();
    let nodes = client.nodes();
    assert!(!nodes.is_empty());
}

#[test]
fn get_node() {
    let client = common::client();
    for name in client.node_names() {
        let node = client.get_node(&name);
        assert!(node.is_ok());
    }
}

#[test]
fn close() {
    let client = Client::new(common::client_policy(), &common::hosts()).unwrap();
    assert_eq!(client.is_connected(), true);

    if let Ok(()) = client.close() {
        assert_eq!(client.is_connected(), false);
    } else {
        assert!(false, "Failed to close client");
    }
}
