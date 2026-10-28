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

//! CRUD operations using the **sync** (blocking) client.
//!
//! The sync client wraps the async client and drives it to completion itself,
//! so this is a plain `fn main` with no runtime of its own. The runtime it
//! blocks on follows the `rt-tokio` / `rt-async-std` feature.
//!
//! Run with the sync feature and either runtime:
//!
//! ```bash
//! cargo run --example crud_sync --no-default-features --features "sync,rt-tokio"
//! cargo run --example crud_sync --no-default-features --features "sync,rt-async-std"
//! ```

#[macro_use]
extern crate aerospike;

use std::env;
use std::time::Instant;

use aerospike::operations;
use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};

fn main() {
    run();
}

fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = std::env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let client = Client::new(&cpolicy, &hosts).expect("Failed to connect to cluster");

    let now = Instant::now();
    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    let key = as_key!("test", "test", "test");

    let bins = [as_bin!("int", 999), as_bin!("str", "Hello, World!")];
    client.put(&wpolicy, &key, &bins).unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All).unwrap();
    println!("Record: {}", rec);

    client.touch(&wpolicy, &key).unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All).unwrap();
    println!("Record: {}", rec);

    let rec = client.get(&rpolicy, &key, Bins::None).unwrap();
    println!("Record Header: {}", rec);

    let exists = client.exists(&rpolicy, &key).unwrap();
    println!("exists: {}", exists);

    let bin = as_bin!("int", "123");
    let ops = &[operations::put(&bin), operations::get()];
    let op_rec = client.operate(&wpolicy, &key, ops).unwrap();
    println!("operate: {}", op_rec);

    let existed = client.delete(&wpolicy, &key).unwrap();
    println!("existed (should be true): {}", existed);

    let existed = client.delete(&wpolicy, &key).unwrap();
    println!("existed (should be false): {}", existed);

    println!("total time: {:?}", now.elapsed());
}
