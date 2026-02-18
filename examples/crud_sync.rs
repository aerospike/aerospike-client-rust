//! CRUD operations using the **sync** (blocking) client.
//!
//! The sync client wraps the async client and must run on a thread where a Tokio runtime
//! is current (the core client uses the runtime for cluster tending). So we run the
//! example inside `runtime.block_on(...)`.
//!
//! Run with the sync feature enabled:
//!
//! ```bash
//! cargo run --example crud_sync --no-default-features --features "rt-tokio,sync"
//! ```

#[macro_use]
extern crate aerospike;

use std::env;
use std::time::Instant;

use aerospike::operations;
use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    rt.block_on(async {
        run();
    });
}

fn run() {
    let cpolicy = ClientPolicy::default();
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
