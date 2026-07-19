use aerospike::{as_bin, as_key};

use std::env;
use std::time::Instant;

use aerospike::operations;
use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`) so the examples
/// stay working and compiling as the API evolves.
pub async fn run() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let now = Instant::now();
    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    let key = as_key!("test", "test", "test");

    let bins = [as_bin!("int", 999), as_bin!("str", "Hello, World!")];
    client.put(&wpolicy, &key, &bins).await.unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All).await;
    println!("Record: {}", rec.unwrap());

    client.touch(&wpolicy, &key).await.unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All).await;
    println!("Record: {}", rec.unwrap());

    let rec = client.get(&rpolicy, &key, Bins::None).await;
    println!("Record Header: {}", rec.unwrap());

    let exists = client.exists(&rpolicy, &key).await.unwrap();
    println!("exists: {}", exists);

    let bin = as_bin!("int", "123");
    let ops = &vec![operations::put(&bin), operations::get()];
    let op_rec = client.operate(&wpolicy, &key, ops).await;
    println!("operate: {}", op_rec.unwrap());

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    println!("existed (should be true): {}", existed);

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    println!("existed (should be false): {}", existed);

    println!("total time: {:?}", now.elapsed());
}
