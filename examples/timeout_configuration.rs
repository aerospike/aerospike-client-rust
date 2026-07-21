use aerospike::{as_bin, as_key};

use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
use std::env;
use std::time::Instant;

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`) so the examples
/// stay working and compiling as the API evolves.
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = std::env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("127.0.0.1:3100"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    println!("Connected to Aerospike!");

    let namespace = "test";
    let set_name = "timeout_demo";
    let key = as_key!(namespace, set_name, "timeout_test");

    // Setup: Write a test record
    let wpolicy = WritePolicy::default();
    let bins = [as_bin!("data", "test value")];
    client.put(&wpolicy, &key, &bins).await.unwrap();
    println!("Test record created\n");

    default_timeout(&client, &key).await;
    socket_and_total_timeouts(&client, &key).await;

    client.delete(&wpolicy, &key).await.ok();
    client.close().await.unwrap();
}

async fn default_timeout(client: &Client, key: &aerospike::Key) {
    println!("socket_timeout: 5000 (default), total_timeout: 0");

    let mut policy = ReadPolicy::default();
    policy.base_policy.socket_timeout = 0;
    policy.base_policy.total_timeout = 0;

    let start = Instant::now();
    match client.get(&policy, key, Bins::All).await {
        Ok(rec) => {
            println!("✓ Read successful in {:?}", start.elapsed());
            println!("  Record: {}\n", rec);
        }
        Err(e) => println!("✗ Error: {:?}\n", e),
    }
}

async fn socket_and_total_timeouts(client: &Client, key: &aerospike::Key) {
    println!("socket_timeout: 2000ms, total_timeout: 5000ms");
    println!("Result: Socket idle timeout of 2s, total command timeout of 5s");

    let mut policy = ReadPolicy::default();
    policy.base_policy.socket_timeout = 2000; // 2 seconds
    policy.base_policy.total_timeout = 5000; // 5 seconds

    let start = Instant::now();
    match client.get(&policy, key, Bins::All).await {
        Ok(rec) => {
            println!("✓ Read successful in {:?}", start.elapsed());
            println!("  Record: {}\n", rec);
        }
        Err(e) => println!("✗ Error: {:?}\n", e),
    }
}
