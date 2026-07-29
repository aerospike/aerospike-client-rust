//! Query server/node information via the info protocol.
//!
//! Port of the Java client's `ServerInfo` example.

use std::env;

use aerospike::{AdminPolicy, Client, ClientPolicy};

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = std::env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let apolicy = AdminPolicy::default();

    println!("cluster nodes: {:?}", client.node_names());

    for node in client.nodes() {
        println!("--- node {} (server {:?}) ---", node.name(), node.version());

        let info = node
            .info(
                &apolicy,
                &["build", "edition", "namespaces", "statistics"],
            )
            .await
            .unwrap();

        println!("build:      {:?}", info.get("build"));
        println!("edition:    {:?}", info.get("edition"));
        println!("namespaces: {:?}", info.get("namespaces"));

        // `statistics` is a large ';'-separated list — show a taste.
        if let Some(stats) = info.get("statistics") {
            for stat in stats.split(';').take(5) {
                println!("stat:       {stat}");
            }
            println!("…           ({} statistics total)", stats.split(';').count());
        }
    }

    client.close().await.unwrap();
}
