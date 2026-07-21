//! Multi-record transactions (MRT): commit and abort.
//!
//! Port of the Java client's `Transaction` / `AsyncTransaction` examples.
//! Requires Aerospike server 8.0+ with a strong-consistency-capable
//! namespace; the example skips gracefully on older servers.

use std::env;
use std::sync::Arc;

use aerospike::{as_bin, as_key};
use aerospike::{AdminPolicy, Bins, Client, ClientPolicy, ReadPolicy, Txn, Value, WritePolicy};

#[tokio::main]
async fn main() {
    run().await;
}

/// True when `ns` is configured with strong consistency (required for MRT).
async fn namespace_is_sc(client: &Client, ns: &str) -> bool {
    let Ok(node) = client.cluster.get_random_node() else {
        return false;
    };
    let info_key = format!("namespace/{ns}");
    match node.info(&AdminPolicy::default(), &[&info_key]).await {
        Ok(map) => map
            .get(&info_key)
            .is_some_and(|info| info.contains("strong-consistency=true")),
        Err(_) => false,
    }
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

    let supported = client
        .cluster
        .get_random_node()
        .map(|n| n.version().supports_mrt())
        .unwrap_or(false);
    if !supported {
        println!("Server does not support multi-record transactions (requires 8.0+); skipping.");
        client.close().await.unwrap();
        return;
    }
    if !namespace_is_sc(&client, "test").await {
        println!(
            "Namespace `test` is not configured with strong-consistency; \
             multi-record transactions require an SC namespace. Skipping."
        );
        client.close().await.unwrap();
        return;
    }

    let rpolicy = ReadPolicy::default();
    let plain = WritePolicy::default();
    let key1 = as_key!("test", "txn_demo", "account-a");
    let key2 = as_key!("test", "txn_demo", "account-b");
    let _ = client.delete(&plain, &key1).await;
    let _ = client.delete(&plain, &key2).await;

    // Seed two "accounts" outside the transaction.
    client.put(&plain, &key1, &[as_bin!("balance", 100)]).await.unwrap();
    client.put(&plain, &key2, &[as_bin!("balance", 0)]).await.unwrap();

    // ---- Commit: transfer 30 from A to B atomically ----
    let txn = Arc::new(Txn::new());
    println!("begin transaction {}", txn.id());

    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    client.put(&wp, &key1, &[as_bin!("balance", 70)]).await.unwrap();
    client.put(&wp, &key2, &[as_bin!("balance", 30)]).await.unwrap();

    let status = client.commit(&txn).await.unwrap();
    println!("commit status: {status:?}");

    let a = client.get(&rpolicy, &key1, Bins::All).await.unwrap();
    let b = client.get(&rpolicy, &key2, Bins::All).await.unwrap();
    println!(
        "after commit: A = {:?}, B = {:?}",
        a.bins.get("balance"),
        b.bins.get("balance")
    );
    assert_eq!(a.bins.get("balance"), Some(&Value::Int(70)));
    assert_eq!(b.bins.get("balance"), Some(&Value::Int(30)));

    // ---- Abort: a failed business check rolls everything back ----
    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    client.put(&wp, &key1, &[as_bin!("balance", -1000)]).await.unwrap();

    // Pretend validation failed; abort instead of committing.
    let status = client.abort(&txn).await.unwrap();
    println!("abort status: {status:?}");

    let a = client.get(&rpolicy, &key1, Bins::All).await.unwrap();
    println!("after abort: A = {:?} (unchanged)", a.bins.get("balance"));
    assert_eq!(a.bins.get("balance"), Some(&Value::Int(70)));

    let _ = client.delete(&plain, &key1).await;
    let _ = client.delete(&plain, &key2).await;
    client.close().await.unwrap();
}
