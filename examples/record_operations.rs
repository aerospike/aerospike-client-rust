//! Single-record operations and write-policy behaviors.
//!
//! Ports of the Java client's `Add`, `Append`, `Prepend`, `Touch`, `Expire`,
//! `Generation`, `Replace`, `StoreKey` and `DeleteBin` examples.

use std::env;

use aerospike::operations::scalar;
use aerospike::{as_bin, as_key};
use aerospike::{
    Bins, Client, ClientPolicy, Expiration, GenerationPolicy, ReadPolicy, RecordExistsAction,
    ResultCode, Value, WritePolicy,
};

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    let key = as_key!("test", "record_ops", "demo");
    let _ = client.delete(&wpolicy, &key).await;

    // ---- Add (integer arithmetic on a bin) ----
    client
        .put(&wpolicy, &key, &[as_bin!("count", 10)])
        .await
        .unwrap();
    client
        .add(&wpolicy, &key, &[as_bin!("count", 5)])
        .await
        .unwrap();
    let rec = client.get(&rpolicy, &key, ["count"]).await.unwrap();
    println!("add: 10 + 5 = {}", rec.bins.get("count").unwrap());

    // ---- Append / Prepend (string concatenation) ----
    client
        .put(&wpolicy, &key, &[as_bin!("greet", "World")])
        .await
        .unwrap();
    client
        .prepend(&wpolicy, &key, &[as_bin!("greet", "Hello, ")])
        .await
        .unwrap();
    client
        .append(&wpolicy, &key, &[as_bin!("greet", "!")])
        .await
        .unwrap();
    let rec = client.get(&rpolicy, &key, ["greet"]).await.unwrap();
    println!("append/prepend: {}", rec.bins.get("greet").unwrap());

    // ---- Expire (write with TTL, observe it counting down) ----
    let mut ttl_policy = WritePolicy::default();
    ttl_policy.expiration = Expiration::Seconds(120);
    client
        .put(&ttl_policy, &key, &[as_bin!("ttl-bin", 1)])
        .await
        .unwrap();
    let rec = client.get(&rpolicy, &key, Bins::None).await.unwrap();
    println!("expire: ttl after write = {:?}", rec.time_to_live());

    // ---- Touch (reset TTL / bump generation without changing data) ----
    client.touch(&ttl_policy, &key).await.unwrap();
    let rec = client.get(&rpolicy, &key, Bins::None).await.unwrap();
    println!("touch: generation now {}", rec.generation);

    // ---- Generation (optimistic concurrency: expect-gen-equal CAS) ----
    let rec = client.get(&rpolicy, &key, Bins::None).await.unwrap();
    let mut cas = WritePolicy::default();
    cas.generation_policy = GenerationPolicy::ExpectGenEqual;
    cas.generation = rec.generation;
    client
        .put(&cas, &key, &[as_bin!("cas-bin", 1)])
        .await
        .unwrap();
    println!("generation: CAS write with matching generation succeeded");

    // A second write with the now-stale generation must fail.
    match client.put(&cas, &key, &[as_bin!("cas-bin", 2)]).await {
        Err(e) if e.server_result_code() == Some(ResultCode::GenerationError) => {
            println!("generation: stale CAS write correctly rejected");
        }
        other => panic!("expected GENERATION_ERROR, got {other:?}"),
    }

    // ---- Replace (RecordExistsAction: replace the whole record) ----
    let mut replace = WritePolicy::default();
    replace.record_exists_action = RecordExistsAction::Replace;
    client
        .put(&replace, &key, &[as_bin!("only-bin", "left")])
        .await
        .unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    println!(
        "replace: record now has {} bin(s): {:?}",
        rec.bins.len(),
        rec.bins.keys().collect::<Vec<_>>()
    );

    // ReplaceOnly on a missing record fails with KEY_NOT_FOUND.
    let missing = as_key!("test", "record_ops", "missing");
    let _ = client.delete(&wpolicy, &missing).await;
    let mut replace_only = WritePolicy::default();
    replace_only.record_exists_action = RecordExistsAction::ReplaceOnly;
    match client.put(&replace_only, &missing, &[as_bin!("b", 1)]).await {
        Err(e) if e.server_result_code() == Some(ResultCode::KeyNotFoundError) => {
            println!("replace: replace-only on missing record correctly rejected");
        }
        other => panic!("expected KEY_NOT_FOUND, got {other:?}"),
    }

    // ---- StoreKey (send the user key so the server stores it) ----
    let mut send_key = WritePolicy::default();
    send_key.send_key = true;
    client
        .put(&send_key, &key, &[as_bin!("k-bin", 1)])
        .await
        .unwrap();
    println!("store key: user key sent with the write (send_key = true)");

    // ---- DeleteBin (write a nil value to remove a single bin) ----
    client
        .put(&wpolicy, &key, &[as_bin!("doomed", 42)])
        .await
        .unwrap();
    client
        .put(&wpolicy, &key, &[as_bin!("doomed", None)])
        .await
        .unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    println!(
        "delete bin: 'doomed' present after nil write? {}",
        rec.bins.contains_key("doomed")
    );

    // ---- Operate (multiple ops on one record atomically) ----
    let ops = [
        scalar::put(&as_bin!("count", 1)),
        scalar::add(&as_bin!("count", 41)),
        scalar::get_bin("count"),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("operate: put+add+get => {:?}", rec.bins.get("count"));
    assert_eq!(rec.bins.get("count"), Some(&Value::Int(42)));

    client.delete(&wpolicy, &key).await.unwrap();
    client.close().await.unwrap();
}
