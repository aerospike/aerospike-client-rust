//! List and Map (CDT) operations, including nested structures.
//!
//! Ports of the Java client's `OperateList`, `OperateMap` and `ListMap`
//! examples.

use std::env;

use aerospike::operations::cdt_context::{ctx_list_index, ctx_map_key};
use aerospike::operations::{lists, maps, scalar};
use aerospike::operations::{ListPolicy, ListReturnType, MapPolicy, MapReturnType};
use aerospike::{as_bin, as_key, as_list, as_map, as_val};
use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, Value, WritePolicy};

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

    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    let lpolicy = ListPolicy::default();
    let mpolicy = MapPolicy::default();
    let key = as_key!("test", "cdt_ops", "demo");
    let _ = client.delete(&wpolicy, &key).await;

    // ============================================================
    // List operations (Java OperateList)
    // ============================================================
    client
        .put(&wpolicy, &key, &[as_bin!("scores", as_list!(10, 20, 30))])
        .await
        .unwrap();

    // Append two values, then read the size — all in one atomic operate call.
    let ops = [
        lists::append_items(&lpolicy, "scores", vec![as_val!(40), as_val!(50)]),
        lists::size("scores"),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("list: append + size => {:?}", rec.bins.get("scores"));

    // Pop the last element and fetch the element ranked highest.
    let ops = [lists::get_by_rank("scores", -1, ListReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("list: highest ranked value = {:?}", rec.bins.get("scores"));

    // Remove elements by index range [0, 2).
    let ops = [
        lists::remove_by_index_range_count("scores", 0, 2, ListReturnType::Values),
        scalar::get_bin("scores"),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("list: after removing first two => {:?}", rec.bins.get("scores"));

    // ============================================================
    // Map operations (Java OperateMap)
    // ============================================================
    let mut items = std::collections::HashMap::new();
    items.insert(as_val!("alpha"), as_val!(1));
    items.insert(as_val!("beta"), as_val!(2));
    items.insert(as_val!("gamma"), as_val!(3));
    let ops = [maps::put_items(&mpolicy, "counters", items)];
    client.operate(&wpolicy, &key, &ops).await.unwrap();

    // Increment one entry and read it back by key.
    let ops = [
        maps::increment_value(&mpolicy, "counters", as_val!("beta"), as_val!(40)),
        maps::get_by_key("counters", as_val!("beta"), MapReturnType::Value),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("map: beta incremented => {:?}", rec.bins.get("counters"));

    // Rank query: entry with the highest value.
    let ops = [maps::get_by_rank("counters", -1, MapReturnType::KeyValue)];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("map: highest entry = {:?}", rec.bins.get("counters"));

    // ============================================================
    // Nested structures (Java ListMap) — CDT contexts
    // ============================================================
    // Bin value: { "prices": [1, 2, 3], "meta": { "owner": "alice" } }
    let nested = as_map!(
        "prices" => as_list!(1, 2, 3),
        "meta" => as_map!("owner" => "alice")
    );
    client
        .put(&wpolicy, &key, &[as_bin!("doc", nested)])
        .await
        .unwrap();

    // Append 4 to doc["prices"] using a map-key context.
    let op = lists::append(&lpolicy, "doc", as_val!(4)).context(vec![ctx_map_key(as_val!("prices"))]);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Read doc["prices"][3] via a map-key context.
    let op = lists::get("doc", 3).context(vec![ctx_map_key(as_val!("prices"))]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    println!("nested: doc[\"prices\"][3] = {:?}", rec.bins.get("doc"));
    assert_eq!(rec.bins.get("doc"), Some(&Value::Int(4)));

    // Read doc["meta"]["owner"].
    let op = maps::get_by_key("doc", as_val!("owner"), MapReturnType::Value)
        .context(vec![ctx_map_key(as_val!("meta"))]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    println!("nested: doc[\"meta\"][\"owner\"] = {:?}", rec.bins.get("doc"));

    // A list-index context works the same way on the outer list.
    client
        .put(&wpolicy, &key, &[as_bin!("matrix", as_list!(as_list!(1, 2), as_list!(3, 4)))])
        .await
        .unwrap();
    let op = lists::get_by_index("matrix", 0, ListReturnType::Values)
        .context(vec![ctx_list_index(1)]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    println!("nested: matrix[1][0] = {:?}", rec.bins.get("matrix"));

    let full = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    println!("final record: {full}");

    client.delete(&wpolicy, &key).await.unwrap();
    client.close().await.unwrap();
}
