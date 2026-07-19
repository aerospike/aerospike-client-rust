//! CDT path expressions: JSONPath-style selection over nested documents.
//!
//! Port of the Java client's `PathExpression` example. Requires Aerospike
//! server 8.1.1+; skips gracefully on older servers.

use std::env;

use aerospike::expressions::{
    eq, exp_map_loop_var, exp_string_loop_var, float_val, le, string_val, ExpType, LoopVarPart,
};
use aerospike::expressions::maps::get_by_key;
use aerospike::operations::cdt_context::{ctx_all_children_with_filter, ctx_map_key};
use aerospike::operations::path::{select_by_path, SelectFlag};
use aerospike::operations::MapReturnType;
#[allow(unused_imports)]
use aerospike::{as_bin, as_key, as_list, as_map, as_val};
use aerospike::{Client, ClientPolicy, Value, WritePolicy};

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

    let supported = client
        .cluster
        .get_random_node()
        .map(|n| n.version().supports_cdt_path_expressions())
        .unwrap_or(false);
    if !supported {
        println!("Server does not support CDT path expressions (requires 8.1.1+); skipping.");
        client.close().await.unwrap();
        return;
    }

    let wpolicy = WritePolicy::default();
    let key = as_key!("test", "path_demo", "bookstore");
    let _ = client.delete(&wpolicy, &key).await;

    // Document: { "book": [ {title, price}, ... ] } — the classic JSONPath
    // bookstore. The goal: `$.book[?(@.price <= 10)].title`.
    let books = as_list!(
        as_map!("title" => "Sayings of the Century", "price" => 8.95_f64),
        as_map!("title" => "Sword of Honour", "price" => 12.99_f64),
        as_map!("title" => "Moby Dick", "price" => 8.99_f64),
        as_map!("title" => "The Lord of the Rings", "price" => 22.99_f64)
    );
    client
        .put(&wpolicy, &key, &[as_bin!("store", as_map!("book" => books))])
        .await
        .unwrap();

    // Path: store["book"]                     (map-key context)
    //         -> each element                 (all-children, filtered on price)
    //         -> each entry with key "title"  (all-children, filtered on key)
    let ctx_book = ctx_map_key(Value::from("book"));
    let ctx_cheap = ctx_all_children_with_filter(le(
        get_by_key(
            MapReturnType::Value,
            ExpType::FLOAT,
            string_val("price".to_string()),
            exp_map_loop_var(LoopVarPart::VALUE),
            &[],
        ),
        float_val(10.0),
    ));
    let ctx_title = ctx_all_children_with_filter(eq(
        exp_string_loop_var(LoopVarPart::MAP_KEY),
        string_val("title".to_string()),
    ));

    let op = select_by_path("store", SelectFlag::VALUE, &[ctx_book, ctx_cheap, ctx_title]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    println!(
        "books cheaper than 10.00: {:?}",
        rec.bins.get("store").unwrap()
    );
    if let Some(Value::List(titles)) = rec.bins.get("store") {
        assert_eq!(titles.len(), 2);
    } else {
        panic!("expected a list of titles");
    }

    client.delete(&wpolicy, &key).await.unwrap();
    client.close().await.unwrap();
}
