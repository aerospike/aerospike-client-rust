//! `ORDER BY <bin> LIMIT k` ("Top-K") queries.
//!
//! The client performs bounded reduction and merges the ordered top `k`
//! records. The server supports wire-level pushdown; this client doesn't use
//! it yet (TODO).
//!
//! Run with:
//!
//! ```bash
//! cargo run --example query_top_k
//! ```

#[allow(unused_imports)]
use aerospike::{as_bin, as_key};

use aerospike::query::{Order, OrderByFlags, OrderByType, PartitionFilter};
use aerospike::{Bins, Client, ClientPolicy, QueryPolicy, Statement, WritePolicy};
use futures::stream::StreamExt;
use std::env;

const DEFAULT_NAMESPACE: &str = "test";
const DEFAULT_SET: &str = "query_top_k";
const SCORE_BIN: &str = "score";
const NAME_BIN: &str = "name";

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`) so the example stays
/// compiling and working as the API evolves.
pub async fn run() {
    let client = connect_to_aerospike().await;
    println!("Connected to Aerospike!");

    populate_test_data(&client).await;

    top_k_query_returns_ordered_records(&client).await;
    top_k_without_order_by_is_rejected(&client).await;
    top_k_out_of_range_is_rejected(&client).await;
    order_by_bin_not_in_projection_is_rejected(&client).await;
    case_insensitive_flag_requires_string_type_is_rejected(&client).await;

    cleanup(&client).await;
    client.close().await.unwrap();
}

async fn connect_to_aerospike() -> Client {
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));

    let mut policy = ClientPolicy::default();
    policy.use_services_alternate = std::env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);

    Client::new(&policy, &hosts)
        .await
        .expect("Failed to connect to cluster")
}

async fn populate_test_data(client: &Client) {
    let wpolicy = WritePolicy::default();
    let scores: &[(&str, i64, &str)] = &[
        ("alice", 92, "Alice"),
        ("bob", 75, "Bob"),
        ("carol", 88, "Carol"),
        ("dave", 61, "Dave"),
        ("erin", 99, "Erin"),
    ];

    for (user_key, score, name) in scores {
        let key = as_key!(DEFAULT_NAMESPACE, DEFAULT_SET, *user_key);
        let bins = [as_bin!(SCORE_BIN, *score), as_bin!(NAME_BIN, *name)];
        client
            .put(&wpolicy, &key, &bins)
            .await
            .expect("Failed to write record");
    }
}

/// A well-formed `order_by`/`top_k` statement returns the globally best
/// results in the requested direction.
async fn top_k_query_returns_ordered_records(client: &Client) {
    println!("\n--- 1. Top three scores in descending order ---");

    let mut stmt = Statement::new(DEFAULT_NAMESPACE, DEFAULT_SET, Bins::All);
    stmt.set_order_by(SCORE_BIN, OrderByType::Integer, Order::Desc);
    stmt.set_top_k(3);

    let scores = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
        .expect("Top-K query should be accepted")
        .into_stream()
        .map(
            |result| match result.expect("Top-K record").bins[SCORE_BIN] {
                aerospike::Value::Int(score) => score,
                ref value => panic!("expected integer score, got {value:?}"),
            },
        )
        .collect::<Vec<_>>()
        .await;
    assert_eq!(scores, vec![99, 92, 88]);
    println!("{scores:?}");
}

/// `set_top_k` without a preceding `set_order_by` is rejected client-side,
/// before any request reaches the server.
async fn top_k_without_order_by_is_rejected(client: &Client) {
    println!("\n--- 2. top_k without order_by is rejected ---");

    let mut stmt = Statement::new(DEFAULT_NAMESPACE, DEFAULT_SET, Bins::All);
    stmt.set_top_k(5);

    match client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
    {
        Ok(_) => panic!("Expected validation error for top_k without order_by"),
        Err(err) => println!("Rejected as expected: {err}"),
    }
}

/// `k` must be in `[1, 1000]`.
async fn top_k_out_of_range_is_rejected(client: &Client) {
    println!("\n--- 3. top_k outside [1, 1000] is rejected ---");

    let mut stmt = Statement::new(DEFAULT_NAMESPACE, DEFAULT_SET, Bins::All);
    stmt.set_order_by(SCORE_BIN, OrderByType::Integer, Order::Desc);
    stmt.set_top_k(0);

    match client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
    {
        Ok(_) => panic!("Expected validation error for top_k == 0"),
        Err(err) => println!("Rejected as expected: {err}"),
    }
}

/// When a bin projection is set (via `Bins::Some`, or `set_operations`), the
/// order-by bin must be one of the projected bins.
async fn order_by_bin_not_in_projection_is_rejected(client: &Client) {
    println!("\n--- 4. order_by bin missing from the bin projection is rejected ---");

    let mut stmt = Statement::new(DEFAULT_NAMESPACE, DEFAULT_SET, Bins::from([NAME_BIN]));
    stmt.set_order_by(SCORE_BIN, OrderByType::Integer, Order::Desc);
    stmt.set_top_k(3);

    match client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
    {
        Ok(_) => panic!("Expected validation error for order_by bin outside the projection"),
        Err(err) => println!("Rejected as expected: {err}"),
    }
}

/// `OrderByFlags::CaseInsensitive` only makes sense for `OrderByType::String`.
async fn case_insensitive_flag_requires_string_type_is_rejected(client: &Client) {
    println!("\n--- 5. CASE_INSENSITIVE flag on a non-STRING type is rejected ---");

    let mut stmt = Statement::new(DEFAULT_NAMESPACE, DEFAULT_SET, Bins::All);
    stmt.set_order_by_with_flags(
        SCORE_BIN,
        OrderByType::Integer,
        Order::Desc,
        OrderByFlags::CaseInsensitive,
    );
    stmt.set_top_k(3);

    match client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
    {
        Ok(_) => panic!("Expected validation error for CASE_INSENSITIVE with type Integer"),
        Err(err) => println!("Rejected as expected: {err}"),
    }
}

async fn cleanup(client: &Client) {
    let wpolicy = WritePolicy::default();
    for user_key in ["alice", "bob", "carol", "dave", "erin"] {
        let key = as_key!(DEFAULT_NAMESPACE, DEFAULT_SET, user_key);
        let _ = client.delete(&wpolicy, &key).await;
    }
}
