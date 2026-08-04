//! `ORDER BY <bin> LIMIT k` ("Top-K") queries — client-side API and validation.
//!
//! # Work in progress — read before using
//!
//! This example demonstrates the parts of Top-K that are implemented today:
//! `Statement::set_order_by`/`set_top_k`, client-side validation, and the
//! wire encode. **The feature cannot actually run against a server yet**,
//! though: the wire encode is capability-gated behind
//! `Version::supports_query_top_k()`, which always evaluates to `false`
//! against a real server for now (this feature has no assigned minimum
//! server version — it's unreleased). So a well-formed statement still
//! passes `Client::query`'s upfront validation (the `Recordset` is created
//! successfully), but draining the resulting stream surfaces a per-node
//! error instead of records, once each node's command actually tries to
//! encode the request. What *is* real and tested today:
//!
//! * The builder methods themselves (`set_order_by`, `set_order_by_with_flags`, `set_top_k`).
//! * `Statement::validate()` (invoked by `Client::query` before any network
//!   I/O) rejecting invalid combinations client-side.
//! * The wire-encode logic itself (`FieldType::OrderBy`/`FieldType::TopK`),
//!   unit-tested byte-for-byte in `commands/buffer.rs`, but not reachable
//!   against a real cluster until the capability gate above is updated.
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

    valid_statement_passes_validation(&client).await;
    top_k_without_order_by_is_rejected(&client).await;
    top_k_out_of_range_is_rejected(&client).await;
    order_by_bin_not_in_projection_is_rejected(&client).await;
    case_insensitive_flag_requires_string_type_is_rejected(&client).await;

    cleanup(&client).await;
    client.close().await.unwrap();
}

async fn connect_to_aerospike() -> Client {
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3100"));

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

/// A well-formed `order_by`/`top_k` statement passes `Statement::validate()`
/// (called internally by `Client::query`), so the call itself succeeds and
/// returns a `Recordset` — but draining it surfaces a capability error from
/// each node's command instead of records, since no real server satisfies
/// `Version::supports_query_top_k()` yet (see the module docs above).
async fn valid_statement_passes_validation(client: &Client) {
    println!("\n--- 1. Well-formed order_by + top_k passes validation, but can't run yet ---");

    let mut stmt = Statement::new(DEFAULT_NAMESPACE, DEFAULT_SET, Bins::All);
    stmt.set_order_by(SCORE_BIN, OrderByType::Integer, Order::Desc);
    stmt.set_top_k(3);

    let rs = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
        .expect("a well-formed statement must pass Client::query's upfront validation");
    println!("Statement accepted by Client::query (Recordset created).");

    let mut stream = rs.into_stream();
    match stream.next().await {
        Some(Err(err)) => println!(
            "Draining the stream surfaces the expected per-node capability error: {err}"
        ),
        Some(Ok(rec)) => panic!(
            "Expected a capability error (no server supports Top-K yet), got a record: {rec:?}"
        ),
        None => panic!("Expected at least one error result from the query stream"),
    }
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
