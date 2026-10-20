// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Secondary indexes beyond the plain bin index, and set lifecycle.
//!
//! - an index on an expression, so a query can match a value the record does
//!   not store as a bin (server 8.1.0+);
//! - an index on a list's elements (`CollectionIndexType::List`);
//! - a set index, the record-presence index that needs no bin at all
//!   (server 8.1.2+);
//! - `IndexType::Integer`, the exact-integer index type (server 8.2.0+);
//! - dropping indexes, and truncating a set.
//!
//! Index creation returns an `IndexTask`; wait on it before querying.
//!
//! ```bash
//! cargo run --example index_management
//! ```

use std::env;

use aerospike::expressions::{int_bin, num_mul};
use aerospike::query::{Filter, PartitionFilter};
use aerospike::{
    as_bin, as_key, as_list, as_val, AdminPolicy, Bins, Client, ClientPolicy, CollectionIndexType,
    IndexType, QueryPolicy, ReadPolicy, Statement, Task, WritePolicy,
};
use futures::StreamExt;

const SET: &str = "index_demo";

async fn count(client: &Client, mut stmt: Statement, filter: Filter) -> usize {
    stmt.set_filter(filter);
    let rs = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
        .unwrap();
    rs.into_stream().filter(|r| std::future::ready(r.is_ok())).count().await
}

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let version = client.random_node().map(|n| n.version().clone()).expect("a node");
    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();

    // ---- Data: price, quantity, and a list of tags per product ----
    for i in 0..50i64 {
        let key = as_key!("test", SET, i);
        let tags = if i % 2 == 0 { as_list!("sale", "new") } else { as_list!("new") };
        client
            .put(&wpolicy, &key, &[as_bin!("price", i * 10), as_bin!("qty", 3), as_bin!("tags", tags)])
            .await
            .unwrap();
    }

    // ---- An index on an expression: price × qty, which no bin holds ----
    let total_idx = "index_demo_total";
    let _ = client.drop_index(&apolicy, "test", SET, total_idx).await;
    let task = client
        .create_index_using_expression(
            &apolicy,
            "test",
            SET,
            total_idx,
            IndexType::Numeric,
            CollectionIndexType::Default,
            &num_mul(vec![int_bin("price"), int_bin("qty")]),
        )
        .await
        .expect("Failed to create expression index");
    task.wait_till_complete(None).await.unwrap();
    // Query it by index name, with a range on the computed value.
    let matches = count(
        &client,
        Statement::new("test", SET, Bins::None),
        Filter::range_by_index(total_idx, 0, 300),
    )
    .await;
    println!("products with price × qty in 0..=300: {matches} (expected 11)");

    // ---- An index on list elements ----
    let tags_idx = "index_demo_tags";
    let _ = client.drop_index(&apolicy, "test", SET, tags_idx).await;
    let task = client
        .create_index_on_bin(
            &apolicy,
            "test",
            SET,
            "tags",
            tags_idx,
            IndexType::String,
            CollectionIndexType::List,
            None,
        )
        .await
        .expect("Failed to create list index");
    task.wait_till_complete(None).await.unwrap();
    let matches = count(
        &client,
        Statement::new("test", SET, Bins::None),
        Filter::contains("tags", "sale", CollectionIndexType::List),
    )
    .await;
    println!("products tagged \"sale\": {matches} (expected 25)");

    // ---- A set index: record presence, no bin involved (server 8.1.2+) ----
    if version.supports_set_index() {
        let set_idx = "index_demo_set";
        let _ = client.drop_index(&apolicy, "test", SET, set_idx).await;
        let task = client
            .create_set_index(&apolicy, "test", SET, set_idx)
            .await
            .expect("Failed to create set index");
        task.wait_till_complete(None).await.unwrap();
        println!("set index {set_idx} created; the server uses it to scan the set");
        let _ = client.drop_index(&apolicy, "test", SET, set_idx).await;
    } else {
        println!("Server does not support set indexes (requires 8.1.2+); skipping.");
    }

    // ---- IndexType::Integer (server 8.2.0+) ----
    if version.supports_integer_index() {
        let int_idx = "index_demo_qty";
        let _ = client.drop_index(&apolicy, "test", SET, int_idx).await;
        let task = client
            .create_index_on_bin(
                &apolicy,
                "test",
                SET,
                "qty",
                int_idx,
                IndexType::Integer,
                CollectionIndexType::Default,
                None,
            )
            .await
            .expect("Failed to create integer index");
        task.wait_till_complete(None).await.unwrap();
        let matches = count(&client, Statement::new("test", SET, Bins::None), Filter::equal("qty", 3)).await;
        println!("products with qty == 3 through the integer index: {matches} (expected 50)");
        let _ = client.drop_index(&apolicy, "test", SET, int_idx).await;
    } else {
        println!("Server does not support INTEGER indexes (requires 8.2.0+); skipping.");
    }

    // ---- Drop what we created ----
    let task = client.drop_index(&apolicy, "test", SET, total_idx).await.unwrap();
    task.wait_till_complete(None).await.unwrap();
    let task = client.drop_index(&apolicy, "test", SET, tags_idx).await.unwrap();
    task.wait_till_complete(None).await.unwrap();
    println!("indexes dropped");

    // ---- Truncate: delete every record of the set written before a point in time ----
    // `before_nanos` is a last-update-time cutoff; 0 means "everything now".
    client.truncate(&apolicy, "test", SET, 0).await.unwrap();
    // The server truncates asynchronously; poll until the records are gone.
    for _ in 0..50 {
        let existed = client
            .exists(&ReadPolicy::default(), &as_key!("test", SET, 0))
            .await
            .unwrap();
        if !existed {
            break;
        }
        aerospike_rt::sleep(std::time::Duration::from_millis(100)).await;
    }
    println!(
        "set truncated; record 0 exists: {}",
        client.exists(&ReadPolicy::default(), &as_key!("test", SET, 0)).await.unwrap()
    );

    client.close().await.unwrap();
}
