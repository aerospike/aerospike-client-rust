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

//! A hybrid vector search: a secondary-index filter narrows the candidates,
//! a vector-distance expression scores each one, and a Top-K query keeps the
//! nearest.
//!
//! Needs Aerospike Server 8.2.1 on every node (vectors and Top-K); the
//! example skips itself otherwise.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example query_vector_top_k
//! ```

#[allow(unused_imports)]
use aerospike::{as_bin, as_key};

use aerospike::expressions::vector::euclidean_squared_distance;
use aerospike::expressions::vector_bin;
use aerospike::operations::exp::{read_exp, ExpReadFlags};
use aerospike::operations::get_bin;
use aerospike::query::{Filter, Order, OrderByType, PartitionFilter};
use aerospike::{
    AdminPolicy, Bins, Client, ClientPolicy, CollectionIndexType, IndexType, QueryPolicy,
    ResultCode, Statement, Task, Value, Vector, WritePolicy,
};
use futures::stream::StreamExt;
use std::env;

const DEFAULT_NAMESPACE: &str = "test";
const DEFAULT_SET: &str = "query_vector_top_k";
const INDEX_NAME: &str = "query_vector_top_k_category";
const PRODUCTS: i64 = 10;

#[tokio::main]
async fn main() {
    run().await;
}

pub async fn run() {
    let client = connect_to_aerospike().await;
    println!("Connected to Aerospike!");

    if !client.nodes().iter().all(|node| {
        node.version().supports_vector() && node.version().supports_query_top_k()
    }) {
        println!("Skipping: vectors and Top-K need Aerospike Server 8.2.1 on every node.");
        client.close().await.unwrap();
        return;
    }

    create_category_index(&client).await;
    write_products(&client).await;

    let ids = nearest_shoes(&client).await;
    // Shoes are the even ids; the three nearest to the query vector are 6, 8 and 4.
    assert_eq!(ids, vec![6, 8, 4], "Top-K mismatch");

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

/// A string index on `category`, so the filter selects one category before
/// any distance is computed.
async fn create_category_index(client: &Client) {
    println!("\n--- 1. Index the category bin ---");
    match client
        .create_index_on_bin(
            &AdminPolicy::default(),
            DEFAULT_NAMESPACE,
            DEFAULT_SET,
            "category",
            INDEX_NAME,
            IndexType::String,
            CollectionIndexType::Default,
            None,
        )
        .await
    {
        Ok(task) => {
            task.wait_till_complete(None)
                .await
                .expect("index creation should finish");
        }
        Err(err) if err.server_result_code() == Some(ResultCode::IndexFound) => {}
        Err(err) => panic!("Failed to create index: {err}"),
    }
}

/// Ten products, alternating between shoes and hats, each with a
/// three-dimensional `float32` embedding.
async fn write_products(client: &Client) {
    println!("\n--- 2. Write ten products with embeddings ---");
    let wpolicy = WritePolicy::default();
    for i in 0..PRODUCTS {
        let key = as_key!(DEFAULT_NAMESPACE, DEFAULT_SET, format!("product{i}"));
        let category = if i % 2 == 0 { "shoes" } else { "hats" };
        let embedding = Vector::float32(vec![i as f32, i as f32 * 0.5, 1.0]);
        client
            .put(
                &wpolicy,
                &key,
                &[
                    as_bin!("id", i),
                    as_bin!("category", category),
                    as_bin!("embedding", embedding),
                ],
            )
            .await
            .expect("Failed to write product");
    }
}

/// Filters on the index, projects the id and the squared Euclidean distance
/// to the query vector, and ranks by that distance, nearest first.
async fn nearest_shoes(client: &Client) -> Vec<i64> {
    println!("\n--- 3. The three shoes nearest to the query vector ---");
    let query = Vector::float32(vec![6.5, 3.25, 1.0]);
    let distance = euclidean_squared_distance(&query, vector_bin("embedding".to_string()));

    let mut stmt = Statement::new(DEFAULT_NAMESPACE, DEFAULT_SET, Bins::All);
    stmt.add_filter(Filter::equal("category", "shoes"));
    stmt.set_operations(vec![
        get_bin("id"),
        read_exp("distance", distance, ExpReadFlags::Default),
    ]);
    stmt.set_order_by("distance", OrderByType::Double, Order::Asc);
    stmt.set_top_k(3);

    client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
        .expect("Top-K query should be accepted")
        .into_stream()
        .map(|result| {
            let record = result.expect("Top-K record");
            let id = match record.bins["id"] {
                Value::Int(id) => id,
                ref value => panic!("expected integer id, got {value:?}"),
            };
            let distance: f64 = match record.bins["distance"] {
                Value::Float(ref f) => f64::from(f.clone()),
                ref value => panic!("expected float distance, got {value:?}"),
            };
            println!("Nearest shoe: id={id} distance={distance:.4}");
            id
        })
        .collect::<Vec<_>>()
        .await
}

async fn cleanup(client: &Client) {
    let wpolicy = WritePolicy::default();
    for i in 0..PRODUCTS {
        let key = as_key!(DEFAULT_NAMESPACE, DEFAULT_SET, format!("product{i}"));
        let _ = client.delete(&wpolicy, &key).await;
    }
    let _ = client
        .drop_index(
            &AdminPolicy::default(),
            DEFAULT_NAMESPACE,
            DEFAULT_SET,
            INDEX_NAME,
        )
        .await;
}
