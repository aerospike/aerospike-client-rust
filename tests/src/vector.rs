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

//! VECTOR particle integration tests.
//!
//! WIP vector-distance tests require `EXP_VECTOR_DIST` and are ignored.

use crate::common;

use aerospike::expressions::vector::{cosine_similarity, dot_product, euclidean_squared_distance};
use aerospike::expressions::vector_bin;
use aerospike::operations::exp::{read_exp, ExpReadFlags};
use aerospike::{as_bin, as_key, Bins, Value, Vector, WritePolicy};

async fn write_query_vector(client: &aerospike::Client, key: &aerospike::Key, v: &Vector) {
    let wpolicy = WritePolicy::default();
    common::delete_durably(client, &wpolicy, key).await.unwrap();
    let bins = vec![as_bin!("embedding", v.clone())];
    client.put(&wpolicy, key, &bins).await.unwrap();
}

fn float_bin(rec: &aerospike::Record, name: &str) -> f64 {
    match rec.bins.get(name) {
        Some(aerospike::Value::Float(f)) => f64::from(f.clone()),
        other => panic!("expected float bin {name:?}, got {other:?}"),
    }
}

#[aerospike_macro::test]
async fn vectors_round_trip_through_server() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "round-trip");
    let write_policy = WritePolicy::default();
    // Non-finite element values are valid vector data. Invalid dimensions and
    // reserved values are rejected by the typed constructors.
    let expected = vec![
        ("f16", Vector::float16(vec![0x3c00, 0x4000])),
        ("i32", Vector::int32(vec![-1, 0, 1])),
        ("f32", Vector::float32(vec![0.5, -1.5, 2.0])),
        ("f64", Vector::float64(vec![0.25, -0.5, 1.0])),
        (
            "f32-special",
            Vector::float32(vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY]),
        ),
        (
            "f64-special",
            Vector::float64(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY]),
        ),
        ("f16-special", Vector::float16(vec![0x7c00, 0xfc00, 0x7e00])),
    ];

    common::delete_durably(&client, &write_policy, &key).await.unwrap();
    let bins = expected
        .iter()
        .map(|(name, vector)| as_bin!(*name, vector.clone()))
        .collect::<Vec<_>>();
    client.put(&write_policy, &key, &bins).await.unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    for (name, vector) in expected {
        assert_eq!(
            record.bins.get(name),
            Some(&Value::Vector(vector)),
            "vector bin {name} should round-trip through the server"
        );
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn vector_bin_can_be_absent_without_being_an_empty_vector() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "missing-vector");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();
    client
        .put(&write_policy, &key, &[as_bin!("scalar", 42)])
        .await
        .unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert!(
        !record.bins.contains_key("vector"),
        "an absent vector bin must not be materialized as an empty vector"
    );
    assert_eq!(record.bins.get("scalar"), Some(&Value::Int(42)));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
#[ignore = "requires a server build with EXP_VECTOR_DIST"]
async fn euclidean_squared_distance_to_self_is_zero() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "v1");

    let v = Vector::float32(vec![0.1, 0.2, 0.3, 0.4]);
    write_query_vector(&client, &key, &v).await;

    let ops = vec![read_exp(
        "dist",
        euclidean_squared_distance(&v, vector_bin("embedding".to_string())),
        ExpReadFlags::Default,
    )];
    let rec = client
        .operate(&WritePolicy::default(), &key, &ops)
        .await
        .unwrap();

    assert!(
        (float_bin(&rec, "dist") - 0.0).abs() < 1e-6,
        "distance from a vector to itself should be 0"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
#[ignore = "requires a server build with EXP_VECTOR_DIST"]
async fn dot_product_matches_sum_of_squares_for_self() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "v2");

    let elements = vec![1.0_f32, 2.0, 3.0];
    let expected: f64 = elements.iter().map(|x| (*x as f64) * (*x as f64)).sum();
    let v = Vector::float32(elements);
    write_query_vector(&client, &key, &v).await;

    let ops = vec![read_exp(
        "dist",
        dot_product(&v, vector_bin("embedding".to_string())),
        ExpReadFlags::Default,
    )];
    let rec = client
        .operate(&WritePolicy::default(), &key, &ops)
        .await
        .unwrap();

    assert!(
        (float_bin(&rec, "dist") - expected).abs() < 1e-3,
        "dot product of a vector with itself should equal the sum of its squares"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
#[ignore = "requires a server build with EXP_VECTOR_DIST"]
async fn cosine_similarity_to_self_is_one() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "v3");

    let v = Vector::float32(vec![0.5, -1.5, 2.0]);
    write_query_vector(&client, &key, &v).await;

    let ops = vec![read_exp(
        "dist",
        cosine_similarity(&v, vector_bin("embedding".to_string())),
        ExpReadFlags::Default,
    )];
    let rec = client
        .operate(&WritePolicy::default(), &key, &ops)
        .await
        .unwrap();

    assert!(
        (float_bin(&rec, "dist") - 1.0).abs() < 1e-6,
        "cosine similarity of a vector with itself should be 1"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
#[ignore = "requires a server build with EXP_VECTOR_DIST"]
async fn euclidean_squared_distance_is_sum_of_squared_differences() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "v4");
    let stored = Vector::float32(vec![0.0, 0.0]);
    let query = Vector::float32(vec![3.0, 4.0]);
    write_query_vector(&client, &key, &stored).await;

    let ops = vec![read_exp(
        "dist",
        euclidean_squared_distance(&query, vector_bin("embedding".to_string())),
        ExpReadFlags::Default,
    )];
    let rec = client
        .operate(&WritePolicy::default(), &key, &ops)
        .await
        .unwrap();

    // Squared L2: 3^2 + 4^2 = 25.
    assert!(
        (float_bin(&rec, "dist") - 25.0).abs() < 1e-6,
        "squared Euclidean distance between [0, 0] and [3, 4] should be 25"
    );

    client.close().await.unwrap();
}
