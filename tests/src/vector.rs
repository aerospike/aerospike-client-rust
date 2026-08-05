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

//! Integration tests for vector distance expressions
//! (`expressions::vector::{l2_squared_distance, dot_product, cosine_similarity}`).
//!
//! These are `#[ignore]`d because the required server-side feature
//! (`EXP_VECTOR_DIST`, vector bin type) is not released yet. Run manually
//! against a server that has it, e.g.:
//! `cargo test -p tests --features rt-tokio vector -- --ignored`.
//!
//! TODO(vector-exp-envelope, vector-exp-metric-semantics): once run against a
//! real server, double-check that these pass and that each metric's *value*
//! (not just sort order) matches the decided semantics -- especially
//! `l2_squared_distance`, which is not known to match server behavior yet.

use crate::common;

use aerospike::expressions::vector::{cosine_similarity, dot_product, l2_squared_distance};
use aerospike::expressions::vector_bin;
use aerospike::operations::exp::{read_exp, ExpReadFlags};
use aerospike::{as_bin, as_key, Vector, WritePolicy};

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

// Distance-to-self is 0 for L2 (squared or not -- both are 0 for identical
// vectors), so this assertion holds regardless of the open
// vector-exp-metric-semantics question.
#[aerospike_macro::test]
#[ignore = "requires a server build with EXP_VECTOR_DIST / vector bin support (unreleased)"]
async fn l2_squared_distance_to_self_is_zero() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "v1");

    let v = Vector::float32(vec![0.1, 0.2, 0.3, 0.4]);
    write_query_vector(&client, &key, &v).await;

    let ops = vec![read_exp(
        "dist",
        l2_squared_distance(&v, vector_bin("embedding".to_string())),
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
#[ignore = "requires a server build with EXP_VECTOR_DIST / vector bin support (unreleased)"]
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
#[ignore = "requires a server build with EXP_VECTOR_DIST / vector bin support (unreleased)"]
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

// TODO(vector-exp-metric-semantics): the tests above only use identity-vector
// cases (distance/similarity to self), because those results (0, sum of
// squares, 1) hold no matter how the open squared-vs-unsquared-L2 question is
// resolved. A test that actually distinguishes the two -- e.g. stored
// [0.0, 0.0] vs. query [3.0, 4.0], where squared L2 is 25.0 and plain
// Euclidean is 5.0 -- still needs to be written and have its expected value
// filled in once that's confirmed against real server behavior.
