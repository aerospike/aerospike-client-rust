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

use std::collections::HashMap;

use aerospike::expressions::vector::{cosine_similarity, dot_product, euclidean_squared_distance};
use aerospike::expressions::{bin_exists, vector_bin};
use aerospike::operations::exp::{read_exp, ExpReadFlags};
use aerospike::{
    as_bin, as_key, BatchOperation, BatchPolicy, BatchReadPolicy, Bins, ReadPolicy, Value, Vector,
    VectorElementType, WritePolicy,
};

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

// A record may hold several vector bins of differing element types and
// dimensions alongside ordinary scalar bins; every bin round-trips.
#[aerospike_macro::test]
async fn multiple_vector_and_scalar_bins_in_one_record() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "multi-bin");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    let f32v = Vector::float32(vec![1.0, 2.0, 3.0]);
    let f64v = Vector::float64(vec![-1.5, 0.0, 2.5, 9.0]);
    let i32v = Vector::int32(vec![i32::MIN, 0, i32::MAX]);
    let bins = vec![
        as_bin!("v_f32", f32v.clone()),
        as_bin!("v_f64", f64v.clone()),
        as_bin!("v_i32", i32v.clone()),
        as_bin!("scalar", 7),
        as_bin!("label", "embedding"),
    ];
    client.put(&write_policy, &key, &bins).await.unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("v_f32"), Some(&Value::Vector(f32v)));
    assert_eq!(record.bins.get("v_f64"), Some(&Value::Vector(f64v)));
    assert_eq!(record.bins.get("v_i32"), Some(&Value::Vector(i32v)));
    assert_eq!(record.bins.get("scalar"), Some(&Value::Int(7)));
    assert_eq!(
        record.bins.get("label"),
        Some(&Value::String("embedding".to_string()))
    );

    client.close().await.unwrap();
}

// Overwriting a vector bin fully replaces its element type and dimensions -
// there is no in-place merge; the last write wins.
#[aerospike_macro::test]
async fn overwriting_a_vector_bin_replaces_element_type_and_dimensions() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "overwrite");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    let first = Vector::float32(vec![1.0, 2.0, 3.0, 4.0]);
    client
        .put(&write_policy, &key, &[as_bin!("embedding", first)])
        .await
        .unwrap();

    let replacement = Vector::int32(vec![9, -9]);
    client
        .put(&write_policy, &key, &[as_bin!("embedding", replacement.clone())])
        .await
        .unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert_eq!(
        record.bins.get("embedding"),
        Some(&Value::Vector(replacement)),
        "the second write must fully replace the first (type and dimensions)"
    );

    client.close().await.unwrap();
}

// A vector bin can be overwritten with a scalar and then a vector again -
// the bin's type is not pinned to VECTOR once written.
#[aerospike_macro::test]
async fn a_vector_bin_can_be_replaced_by_a_scalar_and_back() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "type-churn");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    let v = Vector::float32(vec![0.1, 0.2]);
    client
        .put(&write_policy, &key, &[as_bin!("b", v.clone())])
        .await
        .unwrap();

    client
        .put(&write_policy, &key, &[as_bin!("b", 123)])
        .await
        .unwrap();
    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("b"), Some(&Value::Int(123)));

    let v2 = Vector::float64(vec![9.0, 8.0, 7.0]);
    client
        .put(&write_policy, &key, &[as_bin!("b", v2.clone())])
        .await
        .unwrap();
    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("b"), Some(&Value::Vector(v2)));

    client.close().await.unwrap();
}

// A selective read (`Bins::Some`) returns only the requested vector bin.
#[aerospike_macro::test]
async fn reading_selected_bins_returns_only_the_requested_vector() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "select");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    let wanted = Vector::float32(vec![1.0, 2.0]);
    let bins = vec![
        as_bin!("wanted", wanted.clone()),
        as_bin!("other", Vector::int32(vec![5, 6, 7])),
    ];
    client.put(&write_policy, &key, &bins).await.unwrap();

    let record = client
        .get(&Default::default(), &key, Bins::Some(vec!["wanted".into()]))
        .await
        .unwrap();
    assert_eq!(record.bins.get("wanted"), Some(&Value::Vector(wanted)));
    assert!(
        !record.bins.contains_key("other"),
        "a selective read must not return unrequested bins"
    );

    client.close().await.unwrap();
}

// A single-dimension vector (header + one element) round-trips.
#[aerospike_macro::test]
async fn single_dimension_vector_round_trips_through_server() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "one-dim");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    let v = Vector::float32(vec![42.5]);
    client
        .put(&write_policy, &key, &[as_bin!("embedding", v.clone())])
        .await
        .unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("embedding"), Some(&Value::Vector(v)));

    client.close().await.unwrap();
}

// Same numeric value, four different element types, stored side by side: the
// server preserves each element type distinctly (it is not coalesced to a
// single representation). This guards the element-type byte on the round trip.
#[aerospike_macro::test]
async fn element_type_is_preserved_distinctly_through_the_server() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "types");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    // All four encode the value "one".
    let f16 = Vector::float16(vec![0x3c00]); // 1.0 in IEEE-754 binary16
    let i32v = Vector::int32(vec![1]);
    let f32v = Vector::float32(vec![1.0]);
    let f64v = Vector::float64(vec![1.0]);
    let bins = vec![
        as_bin!("f16", f16.clone()),
        as_bin!("i32", i32v.clone()),
        as_bin!("f32", f32v.clone()),
        as_bin!("f64", f64v.clone()),
    ];
    client.put(&write_policy, &key, &bins).await.unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();

    let element_type = |name: &str| match record.bins.get(name) {
        Some(Value::Vector(v)) => v.element_type(),
        other => panic!("expected vector bin {name:?}, got {other:?}"),
    };
    assert_eq!(element_type("f16"), VectorElementType::Float16);
    assert_eq!(element_type("i32"), VectorElementType::Int32);
    assert_eq!(element_type("f32"), VectorElementType::Float32);
    assert_eq!(element_type("f64"), VectorElementType::Float64);

    // And the values compare unequal across types even though they all mean 1.
    assert_eq!(record.bins.get("f32"), Some(&Value::Vector(f32v.clone())));
    assert_eq!(record.bins.get("f64"), Some(&Value::Vector(f64v.clone())));
    assert_ne!(Value::Vector(f32v), Value::Vector(f64v));

    client.close().await.unwrap();
}

// Non-finite and signed-zero float bits survive the server bit-exact (Value
// equality for vectors compares by IEEE-754 bit pattern), including the
// -0.0 / +0.0 distinction that a naive numeric copy could clobber.
#[aerospike_macro::test]
async fn signed_zero_and_non_finite_survive_the_server_bit_exact() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "bits");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    let f32v = Vector::float32(vec![-0.0, 0.0, f32::NAN, f32::INFINITY, f32::NEG_INFINITY]);
    let f64v = Vector::float64(vec![-0.0, 0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY]);
    let f16v = Vector::float16(vec![0x8000, 0x0000, 0x7e00, 0x7c00, 0xfc00]); // -0, +0, NaN, +Inf, -Inf
    client
        .put(
            &write_policy,
            &key,
            &[
                as_bin!("f32", f32v.clone()),
                as_bin!("f64", f64v.clone()),
                as_bin!("f16", f16v.clone()),
            ],
        )
        .await
        .unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("f32"), Some(&Value::Vector(f32v)));
    assert_eq!(record.bins.get("f64"), Some(&Value::Vector(f64v)));
    assert_eq!(record.bins.get("f16"), Some(&Value::Vector(f16v)));

    // -0.0 must not be flattened to +0.0 by the round trip.
    assert_ne!(
        Value::Vector(Vector::float32(vec![-0.0])),
        Value::Vector(Vector::float32(vec![0.0]))
    );

    client.close().await.unwrap();
}

// int32 vectors preserve the full signed 32-bit range through the server.
#[aerospike_macro::test]
async fn int32_vector_preserves_full_signed_range() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "i32-range");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    let v = Vector::int32(vec![i32::MIN, -1, 0, 1, i32::MAX]);
    client
        .put(&write_policy, &key, &[as_bin!("embedding", v.clone())])
        .await
        .unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("embedding"), Some(&Value::Vector(v)));

    client.close().await.unwrap();
}

// A vector large enough to exceed the 16-bit msgpack length boundary
// round-trips as a top-level bin.
#[aerospike_macro::test]
async fn large_vector_crossing_16bit_length_boundary_round_trips() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "large");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    // 9000 f64 elements => 8 + 9000*8 = 72008 bytes, well past 65_535.
    let data: Vec<f64> = (0..9000).map(|i| i as f64 * 0.5).collect();
    let v = Vector::float64(data);
    client
        .put(&write_policy, &key, &[as_bin!("embedding", v.clone())])
        .await
        .unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("embedding"), Some(&Value::Vector(v)));

    client.close().await.unwrap();
}

// A vector nested inside a CDT list bin round-trips through the server. The
// server treats the nested vector as an opaque msgpack byte string (the same
// scheme as a nested BLOB), so this exercises the CDT path, not the expression
// path.
#[aerospike_macro::test]
async fn vector_nested_in_a_list_bin_round_trips_through_server() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "list-nest");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    let list = Value::List(vec![
        Value::Int(1),
        Value::Vector(Vector::float32(vec![0.5, -1.5, 2.0])),
        Value::Vector(Vector::int32(vec![7, 8, 9])),
        Value::String("tail".to_string()),
    ]);
    client
        .put(&write_policy, &key, &[as_bin!("items", list.clone())])
        .await
        .unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("items"), Some(&list));

    client.close().await.unwrap();
}

// A vector nested as a map value round-trips through the server.
#[aerospike_macro::test]
async fn vector_nested_in_a_map_bin_round_trips_through_server() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "map-nest");
    let write_policy = WritePolicy::default();

    common::delete_durably(&client, &write_policy, &key).await.unwrap();

    let mut map = HashMap::new();
    map.insert(
        Value::from("a"),
        Value::Vector(Vector::float64(vec![1.5, 2.5])),
    );
    map.insert(Value::from("b"), Value::Vector(Vector::int32(vec![-3, 3])));
    let map_value = Value::HashMap(map);
    client
        .put(&write_policy, &key, &[as_bin!("by_key", map_value.clone())])
        .await
        .unwrap();

    let record = client.get(&Default::default(), &key, Bins::All).await.unwrap();
    // Value equality treats HashMap/OrderedMap with matching entries as equal,
    // so this holds regardless of how the server returns the map.
    assert_eq!(record.bins.get("by_key"), Some(&map_value));

    client.close().await.unwrap();
}

// A batch read returns records that contain vector bins.
#[aerospike_macro::test]
async fn batch_read_returns_records_with_vector_bins() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let write_policy = WritePolicy::default();

    let key1 = as_key!(namespace, &set_name, "batch-1");
    let key2 = as_key!(namespace, &set_name, "batch-2");
    let v1 = Vector::float32(vec![1.0, 2.0, 3.0]);
    let v2 = Vector::int32(vec![10, 20]);

    common::delete_durably(&client, &write_policy, &key1).await.unwrap();
    common::delete_durably(&client, &write_policy, &key2).await.unwrap();
    client
        .put(&write_policy, &key1, &[as_bin!("embedding", v1.clone())])
        .await
        .unwrap();
    client
        .put(&write_policy, &key2, &[as_bin!("embedding", v2.clone())])
        .await
        .unwrap();

    let bpr = BatchReadPolicy::default();
    let batch = vec![
        BatchOperation::read(&bpr, key1.clone(), Bins::All),
        BatchOperation::read(&bpr, key2.clone(), Bins::All),
    ];
    let mut results = client.batch(&BatchPolicy::default(), &batch).await.unwrap();

    let r1 = results.remove(0);
    assert_eq!(r1.key, key1);
    assert_eq!(
        r1.record.unwrap().bins.get("embedding"),
        Some(&Value::Vector(v1))
    );

    let r2 = results.remove(0);
    assert_eq!(r2.key, key2);
    assert_eq!(
        r2.record.unwrap().bins.get("embedding"),
        Some(&Value::Vector(v2))
    );

    client.close().await.unwrap();
}

// Reading a vector bin back through the expression engine (a plain
// BLOB-typed bin read, wire-identical to `blob_bin`) currently DOWNS the
// node. Root cause is server-side, in `rt_bin_translate`
// (aerospike-server/as/src/exp/exp_rt.c): its particle-type switch has no
// `AS_PARTICLE_TYPE_VECTOR` arm and falls through to `cf_crash(AS_EXP,
// "unexpected")`. Every expression that loads a vector bin routes through
// `rt_load_bin` -> `rt_bin_translate`, so a filter, `bin_exists`, `bin_type`,
// or read expression over a vector bin all abort asd. The sibling
// `rt_value_translate` was hardened for exactly this case (its default arm
// yields AS_EXP_UNK "rather than crash on a wire-reachable operand"); the fix
// is to give VECTOR the same treatment as BLOB in `rt_bin_translate`.
//
// This is ignored so it never runs against an unpatched node (executing it
// crashes the server). It doubles as a ready repro and a regression guard for
// once the server handles the VECTOR particle on the read path.
#[aerospike_macro::test]
#[ignore = "server bug: reading a vector bin via an expression crashes asd (rt_bin_translate cf_crash on AS_PARTICLE_TYPE_VECTOR)"]
async fn reading_a_vector_bin_via_expression_read_must_not_crash_the_server() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "vector-exp-read");

    let v = Vector::float32(vec![0.5, -1.5, 2.0]);
    write_query_vector(&client, &key, &v).await;

    // A plain read of the vector bin through the expression engine. This is the
    // exact operation that downs the node on current builds.
    let ops = vec![read_exp(
        "out",
        vector_bin("embedding".to_string()),
        ExpReadFlags::Default,
    )];
    let rec = client
        .operate(&WritePolicy::default(), &key, &ops)
        .await
        .expect("reading a vector bin via an expression must not error or crash the node");

    // On a fixed server the bin comes back; the exact returned Value type may
    // need tightening once the server settles the read-path representation.
    assert!(
        rec.bins.contains_key("out"),
        "expression read of a vector bin should return the bin"
    );

    client.close().await.unwrap();
}

// Same server defect from a different entry point: a *filter* expression that
// references a vector bin also routes through `rt_load_bin` ->
// `rt_bin_translate` and crashes the node. Gated for the same reason as the
// read above; a fixed server should simply apply the filter (here it matches,
// so the record is returned).
#[aerospike_macro::test]
#[ignore = "server bug: a filter expression over a vector bin crashes asd (rt_bin_translate cf_crash on AS_PARTICLE_TYPE_VECTOR)"]
async fn filter_expression_referencing_a_vector_bin_must_not_crash_the_server() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "vector-filter");

    let v = Vector::float32(vec![0.5, -1.5, 2.0]);
    write_query_vector(&client, &key, &v).await;

    // `bin_exists` compiles to `bin_type(...) != NULL`, which the server
    // evaluates via `exp_eval_bin_type` -> `rt_load_bin` -> `rt_bin_translate`
    // -> cf_crash for a vector bin. Any predicate touching the bin does this.
    let mut read_policy = ReadPolicy::default();
    read_policy.base_policy.filter_expression = Some(bin_exists("embedding".to_string()));

    let record = client
        .get(&read_policy, &key, Bins::All)
        .await
        .expect("a filter over a vector bin must not crash the node");
    assert_eq!(record.bins.get("embedding"), Some(&Value::Vector(v)));

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
