// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0

//! Verifies that `Ord for Value` matches the server's canonical value
//! ordering: the client-side sort of a mixed-type list must be
//! element-for-element identical to what the server's list `sort`
//! operation produces, and map keys must round-trip in the client's
//! sort order.

use crate::common;
use aerospike::operations::lists;
use aerospike::{
    as_bin, as_key, as_map, as_val, Bins, IndexMap, ListSortFlags, ReadPolicy, Value, WritePolicy,
};

/// The mixed-type corpus: every orderable type, with within-type edge
/// cases (negatives, prefixes, equal-length maps, int/float non-mixing).
fn corpus() -> Vec<Value> {
    vec![
        Value::from("zz"),
        Value::from(3),
        Value::Nil,
        Value::Bool(true),
        Value::Bool(false),
        Value::from(-7),
        Value::from(2.5),
        Value::from(-0.5),
        Value::from("a"),
        Value::Blob(vec![9u8]),
        Value::Blob(vec![1u8, 2u8]),
        Value::List(vec![Value::from(1), Value::from(2)]),
        Value::List(vec![Value::from(1)]),
        Value::List(vec![Value::from(0), Value::from(9)]),
        Value::List(vec![]),
        as_map!("a" => 1),
        as_map!("b" => 0),
        as_map!("a" => 1, "b" => 2),
        as_map!(),
        Value::GeoJSON(r#"{"type":"Point","coordinates":[1.0,1.0]}"#.into()),
        Value::from(1000),
        Value::from(2),
        Value::from(2.0),
    ]
}

#[aerospike_macro::test]
async fn value_ord_matches_server_sort_order() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let wp = WritePolicy::default();

    let values = corpus();
    let key = as_key!(namespace, set_name, "value_ord");
    client
        .put(&wp, &key, &[as_bin!("l", Value::List(values.clone()))])
        .await
        .unwrap();
    let op = lists::sort("l", ListSortFlags::Default);
    client.operate(&wp, &key, &[op]).await.unwrap();
    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    let Some(Value::List(server_sorted)) = rec.bins.get("l") else {
        panic!("expected a list back, got {:?}", rec.bins.get("l"));
    };

    let mut client_sorted = values;
    client_sorted.sort();

    assert_eq!(server_sorted.len(), client_sorted.len());
    for (i, (server, client_v)) in server_sorted.iter().zip(client_sorted.iter()).enumerate() {
        assert_eq!(
            server, client_v,
            "order mismatch at position {i}: server={server:?} client={client_v:?}"
        );
    }
}

#[aerospike_macro::test]
async fn value_ord_matches_server_map_key_order() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let wp = WritePolicy::default();

    // The three legal key types with within-type edge cases.
    let keys: Vec<Value> = vec![
        Value::from(7),
        Value::from(-5),
        Value::from(0),
        Value::from("b"),
        Value::from("a"),
        Value::from(""),
        Value::Blob(vec![1u8]),
        Value::Blob(vec![0u8, 255u8]),
        Value::Blob(vec![]),
    ];

    let mut written: IndexMap<Value, Value> = IndexMap::new();
    for (i, k) in keys.iter().enumerate() {
        written.insert(k.clone(), Value::from(i as i64));
    }
    let key = as_key!(namespace, set_name, "key_ord");
    client
        .put(&wp, &key, &[as_bin!("m", Value::OrderedMap(written))])
        .await
        .unwrap();

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    let Some(Value::OrderedMap(back)) = rec.bins.get("m") else {
        panic!("expected an OrderedMap back, got {:?}", rec.bins.get("m"));
    };

    let mut client_sorted = keys;
    client_sorted.sort();
    let server_order: Vec<&Value> = back.keys().collect();
    assert_eq!(
        server_order,
        client_sorted.iter().collect::<Vec<_>>(),
        "server key order must equal the client's Value sort order"
    );
    let _ = as_val!(0); // keep macro import exercised
}

#[aerospike_macro::test]
async fn map_order_policy_selects_variant_not_order() {
    use aerospike::operations::{maps, MapOrder};
    use aerospike::{MapPolicy, MapWriteMode};

    // Verified server behavior (8.1): the MapOrder in a MapPolicy
    // controls the wire representation of returns, not the pair order —
    // entries come back in canonical key order for BOTH settings, and
    // insertion order is never preserved. Unordered maps decode as
    // OrderedMap (no K-ordered flag), K-ordered maps as SortedMap.
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let wp = WritePolicy::default();

    for (label, policy, expect_sorted_variant) in [
        (
            "unordered",
            MapPolicy::new(MapOrder::Unordered, MapWriteMode::Update),
            false,
        ),
        (
            "key-ordered",
            MapPolicy::new(MapOrder::KeyOrdered, MapWriteMode::Update),
            true,
        ),
    ] {
        let key = as_key!(namespace, set_name, label);
        client.delete(&wp, &key).await.unwrap();
        // Insert one key per operation, in deliberately non-sorted order.
        for k in ["z", "m", "a", "q"] {
            let op = maps::put(&policy, "m", as_val!(k), as_val!(1));
            client.operate(&wp, &key, &[op]).await.unwrap();
        }
        let rec = client
            .get(&ReadPolicy::default(), &key, Bins::All)
            .await
            .unwrap();
        let keys: Vec<String> = match rec.bins.get("m") {
            Some(Value::OrderedMap(m)) if !expect_sorted_variant => {
                m.keys().map(ToString::to_string).collect()
            }
            Some(Value::SortedMap(m)) if expect_sorted_variant => {
                m.keys().map(ToString::to_string).collect()
            }
            other => panic!("{label}: unexpected decode variant: {other:?}"),
        };
        assert_eq!(
            keys,
            vec!["a", "m", "q", "z"],
            "{label}: entries must return in canonical key order"
        );
    }
}
