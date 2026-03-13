// Copyright 2015-2020 Aerospike, Inc.
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

use std::collections::HashMap;

use crate::common;

use aerospike::operations::cdt_context::{ctx_map_key, ctx_map_key_create};
use aerospike::operations::{maps, MapOrder};
use aerospike::{
    as_bin, as_key, as_list, as_map, as_ord_map, as_val, as_values, Bins, MapPolicy, MapReturnType,
    MapWriteFlags, MapWriteMode, ReadPolicy, Value, WritePolicy,
};

#[aerospike_macro::test]
async fn map_operations() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let mpolicy = MapPolicy::default();
    let rpolicy = ReadPolicy::default();

    let key = common::rand_str(10);
    let key = as_key!(namespace, set_name, &key);

    client.delete(&wpolicy, &key).await.unwrap();

    let val = as_map!("a" => 1, "b" => 2);
    let bin_name = "bin";
    let bin = as_bin!(bin_name, val);
    let bins = vec![bin];

    client.put(&wpolicy, &key, &bins).await.unwrap();

    let (k, v) = (as_val!("c"), as_val!(3));
    let op = maps::put(&mpolicy, bin_name, k, v);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns size of map after put
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(3));

    let op = maps::size(bin_name);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns size of map
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(3));

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    assert_eq!(
        *rec.bins.get(bin_name).unwrap(),
        as_map!("a" => 1, "b" => 2, "c" => 3)
    );

    let mut items = HashMap::new();
    items.insert(as_val!("d"), as_val!(4));
    items.insert(as_val!("e"), as_val!(5));
    let op = maps::put_items(&mpolicy, bin_name, items);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns size of map after put
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(5));

    let k = as_val!("e");
    let op = maps::remove_by_key(bin_name, k, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(5));

    let (k, i) = (as_val!("a"), as_val!(19));
    let op = maps::increment_value(&mpolicy, bin_name, k, i);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns value of the key after increment
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(20));

    let (k, i) = (as_val!("a"), as_val!(10));
    let op = maps::decrement_value(&mpolicy, bin_name, k, i);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns value of the key after decrement
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(10));

    let (k, i) = (as_val!("a"), as_val!(5));
    let dec = maps::decrement_value(&mpolicy, bin_name, k, i);
    let (k, i) = (as_val!("a"), as_val!(7));
    let inc = maps::increment_value(&mpolicy, bin_name, k, i);
    let rec = client.operate(&wpolicy, &key, &[dec, inc]).await.unwrap();
    // returns values from multiple ops returned as list
    assert_eq!(
        *rec.bins.get(bin_name).unwrap(),
        Value::MultiResult(as_values!(5, 12))
    );

    let op = maps::clear(bin_name);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // map_clear returns no result
    assert!(rec.bins.get(bin_name).is_none());

    // ---------------------------------------------------------------------------------

    client.delete(&wpolicy, &key).await.unwrap();

    let val = as_ord_map!("a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5);
    let bin_name = "bin";
    let bin = as_bin!(bin_name, val);
    let bins = vec![bin];

    client.put(&wpolicy, &key, &bins.as_slice()).await.unwrap();

    // ---------------------------------------------------------------------------------

    client.delete(&wpolicy, &key).await.unwrap();

    let val = as_map!("a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5);
    let bin_name = "bin";
    let bin = as_bin!(bin_name, val);
    let bins = vec![bin];

    client.put(&wpolicy, &key, &bins.as_slice()).await.unwrap();

    let op = maps::get_by_index(bin_name, 0, MapReturnType::UnorderedMap);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_map!("a" => 1));

    let op = maps::get_by_index(bin_name, 0, MapReturnType::OrderedMap);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_ord_map!("a" => 1));

    let op = maps::get_by_index(bin_name, 0, MapReturnType::KeyValue);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(
        *rec.bins.get(bin_name).unwrap(),
        Value::KeyValueList(vec![(as_val!("a"), as_val!(1))])
    );

    let op = maps::get_by_index(bin_name, 0, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(1));

    let op = maps::get_by_index_range(bin_name, 1, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(2, 3));

    let op = maps::get_by_index_range_from(bin_name, 3, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4, 5));

    let val = as_val!(5);
    let op = maps::get_by_value(bin_name, val, MapReturnType::Index);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4));

    let beg = as_val!(3);
    let end = as_val!(5);
    let op = maps::get_by_value_range(bin_name, beg, end, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let op = maps::get_by_rank(bin_name, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(3));

    let op = maps::get_by_rank_range(bin_name, 2, 3, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4, 5));

    let op = maps::get_by_rank_range_from(bin_name, 2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(3));

    let mkey = as_val!("b");
    let op = maps::get_by_key(bin_name, mkey, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = as_val!("b");
    let mkey2 = as_val!("d");
    let op = maps::get_by_key_range(bin_name, mkey, mkey2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = vec![as_val!("b"), as_val!("d")];
    let op = maps::get_by_key_list(bin_name, mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = vec![as_val!(2), as_val!(3)];
    let op = maps::get_by_value_list(bin_name, mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = vec![as_val!("b"), as_val!("d")];
    let op = maps::remove_by_key_list(bin_name, mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = as_val!("a");
    let mkey2 = as_val!("c");
    let op = maps::remove_by_key_range(bin_name, mkey, mkey2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(1));

    let mkey = as_val!(5);
    let op = maps::remove_by_value(bin_name, mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(1));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = vec![as_val!(4), as_val!(5)];
    let op = maps::remove_by_value_list(bin_name, mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = as_val!(1);
    let mkey2 = as_val!(3);
    let op = maps::remove_by_value_range(bin_name, mkey, mkey2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let op = maps::remove_by_index(bin_name, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let op = maps::remove_by_index_range(bin_name, 1, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4));

    let op = maps::remove_by_index_range_from(bin_name, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(5));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let op = maps::remove_by_rank(bin_name, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let op = maps::remove_by_rank_range(bin_name, 1, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let op = maps::remove_by_rank_range_from(bin_name, 3, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4, 5));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = as_val!("b");
    let op = maps::remove_by_key_relative_index_range(bin_name, mkey, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4, 5));

    let mkey = as_val!("c");
    let op =
        maps::remove_by_key_relative_index_range_count(bin_name, mkey, 0, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = as_val!(3);
    let op =
        maps::remove_by_value_relative_rank_range_count(bin_name, mkey, 2, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(5));

    let mkey = as_val!(2);
    let op = maps::remove_by_value_relative_rank_range(bin_name, mkey, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = as_val!("a");
    let op = maps::get_by_key_relative_index_range(bin_name, mkey, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(2, 3, 4, 5));

    let mkey = as_val!("a");
    let op =
        maps::get_by_key_relative_index_range_count(bin_name, mkey, 1, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(2, 3));

    let mkey = as_val!(2);
    let op = maps::get_by_value_relative_rank_range(bin_name, mkey, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4, 5));

    let mkey = as_val!(2);
    let op =
        maps::get_by_value_relative_rank_range_count(bin_name, mkey, 1, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3));

    let mkey = as_val!("ctxtest");
    let mval = as_map!("x" => 7, "y" => 8, "z" => 9);
    let op = maps::put(&mpolicy, bin_name, mkey.clone(), mval);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let ctx = vec![ctx_map_key(mkey)];
    let xkey = as_val!("y");
    let op = maps::get_by_key(bin_name, xkey, MapReturnType::Value).set_context(ctx);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(8));

    let mkey = as_val!("ctxtest2");
    let ctx = vec![ctx_map_key_create(mkey.clone(), MapOrder::KeyOrdered)];
    let xkey = as_val!("y");
    let xval = as_val!(8);
    let op = [maps::put(&mpolicy, bin_name, xkey.clone(), xval).set_context(ctx.clone())];
    client.operate(&wpolicy, &key, &op).await.unwrap();
    let op = [maps::get_by_key(bin_name, xkey, MapReturnType::Value).set_context(ctx)];
    let rec = client.operate(&wpolicy, &key, &op).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(8));

    let mkey2 = as_val!("ctxtest3");
    let ctx = vec![
        ctx_map_key(mkey),
        ctx_map_key_create(mkey2, MapOrder::Unordered),
    ];
    let xkey = as_val!("c");
    let xval = as_val!(9);
    let op = [maps::put(&mpolicy, bin_name, xkey.clone(), xval).set_context(ctx.clone())];
    client.operate(&wpolicy, &key, &op).await.unwrap();
    let op = [maps::get_by_key(bin_name, xkey, MapReturnType::Value).set_context(ctx)];
    let rec = client.operate(&wpolicy, &key, &op).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(9));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn map_operations_wildcard() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let mpolicy = MapPolicy::default();

    let key = common::rand_str(10);
    let key = as_key!(namespace, set_name, &key);

    client.delete(&wpolicy, &key).await.unwrap();

    let mut items = HashMap::new();
    items.insert(as_val!(4), as_list!("John", 55));
    items.insert(as_val!(5), as_list!("Jim", 95));
    items.insert(as_val!(9), as_list!("Joe", 80));

    let op = maps::put_items(&mpolicy, "bin", items);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns size of map after put
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(3));

    let val = as_list!(Value::from("Joe"), Value::Wildcard);
    let ops = &vec![maps::get_by_value("bin", val, MapReturnType::Key)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(9));
}

#[aerospike_macro::test]
async fn map_create_op() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let mpolicy = MapPolicy::default();

    let key = as_key!(namespace, set_name, "map_create");

    client.delete(&wpolicy, &key).await.unwrap();

    // Create a nested map using map create with context
    // First put a top-level key
    let op = maps::put(&mpolicy, "bin", as_val!("key1"), as_val!(1));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Create a nested map under "nested" key using ctx_map_key_create
    let ctx = vec![ctx_map_key_create(as_val!("nested"), MapOrder::KeyOrdered)];
    let op = maps::put(&mpolicy, "bin", as_val!("a"), as_val!(10)).set_context(ctx);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Verify the nested map value
    let ctx = vec![ctx_map_key(as_val!("nested"))];
    let op = maps::get_by_key("bin", as_val!("a"), MapReturnType::Value).set_context(ctx);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(10));

    // Also test maps::create with empty context (should be same as set_order)
    let key2 = as_key!(namespace, set_name, "map_create_empty_ctx");
    client.delete(&wpolicy, &key2).await.unwrap();

    let op = maps::create("bin", MapOrder::KeyOrdered, vec![]);
    client.operate(&wpolicy, &key2, &[op]).await.unwrap();

    // Put some values and verify they come back in key order
    let op = maps::put(&mpolicy, "bin", as_val!("b"), as_val!(2));
    client.operate(&wpolicy, &key2, &[op]).await.unwrap();
    let op = maps::put(&mpolicy, "bin", as_val!("a"), as_val!(1));
    client.operate(&wpolicy, &key2, &[op]).await.unwrap();

    let op = maps::get_by_index_range_from("bin", 0, MapReturnType::Key);
    let rec = client.operate(&wpolicy, &key2, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("a", "b"));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn map_create_with_index_op() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let mpolicy = MapPolicy::default();

    let key = as_key!(namespace, set_name, "map_create_idx");

    client.delete(&wpolicy, &key).await.unwrap();

    // Create key-ordered map with persisted index
    let op = maps::create_with_index("bin", MapOrder::KeyOrdered);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Put values
    let op = maps::put(&mpolicy, "bin", as_val!("c"), as_val!(3));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();
    let op = maps::put(&mpolicy, "bin", as_val!("a"), as_val!(1));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();
    let op = maps::put(&mpolicy, "bin", as_val!("b"), as_val!(2));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Verify map is key-ordered
    let op = maps::get_by_index_range_from("bin", 0, MapReturnType::Key);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("a", "b", "c"));

    // Verify values are correct
    let op = maps::get_by_key("bin", as_val!("b"), MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(2));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn map_set_policy_op() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let mpolicy = MapPolicy::default();

    let key = as_key!(namespace, set_name, "map_set_policy");

    client.delete(&wpolicy, &key).await.unwrap();

    // Create an unordered map with some data
    let mut items = HashMap::new();
    items.insert(as_val!("c"), as_val!(3));
    items.insert(as_val!("a"), as_val!(1));
    items.insert(as_val!("b"), as_val!(2));
    let op = maps::put_items(&mpolicy, "bin", items);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Change to key-ordered using set_policy
    let ordered_policy = MapPolicy::new(MapOrder::KeyOrdered, MapWriteMode::Update);
    let op = maps::set_policy(&ordered_policy, "bin", vec![]);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Verify the map is now key-ordered
    let op = maps::get_by_index_range_from("bin", 0, MapReturnType::Key);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("a", "b", "c"));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn map_put_with_flags_create_only() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "map_flags_create");

    client.delete(&wpolicy, &key).await.unwrap();

    // Use CREATE_ONLY flag - first put should succeed
    let policy = MapPolicy::new_with_flags(MapOrder::Unordered, MapWriteFlags::CREATE_ONLY);
    let op = maps::put(&policy, "bin", as_val!("a"), as_val!(1));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Second put with same key should fail with CREATE_ONLY
    let op = maps::put(&policy, "bin", as_val!("a"), as_val!(2));
    let result = client.operate(&wpolicy, &key, &[op]).await;
    assert!(result.is_err(), "CREATE_ONLY should fail on existing key");

    // Verify original value preserved
    let op = maps::get_by_key("bin", as_val!("a"), MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(1));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn map_put_with_flags_no_fail() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "map_flags_nofail");

    client.delete(&wpolicy, &key).await.unwrap();

    // Create initial map entry
    let policy = MapPolicy::default();
    let op = maps::put(&policy, "bin", as_val!("a"), as_val!(1));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Use CREATE_ONLY | NO_FAIL - should silently skip existing key
    let policy = MapPolicy::new_with_flags(
        MapOrder::Unordered,
        MapWriteFlags::CREATE_ONLY | MapWriteFlags::NO_FAIL,
    );
    let op = maps::put(&policy, "bin", as_val!("a"), as_val!(99));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Verify original value preserved (silently skipped, no error)
    let op = maps::get_by_key("bin", as_val!("a"), MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(1));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn map_put_with_flags_update_only() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "map_flags_update");

    client.delete(&wpolicy, &key).await.unwrap();

    // Create initial map entry
    let policy = MapPolicy::default();
    let op = maps::put(&policy, "bin", as_val!("a"), as_val!(1));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // UPDATE_ONLY on existing key should succeed
    let policy = MapPolicy::new_with_flags(MapOrder::Unordered, MapWriteFlags::UPDATE_ONLY);
    let op = maps::put(&policy, "bin", as_val!("a"), as_val!(2));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Verify value updated
    let op = maps::get_by_key("bin", as_val!("a"), MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(2));

    // UPDATE_ONLY on non-existing key should fail
    let op = maps::put(&policy, "bin", as_val!("z"), as_val!(99));
    let result = client.operate(&wpolicy, &key, &[op]).await;
    assert!(
        result.is_err(),
        "UPDATE_ONLY should fail on non-existing key"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn map_new_with_flags_and_persisted_index() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "map_persist_idx");

    client.delete(&wpolicy, &key).await.unwrap();

    // Create key-ordered map with persisted index via flags constructor
    let policy =
        MapPolicy::new_with_flags_and_persisted_index(MapOrder::KeyOrdered, MapWriteFlags::DEFAULT);
    let op = maps::put(&policy, "bin", as_val!("c"), as_val!(3));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let op = maps::put(&policy, "bin", as_val!("a"), as_val!(1));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let op = maps::put(&policy, "bin", as_val!("b"), as_val!(2));
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // Verify map is key-ordered
    let op = maps::get_by_index_range_from("bin", 0, MapReturnType::Key);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("a", "b", "c"));

    client.close().await.unwrap();
}
