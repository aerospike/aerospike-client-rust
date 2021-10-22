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
use env_logger;

use aerospike::operations::cdt_context::{ctx_map_key, ctx_map_key_create};
use aerospike::operations::{maps, MapOrder};
use aerospike::{
    as_bin, as_key, as_list, as_map, as_val, Bins, MapPolicy, MapReturnType, ReadPolicy,
    WritePolicy,
};

#[aerospike_macro::test]
async fn map_operations() {
    let _ = env_logger::try_init();

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
    let op = maps::put(&mpolicy, bin_name, &k, &v);
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
    let op = maps::put_items(&mpolicy, bin_name, &items);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns size of map after put
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(5));

    let k = as_val!("e");
    let op = maps::remove_by_key(bin_name, &k, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(5));

    let (k, i) = (as_val!("a"), as_val!(19));
    let op = maps::increment_value(&mpolicy, bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns value of the key after increment
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(20));

    let (k, i) = (as_val!("a"), as_val!(10));
    let op = maps::decrement_value(&mpolicy, bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // returns value of the key after decrement
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(10));

    let (k, i) = (as_val!("a"), as_val!(5));
    let dec = maps::decrement_value(&mpolicy, bin_name, &k, &i);
    let (k, i) = (as_val!("a"), as_val!(7));
    let inc = maps::increment_value(&mpolicy, bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &[dec, inc]).await.unwrap();
    // returns values from multiple ops returned as list
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(5, 12));

    let op = maps::clear(bin_name);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    // map_clear returns no result
    assert!(rec.bins.get(bin_name).is_none());

    client.delete(&wpolicy, &key).await.unwrap();

    let val = as_map!("a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5);
    let bin_name = "bin";
    let bin = as_bin!(bin_name, val);
    let bins = vec![bin];

    client.put(&wpolicy, &key, &bins.as_slice()).await.unwrap();

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
    let op = maps::get_by_value(bin_name, &val, MapReturnType::Index);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4));

    let beg = as_val!(3);
    let end = as_val!(5);
    let op = maps::get_by_value_range(bin_name, &beg, &end, MapReturnType::Count);
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
    let op = maps::get_by_key(bin_name, &mkey, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = as_val!("b");
    let mkey2 = as_val!("d");
    let op = maps::get_by_key_range(bin_name, &mkey, &mkey2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = vec![as_val!("b"), as_val!("d")];
    let op = maps::get_by_key_list(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = vec![as_val!(2), as_val!(3)];
    let op = maps::get_by_value_list(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = vec![as_val!("b"), as_val!("d")];
    let op = maps::remove_by_key_list(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = as_val!("a");
    let mkey2 = as_val!("c");
    let op = maps::remove_by_key_range(bin_name, &mkey, &mkey2, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(1));

    let mkey = as_val!(5);
    let op = maps::remove_by_value(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(1));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = vec![as_val!(4), as_val!(5)];
    let op = maps::remove_by_value_list(bin_name, &mkey, MapReturnType::Count);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(2));

    let mkey = as_val!(1);
    let mkey2 = as_val!(3);
    let op = maps::remove_by_value_range(bin_name, &mkey, &mkey2, MapReturnType::Count);
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
    let op = maps::remove_by_key_relative_index_range(bin_name, &mkey, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(4, 5));

    let mkey = as_val!("c");
    let op =
        maps::remove_by_key_relative_index_range_count(bin_name, &mkey, 0, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = as_val!(3);
    let op = maps::remove_by_value_relative_rank_range_count(
        bin_name,
        &mkey,
        2,
        2,
        MapReturnType::Value,
    );
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(5));

    let mkey = as_val!(2);
    let op = maps::remove_by_value_relative_rank_range(bin_name, &mkey, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4));

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let mkey = as_val!("a");
    let op = maps::get_by_key_relative_index_range(bin_name, &mkey, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(2, 3, 4, 5));

    let mkey = as_val!("a");
    let op =
        maps::get_by_key_relative_index_range_count(bin_name, &mkey, 1, 2, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(2, 3));

    let mkey = as_val!(2);
    let op = maps::get_by_value_relative_rank_range(bin_name, &mkey, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3, 4, 5));

    let mkey = as_val!(2);
    let op =
        maps::get_by_value_relative_rank_range_count(bin_name, &mkey, 1, 1, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(3));

    let mkey = as_val!("ctxtest");
    let mval = as_map!("x" => 7, "y" => 8, "z" => 9);
    let op = maps::put(&mpolicy, bin_name, &mkey, &mval);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let ctx = &vec![ctx_map_key(mkey)];
    let xkey = as_val!("y");
    let op = maps::get_by_key(bin_name, &xkey, MapReturnType::Value).set_context(ctx);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(8));

    let mkey = as_val!("ctxtest2");
    let ctx = &vec![ctx_map_key_create(mkey.clone(), MapOrder::KeyOrdered)];
    let xkey = as_val!("y");
    let xval = as_val!(8);
    let op = [maps::put(&mpolicy, bin_name, &xkey, &xval).set_context(ctx)];
    client.operate(&wpolicy, &key, &op).await.unwrap();
    let op = [maps::get_by_key(bin_name, &xkey, MapReturnType::Value).set_context(ctx)];
    let rec = client.operate(&wpolicy, &key, &op).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(8));

    let mkey2 = as_val!("ctxtest3");
    let ctx = &vec![
        ctx_map_key(mkey),
        ctx_map_key_create(mkey2, MapOrder::Unordered),
    ];
    let xkey = as_val!("c");
    let xval = as_val!(9);
    let op = [maps::put(&mpolicy, bin_name, &xkey, &xval).set_context(ctx)];
    client.operate(&wpolicy, &key, &op).await.unwrap();
    let op = [maps::get_by_key(bin_name, &xkey, MapReturnType::Value).set_context(ctx)];
    let rec = client.operate(&wpolicy, &key, &op).await.unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(9));

    client.close().await.unwrap();
}
