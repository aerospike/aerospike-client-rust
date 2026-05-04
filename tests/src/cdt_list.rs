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

use crate::common;

use aerospike::operations;
use aerospike::operations::lists;
use aerospike::operations::lists::{
    ListOrderType, ListPolicy, ListReturnType, ListSortFlags, ListWriteFlags,
};
use aerospike::{as_bin, as_key, as_list, as_val, as_values, Bins, ReadPolicy, Value, WritePolicy};

#[aerospike_macro::test]
fn cdt_list() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let policy = ReadPolicy::default();

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let val = as_list!("0", 1, 2.1f64);
    let wbin = as_bin!("bin", val.clone());
    let bins = vec![wbin];
    let lpolicy = ListPolicy::default();

    let _ = common::delete_durably(&client, &wpolicy, &key).await;

    client.put(&wpolicy, &key, &bins).await.unwrap();
    let rec = client.get(&policy, &key, Bins::All).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), val);

    let ops = &vec![lists::size("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(3));

    let values = vec![as_val!(9), as_val!(8), as_val!(7)];
    let ops = &vec![
        lists::insert_items(&lpolicy, "bin", 1, values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(6, as_list!("0", 9, 8, 7, 1, 2.1f64)))
    );

    let ops = &vec![lists::pop("bin", 0), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!("0", as_list!(9, 8, 7, 1, 2.1f64)))
    );

    let ops = &vec![lists::pop_range("bin", 0, 2), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(as_list!(9, 8), as_list!(7, 1, 2.1f64)))
    );

    let ops = &vec![lists::pop_range_from("bin", 1), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(as_list!(1, 2.1f64), as_list!(7)))
    );

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(6, as_list!("0", 9, 8, 7, 1, 2.1f64)))
    );

    let ops = &vec![lists::increment(&lpolicy, "bin", 1, 4)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(13));

    let ops = &vec![lists::remove("bin", 1), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(1, as_list!("0", 8, 7, 1, 2.1f64)))
    );

    let ops = &vec![lists::remove_range("bin", 1, 2), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(2, as_list!("0", 1, 2.1f64)))
    );

    let ops = &vec![
        lists::remove_range_from("bin", -1),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(1, as_list!("0", 1)))
    );

    let v = as_val!(2);
    let ops = &vec![lists::set("bin", -1, v), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 2));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let ops = &vec![lists::trim("bin", 1, 1), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(6, as_list!(9)))
    );

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let ops = &vec![lists::get("bin", 1)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(9));

    let ops = &vec![lists::get_range("bin", 1, -1)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(9, 8, 7, 1, 2.1f64, -1)
    );

    let ops = &vec![lists::get_range_from("bin", 2)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7, 1, 2.1f64, -1));

    let rval = Value::from(9);
    let ops = &vec![lists::remove_by_value("bin", rval, ListReturnType::Count)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(1));

    let rval = vec![Value::from(8), Value::from(7)];
    let ops = &vec![lists::remove_by_value_list(
        "bin",
        rval,
        ListReturnType::Count,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(2));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let beg = Value::from(7);
    let end = Value::from(9);
    let ops = &vec![lists::remove_by_value_range(
        "bin",
        ListReturnType::Count,
        beg,
        end,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(2));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let ops = &vec![lists::sort("bin", ListSortFlags::Default)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let ops = &vec![operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(-1, 1, 7, 8, 9, "0", 2.1f64)
    );

    let ops = &vec![lists::remove_by_index("bin", 1, ListReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(1));

    let ops = &vec![lists::remove_by_index_range(
        "bin",
        4,
        ListReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 2.1f64));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let ops = &vec![lists::remove_by_index_range_count(
        "bin",
        0,
        2,
        ListReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 9));

    let ops = &vec![lists::remove_by_rank("bin", 2, ListReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(7));

    let ops = &vec![lists::remove_by_rank_range(
        "bin",
        2,
        ListReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 2.1f64));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let ops = &vec![lists::remove_by_rank_range_count(
        "bin",
        2,
        2,
        ListReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let val = Value::from(1);
    let ops = &vec![lists::remove_by_value_relative_rank_range(
        "bin",
        ListReturnType::Values,
        val,
        1,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, 8, 9, "0", 2.1f64)
    );

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let val = Value::from(1);
    let ops = &vec![lists::remove_by_value_relative_rank_range_count(
        "bin",
        ListReturnType::Values,
        val,
        1,
        2,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let val = Value::from(1);
    let ops = &vec![lists::get_by_value_relative_rank_range_count(
        "bin",
        val,
        2,
        2,
        ListReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 9));

    let val = Value::from(1);
    let ops = &vec![lists::get_by_value("bin", val, ListReturnType::Count)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(1));

    let val = vec![Value::from(1), Value::from("0")];
    let ops = &vec![lists::get_by_value_list("bin", val, ListReturnType::Count)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(2));

    let beg = Value::from(1);
    let end = Value::from(9);
    let ops = &vec![lists::get_by_value_range(
        "bin",
        beg,
        end,
        ListReturnType::Count,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(3));

    let ops = &vec![lists::get_by_index("bin", 3, ListReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(7));

    let ops = &vec![lists::get_by_index_range("bin", 3, ListReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(7, 1, 2.1f64, -1));

    let ops = &vec![lists::get_by_index_range_count(
        "bin",
        0,
        2,
        ListReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 9));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items(&lpolicy, "bin", values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)))
    );

    let ops = &vec![lists::get_by_rank("bin", 2, ListReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(7));

    let ops = &vec![lists::get_by_rank_range("bin", 4, ListReturnType::Values)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(9, "0", 2.1f64));

    let ops = &vec![lists::get_by_rank_range_count(
        "bin",
        2,
        2,
        ListReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7));

    let val = Value::from(1);
    let ops = &vec![lists::get_by_value_relative_rank_range(
        "bin",
        val,
        2,
        ListReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 9, "0", 2.1f64));

    let val = Value::from(1);
    let ops = &vec![lists::get_by_value_relative_rank_range_count(
        "bin",
        val,
        2,
        2,
        ListReturnType::Values,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 9));
    client.close().await.unwrap();
}

#[aerospike_macro::test]
fn cdt_list_wildcard() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let lpolicy = ListPolicy::default();

    let _ = common::delete_durably(&client, &wpolicy, &key).await;

    let list = vec![
        as_list!("John", 55),
        as_list!("Jim", 95),
        as_list!("Joe", 80),
    ];

    let val = as_list!(Value::from("Jim"), Value::Wildcard);
    let ops = &vec![
        lists::append_items(&lpolicy, "bin", list),
        lists::get_by_value("bin", val, ListReturnType::Values),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(as_values!(3, as_list!(as_list!("Jim", 95))))
    );
}

#[aerospike_macro::test]
fn cdt_list_create_with_index() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "create_with_index");
    let lpolicy = ListPolicy::default();

    let _ = common::delete_durably(&client, &wpolicy, &key).await;

    // Create an ordered list with persisted index, then populate and verify ordering
    let ops = &vec![
        lists::create_with_index("bin", ListOrderType::Ordered),
        lists::append(&lpolicy, "bin", as_val!(3)),
        lists::append(&lpolicy, "bin", as_val!(1)),
        lists::append(&lpolicy, "bin", as_val!(2)),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    // Ordered list should sort: [1, 2, 3]. Last result is the get_bin.
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(vec![as_val!(1), as_val!(2), as_val!(3), as_list!(1, 2, 3)])
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
fn cdt_list_set_order_with_index() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "set_order_with_index");
    let lpolicy = ListPolicy::default();

    let _ = common::delete_durably(&client, &wpolicy, &key).await;

    // Create an unordered list first
    let values = as_values![3, 1, 2];
    let ops = &vec![lists::append_items(&lpolicy, "bin", values)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    // Now set it to ordered with persisted index
    let ops = &vec![
        lists::set_order_with_index("bin", ListOrderType::Ordered),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    // After setting to ordered, list should be sorted
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(1, 2, 3));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
fn cdt_list_set_with_policy() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "set_with_policy");
    let lpolicy = ListPolicy::default();

    let _ = common::delete_durably(&client, &wpolicy, &key).await;

    // Create list [1, 2, 3]
    let values = as_values![1, 2, 3];
    let ops = &vec![lists::append_items(&lpolicy, "bin", values)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    // Set index 1 to value 99 using set_with_policy
    let set_policy = ListPolicy::new(ListOrderType::Unordered, ListWriteFlags::Default);
    let ops = &vec![
        lists::set_with_policy(&set_policy, "bin", 1, as_val!(99)),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(1, 99, 3));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
fn cdt_list_increment_by_one() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "increment_by_one");
    let lpolicy = ListPolicy::default();

    let _ = common::delete_durably(&client, &wpolicy, &key).await;

    // Create list [10, 20, 30]
    let values = as_values![10, 20, 30];
    let ops = &vec![lists::append_items(&lpolicy, "bin", values)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    // Increment index 1 by one (20 -> 21)
    let ops = &vec![lists::increment_by_one("bin", 1)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    // increment returns the new value
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(21));

    // Verify the list
    let ops = &vec![operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(10, 21, 30));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
fn cdt_list_increment_by_one_with_policy() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "incr_by_one_policy");
    let lpolicy = ListPolicy::default();

    let _ = common::delete_durably(&client, &wpolicy, &key).await;

    // Create list [10, 20, 30]
    let values = as_values![10, 20, 30];
    let ops = &vec![lists::append_items(&lpolicy, "bin", values)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    // Increment index 0 by one with policy (10 -> 11)
    let ops = &vec![lists::increment_by_one_with_policy(&lpolicy, "bin", 0)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(11));

    // Verify the list
    let ops = &vec![operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(11, 20, 30));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn list_get_by_value_range_nil_end_returns_empty() {
    let client = common::client().await;
    let ns = common::namespace();
    let set = "lv_range_nil";

    let wpolicy = WritePolicy::default();
    let key = as_key!(ns, set, "list_key1");

    let list = as_list!(7, 6, 5, 8, 9, 10);
    let bins = vec![as_bin!("int_bin", list)];
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    client.put(&wpolicy, &key, &bins).await.unwrap();

    // Get
    let op1 = lists::get_by_value_range(
        "int_bin",
        Value::from(7),
        Value::from(9),
        ListReturnType::Values,
    ); // expect: [7, 8]
    let op2 = lists::get_by_value_range(
        "int_bin",
        Value::from(7),
        Value::Nil,
        ListReturnType::Values,
    ); // expect: [7, 8, 9, 10]
    let op3 =
        lists::get_by_value_range("int_bin", Value::from(7), Value::Nil, ListReturnType::Index); // expect: [0, 3, 4, 5]
    let op4 =
        lists::get_by_value_range("int_bin", Value::from(7), Value::Nil, ListReturnType::Rank); // expect: [2, 3, 4, 5]
    let op5 = lists::get_by_value_range(
        "int_bin",
        Value::Nil,
        Value::from(9),
        ListReturnType::Values,
    ); // expect: [7, 6, 5, 8]
    let ops = &vec![op1, op2, op3, op4, op5];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        rec.bins.get("int_bin").unwrap(),
        &aerospike::Value::MultiResult(vec![
            as_list![7, 8],
            as_list![7, 8, 9, 10],
            as_list![0, 3, 4, 5],
            as_list![2, 3, 4, 5],
            as_list![7, 6, 5, 8],
        ])
    );

    // Remove
    let op6 =
        lists::remove_by_value_range("int_bin", ListReturnType::Index, Value::from(7), Value::Nil); // expect: [0, 3, 4, 5]
    let ops = &vec![op6];
    let rec2 = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(rec2.bins.get("int_bin").unwrap(), &as_list!(0, 3, 4, 5));

    let rec3 = client
        .get(&aerospike::ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap(); // expect: [6, 5]
    assert_eq!(rec3.bins.get("int_bin").unwrap(), &as_list!(6, 5));

    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn cdt_list_create_persistent_top_level() {
    // Java parity: ListOperation.create(name, order, pad, persistIndex, ctx)
    // exposes both `pad` (nested-only) and `persist_index` (top-level only).
    // At the top level the persist_index bit takes effect on the order
    // attribute byte; this test exercises that path.
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "create_persistent");
    let lpolicy = ListPolicy::default();

    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    // Create a top-level ordered list with the persisted index enabled,
    // append unsorted values, then verify the list is sorted on read
    // (proving the order attribute was applied server-side).
    let ops = &vec![
        lists::create_persistent("bin", ListOrderType::Ordered, false, true),
        lists::append(&lpolicy, "bin", as_val!(3)),
        lists::append(&lpolicy, "bin", as_val!(1)),
        lists::append(&lpolicy, "bin", as_val!(2)),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::MultiResult(vec![as_val!(1), as_val!(2), as_val!(3), as_list!(1, 2, 3)])
    );

    client.close().await.unwrap();
}
