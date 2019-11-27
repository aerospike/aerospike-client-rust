// Copyright 2015-2018 Aerospike, Inc.
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

use common;
use env_logger;

use aerospike::operations;
use aerospike::operations::lists;
use aerospike::{Bins, ReadPolicy, Value, WritePolicy};

#[test]
fn cdt_list() {
    let _ = env_logger::try_init();

    let client = common::client();
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let policy = ReadPolicy::default();

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let val = as_list!("0", 1, 2.1f64);
    let wbin = as_bin!("bin", val.clone());
    let bins = vec![&wbin];

    client.delete(&wpolicy, &key).unwrap();

    client.put(&wpolicy, &key, &bins).unwrap();
    let rec = client.get(&policy, &key, Bins::All).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), val);

    let ops = &vec![lists::size("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(3));

    let values = vec![as_val!(9), as_val!(8), as_val!(7)];
    let ops = &vec![
        lists::insert_items("bin", 1, &values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(6, as_list!("0", 9, 8, 7, 1, 2.1f64))
    );

    let ops = &vec![lists::pop("bin", 0), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!("0", as_list!(9, 8, 7, 1, 2.1f64))
    );

    let ops = &vec![lists::pop_range("bin", 0, 2), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(9, 8, as_list!(7, 1, 2.1f64))
    );

    let ops = &vec![lists::pop_range_from("bin", 1), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(1, 2.1f64, as_list!(7))
    );

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items("bin", &values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(6, as_list!("0", 9, 8, 7, 1, 2.1f64))
    );

    let ops = &vec![lists::increment("bin", 1, 4)];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(13));

    let ops = &vec![lists::remove("bin", 1), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(1, as_list!("0", 8, 7, 1, 2.1f64))
    );

    let ops = &vec![lists::remove_range("bin", 1, 2), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(2, as_list!("0", 1, 2.1f64))
    );

    let ops = &vec![
        lists::remove_range_from("bin", -1),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(1, as_list!("0", 1)));

    let v = as_val!(2);
    let ops = &vec![lists::set("bin", -1, &v), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 2));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items("bin", &values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let ops = &vec![lists::trim("bin", 1, 1), operations::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(6, as_list!(9)));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![
        lists::clear("bin"),
        lists::append_items("bin", &values),
        operations::get_bin("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1))
    );

    let ops = &vec![lists::get("bin", 1)];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(9));

    let ops = &vec![lists::get_range("bin", 1, -1)];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(9, 8, 7, 1, 2.1f64, -1)
    );

    let ops = &vec![lists::get_range_from("bin", 2)];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7, 1, 2.1f64, -1));
}
