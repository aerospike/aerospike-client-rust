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

use std::collections::HashMap;

use crate::common;
use env_logger;

use aerospike::operations::maps;
use aerospike::{Bins, MapPolicy, MapReturnType, ReadPolicy, WritePolicy, as_list, as_val, as_key, as_map, as_bin};

#[test]
fn map_operations() {
    let _ = env_logger::try_init();

    let client = common::client();
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let mpolicy = MapPolicy::default();
    let rpolicy = ReadPolicy::default();

    let key = common::rand_str(10);
    let key = as_key!(namespace, set_name, &key);

    client.delete(&wpolicy, &key).unwrap();

    let val = as_map!("a" => 1, "b" => 2);
    let bin_name = "bin";
    let bin = as_bin!(bin_name, val);
    let bins = vec![&bin];

    client.put(&wpolicy, &key, &bins).unwrap();

    let (k, v) = (as_val!("c"), as_val!(3));
    let op = maps::put_item(&mpolicy, bin_name, &k, &v);
    let rec = client.operate(&wpolicy, &key, &[op]).unwrap();
    // returns size of map after put
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(3));

    let op = maps::size(bin_name);
    let rec = client.operate(&wpolicy, &key, &[op]).unwrap();
    // returns size of map
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(3));

    let rec = client.get(&rpolicy, &key, Bins::All).unwrap();
    assert_eq!(
        *rec.bins.get(bin_name).unwrap(),
        as_map!("a" => 1, "b" => 2, "c" => 3)
    );

    let mut items = HashMap::new();
    items.insert(as_val!("d"), as_val!(4));
    items.insert(as_val!("e"), as_val!(5));
    let op = maps::put_items(&mpolicy, bin_name, &items);
    let rec = client.operate(&wpolicy, &key, &[op]).unwrap();
    // returns size of map after put
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(5));

    let k = as_val!("e");
    let op = maps::remove_by_key(bin_name, &k, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &[op]).unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(5));

    let (k, i) = (as_val!("a"), as_val!(19));
    let op = maps::increment_value(&mpolicy, bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &[op]).unwrap();
    // returns value of the key after increment
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(20));

    let (k, i) = (as_val!("a"), as_val!(10));
    let op = maps::decrement_value(&mpolicy, bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &[op]).unwrap();
    // returns value of the key after decrement
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(10));

    let (k, i) = (as_val!("a"), as_val!(5));
    let dec = maps::decrement_value(&mpolicy, bin_name, &k, &i);
    let (k, i) = (as_val!("a"), as_val!(7));
    let inc = maps::increment_value(&mpolicy, bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &[dec, inc]).unwrap();
    // returns values from multiple ops returned as list
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_list!(5, 12));

    let op = maps::clear(bin_name);
    let rec = client.operate(&wpolicy, &key, &[op]).unwrap();
    // map_clear returns no result
    assert!(rec.bins.get(bin_name).is_none());

    client.delete(&wpolicy, &key).unwrap();
}
