// Copyright 2016 Aerospike, Inc.
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

use aerospike::{Key, Bin};
use aerospike::{ReadPolicy, WritePolicy, MapPolicy};
use aerospike::Operation;
use aerospike::MapReturnType;
use aerospike::value::*;

use common1;

#[test]
fn map_operations() {
    let ref client = common1::GLOBAL_CLIENT;
    let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
    let set_name = &common1::rand_str(10);

    let wpolicy = WritePolicy::default();
    let mpolicy = MapPolicy::default();
    let rpolicy = ReadPolicy::default();

    let key = common1::rand_str(10);
    let key = as_key!(namespace, set_name, &key);

    client.delete(&wpolicy, &key).unwrap();

    let val = as_map!("a" => 1, "b" => 2);
    let bin_name = "bin";
    let bin = as_bin!(bin_name, val);
    let bins = vec![&bin];

    client.put(&wpolicy, &key, &bins).unwrap();

    let (k, v) = (as_val!("c"), as_val!(3));
    let op = Operation::map_put_item(&mpolicy, &bin_name, &k, &v);
    let rec = client.operate(&wpolicy, &key, &vec![op]).unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(3)); // size of map after put

    let op = Operation::map_size(&mpolicy, &bin_name);
    let rec = client.operate(&wpolicy, &key, &vec![op]).unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(3)); // size of map

    let rec = client.get(&rpolicy, &key, None).unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(),
        as_map!("a" => 1, "b" => 2, "c" => 3));

    let mut items = HashMap::new();
    items.insert(as_val!("d"), as_val!(4));
    items.insert(as_val!("e"), as_val!(5));
    let op = Operation::map_put_items(&mpolicy, &bin_name, &items);
    let rec = client.operate(&wpolicy, &key, &vec![op]).unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(5)); // size of map after put

    let k = as_val!("e");
    let op = Operation::map_remove_by_key(&mpolicy, &bin_name, &k, MapReturnType::Value);
    let rec = client.operate(&wpolicy, &key, &vec![op]).unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(5));

    let (k, i) = (as_val!("a"), as_val!(19));
    let op = Operation::map_increment_value(&mpolicy, &bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &vec![op]).unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(20)); // value of the key after increment

    let (k, i) = (as_val!("a"), as_val!(10));
    let op = Operation::map_decrement_value(&mpolicy, &bin_name, &k, &i);
    let rec = client.operate(&wpolicy, &key, &vec![op]).unwrap();
    assert_eq!(*rec.bins.get(bin_name).unwrap(), as_val!(10)); // value of the key after decrement

    let op = Operation::map_clear(&mpolicy, &bin_name);
    let rec = client.operate(&wpolicy, &key, &vec![op]).unwrap();
    assert!(rec.bins.get(bin_name).is_none()); // map_clear returns no result

    client.delete(&wpolicy, &key).unwrap();
}
