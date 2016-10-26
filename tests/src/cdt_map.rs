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
use aerospike::value::*;

use common1;

#[test]
fn put_item() {
    let ref client = common1::GLOBAL_CLIENT;
    let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
    let set_name = &common1::rand_str(10);

    let wpolicy = WritePolicy::default();
    let mpolicy = MapPolicy::default();
    let rpolicy = ReadPolicy::default();

    let key = common1::rand_str(10);
    let key = as_key!(namespace, set_name, &key);
    let val = as_map!("a" => 1, "b" => 2);
    let wbin = as_bin!("bin", val);
    let bins = vec![&wbin];

    client.delete(&wpolicy, &key).unwrap();
    client.put(&wpolicy, &key, &bins).unwrap();

    let k1 = as_val!("c");
    let v1 = as_val!(3);
    let bin = "bin";
    let op = Operation::map_put_item(&mpolicy, &bin, &k1, &v1);
    let ops = vec![op];
    let rec = client.operate(&wpolicy, &key, &ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(3)); // size of map after put

    let rec = client.get(&rpolicy, &key, None).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
        as_map!("a" => 1, "b" => 2, "c" => 3));
}
