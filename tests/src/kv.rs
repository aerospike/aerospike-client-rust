// Copyright 2015-2017 Aerospike, Inc.
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

use aerospike::{Bins, Value, ReadPolicy, WritePolicy};
use aerospike::operations;

use env_logger;

use common;

#[test]
fn connect() {
    let _ = env_logger::init();

    let client = common::client();
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let policy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let wbin = as_bin!("bin999", "test string");
    let wbin1 = as_bin!("bin vec![int]", as_list![1u32, 2u32, 3u32]);
    let wbin2 = as_bin!("bin vec![u8]", as_blob!(vec![1u8, 2u8, 3u8]));
    let wbin3 = as_bin!("bin map", as_map!(1 => 1, 2 => 2, 3 => "hi!"));
    let wbin4 = as_bin!("bin f64", 1.64f64);
    let wbin5 = as_bin!("bin Nil", None); // Writing None erases the bin!
    let wbin6 = as_bin!("bin Geo",
                        as_geo!(format!(r#"{{ "type": "Point", "coordinates": [{}, {}] }}"#,
                                        17.119381,
                                        19.45612)));
    let wbin7 = as_bin!("bin-name-len-15", "max. bin name length is 15 chars");
    let bins = vec![&wbin, &wbin1, &wbin2, &wbin3, &wbin4, &wbin5, &wbin6, &wbin7];

    client.delete(&wpolicy, &key).unwrap();
    client.put(&wpolicy, &key, &bins).unwrap();

    let record = client.get(&policy, &key, Bins::All).unwrap();
    let bins = record.bins;
    assert_eq!(bins.len(), 7);
    assert_eq!(bins.get("bin999"), Some(&Value::from("test string")));
    assert_eq!(bins.get("bin vec![int]"), Some(&as_list![1u32, 2u32, 3u32]));
    assert_eq!(bins.get("bin vec![u8]"),
               Some(&as_blob!(vec![1u8, 2u8, 3u8])));
    assert_eq!(bins.get("bin map"),
               Some(&as_map!(1 => 1, 2 => 2, 3 => "hi!")));
    assert_eq!(bins.get("bin f64"), Some(&Value::from(1.64f64)));
    assert_eq!(bins.get("bin Geo"),
               Some(&as_geo!(r#"{ "type": "Point", "coordinates": [17.119381, 19.45612] }"#)));
    assert_eq!(bins.get("bin-name-len-15"),
               Some(&Value::from("max. bin name length is 15 chars")));

    client.touch(&wpolicy, &key).unwrap();

    let bins = Bins::from(["bin999", "bin f64"]);
    let record = client.get(&policy, &key, bins).unwrap();
    assert_eq!(record.bins.len(), 2);

    let record = client.get(&policy, &key, Bins::None).unwrap();
    assert_eq!(record.bins.len(), 0);

    let exists = client.exists(&wpolicy, &key).unwrap();
    assert!(exists);

    let ops = &vec![operations::put(&wbin), operations::get()];
    client.operate(&wpolicy, &key, ops).unwrap();

    let existed = client.delete(&wpolicy, &key).unwrap();
    assert!(existed);

    let existed = client.delete(&wpolicy, &key).unwrap();
    assert!(!existed);
}
