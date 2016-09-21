// Copyright 2015-2016 Aerospike, Inc.
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
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

use aerospike::{Client, Host};
use aerospike::{ClientPolicy, ReadPolicy, WritePolicy, ScanPolicy};
use aerospike::{Key, Bin, Recordset};
use aerospike::Operation;
use aerospike::UDFLang;
use aerospike::value::*;

// use log::LogLevel;
use env_logger;

// use std::collections::{HashMap, VecDeque};
use std::sync::{RwLock, Arc, Mutex};
// use std::vec::Vec;
use std::thread;
use std::time::{Instant, Duration};

use common1;

#[test]
fn cdt_list() {
    let _ = env_logger::init();

    let ref client = common1::GLOBAL_CLIENT;

    let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
    let set_name = &common1::rand_str(10);

    let policy = ReadPolicy::default();

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let val = as_list!("0", 1, 2.1f64);
    let wbin = as_bin!("bin", val.clone());
    let bins = vec![&wbin];

    client.delete(&wpolicy, &key).unwrap();

    client.put(&wpolicy, &key, &bins).unwrap();
    let rec = client.get(&policy, &key, None).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), val);

    let ops = &vec![Operation::list_size("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::from(3));

    let values = vec![as_val!(9), as_val!(8), as_val!(7)];
    let ops = &vec![Operation::list_insert("bin", 1, &values), Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!(6, as_list!("0", 9, 8, 7, 1, 2.1f64)));

    let ops = &vec![Operation::list_pop("bin", 0), Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!("0", as_list!(9, 8, 7, 1, 2.1f64)));

    let ops = &vec![Operation::list_pop_range("bin", 0, 2), Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!(9, 8, as_list!(7, 1, 2.1f64)));

    let ops = &vec![Operation::list_pop_range_from("bin", 1), Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!(1, 2.1f64, as_list!(7)));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64];
    let ops = &vec![Operation::list_clear("bin"),
                    Operation::list_append("bin", &values),
                    Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!(6, as_list!("0", 9, 8, 7, 1, 2.1f64)));

    let ops = &vec![Operation::list_remove("bin", 1), Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!(1, as_list!("0", 8, 7, 1, 2.1f64)));

    let ops = &vec![Operation::list_remove_range("bin", 1, 2), Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!(2, as_list!("0", 1, 2.1f64)));

    let ops = &vec![Operation::list_remove_range_from("bin", -1), Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(1, as_list!("0", 1)));

    let v = as_val!(2);
    let ops = &vec![Operation::list_set("bin", -1, &v), Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!("0", 2));


    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![Operation::list_clear("bin"),
                    Operation::list_append("bin", &values),
                    Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)));

    let ops = &vec![Operation::list_trim("bin", 1, 1), Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(6, as_list!(9)));

    let values = as_values!["0", 9, 8, 7, 1, 2.1f64, -1];
    let ops = &vec![Operation::list_clear("bin"),
                    Operation::list_append("bin", &values),
                    Operation::get_bin("bin")];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!(7, as_list!("0", 9, 8, 7, 1, 2.1f64, -1)));

    let ops = &vec![Operation::list_get("bin", 1)];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(9));

    let ops = &vec![Operation::list_get_range("bin", 1, -1)];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(),
               as_list!(9, 8, 7, 1, 2.1f64, -1));

    let ops = &vec![Operation::list_get_range_from("bin", 2)];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_list!(8, 7, 1, 2.1f64, -1));
}
