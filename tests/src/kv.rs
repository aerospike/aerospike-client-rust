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
fn connect() {
    let _ = env_logger::init();

let ref client = common1::GLOBAL_CLIENT;

    let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
    let set_name = &common1::rand_str(10);

    let t: i64 = 1;
    let _ = Key::new(namespace, set_name, Value::from(t));
    let _ = as_key!(namespace, set_name, t);
    let _ = as_key!(namespace, set_name, &t);
    let _ = as_key!(namespace, set_name, 1);
    let _ = as_key!(namespace, set_name, &1);
    let _ = as_key!(namespace, set_name, 1i8);
    let _ = as_key!(namespace, set_name, &1i8);
    let _ = as_key!(namespace, set_name, 1u8);
    let _ = as_key!(namespace, set_name, &1u8);
    let _ = as_key!(namespace, set_name, 1i16);
    let _ = as_key!(namespace, set_name, &1i16);
    let _ = as_key!(namespace, set_name, 1u16);
    let _ = as_key!(namespace, set_name, &1u16);
    let _ = as_key!(namespace, set_name, 1i32);
    let _ = as_key!(namespace, set_name, &1i32);
    let _ = as_key!(namespace, set_name, 1u32);
    let _ = as_key!(namespace, set_name, &1u32);
    let _ = as_key!(namespace, set_name, 1i64);
    let _ = as_key!(namespace, set_name, &1i64);
    // let _ = as_key!(namespace, set_name, 1.0f32);
    // let _ = as_key!(namespace, set_name, &1.0f32);
    let _ = as_key!(namespace, set_name, 1.0f64);
    let _ = as_key!(namespace, set_name, &1.0f64);

    let _ = as_key!(namespace, set_name, "haha");
    let _ = as_key!(namespace, set_name, "haha".to_string());
    let _ = as_key!(namespace, set_name, &"haha".to_string());

let policy = ReadPolicy::default();

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let wbin = as_bin!("bin999", "test string");
    let wbin1 = as_bin!("bin vec![int]", as_list![1u32, 2u32, 3u32]);
    let wbin2 = as_bin!("bin vec![u8]", as_blob!(vec![1u8, 2u8, 3u8]));
    let wbin3 = as_bin!("bin map", as_map!(1 => 1, 2 => 2, 3 => "hi!"));
    let wbin4 = as_bin!("bin f64", 1.64f64);
    let wbin5 = as_bin!("bin Nil", None);
    let wbin6 = as_bin!("bin Geo",
                        as_geo!(format!("{{ \"type\": \"Point\", \"coordinates\": [{}, {}] }}",
                                        17.119381,
                                        19.45612)));
    let bins = vec![&wbin, &wbin1, &wbin2, &wbin3, &wbin4, &wbin5, &wbin6];

client.delete(&wpolicy, &key).unwrap();


    client.put(&wpolicy, &key, &bins).unwrap();
    let rec = client.get(&policy, &key, None).unwrap();
    println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ {}", rec);

    client.touch(&wpolicy, &key).unwrap();
    let rec = client.get(&policy, &key, None);
    println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ {}", rec.unwrap());

    let rec = client.get_header(&policy, &key);
    println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@/// {}", rec.unwrap());

    let exists = client.exists(&wpolicy, &key).unwrap();
    println!("exists: {}", exists);

    let ops = &vec![Operation::put(&wbin), Operation::get()];
    let op_rec = client.operate(&wpolicy, &key, ops);
    println!("operate: {}", op_rec.unwrap());

    let existed = client.delete(&wpolicy, &key).unwrap();
    println!("existed: {}", existed);

    let existed = client.delete(&wpolicy, &key).unwrap();
    println!("existed: {}", existed);
}

#[test]
fn read_me_example() {
    let _ = env_logger::init();

let ref client = common1::GLOBAL_CLIENT;

    let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
    let set_name: String = common1::rand_str(10);

    let mut threads = vec![];
    let now = Instant::now();
    for i in 0..2 {
        let client = client.clone();
        let namespace = namespace.clone();
        let set_name = set_name.clone();
        let t = thread::spawn(move || {
            let policy = ReadPolicy::default();

            let wpolicy = WritePolicy::default();
            let key = as_key!(namespace, &set_name, i);

            let wbin = as_bin!("bin999", 1);
            let bins = vec![&wbin];

            client.put(&wpolicy, &key, &bins).unwrap();
            let rec = client.get(&policy, &key, None);
            println!("Record: {}", rec.unwrap());

            client.touch(&wpolicy, &key).unwrap();
            let rec = client.get(&policy, &key, None);
            println!("Record: {}", rec.unwrap());

            let rec = client.get_header(&policy, &key);
            println!("Record Header: {}", rec.unwrap());

            let exists = client.exists(&wpolicy, &key).unwrap();
            println!("exists: {}", exists);

            let ops = &vec![Operation::put(&wbin), Operation::get()];
            let op_rec = client.operate(&wpolicy, &key, ops);
            println!("operate: {}", op_rec.unwrap());

            let existed = client.delete(&wpolicy, &key).unwrap();
            println!("existed (sould be true): {}", existed);

            let existed = client.delete(&wpolicy, &key).unwrap();
            println!("existed (should be false): {}", existed);
        });

        threads.push(t);
    }

    for t in threads {
        t.join().unwrap();
    }

    println!("total time: {:?}", now.elapsed());
}
