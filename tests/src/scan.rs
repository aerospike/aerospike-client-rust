// // Copyright 2015-2016 Aerospike, Inc.
// //
// // Portions may be licensed to Aerospike, Inc. under one or more contributor
// // license agreements.
// //
// // Licensed under the Apache License, Version 2.0 (the "License"); you may not
// // use this file except in compliance with the License. You may obtain a copy of
// // the License at http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// // WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// // License for the specific language governing permissions and limitations under
// // the License.

// extern crate aerospike;
// extern crate env_logger;

// use std::collections::HashMap;
// use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

// use aerospike::{Client, Host};
// use aerospike::{ClientPolicy, ReadPolicy, WritePolicy, ScanPolicy};
// use aerospike::{Key, Bin, Recordset};
// use aerospike::Operation;
// use aerospike::UDFLang;
// use aerospike::value::*;


// // use std::collections::{HashMap, VecDeque};
// use std::sync::{RwLock, Arc, Mutex};
// // use std::vec::Vec;
// use std::thread;
// use std::time::{Instant, Duration};

// use common1;

// const EXPECTED: usize = 1000;

// #[test]
// fn scan_single_consumer() {

// let _ = env_logger::init();

// let ref client = common1::GLOBAL_CLIENT;

//     let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
//     let set_name = &common1::rand_str(10);

//     let wpolicy = WritePolicy::default();
//     for i in 0..EXPECTED as i64 {
//         let key = as_key!(namespace, set_name, i);
//         let wbin = as_bin!("bin", i);
//         let bins = vec![&wbin];
//         client.delete(&wpolicy, &key).unwrap();
//         client.put(&wpolicy, &key, &bins).unwrap();
//     }

//     let now = Instant::now();
//     let spolicy = ScanPolicy::default();
//     // let node = client.cluster.get_random_node().unwrap();
//     let mut rs = client.scan(&spolicy, namespace, set_name, None).unwrap();

//     let mut count = 0;
//     for res in rs.iter() {
//         match res {
//             Ok(rec) => count += 1,
//             Err(err) => println!("{:?}", err),
//         }
//     }
//     println!("total time: {:?}", now.elapsed());
//     println!("records read: {}", count);

//     assert_eq!(count, EXPECTED);
// }

// #[test]
// fn scan_multi_consumer() {
//     let _ = env_logger::init();

// let ref client = common1::GLOBAL_CLIENT;

//     let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
//     let set_name = &common1::rand_str(10);

//     let wpolicy = WritePolicy::default();
//     for i in 0..EXPECTED as i64 {
//         let key = as_key!(namespace, set_name, i);
//         let wbin = as_bin!("bin", i);
//         let bins = vec![&wbin];
//         client.delete(&wpolicy, &key).unwrap();
//         client.put(&wpolicy, &key, &bins).unwrap();
//     }

//     let now = Instant::now();
//     let mut spolicy = ScanPolicy::default();
//     spolicy.record_queue_size = 4096;
//     let mut rs = client.scan(&spolicy, namespace, set_name, None).unwrap();

//     let mut count = Arc::new(AtomicUsize::new(0));
//     let mut threads = vec![];

//     for i in 0..8 {
//         let count = count.clone();
//         let rs = rs.clone();
//         threads.push(thread::spawn(move || {
//             for res in rs.iter() {
//                 match res {
//                     Ok(rec) => {
//                         count.fetch_add(1, Ordering::Relaxed);
//                     }
//                     Err(_) => (),
//                 }
//             }
//         }));
//     }

//     for t in threads {
//         t.join();
//     }

//     println!("total time: {:?}", now.elapsed());
//     println!("records read: {}", count.load(Ordering::Relaxed));
//     assert_eq!(count.load(Ordering::Relaxed), EXPECTED);
// }
