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

extern crate aerospike;
extern crate env_logger;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use aerospike::{WritePolicy, ScanPolicy};

use common;

const EXPECTED: usize = 1000;

#[test]
fn scan_single_consumer() {
    let _ = env_logger::init();

    let client = &common::GLOBAL_CLIENT;
    let namespace: &str = &common::AEROSPIKE_NAMESPACE;
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..EXPECTED as i64 {
        let key = as_key!(namespace, set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![&wbin];
        client.delete(&wpolicy, &key).unwrap();
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    let now = Instant::now();
    let spolicy = ScanPolicy::default();
    let rs = client.scan(&spolicy, namespace, set_name, None).unwrap();

    let count = (&*rs).filter(|r| r.is_ok()).count();
    assert_eq!(count, EXPECTED);
    println!("total time: {:?}", now.elapsed());
    println!("records read: {}", count);
}

#[test]
fn scan_multi_consumer() {
    let _ = env_logger::init();

    let client = &common::GLOBAL_CLIENT;
    let namespace: &str = &common::AEROSPIKE_NAMESPACE;
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..EXPECTED as i64 {
        let key = as_key!(namespace, set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![&wbin];
        client.delete(&wpolicy, &key).unwrap();
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    let now = Instant::now();
    let mut spolicy = ScanPolicy::default();
    spolicy.record_queue_size = 4096;
    let rs = client.scan(&spolicy, namespace, set_name, None).unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for _ in 0..8 {
        let count = count.clone();
        let rs = rs.clone();
        threads.push(thread::spawn(move || {
                                       let ok = (&*rs).filter(|r| r.is_ok()).count();
                                       count.fetch_add(ok, Ordering::Relaxed);
                                   }));
    }

    for t in threads {
        t.join().expect("Cannot join thread");
    }

    assert_eq!(count.load(Ordering::Relaxed), EXPECTED);
    println!("total time: {:?}", now.elapsed());
    println!("records read: {}", count.load(Ordering::Relaxed));
}
