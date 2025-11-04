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

use aerospike::expressions::*;
use aerospike::ScanPolicy;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use crate::common;
use env_logger;

use aerospike::*;

const EXPECTED: usize = 1000;

fn create_test_set(no_records: usize) -> String {
    let client = common::client();
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![&wbin];
        client.delete(&wpolicy, &key).unwrap();
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    set_name
}

#[test]
fn scan_single_consumer() {
    let _ = env_logger::try_init();

    let client = common::client();
    let namespace = common::namespace();
    let set_name = create_test_set(EXPECTED);

    let spolicy = ScanPolicy::default();
    let rs = client
        .scan(&spolicy, namespace, &set_name, Bins::All)
        .unwrap();

    let count = (&*rs).filter(Result::is_ok).count();
    assert_eq!(count, EXPECTED);
}

#[test]
fn scan_multi_consumer() {
    let _ = env_logger::try_init();

    let client = common::client();
    let namespace = common::namespace();
    let set_name = create_test_set(EXPECTED);

    let mut spolicy = ScanPolicy::default();
    spolicy.record_queue_size = 4096;
    let rs = client
        .scan(&spolicy, namespace, &set_name, Bins::All)
        .unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for _ in 0..8 {
        let count = count.clone();
        let rs = rs.clone();
        threads.push(thread::spawn(move || {
            let ok = (&*rs).filter(Result::is_ok).count();
            count.fetch_add(ok, Ordering::Relaxed);
        }));
    }

    for t in threads {
        t.join().expect("Cannot join thread");
    }

    assert_eq!(count.load(Ordering::Relaxed), EXPECTED);
}

#[test]
fn scan_node() {
    let _ = env_logger::try_init();

    let client = Arc::new(common::client());
    let namespace = common::namespace();
    let set_name = create_test_set(EXPECTED);

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for node in client.nodes() {
        let client = client.clone();
        let count = count.clone();
        let set_name = set_name.clone();
        threads.push(thread::spawn(move || {
            let spolicy = ScanPolicy::default();
            let rs = client
                .scan_node(&spolicy, node, namespace, &set_name, Bins::All)
                .unwrap();
            let ok = (&*rs).filter(Result::is_ok).count();
            count.fetch_add(ok, Ordering::Relaxed);
        }));
    }

    for t in threads {
        t.join().expect("Cannot join thread");
    }

    assert_eq!(count.load(Ordering::Relaxed), EXPECTED);
}

#[test]
fn scan_filtered() {
    let _ = env_logger::try_init();

    let client = common::client();
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let mut spolicy = ScanPolicy::default();
    let mut wpolicy = WritePolicy::default();
    wpolicy.record_exists_action = aerospike::RecordExistsAction::Replace;

    let bin1 = as_bin!("a", "a value");
    let bin2 = as_bin!("b", "another value");
    let bin3 = as_bin!("c", 42);
    let bin4nf = as_bin!("d", 667);
    let bin4f = as_bin!("d", 666);

    let key1 = as_key!(namespace, set_name, 1);
    client
        .put(&wpolicy, &key1, &[&bin1, &bin2, &bin3, &bin4nf])
        .unwrap();

    let key2 = as_key!(namespace, set_name, 2);
    client
        .put(&wpolicy, &key2, &[&bin1, &bin2, &bin3, &bin4nf])
        .unwrap();

    let key3 = as_key!(namespace, set_name, 3);
    client
        .put(&wpolicy, &key3, &[&bin1, &bin2, &bin3, &bin4nf])
        .unwrap();

    // should filter out
    let key5 = as_key!(namespace, set_name, 4);
    client
        .put(&wpolicy, &key5, &[&bin1, &bin2, &bin3, &bin4f])
        .unwrap();

    let fe = Some(ne(int_bin("d".to_string()), int_val(666)));
    spolicy.filter_expression = fe;

    let rs = client
        .scan(&spolicy, common::namespace(), &set_name, Bins::All)
        .unwrap();

    for res in &*rs {
        match res {
            Err(e) => panic!("err: {}", e),
            Ok(rec) => {
                if rec.key.unwrap().digest == key5.digest {
                    panic!("filter not working")
                }
            }
        }
    }
}
