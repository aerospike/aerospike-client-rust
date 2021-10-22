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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use crate::common;
use env_logger;

use aerospike::*;

const EXPECTED: usize = 1000;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![wbin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[aerospike_macro::test]
async fn scan_single_consumer() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let spolicy = ScanPolicy::default();
    let rs = client
        .scan(&spolicy, namespace, &set_name, Bins::All)
        .await
        .unwrap();

    let count = (&*rs).filter(Result::is_ok).count();
    assert_eq!(count, EXPECTED);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn scan_multi_consumer() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut spolicy = ScanPolicy::default();
    spolicy.record_queue_size = 4096;
    let rs = client
        .scan(&spolicy, namespace, &set_name, Bins::All)
        .await
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

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn scan_node() {
    let _ = env_logger::try_init();

    let client = Arc::new(common::client().await);
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for node in client.nodes().await {
        let client = client.clone();
        let count = count.clone();
        let set_name = set_name.clone();
        threads.push(aerospike_rt::spawn(async move {
            let spolicy = ScanPolicy::default();
            let rs = client
                .scan_node(&spolicy, node, namespace, &set_name, Bins::All)
                .await
                .unwrap();
            let ok = (&*rs).filter(Result::is_ok).count();
            count.fetch_add(ok, Ordering::Relaxed);
        }));
    }

    for t in threads {
        t.await;
    }

    assert_eq!(count.load(Ordering::Relaxed), EXPECTED);

    client.close().await.unwrap();
}
