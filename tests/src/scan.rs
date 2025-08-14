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

use crate::common;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use aerospike::query::PartitionFilter;
use env_logger;

use aerospike::*;
use aerospike_rt::time::Instant;

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

    let pf = PartitionFilter::all();
    let spolicy = ScanPolicy::default();

    let rs = client
        .scan(&spolicy, pf, namespace, &set_name, Bins::All)
        .await
        .unwrap();

    let count = (&*rs).filter(Result::is_ok).count();
    assert_eq!(count, EXPECTED);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn scan_single_consumer_with_cursor() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut pf = PartitionFilter::all();
    let mut spolicy = ScanPolicy::default();
    spolicy.max_records = (EXPECTED / 3) as u64;

    let mut count = 0;
    while !pf.done() {
        let rs = client
            .scan(&spolicy, pf, namespace, &set_name, Bins::All)
            .await
            .unwrap();

        count += (&*rs).filter(Result::is_ok).count();
        assert!(rs.is_active() == false);
        pf = rs.partition_filter().await.unwrap();
        if count == 1000 {
            assert!(pf.done() == true);
        }
    }
    assert_eq!(count, EXPECTED);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn scan_single_consumer_rps() {
    let _ = env_logger::try_init();

    let client = common::client().await;

    // only run on single node clusters
    if client.nodes().await.len() != 1 {
        return;
    }

    let node_count = client.cluster.nodes().await.len();
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut spolicy = ScanPolicy::default();
    spolicy.records_per_second = (EXPECTED / 3 / node_count) as u32;

    let start_time = Instant::now();
    let pf = PartitionFilter::all();
    let rs = client
        .scan(&spolicy, pf, namespace, &set_name, Bins::All)
        .await
        .unwrap();

    let count = (&*rs).filter(Result::is_ok).count();
    let duration = start_time.elapsed();
    assert_eq!(count, EXPECTED);

    // Should take at least 3 seconds due to rps
    assert!(duration.as_millis() > 3000);

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
    let pf = PartitionFilter::all();
    let rs = client
        .scan(&spolicy, pf, namespace, &set_name, Bins::All)
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
