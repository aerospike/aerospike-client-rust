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

use futures::stream::StreamExt;

use aerospike::query::PartitionFilter;

use aerospike::*;
use aerospike_rt::time::Instant;

const EXPECTED: usize = 1000;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin1 = as_bin!("bin", i);
        let wbin2 = as_bin!("bin2", "hello");
        let wbin3 = as_bin!("extra", "extra");
        let bins = vec![wbin1, wbin2, wbin3];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[aerospike_macro::test]
async fn scan_single_consumer() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let pf = PartitionFilter::all();
    let qpolicy = QueryPolicy::default();

    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

    let count = rs
        .into_stream()
        .filter(|res| futures::future::ready(res.is_ok()))
        .count()
        .await;
    assert_eq!(count, EXPECTED);

    // ================================ Select bins ====================

    let pf = PartitionFilter::all();
    let mut stmt = Statement::new(
        namespace,
        &set_name,
        Bins::Some(vec!["bin".into(), "bin2".into()]),
    );
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();
    let count = rs
        .into_stream()
        .filter(|res| futures::future::ready(res.is_ok() && res.as_ref().unwrap().bins.len() == 2))
        .count()
        .await;
    assert_eq!(count, EXPECTED);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn scan_single_consumer_no_setname() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = "";

    let pf = PartitionFilter::all();
    let qpolicy = QueryPolicy::default();

    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

    let count = rs
        .into_stream()
        .map(|res| futures::future::ready(res.unwrap()))
        .count()
        .await;

    // no need to check anything;
    assert!(count > 0);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn scan_single_consumer_with_cancel() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut pf = PartitionFilter::all();
    let mut qpolicy = QueryPolicy::default();
    qpolicy.max_records = (EXPECTED / 3) as u64;
    // qpolicy.records_per_second = (EXPECTED / 4) as u32;
    // qpolicy.record_queue_size = 1;

    let mut count = 0;
    while !pf.done() {
        let stmt = Statement::new(namespace, &set_name, Bins::All);
        let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

        count += rs
            .clone()
            .into_stream()
            .take(EXPECTED / 3)
            .filter(|res| futures::future::ready(res.is_ok()))
            .count()
            .await;

        // close the stream
        rs.close();

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
async fn scan_single_consumer_with_cursor() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut pf = PartitionFilter::all();
    let mut qpolicy = QueryPolicy::default();
    qpolicy.max_records = (EXPECTED / 3) as u64;

    let mut count = 0;
    while !pf.done() {
        let stmt = Statement::new(namespace, &set_name, Bins::All);
        let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

        count += rs
            .clone()
            .into_stream()
            .filter(|res| futures::future::ready(res.is_ok()))
            .count()
            .await;
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
    let client = common::client().await;

    // only run on single node clusters
    if client.nodes().len() != 1 {
        return;
    }

    let node_count = client.cluster.nodes().len();
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut qpolicy = QueryPolicy::default();
    qpolicy.records_per_second = (EXPECTED / 3 / node_count) as u32;

    let start_time = Instant::now();
    let pf = PartitionFilter::all();
    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

    let count = rs
        .into_stream()
        .filter(|res| futures::future::ready(res.is_ok()))
        .count()
        .await;
    let duration = start_time.elapsed();
    assert_eq!(count, EXPECTED);

    // Should take at least 3 seconds due to rps
    assert!(duration.as_millis() > 3000);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn scan_multi_consumer() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut qpolicy = QueryPolicy::default();
    qpolicy.record_queue_size = 4096;
    let pf = PartitionFilter::all();
    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for _ in 0..8 {
        let count = count.clone();
        let rs = rs.clone();
        threads.push(aerospike_rt::spawn(async move {
            let ok = rs
                .into_stream()
                .filter(|res| futures::future::ready(res.is_ok()))
                .count()
                .await;
            count.fetch_add(ok, Ordering::Relaxed);
        }));
    }

    futures::future::join_all(threads).await;

    assert_eq!(count.load(Ordering::Relaxed), EXPECTED);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn scan_single_consumer_stream() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut qpolicy = QueryPolicy::default();
    qpolicy.record_queue_size = 4096;
    let pf = PartitionFilter::all();
    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let rs = rs.into_stream();
    tokio::pin!(rs);
    while let Some(res) = rs.next().await {
        if res.is_ok() {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    assert_eq!(count.load(Ordering::Relaxed), EXPECTED);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn scan_multi_consumer_stream() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut qpolicy = QueryPolicy::default();
    qpolicy.record_queue_size = 4096;
    let pf = PartitionFilter::all();
    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for _ in 0..8 {
        let count = count.clone();
        let rs = rs.clone().into_stream();
        threads.push(aerospike_rt::spawn(async move {
            tokio::pin!(rs);
            while let Some(res) = rs.next().await {
                if res.is_ok() {
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for t in threads {
        #[cfg(all(any(feature = "rt-tokio"), not(feature = "rt-async-std")))]
        t.await.expect("Cannot join thread");
        #[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
        t.await;
    }

    assert_eq!(count.load(Ordering::Relaxed), EXPECTED);

    client.close().await.unwrap();
}
