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
        common::delete_durably(client, &wpolicy, &key)
            .await
            .unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[aerospike_macro::test]
async fn scan_single_consumer() {
    let client = common::singleton_client().await;
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
    let stmt = Statement::new(
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
}

#[aerospike_macro::test]
async fn scan_single_consumer_no_setname() {
    let client = common::singleton_client().await;
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
}

#[aerospike_macro::test]
async fn scan_single_consumer_with_cancel() {
    let client = common::singleton_client().await;
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
}

/// The no-loss contract under a genuine mid-stream cancel: unlike
/// `scan_single_consumer_with_cancel` (whose `take()` equals `max_records`,
/// so its channel buffer is empty when it closes), this consumes only half of
/// each round and closes with records still buffered. The resume cursor must
/// point at the last record *consumed*, not the last one fetched, so the
/// union of all rounds is every record exactly once — buffered-but-unseen
/// records are re-fetched, never skipped.
#[aerospike_macro::test]
async fn scan_cancel_midway_resumes_without_loss() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut qpolicy = QueryPolicy::default();
    qpolicy.max_records = 200;

    let mut pf = PartitionFilter::all();
    let mut seen = std::collections::HashSet::new();
    let mut consumed = 0usize;
    let mut rounds = 0usize;
    while !pf.done() {
        rounds += 1;
        assert!(rounds <= 100, "resume loop did not converge");

        let stmt = Statement::new(namespace, &set_name, Bins::None);
        let rs = client.query(&qpolicy, pf, stmt).await.unwrap();
        let mut stream = rs.clone().into_stream();
        let mut got = 0usize;
        while got < 100 {
            match stream.next().await {
                Some(Ok(rec)) => {
                    seen.insert(rec.key.as_ref().unwrap().digest);
                    got += 1;
                }
                Some(Err(err)) => panic!("{err:?}"),
                None => break,
            }
        }
        consumed += got;
        drop(stream);
        rs.close();
        pf = rs.partition_filter().await.unwrap();
    }

    // The contract is at-least-once: every record seen, duplicates
    // permitted. A quiet run delivers exactly once, but under load a
    // mid-query retry can re-deliver records whose cursor commit went
    // stale with the retry's new delivery round.
    assert_eq!(seen.len(), EXPECTED, "records lost across cancel/resume");
    assert!(consumed >= seen.len());
}

/// The multi-consumer flavour of the no-loss contract: two concurrent
/// streams drain one recordset, so a partition's records are consumed out of
/// order across them. The delivery-sequence watermark must keep the cursor on
/// the contiguously consumed prefix — cancelling and resuming may re-deliver
/// (at-least-once) but must never skip a record a consumer left unconsumed.
#[aerospike_macro::test]
async fn scan_multi_consumer_cancel_resumes_without_loss() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let mut qpolicy = QueryPolicy::default();
    qpolicy.max_records = 200;

    let mut pf = PartitionFilter::all();
    let seen = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let mut consumed = 0usize;
    let mut rounds = 0usize;
    while !pf.done() {
        rounds += 1;
        assert!(rounds <= 300, "resume loop did not converge");

        let stmt = Statement::new(namespace, &set_name, Bins::None);
        let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

        let consume = |n: usize| {
            let rs = rs.clone();
            let seen = seen.clone();
            async move {
                let mut stream = rs.into_stream();
                let mut got = 0usize;
                while got < n {
                    match stream.next().await {
                        Some(Ok(rec)) => {
                            seen.lock().unwrap().insert(rec.key.as_ref().unwrap().digest);
                            got += 1;
                        }
                        Some(Err(err)) => panic!("{err:?}"),
                        None => break,
                    }
                }
                got
            }
        };
        let (a, b) = futures::join!(consume(50), consume(50));
        consumed += a + b;
        rs.close();
        pf = rs.partition_filter().await.unwrap();
    }

    let distinct = seen.lock().unwrap().len();
    assert_eq!(distinct, EXPECTED, "records lost across multi-consumer cancel/resume");
    // At-least-once: duplicates are permitted (a resume may re-deliver
    // records consumed beyond the contiguous prefix), never required.
    assert!(consumed >= distinct);
}

/// Cleanup contract: dropping the stream and recordset mid-scan — no explicit
/// close, no cursor read — must wind the query down (`Drop` closes, node
/// tasks unblock and end, half-read connections are invalidated rather than
/// pooled) and leave the client fully usable.
#[aerospike_macro::test]
async fn scan_drop_midway_leaves_client_usable() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    for _ in 0..3 {
        let stmt = Statement::new(namespace, &set_name, Bins::All);
        let rs = client
            .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
            .await
            .unwrap();
        let mut stream = rs.into_stream();
        for _ in 0..50 {
            if stream.next().await.is_none() {
                break;
            }
        }
        // Drop stream and recordset mid-flight.
    }

    // The same client must still serve single commands and a full scan.
    let key = as_key!(namespace, &set_name, 0);
    client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();

    let stmt = Statement::new(namespace, &set_name, Bins::None);
    let rs = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
        .unwrap();
    let count = rs
        .into_stream()
        .filter(|res| futures::future::ready(res.is_ok()))
        .count()
        .await;
    assert_eq!(count, EXPECTED);
}

#[aerospike_macro::test]
async fn scan_single_consumer_with_cursor() {
    let client = common::singleton_client().await;
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
}

#[aerospike_macro::test]
async fn scan_single_consumer_rps() {
    let client = common::singleton_client().await;

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
}

#[aerospike_macro::test]
async fn scan_multi_consumer() {
    let client = common::singleton_client().await;
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
}

#[aerospike_macro::test]
async fn scan_single_consumer_stream() {
    let client = common::singleton_client().await;
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
}

#[aerospike_macro::test]
async fn scan_multi_consumer_stream() {
    let client = common::singleton_client().await;
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
}
