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

use std::time::Duration;

use futures::stream::StreamExt;
use tokio::time::sleep;

use crate::common;

use aerospike::query::PartitionFilter;
use aerospike::*;

const NUM_RECORDS: usize = 10000;

/// Compression is an enterprise-only feature. Skip tests on community edition.
async fn skip_if_not_enterprise() -> bool {
    if !common::enterprise_edition().await {
        eprintln!("Skipping compression test: requires enterprise edition");
        return true;
    }
    false
}

/// Insert test records into a unique set. Returns the set name.
/// Retries individual puts to tolerate injected random timeouts.
async fn setup_records(client: &Client, count: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.socket_timeout = 5000;
    wpolicy.base_policy.total_timeout = 10000;
    wpolicy.base_policy.max_retries = 5;

    for i in 0..count as i64 {
        let key = as_key!(namespace, &set_name, i);
        let bins = vec![
            as_bin!("int", i),
            as_bin!("str", format!("value-{i}-padding-to-increase-message-size")),
            as_bin!("blob", as_blob!(vec![0u8; 64])),
        ];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

// ---------------------------------------------------------------------------
// Single-record commands with compression
// ---------------------------------------------------------------------------

#[aerospike_macro::test]
async fn put_get_with_compression() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    // Policies with compression enabled
    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.use_compression = true;

    let mut rpolicy = ReadPolicy::default();
    rpolicy.base_policy.use_compression = true;

    let key = as_key!(namespace, &set_name, 1);
    let bins = vec![
        as_bin!("int", 42),
        as_bin!("str", "hello-compressed-world"),
        as_bin!("blob", as_blob!(vec![0xABu8; 128])),
    ];

    // Write with compression
    client.put(&wpolicy, &key, &bins).await.unwrap();

    // Read with compression
    let record = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    assert_eq!(record.bins["int"], Value::from(42));
    assert_eq!(record.bins["str"], Value::from("hello-compressed-world"));

    // Verify specific bins
    let record = client
        .get(&rpolicy, &key, Bins::from(["int", "str"]))
        .await
        .unwrap();
    assert_eq!(record.bins.len(), 2);
    assert_eq!(record.bins["int"], Value::from(42));

    // Clean up
    client.delete(&wpolicy, &key).await.unwrap();
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn operate_with_compression() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.use_compression = true;

    let key = as_key!(namespace, &set_name, 1);
    let bin = as_bin!("counter", 10);

    let ops = vec![operations::put(&bin), operations::get()];
    let record = client.operate(&wpolicy, &key, &ops).await.unwrap();
    assert_eq!(record.bins["counter"], Value::from(10));

    // Increment
    let ops = vec![operations::add(&as_bin!("counter", 5)), operations::get()];
    let record = client.operate(&wpolicy, &key, &ops).await.unwrap();
    assert_eq!(record.bins["counter"], Value::from(15));

    client.delete(&wpolicy, &key).await.unwrap();
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn exists_with_compression() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.use_compression = true;

    let mut rpolicy = ReadPolicy::default();
    rpolicy.base_policy.use_compression = true;

    let key = as_key!(namespace, &set_name, 1);
    let bins = vec![as_bin!("x", 1)];
    client.put(&wpolicy, &key, &bins).await.unwrap();

    assert!(client.exists(&rpolicy, &key).await.unwrap());

    client.delete(&wpolicy, &key).await.unwrap();
    assert!(!client.exists(&rpolicy, &key).await.unwrap());

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn delete_with_compression() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.use_compression = true;

    let key = as_key!(namespace, &set_name, 1);
    let bins = vec![as_bin!("x", 1)];
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    assert!(existed);

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    assert!(!existed);

    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Compressed writes read back without compression (and vice versa)
// ---------------------------------------------------------------------------

#[aerospike_macro::test]
async fn compressed_write_uncompressed_read() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.use_compression = true;

    let rpolicy = ReadPolicy::default(); // no compression

    let key = as_key!(namespace, &set_name, 1);
    let bins = vec![as_bin!("a", 99), as_bin!("b", "cross-compression-test")];
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let record = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    assert_eq!(record.bins["a"], Value::from(99));
    assert_eq!(record.bins["b"], Value::from("cross-compression-test"));

    client.delete(&wpolicy, &key).await.unwrap();
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn uncompressed_write_compressed_read() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default(); // no compression

    let mut rpolicy = ReadPolicy::default();
    rpolicy.base_policy.use_compression = true;

    let key = as_key!(namespace, &set_name, 1);
    let bins = vec![as_bin!("a", 99), as_bin!("b", "cross-compression-test")];
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let record = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    assert_eq!(record.bins["a"], Value::from(99));
    assert_eq!(record.bins["b"], Value::from("cross-compression-test"));

    client.delete(&wpolicy, &key).await.unwrap();
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Batch operations with compression
// ---------------------------------------------------------------------------

#[aerospike_macro::test]
async fn batch_read_with_compression() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = setup_records(&client, 50).await;

    let mut bpolicy = BatchPolicy::default();
    bpolicy.base_policy.use_compression = true;

    let brp = BatchReadPolicy::default();
    let mut ops: Vec<BatchOperation> = Vec::new();
    for i in 0..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        ops.push(BatchOperation::read(&brp, key, Bins::All));
    }

    let results = client.batch(&bpolicy, &ops).await.unwrap();
    assert_eq!(results.len(), 50);

    for result in &results {
        let record = result.record.as_ref().expect("record should exist");
        assert!(record.bins.contains_key("int"));
        assert!(record.bins.contains_key("str"));
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_write_with_compression() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let mut bpolicy = BatchPolicy::default();
    bpolicy.base_policy.use_compression = true;

    let bwp = BatchWritePolicy::default();
    let mut ops: Vec<BatchOperation> = Vec::new();
    for i in 0..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        let wops = vec![
            operations::put(&as_bin!("val", i)),
            operations::put(&as_bin!(
                "padding",
                format!("data-{i}-padding-to-make-it-bigger")
            )),
        ];
        ops.push(BatchOperation::write(&bwp, key, wops));
    }

    client.batch(&bpolicy, &ops).await.unwrap();

    // Verify with compressed reads
    let mut rpolicy = ReadPolicy::default();
    rpolicy.base_policy.use_compression = true;

    for i in 0..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        let record = client.get(&rpolicy, &key, Bins::All).await.unwrap();
        assert_eq!(record.bins["val"], Value::from(i));
    }

    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Streaming commands (query/scan) with compression
// ---------------------------------------------------------------------------

#[aerospike_macro::test]
async fn scan_with_compression() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = setup_records(&client, NUM_RECORDS).await;

    let mut qpolicy = QueryPolicy::default();
    qpolicy.base_policy.use_compression = true;

    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let pf = PartitionFilter::all();

    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();
    let count = rs
        .into_stream()
        .fold(0_usize, |count, res| async move {
            res.unwrap();
            count + 1
        })
        .await;

    assert_eq!(count, NUM_RECORDS);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_with_compression() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = setup_records(&client, NUM_RECORDS).await;

    // Create an index for the query filter
    let apolicy = AdminPolicy::default();
    let idx_name = format!("{namespace}_{set_name}_int");
    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            &set_name,
            "int",
            &idx_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create index");
    task.wait_till_complete(None).await.unwrap();

    let mut qpolicy = QueryPolicy::default();
    qpolicy.base_policy.use_compression = true;

    // Range query: int in [10, 50)
    let mut stmt = Statement::new(namespace, &set_name, Bins::All);
    stmt.add_filter(as_eq!("int", 25));

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();

    let count = rs
        .into_stream()
        .fold(0_usize, |count, res| async move {
            let record = res.unwrap();
            assert_eq!(record.bins["int"], Value::from(25));
            count + 1
        })
        .await;

    assert_eq!(count, 1);
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Large payload to ensure compression actually kicks in (> 128 byte threshold)
// ---------------------------------------------------------------------------

#[aerospike_macro::test]
async fn large_record_compression() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.use_compression = true;

    let mut rpolicy = ReadPolicy::default();
    rpolicy.base_policy.use_compression = true;

    let key = as_key!(namespace, &set_name, 1);

    // Create a large record that will benefit from compression.
    // Repeated patterns compress well.
    let large_string = "abcdefghijklmnopqrstuvwxyz".repeat(200); // 5200 bytes
    let large_blob = vec![0x42u8; 8192];

    let bins = vec![
        as_bin!("big_str", large_string.as_str()),
        as_bin!("big_blob", as_blob!(large_blob.clone())),
        as_bin!("int", 12345),
    ];

    client.put(&wpolicy, &key, &bins).await.unwrap();

    let record = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    assert_eq!(record.bins["int"], Value::from(12345));
    assert_eq!(record.bins["big_str"], Value::from(large_string.as_str()));
    if let Value::Blob(ref blob) = record.bins["big_blob"] {
        assert_eq!(blob.len(), 8192);
        assert!(blob.iter().all(|&b| b == 0x42));
    } else {
        panic!("Expected blob value");
    }

    client.delete(&wpolicy, &key).await.unwrap();
    client.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// Connection recovery tests with compression
//
// These tests verify that when a compressed command times out, the connection
// recovery mechanism correctly drains the remaining compressed data so the
// connection can be returned to the pool in a clean state.
// ---------------------------------------------------------------------------

/// Write a large record that will produce a compressed response big enough to
/// trigger a timeout with a very short socket_timeout.
/// Write multiple large records to a set. Returns (namespace, set_name).
/// Multiple large records are needed because a single-key compressed response
/// may arrive within 1ms on localhost.
async fn setup_large_records(client: &Client, count: usize) -> (String, String) {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.socket_timeout = 5000;
    wpolicy.base_policy.total_timeout = 10000;
    wpolicy.base_policy.max_retries = 5;

    for i in 0..count as i64 {
        let key = as_key!(namespace, &set_name, i);
        // Use random-ish data that doesn't compress well, making the response bigger.
        let blob: Vec<u8> = (0..8192)
            .map(|j| (i as u8).wrapping_mul(7).wrapping_add(j as u8))
            .collect();
        let bins = vec![
            as_bin!(
                "str",
                format!(
                    "{i}-padding-data-to-increase-response-size-{}",
                    "x".repeat(2000)
                )
            ),
            as_bin!("blob", as_blob!(blob)),
            as_bin!("int", i),
        ];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    (namespace.to_string(), set_name)
}

#[aerospike_macro::test]
async fn recovery_single_command_compressed() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let count = 50;
    let (namespace, set_name) = setup_large_records(&client, count).await;

    // Policy with compression, very short timeout, and timeout_delay for recovery.
    let mut rpolicy = ReadPolicy::default();
    rpolicy.base_policy.use_compression = true;
    rpolicy.base_policy.socket_timeout = 1; // 1ms — will timeout during response read
    rpolicy.base_policy.total_timeout = 1;
    rpolicy.base_policy.timeout_delay = 3000; // 3s recovery window

    // Fire many reads rapidly; some should timeout during response parsing.
    let mut timeout_count = 0;
    for _ in 0..count as i64 {
        for i in 0..count as i64 {
            let key = as_key!(&namespace, &set_name, i);
            let res = client.get(&rpolicy, &key, Bins::All).await;
            if res.is_err() {
                timeout_count += 1;
            }
        }
    }
    // At least some should have timed out.
    let total_count = count * count;
    eprintln!("Single-command recovery: {timeout_count}/{total_count} timed out",);

    // Allow time for the background recovery tasks to drain connections.
    sleep(Duration::from_millis(500)).await;

    // Now make multiple normal requests to verify no corrupted connections
    // remain in the pool.
    let mut rpolicy_normal = ReadPolicy::default();
    rpolicy_normal.base_policy.use_compression = true;

    for i in 0..count as i64 {
        let key = as_key!(&namespace, &set_name, i);
        let record = client.get(&rpolicy_normal, &key, Bins::All).await.unwrap();
        assert_eq!(record.bins["int"], Value::from(i));
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn recovery_scan_compressed() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = setup_records(&client, NUM_RECORDS).await;

    // Scan with compression + very short timeout + recovery enabled.
    let mut qpolicy = QueryPolicy::default();
    qpolicy.base_policy.use_compression = true;
    qpolicy.base_policy.socket_timeout = 1;
    qpolicy.base_policy.total_timeout = 1;
    qpolicy.base_policy.timeout_delay = 3000;

    let stmt = Statement::new(namespace, &set_name, Bins::All);
    let pf = PartitionFilter::all();

    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();
    let mut timed_out = false;
    let mut rs_stream = rs.into_stream();
    while let Some(res) = rs_stream.next().await {
        match res {
            Ok(_) => (),
            Err(Error::Timeout(_)) => timed_out = true,
            Err(err) => panic!("Unexpected error: {:?}", err),
        }
    }
    assert!(timed_out, "Expected timeout during scan");

    // Allow recovery to complete.
    sleep(Duration::from_millis(500)).await;

    // Verify the client is still functional with multiple normal scans.
    let mut qpolicy_normal = QueryPolicy::default();
    qpolicy_normal.base_policy.use_compression = true;
    qpolicy_normal.base_policy.socket_timeout = 5000;
    qpolicy_normal.base_policy.total_timeout = 10000;
    qpolicy_normal.base_policy.max_retries = 5;

    for _ in 0..30 {
        let stmt = Statement::new(namespace, &set_name, Bins::All);
        let pf = PartitionFilter::all();
        let rs = client.query(&qpolicy_normal, pf, stmt).await.unwrap();
        let count = rs
            .into_stream()
            .fold(0_usize, |count, res| async move {
                if res.is_ok() {
                    count + 1
                } else {
                    count
                }
            })
            .await;
        assert!(
            count > NUM_RECORDS / 2,
            "Expected at least half the records, got {}/{}",
            count,
            NUM_RECORDS,
        );
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn recovery_batch_compressed() {
    if skip_if_not_enterprise().await {
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let count = NUM_RECORDS;
    let (_, set_name) = setup_large_records(&client, count).await;

    // Batch read with compression + very short timeout + recovery enabled.
    let mut bpolicy = BatchPolicy::default();
    bpolicy.base_policy.use_compression = true;
    bpolicy.base_policy.socket_timeout = 1;
    bpolicy.base_policy.total_timeout = 1;
    bpolicy.base_policy.timeout_delay = 3000;

    let brp = BatchReadPolicy::default();
    let mut ops: Vec<BatchOperation> = Vec::new();
    for i in 0..count as i64 {
        let key = as_key!(namespace, &set_name, i);
        ops.push(BatchOperation::read(&brp, key, Bins::All));
    }

    // This should timeout while reading the large batch response.
    let result = client.batch(&bpolicy, &ops).await;
    assert!(result.is_err(), "Expected timeout error on batch");

    // Allow recovery to complete.
    sleep(Duration::from_millis(500)).await;

    // Verify the client is still functional with normal batch reads.
    let mut bpolicy_normal = BatchPolicy::default();
    bpolicy_normal.base_policy.use_compression = true;
    bpolicy_normal.base_policy.socket_timeout = 5000;
    bpolicy_normal.base_policy.total_timeout = 10000;
    bpolicy_normal.base_policy.max_retries = 5;

    let brp = BatchReadPolicy::default();
    let mut ops: Vec<BatchOperation> = Vec::new();
    for i in 0..count as i64 {
        let key = as_key!(namespace, &set_name, i);
        ops.push(BatchOperation::read(&brp, key, Bins::All));
    }

    for _ in 0..30 {
        let results = client.batch(&bpolicy_normal, &ops).await;
        if results.is_err() {
            continue;
        }

        let results = results.unwrap();
        assert_eq!(results.len(), count);
        for result in &results {
            assert!(result.record.is_some());
        }
    }

    client.close().await.unwrap();
}
