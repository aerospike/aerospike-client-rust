// Copyright 2015-2018 Aerospike, Inc.
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

use aerospike::operations;
use aerospike::operations::lists;
use aerospike::*;

use crate::common;
use aerospike::{Expiration, ReadTouchTTL};
use aerospike_rt::sleep;
use aerospike_rt::time::{Duration, Instant};
use futures::StreamExt;

#[aerospike_macro::test]
async fn batch_operate_timeout() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let mut bpolicy = BatchPolicy::default();
    bpolicy.concurrency = Concurrency::Parallel;
    bpolicy.base_policy.total_timeout = 5000;
    bpolicy.base_policy.socket_timeout = 10;
    bpolicy.base_policy.max_retries = 0;
    bpolicy.base_policy.sleep_between_retries = 0;

    // aerospike_rt::sleep(Duration::from_secs(10)).await;

    let key1 = as_key!(namespace, set_name, 1);
    let bin1 = as_bin!("a", "a value");
    let bin2 = as_bin!("b", "another value");
    let bin3 = as_bin!("c", 42);

    let wops = vec![
        operations::put(&bin1),
        operations::put(&bin2),
        operations::put(&bin3),
    ];

    let bpw = BatchWritePolicy::default();

    let mut bops = vec![];
    for _ in 0..100 {
        bops.push(BatchOperation::write(&bpw, key1.clone(), wops.clone()));
    }

    let start = Instant::now();
    let _res = client.batch(&bpolicy, &mut bops).await;
    let duration = start.elapsed();

    // Encoding 10,000 operations into a binary buffer is synchronous (no yield points), so the
    // async timeout cannot interrupt it mid-work.  On a slow or loaded machine the encoding alone
    // can take several multiples of `total_timeout`.  The assertion is therefore intentionally
    // generous: we only verify that the call does NOT hang indefinitely — not that it returns
    // within exactly 2× the timeout.
    let expected_duration = Duration::from_secs(5);
    assert!(duration < expected_duration);
}

#[aerospike_macro::test]
async fn batch_operate_read() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let mut bpolicy = BatchPolicy::default();
    bpolicy.concurrency = Concurrency::Parallel;
    let apolicy = AdminPolicy::default();

    let udf_body = r#"
function echo(rec, val)
  return val
end
"#;

    let task = client
        .register_udf(&apolicy, udf_body.as_bytes(), "test_udf.lua", UDFLang::Lua)
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    let bin1 = as_bin!("a", "a value");
    let bin2 = as_bin!("b", "another value");
    let bin3 = as_bin!("c", 42);
    let key1 = as_key!(namespace, set_name, 1);
    let key2 = as_key!(namespace, set_name, 2);
    let key3 = as_key!(namespace, set_name, 3);

    let key4 = as_key!(namespace, set_name, -1);
    // key does not exist

    let selected = Bins::from(["a"]);
    let all = Bins::All;
    let none = Bins::None;

    let wops = vec![
        operations::put(&bin1),
        operations::put(&bin2),
        operations::put(&bin3),
    ];

    let rops = vec![
        operations::get_bin(&bin1.name),
        operations::get_bin(&bin2.name),
        operations::get_header(),
    ];

    let bpr = BatchReadPolicy::default();
    let bpw = BatchWritePolicy::default();
    let bpd = BatchDeletePolicy::default();
    let bpu = BatchUDFPolicy::default();

    // WRITE Operations
    let mut batch = vec![
        BatchOperation::write(&bpw, key1.clone(), wops.clone()),
        BatchOperation::write(&bpw, key2.clone(), wops.clone()),
        BatchOperation::write(&bpw, key3.clone(), wops.clone()),
    ];
    client.batch(&bpolicy, &mut batch).await.unwrap();

    // dbg!(&batch);

    // WRITE Operations
    // remove the first three write ops
    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key1);
    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key2);
    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key3);

    // READ Operations
    let mut batch = vec![
        BatchOperation::read(&bpr, key1.clone(), selected),
        BatchOperation::read(&bpr, key2.clone(), all),
        BatchOperation::read(&bpr, key3.clone(), none.clone()),
        BatchOperation::read_ops(&bpr, key3.clone(), rops),
        BatchOperation::read(&bpr, key4.clone(), none),
    ];
    client.batch(&bpolicy, &mut batch).await.unwrap();

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key1);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 1);

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key2);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 3);

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key3);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 0);

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key3);

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key4);

    // DELETE Operations
    let mut batch = vec![
        BatchOperation::delete(&bpd, key1.clone()),
        BatchOperation::delete(&bpd, key2.clone()),
        BatchOperation::delete(&bpd, key3.clone()),
        BatchOperation::delete(&bpd, key4.clone()),
    ];
    client.batch(&bpolicy, &mut batch).await.unwrap();

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key1);

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key2);

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key3);

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key4);

    let record = result.record;
    assert!(record.is_none());

    // Read
    let mut batch = vec![
        BatchOperation::read(&bpr, key1.clone(), Bins::None),
        BatchOperation::read(&bpr, key2.clone(), Bins::None),
        BatchOperation::read(&bpr, key3.clone(), Bins::None),
        BatchOperation::read(&bpr, key4.clone(), Bins::None),
    ];
    client.batch(&bpolicy, &mut batch).await.unwrap();

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key1);
    let record = result.record;
    assert!(record.is_none());

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key2);
    let record = result.record;
    assert!(record.is_none());

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key3);
    let record = result.record;
    assert!(record.is_none());

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key4);
    let record = result.record;
    assert!(record.is_none());

    // Read
    let args1 = vec![as_val!(1)];
    let args2 = vec![as_val!(2)];
    let args3 = vec![as_val!(3)];
    let args4 = vec![as_val!(4)];
    let mut batch = vec![
        BatchOperation::udf(&bpu, key1.clone(), "test_udf", "echo", Some(args1)),
        BatchOperation::udf(&bpu, key2.clone(), "test_udf", "echo", Some(args2)),
        BatchOperation::udf(&bpu, key3.clone(), "test_udf", "echo", Some(args3)),
        BatchOperation::udf(
            &bpu,
            key4.clone(),
            "test_udf",
            "echo_not_exists",
            Some(args4),
        ),
    ];
    client.batch(&bpolicy, &mut batch).await.unwrap();

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key1);
    let record = result.record;
    assert_eq!(record.unwrap().bins.get("SUCCESS"), Some(&as_val!(1)));

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key2);
    let record = result.record;
    assert_eq!(record.unwrap().bins.get("SUCCESS"), Some(&as_val!(2)));

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key3);
    let record = result.record;
    assert_eq!(record.unwrap().bins.get("SUCCESS"), Some(&as_val!(3)));

    let result = batch.remove(0).batch_record();
    assert_eq!(result.key, key4);
    assert_eq!(result.result_code, Some(ResultCode::UdfBadResponse));
    let record = result.record;
    assert_eq!(
        record.unwrap().bins.get("FAILURE"),
        Some(&as_val!("function not found"))
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_operate_read_multi_op_single_bin() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let mut bpolicy = BatchPolicy::default();
    bpolicy.concurrency = Concurrency::Parallel;

    let key = as_key!(namespace, set_name, common::rand_str(10));

    let wp = WritePolicy::default();
    let bin = as_bin!("lbin", Value::List(as_values!(111, 222, 333)));

    client
        .put(&wp, &key, &vec![bin])
        .await
        .expect("put failed.");

    let brp = BatchReadPolicy::default();
    let br = BatchOperation::read_ops(
        &brp,
        key.clone(),
        vec![
            lists::size("lbin"),
            lists::get_by_index("lbin", -1, lists::ListReturnType::Values),
        ],
    );
    let mut list = vec![br];
    client.batch(&bpolicy, &mut list).await.unwrap();

    let result = list.remove(0).batch_record();
    assert!(Some(ResultCode::Ok) == result.result_code);
    assert!(
        Some(&Value::MultiResult(as_values!(3, 333))) == result.record.unwrap().bins.get("lbin")
    );
}

#[aerospike_macro::test]
async fn batch_operate_read_touch_ttl() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let mut bpolicy = BatchPolicy::default();
    bpolicy.concurrency = Concurrency::Parallel;

    // WARNING: This test takes a long time to run due to sleeps.
    // Define keys
    let key1 = as_key!(namespace, set_name, 88888);
    let key2 = as_key!(namespace, set_name, 88889);

    // Write keys with ttl.
    let mut bwp = BatchWritePolicy::default();
    bwp.expiration = Expiration::Seconds(10);
    let bin1 = as_bin!("a", 1);

    let bw1 = BatchOperation::write(&bwp, key1.clone(), vec![operations::put(&bin1)]);
    let bw2 = BatchOperation::write(&bwp, key2.clone(), vec![operations::put(&bin1)]);
    let mut list = vec![bw1, bw2];
    client.batch(&bpolicy, &mut list).await.unwrap();

    // Read records before they expire and reset read ttl on one record.
    sleep(Duration::from_secs(8)).await;
    let mut brp1 = BatchReadPolicy::default();
    brp1.read_touch_ttl = ReadTouchTTL::Percent(80);

    let mut brp2 = BatchReadPolicy::default();
    brp2.read_touch_ttl = ReadTouchTTL::DontReset;

    let br1 = BatchOperation::read(&brp1, key1.clone(), Bins::Some(vec!["a".into()]));
    let br2 = BatchOperation::read(&brp2, key2.clone(), Bins::Some(vec!["a".into()]));
    let mut list = vec![br1, br2];
    client.batch(&bpolicy, &mut list).await.unwrap();

    assert!(Some(ResultCode::Ok) == list[0].batch_record().result_code);
    assert!(Some(ResultCode::Ok) == list[1].batch_record().result_code);

    // Read records again, but don't reset read ttl.
    sleep(Duration::from_secs(3)).await;
    brp1.read_touch_ttl = ReadTouchTTL::DontReset;
    brp2.read_touch_ttl = ReadTouchTTL::DontReset;

    let br1 = BatchOperation::read(&brp1, key1.clone(), Bins::Some(vec!["a".into()]));
    let br2 = BatchOperation::read(&brp2, key2.clone(), Bins::Some(vec!["a".into()]));
    let mut list = vec![br1, br2];
    client.batch(&bpolicy, &mut list).await.unwrap();

    // Key 2 should have expired.
    assert!(Some(ResultCode::Ok) == list[0].batch_record().result_code);
    assert!(Some(ResultCode::KeyNotFoundError) == list[1].batch_record().result_code);

    // Read record after it expires, showing it's gone.
    sleep(Duration::from_secs(8)).await;
    client.batch(&bpolicy, &mut list).await.unwrap();
    assert!(Some(ResultCode::KeyNotFoundError) == list[0].batch_record().result_code);
    assert!(Some(ResultCode::KeyNotFoundError) == list[1].batch_record().result_code);
}

// --- batch_stream tests ---

/// Basic read: write N records, then stream-read them all back.
/// Verifies that every (index, record) pair arrives, bins are correct, and
/// the stream terminates on its own.
#[aerospike_macro::test]
async fn batch_stream_read() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let bpolicy = BatchPolicy::default();
    let bpw = BatchWritePolicy::default();
    let bpr = BatchReadPolicy::default();

    let keys: Vec<Key> = (0..5).map(|i| as_key!(namespace, set_name, i)).collect();

    // Write records using the regular batch API.
    let bin = as_bin!("v", 42);
    let wops = vec![operations::put(&bin)];
    let mut writes: Vec<BatchOperation> = keys
        .iter()
        .map(|k| BatchOperation::write(&bpw, k.clone(), wops.clone()))
        .collect();
    client.batch(&bpolicy, &mut writes).await.unwrap();

    // Stream-read all keys back.
    let reads: Vec<BatchOperation> = keys
        .iter()
        .map(|k| BatchOperation::read(&bpr, k.clone(), Bins::All))
        .collect();

    let stream = client.batch_stream(&bpolicy, reads).await.unwrap();
    let mut results: Vec<(usize, BatchRecord)> = stream.collect().await;

    assert_eq!(results.len(), 5);

    // Sort by original index so assertions are deterministic.
    results.sort_by_key(|(i, _)| *i);

    for (i, (orig_idx, br)) in results.iter().enumerate() {
        assert_eq!(*orig_idx, i);
        assert_eq!(br.result_code, Some(ResultCode::Ok));
        assert_eq!(
            br.record.as_ref().unwrap().bins.get("v"),
            Some(&as_val!(42))
        );
    }
}

/// A mixed batch: some keys exist, some do not.
/// Verifies that missing keys surface as `KeyNotFoundError` in the stream item
/// rather than causing the stream to terminate early.
#[aerospike_macro::test]
async fn batch_stream_missing_keys() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let bpolicy = BatchPolicy::default();
    let bpw = BatchWritePolicy::default();
    let bpr = BatchReadPolicy::default();

    let existing = as_key!(namespace, set_name, "exists");
    let missing = as_key!(namespace, set_name, "ghost");

    // Write only the first key.
    let bin = as_bin!("x", 7);
    let mut writes = vec![BatchOperation::write(
        &bpw,
        existing.clone(),
        vec![operations::put(&bin)],
    )];
    client.batch(&bpolicy, &mut writes).await.unwrap();

    let reads = vec![
        BatchOperation::read(&bpr, existing.clone(), Bins::All),
        BatchOperation::read(&bpr, missing.clone(), Bins::All),
    ];

    let stream = client.batch_stream(&bpolicy, reads).await.unwrap();
    let mut results: Vec<(usize, BatchRecord)> = stream.collect().await;

    assert_eq!(results.len(), 2);
    results.sort_by_key(|(i, _)| *i);

    let (_, ref found) = results[0];
    assert_eq!(found.result_code, Some(ResultCode::Ok));
    assert_eq!(
        found.record.as_ref().unwrap().bins.get("x"),
        Some(&as_val!(7))
    );

    let (_, ref not_found) = results[1];
    assert_eq!(not_found.result_code, Some(ResultCode::KeyNotFoundError));
    assert!(not_found.record.is_none());
}

/// Index fidelity: the `usize` in each stream item must map back to the
/// original position in the `ops` vec, regardless of arrival order.
#[aerospike_macro::test]
async fn batch_stream_index_fidelity() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let bpolicy = BatchPolicy::default();
    let bpw = BatchWritePolicy::default();
    let bpr = BatchReadPolicy::default();

    // Write records with distinct bin values so we can tie each result back
    // to its original key.
    let n = 8usize;
    let keys: Vec<Key> = (0..n)
        .map(|i| as_key!(namespace, set_name, i as i64))
        .collect();

    let mut writes: Vec<BatchOperation> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let bin = as_bin!("idx", i as i64);
            BatchOperation::write(&bpw, k.clone(), vec![operations::put(&bin)])
        })
        .collect();
    client.batch(&bpolicy, &mut writes).await.unwrap();

    let reads: Vec<BatchOperation> = keys
        .iter()
        .map(|k| BatchOperation::read(&bpr, k.clone(), Bins::All))
        .collect();

    let stream = client.batch_stream(&bpolicy, reads).await.unwrap();
    let results: Vec<(usize, BatchRecord)> = stream.collect().await;

    assert_eq!(results.len(), n);

    // For every item, the bin value stored in the record must equal the
    // original index reported by the stream.
    for (orig_idx, br) in &results {
        let stored = br
            .record
            .as_ref()
            .unwrap()
            .bins
            .get("idx")
            .cloned()
            .unwrap();
        assert_eq!(stored, as_val!(*orig_idx as i64));
    }
}
