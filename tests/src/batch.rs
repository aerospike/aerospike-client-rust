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

#[aerospike_macro::test]
async fn batch_operate_timeout() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let mut bpolicy = BatchPolicy::default();
    bpolicy.concurrency = Concurrency::Parallel;
    bpolicy.base_policy.total_timeout = 10;
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
    for _ in 0..10000 {
        bops.push(BatchOperation::write(&bpw, key1.clone(), wops.clone()));
    }

    let start = Instant::now();
    let _res = client.batch(&bpolicy, &bops).await;
    let duration = start.elapsed();

    let expected_duration = Duration::from_millis((bpolicy.total_timeout() * 2) as u64);
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

    let batch = vec![
        BatchOperation::write(&bpw, key1.clone(), wops.clone()),
        BatchOperation::write(&bpw, key2.clone(), wops.clone()),
        BatchOperation::write(&bpw, key3.clone(), wops.clone()),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    // dbg!(&results);

    // WRITE Operations
    // remove the first three write ops
    let result = results.remove(0);
    assert_eq!(result.key, key1);
    let result = results.remove(0);
    assert_eq!(result.key, key2);
    let result = results.remove(0);
    assert_eq!(result.key, key3);

    // READ Operations
    let batch = vec![
        BatchOperation::read(&bpr, key1.clone(), selected),
        BatchOperation::read(&bpr, key2.clone(), all),
        BatchOperation::read(&bpr, key3.clone(), none.clone()),
        BatchOperation::read_ops(&bpr, key3.clone(), rops),
        BatchOperation::read(&bpr, key4.clone(), none),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    let result = results.remove(0);
    assert_eq!(result.key, key1);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 1);

    let result = results.remove(0);
    assert_eq!(result.key, key2);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 3);

    let result = results.remove(0);
    assert_eq!(result.key, key3);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 0);

    let result = results.remove(0);
    assert_eq!(result.key, key3);

    let result = results.remove(0);
    assert_eq!(result.key, key4);

    // DELETE Operations
    let batch = vec![
        BatchOperation::delete(&bpd, key1.clone()),
        BatchOperation::delete(&bpd, key2.clone()),
        BatchOperation::delete(&bpd, key3.clone()),
        BatchOperation::delete(&bpd, key4.clone()),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    let result = results.remove(0);
    assert_eq!(result.key, key1);

    let result = results.remove(0);
    assert_eq!(result.key, key2);

    let result = results.remove(0);
    assert_eq!(result.key, key3);

    let result = results.remove(0);
    assert_eq!(result.key, key4);

    let record = result.record;
    assert!(record.is_none());

    // Read
    let batch = vec![
        BatchOperation::read(&bpr, key1.clone(), Bins::None),
        BatchOperation::read(&bpr, key2.clone(), Bins::None),
        BatchOperation::read(&bpr, key3.clone(), Bins::None),
        BatchOperation::read(&bpr, key4.clone(), Bins::None),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    let result = results.remove(0);
    assert_eq!(result.key, key1);
    let record = result.record;
    assert!(record.is_none());

    let result = results.remove(0);
    assert_eq!(result.key, key2);
    let record = result.record;
    assert!(record.is_none());

    let result = results.remove(0);
    assert_eq!(result.key, key3);
    let record = result.record;
    assert!(record.is_none());

    let result = results.remove(0);
    assert_eq!(result.key, key4);
    let record = result.record;
    assert!(record.is_none());

    // Read
    let args1 = vec![as_val!(1)];
    let args2 = vec![as_val!(2)];
    let args3 = vec![as_val!(3)];
    let args4 = vec![as_val!(4)];
    let batch = vec![
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
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    let result = results.remove(0);
    assert_eq!(result.key, key1);
    let record = result.record;
    assert_eq!(record.unwrap().bins.get("SUCCESS"), Some(&as_val!(1)));

    let result = results.remove(0);
    assert_eq!(result.key, key2);
    let record = result.record;
    assert_eq!(record.unwrap().bins.get("SUCCESS"), Some(&as_val!(2)));

    let result = results.remove(0);
    assert_eq!(result.key, key3);
    let record = result.record;
    assert_eq!(record.unwrap().bins.get("SUCCESS"), Some(&as_val!(3)));

    let result = results.remove(0);
    assert_eq!(result.key, key4);
    assert_eq!(result.result_code, Some(ResultCode::UdfBadResponse));
    let record = result.record;
    assert_eq!(
        record.unwrap().bins.get("FAILURE"),
        Some(&as_val!("function not found"))
    );

    client.close().await.unwrap();
}

/// Multiple batch operate results for the same scalar bin merge into MultiResult
/// (batch_operate_command path).
#[aerospike_macro::test]
async fn batch_operate_scalar_multi_op_same_bin_returns_multi_result() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let bpolicy = BatchPolicy::default();

    let key = as_key!(namespace, set_name, 1);
    let wp = WritePolicy::default();
    client
        .put(&wp, &key, &[as_bin!("count", 10i64)])
        .await
        .expect("put failed.");

    let brp = BatchReadPolicy::default();
    let br = BatchOperation::read_ops(
        &brp,
        key.clone(),
        vec![
            operations::get_bin("count"),
            operations::get_bin("count"),
        ],
    );
    let mut results = client.batch(&bpolicy, &[br]).await.unwrap();

    let result = results.remove(0);
    assert_eq!(Some(ResultCode::Ok), result.result_code);
    assert_eq!(
        result.record.unwrap().bins.get("count"),
        Some(&Value::MultiResult(vec![Value::from(10i64), Value::from(10i64)]))
    );

    client.close().await.unwrap();
}

/// Multiple batch operate results for the same list bin merge into MultiResult
/// (batch_operate_command path).
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
    let list = vec![br];
    let mut results = client.batch(&bpolicy, &list).await.unwrap();

    let result = results.remove(0);
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
    let list = vec![bw1, bw2];
    client.batch(&bpolicy, &list).await.unwrap();

    // Read records before they expire and reset read ttl on one record.
    sleep(Duration::from_secs(8)).await;
    let mut brp1 = BatchReadPolicy::default();
    brp1.read_touch_ttl = ReadTouchTTL::Percent(80);

    let mut brp2 = BatchReadPolicy::default();
    brp2.read_touch_ttl = ReadTouchTTL::DontReset;

    let br1 = BatchOperation::read(&brp1, key1.clone(), Bins::Some(vec!["a".into()]));
    let br2 = BatchOperation::read(&brp2, key2.clone(), Bins::Some(vec!["a".into()]));
    let list = vec![br1, br2];
    let recs = client.batch(&bpolicy, &list).await.unwrap();

    assert!(Some(ResultCode::Ok) == recs[0].result_code);
    assert!(Some(ResultCode::Ok) == recs[1].result_code);

    // Read records again, but don't reset read ttl.
    sleep(Duration::from_secs(3)).await;
    brp1.read_touch_ttl = ReadTouchTTL::DontReset;
    brp2.read_touch_ttl = ReadTouchTTL::DontReset;

    let br1 = BatchOperation::read(&brp1, key1.clone(), Bins::Some(vec!["a".into()]));
    let br2 = BatchOperation::read(&brp2, key2.clone(), Bins::Some(vec!["a".into()]));
    let list = vec![br1, br2];
    let recs = client.batch(&bpolicy, &list).await.unwrap();

    // Key 2 should have expired.
    assert!(Some(ResultCode::Ok) == recs[0].result_code);
    assert!(Some(ResultCode::KeyNotFoundError) == recs[1].result_code);

    // Read  record after it expires, showing it's gone.
    sleep(Duration::from_secs(8)).await;
    let recs = client.batch(&bpolicy, &list).await.unwrap();
    assert!(Some(ResultCode::KeyNotFoundError) == recs[0].result_code);
    assert!(Some(ResultCode::KeyNotFoundError) == recs[1].result_code);
}

// A key the cluster cannot route is a per-key outcome, not a reason to throw
// away every other key's result. The splitter used to propagate the routing
// failure with `?`, so one key naming a namespace the cluster does not have
// discarded the whole batch before anything was sent.
#[aerospike_macro::test]
async fn batch_with_one_unroutable_key_still_returns_the_other_rows() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let wpolicy = WritePolicy::default();
    let bpr = BatchReadPolicy::default();

    let good1 = as_key!(namespace, &set_name, "routable-1");
    let good2 = as_key!(namespace, &set_name, "routable-2");
    for key in [&good1, &good2] {
        client
            .put(&wpolicy, key, &[as_bin!("a", 1)])
            .await
            .unwrap();
    }
    // No such namespace, so the client cannot route this one.
    let bad = as_key!("no_such_namespace", &set_name, "unroutable");

    let ops = vec![
        BatchOperation::read(&bpr, good1, Bins::All),
        BatchOperation::read(&bpr, bad, Bins::All),
        BatchOperation::read(&bpr, good2, Bins::All),
    ];

    let records = client
        .batch(&BatchPolicy::default(), &ops)
        .await
        .expect("one unroutable key must not fail the whole batch");

    assert_eq!(records.len(), 3, "every key must come back, in order");
    assert!(
        records[0].record.is_some(),
        "the first routable key must still have its record"
    );
    assert!(
        records[2].record.is_some(),
        "the last routable key must still have its record"
    );
    assert_eq!(
        records[1].result_code,
        Some(ResultCode::PartitionUnavailable),
        "the unroutable key carries its own error"
    );
    assert!(records[1].record.is_none());
    assert!(!records[1].in_doubt, "nothing was sent, so nothing is in doubt");

    client.close().await.unwrap();
}

// A batch where NO key can be routed still fails: there is nothing to report
// per key, and the routing error is more useful than an empty record set.
#[aerospike_macro::test]
async fn batch_with_no_routable_key_fails() {
    let client = common::client().await;
    let set_name = common::rand_str(10);
    let bpr = BatchReadPolicy::default();

    let ops = vec![BatchOperation::read(
        &bpr,
        as_key!("no_such_namespace", &set_name, "unroutable"),
        Bins::All,
    )];

    let err = client
        .batch(&BatchPolicy::default(), &ops)
        .await
        .expect_err("a wholly unroutable batch must fail");
    assert!(
        matches!(err, Error::InvalidNode(_)),
        "expected the routing error, got {:?}",
        err
    );

    client.close().await.unwrap();
}

// A per-key server error is a per-key outcome wherever it lands in the response.
// The arm that handles it on the LAST record used to stamp the row and then fail
// the call anyway, discarding every row it had just filled in.
#[aerospike_macro::test]
async fn batch_per_key_error_on_the_last_record_does_not_fail_the_call() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let wpolicy = WritePolicy::default();

    let key = as_key!(namespace, &set_name, "already-there");
    client
        .put(&wpolicy, &key, &[as_bin!("a", 1)])
        .await
        .unwrap();

    // CreateOnly against an existing record: the server answers KEY_EXISTS_ERROR
    // for this key. With respond_all_keys = false the error STOPS the response,
    // so that record is flagged as the last one - which is the arm that used to
    // fail the whole call. (With respond_all_keys = true the server keeps going
    // and the error arrives as an ordinary per-key record, which was always
    // absorbed correctly.)
    let mut bpw = BatchWritePolicy::default();
    bpw.record_exists_action = RecordExistsAction::CreateOnly;
    let other = as_key!(namespace, &set_name, "also-there");
    client
        .put(&wpolicy, &other, &[as_bin!("a", 1)])
        .await
        .unwrap();
    let ops = vec![
        BatchOperation::write(&bpw, key.clone(), vec![operations::put(&as_bin!("a", 2))]),
        BatchOperation::write(&bpw, other, vec![operations::put(&as_bin!("a", 2))]),
    ];

    let mut bpolicy = BatchPolicy::default();
    bpolicy.respond_all_keys = false;
    let records = client
        .batch(&bpolicy, &ops)
        .await
        .expect("a per-key error must not fail the batch call");

    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].result_code,
        Some(ResultCode::KeyExistsError),
        "the row carries the server's per-key code"
    );

    // The record is untouched.
    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(*rec.bins.get("a").unwrap(), Value::from(1));

    client.close().await.unwrap();
}

// Identical writes over a shared (cloned) op list encode with the wire REPEAT
// flag after the first record. The unit tests prove the encoder emits it; this
// proves the *server* accepts the compressed form and applies the repeated
// header to every digest — all records must succeed and every key must hold the
// data. A silent mis-parse here would corrupt data, so this is the test that
// makes the optimization safe to ship.
#[aerospike_macro::test]
async fn batch_write_repeat_compression() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let bpolicy = BatchPolicy::default();
    let wpolicy = BatchWritePolicy::default();

    let ops = vec![
        operations::put(&as_bin!("a", 7)),
        lists::append(&lists::ListPolicy::default(), "l", as_val!(1)),
    ];

    let mut batch = Vec::new();
    let mut keys = Vec::new();
    for i in 0..8_i64 {
        let key = as_key!(namespace, &set_name, i);
        keys.push(key.clone());
        // A cloned op list shares its encoder Arcs, so every record after the
        // first repeats.
        batch.push(BatchOperation::write(&wpolicy, key, ops.clone()));
    }

    let results = client.batch(&bpolicy, &batch).await.unwrap();
    assert_eq!(results.len(), 8);
    for record in &results {
        assert_eq!(
            record.result_code,
            Some(ResultCode::Ok),
            "repeated batch write failed: {0:?}",
            record
        );
    }

    let rp = ReadPolicy::default();
    for key in &keys {
        let rec = client.get(&rp, key, Bins::All).await.unwrap();
        assert_eq!(rec.bins.get("a"), Some(&Value::from(7_i64)));
        assert_eq!(rec.bins.get("l"), Some(&as_list!(1)));
    }

    // Reads repeat too: same policy, same bins, distinct keys.
    let reads: Vec<BatchOperation> = keys
        .iter()
        .map(|key| BatchOperation::read(&BatchReadPolicy::default(), key.clone(), Bins::All))
        .collect();
    let results = client.batch(&bpolicy, &reads).await.unwrap();
    assert_eq!(results.len(), 8);
    for record in &results {
        assert_eq!(record.result_code, Some(ResultCode::Ok));
        let bins = &record.record.as_ref().expect("record").bins;
        assert_eq!(bins.get("a"), Some(&Value::from(7_i64)));
    }

    client.close().await.unwrap();
}
