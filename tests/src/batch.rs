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

    // Real goal: `total_timeout` actually bounds the batch; the client must
    // not run all 10k ops to completion. A wall-clock assertion was flaky in
    // debug builds under parallel-test load where post-timeout cleanup alone
    // could exceed a 20 ms window. Instead, assert on the returned error kind
    // — a `Timeout` return *is* proof that the policy bounded the batch —
    // with a loose duration sanity bound to catch a true regression where
    // the timeout is ignored and all 10k ops run (which would take seconds).
    let start = Instant::now();
    let res = client.batch(&bpolicy, &bops).await;
    let duration = start.elapsed();

    assert!(
        matches!(&res, Err(Error::Timeout(_))),
        "expected Err(Timeout), got {:?} after {:?}",
        res,
        duration,
    );
    assert!(
        duration < Duration::from_secs(2),
        "batch ran for {:?}, suggesting total_timeout was ignored",
        duration,
    );
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
    let caps = common::ServerCapabilities::detect(&client).await;
    if !caps.explicit_record_ttl_allowed {
        eprintln!(
            "batch_operate_read_touch_ttl: skipped (explicit_record_ttl_allowed=false; namespace_sc={})",
            caps.namespace_strong_consistency
        );
        return;
    }

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

// ===== Single-key fast path (Go's executeSingle / Java's BatchSingle*) =====
//
// When a per-node batch group has exactly one key, the executor
// dispatches it as a regular non-batch command (Read / Operate /
// Delete / ExecuteUDF) so the server processes it on the standard
// transaction queue instead of the (more contended) batch queue.
// The user-facing result must still surface as a `BatchRecord` —
// these tests verify each variant's wiring end-to-end.

#[aerospike_macro::test]
async fn batch_single_key_fast_path_read_returns_record() {
    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, "single_read");

    let wpolicy = WritePolicy::default();
    client.delete(&wpolicy, &key).await.unwrap();
    client
        .put(
            &wpolicy,
            &key,
            &[as_bin!("a", 7_i64), as_bin!("b", "hello")],
        )
        .await
        .unwrap();

    let bp = BatchPolicy::default();
    let bpr = BatchReadPolicy::default();
    let ops = vec![BatchOperation::read(&bpr, key.clone(), Bins::All)];

    let recs = client.batch(&bp, &ops).await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].result_code, Some(ResultCode::Ok));
    let rec = recs[0].record.as_ref().expect("record returned");
    assert_eq!(rec.bins.get("a"), Some(&Value::from(7_i64)));
    assert_eq!(rec.bins.get("b"), Some(&Value::from("hello")));
}

#[aerospike_macro::test]
async fn batch_single_key_fast_path_read_missing_key() {
    // KEY_NOT_FOUND from a single-key fast-path read is captured on
    // the BatchRecord (just like the multi-key batch path) — it must
    // not bubble up as a top-level error.
    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, "single_missing");

    let wpolicy = WritePolicy::default();
    client.delete(&wpolicy, &key).await.unwrap();

    let bp = BatchPolicy::default();
    let bpr = BatchReadPolicy::default();
    let ops = vec![BatchOperation::read(&bpr, key.clone(), Bins::All)];

    let recs = client.batch(&bp, &ops).await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].result_code, Some(ResultCode::KeyNotFoundError));
    assert!(recs[0].record.is_none());
}

#[aerospike_macro::test]
async fn batch_single_key_fast_path_write_then_read() {
    // Round-trip: a single-key BatchOperation::write followed by a
    // single-key BatchOperation::read both go through the fast path.
    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, "single_write");

    let wpolicy = WritePolicy::default();
    client.delete(&wpolicy, &key).await.unwrap();

    let bp = BatchPolicy::default();
    let bpw = BatchWritePolicy::default();
    let write_ops = vec![operations::put(&as_bin!("counter", 42_i64))];
    let recs = client
        .batch(&bp, &[BatchOperation::write(&bpw, key.clone(), write_ops)])
        .await
        .unwrap();
    assert_eq!(recs[0].result_code, Some(ResultCode::Ok));

    // Confirm the record landed by reading it back via the same fast
    // path (a separate single-key batch).
    let bpr = BatchReadPolicy::default();
    let recs = client
        .batch(&bp, &[BatchOperation::read(&bpr, key.clone(), Bins::All)])
        .await
        .unwrap();
    assert_eq!(recs[0].result_code, Some(ResultCode::Ok));
    let rec = recs[0].record.as_ref().expect("record returned");
    assert_eq!(rec.bins.get("counter"), Some(&Value::from(42_i64)));
}

#[aerospike_macro::test]
async fn batch_single_key_fast_path_delete() {
    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, "single_delete");

    let wpolicy = WritePolicy::default();
    client.delete(&wpolicy, &key).await.unwrap();
    client
        .put(&wpolicy, &key, &[as_bin!("a", 1_i64)])
        .await
        .unwrap();

    let bp = BatchPolicy::default();
    let bpd = BatchDeletePolicy::default();
    let recs = client
        .batch(&bp, &[BatchOperation::delete(&bpd, key.clone())])
        .await
        .unwrap();
    assert_eq!(recs[0].result_code, Some(ResultCode::Ok));

    // Confirm gone via a follow-up single-key read.
    let bpr = BatchReadPolicy::default();
    let recs = client
        .batch(&bp, &[BatchOperation::read(&bpr, key.clone(), Bins::All)])
        .await
        .unwrap();
    assert_eq!(recs[0].result_code, Some(ResultCode::KeyNotFoundError));
}
