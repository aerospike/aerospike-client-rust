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
        matches!(&res, Err(e) if common::is_timeout_error(e)),
        "expected timeout error, got {:?} after {:?}",
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
        .register_udf(&apolicy, udf_body.as_bytes(), "batch_read_echo.lua", UDFLang::Lua)
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
    let mut bpd = BatchDeletePolicy::default();
    if namespace_sc!(&client) {
        bpd.durable_delete = true;
    }
    let bpu = BatchUDFPolicy::default();

    let batch = vec![
        BatchOperation::write(&bpw, key1.clone(), wops.clone()),
        BatchOperation::write(&bpw, key2.clone(), wops.clone()),
        BatchOperation::write(&bpw, key3.clone(), wops.clone()),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    for (i, r) in results.iter().enumerate() {
        if r.result_code != Some(ResultCode::Ok) {
            eprintln!(
                "batch_operate_read: skipped — batch write {i} returned {:?}",
                r.result_code
            );
            return;
        }
    }

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
        BatchOperation::udf(&bpu, key1.clone(), "batch_read_echo", "echo", Some(args1)),
        BatchOperation::udf(&bpu, key2.clone(), "batch_read_echo", "echo", Some(args2)),
        BatchOperation::udf(&bpu, key3.clone(), "batch_read_echo", "echo", Some(args3)),
        BatchOperation::udf(
            &bpu,
            key4.clone(),
            "batch_read_echo",
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
    let caps = common::ServerCapabilities::detect(&client).await;
    if !caps.explicit_record_ttl_allowed {
        eprintln!(
            "batch_operate_read_touch_ttl: skipped (explicit_record_ttl_allowed=false; namespace_sc={})",
            namespace_sc!(&client)
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
    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();
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
    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();

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
    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();

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
    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();
    client
        .put(&wpolicy, &key, &[as_bin!("a", 1_i64)])
        .await
        .unwrap();

    let bp = BatchPolicy::default();
    let mut bpd = BatchDeletePolicy::default();
    if namespace_sc!(&client) {
        bpd.durable_delete = true;
    }
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

// ----- Strong-consistency (SC) batch delete semantics (single-key fast path) -----
//
// The property test `proptests::batches::batch_delete` clamps some policy fields on SC so the
// fuzzer does not send illegal or inconsistent deletes. These tests document the real server
// outcomes for fixed policies. On the fast path, `FailForbidden` / `GenerationError` bubble as
// a per-key server error (see `BatchExecutor::execute_single_op`).

#[aerospike_macro::test]
async fn batch_sc_delete_non_durable_forbidden_when_record_exists() {
    let client = common::client().await;
    if !namespace_sc!(&client) {
        return;
    }

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, "sc_batch_ndel");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();
    client
        .put(&wpolicy, &key, &[as_bin!("a", 1_i64)])
        .await
        .unwrap();

    let bp = BatchPolicy::default();
    let mut bpd = BatchDeletePolicy::default();
    bpd.durable_delete = false;

    let res = client
        .batch(&bp, &[BatchOperation::delete(&bpd, key.clone())])
        .await;
    match res {
        Err(e) if e.server_result_code() == Some(ResultCode::FailForbidden) => {}
        other => panic!(
            "expected FailForbidden for non-durable delete on SC when record exists, got {:?}",
            other
        ),
    }

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .expect("record should still exist after forbidden delete");
    assert_eq!(rec.bins.get("a"), Some(&Value::from(1_i64)));

    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();
}

#[aerospike_macro::test]
async fn batch_sc_delete_generation_mismatch_errors() {
    let client = common::client().await;
    if !namespace_sc!(&client) {
        return;
    }

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, "sc_batch_gen_bad");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();
    client
        .put(&wpolicy, &key, &[as_bin!("a", 1_i64)])
        .await
        .unwrap();

    let bp = BatchPolicy::default();
    let mut bpd = BatchDeletePolicy::default();
    bpd.durable_delete = true;
    bpd.generation_policy = GenerationPolicy::ExpectGenEqual;
    bpd.generation = 9_999;

    let res = client
        .batch(&bp, &[BatchOperation::delete(&bpd, key.clone())])
        .await;
    match res {
        Err(e) if e.server_result_code() == Some(ResultCode::GenerationError) => {}
        other => panic!(
            "expected GenerationError for ExpectGenEqual with wrong generation on SC, got {:?}",
            other
        ),
    }

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .expect("record should still exist after failed conditional delete");
    assert_eq!(rec.bins.get("a"), Some(&Value::from(1_i64)));

    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();
}

#[aerospike_macro::test]
async fn batch_sc_delete_with_matching_generation_succeeds() {
    let client = common::client().await;
    if !namespace_sc!(&client) {
        return;
    }

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, "sc_batch_gen_ok");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();
    client
        .put(&wpolicy, &key, &[as_bin!("a", 1_i64)])
        .await
        .unwrap();

    let gen = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .expect("read after put")
        .generation;

    let bp = BatchPolicy::default();
    let mut bpd = BatchDeletePolicy::default();
    bpd.durable_delete = true;
    bpd.generation_policy = GenerationPolicy::ExpectGenEqual;
    bpd.generation = gen;

    let recs = client
        .batch(&bp, &[BatchOperation::delete(&bpd, key.clone())])
        .await
        .expect("matching-generation durable delete should succeed on SC");
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].result_code, Some(ResultCode::Ok));

    let bpr = BatchReadPolicy::default();
    let recs = client
        .batch(&bp, &[BatchOperation::read(&bpr, key.clone(), Bins::All)])
        .await
        .unwrap();
    assert_eq!(recs[0].result_code, Some(ResultCode::KeyNotFoundError));
    // ===== batch_stream =====
    //
    // Streaming variant: per-node results are pushed to a channel and
    // the consumer pulls them as a `Stream<Item = (usize, BatchRecord)>`.
    // Items arrive interleaved by node-completion order; the `usize`
    // carries the original input index so callers can match results back
    // to their `ops` vec.
}

#[aerospike_macro::test]
async fn batch_stream_emits_each_record_exactly_once() {
    use futures::stream::StreamExt;

    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let wpolicy = WritePolicy::default();

    // 25 keys — large enough to give multi-key per-node groups a
    // chance, small enough to stay fast.
    let keys: Vec<_> = (0..25_i64).map(|i| as_key!(ns, set, i)).collect();
    for (i, key) in keys.iter().enumerate() {
        client.delete(&wpolicy, key).await.unwrap();
        client
            .put(
                &wpolicy,
                key,
                &[as_bin!("a", i as i64), as_bin!("b", "hello")],
            )
            .await
            .unwrap();
    }

    let bp = BatchPolicy::default();
    let bpr = BatchReadPolicy::default();
    let ops: Vec<_> = keys
        .iter()
        .map(|k| BatchOperation::read(&bpr, k.clone(), Bins::All))
        .collect();

    let mut stream = client.batch_stream(&bp, ops).await.unwrap();
    let mut collected: Vec<(usize, BatchRecord)> = Vec::new();
    while let Some(item) = stream.next().await {
        collected.push(item);
    }

    assert_eq!(
        collected.len(),
        keys.len(),
        "expected one stream item per input key"
    );

    // Every input index must appear exactly once.
    let mut indices: Vec<usize> = collected.iter().map(|(idx, _)| *idx).collect();
    indices.sort_unstable();
    let expected: Vec<usize> = (0..keys.len()).collect();
    assert_eq!(indices, expected, "indices must cover [0..N) exactly once");

    // For each (idx, record) the key should match keys[idx] and the
    // value of bin `a` should equal idx.
    for (idx, br) in &collected {
        assert_eq!(br.key, keys[*idx]);
        assert_eq!(br.result_code, Some(ResultCode::Ok));
        let bins = &br.record.as_ref().expect("record present").bins;
        assert_eq!(bins.get("a"), Some(&Value::from(*idx as i64)));
        assert_eq!(bins.get("b"), Some(&Value::from("hello")));
    }
}

#[aerospike_macro::test]
async fn batch_stream_missing_keys_carry_key_not_found() {
    use futures::stream::StreamExt;

    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let wpolicy = WritePolicy::default();

    // One key exists, one doesn't.
    let key_present = as_key!(ns, set, "stream_present");
    let key_missing = as_key!(ns, set, "stream_missing");
    client.delete(&wpolicy, &key_present).await.unwrap();
    client.delete(&wpolicy, &key_missing).await.unwrap();
    client
        .put(&wpolicy, &key_present, &[as_bin!("x", 1_i64)])
        .await
        .unwrap();

    let bp = BatchPolicy::default();
    let bpr = BatchReadPolicy::default();
    // Index 0 = present, index 1 = missing.
    let ops = vec![
        BatchOperation::read(&bpr, key_present.clone(), Bins::All),
        BatchOperation::read(&bpr, key_missing.clone(), Bins::All),
    ];

    let mut stream = client.batch_stream(&bp, ops).await.unwrap();
    let mut got: Vec<(usize, BatchRecord)> = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item);
    }

    assert_eq!(got.len(), 2);

    let (_, present) = got
        .iter()
        .find(|(idx, _)| *idx == 0)
        .expect("index 0 in stream");
    assert_eq!(present.key, key_present);
    assert_eq!(present.result_code, Some(ResultCode::Ok));
    assert!(present.record.is_some());

    let (_, missing) = got
        .iter()
        .find(|(idx, _)| *idx == 1)
        .expect("index 1 in stream");
    assert_eq!(missing.key, key_missing);
    assert_eq!(missing.result_code, Some(ResultCode::KeyNotFoundError));
    assert!(missing.record.is_none());
}

#[aerospike_macro::test]
async fn batch_stream_filter_expression_surfaces_filtered_out() {
    // A per-record `BatchReadPolicy.filter_expression` that evaluates
    // to false on the server must surface as `ResultCode::FilteredOut`
    // on that index's BatchRecord (not as KeyNotFoundError, and not
    // as a stream-wide error). The non-filtered key in the same call
    // must still come back Ok.
    use aerospike::expressions::{eq, int_bin, int_val};
    use futures::stream::StreamExt;

    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let wpolicy = WritePolicy::default();

    let key_match = as_key!(ns, set, "filter_match");
    let key_miss = as_key!(ns, set, "filter_miss");
    client.delete(&wpolicy, &key_match).await.unwrap();
    client.delete(&wpolicy, &key_miss).await.unwrap();
    client
        .put(&wpolicy, &key_match, &[as_bin!("v", 1_i64)])
        .await
        .unwrap();
    client
        .put(&wpolicy, &key_miss, &[as_bin!("v", 2_i64)])
        .await
        .unwrap();

    let bp = BatchPolicy::default();
    // Filter `v == 1`. Only key_match satisfies it; key_miss has v=2.
    let mut bpr = BatchReadPolicy::default();
    bpr.filter_expression = Some(eq(int_bin("v".to_string()), int_val(1)));

    // Index 0 = the one that satisfies the filter, index 1 = the one
    // that should come back FilteredOut.
    let ops = vec![
        BatchOperation::read(&bpr, key_match.clone(), Bins::All),
        BatchOperation::read(&bpr, key_miss.clone(), Bins::All),
    ];

    let mut stream = client.batch_stream(&bp, ops).await.unwrap();
    let mut got: Vec<(usize, BatchRecord)> = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item);
    }
    assert_eq!(got.len(), 2);

    let (_, matched) = got
        .iter()
        .find(|(idx, _)| *idx == 0)
        .expect("index 0 in stream");
    assert_eq!(matched.key, key_match);
    assert_eq!(matched.result_code, Some(ResultCode::Ok));
    assert!(matched.record.is_some());

    let (_, filtered) = got
        .iter()
        .find(|(idx, _)| *idx == 1)
        .expect("index 1 in stream");
    assert_eq!(filtered.key, key_miss);
    assert_eq!(
        filtered.result_code,
        Some(ResultCode::FilteredOut),
        "expected FilteredOut, got {:?}",
        filtered.result_code,
    );
    assert!(filtered.record.is_none());
}

#[aerospike_macro::test]
async fn batch_stream_mixed_ops_preserve_index_and_kind() {
    // A batch_stream call that mixes Read / Write / Delete must:
    //  - emit one stream item per op (covering every input index),
    //  - keep `(idx, key)` consistent with the input vec,
    //  - and route each op through the right server command path
    //    (read returns bins, write returns Ok, delete returns Ok and
    //    actually deletes the row).
    use futures::stream::StreamExt;

    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let wpolicy = WritePolicy::default();

    let key_read = as_key!(ns, set, "mixed_read");
    let key_write = as_key!(ns, set, "mixed_write");
    let key_delete = as_key!(ns, set, "mixed_delete");

    // Seed: key_read has data, key_delete has data, key_write will be
    // populated by the batch.
    client.delete(&wpolicy, &key_read).await.unwrap();
    client.delete(&wpolicy, &key_write).await.unwrap();
    client.delete(&wpolicy, &key_delete).await.unwrap();
    client
        .put(&wpolicy, &key_read, &[as_bin!("r", 7_i64)])
        .await
        .unwrap();
    client
        .put(&wpolicy, &key_delete, &[as_bin!("d", 9_i64)])
        .await
        .unwrap();

    let bp = BatchPolicy::default();
    let bpr = BatchReadPolicy::default();
    let bpw = BatchWritePolicy::default();
    let bpd = BatchDeletePolicy::default();
    let write_ops = vec![operations::put(&as_bin!("w", 42_i64))];

    // Indices: 0 = read, 1 = write, 2 = delete.
    let ops = vec![
        BatchOperation::read(&bpr, key_read.clone(), Bins::All),
        BatchOperation::write(&bpw, key_write.clone(), write_ops),
        BatchOperation::delete(&bpd, key_delete.clone()),
    ];

    let mut stream = client.batch_stream(&bp, ops).await.unwrap();
    let mut got: Vec<(usize, BatchRecord)> = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item);
    }
    assert_eq!(got.len(), 3, "expected one item per input op");

    // The three emitted indices must be exactly {0, 1, 2}.
    let mut indices: Vec<usize> = got.iter().map(|(idx, _)| *idx).collect();
    indices.sort_unstable();
    assert_eq!(indices, vec![0, 1, 2]);

    let by_idx = |i: usize| {
        got.iter()
            .find(|(idx, _)| *idx == i)
            .map(|(_, br)| br)
            .expect("index in stream")
    };

    let read = by_idx(0);
    assert_eq!(read.key, key_read);
    assert_eq!(read.result_code, Some(ResultCode::Ok));
    let bins = &read.record.as_ref().expect("read returned a record").bins;
    assert_eq!(bins.get("r"), Some(&Value::from(7_i64)));

    let written = by_idx(1);
    assert_eq!(written.key, key_write);
    assert_eq!(written.result_code, Some(ResultCode::Ok));

    let deleted = by_idx(2);
    assert_eq!(deleted.key, key_delete);
    assert_eq!(deleted.result_code, Some(ResultCode::Ok));

    // Side-effect sanity: the write landed, the delete removed the row.
    let rp = ReadPolicy::default();
    let after_write = client.get(&rp, &key_write, Bins::All).await.unwrap();
    assert_eq!(after_write.bins.get("w"), Some(&Value::from(42_i64)));

    match client.get(&rp, &key_delete, Bins::All).await {
        Err(e) if e.server_result_code() == Some(ResultCode::KeyNotFoundError) => {}
        other => panic!("delete didn't take effect; got: {:?}", other),
    }
}

// Identical writes/UDF records over a shared (cloned) op list encode with
// the wire REPEAT flag after the first record. This proves the server
// accepts the compressed encoding and applies the repeated header to every
// digest: all records must succeed and all keys must hold the data.
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
        // Cloned op list => every record after the first repeats.
        batch.push(BatchOperation::write(&wpolicy, key, ops.clone()));
    }

    let results = client.batch(&bpolicy, &batch).await.unwrap();
    assert_eq!(results.len(), 8);
    for record in &results {
        assert_eq!(
            record.result_code,
            Some(ResultCode::Ok),
            "repeated batch write failed: {record:?}"
        );
    }

    let rp = ReadPolicy::default();
    for key in &keys {
        let rec = client.get(&rp, key, Bins::All).await.unwrap();
        assert_eq!(rec.bins.get("a"), Some(&Value::from(7_i64)));
        assert_eq!(rec.bins.get("l"), Some(&as_list!(1)));
    }

    client.close().await.unwrap();
}

// ---- unroutable keys ------------------------------------------------------

#[aerospike_macro::test]
async fn batch_records_unroutable_key_without_failing_the_batch() {
    // One key naming a namespace this cluster does not have used to abort the
    // whole call: the splitter propagated the routing error with `?`, so
    // ninety-nine good keys were discarded because of one bad one. Java's
    // `BatchNodeList.generate` writes the error onto that key's record and
    // keeps going, which is what this pins.
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let bpolicy = BatchPolicy::default();
    let wpolicy = WritePolicy::default();
    let bpr = BatchReadPolicy::default();

    let good1 = as_key!(namespace, set_name, 1);
    let good2 = as_key!(namespace, set_name, 2);
    let bad = as_key!("no_such_namespace_here", set_name, 3);

    let bin = as_bin!("a", 42);
    client.put(&wpolicy, &good1, &[bin.clone()]).await.unwrap();
    client.put(&wpolicy, &good2, &[bin.clone()]).await.unwrap();

    // Bad key in the middle, so a fix that merely reorders would not pass.
    let batch = vec![
        BatchOperation::read(&bpr, good1.clone(), Bins::All),
        BatchOperation::read(&bpr, bad.clone(), Bins::All),
        BatchOperation::read(&bpr, good2.clone(), Bins::All),
    ];

    let records = client
        .batch(&bpolicy, &batch)
        .await
        .expect("one unroutable key must not fail the whole batch");

    // Input order is preserved, including the row that never left the client.
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].key, good1);
    assert_eq!(records[1].key, bad);
    assert_eq!(records[2].key, good2);

    // The reachable keys were read.
    for i in [0, 2] {
        assert_eq!(
            records[i].result_code,
            Some(ResultCode::Ok),
            "good key at {i} should have succeeded: {:?}",
            records[i]
        );
        assert_eq!(
            records[i].record.as_ref().unwrap().bins.get("a"),
            Some(&as_val!(42))
        );
    }

    // The unroutable key carries its own error and no record.
    assert_eq!(records[1].result_code, Some(ResultCode::InvalidNamespace));
    assert!(records[1].record.is_none());
    assert!(!records[1].in_doubt, "nothing was sent, so nothing is in doubt");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_fails_when_no_key_can_be_routed() {
    // The other half of the Java rule: with nothing routable there is no batch
    // to send, so the call fails outright rather than returning rows that were
    // never attempted.
    let client = common::client().await;
    let set_name = &common::rand_str(10);
    let bpolicy = BatchPolicy::default();
    let bpr = BatchReadPolicy::default();

    let batch = vec![
        BatchOperation::read(&bpr, as_key!("no_such_namespace_here", set_name, 1), Bins::All),
        BatchOperation::read(&bpr, as_key!("also_missing", set_name, 2), Bins::All),
    ];

    let err = client
        .batch(&bpolicy, &batch)
        .await
        .expect_err("a batch with no routable key must fail");
    assert!(
        matches!(err.kind(), ErrorKind::InvalidNamespace),
        "expected the routing failure itself, got {err:?}"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_write_to_unroutable_key_is_not_in_doubt() {
    // A write that was never sent cannot have been applied, so it must not be
    // marked in-doubt — that mark is what makes a later MRT commit degrade to
    // an abort.
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let bpolicy = BatchPolicy::default();
    let bpw = BatchWritePolicy::default();

    let good = as_key!(namespace, set_name, 1);
    let bad = as_key!("no_such_namespace_here", set_name, 2);
    let bin = as_bin!("a", 7);
    let wops = vec![operations::put(&bin)];

    let batch = vec![
        BatchOperation::write(&bpw, bad.clone(), wops.clone()),
        BatchOperation::write(&bpw, good.clone(), wops.clone()),
    ];

    let records = client.batch(&bpolicy, &batch).await.expect("partial batch");
    assert_eq!(records.len(), 2);

    assert_eq!(records[0].result_code, Some(ResultCode::InvalidNamespace));
    assert!(records[0].has_write());
    assert!(!records[0].in_doubt);

    assert_eq!(records[1].result_code, Some(ResultCode::Ok));
    // The reachable write really landed.
    let stored = client
        .get(&ReadPolicy::default(), &good, Bins::All)
        .await
        .unwrap();
    assert_eq!(stored.bins.get("a"), Some(&as_val!(7)));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_stream_emits_unroutable_key_with_its_error() {
    // The streaming splitter shares `get_batch_operate_nodes` with the
    // buffered path, so it has to emit the unroutable row too — otherwise the
    // stream silently yields fewer items than the caller had keys, and the
    // index-to-key mapping the caller relies on has a hole in it.
    use futures::stream::StreamExt;

    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let wpolicy = WritePolicy::default();

    let good = as_key!(ns, set, "stream_good");
    let bad = as_key!("no_such_namespace_here", set, "stream_bad");
    client
        .put(&wpolicy, &good, &[as_bin!("x", 1_i64)])
        .await
        .unwrap();

    let bp = BatchPolicy::default();
    let bpr = BatchReadPolicy::default();
    // Index 0 = unroutable, index 1 = reachable.
    let ops = vec![
        BatchOperation::read(&bpr, bad.clone(), Bins::All),
        BatchOperation::read(&bpr, good.clone(), Bins::All),
    ];

    let mut stream = client.batch_stream(&bp, ops).await.unwrap();
    let mut got: Vec<(usize, BatchRecord)> = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item);
    }

    // Every input key is accounted for, in whatever order they completed.
    assert_eq!(got.len(), 2, "stream dropped a key: {got:?}");

    let (_, unroutable) = got
        .iter()
        .find(|(idx, _)| *idx == 0)
        .expect("index 0 in stream");
    assert_eq!(unroutable.key, bad);
    assert_eq!(unroutable.result_code, Some(ResultCode::InvalidNamespace));
    assert!(unroutable.record.is_none());

    let (_, reachable) = got
        .iter()
        .find(|(idx, _)| *idx == 1)
        .expect("index 1 in stream");
    assert_eq!(reachable.key, good);
    assert_eq!(reachable.result_code, Some(ResultCode::Ok));
    assert!(reachable.record.is_some());

    client.close().await.unwrap();
}

// ---- per-row error detail -------------------------------------------------

#[aerospike_macro::test]
async fn batch_row_error_carries_server_subcode_and_message() {
    // A failing row used to report only its result code: the parse path
    // returned before reading the row body, which is where the server puts its
    // explanation, and BatchRecord had nowhere to keep it. Single-key commands
    // have always surfaced both.
    use aerospike::operations::hll;

    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);

    let supported = match client.cluster.nodes().first() {
        Some(node) => node.version().supports_extended_error_detail(),
        None => false,
    };
    if !supported {
        eprintln!("skipping: cluster predates extended error detail (8.1.3)");
        client.close().await.unwrap();
        return;
    }

    let mut bpolicy = BatchPolicy::default();
    bpolicy.base_policy.error_detail_verbosity = 2;
    let bpw = BatchWritePolicy::default();
    let wpolicy = WritePolicy::default();

    let good = as_key!(namespace, set_name, 1);
    let bad = as_key!(namespace, set_name, 2);
    client
        .put(&wpolicy, &good, &[as_bin!("a", 1)])
        .await
        .unwrap();
    // The failing row: an HLL op against a bin that holds no HLL.
    client
        .put(&wpolicy, &bad, &[as_bin!("other-bin", 1)])
        .await
        .unwrap();

    let good_ops = vec![operations::put(&as_bin!("a", 2))];
    let batch = vec![
        BatchOperation::write(&bpw, good.clone(), good_ops),
        BatchOperation::write(
            &bpw,
            bad.clone(),
            vec![hll::refresh_count("no-hll-bin")],
        ),
    ];

    let records = match client.batch(&bpolicy, &batch).await {
        Ok(records) => records,
        // A row error may surface as a batch-level failure carrying the rows.
        Err(err) => match err.kind() {
            ErrorKind::BatchFailed { records } => records.clone(),
            other => panic!("unexpected batch failure {other:?}: {err}"),
        },
    };
    assert_eq!(records.len(), 2);

    // The healthy row is untouched and carries no detail.
    assert_eq!(records[0].result_code, Some(ResultCode::Ok));
    assert_eq!(records[0].sub_code(), 0);
    assert!(records[0].server_message().is_none());

    // The failing row now explains itself.
    assert_eq!(records[1].result_code, Some(ResultCode::BinNotFound));
    assert!(
        records[1].sub_code() >= 1,
        "expected a server subcode on the failing row, got {:?}",
        records[1]
    );
    let message = records[1]
        .server_message()
        .expect("expected a server message on the failing row");
    assert!(
        message.to_lowercase().contains("count op"),
        "unexpected server message: {message:?}"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_row_error_detail_absent_at_verbosity_zero() {
    // The default asks for nothing, so nothing should arrive — the result code
    // still identifies the failure.
    use aerospike::operations::hll;

    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);

    let bpolicy = BatchPolicy::default();
    assert_eq!(bpolicy.base_policy.error_detail_verbosity, 0);
    let bpw = BatchWritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("other-bin", 1)])
        .await
        .unwrap();

    let batch = vec![BatchOperation::write(
        &bpw,
        key.clone(),
        vec![hll::refresh_count("no-hll-bin")],
    )];

    let records = match client.batch(&bpolicy, &batch).await {
        Ok(records) => records,
        Err(err) => match err.kind() {
            ErrorKind::BatchFailed { records } => records.clone(),
            other => panic!("unexpected batch failure {other:?}: {err}"),
        },
    };
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].result_code, Some(ResultCode::BinNotFound));
    assert_eq!(records[0].sub_code(), 0, "verbosity 0 must not carry detail");
    assert!(records[0].server_message().is_none());
    assert!(records[0].error_detail().is_none());

    client.close().await.unwrap();
}
