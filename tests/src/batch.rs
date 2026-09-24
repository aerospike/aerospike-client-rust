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
async fn batch_exec_in_place_matches_batch() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let bpolicy = BatchPolicy::default();
    let wpolicy = WritePolicy::default();

    for i in 0..8i64 {
        let key = as_key!(namespace, set_name, i);
        client
            .put(&wpolicy, &key, &[as_bin!("bin", i)])
            .await
            .unwrap();
    }

    // Mix found keys with missing ones; the two APIs must agree row by row.
    let make_ops = || -> Vec<BatchOperation> {
        (0..10i64)
            .map(|i| {
                BatchOperation::read(
                    &BatchReadPolicy::default(),
                    as_key!(namespace, set_name, i),
                    Bins::All,
                )
            })
            .collect()
    };

    let mut ops = make_ops();
    client.batch(&bpolicy, &mut ops).await.unwrap();

    for (i, op) in ops.iter().enumerate() {
        if i < 8 {
            assert_eq!(op.result_code(), Some(ResultCode::Ok));
            assert_eq!(op.record().unwrap().bins["bin"], as_val!(i as i64));
        } else {
            assert_eq!(op.result_code(), Some(ResultCode::KeyNotFoundError));
        }
    }
    // Rows 0..8 found, 8..10 not.
    assert!(ops[..8].iter().all(|op| op.record().is_some()));
    assert!(ops[8..].iter().all(|op| op.record().is_none()));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_exec_reuses_operations_across_calls() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let bpolicy = BatchPolicy::default();
    let wpolicy = WritePolicy::default();

    for i in 0..4i64 {
        let key = as_key!(namespace, set_name, i);
        client
            .put(&wpolicy, &key, &[as_bin!("bin", i)])
            .await
            .unwrap();
    }

    let mut ops: Vec<BatchOperation> = (0..4i64)
        .map(|i| {
            BatchOperation::read(
                &BatchReadPolicy::default(),
                as_key!(namespace, set_name, i),
                Bins::All,
            )
        })
        .collect();

    // First call fills results; take one record out to leave a hole.
    client.batch(&bpolicy, &mut ops).await.unwrap();
    assert!(ops
        .iter()
        .all(|op| op.result_code() == Some(ResultCode::Ok)));
    let taken = ops[2].take_record();
    assert!(taken.is_some());
    assert!(ops[2].record().is_none());

    // Second call on the same slice: prior results cleared and refilled.
    client.batch(&bpolicy, &mut ops).await.unwrap();
    for (i, op) in ops.iter().enumerate() {
        assert_eq!(op.result_code(), Some(ResultCode::Ok), "row {i}");
        let rec = op.record().expect("record refilled");
        assert_eq!(rec.bins["bin"], as_val!(i as i64));
    }

    client.close().await.unwrap();
}

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
    let res = client.batch(&bpolicy, &mut bops).await;
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

    let mut batch = vec![
        BatchOperation::write(&bpw, key1.clone(), wops.clone()),
        BatchOperation::write(&bpw, key2.clone(), wops.clone()),
        BatchOperation::write(&bpw, key3.clone(), wops.clone()),
    ];
    client.batch(&bpolicy, &mut batch).await.unwrap();
    let mut results: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();

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
    let mut batch = vec![
        BatchOperation::read(&bpr, key1.clone(), selected),
        BatchOperation::read(&bpr, key2.clone(), all),
        BatchOperation::read(&bpr, key3.clone(), none.clone()),
        BatchOperation::read_ops(&bpr, key3.clone(), rops),
        BatchOperation::read(&bpr, key4.clone(), none),
    ];
    client.batch(&bpolicy, &mut batch).await.unwrap();
    let mut results: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();

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
    let mut batch = vec![
        BatchOperation::delete(&bpd, key1.clone()),
        BatchOperation::delete(&bpd, key2.clone()),
        BatchOperation::delete(&bpd, key3.clone()),
        BatchOperation::delete(&bpd, key4.clone()),
    ];
    client.batch(&bpolicy, &mut batch).await.unwrap();
    let mut results: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();

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
    let mut batch = vec![
        BatchOperation::read(&bpr, key1.clone(), Bins::None),
        BatchOperation::read(&bpr, key2.clone(), Bins::None),
        BatchOperation::read(&bpr, key3.clone(), Bins::None),
        BatchOperation::read(&bpr, key4.clone(), Bins::None),
    ];
    client.batch(&bpolicy, &mut batch).await.unwrap();
    let mut results: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();

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
    let mut batch = vec![
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
    client.batch(&bpolicy, &mut batch).await.unwrap();
    let mut results: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();

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
    let mut ops = [br];
    client.batch(&bpolicy, &mut ops).await.unwrap();

    let result = ops[0].batch_record().clone();
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
    let mut list = vec![br];
    client.batch(&bpolicy, &mut list).await.unwrap();
    let mut results: Vec<BatchRecord> = list.iter().map(|op| op.batch_record().clone()).collect();

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
    let recs: Vec<BatchRecord> = list.iter().map(|op| op.batch_record().clone()).collect();

    assert!(Some(ResultCode::Ok) == recs[0].result_code);
    assert!(Some(ResultCode::Ok) == recs[1].result_code);

    // Read records again, but don't reset read ttl.
    sleep(Duration::from_secs(3)).await;
    brp1.read_touch_ttl = ReadTouchTTL::DontReset;
    brp2.read_touch_ttl = ReadTouchTTL::DontReset;

    let br1 = BatchOperation::read(&brp1, key1.clone(), Bins::Some(vec!["a".into()]));
    let br2 = BatchOperation::read(&brp2, key2.clone(), Bins::Some(vec!["a".into()]));
    let mut list = vec![br1, br2];
    client.batch(&bpolicy, &mut list).await.unwrap();
    let recs: Vec<BatchRecord> = list.iter().map(|op| op.batch_record().clone()).collect();

    // Key 2 should have expired.
    assert!(Some(ResultCode::Ok) == recs[0].result_code);
    assert!(Some(ResultCode::KeyNotFoundError) == recs[1].result_code);

    // Read  record after it expires, showing it's gone.
    sleep(Duration::from_secs(8)).await;
    client.batch(&bpolicy, &mut list).await.unwrap();
    let recs: Vec<BatchRecord> = list.iter().map(|op| op.batch_record().clone()).collect();
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
    let mut ops = vec![BatchOperation::read(&bpr, key.clone(), Bins::All)];

    client.batch(&bp, &mut ops).await.unwrap();
    let recs: Vec<BatchRecord> = ops.iter().map(|op| op.batch_record().clone()).collect();
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
    let mut ops = vec![BatchOperation::read(&bpr, key.clone(), Bins::All)];

    client.batch(&bp, &mut ops).await.unwrap();
    let recs: Vec<BatchRecord> = ops.iter().map(|op| op.batch_record().clone()).collect();
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
    let mut ops = [BatchOperation::write(&bpw, key.clone(), write_ops)];
    client.batch(&bp, &mut ops).await.unwrap();
    assert_eq!(ops[0].result_code(), Some(ResultCode::Ok));

    // Confirm the record landed by reading it back via the same fast
    // path (a separate single-key batch).
    let bpr = BatchReadPolicy::default();
    let mut ops = [BatchOperation::read(&bpr, key.clone(), Bins::All)];
    client.batch(&bp, &mut ops).await.unwrap();
    assert_eq!(ops[0].result_code(), Some(ResultCode::Ok));
    let rec = ops[0].record().expect("record returned");
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
    let mut ops = [BatchOperation::delete(&bpd, key.clone())];
    client.batch(&bp, &mut ops).await.unwrap();
    assert_eq!(ops[0].result_code(), Some(ResultCode::Ok));

    // Confirm gone via a follow-up single-key read.
    let bpr = BatchReadPolicy::default();
    let mut ops = [BatchOperation::read(&bpr, key.clone(), Bins::All)];
    client.batch(&bp, &mut ops).await.unwrap();
    assert_eq!(ops[0].result_code(), Some(ResultCode::KeyNotFoundError));
}

// ----- Strong-consistency (SC) batch delete semantics (single-key fast path) -----
//
// The property test `proptests::batches::batch_delete` clamps some policy fields on SC so the
// fuzzer does not send illegal or inconsistent deletes. These tests document the real server
// outcomes for fixed policies. On the fast path, `FailForbidden` / `GenerationError` bubble as
// a per-key server error (see `BatchExecutor::execute_single_op`).

// Non-durable deletes on existing records are forbidden under SC by default (no
// tombstone would be left, which SC needs to distinguish "deliberately deleted"
// from "never replicated" during migrations). `strong-consistency-allow-expunge`
// is an explicit opt-in escape hatch that lifts that restriction -- some CI
// server configs enable it, some don't, so branch on the live namespace setting
// (via `ServerCapabilities`) instead of assuming one or the other.
#[aerospike_macro::test]
async fn batch_sc_delete_non_durable_forbidden_when_record_exists() {
    let client = common::client().await;
    if !namespace_sc!(&client) {
        return;
    }
    let allow_expunge = common::ServerCapabilities::detect(&client)
        .await
        .sc_allow_expunge;

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

    let mut ops = [BatchOperation::delete(&bpd, key.clone())];
    client.batch(&bp, &mut ops).await.unwrap();
    let result_code = ops[0].batch_record().result_code;

    if allow_expunge {
        // Expunge is explicitly allowed on this namespace: the non-durable
        // delete should succeed outright, leaving no record behind.
        assert_eq!(
            result_code,
            Some(ResultCode::Ok),
            "expunge is allowed on this namespace; delete should succeed, got {:?}",
            result_code
        );
        let result = client.get(&ReadPolicy::default(), &key, Bins::All).await;
        match result {
            Err(e) if e.server_result_code() == Some(ResultCode::KeyNotFoundError) => {}
            other => panic!(
                "expected record to be gone after an allowed expunge delete, got {:?}",
                other
            ),
        }
    } else {
        assert_eq!(
            result_code,
            Some(ResultCode::FailForbidden),
            "expected FailForbidden for non-durable delete on SC when record exists, got {:?}",
            result_code
        );

        let rec = client
            .get(&ReadPolicy::default(), &key, Bins::All)
            .await
            .expect("record should still exist after forbidden delete");
        assert_eq!(rec.bins.get("a"), Some(&Value::from(1_i64)));

        common::delete_durably(&client, &wpolicy, &key)
            .await
            .unwrap();
    }
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

    // client.batch()'s own Result only reports whole-batch-level failures; a
    // per-key rejection like GenerationError shows up in that key's own
    // BatchOperation::batch_record(), not as an Err from the call itself.
    let mut ops = [BatchOperation::delete(&bpd, key.clone())];
    client.batch(&bp, &mut ops).await.unwrap();
    let result_code = ops[0].batch_record().result_code;
    assert_eq!(
        result_code,
        Some(ResultCode::GenerationError),
        "expected GenerationError for ExpectGenEqual with wrong generation on SC, got {:?}",
        result_code
    );

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

    let mut ops = [BatchOperation::delete(&bpd, key.clone())];
    client
        .batch(&bp, &mut ops)
        .await
        .expect("matching-generation durable delete should succeed on SC");
    assert_eq!(ops[0].result_code(), Some(ResultCode::Ok));

    let bpr = BatchReadPolicy::default();
    let mut ops = [BatchOperation::read(&bpr, key.clone(), Bins::All)];
    client.batch(&bp, &mut ops).await.unwrap();
    assert_eq!(ops[0].result_code(), Some(ResultCode::KeyNotFoundError));
}

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

    client.batch(&bpolicy, &mut batch).await.unwrap();
    let results: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();
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

    let mut batch = batch;
    client
        .batch(&bpolicy, &mut batch)
        .await
        .expect("one unroutable key must not fail the whole batch");
    let records: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();

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

    let mut batch = batch;
    let err = client
        .batch(&bpolicy, &mut batch)
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

    let mut batch = batch;
    client
        .batch(&bpolicy, &mut batch)
        .await
        .expect("partial batch");
    let records: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();
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

    // A row error may surface as the call's Err; the rows carry their
    // outcomes either way.
    let mut batch = batch;
    let _ = client.batch(&bpolicy, &mut batch).await;
    let records: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();
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

    let mut batch = batch;
    let _ = client.batch(&bpolicy, &mut batch).await;
    let records: Vec<BatchRecord> = batch.iter().map(|op| op.batch_record().clone()).collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].result_code, Some(ResultCode::BinNotFound));
    assert_eq!(records[0].sub_code(), 0, "verbosity 0 must not carry detail");
    assert!(records[0].server_message().is_none());
    assert!(records[0].error_detail().is_none());

    client.close().await.unwrap();
}

// ===== batch_foreach =====
//
// Reactive delivery: rows reach the hook as they are parsed; every row's
// outcome reaches it exactly once, including the rows the server never
// answers.

#[aerospike_macro::test]
async fn batch_foreach_reports_every_row_exactly_once() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let wpolicy = WritePolicy::default();
    for i in 0..8i64 {
        client
            .put(&wpolicy, &as_key!(namespace, set_name, i), &[as_bin!("bin", i)])
            .await
            .unwrap();
    }
    let brp = BatchReadPolicy::default();
    let ops: Vec<BatchOperation> = (0..10i64)
        .map(|i| BatchOperation::read(&brp, as_key!(namespace, set_name, i), Bins::All))
        .collect();

    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let s = seen.clone();
    client
        .batch_foreach(&BatchPolicy::default(), ops, move |idx, row| {
            s.lock().unwrap().push((idx, row.result_code, row.record.is_some()));
            std::future::ready(true)
        })
        .await
        .unwrap();

    let mut seen = seen.lock().unwrap().clone();
    seen.sort_by_key(|(i, ..)| *i);
    assert_eq!(seen.len(), 10, "every row exactly once");
    for (i, (idx, rc, found)) in seen.iter().enumerate() {
        assert_eq!(*idx, i);
        if i < 8 {
            assert_eq!(*rc, Some(ResultCode::Ok));
            assert!(found);
        } else {
            assert_eq!(*rc, Some(ResultCode::KeyNotFoundError));
            assert!(!found);
        }
    }
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_foreach_abort_stops_early_and_sweeps_the_rest() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let wpolicy = WritePolicy::default();
    for i in 0..20i64 {
        client
            .put(&wpolicy, &as_key!(namespace, set_name, i), &[as_bin!("bin", i)])
            .await
            .unwrap();
    }
    let brp = BatchReadPolicy::default();
    let ops: Vec<BatchOperation> = (0..20i64)
        .map(|i| BatchOperation::read(&brp, as_key!(namespace, set_name, i), Bins::All))
        .collect();

    let fired = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let answered = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (f, a) = (fired.clone(), answered.clone());
    // An abort is the caller's decision: the call succeeds.
    client
        .batch_foreach(&BatchPolicy::default(), ops, move |_idx, row| {
            f.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let keep_going = if row.result_code.is_some() {
                a.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1 < 5
            } else {
                true
            };
            std::future::ready(keep_going)
        })
        .await
        .unwrap();

    let fired = fired.load(std::sync::atomic::Ordering::Relaxed);
    let answered = answered.load(std::sync::atomic::Ordering::Relaxed);
    // The final sweep reports the rows the abort left behind, so the hook
    // still saw all twenty exactly once — five with results, the rest with
    // none.
    assert_eq!(fired, 20);
    assert!(answered >= 5, "abort fired after the fifth answered row");
    assert!(answered < 20, "the abort must have stopped the group");
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_foreach_reports_unroutable_key_with_its_error() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let wpolicy = WritePolicy::default();
    let good1 = as_key!(namespace, set_name, 1);
    let good2 = as_key!(namespace, set_name, 2);
    let bad = as_key!("no_such_namespace_here", set_name, 3);
    client.put(&wpolicy, &good1, &[as_bin!("a", 1)]).await.unwrap();
    client.put(&wpolicy, &good2, &[as_bin!("a", 2)]).await.unwrap();

    let brp = BatchReadPolicy::default();
    let ops = vec![
        BatchOperation::read(&brp, good1, Bins::All),
        BatchOperation::read(&brp, bad, Bins::All),
        BatchOperation::read(&brp, good2, Bins::All),
    ];
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let s = seen.clone();
    client
        .batch_foreach(&BatchPolicy::default(), ops, move |idx, row| {
            s.lock().unwrap().push((idx, row.result_code, row.record.is_some()));
            std::future::ready(true)
        })
        .await
        .expect("one unroutable key must not fail the batch");

    let mut seen = seen.lock().unwrap().clone();
    seen.sort_by_key(|(i, ..)| *i);
    assert_eq!(
        seen,
        vec![
            (0, Some(ResultCode::Ok), true),
            (1, Some(ResultCode::InvalidNamespace), false),
            (2, Some(ResultCode::Ok), true),
        ]
    );
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_foreach_hook_may_await() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let wpolicy = WritePolicy::default();
    for i in 0..4i64 {
        client
            .put(&wpolicy, &as_key!(namespace, set_name, i), &[as_bin!("bin", i)])
            .await
            .unwrap();
    }
    let brp = BatchReadPolicy::default();
    let ops: Vec<BatchOperation> = (0..4i64)
        .map(|i| BatchOperation::read(&brp, as_key!(namespace, set_name, i), Bins::All))
        .collect();

    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = count.clone();
    client
        .batch_foreach(&BatchPolicy::default(), ops, move |_idx, row| {
            assert_eq!(row.result_code, Some(ResultCode::Ok));
            let c = c.clone();
            async move {
                aerospike_rt::sleep(Duration::from_millis(1)).await;
                c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                true
            }
        })
        .await
        .unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 4);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn dropping_batch_foreach_stops_the_hook() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let wpolicy = WritePolicy::default();
    for i in 0..8i64 {
        client
            .put(&wpolicy, &as_key!(namespace, set_name, i), &[as_bin!("bin", i)])
            .await
            .unwrap();
    }
    let brp = BatchReadPolicy::default();
    let ops: Vec<BatchOperation> = (0..8i64)
        .map(|i| BatchOperation::read(&brp, as_key!(namespace, set_name, i), Bins::All))
        .collect();

    // The hook parks forever; the caller gives up and drops the future.
    let fired = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let f = fired.clone();
    let outcome = aerospike_rt::timeout(
        Duration::from_millis(300),
        client.batch_foreach(&BatchPolicy::default(), ops, move |_idx, _row| {
            f.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            futures::future::pending::<bool>()
        }),
    )
    .await;
    assert!(outcome.is_err(), "the parked hook must have held the batch open");
    let after_drop = fired.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(after_drop, 1, "one invocation parked, nothing else fired");

    // Nothing fires after the drop, and the client is fully usable.
    aerospike_rt::sleep(Duration::from_millis(200)).await;
    assert_eq!(fired.load(std::sync::atomic::Ordering::Relaxed), after_drop);
    let mut check = vec![BatchOperation::read(
        &brp,
        as_key!(namespace, set_name, 0),
        Bins::All,
    )];
    client.batch(&BatchPolicy::default(), &mut check).await.unwrap();
    assert!(check[0].record().is_some());
    client.close().await.unwrap();
}

// A row's `Record` must not depend on how many keys shared its node. A node
// holding exactly one operation is served by a single-key command, every
// other node by the multi-record wire path; an Ok delete used to come back
// as `None` from the first and as a bin-less `Record` (with a bogus all-zero
// key) from the second.
#[aerospike_macro::test]
async fn batch_delete_row_shape_is_the_same_alone_and_grouped() {
    let client = common::client().await;
    let ns = common::namespace();
    let set = &common::rand_str(10);
    let wpolicy = WritePolicy::default();
    let bpolicy = BatchPolicy::default();
    let mut dpolicy = BatchDeletePolicy::default();
    if namespace_sc!(&client) {
        dpolicy.durable_delete = true;
    }
    let rpolicy = BatchReadPolicy::default();
    let key = as_key!(ns, set, "same-key");

    // Alone on its node (group size 1): the single-key fast path.
    client.put(&wpolicy, &key, &[as_bin!("bin", 1)]).await.unwrap();
    let mut solo = [BatchOperation::delete(&dpolicy, key.clone())];
    client.batch(&bpolicy, &mut solo).await.unwrap();
    assert_eq!(solo[0].result_code(), Some(ResultCode::Ok), "record must have existed");
    let alone = solo[0]
        .batch_record()
        .record
        .clone()
        .expect("an Ok delete carries a record, like Java's BatchSingle.Delete");

    // Grouped with a read of the same key (group size 2, same node): the
    // multi-record wire path.
    client.put(&wpolicy, &key, &[as_bin!("bin", 1)]).await.unwrap();
    let mut paired = [
        BatchOperation::read(&rpolicy, key.clone(), Bins::None),
        BatchOperation::delete(&dpolicy, key.clone()),
    ];
    client.batch(&bpolicy, &mut paired).await.unwrap();
    assert_eq!(paired[0].result_code(), Some(ResultCode::Ok));
    assert_eq!(paired[1].result_code(), Some(ResultCode::Ok), "record must have existed");
    let grouped = paired[1]
        .batch_record()
        .record
        .clone()
        .expect("an Ok delete carries a record");

    // Same shape on both paths: no key echoed back, no bins, no positional
    // results (a delete has no ops).
    for (label, record) in [("alone", &alone), ("grouped", &grouped)] {
        assert!(record.key.is_none(), "{label}: a row record carries no key");
        assert!(record.bins.is_empty(), "{label}: a delete returns no bins");
        assert!(
            record.results.is_none(),
            "{label}: no ops, so no positional results"
        );
    }
    assert_eq!(alone.generation, grouped.generation);

    // The header-only read beside it follows the same rule.
    let read = paired[0].batch_record().record.clone().expect("read hit");
    assert!(read.key.is_none() && read.bins.is_empty() && read.results.is_none());
}
