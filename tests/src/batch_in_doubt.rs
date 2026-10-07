// Copyright 2015-2026 Aerospike, Inc.
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

//! In-doubt reporting when a *write* command times out client-side.
//!
//! A write that reached the wire and whose response never arrived may or may not
//! have been applied, so the error has to say so structurally - `in_doubt` plus a
//! TIMEOUT result code - not only in its message. The batch path and the
//! single-key path must agree on that, which is what these tests pin.
//!
//! Ported from the Java SDK's
//! `UdfTest.batchUdfLongWaitFailsWithClientTimeoutMarksInDoubt`, which asserts
//! `ae.inDoubt` and `ResultCode.TIMEOUT` on the thrown exception.

use crate::common;

use aerospike::{
    as_bin, as_key, AdminPolicy, BatchOperation, BatchPolicy, BatchReadPolicy, BatchUdfPolicy,
    Bins, Client, ClientResultCode, Key, ResultCode, Task, UdfLang, Value, WritePolicy,
};

// A UDF that occupies the server for at least `secs` before writing. `os.clock()`
// is CPU time, so this spins on wall clock via `os.time()`, which ticks in whole
// seconds. Stopping once the clock *reaches* `stop` would end at the next second
// boundary and wait anywhere from 0 to `secs` seconds, so the spin runs until the
// clock moves past it.
// The stall must outlive the 250 ms socket timeout without touching the
// clock: since AER-6914 (server 8.2) the UDF sandbox drops `os`, `io`,
// `debug` and `load*` to prevent escapes, so `os.time()` is a nil index and
// the UDF would fail instantly instead of stalling. A pure-Lua loop works on
// every server. The count is calibrated, not maximal: measured ~13 ns per
// iteration on 8.2, so `WAIT_ITERS` spins for ~530 ms — comfortably past the
// 250 ms socket timeout, yet under the server's 1 s transaction deadline, so
// the server rarely has to kill one (a couple per group run, when the tests'
// own UDFs queue behind each other). That matters because every spinning UDF
// pins a service thread: the earlier 200 M-iteration spin ran until the
// server killed it at 1 s, and a 50-row batch of them held all six threads
// of the dev node for ~8 s, timing out unrelated tests' 1 s-deadline
// commands across the whole suite. Hence also the small row counts below.
const WAIT_UDF: &str = r#"
function wait_and_update(rec, iters)
  local x = 0
  for i = 1, iters do x = (x + i) % 7 end
  if aerospike:exists(rec) then
    rec['bin'] = 1
    aerospike:update(rec)
  else
    rec['bin'] = 1
    aerospike:create(rec)
  end
  return 1
end
"#;

const WAIT_ITERS: i64 = 40_000_000;
const SOCKET_TIMEOUT_MS: u32 = 250;

async fn register_wait_udf(client: &Client) {
    let task = client
        .register_udf(
            &AdminPolicy::default(),
            WAIT_UDF.as_bytes(),
            "wait_udf.lua",
            UdfLang::Lua,
        )
        .await
        .expect("register wait_udf");
    task.wait_till_complete(None).await.expect("udf registered");
}

// Every key of the batch, so all rows are unanswered writes.
fn keys(namespace: &str, set_name: &str, count: usize) -> Vec<Key> {
    (0..count)
        .map(|i| as_key!(namespace, set_name, format!("B-UDF_{i}")))
        .collect()
}

/// The single-key path: the reference behaviour the batch path has to match.
#[aerospike_macro::test]
async fn single_key_udf_client_timeout_marks_in_doubt() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    register_wait_udf(&client).await;

    let mut wpolicy = WritePolicy::default();
    wpolicy.base_policy.socket_timeout = SOCKET_TIMEOUT_MS;
    wpolicy.base_policy.total_timeout = 0;
    wpolicy.base_policy.max_retries = 0;

    let key = as_key!(namespace, &set_name, "S-UDF");
    let err = client
        .execute_udf(
            &wpolicy,
            &key,
            "wait_udf",
            "wait_and_update",
            Some(&[Value::from(WAIT_ITERS)]),
        )
        .await
        .expect_err("the UDF outruns the socket timeout");

    assert!(
        err.is_client_timeout(),
        "expected a client timeout, got {err:?}"
    );
    // Retry exhaustion is MAX_RETRIES_EXCEEDED (-11) on both the single-key
    // and the batch path (Go does the same; Java keeps 9 behind its Timeout
    // exception type, which `ErrorKind::Timeout` mirrors here).
    assert_eq!(
        err.client_result_code(),
        Some(ClientResultCode::MaxRetriesExceeded),
        "expected MAX_RETRIES_EXCEEDED, got {err}"
    );
    assert!(
        err.in_doubt(),
        "a write that reached the wire and never answered is in doubt: {err}"
    );

    client.close().await.unwrap();
}

/// The batch path must report the same thing: `in_doubt` and TIMEOUT, reachable
/// structurally rather than only in the message.
#[aerospike_macro::test]
async fn batch_udf_client_timeout_marks_in_doubt() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    register_wait_udf(&client).await;

    let mut bpolicy = BatchPolicy::default();
    bpolicy.base_policy.socket_timeout = SOCKET_TIMEOUT_MS;
    bpolicy.base_policy.total_timeout = 0;
    bpolicy.base_policy.max_retries = 0;

    // Four rows: enough to prove every unanswered row is stamped, few enough
    // that the spinning UDFs leave service threads free for the rest of the
    // suite (see the note on `WAIT_UDF`).
    let upolicy = BatchUdfPolicy::default();
    let ops: Vec<BatchOperation> = keys(namespace, &set_name, 4)
        .into_iter()
        .map(|key| {
            BatchOperation::udf(
                &upolicy,
                key,
                "wait_udf",
                "wait_and_update",
                Some(vec![Value::from(WAIT_ITERS)]),
            )
        })
        .collect();

    let mut ops = ops;
    let err = client
        .batch(&bpolicy, &mut ops)
        .await
        .expect_err("the UDF outruns the socket timeout");

    // The defect this pins: the error must say in-doubt. The rows were
    // always marked; the error a caller checks must agree with them.
    assert!(
        err.in_doubt(),
        "batch writes that reached the wire and never answered are in doubt: {err}"
    );
    assert!(
        err.is_client_timeout(),
        "expected a client timeout, got {err:?}"
    );

    // Per-row outcomes live on the operations themselves: each unanswered
    // write marked in-doubt and stamped TIMEOUT. This is what a wrapper maps
    // to its own exception, as the Java SDK does with its per-row records.
    assert_eq!(ops.len(), 4);
    assert!(
        ops.iter().all(|op| op.in_doubt()),
        "every unanswered write row is in doubt"
    );
    assert!(
        ops.iter()
            .all(|op| op.result_code() == Some(ResultCode::Timeout)),
        "every unanswered row is stamped TIMEOUT"
    );

    client.close().await.unwrap();
}

/// A batch of *reads* that times out is never in doubt: nothing could have been
/// applied. Guards the fix against marking everything.
#[aerospike_macro::test]
async fn batch_read_client_timeout_is_not_in_doubt() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    register_wait_udf(&client).await;

    // Put the keys first (with a normal policy), then read them back under a
    // socket timeout small enough to fail, using a UDF-loaded server.
    let wpolicy = WritePolicy::default();
    let all_keys = keys(namespace, &set_name, 50);
    for key in &all_keys {
        client
            .put(&wpolicy, key, &[as_bin!("bin", 0)])
            .await
            .unwrap();
    }

    let mut bpolicy = BatchPolicy::default();
    bpolicy.base_policy.socket_timeout = 1; // 1ms: no batch read completes
    bpolicy.base_policy.total_timeout = 0;
    bpolicy.base_policy.max_retries = 0;

    let bpr = BatchReadPolicy::default();
    let ops: Vec<BatchOperation> = all_keys
        .into_iter()
        .map(|key| BatchOperation::read(&bpr, key, Bins::All))
        .collect();

    let mut ops = ops;
    match client.batch(&bpolicy, &mut ops).await {
        Ok(()) => {
            // 1ms was enough on this machine; nothing to assert.
        }
        Err(err) => {
            assert!(!err.in_doubt(), "a read timeout is never in doubt: {err}");
        }
    }

    client.close().await.unwrap();
}

/// A node group of exactly one key takes the single-record fast path. A
/// client timeout there must stamp the row exactly as the multi-key path
/// stamps a node's unanswered rows — TIMEOUT and, for a write, in-doubt —
/// so a batch reports the same failure the same way however its keys hashed
/// across nodes. The singleton row used to come back untouched
/// (`result_code == None`, `in_doubt == false`).
#[aerospike_macro::test]
async fn singleton_group_client_timeout_stamps_row_like_grouped() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    register_wait_udf(&client).await;

    let mut bpolicy = BatchPolicy::default();
    bpolicy.base_policy.socket_timeout = SOCKET_TIMEOUT_MS;
    bpolicy.base_policy.total_timeout = 0;
    bpolicy.base_policy.max_retries = 0;
    let upolicy = BatchUdfPolicy::default();
    let udf = |key| {
        BatchOperation::udf(
            &upolicy,
            key,
            "wait_udf",
            "wait_and_update",
            Some(vec![Value::from(WAIT_ITERS)]),
        )
    };

    // One key: necessarily alone on its node — the fast path.
    let mut alone: Vec<BatchOperation> =
        keys(namespace, &set_name, 1).into_iter().map(udf).collect();
    let err = client
        .batch(&bpolicy, &mut alone)
        .await
        .expect_err("the UDF outruns the socket timeout");
    assert!(
        err.is_client_timeout(),
        "expected a client timeout, got {err:?}"
    );
    assert!(err.in_doubt(), "an unanswered write is in doubt: {err}");
    assert_eq!(
        alone[0].result_code(),
        Some(ResultCode::Timeout),
        "the singleton row must be stamped TIMEOUT, not left untouched"
    );
    assert!(
        alone[0].in_doubt(),
        "the singleton write row must be in doubt"
    );

    // nodes + 1 keys: at least one node holds two or more — the multi-key
    // path — while others may still be singletons. Every unanswered row must
    // look the same regardless.
    let key_count = client.nodes().len() + 1;
    let mut grouped: Vec<BatchOperation> = keys(namespace, &set_name, key_count)
        .into_iter()
        .map(udf)
        .collect();
    let err = client
        .batch(&bpolicy, &mut grouped)
        .await
        .expect_err("the UDF outruns the socket timeout");
    assert!(err.in_doubt());
    for (i, op) in grouped.iter().enumerate() {
        assert_eq!(op.result_code(), Some(ResultCode::Timeout), "row {i}");
        assert!(op.in_doubt(), "row {i} must be in doubt");
    }

    client.close().await.unwrap();
}

/// `Error::iteration()` and the "after N tries" message report the attempts
/// that were actually made. The budget check that finds the budget spent is
/// not a try: with `max_retries = 0` there is exactly one wire attempt, and
/// the error must say 1, not 2 — the value Java reports, and the one the
/// Python SDK exposes as `TimeoutError.iteration`. The sub-error list is
/// the independent witness: it holds every attempt but the last, which
/// rides the cause chain, so it must have `max_retries` entries.
#[aerospike_macro::test]
async fn timeout_reports_the_attempts_actually_made() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    register_wait_udf(&client).await;

    for max_retries in [0u32, 1, 2] {
        let mut wpolicy = WritePolicy::default();
        wpolicy.base_policy.socket_timeout = SOCKET_TIMEOUT_MS;
        wpolicy.base_policy.total_timeout = 0;
        wpolicy.base_policy.max_retries = max_retries as usize;

        let key = as_key!(namespace, &set_name, format!("iter-{max_retries}"));
        let err = client
            .execute_udf(
                &wpolicy,
                &key,
                "wait_udf",
                "wait_and_update",
                Some(&[Value::from(WAIT_ITERS)]),
            )
            .await
            .expect_err("the UDF outruns the socket timeout");

        let attempts = max_retries + 1;
        assert!(
            err.is_client_timeout(),
            "max_retries={max_retries}: {err:?}"
        );
        // Retry exhaustion is MAX_RETRIES_EXCEEDED (-11), as in Java and Go —
        // the single-key path used to report the server timeout code 9.
        assert_eq!(
            err.client_result_code(),
            Some(ClientResultCode::MaxRetriesExceeded),
            "max_retries={max_retries}: {err}"
        );
        assert_eq!(
            err.iteration(),
            Some(attempts),
            "max_retries={max_retries}: iteration must equal the attempts made: {err}"
        );
        assert_eq!(
            err.sub_errors().len(),
            max_retries as usize,
            "max_retries={max_retries}: one sub-error per attempt but the last: {err}"
        );
        let text = err.to_string();
        assert!(
            text.contains(&format!("after {attempts} tries")),
            "max_retries={max_retries}: message must count {attempts} tries: {text}"
        );
    }

    client.close().await.unwrap();
}
