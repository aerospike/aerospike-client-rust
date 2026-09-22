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
    as_bin, as_key, AdminPolicy, BatchOperation, BatchPolicy, BatchReadPolicy, BatchUDFPolicy,
    Bins, Client, Key, ResultCode, Task, UDFLang, Value, WritePolicy,
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
// every server; measured ~14 ns per iteration on 8.2, so `secs` is scaled to
// ~200 M iterations per requested second — over ten times the timeout.
const WAIT_UDF: &str = r#"
function wait_and_update(rec, secs)
  local n = secs * 200000000
  local x = 0
  for i = 1, n do x = (x + i) % 7 end
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

const WAIT_SECS: i64 = 1;
const SOCKET_TIMEOUT_MS: u32 = 250;

async fn register_wait_udf(client: &Client) {
    let task = client
        .register_udf(
            &AdminPolicy::default(),
            WAIT_UDF.as_bytes(),
            "wait_udf.lua",
            UDFLang::Lua,
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
            Some(&[Value::from(WAIT_SECS)]),
        )
        .await
        .expect_err("the UDF outruns the socket timeout");

    assert!(
        err.is_client_timeout(),
        "expected a client timeout, got {err:?}"
    );
    assert_eq!(
        err.result_code(),
        i32::from(u8::from(ResultCode::Timeout)),
        "expected TIMEOUT, got {err}"
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

    let upolicy = BatchUDFPolicy::default();
    let ops: Vec<BatchOperation> = keys(namespace, &set_name, 50)
        .into_iter()
        .map(|key| {
            BatchOperation::udf(
                &upolicy,
                key,
                "wait_udf",
                "wait_and_update",
                Some(vec![Value::from(WAIT_SECS)]),
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
    assert_eq!(ops.len(), 50);
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
            assert!(
                !err.in_doubt(),
                "a read timeout is never in doubt: {err}"
            );
        }
    }

    client.close().await.unwrap();
}
