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

//! Live integration tests for the blocking (sync) client.
//!
//! These are ordinary `#[test]` functions — no async runtime is set up
//! anywhere, which is exactly the point: the sync client must work from
//! plain threaded code (it owns a small fallback runtime internally).
//!
//! A reachable server is required; configure it with `AEROSPIKE_HOSTS`
//! (default `127.0.0.1:3000`) and `AEROSPIKE_USE_SERVICES_ALTERNATE=true`
//! where needed, e.g.:
//!
//! ```text
//! AEROSPIKE_USE_SERVICES_ALTERNATE=true AEROSPIKE_HOSTS=localhost:3100 \
//!     cargo test -p aerospike-sync --test sync_client
//! ```

use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use aerospike_sync::{
    as_bin, as_key, as_val, BatchOperation, BatchPolicy, BatchReadPolicy, Bins, Client,
    ClientPolicy, ReadPolicy, ResultCode, Value, WritePolicy,
};

fn namespace() -> String {
    std::env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

/// Unique set name per test run so parallel/repeated runs don't collide.
fn unique_set(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{nanos:x}")
}

/// One shared client for all tests (the client is thread-safe and the
/// tests run on multiple test threads — exactly the usage pattern the
/// sync facade is for).
fn client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        let hosts =
            std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
        let policy = ClientPolicy {
            use_services_alternate: std::env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(false),
            ..ClientPolicy::default()
        };
        Client::new(&policy, &hosts).unwrap_or_else(|e| {
            panic!("sync tests: could not connect to AEROSPIKE_HOSTS={hosts}: {e}")
        })
    })
}

#[test]
fn connects_and_sees_the_cluster() {
    let client = client();
    assert!(client.is_connected());
    let names = client.node_names();
    assert!(!names.is_empty(), "expected at least one node");
}

#[test]
fn kv_roundtrip() {
    let client = client();
    let ns = namespace();
    let set = unique_set("sync_kv");
    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    let key = as_key!(&ns, &set, 1);

    // put + get
    client
        .put(
            &wpolicy,
            &key,
            &[as_bin!("int", 42), as_bin!("str", "hello")],
        )
        .unwrap();
    let record = client.get(&rpolicy, &key, Bins::All).unwrap();
    assert_eq!(record.bins.get("int"), Some(&as_val!(42)));
    assert_eq!(record.bins.get("str"), Some(&as_val!("hello")));

    // single-bin projection
    let record = client.get(&rpolicy, &key, Bins::from(["str"])).unwrap();
    assert_eq!(record.bins.len(), 1);
    assert_eq!(record.bins.get("str"), Some(&as_val!("hello")));

    // exists / touch / add / append
    assert!(client.exists(&rpolicy, &key).unwrap());
    client.touch(&wpolicy, &key).unwrap();
    client.add(&wpolicy, &key, &[as_bin!("int", 8)]).unwrap();
    client
        .append(&wpolicy, &key, &[as_bin!("str", " world")])
        .unwrap();
    let record = client.get(&rpolicy, &key, Bins::All).unwrap();
    assert_eq!(record.bins.get("int"), Some(&as_val!(50)));
    assert_eq!(record.bins.get("str"), Some(&as_val!("hello world")));

    // delete: true (existed), then a get returns KeyNotFoundError
    assert!(client.delete(&wpolicy, &key).unwrap());
    assert!(!client.exists(&rpolicy, &key).unwrap());
    let err = client.get(&rpolicy, &key, Bins::All).unwrap_err();
    assert_eq!(
        err.server_result_code(),
        Some(ResultCode::KeyNotFoundError),
        "unexpected error: {err}"
    );
}

#[test]
fn operate_combines_write_and_read() {
    use aerospike_sync::operations;

    let client = client();
    let ns = namespace();
    let set = unique_set("sync_ops");
    let key = as_key!(&ns, &set, "op-key");
    let wpolicy = WritePolicy::default();

    let counter = as_bin!("counter", 5);
    let ops = [operations::add(&counter), operations::get_bin("counter")];
    client
        .put(&wpolicy, &key, &[as_bin!("counter", 10)])
        .unwrap();
    let record = client.operate(&wpolicy, &key, &ops).unwrap();
    assert_eq!(record.bins.get("counter"), Some(&as_val!(15)));
}

#[test]
fn batch_reads_return_all_records_in_order() {
    let client = client();
    let ns = namespace();
    let set = unique_set("sync_batch");
    let wpolicy = WritePolicy::default();
    const COUNT: i64 = 25;

    for i in 0..COUNT {
        let key = as_key!(&ns, &set, i);
        client.put(&wpolicy, &key, &[as_bin!("i", i)]).unwrap();
    }

    let brp = BatchReadPolicy::default();
    // Every existing key plus one that doesn't exist.
    let mut ops: Vec<BatchOperation> = (0..COUNT)
        .map(|i| BatchOperation::read(&brp, as_key!(&ns, &set, i), Bins::All))
        .collect();
    ops.push(BatchOperation::read(
        &brp,
        as_key!(&ns, &set, "missing"),
        Bins::All,
    ));

    let results = client.batch(&BatchPolicy::default(), &ops).unwrap();
    assert_eq!(results.len(), COUNT as usize + 1);
    for (i, br) in results.iter().take(COUNT as usize).enumerate() {
        let record = br
            .record
            .as_ref()
            .unwrap_or_else(|| panic!("batch record {i} missing"));
        assert_eq!(record.bins.get("i"), Some(&as_val!(i as i64)));
    }
    assert!(
        results[COUNT as usize].record.is_none(),
        "missing key must have no record"
    );
}

#[test]
fn batch_stream_yields_every_index_exactly_once() {
    let client = client();
    let ns = namespace();
    let set = unique_set("sync_bstream");
    let wpolicy = WritePolicy::default();
    const COUNT: usize = 40;

    for i in 0..COUNT {
        let key = as_key!(&ns, &set, i as i64);
        client
            .put(&wpolicy, &key, &[as_bin!("i", i as i64)])
            .unwrap();
    }

    let brp = BatchReadPolicy::default();
    let ops: Vec<BatchOperation> = (0..COUNT)
        .map(|i| BatchOperation::read(&brp, as_key!(&ns, &set, i as i64), Bins::All))
        .collect();

    // Items arrive in per-node completion order; every original index
    // must appear exactly once with the right payload.
    let mut seen = [false; COUNT];
    for (idx, br) in client.batch_stream(&BatchPolicy::default(), ops).unwrap() {
        assert!(!seen[idx], "index {idx} yielded twice");
        seen[idx] = true;
        let record = br.record.expect("existing key must have a record");
        assert_eq!(record.bins.get("i"), Some(&as_val!(idx as i64)));
    }
    assert!(seen.iter().all(|s| *s), "not all indexes were yielded");
}

#[test]
fn query_streams_all_records() {
    use aerospike_sync::{PartitionFilter, QueryPolicy, Statement};

    let client = client();
    let ns = namespace();
    let set = unique_set("sync_query");
    let wpolicy = WritePolicy::default();
    const COUNT: i64 = 1000;

    for i in 0..COUNT {
        let key = as_key!(&ns, &set, i);
        client.put(&wpolicy, &key, &[as_bin!("i", i)]).unwrap();
    }

    // Filterless query (scan) over the set, consumed through the
    // blocking Recordset iterator — the sync client's query API.
    let stmt = Statement::new(&ns, &set, Bins::All);
    let recordset = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .unwrap();

    let mut total = 0i64;
    let mut count = 0usize;
    for result in &*recordset {
        let record = result.unwrap();
        if let Some(Value::Int(i)) = record.bins.get("i") {
            total += i;
        }
        count += 1;
    }
    assert_eq!(count, COUNT as usize);
    assert_eq!(total, COUNT * (COUNT - 1) / 2);
}

#[test]
fn batch_stream_with_empty_ops_terminates() {
    // Zero batch operations: the iterator must end immediately (or the
    // call must error cleanly) — never hang. Watchdogged so a wedge is a
    // failure, not a stuck test run.
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let client = client();
        let outcome = match client.batch_stream(&BatchPolicy::default(), Vec::new()) {
            Ok(stream) => stream.count(), // must be 0 items
            Err(_) => 0,
        };
        let _ = done_tx.send(outcome);
    });
    let items = done_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("empty batch_stream wedged");
    assert_eq!(items, 0);
}

#[test]
fn dropping_batch_stream_early_is_safe() {
    // Consuming only part of a batch stream and dropping the iterator
    // must not wedge the client: the per-node producers observe the
    // closed channel and stop. The client must remain fully usable.
    let client = client();
    let ns = namespace();
    let set = unique_set("sync_bdrop");
    let wpolicy = WritePolicy::default();
    const COUNT: usize = 40;

    for i in 0..COUNT {
        let key = as_key!(&ns, &set, i as i64);
        client
            .put(&wpolicy, &key, &[as_bin!("i", i as i64)])
            .unwrap();
    }

    let brp = BatchReadPolicy::default();
    let ops: Vec<BatchOperation> = (0..COUNT)
        .map(|i| BatchOperation::read(&brp, as_key!(&ns, &set, i as i64), Bins::All))
        .collect();

    let mut stream = client.batch_stream(&BatchPolicy::default(), ops).unwrap();
    // Take a few items, then walk away mid-stream.
    for _ in 0..3 {
        assert!(stream.next().is_some());
    }
    drop(stream);

    // The client is still healthy afterwards.
    let key = as_key!(&ns, &set, 0);
    let record = client.get(&ReadPolicy::default(), &key, Bins::All).unwrap();
    assert_eq!(record.bins.get("i"), Some(&as_val!(0)));
}

#[test]
fn dropping_recordset_mid_iteration_is_safe() {
    use aerospike_sync::{PartitionFilter, QueryPolicy, Statement};

    // Abandoning a query early: with a small record queue the workers
    // are still pushing when the consumer walks away; the closed channel
    // must fail their sends fast (previously they could await forever on
    // a full queue). The client must remain usable afterwards.
    let client = client();
    let ns = namespace();
    let set = unique_set("sync_qdrop");
    let wpolicy = WritePolicy::default();
    const COUNT: i64 = 200;

    for i in 0..COUNT {
        let key = as_key!(&ns, &set, i);
        client.put(&wpolicy, &key, &[as_bin!("i", i)]).unwrap();
    }

    let qp = QueryPolicy {
        record_queue_size: 4, // keep workers blocked on a full queue
        ..QueryPolicy::default()
    };
    let stmt = Statement::new(&ns, &set, Bins::All);
    let recordset = client.query(&qp, PartitionFilter::all(), stmt).unwrap();

    let mut taken = 0;
    for result in &*recordset {
        result.unwrap();
        taken += 1;
        if taken == 2 {
            break; // abandon mid-stream
        }
    }
    drop(recordset);

    // Follow-up op proves nothing wedged.
    let key = as_key!(&ns, &set, 0);
    let record = client.get(&ReadPolicy::default(), &key, Bins::All).unwrap();
    assert_eq!(record.bins.get("i"), Some(&as_val!(0)));
}

#[test]
fn path_select_loop_survives_repeated_server_rejections() {
    // Regression repro turned test: looping `operate(select_by_path
    // VALUE, all_children)` through the blocking client — alternating a
    // select the server REJECTS every time (path select on a scalar bin)
    // with one it accepts (same select on a map bin) — must complete all
    // iterations. The original report was a wedge where not even the
    // policy timeouts fired, because the timers lived on the same starved
    // executor as the command. A watchdog thread turns a wedge into a
    // test failure instead of a hung test run.
    use std::collections::HashMap;

    use aerospike_sync::operations::cdt_context::ctx_all_children;
    use aerospike_sync::operations::path::{select_by_path, SelectFlag};

    const ITERATIONS: usize = 200;

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let client = client();
        let ns = namespace();
        let set = unique_set("sync_path_neg");
        let key = as_key!(&ns, &set, "hang_probe_neg");

        // Same policy shape as the repro: generous request timeouts that
        // must never be what saves us.
        let wp = {
            let mut wp = WritePolicy::default();
            wp.base_policy.total_timeout = 10_000;
            wp.base_policy.socket_timeout = 10_000;
            wp
        };

        // Seed a scalar bin "s" (the negative target) and a small string
        // map bin "m" (the positive target).
        let mut map: HashMap<Value, Value> = HashMap::new();
        map.insert(as_val!("a"), as_val!("open"));
        map.insert(as_val!("b"), as_val!("close"));
        map.insert(as_val!("c"), as_val!("open"));
        client
            .put(&wp, &key, &[as_bin!("s", 1), as_bin!("m", map)])
            .expect("seed put failed");

        for i in 0..ITERATIONS {
            // Negative path: a select on a scalar bin is rejected by the
            // server — every iteration must return an error, not wedge.
            let ctx = vec![ctx_all_children()];
            let op = select_by_path("s", SelectFlag::VALUE, &ctx);
            let err = client
                .operate(&wp, &key, &[op])
                .expect_err("select on a scalar bin must be rejected");
            assert!(
                err.server_result_code().is_some(),
                "iter {i}: expected a server rejection, got: {err}"
            );

            // Positive path: the same select on the map bin succeeds.
            let ctx = vec![ctx_all_children()];
            let op = select_by_path("m", SelectFlag::VALUE, &ctx);
            let record = client
                .operate(&wp, &key, &[op])
                .unwrap_or_else(|e| panic!("iter {i}: map select failed: {e}"));
            assert!(record.bins.contains_key("m"), "iter {i}: missing bin");
        }
        let _ = done_tx.send(());
    });

    // Wedge guard: the loop is a few hundred fast round trips; if it
    // hasn't finished well within this window, the hang reproduced.
    done_rx
        .recv_timeout(std::time::Duration::from_secs(120))
        .expect("path-select loop wedged: no completion within 120s");
}

#[cfg(feature = "rt-tokio")]
#[test]
fn works_from_inside_a_tokio_runtime_too() {
    // Regression guard for the `block_on` shim: calling the blocking
    // client from within a multi-thread tokio runtime must not panic
    // ("cannot block the current thread from within a runtime") and must
    // not bind the client's background tasks to the caller's runtime —
    // the client futures run on the crate's dedicated runtime via
    // `block_in_place`.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        tokio::task::spawn(async {
            let client = client();
            let ns = namespace();
            let set = unique_set("sync_in_rt");
            let key = as_key!(&ns, &set, 1);
            client
                .put(&WritePolicy::default(), &key, &[as_bin!("x", 1)])
                .unwrap();
            let record = client.get(&ReadPolicy::default(), &key, Bins::All).unwrap();
            assert_eq!(record.bins.get("x"), Some(&as_val!(1)));
        })
        .await
        .unwrap();
    });
}

#[cfg(feature = "rt-async-std")]
#[test]
fn works_from_inside_an_async_std_task_too() {
    // async-std counterpart: calling the blocking client from within an
    // async-std context must work; `spawn_blocking` is the sanctioned
    // way to run blocking code from an async-std task.
    async_std::task::block_on(async {
        async_std::task::spawn_blocking(|| {
            let client = client();
            let ns = namespace();
            let set = unique_set("sync_in_astd");
            let key = as_key!(&ns, &set, 1);
            client
                .put(&WritePolicy::default(), &key, &[as_bin!("x", 1)])
                .unwrap();
            let record = client.get(&ReadPolicy::default(), &key, Bins::All).unwrap();
            assert_eq!(record.bins.get("x"), Some(&as_val!(1)));
        })
        .await;
    });
}
