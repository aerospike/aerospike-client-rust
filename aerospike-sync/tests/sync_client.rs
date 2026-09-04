// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Tests for the blocking facade's runtime bridge.
//!
//! Every case here is really the same question: does a blocking call make
//! progress from the thread it was called on? The old bridge
//! (`futures::executor::block_on`) drove no reactor, so the answer depended on
//! whether the caller happened to have a multi-threaded tokio runtime in scope
//! — and when it did not, calls panicked or hung.
//!
//! The four bridge cases need no server; they connect to a closed port and
//! only care that the call comes back at all. `query_with_the_blocking_iterator_terminates`
//! needs a running server, like this crate's doc tests do.

use std::sync::mpsc;
use std::time::Duration;

use aerospike_sync::{Bins, Client, ClientPolicy, Statement};

/// A port nothing listens on, so `Client::new` fails fast instead of
/// depending on a server.
const CLOSED_PORT: &str = "127.0.0.1:1";

/// How long any bridge call gets before the test calls it hung. Generous: the
/// point is to separate "returns" from "never returns", not to measure.
const PATIENCE: Duration = Duration::from_secs(15);

fn unreachable_policy() -> ClientPolicy {
    ClientPolicy {
        timeout: 500,
        fail_if_not_connected: true,
        ..ClientPolicy::default()
    }
}

/// Run `f` on its own thread and fail if it does not finish within
/// [`PATIENCE`]. A panic inside `f` also lands here as a timeout, because the
/// sender dies with the thread — either way the test fails instead of hanging
/// the suite.
fn within_patience<F>(what: &str, f: F)
where
    F: FnOnce() + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(PATIENCE)
        .unwrap_or_else(|_| panic!("{0} did not return (panicked or hung)", what));
}

/// The plain case, and the one the old bridge could not do at all: no tokio
/// runtime anywhere in sight. `Client::new` spawns the cluster tend task, and
/// spawning needs a runtime — with nothing driving one, the call used to panic
/// before it could return an error.
#[test]
fn client_new_from_a_plain_thread_needs_no_ambient_runtime() {
    within_patience("Client::new on a plain thread", || {
        let result = Client::new(&unreachable_policy(), &CLOSED_PORT.to_string());
        assert!(
            result.is_err(),
            "expected a connection error against a closed port"
        );
    });
}

/// A current-thread runtime is the worst case for a blocking call: the calling
/// thread *is* the executor, so blocking on it starves the very tasks the call
/// is waiting for. The bridge must move the wait off this thread.
#[test]
fn blocking_call_inside_a_current_thread_runtime_does_not_deadlock() {
    within_patience("Client::new inside a current-thread runtime", || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(async { Client::new(&unreachable_policy(), &CLOSED_PORT.to_string()) });
        assert!(result.is_err(), "expected a connection error");
    });
}

/// Same call from inside a multi-threaded runtime. Blocking one of its worker
/// threads is allowed, and the future runs on the facade's own runtime either
/// way.
#[test]
fn blocking_call_inside_a_multi_thread_runtime_works() {
    within_patience("Client::new inside a multi-thread runtime", || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(async { Client::new(&unreachable_policy(), &CLOSED_PORT.to_string()) });
        assert!(result.is_err(), "expected a connection error");
    });
}

/// The pattern this crate's own documentation used to require: build a runtime
/// and `enter()` it before calling the blocking client. Such a thread holds a
/// runtime handle without being one of that runtime's worker threads, which is
/// precisely the shape `tokio::task::block_in_place` panics on — so this test
/// pins the bridge's choice to wait on a scoped thread instead.
#[test]
fn an_entered_runtime_guard_still_works() {
    within_patience("Client::new under an entered runtime guard", || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();
        let result = Client::new(&unreachable_policy(), &CLOSED_PORT.to_string());
        assert!(result.is_err(), "expected a connection error");
    });
}

/// End-to-end: a scan consumed through the blocking iterator must yield every
/// record and then *end*. The iterator now parks on the channel, so ending
/// depends on `Recordset::close()` closing it; if it did not, this test would
/// hang forever rather than fail.
///
/// Needs a server, like the doc tests: `AEROSPIKE_HOSTS` (default
/// `127.0.0.1:3000`), `AEROSPIKE_NAMESPACE` (default `test`), and
/// `AEROSPIKE_USE_SERVICES_ALTERNATE`.
#[test]
fn query_with_the_blocking_iterator_terminates() {
    use aerospike_sync::{
        as_bin, as_key, AdminPolicy, PartitionFilter, QueryPolicy, WritePolicy,
    };

    let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let namespace = std::env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set_name = "sync_blocking_iter";

    let policy = ClientPolicy {
        use_services_alternate: std::env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
            .map(|v| {
                let v = v.trim().to_string();
                v.eq_ignore_ascii_case("true") || v == "1"
            })
            .unwrap_or(false),
        ..ClientPolicy::default()
    };

    let client = Client::new(&policy, &hosts).expect("connect");
    client
        .truncate(&AdminPolicy::default(), &namespace, set_name, 0)
        .expect("truncate");
    // Truncate is asynchronous on the server; give it a moment so leftovers
    // from a previous run cannot inflate the count.
    std::thread::sleep(Duration::from_millis(500));

    let wpolicy = WritePolicy::default();
    for i in 0..10 {
        let key = as_key!(namespace.clone(), set_name.to_string(), i);
        client
            .put(&wpolicy, &key, &[as_bin!("i", i)])
            .expect("put");
    }

    let (tx, rx) = mpsc::channel();
    let iterating = std::thread::spawn(move || {
        let statement = Statement::new(&namespace, set_name, Bins::All);
        let recordset = client
            .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
            .expect("query");

        let mut count = 0;
        for record in &*recordset {
            record.expect("record");
            count += 1;
        }
        let _ = tx.send(count);
        client.close().expect("close");
    });

    let count = rx
        .recv_timeout(PATIENCE)
        .expect("blocking iteration never ended");
    iterating.join().unwrap();
    assert_eq!(count, 10, "expected every record, and then the end");
}
