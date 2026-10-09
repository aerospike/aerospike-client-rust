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

//! Error handling: reading an `Error` the way the client intends.
//!
//! Every failure is one `Error` value. Its `kind()` says where it came from
//! (the server, a timeout, the network, a bad argument); `result_code()` is
//! the cross-client numeric code (server codes are positive, client codes
//! negative); `matches()` is the one-line test for "is it this server
//! status"; and `in_doubt()` answers the question that matters after a
//! failed write: may it have been applied anyway?
//!
//! With `error_detail_verbosity` raised on a policy, a server 8.2.0+ node
//! also attaches a sub-code and a message explaining which operation
//! failed and why.
//!
//! ```bash
//! cargo run --example error_handling
//! ```

use std::env;

use aerospike::operations::scalar;
use aerospike::{
    as_bin, as_key, Bins, Client, ClientPolicy, Error, ErrorKind, ReadPolicy, RecordExistsAction,
    ResultCode, WritePolicy,
};

const SET: &str = "errors_demo";

fn describe(err: &Error) {
    println!("  display:      {err}");
    println!("  result_code:  {}", err.result_code());
    println!("  in_doubt:     {}", err.in_doubt());
    match err.kind() {
        ErrorKind::Server { rc, .. } => println!("  kind:         server status {rc:?}"),
        ErrorKind::Timeout => println!("  kind:         client-side timeout"),
        ErrorKind::Connection => println!("  kind:         connection failure"),
        ErrorKind::InvalidArgument => println!("  kind:         invalid argument"),
        other => println!("  kind:         {other:?}"),
    }
}

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    let key = as_key!("test", SET, "account");
    let _ = client.delete(&wpolicy, &key).await;

    // ---- A server status: the record is not there ----
    // `matches` is the idiomatic check; it is false for client-side errors,
    // so a timeout never masquerades as "not found".
    println!("1. get on a missing key");
    let err = client.get(&rpolicy, &key, Bins::All).await.unwrap_err();
    describe(&err);
    if err.matches(&[ResultCode::KeyNotFoundError]) {
        println!("  -> handled as a miss, not a failure");
    }

    // ---- A write-policy conflict: create-only on an existing record ----
    println!("2. create-only put on an existing record");
    client.put(&wpolicy, &key, &[as_bin!("balance", 100)]).await.unwrap();
    let mut create_only = WritePolicy::default();
    create_only.record_exists_action = RecordExistsAction::CreateOnly;
    let err = client
        .put(&create_only, &key, &[as_bin!("balance", 0)])
        .await
        .unwrap_err();
    describe(&err);
    assert!(err.matches(&[ResultCode::KeyExistsError]));

    // ---- A client-side error: no round trip was made ----
    // Bin names are limited to 15 bytes; the client refuses before sending.
    println!("3. a bin name that is too long");
    let err = client
        .put(&wpolicy, &key, &[as_bin!("this_bin_name_is_too_long", 1)])
        .await
        .unwrap_err();
    describe(&err);
    println!("  client code:  {:?}", err.client_result_code());

    // ---- A type error, with the server's explanation attached ----
    // `error_detail_verbosity` 0 (default) returns just the status; 1 adds
    // a sub-code; 2 adds the server's message; 3 adds an expression trace.
    println!("4. string append on an integer bin, verbosity 2");
    let mut verbose = WritePolicy::default();
    verbose.base_policy.error_detail_verbosity = 2;
    let err = client
        .operate(&verbose, &key, &[scalar::append(&as_bin!("balance", "oops"))])
        .await
        .unwrap_err();
    describe(&err);
    let detailed = client
        .random_node()
        .map(|n| n.version().supports_extended_error_detail())
        .unwrap_or(false);
    if detailed {
        println!("  sub_code:     {}", err.sub_code());
        println!("  server says:  {:?}", err.server_message());
    } else {
        println!("  (server older than 8.2.0: no sub-code or message is attached)");
    }

    // ---- In doubt: the question to ask after a failed write ----
    // A write that fails before it is sent (an argument error, a tripped
    // circuit breaker) is never in doubt. A timeout *after* the request went
    // out may or may not have been applied, and `in_doubt()` says so. The
    // call below fails on the client, so it reports false; a network cut
    // mid-write would report true.
    println!("5. in-doubt on a client-side failure");
    let err = client
        .put(&wpolicy, &key, &[as_bin!("this_bin_name_is_too_long", 1)])
        .await
        .unwrap_err();
    println!("  in_doubt: {} (nothing was sent)", err.in_doubt());

    // ---- Timeouts: client deadline vs server status ----
    // A client-side timeout is `ErrorKind::Timeout`. If the server itself
    // reports one, it arrives as `ErrorKind::Server` with `ResultCode::Timeout`.
    // `is_client_timeout()` distinguishes the two.
    println!("6. telling the two timeouts apart");
    match client.get(&rpolicy, &key, Bins::All).await {
        Ok(rec) => println!("  read ok: balance = {:?}", rec.bins.get("balance")),
        Err(e) if e.is_client_timeout() => println!("  the client gave up waiting: {e}"),
        Err(e) if e.matches(&[ResultCode::Timeout]) => println!("  the server timed out: {e}"),
        Err(e) => println!("  other error: {e}"),
    }

    let _ = client.delete(&wpolicy, &key).await;
    client.close().await.unwrap();
}
