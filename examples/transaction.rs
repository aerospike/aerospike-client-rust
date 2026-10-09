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

//! Multi-record transactions (MRT): commit and abort.
//!
//! Port of the Java client's `Transaction` / `AsyncTransaction` examples.
//! Requires Aerospike server 8.0+ with a strong-consistency-capable
//! namespace; the example skips gracefully on older servers.

use std::env;
use std::sync::Arc;

use aerospike::{as_bin, as_key};
use aerospike::{
    AdminPolicy, Bins, Client, ClientPolicy, ReadPolicy, Txn, TxnRollPolicy, TxnVerifyPolicy, Value,
    WritePolicy,
};

#[tokio::main]
async fn main() {
    run().await;
}

/// True when `ns` is configured with strong consistency (required for MRT).
async fn namespace_is_sc(client: &Client, ns: &str) -> bool {
    let Ok(node) = client.random_node() else {
        return false;
    };
    let info_key = format!("namespace/{ns}");
    match node.info(&AdminPolicy::default(), &[&info_key]).await {
        Ok(map) => map
            .get(&info_key)
            .is_some_and(|info| info.contains("strong-consistency=true")),
        Err(_) => false,
    }
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = std::env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let supported = client
        .random_node()
        .map(|n| n.version().supports_mrt())
        .unwrap_or(false);
    if !supported {
        println!("Server does not support multi-record transactions (requires 8.0+); skipping.");
        client.close().await.unwrap();
        return;
    }
    // Transactions need a strong-consistency namespace; `AEROSPIKE_NAMESPACE`
    // selects it (default `test`).
    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| String::from("test"));
    if !namespace_is_sc(&client, &namespace).await {
        println!(
            "Namespace `{namespace}` is not configured with strong-consistency; \
             multi-record transactions require an SC namespace. Skipping."
        );
        client.close().await.unwrap();
        return;
    }

    let rpolicy = ReadPolicy::default();
    let plain = WritePolicy::default();
    let key1 = as_key!(namespace.as_str(), "txn_demo", "account-a");
    let key2 = as_key!(namespace.as_str(), "txn_demo", "account-b");
    let _ = client.delete(&plain, &key1).await;
    let _ = client.delete(&plain, &key2).await;

    // Seed two "accounts" outside the transaction.
    client
        .put(&plain, &key1, &[as_bin!("balance", 100)])
        .await
        .unwrap();
    client
        .put(&plain, &key2, &[as_bin!("balance", 0)])
        .await
        .unwrap();

    // ---- Commit: transfer 30 from A to B atomically ----
    let txn = Arc::new(Txn::new());
    println!("begin transaction {}", txn.id());

    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    client
        .put(&wp, &key1, &[as_bin!("balance", 70)])
        .await
        .unwrap();
    client
        .put(&wp, &key2, &[as_bin!("balance", 30)])
        .await
        .unwrap();

    let status = client.commit(&txn).await.unwrap();
    println!("commit status: {status:?}");

    let a = client.get(&rpolicy, &key1, Bins::All).await.unwrap();
    let b = client.get(&rpolicy, &key2, Bins::All).await.unwrap();
    println!(
        "after commit: A = {:?}, B = {:?}",
        a.bins.get("balance"),
        b.bins.get("balance")
    );
    assert_eq!(a.bins.get("balance"), Some(&Value::Int(70)));
    assert_eq!(b.bins.get("balance"), Some(&Value::Int(30)));

    // ---- Abort: a failed business check rolls everything back ----
    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    client
        .put(&wp, &key1, &[as_bin!("balance", -1000)])
        .await
        .unwrap();

    // Pretend validation failed; abort instead of committing.
    let status = client.abort(&txn).await.unwrap();
    println!("abort status: {status:?}");

    let a = client.get(&rpolicy, &key1, Bins::All).await.unwrap();
    println!("after abort: A = {:?} (unchanged)", a.bins.get("balance"));
    assert_eq!(a.bins.get("balance"), Some(&Value::Int(70)));

    // ---- Tuning the commit and abort phases ----
    // `commit` runs two batch phases: *verify* (every read in the transaction
    // still sees the version it saw) and *roll* (forward on commit, back on
    // abort). Each phase takes its own policy for timeouts and retries;
    // `commit`/`abort` use the defaults, the `_with_*` variants take yours.
    let mut verify = TxnVerifyPolicy::default();
    verify.batch_policy.base_policy.total_timeout = 5_000;
    let mut roll = TxnRollPolicy::default();
    roll.batch_policy.base_policy.total_timeout = 5_000;
    roll.batch_policy.base_policy.max_retries = 5;

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());
    client.put(&wp, &key2, &[as_bin!("balance", 31)]).await.unwrap();
    let status = client.commit_with_policies(&verify, &roll, &txn).await.unwrap();
    println!("commit_with_policies status: {status:?}");

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());
    client.put(&wp, &key2, &[as_bin!("balance", 0)]).await.unwrap();
    let status = client.abort_with_policy(&roll, &txn).await.unwrap();
    println!("abort_with_policy status: {status:?}");
    let b = client.get(&rpolicy, &key2, Bins::All).await.unwrap();
    assert_eq!(b.bins.get("balance"), Some(&Value::Int(31)));

    let _ = client.delete(&plain, &key1).await;
    let _ = client.delete(&plain, &key2).await;
    client.close().await.unwrap();
}
