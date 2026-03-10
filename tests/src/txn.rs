// Copyright 2015-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration tests for Multi-Record Transactions (MRT).
//! Requires Aerospike Server version >= 8.0 with strong-consistency enabled.

use std::sync::Arc;

use aerospike::{
    as_bin, as_key, operations, Bins, CommitStatus, AbortStatus, Error, ReadPolicy, ResultCode,
    Txn, Value, WritePolicy,
};
use aerospike::policy::AdminPolicy;

use crate::common;

/// Check if the server supports MRT (version >= 8.0).
async fn server_supports_mrt(client: &aerospike::Client) -> bool {
    match client.cluster.get_random_node() {
        Ok(node) => node.version().supports_mrt(),
        Err(_) => false,
    }
}

/// Check if the namespace is configured with strong-consistency.
async fn namespace_is_sc(client: &aerospike::Client, ns: &str) -> bool {
    let node = match client.cluster.get_random_node() {
        Ok(n) => n,
        Err(_) => return false,
    };

    let info_key = format!("namespace/{ns}");
    let result = node.info(&AdminPolicy::default(), &[&info_key]).await;
    match result {
        Ok(map) => {
            if let Some(info) = map.get(&info_key) {
                info.contains("strong-consistency=true")
                    || info.contains("strong-consistency-allow-expunge=true")
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

/// Skip test if MRT is not supported.
macro_rules! skip_if_no_mrt {
    ($client:expr) => {
        if !server_supports_mrt($client).await {
            eprintln!("Skipping MRT test: server version < 8.0");
            return;
        }
        if !namespace_is_sc($client, common::namespace()).await {
            eprintln!("Skipping MRT test: namespace not configured with strong-consistency");
            return;
        }
    };
}

// =============================================================================
// Empty transaction tests
// =============================================================================

#[aerospike_macro::test]
async fn txn_commit_empty() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let txn = Arc::new(Txn::new());
    let status = client.commit(&txn).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);
}

#[aerospike_macro::test]
async fn txn_abort_empty() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let txn = Arc::new(Txn::new());
    let status = client.abort(&txn).await.unwrap();
    assert_eq!(status, AbortStatus::Ok);
}

// =============================================================================
// Write and commit
// =============================================================================

#[aerospike_macro::test]
async fn txn_write_and_commit() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    // Write inside txn
    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    client
        .put(&wp, &key, &[as_bin!("bin", "val2")])
        .await
        .unwrap();

    let status = client.commit(&txn).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);

    // Verify committed value
    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val2")));
}

// =============================================================================
// Write twice in same txn
// =============================================================================

#[aerospike_macro::test]
async fn txn_write_twice() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    // Pre-populate without txn
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    // Write twice in txn
    client
        .put(&wp, &key, &[as_bin!("bin", "val2")])
        .await
        .unwrap();

    let status = client.commit(&txn).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val2")));
}

// =============================================================================
// Write conflict between two transactions
// =============================================================================

#[aerospike_macro::test]
async fn txn_write_conflict() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    let txn1 = Arc::new(Txn::new());
    let mut wp1 = WritePolicy::default();
    wp1.base_policy.txn = Some(txn1.clone());

    let txn2 = Arc::new(Txn::new());
    let mut wp2 = WritePolicy::default();
    wp2.base_policy.txn = Some(txn2.clone());

    // txn1 writes first
    client
        .put(&wp1, &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    // txn2 should be blocked
    let err = client
        .put(&wp2, &key, &[as_bin!("bin", "val2")])
        .await
        .unwrap_err();

    match err {
        Error::ServerError(ResultCode::MrtBlocked, _, _) => {}
        other => panic!("Expected MrtBlocked, got: {other:?}"),
    }

    // Commit txn1
    let status = client.commit(&txn1).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);

    // Commit txn2 (empty, no writes succeeded)
    let status = client.commit(&txn2).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);

    // Verify txn1's value won
    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val1")));
}

// =============================================================================
// Blocked before other transaction is committed
// =============================================================================

#[aerospike_macro::test]
async fn txn_blocked_before_commit() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    // Write inside txn
    client
        .put(&wp, &key, &[as_bin!("bin", "val2")])
        .await
        .unwrap();

    // Non-txn write to the same key should be blocked
    let err = client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val3")])
        .await
        .unwrap_err();

    match err {
        Error::ServerError(ResultCode::MrtBlocked, _, _) => {}
        other => panic!("Expected MrtBlocked, got: {other:?}"),
    }

    // Clean up: commit the txn
    client.commit(&txn).await.unwrap();
}

// =============================================================================
// Write and read within txn
// =============================================================================

#[aerospike_macro::test]
async fn txn_write_and_read() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    client
        .put(&wp, &key, &[as_bin!("bin", "val2")])
        .await
        .unwrap();

    let status = client.commit(&txn).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val2")));
}

// =============================================================================
// Write and abort
// =============================================================================

#[aerospike_macro::test]
async fn txn_write_and_abort() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    client
        .put(&wp, &key, &[as_bin!("bin", "val2")])
        .await
        .unwrap();

    // Read inside txn should see val2
    let mut rp = ReadPolicy::default();
    rp.base_policy.txn = Some(txn.clone());

    let record = client.get(&rp, &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val2")));

    // Abort
    let status = client.abort(&txn).await.unwrap();
    assert_eq!(status, AbortStatus::Ok);

    // After abort, should see original value
    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val1")));
}

// =============================================================================
// Delete and commit
// =============================================================================

#[aerospike_macro::test]
async fn txn_delete_and_commit() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.durable_delete = true;
    wp.base_policy.txn = Some(txn.clone());

    let existed = client.delete(&wp, &key).await.unwrap();
    assert!(existed);

    let status = client.commit(&txn).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);

    // Key should be gone
    let result = client.get(&ReadPolicy::default(), &key, Bins::All).await;
    match result {
        Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {}
        other => panic!("Expected KeyNotFoundError, got: {other:?}"),
    }
}

// =============================================================================
// Delete and abort
// =============================================================================

#[aerospike_macro::test]
async fn txn_delete_and_abort() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.durable_delete = true;
    wp.base_policy.txn = Some(txn.clone());

    let existed = client.delete(&wp, &key).await.unwrap();
    assert!(existed);

    let status = client.abort(&txn).await.unwrap();
    assert_eq!(status, AbortStatus::Ok);

    // After abort, record should still exist
    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val1")));
}

// =============================================================================
// Delete twice in same txn
// =============================================================================

#[aerospike_macro::test]
async fn txn_delete_twice() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.durable_delete = true;
    wp.base_policy.txn = Some(txn.clone());

    let existed = client.delete(&wp, &key).await.unwrap();
    assert!(existed);

    let existed = client.delete(&wp, &key).await.unwrap();
    assert!(!existed);

    let status = client.commit(&txn).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);

    // Key should be gone
    let result = client.get(&ReadPolicy::default(), &key, Bins::All).await;
    match result {
        Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {}
        other => panic!("Expected KeyNotFoundError, got: {other:?}"),
    }
}

// =============================================================================
// Touch and commit
// =============================================================================

#[aerospike_macro::test]
async fn txn_touch_and_commit() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    client.touch(&wp, &key).await.unwrap();

    let status = client.commit(&txn).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val1")));
    assert!(record.generation > 1);
}

// =============================================================================
// Touch and abort
// =============================================================================

#[aerospike_macro::test]
async fn txn_touch_and_abort() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("bin", "val1")])
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    client.touch(&wp, &key).await.unwrap();

    let status = client.abort(&txn).await.unwrap();
    assert_eq!(status, AbortStatus::Ok);

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val1")));
}

// =============================================================================
// Operate write and commit
// =============================================================================

#[aerospike_macro::test]
async fn txn_operate_write_and_commit() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate with two bins
    client
        .put(
            &WritePolicy::default(),
            &key,
            &[as_bin!("bin", "val1"), as_bin!("bin2", "bal1")],
        )
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    let bin = as_bin!("bin", "val2");
    let record = client
        .operate(
            &wp,
            &key,
            &[operations::put(&bin), operations::get_bin("bin2")],
        )
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin2"), Some(&Value::from("bal1")));

    let status = client.commit(&txn).await.unwrap();
    assert_eq!(status, CommitStatus::Ok);

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val2")));
}

// =============================================================================
// Operate write and abort
// =============================================================================

#[aerospike_macro::test]
async fn txn_operate_write_and_abort() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);
    let key = as_key!(ns, set, &common::rand_str(50));

    // Pre-populate with two bins
    client
        .put(
            &WritePolicy::default(),
            &key,
            &[as_bin!("bin", "val1"), as_bin!("bin2", "bal1")],
        )
        .await
        .unwrap();

    let txn = Arc::new(Txn::new());
    let mut wp = WritePolicy::default();
    wp.base_policy.txn = Some(txn.clone());

    let bin = as_bin!("bin", "val2");
    let record = client
        .operate(
            &wp,
            &key,
            &[operations::put(&bin), operations::get_bin("bin2")],
        )
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin2"), Some(&Value::from("bal1")));

    let status = client.abort(&txn).await.unwrap();
    assert_eq!(status, AbortStatus::Ok);

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(record.bins.get("bin"), Some(&Value::from("val1")));
}

// =============================================================================
// Version mismatch on commit (read then external write then commit)
// =============================================================================

#[aerospike_macro::test]
async fn txn_version_mismatch_on_commit() {
    let client = common::client().await;
    skip_if_no_mrt!(&client);

    let ns = common::namespace();
    let set = &common::rand_str(10);

    for count in [1, 10] {
        let mut keys = Vec::with_capacity(count);
        for i in 0..count {
            let key = as_key!(ns, set, i as i64);
            client
                .put(&WritePolicy::default(), &key, &[as_bin!("bin", 1000)])
                .await
                .unwrap();
            keys.push(key);
        }

        let txn = Arc::new(Txn::new());
        let mut rp = ReadPolicy::default();
        rp.base_policy.txn = Some(txn.clone());

        // Read all keys in txn (records versions)
        for key in &keys {
            let rec = client.get(&rp, key, Bins::All).await.unwrap();
            assert!(rec.bins.contains_key("bin"));
        }

        // Modify key[0] outside txn to cause version mismatch
        let key0 = as_key!(ns, set, 0i64);
        client
            .put(&WritePolicy::default(), &key0, &[as_bin!("bin", 999)])
            .await
            .unwrap();

        // Commit should fail with version mismatch
        let err = client.commit(&txn).await.unwrap_err();
        let err_str = format!("{err}");
        assert!(
            err_str.contains("VersionMismatch") || err_str.contains("verify"),
            "Expected version mismatch error, got: {err_str}"
        );
    }
}
