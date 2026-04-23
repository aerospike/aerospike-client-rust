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
use aerospike::{
    as_bin, as_blob, as_geo, as_key, as_list, as_map, as_val, Bins, ReadPolicy, Value, WritePolicy,
};
use aerospike::{
    operations, Error, Expiration, GenerationPolicy, ReadTouchTTL, RecordExistsAction, ResultCode,
};
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

use crate::common::{self};

#[aerospike_macro::test]
async fn read_touch_ttl() {
    let client = common::client().await;
    let caps = common::ServerCapabilities::detect(&client).await;
    if !caps.explicit_record_ttl_allowed {
        eprintln!(
            "read_touch_ttl: skipped (explicit_record_ttl_allowed=false; namespace_sc={})",
            caps.namespace_strong_consistency
        );
        return;
    }

    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, -1);
    let bin = as_bin!("expireBinName", "expirevalue");

    // Specify that record expires 2 seconds after it's written.
    let write_policy = WritePolicy::new(0, Expiration::Seconds(2));
    let bins = [bin.clone()];
    client.put(&write_policy, &key, &bins).await.unwrap();

    // Read the record before it expires and reset read ttl.
    sleep(Duration::from_secs(1)).await;
    let mut read_policy = ReadPolicy::default();
    read_policy.base_policy.read_touch_ttl = ReadTouchTTL::Percent(80);
    let record = client.get(&read_policy, &key, Bins::All).await.unwrap();
    assert!(record.bins.get(&bin.clone().name) == Some(&bin.clone().value.into()));

    // Read the record again, but don't reset read ttl.
    sleep(Duration::from_secs(1)).await;
    read_policy.base_policy.read_touch_ttl = ReadTouchTTL::DontReset;
    let record = client.get(&read_policy, &key, Bins::All).await.unwrap();
    assert!(record.bins.get(&bin.clone().name) == Some(&bin.clone().value.into()));

    // Read the record after it expires, showing it's gone.
    sleep(Duration::from_secs(2)).await;
    let rp = ReadPolicy::default();
    let record = client.get(&rp, &key, Bins::All).await;
    match record {
        Err(_) => (),
        _ => panic!("expected key not found error"),
    }
}

#[aerospike_macro::test]
async fn invalid_delete() {
    let client = common::client().await;
    let wpolicy = WritePolicy::default();

    // the namespace will be invalid
    let invalid_ns_key = as_key!(common::rand_str(14), common::rand_str(10), -1);
    client
        .delete(&wpolicy, &invalid_ns_key)
        .await
        .expect_err("Should have errored out");
}

#[aerospike_macro::test]
async fn connect() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let policy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);

    client.delete(&wpolicy, &key).await.unwrap();

    let bins = [
        as_bin!("bin999", "test string"),
        as_bin!("bin bool", true),
        as_bin!("bin vec![int]", as_list![1u32, 2u32, 3u32]),
        as_bin!("bin vec![u8]", as_blob!(vec![1u8, 2u8, 3u8])),
        as_bin!("bin map", as_map!(1 => 1, 2 => 2, 3 => "hi!", 4 => false)),
        as_bin!("bin f64", 1.64f64),
        as_bin!("bin Nil", None), // Writing None erases the bin!
        as_bin!(
            "bin Geo",
            as_geo!(format!(
                r#"{{ "type": "Point", "coordinates": [{}, {}] }}"#,
                17.119_381, 19.45612
            ))
        ),
        as_bin!("bin-name-len-15", "max. bin name length is 15 chars"),
    ];
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let record = client.get(&policy, &key, Bins::All).await.unwrap();
    let bins = record.bins;
    assert_eq!(bins.len(), 8);
    assert_eq!(bins.get("bin bool"), Some(&Value::Bool(true)));
    assert_eq!(bins.get("bin999"), Some(&Value::from("test string")));
    assert_eq!(bins.get("bin vec![int]"), Some(&as_list![1u32, 2u32, 3u32]));
    assert_eq!(
        bins.get("bin vec![u8]"),
        Some(&as_blob!(vec![1u8, 2u8, 3u8]))
    );
    assert_eq!(
        bins.get("bin map"),
        Some(&as_map!(1 => 1, 2 => 2, 3 => "hi!", 4 => false))
    );
    assert_eq!(bins.get("bin f64"), Some(&Value::from(1.64f64)));
    assert_eq!(
        bins.get("bin Geo"),
        Some(&as_geo!(
            r#"{ "type": "Point", "coordinates": [17.119381, 19.45612] }"#
        ))
    );
    assert_eq!(
        bins.get("bin-name-len-15"),
        Some(&Value::from("max. bin name length is 15 chars"))
    );

    client.touch(&wpolicy, &key).await.unwrap();

    let bins = Bins::from(["bin999", "bin f64"]);
    let record = client.get(&policy, &key, bins).await.unwrap();
    assert_eq!(record.bins.len(), 2);

    let record = client.get(&policy, &key, Bins::None).await.unwrap();
    assert_eq!(record.bins.len(), 0);

    let exists = client.exists(&policy, &key).await.unwrap();
    assert!(exists);

    let bin = as_bin!("bin999", "test string");
    let ops = &vec![operations::put(&bin), operations::get()];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    assert!(existed);

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    assert!(!existed);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn operate_empty_ops_returns_parameter_error() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, 1);

    let wpolicy = WritePolicy::default();

    // Calling operate with an empty operations slice must be rejected
    // client-side with a ParameterError, instead of being forwarded to the
    // server (which would either reject it with an opaque error or perform
    // a meaningless round-trip). The server happens to also reject empty
    // ops with ParameterError, so we additionally verify that the error
    // message identifies the client-side guard — a server response would
    // carry the node address in that field instead.
    let result = client.operate(&wpolicy, &key, &[]).await;

    match result {
        Err(Error::ServerError(ResultCode::ParameterError, _, ref msg))
            if msg.contains("no operations") => {}
        Err(other) => panic!(
            "expected client-side ParameterError ('operate called with no \
             operations'); got {:?}",
            other
        ),
        Ok(_) => panic!("expected ParameterError, got Ok"),
    }

    client.close().await.unwrap();
}

// ===== Java-parity write-policy tests =====
//
// Mirrors `TestReplace`, `TestGeneration`, and the basic `TestExpire`
// behaviors from the Java client's `sync/basic` suite. These exercise
// `RecordExistsAction` and `GenerationPolicy` shapes that weren't
// covered by the existing kv tests.

#[aerospike_macro::test]
async fn replace_drops_unreferenced_bins() {
    // RecordExistsAction::Replace deletes bins not referenced by the
    // new write — matching Java's TestReplace.replace().
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, "replacekey");

    let wpolicy = WritePolicy::default();
    let bin1 = as_bin!("bin1", "value1");
    let bin2 = as_bin!("bin2", "value2");
    let bin3 = as_bin!("bin3", "value3");

    client.delete(&wpolicy, &key).await.unwrap();
    client.put(&wpolicy, &key, &[bin1, bin2]).await.unwrap();

    let mut replace_policy = WritePolicy::default();
    replace_policy.record_exists_action = RecordExistsAction::Replace;
    client.put(&replace_policy, &key, &[bin3]).await.unwrap();

    let policy = ReadPolicy::default();
    let record = client.get(&policy, &key, Bins::All).await.unwrap();
    assert!(
        !record.bins.contains_key("bin1"),
        "bin1 should have been dropped, found: {:?}",
        record.bins
    );
    assert!(
        !record.bins.contains_key("bin2"),
        "bin2 should have been dropped, found: {:?}",
        record.bins
    );
    assert_eq!(record.bins.get("bin3"), Some(&Value::from("value3")));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn replace_only_fails_when_record_missing() {
    // RecordExistsAction::ReplaceOnly must fail with KeyNotFound on a
    // non-existent record — matching Java's TestReplace.replaceOnly().
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, "replaceonlykey");

    let wpolicy = WritePolicy::default();
    client.delete(&wpolicy, &key).await.unwrap();

    let mut replace_only = WritePolicy::default();
    replace_only.record_exists_action = RecordExistsAction::ReplaceOnly;
    let bin = as_bin!("bin", "value");

    match client.put(&replace_only, &key, &[bin]).await {
        Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {}
        Err(other) => panic!("expected KeyNotFoundError, got: {:?}", other),
        Ok(_) => panic!("expected error, got Ok"),
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn generation_policy_expect_gen_equal() {
    // After two puts the record's generation is 2; a third put with
    // EXPECT_GEN_EQUAL=2 must succeed and a follow-up with the wrong
    // generation must fail with GenerationError. Matches Java
    // TestGeneration.generation().
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, "genkey");

    let wpolicy = WritePolicy::default();
    let policy = ReadPolicy::default();
    client.delete(&wpolicy, &key).await.unwrap();

    client
        .put(&wpolicy, &key, &[as_bin!("genbin", "genvalue1")])
        .await
        .unwrap();
    client
        .put(&wpolicy, &key, &[as_bin!("genbin", "genvalue2")])
        .await
        .unwrap();

    let record = client.get(&policy, &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("genbin"), Some(&Value::from("genvalue2")));
    let actual_gen = record.generation;

    // Same-generation write should succeed.
    let mut gen_match = WritePolicy::default();
    gen_match.generation_policy = GenerationPolicy::ExpectGenEqual;
    gen_match.generation = actual_gen;
    client
        .put(&gen_match, &key, &[as_bin!("genbin", "genvalue3")])
        .await
        .unwrap();

    // Mismatched-generation write must error.
    let mut gen_wrong = WritePolicy::default();
    gen_wrong.generation_policy = GenerationPolicy::ExpectGenEqual;
    gen_wrong.generation = 9999;
    match client
        .put(&gen_wrong, &key, &[as_bin!("genbin", "genvalue4")])
        .await
    {
        Err(Error::ServerError(ResultCode::GenerationError, _, _)) => {}
        Err(other) => panic!("expected GenerationError, got: {:?}", other),
        Ok(_) => panic!("expected GenerationError, got Ok"),
    }

    // The successful put left genvalue3; the failed one didn't apply.
    let record = client.get(&policy, &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.get("genbin"), Some(&Value::from("genvalue3")));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn create_only_fails_when_record_exists() {
    // RecordExistsAction::CreateOnly must fail with KeyExistsError when
    // the record already exists — completes the Java-parity matrix
    // (Replace / ReplaceOnly / Generation already covered above).
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, "createonlykey");

    let wpolicy = WritePolicy::default();
    client.delete(&wpolicy, &key).await.unwrap();
    client
        .put(&wpolicy, &key, &[as_bin!("bin", "first")])
        .await
        .unwrap();

    let mut create_only = WritePolicy::default();
    create_only.record_exists_action = RecordExistsAction::CreateOnly;
    match client
        .put(&create_only, &key, &[as_bin!("bin", "second")])
        .await
    {
        Err(Error::ServerError(ResultCode::KeyExistsError, _, _)) => {}
        Err(other) => panic!("expected KeyExistsError, got: {:?}", other),
        Ok(_) => panic!("expected KeyExistsError, got Ok"),
    }

    client.close().await.unwrap();
}
