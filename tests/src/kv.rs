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
use aerospike::{operations, Error, Expiration, ReadTouchTTL, ResultCode};
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

use crate::common::{self};

#[aerospike_macro::test]
async fn read_touch_ttl() {
    let client = common::client().await;
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

/// Multiple operate results for the same scalar bin merge into MultiResult (read_command path).
#[aerospike_macro::test]
async fn operate_multi_op_same_bin_returns_multi_result() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, 1);
    let wpolicy = WritePolicy::default();

    client
        .put(&wpolicy, &key, &[as_bin!("count", 10i64)])
        .await
        .unwrap();

    let ops = &[operations::get_bin("count"), operations::get_bin("count")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        rec.bins.get("count"),
        Some(&Value::MultiResult(vec![
            Value::from(10i64),
            Value::from(10i64)
        ]))
    );

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

#[aerospike_macro::test]
async fn infinity_and_wildcard_are_rejected_not_fatal() {
    // INF and wildcard exist only as bounds inside msgpack payloads (CDT
    // arguments, expressions). Handing one to the client as an ordinary bin
    // value used to abort the whole process from `Value::particle_type`'s
    // `unreachable!()`; Java answers PARAMETER_ERROR. A client-side mistake in
    // one command must not take down the caller.
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, "inf_wildcard");

    for value in [Value::Infinity, Value::Wildcard] {
        let bin = as_bin!("b", value.clone());
        let err = client
            .put(&wpolicy, &key, &[bin])
            .await
            .expect_err("storing INF/wildcard as a bin value must be an error");
        assert!(
            matches!(err, Error::InvalidArgument(_)),
            "expected InvalidArgument for {:?}, got {:?}",
            value,
            err
        );
    }

    // Same for a record key built from one: the digest needs a particle type.
    for value in [Value::Infinity, Value::Wildcard] {
        let bad_key = aerospike::Key::new(namespace, set_name.as_str(), value.clone());
        assert!(
            bad_key.is_err(),
            "a key made from {:?} must be rejected",
            value
        );
    }

    // The client is still usable afterwards.
    client
        .put(&wpolicy, &key, &[as_bin!("b", 1)])
        .await
        .expect("an ordinary write still works after the rejection");

    client.close().await.unwrap();
}
