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

use aerospike::*;

use env_logger;

use crate::common;
use aerospike::{operations, Expiration, ReadTouchTTL};
use aerospike_rt::sleep;
use aerospike_rt::time::Duration;

#[aerospike_macro::test]
async fn batch_operate_read() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let mut bpolicy = BatchPolicy::default();
    bpolicy.concurrency = Concurrency::Parallel;

    let udf_body = r#"
function echo(rec, val)
  return val
end
"#;

    let task = client
        .register_udf(udf_body.as_bytes(), "test_udf.lua", UDFLang::Lua)
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
    let bpd = BatchDeletePolicy::default();
    let bpu = BatchUDFPolicy::default();

    let batch = vec![
        BatchOperation::write(&bpw, key1.clone(), wops.clone()),
        BatchOperation::write(&bpw, key2.clone(), wops.clone()),
        BatchOperation::write(&bpw, key3.clone(), wops.clone()),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    // dbg!(&results);

    // WRITE Operations
    // remove the first three write ops
    let result = results.remove(0);
    assert_eq!(result.key, key1);
    let result = results.remove(0);
    assert_eq!(result.key, key2);
    let result = results.remove(0);
    assert_eq!(result.key, key3);

    // READ Operations
    let batch = vec![
        BatchOperation::read(&bpr, key1.clone(), selected),
        BatchOperation::read(&bpr, key2.clone(), all),
        BatchOperation::read(&bpr, key3.clone(), none.clone()),
        BatchOperation::read_ops(&bpr, key3.clone(), rops),
        BatchOperation::read(&bpr, key4.clone(), none),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

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
    let batch = vec![
        BatchOperation::delete(&bpd, key1.clone()),
        BatchOperation::delete(&bpd, key2.clone()),
        BatchOperation::delete(&bpd, key3.clone()),
        BatchOperation::delete(&bpd, key4.clone()),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

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
    let batch = vec![
        BatchOperation::read(&bpr, key1.clone(), Bins::None),
        BatchOperation::read(&bpr, key2.clone(), Bins::None),
        BatchOperation::read(&bpr, key3.clone(), Bins::None),
        BatchOperation::read(&bpr, key4.clone(), Bins::None),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

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
    let args1 = &[as_val!(1)];
    let args2 = &[as_val!(2)];
    let args3 = &[as_val!(3)];
    let args4 = &[as_val!(4)];
    let batch = vec![
        BatchOperation::udf(&bpu, key1.clone(), "test_udf", "echo", Some(args1)),
        BatchOperation::udf(&bpu, key2.clone(), "test_udf", "echo", Some(args2)),
        BatchOperation::udf(&bpu, key3.clone(), "test_udf", "echo", Some(args3)),
        BatchOperation::udf(&bpu, key4.clone(), "test_udf", "echo", Some(args4)),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

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
    let record = result.record;
    assert_eq!(record.unwrap().bins.get("SUCCESS"), Some(&as_val!(4)));

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn batch_operate_read_touch_ttl() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace: &str = "test"; //common::namespace();
    let set_name = "test"; // &common::rand_str(10);
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
    let list = vec![bw1, bw2];
    client.batch(&bpolicy, &list).await.unwrap();

    // Read records before they expire and reset read ttl on one record.
    sleep(Duration::from_secs(8)).await;
    let mut brp1 = BatchReadPolicy::default();
    brp1.read_touch_ttl = ReadTouchTTL::Percent(80);

    let mut brp2 = BatchReadPolicy::default();
    brp2.read_touch_ttl = ReadTouchTTL::DontReset;

    let br1 = BatchOperation::read(&brp1, key1.clone(), Bins::Some(vec!["a".into()]));
    let br2 = BatchOperation::read(&brp2, key2.clone(), Bins::Some(vec!["a".into()]));
    let list = vec![br1, br2];
    let recs = client.batch(&bpolicy, &list).await.unwrap();

    assert!(Some(ResultCode::Ok) == recs[0].result_code);
    assert!(Some(ResultCode::Ok) == recs[1].result_code);

    // Read records again, but don't reset read ttl.
    sleep(Duration::from_secs(3)).await;
    brp1.read_touch_ttl = ReadTouchTTL::DontReset;
    brp2.read_touch_ttl = ReadTouchTTL::DontReset;

    let br1 = BatchOperation::read(&brp1, key1.clone(), Bins::Some(vec!["a".into()]));
    let br2 = BatchOperation::read(&brp2, key2.clone(), Bins::Some(vec!["a".into()]));
    let list = vec![br1, br2];
    let recs = client.batch(&bpolicy, &list).await.unwrap();

    // Key 2 should have expired.
    assert!(Some(ResultCode::Ok) == recs[0].result_code);
    assert!(Some(ResultCode::KeyNotFoundError) == recs[1].result_code);

    // Read  record after it expires, showing it's gone.
    sleep(Duration::from_secs(8)).await;
    let recs = client.batch(&bpolicy, &list).await.unwrap();
    assert!(Some(ResultCode::KeyNotFoundError) == recs[0].result_code);
    assert!(Some(ResultCode::KeyNotFoundError) == recs[1].result_code);
}
