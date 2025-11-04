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

use aerospike::expressions::*;
use aerospike::BatchRead;
use aerospike::Bins;
use aerospike::{as_bin, as_key, BatchPolicy, Concurrency, WritePolicy};

use env_logger;

use crate::common;
use crate::common::rand_str;

#[test]
fn batch_get() {
    let _ = env_logger::try_init();

    let client = common::client();
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let mut bpolicy = BatchPolicy::default();
    bpolicy.concurrency = Concurrency::Parallel;
    let mut wpolicy = WritePolicy::default();
    wpolicy.record_exists_action = aerospike::RecordExistsAction::Replace;

    let bin1 = as_bin!("a", "a value");
    let bin2 = as_bin!("b", "another value");
    let bin3 = as_bin!("c", 42);
    let bin4nf = as_bin!("d", 667);
    let bin4f = as_bin!("d", 666);

    let key1 = as_key!(namespace, set_name, 1);
    client
        .put(&wpolicy, &key1, &[&bin1, &bin2, &bin3, &bin4nf])
        .unwrap();

    let key2 = as_key!(namespace, set_name, 2);
    client
        .put(&wpolicy, &key2, &[&bin1, &bin2, &bin3, &bin4nf])
        .unwrap();

    let key3 = as_key!(namespace, set_name, 3);
    client
        .put(&wpolicy, &key3, &[&bin1, &bin2, &bin3, &bin4nf])
        .unwrap();

    // key does not exist
    let key4 = as_key!(namespace, set_name, -1);

    // should filter out
    let key5 = as_key!(namespace, set_name, 4);
    client
        .put(&wpolicy, &key5, &[&bin1, &bin2, &bin3, &bin4f])
        .unwrap();

    let selected = Bins::from(["a"]);
    let all = Bins::All;
    let none = Bins::None;

    let fe = Some(ne(int_bin("d".to_string()), int_val(666)));
    bpolicy.filter_expression = fe;

    let batch = vec![
        BatchRead::new(key1.clone(), &selected),
        BatchRead::new(key2.clone(), &all),
        BatchRead::new(key3.clone(), &none),
        BatchRead::new(key4.clone(), &none),
        BatchRead::new(key5.clone(), &all),
    ];
    let mut results = client.batch_get(&bpolicy, batch).unwrap();

    let result = results.remove(0);
    assert_eq!(result.key, key1);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 1);

    let result = results.remove(0);
    assert_eq!(result.key, key2);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 4);

    let result = results.remove(0);
    assert_eq!(result.key, key3);
    let record = result.record.unwrap();
    assert_eq!(record.bins.keys().count(), 0);

    let result = results.remove(0);
    assert_eq!(result.key, key4);
    let record = result.record;
    assert!(record.is_none());

    let result = results.remove(0);
    assert_eq!(result.key, key5);
    let record = result.record;
    assert!(record.is_none());
}
