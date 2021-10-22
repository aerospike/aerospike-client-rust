// Copyright 2015-2020 Aerospike, Inc.
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

use std::thread;
use std::time::Duration;

use crate::common;
use env_logger;

use aerospike::Task;
use aerospike::*;

const EXPECTED: usize = 100;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let wpolicy = WritePolicy::default();

    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![wbin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[aerospike_macro::test]
#[should_panic(expected = "IndexFound")]
async fn recreate_index() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let ns = common::namespace();
    let set = create_test_set(&client, EXPECTED).await;
    let bin = "bin";
    let index = format!("{}_{}_{}", ns, set, bin);

    let _ = client.drop_index(ns, &set, &index).await;
    thread::sleep(Duration::from_millis(1000));

    let task = client
        .create_index(ns, &set, bin, &index, IndexType::Numeric)
        .await
        .expect("Failed to create index");
    task.wait_till_complete(None).await.unwrap();

    let task = client
        .create_index(ns, &set, bin, &index, IndexType::Numeric)
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    client.close().await.unwrap();
}
