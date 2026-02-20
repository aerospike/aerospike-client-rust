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

use std::sync::Arc;

use aerospike::*;
use aerospike_rt::time::Duration;

use crate::common;

#[aerospike_macro::test]
async fn mntn_pool_case_simple_get() {
    let client = Arc::new(common::client().await);
    let namespace: &str = common::namespace();
    let set_name = common::rand_str(10);

    // If this is not failing, try increasing the flood count
    const FLOOD_COUNT: usize = 40;

    let rp = ReadPolicy {
        base_policy: BasePolicy {
            timeout_delay: 10,
            ..BasePolicy::default()
        },
        ..ReadPolicy::default()
    };

    const KEYS: &str = "abcdefghij";

    // Put values into `test.test` set in Aerospike
    // Values are mapped as follows:
    // "a" -> 0
    // "b" -> 1
    // "c" -> 2
    // ... and so on
    for i in 0..10 {
        let set_name = set_name.clone();
        let key = &KEYS[i..i + 1];
        let key = Key::new(namespace, &set_name, Value::String(key.to_string()))
            .expect("Failed to create key");
        client
            .put(
                &WritePolicy::default(),
                &key,
                &[Bin::new("value".to_string(), Value::Int(i as i64))],
            )
            .await
            .expect("Failed to put value into Aerospike");
    }

    // Flood the client with requests that we abort almost immediately afterwards
    for _ in 0..FLOOD_COUNT {
        let set_name = set_name.clone();
        let rp = rp.clone();
        let flood_task_client = client.clone();
        let flood_task = tokio::spawn(async move {
            let key = &KEYS[0..1]; // "a"
            let key = Key::new(namespace, &set_name, Value::String(key.to_string()))
                .expect("Failed to create key");

            flood_task_client
                .get(&rp, &key, ["value"])
                .await
                .expect("Failed to get value from Aerospike");
        });

        // Gives enough time for `task_client.get` to be reached
        tokio::time::sleep(Duration::from_micros(5)).await;

        flood_task.abort();
    }

    let set_name = set_name.clone();
    let key = &KEYS[1..2]; // "b"
    let key = Key::new(namespace, &set_name, Value::String(key.to_string()))
        .expect("Failed to create key");
    let different_request = client
        .get(&ReadPolicy::default(), &key, ["value"])
        .await
        .expect("Failed to get value from Aerospike");

    // It's expected that the flood requests would be dropped when the task got aborted, so the latest request
    // should get the appropriate response, not the response from one of the flood requests.
    match different_request.bins.get("value").unwrap() {
        Value::Int(0) => {
            panic!("We got the wrong response from the latest request. The expected response is 1, but we got 0, which was the intended response to a previous request we sent.");
        }
        Value::Int(1) => {
            // We got the expected response from the latest request (1)
        }
        value => {
            panic!("We got an unexpected response from the latest request: {:?}. Expected either 0 or 1.", value);
        }
    }
}

#[aerospike_macro::test]
async fn mntn_pool_case_simple_batch_get() {
    let client = Arc::new(common::client().await);
    let namespace: &str = common::namespace();
    let set_name = common::rand_str(10);

    // If this is not failing, try increasing the flood count
    const FLOOD_COUNT: usize = 40;

    let bp = BatchPolicy {
        base_policy: BasePolicy {
            // total_timeout: 100,
            socket_timeout: 1,
            timeout_delay: 10,
            ..BasePolicy::default()
        },
        ..BatchPolicy::default()
    };

    let bpr = BatchReadPolicy::default();

    const KEYS: &str = "abcdefghij";

    // Put values into `test.test` set in Aerospike
    // Values are mapped as follows:
    // "a" -> 0
    // "b" -> 1
    // "c" -> 2
    // ... and so on
    for i in 0..10 {
        let set_name = set_name.clone();
        let key = &KEYS[i..i + 1];
        let key = Key::new(namespace, &set_name, Value::String(key.to_string()))
            .expect("Failed to create key");
        client
            .put(
                &WritePolicy::default(),
                &key,
                &[Bin::new("value".to_string(), Value::Int(i as i64))],
            )
            .await
            .expect("Failed to put value into Aerospike");
    }

    // Flood the client with requests that we abort almost immediately afterwards
    for _ in 0..FLOOD_COUNT {
        let set_name = set_name.clone();
        let bp = bp.clone();
        let bpr = bpr.clone();
        let flood_task_client = client.clone();
        let flood_task = tokio::spawn(async move {
            let key = &KEYS[0..1]; // "a"
            let key = Key::new(namespace, &set_name, Value::String(key.to_string()))
                .expect("Failed to create key");

            let batch = vec![BatchOperation::read(&bpr, key, Bins::All)];
            flood_task_client
                .batch(&bp, &batch)
                .await
                .expect("Failed to get value from Aerospike");
        });

        // Gives enough time for `task_client.get` to be reached
        tokio::time::sleep(Duration::from_micros(5)).await;

        flood_task.abort();
    }

    let set_name = set_name.clone();
    let key = &KEYS[1..2]; // "b"
    let key = Key::new(namespace, &set_name, Value::String(key.to_string()))
        .expect("Failed to create key");
    let different_request = client
        .get(&ReadPolicy::default(), &key, ["value"])
        .await
        .expect("Failed to get value from Aerospike");

    // It's expected that the flood requests would be dropped when the task got aborted, so the latest request
    // should get the appropriate response, not the response from one of the flood requests.
    match different_request.bins.get("value").unwrap() {
        Value::Int(0) => {
            panic!("We got the wrong response from the latest request. The expected response is 1, but we got 0, which was the intended response to a previous request we sent.");
        }
        Value::Int(1) => {
            // We got the expected response from the latest request (1)
        }
        value => {
            panic!("We got an unexpected response from the latest request: {:?}. Expected either 0 or 1.", value);
        }
    }
}
