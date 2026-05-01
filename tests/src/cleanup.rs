// Copyright 2015-2026 Aerospike, Inc.
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

//! Post-test cleanup: drops every secondary index in the test namespace and
//! truncates the namespace.

use std::collections::HashMap;

use aerospike::{AdminPolicy, Client};

use crate::common;

async fn list_indexes(client: &Client, namespace: &str) -> Vec<HashMap<String, String>> {
    let node = client
        .cluster
        .get_random_node()
        .expect("no nodes available");

    let cmd = format!("sindex-list:namespace={}", namespace);
    let res = node
        .info(&AdminPolicy::default(), &[&cmd])
        .await
        .expect("sindex info request failed");

    let sindex_str = res.get(&cmd).map(String::as_str).unwrap_or("");

    sindex_str
        .split(';')
        .map(|entry| {
            entry
                .split(':')
                .filter(|kv| !kv.trim().is_empty())
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        })
        .collect()
}

async fn drop_indexes(client: &Client, namespace: &str) {
    let policy = AdminPolicy::default();
    for index in list_indexes(client, namespace).await {
        if index.get("ns").map(String::as_str) != Some(namespace) {
            continue;
        }
        let set = index.get("set").map(String::as_str).unwrap_or("");
        let Some(name) = index.get("indexname") else {
            continue;
        };

        match client.drop_index(&policy, namespace, set, name).await {
            Ok(_) => println!("Success: Removing Namespace/Set/Index: {namespace}/{set}/{name}"),
            Err(e) => {
                println!("Failed: Removing Namespace/Set/Index: {namespace}/{set}/{name} ({e})")
            }
        }
    }
}

async fn truncate_namespace(client: &Client, namespace: &str) {
    match client
        .truncate(&AdminPolicy::default(), namespace, "", 0)
        .await
    {
        Ok(_) => println!("Success: Removing Namespace: {namespace}"),
        Err(e) => println!("Failed: Removing Namespace: {namespace} ({e})"),
    }
}

#[ignore]
#[aerospike_macro::test]
async fn cleanup_after_tests() {
    let client = common::client().await;
    let namespace = common::namespace();
    drop_indexes(&client, namespace).await;
    truncate_namespace(&client, namespace).await;
    client.close().await.unwrap();
}
