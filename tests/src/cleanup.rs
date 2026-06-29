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

use aerospike::{AdminPolicy, Client};

use crate::common;

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
    common::drop_all_indexes(&client, namespace).await;
    truncate_namespace(&client, namespace).await;
    client.close().await.unwrap();
}
