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

#![allow(dead_code)]

use std::env;

use rand;
use rand::distributions::Alphanumeric;
use rand::Rng;

use aerospike::{Client, ClientPolicy};

lazy_static! {
    static ref AEROSPIKE_HOSTS: String =
        env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3102"));
    static ref AEROSPIKE_NAMESPACE: String =
        env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| String::from("test"));
    static ref AEROSPIKE_CLUSTER: Option<String> = env::var("AEROSPIKE_CLUSTER").ok();
    static ref AEROSPIKE_USE_SERVICES_ALTERNATE: bool =
        env::var("AEROSPIKE_USE_SERVICES_ALTERNATE").is_ok();
    static ref GLOBAL_CLIENT_POLICY: ClientPolicy = {
        let mut policy = ClientPolicy::default();
        if let Ok(user) = env::var("AEROSPIKE_USER") {
            let password = env::var("AEROSPIKE_PASSWORD").unwrap_or_default();
            policy.set_user_password(user, password).unwrap();
        }
        policy.cluster_name = AEROSPIKE_CLUSTER.clone();
        policy.use_services_alternate = AEROSPIKE_USE_SERVICES_ALTERNATE.clone();
        policy
    };
}

pub fn hosts() -> &'static str {
    &*AEROSPIKE_HOSTS
}

pub fn namespace() -> &'static str {
    &*AEROSPIKE_NAMESPACE
}

pub fn client_policy() -> &'static ClientPolicy {
    &*GLOBAL_CLIENT_POLICY
}

pub async fn client() -> Client {
    Client::new(&GLOBAL_CLIENT_POLICY, &*AEROSPIKE_HOSTS)
        .await
        .unwrap()
}

pub fn rand_str(sz: usize) -> String {
    let rng = rand::thread_rng();
    rng.sample_iter(&Alphanumeric).take(sz).collect()
}

pub async fn enterprise_edition() -> bool {
    let client = client().await;
    let node = client.cluster.get_random_node().await;
    if let Err(_) = node {
        return false;
    }
    let node = node.unwrap();
    let edition = node.info(&vec!["edition"]).await;
    if let Err(_) = edition {
        return false;
    }

    if let Some(edition) = edition.unwrap().get("edition") {
        return edition.to_lowercase().contains("enterprise");
    }

    false
}

pub async fn security_enabled() -> bool {
    if !enterprise_edition().await {
        return false;
    }

    let client = client().await;
    let roles = client.query_users(None).await;
    if let Err(_) = roles {
        return false;
    }

    true
}
