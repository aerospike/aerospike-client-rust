// Copyright 2016 Aerospike, Inc.
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

use std::env;
use std::sync::Arc;

use rand;
use rand::Rng;

use aerospike::{ClientPolicy, Client};

pub fn rand_str(sz: usize) -> String {
    rand::thread_rng()
        .gen_ascii_chars()
        .take(sz)
        .collect()
}

lazy_static! {
    pub static ref AEROSPIKE_NAMESPACE: String = match env::var("AEROSPIKE_NAMESPACE") {
        Ok(s) => s,
        Err(_) => "test".to_string(),
    };
    pub static ref AEROSPIKE_SET: String = match env::var("AEROSPIKE_SET") {
        Ok(s) => s,
        Err(_) => "bench".to_string(),
    };
    pub static ref GLOBAL_CLIENT_POLICY: ClientPolicy = {
        let mut cp = ClientPolicy::default();
        match env::var("AEROSPIKE_USER") {
            Ok(user) => {
                let pass =  match env::var("AEROSPIKE_PASSWORD") {
                    Ok(pass) => pass,
                    Err(_) => "".to_string(),
                };

                cp.set_user_password(Some((user, pass))).unwrap();
            }
            Err(_) => (),
        }

        cp
    };
    pub static ref GLOBAL_CLIENT: Arc<Client> = {
        let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("127.0.0.1"));
        Arc::new(Client::new(&GLOBAL_CLIENT_POLICY, &hosts).unwrap())
    };
}
