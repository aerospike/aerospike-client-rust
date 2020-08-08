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

use crate::common;
use env_logger;

use aerospike::operations::bitwise;
use aerospike::operations::bitwise::BitPolicy;
use aerospike::{as_bin, as_key, Value, WritePolicy};

#[test]
fn cdt_bitwise() {
    let _ = env_logger::try_init();

    let client = common::client();
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let val = Value::Blob(vec![0b00000101]);
    let wbin = as_bin!("bin", val.clone());
    let bins = vec![&wbin];
    let bpolicy = BitPolicy::default();

    client.delete(&wpolicy, &key).unwrap();

    client.put(&wpolicy, &key, &bins).unwrap();
    let ops = &vec![bitwise::insert("bin2", 0, &val ,&bpolicy), bitwise::get("bin2", 0, 8)];
    let rec = client.operate(&wpolicy, &key, ops).unwrap();
    assert_eq!(*rec.bins.get("bin2").unwrap(), Value::Blob(vec![0b00000101]));
}
