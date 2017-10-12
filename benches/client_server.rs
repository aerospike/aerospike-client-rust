// Copyright 2015-2017 Aerospike, Inc.
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

#[macro_use]
extern crate aerospike;
#[macro_use]
extern crate lazy_static;
extern crate rand;
#[macro_use]
extern crate bencher;

use aerospike::{Bins, ReadPolicy, WritePolicy};

use bencher::Bencher;

#[path="../tests/common/mod.rs"]
mod common;

lazy_static! {
    static ref TEST_SET: String = common::rand_str(10);
}

fn single_key_read(bench: &mut Bencher) {
    let client = common::client();
    let namespace = common::namespace();
    let key = as_key!(namespace, &TEST_SET, common::rand_str(10));
    let wbin = as_bin!("i", 1);
    let bins = vec![&wbin];
    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    client.put(&wpolicy, &key, &bins).unwrap();

    bench.iter(|| client.get(&rpolicy, &key, Bins::All).unwrap());
}

fn single_key_read_header(bench: &mut Bencher) {
    let client = common::client();
    let namespace = common::namespace();
    let key = as_key!(namespace, &TEST_SET, common::rand_str(10));
    let wbin = as_bin!("i", 1);
    let bins = vec![&wbin];
    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    client.put(&wpolicy, &key, &bins).unwrap();

    bench.iter(|| client.get(&rpolicy, &key, Bins::None).unwrap());
}

benchmark_group!(benches, single_key_read, single_key_read_header);
benchmark_main!(benches);
