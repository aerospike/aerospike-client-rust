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

#[macro_use]
extern crate bencher;
#[macro_use]
extern crate lazy_static;
extern crate rand;

use aerospike::{Bins, ReadPolicy, WritePolicy};

use aerospike::{as_bin, as_key};
use bencher::Bencher;

#[path = "../tests/common/mod.rs"]
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

fn single_key_write(bench: &mut Bencher) {
    let client = common::client();
    let namespace = common::namespace();
    let key = as_key!(namespace, &TEST_SET, common::rand_str(10));
    let wpolicy = WritePolicy::default();

    let bin1 = as_bin!("str1", common::rand_str(256));
    let bin2 = as_bin!("str1", common::rand_str(256));
    let bin3 = as_bin!("str1", common::rand_str(256));
    let bin4 = as_bin!("str1", common::rand_str(256));
    let bins = [bin1, bin2, bin3, bin4];

    bench.iter(|| {
        client.put(&wpolicy, &key, &bins).unwrap();
    });
}

benchmark_group!(
    benches,
    single_key_read,
    single_key_read_header,
    single_key_write,
);
benchmark_main!(benches);
