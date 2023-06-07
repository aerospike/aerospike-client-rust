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
extern crate lazy_static;
extern crate rand;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use aerospike::{Bins, ReadPolicy, WritePolicy, Value, Client, Key};
use aerospike::{as_bin, as_key};
use criterion::{criterion_group, criterion_main, Criterion};

#[path = "../tests/common/mod.rs"]
mod common;

lazy_static! {
    static ref TEST_SET: String = common::rand_str(10);
}

async fn single_key_read(client: Arc<Client>, key: &Key) {
    let rpolicy = ReadPolicy::default();
    client.get::<HashMap<String, Value>, Bins>(&rpolicy, &key, Bins::All).await.unwrap();
}

fn run_single_key_read(bench: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = Arc::new(rt.block_on(common::client()));
    let namespace = common::namespace();
    let key = as_key!(namespace, &TEST_SET, common::rand_str(10));
    let wbin = as_bin!("i", 1);
    let bins = vec![wbin];

    let wpolicy = WritePolicy::default();
    rt.block_on(client.put(&wpolicy, &key, &bins)).unwrap();

    let key2 = as_key!(namespace, &TEST_SET, common::rand_str(10));
    rt.block_on(client.put(&wpolicy, &key2, &bins)).unwrap();

    let mut group = bench.benchmark_group("single operations");
    group.sample_size(1000).measurement_time(Duration::from_secs(40));

    group.bench_function("single_key_read", |b| {
        b.to_async(&rt).iter(|| single_key_read(client.clone(), &key))
    });

    group.bench_function("single_key_read_header", |b| {
        b.to_async(&rt).iter(|| single_key_read_header(client.clone(), &key2))
    });

    group.bench_function("single_key_write", |b| {
        b.to_async(&rt).iter(|| single_key_write(client.clone()))
    });

    group.finish()
}

async fn single_key_read_header(client: Arc<Client>, key: &Key) {
    let rpolicy = ReadPolicy::default();
    client.get::<HashMap<String, Value>, Bins>(&rpolicy, &key, Bins::None).await.unwrap();
}

async fn single_key_write(client: Arc<Client>) {
    let namespace = common::namespace();
    let key = as_key!(namespace, &TEST_SET, common::rand_str(10));
    let wpolicy = WritePolicy::default();

    let bin1 = as_bin!("str1", common::rand_str(256));
    let bin2 = as_bin!("str1", common::rand_str(256));
    let bin3 = as_bin!("str1", common::rand_str(256));
    let bin4 = as_bin!("str1", common::rand_str(256));
    let bins = [bin1, bin2, bin3, bin4];

    client.put(&wpolicy, &key, &bins).await.unwrap();
}



criterion_group!(
    benches,
    run_single_key_read,
    //single_key_read_header,
    //single_key_write,
);
criterion_main!(benches);
