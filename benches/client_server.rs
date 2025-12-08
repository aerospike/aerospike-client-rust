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

use rand::Rng;

use aerospike::operations;
use aerospike::{as_bin, as_key, as_val};
use aerospike::{
    AdminPolicy, BatchDeletePolicy, BatchOperation, BatchPolicy, BatchReadPolicy, BatchUDFPolicy,
    BatchWritePolicy, Bins, ReadPolicy, Task, UDFLang, WritePolicy,
};

use bencher::Bencher;

#[path = "../tests/common/mod.rs"]
mod common;

lazy_static! {
    static ref TEST_SET: String = common::rand_str(10);
}

fn rand_key_from_range(low: i64, high: i64) -> i64 {
    rand::thread_rng().gen_range(low, high)
}

fn single_key_read(bench: &mut Bencher) {
    let client = common::RUNTIME.block_on(common::singleton_client());
    let namespace = common::namespace();
    let key = as_key!(namespace, &TEST_SET, rand_key_from_range(1, 1000));
    let wbin = as_bin!("i", 1);
    let bins = vec![wbin];
    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    common::RUNTIME
        .block_on(client.put(&wpolicy, &key, &bins))
        .unwrap();

    bench.iter(|| {
        common::RUNTIME
            .block_on(client.get(&rpolicy, &key, Bins::All))
            .unwrap();
    });
}

fn single_key_read_header(bench: &mut Bencher) {
    let client = common::RUNTIME.block_on(common::singleton_client());
    let namespace = common::namespace();
    let key = as_key!(namespace, &TEST_SET, rand_key_from_range(1, 1000));
    let wbin = as_bin!("i", 1);
    let bins = vec![wbin];
    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    common::RUNTIME
        .block_on(client.put(&wpolicy, &key, &bins))
        .unwrap();

    bench.iter(|| {
        common::RUNTIME
            .block_on(client.get(&rpolicy, &key, Bins::None))
            .unwrap()
    });
}

fn single_key_write(bench: &mut Bencher) {
    let client = common::RUNTIME.block_on(common::singleton_client());
    let namespace = common::namespace();
    let key = as_key!(namespace, &TEST_SET, rand_key_from_range(1, 1000));
    let wpolicy = WritePolicy::default();

    let bin1 = as_bin!("str1", common::rand_str(256));
    let bin2 = as_bin!("str1", common::rand_str(256));
    let bin3 = as_bin!("str1", common::rand_str(256));
    let bin4 = as_bin!("str1", common::rand_str(256));
    let bins = [bin1, bin2, bin3, bin4];

    bench.iter(|| {
        common::RUNTIME
            .block_on(client.put(&wpolicy, &key, &bins))
            .unwrap();
    });
}

fn single_key_operate_write_get_delete(bench: &mut Bencher) {
    let client = common::RUNTIME.block_on(common::singleton_client());
    let namespace = common::namespace();
    let key = as_key!(namespace, &TEST_SET, rand_key_from_range(1, 1000));
    let wpolicy = WritePolicy::default();

    let bin1 = as_bin!("bin1", common::rand_str(256));
    let bin2 = as_bin!("bin2", common::rand_str(256));
    let bin3 = as_bin!("bin3", common::rand_str(256));
    let bin4 = as_bin!("bin4", 1);
    let bins = [bin1.clone(), bin2.clone(), bin3.clone(), bin4.clone()];

    common::RUNTIME
        .block_on(client.delete(&wpolicy, &key))
        .unwrap();

    common::RUNTIME
        .block_on(client.put(&wpolicy, &key, &bins))
        .unwrap();

    let ops = vec![
        operations::put(&bin1),
        operations::put(&bin2),
        operations::put(&bin3),
        operations::put(&bin4),
        operations::get_bin("bin3"),
        operations::append(&bin1),
        operations::prepend(&bin1),
        operations::add(&bin4),
    ];

    bench.iter(|| {
        common::RUNTIME
            .block_on(client.operate(&wpolicy, &key, &ops))
            .unwrap();
    });
}

fn multi_key_batch(bench: &mut Bencher) {
    let client = common::RUNTIME.block_on(common::singleton_client());
    let namespace = common::namespace();
    let bpolicy = BatchPolicy::default();
    let apolicy = AdminPolicy::default();

    let udf_body = r#"
function echo(rec, val)
  return val
end
"#;
    let task = common::RUNTIME
        .block_on(client.register_udf(&apolicy, udf_body.as_bytes(), "test_udf.lua", UDFLang::Lua))
        .unwrap();

    common::RUNTIME
        .block_on(task.wait_till_complete(None))
        .unwrap();

    let bin1 = as_bin!("a", "a value");
    let bin2 = as_bin!("b", "another value");
    let bin3 = as_bin!("c", 42);
    let key1 = as_key!(namespace, &TEST_SET, rand_key_from_range(1, 1000));
    let key2 = as_key!(namespace, &TEST_SET, rand_key_from_range(1, 1000));
    let key3 = as_key!(namespace, &TEST_SET, rand_key_from_range(1, 1000));

    let key4 = as_key!(namespace, &TEST_SET, rand_key_from_range(1, 1000));
    // key does not exist

    let selected = Bins::from(["a"]);
    let all = Bins::All;
    let none = Bins::None;

    let bpr = BatchReadPolicy::default();
    let bpw = BatchWritePolicy::default();
    let bpd = BatchDeletePolicy::default();
    let bpu = BatchUDFPolicy::default();

    let wops = vec![
        operations::put(&bin1),
        operations::put(&bin2),
        operations::put(&bin3),
    ];

    let rops = vec![
        operations::get_bin(&bin1.name),
        operations::get_bin(&bin2.name),
        operations::get_header(),
    ];

    let args1 = &[as_val!(1)];
    let batch = vec![
        BatchOperation::write(&bpw, key1.clone(), wops.clone()),
        BatchOperation::write(&bpw, key2.clone(), wops.clone()),
        BatchOperation::write(&bpw, key3.clone(), wops.clone()),
        BatchOperation::read(&bpr, key1.clone(), selected),
        BatchOperation::read(&bpr, key2.clone(), all),
        BatchOperation::read(&bpr, key3.clone(), none.clone()),
        BatchOperation::read_ops(&bpr, key3.clone(), rops),
        BatchOperation::read(&bpr, key4.clone(), none),
        BatchOperation::delete(&bpd, key4.clone()),
        BatchOperation::udf(&bpu, key1.clone(), "test_udf", "echo", Some(args1)),
    ];

    bench.iter(|| {
        common::RUNTIME
            .block_on(client.batch(&bpolicy, &batch))
            .unwrap();
    });
}

benchmark_group!(
    benches,
    single_key_read,
    single_key_read_header,
    single_key_write,
    single_key_operate_write_get_delete,
    multi_key_batch,
);
benchmark_main!(benches);
