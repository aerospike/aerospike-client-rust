// Copyright 2015-2020 Aerospike, Inc.
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

use aerospike::exp::Expression;
use aerospike::Task;
use aerospike::*;

const EXPECTED: usize = 1000;

fn create_test_set(no_records: usize) -> String {
    let client = common::client();
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![&wbin];
        client.delete(&wpolicy, &key).unwrap();
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    let task = client
        .create_index(
            &wpolicy,
            namespace,
            &set_name,
            "bin",
            &format!("{}_{}_{}", namespace, set_name, "bin"),
            IndexType::Numeric,
        )
        .expect("Failed to create index");
    task.wait_till_complete(None).unwrap();

    set_name
}

#[test]
fn query_exp() {
    let _ = env_logger::try_init();

    let client = common::client();
    let namespace = common::namespace();
    let set_name = create_test_set(EXPECTED);
    let mut qpolicy = QueryPolicy::default();
    qpolicy.filter_expression = Some(Expression::eq(
        Expression::int_bin("bin".to_string()),
        Expression::int_val(1),
    ));
    // Filter Query
    let statement = Statement::new(namespace, &set_name, Bins::All);
    let rs = client.query(&qpolicy, statement).unwrap();
    let mut count = 0;

    for res in &*rs {
        match res {
            Ok(rec) => {
                assert_eq!(rec.bins["bin"], as_val!(1));
                count += 1;
            }
            Err(err) => panic!(format!("{:?}", err)),
        }
    }
    assert_eq!(count, 1);
}
