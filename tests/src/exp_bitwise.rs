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

use aerospike::exp::{ExpType, Expression, FilterExpression};
use aerospike::Task;
use aerospike::*;
use std::sync::Arc;
use aerospike::exp::bit_exp::BitExpression;
use aerospike::operations::bitwise::{BitwiseResizeFlags, BitPolicy};

const EXPECTED: usize = 100;

fn create_test_set(no_records: usize) -> String {
    let client = common::client();
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let ibin = as_bin!("bin", as_blob!(vec![0b00000001, 0b01000010]));
        let bins = vec![&ibin];
        client.delete(&wpolicy, &key).unwrap();
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    set_name
}

#[test]
fn expression_bitwise() {
    let _ = env_logger::try_init();

    let set_name = create_test_set(EXPECTED);

    // EQ
    let rs = test_filter(
        Expression::eq(BitExpression::count(Expression::int_val(0), Expression::int_val(16), Expression::blob_bin("bin".to_string())), Expression::int_val(3)),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "EQ Test Failed");
}

fn test_filter(filter: FilterExpression, set_name: &str) -> Arc<Recordset> {
    let client = common::client();
    let namespace = common::namespace();

    let mut qpolicy = QueryPolicy::default();
    qpolicy.filter_expression = Some(filter);

    let statement = Statement::new(namespace, set_name, Bins::All);
    client.query(&qpolicy, statement).unwrap()
}

fn count_results(rs: Arc<Recordset>) -> usize {
    let mut count = 0;

    for res in &*rs {
        match res {
            Ok(_) => {
                count += 1;
            }
            Err(err) => panic!(format!("{:?}", err)),
        }
    }

    count
}
