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

use aerospike::exp::bit_exp::BitExpression;
use aerospike::exp::{Expression, FilterExpression};
use aerospike::operations::bitwise::{BitPolicy, BitwiseOverflowActions, BitwiseResizeFlags};
use aerospike::*;
use std::sync::Arc;

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
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(16),
                Expression::blob_bin("bin".to_string()),
            ),
            Expression::int_val(3),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(16),
                BitExpression::resize(
                    &BitPolicy::default(),
                    Expression::int_val(4),
                    BitwiseResizeFlags::Default,
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(3),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "RESIZE Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(16),
                BitExpression::insert(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::blob_val(vec![0b11111111]),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(9),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "INSERT Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::remove(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(1),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::set(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(8),
                    Expression::blob_val(vec![0b10101010]),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(4),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "SET Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::or(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(8),
                    Expression::blob_val(vec![0b10101010]),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(5),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "OR Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::xor(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(8),
                    Expression::blob_val(vec![0b10101011]),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(4),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "XOR Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::and(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(8),
                    Expression::blob_val(vec![0b10101011]),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "AND Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::not(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(8),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(7),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "NOT Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::lshift(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(16),
                    Expression::int_val(9),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "LSHIFT Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::rshift(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(8),
                    Expression::int_val(3),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "RSHIFT Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::add(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(8),
                    Expression::int_val(128),
                    false,
                    BitwiseOverflowActions::Wrap,
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "ADD Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::subtract(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(8),
                    Expression::int_val(1),
                    false,
                    BitwiseOverflowActions::Wrap,
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "SUBTRACT Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::count(
                Expression::int_val(0),
                Expression::int_val(8),
                BitExpression::set_int(
                    &BitPolicy::default(),
                    Expression::int_val(0),
                    Expression::int_val(8),
                    Expression::int_val(255),
                    Expression::blob_bin("bin".to_string()),
                ),
            ),
            Expression::int_val(8),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "SET INT Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::get(
                Expression::int_val(0),
                Expression::int_val(8),
                Expression::blob_bin("bin".to_string()),
            ),
            Expression::blob_val(vec![0b00000001]),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::lscan(
                Expression::int_val(8),
                Expression::int_val(8),
                Expression::bool_val(true),
                Expression::blob_bin("bin".to_string()),
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "LSCAN Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::rscan(
                Expression::int_val(8),
                Expression::int_val(8),
                Expression::bool_val(true),
                Expression::blob_bin("bin".to_string()),
            ),
            Expression::int_val(6),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "RSCAN Test Failed");

    let rs = test_filter(
        Expression::eq(
            BitExpression::get_int(
                Expression::int_val(0),
                Expression::int_val(8),
                false,
                Expression::blob_bin("bin".to_string()),
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "RSCAN Test Failed");
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
