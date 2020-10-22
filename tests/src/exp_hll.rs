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

use aerospike::exp::hll_exp::HLLExpression;
use aerospike::exp::list_exp::ListExpression;
use aerospike::exp::{ExpType, Expression, FilterExpression};
use aerospike::operations::hll::HLLPolicy;
use aerospike::operations::lists::ListReturnType;
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
        let ibin = as_bin!("bin", i);
        let lbin = as_bin!("lbin", as_list!(i, "a"));
        let bins = vec![&ibin, &lbin];
        client.delete(&wpolicy, &key).unwrap();
        client.put(&wpolicy, &key, &bins).unwrap();
        let data = vec![Value::from("asd"), Value::from(i)];
        let data2 = vec![Value::from("asd"), Value::from(i), Value::from(i + 1)];
        let ops = [
            operations::hll::add_with_index_and_min_hash(
                &HLLPolicy::default(),
                "hllbin",
                &data,
                8,
                0,
            ),
            operations::hll::add_with_index_and_min_hash(
                &HLLPolicy::default(),
                "hllbin2",
                &data2,
                8,
                0,
            ),
        ];
        client.operate(&WritePolicy::default(), &key, &ops).unwrap();
    }

    set_name
}

#[test]
fn expression_hll() {
    let _ = env_logger::try_init();

    let set_name = create_test_set(EXPECTED);

    let rs = test_filter(
        Expression::eq(
            HLLExpression::get_count(HLLExpression::add_with_index_and_min_hash(
                HLLPolicy::default(),
                Expression::list_val(vec![Value::from(48715414)]),
                Expression::int_val(8),
                Expression::int_val(0),
                Expression::hll_bin("hllbin".to_string()),
            )),
            Expression::int_val(3),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 98, "HLL INIT Test Failed");

    let rs = test_filter(
        Expression::eq(
            HLLExpression::may_contain(
                Expression::list_val(vec![Value::from(55)]),
                Expression::hll_bin("hllbin".to_string()),
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "HLL MAY CONTAIN Test Failed");

    let rs = test_filter(
        Expression::lt(
            ListExpression::get_by_index(
                ListReturnType::Values,
                ExpType::INT,
                Expression::int_val(0),
                HLLExpression::describe(Expression::hll_bin("hllbin".to_string())),
                &[],
            ),
            Expression::int_val(10),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "HLL DESCRIBE Test Failed");

    let rs = test_filter(
        Expression::eq(
            HLLExpression::get_count(HLLExpression::get_union(
                Expression::hll_bin("hllbin".to_string()),
                Expression::hll_bin("hllbin2".to_string()),
            )),
            Expression::int_val(3),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 97, "HLL GET UNION Test Failed");

    let rs = test_filter(
        Expression::eq(
            HLLExpression::get_union_count(
                Expression::hll_bin("hllbin".to_string()),
                Expression::hll_bin("hllbin2".to_string()),
            ),
            Expression::int_val(3),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 97, "HLL GET UNION COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            HLLExpression::get_intersect_count(
                Expression::hll_bin("hllbin".to_string()),
                Expression::hll_bin("hllbin2".to_string()),
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 99, "HLL GET INTERSECT COUNT Test Failed");

    let rs = test_filter(
        Expression::gt(
            HLLExpression::get_similarity(
                Expression::hll_bin("hllbin".to_string()),
                Expression::hll_bin("hllbin2".to_string()),
            ),
            Expression::float_val(0.5f64),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 99, "HLL GET INTERSECT COUNT Test Failed");
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
