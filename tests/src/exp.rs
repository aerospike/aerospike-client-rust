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

const EXPECTED: usize = 100;

fn create_test_set(no_records: usize) -> String {
    let client = common::client();
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let ibin = as_bin!("bin", i);
        let sbin = as_bin!("bin2", format!("{}", i));
        let fbin = as_bin!("bin3", i as f64 / 3 as f64);
        let str = format!("{}{}", "blob", i);
        let bbin = as_bin!("bin4", str.as_bytes());
        let lbin = as_bin!("bin5", as_list!("a", "b", i));
        let mbin = as_bin!("bin6", as_map!("a" => "test", "b" => i));
        let bins = vec![&ibin, &sbin, &fbin, &bbin, &lbin, &mbin];
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
fn expression_compare() {
    let _ = env_logger::try_init();

    let set_name = create_test_set(EXPECTED);

    // EQ
    let rs = test_filter(
        Expression::eq(
            Expression::int_bin("bin".to_string()),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "EQ Test Failed");

    // NE
    let rs = test_filter(
        Expression::ne(
            Expression::int_bin("bin".to_string()),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 99, "NE Test Failed");

    // LT
    let rs = test_filter(
        Expression::lt(
            Expression::int_bin("bin".to_string()),
            Expression::int_val(10),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 10, "LT Test Failed");

    // LE
    let rs = test_filter(
        Expression::le(
            Expression::int_bin("bin".to_string()),
            Expression::int_val(10),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 11, "LE Test Failed");

    // GT
    let rs = test_filter(
        Expression::gt(
            Expression::int_bin("bin".to_string()),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 98, "GT Test Failed");

    // GE
    let rs = test_filter(
        Expression::ge(
            Expression::int_bin("bin".to_string()),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 99, "GT Test Failed");
}

#[test]
fn expression_condition() {
    let _ = env_logger::try_init();

    let set_name = create_test_set(EXPECTED);

    // AND
    let rs = test_filter(
        Expression::and(vec![
            Expression::eq(
                Expression::int_bin("bin".to_string()),
                Expression::int_val(1),
            ),
            Expression::eq(
                Expression::string_bin("bin2".to_string()),
                Expression::string_val("1".to_string()),
            ),
        ]),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "AND Test Failed");

    // OR
    let rs = test_filter(
        Expression::or(vec![
            Expression::eq(
                Expression::int_bin("bin".to_string()),
                Expression::int_val(1),
            ),
            Expression::eq(
                Expression::int_bin("bin".to_string()),
                Expression::int_val(3),
            ),
        ]),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 2, "OR Test Failed");

    // NOT
    let rs = test_filter(
        Expression::not(Expression::eq(
            Expression::int_bin("bin".to_string()),
            Expression::int_val(1),
        )),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 99, "NOT Test Failed");
}

#[test]
fn expression_data_types() {
    let _ = env_logger::try_init();

    let set_name = create_test_set(EXPECTED);

    // INT
    let rs = test_filter(
        Expression::eq(
            Expression::int_bin("bin".to_string()),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "INT Test Failed");

    // STRING
    let rs = test_filter(
        Expression::eq(
            Expression::string_bin("bin2".to_string()),
            Expression::string_val("1".to_string()),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "STRING Test Failed");

    let rs = test_filter(
        Expression::eq(
            Expression::float_bin("bin3".to_string()),
            Expression::float_val(2f64),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "FLOAT Test Failed");

    let rs = test_filter(
        Expression::eq(
            Expression::blob_bin("bin4".to_string()),
            Expression::blob_val(format!("{}{}", "blob", 5).into_bytes()),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "BLOB Test Failed");

    let rs = test_filter(
        Expression::eq(
            Expression::bin("bin4".to_string(), ExpType::BLOB),
            Expression::blob_val(format!("{}{}", "blob", 5).into_bytes()),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "BLOB BIN Test Failed");

    let rs = test_filter(
        Expression::ne(
            Expression::bin_type("bin".to_string()),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "BIN TYPE Test Failed");
}

#[test]
fn expression_rec_ops() {
    let _ = env_logger::try_init();

    let set_name = create_test_set(EXPECTED);

    // dev size 0 because inmemory
    let rs = test_filter(
        Expression::gt(Expression::device_size(), Expression::int_val(0)),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "DEVICE SIZE Test Failed");

    let rs = test_filter(
        Expression::gt(Expression::last_update(), Expression::int_val(15000)),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "LAST UPDATE Test Failed");

    let rs = test_filter(
        Expression::gt(Expression::since_update(), Expression::int_val(150)),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "SINCE UPDATE Test Failed");

    // Records dont expire
    let rs = test_filter(
        Expression::gt(Expression::void_time(), Expression::int_val(1500000000)),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "VOID TIME Test Failed");

    let rs = test_filter(
        Expression::gt(Expression::ttl(), Expression::int_val(0)),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "TTL Test Failed");

    let rs = test_filter(Expression::not(Expression::is_tombstone()), &set_name);
    let count = count_results(rs);
    assert_eq!(count, 100, "TOMBSTONE Test Failed");

    let rs = test_filter(
        Expression::eq(
            Expression::set_name(),
            Expression::string_val(set_name.clone()),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "SET NAME Test Failed");

    let rs = test_filter(Expression::bin_exists("bin4".to_string()), &set_name);
    let count = count_results(rs);
    assert_eq!(count, 100, "BIN EXISTS Test Failed");

    let rs = test_filter(
        Expression::eq(Expression::digest_modulo(3), Expression::int_val(1)),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count > 0 && count < 100, true, "DIGEST MODULO Test Failed");

    let rs = test_filter(
        Expression::eq(Expression::key(ExpType::INT), Expression::int_val(50)),
        &set_name,
    );
    let count = count_results(rs);
    // 0 because key is not saved
    assert_eq!(count, 0, "KEY Test Failed");

    let rs = test_filter(Expression::key_exists(), &set_name);
    let count = count_results(rs);
    // 0 because key is not saved
    assert_eq!(count, 0, "KEY EXISTS Test Failed");

    /* let rs = test_filter(
        Expression::eq(Expression::nil(), Expression::nil()),
        &set_name,
    );
    let count = count_results(rs);
    // 0 because key is not saved
    assert_eq!(count, 0, "NIL Test Failed"); */
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
