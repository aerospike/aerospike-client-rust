use crate::common;
use env_logger;

use aerospike::exp::bit_exp::BitExpression;
use aerospike::exp::list_exp::ListExpression;
use aerospike::exp::{Expression, FilterExpression, ExpType};
use aerospike::operations::bitwise::{BitPolicy, BitwiseOverflowActions, BitwiseResizeFlags};
use aerospike::operations::lists::{ListPolicy, ListReturnType};
use aerospike::*;
use std::sync::Arc;
use aerospike::operations::lists::ListReturnType::Values;

const EXPECTED: usize = 100;

fn create_test_set(no_records: usize) -> String {
    let client = common::client();
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let ibin = as_bin!("bin", as_list!(1, 2, 3, i));
        let bins = vec![&ibin];
        client.delete(&wpolicy, &key).unwrap();
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    set_name
}

#[test]
fn expression_list() {
    let _ = env_logger::try_init();

    let set_name = create_test_set(EXPECTED);

    // EQ
    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::append(
                    ListPolicy::default(),
                    Expression::int_val(999),
                    Expression::list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(5),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "SIZE AND APPEND Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::append_items(
                    ListPolicy::default(),
                    Expression::list_val(vec![Value::from(555), Value::from("asd")]),
                    Expression::list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(6),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "SIZE AND APPEND ITEMS Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::clear(Expression::list_bin("bin".to_string()), &[]),
                &[],
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "CLEAR Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_value(
                ListReturnType::Count,
                Expression::int_val(234),
                ListExpression::insert(
                    ListPolicy::default(),
                    Expression::int_val(1),
                    Expression::int_val(234),
                    Expression::list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY VALUE AND INSERT Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_value_list(
                ListReturnType::Count,
                Expression::list_val(vec![Value::from(51), Value::from(52)]),
                Expression::list_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 2, "GET BY VALUE LIST Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::insert_items(
                    ListPolicy::default(),
                    Expression::int_val(4),
                    Expression::list_val(vec![Value::from(222), Value::from(223)]),
                    Expression::list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(6),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "INSERT LIST Test Failed");

    let rs = test_filter(
        Expression::eq(
           ListExpression::get_by_index(
               ListReturnType::Values,
               ExpType::INT,
               Expression::int_val(3),
               ListExpression::increment(
                   ListPolicy::default(),
                   Expression::int_val(3),
                   Expression::int_val(100),
                   Expression::list_bin("bin".to_string()),
                   &[]),
               &[]),
            Expression::int_val(102)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX AND INCREMENT Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_index(
                ListReturnType::Values,
                ExpType::INT,
                Expression::int_val(3),
                ListExpression::set(
                    ListPolicy::default(),
                    Expression::int_val(3),
                    Expression::int_val(100),
                    Expression::list_bin("bin".to_string()),
                    &[]),
                &[]),
            Expression::int_val(100)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY INDEX AND SET Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_index_range_count(
                ListReturnType::Values,
                Expression::int_val(2),
                Expression::int_val(2),
                Expression::list_bin("bin".to_string()),
                &[]),
            Expression::list_val(vec![Value::from(3), Value::from(15)])
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_index_range(
                ListReturnType::Values,
                Expression::int_val(2),
                Expression::list_bin("bin".to_string()),
                &[]),
            Expression::list_val(vec![Value::from(3), Value::from(15)])
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_rank(
                ListReturnType::Values,
                ExpType::INT,
                Expression::int_val(3),
                Expression::list_bin("bin".to_string()),
                &[]),
            Expression::int_val(25)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY RANK Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_rank_range(
                ListReturnType::Values,
                Expression::int_val(2),
                Expression::list_bin("bin".to_string()),
                &[]),
            Expression::list_val(vec![Value::from(3), Value::from(25)])
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY RANK RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_rank_range_count(
                ListReturnType::Values,
                Expression::int_val(2),
                Expression::int_val(2),
                Expression::list_bin("bin".to_string()),
                &[]),
            Expression::list_val(vec![Value::from(3), Value::from(3)])
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_value_range(
                ListReturnType::Values,
                Some(Expression::int_val(1)),
                Some(Expression::int_val(3)),
                Expression::list_bin("bin".to_string()),
                &[]),
            Expression::list_val(vec![ Value::from(1), Value::from(2)])
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 98, "GET BY VALUE RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_value_relative_rank_range(
                ListReturnType::Count,
                Expression::int_val(2),
                Expression::int_val(0),
                Expression::list_bin("bin".to_string()),
                &[]),
            Expression::int_val(3)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 98, "GET BY VAL REL RANK RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::get_by_value_relative_rank_range_count(
                ListReturnType::Values,
                Expression::int_val(2),
                Expression::int_val(1),
                Expression::int_val(1),
                Expression::list_bin("bin".to_string()),
                &[]),
            Expression::list_val(vec![Value::from(3)])
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 99, "GET BY VAL REL RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_value(Expression::int_val(3), Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(3)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 99, "REMOVE BY VALUE Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_value_list(Expression::list_val(vec![Value::from(1), Value::from(2)]), Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(2)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 98, "REMOVE BY VALUE LIST Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_value_range(Some(Expression::int_val(1)), Some(Expression::int_val(3)),Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(2)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 98, "REMOVE BY VALUE RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_value_relative_rank_range(Expression::int_val(3), Expression::int_val(1),Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(3)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 97, "REMOVE BY VALUE REL RANK RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_value_relative_rank_range_count(Expression::int_val(2), Expression::int_val(1),Expression::int_val(1),Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(3)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY VALUE REL RANK RANGE LIST Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_index(Expression::int_val(0), Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(3)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_index_range(Expression::int_val(2), Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(2)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_index_range_count(Expression::int_val(2), Expression::int_val(1), Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(3)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_index_range_count(Expression::int_val(2), Expression::int_val(1), Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(3)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_rank(Expression::int_val(2), Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(3)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY RANK Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_rank_range(Expression::int_val(2), Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(2)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY RANK RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            ListExpression::size(
                ListExpression::remove_by_rank_range_count(Expression::int_val(2),Expression::int_val(1), Expression::list_bin("bin".to_string()), &[]),
                &[]),
            Expression::int_val(3)
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY RANK RANGE COUNT Test Failed");
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
