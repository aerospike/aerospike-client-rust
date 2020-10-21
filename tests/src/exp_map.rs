use crate::common;
use env_logger;

use aerospike::exp::map_exp::MapExpression;
use aerospike::exp::{ExpType, Expression, FilterExpression};
use aerospike::*;
use std::collections::HashMap;
use std::sync::Arc;

const EXPECTED: usize = 100;

fn create_test_set(no_records: usize) -> String {
    let client = common::client();
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let ibin = as_bin!("bin", as_map!("test" => i , "test2" => "a"));
        let bins = vec![&ibin];
        client.delete(&wpolicy, &key).unwrap();
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    set_name
}

#[test]
fn expression_map() {
    let _ = env_logger::try_init();

    let set_name = create_test_set(EXPECTED);

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_key(
                MapReturnType::Value,
                ExpType::INT,
                Expression::string_val("test3".to_string()),
                MapExpression::put(
                    &MapPolicy::default(),
                    Expression::string_val("test3".to_string()),
                    Expression::int_val(999),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(999),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY AND APPEND Test Failed");

    let mut map: HashMap<Value, Value> = HashMap::new();
    map.insert(Value::from("test4"), Value::from(333));
    map.insert(Value::from("test5"), Value::from(444));
    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_key_list(
                MapReturnType::Value,
                Expression::list_val(vec![Value::from("test4"), Value::from("test5")]),
                MapExpression::put_items(
                    &MapPolicy::default(),
                    Expression::map_val(map),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::list_val(vec![Value::from(333), Value::from(444)]),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY LIST AND APPEND LIST Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_value(
                MapReturnType::Count,
                Expression::int_val(5),
                MapExpression::increment(
                    &MapPolicy::default(),
                    Expression::string_val("test".to_string()),
                    Expression::int_val(1),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY VALUE AND INCREMENT Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::clear(Expression::map_bin("bin".to_string()), &[]),
                &[],
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "SIZE AND CLEAR Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_value_list(
                MapReturnType::Count,
                Expression::list_val(vec![Value::from(1), Value::from("a")]),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY VALUE LIST Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_value_relative_rank_range(
                MapReturnType::Count,
                Expression::int_val(1),
                Expression::int_val(0),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 99, "GET BY VALUE REL RANK RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_value_relative_rank_range_count(
                MapReturnType::Count,
                Expression::int_val(1),
                Expression::int_val(0),
                Expression::int_val(1),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY VALUE REL RANK RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_value_relative_rank_range_count(
                MapReturnType::Count,
                Expression::int_val(1),
                Expression::int_val(0),
                Expression::int_val(1),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY VALUE REL RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_value_relative_rank_range_count(
                MapReturnType::Count,
                Expression::int_val(1),
                Expression::int_val(0),
                Expression::int_val(1),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY VALUE REL RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_value_relative_rank_range_count(
                MapReturnType::Count,
                Expression::int_val(1),
                Expression::int_val(0),
                Expression::int_val(1),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY VALUE REL RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_index(
                MapReturnType::Value,
                ExpType::INT,
                Expression::int_val(0),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_index_range(
                MapReturnType::Count,
                Expression::int_val(0),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY INDEX RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_index_range_count(
                MapReturnType::Value,
                Expression::int_val(0),
                Expression::int_val(1),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::list_val(vec![Value::from(2)]),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_rank(
                MapReturnType::Value,
                ExpType::INT,
                Expression::int_val(0),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY RANK Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_rank_range(
                MapReturnType::Value,
                Expression::int_val(1),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::list_val(vec![Value::from("a")]),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY RANK RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_rank_range_count(
                MapReturnType::Value,
                Expression::int_val(0),
                Expression::int_val(1),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::list_val(vec![Value::from(15)]),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_value_range(
                MapReturnType::Count,
                Some(Expression::int_val(0)),
                Some(Expression::int_val(18)),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 18, "GET BY VALUE RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_key_range(
                MapReturnType::Count,
                None,
                Some(Expression::string_val("test25".to_string())),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_key_relative_index_range(
                MapReturnType::Count,
                Expression::string_val("test".to_string()),
                Expression::int_val(0),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(2),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY REL INDEX RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::get_by_key_relative_index_range_count(
                MapReturnType::Count,
                Expression::string_val("test".to_string()),
                Expression::int_val(0),
                Expression::int_val(1),
                Expression::map_bin("bin".to_string()),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY REL INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_key(
                    Expression::string_val("test".to_string()),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY KEY Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_key_list(
                    Expression::list_val(vec![Value::from("test"), Value::from("test2")]),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY KEY LIST Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_key_range(
                    Some(Expression::string_val("test".to_string())),
                    None,
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY KEY RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_key_relative_index_range(
                    Expression::string_val("test".to_string()),
                    Expression::int_val(0),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY KEY REL INDEX RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_key_relative_index_range_count(
                    Expression::string_val("test".to_string()),
                    Expression::int_val(0),
                    Expression::int_val(1),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(
        count, 100,
        "REMOVE BY KEY REL INDEX RANGE COUNT Test Failed"
    );

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_value(
                    Expression::int_val(5),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "REMOVE BY VALUE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_value_list(
                    Expression::list_val(vec![Value::from("a"), Value::from(15)]),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 1, "REMOVE BY VALUE LIST Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_value_range(
                    Some(Expression::int_val(5)),
                    Some(Expression::int_val(15)),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 10, "REMOVE BY VALUE RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_index(
                    Expression::int_val(0),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_index_range(
                    Expression::int_val(0),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_index_range_count(
                    Expression::int_val(0),
                    Expression::int_val(1),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_rank(
                    Expression::int_val(0),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY RANK Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_rank_range(
                    Expression::int_val(0),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(0),
        ),
        &set_name,
    );
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY RANK RANGE Test Failed");

    let rs = test_filter(
        Expression::eq(
            MapExpression::size(
                MapExpression::remove_by_rank_range_count(
                    Expression::int_val(0),
                    Expression::int_val(1),
                    Expression::map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            Expression::int_val(1),
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
