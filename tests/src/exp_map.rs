use crate::common;
use env_logger;

use aerospike::expressions::maps::*;
use aerospike::expressions::*;
use aerospike::*;
use std::collections::HashMap;
use std::sync::Arc;

const EXPECTED: usize = 100;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let ibin = as_bin!("bin", as_map!("test" => i , "test2" => "a"));
        let bins = vec![ibin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[aerospike_macro::test]
fn expression_map() {
    let _ = env_logger::try_init();
    let client = common::client().await;
    let set_name = create_test_set(&client, EXPECTED).await;

    let rs = test_filter(
        &client,
        eq(
            get_by_key(
                MapReturnType::Value,
                ExpType::INT,
                string_val("test3".to_string()),
                put(
                    &MapPolicy::default(),
                    string_val("test3".to_string()),
                    int_val(999),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(999),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY AND APPEND Test Failed");

    let mut map: HashMap<Value, Value> = HashMap::new();
    map.insert(Value::from("test4"), Value::from(333));
    map.insert(Value::from("test5"), Value::from(444));
    let rs = test_filter(
        &client,
        eq(
            get_by_key_list(
                MapReturnType::Value,
                list_val(vec![Value::from("test4"), Value::from("test5")]),
                put_items(
                    &MapPolicy::default(),
                    map_val(map),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            list_val(vec![Value::from(333), Value::from(444)]),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY LIST AND APPEND LIST Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value(
                MapReturnType::Count,
                int_val(5),
                increment(
                    &MapPolicy::default(),
                    string_val("test".to_string()),
                    int_val(1),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY VALUE AND INCREMENT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(clear(map_bin("bin".to_string()), &[]), &[]),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "SIZE AND CLEAR Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_list(
                MapReturnType::Count,
                list_val(vec![Value::from(1), Value::from("a")]),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY VALUE LIST Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_relative_rank_range(
                MapReturnType::Count,
                int_val(1),
                int_val(0),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "GET BY VALUE REL RANK RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_relative_rank_range_count(
                MapReturnType::Count,
                int_val(1),
                int_val(0),
                int_val(1),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY VALUE REL RANK RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_relative_rank_range_count(
                MapReturnType::Count,
                int_val(1),
                int_val(0),
                int_val(1),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY VALUE REL RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_relative_rank_range_count(
                MapReturnType::Count,
                int_val(1),
                int_val(0),
                int_val(1),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY VALUE REL RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_relative_rank_range_count(
                MapReturnType::Count,
                int_val(1),
                int_val(0),
                int_val(1),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY VALUE REL RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_index(
                MapReturnType::Value,
                ExpType::INT,
                int_val(0),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_index_range(
                MapReturnType::Count,
                int_val(0),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY INDEX RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_index_range_count(
                MapReturnType::Value,
                int_val(0),
                int_val(1),
                map_bin("bin".to_string()),
                &[],
            ),
            list_val(vec![Value::from(2)]),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_rank(
                MapReturnType::Value,
                ExpType::INT,
                int_val(0),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY RANK Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_rank_range(
                MapReturnType::Value,
                int_val(1),
                map_bin("bin".to_string()),
                &[],
            ),
            list_val(vec![Value::from("a")]),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY RANK RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_rank_range_count(
                MapReturnType::Value,
                int_val(0),
                int_val(1),
                map_bin("bin".to_string()),
                &[],
            ),
            list_val(vec![Value::from(15)]),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_range(
                MapReturnType::Count,
                Some(int_val(0)),
                Some(int_val(18)),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 18, "GET BY VALUE RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_key_range(
                MapReturnType::Count,
                None,
                Some(string_val("test25".to_string())),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_key_relative_index_range(
                MapReturnType::Count,
                string_val("test".to_string()),
                int_val(0),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY REL INDEX RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_key_relative_index_range_count(
                MapReturnType::Count,
                string_val("test".to_string()),
                int_val(0),
                int_val(1),
                map_bin("bin".to_string()),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY KEY REL INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key(
                    string_val("test".to_string()),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY KEY Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key_list(
                    list_val(vec![Value::from("test"), Value::from("test2")]),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY KEY LIST Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key_range(
                    Some(string_val("test".to_string())),
                    None,
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY KEY RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key_relative_index_range(
                    string_val("test".to_string()),
                    int_val(0),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY KEY REL INDEX RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key_relative_index_range_count(
                    string_val("test".to_string()),
                    int_val(0),
                    int_val(1),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(
        count, 100,
        "REMOVE BY KEY REL INDEX RANGE COUNT Test Failed"
    );

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value(int_val(5), map_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "REMOVE BY VALUE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value_list(
                    list_val(vec![Value::from("a"), Value::from(15)]),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "REMOVE BY VALUE LIST Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value_range(
                    Some(int_val(5)),
                    Some(int_val(15)),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 10, "REMOVE BY VALUE RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_index(int_val(0), map_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_index_range(int_val(0), map_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_index_range_count(
                    int_val(0),
                    int_val(1),
                    map_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_rank(int_val(0), map_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY RANK Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_rank_range(int_val(0), map_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY RANK RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_rank_range_count(int_val(0), int_val(1), map_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "REMOVE BY RANK RANGE COUNT Test Failed");
    client.close().await.unwrap();
}

async fn test_filter(client: &Client, filter: FilterExpression, set_name: &str) -> Arc<Recordset> {
    let namespace = common::namespace();

    let mut qpolicy = QueryPolicy::default();
    qpolicy.filter_expression = Some(filter);

    let statement = Statement::new(namespace, set_name, Bins::All);
    client.query(&qpolicy, statement).await.unwrap()
}

fn count_results(rs: Arc<Recordset>) -> usize {
    let mut count = 0;

    for res in &*rs {
        match res {
            Ok(_) => {
                count += 1;
            }
            Err(err) => panic!("{:?}", err),
        }
    }

    count
}
