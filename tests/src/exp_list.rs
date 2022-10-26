use crate::common;
use env_logger;

use aerospike::expressions::lists::*;
use aerospike::expressions::*;
use aerospike::operations::lists::{ListPolicy, ListReturnType};
use aerospike::*;
use std::sync::Arc;

const EXPECTED: usize = 100;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let ibin = as_bin!("bin", as_list!(1, 2, 3, i));
        let bins = vec![ibin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[aerospike_macro::test]
async fn expression_list() {
    let _ = env_logger::try_init();
    let client = common::client().await;

    let set_name = create_test_set(&client, EXPECTED).await;

    // EQ
    let rs = test_filter(
        &client,
        eq(
            size(
                append(
                    ListPolicy::default(),
                    int_val(999),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(5),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "SIZE AND APPEND Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                append_items(
                    ListPolicy::default(),
                    list_val(vec![Value::from(555), Value::from("asd")]),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(6),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "SIZE AND APPEND ITEMS Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(clear(list_bin("bin".to_string()), &[]), &[]),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "CLEAR Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value(
                ListReturnType::Count,
                int_val(234),
                insert(
                    ListPolicy::default(),
                    int_val(1),
                    int_val(234),
                    list_bin("bin".to_string()),
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
    assert_eq!(count, 100, "GET BY VALUE AND INSERT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_list(
                ListReturnType::Count,
                list_val(vec![Value::from(51), Value::from(52)]),
                list_bin("bin".to_string()),
                &[],
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 2, "GET BY VALUE LIST Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                insert_items(
                    ListPolicy::default(),
                    int_val(4),
                    list_val(vec![Value::from(222), Value::from(223)]),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(6),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "INSERT LIST Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_index(
                ListReturnType::Values,
                ExpType::INT,
                int_val(3),
                increment(
                    ListPolicy::default(),
                    int_val(3),
                    int_val(100),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(102),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX AND INCREMENT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_index(
                ListReturnType::Values,
                ExpType::INT,
                int_val(3),
                set(
                    ListPolicy::default(),
                    int_val(3),
                    int_val(100),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(100),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "GET BY INDEX AND SET Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_index_range_count(
                ListReturnType::Values,
                int_val(2),
                int_val(2),
                list_bin("bin".to_string()),
                &[],
            ),
            list_val(vec![Value::from(3), Value::from(15)]),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_index_range(
                ListReturnType::Values,
                int_val(2),
                list_bin("bin".to_string()),
                &[],
            ),
            list_val(vec![Value::from(3), Value::from(15)]),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY INDEX RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_rank(
                ListReturnType::Values,
                ExpType::INT,
                int_val(3),
                list_bin("bin".to_string()),
                &[],
            ),
            int_val(25),
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
                ListReturnType::Values,
                int_val(2),
                list_bin("bin".to_string()),
                &[],
            ),
            list_val(vec![Value::from(3), Value::from(25)]),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "GET BY RANK RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_rank_range_count(
                ListReturnType::Values,
                int_val(2),
                int_val(2),
                list_bin("bin".to_string()),
                &[],
            ),
            list_val(vec![Value::from(3), Value::from(3)]),
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
                ListReturnType::Values,
                Some(int_val(1)),
                Some(int_val(3)),
                list_bin("bin".to_string()),
                &[],
            ),
            list_val(vec![Value::from(1), Value::from(2)]),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 98, "GET BY VALUE RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_relative_rank_range(
                ListReturnType::Count,
                int_val(2),
                int_val(0),
                list_bin("bin".to_string()),
                &[],
            ),
            int_val(3),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 98, "GET BY VAL REL RANK RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_by_value_relative_rank_range_count(
                ListReturnType::Values,
                int_val(2),
                int_val(1),
                int_val(1),
                list_bin("bin".to_string()),
                &[],
            ),
            list_val(vec![Value::from(3)]),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "GET BY VAL REL RANK RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value(int_val(3), list_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(3),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "REMOVE BY VALUE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value_list(
                    list_val(vec![Value::from(1), Value::from(2)]),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 98, "REMOVE BY VALUE LIST Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value_range(
                    Some(int_val(1)),
                    Some(int_val(3)),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 98, "REMOVE BY VALUE RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value_relative_rank_range(
                    int_val(3),
                    int_val(1),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(3),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 97, "REMOVE BY VALUE REL RANK RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value_relative_rank_range_count(
                    int_val(2),
                    int_val(1),
                    int_val(1),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(3),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(
        count, 100,
        "REMOVE BY VALUE REL RANK RANGE LIST Test Failed"
    );

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_index(int_val(0), list_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(3),
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
                remove_by_index_range(int_val(2), list_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(2),
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
                    int_val(2),
                    int_val(1),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(3),
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
                remove_by_index_range_count(
                    int_val(2),
                    int_val(1),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(3),
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
                remove_by_rank(int_val(2), list_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(3),
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
                remove_by_rank_range(int_val(2), list_bin("bin".to_string()), &[]),
                &[],
            ),
            int_val(2),
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
                remove_by_rank_range_count(
                    int_val(2),
                    int_val(1),
                    list_bin("bin".to_string()),
                    &[],
                ),
                &[],
            ),
            int_val(3),
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
