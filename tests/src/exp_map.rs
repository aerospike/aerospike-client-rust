use crate::common;

use crate::src::count_results;

use aerospike::expressions::maps::*;
use aerospike::expressions::*;
use aerospike::query::PartitionFilter;
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
        common::delete_durably(client, &wpolicy, &key)
            .await
            .unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[aerospike_macro::test]
fn expression_map() {
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "GET BY KEY REL INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY KEY Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key_list(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY KEY LIST Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key_range(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY KEY RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key_relative_index_range(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY KEY REL INDEX RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_key_relative_index_range_count(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(
        count, 100,
        "REMOVE BY KEY REL INDEX RANGE COUNT Test Failed"
    );

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value(
                    MapReturnType::None,
                    int_val(5),
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
    let count = count_results(rs).await;
    assert_eq!(count, 1, "REMOVE BY VALUE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value_list(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 1, "REMOVE BY VALUE LIST Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_value_range(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 10, "REMOVE BY VALUE RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_index(
                    MapReturnType::None,
                    int_val(0),
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY INDEX Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_index_range(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY INDEX RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_index_range_count(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY INDEX RANGE COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_rank(
                    MapReturnType::None,
                    int_val(0),
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY RANK Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_rank_range(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY RANK RANGE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            size(
                remove_by_rank_range_count(
                    MapReturnType::None,
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
    let count = count_results(rs).await;
    assert_eq!(count, 100, "REMOVE BY RANK RANGE COUNT Test Failed");
    client.close().await.unwrap();
}

async fn test_filter(client: &Client, filter: Expression, set_name: &str) -> Arc<Recordset> {
    let namespace = common::namespace();

    let mut qpolicy = QueryPolicy::default();
    qpolicy.base_policy.filter_expression = Some(filter);

    let statement = Statement::new(namespace, set_name, Bins::All);
    let pf = PartitionFilter::all();
    client.query(&qpolicy, pf, statement).await.unwrap()
}

// Server 6.3+ supports comparing ORDERED map values in expressions:
// with a K-ordered stored bin and a `BTreeMap` literal (map_val packs
// it with the K-ordered wire flag), eq/ne/gt/ge/le/lt all evaluate by
// the canonical map order (length first, then entry-wise). Unordered
// operands on either side are not comparable on servers before
// AER-6930 (verified FilteredOut on 8.1.2.0), so those cases are only
// checked leniently.
#[aerospike_macro::test]
async fn ordered_map_expression_comparisons() {
    use std::collections::BTreeMap;

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let wpolicy = WritePolicy::default();

    let pair = |k: &str, v: i64| (as_val!(k), as_val!(v));
    let key = as_key!(namespace, set_name, "ordered_map_cmp");

    // Bin stored as a K-ordered map (SortedMap carries the ordered wire flag).
    client
        .put(
            &wpolicy,
            &key,
            &[Bin::new(
                "m".into(),
                as_sorted_map!("a" => 1, "b" => 2, "c" => 3),
            )],
        )
        .await
        .unwrap();

    let same: BTreeMap<Value, Value> = [pair("a", 1), pair("b", 2), pair("c", 3)].into();
    let smaller: BTreeMap<Value, Value> = [pair("a", 1), pair("b", 2)].into();

    // (label, filter, expect_match)
    let cases: Vec<(&str, Expression, bool)> = vec![
        (
            "eq same",
            eq(map_bin("m".into()), map_val(same.clone())),
            true,
        ),
        (
            "ne same",
            ne(map_bin("m".into()), map_val(same.clone())),
            false,
        ),
        (
            "ne smaller",
            ne(map_bin("m".into()), map_val(smaller.clone())),
            true,
        ),
        // Canonical map order is length-first: a 3-entry map sorts after
        // a 2-entry map.
        (
            "gt smaller",
            gt(map_bin("m".into()), map_val(smaller.clone())),
            true,
        ),
        (
            "lt smaller",
            lt(map_bin("m".into()), map_val(smaller)),
            false,
        ),
        (
            "ge same",
            ge(map_bin("m".into()), map_val(same.clone())),
            true,
        ),
        ("le same", le(map_bin("m".into()), map_val(same)), true),
    ];

    for (label, filter, expect_match) in cases {
        let mut fpolicy = WritePolicy::default();
        fpolicy.base_policy.filter_expression = Some(filter);
        let result = client
            .operate(&fpolicy, &key, &[operations::get_bin("m")])
            .await;
        if expect_match {
            assert!(
                result.is_ok(),
                "{label}: expected the filter to match, got {result:?}"
            );
        } else {
            let err = result.expect_err(&format!("{label}: expected FilteredOut"));
            assert_eq!(
                err.server_result_code(),
                Some(ResultCode::FilteredOut),
                "{label}: expected a clean FilteredOut evaluation"
            );
        }
    }

    client.close().await.unwrap();
}
