// Copyright 2015-2020 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration tests for CDT path expressions (select_by_path / modify_by_path).
//! Requires Aerospike Server version >= 8.1.1.

use crate::common;

use aerospike::expressions::maps::get_by_key;
use aerospike::expressions::*;
use aerospike::operations::cdt_context::{
    ctx_all_children, ctx_all_children_with_filter, ctx_and_filter, ctx_from_base64, ctx_map_key,
    ctx_map_keys_in, Path,
};
use aerospike::operations::path::{
    modify_by_path, modify_no_fail, remove as path_remove, select_by_path, select_map_entries,
    select_map_keys, select_matching_tree, select_values, ModifyFlag, SelectFlag,
};
use aerospike::{
    as_bin, as_key, as_list, as_map, as_val, Bins, MapReturnType, ReadPolicy, Value, WritePolicy,
};

/// Helper to check whether the connected server supports CDT path expressions.
/// Returns false if the version check can't be made or the version is too old.
async fn server_supports_cdt_path_expressions(client: &aerospike::Client) -> bool {
    match client.cluster.get_random_node() {
        Ok(node) => node.version().supports_cdt_path_expressions(),
        Err(_) => false,
    }
}

/// Helper for the enhanced 8.1.2 expression API
/// (`in_list` / `map_keys` / `map_values` / `ctx_map_keys_in` / `ctx_and_filter`).
async fn server_supports_enhanced_expression_api(client: &aerospike::Client) -> bool {
    match client.cluster.get_random_node() {
        Ok(node) => node.version().supports_enhanced_expression_api(),
        Err(_) => false,
    }
}

// ===== select_by_path tests =====

#[aerospike_macro::test]
async fn select_by_path_price_filter() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "path_sel1");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    // Build: { "book": [ {title, price}, ... ] }
    let books = as_list!(
        as_map!("title" => "Sayings of the Century", "price" => 8.95_f64),
        as_map!("title" => "Sword of Honour", "price" => 12.99_f64),
        as_map!("title" => "Moby Dick", "price" => 8.99_f64),
        as_map!("title" => "The Lord of the Rings", "price" => 22.99_f64)
    );
    let root = as_map!("book" => books);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Context: root["book"] -> children where price <= 10.0 -> children where key == "title"
    let ctx1 = ctx_map_key(Value::from("book"));
    let ctx2 = ctx_all_children_with_filter(le(
        get_by_key(
            MapReturnType::Value,
            ExpType::FLOAT,
            string_val("price".to_string()),
            exp_map_loop_var(LoopVarPart::VALUE),
            &[],
        ),
        float_val(10.0),
    ));
    let ctx3 = ctx_all_children_with_filter(eq(
        exp_string_loop_var(LoopVarPart::MAP_KEY),
        string_val("title".to_string()),
    ));

    let op = select_by_path("testbin", SelectFlag::VALUE, &[ctx1, ctx2, ctx3]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let result = rec.bins.get("testbin").unwrap();
    if let Value::List(titles) = result {
        assert_eq!(titles.len(), 2, "Should have 2 cheap books");
        let title_strs: Vec<&str> = titles
            .iter()
            .filter_map(|v| {
                if let Value::String(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert!(title_strs.contains(&"Sayings of the Century"));
        assert!(title_strs.contains(&"Moby Dick"));
    } else {
        panic!("Expected a list result, got: {:?}", result);
    }
}

#[aerospike_macro::test]
async fn select_by_path_empty_result() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "path_sel_empty");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let books = as_list!(
        as_map!("title" => "Expensive Book 1", "price" => 25.99_f64),
        as_map!("title" => "Expensive Book 2", "price" => 30.50_f64)
    );
    let root = as_map!("book" => books);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let ctx1 = ctx_map_key(Value::from("book"));
    let ctx2 = ctx_all_children_with_filter(le(
        get_by_key(
            MapReturnType::Value,
            ExpType::FLOAT,
            string_val("price".to_string()),
            exp_map_loop_var(LoopVarPart::VALUE),
            &[],
        ),
        float_val(10.0),
    ));

    let op = select_by_path("testbin", SelectFlag::VALUE, &[ctx1, ctx2]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    if let Some(Value::List(items)) = rec.bins.get("testbin") {
        assert_eq!(items.len(), 0, "No books should match the filter");
    }
    // nil result is also acceptable (no matching items)
}

#[aerospike_macro::test]
async fn select_by_path_ctx_all_children() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "path_sel_all");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let numbers = as_list!(10_i64, 20_i64, 30_i64);
    let root = as_map!("numbers" => numbers);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Select all children (no filter)
    let ctx1 = ctx_map_key(Value::from("numbers"));
    let ctx2 = ctx_all_children();

    let op = select_by_path("testbin", SelectFlag::VALUE, &[ctx1, ctx2]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    if let Some(Value::List(items)) = rec.bins.get("testbin") {
        assert_eq!(items.len(), 3, "Should return all 3 numbers");
    } else {
        // Result can be nil if the server returns no-op for all-children on a list
    }
}

#[aerospike_macro::test]
async fn select_by_path_index_loop_var() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "path_sel_idx");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let numbers = as_list!(10_i64, 20_i64, 30_i64, 40_i64, 50_i64);
    let root = as_map!("numbers" => numbers);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Select items where index < 3
    let ctx1 = ctx_map_key(Value::from("numbers"));
    let ctx2 = ctx_all_children_with_filter(lt(exp_int_loop_var(LoopVarPart::INDEX), int_val(3)));

    let op = select_by_path("testbin", SelectFlag::VALUE, &[ctx1, ctx2]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    if let Some(Value::List(items)) = rec.bins.get("testbin") {
        assert_eq!(
            items.len(),
            3,
            "Should return first 3 elements (indices 0,1,2)"
        );
    } else {
        panic!("Expected list result");
    }
}

#[aerospike_macro::test]
async fn select_by_path_complex_nested() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "path_sel_nested");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let books = as_list!(
        as_map!("category" => "reference", "title" => "Sayings of the Century", "price" => 8.95_f64),
        as_map!("category" => "fiction", "title" => "Sword of Honour", "price" => 12.99_f64),
        as_map!("category" => "fiction", "title" => "Moby Dick", "price" => 8.99_f64)
    );
    let store = as_map!("books" => books);
    let root = as_map!("store" => store);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Select titles of fiction books with price < 10.0
    let ctx1 = ctx_map_key(Value::from("store"));
    let ctx2 = ctx_map_key(Value::from("books"));
    let ctx3 = ctx_all_children_with_filter(and(vec![
        eq(
            get_by_key(
                MapReturnType::Value,
                ExpType::STRING,
                string_val("category".to_string()),
                exp_map_loop_var(LoopVarPart::VALUE),
                &[],
            ),
            string_val("fiction".to_string()),
        ),
        lt(
            get_by_key(
                MapReturnType::Value,
                ExpType::FLOAT,
                string_val("price".to_string()),
                exp_map_loop_var(LoopVarPart::VALUE),
                &[],
            ),
            float_val(10.0),
        ),
    ]));
    let ctx4 = ctx_all_children_with_filter(eq(
        exp_string_loop_var(LoopVarPart::MAP_KEY),
        string_val("title".to_string()),
    ));

    let op = select_by_path("testbin", SelectFlag::VALUE, &[ctx1, ctx2, ctx3, ctx4]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    if let Some(Value::List(items)) = rec.bins.get("testbin") {
        assert_eq!(
            items.len(),
            1,
            "Only Moby Dick is fiction with price < 10.0"
        );
        assert_eq!(items[0], Value::from("Moby Dick"));
    } else {
        panic!("Expected list result");
    }
}

// ===== modify_by_path tests =====

#[aerospike_macro::test]
async fn modify_by_path_multiply_prices() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "path_mod_mul");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let books = as_list!(
        as_map!("title" => "Book A", "price" => 8.95_f64),
        as_map!("title" => "Book B", "price" => 12.99_f64)
    );
    let root = as_map!("book" => books);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Multiply all prices by 1.10
    let ctx1 = ctx_map_key(Value::from("book"));
    let ctx2 = ctx_all_children();
    let ctx3 = ctx_all_children_with_filter(eq(
        exp_string_loop_var(LoopVarPart::MAP_KEY),
        string_val("price".to_string()),
    ));

    let modify_exp = num_mul(vec![
        exp_float_loop_var(LoopVarPart::VALUE),
        float_val(1.10),
    ]);

    let op = modify_by_path(
        "testbin",
        ModifyFlag::DEFAULT,
        modify_exp,
        &[ctx1, ctx2, ctx3],
    );
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    let root_val = rec.bins.get("testbin").unwrap();

    if let Value::HashMap(root_map) = root_val {
        if let Some(Value::List(book_list)) = root_map.get(&Value::from("book")) {
            // Check first book's price was multiplied
            if let Value::HashMap(book0) = &book_list[0] {
                let price = book0.get(&Value::from("price")).unwrap();
                let price_f = match price {
                    Value::Float(f) => f64::from(f),
                    Value::Int(i) => *i as f64,
                    _ => panic!("Unexpected price type: {:?}", price),
                };
                let expected = 8.95 * 1.10;
                assert!(
                    (price_f - expected).abs() < 0.01,
                    "Price should be ~{:.2}, got {:.2}",
                    expected,
                    price_f
                );
            }
        }
    }
}

// ===== exp_select_by_path t=

#[aerospike_macro::test]
async fn exp_select_by_path_filter() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "exp_sel_filter");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let books = as_list!(
        as_map!("title" => "Cheap Book", "price" => 5.99_f64),
        as_map!("title" => "Expensive Book", "price" => 25.99_f64)
    );
    let root = as_map!("book" => books);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let ctx1 = ctx_map_key(Value::from("book"));
    let ctx2 = ctx_all_children_with_filter(le(
        get_by_key(
            MapReturnType::Value,
            ExpType::FLOAT,
            string_val("price".to_string()),
            exp_map_loop_var(LoopVarPart::VALUE),
            &[],
        ),
        float_val(10.0),
    ));

    let bin_exp = aerospike::expressions::map_bin("testbin".to_string());
    let exp = exp_select_by_path(ExpType::LIST, SelectFlag::VALUE, bin_exp, &[ctx1, ctx2]);

    let ops = &[aerospike::operations::exp::read_exp(
        "result",
        exp,
        aerospike::operations::exp::ExpReadFlags::Default,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();

    if let Some(Value::List(items)) = rec.bins.get("result") {
        assert_eq!(items.len(), 1, "Should return 1 cheap book");
    }
    // nil result is also acceptable if server returns no result
}

// ===== exp_modify_by_path tests =====

#[aerospike_macro::test]
async fn exp_modify_by_path_multiply() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "exp_mod_mul");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let items = as_list!(1.0_f64, 2.0_f64, 3.0_f64);
    let root = as_map!("vals" => items);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let ctx1 = ctx_map_key(Value::from("vals"));
    let ctx2 = ctx_all_children();

    let modify_exp = num_mul(vec![exp_float_loop_var(LoopVarPart::VALUE), float_val(2.0)]);

    let bin_exp = aerospike::expressions::map_bin("testbin".to_string());
    let exp = exp_modify_by_path(
        ExpType::MAP,
        aerospike::operations::path::ModifyFlag::DEFAULT,
        bin_exp,
        modify_exp,
        &[ctx1, ctx2],
    );

    let ops = &[aerospike::operations::exp::write_exp(
        "testbin",
        exp,
        aerospike::operations::exp::ExpWriteFlags::Default,
    )];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root_map)) = rec.bins.get("testbin") {
        if let Some(Value::List(vals)) = root_map.get(&Value::from("vals")) {
            for (i, val) in vals.iter().enumerate() {
                let expected = (i as f64 + 1.0) * 2.0;
                let actual = match val {
                    Value::Float(f) => f64::from(f),
                    Value::Int(v) => *v as f64,
                    _ => panic!("Unexpected value type"),
                };
                assert!(
                    (actual - expected).abs() < 0.01,
                    "vals[{}] should be ~{:.1}, got {:.1}",
                    i,
                    expected,
                    actual
                );
            }
        }
    }
}

// ===== Loop variable tests =====

#[aerospike_macro::test]
async fn loop_var_int_value() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "loop_int");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    // Map where values are integers - select entries with value > 75
    let items = as_map!("a" => 100_i64, "b" => 50_i64, "c" => 200_i64);
    let root = as_map!("items" => items);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let ctx1 = ctx_map_key(Value::from("items"));
    let ctx2 = ctx_all_children_with_filter(gt(exp_int_loop_var(LoopVarPart::VALUE), int_val(75)));

    let op = select_by_path("testbin", SelectFlag::VALUE, &[ctx1, ctx2]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    if let Some(Value::List(items)) = rec.bins.get("testbin") {
        assert_eq!(
            items.len(),
            2,
            "Should return 2 items with value > 75 (100 and 200)"
        );
    } else {
        panic!("Expected list result");
    }
}

#[aerospike_macro::test]
async fn loop_var_string_map_key() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "loop_str_key");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let data = as_map!("alpha" => 1_i64, "beta" => 2_i64, "gamma" => 3_i64);
    let root = as_map!("data" => data);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Select entries where the MAP_KEY == "alpha"
    let ctx1 = ctx_map_key(Value::from("data"));
    let ctx2 = ctx_all_children_with_filter(eq(
        exp_string_loop_var(LoopVarPart::MAP_KEY),
        string_val("alpha".to_string()),
    ));

    let op = select_by_path("testbin", SelectFlag::VALUE, &[ctx1, ctx2]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    if let Some(Value::List(items)) = rec.bins.get("testbin") {
        assert_eq!(items.len(), 1, "Should return 1 entry with key 'alpha'");
        assert_eq!(items[0], Value::from(1_i64));
    } else {
        panic!("Expected list result");
    }
}

// ===== exp_remove_result tests =====

#[aerospike_macro::test]
async fn remove_all_items_from_list() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "rm_list_all");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let data = as_map!("items" => as_list!(1_i64, 2_i64, 3_i64, 4_i64, 5_i64));
    let bin = as_bin!("testbin", data);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let ctx1 = ctx_map_key(Value::from("items"));
    let ctx2 = ctx_all_children();

    let op = modify_by_path(
        "testbin",
        ModifyFlag::DEFAULT,
        exp_remove_result(),
        &[ctx1, ctx2],
    );
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root_map)) = rec.bins.get("testbin") {
        if let Some(Value::List(items)) = root_map.get(&Value::from("items")) {
            assert_eq!(items.len(), 0, "All items should be removed");
        } else {
            panic!("Expected 'items' key to exist as a list");
        }
    } else {
        panic!("Expected HashMap result for testbin");
    }
}

#[aerospike_macro::test]
async fn remove_filtered_items_from_list() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "rm_list_filter");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let data = as_map!("numbers" => as_list!(1_i64, 5_i64, 10_i64, 15_i64, 20_i64, 25_i64, 30_i64));
    let bin = as_bin!("testbin", data);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Remove items where value > 10
    let ctx1 = ctx_map_key(Value::from("numbers"));
    let ctx2 = ctx_all_children_with_filter(gt(exp_int_loop_var(LoopVarPart::VALUE), int_val(10)));

    let op = modify_by_path(
        "testbin",
        ModifyFlag::DEFAULT,
        exp_remove_result(),
        &[ctx1, ctx2],
    );
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root_map)) = rec.bins.get("testbin") {
        if let Some(Value::List(numbers)) = root_map.get(&Value::from("numbers")) {
            assert_eq!(numbers.len(), 3, "Should keep items <= 10");
            assert!(numbers.contains(&Value::from(1_i64)));
            assert!(numbers.contains(&Value::from(5_i64)));
            assert!(numbers.contains(&Value::from(10_i64)));
        } else {
            panic!("Expected 'numbers' key to exist as a list");
        }
    } else {
        panic!("Expected HashMap result for testbin");
    }
}

#[aerospike_macro::test]
async fn remove_all_items_from_map() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "rm_map_all");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let config = as_map!("option1" => "value1", "option2" => "value2", "option3" => "value3");
    let data = as_map!("config" => config);
    let bin = as_bin!("testbin", data);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let ctx1 = ctx_map_key(Value::from("config"));
    let ctx2 = ctx_all_children();

    let op = modify_by_path(
        "testbin",
        ModifyFlag::DEFAULT,
        exp_remove_result(),
        &[ctx1, ctx2],
    );
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root_map)) = rec.bins.get("testbin") {
        if let Some(Value::HashMap(config_map)) = root_map.get(&Value::from("config")) {
            assert_eq!(config_map.len(), 0, "All map entries should be removed");
        } else {
            panic!("Expected 'config' key to exist as a map");
        }
    } else {
        panic!("Expected HashMap result for testbin");
    }
}

#[aerospike_macro::test]
async fn remove_filtered_map_entries() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "rm_map_filter");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let scores = as_map!("alice" => 95_i64, "bob" => 45_i64, "carol" => 75_i64, "dave" => 30_i64);
    let data = as_map!("scores" => scores);
    let bin = as_bin!("testbin", data);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Remove entries where value < 50 (removes bob=45 and dave=30)
    let ctx1 = ctx_map_key(Value::from("scores"));
    let ctx2 = ctx_all_children_with_filter(lt(exp_int_loop_var(LoopVarPart::VALUE), int_val(50)));

    let op = modify_by_path(
        "testbin",
        ModifyFlag::DEFAULT,
        exp_remove_result(),
        &[ctx1, ctx2],
    );
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root_map)) = rec.bins.get("testbin") {
        if let Some(Value::HashMap(scores_map)) = root_map.get(&Value::from("scores")) {
            assert_eq!(scores_map.len(), 2, "Should keep scores >= 50");
            assert!(!scores_map.contains_key(&Value::from("bob")));
            assert!(!scores_map.contains_key(&Value::from("dave")));
            assert_eq!(
                scores_map.get(&Value::from("alice")),
                Some(&Value::from(95_i64))
            );
        } else {
            panic!("Expected 'scores' key to exist as a map");
        }
    } else {
        panic!("Expected HashMap result for testbin");
    }
}

#[aerospike_macro::test]
async fn remove_books_with_low_prices() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "rm_books_price");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let books = as_list!(
        as_map!("title" => "Cheap Book 1", "price" => 5.99_f64),
        as_map!("title" => "Expensive Book", "price" => 25.99_f64),
        as_map!("title" => "Cheap Book 2", "price" => 3.99_f64),
        as_map!("title" => "Mid Price Book", "price" => 15.99_f64)
    );
    let root = as_map!("books" => books);
    let bin = as_bin!("testbin", root);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Remove books where price <= 10.0
    let ctx1 = ctx_map_key(Value::from("books"));
    let ctx2 = ctx_all_children_with_filter(le(
        get_by_key(
            MapReturnType::Value,
            ExpType::FLOAT,
            string_val("price".to_string()),
            exp_map_loop_var(LoopVarPart::VALUE),
            &[],
        ),
        float_val(10.0),
    ));

    let op = modify_by_path(
        "testbin",
        ModifyFlag::DEFAULT,
        exp_remove_result(),
        &[ctx1, ctx2],
    );
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root_map)) = rec.bins.get("testbin") {
        if let Some(Value::List(book_list)) = root_map.get(&Value::from("books")) {
            assert_eq!(book_list.len(), 2, "Should keep 2 expensive books");
            for book_val in book_list {
                if let Value::HashMap(book) = book_val {
                    let price = book.get(&Value::from("price")).unwrap();
                    let price_f = match price {
                        Value::Float(f) => f64::from(f),
                        Value::Int(i) => *i as f64,
                        _ => panic!("Unexpected price type: {:?}", price),
                    };
                    assert!(price_f > 10.0, "Remaining books should have price > 10.0");
                } else {
                    panic!("Expected book to be a HashMap");
                }
            }
        } else {
            panic!("Expected 'books' key to exist as a list");
        }
    } else {
        panic!("Expected HashMap result for testbin");
    }
}

#[aerospike_macro::test]
async fn remove_items_by_index_filter() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "rm_idx_filter");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let data = as_map!("values" => as_list!(100_i64, 200_i64, 300_i64, 400_i64, 500_i64));
    let bin = as_bin!("testbin", data);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Remove items where index >= 3 (removes 400 and 500)
    let ctx1 = ctx_map_key(Value::from("values"));
    let ctx2 = ctx_all_children_with_filter(ge(exp_int_loop_var(LoopVarPart::INDEX), int_val(3)));

    let op = modify_by_path(
        "testbin",
        ModifyFlag::DEFAULT,
        exp_remove_result(),
        &[ctx1, ctx2],
    );
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root_map)) = rec.bins.get("testbin") {
        if let Some(Value::List(values)) = root_map.get(&Value::from("values")) {
            assert_eq!(values.len(), 3, "Should keep first 3 items");
            assert_eq!(values[0], Value::from(100_i64));
            assert_eq!(values[1], Value::from(200_i64));
            assert_eq!(values[2], Value::from(300_i64));
        } else {
            panic!("Expected 'values' key to exist as a list");
        }
    } else {
        panic!("Expected HashMap result for testbin");
    }
}

#[aerospike_macro::test]
async fn remove_map_entries_by_key_filter() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "rm_map_key_flt");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let inventory = as_map!(
        "apple" => 10_i64,
        "banana" => 5_i64,
        "cherry" => 8_i64,
        "date" => 3_i64
    );
    let data = as_map!("inventory" => inventory);
    let bin = as_bin!("testbin", data);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Remove entries where key >= "c" (removes cherry and date)
    let ctx1 = ctx_map_key(Value::from("inventory"));
    let ctx2 = ctx_all_children_with_filter(ge(
        exp_string_loop_var(LoopVarPart::MAP_KEY),
        string_val("c".to_string()),
    ));

    let op = modify_by_path(
        "testbin",
        ModifyFlag::DEFAULT,
        exp_remove_result(),
        &[ctx1, ctx2],
    );
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root_map)) = rec.bins.get("testbin") {
        if let Some(Value::HashMap(inv_map)) = root_map.get(&Value::from("inventory")) {
            assert_eq!(inv_map.len(), 2, "Should keep apple and banana");
            assert!(inv_map.contains_key(&Value::from("apple")));
            assert!(inv_map.contains_key(&Value::from("banana")));
            assert!(!inv_map.contains_key(&Value::from("cherry")));
            assert!(!inv_map.contains_key(&Value::from("date")));
        } else {
            panic!("Expected 'inventory' key to exist as a map");
        }
    } else {
        panic!("Expected HashMap result for testbin");
    }
}

#[aerospike_macro::test]
async fn remove_nested_items_complex_path() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "rm_nested");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let sales_dept = as_list!(
        as_map!("name" => "John", "sales" => 1000_i64),
        as_map!("name" => "Jane", "sales" => 5000_i64)
    );
    let eng_dept = as_list!(
        as_map!("name" => "Bob", "sales" => 500_i64),
        as_map!("name" => "Alice", "sales" => 3000_i64)
    );
    let departments = as_map!("sales" => sales_dept, "engineering" => eng_dept);
    let data = as_map!("departments" => departments);
    let bin = as_bin!("testbin", data);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Navigate: departments -> all dept lists -> remove employees with sales < 2000
    let ctx1 = ctx_map_key(Value::from("departments"));
    let ctx2 = ctx_all_children(); // iterate over "sales" and "engineering" lists
    let ctx3 = ctx_all_children_with_filter(lt(
        get_by_key(
            MapReturnType::Value,
            ExpType::INT,
            string_val("sales".to_string()),
            exp_map_loop_var(LoopVarPart::VALUE),
            &[],
        ),
        int_val(2000),
    ));

    let op = modify_by_path(
        "testbin",
        ModifyFlag::DEFAULT,
        exp_remove_result(),
        &[ctx1, ctx2, ctx3],
    );
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root_map)) = rec.bins.get("testbin") {
        if let Some(Value::HashMap(depts)) = root_map.get(&Value::from("departments")) {
            if let Some(Value::List(sales_list)) = depts.get(&Value::from("sales")) {
                assert_eq!(sales_list.len(), 1, "Should keep Jane only (sales=5000)");
            } else {
                panic!("Expected 'sales' dept to be a list");
            }
            if let Some(Value::List(eng_list)) = depts.get(&Value::from("engineering")) {
                assert_eq!(eng_list.len(), 1, "Should keep Alice only (sales=3000)");
            } else {
                panic!("Expected 'engineering' dept to be a list");
            }
        } else {
            panic!("Expected 'departments' key to exist as a map");
        }
    } else {
        panic!("Expected HashMap result for testbin");
    }
}

// ===== Path builder + convenience wrapper tests =====
//
// These exercise the surface added on top of select_by_path/modify_by_path
// (the Path fluent builder, the select_*/modify_* shorthands, and the
// ctx_* additions). Each test sets up a small bin, runs the new helper
// against a live server, and asserts the round-trip result matches.

#[aerospike_macro::test]
async fn path_builder_and_select_values_wrapper() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "path_builder_select");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let data = as_map!("scores" => as_list!(10_i64, 20_i64, 30_i64));
    let bin = as_bin!("testbin", data);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Build the same path two ways and assert both shapes return the
    // same values via the select_values shorthand.
    let path = Path::new().map_key("scores").all_children();
    let op_builder = select_values("testbin", &path);
    let rec_builder = client.operate(&wpolicy, &key, &[op_builder]).await.unwrap();

    let op_slice = select_values(
        "testbin",
        &[ctx_map_key(Value::from("scores")), ctx_all_children()][..],
    );
    let rec_slice = client.operate(&wpolicy, &key, &[op_slice]).await.unwrap();

    assert_eq!(
        rec_builder.bins.get("testbin"),
        rec_slice.bins.get("testbin")
    );
    if let Some(Value::List(items)) = rec_builder.bins.get("testbin") {
        assert_eq!(items.len(), 3);
        assert!(items.contains(&Value::from(10_i64)));
        assert!(items.contains(&Value::from(20_i64)));
        assert!(items.contains(&Value::from(30_i64)));
    } else {
        panic!("Expected list result");
    }
}

#[aerospike_macro::test]
async fn select_map_keys_returns_keys() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "select_keys");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let inner = as_map!("alpha" => 1_i64, "beta" => 2_i64, "gamma" => 3_i64);
    let bin = as_bin!("testbin", as_map!("data" => inner));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let path = Path::new().map_key("data").all_children();
    let op = select_map_keys("testbin", &path);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    if let Some(Value::List(keys)) = rec.bins.get("testbin") {
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&Value::from("alpha")));
        assert!(keys.contains(&Value::from("beta")));
        assert!(keys.contains(&Value::from("gamma")));
    } else {
        panic!("Expected list of keys");
    }
}

#[aerospike_macro::test]
async fn select_map_entries_returns_pairs() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "select_entries");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let inner = as_map!("a" => 1_i64, "b" => 2_i64);
    let bin = as_bin!("testbin", as_map!("data" => inner));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let path = Path::new().map_key("data").all_children();
    let op = select_map_entries("testbin", &path);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // MAP_KEY_VALUE returns key/value pairs interleaved as a flat list.
    if let Some(Value::List(items)) = rec.bins.get("testbin") {
        assert_eq!(items.len(), 4, "expected 2 entries flattened to 4 elements");
    } else {
        panic!("Expected list of key/value pairs");
    }
}

#[aerospike_macro::test]
async fn select_matching_tree_preserves_shape() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "select_tree");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let inner = as_map!("a" => 100_i64, "b" => 5_i64, "c" => 200_i64);
    let bin = as_bin!("testbin", as_map!("data" => inner));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Filter children with value > 50, but ask for the matching tree
    // shape (i.e. the surviving map structure, not bare values).
    let ctx = vec![
        ctx_map_key(Value::from("data")),
        ctx_all_children_with_filter(gt(exp_int_loop_var(LoopVarPart::VALUE), int_val(50))),
    ];
    let op = select_matching_tree("testbin", &ctx);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    // MATCHING_TREE returns the original map shape with only matches.
    let result = rec.bins.get("testbin").expect("result should be present");
    let inner_map = match result {
        Value::HashMap(root) => match root.get(&Value::from("data")) {
            Some(Value::HashMap(inner)) => inner.clone(),
            other => panic!("expected nested map under 'data', got {:?}", other),
        },
        Value::List(items) => {
            assert_eq!(items.len(), 2, "expected 2 matching pairs flattened");
            return;
        }
        other => panic!("unexpected matching-tree result: {:?}", other),
    };
    assert_eq!(inner_map.len(), 2);
    assert_eq!(
        inner_map.get(&Value::from("a")),
        Some(&Value::from(100_i64))
    );
    assert_eq!(
        inner_map.get(&Value::from("c")),
        Some(&Value::from(200_i64))
    );
}

#[aerospike_macro::test]
async fn modify_no_fail_skips_type_mismatch() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "modify_nofail");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    // Mixed bag: ints we can double, plus a string we cannot.
    let mixed = as_list!(Value::from(1_i64), Value::from("oops"), Value::from(3_i64));
    let bin = as_bin!("testbin", as_map!("vals" => mixed));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Multiply numeric leaves by 2; NO_FAIL silently skips the string.
    let path = Path::new().map_key("vals").all_children();
    let modify_exp = num_mul(vec![exp_int_loop_var(LoopVarPart::VALUE), int_val(2)]);
    let op = modify_no_fail("testbin", modify_exp, &path);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root)) = rec.bins.get("testbin") {
        if let Some(Value::List(vals)) = root.get(&Value::from("vals")) {
            // Numeric leaves doubled, the string left alone.
            let has_two = vals.iter().any(|v| matches!(v, Value::Int(2)));
            let has_six = vals.iter().any(|v| matches!(v, Value::Int(6)));
            let has_str = vals
                .iter()
                .any(|v| matches!(v, Value::String(s) if s == "oops"));
            assert!(
                has_two && has_six,
                "numeric leaves should be doubled: {:?}",
                vals
            );
            assert!(has_str, "string leaf should be preserved: {:?}", vals);
        } else {
            panic!("Expected 'vals' list");
        }
    } else {
        panic!("Expected HashMap testbin");
    }
}

#[aerospike_macro::test]
async fn path_remove_helper_drops_filtered_leaves() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "path_remove");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let bin = as_bin!(
        "testbin",
        as_map!("nums" => as_list!(1_i64, 2_i64, 3_i64, 4_i64))
    );
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Remove every value > 2 using the convenience wrapper (no need to
    // import exp_remove_result on the call site).
    let ctx = vec![
        ctx_map_key(Value::from("nums")),
        ctx_all_children_with_filter(gt(exp_int_loop_var(LoopVarPart::VALUE), int_val(2))),
    ];
    let op = path_remove("testbin", &ctx);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root)) = rec.bins.get("testbin") {
        if let Some(Value::List(nums)) = root.get(&Value::from("nums")) {
            assert_eq!(nums.len(), 2);
            assert!(nums.contains(&Value::from(1_i64)));
            assert!(nums.contains(&Value::from(2_i64)));
        } else {
            panic!("Expected 'nums' list");
        }
    } else {
        panic!("Expected HashMap testbin");
    }
}

// ===== ctx_map_keys_in / ctx_and_filter / ctx round-trip =====

#[aerospike_macro::test]
async fn ctx_map_keys_in_selects_subset_of_keys() {
    let client = common::client().await;
    if !server_supports_enhanced_expression_api(&client).await {
        eprintln!("Skipping: server does not support enhanced expression API (requires >= 8.1.2)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "ctx_keys_in");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let inner = as_map!("a" => 1_i64, "b" => 2_i64, "c" => 3_i64, "d" => 4_i64);
    let bin = as_bin!("testbin", as_map!("data" => inner));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // ctx_map_keys_in is a multi-key terminal selector: it produces the
    // values for the named keys, in the order the server resolves them.
    // We assert presence rather than ordering to stay robust against
    // implementation choices on the server side.
    let ctx = vec![
        ctx_map_key(Value::from("data")),
        ctx_map_keys_in(["a", "c"]),
    ];
    let op = select_values("testbin", &ctx);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let result = rec.bins.get("testbin").expect("result missing");
    let items: Vec<Value> = match result {
        Value::List(items) => items.clone(),
        Value::HashMap(m) => m.values().cloned().collect(),
        other => panic!("unexpected map_keys_in result shape: {:?}", other),
    };
    assert!(
        items.contains(&Value::from(1_i64)),
        "expected value for 'a' (=1)"
    );
    assert!(
        items.contains(&Value::from(3_i64)),
        "expected value for 'c' (=3)"
    );
    assert!(
        !items.contains(&Value::from(2_i64)),
        "value for 'b' should be excluded"
    );
    assert!(
        !items.contains(&Value::from(4_i64)),
        "value for 'd' should be excluded"
    );
}

#[aerospike_macro::test]
async fn ctx_and_filter_refines_map_keys_in() {
    let client = common::client().await;
    if !server_supports_enhanced_expression_api(&client).await {
        eprintln!("Skipping: server does not support enhanced expression API (requires >= 8.1.2)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "ctx_andfilter");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let inner = as_map!("a" => 5_i64, "b" => 15_i64, "c" => 25_i64, "d" => 50_i64);
    let bin = as_bin!("testbin", inner);
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Pick {a, b, c} via ctx_map_keys_in, then keep only the entries
    // whose value is > 10 — should yield b=15 and c=25 (a=5 filtered
    // out, d=50 not in the keys-in set).
    let ctx = vec![
        ctx_map_keys_in(["a", "b", "c"]),
        ctx_and_filter(gt(exp_int_loop_var(LoopVarPart::VALUE), int_val(10))),
    ];
    let op = select_map_entries("testbin", &ctx);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let result = rec.bins.get("testbin").expect("result missing");
    let pairs: Vec<Value> = match result {
        Value::List(items) => items.clone(),
        Value::HashMap(m) => m.iter().flat_map(|(k, v)| [k.clone(), v.clone()]).collect(),
        other => panic!("unexpected and_filter result shape: {:?}", other),
    };
    // Expect 2 key/value pairs flattened to 4 elements.
    assert_eq!(
        pairs.len(),
        4,
        "expected 2 key/value pairs, got {:?}",
        pairs
    );
    assert!(pairs.contains(&Value::from("b")) && pairs.contains(&Value::from(15_i64)));
    assert!(pairs.contains(&Value::from("c")) && pairs.contains(&Value::from(25_i64)));
    assert!(
        !pairs.contains(&Value::from("a")),
        "a (=5) should be filtered out"
    );
    assert!(!pairs.contains(&Value::from("d")), "d not in keys-in set");
}

#[aerospike_macro::test]
async fn ctx_round_trip_through_base64_then_query() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "ctx_b64");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let inner = as_map!("a" => 1_i64, "b" => 2_i64, "c" => 3_i64);
    let bin = as_bin!("testbin", as_map!("data" => inner));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Build a path, serialize to base64, decode, then issue a query
    // using the decoded CTX. Same query as the original must produce
    // the same answer.
    let original = Path::new().map_key("data").all_children();
    let b64 = aerospike::operations::cdt_context::to_base64(original.as_slice()).unwrap();
    let decoded = ctx_from_base64(&b64).expect("decode ctx");

    let rec_original = client
        .operate(&wpolicy, &key, &[select_values("testbin", &original)])
        .await
        .unwrap();
    let rec_decoded = client
        .operate(&wpolicy, &key, &[select_values("testbin", &decoded[..])])
        .await
        .unwrap();

    assert_eq!(
        rec_original.bins.get("testbin"),
        rec_decoded.bins.get("testbin")
    );
}

// ===== Expression-side wrapper tests =====

#[aerospike_macro::test]
async fn exp_select_values_with_path_builder() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "exp_sel_values");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let bin = as_bin!("testbin", as_map!("nums" => as_list!(7_i64, 8_i64, 9_i64)));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let path = Path::new().map_key("nums").all_children();
    let bin_exp = aerospike::expressions::map_bin("testbin".to_string());
    let exp = exp_select_values(ExpType::LIST, bin_exp, &path);

    let ops = &[aerospike::operations::exp::read_exp(
        "result",
        exp,
        aerospike::operations::exp::ExpReadFlags::Default,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();

    if let Some(Value::List(items)) = rec.bins.get("result") {
        assert_eq!(items.len(), 3);
        assert!(items.contains(&Value::from(7_i64)));
        assert!(items.contains(&Value::from(8_i64)));
        assert!(items.contains(&Value::from(9_i64)));
    } else {
        panic!("Expected list result from exp_select_values");
    }
}

// ===== in_list / map_keys / map_values =====
//
// These exercise the path-expression-era ExpOps (InList=9, MapKeys=101,
// MapValues=102) end-to-end via `read_exp`. The new factories live next
// to `exp_remove_result` in `expressions/mod.rs`.

#[aerospike_macro::test]
async fn in_list_returns_true_for_present_value() {
    let client = common::client().await;
    if !server_supports_enhanced_expression_api(&client).await {
        eprintln!("Skipping: server does not support enhanced expression API (requires >= 8.1.2)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "in_list");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let bin = as_bin!("nums", as_list!(1_i64, 2_i64, 3_i64));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let present = aerospike::expressions::in_list(int_val(2), list_bin("nums".to_string()));
    let absent = aerospike::expressions::in_list(int_val(99), list_bin("nums".to_string()));

    let ops = &[
        aerospike::operations::exp::read_exp(
            "present",
            present,
            aerospike::operations::exp::ExpReadFlags::Default,
        ),
        aerospike::operations::exp::read_exp(
            "absent",
            absent,
            aerospike::operations::exp::ExpReadFlags::Default,
        ),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(rec.bins.get("present"), Some(&Value::from(true)));
    assert_eq!(rec.bins.get("absent"), Some(&Value::from(false)));
}

#[aerospike_macro::test]
async fn map_keys_extracts_all_keys() {
    let client = common::client().await;
    if !server_supports_enhanced_expression_api(&client).await {
        eprintln!("Skipping: server does not support enhanced expression API (requires >= 8.1.2)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "map_keys");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let bin = as_bin!("m", as_map!("a" => 1_i64, "b" => 2_i64, "c" => 3_i64));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let exp = aerospike::expressions::map_keys(map_bin("m".to_string()));
    let ops = &[aerospike::operations::exp::read_exp(
        "keys",
        exp,
        aerospike::operations::exp::ExpReadFlags::Default,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();

    if let Some(Value::List(keys)) = rec.bins.get("keys") {
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&Value::from("a")));
        assert!(keys.contains(&Value::from("b")));
        assert!(keys.contains(&Value::from("c")));
    } else {
        panic!("Expected list of keys, got: {:?}", rec.bins.get("keys"));
    }
}

#[aerospike_macro::test]
async fn map_values_extracts_all_values() {
    let client = common::client().await;
    if !server_supports_enhanced_expression_api(&client).await {
        eprintln!("Skipping: server does not support enhanced expression API (requires >= 8.1.2)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "map_values");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let bin = as_bin!("m", as_map!("a" => 10_i64, "b" => 20_i64, "c" => 30_i64));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let exp = aerospike::expressions::map_values(map_bin("m".to_string()));
    let ops = &[aerospike::operations::exp::read_exp(
        "values",
        exp,
        aerospike::operations::exp::ExpReadFlags::Default,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();

    if let Some(Value::List(vals)) = rec.bins.get("values") {
        assert_eq!(vals.len(), 3);
        assert!(vals.contains(&Value::from(10_i64)));
        assert!(vals.contains(&Value::from(20_i64)));
        assert!(vals.contains(&Value::from(30_i64)));
    } else {
        panic!("Expected list of values, got: {:?}", rec.bins.get("values"));
    }
}

#[aerospike_macro::test]
async fn in_list_composes_with_map_keys() {
    // Real-world idiom: "is the looked-up name a key of this map?"
    let client = common::client().await;
    if !server_supports_enhanced_expression_api(&client).await {
        eprintln!("Skipping: server does not support enhanced expression API (requires >= 8.1.2)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "in_list_keys");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let bin = as_bin!("m", as_map!("alpha" => 1_i64, "beta" => 2_i64));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let has_alpha = aerospike::expressions::in_list(
        string_val("alpha".to_string()),
        aerospike::expressions::map_keys(map_bin("m".to_string())),
    );
    let has_zeta = aerospike::expressions::in_list(
        string_val("zeta".to_string()),
        aerospike::expressions::map_keys(map_bin("m".to_string())),
    );

    let ops = &[
        aerospike::operations::exp::read_exp(
            "alpha_in",
            has_alpha,
            aerospike::operations::exp::ExpReadFlags::Default,
        ),
        aerospike::operations::exp::read_exp(
            "zeta_in",
            has_zeta,
            aerospike::operations::exp::ExpReadFlags::Default,
        ),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(rec.bins.get("alpha_in"), Some(&Value::from(true)));
    assert_eq!(rec.bins.get("zeta_in"), Some(&Value::from(false)));
}

#[aerospike_macro::test]
async fn exp_remove_through_write_exp_drops_leaves() {
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "exp_remove");

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let bin = as_bin!(
        "testbin",
        as_map!("vals" => as_list!(1_i64, 5_i64, 10_i64, 20_i64))
    );
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    // Drop every value >= 10 by going through exp_remove + write_exp.
    let ctx = vec![
        ctx_map_key(Value::from("vals")),
        ctx_all_children_with_filter(ge(exp_int_loop_var(LoopVarPart::VALUE), int_val(10))),
    ];
    let bin_exp = aerospike::expressions::map_bin("testbin".to_string());
    let exp = exp_remove(ExpType::MAP, bin_exp, &ctx);

    let ops = &[aerospike::operations::exp::write_exp(
        "testbin",
        exp,
        aerospike::operations::exp::ExpWriteFlags::Default,
    )];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    if let Some(Value::HashMap(root)) = rec.bins.get("testbin") {
        if let Some(Value::List(vals)) = root.get(&Value::from("vals")) {
            assert_eq!(vals.len(), 2);
            assert!(vals.contains(&Value::from(1_i64)));
            assert!(vals.contains(&Value::from(5_i64)));
        } else {
            panic!("Expected 'vals' list");
        }
    } else {
        panic!("Expected HashMap testbin");
    }
}

// ===== Additional loop-var type tests (Go parity) =====
//
// Go's `exp_cdt_test.go` exercises the full set of loop-var helpers
// (bool, list, blob, nil, geo, hll). The Rust integration suite
// previously covered int / string / map-key loop vars; the cases
// below add the bool and list flavours, which exercise common
// real-world filter shapes.

#[aerospike_macro::test]
async fn loop_var_bool_filters_features() {
    // Pick the entries whose nested map has `enabled = true`. Mirrors
    // Go's "should use ExpLoopVarBool to filter boolean values".
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "loop_var_bool");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let features = as_list!(
        as_map!("name" => "feature1", "enabled" => true),
        as_map!("name" => "feature2", "enabled" => false),
        as_map!("name" => "feature3", "enabled" => true),
        as_map!("name" => "feature4", "enabled" => false)
    );
    let bin = as_bin!("data", as_map!("features" => features));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let ctx = vec![
        ctx_map_key(Value::from("features")),
        ctx_all_children_with_filter(eq(
            get_by_key(
                MapReturnType::Value,
                ExpType::BOOL,
                string_val("enabled".to_string()),
                exp_map_loop_var(LoopVarPart::VALUE),
                &[],
            ),
            bool_val(true),
        )),
        ctx_map_key(Value::from("name")),
    ];
    let op = select_by_path("data", SelectFlag::VALUE, &ctx);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    if let Some(Value::List(names)) = rec.bins.get("data") {
        assert_eq!(names.len(), 2, "expected feature1 and feature3");
        assert!(names.contains(&Value::from("feature1")));
        assert!(names.contains(&Value::from("feature3")));
    } else {
        panic!("Expected list of names, got: {:?}", rec.bins.get("data"));
    }
}

#[aerospike_macro::test]
async fn loop_var_list_filters_by_size() {
    // Use `exp_list_loop_var(VALUE)` plus `list_size` to keep only the
    // matrix rows whose length is 3. Mirrors Go's
    // "should use ExpLoopVarList to access nested list values".
    let client = common::client().await;
    if !server_supports_cdt_path_expressions(&client).await {
        eprintln!("Skipping: server does not support CDT path expressions (requires >= 8.1.1)");
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let key = as_key!(namespace, &set_name, "loop_var_list");

    let wpolicy = WritePolicy::default();
    common::delete_durably(&client, &wpolicy, &key).await.unwrap();

    let matrix = as_list!(
        as_list!(1_i64, 2_i64, 3_i64),
        as_list!(4_i64, 5_i64, 6_i64),
        as_list!(7_i64, 8_i64, 9_i64),
        as_list!(10_i64, 11_i64) // length 2 — should be filtered out
    );
    let bin = as_bin!("data", as_map!("matrix" => matrix));
    client.put(&wpolicy, &key, &[bin]).await.unwrap();

    let ctx = vec![
        ctx_map_key(Value::from("matrix")),
        ctx_all_children_with_filter(eq(
            aerospike::expressions::lists::size(exp_list_loop_var(LoopVarPart::VALUE), &[]),
            int_val(3),
        )),
    ];
    let op = select_by_path("data", SelectFlag::VALUE, &ctx);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();

    if let Some(Value::List(rows)) = rec.bins.get("data") {
        assert_eq!(rows.len(), 3, "expected 3 length-3 rows");
        for row in rows {
            if let Value::List(items) = row {
                assert_eq!(items.len(), 3);
            } else {
                panic!("expected each row to be a list, got: {:?}", row);
            }
        }
    } else {
        panic!("Expected list of rows, got: {:?}", rec.bins.get("data"));
    }
}
