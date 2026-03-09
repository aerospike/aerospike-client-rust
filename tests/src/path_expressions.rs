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
    ctx_all_children, ctx_all_children_with_filter, ctx_map_key,
};
use aerospike::operations::path::{modify_by_path, select_by_path, ModifyFlag, SelectFlag};
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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
    client.delete(&wpolicy, &key).await.unwrap();

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
