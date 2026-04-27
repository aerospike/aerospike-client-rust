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

use aerospike::query::Filter;
use aerospike::Task;
use aerospike::*;
use aerospike_rt::time::Duration;

#[aerospike_macro::test]
async fn execute_udf() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let apolicy = AdminPolicy::default();
    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let wbin = as_bin!("bin", 10);
    let bins = vec![wbin];
    client.put(&wpolicy, &key, &bins).await.unwrap();

    let udf_body1 = r#"
function func_div(rec, div)
  local ret = map()
  local x = rec['bin']
  rec['bin2'] = math.floor(x / div)
  aerospike:update(rec)
  ret['status'] = 'OK'
  ret['res'] = math.floor(x / div)
  return ret
end
"#;

    let udf_body2 = r#"
function echo(rec, val)
  return val
end
"#;

    let task = client
        .register_udf(
            &apolicy,
            udf_body1.as_bytes(),
            "test_udf1.lua",
            UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    let task = client
        .register_udf(
            &apolicy,
            udf_body2.as_bytes(),
            "test_udf2.lua",
            UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    let res = client
        .execute_udf(
            &wpolicy,
            &key,
            "test_udf2",
            "echo",
            Some(&[as_val!("ha ha...")]),
        )
        .await;
    assert_eq!(Some(as_val!("ha ha...")), res.unwrap());

    let res = client
        .execute_udf(&wpolicy, &key, "test_udf1", "func_div", Some(&[as_val!(2)]))
        .await;
    if let Ok(Some(Value::HashMap(values))) = res {
        assert_eq!(values.get(&as_val!("status")), Some(&as_val!("OK")));
        assert_eq!(values.get(&as_val!("res")), Some(&as_val!(5)));
    } else {
        panic!("UDF function did not return expected value");
    }

    let res = client
        .execute_udf(&wpolicy, &key, "test_udf1", "no_such_function", None)
        .await;
    if let Err(Error::UdfBadResponse(response)) = res {
        assert_eq!(response, "function not found".to_string());
    } else {
        panic!("UDF function did not return the expected error");
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_execute_udf_with_filter() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let apolicy = AdminPolicy::default();
    let wpolicy = WritePolicy::default();

    // Create test records
    for i in 0..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        let bins = vec![as_bin!("bin", i)];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    // Create index for filter query
    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            &set_name,
            "bin",
            &format!("{}_{}_{}", namespace, set_name, "bin"),
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create index");
    task.wait_till_complete(None).await.unwrap();

    // Register a UDF that doubles the bin value
    let udf_body = r#"
function double_bin(rec)
  rec['bin'] = rec['bin'] * 2
  aerospike:update(rec)
end
"#;
    let task = client
        .register_udf(
            &apolicy,
            udf_body.as_bytes(),
            "test_bg_udf.lua",
            UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    // Apply UDF to records in range [0, 9] using a filter
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::range("bin", 0, 9));
    let task = client
        .query_execute_udf(&wpolicy, statement, "test_bg_udf", "double_bin", None)
        .await
        .expect("query_execute_udf failed");
    task.wait_till_complete(Some(Duration::from_secs(30)))
        .await
        .expect("task did not complete");

    // Verify the affected records were doubled
    let rpolicy = ReadPolicy::default();
    for i in 0..10_i64 {
        let key = as_key!(namespace, &set_name, i);
        let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
        let val: i64 = rec.bins["bin"].clone().into();
        assert_eq!(val, i * 2, "record {i} was not doubled");
    }

    // Verify records outside the filter were NOT modified
    for i in 10..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
        let val: i64 = rec.bins["bin"].clone().into();
        assert_eq!(val, i, "record {i} should not have been modified");
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_execute_udf_scan_all() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let apolicy = AdminPolicy::default();
    let wpolicy = WritePolicy::default();

    // Create test records
    for i in 0..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        let bins = vec![as_bin!("bin", i)];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    // Register a UDF that adds a new bin
    let udf_body = r#"
function add_marker(rec)
  rec['marker'] = 'tagged'
  aerospike:update(rec)
end
"#;
    let task = client
        .register_udf(
            &apolicy,
            udf_body.as_bytes(),
            "test_bg_udf2.lua",
            UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    // Apply UDF without filter (scan mode) to all records
    let statement = Statement::new(namespace, &set_name, Bins::All);
    let task = client
        .query_execute_udf(&wpolicy, statement, "test_bg_udf2", "add_marker", None)
        .await
        .expect("query_execute_udf scan failed");
    task.wait_till_complete(Some(Duration::from_secs(30)))
        .await
        .expect("task did not complete");

    // Verify all records have the marker bin
    let rpolicy = ReadPolicy::default();
    for i in 0..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
        assert_eq!(
            rec.bins["marker"],
            as_val!("tagged"),
            "record {i} missing marker"
        );
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_execute_udf_with_args() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let apolicy = AdminPolicy::default();
    let wpolicy = WritePolicy::default();

    // Create test records
    for i in 0..20_i64 {
        let key = as_key!(namespace, &set_name, i);
        let bins = vec![as_bin!("bin", i)];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    // Register a UDF that adds a value to the bin
    let udf_body = r#"
function add_val(rec, val)
  rec['bin'] = rec['bin'] + val
  aerospike:update(rec)
end
"#;
    let task = client
        .register_udf(
            &apolicy,
            udf_body.as_bytes(),
            "test_bg_udf3.lua",
            UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    // Apply UDF with arguments (scan mode)
    let statement = Statement::new(namespace, &set_name, Bins::All);
    let task = client
        .query_execute_udf(
            &wpolicy,
            statement,
            "test_bg_udf3",
            "add_val",
            Some(&[as_val!(100)]),
        )
        .await
        .expect("query_execute_udf with args failed");
    task.wait_till_complete(Some(Duration::from_secs(30)))
        .await
        .expect("task did not complete");

    // Verify all records had 100 added
    let rpolicy = ReadPolicy::default();
    for i in 0..20_i64 {
        let key = as_key!(namespace, &set_name, i);
        let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
        let val: i64 = rec.bins["bin"].clone().into();
        assert_eq!(val, i + 100, "record {i} not updated correctly");
    }

    client.close().await.unwrap();
}
