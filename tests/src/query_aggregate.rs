// Copyright 2015-2026 Aerospike, Inc.
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

//! Stream UDF aggregation tests (`lua` feature). Ports of the Go client's
//! `query_aggregate_test.go`: the server runs the server-scope portion of
//! the stream UDF and the client combines the per-node partials in its
//! embedded Lua interpreter.

use crate::common;

use aerospike::Task;
use aerospike::*;
use futures::StreamExt;

const SUM_UDF: &str = r"
function sum_single_bin(s, bin_name)
    local function mapper(rec)
        return rec[bin_name]
    end
    local function reducer(v1, v2)
        return v1 + v2
    end
    return s : map(mapper) : reduce(reducer)
end
";

const AVERAGE_UDF: &str = r"
function average(s, bin_name)
    local function aggregate_stats(agg, rec)
        agg['sum'] = agg['sum'] + rec[bin_name]
        agg['count'] = agg['count'] + 1
        return agg
    end
    local function reduce_stats(a, b)
        local out = map()
        out['sum'] = a['sum'] + b['sum']
        out['count'] = a['count'] + b['count']
        return out
    end
    local function div(a)
        return a['sum'] / a['count']
    end
    return s : aggregate(map{sum = 0, count = 0}, aggregate_stats)
             : reduce(reduce_stats)
             : map(div)
end
";

/// Register the UDF on the server and make the same source available to
/// the client-side Lua runtime.
async fn register_udf(client: &Client, package: &str, source: &str) {
    let task = client
        .register_udf(
            &AdminPolicy::default(),
            source.as_bytes(),
            &format!("{package}.lua"),
            UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();
    aerospike::lua::register_package(package, source);
}

async fn fill_set(client: &Client, namespace: &str, set_name: &str, count: i64) {
    let wpolicy = WritePolicy::default();
    for i in 1..=count {
        let key = as_key!(namespace, set_name, i);
        let bin = as_bin!("bin1", i);
        client.put(&wpolicy, &key, &[bin]).await.unwrap();
    }
}

async fn collect(rs: std::sync::Arc<ResultSet>) -> Vec<Value> {
    let mut stream = rs.into_stream();
    let mut out = Vec::new();
    while let Some(value) = stream.next().await {
        out.push(value.unwrap());
    }
    out
}

fn as_f64(value: &Value) -> f64 {
    match value {
        Value::Int(i) => *i as f64,
        Value::Float(f) => f.into(),
        other => panic!("expected a numeric aggregation result, got {other:?}"),
    }
}

#[aerospike_macro::test]
async fn query_aggregate_sum() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    const COUNT: i64 = 100;

    register_udf(&client, "test_agg_sum", SUM_UDF).await;
    fill_set(&client, namespace, set_name, COUNT).await;

    let stmt = Statement::new(namespace, set_name, Bins::All);
    let rs = client
        .query_aggregate(
            &QueryPolicy::default(),
            stmt,
            "test_agg_sum",
            "sum_single_bin",
            Some(&[as_val!("bin1")]),
        )
        .await
        .unwrap();

    let results = collect(rs).await;
    assert_eq!(results.len(), 1, "one aggregated value expected");
    let expected = (COUNT * (COUNT + 1) / 2) as f64;
    assert!((as_f64(&results[0]) - expected).abs() < f64::EPSILON);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_aggregate_average() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    const COUNT: i64 = 100;

    register_udf(&client, "test_agg_avg", AVERAGE_UDF).await;
    fill_set(&client, namespace, set_name, COUNT).await;

    let stmt = Statement::new(namespace, set_name, Bins::All);
    let rs = client
        .query_aggregate(
            &QueryPolicy::default(),
            stmt,
            "test_agg_avg",
            "average",
            Some(&[as_val!("bin1")]),
        )
        .await
        .unwrap();

    let results = collect(rs).await;
    assert_eq!(results.len(), 1, "one aggregated value expected");
    // average of 1..=100
    assert!((as_f64(&results[0]) - 50.5).abs() < f64::EPSILON);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_aggregate_empty_set_yields_no_values() {
    let client = common::client().await;
    let namespace = common::namespace();
    // Random set name that contains no records.
    let set_name = &common::rand_str(10);

    register_udf(&client, "test_agg_empty", SUM_UDF).await;

    let stmt = Statement::new(namespace, set_name, Bins::All);
    let rs = client
        .query_aggregate(
            &QueryPolicy::default(),
            stmt,
            "test_agg_empty",
            "sum_single_bin",
            Some(&[as_val!("bin1")]),
        )
        .await
        .unwrap();

    let results = collect(rs).await;
    assert!(results.is_empty(), "no values expected, got {results:?}");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_aggregate_loads_package_from_lua_path() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    const COUNT: i64 = 10;

    // Server side under a package name that is NOT registered in memory,
    // so the client must load it from the filesystem.
    let task = client
        .register_udf(
            &AdminPolicy::default(),
            SUM_UDF.as_bytes(),
            "test_agg_file.lua",
            UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    let dir = std::env::temp_dir().join("aerospike_rust_lua_test");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("test_agg_file.lua"), SUM_UDF).unwrap();
    aerospike::lua::set_lua_path(&dir);

    fill_set(&client, namespace, set_name, COUNT).await;

    let stmt = Statement::new(namespace, set_name, Bins::All);
    let rs = client
        .query_aggregate(
            &QueryPolicy::default(),
            stmt,
            "test_agg_file",
            "sum_single_bin",
            Some(&[as_val!("bin1")]),
        )
        .await
        .unwrap();

    let results = collect(rs).await;
    assert_eq!(results.len(), 1);
    assert!((as_f64(&results[0]) - 55.0).abs() < f64::EPSILON);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_aggregate_missing_client_package_errors() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    // Registered on the server, but no client-side source anywhere.
    let task = client
        .register_udf(
            &AdminPolicy::default(),
            SUM_UDF.as_bytes(),
            "test_agg_missing.lua",
            UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    fill_set(&client, namespace, set_name, 3).await;

    let stmt = Statement::new(namespace, set_name, Bins::All);
    let rs = client
        .query_aggregate(
            &QueryPolicy::default(),
            stmt,
            "test_agg_missing",
            "sum_single_bin",
            Some(&[as_val!("bin1")]),
        )
        .await
        .unwrap();

    let mut stream = rs.into_stream();
    let mut saw_error = false;
    while let Some(result) = stream.next().await {
        if let Err(err) = result {
            saw_error = true;
            assert!(err.to_string().contains("test_agg_missing"), "{err}");
        }
    }
    assert!(saw_error, "expected the missing package to surface an error");

    client.close().await.unwrap();
}
