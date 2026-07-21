//! Stream UDF aggregation: run a map/reduce Lua UDF over a query, with the
//! server computing per-node partials and the client combining them in an
//! embedded Lua interpreter (requires the `lua` cargo feature).
//!
//! Port of the Java client's `QueryAverage`/`QuerySum` aggregation examples
//! and the Go client's `query-aggregate` examples.

use std::env;

use aerospike::{as_bin, as_key, as_val};
use aerospike::{
    AdminPolicy, Bins, Client, ClientPolicy, QueryPolicy, Statement, Task, UDFLang, Value,
    WritePolicy,
};
use futures::StreamExt;

/// The same source runs on both sides: the server executes the
/// server-scope operations (`map`, `aggregate`), the client executes the
/// rest (from `reduce` onward).
const UDF: &str = r"
function sum_single_bin(s, bin_name)
    local function mapper(rec)
        return rec[bin_name]
    end
    local function reducer(v1, v2)
        return v1 + v2
    end
    return s : map(mapper) : reduce(reducer)
end

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

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = std::env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let namespace = "test";
    let set_name = "aggregate_example";

    // ---- Register the UDF on the server AND make the source available to
    // the client-side Lua runtime (in memory; a `.lua` file in the
    // directory set by `aerospike::lua::set_lua_path` works too) ----
    let task = client
        .register_udf(
            &AdminPolicy::default(),
            UDF.as_bytes(),
            "example_aggregate.lua",
            UDFLang::Lua,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();
    aerospike::lua::register_package("example_aggregate", UDF);
    println!("registered example_aggregate.lua");

    // ---- Write some records ----
    let wpolicy = WritePolicy::default();
    let count = 100i64;
    for i in 1..=count {
        let key = as_key!(namespace, set_name, i);
        client
            .put(&wpolicy, &key, &[as_bin!("score", i)])
            .await
            .unwrap();
    }
    println!("wrote {count} records");

    // ---- Sum the `score` bin across the whole set ----
    let stmt = Statement::new(namespace, set_name, Bins::All);
    let rs = client
        .query_aggregate(
            &QueryPolicy::default(),
            stmt,
            "example_aggregate",
            "sum_single_bin",
            Some(&[as_val!("score")]),
        )
        .await
        .unwrap();
    let mut stream = rs.into_stream();
    while let Some(result) = stream.next().await {
        let value = result.unwrap();
        println!("sum(score) = {value}");
        assert_eq!(numeric(&value), (count * (count + 1) / 2) as f64);
    }

    // ---- Average via aggregate + reduce + final map ----
    let stmt = Statement::new(namespace, set_name, Bins::All);
    let rs = client
        .query_aggregate(
            &QueryPolicy::default(),
            stmt,
            "example_aggregate",
            "average",
            Some(&[as_val!("score")]),
        )
        .await
        .unwrap();
    let mut stream = rs.into_stream();
    while let Some(result) = stream.next().await {
        let value = result.unwrap();
        println!("avg(score) = {value}");
        assert_eq!(numeric(&value), 50.5);
    }

    client.close().await.unwrap();
    println!("query_aggregate example finished");
}

fn numeric(value: &Value) -> f64 {
    match value {
        Value::Int(i) => *i as f64,
        Value::Float(f) => f.into(),
        other => panic!("expected numeric value, got {other:?}"),
    }
}
