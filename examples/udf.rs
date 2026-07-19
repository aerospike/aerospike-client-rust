//! User-defined functions (UDFs): register, execute on a record, and run a
//! background UDF over a whole set.
//!
//! Ports of the Java client's `AsyncUserDefinedFunction` and `QueryExecute`
//! examples.

use std::env;

use aerospike::{as_bin, as_key, as_val};
use aerospike::{
    AdminPolicy, Bins, Client, ClientPolicy, ReadPolicy, Statement, Task, UDFLang, Value,
    WritePolicy,
};

const UDF: &str = r#"
function double_bin(rec, name)
    rec[name] = rec[name] * 2
    aerospike:update(rec)
end

function echo(rec, val)
    return val
end
"#;

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let apolicy = AdminPolicy::default();
    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();

    // ---- Register the UDF module and wait for cluster-wide distribution ----
    let task = client
        .register_udf(&apolicy, UDF.as_bytes(), "example_udf.lua", UDFLang::Lua)
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();
    println!("registered example_udf.lua");

    // ---- Execute a UDF against a single record ----
    let key = as_key!("test", "udf_demo", "record-1");
    client.put(&wpolicy, &key, &[as_bin!("n", 21)]).await.unwrap();

    client
        .execute_udf(&wpolicy, &key, "example_udf", "double_bin", Some(&[as_val!("n")]))
        .await
        .unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    println!("double_bin(21) => {:?}", rec.bins.get("n"));
    assert_eq!(rec.bins.get("n"), Some(&Value::Int(42)));

    // A UDF can also return a value directly.
    let echoed = client
        .execute_udf(&wpolicy, &key, "example_udf", "echo", Some(&[as_val!("pong")]))
        .await
        .unwrap();
    println!("echo => {echoed:?}");

    // ---- Background UDF over a whole set (server-side, no data returned) ----
    let set_name = "udf_demo_bg";
    for i in 0..10i64 {
        let key = as_key!("test", set_name, i);
        client.put(&wpolicy, &key, &[as_bin!("n", i)]).await.unwrap();
    }

    let stmt = Statement::new("test", set_name, Bins::All);
    let task = client
        .query_execute_udf(&wpolicy, stmt, "example_udf", "double_bin", Some(&[as_val!("n")]))
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();
    println!("background UDF applied to every record of {set_name}");

    let rec = client
        .get(&rpolicy, &as_key!("test", set_name, 7i64), Bins::All)
        .await
        .unwrap();
    println!("record 7 after background double: {:?}", rec.bins.get("n"));
    assert_eq!(rec.bins.get("n"), Some(&Value::Int(14)));

    // ---- Cleanup ----
    let task = client.remove_udf(&apolicy, "example_udf.lua").await.unwrap();
    let _ = task.wait_till_complete(None).await;
    for i in 0..10i64 {
        let _ = client.delete(&wpolicy, &as_key!("test", set_name, i)).await;
    }
    let _ = client.delete(&wpolicy, &key).await;
    client.close().await.unwrap();
}
