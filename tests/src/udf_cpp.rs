// Copyright 2015-2026 Aerospike, Inc.
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

//! WASM UDFs written in C++, compiled freestanding by clang over the C++ header, whose UDF_* macros
//! derive the exports and signatures from a function's parameter types. `counter_cpp.wasm` drives the
//! generated argument decoding through every rejection, and `udf_cpp.wasm`
//! (`aerospike-udf-sdk-wasm-cpp/examples/udf`) is the SDK's fixture, one function per corner of the record
//! API, held here to the Rust fixture tests' expectations. The guard, resolver, counter and CRDT modules
//! sit in the agreement tests of `events.rs` and `xdr_merge.rs`. The fixtures are built as
//! `aerospike-udf-sdk-wasm-cpp/README.md` says.

use crate::common;
use aerospike::Task;
use aerospike::*;
use std::collections::HashMap;

const COUNTER: &[u8] = include_bytes!("../fixtures/wasm/counter_cpp.wasm");
const FIXTURE: &[u8] = include_bytes!("../fixtures/wasm/udf_cpp.wasm");

/// The entries of a map value, whichever map flavour the client decoded it as.
fn map_entries(value: &Value) -> Vec<(Value, Value)> {
    match value {
        Value::HashMap(map) => map.clone().into_iter().collect(),
        Value::OrderedMap(map) => map.clone().into_iter().collect(),
        Value::SortedMap(map) => map.clone().into_iter().collect(),
        _ => Vec::new(),
    }
}

/// The value under a string key in a map value.
fn entry<'a>(pairs: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    pairs
        .iter()
        .find(|(k, _)| matches!(k, Value::String(s) if s == key))
        .map(|(_, v)| v)
}

fn skipped(test: &str, reason: &str) {
    eprintln!("skipped {test}: {reason}");
}

async fn register(client: &Client, module: &[u8], package: &str) -> std::result::Result<(), String> {
    match client
        .register_udf(&AdminPolicy::default(), module, &format!("{package}.wasm"), UdfLang::Wasm)
        .await
    {
        Ok(task) => task
            .wait_till_complete(None)
            .await
            .map(|_| ())
            .map_err(|e| format!("the module did not become available: {e}")),
        Err(e) => {
            let detail = format!("{e:?}");

            if detail.contains("invalid_udf_type") {
                Err("this server does not support WASM UDFs".to_string())
            } else {
                panic!("registering the module failed for a reason other than WASM support: {e:?}");
            }
        }
    }
}

/// The exports UDF_FUNCTION derives at compile time: the signature comes from the C++ parameter types (a
/// `Text<32>` is `str[32]`, as a Rust `Str<32>` is), the arguments are decoded into the parameters and
/// checked before the function runs, the return value is the result, and a returned Error fails the
/// call with its message.
#[aerospike_macro::test]
async fn cpp_generated_exports_decode_arguments() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("cpp_counter_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);

    if let Err(reason) = register(&client, COUNTER, package).await {
        skipped("cpp_generated_exports_decode_arguments", &reason);
        return;
    }

    let write = WritePolicy::default();

    // `UDF_FUNCTION(increment, Result<int64_t>, Text<32> site, int64_t n)` describes itself from its types.
    assert_eq!(
        client.execute_udf(&write, &key, package, "__udf_sig_increment", &[]).await.unwrap(),
        Some(Value::from("fn increment(site: str[32], n: int) -> value"))
    );
    assert_eq!(
        client.execute_udf(&write, &key, package, "__udf_sig_value", &[]).await.unwrap(),
        Some(Value::from("fn value() -> value"))
    );

    // A matching call: the arguments reach the method typed, and its return is the result.
    assert_eq!(
        client
            .execute_udf(&write, &key, package, "increment", &[Value::from("dc1"), Value::from(3)])
            .await
            .unwrap(),
        Some(Value::Int(3))
    );
    assert_eq!(client.execute_udf(&write, &key, package, "value", &[]).await.unwrap(), Some(Value::Int(3)));

    // Rejected before the method runs, naming the argument, the way the Rust macro does.
    let rejections = [
        (vec![Value::from("dc1")], "increment: argument 2 (n) is missing"),
        (vec![Value::from("dc1"), Value::from("x")], "increment: argument 2 (n) is not an int"),
        (vec![Value::from(1), Value::from(2)], "increment: argument 1 (site) is not a string"),
        (
            vec![Value::from("dc1"), Value::from(1), Value::from(2)],
            "increment takes at most 2 arguments, got 3",
        ),
        (vec![Value::from(1)], "value takes at most 0 arguments, got 1"),
    ];

    for (args, want) in rejections {
        let function = if want.starts_with("value") { "value" } else { "increment" };
        let text = client
            .execute_udf(&write, &key, package, function, &args)
            .await
            .expect_err("a call that does not fit the parameters must be rejected")
            .to_string();

        assert!(text.contains(want), "{function}({args:?}): {text}");
    }

    // A string longer than the Text<32> parameter is rejected before the body, as a Rust Str<32> is.
    let text = client
        .execute_udf(&write, &key, package, "increment", &[Value::from("s".repeat(40)), Value::from(1)])
        .await
        .expect_err("a too-long string must be rejected")
        .to_string();

    assert!(text.contains("increment: argument 1 (site) is 40 bytes, at most 32"), "{text}");

    // None of that touched the record.
    assert_eq!(client.execute_udf(&write, &key, package, "value", &[]).await.unwrap(), Some(Value::Int(3)));

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// The record API of the C++ SDK, driven through its fixture module (`aerospike-udf-sdk-wasm-cpp/examples/udf`):
/// range-for over lists and maps, the typed bin readers, the bool, nil, GeoJSON and packed-list writes,
/// integer map keys, a GeoJSON result, the record's metadata, its TTL and its deletion. The expectations
/// are those of the Rust fixture's tests in `udf_wasm.rs`.
#[aerospike_macro::test]
async fn cpp_sdk_covers_the_rust_surface() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("cpp_fixture_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);
    let write = WritePolicy::default();

    let m = Value::from(HashMap::from([(Value::from("a"), Value::from(1)), (Value::from("b"), Value::from(2))]));
    let doc = Value::from(vec![
        Value::from(10),
        Value::from("x"),
        Value::from(HashMap::from([(Value::from("k"), Value::from(1))])),
    ]);

    client
        .put(
            &write,
            &key,
            &[
                as_bin!("l", Value::from(vec![Value::from(1), Value::from(2), Value::from(3)])),
                as_bin!("i", 7),
                as_bin!("d", 2.5),
                as_bin!("s", "seven"),
                as_bin!("b", true),
                as_bin!("m", m),
                as_bin!("doc", doc),
            ],
        )
        .await
        .unwrap();

    if let Err(reason) = register(&client, FIXTURE, package).await {
        skipped("cpp_sdk_covers_the_rust_surface", &reason);
        return;
    }

    let call = |function: &'static str, args: Vec<Value>| {
        let client = client.clone();
        let key = key.clone();
        let package = package.clone();
        async move { client.execute_udf(&WritePolicy::default(), &key, &package, function, &args).await }
    };

    // A list walked with a cursor.
    assert_eq!(call("sum_list", vec![]).await.unwrap(), Some(Value::Int(6)));

    // The typed readers, and a map walked entry by entry.
    let typed = call("typed", vec![]).await.unwrap().expect("typed returns a map");
    let typed = map_entries(&typed);

    assert_eq!(entry(&typed, "i"), Some(&Value::Int(7)));
    assert_eq!(entry(&typed, "d"), Some(&Value::from(2.5f64)));
    assert_eq!(entry(&typed, "s"), Some(&Value::from("seven")));
    assert_eq!(entry(&typed, "b"), Some(&Value::Bool(true)));
    assert_eq!(entry(&typed, "msum"), Some(&Value::Int(3)));

    let mut keys = match entry(&typed, "keys") {
        Some(Value::List(keys)) => keys.clone(),
        other => panic!("keys should be a list: {other:?}"),
    };
    keys.sort_by_key(|k| k.to_string());
    assert_eq!(keys, vec![Value::from("a"), Value::from("b")]);

    // The record's metadata.
    let described = call("describe", vec![]).await.unwrap().expect("describe returns a map");
    let described = map_entries(&described);

    assert_eq!(entry(&described, "generation"), Some(&Value::Int(1)));
    assert_eq!(entry(&described, "bins"), Some(&Value::Int(7)));
    assert_eq!(entry(&described, "set"), Some(&Value::from(set_name.as_str())));
    assert!(matches!(entry(&described, "size"), Some(Value::Int(n)) if *n > 0), "{:?}", entry(&described, "size"));
    assert!(matches!(entry(&described, "lut"), Some(Value::Int(n)) if *n > 0), "{:?}", entry(&described, "lut"));

    // The writes the typed natives do not cover.
    assert_eq!(call("write_lists", vec![]).await.unwrap(), Some(Value::Int(1)));

    let record = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();

    assert_eq!(
        record.bins.get("w"),
        Some(&Value::List(vec![Value::Int(5), Value::Int(-1), Value::Int(1 << 40)])),
        "the integer list"
    );
    assert_eq!(record.bins.get("wd"), Some(&Value::List(vec![Value::from(0.5f64), Value::from(-4.0f64)])));
    assert_eq!(record.bins.get("flag"), Some(&Value::Bool(true)));
    assert_eq!(record.bins.get("gone"), None, "a nil write is no bin");
    assert_eq!(
        record.bins.get("geo"),
        Some(&Value::GeoJson("{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}".to_string())),
        "the GeoJSON kept its kind"
    );

    // Stored values into builders by position, a guest GeoJSON, an integer map key.
    let relisted = call("relist", vec![Value::from(1)]).await.unwrap();
    let relisted = match relisted {
        Some(Value::List(items)) => items,
        other => panic!("relist returns a list: {other:?}"),
    };

    assert_eq!(relisted.len(), 3);
    assert_eq!(relisted[0], Value::from("x"), "doc[1] by position");
    assert_eq!(
        relisted[1],
        Value::GeoJson("{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}".to_string()),
        "a guest GeoJSON stays a GeoJSON"
    );

    let inner = map_entries(&relisted[2]);

    assert_eq!(entry(&inner, "first"), Some(&Value::Int(10)), "doc[0] by position");
    assert_eq!(
        inner.iter().find(|(k, _)| *k == Value::Int(1)).map(|(_, v)| v),
        Some(&Value::from("one")),
        "the entry under the integer key"
    );

    // A GeoJSON read and written back as the kind it is, and one returned from the guest.
    assert_eq!(call("geo_roundtrip", vec![Value::from("geo")]).await.unwrap(), Some(Value::Int(40)));
    assert_eq!(
        call("geo_result", vec![]).await.unwrap(),
        Some(Value::GeoJson("{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}".to_string()))
    );

    // The TTL, where the namespace allows one without nsup.
    match call("touch_ttl", vec![Value::from(300)]).await {
        Ok(result) => {
            assert_eq!(result, Some(Value::Int(1)));

            let record = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();

            assert_eq!(record.bins.get("touched"), Some(&Value::Int(1)));
            assert!(record.time_to_live().is_some(), "the record now expires");
        }
        Err(e) => eprintln!("touch_ttl: the namespace refused a TTL ({e}); skipping that check"),
    }

    // And the record's deletion.
    assert_eq!(call("remove_me", vec![]).await.unwrap(), Some(Value::Int(1)));
    assert!(!client.exists(&ReadPolicy::default(), &key).await.unwrap(), "the record is gone");

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}
