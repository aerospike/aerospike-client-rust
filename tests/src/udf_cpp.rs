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
//! (`aerospike-udf-sdk/wasm-cpp/examples/udf`) is the SDK's fixture, one function per corner of the record
//! API, held here to the Rust fixture tests' expectations. The resolver, counter and CRDT modules
//! sit in the agreement tests of `events.rs` and `xdr_merge.rs`. `apitest_c.wasm` is here for one export
//! outside the conformance specification, `stop_probe`, which tests the C SDK's reads that stop the call.
//! The fixtures are built as `aerospike-udf-sdk/wasm-cpp/README.md` says.

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

/// The record API of the C++ SDK, driven through its fixture module (`aerospike-udf-sdk/wasm-cpp/examples/udf`):
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

/// The record's lifecycle as `udf::Record` has it: `save()` writes what was staged and creates the record
/// when there is none, `remove(name)` takes a bin away, a nil written through `set` does the same, and
/// `delete_record()` deletes the record without a save. The C++ counterpart of
/// `a_wasm_udf_saves_removes_and_deletes` in `udf_wasm.rs`.
#[aerospike_macro::test]
async fn cpp_udf_saves_removes_and_deletes() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("cpp_verbs_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let read = ReadPolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client
        .put(&write, &key, &[as_bin!("a", 1), as_bin!("b", 2), as_bin!("c", 3)])
        .await
        .unwrap();

    if let Err(reason) = register(&client, FIXTURE, package).await {
        skipped("cpp_udf_saves_removes_and_deletes", &reason);
        return;
    }

    // Nothing staged, on a record that exists.
    let before = client.get(&read, &key, Bins::All).await.unwrap();
    let result = client.execute_udf(&write, &key, package, "save_nothing", &[]).await.unwrap();
    let after = client.get(&read, &key, Bins::All).await.unwrap();

    assert_eq!(result, Some(Value::Bool(true)));
    assert_eq!(after.bins, before.bins, "a save with nothing staged changes no bin");
    assert_eq!(
        after.generation,
        before.generation + 1,
        "but it is a write all the same: the server treats it as a touch"
    );

    // A bin removed by name, and one removed by writing nil to it.
    let result = client
        .execute_udf(&write, &key, package, "drop_bin", &[Value::from("a")])
        .await
        .unwrap();
    assert_eq!(result, Some(Value::Int(2)), "remove takes the bin away");

    let result = client
        .execute_udf(&write, &key, package, "nil_bin", &[Value::from("b")])
        .await
        .unwrap();
    assert_eq!(result, Some(Value::Int(1)), "a nil written to a bin removes it too");

    let record = client.get(&read, &key, Bins::All).await.unwrap();
    assert_eq!(record.bins.len(), 1);
    assert_eq!(record.bins.get("c"), Some(&Value::Int(3)));

    // Nothing staged, on a key with no record: a record with no bins cannot exist, so nothing is made
    // and the call is answered as the server answers any operation on a key that is not there.
    let missing = as_key!(namespace, set_name, 2);
    let result = client.execute_udf(&write, &missing, package, "save_nothing", &[]).await;
    assert!(
        matches!(&result, Err(e) if e.matches(&[ResultCode::KeyNotFoundError])),
        "a save of nothing to no record: {result:?}"
    );
    assert!(client.get(&read, &missing, Bins::All).await.is_err(), "and no record was made");

    // Delete needs no save, and takes what was staged with it. Inside the call the record still answers
    // that it exists: the server carries the delete out when the call returns.
    let result = client.execute_udf(&write, &key, package, "delete_it", &[]).await.unwrap();
    assert_eq!(result, Some(Value::Bool(true)), "exists() is unchanged for the rest of the call");
    assert!(client.get(&read, &key, Bins::All).await.is_err(), "and the record is gone from the server");

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// `Value::at(index)` reads one element of a stored list without walking to it, and `Map::contains` asks
/// the host whether a map the call is building has a key, string or integer. The C++ counterpart of
/// `a_wasm_udf_indexes_a_list_and_asks_a_built_map` in `udf_wasm.rs`.
#[aerospike_macro::test]
async fn cpp_udf_indexes_a_list_and_asks_a_built_map() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("cpp_index_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let list = as_list!(10, "twenty", as_list!(1, 2), 40);
    client.put(&write, &key, &[as_bin!("l", list)]).await.unwrap();

    if let Err(reason) = register(&client, FIXTURE, package).await {
        skipped("cpp_udf_indexes_a_list_and_asks_a_built_map", &reason);
        return;
    }

    let probe = |index: i64| {
        let client = client.clone();
        let key = key.clone();
        let package = package.clone();
        let write = write.clone();

        async move {
            let result = client
                .execute_udf(&write, &key, &package, "index_probe", &[Value::from("l"), Value::Int(index)])
                .await
                .unwrap()
                .expect("index_probe returns a map");
            let pairs = map_entries(&result);

            assert!(!pairs.is_empty(), "expected a map, got {result:?}");

            move |name: &str| -> Option<Value> { entry(&pairs, name).cloned() }
        }
    };

    let field = probe(0).await;
    assert_eq!(field("len"), Some(Value::Int(4)));
    assert_eq!(field("at"), Some(Value::Int(10)), "the first element");
    assert_eq!(field("knows"), Some(Value::Bool(true)), "a built map has the keys it was given");
    assert_eq!(field("invents"), Some(Value::Bool(false)), "and no others");

    assert_eq!(probe(1).await("at"), Some(Value::String("twenty".into())), "past a scalar");
    assert_eq!(probe(3).await("at"), Some(Value::Int(40)), "past a nested list, stepped over whole");
    assert_eq!(
        probe(2).await("at"),
        Some(Value::List(vec![Value::Int(1), Value::Int(2)])),
        "a nested list comes back as one"
    );
    assert_eq!(probe(4).await("at"), Some(Value::String("absent".into())), "one past the end");
    assert_eq!(probe(1 << 40).await("at"), Some(Value::String("absent".into())), "far past the end");
    assert_eq!(probe(-1).await("at"), Some(Value::String("absent".into())), "before the start");

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

const APITEST_C: &[u8] = include_bytes!("../fixtures/wasm/apitest_c.wasm");

/// What the C++ SDK fails a call with when it cannot read a bin or a stored list or map
/// (`detail::unreadable`).
const CPP_UNREADABLE: &str = "could not read a bin or a stored list or map: the call has spent the memory it \
                              may hold on the server, or the value is no longer readable after a failed save";
/// What the C SDK fails a call with when a bin read fails for a reason other than "no such bin"
/// (`as_udf_rec_unreadable`).
const C_UNREADABLE: &str = "could not read a bin: the call has spent the memory it may hold on the server";

/// Calls `function(what)` on `key` and asserts that it failed with `message` rather than answering, and
/// that the record is as it was before: the probes save a bin `x` first, which a failed call undoes.
async fn assert_stops(client: &Client, key: &Key, package: &str, function: &str, what: &str, message: &str) {
    let read = ReadPolicy::default();
    let before = client.get(&read, key, Bins::All).await.unwrap();
    let result = client
        .execute_udf(&WritePolicy::default(), key, package, function, &[Value::from(what)])
        .await;

    match result {
        Ok(answer) => panic!("{function}({what}) answered {answer:?}; it must stop the call"),
        Err(e) => {
            let text = e.to_string();
            assert!(text.contains(message), "{function}({what}) failed, but not with the SDK's message: {text}");
        }
    }

    let after = client.get(&read, key, Bins::All).await.unwrap();
    assert_eq!(after.bins, before.bins, "{function}({what}): a call that stops changes no bin");
    assert_eq!(after.generation, before.generation, "{function}({what}): and writes nothing");
}

/// A walk of a stored list or map that the host cannot open, and a lookup in a stored map it can no longer
/// read, stop the call with the SDK's message: a range-for cannot return an error, and an empty walk or a
/// "no such key" there would be a wrong answer taken for a right one. The walks are starved of handles
/// (every one of the call's 256 taken by empty lists); a lookup takes no handle, so it is starved
/// instead by a save the server refuses, which ends every value read from the stored record. A bin read
/// before the table filled is read again on the handle it already has.
#[aerospike_macro::test]
async fn cpp_unreadable_collections_stop_the_call() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("cpp_stop_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let map = as_map!("a" => 1, "b" => 2);
    client
        .put(&write, &key, &[as_bin!("l", as_list!(1, 2, 3)), as_bin!("m", map)])
        .await
        .unwrap();

    if let Err(reason) = register(&client, FIXTURE, package).await {
        skipped("cpp_unreadable_collections_stop_the_call", &reason);
        return;
    }

    for what in ["walk", "map_walk", "contains", "get"] {
        assert_stops(&client, &key, package, "stop_probe", what, CPP_UNREADABLE).await;
    }

    // Reading l again reuses the handle of the first read, so a full table does not stop it.
    let result = client
        .execute_udf(&write, &key, package, "stop_probe", &[Value::from("reread")])
        .await
        .unwrap();
    assert_eq!(result, Some(Value::Int(3)), "a bin read before the table filled reads again");

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// The C SDK's `as_udf_rec_has` and `as_udf_rec_get` answer only yes or no, so a read that fails because
/// no handle is free stops the call with the header's message instead of answering "no such bin". The
/// bin read before the table filled is read again on the handle it already has.
#[aerospike_macro::test]
async fn c_unreadable_bins_stop_the_call() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("c_stop_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client
        .put(&write, &key, &[as_bin!("s", "hello"), as_bin!("l", as_list!(1, 2, 3))])
        .await
        .unwrap();

    if let Err(reason) = register(&client, APITEST_C, package).await {
        skipped("c_unreadable_bins_stop_the_call", &reason);
        return;
    }

    for what in ["has", "get"] {
        assert_stops(&client, &key, package, "stop_probe", what, C_UNREADABLE).await;
    }

    let result = client
        .execute_udf(&write, &key, package, "stop_probe", &[Value::from("reread")])
        .await
        .unwrap();
    assert_eq!(result, Some(Value::Int(5)), "a bin read before the table filled reads again");

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// Calls `churn(count)` on a record whose bin `nested` holds `count` lists `[i, 1]`, and checks its answer:
/// `count` maps each holding a list built and given back as they go, and `count` walks of the nested lists
/// with a cursor each, closed as each ends. 6000 handles in one call, a few at a time.
async fn assert_churn(module: &[u8], prefix: &str, test: &str) {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("{prefix}_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let count = 2000i64;
    let nested: Vec<Value> = (0..count).map(|i| as_list!(i, 1)).collect();
    client.put(&write, &key, &[as_bin!("nested", Value::List(nested))]).await.unwrap();

    if let Err(reason) = register(&client, module, package).await {
        skipped(test, &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "churn", &[Value::Int(count)])
        .await
        .unwrap();
    let sum = (0..count).sum::<i64>() + count;

    assert_eq!(result, Some(Value::List(vec![Value::Int(count), Value::Int(sum)])), "{test}");

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// The C++ SDK gives a built collection's handle back when it is destroyed, and a range-for's cursor when the
/// loop ends.
#[aerospike_macro::test]
async fn a_cpp_udf_gives_back_what_it_builds_and_walks() {
    assert_churn(FIXTURE, "cpp_churn", "a_cpp_udf_gives_back_what_it_builds_and_walks").await;
}

/// The C SDK gives back what `as_udf_handle_close` is given: the C apitest's `churn` closes every map, list
/// and cursor it makes.
#[aerospike_macro::test]
async fn a_c_udf_gives_back_what_it_closes() {
    assert_churn(APITEST_C, "c_churn", "a_c_udf_gives_back_what_it_closes").await;
}
