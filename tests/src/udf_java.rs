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

//! WASM UDFs written in Java, compiled by TeaVM's linear-memory backend. `hello_java.wasm` is the bare
//! contract without the SDK (`aerospike-udf-sdk-wasm-java/examples/hello`); `counter_java.wasm` is built
//! from annotated methods, so its exports are the SDK's annotation processor's work, and the second test
//! drives that generated code through every rejection. The SDK's other examples are held to the other
//! languages' behaviour in `udf_vector.rs` and `events.rs`. The fixtures under
//! `tests/fixtures/wasm/*_java.wasm` are built as `aerospike-udf-sdk-wasm-java/README.md` says.

use crate::common;
use aerospike::Task;
use aerospike::*;
use std::collections::HashMap;

const HELLO: &[u8] = include_bytes!("../fixtures/wasm/hello_java.wasm");
const COUNTER: &[u8] = include_bytes!("../fixtures/wasm/counter_java.wasm");
const FIXTURE: &[u8] = include_bytes!("../fixtures/wasm/udf_java.wasm");

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

/// The smallest Java module: a function that answers a constant and one that adds its two arguments,
/// both through the server's imports, with the TeaVM runtime's own imports served by the host.
#[aerospike_macro::test]
async fn a_java_udf_runs() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("java_hello_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);

    if let Err(reason) = register(&client, HELLO, package).await {
        skipped("a_java_udf_runs", &reason);
        return;
    }

    let write = WritePolicy::default();

    assert_eq!(client.execute_udf(&write, &key, package, "hello", &[]).await.unwrap(), Some(Value::Int(42)));

    // An uncaught Java exception fails the call; a caught one is ordinary Java.
    let thrown = client.execute_udf(&write, &key, package, "probe_throw", &[Value::from(2), Value::from(3)]).await;
    eprintln!("probe_throw: {thrown:?}");
    assert!(thrown.is_err(), "an uncaught exception should fail the call: {thrown:?}");
    assert_eq!(
        client.execute_udf(&write, &key, package, "probe_catch", &[Value::from(2), Value::from(3)]).await.unwrap(),
        Some(Value::Int(7)),
        "a caught exception is ordinary Java"
    );

    assert_eq!(
        client.execute_udf(&write, &key, package, "add", &[Value::from(2), Value::from(3)]).await.unwrap(),
        Some(Value::Int(5))
    );

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// The exports the annotation processor generates for `@Function` methods: the signature is derived from
/// the Java parameter types, the arguments are decoded into the parameters and checked before the method
/// runs, the return value is the result, and a thrown `UdfException` fails the call with its message.
#[aerospike_macro::test]
async fn generated_exports_decode_arguments() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("java_counter_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);

    if let Err(reason) = register(&client, COUNTER, package).await {
        skipped("generated_exports_decode_arguments", &reason);
        return;
    }

    let write = WritePolicy::default();

    // `static long increment(String site, long n)` describes itself from its Java types.
    assert_eq!(
        client.execute_udf(&write, &key, package, "__udf_sig_increment", &[]).await.unwrap(),
        Some(Value::from("fn increment(site: str, n: int) -> value"))
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

    // A thrown UdfException fails the call with its message.
    let text = client
        .execute_udf(&write, &key, package, "increment", &[Value::from("s".repeat(40)), Value::from(1)])
        .await
        .expect_err("a thrown UdfException must fail the call")
        .to_string();

    assert!(text.contains("increment: the site name is too long"), "{text}");

    // None of that touched the record.
    assert_eq!(client.execute_udf(&write, &key, package, "value", &[]).await.unwrap(), Some(Value::Int(3)));

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// The record API of the Java SDK, driven through its fixture module
/// (`aerospike-udf-sdk-wasm-java/examples/udf`): lists and maps walked, the typed bin readers, the
/// bool, nil, GeoJSON and packed-list writes, integer map keys, a GeoJSON result, the record's metadata,
/// its TTL and its deletion. The expectations are those of the Rust fixture's tests in `udf_wasm.rs`.
#[aerospike_macro::test]
async fn java_sdk_covers_the_rust_surface() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("java_fixture_{}", common::rand_str(6));
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
        skipped("java_sdk_covers_the_rust_surface", &reason);
        return;
    }

    let call = |function: &'static str, args: Vec<Value>| {
        let client = client.clone();
        let key = key.clone();
        let package = package.clone();
        async move { client.execute_udf(&WritePolicy::default(), &key, &package, function, &args).await }
    };

    // A list walked with a for-each.
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

/// What the record's own verbs do, through the Java `Record`: `save` with nothing staged, `remove` of a
/// bin, a nil written to a bin, and `delete`. The expectations are those of
/// `a_wasm_udf_saves_removes_and_deletes` in `udf_wasm.rs`.
#[aerospike_macro::test]
async fn a_java_udf_saves_removes_and_deletes() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("java_verbs_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let read = ReadPolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client
        .put(&write, &key, &[as_bin!("a", 1), as_bin!("b", 2), as_bin!("c", 3)])
        .await
        .unwrap();

    if let Err(reason) = register(&client, FIXTURE, package).await {
        skipped("a_java_udf_saves_removes_and_deletes", &reason);
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

/// An element of a stored list by its index (`ListValue.get`), and a built map asked whether it has a key
/// (`MapBuilder.containsKey`): each one call to the host. The expectations are those of
/// `a_wasm_udf_indexes_a_list_and_asks_a_built_map` in `udf_wasm.rs`.
#[aerospike_macro::test]
async fn a_java_udf_indexes_a_list_and_asks_a_built_map() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("java_index_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let list = as_list!(10, "twenty", as_list!(1, 2), 40);
    client.put(&write, &key, &[as_bin!("l", list)]).await.unwrap();

    if let Err(reason) = register(&client, FIXTURE, package).await {
        skipped("a_java_udf_indexes_a_list_and_asks_a_built_map", &reason);
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

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// The two things the Java SDK has that the others do not need: the bridge to `java.util` (a stored list
/// and map copied out as a `List` and a `Map`, a `List` written to a bin, a `Map` returned), and a
/// for-each that is left early, many more times than a call has handle slots, without running out of them.
#[aerospike_macro::test]
async fn a_java_udf_bridges_java_util_and_leaves_loops_early() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("java_bridge_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let m = Value::from(HashMap::from([(Value::from("a"), Value::from(1)), (Value::from("b"), Value::from("two"))]));
    client
        .put(&write, &key, &[as_bin!("l", as_list!(1, 2, 3)), as_bin!("m", m)])
        .await
        .unwrap();

    if let Err(reason) = register(&client, FIXTURE, package).await {
        skipped("a_java_udf_bridges_java_util_and_leaves_loops_early", &reason);
        return;
    }

    let bridged = client
        .execute_udf(&write, &key, package, "bridge", &[])
        .await
        .unwrap()
        .expect("bridge returns a map");
    let bridged = map_entries(&bridged);

    assert_eq!(
        entry(&bridged, "items"),
        Some(&Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])),
        "the stored list, copied out and handed back"
    );
    assert_eq!(entry(&bridged, "size"), Some(&Value::Int(3)));
    assert_eq!(entry(&bridged, "half"), Some(&Value::from(0.5f64)));
    assert_eq!(entry(&bridged, "yes"), Some(&Value::Bool(true)));
    assert_eq!(entry(&bridged, "nothing"), Some(&Value::Nil), "a null is nil");

    let entries = map_entries(entry(&bridged, "entries").expect("the stored map, copied out and handed back"));

    assert_eq!(entry(&entries, "a"), Some(&Value::Int(1)));
    assert_eq!(entry(&entries, "b"), Some(&Value::from("two")));

    let record = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();

    assert_eq!(
        record.bins.get("doubled"),
        Some(&Value::List(vec![Value::Int(2), Value::Int(4), Value::Int(6)])),
        "a java.util.List written to a bin"
    );

    // A call has 256 handle slots; 1000 abandoned walks would need 1000 if a walk kept its cursor.
    assert_eq!(
        client.execute_udf(&write, &key, package, "early_exit", &[Value::from(1000)]).await.unwrap(),
        Some(Value::Int(6)),
        "the list is still walkable after 1000 loops left early"
    );

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// **The record's bin access, timed inside the guest**: one call loops over `n` reads (`bench_get`) or `n`
/// reads and staged writes (`bench_set`) of an integer bin, and the difference between a long and an empty
/// loop is what one access costs, with the round trip and the instance taken out. It is how the cost of
/// addressing a bin by name through `Record` was held to what a name encoded ahead of time used to cost.
///
/// AEROSPIKE_HOSTS=localhost:3000 cargo test --no-default-features --features async,serialization,rt-tokio,tls --test lib -- java_record_bench --ignored --nocapture
#[aerospike_macro::test]
#[ignore]
async fn java_record_bench() {
    const N: i64 = 100_000;
    const ROUNDS: usize = 15;

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("java_bench_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);
    let write = WritePolicy::default();

    client.put(&write, &key, &[as_bin!("i", 7)]).await.unwrap();

    if let Err(reason) = register(&client, FIXTURE, package).await {
        skipped("java_record_bench", &reason);
        return;
    }

    for function in ["bench_get", "bench_set"] {
        let mut best = [f64::MAX; 2];

        for _ in 0..ROUNDS {
            for (slot, n) in [0, N].into_iter().enumerate() {
                let started = std::time::Instant::now();

                client.execute_udf(&write, &key, package, function, &[Value::from(n)]).await.unwrap();
                best[slot] = best[slot].min(started.elapsed().as_secs_f64());
            }
        }

        eprintln!(
            "  {function}: {:.1} ns per iteration ({N} iterations in {:.1} ms, an empty call {:.2} ms)",
            (best[1] - best[0]) * 1e9 / N as f64,
            best[1] * 1000.0,
            best[0] * 1000.0
        );
    }

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}
