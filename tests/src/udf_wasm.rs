// Copyright 2026 Aerospike, Inc.
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

//! WebAssembly UDFs, against a server that supports them.
//!
//! These mirror `udf.rs`, which does the same things in Lua, so the two languages can be compared by
//! reading them side by side.
//!
//! # They self-skip
//!
//! `WASM` is a newer `udf-type` than most servers know, and a server without it answers `udf-put` with
//! `error=invalid_udf_type`. Every test here asks first and returns early when the answer is no —
//! reported on stderr, so a skip is visible rather than silent. Against a server that does support it
//! they run in full.
//!
//! The module they use is a checked-in fixture; `tests/fixtures/wasm/README.md` says why, and keeps the
//! source it was built from beside it.

use std::collections::HashMap;

use crate::common;

use aerospike::Task;
use aerospike::*;

/// The compiled fixture. Included at build time so a missing file is a compile error rather than a test that
/// fails for the wrong reason.
///
/// **One fixture, because there is one engine.** This was briefly two, with a probe to work out which of
/// mod-wasm (Rust/wasmtime) and mod-wamr (C/WAMR) a server was running, since the two had incompatible guest
/// ABIs and the choice was invisible from a client. mod-wasm is gone, so the probe and the second fixture went
/// with it.
const MODULE: &[u8] = include_bytes!("../fixtures/wasm/udf_test.wasm");

/// Register the fixture, or report that this server cannot.
///
/// `Ok(())` means it registered and ran. `Err(reason)` means the server does not do WASM, which is not a test
/// failure — the caller returns early.
///
/// It is **probed by calling it**, not just registered: validation only loads a module and does not resolve its
/// imports, so a module the server cannot actually run registers perfectly happily and fails at the first
/// apply. `abi_probe` calls an import and returns 2, which is the cheapest proof the whole path works.
///
/// Spelled `std::result::Result` because `aerospike::*` brings its own one-parameter `Result` alias into scope,
/// and this one carries a reason rather than an `aerospike::Error`.
async fn register(client: &Client, package: &str, key: &Key) -> std::result::Result<(), String> {
    let policy = AdminPolicy::default();
    let write = WritePolicy::default();

    match client
        .register_udf(&policy, MODULE, &format!("{package}.wasm"), UdfLang::Wasm)
        .await
    {
        Ok(task) => {
            if let Err(e) = task.wait_till_complete(None).await {
                return Err(format!("the module did not become available: {e}"));
            }
        }
        // The registration itself was refused. Told apart from a broken cluster by the token the server uses
        // for a language it does not know.
        //
        // Matched against the **`Debug`** rendering, not `Display`, and that is not laziness: the server
        // answers `udf-put` with `error=invalid_udf_type`, and that text ends up in the `node` field of a
        // *nested* source error, so `to_string()` yields only "Error -1: Error registering UDF" and contains no
        // token at all. An earlier version matched on `Display` and would have panicked on a server without
        // WASM support — or worse, skipped on a genuinely broken one.
        Err(e) => {
            let detail = format!("{e:?}");

            if detail.contains("invalid_udf_type") {
                return Err(
                    "this server does not support WASM UDFs (udf-put answered invalid_udf_type)"
                        .to_owned(),
                );
            }

            panic!("registering a wasm udf failed for an unexpected reason: {detail}");
        }
    }

    match client
        .execute_udf(&write, key, package, "abi_probe", &[])
        .await
    {
        Ok(Some(Value::Int(2))) => Ok(()),
        other => panic!(
            "the server accepts WASM UDFs but could not run one: abi_probe answered {other:?}, wanted 2"
        ),
    }
}

/// Print why a test did nothing, so a skip is not mistaken for a pass.
fn skipped(test: &str, reason: &str) {
    eprintln!("skipping {test}: {reason}");
}

/// A record UDF that reads a bin, writes it and returns the new value.
///
/// The Lua equivalent is `execute_udf` in `udf.rs`. What matters here beyond "it ran" is that the write
/// **persisted**: a UDF that stages a bin without committing returns a plausible value and changes
/// nothing, which is the easiest way to get this wrong.
#[aerospike_macro::test]
async fn a_wasm_udf_reads_and_writes_its_record() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_rw_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client.put(&write, &key, &[as_bin!("n", 10)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_reads_and_writes_its_record", &reason);
        return;
    }

    // Once, twice: the second call must see what the first wrote.
    for expected in [11i64, 12] {
        let result = client
            .execute_udf(&write, &key, package, "increment", &[])
            .await
            .unwrap();

        assert_eq!(
            result,
            Some(Value::Int(expected)),
            "the udf returns the value it wrote"
        );
    }

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(
        record.bins.get("n"),
        Some(&Value::Int(12)),
        "and the record holds it — a staged write that was never committed would still return 12 \
         while leaving 10 here"
    );
}

/// Arguments reach the guest, in order and with their types.
#[aerospike_macro::test]
async fn arguments_reach_a_wasm_udf() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_args_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client
        .put(&write, &key, &[as_bin!("other", 1)])
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("arguments_reach_a_wasm_udf", &reason);
        return;
    }

    // A string and three integers: the string names the bin, the integers are summed into it.
    let args = vec![
        Value::from("total"),
        Value::from(10),
        Value::from(20),
        Value::from(12),
    ];

    let result = client
        .execute_udf(&write, &key, package, "add_all", &args)
        .await
        .unwrap();
    assert_eq!(
        result,
        Some(Value::Int(42)),
        "the arguments arrived in order and as integers"
    );

    // Again, to show it accumulated rather than recomputed.
    let result = client
        .execute_udf(&write, &key, package, "add_all", &args)
        .await
        .unwrap();
    assert_eq!(result, Some(Value::Int(84)));
}

/// Nested collections survive the trip, which is the whole reason values cross as msgpack.
#[aerospike_macro::test]
async fn a_wasm_udf_returns_nested_collections() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_nest_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_returns_nested_collections", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "nested", &[])
        .await
        .unwrap();

    let Some(Value::List(items)) = result else {
        panic!("expected a list, got {result:?}");
    };

    assert_eq!(items.len(), 4);
    assert_eq!(items[0], Value::Int(1));
    assert_eq!(
        items[1],
        Value::String("two".to_string()),
        "a string must not come back as a blob — Aerospike's msgpack puts a type marker inside the \
         string's length, and getting that wrong shifts every byte"
    );
    assert_eq!(
        items[2],
        Value::List(vec![Value::Bool(true), Value::Nil]),
        "a list inside a list, with a bool and a nil"
    );
    assert!(
        matches!(
            items[3],
            Value::HashMap(_) | Value::OrderedMap(_) | Value::SortedMap(_)
        ),
        "and a map, in whichever representation the engine produced — mod-wamr builds an as_orderedmap, \
         which is ordered by key and so arrives as a SortedMap: {:?}",
        items[3]
    );
}

/// The record's own metadata, read from inside the UDF.
#[aerospike_macro::test]
async fn a_wasm_udf_sees_its_record_metadata() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_meta_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client
        .put(&write, &key, &[as_bin!("a", 1), as_bin!("b", 2)])
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_sees_its_record_metadata", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "describe", &[])
        .await
        .unwrap();

    // Either map representation, since which one the client builds is its business.
    // Any of the three map representations: see the note in the nested-collections test.
    let pairs: Vec<(Value, Value)> = match result {
        Some(Value::HashMap(map)) => map.into_iter().collect(),
        Some(Value::OrderedMap(map)) => map.into_iter().collect(),
        Some(Value::SortedMap(map)) => map.into_iter().collect(),
        other => panic!("expected a map, got {other:?}"),
    };

    let field = |name: &str| -> Option<Value> {
        pairs
            .iter()
            .find(|(key, _)| *key == Value::String(name.to_string()))
            .map(|(_, value)| value.clone())
    };

    assert_eq!(
        field("generation"),
        Some(Value::Int(1)),
        "a record written once is generation 1"
    );
    assert_eq!(
        field("bins"),
        Some(Value::Int(2)),
        "and it has the two bins that were written"
    );
    assert_eq!(
        field("set"),
        Some(Value::String(set_name.to_string())),
        "the set name comes back as a string"
    );
    assert!(
        matches!(field("size"), Some(Value::Int(n)) if n > 0),
        "the record has a size: {:?}",
        field("size")
    );
}

/// What the record's own verbs do: `save` with nothing staged, `remove` of a bin, a nil written to a bin,
/// and `delete`.
#[aerospike_macro::test]
async fn a_wasm_udf_saves_removes_and_deletes() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_verbs_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let read = ReadPolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client
        .put(&write, &key, &[as_bin!("a", 1), as_bin!("b", 2), as_bin!("c", 3)])
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_saves_removes_and_deletes", &reason);
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
}

/// An element of a stored list by its index, and a built map asked whether it has a key: each one call to
/// the host, where a list used to be walked from its start and a built map packed whole to be asked.
#[aerospike_macro::test]
async fn a_wasm_udf_indexes_a_list_and_asks_a_built_map() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_index_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let list = as_list!(10, "twenty", as_list!(1, 2), 40);
    client.put(&write, &key, &[as_bin!("l", list)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_indexes_a_list_and_asks_a_built_map", &reason);
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
                .unwrap();
            let pairs: Vec<(Value, Value)> = match result {
                Some(Value::HashMap(map)) => map.into_iter().collect(),
                Some(Value::OrderedMap(map)) => map.into_iter().collect(),
                Some(Value::SortedMap(map)) => map.into_iter().collect(),
                other => panic!("expected a map, got {other:?}"),
            };

            move |name: &str| -> Option<Value> {
                pairs
                    .iter()
                    .find(|(key, _)| *key == Value::String(name.to_string()))
                    .map(|(_, value)| value.clone())
            }
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
}

/// A function the module does not export is an error naming it, not a silent nothing.
#[aerospike_macro::test]
async fn calling_a_function_a_wasm_module_does_not_export_is_an_error() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_miss_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped(
            "calling_a_function_a_wasm_module_does_not_export_is_an_error",
            &reason,
        );
        return;
    }

    let error = client
        .execute_udf(&write, &key, package, "no_such_function", &[])
        .await
        .expect_err("a missing export must fail rather than return nothing");

    // The server's own UDF failure. What matters is that it is reported at all: an unexported function
    // could plausibly have been treated as a UDF that returned nil.
    let text = error.to_string();
    assert!(
        !text.is_empty(),
        "the failure should say something: {error:?}"
    );
}

/// Registering something that is not a module is refused at registration, not at first call.
///
/// The point of validating on `udf-put`: the client is still there to be told.
#[aerospike_macro::test]
async fn bytes_that_are_not_a_wasm_module_are_refused_when_registered() {
    let client = common::client().await;
    let policy = AdminPolicy::default();
    let name = &format!("wasm_junk_{}.wasm", common::rand_str(6));

    // Probe with the real fixture first, so a server without WASM support skips instead of "passing"
    // this test for the wrong reason — it would refuse these bytes too, but for the wrong reason.
    let probe = &format!("wasm_probe_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(common::namespace(), &common::rand_str(10), 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, probe, &key).await {
        skipped(
            "bytes_that_are_not_a_wasm_module_are_refused_when_registered",
            &reason,
        );
        return;
    }

    let error = client
        .register_udf(&policy, b"this is not a wasm module", name, UdfLang::Wasm)
        .await
        .expect_err("nonsense bytes must not register");

    let text = error.to_string();
    assert!(
        text.contains("compile_error") || text.contains("error"),
        "the refusal should say what was wrong: {text}"
    );
}

/// A list bin is read element by element, and adds up.
///
/// The point is less the sum than the path it took. On `mod-wamr` the list stays in the record and each
/// element is one import call through a handle; on `mod-wasm` the whole list is serialised to msgpack,
/// copied into guest memory and parsed before the first element can be read. Both fixtures export
/// `sum_list`, so this asserts the two data models produce the same answer — which is the only part a
/// client can see, and the part that must not differ.
#[aerospike_macro::test]
async fn a_wasm_udf_reads_a_list_bin() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_list_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let items = vec![
        Value::Int(1),
        Value::Int(2),
        Value::Int(3),
        Value::Int(-6),
        Value::Int(100),
    ];

    client
        .put(&write, &key, &[as_bin!("l", Value::List(items))])
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_reads_a_list_bin", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "sum_list", &[])
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(Value::Int(100)),
        "1 + 2 + 3 - 6 + 100, read through the engine's collection path"
    );
}

/// A guest that tries to take more memory than it is allowed is stopped.
///
/// `max-memory` exists because a UDF runs inside a write transaction holding a record lock, so an
/// unbounded guest is a stalled partition rather than a slow query. The fixtures reach for memory in the
/// way their ABI makes natural — `mod-wasm`'s allocates, `mod-wamr`'s grows linear memory directly, since
/// it has no allocator — and this accepts either outcome, because *how* the refusal surfaces is the
/// engine's business:
///
/// - a trap, when the guest cannot handle a failed allocation (an error to the client);
/// - a bounded number, when the guest notices the refusal and reports how far it got.
///
/// What is **not** acceptable is growing without limit, which the fixtures signal by failing with a
/// message rather than returning a number — so a broken cap cannot look like a pass.
#[aerospike_macro::test]
async fn a_wasm_udf_cannot_outgrow_its_memory_limit() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_hog_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_cannot_outgrow_its_memory_limit", &reason);
        return;
    }

    match client.execute_udf(&write, &key, package, "hog", &[]).await {
        // Refused, and the guest lived to say how far it got.
        Ok(Some(Value::Int(grown))) => assert!(
            grown < 512,
            "the guest grew {grown} MiB before being refused, which is not a limit"
        ),
        // Refused, and the guest could not carry on — the ordinary outcome for a failed allocation.
        Err(_) => {}
        // Anything else means the cap did not engage. The fixtures fail with a message in that case, so
        // this arm is where an ineffective limit lands.
        other => panic!("memory was not capped: {other:?}"),
    }
}

/// A stream UDF aggregates across the records a query matched.
///
/// # Why this goes through `query()` and not `query_aggregate`
///
/// `query_aggregate` pipes the server's partial results into a **client-side Lua** pipeline (mlua) to run the
/// final reduce — that is how Aerospike's clients make Lua aggregation look seamless across nodes. A WASM
/// module cannot take part in it. Through a plain `query()` with an aggregate function set, the caller instead
/// receives the per-node partials and combines them, which is what this does.
///
/// A read after a write the call has not saved, and a value kept across a write to its bin.
///
/// Both went wrong in the server, not the SDK. A list, map or blob bin was read from where it is stored
/// without asking whether a value was staged over it, so a UDF that wrote such a bin and read it back
/// got what was there before. And a string bin is handed to a guest as a position in a value the record
/// owns, which the record frees when the bin is written again: the guest's handle then read freed memory.
#[aerospike_macro::test]
async fn a_wasm_udf_reads_what_it_staged_and_what_it_read_before() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_staged_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let text = "a string long enough to live on the heap, and to be noticed if it changes";

    client
        .put(
            &write,
            &key,
            &[
                as_bin!("sl", as_list!(1, 2, 3)),
                as_bin!("sm", as_map!("k" => 1)),
                as_bin!("s", text),
            ],
        )
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_reads_what_it_staged_and_what_it_read_before", &reason);
        return;
    }

    let result = client.execute_udf(&write, &key, package, "staged_read", &[]).await.unwrap();

    assert_eq!(
        result,
        Some(as_list!(1, 1)),
        "a list of one staged over a list of three reads as one, and a removed map is gone"
    );

    let result = client.execute_udf(&write, &key, package, "stale_read", &[]).await.unwrap();

    assert_eq!(
        result,
        Some(Value::Bool(true)),
        "a string read before its bin was written 64 times over is still that string"
    );
}

/// Text that is not a GeoJSON document is not saved as a GeoJSON bin.
///
/// The save used to build the bin anyway - a geospatial bin with nothing in it to index - and say so in
/// the server's log. It fails now, as the same document sent as a client's write always has.
#[aerospike_macro::test]
async fn a_wasm_udf_cannot_save_a_geojson_bin_that_is_not_geojson() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_badgeo_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_cannot_save_a_geojson_bin_that_is_not_geojson", &reason);
        return;
    }

    let result = client.execute_udf(&write, &key, package, "bad_geo", &[]).await;

    // Staging is accepted (0) and the save is refused (-4); a UDF whose write failed fails as a whole
    // on some paths, which says the same thing.
    match &result {
        Ok(value) => assert_eq!(value, &Some(as_list!(0, -4)), "the save of a bad document should be refused"),
        Err(e) => assert!(format!("{e:?}").to_lowercase().contains("geo"), "an unexpected failure: {e:?}"),
    }

    let record = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();

    assert!(!record.bins.contains_key("geo"), "the bad document was stored: {:?}", record.bins);
    assert_eq!(record.bins.get("n"), Some(&Value::Int(1)), "and the record is as it was");
}

/// A stream UDF cannot write to the records it is shown, by any host call.
#[aerospike_macro::test]
async fn a_wasm_stream_udf_cannot_write() {
    use futures::StreamExt;

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_poke_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_stream_udf_cannot_write", &reason);
        return;
    }

    const RECORDS: i64 = 20;

    for i in 2..=RECORDS {
        let key = as_key!(namespace, set_name, i);

        client.put(&write, &key, &[as_bin!("n", i)]).await.unwrap();
    }

    let mut statement = Statement::new(namespace, set_name, Bins::All);

    statement.set_aggregate_function(package, "poke", &[]);

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut refused = 0i64;
    let mut written = 0i64;
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.unwrap();

        for (_, value) in record.bins.iter() {
            let pairs: Vec<(Value, Value)> = match value {
                Value::HashMap(map) => map.clone().into_iter().collect(),
                Value::OrderedMap(map) => map.clone().into_iter().collect(),
                Value::SortedMap(map) => map.clone().into_iter().collect(),
                _ => continue,
            };

            for (name, n) in pairs {
                if let (Value::String(name), Value::Int(n)) = (&name, &n) {
                    match name.as_str() {
                        "refused" => refused += n,
                        "written" => written += n,
                        _ => {}
                    }
                }
            }
        }
    }

    assert_eq!(written, 0, "a stream UDF wrote to a record");
    assert_eq!(refused, RECORDS, "every record's write is refused");

    let record = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();

    assert!(!record.bins.contains_key("poked"), "and no record gained a bin");
}

#[aerospike_macro::test]
async fn a_wasm_stream_udf_aggregates() {
    use futures::StreamExt;

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_agg_{}", common::rand_str(6));

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_stream_udf_aggregates", &reason);
        return;
    }

    // Records 1..=50, so the expected total is not a number a wrong answer reaches by accident. Key 1 already
    // holds n=1 from the registration probe above, so start at 2 and count it in.
    const RECORDS: i64 = 50;

    for i in 2..=RECORDS {
        let key = as_key!(namespace, set_name, i);

        client.put(&write, &key, &[as_bin!("n", i)]).await.unwrap();
    }

    let expected: i64 = (1..=RECORDS).sum();

    // --- a scalar partial ---

    let mut statement = Statement::new(namespace, set_name, Bins::All);

    statement.set_aggregate_function(package, "sum", &[]);

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut total = 0i64;
    let mut partials = 0;
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.unwrap();

        for (_, value) in record.bins.iter() {
            if let Value::Int(n) = value {
                total += n;
                partials += 1;
            }
        }
    }

    assert!(partials > 0, "the aggregation produced no partial at all");
    assert_eq!(
        total, expected,
        "{partials} partial(s) must add up to {expected}"
    );

    // --- a map partial, to show a non-scalar result and that every record was seen exactly once ---

    let mut statement = Statement::new(namespace, set_name, Bins::All);

    statement.set_aggregate_function(package, "stats", &[]);

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut count = 0i64;
    let mut summed = 0i64;
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.unwrap();

        for (_, value) in record.bins.iter() {
            let pairs: Vec<(Value, Value)> = match value {
                Value::HashMap(map) => map.clone().into_iter().collect(),
                Value::OrderedMap(map) => map.clone().into_iter().collect(),
                Value::SortedMap(map) => map.clone().into_iter().collect(),
                // Not a map: nil, or a node that matched nothing. Skipped rather than failed.
                _ => continue,
            };

            for (name, n) in pairs {
                if let (Value::String(name), Value::Int(n)) = (&name, &n) {
                    match name.as_str() {
                        "count" => count += n,
                        "sum" => summed += n,
                        _ => {}
                    }
                }
            }
        }
    }

    assert_eq!(
        count, RECORDS,
        "every record must be seen exactly once — a per-record handle table that was not reset, or a \
         record counted twice, would show up here"
    );
    assert_eq!(
        summed, expected,
        "and the sum must agree with the scalar aggregation"
    );
}

/// **A GeoJson bin survives a round trip through a WASM UDF.**
///
/// Until `set_geojson` existed, `mod-wamr` had no GEOJSON kind at all: a geojson bin read as `Type::Other`, its text
/// was unreachable, and the only way to write text back was `set_str` — which keeps the document and silently turns a
/// geospatially indexable bin into an ordinary string. The document would look correct and geo queries would stop
/// matching it.
///
/// So this asserts three things a guest now gets: the bin reports as GeoJson, its text is readable, and writing it back
/// leaves it a geojson bin rather than a string.
#[aerospike_macro::test]
async fn a_wasm_udf_round_trips_a_geojson_bin() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set = &format!("wasm_geo_{}", common::rand_str(6));
    let package = &format!("wasm_geo_{}", common::rand_str(6));

    let key = as_key!(namespace, set, "geo");
    let doc = r#"{"type":"Point","coordinates":[-122.4194,37.7749]}"#;

    client
        .put(
            &WritePolicy::default(),
            &key,
            &[as_bin!("loc", Value::GeoJson(doc.to_string()))],
        )
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_round_trips_a_geojson_bin", &reason);
        return;
    }

    let before = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap()
        .bins
        .get("loc")
        .cloned();

    assert!(
        matches!(before, Some(Value::GeoJson(_))),
        "the bin starts as geojson, got {before:?}"
    );

    let returned = client
        .execute_udf(
            &WritePolicy::default(),
            &key,
            &format!("{package}.wasm"),
            "geo_roundtrip",
            &[as_val!("loc")],
        )
        .await
        .expect("the geojson round trip should succeed");

    // The UDF reports the document length, which is how we know it read the text rather than guessing.
    assert_eq!(
        returned,
        Some(Value::Int(doc.len() as i64)),
        "the UDF should report the document's length"
    );

    let after = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap()
        .bins
        .get("loc")
        .cloned();

    assert_eq!(
        after,
        Some(Value::GeoJson(doc.to_string())),
        "the bin must still be geojson, with the document unchanged"
    );

    client.delete(&WritePolicy::default(), &key).await.unwrap();
}

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

/// **A WASM UDF reads lists and maps straight out of their packed form.**
///
/// `bin_coll` makes the server deserialise a collection bin into one `as_val` per element before the guest
/// reads anything. The packed accessors - `bin_list_len`, `bin_list_ints_into`, `bin_map_get` and friends,
/// ported from mod-native - decode the stored particle in place instead. The fixture's `packed_probe`
/// exercises the whole family against three bins and reports what it saw; this asserts every figure against
/// what was written, so a decoder that miscounted, mis-skipped or mis-typed anything shows up by name.
#[aerospike_macro::test]
async fn a_wasm_udf_reads_collections_from_their_packed_form() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_packed_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    let ints = Value::List(vec![
        Value::Int(1),
        Value::Int(2),
        Value::Int(3),
        Value::Int(-6),
        Value::Int(100),
    ]);
    let doubles = Value::List(vec![Value::from(1.5f64), Value::from(-2.25f64)]);
    let mut map: HashMap<Value, Value> = HashMap::new();
    map.insert(Value::from("x"), Value::Int(42));
    map.insert(Value::from("f"), Value::from(2.5f64));
    map.insert(Value::from("s"), Value::from("str"));
    map.insert(Value::Int(7), Value::Int(70));
    map.insert(
        Value::from("l"),
        Value::List(vec![Value::Int(1), Value::Int(2)]),
    );

    client
        .put(
            &write,
            &key,
            &[
                as_bin!("l", ints),
                as_bin!("d", doubles),
                as_bin!("m", Value::HashMap(map)),
            ],
        )
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_reads_collections_from_their_packed_form", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "packed_probe", &[])
        .await
        .unwrap()
        .expect("packed_probe returns a map");
    let pairs = map_entries(&result);

    assert_eq!(entry(&pairs, "len"), Some(&Value::Int(5)), "bin_list_len: {result:?}");
    assert_eq!(entry(&pairs, "sum"), Some(&Value::Int(100)), "bin_list_ints_into: {result:?}");
    assert_eq!(entry(&pairs, "tail"), Some(&Value::Int(2)), "a window from element 3: {result:?}");
    assert_eq!(
        entry(&pairs, "dsum"),
        Some(&Value::from(-0.75f64)),
        "bin_list_doubles_into: {result:?}"
    );
    assert_eq!(entry(&pairs, "mlen"), Some(&Value::Int(5)), "bin_map_len: {result:?}");
    assert_eq!(entry(&pairs, "x"), Some(&Value::Int(42)), "bin_map_get, an integer: {result:?}");
    assert_eq!(
        entry(&pairs, "f"),
        Some(&Value::from(2.5f64)),
        "bin_map_get, a double: {result:?}"
    );
    assert_eq!(
        entry(&pairs, "s"),
        Some(&Value::from("str")),
        "bin_map_get_str, a string's bytes: {result:?}"
    );
    assert_eq!(
        entry(&pairs, "k7"),
        Some(&Value::Int(70)),
        "bin_map_get_i, an integer key: {result:?}"
    );
    assert_eq!(
        entry(&pairs, "nested"),
        Some(&Value::Int(2)),
        "a nested list is reported with its length: {result:?}"
    );
    assert_eq!(
        entry(&pairs, "missing"),
        Some(&Value::Bool(true)),
        "a missing key is Absent, not an error: {result:?}"
    );
    assert_eq!(
        entry(&pairs, "found"),
        Some(&Value::Int(203)),
        "Cursor::find hands the nested list over with its cursor (2 elements summing to 3): {result:?}"
    );
    assert_eq!(
        entry(&pairs, "bulk"),
        Some(&Value::Int(3)),
        "bin_map_get_ints_into reads the list under a key in one call: {result:?}"
    );
    assert_eq!(
        entry(&pairs, "run"),
        Some(&Value::Int(100)),
        "Cursor::ints_into reads a run of integers in one call: {result:?}"
    );
}

/// **A cursor walks a nested document in place, descending into its collections.**
///
/// The fixture's `cursor_probe` flattens everything a cursor over `doc` yields, bracketing nested lists and
/// maps with their lengths. The expected list pins the order values come out in, that the parent walk
/// resumes after a child, that a map yields key then value, and that strings come back through
/// `Cursor::bytes` intact.
#[aerospike_macro::test]
async fn a_wasm_udf_walks_a_document_with_a_cursor() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_cursor_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    let mut tail: HashMap<Value, Value> = HashMap::new();
    tail.insert(Value::from("k"), Value::Int(7));

    let doc = Value::List(vec![
        Value::Int(1),
        Value::from("two"),
        Value::from(3.5f64),
        Value::Bool(true),
        Value::Nil,
        Value::List(vec![Value::Int(10), Value::Int(20)]),
        Value::HashMap(tail),
    ]);

    client.put(&write, &key, &[as_bin!("doc", doc)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_walks_a_document_with_a_cursor", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "cursor_probe", &[])
        .await
        .unwrap();

    let expected = Value::List(vec![
        Value::Int(7),
        Value::Int(1),
        Value::from("two"),
        Value::from(3.5f64),
        Value::Bool(true),
        Value::Nil,
        Value::from("["),
        Value::Int(2),
        Value::Int(10),
        Value::Int(20),
        Value::from("]"),
        Value::from("{"),
        Value::Int(1),
        Value::from("k"),
        Value::Int(7),
        Value::from("}"),
    ]);

    assert_eq!(result, Some(expected), "the flattened walk");
}

/// **The packed writers store a list without building a value per element.**
///
/// `set_list_ints` / `set_list_doubles` pack the guest's slice straight into a LIST particle and apply it as
/// one op. What matters to a client is only that the bins read back as ordinary lists - including an integer
/// past 32 bits and a negative one, which exercise the packer's width selection.
#[aerospike_macro::test]
async fn a_wasm_udf_writes_lists_from_their_packed_form() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_write_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_writes_lists_from_their_packed_form", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "write_lists", &[])
        .await
        .unwrap();

    assert_eq!(result, Some(Value::Int(1)), "both writes were accepted");

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    assert_eq!(
        record.bins.get("w"),
        Some(&Value::List(vec![
            Value::Int(5),
            Value::Int(-1),
            Value::Int(1 << 40)
        ])),
        "the integer list, read back as a list"
    );
    assert_eq!(
        record.bins.get("wd"),
        Some(&Value::List(vec![Value::from(0.5f64), Value::from(-4.0f64)])),
        "the double list"
    );
}

/// **A nested value comes back exactly as stored, without the guest decoding it.**
///
/// `last_of_doc` iterates `doc` and returns the last value it saw - a map - as its result. The guest holds
/// only the value's position; the server unpacks it from the record's own bytes. `copy_vals` does the same
/// through `set`, writing the list under `m.vals` into another bin.
#[aerospike_macro::test]
async fn a_wasm_udf_returns_and_writes_stored_values_by_position() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_pos_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    let mut tail: HashMap<Value, Value> = HashMap::new();
    tail.insert(Value::from("k"), Value::Int(7));
    tail.insert(Value::from("s"), Value::from("seven"));

    let doc = Value::List(vec![
        Value::Int(1),
        Value::List(vec![Value::Int(10), Value::Int(20)]),
        Value::HashMap(tail.clone()),
    ]);

    let mut m: HashMap<Value, Value> = HashMap::new();
    m.insert(
        Value::from("vals"),
        Value::List(vec![Value::Int(3), Value::Int(4), Value::Int(5)]),
    );
    m.insert(Value::from("tag"), Value::from("x"));

    client
        .put(&write, &key, &[as_bin!("doc", doc), as_bin!("m", Value::HashMap(m))])
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_returns_and_writes_stored_values_by_position", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "last_of_doc", &[])
        .await
        .unwrap()
        .expect("last_of_doc returns the map");

    let mut pairs = map_entries(&result);
    pairs.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));

    assert_eq!(
        pairs,
        vec![
            (Value::from("k"), Value::Int(7)),
            (Value::from("s"), Value::from("seven")),
        ],
        "the stored map, unpacked by the server: {result:?}"
    );

    let result = client
        .execute_udf(&write, &key, package, "copy_vals", &[])
        .await
        .unwrap();

    assert_eq!(result, Some(Value::Int(3)), "copy_vals reports the list's length");

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    assert_eq!(
        record.bins.get("copy"),
        Some(&Value::List(vec![Value::Int(3), Value::Int(4), Value::Int(5)])),
        "the list under m.vals, written to another bin from its stored bytes"
    );
}

/// **The argument contract is enforced before the body runs.**
///
/// `add_all` is declared as `(name: &str, numbers: Args)` and `increment` takes nothing. A call that does
/// not match is rejected with a message naming the function, the position and the parameter, and the
/// record is untouched.
#[aerospike_macro::test]
async fn a_wasm_udf_rejects_arguments_that_do_not_match_its_signature() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_sig_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 5)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_rejects_arguments_that_do_not_match_its_signature", &reason);
        return;
    }

    // The wrong kind in the first position.
    let error = client
        .execute_udf(&write, &key, package, "add_all", &[Value::Int(1), Value::Int(2)])
        .await
        .expect_err("an integer where a string is declared must be rejected");
    let text = error.to_string();

    assert!(
        text.contains("add_all: argument 0 (name): expected a string, got an integer"),
        "the rejection names the parameter and the kinds: {text}"
    );

    // Too many arguments for a function that takes none.
    let error = client
        .execute_udf(&write, &key, package, "increment", &[Value::Int(1)])
        .await
        .expect_err("an argument to a function that takes none must be rejected");
    let text = error.to_string();

    assert!(
        text.contains("increment takes at most 0 arguments, got 1"),
        "the rejection names the arity: {text}"
    );

    // And the record was never touched by either.
    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    assert_eq!(record.bins.get("n"), Some(&Value::Int(5)), "rejected before the body ran");

    // A matching call still works, with the rest of the arguments typed as they are used.
    let result = client
        .execute_udf(
            &write,
            &key,
            package,
            "add_all",
            &[Value::from("n"), Value::Int(10), Value::Int(-3)],
        )
        .await
        .unwrap();

    assert_eq!(result, Some(Value::Int(12)), "5 + 10 - 3 through the typed signature");
}

/// **Stored values go into built collections by position, a guest GeoJson stays a GeoJson, and a built map
/// takes integer keys.**
///
/// The fixture's `relist` builds `[doc[1], <geojson>, {1: "one", "first": doc[0]}]`: the nested list and the
/// first element come straight from the record's bytes, with the server building them.
#[aerospike_macro::test]
async fn a_wasm_udf_builds_collections_from_stored_values() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_build_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    let mut tail: HashMap<Value, Value> = HashMap::new();
    tail.insert(Value::from("k"), Value::Int(7));

    let doc = Value::List(vec![
        Value::Int(1),
        Value::List(vec![Value::Int(10), Value::Int(20)]),
        Value::HashMap(tail),
    ]);

    client.put(&write, &key, &[as_bin!("doc", doc)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_builds_collections_from_stored_values", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "relist", &[Value::Int(1)])
        .await
        .unwrap()
        .expect("relist returns a list");

    let Value::List(items) = &result else {
        panic!("expected a list, got {result:?}");
    };

    assert_eq!(items.len(), 3, "three elements: {result:?}");
    assert_eq!(
        items[0],
        Value::List(vec![Value::Int(10), Value::Int(20)]),
        "doc[1], pushed by position: {result:?}"
    );
    assert_eq!(
        items[1],
        Value::GeoJson("{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}".to_string()),
        "a guest geojson is a geojson in the result: {result:?}"
    );

    let mut pairs = map_entries(&items[2]);
    pairs.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));

    assert_eq!(
        pairs,
        vec![
            (Value::Int(1), Value::from("one")),
            (Value::from("first"), Value::Int(1)),
        ],
        "an integer key and a stored value under a string key: {result:?}"
    );
}

/// **An HLL bin reads as an HLL and is written back as one.**
///
/// The fixture's `copy_hll` reads bin `h`, writes it to `h2` by position, and reports its size. The copy must
/// still be an HLL - the server's HLL operations keep working on it - which a guest could not do from bytes,
/// since the subtype that makes it an HLL is not something the ABI carries from guest memory.
#[aerospike_macro::test]
async fn a_wasm_udf_copies_an_hll_bin_intact() {
    use aerospike::operations::hll::{self, HllPolicy, HllWriteFlags};

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_hll_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    let members: Vec<Value> = (0..50).map(|i| Value::from(format!("user-{i}"))).collect();
    client
        .operate(
            &write,
            &key,
            &[hll::add_with_index(&HllPolicy::new(HllWriteFlags::DEFAULT), "h", members, 8)],
        )
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_copies_an_hll_bin_intact", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "copy_hll", &[])
        .await
        .unwrap();

    assert!(matches!(result, Some(Value::Int(n)) if n > 0), "the HLL's size: {result:?}");

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    assert_eq!(
        record.bins.get("h2"),
        record.bins.get("h"),
        "the copy is byte-for-byte the HLL"
    );
    assert!(
        matches!(record.bins.get("h2"), Some(Value::Hll(_))),
        "and still an HLL, not a plain blob: {:?}",
        record.bins.get("h2")
    );

    // The server's own HLL operations accept the copy, which is the proof that matters.
    let counted = client
        .operate(&write, &key, &[hll::get_count("h2")])
        .await
        .unwrap();

    assert!(
        matches!(counted.bins.get("h2"), Some(Value::Int(n)) if *n > 40),
        "get_count on the copy: {:?}",
        counted.bins.get("h2")
    );
}

/// **A WASM UDF takes strings of any length when it says so, and bounds them when it says that.**
///
/// A `String` parameter (the SDK's `alloc` feature) is allocated at the argument's reported length, however
/// long; a `Str<8>` parameter rejects a longer argument before the body runs, naming the limit. Neither
/// goes through the 256-byte buffer a `&str` parameter has.
#[aerospike_macro::test]
async fn a_wasm_udf_sizes_its_string_parameters() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_strs_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_sizes_its_string_parameters", &reason);
        return;
    }

    // Four times what a `&str` parameter holds.
    let long: String = (0..1024u32).map(|i| char::from(b'a' + (i % 26) as u8)).collect();
    let marked = format!("{long}!");
    let result = client
        .execute_udf(&write, &key, package, "echo", &[Value::from(long.as_str())])
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(Value::List(vec![Value::from(marked.as_str()), Value::Int(1025)])),
        "a String parameter holds the whole argument: {result:?}"
    );

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    assert_eq!(
        record.bins.get("echo"),
        Some(&Value::from(marked.as_str())),
        "and a heap string can be written to a bin"
    );

    // One byte over a `Str<8>`.
    let error = client
        .execute_udf(&write, &key, package, "tag", &[Value::from("abcdefghi")])
        .await
        .expect_err("nine bytes into a Str<8> must be rejected");
    let text = error.to_string();

    assert!(
        text.contains("tag: argument 0 (label) is 9 bytes, at most 8"),
        "the rejection names the limit: {text}"
    );

    let result = client
        .execute_udf(&write, &key, package, "tag", &[Value::from("abcdefgh")])
        .await
        .unwrap();

    assert_eq!(result, Some(Value::Int(8)), "eight bytes fit exactly");

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    assert_eq!(record.bins.get("tag"), Some(&Value::from("abcdefgh")), "and were written");
}

/// **A WASM UDF fills built collections a window at a time.**
///
/// `List::extend` and `Map::extend` cross 128 values (64 pairs) per import call instead of one each. 300 of
/// each is three windows for the lists and five for the map, so a window boundary that dropped, doubled or
/// misordered a value would show in the count or the order.
#[aerospike_macro::test]
async fn a_wasm_udf_fills_collections_in_batches() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_bulk_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_fills_collections_in_batches", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "bulk", &[Value::Int(300)])
        .await
        .unwrap()
        .expect("bulk returns a list");

    let Value::List(parts) = &result else {
        panic!("expected a list, got {result:?}");
    };

    assert_eq!(parts.len(), 3, "ints, labels, map: {result:?}");

    let ints: Vec<Value> = (0..300).map(Value::Int).collect();

    assert_eq!(parts[0], Value::List(ints), "300 integers, in order");

    let labels: Vec<Value> = (0..300).map(|i| Value::from(format!("v{i}"))).collect();

    assert_eq!(parts[1], Value::List(labels), "300 guest strings, copied out of guest memory in order");

    let mut pairs = map_entries(&parts[2]);

    assert_eq!(pairs.len(), 300, "300 entries: {:?}", parts[2]);
    pairs.sort_by_key(|(_, v)| match v {
        Value::Int(i) => *i,
        _ => -1,
    });

    for (i, (k, v)) in pairs.iter().enumerate() {
        assert_eq!(
            (k, v),
            (&Value::from(format!("v{i}")), &Value::Int(i as i64)),
            "entry {i}"
        );
    }
}

/// **A WASM UDF copies a stored collection by position, a window at a time.**
///
/// `extend(stored.iter())` hands the host every element's position in batches of 128, and the host builds
/// each from the record's bytes - nothing is decoded in the guest. 200 mixed elements is two windows, with
/// strings, doubles, nils and nested lists among them; the map goes the same way through `entries()`.
#[aerospike_macro::test]
async fn a_wasm_udf_copies_stored_collections_in_batches() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_copy_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    let doc: Vec<Value> = (0..200i64)
        .map(|i| match i % 5 {
            0 => Value::Int(i),
            1 => Value::from(format!("s{i}")),
            2 => Value::List(vec![Value::Int(i), Value::Int(i + 1)]),
            3 => Value::from(i as f64 / 2.0),
            _ => Value::Nil,
        })
        .collect();

    let mut m: HashMap<Value, Value> = HashMap::new();

    m.insert(Value::from("a"), Value::Int(1));
    m.insert(Value::from("b"), Value::List(vec![Value::Int(1), Value::Int(2)]));
    m.insert(Value::from("c"), Value::from("x"));
    m.insert(Value::Int(7), Value::from("seven"));

    client
        .put(
            &write,
            &key,
            &[
                as_bin!("doc", Value::List(doc.clone())),
                as_bin!("m", Value::HashMap(m.clone())),
            ],
        )
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_copies_stored_collections_in_batches", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, package, "copy_list", &[])
        .await
        .unwrap()
        .expect("copy_list returns a list");

    let Value::List(parts) = &result else {
        panic!("expected a list, got {result:?}");
    };

    assert_eq!(parts.len(), 2, "the list copy and the map copy: {result:?}");
    assert_eq!(parts[0], Value::List(doc), "every element, by position, in order");

    let by_key = |pairs: &[(Value, Value)]| {
        let mut sorted: Vec<(Value, Value)> = pairs.to_vec();
        sorted.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));
        sorted
    };
    let want: Vec<(Value, Value)> = m.into_iter().collect();

    assert_eq!(by_key(&map_entries(&parts[1])), by_key(&want), "every entry, by position");
}

/// **A WASM module describes its own exports.**
///
/// Beside every UDF the SDK's macros export `__udf_sig_<name>`, and the host serves it as a string result
/// when it is executed like any other function - without running any body. The grammar is the contract: a
/// client can check a call against it before making it, or generate typed bindings from it.
#[aerospike_macro::test]
async fn a_wasm_module_describes_its_exports() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_desc_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 5)]).await.unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_module_describes_its_exports", &reason);
        return;
    }

    let signatures = [
        ("__udf_sig_increment", "fn increment() -> value"),
        ("__udf_sig_add_all", "fn add_all(name: str[256], numbers: any...) -> value"),
        ("__udf_sig_relist", "fn relist(index: int) -> value"),
        ("__udf_sig_tag", "fn tag(label: str[8]) -> value"),
        ("__udf_sig_echo", "fn echo(text: str) -> value"),
        ("__udf_sig_sum", "stream sum { record() end() }"),
    ];

    for (export, want) in signatures {
        let got = client
            .execute_udf(&write, &key, package, export, &[])
            .await
            .unwrap();

        assert_eq!(got, Some(Value::from(want)), "{export}");
    }

    // Describing `increment` did not run it.
    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    assert_eq!(record.bins.get("n"), Some(&Value::Int(5)), "no body ran");

    // A UDF with no signature export is reported the way a missing function is.
    let error = client
        .execute_udf(&write, &key, package, "__udf_sig_nope", &[])
        .await
        .expect_err("no such export");

    assert!(
        error.to_string().contains("does not export __udf_sig_nope"),
        "names the missing export: {error}"
    );
}
