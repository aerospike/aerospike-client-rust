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
async fn register(
    client: &Client,
    package: &str,
    key: &Key,
) -> std::result::Result<(), String> {
    let policy = AdminPolicy::default();
    let write = WritePolicy::default();

    match client
        .register_udf(&policy, MODULE, &format!("{package}.wasm"), UDFLang::Wasm)
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
        .execute_udf(&write, key, package, "abi_probe", None)
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
    client
        .put(&write, &key, &[as_bin!("n", 10)])
        .await
        .unwrap();

    if let Err(reason) = register(&client, package, &key).await {
        skipped("a_wasm_udf_reads_and_writes_its_record", &reason);
        return;
    }



    // Once, twice: the second call must see what the first wrote.
    for expected in [11i64, 12] {
        let result = client
            .execute_udf(&write, &key, package, "increment", None)
            .await
            .unwrap();

        assert_eq!(
            result,
            Some(Value::Int(expected)),
            "the udf returns the value it wrote"
        );
    }

    let record = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();
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
    client.put(&write, &key, &[as_bin!("other", 1)]).await.unwrap();

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
        .execute_udf(&write, &key, package, "add_all", Some(&args))
        .await
        .unwrap();
    assert_eq!(
        result,
        Some(Value::Int(42)),
        "the arguments arrived in order and as integers"
    );

    // Again, to show it accumulated rather than recomputed.
    let result = client
        .execute_udf(&write, &key, package, "add_all", Some(&args))
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
        .execute_udf(&write, &key, package, "nested", None)
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
        .execute_udf(&write, &key, package, "describe", None)
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
        .execute_udf(&write, &key, package, "no_such_function", None)
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
        .register_udf(&policy, b"this is not a wasm module", name, UDFLang::Wasm)
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
        .execute_udf(&write, &key, package, "sum_list", None)
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

    match client.execute_udf(&write, &key, package, "hog", None).await {
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

    statement.set_aggregate_function(package, "sum", None);

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

    statement.set_aggregate_function(package, "stats", None);

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
    assert_eq!(summed, expected, "and the sum must agree with the scalar aggregation");
}

/// **A GeoJSON bin survives a round trip through a WASM UDF.**
///
/// Until `set_geojson` existed, `mod-wamr` had no GEOJSON kind at all: a geojson bin read as `Type::Other`, its text
/// was unreachable, and the only way to write text back was `set_str` — which keeps the document and silently turns a
/// geospatially indexable bin into an ordinary string. The document would look correct and geo queries would stop
/// matching it.
///
/// So this asserts three things a guest now gets: the bin reports as GeoJSON, its text is readable, and writing it back
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
            &[as_bin!("loc", Value::GeoJSON(doc.to_string()))],
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
        matches!(before, Some(Value::GeoJSON(_))),
        "the bin starts as geojson, got {before:?}"
    );

    let returned = client
        .execute_udf(
            &WritePolicy::default(),
            &key,
            &format!("{package}.wasm"),
            "geo_roundtrip",
            Some(&[as_val!("loc")]),
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
        Some(Value::GeoJSON(doc.to_string())),
        "the bin must still be geojson, with the document unchanged"
    );

    client.delete(&WritePolicy::default(), &key).await.unwrap();
}
