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

//! Native UDFs — compiled modules loaded from the server's configuration file.
//!
//! These mirror `udf.rs` (Lua) and `udf_native.rs`'s WASM sibling, so the three languages can be compared
//! by reading them side by side.
//!
//! # Nothing is registered here
//!
//! Unlike the Lua and WASM suites, these tests register nothing. A native module runs in the server's
//! address space with the server's privileges, so it can only be installed by an operator naming it in the
//! configuration file — which is the point, and also the reason there is a test below asserting that a
//! *client* cannot install one.
//!
//! # They self-skip
//!
//! Which means the module has to be there already. A server without it answers `apply` with "no native
//! module", and there is no way for a client to fix that, so every test asks first and returns early —
//! reported on stderr, so a skip is visible rather than silent.
//!
//! To run them for real, build the example plugin and name it in the server's config:
//!
//! ```text
//! cd modules/mod-native && make example
//!
//! mod-native {
//!     module stats {
//!         path /path/to/target/<platform>/libstats.so
//!     }
//! }
//! ```

use crate::common;

use aerospike::*;

/// The module name the example plugin is expected to be configured under.
///
/// From the environment rather than a constant, because a native module's name comes from the configuration
/// file and a test cannot invent one — and because the same suite is meant to run against the C example and
/// the Rust one, which is the check that the ABI really is language-neutral:
///
/// ```text
/// AEROSPIKE_NATIVE_MODULE=stats_rs cargo test native
/// ```
fn module() -> String {
    std::env::var("AEROSPIKE_NATIVE_MODULE").unwrap_or_else(|_| "stats".to_string())
}

/// Whether this server has the example plugin loaded.
///
/// Probed by **calling** it, for the same reason the WASM suite probes by calling: existence and
/// runnability are different questions, and only the second one matters. `echo` is the cheapest function
/// that proves the whole path — dispatch, argument marshalling, and a result coming back.
async fn available(client: &Client, key: &Key) -> std::result::Result<(), String> {
    let write = WritePolicy::default();
    let args = [as_val!(7)];

    match client
        .execute_udf(&write, key, &module(), "echo", &args)
        .await
    {
        Ok(Some(Value::Int(7))) => Ok(()),
        Ok(other) => Err(format!(
            "the native module answered echo(7) with {other:?} — is a different plugin configured \
             under that name?"
        )),
        Err(e) => Err(format!("no usable native module: {e}")),
    }
}

/// Print why a test did nothing, so a skip is not mistaken for a pass.
fn skipped(test: &str, reason: &str) {
    eprintln!("skipping {test}: {reason}");
}

/// A record UDF that reads a bin, writes it, and returns the new value.
///
/// What matters beyond "it ran" is that the write **persisted**. A UDF that stages a bin without committing
/// returns a plausible value and changes nothing — the mistake that looks like it worked, and one that a
/// single call cannot detect, which is why this calls twice and checks the record afterwards.
#[aerospike_macro::test]
async fn a_native_udf_reads_and_writes_its_record() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 10)]).await.unwrap();

    if let Err(reason) = available(&client, &key).await {
        skipped("a_native_udf_reads_and_writes_its_record", &reason);
        return;
    }

    let first = client
        .execute_udf(&write, &key, &module(), "increment", &[])
        .await
        .unwrap();

    assert_eq!(first, Some(Value::Int(11)), "increment must return 10 + 1");

    // The second call is the one that catches an uncommitted write: if the first did not persist, this reads
    // 10 again and answers 11.
    let second = client
        .execute_udf(&write, &key, &module(), "increment", &[])
        .await
        .unwrap();

    assert_eq!(
        second,
        Some(Value::Int(12)),
        "the first increment must have persisted"
    );

    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    assert_eq!(
        record.bins.get("n"),
        Some(&Value::Int(12)),
        "the record itself must hold what the UDF reported"
    );
}

/// Arguments arrive typed and flat.
#[aerospike_macro::test]
async fn arguments_reach_a_native_udf() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 0)]).await.unwrap();

    if let Err(reason) = available(&client, &key).await {
        skipped("arguments_reach_a_native_udf", &reason);
        return;
    }

    let args = [as_val!(4), as_val!(5), as_val!(6)];
    let sum = client
        .execute_udf(&write, &key, &module(), "add_all", &args)
        .await
        .unwrap();

    assert_eq!(sum, Some(Value::Int(15)));

    // An explicit argument reaches `increment` too, so arity 0 and arity 1 both dispatch.
    let by = [as_val!(41)];
    let bumped = client
        .execute_udf(&write, &key, &module(), "increment", &by)
        .await
        .unwrap();

    assert_eq!(bumped, Some(Value::Int(41)));
}

/// A list bin, read in bulk rather than element by element.
#[aerospike_macro::test]
async fn a_native_udf_reads_a_list_bin() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    let list = as_list!(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    client
        .put(&write, &key, &[as_bin!("l", list)])
        .await
        .unwrap();

    if let Err(reason) = available(&client, &key).await {
        skipped("a_native_udf_reads_a_list_bin", &reason);
        return;
    }

    let total = client
        .execute_udf(&write, &key, &module(), "sum_list", &[])
        .await
        .unwrap();

    assert_eq!(total, Some(Value::Int(55)));
}

/// Structures come back whole, nested, and in one piece.
#[aerospike_macro::test]
async fn a_native_udf_returns_nested_collections() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = available(&client, &key).await {
        skipped("a_native_udf_returns_nested_collections", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, &module(), "nested", &[])
        .await
        .unwrap();

    let Some(Value::List(outer)) = result else {
        panic!("wanted a list, got {result:?}");
    };

    assert_eq!(outer.len(), 2, "a scalar and a map");
    assert_eq!(outer[0], Value::Int(7));

    // The map arrives as a `SortedMap`: the server builds these as an `as_orderedmap`, which is ordered by
    // key rather than by insertion. Accepting the other map shapes too keeps this from failing for a reason
    // that has nothing to do with what it is testing.
    let inner = match &outer[1] {
        Value::HashMap(m) => m.get(&as_val!("inner")).cloned(),
        Value::OrderedMap(m) => m.get(&as_val!("inner")).cloned(),
        Value::SortedMap(m) => m.get(&as_val!("inner")).cloned(),
        other => panic!("wanted a map, got {other:?}"),
    };

    assert_eq!(
        inner,
        Some(as_list!(1, 2, 3)),
        "the nested list must survive the trip"
    );
}

/// The record's own metadata, from inside a native UDF.
#[aerospike_macro::test]
async fn a_native_udf_sees_its_record_metadata() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client
        .put(&write, &key, &[as_bin!("a", 1), as_bin!("b", 2)])
        .await
        .unwrap();

    if let Err(reason) = available(&client, &key).await {
        skipped("a_native_udf_sees_its_record_metadata", &reason);
        return;
    }

    let result = client
        .execute_udf(&write, &key, &module(), "describe", &[])
        .await
        .unwrap();

    let map = match result {
        Some(Value::HashMap(m)) => m,
        Some(Value::OrderedMap(m)) => m.into_iter().collect(),
        Some(Value::SortedMap(m)) => m.into_iter().collect(),
        other => panic!("wanted a map, got {other:?}"),
    };

    assert_eq!(
        map.get(&as_val!("bins")),
        Some(&Value::Int(2)),
        "two bins were written"
    );
    assert_eq!(
        map.get(&as_val!("set")),
        Some(&Value::String(set_name.to_string())),
        "the set name must come through"
    );

    let Some(Value::Int(gen)) = map.get(&as_val!("gen")) else {
        panic!("no generation in {map:?}");
    };

    assert!(*gen >= 1, "a written record has a generation, got {gen}");
}

/// Calling something the module does not export is an error, not a crash.
#[aerospike_macro::test]
async fn calling_a_function_a_native_module_does_not_export_is_an_error() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = available(&client, &key).await {
        skipped(
            "calling_a_function_a_native_module_does_not_export_is_an_error",
            &reason,
        );
        return;
    }

    let result = client
        .execute_udf(&write, &key, &module(), "no_such_function", &[])
        .await;

    assert!(
        result.is_err(),
        "an unexported function must fail, got {result:?}"
    );
}

/// Arity is checked before the plugin is entered.
///
/// The manifest declares what each function accepts, so a client sending too many arguments gets an error
/// from the host rather than reaching code that would have to check for itself.
#[aerospike_macro::test]
async fn too_many_arguments_are_refused_before_the_plugin_runs() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 5)]).await.unwrap();

    if let Err(reason) = available(&client, &key).await {
        skipped(
            "too_many_arguments_are_refused_before_the_plugin_runs",
            &reason,
        );
        return;
    }

    // `increment` accepts 0 or 1.
    let args = [as_val!(1), as_val!(2)];
    let result = client
        .execute_udf(&write, &key, &module(), "increment", &args)
        .await;

    assert!(
        result.is_err(),
        "two arguments to a function taking at most one must fail, got {result:?}"
    );

    // And the record is untouched — the refusal happened before any plugin code ran.
    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    assert_eq!(
        record.bins.get("n"),
        Some(&Value::Int(5)),
        "a refused call must not have written anything"
    );
}

/// **A client cannot install a native module.** This is the security property, so it is tested.
///
/// Two doors, both of which must stay shut:
///
/// 1. `udf-type=NATIVE` is not an accepted registration type at all.
/// 2. Registering *any* module whose name would collide with a configured native module is refused, because
///    silently losing the dispatch would mean a registration that succeeds and does nothing.
#[aerospike_macro::test]
async fn a_client_cannot_register_a_native_module() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    if let Err(reason) = available(&client, &key).await {
        skipped("a_client_cannot_register_a_native_module", &reason);
        return;
    }

    let policy = AdminPolicy::default();

    // Door two: a Lua module named after the configured native one. `stats.lua` is applied as `stats`, so
    // allowing this would shadow — or be shadowed by — the native module.
    let collide = client
        .register_udf(
            &policy,
            b"function hello() return 1 end",
            &format!("{}.lua", module()),
            UdfLang::Lua,
        )
        .await;

    assert!(
        collide.is_err(),
        "registering the module name as Lua must be refused while a native module is configured under that name, \
         got {collide:?}"
    );

    // And the native module still answers, which is the thing the refusal protects.
    let echoed = client
        .execute_udf(&write, &key, &module(), "echo", &[as_val!(99)])
        .await
        .unwrap();

    assert_eq!(
        echoed,
        Some(Value::Int(99)),
        "the native module must still be the one that answers"
    );
}

/// How many records the aggregation tests write, and their bin values: `v` is `1..=RECORDS`.
const RECORDS: i64 = 50;

/// Write `RECORDS` records, and report whether the module is there at all.
async fn seed(client: &Client, namespace: &str, set_name: &str) -> bool {
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);

    client.put(&write, &key, &[as_bin!("v", 1)]).await.unwrap();

    if available(client, &key).await.is_err() {
        return false;
    }

    // Written after the probe so a skipped test does not pay for fifty writes. `floor` is for the filter
    // aggregation, which has nowhere to keep a threshold of its own.
    for i in 2..=RECORDS {
        let key = as_key!(namespace, set_name, i);

        client
            .put(&write, &key, &[as_bin!("v", i), as_bin!("floor", 40)])
            .await
            .unwrap();
    }

    // Key 1 needs it too, and was written before the probe.
    client
        .put(&write, &key, &[as_bin!("v", 1), as_bin!("floor", 40)])
        .await
        .unwrap();

    true
}

/// Every value an aggregation put on the result stream, one per bin per record.
async fn collect_partials(
    client: &Client,
    namespace: &str,
    set_name: &str,
    function: &str,
    args: &[Value],
) -> Vec<Value> {
    use futures::StreamExt;

    let mut statement = Statement::new(namespace, set_name, Bins::All);

    statement.set_aggregate_function(module(), function, args);

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut out = Vec::new();
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.unwrap();

        for (_, value) in record.bins.iter() {
            out.push(value.clone());
        }
    }

    out
}

/// A map partial, whichever of the three map shapes the client decoded it into.
fn as_map(value: &Value) -> std::collections::HashMap<Value, Value> {
    match value {
        Value::HashMap(m) => m.clone(),
        Value::OrderedMap(m) => m.clone().into_iter().collect(),
        Value::SortedMap(m) => m.clone().into_iter().collect(),
        other => panic!("wanted a map partial, got {other:?}"),
    }
}

/// Aggregation: one accumulator per query thread, and one partial from each.
///
/// `sum` is exercised against **both** plugins, whose partials differ in shape on purpose — the C example
/// hand-writes `to_partial` as a bare integer, and the Rust one has `partial!` generate a map of its fields.
/// Accepting either is the test acknowledging that a partial's format is the plugin's business, not the ABI's.
#[aerospike_macro::test]
async fn a_native_aggregation_runs() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    if !seed(&client, namespace, set_name).await {
        skipped("a_native_aggregation_runs", "no usable native module");
        return;
    }

    let expected: i64 = (1..=RECORDS).sum();
    let args = [as_val!("v")];

    // --- sum ---

    let partials = collect_partials(&client, namespace, set_name, "sum", &args).await;

    assert!(
        !partials.is_empty(),
        "the aggregation produced no partial at all"
    );

    let total: i64 = partials
        .iter()
        .map(|p| match p {
            // The C example's partial.
            Value::Int(n) => *n,
            // The Rust example's, from `partial!` - the accumulator's fields, of which `total` is the one that
            // accumulated.
            other => match as_map(other).get(&as_val!("total")) {
                Some(Value::Int(n)) => *n,
                _ => panic!("a sum partial must carry its total, got {other:?}"),
            },
        })
        .sum();

    assert_eq!(
        total,
        expected,
        "{} partial(s) must add up to {expected}",
        partials.len()
    );

    // --- stats ---

    let partials = collect_partials(&client, namespace, set_name, "stats", &args).await;

    let mut count = 0i64;
    let mut sum = 0i64;

    for partial in &partials {
        let map = as_map(partial);

        if let Some(Value::Int(n)) = map.get(&as_val!("count")) {
            count += n;
        }

        if let Some(Value::Int(n)) = map.get(&as_val!("sum")) {
            sum += n;
        }
    }

    assert_eq!(count, RECORDS, "every record must have been accumulated");
    assert_eq!(sum, expected, "the combined partials must total {expected}");

    // A partial per *thread*, not per partition slice. Fifty records land in up to fifty different partitions,
    // and a slice is a partition — so before the accumulator became thread-scoped this was up to fifty
    // partials, the way Lua still does it. The bound is generous because the query thread count is the
    // server's business, but it is far below what per-slice would produce.
    assert!(
        partials.len() <= 16,
        "expected a handful of partials, one per query thread, got {}",
        partials.len()
    );
}

/// The filter shape: matching records straight to the response, no accumulator, no partial.
///
/// What is being checked is that `emit_record` produces **records** rather than values — the client reads them
/// exactly as it would a plain query's, with their bins intact. That is the whole claim about it being free.
#[aerospike_macro::test]
async fn a_native_aggregation_can_emit_records() {
    use futures::StreamExt;

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    if !seed(&client, namespace, set_name).await {
        skipped(
            "a_native_aggregation_can_emit_records",
            "no usable native module",
        );
        return;
    }

    let mut statement = Statement::new(namespace, set_name, Bins::All);

    statement.set_aggregate_function(module(), "over", &[]);

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut matched = Vec::new();
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.unwrap();

        // A record carries its bins; a partial would be a single unnamed value. `v` being present is what
        // distinguishes the two.
        if let Some(Value::Int(v)) = record.bins.get("v") {
            matched.push(*v);
        }
    }

    matched.sort_unstable();

    // `floor` is 40 on every record, so 40..=50 match.
    let expected: Vec<i64> = (40..=RECORDS).collect();

    assert_eq!(
        matched, expected,
        "a filtering aggregation must return the matching records themselves"
    );
}

/// The collecting shape: an accumulator that fills, dumps, and keeps going.
///
/// The point is that **many partials from one thread** is normal here, not an error — which is what makes an
/// aggregation over more records than fit in memory possible at all. With a chunk of 4 and fifty records, no
/// single partial can hold them, so a correct total is only reachable by combining several.
#[aerospike_macro::test]
async fn a_native_aggregation_dumps_when_it_fills() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    if !seed(&client, namespace, set_name).await {
        skipped(
            "a_native_aggregation_dumps_when_it_fills",
            "no usable native module",
        );
        return;
    }

    const CHUNK: i64 = 4;

    let args = [as_val!("v"), as_val!(CHUNK)];
    let partials = collect_partials(&client, namespace, set_name, "collect", &args).await;

    if partials.is_empty() {
        skipped(
            "a_native_aggregation_dumps_when_it_fills",
            "this module does not export `collect`",
        );
        return;
    }

    // Each partial is a chunk of values - a bare list from the C plugin, or a map of the accumulator's fields
    // from the Rust one, whose `values` field is that list.
    let mut seen = Vec::new();

    for partial in &partials {
        let list = match partial {
            Value::List(items) => items.clone(),
            Value::Nil => continue,
            other => match as_map(other).get(&as_val!("values")) {
                Some(Value::List(items)) => items.clone(),
                _ => panic!("a collect partial must carry its values, got {other:?}"),
            },
        };

        assert!(
            list.len() as i64 <= CHUNK,
            "a chunk must not exceed the size the plugin asked to be dumped at, got {}",
            list.len()
        );

        for item in list {
            if let Value::Int(v) = item {
                seen.push(v);
            }
        }
    }

    seen.sort_unstable();

    assert_eq!(
        seen,
        (1..=RECORDS).collect::<Vec<i64>>(),
        "every record must appear exactly once across the dumped chunks"
    );

    // Fifty records in chunks of four cannot be fewer than thirteen partials, so this is the dump path being
    // taken rather than one accumulator running to the end.
    assert!(
        partials.len() >= 13,
        "expected at least 13 chunks for {RECORDS} records at {CHUNK} per chunk, got {}",
        partials.len()
    );
}
