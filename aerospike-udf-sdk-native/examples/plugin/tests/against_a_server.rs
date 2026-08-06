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

//! A client combining a **real server's** partials with this plugin's own code.
//!
//! This is the claim the whole aggregation design rests on — "the client uses the exact same code, statically
//! linked" — so it is tested against a live server rather than asserted. The `Stats` type here is the type the
//! server ran, from this crate, compiled once as a `cdylib` for `asd` and once as a `lib` for this test.
//!
//! # Why it lives here and not in the main test suite
//!
//! Depending on this crate from the repository root would make it a second workspace root inside the root
//! workspace, which cargo refuses — and the alternative, folding a plugin that builds a shared object with its
//! own release profile into the client workspace, would cost every client build a warning about an ignored
//! profile. So the test lives with the code it tests:
//!
//! ```sh
//! cd aerospike-udf-sdk-native/examples/plugin
//! AEROSPIKE_HOSTS=… AEROSPIKE_USE_SERVICES_ALTERNATE=true cargo test
//! ```
//!
//! `make native-udf-test` at the repository root does exactly that.
//!
//! # It self-skips
//!
//! Like the rest of the native suite: a native module is installed by an operator, so a test cannot put one
//! there. Every skip is reported on stderr rather than passing quietly.

use std::collections::HashMap;
use std::env;

use aerospike_core::{
    as_bin, as_key, as_val, Bins, Client, ClientPolicy, PartitionFilter, QueryPolicy, Statement,
    Value, WritePolicy,
};
use aerospike_udf_example_native::Stats;
use aerospike_udf_sdk_native::client::Combiner;

use futures::StreamExt;

/// How many records the aggregation runs over. `v` is `1..=RECORDS`.
const RECORDS: i64 = 50;

/// The module name this plugin is expected to be configured under.
fn module() -> String {
    env::var("AEROSPIKE_NATIVE_MODULE").unwrap_or_else(|_| "stats_rs".to_string())
}

fn hosts() -> String {
    env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string())
}

fn namespace() -> String {
    env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

fn skipped(reason: &str) {
    eprintln!("skipping a_client_combines_a_servers_partials: {reason}");
}

async fn connect() -> Option<Client> {
    let mut policy = ClientPolicy::default();

    // The VM advertises an address a host machine cannot reach, which is what this is for - the same setting
    // the main suite needs.
    policy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
        .unwrap_or(false);

    Client::new(&policy, &hosts()).await.ok()
}

/// A map value, whichever of the three map shapes the client decoded it into.
fn as_map(value: &Value) -> HashMap<Value, Value> {
    match value {
        Value::HashMap(m) => m.clone(),
        Value::OrderedMap(m) => m.clone().into_iter().collect(),
        Value::SortedMap(m) => m.clone().into_iter().collect(),
        other => panic!("wanted a map, got {other:?}"),
    }
}

#[tokio::test]
async fn a_client_combines_a_servers_partials() {
    let Some(client) = connect().await else {
        skipped(&format!("could not connect to {}", hosts()));
        return;
    };

    let namespace = namespace();
    let set_name = format!("combine_{}", std::process::id());

    let write = WritePolicy::default();

    // Probe by calling, because existence and runnability are different questions.
    let probe = as_key!(namespace.as_str(), set_name.as_str(), 1i64);

    client
        .put(&write, &probe, &[as_bin!("v", 1)])
        .await
        .unwrap();

    if let Err(e) = client
        .execute_udf(&write, &probe, &module(), "echo", Some(&[as_val!(7)]))
        .await
    {
        skipped(&format!("no usable native module '{}': {e}", module()));
        return;
    }

    for i in 2..=RECORDS {
        let key = as_key!(namespace.as_str(), set_name.as_str(), i);

        client.put(&write, &key, &[as_bin!("v", i)]).await.unwrap();
    }

    // --- what the server sends ---

    let args = [as_val!("v")];
    let mut statement = Statement::new(namespace.as_str(), set_name.as_str(), Bins::All);

    statement.set_aggregate_function(&module(), "stats", Some(&args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut partials = Vec::new();
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        for (_, value) in record.unwrap().bins.iter() {
            partials.push(value.clone());
        }
    }

    assert!(
        !partials.is_empty(),
        "the server produced no partial at all"
    );

    // --- what the client does with them ---

    let mut combiner = Combiner::<Stats>::new(&args).expect("could not build a combiner");

    for partial in &partials {
        // A partial the *server* produced, read back by the code that wrote it. A C plugin's partial has a
        // different shape, so this only holds for the Rust module - reported rather than silently passed.
        if let Err(e) = combiner.absorb(partial) {
            skipped(&format!(
                "'{}' is not this plugin, so its partials are a different shape: {e}",
                module()
            ));
            return;
        }
    }

    assert_eq!(
        combiner.absorbed(),
        partials.len(),
        "every partial the server sent must have been absorbed"
    );

    let answer = combiner.finish().unwrap();
    let map = as_map(&answer);

    let expected: i64 = (1..=RECORDS).sum();

    assert_eq!(map.get(&as_val!("count")), Some(&Value::Int(RECORDS)));
    assert_eq!(map.get(&as_val!("sum")), Some(&Value::Int(expected)));
    assert_eq!(
        map.get(&as_val!("min")),
        Some(&Value::Int(1)),
        "min across every partial, which no single one could give"
    );
    assert_eq!(map.get(&as_val!("max")), Some(&Value::Int(RECORDS)));

    // The mean: computed here, on fully merged data, by this plugin's own `finalize`. It was in no partial and
    // no node could have produced it - which is the whole reason `to_partial` and `finalize` are separate.
    let Some(Value::Float(mean)) = map.get(&as_val!("mean")) else {
        panic!("no mean in {map:?}");
    };

    assert_eq!(
        f64::from(mean.clone()),
        expected as f64 / RECORDS as f64,
        "the mean must come from every record, not from one node's share"
    );

    eprintln!(
        "combined {} server partial(s) into count={RECORDS} sum={expected}",
        partials.len()
    );
}

/// **An HLL bin survives a native UDF that tries to write bytes over it.**
///
/// A blob particle carries a subtype — `AS_BYTES_HLL` for an HLL — and the server maps that subtype straight onto the
/// stored particle type. The native ABI flattens a value to a type, a length and a pointer, with nowhere to put it,
/// and `as_bytes_init_wrap` stamps `AS_BYTES_BLOB` on everything the plugin hands back. So the prediction is that
/// copying an HLL bin yields a bin holding the same bytes under particle type BLOB rather than HLL — and that every
/// later HLL operation on it fails.
///
/// That is what it used to do, verified live: 3083 bytes identical, particle type HLL -> BLOB, and `get_count`
/// failing afterwards with BinTypeError. `set_bin` now refuses instead — a plugin cannot carry the subtype back, so it
/// is not allowed to destroy it — and this asserts the refusal *and* that the bin is untouched.
///
/// A plain blob bin is written in the same test as a control, because a guard that also blocks ordinary blob writes
/// would be worse than the bug.
#[tokio::test]
async fn an_hll_bin_survives_a_native_blob_write() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("hll_probe_{}", std::process::id());
    let key = as_key!(namespace.as_str(), set.as_str(), "h");
    let write = WritePolicy::default();

    let _ = client.delete(&write, &key).await;

    // A genuine HLL bin, built by the server's own HLL machinery.
    let policy = aerospike_core::operations::hll::HLLPolicy::default();
    let ops = [
        aerospike_core::operations::hll::init(&policy, "h", 12),
    ];

    client.operate(&write, &key, &ops).await.expect("hll init");

    let ops = [aerospike_core::operations::hll::add(
        &policy,
        "h",
        vec![as_val!("a"), as_val!("b"), as_val!("c")],
    )];

    client.operate(&write, &key, &ops).await.expect("hll add");

    // Whatever the client reports for an untouched HLL bin is the reference - no assumption about how it surfaces.
    let before = client
        .get(&aerospike_core::ReadPolicy::default(), &key, Bins::All)
        .await
        .expect("read back")
        .bins
        .get("h")
        .cloned()
        .expect("h present");

    eprintln!("\n  HLL bin as written:  {before:?}");

    // The count, which is what actually breaks if the type is lost.
    let count_ops = [aerospike_core::operations::hll::get_count("h")];
    let count_before = client.operate(&write, &key, &count_ops).await;

    eprintln!("  get_count before:    {:?}", count_before.as_ref().map(|r| r.bins.get("h").cloned()));

    // Now the round trip through a native UDF.
    let module = env::var("AEROSPIKE_NATIVE_MODULE").unwrap_or_else(|_| "stats_rs".to_string());

    let copied = client
        .execute_udf(&write, &key, &module, "copy_bin", Some(&[as_val!("h"), as_val!("h2")]))
        .await;

    eprintln!("  copy_bin h -> h2:    {copied:?}");

    // **Copying to a *new* bin is allowed, and should be.** Nothing is being relabelled: the caller asked for a blob
    // holding those bytes and got one. The guard is deliberately narrow - it fires only when a write would change what
    // an existing bin *is*.
    assert!(copied.is_ok(), "copying HLL bytes into a fresh blob bin is legitimate, got {copied:?}");

    let after = client
        .get(&aerospike_core::ReadPolicy::default(), &key, Bins::All)
        .await
        .expect("read after")
        .bins
        .clone();

    assert!(
        matches!(after.get("h2"), Some(aerospike_core::Value::Blob(_))),
        "the copy is a plain blob, which is what was asked for"
    );

    assert!(
        matches!(after.get("h"), Some(aerospike_core::Value::HLL(_))),
        "and the original is untouched, got {:?}",
        after.get("h")
    );

    // **The case that used to destroy data**: writing back over the HLL bin itself.
    let in_place = client
        .execute_udf(&write, &key, &module, "copy_bin", Some(&[as_val!("h"), as_val!("h")]))
        .await;

    eprintln!("  copy_bin h -> h:     {in_place:?}");

    assert!(
        in_place.is_err(),
        "writing a plain blob over an existing HLL bin must be refused, got {in_place:?}"
    );

    let survived = client
        .get(&aerospike_core::ReadPolicy::default(), &key, Bins::All)
        .await
        .expect("read after refusal")
        .bins
        .clone();

    assert!(
        matches!(survived.get("h"), Some(aerospike_core::Value::HLL(_))),
        "the refused write must leave the HLL intact, got {:?}",
        survived.get("h")
    );

    // And it still behaves as an HLL, which is the property that actually matters.
    let count_after = client
        .operate(&write, &key, &[aerospike_core::operations::hll::get_count("h")])
        .await
        .expect("get_count must still work after the refusal");

    eprintln!("  get_count after:     {:?}", count_after.bins.get("h"));
    assert_eq!(count_after.bins.get("h"), Some(&aerospike_core::Value::Int(3)));

    // **The control**: an ordinary blob bin must still be writable, or the guard is worse than the bug.
    client
        .put(&write, &key, &[as_bin!("plain", vec![1u8, 2, 3, 4])])
        .await
        .expect("write a plain blob bin");

    let plain = client
        .execute_udf(&write, &key, &module, "copy_bin", Some(&[as_val!("plain"), as_val!("plain2")]))
        .await;

    eprintln!("  copy_bin plain:      {plain:?}");
    assert!(plain.is_ok(), "an ordinary blob must still round-trip, got {plain:?}");

    let final_bins = client
        .get(&aerospike_core::ReadPolicy::default(), &key, Bins::All)
        .await
        .expect("final read")
        .bins
        .clone();

    assert_eq!(
        final_bins.get("plain2"),
        Some(&aerospike_core::Value::Blob(vec![1, 2, 3, 4])),
        "the plain blob copy must have landed"
    );

    eprintln!("  plain2:              {:?}\n", final_bins.get("plain2"));

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &namespace, &set, 0)
        .await;
}
