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

//! JSONPath against real records.
//!
//! Everything here needs a record, which is the half the unit tests cannot reach: the lazy adapter only does
//! anything when there is a record behind it. Run with the module configured:
//!
//! ```text
//! mod-native {
//!     module jsonpath {
//!         path /path/to/target/release/libaerospike_udf_example_jsonpath.so
//!     }
//! }
//! ```
//!
//! ```sh
//! AEROSPIKE_HOSTS=… AEROSPIKE_USE_SERVICES_ALTERNATE=true cargo test
//! ```
//!
//! Every test self-skips with a note on stderr if the module is not there — a native module is installed by an
//! operator, so a test cannot put one in place.

use std::collections::HashMap;
use std::env;

use aerospike_core::{
    as_bin, as_key, as_list, as_map, as_val, Bins, Client, ClientPolicy, Key, PartitionFilter,
    QueryPolicy, ReadPolicy, Statement, Value, WritePolicy,
};

use futures::StreamExt;

fn module() -> String {
    env::var("AEROSPIKE_JSONPATH_MODULE").unwrap_or_else(|_| "jsonpath".to_string())
}

fn hosts() -> String {
    env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string())
}

fn namespace() -> String {
    env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

fn skipped(test: &str, reason: &str) {
    eprintln!("skipping {test}: {reason}");
}

async fn connect() -> Option<Client> {
    let mut policy = ClientPolicy::default();

    policy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
        .unwrap_or(false);

    Client::new(&policy, &hosts()).await.ok()
}

/// One record shaped like a small JSON document, and the key to it.
///
/// Nested on purpose: a map inside the record, a list of maps inside that. A path that only reaches `$.id` must
/// not walk any of it, which is the property the whole adapter exists for — and a path that reaches
/// `$.orders[?…]` must walk exactly the part it names.
async fn seed(client: &Client, namespace: &str, set: &str) -> Key {
    let write = WritePolicy::default();
    let key = as_key!(namespace, set, 1i64);

    let address = as_map!("city" => "Amsterdam", "zip" => "1011");

    let orders = as_list!(
        as_map!("id" => 10i64, "total" => 45i64, "tag" => "book"),
        as_map!("id" => 11i64, "total" => 150i64, "tag" => "laptop"),
        as_map!("id" => 12i64, "total" => 990i64, "tag" => "phone")
    );

    client
        .put(
            &write,
            &key,
            &[
                as_bin!("id", 1i64),
                as_bin!("name", "Ada"),
                as_bin!("score", 2.5f64),
                as_bin!("active", true),
                as_bin!("address", address),
                as_bin!("orders", orders),
                as_bin!("tags", as_list!("a", "b", "c")),
                // Doubles, for the cross-type comparison a filter has to get right.
                as_bin!("scores", as_list!(1.5f64, 20.5f64)),
            ],
        )
        .await
        .unwrap();

    key
}

async fn available(client: &Client, key: &Key) -> bool {
    client
        .execute_udf(
            &WritePolicy::default(),
            key,
            &module(),
            "jsonpath_exists",
            Some(&[as_val!("$.id")]),
        )
        .await
        .is_ok()
}

/// Run `jsonpath` and get the matches as a list.
async fn query(client: &Client, key: &Key, path: &str) -> Vec<Value> {
    match client
        .execute_udf(
            &WritePolicy::default(),
            key,
            &module(),
            "jsonpath",
            Some(&[as_val!(path)]),
        )
        .await
    {
        Ok(Some(Value::List(items))) => items,
        Ok(other) => panic!("wanted a list for {path}, got {other:?}"),
        Err(e) => panic!("{path} failed: {e}"),
    }
}

fn as_map_of(value: &Value) -> HashMap<Value, Value> {
    match value {
        Value::HashMap(m) => m.clone(),
        Value::OrderedMap(m) => m.clone().into_iter().collect(),
        Value::SortedMap(m) => m.clone().into_iter().collect(),
        other => panic!("wanted a map, got {other:?}"),
    }
}

/// The whole point in one test: a record is a JSON document, and its bins are the root's keys.
#[tokio::test]
async fn a_path_reads_a_record_as_json() {
    let Some(client) = connect().await else {
        skipped("a_path_reads_a_record_as_json", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_read_{}", std::process::id());
    let key = seed(&client, &namespace, &set).await;

    if !available(&client, &key).await {
        skipped("a_path_reads_a_record_as_json", "no usable jsonpath module");
        return;
    }

    // A scalar bin at the root.
    assert_eq!(query(&client, &key, "$.id").await, vec![Value::Int(1)]);
    assert_eq!(
        query(&client, &key, "$.name").await,
        vec![Value::String("Ada".to_string())]
    );
    assert_eq!(query(&client, &key, "$.active").await, vec![Value::Bool(true)]);

    // A key inside a map bin - two levels, only one of which is a bin.
    assert_eq!(
        query(&client, &key, "$.address.city").await,
        vec![Value::String("Amsterdam".to_string())]
    );

    // Every element of a list bin.
    assert_eq!(
        query(&client, &key, "$.tags[*]").await,
        vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ]
    );

    // One element by index, and by negative index.
    assert_eq!(
        query(&client, &key, "$.tags[0]").await,
        vec![Value::String("a".to_string())]
    );
    assert_eq!(
        query(&client, &key, "$.tags[-1]").await,
        vec![Value::String("c".to_string())]
    );

    // A path that matches nothing is an empty list, not an error - a document simply may not have that key.
    assert!(query(&client, &key, "$.nosuchbin").await.is_empty());
}

/// Filters, which is the reason to want JSONPath rather than a bin read.
#[tokio::test]
async fn a_filter_selects_inside_a_list_of_maps() {
    let Some(client) = connect().await else {
        skipped("a_filter_selects_inside_a_list_of_maps", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_filter_{}", std::process::id());
    let key = seed(&client, &namespace, &set).await;

    if !available(&client, &key).await {
        skipped("a_filter_selects_inside_a_list_of_maps", "no usable jsonpath module");
        return;
    }

    // The ids of orders over 100 - a comparison inside a list of maps, which needs the list expanded and each
    // map's `total` read.
    let mut ids = query(&client, &key, "$.orders[?@.total > 100].id").await;

    ids.sort();

    assert_eq!(ids, vec![Value::Int(11), Value::Int(12)]);

    // A string comparison.
    assert_eq!(
        query(&client, &key, "$.orders[?@.tag == 'book'].id").await,
        vec![Value::Int(10)]
    );

    // Recursive descent: every `total` at any depth, wherever it lives.
    let mut totals = query(&client, &key, "$..total").await;

    totals.sort();

    assert_eq!(
        totals,
        vec![Value::Int(45), Value::Int(150), Value::Int(990)],
        "recursive descent has to reach into a list bin's maps"
    );

    // A double compared against an *integer* literal, which `serde_json`'s Queryable would not match. `@` on
    // its own is the element being tested, so this reads "elements of `scores` greater than ten".
    assert_eq!(
        query(&client, &key, "$.scores[?@ > 10]").await,
        vec![Value::from(20.5f64)],
        "an integer literal must compare against a double element - see Node's as_f64"
    );

    // **Note what is *not* here: a filter on the root itself.** `$[?@.score == 2.5]` does not mean "the record,
    // if its score is 2.5" - RFC 9535 applies a filter to the *members* of the object it follows, binding `@` to
    // each member in turn. So a record-level predicate is written as a path that matches something when the
    // record qualifies, which is what `jsonpath_filter` below relies on.
}

/// `jsonpath_one` and `jsonpath_exists` — the two shapes that are not "give me everything".
#[tokio::test]
async fn one_and_exists() {
    let Some(client) = connect().await else {
        skipped("one_and_exists", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_one_{}", std::process::id());
    let key = seed(&client, &namespace, &set).await;

    if !available(&client, &key).await {
        skipped("one_and_exists", "no usable jsonpath module");
        return;
    }

    let write = WritePolicy::default();

    // A string result: the interesting case, because it has to be copied into memory the host owns before it can
    // be a result at all.
    let first = client
        .execute_udf(&write, &key, &module(), "jsonpath_one", Some(&[as_val!("$.name")]))
        .await
        .unwrap();

    assert_eq!(first, Some(Value::String("Ada".to_string())));

    // No match is nil, not an error.
    let none = client
        .execute_udf(&write, &key, &module(), "jsonpath_one", Some(&[as_val!("$.nope")]))
        .await
        .unwrap();

    // `None`, not `Some(Nil)`: the client collapses a nil UDF result to no value at all. Worth pinning,
    // because "no match" and "the UDF returned nothing" are indistinguishable from here and a caller has to
    // know that.
    assert_eq!(none, None);

    for (path, expected) in [("$.address.city", true), ("$.address.country", false)] {
        let got = client
            .execute_udf(&write, &key, &module(), "jsonpath_exists", Some(&[as_val!(path)]))
            .await
            .unwrap();

        assert_eq!(got, Some(Value::Bool(expected)), "exists({path})");
    }
}

/// A malformed path is an error, not an empty result.
///
/// The distinction matters: a typo in a path that answered "no matches" would be a silent bug in whatever asked.
#[tokio::test]
async fn a_bad_path_is_an_error() {
    let Some(client) = connect().await else {
        skipped("a_bad_path_is_an_error", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_bad_{}", std::process::id());
    let key = seed(&client, &namespace, &set).await;

    if !available(&client, &key).await {
        skipped("a_bad_path_is_an_error", "no usable jsonpath module");
        return;
    }

    let result = client
        .execute_udf(
            &WritePolicy::default(),
            &key,
            &module(),
            "jsonpath",
            Some(&[as_val!("$.[[[nonsense")]),
        )
        .await;

    assert!(result.is_err(), "a malformed path must fail, got {result:?}");
}

/// **An ambiguous map key fails the call rather than answering it.**
///
/// A map holding both the integer key `5` and the string key `"5"` cannot be addressed by a JSONPath name: both
/// render to `'5'`. Picking one would be a wrong answer that looks right, so the adapter records the collision
/// during expansion and the UDF turns it into an error afterwards — the trait itself has no error channel.
#[tokio::test]
async fn an_ambiguous_map_key_is_refused() {
    let Some(client) = connect().await else {
        skipped("an_ambiguous_map_key_is_refused", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_ambig_{}", std::process::id());
    let write = WritePolicy::default();

    // A clean record first, to establish the module is there.
    let key = seed(&client, &namespace, &set).await;

    if !available(&client, &key).await {
        skipped("an_ambiguous_map_key_is_refused", "no usable jsonpath module");
        return;
    }

    // Now one whose map is ambiguous: the number 5 and the string "5" both render to the name '5'.
    let bad = as_key!(namespace.as_str(), set.as_str(), 2i64);
    let mut ambiguous = HashMap::new();

    ambiguous.insert(Value::Int(5), Value::String("by number".to_string()));
    ambiguous.insert(Value::String("5".to_string()), Value::String("by name".to_string()));

    client
        .put(
            &write,
            &bad,
            &[as_bin!("id", 2i64), as_bin!("m", Value::HashMap(ambiguous))],
        )
        .await
        .unwrap();

    let result = client
        .execute_udf(&write, &bad, &module(), "jsonpath", Some(&[as_val!("$.m['5']")]))
        .await;

    let Err(e) = result else {
        panic!("an ambiguous key must be refused, got {result:?}");
    };

    let detail = format!("{e:?}");

    assert!(
        detail.contains("ambiguous"),
        "the error should say what is wrong, got {detail}"
    );

    // And a record without the ambiguity still works, so the refusal is about the data and not the module.
    assert_eq!(query(&client, &key, "$.id").await, vec![Value::Int(1)]);
}

/// The filter aggregation: JSONPath over a whole set, returning only the records that match.
///
/// This is the case the emit path exists for. What is checked is that **records** come back — with their bins —
/// rather than partials, because that is what makes this cheaper than filtering on the client.
#[tokio::test]
async fn the_filter_aggregation_returns_matching_records() {
    let Some(client) = connect().await else {
        skipped("the_filter_aggregation_returns_matching_records", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_agg_{}", std::process::id());
    let write = WritePolicy::default();

    let probe = seed(&client, &namespace, &set).await;

    if !available(&client, &probe).await {
        skipped(
            "the_filter_aggregation_returns_matching_records",
            "no usable jsonpath module",
        );
        return;
    }

    // Twenty records, each with a list of one order. The ten whose order totals more than 10 are the ones the
    // path should select — and it selects them by *matching something* in them, because that is how a
    // record-level predicate is expressed in JSONPath.
    for i in 1..=20i64 {
        let key = as_key!(namespace.as_str(), set.as_str(), 100 + i);

        client
            .put(
                &write,
                &key,
                &[
                    as_bin!("id", i),
                    as_bin!("nested", as_list!(as_map!("total" => i))),
                ],
            )
            .await
            .unwrap();
    }

    let args = [as_val!("$.nested[?@.total > 10]")];
    let mut statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

    statement.set_aggregate_function(&module(), "jsonpath_filter", Some(&args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut matched = Vec::new();
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.unwrap();

        // A record carries its bins; a partial would be one unnamed value. `nested` being present is what
        // distinguishes the two.
        if record.bins.contains_key("nested") {
            if let Some(Value::Int(id)) = record.bins.get("id") {
                matched.push(*id);
            }
        }
    }

    matched.sort_unstable();

    assert_eq!(
        matched,
        (11..=20).collect::<Vec<i64>>(),
        "the filter must return the matching records themselves, with their bins"
    );
}

/// The collecting aggregation, combined on the client by the plugin's own code.
#[tokio::test]
async fn the_extract_aggregation_collects_values() {
    use aerospike_udf_example_jsonpath::owned::Owned;
    use aerospike_udf_example_jsonpath::JsonPathExtract;
    use aerospike_udf_sdk_native::client::Combiner;

    let Some(client) = connect().await else {
        skipped("the_extract_aggregation_collects_values", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_ext_{}", std::process::id());
    let write = WritePolicy::default();

    let probe = seed(&client, &namespace, &set).await;

    if !available(&client, &probe).await {
        skipped("the_extract_aggregation_collects_values", "no usable jsonpath module");
        return;
    }

    // Deleted so the probe record's own shape does not join the answer.
    client.delete(&write, &probe).await.unwrap();

    for i in 1..=20i64 {
        let key = as_key!(namespace.as_str(), set.as_str(), 200 + i);

        client
            .put(&write, &key, &[as_bin!("nested", as_map!("score" => i))])
            .await
            .unwrap();
    }

    // A chunk of 6 over 20 records, so it dumps and the client has to combine several partials.
    let args = [as_val!("$.nested.score"), as_val!(6)];
    let mut statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

    statement.set_aggregate_function(&module(), "jsonpath_extract", Some(&args));

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

    assert!(!partials.is_empty(), "the aggregation produced nothing");

    // Read chunk by chunk with the plugin's own `from_partial` - the collecting shape's client mode.
    let combiner = Combiner::<JsonPathExtract>::new(&args).unwrap();
    let mut scores = Vec::new();

    for partial in &partials {
        let chunk = combiner.read(partial).unwrap();

        for value in &chunk.values {
            if let Owned::Int(n) = value {
                scores.push(*n);
            }
        }
    }

    scores.sort_unstable();

    assert_eq!(
        scores,
        (1..=20).collect::<Vec<i64>>(),
        "every record's score must appear exactly once across the dumped chunks"
    );

    eprintln!(
        "extracted {} scores from {} partial(s)",
        scores.len(),
        partials.len()
    );

    // The set is left behind on purpose here - it is small, and the read policy below shows the records are
    // untouched by a read-only aggregation.
    let survivor = client
        .get(
            &ReadPolicy::default(),
            &as_key!(namespace.as_str(), set.as_str(), 201i64),
            Bins::All,
        )
        .await
        .unwrap();

    assert!(
        survivor.bins.contains_key("nested"),
        "an aggregation must not have modified the records it read"
    );
}

/// **Drilling into a string bin that holds JSON.**
///
/// The record shape this is here for:
///
/// ```text
/// bin1:    "string"
/// bin2:    42
/// bin_map: { "A": [1, 2, 3, { "a": 1, "b": "{\"a\": 1, \"b\": 2}" }] }
/// ```
///
/// `$.bin_map.A[3].b` is a **string** whose text happens to be a JSON object. A path that stops there gets the
/// string; a path that keeps going gets parsed and keeps going. That is the whole rule: **descending is what
/// triggers the parse**, so no existing path changes meaning.
#[tokio::test]
async fn a_path_drills_into_an_embedded_json_string() {
    let Some(client) = connect().await else {
        skipped("a_path_drills_into_an_embedded_json_string", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_embed_{}", std::process::id());
    let write = WritePolicy::default();
    let key = as_key!(namespace.as_str(), set.as_str(), 1i64);

    // The innermost value is JSON *text*, exactly as an application that stored a serialized blob would have it.
    let inner = r#"{"a": 1, "b": 2}"#;

    client
        .put(
            &write,
            &key,
            &[
                as_bin!("bin1", "string"),
                as_bin!("bin2", 42i64),
                as_bin!(
                    "bin_map",
                    as_map!("A" => as_list!(
                        1i64,
                        2i64,
                        3i64,
                        as_map!("a" => 1i64, "b" => inner)
                    ))
                ),
            ],
        )
        .await
        .unwrap();

    if !available(&client, &key).await {
        skipped("a_path_drills_into_an_embedded_json_string", "no usable jsonpath module");
        return;
    }

    // Stopping at the string gives the string, unchanged.
    assert_eq!(
        query(&client, &key, "$.bin_map.A[3].b").await,
        vec![Value::String(inner.to_string())],
        "a path that stops at the string must get the string, not a parsed document"
    );

    // Going one further parses it. This is the question that prompted the feature.
    assert_eq!(
        query(&client, &key, "$.bin_map.A[3].b.b").await,
        vec![Value::Int(2)],
        "descending into a string bin must parse it as JSON and keep going"
    );

    assert_eq!(
        query(&client, &key, "$.bin_map.A[3].b.a").await,
        vec![Value::Int(1)]
    );

    // Filters work inside an embedded document too - it is nodes all the way down by the time the engine sees it.
    assert_eq!(
        query(&client, &key, "$.bin_map.A[3].b[?@ > 1]").await,
        vec![Value::Int(2)],
        "a filter should apply to an embedded document's members"
    );

    // **A string that is not JSON is still just a string.** Descending into it matches nothing rather than
    // failing - the same answer as descending into any scalar.
    assert!(
        query(&client, &key, "$.bin1.anything").await.is_empty(),
        "an ordinary string must not become a document"
    );

    // And a string that *would* parse as a JSON scalar is deliberately left alone: only `{` and `[` are
    // attempted. Were that not so, a bin holding the text "42" would compare equal to the number 42.
    let scalar_key = as_key!(namespace.as_str(), set.as_str(), 2i64);

    client
        .put(&write, &scalar_key, &[as_bin!("s", "42"), as_bin!("id", 2i64)])
        .await
        .unwrap();

    assert_eq!(
        query(&client, &scalar_key, "$.s").await,
        vec![Value::String("42".to_string())],
        "a string holding a JSON scalar stays a string"
    );

    // Nested two deep: a document inside a document inside a string.
    let deep_key = as_key!(namespace.as_str(), set.as_str(), 3i64);
    let deep = r#"{"outer": "{\"inner\": 7}"}"#;

    client
        .put(&write, &deep_key, &[as_bin!("payload", deep)])
        .await
        .unwrap();

    assert_eq!(
        query(&client, &deep_key, "$.payload.outer.inner").await,
        vec![Value::Int(7)],
        "a document nested inside a document should be reachable, one parse per level and no deeper"
    );
}

/// The two record functions that answer *about* a path rather than with its values.
///
/// Both are callable on a single record by key, like the other three — which is the point of them being record
/// UDFs rather than aggregations: no query, no scan, one key.
#[tokio::test]
async fn count_and_paths_on_a_single_record() {
    let Some(client) = connect().await else {
        skipped("count_and_paths_on_a_single_record", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_count_{}", std::process::id());
    let key = seed(&client, &namespace, &set).await;

    if !available(&client, &key).await {
        skipped("count_and_paths_on_a_single_record", "no usable jsonpath module");
        return;
    }

    let write = WritePolicy::default();

    // `count` agrees with what `jsonpath` returns, which is the property that makes it usable as a cheap
    // pre-check.
    for path in ["$.id", "$.tags[*]", "$..total", "$.nosuchbin", "$.orders[?@.total > 100]"] {
        let counted = client
            .execute_udf(&write, &key, &module(), "jsonpath_count", Some(&[as_val!(path)]))
            .await
            .unwrap();

        let listed = query(&client, &key, path).await.len() as i64;

        assert_eq!(
            counted,
            Some(Value::Int(listed)),
            "count and the value list must agree for {path}"
        );
    }

    // `paths` answers *where*, in normalized form: concrete locations, no wildcards left.
    let paths = client
        .execute_udf(&write, &key, &module(), "jsonpath_paths", Some(&[as_val!("$..total")]))
        .await
        .unwrap();

    let Some(Value::List(mut items)) = paths else {
        panic!("wanted a list of paths, got {paths:?}");
    };

    items.sort();

    assert_eq!(
        items,
        vec![
            Value::String("$['orders'][0]['total']".to_string()),
            Value::String("$['orders'][1]['total']".to_string()),
            Value::String("$['orders'][2]['total']".to_string()),
        ],
        "a normalized path names a concrete location - bin, index, key - which is what a write would need"
    );

    // A path that matches nothing has no locations, and that is not an error.
    let none = client
        .execute_udf(&write, &key, &module(), "jsonpath_paths", Some(&[as_val!("$.nope")]))
        .await
        .unwrap();

    assert_eq!(none, Some(Value::List(vec![])));

    // And both refuse a malformed path rather than answering zero.
    for f in ["jsonpath_count", "jsonpath_paths"] {
        let bad = client
            .execute_udf(&write, &key, &module(), f, Some(&[as_val!("$.[[[")]))
            .await;

        assert!(bad.is_err(), "{f} must refuse a malformed path, got {bad:?}");
    }
}

/// **What a path that cannot possibly match does — the whole matrix.**
///
/// The answer for every case below is the same: **an empty result, not an error.** That is not an accident of this
/// implementation, it is what JSONPath is: RFC 9535 defines a query as producing a *nodelist*, and a selector that
/// does not apply to a node simply selects nothing from it. There are no runtime type errors in the language, and
/// [`Queryable`]'s methods return `Option` rather than `Result` precisely because there is nothing to report.
///
/// So the only two things that *are* errors are a **malformed path** (rejected at compile time) and an
/// **ambiguous map key** (which this module adds, because it is the one case where answering would mean answering
/// wrongly).
///
/// [`Queryable`]: jsonpath_rust::query::queryable::Queryable
#[tokio::test]
async fn every_kind_of_miss_is_empty_rather_than_an_error() {
    let Some(client) = connect().await else {
        skipped("every_kind_of_miss_is_empty_rather_than_an_error", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_miss_{}", std::process::id());
    let key = seed(&client, &namespace, &set).await;

    if !available(&client, &key).await {
        skipped("every_kind_of_miss_is_empty_rather_than_an_error", "no usable jsonpath module");
        return;
    }

    // The seeded record: id=1 (int), name="Ada" (string), address={city,zip} (map),
    // tags=["a","b","c"] (list), orders=[3 maps], scores=[1.5, 20.5], score=2.5, active=true.
    let misses = [
        // A key that is not there, at the root and nested.
        ("$.nosuchbin", "missing bin"),
        ("$.address.country", "missing key in a map that exists"),
        ("$..nosuchkey", "recursive descent that finds nothing"),
        // A key asked of something that cannot hold keys.
        ("$.id.anything", "key on an integer"),
        ("$.name.anything", "key on a string that is not JSON"),
        ("$.active.anything", "key on a boolean"),
        ("$.score.anything", "key on a double"),
        ("$.tags.anything", "key on a list"),
        // An index asked of something that cannot be indexed.
        ("$.id[0]", "index on an integer"),
        ("$.name[0]", "index on a string"),
        ("$.address[0]", "index on a map"),
        // An index that is out of range, both directions.
        ("$.tags[99]", "index past the end"),
        ("$.tags[-99]", "negative index past the start"),
        // A filter applied to something with no members to filter.
        ("$.id[?@ > 0]", "filter on an integer"),
        ("$.name[?@ > 0]", "filter on a string"),
        // A filter that matches none of the members it does have.
        ("$.orders[?@.total > 100000]", "filter that matches nothing"),
        ("$.orders[?@.nosuchkey == 1]", "filter on a key the members lack"),
    ];

    for (path, what) in misses {
        // `jsonpath` - an empty list.
        assert!(
            query(&client, &key, path).await.is_empty(),
            "{what} ({path}) should be an empty list"
        );

        let write = WritePolicy::default();

        // `jsonpath_count` - zero.
        assert_eq!(
            client
                .execute_udf(&write, &key, &module(), "jsonpath_count", Some(&[as_val!(path)]))
                .await
                .unwrap(),
            Some(Value::Int(0)),
            "{what} ({path}) should count zero"
        );

        // `jsonpath_exists` - false.
        assert_eq!(
            client
                .execute_udf(&write, &key, &module(), "jsonpath_exists", Some(&[as_val!(path)]))
                .await
                .unwrap(),
            Some(Value::Bool(false)),
            "{what} ({path}) should not exist"
        );

        // `jsonpath_one` - nil, which the client collapses to no value at all.
        assert_eq!(
            client
                .execute_udf(&write, &key, &module(), "jsonpath_one", Some(&[as_val!(path)]))
                .await
                .unwrap(),
            None,
            "{what} ({path}) should answer nil"
        );

        // `jsonpath_paths` - an empty list of locations.
        assert_eq!(
            client
                .execute_udf(&write, &key, &module(), "jsonpath_paths", Some(&[as_val!(path)]))
                .await
                .unwrap(),
            Some(Value::List(vec![])),
            "{what} ({path}) should have no locations"
        );
    }

    // A slice that runs past the end is clamped rather than refused - `tags` has three elements.
    assert_eq!(
        query(&client, &key, "$.tags[1:99]").await,
        vec![
            Value::String("b".to_string()),
            Value::String("c".to_string()),
        ],
        "an over-long slice should be clamped to what is there"
    );

    // A slice entirely past the end is empty, not an error.
    assert!(query(&client, &key, "$.tags[50:99]").await.is_empty());
}

/// **What a miss does inside an aggregation, and what an error does.**
///
/// The distinction matters far more here than for a single record: an aggregation runs over every record in a set,
/// so "miss" has to mean *this record does not match* while "error" means *the whole query stops*. Getting those
/// the wrong way round would either lose data silently or make one pathological record fatal to a scan.
#[tokio::test]
async fn a_miss_skips_a_record_but_an_error_stops_the_query() {
    let Some(client) = connect().await else {
        skipped("a_miss_skips_a_record_but_an_error_stops_the_query", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("jp_aggmiss_{}", std::process::id());
    let write = WritePolicy::default();

    let probe = seed(&client, &namespace, &set).await;

    if !available(&client, &probe).await {
        skipped("a_miss_skips_a_record_but_an_error_stops_the_query", "no usable jsonpath module");
        return;
    }

    client.delete(&write, &probe).await.unwrap();

    // Ten records of mixed shape. Only the even ones have the nested list the path names; the odd ones hold an
    // integer where the others hold a list, which is a type mismatch rather than a missing bin.
    for i in 1..=10i64 {
        let key = as_key!(namespace.as_str(), set.as_str(), i);

        if i % 2 == 0 {
            client
                .put(
                    &write,
                    &key,
                    &[as_bin!("id", i), as_bin!("nested", as_list!(as_map!("total" => i)))],
                )
                .await
                .unwrap();
        } else {
            client
                .put(&write, &key, &[as_bin!("id", i), as_bin!("nested", i)])
                .await
                .unwrap();
        }
    }

    // The path names a list of maps. Half the records have an integer there instead.
    let args = [as_val!("$.nested[?@.total > 0]")];
    let mut statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

    statement.set_aggregate_function(&module(), "jsonpath_filter", Some(&args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut matched = Vec::new();
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        let record = record.expect("a type mismatch must not fail the query");

        if let Some(Value::Int(id)) = record.bins.get("id") {
            matched.push(*id);
        }
    }

    matched.sort_unstable();

    assert_eq!(
        matched,
        vec![2, 4, 6, 8, 10],
        "records whose shape does not fit the path are skipped, not fatal"
    );

    // --- and now the one case that *is* fatal ---

    // One record with an ambiguous map key, among nine that are fine.
    let bad = as_key!(namespace.as_str(), set.as_str(), 99i64);
    let mut ambiguous = HashMap::new();

    ambiguous.insert(Value::Int(5), Value::String("by number".to_string()));
    ambiguous.insert(Value::String("5".to_string()), Value::String("by name".to_string()));

    client
        .put(
            &write,
            &bad,
            &[as_bin!("id", 99i64), as_bin!("nested", Value::HashMap(ambiguous))],
        )
        .await
        .unwrap();

    let mut statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

    statement.set_aggregate_function(&module(), "jsonpath_filter", Some(&args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut stream = records.into_stream();
    let mut failed = false;

    while let Some(record) = stream.next().await {
        if record.is_err() {
            failed = true;
        }
    }

    // **This is a deliberate choice, and it is the harsh one.** One record whose keys are ambiguous fails the
    // whole query rather than being skipped. The alternative - skip it and carry on - would return a result set
    // that is silently missing rows, and a filter whose answer is quietly incomplete is worse than one that stops
    // and says why. It is worth knowing about before pointing a path at a billion records.
    assert!(
        failed,
        "an ambiguous map key must fail the query rather than silently drop the record"
    );
}
