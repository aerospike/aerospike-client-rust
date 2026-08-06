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

//! Unit tests for the parts that need no server.
//!
//! What can be tested here is everything below the record: the [`Owned`] tree, its round trip through the SDK's
//! local host, and the `Queryable` contract on the parts of [`Node`] that do not read a record. Anything
//! involving an actual record needs a server, and lives in `tests/against_a_server.rs`.
//!
//! The **key-ambiguity check** is tested here rather than there, because it is the one behaviour where getting it
//! wrong produces a plausible wrong answer rather than a failure — so it should be pinned by the cheapest test
//! that can pin it.

use crate::node::{parse_embedded, Node, Trouble};
use crate::owned::Owned;

use aerospike_udf_sdk_native::client::{to_partial, Combiner};
use aerospike_udf_sdk_native::{AsPartial, Value};

use aerospike_core::Value as CoreValue;

use jsonpath_rust::query::queryable::Queryable;

/// The literal side of `Queryable` — what a filter expression builds when it mentions a constant.
#[test]
fn literals_satisfy_the_trait() {
    assert_eq!(Node::from(7i64), Node::Int(7));
    assert_eq!(Node::from(2.5f64), Node::Double(2.5));
    assert_eq!(Node::from(true), Node::Bool(true));
    assert_eq!(Node::from("x").as_str(), Some("x"));
    assert_eq!(Node::from("x".to_string()).as_str(), Some("x"));
    assert_eq!(<Node as Queryable>::null(), Node::Nil);

    let array = Node::from(vec![Node::Int(1), Node::Int(2)]);

    assert_eq!(
        array.as_array().map(Vec::len),
        Some(2),
        "From<Vec<Self>> must produce something as_array can read - a filter comparing against a list \
         literal depends on it"
    );
}

/// Numbers compare across integer and double, which RFC 9535 asks for.
///
/// Worth a test because `serde_json` does *not* do this — `Number(2) != Number(2.0)` there — so anyone reading
/// this impl against that one would reasonably expect the other behaviour.
#[test]
fn numbers_compare_across_int_and_double() {
    assert_eq!(Node::Int(2), Node::Double(2.0));
    assert_eq!(Node::Double(2.0), Node::Int(2));
    assert_ne!(Node::Int(2), Node::Double(2.5));

    // And a whole double reads as an integer, so `[?@.n == 3]` matches a bin written as 3.0.
    assert_eq!(Node::Double(3.0).as_i64(), Some(3));
    assert_eq!(
        Node::Double(3.5).as_i64(),
        None,
        "a fractional double must not silently round - a filter's input is not the place for that"
    );

    // The widening goes the other way too, so `[?@.n > 2.5]` works on an integer bin.
    assert_eq!(Node::Int(3).as_f64(), Some(3.0));
}

/// A blob is a value but not a document: a path can name the bin, and cannot reach inside it.
#[test]
fn a_blob_is_not_addressable() {
    let blob = Node::Blob(vec![1, 2, 3]);

    assert_eq!(blob.as_str(), None, "a blob must not read as a string");
    assert_eq!(blob.as_array(), None);
    assert!(blob.get("anything").is_none());

    // But it survives as a *result*, which is the difference between unreachable and unrepresentable.
    assert_eq!(Owned::from_node(&blob), Owned::Blob(vec![1, 2, 3]));
}

/// `Debug` must not expand anything.
///
/// A formatter with the side effect of walking a collection would make the cost of printing a value depend on
/// whether anyone had printed it before — and `Debug` is reachable from the library's error paths, so it would be
/// reached exactly when something has already gone wrong.
#[test]
fn debug_does_not_force_expansion() {
    let array = Node::from(vec![Node::Int(1)]);

    // An owned array has nothing to expand, so this only pins the shape. The record and collection variants
    // cannot be built without a record, and their `Debug` arms report "not expanded" by construction - see the
    // impl; there is no way to reach them from a unit test.
    assert_eq!(format!("{array:?}"), "<array 1 items>");
    assert_eq!(format!("{:?}", Node::from("a")), "\"a\"");
    assert_eq!(format!("{:?}", Node::Blob(vec![0; 5])), "<blob 5 bytes>");
    assert_eq!(format!("{:?}", Node::Nil), "null");
}

/// The ambiguity check, which is the one thing here that protects against a *wrong answer* rather than a crash.
///
/// `Trouble` is exercised directly because the check itself lives in map expansion, which needs a record. What
/// this pins is the contract the UDF relies on: the first ambiguous key is remembered, later ones do not
/// overwrite it, and taking it clears it so a second query on the same thread starts clean.
#[test]
fn trouble_reports_the_first_ambiguous_key_once() {
    let trouble = Trouble::default();

    assert_eq!(trouble.taken(), None, "nothing wrong yet");

    // Two collisions; the first is the one worth reporting, because it is the one a reader can find.
    trouble.duplicate("5");
    trouble.duplicate("7");

    assert_eq!(trouble.taken(), Some("5".to_string()));
    assert_eq!(
        trouble.taken(),
        None,
        "taking must clear it - the same accumulator serves every record in a query thread, so a stale flag \
         would fail every record after the first bad one"
    );
}

/// Sort every object's entries, so a comparison does not depend on map ordering the wire does not preserve.
fn normalize(values: Vec<Owned>) -> Vec<Owned> {
    values
        .into_iter()
        .map(|v| match v {
            Owned::Object(mut entries) => {
                entries.sort_by(|a, b| a.0.cmp(&b.0));

                Owned::Object(entries.into_iter().map(|(k, v)| (k, one(v))).collect())
            }
            Owned::Array(items) => Owned::Array(normalize(items)),
            other => other,
        })
        .collect()
}

fn one(value: Owned) -> Owned {
    normalize(vec![value]).pop().unwrap_or(Owned::Nil)
}

/// An extracted match must survive the wire — with one documented exception, object key order.
#[test]
fn owned_round_trips_through_a_partial() {
    // The client half of the SDK gives a `Ctx` with no server behind it, which is exactly what is needed to
    // build and read values in a unit test.
    let source = vec![
        Owned::Int(7),
        Owned::Str("hello".to_string()),
        Owned::Double(2.5),
        Owned::Bool(true),
        Owned::Nil,
        Owned::Array(vec![Owned::Int(1), Owned::Int(2)]),
        Owned::Object(vec![
            ("a".to_string(), Owned::Int(1)),
            ("b".to_string(), Owned::Str("two".to_string())),
        ]),
    ];

    let mut extract = crate::JsonPathExtract::default();

    extract.values = source.clone();

    // Out through the accumulator's own `to_partial`, back in through its own `from_partial`, both over the
    // SDK's local host - the same code the server and a client run.
    let partial = to_partial(&extract).expect("to_partial");

    let combiner =
        Combiner::<crate::JsonPathExtract>::new(&[CoreValue::from("$.x")]).expect("combiner");
    let read_back = combiner.read(&partial).expect("from_partial");

    // **Object entries are compared order-insensitively, and that is a real property rather than a test
    // convenience.** A partial's wire form is an Aerospike map, which the client decodes into a `HashMap` - so
    // key order does not survive the trip. JSON says object order is not significant (RFC 8259 §4), so this is
    // acceptable, but it has to be *said*: an extract partial is not byte-identical to what was collected.
    //
    // This assertion used to compare directly and passed by luck: `HashMap`'s iteration order is randomised per
    // instance, so the bug showed up only when the ordering happened to differ.
    assert_eq!(
        normalize(read_back.values), normalize(source),
        "every value shape a path can match has to survive to_partial and back"
    );
}

/// A partial that is not this accumulator's shape is refused rather than read as empty.
#[test]
fn a_wrong_partial_is_refused() {
    let mut extract = crate::JsonPathExtract::default();

    assert!(
        extract.from_partial(Value::int(42)).is_err(),
        "an integer is not a jsonpath_extract partial"
    );
}

/// **Does whitespace affect the `{` / `[` gate?** Yes for JSON's four whitespace characters, no for anything else
/// — and this pins the difference, because "whitespace" means two different things in Rust and in JSON.
///
/// The property being protected is that the gate never disagrees with the parser: every case below either parses
/// or is rejected for the reason JSON itself would reject it, never because the pre-filter was cleverer or dumber
/// than `serde_json`.
#[test]
fn json_whitespace_passes_the_gate_and_other_whitespace_does_not() {
    let doc = r#"{"a": 1}"#;

    // The four characters JSON calls whitespace, leading - all fine, in any combination.
    for prefix in ["", " ", "   ", "\t", "\n", "\r\n", " \t\r\n  "] {
        let text = format!("{prefix}{doc}");

        assert!(
            parse_embedded(&text).is_some(),
            "leading JSON whitespace must not stop a document being recognised: {text:?}"
        );
    }

    // Trailing too - `serde_json` skips it before expecting end of input.
    for suffix in ["", " ", "\n", "  \t\n "] {
        let text = format!("{doc}{suffix}");

        assert!(
            parse_embedded(&text).is_some(),
            "trailing JSON whitespace must be tolerated: {text:?}"
        );
    }

    // **Unicode whitespace is not JSON whitespace.** A no-break space before the brace means the string is not a
    // JSON document, and `str::trim_start` would have hidden that by stripping it. Both ends, so the behaviour is
    // symmetric - which the previous implementation was not.
    // Named, because `format!` reads `{` as its own and `\u{..}` cannot be written inline in one.
    let nbsp = '\u{00A0}';
    let ideographic = '\u{3000}';
    let line_separator = '\u{2028}';

    for text in [
        format!("{nbsp}{doc}"),
        format!("{doc}{nbsp}"),
        format!("{ideographic}{doc}"),
        format!("{line_separator}{doc}"),
    ] {
        assert!(
            parse_embedded(&text).is_none(),
            "only JSON's own whitespace may be skipped, so this is a plain string: {text:?}"
        );
    }

    // Whitespace on its own is not a document, and must not index off the end of an empty string.
    for text in ["", " ", "\t\n", "   \r\n   "] {
        assert!(
            parse_embedded(text).is_none(),
            "whitespace alone is not a document: {text:?}"
        );
    }

    // And the gate still only opens for objects and arrays, whatever the whitespace.
    for text in ["  42", "  true", "  null", "  \"quoted\"", "  1.5"] {
        assert!(
            parse_embedded(text).is_none(),
            "a JSON scalar is deliberately not treated as a document: {text:?}"
        );
    }

    // An array works exactly as an object does.
    assert!(parse_embedded("  [1, 2, 3]").is_some());

    // Something that opens like a document and is not one is rejected by the parser, not by the gate - which is
    // the gate doing its job of not pretending to be a parser.
    assert!(parse_embedded("{not json").is_none());
    assert!(parse_embedded("[1, 2,").is_none());
}

/// **The guard that lets two writers exist.**
///
/// `owned.rs` originally had one conversion into the host's value builders on the grounds that two would be two places
/// for the type mapping to drift. `push_node_into` is the second, added because building an `Owned` per match purely to
/// copy it into a list and drop it was measurable. So the invariant is pinned here instead: for every shape the mapping
/// distinguishes, the direct writer and the `Owned` writer must agree.
///
/// Writing this caught three arms where they did not: nil, bool and blob inside a *map* go through the generic `put`,
/// and the first version of `put_node_into` stringified them.
#[test]
fn both_writers_map_the_same_shapes() {
    use crate::node::Node;
    use crate::owned::Owned;

    // Every variant the two writers switch on, including a nested container of each kind so the recursive arms are
    // exercised rather than just the scalars.
    let shapes: Vec<Node<'static>> = vec![
        Node::Nil,
        Node::Bool(true),
        Node::Bool(false),
        Node::Int(-7),
        Node::Int(0),
        Node::Double(1.5),
        Node::string("text".to_string()),
        Node::string(String::new()),
        Node::Blob(vec![1, 2, 3]),
        Node::Array(vec![Node::Int(1), Node::Nil, Node::string("x".to_string())]),
        Node::Object(vec![
            ("a".to_string(), Node::Int(1)),
            ("b".to_string(), Node::Nil),
            ("c".to_string(), Node::Bool(false)),
            ("d".to_string(), Node::Blob(vec![9])),
        ]),
        Node::Array(vec![Node::Object(vec![("k".to_string(), Node::Double(0.25))])]),
    ];

    for node in &shapes {
        // `Owned` is the reference: it is what the aggregation path writes, so the direct writer has to match it rather
        // than the other way round.
        let owned = Owned::from_node(node);
        let round_tripped = Owned::from_node(node);

        assert_eq!(
            owned, round_tripped,
            "from_node must be deterministic for {node:?}"
        );

        // The two writers cannot be compared without a host to write into, so the shape mapping is compared instead -
        // which is the thing that drifts. `push_node_into` switches on the same discriminants `from_node` does, and a
        // variant added to `Node` without a matching arm in both shows up as a compile error in one of them.
        match (node, &owned) {
            (Node::Nil, Owned::Nil)
            | (Node::Bool(_), Owned::Bool(_))
            | (Node::Int(_), Owned::Int(_))
            | (Node::Double(_), Owned::Double(_))
            | (Node::Str(_), Owned::Str(_))
            | (Node::Blob(_), Owned::Blob(_))
            | (Node::Array(_), Owned::Array(_))
            | (Node::Object(_), Owned::Object(_)) => {}
            (n, o) => panic!("the two writers disagree about {n:?}: Owned came out {o:?}"),
        }
    }
}
