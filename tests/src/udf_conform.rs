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

//! The `apitest` conformance module of every guest SDK, on a real server.
//!
//! `apitest` is one module written to one specification in each SDK (Rust, Go, C, C++, Java). The
//! conformance harness in the server tree (`modules/mod-wamr/test/conform.h`) calls all five on a model of
//! the server's record, thousands of generated cases at a time. This is its counterpart against the server
//! itself: a fixed table of cases, each run by all five modules on a record of its own, and held two ways,
//! the five answers against each other and every answer against the one written in the table. The record
//! is read back after every call and held the same two ways, so a call that fails is seen to write nothing.
//!
//! A case where the languages are allowed to differ (`test/conform.allow`: a string that is not UTF-8, a
//! string over 64 KiB in C and C++) is not in the table.
//!
//! `UDF_CONFORM_SHOW=1` prints every answer, for writing a new case.

use crate::common;
use aerospike::operations;
use aerospike::Task;
use aerospike::*;
use std::collections::BTreeMap;
use std::collections::HashMap;

const MODULES: [(&str, &[u8]); 5] = [
    ("rust", include_bytes!("../fixtures/wasm/apitest.wasm")),
    ("go", include_bytes!("../fixtures/wasm/apitest_go.wasm")),
    ("c", include_bytes!("../fixtures/wasm/apitest_c.wasm")),
    ("cpp", include_bytes!("../fixtures/wasm/apitest_cpp.wasm")),
    ("java", include_bytes!("../fixtures/wasm/apitest_java.wasm")),
];

const NAMESPACE: &str = "test";
const GEO_POINT: &str = r#"{"type":"Point","coordinates":[1,2]}"#;

/// The bins of the record a case runs on, unless it runs on a key with no record. `h` (an HLL) is added by
/// an operation, as no value of the client writes one.
fn stored_bins() -> Vec<Bin> {
    vec![
        Bin::new("n", i(42)),
        Bin::new("n2", i(7)),
        Bin::new("d", f(1.5)),
        Bin::new("t", b(true)),
        Bin::new("f", b(false)),
        Bin::new("s", s("a string")),
        Bin::new("e", s("")),
        Bin::new("b", Value::Blob(vec![0, 1, 2, 3])),
        Bin::new("g", Value::GeoJson(GEO_POINT.to_string())),
        Bin::new("l", list(vec![i(1), s("two"), i(3)])),
        Bin::new("el", list(vec![])),
        Bin::new("m", map(vec![(s("k1"), i(1)), (i(1), s("one"))])),
        Bin::new("em", map(vec![])),
    ]
}

/// What the stored record has: the bins above and `h`.
const STORED_BINS: i64 = 14;

fn i(v: i64) -> Value {
    Value::Int(v)
}

fn s(v: &str) -> Value {
    Value::from(v)
}

fn f(v: f64) -> Value {
    Value::from(v)
}

fn b(v: bool) -> Value {
    Value::Bool(v)
}

fn blob(v: &[u8]) -> Value {
    Value::Blob(v.to_vec())
}

fn list(v: Vec<Value>) -> Value {
    Value::List(v)
}

fn map(v: Vec<(Value, Value)>) -> Value {
    Value::SortedMap(v.into_iter().collect())
}

fn tag(t: &str) -> Value {
    list(vec![s(t)])
}

fn some(v: Value) -> Value {
    list(vec![s("some"), v])
}

/// Every map as a sorted map, wherever it is: the client decodes a map as one flavour or another by how
/// the module packed it, which is not what is being compared.
fn norm(v: &Value) -> Value {
    match v {
        Value::List(l) => Value::List(l.iter().map(norm).collect()),
        Value::HashMap(m) => Value::SortedMap(m.iter().map(|(k, v)| (norm(k), norm(v))).collect()),
        Value::OrderedMap(m) => Value::SortedMap(m.iter().map(|(k, v)| (norm(k), norm(v))).collect()),
        Value::SortedMap(m) => Value::SortedMap(m.iter().map(|(k, v)| (norm(k), norm(v))).collect()),
        other => other.clone(),
    }
}

/// A value as a line: long ones cut, so that a 5000-element answer does not fill the screen.
fn show(v: &Value) -> String {
    let text = format!("{v}");

    if text.len() > 300 {
        format!("{}... ({} chars)", &text[..300], text.len())
    } else {
        text
    }
}

#[derive(Clone)]
enum Rec {
    /// The stored bins.
    Stored,
    /// A key nothing was written under.
    NoRecord,
}

#[derive(Clone)]
enum Answer {
    /// The call succeeds with this value (nil is `Value::Nil`).
    Is(Value),
    /// The call fails, with a message that holds this.
    Fails(String),
}

/// What the record is after the call, from what it was before.
#[derive(Clone)]
struct After {
    /// There is a record.
    exists: bool,
    /// Bins written (`Some`) or removed (`None`).
    changes: Vec<(&'static str, Option<Value>)>,
    /// The record was written: its generation moved on by one.
    written: bool,
    /// Its time to live is at most this many seconds.
    ttl_at_most: Option<u64>,
}

/// The record as the call found it.
fn same() -> After {
    After { exists: true, changes: vec![], written: false, ttl_at_most: None }
}

/// No record, before and after.
fn none() -> After {
    After { exists: false, changes: vec![], written: false, ttl_at_most: None }
}

fn wrote(changes: Vec<(&'static str, Option<Value>)>) -> After {
    After { exists: true, changes, written: true, ttl_at_most: None }
}

#[derive(Clone)]
struct Case {
    name: String,
    rec: Rec,
    export: &'static str,
    args: Vec<Value>,
    answer: Answer,
    after: After,
}

fn case(name: impl Into<String>, rec: Rec, export: &'static str, args: Vec<Value>, answer: Answer, after: After) -> Case {
    Case { name: name.into(), rec, export, args, answer, after }
}

fn is(v: Value) -> Answer {
    Answer::Is(v)
}

fn fails(text: &str) -> Answer {
    Answer::Fails(text.to_string())
}

/// `mk`'s value, from the specification.
fn mk_value(what: &str, n: i64) -> Value {
    let mixed = |k: i64| match k % 6 {
        0 => i(k),
        1 => f(k as f64 + 0.5),
        2 => b(k % 4 == 2),
        3 => s(&format!("s{k}")),
        4 => blob(&[(k & 0xff) as u8]),
        _ => Value::Nil,
    };

    match what {
        "list_int" => list((0..n).map(i).collect()),
        "list_mixed" => list((0..n).map(mixed).collect()),
        "map_str" => map((0..n).map(|k| (s(&format!("k{k}")), i(k))).collect()),
        "map_int" => map((0..n).map(|k| (i(k), s(&format!("v{k}")))).collect()),
        "map_mixed" => map((0..n).map(|k| (s(&format!("k{k}")), mixed(k))).collect()),
        "nested" => {
            let mut v = i(n);

            for _ in 0..n {
                v = list(vec![v]);
            }

            v
        }
        "string" => s(&"x".repeat(n as usize)),
        "blob" => Value::Blob((0..n).map(|i| (i & 0xff) as u8).collect()),
        _ => unreachable!(),
    }
}

/// The table.
fn cases() -> Vec<Case> {
    use Rec::{NoRecord, Stored};

    let mut out = Vec::new();
    let stored_list = list(vec![i(1), s("two"), i(3)]);
    let stored_map = map(vec![(s("k1"), i(1)), (i(1), s("one"))]);

    // rd: every reader on an absent bin, a bin of its kind, and a bin of another kind.
    let readers: Vec<(&str, &str, Value, &str, Value)> = vec![
        ("has", "n", some(b(true)), "s", some(b(true))),
        ("get", "n", some(i(42)), "s", some(s("a string"))),
        ("bin", "n", some(i(42)), "l", some(stored_list.clone())),
        ("int", "n", some(i(42)), "s", tag("wrong")),
        ("double", "d", some(f(1.5)), "n", tag("wrong")),
        ("bool", "t", some(b(true)), "n", tag("wrong")),
        ("string", "s", some(s("a string")), "g", tag("wrong")),
        ("blob", "b", some(blob(&[0, 1, 2, 3])), "s", tag("wrong")),
        ("list", "l", some(stored_list.clone()), "m", tag("wrong")),
        ("map", "m", some(stored_map.clone()), "l", tag("wrong")),
        ("int_or", "n", some(i(42)), "s", tag("wrong")),
        ("double_or", "d", some(f(1.5)), "n", tag("wrong")),
        ("bool_or", "f", some(b(false)), "n", tag("wrong")),
    ];

    for (how, kind, kind_answer, other, other_answer) in readers {
        let absent = match how {
            "has" => some(b(false)),
            "bin" => tag("absent"),
            "int_or" => some(i(7)),
            "double_or" => some(f(7.5)),
            "bool_or" => some(b(true)),
            _ => tag("none"),
        };

        out.push(case(format!("rd {how} absent"), Stored, "rd", vec![s("zz"), s(how)], is(absent), same()));
        out.push(case(format!("rd {how} {kind}"), Stored, "rd", vec![s(kind), s(how)], is(kind_answer), same()));
        out.push(case(format!("rd {how} {other}"), Stored, "rd", vec![s(other), s(how)], is(other_answer), same()));
    }

    // Two more of another kind: a string reader on an integer, a blob reader on GeoJSON and on HLL.
    out.push(case("rd string n", Stored, "rd", vec![s("n"), s("string")], is(tag("wrong")), same()));
    out.push(case("rd blob g", Stored, "rd", vec![s("g"), s("blob")], is(tag("wrong")), same()));
    out.push(case("rd blob h", Stored, "rd", vec![s("h"), s("blob")], is(tag("wrong")), same()));
    out.push(case("rd int on no record", NoRecord, "rd", vec![s("n"), s("int")], is(tag("none")), none()));

    // wr: every how, with a `v` it takes, a missing one, and one of a kind it cannot take.
    let ok = |bins: i64| list(vec![s("ok"), s("ok"), i(bins)]);
    let wrong = || list(vec![s("wrong"), s("skipped"), i(STORED_BINS)]);
    let ints_257: Vec<Value> = (0..257).map(i).collect();
    let doubles_257: Vec<Value> = (0..257).map(|k| f(k as f64 + 0.5)).collect();

    out.extend([
        case("wr set int", Stored, "wr", vec![s("w"), s("set"), i(5)], is(ok(STORED_BINS + 1)), wrote(vec![("w", Some(i(5)))])),
        case("wr set list", Stored, "wr", vec![s("w"), s("set"), list(vec![i(1), s("a")])], is(ok(STORED_BINS + 1)), wrote(vec![("w", Some(list(vec![i(1), s("a")])))])),
        case("wr set over", Stored, "wr", vec![s("n"), s("set"), s("now a string")], is(ok(STORED_BINS)), wrote(vec![("n", Some(s("now a string")))])),
        case("wr set nil", Stored, "wr", vec![s("n"), s("set"), Value::Nil], is(ok(STORED_BINS - 1)), wrote(vec![("n", None)])),
        case("wr set missing", Stored, "wr", vec![s("w"), s("set")], is(wrong()), same()),
        case("wr blob string", Stored, "wr", vec![s("w"), s("blob"), s("abc")], is(ok(STORED_BINS + 1)), wrote(vec![("w", Some(blob(b"abc")))])),
        case("wr blob blob", Stored, "wr", vec![s("w"), s("blob"), blob(&[9, 8])], is(ok(STORED_BINS + 1)), wrote(vec![("w", Some(blob(&[9, 8])))])),
        case("wr blob int", Stored, "wr", vec![s("w"), s("blob"), i(5)], is(wrong()), same()),
        case("wr blob missing", Stored, "wr", vec![s("w"), s("blob")], is(wrong()), same()),
        case("wr geojson point", Stored, "wr", vec![s("w"), s("geojson"), s(GEO_POINT)], is(ok(STORED_BINS + 1)), wrote(vec![("w", Some(Value::GeoJson(GEO_POINT.to_string())))])),
        case("wr geojson int", Stored, "wr", vec![s("w"), s("geojson"), i(5)], is(wrong()), same()),
        case("wr geojson missing", Stored, "wr", vec![s("w"), s("geojson")], is(wrong()), same()),
        case("wr ints", Stored, "wr", vec![s("w"), s("ints"), list(vec![i(1), i(2), i(3)])], is(ok(STORED_BINS + 1)), wrote(vec![("w", Some(list(vec![i(1), i(2), i(3)])))])),
        case("wr ints empty", Stored, "wr", vec![s("w"), s("ints"), list(vec![])], is(ok(STORED_BINS + 1)), wrote(vec![("w", Some(list(vec![])))])),
        case("wr ints with a double", Stored, "wr", vec![s("w"), s("ints"), list(vec![i(1), f(2.5)])], is(wrong()), same()),
        case("wr ints 257", Stored, "wr", vec![s("w"), s("ints"), list(ints_257)], is(wrong()), same()),
        case("wr ints string", Stored, "wr", vec![s("w"), s("ints"), s("x")], is(wrong()), same()),
        case("wr ints missing", Stored, "wr", vec![s("w"), s("ints")], is(wrong()), same()),
        case("wr doubles", Stored, "wr", vec![s("w"), s("doubles"), list(vec![f(1.5), f(-2.25)])], is(ok(STORED_BINS + 1)), wrote(vec![("w", Some(list(vec![f(1.5), f(-2.25)])))])),
        case("wr doubles with an int", Stored, "wr", vec![s("w"), s("doubles"), list(vec![f(1.5), i(2)])], is(wrong()), same()),
        case("wr doubles 257", Stored, "wr", vec![s("w"), s("doubles"), list(doubles_257)], is(wrong()), same()),
        case("wr doubles missing", Stored, "wr", vec![s("w"), s("doubles")], is(wrong()), same()),
        case("wr remove", Stored, "wr", vec![s("n"), s("remove")], is(ok(STORED_BINS - 1)), wrote(vec![("n", None)])),
        // A save that fails fails the call, whatever the module answers after it, and nothing is written.
        case("wr geojson not a document", Stored, "wr", vec![s("w"), s("geojson"), s("not geojson")], fails("Invalid GeoJson"), same()),
        // A save with nothing staged creates no record, and fails the call.
        case("wr remove on no record", NoRecord, "wr", vec![s("zz"), s("remove")], fails("Key not found"), none()),
        case("wr remove absent", Stored, "wr", vec![s("zz"), s("remove")], is(ok(STORED_BINS)), wrote(vec![])),
        case("wr set on no record", NoRecord, "wr", vec![s("w"), s("set"), i(5)], is(ok(1)), After { exists: true, changes: vec![("w", Some(i(5)))], written: true, ttl_at_most: None }),
        case("wr ints on no record", NoRecord, "wr", vec![s("w"), s("ints"), list(vec![i(1)])], is(list(vec![s("refused"), s("skipped"), i(0)])), none()),
    ]);

    // meta, on a record and on a key with no record. The set and the key are each language's own and are
    // put back as `$SET` and `$KEY` before anything is compared.
    out.push(case(
        "meta",
        Stored,
        "meta",
        vec![],
        is(map(vec![
            (s("exists"), b(true)),
            (s("generation"), i(1)),
            (s("ttl"), i(-1)),
            (s("bins"), i(STORED_BINS)),
            (s("sized"), b(true)),
            (s("set"), s("$SET")),
            (s("key"), s("$KEY")),
            (s("digest"), i(20)),
        ])),
        same(),
    ));
    out.push(case(
        "meta on no record",
        NoRecord,
        "meta",
        vec![],
        is(map(vec![
            (s("exists"), b(false)),
            (s("generation"), i(0)),
            (s("ttl"), i(-1)),
            (s("bins"), i(0)),
            (s("sized"), b(false)),
            // A key with no record has no set: the set is the record's.
            (s("set"), s("")),
            (s("key"), Value::Nil),
            (s("digest"), i(20)),
        ])),
        none(),
    ));

    // mk: each `what` at 0, 1 and 5000, and out of range.
    for what in ["list_int", "list_mixed", "map_str", "map_int", "map_mixed", "nested", "string", "blob"] {
        for n in [0, 1, 5000] {
            let answer = if what == "nested" && n == 5000 {
                // Deeper than the host builds.
                fails("mk: refused")
            } else if what == "nested" && n == 0 {
                // Outside the specification (`n >= 1`): every module builds the one list it starts with.
                is(list(vec![i(0)]))
            } else {
                is(mk_value(what, n))
            };

            out.push(case(format!("mk {what} {n}"), Stored, "mk", vec![s(what), i(n)], answer, same()));
        }

        out.push(case(format!("mk {what} -1"), Stored, "mk", vec![s(what), i(-1)], fails("mk: refused"), same()));
        out.push(case(format!("mk {what} 1000001"), Stored, "mk", vec![s(what), i(1_000_001)], fails("mk: refused"), same()));
    }

    // walk.
    out.extend([
        case("walk list", Stored, "walk", vec![s("l")], is(list(vec![s("list"), i(3), i(4), i(1), i(1), i(3), s("none")])), same()),
        case("walk map", Stored, "walk", vec![s("m")], is(list(vec![s("map"), i(2), i(1), b(true), b(true), i(1), s("one")])), same()),
        case("walk empty list", Stored, "walk", vec![s("el")], is(list(vec![s("list"), i(0), i(0), i(0), Value::Nil, Value::Nil, s("none")])), same()),
        case("walk empty map", Stored, "walk", vec![s("em")], is(list(vec![s("map"), i(0), i(0), b(false), b(false), Value::Nil, Value::Nil])), same()),
        case("walk scalar", Stored, "walk", vec![s("n")], is(tag("wrong")), same()),
        case("walk absent", Stored, "walk", vec![s("zz")], is(tag("none")), same()),
    ]);

    // ar: 0, 1, 2, 128 and 129 arguments.
    let many = |count: usize| -> Vec<Value> {
        let mut args = vec![i(1), s("b")];

        args.extend((2..count).map(|k| i(k as i64)));

        args
    };
    let mut ar_128 = vec![i(1), s("b"), i(126)];

    ar_128.extend((0..126).map(|_| s("int")));

    out.extend([
        case("ar 0", Stored, "ar", vec![], fails(""), same()),
        case("ar 1", Stored, "ar", vec![i(5)], is(list(vec![i(5), Value::Nil, i(0)])), same()),
        case("ar 3", Stored, "ar", vec![i(5), s("x"), blob(&[1])], is(list(vec![i(5), s("x"), i(1), s("blob")])), same()),
        case("ar 128", Stored, "ar", many(128), is(list(ar_128)), same()),
        case("ar 129", Stored, "ar", many(129), fails("128"), same()),
    ]);

    // life.
    out.extend([
        case("life staged", Stored, "life", vec![s("staged")], is(list(vec![s("some"), i(1), i(STORED_BINS)])), same()),
        case("life save_twice", Stored, "life", vec![s("save_twice")], is(list(vec![s("ok"), s("ok")])), wrote(vec![("t", Some(i(2)))])),
        // The record still exists for the rest of the call; it is gone once the call has ended.
        case("life delete", Stored, "life", vec![s("delete")], is(list(vec![s("ok"), b(true)])), none()),
        case("life ttl", Stored, "life", vec![s("ttl")], is(list(vec![s("ok"), s("ok")])), After { exists: true, changes: vec![], written: true, ttl_at_most: Some(100) }),
        case("life fail_after_save", Stored, "life", vec![s("fail_after_save")], fails("life: failing"), same()),
        case("life staged on no record", NoRecord, "life", vec![s("staged")], is(list(vec![s("some"), i(1), i(0)])), none()),
        case("life delete on no record", NoRecord, "life", vec![s("delete")], is(list(vec![s("refused"), b(false)])), none()),
        case("life ttl on no record", NoRecord, "life", vec![s("ttl")], fails("Key not found"), none()),
        case("life fail_after_save on no record", NoRecord, "life", vec![s("fail_after_save")], fails("life: failing"), none()),
    ]);

    // res.
    out.extend([
        case("res nil", Stored, "res", vec![s("nil")], is(Value::Nil), same()),
        case("res int", Stored, "res", vec![s("int")], is(i(i64::MIN)), same()),
        case("res double", Stored, "res", vec![s("double")], is(f(1e308)), same()),
        case("res bool", Stored, "res", vec![s("bool")], is(b(true)), same()),
        case("res string", Stored, "res", vec![s("string")], is(s("")), same()),
        case("res blob", Stored, "res", vec![s("blob")], is(blob(&[])), same()),
        case("res geojson", Stored, "res", vec![s("geojson")], is(Value::GeoJson(GEO_POINT.to_string())), same()),
        case("res list", Stored, "res", vec![s("list")], is(list(vec![])), same()),
        case("res map", Stored, "res", vec![s("map")], is(map(vec![])), same()),
        case("res fail_long", Stored, "res", vec![s("fail_long")], fails(&"x".repeat(100)), same()),
    ]);

    // txt.
    let big = "x".repeat(64 * 1024);

    out.extend([
        case("txt empty", Stored, "txt", vec![s("")], is(list(vec![i(0), s(""), i(-1)])), same()),
        case("txt hello", Stored, "txt", vec![s("hello")], is(list(vec![i(5), s("hello"), i(104)])), same()),
        case("txt 64 KiB", Stored, "txt", vec![s(&big)], is(list(vec![i(64 * 1024), s(&big), i(120)])), same()),
    ]);

    // cmp: scalars, lists, maps, a pair of two kinds, and an absent bin.
    out.extend([
        case("cmp int int", Stored, "cmp", vec![s("n"), s("n2")], is(tag("wrong")), same()),
        // The host orders strings as it orders lists and maps; integers it calls not comparable.
        case("cmp string string", Stored, "cmp", vec![s("s"), s("e")], is(some(i(1))), same()),
        case("cmp list list", Stored, "cmp", vec![s("l"), s("el")], is(some(i(1))), same()),
        case("cmp list self", Stored, "cmp", vec![s("l"), s("l")], is(some(i(0))), same()),
        case("cmp empty list list", Stored, "cmp", vec![s("el"), s("l")], is(some(i(-1))), same()),
        case("cmp map map", Stored, "cmp", vec![s("m"), s("em")], is(some(i(1))), same()),
        // Lists and maps are ordered against each other, by kind.
        case("cmp list map", Stored, "cmp", vec![s("l"), s("m")], is(some(i(1))), same()),
        case("cmp int list", Stored, "cmp", vec![s("n"), s("l")], is(tag("wrong")), same()),
        case("cmp absent", Stored, "cmp", vec![s("l"), s("zz")], is(tag("none")), same()),
    ]);

    // val: every kind an argument can be.
    let hll = Value::Hll(vec![0; 8]);
    let vals: Vec<(&str, &str, Value, Option<i64>)> = vec![
        ("nil", "nil", Value::Nil, None),
        ("bool", "bool", b(false), None),
        ("int", "int", i(-3), None),
        ("double", "double", f(0.25), None),
        ("string", "string", s("héllo"), None),
        ("blob", "blob", blob(&[0, 255]), None),
        ("list", "list", list(vec![i(1), s("a"), list(vec![])]), Some(3)),
        ("map", "map", map(vec![(s("a"), i(1)), (i(2), b(true))]), Some(2)),
        ("geojson", "geojson", Value::GeoJson(GEO_POINT.to_string()), None),
        ("hll", "hll", hll, None),
    ];

    for (name, kind, v, len) in vals {
        let mut answer = vec![s(kind), v.clone()];

        if let Some(len) = len {
            answer.push(i(len));
        }

        out.push(case(format!("val {name}"), Stored, "val", vec![v], is(list(answer)), same()));
    }

    out
}

/// One language's run of one case.
#[derive(Clone, Debug, PartialEq)]
struct Run {
    /// `Ok(answer)` or `Err(message)`.
    answer: std::result::Result<Value, String>,
    /// The record after: `None` for no record, else its bins, its generation, and whether its time to live
    /// is at most what the case asks (`true` when it asks nothing).
    record: Option<(Value, u32, bool)>,
}

/// Puts the language's own set and key back as `$SET` and `$KEY`.
fn unname(v: &Value, set: &str, key: &str) -> Value {
    match v {
        Value::String(t) if t == set => s("$SET"),
        Value::String(t) if t == key => s("$KEY"),
        Value::List(l) => Value::List(l.iter().map(|v| unname(v, set, key)).collect()),
        Value::SortedMap(m) => Value::SortedMap(m.iter().map(|(k, v)| (k.clone(), unname(v, set, key))).collect()),
        other => other.clone(),
    }
}

async fn read_back(client: &Client, key: &Key, ttl_at_most: Option<u64>) -> Option<(Value, u32, bool)> {
    match client.get(&ReadPolicy::default(), key, Bins::All).await {
        Ok(record) => {
            let bins: BTreeMap<Value, Value> = record.bins.iter().map(|(k, v)| (s(k), norm(v))).collect();
            let ttl_ok = match ttl_at_most {
                // The client works the TTL out from the record's absolute expiry and this machine's clock,
                // which can be a second behind the server's: a TTL of 100 then reads as 101.
                Some(at_most) => record.time_to_live().is_some_and(|ttl| ttl.as_secs() <= at_most + 1),
                None => true,
            };

            Some((Value::SortedMap(bins), record.generation, ttl_ok))
        }
        Err(e) if e.matches(&[ResultCode::KeyNotFoundError]) => None,
        Err(e) => panic!("reading back {key:?}: {e}"),
    }
}

async fn register(client: &Client, package: &str, module: &[u8]) -> std::result::Result<(), String> {
    match client.register_udf(&AdminPolicy::default(), module, &format!("{package}.wasm"), UdfLang::Wasm).await {
        Ok(task) => task
            .wait_till_complete(None)
            .await
            .map(|_| ())
            .map_err(|e| format!("the module did not become available: {e}")),
        Err(e) if format!("{e:?}").contains("invalid_udf_type") => Err("this server does not support WASM UDFs".to_string()),
        Err(e) => panic!("registering {package} failed: {e:?}"),
    }
}

/// The record after, as the case says it is, from the record as it was set up.
fn expected_record(c: &Case, before: &Option<(Value, u32, bool)>) -> Option<(Value, u32, bool)> {
    if !c.after.exists {
        return None;
    }

    let (mut bins, generation) = match before {
        Some((Value::SortedMap(bins), generation, _)) => (bins.clone(), *generation),
        _ => (BTreeMap::new(), 0),
    };

    for (name, value) in &c.after.changes {
        match value {
            Some(v) => bins.insert(s(name), norm(v)),
            None => bins.remove(&s(name)),
        };
    }

    let generation = if c.after.written { generation + 1 } else { generation };

    Some((Value::SortedMap(bins), generation, true))
}

#[aerospike_macro::test]
async fn the_five_apitest_modules_agree_on_the_server() {
    let client = common::client().await;
    let set = format!("conform_{}", common::rand_str(8));
    let tail = common::rand_str(6);
    let show_all = std::env::var("UDF_CONFORM_SHOW").is_ok();
    let mut packages = Vec::new();

    for (lang, module) in MODULES {
        let package = format!("conform_{lang}_{tail}");

        if let Err(reason) = register(&client, &package, module).await {
            eprintln!("skipped the_five_apitest_modules_agree_on_the_server: {reason}");
            return;
        }

        packages.push((lang, package));
    }

    let setup = WritePolicy { send_key: true, ..WritePolicy::default() };
    let mut ops: Vec<operations::Operation> = stored_bins().iter().map(operations::put).collect();

    ops.push(operations::hll::init(&operations::hll::HllPolicy::default(), "h", 8));

    let mut problems = Vec::new();

    for (number, c) in cases().iter().enumerate() {
        let mut runs: Vec<(&str, Run)> = Vec::new();

        for (lang, package) in &packages {
            let user_key = format!("{number}-{lang}");
            let key = Key::new(NAMESPACE, set.clone(), s(&user_key)).unwrap();

            if let Rec::Stored = c.rec {
                client.operate(&setup, &key, &ops).await.unwrap();
            }

            let before = read_back(&client, &key, None).await;
            let answer = match client.execute_udf(&WritePolicy::default(), &key, package, c.export, &c.args).await {
                Ok(v) => Ok(unname(&norm(&v.unwrap_or(Value::Nil)), &set, &user_key)),
                Err(e) => Err(format!("{e}")),
            };
            let run = Run { answer, record: read_back(&client, &key, c.after.ttl_at_most).await };

            if show_all {
                match &run.answer {
                    Ok(v) => println!("{} [{lang}] = {}", c.name, show(v)),
                    Err(e) => println!("{} [{lang}] failed: {e}", c.name),
                }
            }

            // Against the table.
            match (&c.answer, &run.answer) {
                (Answer::Is(want), Ok(got)) if norm(want) == *got => {}
                (Answer::Fails(text), Err(message)) if message.contains(text.as_str()) => {}
                (want, got) => {
                    let want = match want {
                        Answer::Is(v) => show(v),
                        Answer::Fails(text) => format!("a failure saying {text:?}"),
                    };
                    let got = match got {
                        Ok(v) => show(v),
                        Err(e) => format!("failed: {e}"),
                    };

                    problems.push(format!("{} [{lang}]: answered {got}, the table says {want}", c.name));
                }
            }

            let want = expected_record(c, &before);

            if run.record != want {
                problems.push(format!(
                    "{} [{lang}]: left {:?}, the table says {:?}",
                    c.name,
                    run.record.as_ref().map(|(bins, generation, ttl)| (show(bins), generation, ttl)),
                    want.as_ref().map(|(bins, generation, ttl)| (show(bins), generation, ttl)),
                ));
            }

            let _ = client.delete(&WritePolicy::default(), &key).await;

            runs.push((lang, run));
        }

        // Against each other: what failed is held by whether it failed, not by its message, which names
        // the module.
        let first = &runs[0].1;

        for (lang, run) in &runs[1..] {
            let same_answer = match (&first.answer, &run.answer) {
                (Ok(a), Ok(b)) => a == b,
                (Err(_), Err(_)) => true,
                _ => false,
            };

            if !same_answer || first.record.as_ref().map(|r| (&r.0, r.2)) != run.record.as_ref().map(|r| (&r.0, r.2)) {
                problems.push(format!("{}: {lang} differs from {}", c.name, runs[0].0));
            }
        }
    }

    // agg, as an aggregation query over records of its own: integers counted, the rest not.
    let agg_set = format!("conform_agg_{}", common::rand_str(8));
    let write = WritePolicy::default();

    for k in 0..20i64 {
        let key = Key::new(NAMESPACE, agg_set.clone(), i(k)).unwrap();
        let bin = match k % 4 {
            0 => Bin::new("n", s("five")),
            1 => Bin::new("n", f(2.5)),
            _ => Bin::new("n", i(k)),
        };

        client.put(&write, &key, &[bin, Bin::new("s", s("a string"))]).await.unwrap();
    }

    client.put(&write, &Key::new(NAMESPACE, agg_set.clone(), i(100)).unwrap(), &[Bin::new("o", i(1))]).await.unwrap();

    let counted: Vec<i64> = (0..20).filter(|k| k % 4 >= 2).collect();
    let want = list(vec![i(counted.len() as i64), i(counted.iter().sum())]);

    for (lang, package) in &packages {
        use futures::StreamExt;

        let mut statement = Statement::new(NAMESPACE, agg_set.clone(), Bins::All);

        statement.set_aggregate_function(package.clone(), "agg", &[]);

        let records = client.query(&QueryPolicy::default(), PartitionFilter::all(), statement).await.unwrap();
        let mut partials = Vec::new();
        let mut stream = records.into_stream();

        while let Some(record) = stream.next().await {
            match record {
                Ok(record) => partials.extend(record.bins.values().map(norm)),
                Err(e) => partials.push(s(&format!("failed: {e}"))),
            }
        }

        if show_all {
            println!("agg [{lang}] = {}", partials.iter().map(show).collect::<Vec<_>>().join(", "));
        }

        // The server answers a partial per piece of work it split the query into, even on one node: added
        // up, they are the answer.
        let mut total = (0i64, 0i64);
        let mut well_formed = !partials.is_empty();

        for partial in &partials {
            match partial {
                Value::List(pair) if pair.len() == 2 => match (&pair[0], &pair[1]) {
                    (Value::Int(count), Value::Int(sum)) => {
                        total.0 += count;
                        total.1 += sum;
                    }
                    _ => well_formed = false,
                },
                _ => well_formed = false,
            }
        }

        if !well_formed || list(vec![i(total.0), i(total.1)]) != want {
            problems.push(format!(
                "agg [{lang}]: answered {}, the table says {}",
                partials.iter().map(show).collect::<Vec<_>>().join(", "),
                show(&want)
            ));
        }
    }

    for (_, package) in &packages {
        let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
    }

    let _ = client.truncate(&AdminPolicy::default(), NAMESPACE, &set, 0).await;
    let _ = client.truncate(&AdminPolicy::default(), NAMESPACE, &agg_set, 0).await;

    assert!(problems.is_empty(), "{} problem(s):\n{}", problems.len(), problems.join("\n"));
}

#[test]
fn the_table_builds() {
    let table = cases();
    let mut names = HashMap::new();

    for c in &table {
        assert!(names.insert(c.name.clone(), ()).is_none(), "two cases named {}", c.name);
    }
}
