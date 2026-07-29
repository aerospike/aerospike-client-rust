// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0

//! In-process tests for the client-side Lua aggregation pipeline. These
//! drive `apply_stream` exactly like `Client::query_aggregate` does — with
//! channel-backed input/output streams — but feed the input directly
//! instead of from a server, so no cluster is required.

use std::collections::HashMap;

use super::values::{lua_to_value, value_to_lua};
use super::*;
use crate::Value;

/// Run the client-side portion of `function` from an in-memory `source`
/// package over `input`, returning everything written to the output stream.
fn run(
    package: &str,
    source: &str,
    function: &str,
    args: Vec<Value>,
    input: Vec<Value>,
) -> Result<Vec<Value>> {
    register_package(package, source);
    let (input_tx, input_rx) = async_channel::unbounded();
    let (output_tx, output_rx) = async_channel::unbounded();
    for value in input {
        input_tx.send_blocking(value).expect("input send");
    }
    drop(input_tx); // close the input stream
    futures::executor::block_on(run_aggregate_pipeline(
        package, function, args, input_rx, output_tx,
    ))?;
    let mut out = Vec::new();
    while let Ok(value) = output_rx.try_recv() {
        out.push(value);
    }
    Ok(out)
}

#[test]
fn map_reduce_sums_partials() {
    // Client scope of map+reduce = the reduce only: sums the per-node
    // partial sums.
    let source = r"
        function sum_test(s, bin)
            local function mapper(rec) return rec[bin] end
            local function reducer(a, b) return a + b end
            return s : map(mapper) : reduce(reducer)
        end
    ";
    let out = run(
        "pipeline_sum",
        source,
        "sum_test",
        vec![as_val!("bin1")],
        vec![as_val!(10), as_val!(20), as_val!(30)],
    )
    .unwrap();
    assert_eq!(out, vec![as_val!(60)]);
}

#[test]
fn aggregate_reduce_map_computes_average() {
    // Client scope = reduce (merge the partial sum/count maps) followed by
    // the final map (divide). Exercises LuaMap indexing and float results.
    let source = r"
        function average_test(s, bin)
            local function agg(out, rec)
                out['sum'] = out['sum'] + rec[bin]
                out['count'] = out['count'] + 1
                return out
            end
            local function red(a, b)
                local out = map()
                out['sum'] = a['sum'] + b['sum']
                out['count'] = a['count'] + b['count']
                return out
            end
            local function fin(v) return v['sum'] / v['count'] end
            return s : aggregate(map{sum = 0, count = 0}, agg) : reduce(red) : map(fin)
        end
    ";
    let partial = |sum: i64, count: i64| as_map!("sum" => sum, "count" => count);
    let out = run(
        "pipeline_avg",
        source,
        "average_test",
        vec![as_val!("bin1")],
        vec![partial(6, 3), partial(4, 2)],
    )
    .unwrap();
    assert_eq!(out, vec![as_val!(2.0)]);
}

#[test]
fn groupby_merges_partial_group_maps() {
    // groupby => aggregate(map(), _agg) : reduce(map.merge with a list
    // merge function). The client reduce merges the per-node group maps.
    // Exercises map.merge with a Lua merge function, list.clone,
    // list.append, and list.iterator.
    let source = r"
        function group_test(s)
            return s : groupby(function(v) return v % 2 end)
        end
    ";
    let group = |entries: Vec<(i64, Vec<i64>)>| {
        let mut m = HashMap::new();
        for (k, items) in entries {
            m.insert(
                Value::Int(k),
                Value::List(items.into_iter().map(Value::Int).collect()),
            );
        }
        Value::HashMap(m)
    };
    let out = run(
        "pipeline_group",
        source,
        "group_test",
        vec![],
        vec![
            group(vec![(0, vec![2, 4]), (1, vec![1])]),
            group(vec![(0, vec![6]), (1, vec![3, 5])]),
        ],
    )
    .unwrap();

    assert_eq!(out.len(), 1);
    let Value::HashMap(groups) = &out[0] else {
        panic!("expected a map result, got {:?}", out[0]);
    };
    assert_eq!(groups.len(), 2);
    let Some(Value::List(even)) = groups.get(&Value::Int(0)) else {
        panic!("missing even group");
    };
    let Some(Value::List(odd)) = groups.get(&Value::Int(1)) else {
        panic!("missing odd group");
    };
    assert_eq!(even.len(), 3);
    assert_eq!(odd.len(), 3);
}

#[test]
fn filter_in_client_scope_after_reduce() {
    // Ops after the reduce run on the client: filter the merged list.
    let source = r"
        function filter_test(s, threshold)
            local function red(a, b)
                return list.merge(a, b)
            end
            local function big(l)
                local out = list()
                for v in list.iterator(l) do
                    if v >= threshold then
                        list.append(out, v)
                    end
                end
                return out
            end
            return s : reduce(red) : map(big)
        end
    ";
    let out = run(
        "pipeline_filter",
        source,
        "filter_test",
        vec![as_val!(10)],
        vec![
            Value::List(vec![as_val!(5), as_val!(15)]),
            Value::List(vec![as_val!(25), as_val!(3)]),
        ],
    )
    .unwrap();
    assert_eq!(out, vec![Value::List(vec![as_val!(15), as_val!(25)])]);
}

#[test]
fn missing_function_is_an_error() {
    let err = run("pipeline_nofn", "-- empty package", "no_such_fn", vec![], vec![]).unwrap_err();
    assert!(err.to_string().contains("no_such_fn"), "{err}");
}

// Needs a runtime: the missing-package path reads the filesystem via
// `aerospike_rt::fs`.
#[aerospike_macro::test]
async fn missing_package_is_an_error() {
    let (_input_tx, input_rx) = async_channel::unbounded::<Value>();
    let (output_tx, _output_rx) = async_channel::unbounded();
    let err = run_aggregate_pipeline(
        "no_such_package_xyz",
        "f",
        vec![],
        input_rx,
        output_tx,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("no_such_package_xyz"), "{err}");
}

#[test]
fn lua_runtime_error_is_reported() {
    let source = r"
        function boom_test(s)
            return s : reduce(function(a, b) return a + error('kaput') end)
        end
    ";
    let err = run(
        "pipeline_boom",
        source,
        "boom_test",
        vec![],
        vec![as_val!(1), as_val!(2)],
    )
    .unwrap_err();
    assert!(err.to_string().contains("kaput"), "{err}");
}

#[test]
fn value_conversion_round_trip() {
    let lua = new_instance().unwrap();

    // Scalars survive the round trip with types intact.
    let cases = vec![
        Value::Nil,
        as_val!(true),
        as_val!(42),
        as_val!(-7),
        as_val!(3.25),
        as_val!("hello"),
    ];
    for case in cases {
        let lua_value = value_to_lua(&lua, case.clone()).unwrap();
        assert_eq!(lua_to_value(&lua_value).unwrap(), case, "{case:?}");
    }

    // Nested list/map structures survive via the userdata wrappers.
    let nested = Value::List(vec![
        as_val!(1),
        as_map!("k" => "v", "n" => 2),
        Value::List(vec![as_val!(2.5)]),
    ]);
    let lua_value = value_to_lua(&lua, nested.clone()).unwrap();
    assert_eq!(lua_to_value(&lua_value).unwrap(), nested);

    // Blobs, HLLs, and GeoJSON become typed userdata and round-trip
    // without losing their variant.
    for case in [
        Value::Blob(vec![1, 2, 255]),
        Value::HLL(vec![3, 4, 5]),
        Value::GeoJSON(r#"{"type":"Point","coordinates":[1.0,2.0]}"#.to_owned()),
    ] {
        let lua_value = value_to_lua(&lua, case.clone()).unwrap();
        assert_eq!(lua_to_value(&lua_value).unwrap(), case, "{case:?}");
    }
}

#[test]
fn bytes_library_semantics() {
    let lua = new_instance().unwrap();
    let script = r"
        local b = bytes(16)
        assert(bytes.size(b) == 0 and #b == 0)
        assert(bytes.get_type(b) == 4)  -- BLOB

        -- appends grow the buffer
        assert(bytes.append_byte(b, 0xFF))
        assert(bytes.append_int16(b, 0x0102))
        assert(bytes.append_int32_le(b, 0x03040506))
        assert(bytes.append_int64(b, 0x0708090A0B0C0D0E))
        assert(#b == 1 + 2 + 4 + 8)

        -- 1-based indexing; get_byte is unsigned
        assert(b[1] == 0xFF and bytes.get_byte(b, 1) == 0xFF)
        assert(bytes.get_int16(b, 2) == 0x0102)
        assert(bytes.get_int16_be(b, 2) == 0x0102)
        assert(bytes.get_int32_le(b, 4) == 0x03040506)
        assert(bytes.get_int64_be(b, 8) == 0x0708090A0B0C0D0E)

        -- endianness is real: the LE int32 read back as BE differs
        assert(bytes.get_int32(b, 4) ~= 0x03040506)

        -- __newindex writes a byte, auto-growing
        b[20] = 0x7F
        assert(#b == 20 and b[20] == 0x7F and b[16] == 0)

        -- out-of-range reads are 0; setters with bad offsets return false
        assert(bytes.get_byte(b, 99) == 0)
        assert(bytes.get_int64(b, 99) == 0)
        assert(bytes.set_int32(b, 0, 1) == false)

        -- strings and nested bytes
        local s = bytes()
        assert(bytes.set_string(s, 1, 'hello'))
        assert(bytes.get_string(s, 1, 5) == 'hello')
        assert(bytes.append_string(s, ' world'))
        assert(bytes.get_string(s, 1, 11) == 'hello world')
        local sub = bytes.get_bytes(s, 7, 5)
        assert(bytes.get_string(sub, 1, 5) == 'world')
        assert(bytes.set_bytes(s, 1, sub))
        assert(bytes.get_string(s, 1, 11) == 'world world')

        -- var ints round-trip and report their width
        local v = bytes()
        local written = bytes.append_var_int(v, 300)
        assert(written == 2)
        local value, size = bytes.get_var_int(v, 1)
        assert(value == 300 and size == 2)
        assert(bytes.set_var_int(v, 1, 5) == 1)
        assert(bytes.get_var_int(v, 1) == 5)

        -- type tag is mutable
        assert(bytes.set_type(v, 18))  -- HLL
        assert(bytes.get_type(v) == 18)

        assert(bytes.is_bytes(b) and not bytes.is_bytes('nope'))
        assert(tostring(bytes.get_bytes(s, 1, 2)) == '776F')
        return true
    ";
    let ok: bool = lua.load(script).eval().unwrap();
    assert!(ok);
}

#[test]
fn bytes_flow_through_the_pipeline() {
    // A client-scope reduce that concatenates blob partials via the
    // `bytes` library, proving Blob values arrive as bytes userdata and
    // return as Blob.
    let source = r"
        function concat_blobs(s)
            local function red(a, b)
                local out = bytes()
                bytes.append_bytes(out, a)
                bytes.append_bytes(out, b)
                return out
            end
            return s : reduce(red)
        end
    ";
    let out = run(
        "pipeline_bytes",
        source,
        "concat_blobs",
        vec![],
        vec![Value::Blob(vec![1, 2]), Value::Blob(vec![3])],
    )
    .unwrap();
    assert_eq!(out, vec![Value::Blob(vec![1, 2, 3])]);
}

#[test]
fn geojson_stringifies_in_lua() {
    let lua = new_instance().unwrap();
    let geo = r#"{"type":"Point","coordinates":[1.0,2.0]}"#;
    let lua_value = value_to_lua(&lua, Value::GeoJSON(geo.to_owned())).unwrap();
    lua.globals().set("g", lua_value).unwrap();
    let text: String = lua.load("return tostring(g)").eval().unwrap();
    assert_eq!(text, geo);
}

#[test]
fn lua_tables_convert_by_shape() {
    let lua = new_instance().unwrap();

    // Sequences become lists.
    let seq: mlua::Value = lua.load("return {1, 2, 3}").eval().unwrap();
    assert_eq!(
        lua_to_value(&seq).unwrap(),
        Value::List(vec![as_val!(1), as_val!(2), as_val!(3)])
    );

    // Keyed tables become maps.
    let keyed: mlua::Value = lua.load("return {a = 1}").eval().unwrap();
    assert_eq!(lua_to_value(&keyed).unwrap(), as_map!("a" => 1));

    // Empty tables become empty maps.
    let empty: mlua::Value = lua.load("return {}").eval().unwrap();
    assert_eq!(lua_to_value(&empty).unwrap(), Value::HashMap(HashMap::new()));
}

#[test]
fn list_and_map_library_semantics() {
    let lua = new_instance().unwrap();
    let script = r"
        local l = list{1, 2, 3}
        assert(list.size(l) == 3)
        assert(#l == 3)
        assert(l[2] == 2)
        list.append(l, 4)
        list.prepend(l, 0)
        assert(#l == 5 and l[1] == 0 and l[5] == 4)
        local taken = list.take(l, 2)
        assert(#taken == 2 and taken[2] == 1)
        local dropped = list.drop(l, 3)
        assert(#dropped == 2 and dropped[1] == 3)
        assert(list.is_list(l) and not list.is_map)

        local m = map{a = 1, b = 2}
        assert(map.size(m) == 2)
        assert(m['a'] == 1)
        m['c'] = 3
        assert(map.size(m) == 3)
        m['c'] = nil
        assert(map.size(m) == 2)
        assert(map.is_map(m) and not map.is_map(l))
        local merged = map.merge(m, map{b = 10, d = 4}, function(v1, v2) return v1 + v2 end)
        assert(merged['a'] == 1 and merged['b'] == 12 and merged['d'] == 4)

        local total = 0
        for k, v in map.pairs(merged) do
            total = total + v
        end
        assert(total == 17)
        return true
    ";
    let ok: bool = lua.load(script).eval().unwrap();
    assert!(ok);
}
