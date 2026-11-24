// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
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

//! `apitest`: the conformance module, as the Rust SDK writes it.
//!
//! The same module exists in every guest SDK (Rust, Go, C, C++, Java), written to one specification. A
//! harness calls every export of all five with the same record and the same arguments and compares the
//! answers, so what matters here is that each export uses the SDK's public API the way a user would: no
//! host import is called directly, and where the SDK answers in its own way the module passes that answer
//! on rather than smoothing it over.
//!
//! Most exports answer a list whose first element is a tag: `"some"` with a value after it, `"none"` for
//! the SDK's ordinary "nothing" (`Ok(None)`), or an [`Error`] by what it is (see [`tag`]). `mk` and `res`
//! answer the value itself.
//!
//! This is a test instrument, not an example of a UDF. `tests/fixtures/wasm/apitest.wasm` is its build.

#![no_std]

extern crate alloc;

use alloc::{format, string::String, vec, vec::Vec};

use aerospike_udf_sdk_wasm::{
    self as as_udf, Args, Coll, Error, EventRecord, Output, Pair, Record, Resolution, Return, Text, Value,
};

//==========================================================
// Answers.
//

/// An SDK error as the tag the specification gives its status.
fn tag(e: Error) -> &'static str {
    match e {
        Error::Absent => "absent",
        Error::WrongType => "wrong",
        Error::TooSmall(_) => "small",
        Error::Refused => "refused",
        Error::Malformed | Error::Custom(_) => "err",
    }
}

/// The status of a call that answers nothing: `"ok"`, or the error's tag.
fn status(r: Result<(), Error>) -> &'static str {
    match r {
        Ok(()) => "ok",
        Err(e) => tag(e),
    }
}

/// `[tag]`
fn only(tag: &str) -> Result<Output<'static>, Error> {
    let out = as_udf::list(1)?;

    out.push(tag)?;

    Ok(out.into())
}

/// `["some", value]`
fn some<'a>(value: impl Into<Value<'a>>) -> Result<Output<'static>, Error> {
    let out = as_udf::list(2)?;

    out.push("some")?.push(value)?;

    Ok(out.into())
}

/// What a typed reader gave: `["some", value]`, `["none"]`, or the error's tag.
fn read<'a, T: Into<Value<'a>>>(r: Result<Option<T>, Error>) -> Result<Output<'static>, Error> {
    match r {
        Ok(Some(value)) => some(value),
        Ok(None) => only("none"),
        Err(e) => only(tag(e)),
    }
}

/// What a reader with a default gave: `["some", value]`, or the error's tag.
fn read_or<'a, T: Into<Value<'a>>>(r: Result<T, Error>) -> Result<Output<'static>, Error> {
    match r {
        Ok(value) => some(value),
        Err(e) => only(tag(e)),
    }
}

/// The kind of a value, by its variant: a Rust caller matches on [`Value`], whose own
/// [`kind`](Value::kind) is an English phrase for a message.
fn kind(v: &Value<'_>) -> &'static str {
    match v {
        Value::Nil => "nil",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Double(_) => "double",
        Value::String(_) => "string",
        Value::Blob(_) => "blob",
        Value::List(_) => "list",
        Value::Map(_) => "map",
        Value::GeoJson(_) => "geojson",
        Value::Hll(_) => "hll",
        Value::Other => "other",
    }
}

//==========================================================
// What `wr` and `mk` need.
//

/// The bytes of a string or a blob, which are the two kinds `wr`'s `blob` takes: anything else is
/// `WrongType`, a GeoJson document and an HLL included, though the SDK's `bytes` reader would take them.
fn bytes_of(v: &Value<'_>) -> Result<Vec<u8>, Error> {
    match v {
        Value::String(_) | Value::Blob(_) => v.bytes()?.to_vec(),
        _ => Err(Error::WrongType),
    }
}

/// The most elements `wr` hands to `set_ints` and `set_doubles`.
const MAX_NUMBERS: usize = 256;

/// The integers of a list. Anything else is `WrongType`: a value that is not a list, a list with an element
/// that is not an integer, a list of more than 256.
fn ints_of(v: &Value<'_>) -> Result<Vec<i64>, Error> {
    let list = v.list()?;

    if list.len() > MAX_NUMBERS {
        return Err(Error::WrongType);
    }

    let mut out = Vec::with_capacity(list.len());

    for item in list.iter() {
        out.push(item?.int()?);
    }

    Ok(out)
}

/// The doubles of a list, with the same rules: an integer among them is `WrongType`.
fn doubles_of(v: &Value<'_>) -> Result<Vec<f64>, Error> {
    let list = v.list()?;

    if list.len() > MAX_NUMBERS {
        return Err(Error::WrongType);
    }

    let mut out = Vec::with_capacity(list.len());

    for item in list.iter() {
        out.push(item?.double()?);
    }

    Ok(out)
}

/// A builder's failure as `mk` reports it: the call fails with `mk: <tag>`.
fn mk_error(e: Error) -> Error {
    Error::Custom(match tag(e) {
        "absent" => "mk: absent",
        "wrong" => "mk: wrong",
        "small" => "mk: small",
        "refused" => "mk: refused",
        _ => "mk: err",
    })
}

/// Element `i` of `list_mixed`, handed to `f`: the string and the blob among them live only that long.
fn with_mixed<R>(i: i64, f: impl FnOnce(Value<'_>) -> R) -> R {
    match i % 6 {
        0 => f(Value::Int(i)),
        1 => f(Value::Double(i as f64 + 0.5)),
        2 => f(Value::Bool(i % 4 == 2)),
        3 => f(Value::from(format!("s{i}").as_str())),
        4 => f(Value::from(&[(i & 0xff) as u8][..])),
        _ => f(Value::Nil),
    }
}

/// What `mk` answers: a collection the host holds, or a string or blob the function built and owns. A
/// [`Value`] only borrows a string, so the owned kinds are returned as themselves; each is delivered as the
/// SDK delivers it.
enum Made {
    Value(Output<'static>),
    Text(String),
    Bytes(Vec<u8>),
}

impl Return for Made {
    fn deliver(self) -> Result<(), Error> {
        match self {
            Made::Value(value) => value.deliver(),
            Made::Text(text) => text.deliver(),
            Made::Bytes(bytes) => bytes.deliver(),
        }
    }
}

/// The largest `n` `mk` builds.
const MK_MAX: i64 = 1_000_000;

fn mk_value(what: &str, n: i64) -> Result<Made, Error> {
    // Refused before anything is built.
    if !(0..=MK_MAX).contains(&n) {
        return Err(Error::Refused);
    }

    let room = n as usize;

    let built = match what {
        "list_int" => {
            let out = as_udf::list(room)?;

            for i in 0..n {
                out.push(i)?;
            }

            out.into()
        }
        "list_mixed" => {
            let out = as_udf::list(room)?;

            for i in 0..n {
                with_mixed(i, |v| out.push(v).map(|_| ()))?;
            }

            out.into()
        }
        "map_str" => {
            let out = as_udf::map(room)?;

            for i in 0..n {
                out.insert(format!("k{i}").as_str(), i)?;
            }

            out.into()
        }
        "map_int" => {
            let out = as_udf::map(room)?;

            for i in 0..n {
                out.insert(i, format!("v{i}").as_str())?;
            }

            out.into()
        }
        "map_mixed" => {
            let out = as_udf::map(room)?;

            for i in 0..n {
                with_mixed(i, |v| out.insert(format!("k{i}").as_str(), v).map(|_| ()))?;
            }

            out.into()
        }
        "nested" => {
            let mut out = as_udf::list(1)?;

            out.push(n)?;

            for _ in 1..n {
                let outer = as_udf::list(1)?;

                // Deeper than the host nests, or more lists than a call may hold: refused.
                outer.push(&out)?;
                out = outer;
            }

            out.into()
        }
        "string" => return Ok(Made::Text("x".repeat(room))),
        "blob" => return Ok(Made::Bytes((0..room).map(|i| (i & 0xff) as u8).collect())),
        _ => return Err(Error::Custom("mk: unknown what")),
    };

    Ok(Made::Value(built))
}

//==========================================================
// What `res` answers with.
//

const POINT: &str = r#"{"type":"Point","coordinates":[1,2]}"#;

const XS: [u8; 600] = [b'x'; 600];

/// 600 bytes of `x`: longer than the message of a failure may be.
const LONG: &str = match core::str::from_utf8(&XS) {
    Ok(text) => text,
    Err(_) => "",
};

//==========================================================
// The record UDFs.
//

as_udf::export! {
    /// Read bin `name` with one reader of the record.
    fn rd(rec: &Record, name: String, how: String) -> Result<Output, Error> {
        let name = name.as_str();

        match how.as_str() {
            "has" => some(rec.has(name)),
            "get" => read(rec.get(name)),
            "bin" => read_or(rec.bin(name)),
            "int" => read(rec.int(name)),
            "double" => read(rec.double(name)),
            "bool" => read(rec.bool(name)),
            "string" => read(rec.string(name).map(|text| text.map(Value::String))),
            "blob" => read(rec.blob(name).map(|blob| blob.map(Value::Blob))),
            "list" => read(rec.list(name).map(|list| list.map(Value::List))),
            "map" => read(rec.map(name).map(|map| map.map(Value::Map))),
            "int_or" => read_or(rec.int_or(name, 7)),
            "double_or" => read_or(rec.double_or(name, 7.5)),
            "bool_or" => read_or(rec.bool_or(name, true)),
            _ => Err(Error::Custom("rd: unknown how")),
        }
    }

    /// Stage one write and save: `[stage, save, bins]`.
    ///
    /// `v` is taken as the rest of the arguments, of which there may be one: an optional parameter would
    /// make a `v` that was not passed and one passed as nil the same `None`, and `set` writes a nil (which
    /// removes the bin) but has nothing to write when there is no `v`.
    fn wr(rec: &mut Record, name: String, how: String, v: Args) -> Result<Output, Error> {
        if v.len() > 1 {
            return Err(Error::Custom("wr takes at most 3 arguments"));
        }

        let name = name.as_str();
        let v = v.get(0);
        // A `v` that is missing, or of a kind the write cannot take, is `WrongType`, and nothing is staged.
        let given = v.ok_or(Error::WrongType);

        let staged = match how.as_str() {
            "set" => given.and_then(|v| rec.set(name, v)),
            "blob" => given.and_then(|v| bytes_of(&v)).and_then(|bytes| rec.set_blob(name, &bytes)),
            "geojson" => given
                .and_then(|v| v.string())
                .and_then(Text::to_string)
                .and_then(|text| rec.set_geojson(name, &text)),
            "ints" => given.and_then(|v| ints_of(&v)).and_then(|ints| rec.set_ints(name, &ints)),
            "doubles" => given.and_then(|v| doubles_of(&v)).and_then(|doubles| rec.set_doubles(name, &doubles)),
            "remove" => rec.remove(name),
            _ => return Err(Error::Custom("wr: unknown how")),
        };

        let saved = match staged {
            Ok(()) => status(rec.save()),
            Err(_) => "skipped",
        };

        let out = as_udf::list(3)?;

        out.push(status(staged))?.push(saved)?.push(rec.bin_count())?;

        Ok(out.into())
    }

    /// Everything the record says about itself, as a map.
    fn meta(rec: &Record) -> Result<Output, Error> {
        let out = as_udf::map(8)?;

        out.insert("exists", rec.exists())?
            .insert("generation", rec.generation())?
            .insert("ttl", rec.ttl().map_or(-1, i64::from))?
            .insert("bins", rec.bin_count())?
            .insert("sized", rec.size() > 0)?;

        let mut set = [0u8; 64];

        match rec.set_name(&mut set) {
            Ok(name) => out.insert("set", name)?,
            Err(Error::Absent) => out.insert("set", "")?,
            Err(e) => return Err(e),
        };

        // A key is copied into a buffer of the caller's, which is told how long it is when it does not fit.
        let mut key = vec![0u8; 256];

        if let Err(Error::TooSmall(n)) = rec.key(&mut key) {
            key.resize(n, 0);
        }

        match rec.key(&mut key) {
            Ok(value) => out.insert("key", value)?,
            Err(Error::Absent) => out.insert("key", Value::Nil)?,
            Err(e) => return Err(e),
        };

        let mut digest = [0u8; 20];

        out.insert("digest", rec.digest(&mut digest)?.len() as i64)?;

        Ok(out.into())
    }

    /// The kind of a value and the value passed back: `[kind, v]`, and its length for a list or map.
    fn val(v: Value) -> Result<Output, Error> {
        let out = as_udf::list(3)?;

        out.push(kind(&v))?.push(v)?;

        if let Value::List(coll) | Value::Map(coll) = v {
            out.push(coll.len() as i64)?;
        }

        Ok(out.into())
    }

    /// Build a value with the builders, an element per call, and answer it.
    fn mk(what: String, n: i64) -> Result<Made, Error> {
        mk_value(what.as_str(), n).map_err(|e| match e {
            Error::Custom(_) => e,
            e => mk_error(e),
        })
    }

    /// Walk the list or map in bin `name` with iteration and lookup.
    fn walk(rec: &Record, name: String) -> Result<Output, Error> {
        let coll = match rec.get(name.as_str()) {
            Ok(Some(value)) => value,
            Ok(None) => return only("none"),
            Err(e) => return only(tag(e)),
        };

        let out = as_udf::list(7)?;
        let mut sum = 0i64;

        match coll {
            Value::List(list) => {
                let len = list.len();
                let mut strings = 0i64;

                for item in list.iter() {
                    let item = item?;

                    if let Value::Int(i) = item {
                        sum = sum.wrapping_add(i);
                    } else if item.is_string() {
                        strings += 1;
                    }
                }

                // An index with nothing at it is `None`, an empty list's first and last among them.
                let first = list.at(0)?.unwrap_or(Value::Nil);
                let last = match len.checked_sub(1) {
                    Some(i) => list.at(i)?.unwrap_or(Value::Nil),
                    None => Value::Nil,
                };

                let past = match list.at(len) {
                    Ok(Some(_)) => "some",
                    Ok(None) => "none",
                    Err(e) => tag(e),
                };

                out.push("list")?
                    .push(len as i64)?
                    .push(sum)?
                    .push(strings)?
                    .push(first)?
                    .push(last)?
                    .push(past)?;
            }
            Value::Map(map) => {
                for entry in map.entries() {
                    let (_, value) = entry?;

                    if let Value::Int(i) = value {
                        sum = sum.wrapping_add(i);
                    }
                }

                let at_k1 = map.get("k1")?;
                let at_1 = map.get(1i64)?;

                out.push("map")?
                    .push(map.len() as i64)?
                    .push(sum)?
                    .push(at_k1.is_some())?
                    .push(at_1.is_some())?
                    .push(at_k1.unwrap_or(Value::Nil))?
                    .push(at_1.unwrap_or(Value::Nil))?;
            }
            _ => return only("wrong"),
        }

        Ok(out.into())
    }

    /// The arguments as they were decoded: `[a, b, count, kinds...]`.
    fn ar(a: i64, b: Option<String>, rest: Args) -> Result<Output, Error> {
        let out = as_udf::list(3 + rest.len())?;

        out.push(a)?;

        match &b {
            Some(text) => out.push(text.as_str())?,
            None => out.push(Value::Nil)?,
        };

        out.push(rest.len() as i64)?;

        for v in rest.iter() {
            out.push(kind(&v))?;
        }

        Ok(out.into())
    }

    /// What staging, saving, deleting and failing do to the record.
    fn life(rec: &mut Record, what: String) -> Result<Output, Error> {
        match what.as_str() {
            "staged" => {
                if let Err(e) = rec.set("t", 1) {
                    return only(tag(e));
                }

                let out = as_udf::list(3)?;

                match rec.int_or("t", -1) {
                    Ok(t) => out.push("some")?.push(t)?,
                    Err(e) => out.push(tag(e))?,
                };

                out.push(rec.bin_count())?;

                Ok(out.into())
            }
            "save_twice" => {
                if let Err(e) = rec.set("t", 1) {
                    return only(tag(e));
                }

                let first = status(rec.save());

                if let Err(e) = rec.set("t", 2) {
                    return only(tag(e));
                }

                let second = status(rec.save());
                let out = as_udf::list(2)?;

                out.push(first)?.push(second)?;

                Ok(out.into())
            }
            "delete" => {
                let deleted = status(rec.delete());
                let out = as_udf::list(2)?;

                out.push(deleted)?.push(rec.exists())?;

                Ok(out.into())
            }
            "ttl" => {
                // A TTL is staged like a bin: it is the save that writes it.
                if let Err(e) = rec.set_ttl(100) {
                    return only(tag(e));
                }

                let saved = status(rec.save());
                let out = as_udf::list(2)?;

                out.push("ok")?.push(saved)?;

                Ok(out.into())
            }
            "fail_after_save" => {
                if let Err(e) = rec.set("t", 9) {
                    return only(tag(e));
                }

                rec.save()?;

                Err(Error::Custom("life: failing"))
            }
            _ => Err(Error::Custom("life: unknown what")),
        }
    }

    /// A result of each kind, answered as itself.
    fn res(kind: String) -> Result<Output, Error> {
        match kind.as_str() {
            "nil" => Ok(Value::Nil.into()),
            "int" => Ok(Value::Int(i64::MIN).into()),
            "double" => Ok(Value::Double(1e308).into()),
            "bool" => Ok(Value::Bool(true).into()),
            "string" => Ok(Value::from("").into()),
            "blob" => Ok(Value::from(&[0u8; 0][..]).into()),
            "geojson" => Ok(Value::GeoJson(Text::from(POINT)).into()),
            "list" => Ok(as_udf::list(0)?.into()),
            "map" => Ok(as_udf::map(0)?.into()),
            "fail_long" => Err(Error::Custom(LONG)),
            _ => Err(Error::Custom("res: unknown kind")),
        }
    }

    /// A string of any length: `[len, s, first]`.
    fn txt(s: String) -> Result<Output, Error> {
        let out = as_udf::list(3)?;

        out.push(s.len() as i64)?
            .push(s.as_str())?
            .push(s.as_bytes().first().map_or(-1, |b| i64::from(*b)))?;

        Ok(out.into())
    }

    /// Compare the stored values of two bins.
    fn cmp(rec: &Record, a: String, b: String) -> Result<Output, Error> {
        let (a, b) = match (rec.get(a.as_str()), rec.get(b.as_str())) {
            (Ok(Some(a)), Ok(Some(b))) => (a, b),
            (Err(e), _) | (_, Err(e)) => return only(tag(e)),
            _ => return only("none"),
        };

        match a.cmp_stored(&b) {
            Ok(order) => some(order as i64),
            Err(e) => only(tag(e)),
        }
    }
}

//==========================================================
// The stream UDF.
//
// One instance sees every record a query thread reads, so the accumulators are plain statics.
//

static mut COUNT: i64 = 0;
static mut SUM: i64 = 0;

as_udf::stream! {
    /// Count the records that have an integer bin `n`, and add it up: `[count, sum]`.
    fn agg {
        record(rec: &Record) -> Result<(), Error> {
            // A record without the bin, or with one of another kind, is not counted.
            if let Ok(Some(n)) = rec.int("n") {
                unsafe {
                    COUNT += 1;
                    SUM = SUM.wrapping_add(n);
                }
            }

            Ok(())
        }

        end() -> Result<Output, Error> {
            let out = as_udf::list(2)?;

            unsafe {
                out.push(COUNT)?.push(SUM)?;

                // Left as it was found, for whichever query this instance serves next.
                COUNT = 0;
                SUM = 0;
            }

            Ok(out.into())
        }
    }
}

//==========================================================
// The merge handler.
//

/// `[generation, last_update_time, ttl, source_id, a, b]` of one side.
fn side_meta(side: &EventRecord, a: bool, b: bool) -> Result<as_udf::List, Error> {
    let out = as_udf::list(6)?;

    out.push(side.generation())?
        .push(side.last_update_time())?
        .push(side.ttl())?
        .push(side.source_id())?
        .push(a)?
        .push(b)?;

    Ok(out)
}

/// `[of current, of incoming]`
fn sides(current: i64, incoming: i64) -> Result<as_udf::List, Error> {
    let out = as_udf::list(2)?;

    out.push(current)?.push(incoming)?;

    Ok(out)
}

as_udf::event! {
    /// Everything a handler can ask of the two sides, as the bins of the final record.
    on xdr_merge
    fn merge_api(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let pair = Pair::of(current, incoming);
        let out = as_udf::map(18)?;

        // A `Both` is read once, so the bin is asked for once per merge.
        if let Some(max) = pair.int("n")?.max() {
            out.insert("max", max)?;
        }

        if let Some(min) = pair.int("n")?.min() {
            out.insert("min", min)?;
        }

        // A missing side counts as zero; neither side, and `sum` answers nothing, as `max` and `min` do.
        if let Some(sum) = pair.int("n")?.sum() {
            out.insert("sum", sum)?;
        }

        if let Some(text) = pair.string("s")?.newer() {
            out.insert("newer_s", Value::String(text))?;
        }

        if let Some(text) = pair.string("s")?.older() {
            out.insert("older_s", Value::String(text))?;
        }

        if let Some(text) = pair.string("s")?.current_or_incoming() {
            out.insert("c_or_i", Value::String(text))?;
        }

        if let Some(text) = pair.string("s")?.incoming_or_current() {
            out.insert("i_or_c", Value::String(text))?;
        }

        out.insert("i_newer", pair.incoming_is_newer("s"))?
            .insert("c_names", names(current.bin_names()))?
            .insert("i_names", names(incoming.bin_names()))?
            .insert("c_del", names(current.deleted_bins()))?
            .insert("i_del", names(incoming.deleted_bins()))?
            .insert(
                "c_meta",
                &side_meta(&current, current.is_tombstone(), current.uses_bin_convergence())?,
            )?
            .insert("i_meta", &side_meta(&incoming, incoming.is_delete(), incoming.is_partial())?)?
            .insert(
                "blut",
                &sides(current.bin_last_update_time("s"), incoming.bin_last_update_time("s"))?,
            )?
            .insert("bsrc", &sides(current.bin_source_id("s"), incoming.bin_source_id("s"))?)?;

        Ok(Resolution::Record { bins: out, ttl: None })
    }
}

/// A list of names one side gave, as the value it is.
fn names(list: Coll) -> Value<'static> {
    Value::List(list)
}

/// A `no_std` cdylib needs a panic handler. A trap, which the host reports with the function's name, is
/// more useful than anything a guest could say for itself.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}
