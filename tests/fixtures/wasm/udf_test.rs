//! WebAssembly UDFs for the WAMR engine.
//!
//! Deliberately the **same functions** as the Lua script beside it, so the runtimes can be compared on
//! identical work rather than on whatever each SDK made convenient. This is also the fixture
//! `tests/src/udf_wasm.rs` registers, so its functions and their answers are a contract with those tests.
//!
//! What differs from Lua is how a function gets its data: a bin is one import call returning a [`Value`],
//! a collection iterates in windows of 128 values per import, arguments are typed in the signature, and the
//! function's own `Result` is its answer. There is no msgpack here, and no allocator except where a function
//! asks for one: `echo` takes a `String`, so this crate enables the SDK's `allocator` feature - which is
//! what a UDF that only ever needs `&str` or `Str<N>` leaves off.

#![no_std]

extern crate alloc;

use alloc::{format, string::String, vec::Vec};

use aerospike_udf_sdk_wasm::{self as as_udf, Args, Coll, Error, Level, List, Str, Value};

as_udf::export! {
    /// Read a bin, write it back incremented, and return the new value.
    ///
    /// The read-modify-write a real UDF does, and the cheapest this ABI gets: two import calls, no buffer
    /// and no allocation on either side. A record without the bin starts from zero.
    fn increment() -> Result<Value, Error> {
        let n = match as_udf::bin("n") {
            Ok(value) => value.int()?,
            Err(Error::Absent) => 0,
            Err(e) => return Err(e),
        };

        as_udf::set("n", n + 1)?;

        // **Staged until committed** - the one mistake that looks like it worked. `set` only writes into
        // the record's own cache; without this the UDF returns a perfectly plausible new value and changes
        // nothing on disk. `create` when there is no record yet, `update` when there is: the server refuses
        // an update to a record that does not exist, so a counter starting from nothing needs both.
        if as_udf::exists() {
            as_udf::update()?;
        } else {
            as_udf::create()?;
        }

        Ok(Value::Int(n + 1))
    }

    /// Sum the integer arguments into the bin the first argument names.
    ///
    /// The macro has already checked that `name` is a string by the time this runs; `numbers` is whatever
    /// followed it, and each is checked as it is used.
    fn add_all(name: &str, numbers: Args) -> Result<Value, Error> {
        let mut total = match as_udf::bin(name) {
            Ok(value) => value.int()?,
            Err(Error::Absent) => 0,
            Err(e) => return Err(e),
        };

        for n in numbers.iter() {
            total += n.int()?;
        }

        as_udf::set(name, total)?;
        as_udf::update()?;

        Ok(Value::Int(total))
    }

    /// Read a GeoJson bin and write it straight back, to prove the particle type survives.
    ///
    /// **The point is what it does *not* do.** The document is written back as the geojson value it was
    /// read as - `set` sees [`Value::GeoJson`] and keeps the particle type - where writing its text back as a
    /// string would keep the bytes and quietly turn a geospatially indexable bin into an ordinary string.
    /// Reports the document's length so a caller can tell the round trip actually happened.
    fn geo_roundtrip(name: &str) -> Result<Value, Error> {
        let doc = as_udf::bin(name)?;

        let Value::GeoJson(text) = doc else {
            return Err(Error::Custom("that bin does not hold geojson"));
        };

        // Written back from its stored bytes: the guest never decodes the document.
        as_udf::set_value(name, &doc)?;
        as_udf::update()?;

        Ok(Value::Int(text.len() as i64))
    }

    /// Everything the record knows about itself, as a map.
    fn describe() -> Result<Value, Error> {
        let out = as_udf::map(6)?;

        out.put("generation", as_udf::generation())?
            .put("bins", as_udf::bin_count())?
            .put("size", as_udf::size())?;

        let mut buf = [0u8; 64];

        match as_udf::set_name(&mut buf) {
            Ok(set) => out.put("set", set)?,
            Err(_) => out.put("set", Value::Nil)?,
        };

        // The key is only there when the record was written with send_key.
        let mut key_buf = [0u8; 256];

        match as_udf::key(&mut key_buf) {
            Ok(key) => out.put("key", key)?,
            Err(_) => out.put("key", Value::Nil)?,
        };

        match as_udf::time_to_live() {
            Some(ttl) => out.put("ttl", i64::from(ttl))?,
            None => out.put("ttl", Value::Nil)?,
        };

        Ok(out.into_value())
    }

    /// A nested value, to prove collections survive the trip in both directions.
    ///
    /// The same shape the other SDKs return: `[1, "two", [true, nil], {"three": 3.5}]`.
    fn nested() -> Result<Value, Error> {
        let inner = as_udf::list(2)?;

        inner.push(true)?.push(Value::Nil)?;

        let tail = as_udf::map(1)?;

        tail.put("three", 3.5)?;

        let out = as_udf::list(4)?;

        out.push(1)?.push("two")?.push(inner)?.push(tail)?;

        Ok(out.into_value())
    }

    /// Say which guest ABI linked, by calling an import that only this one has.
    ///
    /// Returns 2 for this ABI. Read-only: the bin it asks for need not exist.
    fn abi_probe() -> Result<Value, Error> {
        let _ = as_udf::bin("abi_probe_does_not_need_this_bin");

        Ok(Value::Int(2))
    }

    /// Return the integer 42, and nothing else.
    ///
    /// The floor of the whole system: one import call, no record access, no allocation. What it measures is
    /// per-call overhead - instantiate, call, tear down - which is where WASM was found to spend almost
    /// everything on a trivial UDF.
    fn answer() -> Result<Value, Error> {
        Ok(Value::Int(42))
    }

    /// Return nothing at all, which is nil.
    ///
    /// Subtracted from `answer` to price the result path on its own.
    fn nothing() -> Result<Value, Error> {
        Ok(Value::Nil)
    }

    /// Compute-bound: 200,000 multiply-adds.
    ///
    /// The other half of the comparison, and the half that decides whether an interpreter is acceptable. A
    /// trivial UDF measures setup; this measures execution, where a JIT or AoT-compiled module should pull
    /// far ahead of a bytecode interpreter - and where Lua's own interpreter is the thing to beat.
    fn work() -> Result<Value, Error> {
        // **Seeded from the record.** `sum(i*i)` over a literal range is a constant, and an optimiser is
        // entitled to fold the whole loop away - which would make this measure nothing at all. The seed is
        // only knowable at run time, so the arithmetic has to actually happen.
        let seed = as_udf::bin("n").and_then(|v| v.int()).unwrap_or(0);
        let mut sum: i64 = 0;

        for i in 1..=200_000i64 {
            sum = sum.wrapping_add(i.wrapping_mul(i ^ seed));
        }

        Ok(Value::Int(sum))
    }

    /// Grow linear memory until the host refuses, to check that it does.
    ///
    /// **Not an allocator test** - this ABI has no allocator, which is the point of it. A guest that wants
    /// more memory asks wasm for it directly, so that is what the `max-memory` ceiling has to stop.
    ///
    /// Bounded at 512 MiB rather than looping forever: if the cap did *not* work, the only other limit would
    /// be the transaction deadline, and the point is to find out rather than to find out the hard way. Each
    /// new region is touched, so the pages are real and not merely reserved.
    fn hog() -> Result<Value, Error> {
        // 16 pages = 1 MiB.
        const STEP_PAGES: usize = 16;
        const LIMIT_MIB: usize = 512;

        let mut grown_mib = 0;

        for _ in 0..LIMIT_MIB {
            let previous = core::arch::wasm32::memory_grow(0, STEP_PAGES);

            // usize::MAX is how wasm reports a refusal, which is what the cap should produce.
            if previous == usize::MAX {
                return Ok(Value::Int(grown_mib as i64));
            }

            // Touch the first byte of what was just added, so the page is committed rather than reserved.
            let at = (previous * 65536) as *mut u8;

            unsafe { at.write_volatile(1) };
            grown_mib += 1;
        }

        // Reached only if nothing stopped it, which is the failure this looks for. Reported as a failure
        // rather than a number, so a test cannot mistake it for success.
        Err(Error::Custom("memory grew past 512 MiB without being refused"))
    }

    /// Write one line at each level to the server log.
    ///
    /// `log` is the one import with no return value and no effect on the record, so a client-side assertion
    /// cannot check it; it needs a function of its own.
    fn chatter() -> Result<Value, Error> {
        as_udf::log_at(Level::Warn, "chatter at warning");
        as_udf::log_at(Level::Info, "chatter at info");
        as_udf::log_at(Level::Debug, "chatter at debug");

        Ok("logged".into())
    }

    /// Sum a list bin.
    ///
    /// **Nothing is deserialised.** The list iterates straight out of its stored bytes, 128 values per
    /// import, where the handle path of old had the host build one `as_val` per element before the first
    /// read. An element that is not an integer fails the call: the UDF decides what a stray value means,
    /// not the library.
    fn sum_list() -> Result<Value, Error> {
        let mut total = 0i64;

        for v in as_udf::bin("l")?.list()?.iter() {
            match v? {
                Value::Int(i) => total += i,
                other => {
                    let _ = other;
                    return Err(Error::Custom("bin `l` holds something that is not an integer"));
                }
            }
        }

        Ok(Value::Int(total))
    }

    /// What the readers see of three bins - `l` (integers), `d` (doubles) and `m` (a map) - as a map.
    ///
    /// Every entry is one of the accessors, so the test that reads this back checks the whole family
    /// against the values the client wrote. -1 (or "?") means an accessor declined.
    fn packed_probe() -> Result<Value, Error> {
        let out = as_udf::map(16)?;
        let l = as_udf::bin("l")?.list()?;
        let d = as_udf::bin("d")?.list()?;
        let m = as_udf::bin("m")?.map()?;

        out.put("len", l.len() as i64)?;
        out.put("sum", l.iter().map(|v| v.and_then(|v| v.int()).unwrap_or(0)).sum::<i64>())?;
        out.put("tail", l.iter().skip(3).count() as i64)?;
        out.put("dsum", d.iter().map(|v| v.and_then(|v| v.double()).unwrap_or(0.0)).sum::<f64>())?;
        out.put("mlen", m.len() as i64)?;
        out.put("x", m.get("x").and_then(|v| v.int()).unwrap_or(-1))?;
        out.put("f", m.get("f").and_then(|v| v.double()).unwrap_or(-1.0))?;

        let mut text = [0u8; 32];

        match m.get("s").and_then(|v| v.text()) {
            Ok(t) => out.put("s", t.as_str(&mut text).unwrap_or("?"))?,
            Err(_) => out.put("s", "?")?,
        };

        out.put("k7", m.get_i(7).and_then(|v| v.int()).unwrap_or(-1))?;
        // A nested list is a value like any other, with its length.
        out.put("nested", m.get("l").and_then(|v| v.list()).map_or(-1, |c| c.len() as i64))?;
        out.put("missing", matches!(m.get("nope"), Err(Error::Absent)))?;
        // The nested list, entered and summed: its length times 100 plus its sum.
        out.put(
            "found",
            match m.get("l").and_then(|v| v.list()) {
                Ok(inner) => inner.len() as i64 * 100 + sum_ints(inner),
                Err(_) => -1,
            },
        )?;
        out.put("bulk", m.get("l").and_then(|v| v.list()).map_or(-1, sum_ints))?;
        out.put("run", sum_ints(l))?;

        Ok(out.into_value())
    }

    /// Walk bin `doc` and flatten everything it meets into one list.
    ///
    /// Nested collections are bracketed with their length, so the test can see the descent and that the
    /// parent walk resumes after the child.
    fn cursor_probe() -> Result<Value, Error> {
        let out = as_udf::list(32)?;
        let doc = as_udf::bin("doc")?.list()?;

        out.push(doc.len() as i64)?;
        walk(doc, &out)?;

        Ok(out.into_value())
    }

    /// Two bins written through the packed writers: no value built per element.
    fn write_lists() -> Result<Value, Error> {
        as_udf::set_list_ints("w", &[5, -1, 1 << 40])?;
        as_udf::set_list_doubles("wd", &[0.5, -4.0])?;

        Ok(Value::Int(1))
    }

    /// Hand a nested value back exactly as stored: the map under `doc`'s last element.
    ///
    /// The guest never decodes it - the result is set from the record's bytes by position.
    fn last_of_doc() -> Result<Value, Error> {
        let doc = as_udf::bin("doc")?.list()?;
        let mut last = Value::Nil;

        for v in doc.iter() {
            last = v?;
        }

        Ok(last)
    }

    /// Build a list out of stored values, a guest GeoJson and a map with an integer key.
    ///
    /// `[doc[index], <geojson>, {1: "one", "first": doc[0]}]` - the stored values go into the builders by
    /// position, built by the server from the record's bytes; the GeoJson stays a GeoJson rather than
    /// becoming a string.
    fn relist(index: i64) -> Result<Value, Error> {
        let doc = as_udf::bin("doc")?.list()?;
        let picked = doc.iter().nth(index as usize).ok_or(Error::Absent)??;
        let first = doc.iter().next().ok_or(Error::Absent)??;
        let out = as_udf::list(3)?;
        let inner = as_udf::map(2)?;

        inner.put_i(1, "one")?.put("first", first)?;
        out.push(picked)?
            .push(Value::GeoJson("{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}".into()))?
            .push(inner)?;

        Ok(out.into_value())
    }

    /// Copy HLL bin `h` to bin `h2`, by position, and report its size.
    ///
    /// An HLL is readable as its bytes and writable back only from where it sits, so the subtype that makes
    /// it an HLL - the server's to set - survives the trip.
    fn copy_hll() -> Result<Value, Error> {
        let h = as_udf::bin("h")?;
        let bytes = h.hll()?;

        as_udf::set_value("h2", &h)?;
        as_udf::update()?;

        Ok(Value::Int(bytes.len() as i64))
    }

    /// Copy the list under key `vals` of map bin `m` into bin `copy`, from its stored bytes.
    fn copy_vals() -> Result<Value, Error> {
        let vals = as_udf::bin("m")?.map()?.get("vals")?;

        as_udf::set_value("copy", &vals)?;
        as_udf::update()?;

        Ok(Value::Int(vals.list()?.len() as i64))
    }

    /// Echo a string of any length back with a mark appended, as `[text!, length]`.
    ///
    /// A `String` parameter: allocated once at the argument's reported length, however long. A `&str`
    /// parameter would have refused anything past 256 bytes. The string is written into bin `echo` too,
    /// since a result borrows and a `String` is the function's own.
    fn echo(text: String) -> Result<Value, Error> {
        let mut marked = text;

        marked.push('!');
        as_udf::set("echo", marked.as_str())?;
        as_udf::update()?;

        let out = as_udf::list(2)?;

        out.push(marked.as_str())?.push(marked.len() as i64)?;

        Ok(out.into_value())
    }

    /// Write a short label into bin `tag`: a `Str<8>` parameter, so a longer argument is rejected before
    /// the body runs, with the limit in the message.
    fn tag(label: Str<8>) -> Result<Value, Error> {
        as_udf::set("tag", label.as_str())?;
        as_udf::update()?;

        Ok(Value::Int(label.len() as i64))
    }

    /// Build `[ints, labels, {label: i}]` with `count` of each, a window per import call rather than one
    /// import per element: `extend` over iterators of integers, borrowed strings and pairs.
    fn bulk(count: i64) -> Result<Value, Error> {
        let labels: Vec<String> = (0..count).map(|i| format!("v{i}")).collect();
        let ints = as_udf::list(count as usize)?;
        let strs = as_udf::list(count as usize)?;
        let by_label = as_udf::map(count as usize)?;

        ints.extend(0..count)?;
        strs.extend(labels.iter().map(String::as_str))?;
        by_label.extend(labels.iter().enumerate().map(|(i, l)| (l.as_str(), i as i64)))?;

        let out = as_udf::list(3)?;

        out.push(ints)?.push(strs)?.push(by_label)?;

        Ok(out.into_value())
    }

    /// Copy list bin `doc` and map bin `m` into new collections element by element, by position, a window
    /// at a time: `extend` straight over a stored collection's iterator. Returns `[copy of doc, copy of m]`.
    fn copy_list() -> Result<Value, Error> {
        let doc = as_udf::bin("doc")?.list()?;
        let m = as_udf::bin("m")?.map()?;
        let list_copy = as_udf::list(doc.len())?;
        let map_copy = as_udf::map(m.len())?;

        list_copy.extend(doc.iter())?;
        map_copy.extend(m.entries())?;

        let out = as_udf::list(2)?;

        out.push(list_copy)?.push(map_copy)?;

        Ok(out.into_value())
    }
}

/// The integers of a list, non-integers counted as 0.
fn sum_ints(list: Coll) -> i64 {
    list.iter().map(|v| v.and_then(|v| v.int()).unwrap_or(0)).sum()
}

/// Flatten what a collection yields into `out`, descending into nested collections.
fn walk(c: Coll, out: &List) -> Result<(), Error> {
    let mut buf = [0u8; 64];

    for item in c.iter() {
        match item? {
            Value::Nil => out.push(Value::Nil)?,
            Value::Bool(b) => out.push(b)?,
            Value::Int(i) => out.push(i)?,
            Value::Double(d) => out.push(d)?,
            Value::Str(t) => out.push(t.as_str(&mut buf)?)?,
            Value::Blob(b) | Value::Hll(b) => out.push(b.bytes(&mut buf)?)?,
            Value::GeoJson(t) => out.push(t.as_str(&mut buf)?)?,
            Value::List(inner) => {
                out.push("[")?.push(inner.len() as i64)?;
                walk(inner, out)?;
                out.push("]")?
            }
            Value::Map(inner) => {
                out.push("{")?.push(inner.len() as i64)?;
                walk(inner, out)?;
                out.push("}")?
            }
            Value::Other => out.push("?")?,
        };
    }

    Ok(())
}

/// Rust wants a panic handler for a `no_std` cdylib (and, with an allocator, an allocation failure panics).
///
/// A trap rather than a message: `panic = "abort"` in a wasm library becomes `unreachable`, which the host
/// reports as a trap with a backtrace - which is more than a guest could usefully say for itself.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

//==========================================================
// Stream (aggregation) UDFs.
//
// One instance per stream, so the accumulators are plain statics; `end` returns the node's partial.
//

/// The running total for `sum` and `stats`.
static mut TOTAL: i64 = 0;

/// How many records `stats` has seen.
static mut SEEN: i64 = 0;

/// The running total for `sumnested`.
static mut NESTED: i64 = 0;

as_udf::stream! {
    /// Sum bin `n` across every record the query matched.
    ///
    /// One import call per record, no serialisation, and the total accumulates in the guest's own memory.
    fn sum {
        record() -> Result<(), Error> {
            if let Ok(v) = as_udf::bin("n") {
                unsafe { TOTAL += v.int()? };
            }

            Ok(())
        }

        end() -> Result<Value, Error> {
            // One partial per node. A client with more than one node adds them up itself.
            Ok(Value::Int(unsafe { TOTAL }))
        }
    }

    /// Count the records the query matched, and report it with the sum, as a map.
    ///
    /// Here to show a non-scalar aggregation result, and that the collection builders work at `end` just as
    /// they do in a record UDF.
    fn stats {
        record() -> Result<(), Error> {
            unsafe {
                SEEN += 1;

                if let Ok(v) = as_udf::bin("n") {
                    TOTAL += v.int()?;
                }
            }

            Ok(())
        }

        end() -> Result<Value, Error> {
            let out = as_udf::map(2)?;

            unsafe {
                out.put("count", SEEN)?.put("sum", TOTAL)?;
            }

            Ok(out.into_value())
        }
    }

    /// Sum a list nested inside a map bin, across every record a query matched.
    ///
    /// The read path this ABI exists for, end to end: a map bin, a key inside it, a list under that key, and
    /// the elements of the list - **three import calls per record, nothing deserialised**. The Lua
    /// counterpart does the same work through the server's own object model.
    fn sumnested {
        record() -> Result<(), Error> {
            let Ok(m) = as_udf::bin("m") else {
                return Ok(());
            };

            let Ok(vals) = m.map()?.get("vals") else {
                return Ok(());
            };

            for v in vals.list()?.iter() {
                unsafe { NESTED += v?.int()? };
            }

            Ok(())
        }

        end() -> Result<Value, Error> {
            Ok(Value::Int(unsafe { NESTED }))
        }
    }
}
