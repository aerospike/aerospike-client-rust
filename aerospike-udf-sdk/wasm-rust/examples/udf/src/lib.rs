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

use aerospike_udf_sdk_wasm::{self as as_udf, Args, Coll, Error, Level, List, Map, Record, Resolution, Str, Value};

// The host calls themselves, for the three functions below that do what the SDK's own types do not allow:
// keep a value across a write to its bin, and write from a stream. A module is not obliged to use the
// SDK, so the server has to be right about both without it.
#[link(wasm_import_module = "aerospike")]
extern "C" {
    #[link_name = "bin_value"]
    fn raw_bin_value(name: *const u8, name_len: u32, out: *mut u8, out_cap: u32) -> i32;
    #[link_name = "cursor_bytes_at"]
    fn raw_cursor_bytes_at(source: i32, at: u32, out: *mut u8, out_cap: u32) -> i32;
    #[link_name = "set_str"]
    fn raw_set_str(name: *const u8, name_len: u32, value: *const u8, value_len: u32) -> i32;
    #[link_name = "set_list_ints"]
    fn raw_set_list_ints(name: *const u8, name_len: u32, values: *const u8, values_sz: u32) -> i32;
    #[link_name = "set_geojson"]
    fn raw_set_geojson(name: *const u8, name_len: u32, value: *const u8, value_len: u32) -> i32;
    #[link_name = "rec_update"]
    fn raw_rec_update() -> i32;
}

as_udf::export! {
    /// Read a bin, write it back incremented, and return the new value.
    ///
    /// The read-modify-write a real UDF does, and the cheapest this ABI gets: two import calls, no buffer
    /// and no allocation on either side. A record without the bin starts from zero.
    fn increment(rec: &mut Record) -> Result<Value, Error> {
        let n = match rec.bin("n") {
            Ok(value) => value.int()?,
            Err(Error::Absent) => 0,
            Err(e) => return Err(e),
        };

        rec.set("n", n + 1)?;

        // **Staged until saved** - the one mistake that looks like it worked. `set` only writes into the
        // record's own cache; without this the UDF returns a perfectly plausible new value and changes
        // nothing on disk. `save` creates the record when there is none yet, so a counter starting from
        // nothing needs no more than this.
        rec.save()?;

        Ok(Value::Int(n + 1))
    }

    /// Sum the integer arguments into the bin the first argument names.
    ///
    /// The macro has already checked that `name` is a string by the time this runs; `numbers` is whatever
    /// followed it, and each is checked as it is used.
    fn add_all(rec: &mut Record, name: &str, numbers: Args) -> Result<Value, Error> {
        let mut total = match rec.bin(name) {
            Ok(value) => value.int()?,
            Err(Error::Absent) => 0,
            Err(e) => return Err(e),
        };

        for n in numbers.iter() {
            total += n.int()?;
        }

        rec.set(name, total)?;
        rec.save()?;

        Ok(Value::Int(total))
    }

    /// Read a GeoJson bin and write it straight back, to prove the particle type survives.
    ///
    /// **The point is what it does *not* do.** The document is written back as the geojson value it was
    /// read as - `set` sees [`Value::GeoJson`] and keeps the particle type - where writing its text back as a
    /// string would keep the bytes and quietly turn a geospatially indexable bin into an ordinary string.
    /// Reports the document's length so a caller can tell the round trip actually happened.
    fn geo_roundtrip(rec: &mut Record, name: &str) -> Result<Value, Error> {
        let doc = rec.bin(name)?;

        let Value::GeoJson(text) = doc else {
            return Err(Error::Custom("that bin does not hold geojson"));
        };

        // Written back from its stored bytes: the guest never decodes the document.
        rec.set_value(name, &doc)?;
        rec.save()?;

        Ok(Value::Int(text.len() as i64))
    }

    /// Everything the record knows about itself, as a map.
    fn describe(rec: &Record) -> Result<Map, Error> {
        let out = as_udf::map(6)?;

        out.insert("generation", rec.generation())?
            .insert("bins", rec.bin_count())?
            .insert("size", rec.size())?;

        let mut buf = [0u8; 64];

        match rec.set_name(&mut buf) {
            Ok(set) => out.insert("set", set)?,
            Err(_) => out.insert("set", Value::Nil)?,
        };

        // The key is only there when the record was written with send_key.
        let mut key_buf = [0u8; 256];

        match rec.key(&mut key_buf) {
            Ok(key) => out.insert("key", key)?,
            Err(_) => out.insert("key", Value::Nil)?,
        };

        match rec.ttl() {
            Some(ttl) => out.insert("ttl", i64::from(ttl))?,
            None => out.insert("ttl", Value::Nil)?,
        };

        Ok(out)
    }

    /// A nested value, to prove collections survive the trip in both directions.
    ///
    /// The same shape the other SDKs return: `[1, "two", [true, nil], {"three": 3.5}]`.
    fn nested() -> Result<List, Error> {
        let inner = as_udf::list(2)?;

        inner.push(true)?.push(Value::Nil)?;

        let tail = as_udf::map(1)?;

        tail.insert("three", 3.5)?;

        let out = as_udf::list(4)?;

        out.push(1)?.push("two")?.push(&inner)?.push(&tail)?;

        Ok(out)
    }

    /// Say which guest ABI linked, by calling an import that only this one has.
    ///
    /// Returns 2 for this ABI. Read-only: the bin it asks for need not exist.
    fn abi_probe(rec: &Record) -> Result<Value, Error> {
        let _ = rec.bin("abi_probe_does_not_need_this_bin");

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

    /// Save with nothing staged, and report what that did: whether the record exists afterwards.
    ///
    /// Exists so that a test can pin down what `save` means when there is nothing to save, on a record
    /// that is there and on one that is not.
    fn save_nothing(rec: &mut Record) -> Result<Value, Error> {
        rec.save()?;

        Ok(Value::Bool(rec.exists()))
    }

    /// Delete the record, staged writes and all. Answers what `exists` says straight afterwards.
    fn delete_it(rec: &mut Record) -> Result<Value, Error> {
        rec.set("n", 99)?;
        rec.delete()?;

        Ok(Value::Bool(rec.exists()))
    }

    /// Remove a bin by name and save.
    fn drop_bin(rec: &mut Record, name: &str) -> Result<Value, Error> {
        rec.remove(name)?;
        rec.save()?;

        Ok(Value::Int(rec.bin_count()))
    }

    /// Set a bin to nil and save, which removes it: a bin cannot hold nil.
    fn nil_bin(rec: &mut Record, name: &str) -> Result<Value, Error> {
        rec.set(name, Value::Nil)?;
        rec.save()?;

        Ok(Value::Int(rec.bin_count()))
    }

    /// One element of a list bin by its index, and whether a map the call builds knows its keys.
    fn index_probe(rec: &Record, name: &str, index: i64) -> Result<Map, Error> {
        let out = as_udf::map(4)?;
        let list = rec.bin(name)?.list()?;

        out.insert("len", list.len() as i64)?;

        // An index too large for this target's `usize` is past the end of any list it could hold, and a
        // negative one is before the start of every list: nothing is there either way.
        let at = match usize::try_from(index) {
            Ok(i) => list.at(i)?,
            Err(_) => None,
        };

        match at {
            Some(v) => out.insert("at", v)?,
            None => out.insert("at", "absent")?,
        };

        out.insert(7, "seven")?;

        let knows = out.contains_key("len") && out.contains_key(7);
        let invents = out.contains_key("nope") || out.contains_key(8);

        out.insert("knows", knows)?.insert("invents", invents)?;

        Ok(out)
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
    fn work(rec: &Record) -> Result<Value, Error> {
        // **Seeded from the record.** `sum(i*i)` over a literal range is a constant, and an optimiser is
        // entitled to fold the whole loop away - which would make this measure nothing at all. The seed is
        // only knowable at run time, so the arithmetic has to actually happen.
        let seed = rec.bin("n").and_then(|v| v.int()).unwrap_or(0);
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
        as_udf::log(Level::Warn, "chatter at warning");
        as_udf::log(Level::Info, "chatter at info");
        as_udf::log(Level::Debug, "chatter at debug");

        Ok("logged".into())
    }

    /// Sum a list bin.
    ///
    /// **Nothing is deserialised.** The list iterates straight out of its stored bytes, 128 values per
    /// import, where the handle path of old had the host build one `as_val` per element before the first
    /// read. An element that is not an integer fails the call: the UDF decides what a stray value means,
    /// not the library.
    fn sum_list(rec: &Record) -> Result<Value, Error> {
        let mut total = 0i64;

        for v in rec.bin("l")?.list()?.iter() {
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
    fn packed_probe(rec: &Record) -> Result<Map, Error> {
        let out = as_udf::map(16)?;
        let l = rec.bin("l")?.list()?;
        let d = rec.bin("d")?.list()?;
        let m = rec.bin("m")?.map()?;

        out.insert("len", l.len() as i64)?;
        out.insert("sum", l.iter().map(|v| v.and_then(|v| v.int()).unwrap_or(0)).sum::<i64>())?;
        out.insert("tail", l.iter().skip(3).count() as i64)?;
        out.insert("dsum", d.iter().map(|v| v.and_then(|v| v.double()).unwrap_or(0.0)).sum::<f64>())?;
        out.insert("mlen", m.len() as i64)?;
        // A key that must be there: the value, or `Absent` like a bin that must be.
        let under = |key: &str| m.get(key)?.ok_or(Error::Absent);

        out.insert("x", under("x").and_then(|v| v.int()).unwrap_or(-1))?;
        out.insert("f", under("f").and_then(|v| v.double()).unwrap_or(-1.0))?;

        let mut text = [0u8; 32];

        match under("s").and_then(|v| v.string()) {
            Ok(t) => out.insert("s", t.as_str(&mut text).unwrap_or("?"))?,
            Err(_) => out.insert("s", "?")?,
        };

        out.insert("k7", m.get(7).and_then(|v| v.ok_or(Error::Absent)?.int()).unwrap_or(-1))?;
        // A nested list is a value like any other, with its length.
        out.insert("nested", under("l").and_then(|v| v.list()).map_or(-1, |c| c.len() as i64))?;
        // A key the map does not have is nothing, not an error.
        out.insert("missing", matches!(m.get("nope"), Ok(None)))?;
        // The nested list, entered and summed: its length times 100 plus its sum.
        out.insert(
            "found",
            match under("l").and_then(|v| v.list()) {
                Ok(inner) => inner.len() as i64 * 100 + sum_ints(inner),
                Err(_) => -1,
            },
        )?;
        out.insert("bulk", under("l").and_then(|v| v.list()).map_or(-1, sum_ints))?;
        out.insert("run", sum_ints(l))?;

        Ok(out)
    }

    /// Walk bin `doc` and flatten everything it meets into one list.
    ///
    /// Nested collections are bracketed with their length, so the test can see the descent and that the
    /// parent walk resumes after the child.
    fn cursor_probe(rec: &Record) -> Result<List, Error> {
        let out = as_udf::list(32)?;
        let doc = rec.bin("doc")?.list()?;

        out.push(doc.len() as i64)?;
        walk(doc, &out)?;

        Ok(out)
    }

    /// What a read gives after a write the call has not saved: the staged value, never the stored one.
    ///
    /// `sl` is stored as a list of three and `sm` as a map. The answer is how long `sl` reads once a list
    /// of one is staged over it, and whether `sm` is gone once it is removed.
    fn staged_read(rec: &mut Record) -> Result<List, Error> {
        let one = as_udf::list(1)?;

        one.push(9i64)?;
        rec.set("sl", &one)?;

        let len = rec.list("sl")?.map_or(-1, |l| l.len() as i64);

        rec.remove("sm")?;

        let gone = i64::from(rec.map("sm")?.is_none());
        let out = as_udf::list(2)?;

        out.push(len)?;
        out.push(gone)?;

        Ok(out)
    }

    /// Stage text that is not a GeoJSON document as a GeoJSON bin, save, and answer what each step said.
    ///
    /// Staging takes any text; it is the save that builds the bin, and the server does not build a
    /// geospatial bin from a document it could not have been sent as a write.
    fn bad_geo(rec: &mut Record) -> Result<List, Error> {
        let _ = rec;
        let text = b"not a document";
        let out = as_udf::list(2)?;

        unsafe {
            out.push(i64::from(raw_set_geojson(b"geo".as_ptr(), 3, text.as_ptr(), text.len() as u32)))?;
            out.push(i64::from(raw_rec_update()))?;
        }

        Ok(out)
    }

    /// A string read before its bin is written again, read once more afterwards: whether it is still the
    /// string it was.
    ///
    /// Through the host calls directly, because the SDK's borrows do not allow the order. The bin is
    /// written many times over, so that whatever let go of the first value has had every chance to see
    /// its memory used again.
    fn stale_read(rec: &mut Record) -> Result<Value, Error> {
        let _ = rec;
        let mut value = [0u8; 16];
        let mut before = [0u8; 96];
        let mut after = [0u8; 96];

        unsafe {
            if raw_bin_value(b"s".as_ptr(), 1, value.as_mut_ptr(), 16) != 0 {
                return Err(Error::Custom("no string bin `s`"));
            }

            let mut payload = [0u8; 8];

            payload.copy_from_slice(&value[8..16]);

            let payload = i64::from_le_bytes(payload);
            let (source, at) = ((payload >> 32) as i32, payload as u32);
            let n = raw_cursor_bytes_at(source, at, before.as_mut_ptr(), 96);

            for i in 0..64u8 {
                let text = [b'A' + (i % 26); 80];

                raw_set_str(b"s".as_ptr(), 1, text.as_ptr(), 80);
            }

            let m = raw_cursor_bytes_at(source, at, after.as_mut_ptr(), 96);

            Ok(Value::Bool(n > 0 && n == m && before == after))
        }
    }

    /// Two bins written through the packed writers: no value built per element.
    fn write_lists(rec: &mut Record) -> Result<Value, Error> {
        rec.set_ints("w", &[5, -1, 1 << 40])?;
        rec.set_doubles("wd", &[0.5, -4.0])?;

        Ok(Value::Int(1))
    }

    /// Hand a nested value back exactly as stored: the map under `doc`'s last element.
    ///
    /// The guest never decodes it - the result is set from the record's bytes by position.
    fn last_of_doc(rec: &Record) -> Result<Value, Error> {
        let doc = rec.bin("doc")?.list()?;
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
    fn relist(rec: &Record, index: i64) -> Result<List, Error> {
        let doc = rec.bin("doc")?.list()?;
        let picked = doc.iter().nth(index as usize).ok_or(Error::Absent)??;
        let first = doc.iter().next().ok_or(Error::Absent)??;
        let out = as_udf::list(3)?;
        let inner = as_udf::map(2)?;

        inner.insert(1, "one")?.insert("first", first)?;
        out.push(picked)?
            .push(Value::GeoJson("{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}".into()))?
            .push(&inner)?;

        Ok(out)
    }

    /// Copy HLL bin `h` to bin `h2`, by position, and report its size.
    ///
    /// An HLL is readable as its bytes and writable back only from where it sits, so the subtype that makes
    /// it an HLL - the server's to set - survives the trip.
    fn copy_hll(rec: &mut Record) -> Result<Value, Error> {
        let h = rec.bin("h")?;
        let bytes = h.hll()?;

        rec.set_value("h2", &h)?;
        rec.save()?;

        Ok(Value::Int(bytes.len() as i64))
    }

    /// Copy the list under key `vals` of map bin `m` into bin `copy`, from its stored bytes.
    fn copy_vals(rec: &mut Record) -> Result<Value, Error> {
        let vals = rec.bin("m")?.map()?.get("vals")?.ok_or(Error::Absent)?;

        rec.set_value("copy", &vals)?;
        rec.save()?;

        Ok(Value::Int(vals.list()?.len() as i64))
    }

    /// Echo a string of any length back with a mark appended, as `[text!, length]`.
    ///
    /// A `String` parameter: allocated once at the argument's reported length, however long. A `&str`
    /// parameter would have refused anything past 256 bytes. The string is written into bin `echo` too.
    /// (A function whose whole answer is a string it built returns it owned, `-> Result<String, Error>`;
    /// this one answers a list, which copies the text when it is pushed.)
    fn echo(rec: &mut Record, text: String) -> Result<List, Error> {
        let mut marked = text;

        marked.push('!');
        rec.set("echo", marked.as_str())?;
        rec.save()?;

        let out = as_udf::list(2)?;

        out.push(marked.as_str())?.push(marked.len() as i64)?;

        Ok(out)
    }

    /// Write a short label into bin `tag`: a `Str<8>` parameter, so a longer argument is rejected before
    /// the body runs, with the limit in the message.
    fn tag(rec: &mut Record, label: Str<8>) -> Result<Value, Error> {
        rec.set("tag", label.as_str())?;
        rec.save()?;

        Ok(Value::Int(label.len() as i64))
    }

    /// Build `[ints, labels, {label: i}]` with `count` of each, a window per import call rather than one
    /// import per element: `extend` over iterators of integers, borrowed strings and pairs.
    fn bulk(count: i64) -> Result<List, Error> {
        let labels: Vec<String> = (0..count).map(|i| format!("v{i}")).collect();
        let ints = as_udf::list(count as usize)?;
        let strs = as_udf::list(count as usize)?;
        let by_label = as_udf::map(count as usize)?;

        ints.extend(0..count)?;
        strs.extend(labels.iter().map(String::as_str))?;
        by_label.extend(labels.iter().enumerate().map(|(i, l)| (l.as_str(), i as i64)))?;

        let out = as_udf::list(3)?;

        out.push(&ints)?.push(&strs)?.push(&by_label)?;

        Ok(out)
    }

    /// Copy list bin `doc` and map bin `m` into new collections element by element, by position, a window
    /// at a time: `extend` straight over a stored collection's iterator. Returns `[copy of doc, copy of m]`.
    fn copy_list(rec: &Record) -> Result<List, Error> {
        let doc = rec.bin("doc")?.list()?;
        let m = rec.bin("m")?.map()?;
        let list_copy = as_udf::list(doc.len())?;
        let map_copy = as_udf::map(m.len())?;

        list_copy.extend(doc.iter())?;
        map_copy.extend(m.entries())?;

        let out = as_udf::list(2)?;

        out.push(&list_copy)?.push(&map_copy)?;

        Ok(out)
    }

    /// Build `count` maps, each holding a list, nest each in one outer list by reference, then walk every
    /// nested list with an iterator of its own. Answers `[elements, sum]`.
    ///
    /// Every map, list and iterator gives its handle back when it is dropped, so the call holds a few at a
    /// time however large `count` is: what bounds it is the call's memory, not a number of handles.
    fn churn(count: i64) -> Result<List, Error> {
        let out = as_udf::list(0)?;

        for i in 0..count {
            let inner = as_udf::list(2)?;
            let entry = as_udf::map(1)?;

            inner.push(i)?.push(1)?;
            entry.insert("l", &inner)?;
            out.push(&entry)?;
        }

        let mut sum = 0;

        for item in out.as_value().list()?.iter() {
            let Some(l) = item?.map()?.get("l")? else {
                return Err(Error::Absent);
            };

            for v in l.list()?.iter() {
                sum += v?.int()?;
            }
        }

        let answer = as_udf::list(2)?;

        answer.push(out.as_value().list()?.len() as i64)?.push(sum)?;

        Ok(answer)
    }

    /// Build a list of `maps` small maps, `{"i": i}`, and walk every nested list of the bin `nested`, each
    /// with an iterator of its own, in one call. Answers `[maps built, lists walked, sum of their integers]`.
    ///
    /// Thousands of handles in one call: the host's table grows past its inline slots as it needs to, and
    /// what the SDK gives back on drop is reused.
    fn many_handles(rec: &mut Record, maps: i64) -> Result<List, Error> {
        let out = as_udf::list(0)?;

        for i in 0..maps {
            let entry = as_udf::map(1)?;

            entry.insert("i", i)?;
            out.push(&entry)?;
        }

        let mut lists = 0i64;
        let mut sum = 0i64;

        for item in rec.bin("nested")?.list()?.iter() {
            for v in item?.list()?.iter() {
                sum += v?.int()?;
            }

            lists += 1;
        }

        let answer = as_udf::list(3)?;

        answer.push(out.as_value().list()?.len() as i64)?.push(lists)?.push(sum)?;

        Ok(answer)
    }

    /// Build `levels` maps, each the only entry of the one above it, from the top down: the root first,
    /// then each level put into the one made just before it, which is already inside the rest of the
    /// chain. Saves the chain as the bin `deep`, reads it back level by level, and answers
    /// `[levels built, levels read]`.
    ///
    /// The host lets a call nest 64 levels, in whatever order it builds them; the put that would make the
    /// 65th is refused, and the call fails with nothing saved.
    fn deep_maps(rec: &mut Record, levels: i64) -> Result<List, Error> {
        let root = as_udf::map(1)?;
        let mut at = as_udf::map(1)?;

        root.insert("d", &at)?;

        for _ in 2..levels {
            let next = as_udf::map(1)?;

            at.insert("d", &next)?;
            at = next;
        }

        at.insert("leaf", 1i64)?;
        rec.set("deep", &root)?;
        rec.save()?;

        let mut read = 0i64;
        let mut level = Some(rec.bin("deep")?);

        while let Some(value) = level {
            read += 1;
            level = value.map()?.get("d")?;
        }

        let answer = as_udf::list(2)?;

        answer.push(levels)?.push(read)?;

        Ok(answer)
    }

    /// Stage a bin, save, then fail the call. A test reads the record afterwards to see whether a save
    /// that succeeded is undone by a failure that follows it.
    fn save_then_fail(rec: &mut Record) -> Result<Value, Error> {
        rec.set("kept", 1)?;
        rec.save()?;

        Err(Error::Custom("failed after the save"))
    }

    /// The same with a list written by `set_ints`, which is written at once and not staged.
    fn ints_then_fail(rec: &mut Record) -> Result<Value, Error> {
        rec.set_ints("ints", &[1, 2, 3])?;

        Err(Error::Custom("failed after set_ints"))
    }

    /// `bin_count` before a write is staged, once it is staged, and once it is saved.
    fn count_staged(rec: &mut Record) -> Result<List, Error> {
        let out = as_udf::list(3)?;

        out.push(rec.bin_count())?;
        rec.set("staged", 1)?;
        out.push(rec.bin_count())?;
        rec.save()?;
        out.push(rec.bin_count())?;

        Ok(out)
    }

    /// What every metadata call answers. Exists to pin down what they say on a key with no record.
    fn meta_probe(rec: &Record) -> Result<Map, Error> {
        let out = as_udf::map(8)?;

        out.insert("exists", Value::Bool(rec.exists()))?;
        out.insert("generation", rec.generation())?;
        out.insert("ttl", rec.ttl().map_or(-1, i64::from))?;
        out.insert("lut", rec.last_update_time())?;
        out.insert("size", rec.size())?;
        out.insert("bins", rec.bin_count())?;
        out.insert("has", Value::Bool(rec.has("a")))?;

        Ok(out)
    }

    /// One write verb chosen by name, with what it answered and whether the record exists afterwards
    /// as the call sees it. Exists to pin down the verbs on a key with no record.
    fn verb_probe(rec: &mut Record, verb: &str) -> Result<List, Error> {
        let answer = match verb {
            "set_ints" => rec.set_ints("v", &[1, 2]),
            "set_doubles" => rec.set_doubles("v", &[1.5]),
            "delete" => rec.delete(),
            "remove" => rec.remove("nope").and_then(|()| rec.save()),
            "ttl_never" => rec.set_ttl(u32::MAX).and_then(|()| rec.save()),
            "ttl_zero" => rec.set_ttl(0).and_then(|()| rec.save()),
            _ => Err(Error::Custom("no such verb")),
        };
        let out = as_udf::list(2)?;

        out.push(match answer {
            Ok(()) => "ok",
            Err(Error::Refused) => "refused",
            Err(Error::Malformed) => "malformed",
            Err(Error::Absent) => "absent",
            Err(_) => "other",
        })?;
        out.push(Value::Bool(rec.exists()))?;

        Ok(out)
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
            Value::String(t) => out.push(t.as_str(&mut buf)?)?,
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

/// What `poke` was answered: refusals, and anything else.
static mut REFUSED: i64 = 0;
static mut WRITTEN: i64 = 0;

as_udf::event! {
    /// Answers the metadata a merge is handed, as a record, and writes a log line. Exists so a test can
    /// read the numbers: what the per-bin times are counted in, and what the record-level ones hold.
    on xdr_merge
    fn meta_merge(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let out = as_udf::map(10)?;

        out.insert("c_blut", current.bin_last_update_time("a"))?;
        out.insert("c_bsrc", current.bin_source_id("a"))?;
        out.insert("c_lut", current.last_update_time())?;
        out.insert("c_gen", current.generation())?;
        out.insert("i_blut", incoming.bin_last_update_time("a"))?;
        out.insert("i_bsrc", incoming.bin_source_id("a"))?;
        out.insert("i_lut", incoming.last_update_time())?;
        out.insert("i_gen", incoming.generation())?;
        out.insert("now", as_udf::now().unwrap_or(-1))?;
        as_udf::log(Level::Info, "meta_merge ran");

        Ok(Resolution::Record { bins: out })
    }
}

as_udf::stream! {
    /// Sum bin `n` across every record the query matched.
    ///
    /// One import call per record, no serialisation, and the total accumulates in the guest's own memory.
    fn sum {
        record(rec: &Record) -> Result<(), Error> {
            if let Ok(v) = rec.bin("n") {
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
        record(rec: &Record) -> Result<(), Error> {
            unsafe {
                SEEN += 1;

                if let Ok(v) = rec.bin("n") {
                    TOTAL += v.int()?;
                }
            }

            Ok(())
        }

        end() -> Result<Map, Error> {
            let out = as_udf::map(2)?;

            unsafe {
                out.insert("count", SEEN)?.insert("sum", TOTAL)?;
            }

            Ok(out)
        }
    }

    /// Try to write to every record a query matched, and count what the server said.
    ///
    /// A stream's record is the query's, and read-only. This goes by the one host call that does not use
    /// the record's own write hook, which is the one that has to refuse of its own accord.
    fn poke {
        record(rec: &Record) -> Result<(), Error> {
            let _ = rec;
            let values = [1i64, 2, 3];
            let rc = unsafe { raw_set_list_ints(b"poked".as_ptr(), 5, values.as_ptr().cast(), 24) };

            unsafe {
                if rc == -4 {
                    REFUSED += 1;
                } else {
                    WRITTEN += 1;
                }
            }

            Ok(())
        }

        end() -> Result<Map, Error> {
            let out = as_udf::map(2)?;

            unsafe {
                out.insert("refused", REFUSED)?.insert("written", WRITTEN)?;
            }

            Ok(out)
        }
    }

    /// Sum a list nested inside a map bin, across every record a query matched.
    ///
    /// The read path this ABI exists for, end to end: a map bin, a key inside it, a list under that key, and
    /// the elements of the list - **three import calls per record, nothing deserialised**. The Lua
    /// counterpart does the same work through the server's own object model.
    fn sumnested {
        record(rec: &Record) -> Result<(), Error> {
            let Ok(m) = rec.bin("m") else {
                return Ok(());
            };

            let Ok(Some(vals)) = m.map()?.get("vals") else {
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
