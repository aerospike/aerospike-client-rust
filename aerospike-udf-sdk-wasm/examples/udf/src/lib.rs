//! WebAssembly UDFs for the WAMR engine.
//!
//! Deliberately the **same functions** as `modules/mod-wasm/examples/sdk` and the Lua script beside it, so
//! the three runtimes can be compared on identical work rather than on whatever each SDK made convenient.
//!
//! What differs is how they get their data. There is no msgpack here and no allocator: reading a bin is one
//! import call returning a machine integer, and a returned collection is built directly in the server's own
//! representation.

#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, Args, Level};

as_udf::export! {
    /// Read a bin, write it back incremented, and return the new value.
    ///
    /// The read-modify-write a real UDF does, and the cheapest this ABI gets: two import calls, no buffer
    /// and no allocation on either side.
    fn increment(_args: Args) -> i32 {
        let next = as_udf::bin_int("n") + 1;

        if !as_udf::set_int("n", next) {
            as_udf::fail("the server refused the write");
            return 1;
        }

        // **Staged until committed** - the one mistake that looks like it worked. `set_int` only writes into
        // the record's own cache; without this the UDF returns a perfectly plausible new value and changes
        // nothing on disk. This example omitted it, and the client test that reads the bin back after two
        // calls is exactly what caught it: the second call returned 11 again instead of 12.
        //
        // `create` when there is no record yet, `update` when there is: the server refuses an update to a
        // record that does not exist, so a counter starting from nothing needs both.
        let committed = if as_udf::exists() {
            as_udf::update()
        } else {
            as_udf::create()
        };

        if !committed {
            as_udf::fail("the server refused to commit");
            return 1;
        }

        as_udf::result_int(next);

        0
    }

    /// Sum the integer arguments into the bin the first argument names.
    ///
    /// Arguments arrive as a handle to the server's own `as_list` - **nothing was serialised to get them
    /// here**, which is the clearest single illustration of what this ABI buys.
    fn add_all(args: Args) -> i32 {
        let mut name_buf = [0u8; 16];

        let Ok(name) = args.str_at(0, &mut name_buf) else {
            as_udf::fail("add_all wants a bin name as its first argument");
            return 1;
        };

        // Read before the loop: `name` borrows `name_buf`, and the running total must not.
        let mut total = as_udf::bin_int(name);

        for i in 1..args.len() {
            total += args.int_at(i);
        }

        if !as_udf::set_int(name, total) || !as_udf::update() {
            as_udf::fail("the write was refused");
            return 1;
        }

        as_udf::result_int(total);

        0
    }

    /// Read a GeoJSON bin and write it straight back, to prove the particle type survives.
    ///
    /// **The point is what it does *not* do.** Before `set_geojson` existed, a geojson bin read as `Type::Other` and
    /// the only way to put text back was `set_str` - which keeps the document and quietly turns a geospatially
    /// indexable bin into an ordinary string. The bytes looked right and geo queries stopped matching.
    ///
    /// Takes the bin name as its first argument, and reports the document's length so a caller can tell the round trip
    /// actually happened.
    fn geo_roundtrip(args: Args) -> i32 {
        let mut name_buf = [0u8; 16];

        let Ok(name) = args.str_at(0, &mut name_buf) else {
            as_udf::fail("the first argument must be a bin name");
            return 1;
        };

        // Copied to a local so the bin name is not borrowed from the same buffer the document goes into.
        let mut owned_name = [0u8; 16];
        let n = name.len();

        owned_name[..n].copy_from_slice(name.as_bytes());

        let Ok(bin) = core::str::from_utf8(&owned_name[..n]) else {
            as_udf::fail("bin name is not utf-8");
            return 1;
        };

        if as_udf::bin(bin) != as_udf::Type::GeoJson {
            as_udf::fail("that bin does not hold geojson");
            return 1;
        }

        let mut doc = [0u8; 1024];

        let Ok(text) = as_udf::bin_str(bin, &mut doc) else {
            as_udf::fail("could not read the document");
            return 1;
        };

        let len = text.len() as i64;

        // `set_geojson`, not `set_str` - that difference is the whole reason this function exists.
        if !as_udf::set_geojson(bin, text) {
            as_udf::fail("the geojson write was refused");
            return 1;
        }

        if !as_udf::update() {
            as_udf::fail("the server refused to commit");
            return 1;
        }

        as_udf::result_int(len);

        0
    }

    /// Everything the record knows about itself, as a map.
    fn describe(_args: Args) -> i32 {
        let Some(out) = as_udf::map(6) else {
            as_udf::fail("could not build the result map");
            return 1;
        };

        out.put_int("generation", as_udf::generation());
        out.put_int("bins", as_udf::bin_count());
        out.put_int("size", as_udf::size());

        let mut buf = [0u8; 64];

        match as_udf::set_name(&mut buf) {
            Ok(set) => out.put_str("set", set),
            Err(_) => out.put_nil("set"),
        };

        // The key is only there when the record was written with send_key.
        let mut key_buf = [0u8; 256];

        match as_udf::key() {
            as_udf::Type::Int => out.put_int("key", as_udf::key_as_int()),
            as_udf::Type::Str => match as_udf::key_as_bytes(&mut key_buf) {
                Ok(bytes) => match core::str::from_utf8(bytes) {
                    Ok(text) => out.put_str("key", text),
                    Err(_) => out.put_nil("key"),
                },
                Err(_) => out.put_nil("key"),
            },
            _ => out.put_nil("key"),
        };

        match as_udf::time_to_live() {
            Some(ttl) => out.put_int("ttl", i64::from(ttl)),
            None => out.put_nil("ttl"),
        };

        as_udf::result_coll(&out.build());

        0
    }

    /// A nested value, to prove collections survive the trip in both directions.
    ///
    /// The same shape the other two SDKs return: `[1, "two", [true, nil], {"three": 3.5}]`.
    fn nested(_args: Args) -> i32 {
        let (Some(out), Some(inner), Some(tail)) =
            (as_udf::list(4), as_udf::list(2), as_udf::map(1)) else {
            as_udf::fail("could not build the result");
            return 1;
        };

        inner.push_bool(true).push_nil();
        tail.put_double("three", 3.5);

        out.push_int(1)
            .push_str("two")
            .push_list(&inner)
            .push_map(&tail);

        as_udf::result_coll(&out.build());

        0
    }

    /// Say which guest ABI linked, by calling an import that only this one has.
    ///
    /// See the counterpart in mod-wasm's example for why this exists rather than reusing `answer`: the two
    /// ABIs share `result_int`, so `answer` links and runs on either engine and proves nothing.
    ///
    /// Returns 2 for this ABI. Read-only.
    fn abi_probe(_args: Args) -> i32 {
        let _ = as_udf::bin_int("abi_probe_does_not_need_this_bin");

        as_udf::result_int(2);

        0
    }

    /// Return the integer 42, and nothing else.
    ///
    /// The floor of the whole system: one import call, no record access, no allocation. What it measures is
    /// per-call overhead - instantiate, call, tear down - which is where WASM was found to spend almost
    /// everything on a trivial UDF.
    fn answer(_args: Args) -> i32 {
        as_udf::result_int(42);

        0
    }

    /// Return nothing at all, which is nil.
    ///
    /// Subtracted from `answer` to price the result path on its own.
    fn nothing(_args: Args) -> i32 {
        0
    }

    /// Compute-bound: 200,000 multiply-adds.
    ///
    /// The other half of the comparison, and the half that decides whether an interpreter is acceptable. A
    /// trivial UDF measures setup; this measures execution, where a JIT or AoT-compiled module should pull
    /// far ahead of a bytecode interpreter - and where Lua's own interpreter is the thing to beat.
    fn work(_args: Args) -> i32 {
        // **Seeded from the record.** `sum(i*i)` over a literal range is a constant, and an optimiser is
        // entitled to fold the whole loop away - which would make this measure nothing at all. The seed is
        // only knowable at run time, so the arithmetic has to actually happen.
        let seed = as_udf::bin_int("n");
        let mut sum: i64 = 0;

        for i in 1..=200_000i64 {
            sum = sum.wrapping_add(i.wrapping_mul(i ^ seed));
        }

        as_udf::result_int(sum);

        0
    }

    /// Grow linear memory until the host refuses, to check that it does.
    ///
    /// **Not an allocator test** — this ABI has no allocator, which is the point of it. A guest that wants
    /// more memory asks wasm for it directly, so that is what the `max-memory` ceiling has to stop.
    ///
    /// Bounded at 512 MiB rather than looping forever: if the cap did *not* work, the only other limit would
    /// be the transaction deadline, and the point is to find out rather than to find out the hard way. Each
    /// new region is touched, so the pages are real and not merely reserved.
    fn hog(_args: Args) -> i32 {
        // 16 pages = 1 MiB.
        const STEP_PAGES: usize = 16;
        const LIMIT_MIB: usize = 512;

        let mut grown_mib = 0;

        for _ in 0..LIMIT_MIB {
            let previous = core::arch::wasm32::memory_grow(0, STEP_PAGES);

            // usize::MAX is how wasm reports a refusal, which is what the cap should produce.
            if previous == usize::MAX {
                as_udf::result_int(grown_mib as i64);

                return 0;
            }

            // Touch the first byte of what was just added, so the page is committed rather than reserved.
            let at = (previous * 65536) as *mut u8;

            unsafe { at.write_volatile(1) };

            grown_mib += 1;
        }

        // Reached only if nothing stopped it, which is the failure this looks for. Reported as a failure
        // rather than a number, so a test cannot mistake it for success.
        as_udf::fail("memory grew past 512 MiB without being refused");

        1
    }

    /// Write one line at each level to the server log.
    ///
    /// `log` is the one import with no return value and no effect on the record, so a client-side assertion
    /// cannot check it; it needs a function of its own.
    fn chatter(_args: Args) -> i32 {
        as_udf::log_at(Level::Warn, "chatter at warning");
        as_udf::log_at(Level::Info, "chatter at info");
        as_udf::log_at(Level::Debug, "chatter at debug");

        as_udf::result_str("logged");

        0
    }

    /// Walk a list bin and sum it, without copying it.
    ///
    /// The read path this ABI is built for: the list stays in the record and each element is one import
    /// call, where the wasmtime path would serialise the whole list, copy it into guest memory and parse it.
    fn sum_list(_args: Args) -> i32 {
        let Some(list) = as_udf::bin_coll("l") else {
            as_udf::fail("bin `l` is not a list");
            return 1;
        };

        let mut total = 0i64;

        for i in 0..list.len() {
            total += list.int_at(i);
        }

        as_udf::result_int(total);

        0
    }
}

/// Nothing here allocates, but Rust still wants a panic handler for a `no_std` cdylib.
///
/// A trap rather than a message: `panic = "abort"` in a wasm library becomes `unreachable`, which the host
/// reports as a trap with a backtrace - which is more than a guest could usefully say for itself.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

//==========================================================
// Aggregation.
//
// A stream UDF is driven by the host, one record at a time, and the **whole stream shares one instance** — so
// unlike a record UDF it can keep state across calls. That is what makes an accumulator possible at all, and
// it is why these live in `static mut` rather than being threaded through the host.
//
// Single-threaded by construction: one instance serves one stream on one thread, so there is no contention on
// these statics and no lock to take.
//

/// The running total for `sum`, and the count for `count`.
static mut TOTAL: i64 = 0;
static mut SEEN: i64 = 0;

as_udf::stream! {
    /// Sum bin `n` across every record the query matched.
    ///
    /// The aggregation counterpart of `sum_list`: one import call per record, no serialisation, and the total
    /// accumulates in the guest's own memory.
    fn sum {
        record(_args: Args) -> i32 {
            unsafe { TOTAL += as_udf::bin_int("n") };

            0
        }

        end(_args: Args) -> i32 {
            // One partial per node. A client with more than one node adds them up itself — see the note on
            // `stream!`.
            as_udf::result_int(unsafe { TOTAL });

            0
        }
    }

    /// Count the records the query matched, and report it with the sum, as a map.
    ///
    /// Here to show a non-scalar aggregation result, and that the collection builders work at `end` just as
    /// they do in a record UDF.
    fn stats {
        record(_args: Args) -> i32 {
            unsafe {
                SEEN += 1;
                TOTAL += as_udf::bin_int("n");
            }

            0
        }

        end(_args: Args) -> i32 {
            let Some(out) = as_udf::map(2) else {
                as_udf::fail("could not build the result map");
                return 1;
            };

            unsafe {
                out.put_int("count", SEEN);
                out.put_int("sum", TOTAL);
            }

            as_udf::result_coll(&out.build());

            0
        }
    }
}

/// The running total for `sumnested`.
static mut NESTED: i64 = 0;

as_udf::stream! {
    /// Sum a list nested inside a map bin, across every record a query matched.
    ///
    /// The read path this ABI exists for, end to end: a map bin, a key inside it, a list under that key, and
    /// the elements of the list — all read **in place** through handles, with nothing serialised. The Lua
    /// counterpart in `bench.lua` does the same work through the server's own object model.
    ///
    /// The map is scanned for its key rather than looked up: this ABI indexes a map's entries (the host
    /// flattens them once per handle) and has no by-key accessor, which for a map of a few keys is a scan of a
    /// few keys. A wide map would want one.
    fn sumnested {
        record(_args: Args) -> i32 {
            let Some(map) = as_udf::bin_coll("m") else {
                return 0;
            };

            // **One hash lookup and one bulk read**, where this used to scan the map's keys and then read the
            // list element by element. The scan cost an allocation per record (the host flattens a map's
            // entries to index them) plus an import call and a byte copy per key compared, and measured 1.48x
            // slower than Lua. This is two calls per record in total.
            let Some(list) = map.get_coll("vals") else {
                return 0;
            };

            // Sized for the workload's 1..=10 elements, with room to spare; a longer list reports how much it
            // needs rather than being truncated.
            let mut values = [0i64; 32];

            match list.ints_into(&mut values) {
                Ok(read) => unsafe { NESTED += read.iter().sum::<i64>() },
                Err(_) => {
                    // Too long for the buffer, or not a list of integers. Fall back to element-at-a-time
                    // rather than dropping the record: correctness first.
                    let mut sum = 0i64;

                    for j in 0..list.len() {
                        sum += list.int_at(j);
                    }

                    unsafe { NESTED += sum };
                }
            }

            0
        }

        end(_args: Args) -> i32 {
            as_udf::result_int(unsafe { NESTED });

            0
        }
    }
}
