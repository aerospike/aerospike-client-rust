//! An `xdr-merge` handler that answers whatever the incoming record tells it to.
//!
//! Not an example of a merge: a test instrument. The server takes a handler's answer and acts on it - keeps
//! a record, replaces it, writes the bins it is handed - so the answer is input to the server from code the
//! server did not write. `tests/src/xdr_merge_limits.rs` binds this to a set, writes records whose `do` bin
//! names an answer, and holds the server to what it does with each: the well-formed ones, and every
//! malformed one this file can make.
//!
//! The incoming record says what to do in two bins: `do`, a name from the table in `answer`, and `n`, a
//! number that answer uses (how many bins, how deep, which TTL). A record with no `do` gets `policy`.
//!
//! Written against the host calls directly, not the SDK's types, because most of these answers are ones the
//! SDK's `Resolution` cannot express. The signature export is the SDK's, so that the server accepts the
//! function as a handler of this event.
//!
//! ```text
//! event-set:event=xdr-merge;namespace=test;set=probe;module=merge_probe;function=probe;on-failure=fail
//! ```

#![no_std]

use aerospike_udf_sdk_wasm as as_udf;

#[link(wasm_import_module = "aerospike")]
extern "C" {
    fn event_bin_value(which: i32, name: *const u8, name_len: u32, out: *mut u8, out_cap: u32) -> i32;
    fn cursor_bytes_at(source: i32, at: u32, out: *mut u8, out_cap: u32) -> i32;
    fn map_new(capacity: u32) -> i32;
    fn list_new(capacity: u32) -> i32;
    fn val_int(value: i64) -> i32;
    fn val_double(value: f64) -> i32;
    fn val_bool(value: i32) -> i32;
    fn val_nil() -> i32;
    fn val_str(value: *const u8, value_len: u32) -> i32;
    fn val_blob(value: *const u8, value_len: u32) -> i32;
    fn val_geojson(value: *const u8, value_len: u32) -> i32;
    fn map_put(map: i32, key: i32, value: i32) -> i32;
    fn list_push(list: i32, value: i32) -> i32;
    fn result_handle(handle: i32) -> i32;
    fn result_int(value: i64);
    fn result_nil();
    fn result_str(value: *const u8, value_len: u32);
    #[link_name = "fail"]
    fn host_fail(message: *const u8, message_len: u32);
}

as_udf::event!(@sig probe, "event xdr-merge/1 probe(current: record, incoming: record, conflict: conflict) -> resolution");

/// A bin of one side as its 16-byte value, or none.
unsafe fn side_bin(which: i32, name: &[u8]) -> Option<[u8; 16]> {
    let mut value = [0u8; 16];

    if event_bin_value(which, name.as_ptr(), name.len() as u32, value.as_mut_ptr(), 16) != 0 {
        return None;
    }

    Some(value)
}

fn word(value: &[u8; 16], at: usize) -> i64 {
    let mut bytes = [0u8; 8];

    bytes.copy_from_slice(&value[at..at + 8]);

    i64::from_le_bytes(bytes)
}

/// What the incoming record asks for. The dry run also calls a handler with the sides swapped, to check
/// that a merge is symmetric, so the other side is asked when the incoming one does not say.
unsafe fn asked<'a>(buf: &'a mut [u8; 32]) -> (&'a [u8], i64) {
    for which in [1, 0] {
        let Some(value) = side_bin(which, b"do") else { continue };
        let payload = word(&value, 8);
        let n = cursor_bytes_at((payload >> 32) as i32, payload as u32, buf.as_mut_ptr(), 32);

        if n < 0 {
            continue;
        }

        let number = side_bin(which, b"n").map_or(0, |v| word(&v, 8));

        return (&buf[..n as usize], number);
    }

    // Neither side says: an ordinary record, and the namespace's policy for it.
    buf[..6].copy_from_slice(b"policy");

    (&buf[..6], 0)
}

unsafe fn text(s: &[u8]) -> i32 {
    val_str(s.as_ptr(), s.len() as u32)
}

unsafe fn put(map: i32, key: &[u8], value: i32) {
    map_put(map, text(key), value);
}

/// `{verdict: <verdict>, bins: <bins>, ttl: <ttl>}`, with a part left out when its handle is zero.
unsafe fn answer_map(verdict: i32, bins: i32, ttl: i32) {
    let out = map_new(3);

    if verdict != 0 {
        put(out, b"verdict", verdict);
    }

    if bins != 0 {
        put(out, b"bins", bins);
    }

    if ttl != 0 {
        put(out, b"ttl", ttl);
    }

    result_handle(out);
}

unsafe fn record(bins: i32) {
    answer_map(text(b"record"), bins, 0);
}

/// A map of one bin, `merged`, holding `value`.
unsafe fn one_bin(value: i32) -> i32 {
    let bins = map_new(1);

    put(bins, b"merged", value);

    bins
}

/// `prefix` followed by `i` in decimal.
fn numbered<'a>(buf: &'a mut [u8; 24], prefix: &[u8], mut i: u32) -> &'a [u8] {
    let mut digits = [0u8; 10];
    let mut n = 0;

    loop {
        digits[n] = b'0' + (i % 10) as u8;
        n += 1;
        i /= 10;

        if i == 0 {
            break;
        }
    }

    buf[..prefix.len()].copy_from_slice(prefix);

    for d in 0..n {
        buf[prefix.len() + d] = digits[n - 1 - d];
    }

    &buf[..prefix.len() + n]
}

#[no_mangle]
pub extern "C" fn probe(_args: i32, _argc: i32) -> i32 {
    unsafe { answer() }
}

unsafe fn answer() -> i32 {
    let mut buf = [0u8; 32];
    let (what, n) = asked(&mut buf);

    match what {
        // The handler does not answer at all.
        b"trap" => core::arch::wasm32::unreachable(),
        b"spin" => loop {
            core::hint::black_box(0);
        },
        b"fail" => {
            host_fail(b"the probe was told to fail".as_ptr(), 26);
            return 1;
        }
        b"nonzero" => return 1,
        b"silent" => return 0,

        // An answer of the wrong shape.
        b"int" => result_int(5),
        b"nil" => result_nil(),
        b"list" => {
            result_handle(list_new(0));
        }
        b"m-empty" => {
            result_handle(map_new(0));
        }
        b"m-verdict-int" => answer_map(val_int(5), 0, 0),
        b"m-verdict-nul" => answer_map(text(b"record\0x"), one_bin(val_int(1)), 0),

        // A verdict by name: the three there are, and ones there are not.
        b"policy" | b"keep-local" | b"take-remote" | b"record" | b"banana" | b"" | b"KEEP-LOCAL" => {
            result_str(what.as_ptr(), what.len() as u32);
        }
        b"m-keep" => answer_map(text(b"keep-local"), one_bin(val_int(1)), 0),
        b"m-take" => answer_map(text(b"take-remote"), 0, 0),
        b"m-take-ttl" => answer_map(text(b"take-remote"), 0, val_int(n)),

        // A record, with bins that are not bins.
        b"r-nobins" => answer_map(text(b"record"), 0, 0),
        b"r-bins-int" => record(val_int(5)),
        b"r-bins-list" => record(list_new(0)),
        b"r-bins-nil" => record(val_nil()),

        // A record, well-formed.
        b"r-ok" => record(one_bin(val_int(n))),
        b"r-empty" => record(map_new(0)),
        b"r-nil" => {
            let bins = map_new(2);

            put(bins, b"merged", val_nil());
            put(bins, b"base", val_nil());
            record(bins);
        }
        b"r-kinds" => {
            let bins = map_new(9);
            let list = list_new(2);
            let inner = map_new(1);
            let point = b"{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}";

            list_push(list, val_int(1));
            list_push(list, text(b"two"));
            put(inner, b"k", val_double(1.5));
            put(bins, b"i", val_int(i64::MIN));
            put(bins, b"d", val_double(-0.0));
            put(bins, b"t", val_bool(1));
            put(bins, b"s", text(b"text"));
            put(bins, b"e", text(b""));
            put(bins, b"b", val_blob(b"\0\x01\x02".as_ptr(), 3));
            put(bins, b"g", val_geojson(point.as_ptr(), point.len() as u32));
            put(bins, b"l", list);
            put(bins, b"m", inner);
            record(bins);
        }

        // A record whose bins have names a bin cannot have.
        b"r-15" => {
            let bins = map_new(1);

            put(bins, b"0123456789abcde", val_int(1));
            record(bins);
        }
        b"r-long" => {
            let bins = map_new(2);

            put(bins, b"merged", val_int(1));
            put(bins, b"0123456789abcdef", val_int(1));
            record(bins);
        }
        b"r-nul" => {
            let bins = map_new(1);

            // On its own: beside a bin `a` the two would be one key to the map itself, which orders
            // strings as C strings.
            put(bins, b"a\0b", val_int(1));
            record(bins);
        }
        b"r-noname" => {
            let bins = map_new(1);

            put(bins, b"", val_int(1));
            record(bins);
        }
        b"r-intkey" => {
            let bins = map_new(2);

            map_put(bins, val_int(5), val_int(1));
            map_put(bins, list_new(0), val_int(1));
            put(bins, b"merged", val_int(1));
            record(bins);
        }

        // A record that is too much: `n` bins, a collection `n` deep, a blob of `n` KiB.
        b"r-many" => {
            let bins = map_new(n as u32);
            let one = val_int(1);

            for i in 0..n as u32 {
                let mut name = [0u8; 24];

                put(bins, numbered(&mut name, b"b", i), one);
            }

            record(bins);
        }
        b"r-deep" => {
            let mut inner = list_new(1);

            for _ in 1..n {
                let outer = list_new(1);

                if outer == 0 || list_push(outer, inner) != 0 {
                    break;
                }

                inner = outer;
            }

            record(one_bin(inner));
        }
        b"r-big" => {
            static KIB: [u8; 1024] = [0x5a; 1024];
            let list = list_new(n as u32);

            for _ in 0..n {
                let chunk = val_blob(KIB.as_ptr(), 1024);

                if chunk == 0 || list_push(list, chunk) != 0 {
                    break;
                }

                // Only the last handle can be closed, and a chunk is the last: keep the table from filling.
                as_udf_close(chunk);
            }

            record(one_bin(list));
        }
        b"r-geo-bad" => {
            record(one_bin(val_geojson(b"not a document".as_ptr(), 14)));
        }

        // A record with a time to live of `n`, and with one that is not a number.
        b"r-ttl" => answer_map(text(b"record"), one_bin(val_int(1)), val_int(n)),
        b"r-ttl-str" => answer_map(text(b"record"), one_bin(val_int(1)), text(b"soon")),

        _ => result_str(b"policy".as_ptr(), 6),
    }

    0
}

#[link(wasm_import_module = "aerospike")]
extern "C" {
    #[link_name = "cursor_close"]
    fn as_udf_close(handle: i32);
}

//==========================================================
// Signatures that are not signatures.
//
// The server asks a function's signature export what the function is before it binds it to an event: the
// export answers the address of a string in the module's memory. These answer everything else. Each is a
// handler by name only - `s_zero`, `s_wild` and the rest - and binding any of them has to be refused.
//

/// One past a megabyte of `a`: a signature far longer than any the server keeps.
static LONG: [u8; 100_001] = {
    let mut text = [b'a'; 100_001];

    text[100_000] = 0;
    text
};

macro_rules! signature {
    ($name:ident, $export:literal, $body:expr) => {
        #[export_name = $export]
        pub extern "C" fn $name() -> i32 {
            $body
        }
    };
}

// No string at all, and an address that is not the module's.
signature!(sig_zero, "__udf_sig_s_zero", 0);
signature!(sig_wild, "__udf_sig_s_wild", 0x7fff_ff00);
signature!(sig_negative, "__udf_sig_s_negative", -8);

// The last byte of memory, which nothing terminates.
signature!(sig_edge, "__udf_sig_s_edge", {
    let end = core::arch::wasm32::memory_size(0) * 65536;

    unsafe { *((end - 1) as *mut u8) = b'x' };

    (end - 1) as i32
});

// A string that says the right thing and then tries to say more to whoever reads the server's answer.
signature!(
    sig_control,
    "__udf_sig_s_control",
    b"event xdr-merge/9 s_control\nok;event-list=\x1b[2Jforged\0".as_ptr() as i32
);
signature!(sig_long, "__udf_sig_s_long", LONG.as_ptr() as i32);
signature!(sig_empty, "__udf_sig_s_empty", b"\0".as_ptr() as i32);

// The wrong event, the wrong version, and a version that is not a number.
signature!(sig_other, "__udf_sig_s_other", b"event before-write/1 s_other(a: record)\0".as_ptr() as i32);
signature!(sig_version, "__udf_sig_s_version", b"event xdr-merge/99 s_version()\0".as_ptr() as i32);
signature!(sig_no_version, "__udf_sig_s_noversion", b"event xdr-merge/ s_noversion()\0".as_ptr() as i32);
signature!(sig_big_version, "__udf_sig_s_bigversion", b"event xdr-merge/4294967297 s_bigversion()\0".as_ptr() as i32);
signature!(sig_plain, "__udf_sig_s_plain", b"s_plain(a: int) -> int\0".as_ptr() as i32);

// One that never answers, and one that traps.
signature!(sig_spin, "__udf_sig_s_spin", loop {
    core::hint::black_box(0);
});
signature!(sig_trap, "__udf_sig_s_trap", core::arch::wasm32::unreachable());

// A right signature for a function the module does not have.
signature!(
    sig_ghost,
    "__udf_sig_s_ghost",
    b"event xdr-merge/1 s_ghost(current: record, incoming: record, conflict: conflict) -> resolution\0".as_ptr() as i32
);

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}
