//! Writing an Aerospike WASM UDF for the WAMR engine.
//!
//! # The shape of a UDF
//!
//! ```ignore
//! use aerospike_udf_sdk_wasm::{export, Error, Record, Value};
//!
//! export! {
//!     /// Move `amount` out of this record's balance, unless `dry_run`.
//!     fn withdraw(rec: &mut Record, amount: i64, dry_run: Option<bool>) -> Result<Value, Error> {
//!         let balance = rec.bin("balance")?.int()?;
//!
//!         if balance < amount {
//!             return Err(Error::Custom("insufficient funds"));
//!         }
//!
//!         if dry_run != Some(true) {
//!             rec.set("balance", balance - amount)?;
//!             rec.save()?;
//!         }
//!
//!         Ok(Value::Int(balance - amount))
//!     }
//! }
//! ```
//!
//! Four ideas carry the whole API:
//!
//! - **The record is a [`Record`]**, the function's first parameter: `&Record` to read it, `&mut Record` to
//!   write it too, and left out by a function that does neither. Everything about the record is a method
//!   on it; what is left at the crate's top level is not about the record - the clock, the log, the
//!   builders of lists and maps. A bin write is staged until [`Record::save`].
//! - **Every read is a [`Value`]**, the guest-side analogue of the client's `Value`: [`Record::bin`] returns one, a
//!   collection iterates them, a map lookup returns one, and a UDF returns one as its result. It is *borrowed*
//!   rather than owned, because the guest need not have an allocator: a string is its length and where it
//!   sits, fetched into a buffer you own with [`Text::as_str`], into a [`Str`] of a fixed capacity, or - with
//!   the `alloc` feature - into a `String`; a nested list or map is a [`Coll`] you iterate or look up, and
//!   each of those is one import call for a window of elements, not one per element.
//! - **Every failure is an [`Error`]**, and the UDF's own signature is `Result<Value, Error>`: `Ok` is the
//!   result the client receives, `Err` fails the call with the error's message. `?` works everywhere.
//! - **Arguments are typed in the signature.** The [`export!`] macro decodes them, checks the count and the
//!   kinds, and rejects a mismatched call with a precise message before the body runs.
//!
//! # What makes this different from the wasmtime SDK
//!
//! There is no msgpack here, and no allocator. The host is C, holds the record natively, and decodes a
//! collection bin's stored bytes in place rather than deserialising it; a batch of 128 values crosses in one
//! import call. Consequences worth knowing:
//!
//! - **`no_std`, and no allocator unless you ask for one.** Nothing here allocates by default. Buffers are
//!   yours: anything that gives you bytes takes a `&mut [u8]` or fills a [`Str`] / [`Bytes`] of the capacity
//!   you declare, and a buffer that is too small reports the size needed ([`Error::TooSmall`]) rather than
//!   truncating. The `alloc` feature adds `String` and `Vec<u8>` as parameter and return types and as what
//!   [`Text::to_string`] / [`Blob::to_vec`] return, for a UDF that wants a string of any length; the
//!   `allocator` feature adds a global allocator (dlmalloc) so such a UDF crate needs nothing else. Both are
//!   off by default, because a module without an allocator is smaller and that was the measured cost.
//! - **A value borrows from the call.** It is valid until the call returns, or until a `set_*` on the bin it
//!   came from. Finish reading a bin before writing it.
//! - **A module built against this SDK does not run on the wasmtime engine**, and vice versa. The ABIs are
//!   different by design.

#![no_std]
#![allow(clippy::missing_safety_doc)]

#[cfg(feature = "alloc")]
extern crate alloc;

use core::fmt::Write as _;

// The one allocator a UDF needs, when it asks for one: dlmalloc grows linear memory as it goes and gives
// memory back for reuse, which matters because a module's instance - and its memory - outlives any one call.
#[cfg(all(feature = "allocator", target_arch = "wasm32"))]
#[global_allocator]
static ALLOCATOR: dlmalloc::GlobalDlmalloc = dlmalloc::GlobalDlmalloc;

//==========================================================
// The imports.
//

#[link(wasm_import_module = "aerospike")]
extern "C" {
    // Bins - reading. One import for any kind: the host writes a 16-byte value.
    #[link_name = "bin_value"]
    fn raw_bin_value(name: *const u8, name_len: i32, out: *mut u8, out_cap: i32) -> i32;

    // Bins - writing.
    #[link_name = "set_int"]
    fn raw_set_int(name: *const u8, name_len: i32, value: i64) -> i32;
    #[link_name = "set_double"]
    fn raw_set_double(name: *const u8, name_len: i32, value: f64) -> i32;
    #[link_name = "set_bool"]
    fn raw_set_bool(name: *const u8, name_len: i32, value: i32) -> i32;
    #[link_name = "set_str"]
    fn raw_set_str(name: *const u8, name_len: i32, value: *const u8, value_len: i32) -> i32;
    #[link_name = "set_blob"]
    fn raw_set_blob(name: *const u8, name_len: i32, value: *const u8, value_len: i32) -> i32;
    #[link_name = "set_geojson"]
    fn raw_set_geojson(name: *const u8, name_len: i32, value: *const u8, value_len: i32) -> i32;
    #[link_name = "set_nil"]
    fn raw_set_nil(name: *const u8, name_len: i32) -> i32;
    #[link_name = "set_handle"]
    fn raw_set_handle(name: *const u8, name_len: i32, handle: i32) -> i32;
    #[link_name = "set_at"]
    fn raw_set_at(name: *const u8, name_len: i32, src: i32, at: i32) -> i32;
    #[link_name = "bin_remove"]
    fn raw_bin_remove(name: *const u8, name_len: i32) -> i32;
    #[link_name = "set_list_ints"]
    fn raw_set_list_ints(name: *const u8, name_len: i32, vals: *const u8, vals_len: i32) -> i32;
    #[link_name = "set_list_doubles"]
    fn raw_set_list_doubles(name: *const u8, name_len: i32, vals: *const u8, vals_len: i32) -> i32;

    // Collections, by position in a source.
    #[link_name = "cursor_values"]
    fn raw_cursor_values(cursor: i32, out: *mut u8, out_cap: i32) -> i32;
    #[link_name = "cursor_bytes_at"]
    fn raw_cursor_bytes_at(src: i32, at: i32, out: *mut u8, out_cap: i32) -> i32;
    #[link_name = "cursor_enter_at"]
    fn raw_cursor_enter_at(src: i32, at: i32) -> i32;
    #[link_name = "coll_get"]
    fn raw_coll_get(src: i32, at: i32, key: *const u8, key_len: i32, out: *mut u8, out_cap: i32) -> i32;
    #[link_name = "coll_get_i"]
    fn raw_coll_get_i(src: i32, at: i32, key: i64, out: *mut u8, out_cap: i32) -> i32;
    #[link_name = "list_get"]
    fn raw_list_get(src: i32, at: i32, index: i32, out: *mut u8, out_cap: i32) -> i32;
    #[link_name = "cursor_close"]
    fn raw_cursor_close(cursor: i32);
    #[link_name = "pack_handle"]
    fn raw_pack_handle(handle: i32) -> i32;
    #[link_name = "args_values"]
    fn raw_args_values(handle: i32, out: *mut u8, out_cap: i32) -> i32;
    fn coll_size(handle: i32) -> i32;

    // Collections - building.
    fn list_new(capacity: i32) -> i32;
    fn map_new(capacity: i32) -> i32;
    #[link_name = "map_has"]
    fn raw_map_has(handle: i32, key: *const u8, key_len: i32) -> i32;
    #[link_name = "map_has_i"]
    fn raw_map_has_i(handle: i32, key: i64) -> i32;
    // Values into a built collection, by value: 16-byte values, or 32-byte key-value pairs. Every push and
    // insert goes through these two, one element or many, because they take none of the call's host handles:
    // the per-value natives (`val_double`, `map_put`, ...) hold one each for the rest of the call.
    fn list_push_values(handle: i32, vals: *const u8, vals_len: i32) -> i32;
    fn map_put_entries(handle: i32, pairs: *const u8, pairs_len: i32) -> i32;

    // Results.
    #[link_name = "result_nil"]
    fn raw_result_nil();
    #[link_name = "result_int"]
    fn raw_result_int(value: i64);
    #[link_name = "result_double"]
    fn raw_result_double(value: f64);
    #[link_name = "result_bool"]
    fn raw_result_bool(value: i32);
    #[link_name = "result_str"]
    fn raw_result_str(value: *const u8, value_len: i32);
    #[link_name = "result_blob"]
    fn raw_result_blob(value: *const u8, value_len: i32);
    #[link_name = "result_geojson"]
    fn raw_result_geojson(value: *const u8, value_len: i32);
    #[link_name = "result_handle"]
    fn raw_result_handle(handle: i32) -> i32;
    #[link_name = "result_at"]
    fn raw_result_at(src: i32, at: i32) -> i32;
    #[link_name = "fail"]
    fn raw_fail(message: *const u8, message_len: i32);

    // Metadata.
    fn gen() -> i64;
    #[link_name = "ttl"]
    fn raw_ttl() -> i64;
    fn last_update() -> i64;
    fn record_size() -> i64;
    fn numbins() -> i64;
    fn current_time() -> i64;
    #[link_name = "set_ttl"]
    fn raw_set_ttl(seconds: i32) -> i32;
    #[link_name = "set_name"]
    fn raw_set_name(out: *mut u8, out_cap: i32) -> i32;
    fn key_type() -> i32;
    fn key_int() -> i64;
    #[link_name = "key_bytes"]
    fn raw_key_bytes(out: *mut u8, out_cap: i32) -> i32;
    #[link_name = "digest"]
    fn raw_digest(out: *mut u8, out_cap: i32) -> i32;

    // Events: a bin of one of the event's two records, 0 the current one and 1 the incoming.
    #[link_name = "event_bin_value"]
    fn raw_event_bin_value(which: i32, name: *const u8, name_len: i32, out: *mut u8, out_cap: i32) -> i32;

    // A stored value's packed bytes, and two stored values compared byte by byte.
    #[link_name = "coll_bytes_at"]
    fn raw_coll_bytes_at(src: i32, at: i32, out: *mut u8, out_cap: i32) -> i32;
    #[link_name = "coll_cmp_at"]
    fn raw_coll_cmp_at(src1: i32, at1: i32, src2: i32, at2: i32) -> i32;

    #[link_name = "rec_create"]
    fn raw_rec_create() -> i32;
    #[link_name = "rec_update"]
    fn raw_rec_update() -> i32;
    #[link_name = "rec_remove"]
    fn raw_rec_remove() -> i32;
    #[link_name = "rec_exists"]
    fn raw_rec_exists() -> i32;
    #[link_name = "log"]
    fn raw_log(level: i32, message: *const u8, message_len: i32);
}

//==========================================================
// Status codes, mirroring mod_wamr_internal.h.
//

const OK: i32 = 0;
const ERR: i32 = -1;
const NOT_FOUND: i32 = -2;
const TYPE_MISMATCH: i32 = -3;
const REFUSED: i32 = -4;
const NEEDED_BASE: i32 = -16;

/// The 16-byte value the host hands back for anything it reads: kind, length, payload.
const VALUE_SZ: usize = 16;

/// How many values an iterator fetches per import call, and a batch carries to the host: 2 KB of guest stack.
const WINDOW: usize = 128;

// Kinds a guest uses only in the values it hands the host in a batch (see `Value::raw`): a string, blob
// or GeoJson of the guest's by address, and a built collection by handle. The host never produces them.
const GUEST_STR: i32 = 16;
const GUEST_BLOB: i32 = 17;
const GUEST_GEOJSON: i32 = 18;
const HANDLE: i32 = 19;

//==========================================================
// Errors.
//

/// Why a call did not do what was asked. The one error type: every fallible call returns it, and a UDF
/// returns it to fail the call with the error's [`message`](Error::message).
///
/// `?` passes one on; [`Error::Custom`] carries a message of the UDF's own. An error returned from a UDF is
/// all the client sees of the call: no result arrives with it, and **the call writes nothing**. Bins staged
/// but not saved are dropped, and so is what the call had already written: a [`Record::save`] that succeeded,
/// and a [`Record::set_ints`] or [`Record::set_doubles`], are undone when the call then fails.
///
/// ```ignore
/// let balance = rec.bin("balance")?.int()?;      // Absent, or WrongType
///
/// if balance < amount {
///     return Err(Error::Custom("insufficient funds"));
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// No such bin, map key, list index or argument. Also what a read answers for a name that cannot be a bin
    /// name: empty, longer than 15 bytes, or with a zero byte in it.
    Absent,
    /// The value is not the kind asked for: `int()` on a string, `list()` on an integer. Also a value that
    /// cannot go where it was sent: [`Value::Other`], an HLL made from guest bytes, or guest bytes written as
    /// a plain blob over a bin that holds an HLL.
    WrongType,
    /// The buffer was too small; this many bytes are needed. Nothing was written and nothing truncated: call
    /// again with a buffer of that size.
    TooSmall(usize),
    /// The host would not do it. The record is read-only here (a stream UDF's record; an event handler has no
    /// record to write), the server rejected the write, the clock was asked for in an event handler, a
    /// collection would nest more than 64 deep, a call came with more than [`MAX_ARGS`] arguments, or the
    /// call has used up the memory it may hold on the server: every collection, string read and open
    /// iterator counts against that budget, and there is no limit on how many of them there are. It is also
    /// what [`List::push`] and [`Map::insert`] answer when the host does not take the value, whatever the
    /// reason.
    Refused,
    /// The host could not decode the stored bytes, or text that had to be UTF-8 was not. Also what a write
    /// answers for a name that cannot be a bin name (empty, longer than 15 bytes, or with a zero byte in it),
    /// and what using a value gives once it is no longer valid (see [`Value`]). Not something a UDF can
    /// repair; pass it on.
    Malformed,
    /// A failure of the UDF's own, with the message the client will see.
    Custom(&'static str),
}

impl Error {
    /// The message a client sees when a UDF returns this. A fixed text per variant - `"absent: no such bin,
    /// key, element or argument"`, `"wrong type"`, `"buffer too small"`, `"refused"`, `"malformed value"` -
    /// and for [`Error::Custom`] the message it carries. The size in [`Error::TooSmall`] is not part of it.
    #[must_use]
    pub fn message(&self) -> &'static str {
        match self {
            Error::Absent => "absent: no such bin, key, element or argument",
            Error::WrongType => "wrong type",
            Error::TooSmall(_) => "buffer too small",
            Error::Refused => "refused",
            Error::Malformed => "malformed value",
            Error::Custom(message) => message,
        }
    }

    /// The host's status for this error, as the number the host itself uses, for a caller that reports a
    /// failure in the terms the other SDKs do: `-2` (`NOT_FOUND`) for [`Error::Absent`], `-3`
    /// (`TYPE_MISMATCH`) for [`Error::WrongType`], `-4` (`REFUSED`) for [`Error::Refused`], `-16 - n` (the
    /// too-small form, which carries the size) for [`Error::TooSmall`]`(n)`, and `-1` (`ERR`) for
    /// [`Error::Malformed`] and for [`Error::Custom`], which is no status of the host's. The variant is the
    /// status already, and is what Rust code matches on; this is the same fact as a number.
    #[must_use]
    pub fn status(&self) -> i32 {
        match self {
            Error::Absent => NOT_FOUND,
            Error::WrongType => TYPE_MISMATCH,
            Error::Refused => REFUSED,
            Error::TooSmall(n) => NEEDED_BASE.saturating_sub(i32::try_from(*n).unwrap_or(i32::MAX)),
            Error::Malformed | Error::Custom(_) => ERR,
        }
    }

    fn from_status(rc: i32) -> Error {
        match rc {
            NOT_FOUND => Error::Absent,
            TYPE_MISMATCH => Error::WrongType,
            REFUSED => Error::Refused,
            rc if rc <= NEEDED_BASE => Error::TooSmall((NEEDED_BASE - rc) as usize),
            _ => Error::Malformed,
        }
    }
}

// Turn one of the (out, cap) conventions into the length written.
fn bytes_result(rc: i32, cap: usize) -> Result<usize, Error> {
    if rc >= 0 {
        let written = rc as usize;

        // Cannot happen unless the host is wrong about its own buffer, and silently trusting it is how a
        // read walks off the end of a slice.
        if written > cap {
            return Err(Error::Malformed);
        }

        return Ok(written);
    }

    Err(Error::from_status(rc))
}

/// How severe a line written with [`log`] is. Each maps to the server's log level of the same name, under the
/// server's `udf` log context, whose configured level decides whether `Info` and `Debug` lines are written at
/// all. `Warn` is the loudest.
#[derive(Clone, Copy, Debug)]
pub enum Level {
    /// The server's `warning` level.
    Warn = 0,
    /// The server's `info` level.
    Info = 1,
    /// The server's `debug` level.
    Debug = 2,
}

//==========================================================
// Values.
//

/// One value: a bin, an element, a map entry, an argument, or a UDF's result.
///
/// The guest-side analogue of the client's `Value`, with one difference that runs through everything: it is
/// **borrowed, never owned**, because the guest has no allocator. A string or blob is a [`Text`] / [`Blob`]
/// that knows where its bytes are and copies them out on request; a nested list or map is a [`Coll`] that
/// opens a window onto the stored bytes when iterated. Reading a value costs nothing until its bytes or
/// elements are asked for.
///
/// ```ignore
/// let v = rec.bin("profile")?;                    // Value::Map
///
/// if let Some(age) = v.map()?.get("age")? {       // one lookup on the host; None for no such key
///     let age = age.int()?;
/// }
///
/// let mut buf = [0u8; 64];
/// let name = v.map()?.get("name")?.ok_or(Error::Absent)?;
/// let name = name.as_str(&mut buf)?;              // the text, copied into buf
/// ```
///
/// The accessors (`int`, `double`, `bool`, `string`, `blob`, `geojson`, `list`, `map`, `hll`) do not
/// convert: each answers `Err(WrongType)` for any kind but its own, [`Value::Nil`] included. An integer is
/// not a double, a string is not a blob, and a GeoJson document is not a string. The one reader that takes
/// several kinds says so in its name: [`bytes`](Value::bytes), the raw bytes of a string, blob, GeoJson
/// document or HLL.
///
/// A value read from the record or from the arguments is valid for the rest of the call, or until the bin it
/// came from is written; finish reading a bin before writing it. One more case ends it early: when the server
/// refuses a write ([`Record::save`], [`Record::delete`], [`Record::set_ints`] returning `Err(Refused)`), a
/// list, map or blob read from a bin before that is gone. Using it afterwards gives `Err(Malformed)`, and
/// iterating it one `Err(Refused)` item. Read the bin again.
///
/// Values a UDF builds use the same type: `Value::Int(3)`, `"text".into()`, or a [`List`] / [`Map`] built on
/// the host. [`Value::List`] and [`Value::Map`] then hold a [`Coll::Built`]. A value made from a `&str` or
/// `&[u8]` borrows it, and is copied by the host when it is written, pushed or returned.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value<'a> {
    /// Nil. Never what a bin holds - a nil bin is an absent one - but an argument, a list element or a map
    /// value can be nil, and a UDF returns it for "no result".
    Nil,
    /// A boolean.
    Bool(bool),
    /// A 64-bit signed integer, the server's only integer type.
    Int(i64),
    /// A 64-bit float. Never converted to or from [`Value::Int`].
    Double(f64),
    /// A string: its text, to fetch with [`Text::as_str`] or [`Text::to_inline`].
    String(Text<'a>),
    /// A blob: its bytes, to fetch with [`Blob::bytes`] or [`Blob::to_inline`].
    Blob(Blob<'a>),
    /// A GeoJson document: its text, read with [`Value::geojson`]. Distinct from [`Value::String`] on purpose:
    /// the particle type is what makes a bin geospatially indexable, and writing a document back as a string
    /// would silently make it unqueryable.
    GeoJson(Text<'a>),
    /// A list: iterate it with [`Coll::iter`], index it with [`Coll::at`].
    List(Coll),
    /// A map: iterate it with [`Coll::entries`], look a key up with [`Coll::get`].
    Map(Coll),
    /// An HLL: the bytes the server's HLL operations maintain. Readable, and writable back **by position**
    /// only - a UDF cannot make one from guest bytes, since the subtype that makes it an HLL is the
    /// server's to set.
    Hll(Blob<'a>),
    /// A kind this SDK has no variant for. It can be passed over, not read; writing it to a bin or putting it
    /// in a collection is `Err(WrongType)`, and returning it returns nil.
    Other,
}

/// The text of a string or of a GeoJson document: in the record, or in the guest's own memory.
///
/// What [`Value::string`], [`Value::geojson`] and the typed readers ([`Record::string`]) give. Its length is known at once; the
/// text itself is copied out when asked for, into a buffer of the caller's ([`as_str`](Text::as_str)), a
/// [`Str`] ([`to_inline`](Text::to_inline)) or, with the `alloc` feature, a `String`
/// ([`to_string`](Text::to_string)).
///
/// ```ignore
/// if let Some(owner) = rec.string("owner")? {
///     let mut buf = [0u8; 64];
///     let owner = owner.as_str(&mut buf)?;        // Err(TooSmall(n)) past 64 bytes
///     log(Level::Info, owner);
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Text<'a> {
    /// `len` bytes at a position in a source the host holds. What a read yields. `src` and `at` are the
    /// host's own way of finding the bytes again, meaningful only to it and only during the call.
    Stored { src: i32, at: u32, len: usize },
    /// A `&str` of the guest's. What a UDF builds; `"text".into()` makes one.
    Guest(&'a str),
}

/// A blob's bytes (or an HLL's): in the record, or in the guest's own memory. The counterpart of [`Text`] for
/// bytes, and read the same way: the length at once, the bytes copied out by [`bytes`](Blob::bytes),
/// [`to_inline`](Blob::to_inline) or, with the `alloc` feature, [`to_vec`](Blob::to_vec).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Blob<'a> {
    /// `len` bytes at a position in a source the host holds. What a read yields.
    Stored { src: i32, at: u32, len: usize },
    /// A `&[u8]` of the guest's. What a UDF builds; `bytes.into()` makes a [`Value::Blob`] of one.
    Guest(&'a [u8]),
}

/// A list or map: `len` elements (or entries) somewhere the host can walk. What [`Value::list`],
/// [`Value::map`] and the typed readers ([`Record::list`], [`Record::map`]) give, and what a `Coll` parameter
/// of a UDF receives.
///
/// A `Coll` is a small `Copy` handle; the elements stay on the host until they are asked for. Whether it is a
/// list or a map is known to the [`Value`] it came out of, not to the `Coll`: [`at`](Coll::at) finds nothing
/// (`Ok(None)`) on a map, and [`get`](Coll::get) nothing on a list.
///
/// ```ignore
/// let mut total = 0;
///
/// for item in rec.bin("scores")?.list()?.iter() {
///     total += item?.int()?;
/// }
///
/// for entry in rec.bin("counts")?.map()?.entries() {
///     let (site, count) = entry?;
///     // ...
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coll {
    /// In the record, or in the packed copy of an argument list: at a position in a source the host holds.
    /// `len` is the number of elements of a list or of entries of a map.
    Stored { src: i32, at: u32, len: usize },
    /// Built on the host by [`list`] / [`map`], identified by its handle. It can be read like a stored one,
    /// at the price of the host packing a copy of it first.
    Built(i32),
}

impl<'a> Value<'a> {
    fn from_raw(v: &[u8; VALUE_SZ]) -> Value<'static> {
        let kind = i32::from_le_bytes([v[0], v[1], v[2], v[3]]);
        let len = u32::from_le_bytes([v[4], v[5], v[6], v[7]]) as usize;
        let payload = i64::from_le_bytes([v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]]);
        let src = (payload >> 32) as i32;
        let at = (payload & 0xffff_ffff) as u32;

        match kind {
            0 => Value::Nil,
            1 => Value::Bool(payload != 0),
            2 => Value::Int(payload),
            3 => Value::Double(f64::from_bits(payload as u64)),
            4 => Value::String(Text::Stored { src, at, len }),
            5 => Value::Blob(Blob::Stored { src, at, len }),
            6 => Value::List(Coll::Stored { src, at, len }),
            7 => Value::Map(Coll::Stored { src, at, len }),
            9 => Value::GeoJson(Text::Stored { src, at, len }),
            10 => Value::Hll(Blob::Stored { src, at, len }),
            _ => Value::Other,
        }
    }

    /// What kind of value this is, as an English phrase for a message: `"nil"`, `"a boolean"`, `"an
    /// integer"`, `"a double"`, `"a string"`, `"a blob"`, `"a geojson document"`, `"a list"`, `"a map"`, `"an
    /// HLL"` or `"an unsupported value"`. It is what the rejection of a mistyped argument says it got.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Value::Nil => "nil",
            Value::Bool(_) => "a boolean",
            Value::Int(_) => "an integer",
            Value::Double(_) => "a double",
            Value::String(_) => "a string",
            Value::Blob(_) => "a blob",
            Value::GeoJson(_) => "a geojson document",
            Value::List(_) => "a list",
            Value::Map(_) => "a map",
            Value::Hll(_) => "an HLL",
            Value::Other => "an unsupported value",
        }
    }

    /// The bytes of an HLL, or `Err(WrongType)` for any other kind, a plain blob included. They are the
    /// server's own encoding of the sketch: something to copy, compare or hand back, not to interpret. An HLL
    /// bin is written back by passing the [`Value`] itself to [`Record::set`], never from these bytes.
    pub fn hll(&self) -> Result<Blob<'a>, Error> {
        match self {
            Value::Hll(b) => Ok(*b),
            _ => Err(Error::WrongType),
        }
    }

    /// The integer, or `Err(WrongType)` for any other kind. A double is not converted.
    pub fn int(&self) -> Result<i64, Error> {
        match self {
            Value::Int(i) => Ok(*i),
            _ => Err(Error::WrongType),
        }
    }

    /// The double, or `Err(WrongType)` for any other kind. An integer is not converted.
    pub fn double(&self) -> Result<f64, Error> {
        match self {
            Value::Double(d) => Ok(*d),
            _ => Err(Error::WrongType),
        }
    }

    /// The boolean, or `Err(WrongType)` for any other kind.
    pub fn bool(&self) -> Result<bool, Error> {
        match self {
            Value::Bool(b) => Ok(*b),
            _ => Err(Error::WrongType),
        }
    }

    /// The text of a string, or `Err(WrongType)` for any other kind, a GeoJson document included: read that
    /// with [`geojson`](Value::geojson). Nothing is copied yet: the [`Text`] knows the length, and
    /// [`Text::as_str`] or [`Text::to_inline`] fetches the bytes.
    pub fn string(&self) -> Result<Text<'a>, Error> {
        match self {
            Value::String(t) => Ok(*t),
            _ => Err(Error::WrongType),
        }
    }

    /// The text of a GeoJson document, or `Err(WrongType)` for any other kind, a string included.
    pub fn geojson(&self) -> Result<Text<'a>, Error> {
        match self {
            Value::GeoJson(t) => Ok(*t),
            _ => Err(Error::WrongType),
        }
    }

    /// The bytes of a blob, or `Err(WrongType)` for any other kind. Strict: a string is not a blob, nor is
    /// a GeoJson document or an HLL (see [`Value::hll`]). [`bytes`](Value::bytes) reads the raw bytes of
    /// any of them.
    pub fn blob(&self) -> Result<Blob<'a>, Error> {
        match self {
            Value::Blob(b) => Ok(*b),
            _ => Err(Error::WrongType),
        }
    }

    /// The raw bytes of a value that has bytes: a string's or GeoJson document's text, a blob's content, an
    /// HLL's encoding. The one reader that takes more than one kind, for code that copies, hashes or
    /// compares content without caring which of them it is; `Err(WrongType)` for anything else (nil, a
    /// boolean, a number, a list, a map). Nothing is copied yet: [`Blob::bytes`] or [`Blob::to_inline`]
    /// fetches them. A list's or map's packed bytes are [`Coll::bytes`].
    pub fn bytes(&self) -> Result<Blob<'a>, Error> {
        match self {
            Value::Blob(b) | Value::Hll(b) => Ok(*b),
            Value::String(Text::Guest(s)) | Value::GeoJson(Text::Guest(s)) => Ok(Blob::Guest(s.as_bytes())),
            Value::String(Text::Stored { src, at, len }) | Value::GeoJson(Text::Stored { src, at, len }) => {
                Ok(Blob::Stored { src: *src, at: *at, len: *len })
            }
            _ => Err(Error::WrongType),
        }
    }

    /// The list, as a [`Coll`] to iterate, or `Err(WrongType)` when this is not a list.
    pub fn list(&self) -> Result<Coll, Error> {
        match self {
            Value::List(c) => Ok(*c),
            _ => Err(Error::WrongType),
        }
    }

    /// The map, as a [`Coll`] to iterate or look up, or `Err(WrongType)` when this is not a map.
    pub fn map(&self) -> Result<Coll, Error> {
        match self {
            Value::Map(c) => Ok(*c),
            _ => Err(Error::WrongType),
        }
    }

    /// A string's or GeoJson document's text, copied into a buffer you own, in one step. `out` must hold the
    /// whole text: a shorter buffer is `Err(TooSmall(n))` with the `n` bytes needed, and nothing is
    /// truncated. `Err(WrongType)` for any other kind. The `&str` borrows `out`. See [`Text::as_str`].
    pub fn as_str<'b>(&'b self, out: &'b mut [u8]) -> Result<&'b str, Error> {
        match self {
            Value::String(t) | Value::GeoJson(t) => t.as_str(out),
            _ => Err(Error::WrongType),
        }
    }

    /// Whether this is [`Value::Nil`].
    #[must_use]
    pub fn is_nil(&self) -> bool {
        matches!(self, Value::Nil)
    }

    /// Whether this is an integer.
    #[must_use]
    pub fn is_int(&self) -> bool {
        matches!(self, Value::Int(_))
    }

    /// Whether this is a double.
    #[must_use]
    pub fn is_double(&self) -> bool {
        matches!(self, Value::Double(_))
    }

    /// Whether this is a boolean.
    #[must_use]
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Whether this is a string. A GeoJson document is not one, though it reads as text.
    #[must_use]
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Whether this is a blob. An HLL is not one.
    #[must_use]
    pub fn is_blob(&self) -> bool {
        matches!(self, Value::Blob(_))
    }

    /// Whether this is a GeoJson document. A string is not one.
    #[must_use]
    pub fn is_geojson(&self) -> bool {
        matches!(self, Value::GeoJson(_))
    }

    /// Whether this is an HLL. A blob is not one.
    #[must_use]
    pub fn is_hll(&self) -> bool {
        matches!(self, Value::Hll(_))
    }

    /// Whether this is a list.
    #[must_use]
    pub fn is_list(&self) -> bool {
        matches!(self, Value::List(_))
    }

    /// Whether this is a map.
    #[must_use]
    pub fn is_map(&self) -> bool {
        matches!(self, Value::Map(_))
    }

    /// Whether this is a string, blob, GeoJson document, HLL, list or map the host holds - read from the
    /// record, an argument or a collection - as opposed to a scalar or something the guest built. Only such a
    /// value can be given to [`stored_bytes`](Value::stored_bytes) and [`cmp_stored`](Value::cmp_stored).
    #[must_use]
    pub fn is_stored(&self) -> bool {
        stored_position(self).is_some()
    }
}

impl Value<'_> {
    /// The value's bytes as the host holds them, copied into `buf`, for a value read from the record: a
    /// string, blob, GeoJson document, HLL, list or map.
    ///
    /// A list or map, and any value that is an element of one, comes out as its msgpack, header included,
    /// exactly as the server stores it. A string, blob or HLL that is a bin of its own comes out as its
    /// content alone. Either way it is the encoding a convergent merge compares when nothing else tells two
    /// candidates apart.
    ///
    /// `Err(TooSmall(n))` when `buf` is shorter than the `n` bytes needed; `Err(WrongType)` for a scalar or a
    /// value the guest built ([`is_stored`](Value::is_stored) says which in advance).
    pub fn stored_bytes<'b>(&self, buf: &'b mut [u8]) -> Result<&'b [u8], Error> {
        let (src, at) = stored_position(self).ok_or(Error::WrongType)?;
        let n = bytes_result(unsafe { raw_coll_bytes_at(src, at as i32, buf.as_mut_ptr(), buf.len() as i32) }, buf.len())?;

        Ok(&buf[..n])
    }

    /// Compare two stored values byte by byte, as `memcmp` would on the bytes
    /// [`stored_bytes`](Value::stored_bytes) gives: the first byte that differs decides, a shorter prefix
    /// comes first. Neither value is copied into the guest, so it works on values of any size. Equal content
    /// built the same way encodes the same, since the server stores lists in order and the maps a UDF builds
    /// key-ordered, so this is the last tie-break of a deterministic merge. It is an order on encodings, not
    /// on meaning: do not read "greater" as numerically or alphabetically greater.
    ///
    /// `Err(WrongType)` when either value is a scalar or guest-built, or the host cannot find the bytes of one
    /// (a value that is no longer valid): what cannot be compared is an error, never an ordering.
    pub fn cmp_stored(&self, other: &Value<'_>) -> Result<core::cmp::Ordering, Error> {
        let (a, at_a) = stored_position(self).ok_or(Error::WrongType)?;
        let (b, at_b) = stored_position(other).ok_or(Error::WrongType)?;
        let c = unsafe { raw_coll_cmp_at(a, at_a as i32, b, at_b as i32) };

        if c < -1 {
            return Err(Error::from_status(c));
        }

        Ok(c.cmp(&0))
    }
}

/// An integer as a value: [`Value::Int`].
impl From<i64> for Value<'_> {
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}

/// A double as a value: [`Value::Double`].
impl From<f64> for Value<'_> {
    fn from(value: f64) -> Self {
        Value::Double(value)
    }
}

/// A boolean as a value: [`Value::Bool`].
impl From<bool> for Value<'_> {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

/// A `&str` as a value: a [`Value::String`] that borrows the text. This is what lets `rec.set("name",
/// "alice")` and `list.push("alice")` take a string directly.
impl<'a> From<&'a str> for Value<'a> {
    fn from(value: &'a str) -> Self {
        Value::String(Text::Guest(value))
    }
}

/// A `&[u8]` as a value: a [`Value::Blob`] that borrows the bytes.
impl<'a> From<&'a [u8]> for Value<'a> {
    fn from(value: &'a [u8]) -> Self {
        Value::Blob(Blob::Guest(value))
    }
}

/// A `&str` as a [`Text::Guest`], for building a [`Value::GeoJson`] from text of the guest's.
impl<'a> From<&'a str> for Text<'a> {
    fn from(value: &'a str) -> Self {
        Text::Guest(value)
    }
}

impl Text<'_> {
    /// How many bytes the text has - bytes, not characters. Known without fetching the text, so it is the way
    /// to size a buffer or reject an over-long string before copying anything.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Text::Stored { len, .. } => *len,
            Text::Guest(s) => s.len(),
        }
    }

    /// Whether the text has no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The text, into a buffer you own. A guest string comes back as itself, nothing copied and `out`
    /// untouched; a stored one is copied into `out` in one import call, and the `&str` returned borrows
    /// `out`.
    ///
    /// `out` must hold all of it: `Err(TooSmall(n))` says to try again with `n` bytes ([`len`](Text::len)
    /// tells in advance), and nothing is truncated. `Err(Malformed)` when the stored bytes are not UTF-8.
    pub fn as_str<'b>(&'b self, out: &'b mut [u8]) -> Result<&'b str, Error> {
        match self {
            Text::Guest(s) => Ok(s),
            Text::Stored { src, at, .. } => {
                let n = fetch_bytes(*src, *at, out)?;

                core::str::from_utf8(&out[..n]).map_err(|_| Error::Malformed)
            }
        }
    }

    /// The text's bytes, without the UTF-8 check: [`as_str`](Text::as_str) for a caller that compares or
    /// copies bytes and does not need a `str`. The same buffer rules: `Err(TooSmall(n))` when `out` is
    /// shorter than the text.
    pub fn bytes<'b>(&'b self, out: &'b mut [u8]) -> Result<&'b [u8], Error> {
        match self {
            Text::Guest(s) => Ok(s.as_bytes()),
            Text::Stored { src, at, .. } => {
                let n = fetch_bytes(*src, *at, out)?;

                Ok(&out[..n])
            }
        }
    }
}

impl Blob<'_> {
    /// How many bytes the blob has, known without fetching them.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Blob::Stored { len, .. } => *len,
            Blob::Guest(b) => b.len(),
        }
    }

    /// Whether the blob has no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes, into a buffer you own. A guest blob comes back as itself, nothing copied; a stored one is
    /// copied into `out` in one import call and the slice returned borrows `out`. `Err(TooSmall(n))` when
    /// `out` is shorter than the `n` bytes the blob has; nothing is truncated.
    pub fn bytes<'b>(&'b self, out: &'b mut [u8]) -> Result<&'b [u8], Error> {
        match self {
            Blob::Guest(b) => Ok(b),
            Blob::Stored { src, at, .. } => {
                let n = fetch_bytes(*src, *at, out)?;

                Ok(&out[..n])
            }
        }
    }
}

//==========================================================
// Owned strings and bytes.
//

/// A string in a buffer of its own, `N` bytes at most. What a UDF declares when it wants text to keep - an
/// argument, a bin's value - without an allocator; derefs to `str`. It is `Copy` and lives on the guest's
/// stack, `N` bytes plus a length, so choose `N` for the data rather than generously.
///
/// As a parameter type, `N` is the longest argument the UDF accepts: a longer one is rejected before the body
/// runs, from its reported length, without a copy. `&str` is the same with a capacity of 256; with the
/// `alloc` feature, `String` takes any length.
///
/// ```ignore
/// export! {
///     fn rename(rec: &mut Record, name: Str<32>) -> Result<Value, Error> {
///         let old: Option<Str<32>> = rec.string("name")?.map(|t| t.to_inline()).transpose()?;
///
///         rec.set("name", &name)?;
///         rec.save()?;
///
///         Ok(Value::Bool(old.is_some_and(|old| old != name)))
///     }
/// }
/// ```
#[derive(Clone, Copy)]
pub struct Str<const N: usize> {
    len: usize,
    buf: [u8; N],
}

impl<const N: usize> Str<N> {
    /// The most bytes a `Str<N>` holds: `N`.
    pub const CAPACITY: usize = N;

    /// Copy a string in. `Err(TooSmall(len))` when it does not fit, `len` being the string's length in bytes;
    /// nothing is truncated.
    pub fn new(text: &str) -> Result<Self, Error> {
        if text.len() > N {
            return Err(Error::TooSmall(text.len()));
        }

        let mut s = Str { len: text.len(), buf: [0; N] };

        s.buf[..text.len()].copy_from_slice(text.as_bytes());

        Ok(s)
    }

    /// The string held: its own length, not the capacity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        // Only ever filled from a `&str`, or from bytes that passed the UTF-8 check on the way in.
        core::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

/// Derefs to `str`, so every `&str` method works on a [`Str`].
impl<const N: usize> core::ops::Deref for Str<N> {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

/// The string held, for anything generic over `AsRef<str>`.
impl<const N: usize> AsRef<str> for Str<N> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Two [`Str`]s are equal when the strings they hold are.
impl<const N: usize> PartialEq for Str<N> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

/// Compares the string held with a `str`.
impl<const N: usize> PartialEq<str> for Str<N> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

/// Compares the string held with a `&str`, so `name == "alice"` works.
impl<const N: usize> PartialEq<&str> for Str<N> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Formats as the `str` does: quoted and escaped.
impl<const N: usize> core::fmt::Debug for Str<N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(self.as_str(), f)
    }
}

/// Writes the string held, as it is.
impl<const N: usize> core::fmt::Display for Str<N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A [`Str`] by reference as a value: a [`Value::String`] that borrows its text, so `rec.set("name", &name)`
/// works.
impl<'a, const N: usize> From<&'a Str<N>> for Value<'a> {
    fn from(value: &'a Str<N>) -> Self {
        Value::String(Text::Guest(value.as_str()))
    }
}

/// Bytes in a buffer of their own, `N` at most: [`Str`] for a blob. Derefs to `[u8]`. As a parameter type it
/// takes a blob argument of up to `N` bytes and rejects a longer one before the body runs.
#[derive(Clone, Copy)]
pub struct Bytes<const N: usize> {
    len: usize,
    buf: [u8; N],
}

impl<const N: usize> Bytes<N> {
    /// The most bytes a `Bytes<N>` holds: `N`.
    pub const CAPACITY: usize = N;

    /// Copy bytes in. `Err(TooSmall(len))` when they do not fit, `len` being how many there are; nothing is
    /// truncated.
    pub fn new(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > N {
            return Err(Error::TooSmall(bytes.len()));
        }

        let mut b = Bytes { len: bytes.len(), buf: [0; N] };

        b.buf[..bytes.len()].copy_from_slice(bytes);

        Ok(b)
    }

    /// The bytes held: their own length, not the capacity.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// Derefs to `[u8]`, so every slice method works on a [`Bytes`].
impl<const N: usize> core::ops::Deref for Bytes<N> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// The bytes held, for anything generic over `AsRef<[u8]>`.
impl<const N: usize> AsRef<[u8]> for Bytes<N> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Two [`Bytes`] are equal when the bytes they hold are.
impl<const N: usize> PartialEq for Bytes<N> {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

/// Compares the bytes held with a slice.
impl<const N: usize> PartialEq<[u8]> for Bytes<N> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}

/// Formats as the byte slice does.
impl<const N: usize> core::fmt::Debug for Bytes<N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(self.as_bytes(), f)
    }
}

/// A [`Bytes`] by reference as a value: a [`Value::Blob`] that borrows its bytes.
impl<'a, const N: usize> From<&'a Bytes<N>> for Value<'a> {
    fn from(value: &'a Bytes<N>) -> Self {
        Value::Blob(Blob::Guest(value.as_bytes()))
    }
}

impl Text<'_> {
    /// The text in a [`Str`] of its own, in one import call: the way to keep a string past the buffer it
    /// would otherwise borrow. `N` usually comes from the type it is assigned to: `let name: Str<32> =
    /// text.to_inline()?`. `Err(TooSmall(len))` when the text is longer than `N`, known before anything is
    /// copied; `Err(Malformed)` when stored bytes are not UTF-8.
    pub fn to_inline<const N: usize>(self) -> Result<Str<N>, Error> {
        match self {
            Text::Guest(s) => Str::new(s),
            Text::Stored { len, .. } if len > N => Err(Error::TooSmall(len)),
            Text::Stored { src, at, .. } => {
                let mut s = Str { len: 0, buf: [0; N] };
                let n = fetch_bytes(src, at, &mut s.buf)?;

                core::str::from_utf8(&s.buf[..n]).map_err(|_| Error::Malformed)?;
                s.len = n;

                Ok(s)
            }
        }
    }

    /// The text as a `String` of any length, in one import call and one allocation. Needs the `alloc`
    /// feature. `Err(Malformed)` when stored bytes are not UTF-8.
    #[cfg(feature = "alloc")]
    pub fn to_string(self) -> Result<alloc::string::String, Error> {
        match self {
            Text::Guest(s) => Ok(alloc::string::String::from(s)),
            Text::Stored { src, at, len } => {
                let mut bytes = alloc::vec![0u8; len];
                let n = fetch_bytes(src, at, &mut bytes)?;

                bytes.truncate(n);

                alloc::string::String::from_utf8(bytes).map_err(|_| Error::Malformed)
            }
        }
    }
}

impl Blob<'_> {
    /// The bytes in a [`Bytes`] of their own, in one import call. `Err(TooSmall(len))` when there are more
    /// than `N`, known before anything is copied. See [`Text::to_inline`].
    pub fn to_inline<const N: usize>(self) -> Result<Bytes<N>, Error> {
        match self {
            Blob::Guest(b) => Bytes::new(b),
            Blob::Stored { len, .. } if len > N => Err(Error::TooSmall(len)),
            Blob::Stored { src, at, .. } => {
                let mut b = Bytes { len: 0, buf: [0; N] };

                b.len = fetch_bytes(src, at, &mut b.buf)?;

                Ok(b)
            }
        }
    }

    /// The bytes as a `Vec<u8>` of any length, in one import call and one allocation. Needs the `alloc`
    /// feature.
    #[cfg(feature = "alloc")]
    pub fn to_vec(self) -> Result<alloc::vec::Vec<u8>, Error> {
        match self {
            Blob::Guest(b) => Ok(alloc::vec::Vec::from(b)),
            Blob::Stored { src, at, len } => {
                let mut bytes = alloc::vec![0u8; len];
                let n = fetch_bytes(src, at, &mut bytes)?;

                bytes.truncate(n);

                Ok(bytes)
            }
        }
    }
}

fn fetch_bytes(src: i32, at: u32, out: &mut [u8]) -> Result<usize, Error> {
    let rc = unsafe { raw_cursor_bytes_at(src, at as i32, out.as_mut_ptr(), out.len() as i32) };

    bytes_result(rc, out.len())
}

//==========================================================
// Collections.
//

impl Coll {
    /// How many elements a list has, or entries a map has (a map of three keys is 3, though iterating it
    /// yields six values). Known at once for a stored collection; a built one is asked on the host each time,
    /// so it counts what has been added so far.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Coll::Stored { len, .. } => *len,
            Coll::Built(h) => {
                let n = unsafe { coll_size(*h) };

                if n < 0 {
                    0
                } else {
                    n as usize
                }
            }
        }
    }

    /// Whether a list has no elements, or a map no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Walk the collection: every element of a list in order, or every key and value of a map, alternately
    /// (use [`entries`](Coll::entries) to get a map's as pairs).
    ///
    /// The import calls are hidden. The iterator fetches 128 values per call into a window on the guest's
    /// stack (2 KB), so a ten-thousand-element list is 79 imports, not ten thousand. Each item is a
    /// `Result<Value, Error>`: a malformed collection surfaces as an `Err` item rather than as a quiet end,
    /// and the iterator ends after it.
    ///
    /// A nested list or map comes out as a [`Value::List`] / [`Value::Map`] that iterates the same way, and
    /// the walk continues after it.
    ///
    /// An iterator holds a cursor on the host until it is dropped, which gives it back. When the call's
    /// memory on the server has no room for one, or the collection is no longer valid (see [`Value`]), the
    /// iterator yields one `Err(Refused)` and ends. A built collection is walked through a packed copy of
    /// it, so finish building it before iterating it; the copy stays until the call ends, because the
    /// values the walk yields read from it.
    ///
    /// ```ignore
    /// let mut longest = 0;
    ///
    /// for item in tags.iter() {
    ///     longest = longest.max(item?.string()?.len());
    /// }
    /// ```
    #[must_use]
    pub fn iter(&self) -> Iter {
        let cursor = match *self {
            Coll::Stored { src, at, .. } => unsafe { raw_cursor_enter_at(src, at as i32) },
            // A built collection is walked through a packed copy, made on request.
            Coll::Built(h) => {
                let src = unsafe { raw_pack_handle(h) };

                if src <= 0 {
                    0
                } else {
                    unsafe { raw_cursor_enter_at(src, 0) }
                }
            }
        };

        Iter {
            cursor,
            window: [[0; VALUE_SZ]; WINDOW],
            at: 0,
            filled: 0,
            // A cursor that could not open yields one error and ends.
            done: false,
            failed: cursor <= 0,
        }
    }

    /// The collection's bytes as stored - its msgpack, header included - copied into `buf`.
    /// `Err(TooSmall(n))` when `buf` is shorter than the `n` bytes needed. A built collection is packed
    /// first, and `Err(Refused)` when the host cannot. See [`Value::stored_bytes`].
    pub fn bytes<'b>(&self, buf: &'b mut [u8]) -> Result<&'b [u8], Error> {
        let (src, at) = self.position()?;
        let rc = unsafe { raw_coll_bytes_at(src, at as i32, buf.as_mut_ptr(), buf.len() as i32) };

        self.done_with(src);

        let n = bytes_result(rc, buf.len())?;

        Ok(&buf[..n])
    }

    /// Compare two collections' stored bytes, as `memcmp` would, without copying either into the guest: the
    /// deterministic tie-break between two candidates that are otherwise equal. An order on encodings, not on
    /// content. See [`Value::cmp_stored`]. `Err(WrongType)` when the host cannot find the bytes of one of
    /// them, `Err(Refused)` when a built collection cannot be packed: never an ordering.
    pub fn cmp_bytes(&self, other: &Coll) -> Result<core::cmp::Ordering, Error> {
        let (a, at_a) = self.position()?;
        let (b, at_b) = match other.position() {
            Ok(pos) => pos,
            Err(e) => {
                self.done_with(a);
                return Err(e);
            }
        };
        let c = unsafe { raw_coll_cmp_at(a, at_a as i32, b, at_b as i32) };

        other.done_with(b);
        self.done_with(a);

        if c < -1 {
            return Err(Error::from_status(c));
        }

        Ok(c.cmp(&0))
    }

    /// A map's entries as `(key, value)` pairs, 64 per import call. Each item is a `Result<(Value, Value),
    /// Error>`. A stored map comes out in the order it is stored; a map built with [`map`] is ordered by key.
    ///
    /// For a map only: on a list it would pair up neighbouring elements, and end with `Err(Malformed)` when
    /// their number is odd.
    ///
    /// ```ignore
    /// for entry in rec.bin("counts")?.map()?.entries() {
    ///     let (site, count) = entry?;
    ///     let mut buf = [0u8; 32];
    ///
    ///     total += count.int()?;
    ///     log(Level::Debug, site.as_str(&mut buf)?);
    /// }
    /// ```
    #[must_use]
    pub fn entries(&self) -> Entries {
        Entries { values: self.iter() }
    }

    /// The value under a key of a map: `coll.get("name")` for a string key, `coll.get(7)` for an integer one.
    /// A string key matches only a string key with the same bytes, an integer only an integer.
    ///
    /// `Ok(None)` for no such key: not being there is not an error. A key whose value is nil is
    /// `Ok(Some(Value::Nil))`. A list has no keys, so on a list every key is `Ok(None)` too. An `Err` means
    /// the map could not be read at all.
    ///
    /// ```ignore
    /// let limit = match settings.get("limit")? {
    ///     Some(v) => v.int()?,                        // Err(WrongType) when it is not an integer
    ///     None => 100,
    /// };
    /// ```
    ///
    /// The host walks the stored entries in place, so the cost grows with the map but nothing is copied into
    /// the guest; a stored map takes one import call. A built map is read through a packed copy of it, which
    /// costs the whole map and is not promised to show entries inserted after the map was first read: to
    /// ask a map under construction whether it has a key use [`Map::contains_key`].
    pub fn get(&self, key: impl MapKey) -> Result<Option<Value<'static>>, Error> {
        key.lookup_in(self)
    }

    /// The element at `index` of a list, counting from 0, in one import call: the host steps over the
    /// elements before it where they are stored, so nothing before it is fetched.
    ///
    /// `Ok(None)` past the end: not being there is not an error. An element that is nil is
    /// `Ok(Some(Value::Nil))`. A map has no positions, so on a map every index is `Ok(None)` too. For more
    /// than a few elements, [`iter`](Coll::iter) is cheaper than indexing each: every `at` starts again from
    /// the front.
    pub fn at(&self, index: usize) -> Result<Option<Value<'static>>, Error> {
        let (src, at) = self.position()?;
        let mut raw = [0u8; VALUE_SZ];
        // The host counts elements in 32 bits, so there is nothing at an index beyond them.
        let Ok(index) = i32::try_from(index) else {
            return Ok(None);
        };
        let rc = unsafe { raw_list_get(src, at as i32, index, raw.as_mut_ptr(), VALUE_SZ as i32) };

        found(rc, &raw)
    }

    fn get_str(&self, key: &str) -> Result<Option<Value<'static>>, Error> {
        let (src, at) = self.position()?;
        let mut raw = [0u8; VALUE_SZ];
        let rc = unsafe {
            raw_coll_get(
                src,
                at as i32,
                key.as_ptr(),
                key.len() as i32,
                raw.as_mut_ptr(),
                VALUE_SZ as i32,
            )
        };

        found(rc, &raw)
    }

    fn get_int(&self, key: i64) -> Result<Option<Value<'static>>, Error> {
        let (src, at) = self.position()?;
        let mut raw = [0u8; VALUE_SZ];
        let rc = unsafe { raw_coll_get_i(src, at as i32, key, raw.as_mut_ptr(), VALUE_SZ as i32) };

        found(rc, &raw)
    }

    // Give back the packed copy `position` made of a built collection, once nothing read from it is handed
    // out (a byte copy, a comparison). A lookup keeps it: the value it returns may read from it.
    fn done_with(&self, src: i32) {
        if let Coll::Built(_) = self {
            unsafe { raw_cursor_close(src) };
        }
    }

    // Where this collection sits, packing a built one on request.
    fn position(&self) -> Result<(i32, u32), Error> {
        match *self {
            Coll::Stored { src, at, .. } => Ok((src, at)),
            Coll::Built(h) => {
                let src = unsafe { raw_pack_handle(h) };

                if src <= 0 {
                    Err(Error::Refused)
                } else {
                    Ok((src, 0))
                }
            }
        }
    }
}

// What a lookup in a collection answers: the value, nothing for "not there", an error for anything else.
fn found(rc: i32, raw: &[u8; VALUE_SZ]) -> Result<Option<Value<'static>>, Error> {
    match rc {
        OK => Ok(Some(Value::from_raw(raw))),
        NOT_FOUND => Ok(None),
        rc => Err(Error::from_status(rc)),
    }
}

/// The values of a collection, 128 per import call. Made by [`Coll::iter`]; an `Iterator` of `Result<Value,
/// Error>`. About 2 KB on the guest's stack, and a cursor on the host until it is dropped.
pub struct Iter {
    cursor: i32,
    window: [[u8; VALUE_SZ]; WINDOW],
    at: usize,
    filled: usize,
    done: bool,
    failed: bool,
}

/// Yields each value as `Ok`. A failure is yielded once, as an `Err` item, and the iterator ends after it.
impl Iterator for Iter {
    type Item = Result<Value<'static>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            self.failed = false;
            self.done = true;

            return Some(Err(Error::Refused));
        }

        if self.at == self.filled {
            if self.done {
                return None;
            }

            let rc = unsafe {
                raw_cursor_values(
                    self.cursor,
                    self.window.as_mut_ptr().cast::<u8>(),
                    (WINDOW * VALUE_SZ) as i32,
                )
            };

            if rc < 0 {
                self.done = true;

                return Some(Err(Error::from_status(rc)));
            }

            self.filled = rc as usize;
            self.done = self.filled < WINDOW;
            self.at = 0;

            if self.filled == 0 {
                return None;
            }
        }

        let value = Value::from_raw(&self.window[self.at]);

        self.at += 1;

        Some(Ok(value))
    }
}

/// Gives the iterator's cursor back to the host when the iterator is dropped.
impl Drop for Iter {
    fn drop(&mut self) {
        // Give the cursor back, in whatever order iterators end: the host reuses its slot.
        if self.cursor > 0 {
            unsafe { raw_cursor_close(self.cursor) };
        }
    }
}

/// A map's entries, key and value together. Made by [`Coll::entries`]; an `Iterator` of `Result<(Value,
/// Value), Error>`.
pub struct Entries {
    values: Iter,
}

/// Yields each entry as `Ok((key, value))`. A key with no value after it is `Err(Malformed)`; a failure of
/// the walk underneath is passed on as it is.
impl Iterator for Entries {
    type Item = Result<(Value<'static>, Value<'static>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = match self.values.next()? {
            Ok(key) => key,
            Err(e) => return Some(Err(e)),
        };

        match self.values.next() {
            Some(Ok(value)) => Some(Ok((key, value))),
            Some(Err(e)) => Some(Err(e)),
            None => Some(Err(Error::Malformed)),
        }
    }
}

//==========================================================
// The record.
//

/// What any record can be asked: the one a record UDF is called on ([`Record`]) and either side of an event
/// ([`EventRecord`]). A helper that takes `&impl ReadRecord` reads a bin the same way in both.
///
/// Two methods are the record's own: `bin(name)`, a bin whatever it holds, and the metadata `generation()`
/// and `last_update_time()`. The rest are provided from `bin`, and come in three forms:
///
/// - `bin(name)` is for a bin that must be there: `Err(Absent)` when it is not.
/// - `get(name)` and the typed readers `int`, `double`, `bool`, `string`, `blob`, `list`, `map` are for a bin
///   that may not be: `Ok(None)` when it is absent or nil, `Err(WrongType)` when it holds another kind. Nothing
///   is converted: `int` on a double bin is `Err(WrongType)`, and so are `string` on a GeoJson bin and `blob`
///   on a string bin. `bytes(name)` is the one reader that takes several kinds: the raw bytes of a string,
///   blob, GeoJson or HLL bin.
/// - `int_or`, `double_or`, `bool_or` take the default to use when the bin is absent or nil, and are still
///   `Err(WrongType)` for a bin of another kind. `has(name)` says only whether the bin is there.
///
/// A `name` is a bin name: 1 to 15 bytes. Any other name reads as an absent bin.
///
/// ```ignore
/// fn total(rec: &impl ReadRecord) -> Result<i64, Error> {
///     let balance = rec.bin("balance")?.int()?;      // must be there
///     let bonus = rec.int_or("bonus", 0)?;           // 0 when absent
///     let mut buf = [0u8; 64];
///
///     if let Some(owner) = rec.string("owner")? {    // None when absent
///         log(Level::Info, owner.as_str(&mut buf)?);
///     }
///
///     Ok(balance + bonus)
/// }
/// ```
pub trait ReadRecord {
    /// A bin, whatever it holds. `Err(Absent)` when the record has no such bin, or `name` cannot be a bin
    /// name (empty, or longer than 15 bytes).
    fn bin(&self, name: &str) -> Result<Value<'static>, Error>;

    /// The record's generation: the server's count of the writes the record has taken. Zero on a key with no
    /// record, and on both sides of an `xdr-merge` event.
    fn generation(&self) -> i64;

    /// When the record was last written, in milliseconds since the Citrusleaf epoch, 2010-01-01T00:00:00Z:
    /// Unix milliseconds minus 1 262 304 000 000. Zero on a key with no record, and on both sides of an
    /// `xdr-merge` event.
    fn last_update_time(&self) -> i64;

    /// A bin that may be absent: `Ok(None)` when it is absent or nil, `Ok(Some(value))` otherwise, whatever
    /// kind it holds.
    fn get(&self, name: &str) -> Result<Option<Value<'static>>, Error> {
        match self.bin(name) {
            Ok(Value::Nil) | Err(Error::Absent) => Ok(None),
            Ok(v) => Ok(Some(v)),
            Err(e) => Err(e),
        }
    }

    /// Whether the record has the bin. False for a name that cannot be a bin name. Costs the same one import
    /// call as reading the bin.
    fn has(&self, name: &str) -> bool {
        self.bin(name).is_ok()
    }

    /// An integer bin, or `None` when the bin is absent or nil. A bin of another kind, a double included, is
    /// `Err(WrongType)`.
    fn int(&self, name: &str) -> Result<Option<i64>, Error> {
        self.get(name)?.map(|v| v.int()).transpose()
    }

    /// An integer bin, or `default` when it is absent or nil. A bin of another kind is still
    /// `Err(WrongType)`.
    fn int_or(&self, name: &str, default: i64) -> Result<i64, Error> {
        Ok(self.int(name)?.unwrap_or(default))
    }

    /// A double bin, or `default` when it is absent or nil. A bin of another kind, an integer included, is
    /// still `Err(WrongType)`.
    fn double_or(&self, name: &str, default: f64) -> Result<f64, Error> {
        Ok(self.double(name)?.unwrap_or(default))
    }

    /// A boolean bin, or `default` when it is absent or nil. A bin of another kind is still `Err(WrongType)`.
    fn bool_or(&self, name: &str, default: bool) -> Result<bool, Error> {
        Ok(self.bool(name)?.unwrap_or(default))
    }

    /// A double bin, or `None` when the bin is absent or nil. A bin of another kind, an integer included, is
    /// `Err(WrongType)`.
    fn double(&self, name: &str) -> Result<Option<f64>, Error> {
        self.get(name)?.map(|v| v.double()).transpose()
    }

    /// A boolean bin, or `None` when the bin is absent or nil. A bin of another kind is `Err(WrongType)`.
    fn bool(&self, name: &str) -> Result<Option<bool>, Error> {
        self.get(name)?.map(|v| v.bool()).transpose()
    }

    /// A string bin's text, or `None` when the bin is absent or nil. A bin of another kind is
    /// `Err(WrongType)`, a GeoJson bin included: read that with `bin(name)?.geojson()`. The [`Text`] is
    /// fetched with [`Text::as_str`] or [`Text::to_inline`].
    fn string(&self, name: &str) -> Result<Option<Text<'static>>, Error> {
        self.get(name)?.map(|v| v.string()).transpose()
    }

    /// A blob bin's bytes, or `None` when the bin is absent or nil. Strict: a bin of another kind is
    /// `Err(WrongType)`, a string, GeoJson or HLL bin included. [`bytes`](ReadRecord::bytes) reads the raw
    /// bytes of any of those.
    fn blob(&self, name: &str) -> Result<Option<Blob<'static>>, Error> {
        self.get(name)?.map(|v| v.blob()).transpose()
    }

    /// The raw bytes of a bin that has bytes - a string, blob, GeoJson or HLL bin - or `None` when the bin
    /// is absent or nil. A bin of any other kind (a number, a boolean, a list, a map) is `Err(WrongType)`.
    /// See [`Value::bytes`].
    fn bytes(&self, name: &str) -> Result<Option<Blob<'static>>, Error> {
        self.get(name)?.map(|v| v.bytes()).transpose()
    }

    /// A list bin as a [`Coll`] to iterate or index, or `None` when the bin is absent or nil. A bin of
    /// another kind, a map included, is `Err(WrongType)`.
    fn list(&self, name: &str) -> Result<Option<Coll>, Error> {
        self.get(name)?.map(|v| v.list()).transpose()
    }

    /// A map bin as a [`Coll`] to iterate or look up, or `None` when the bin is absent or nil. A bin of
    /// another kind, a list included, is `Err(WrongType)`.
    fn map(&self, name: &str) -> Result<Option<Coll>, Error> {
        self.get(name)?.map(|v| v.map()).transpose()
    }
}

/// The record a UDF was called on.
///
/// A record UDF, and the per-record half of a stream UDF, takes it as its first parameter, before the call's
/// arguments. It is not one of them: a client does not pass it and the function's signature export does not
/// list it. A function that does not touch the record leaves the parameter out.
///
/// ```ignore
/// export! {
///     fn withdraw(rec: &mut Record, amount: i64) -> Result<Value, Error> {
///         let balance = rec.bin("balance")?.int()?;
///
///         rec.set("balance", balance - amount)?;
///         rec.save()?;
///
///         Ok(Value::Int(balance - amount))
///     }
///
///     fn balance(rec: &Record) -> Result<Value, Error> {
///         rec.bin("balance")
///     }
/// }
/// ```
///
/// `&Record` reads; `&mut Record` also writes. **A bin write is staged**: [`set`](Record::set) and
/// [`remove`](Record::remove) change the record in hand, and nothing reaches the server's record until
/// [`save`](Record::save). [`delete`](Record::delete) is the exception: it needs no save. A read after a
/// staged write sees the staged value, though [`bin_count`](Record::bin_count) does not count a staged bin
/// until it is saved.
///
/// **A call that fails writes nothing**, even what it had already saved: a `save` that succeeded, and a
/// [`set_ints`](Record::set_ints) or [`set_doubles`](Record::set_doubles), are undone when the function then
/// returns an `Err`.
///
/// A UDF can be called on a key that has no record. There [`exists`](Record::exists) is false, every bin
/// reads as absent, [`generation`](Record::generation), [`last_update_time`](Record::last_update_time),
/// [`size`](Record::size) and `bin_count` are 0, and there is no TTL; reading creates nothing. A `save` with a
/// bin staged creates the record. `set_ints`, `set_doubles` and `delete` are `Err(Refused)` and create
/// nothing, and a `save` with no bin staged (only a [`remove`](Record::remove) or a
/// [`set_ttl`](Record::set_ttl)) creates nothing and fails the call with "key not found".
///
/// Every `name` is a bin name: 1 to 15 bytes, with no zero byte. A read under any other name finds no bin
/// (`Err(Absent)`, or `None`); a write under one is `Err(Malformed)`.
///
/// The record is locked for the whole call, so nothing changes between a read and the save that follows it. A
/// stream UDF's record is the query's and read-only: [`stream!`] accepts only `&Record` there, and every
/// write, [`set_ttl`](Record::set_ttl), `save` and `delete` included, is `Err(Refused)`. An event handler has
/// no `Record` at all; it is given [`EventRecord`]s.
///
/// The reading methods are also on [`ReadRecord`], which [`EventRecord`] implements too.
pub struct Record {
    // Not constructible outside the SDK: there is exactly one record per call, and the export macros hand
    // it over. It holds nothing, because the host knows which record a call is on.
    _one_per_call: (),
}

impl Record {
    /// The record of the call in progress, for an export written without the macros, which otherwise
    /// pass it as the first parameter. Every one of these is the same record, so make one per call.
    #[must_use]
    pub fn current() -> Record {
        Record { _one_per_call: () }
    }

    //----------------------------------------------------------
    // Reading bins.
    //

    /// A bin, whatever it holds, in one import call. The form for a bin that must be there: `Err(Absent)`
    /// when the record has no such bin, when the record does not exist, or when `name` cannot be a bin name.
    /// It never returns [`Value::Nil`], since a bin cannot hold nil.
    ///
    /// A scalar comes back whole. A string, blob, list or map comes back as its length and position, and is
    /// fetched or walked when asked ([`Text::as_str`], [`Coll::iter`]); see [`Value`] for how long it stays
    /// valid. A bin staged by [`set`](Record::set) earlier in the call reads as the staged value.
    ///
    /// ```ignore
    /// let balance = rec.bin("balance")?.int()?;
    /// let tags = rec.bin("tags")?.list()?;
    /// ```
    pub fn bin(&self, name: &str) -> Result<Value<'static>, Error> {
        let mut raw = [0u8; VALUE_SZ];
        let rc = unsafe { raw_bin_value(name.as_ptr(), name.len() as i32, raw.as_mut_ptr(), VALUE_SZ as i32) };

        if rc != OK {
            return Err(Error::from_status(rc));
        }

        Ok(Value::from_raw(&raw))
    }

    /// A bin that may be absent: `Ok(None)` when the record has no such bin, `Ok(Some(value))` otherwise,
    /// whatever kind it holds. The untyped form of the typed readers below.
    pub fn get(&self, name: &str) -> Result<Option<Value<'static>>, Error> {
        ReadRecord::get(self, name)
    }

    /// Whether the record has the bin. False for a name that cannot be a bin name, and for every name when
    /// the record does not exist. Costs the same one import call as reading the bin.
    #[must_use]
    pub fn has(&self, name: &str) -> bool {
        ReadRecord::has(self, name)
    }

    /// An integer bin, or `None` when the bin is absent. A bin of another kind is `Err(WrongType)`, a double
    /// included: nothing is converted.
    ///
    /// The typed readers - `int`, `double`, `bool`, `string`, `blob`, `list`, `map` - are the form for a bin
    /// that may not be there, and the `_or` forms take the default in the same call:
    ///
    /// ```ignore
    /// let visits = rec.int_or("visits", 0)?;
    ///
    /// if let Some(limit) = rec.int("limit")? {
    ///     // ...
    /// }
    /// ```
    pub fn int(&self, name: &str) -> Result<Option<i64>, Error> {
        ReadRecord::int(self, name)
    }

    /// An integer bin, or `default` when it is absent. A bin of another kind is still `Err(WrongType)`: the
    /// default stands in for a missing bin, not for a wrong one.
    pub fn int_or(&self, name: &str, default: i64) -> Result<i64, Error> {
        ReadRecord::int_or(self, name, default)
    }

    /// A double bin, or `default` when it is absent. A bin of another kind, an integer included, is still
    /// `Err(WrongType)`.
    pub fn double_or(&self, name: &str, default: f64) -> Result<f64, Error> {
        ReadRecord::double_or(self, name, default)
    }

    /// A boolean bin, or `default` when it is absent. A bin of another kind is still `Err(WrongType)`.
    pub fn bool_or(&self, name: &str, default: bool) -> Result<bool, Error> {
        ReadRecord::bool_or(self, name, default)
    }

    /// A double bin, or `None` when the bin is absent. A bin of another kind is `Err(WrongType)`, an integer
    /// included: nothing is converted.
    pub fn double(&self, name: &str) -> Result<Option<f64>, Error> {
        ReadRecord::double(self, name)
    }

    /// A boolean bin, or `None` when the bin is absent. A bin of another kind is `Err(WrongType)`.
    pub fn bool(&self, name: &str) -> Result<Option<bool>, Error> {
        ReadRecord::bool(self, name)
    }

    /// A string bin's text, or `None` when the bin is absent. A bin of another kind is `Err(WrongType)`, a
    /// GeoJson bin included: read that with `rec.bin(name)?.geojson()`. The [`Text`] has the length; the
    /// text itself is fetched with [`Text::as_str`] (into a buffer) or [`Text::to_inline`] (into a [`Str`]).
    pub fn string(&self, name: &str) -> Result<Option<Text<'static>>, Error> {
        ReadRecord::string(self, name)
    }

    /// A blob bin's bytes, or `None` when the bin is absent. Strict: a bin of another kind is
    /// `Err(WrongType)`, a string, GeoJson or HLL bin included ([`bytes`](Record::bytes) reads the raw bytes
    /// of any of those, `rec.bin(name)?.hll()` an HLL as one). The bytes are fetched with [`Blob::bytes`] or
    /// [`Blob::to_inline`].
    pub fn blob(&self, name: &str) -> Result<Option<Blob<'static>>, Error> {
        ReadRecord::blob(self, name)
    }

    /// The raw bytes of a bin that has bytes - a string, blob, GeoJson or HLL bin - or `None` when the bin
    /// is absent. A bin of any other kind is `Err(WrongType)`. For code that copies, hashes or compares
    /// content whichever of them holds it; see [`Value::bytes`].
    pub fn bytes(&self, name: &str) -> Result<Option<Blob<'static>>, Error> {
        ReadRecord::bytes(self, name)
    }

    /// A list bin as a [`Coll`] to iterate or index, or `None` when the bin is absent. A bin of another kind,
    /// a map included, is `Err(WrongType)`.
    pub fn list(&self, name: &str) -> Result<Option<Coll>, Error> {
        ReadRecord::list(self, name)
    }

    /// A map bin as a [`Coll`] to iterate or look up by key, or `None` when the bin is absent. A bin of
    /// another kind, a list included, is `Err(WrongType)`.
    pub fn map(&self, name: &str) -> Result<Option<Coll>, Error> {
        ReadRecord::map(self, name)
    }

    /// How many bins the record has. It does not count a staged bin until it is saved: after a
    /// [`set`](Record::set) of a new bin the count is what it was, though the bin itself reads as staged.
    /// Zero on a key with no record.
    #[must_use]
    pub fn bin_count(&self) -> i64 {
        unsafe { numbins() }
    }

    //----------------------------------------------------------
    // Writing bins.
    //

    /// Write a value to a bin, replacing what the bin held. **Staged until [`save`](Record::save)**: a UDF
    /// that returns without saving has written nothing.
    ///
    /// ```ignore
    /// rec.set("visits", visits + 1)?;                 // an i64
    /// rec.set("name", "alice")?;                      // a &str
    /// rec.set("tags", list)?;                         // a built List or Map
    /// rec.set("backup", rec.bin("profile")?)?;        // a value read from the record
    /// rec.save()?;
    /// ```
    ///
    /// `value` is anything that converts to a [`Value`]: an `i64`, `f64`, `bool`, `&str`, `&[u8]`, a
    /// `&Str<N>` or `&Bytes<N>`, a [`List`] or [`Map`], or a `Value`. A value read from the record is written
    /// back from its stored bytes without the guest decoding it, which is how a sub-document is copied from
    /// one bin to another. A built [`List`] / [`Map`] is handed over whole. A string or blob of the guest's
    /// is copied during the call, so it may be any `&str` - a literal, a buffer of the UDF's, a `String` -
    /// not only a `'static` one. A `&str` is written as a string: for a GeoJson bin use
    /// [`set_geojson`](Record::set_geojson).
    ///
    /// Writing [`Value::Nil`] removes the bin, as [`remove`](Record::remove) does: a bin cannot hold nil.
    ///
    /// `Err(Malformed)` when `name` cannot be a bin name (empty, longer than 15 bytes, or with a zero byte).
    /// `Err(Refused)` when the record is read-only (a stream UDF's), the value nests collections more than 64
    /// deep, or the call's budget on the host is spent. `Err(WrongType)` for [`Value::Other`], an HLL made
    /// from guest bytes, and guest bytes written over a bin that holds an HLL; an HLL read from the record
    /// can be written back as the `Value` it came as.
    pub fn set<'a>(&mut self, name: &str, value: impl Into<Value<'a>>) -> Result<(), Error> {
        self.set_value(name, &value.into())
    }

    /// [`set`](Record::set) for a value held by reference, for a caller that has a `&Value` and wants to keep
    /// it. Staged, with the same rules and the same errors.
    pub fn set_value(&mut self, name: &str, value: &Value<'_>) -> Result<(), Error> {
        let n = name.as_ptr();
        let nl = name.len() as i32;
        let rc = unsafe {
            match value {
                Value::Nil => raw_set_nil(n, nl),
                Value::Bool(b) => raw_set_bool(n, nl, i32::from(*b)),
                Value::Int(i) => raw_set_int(n, nl, *i),
                Value::Double(d) => raw_set_double(n, nl, *d),
                Value::String(Text::Guest(s)) => raw_set_str(n, nl, s.as_ptr(), s.len() as i32),
                Value::GeoJson(Text::Guest(s)) => raw_set_geojson(n, nl, s.as_ptr(), s.len() as i32),
                Value::Blob(Blob::Guest(b)) => raw_set_blob(n, nl, b.as_ptr(), b.len() as i32),
                Value::String(Text::Stored { src, at, .. })
                | Value::GeoJson(Text::Stored { src, at, .. })
                | Value::Blob(Blob::Stored { src, at, .. })
                | Value::Hll(Blob::Stored { src, at, .. })
                | Value::List(Coll::Stored { src, at, .. })
                | Value::Map(Coll::Stored { src, at, .. }) => raw_set_at(n, nl, *src, *at as i32),
                Value::List(Coll::Built(h)) | Value::Map(Coll::Built(h)) => raw_set_handle(n, nl, *h),
                // Guest bytes cannot be an HLL: the subtype is the server's to set.
                Value::Hll(Blob::Guest(_)) | Value::Other => return Err(Error::WrongType),
            }
        };

        status(rc)
    }

    /// Write bytes to a bin as a blob. Staged, like [`set`](Record::set), which does the same for a `&[u8]`;
    /// this form says the kind at the call. The bytes are copied during the call. `Err(WrongType)` when the
    /// bin holds an HLL, which plain bytes would silently turn into a blob.
    pub fn set_blob(&mut self, name: &str, value: &[u8]) -> Result<(), Error> {
        self.set_value(name, &Value::Blob(Blob::Guest(value)))
    }

    /// Write a GeoJson document to a bin from its text. Staged, like [`set`](Record::set). A document written
    /// with `set` from a `&str` would be a string, which no geospatial index reads. The text is not checked
    /// here: the server checks the document when it writes the record.
    pub fn set_geojson(&mut self, name: &str, value: &str) -> Result<(), Error> {
        self.set_value(name, &Value::GeoJson(Text::Guest(value)))
    }

    /// Remove a bin. Staged, like [`set`](Record::set): the bin is gone from the server's record after
    /// [`save`](Record::save), and reads as absent for the rest of the call. The same as setting it to
    /// [`Value::Nil`], with the same errors. Removing a bin that is not there is not an error.
    pub fn remove(&mut self, name: &str) -> Result<(), Error> {
        status(unsafe { raw_bin_remove(name.as_ptr(), name.len() as i32) })
    }

    /// Write a slice of integers to a bin as a list, **without building a value per element**: the fast way
    /// to store a numeric array computed in the guest.
    ///
    /// The host packs the slice straight into a LIST particle and applies it as one op, which takes effect
    /// immediately rather than being staged: [`save`](Record::save) is not needed. The list replaces whatever
    /// the bin held, in the slice's order; an empty slice writes an empty list. It is still part of the call:
    /// when the function then returns an `Err`, the write is undone.
    ///
    /// `Err(Malformed)` when `name` cannot be a bin name. `Err(Refused)` when the record is read-only (a
    /// stream UDF's), when the key has no record (nothing is created: a record is made by a `save` with a bin
    /// staged), or when the server rejects the write, after which values read from the record earlier are no
    /// longer valid (see [`Value`]).
    pub fn set_ints(&mut self, name: &str, values: &[i64]) -> Result<(), Error> {
        let bytes = core::mem::size_of_val(values);

        status(unsafe {
            raw_set_list_ints(name.as_ptr(), name.len() as i32, values.as_ptr().cast::<u8>(), bytes as i32)
        })
    }

    /// The counterpart of [`set_ints`](Record::set_ints) for doubles: the slice becomes a list of doubles in
    /// the bin, applied at once and not staged, with the same errors: `Err(Refused)` on a key with no record
    /// among them. Undone, like `set_ints`, when the call then fails.
    pub fn set_doubles(&mut self, name: &str, values: &[f64]) -> Result<(), Error> {
        let bytes = core::mem::size_of_val(values);

        status(unsafe {
            raw_set_list_doubles(name.as_ptr(), name.len() as i32, values.as_ptr().cast::<u8>(), bytes as i32)
        })
    }

    //----------------------------------------------------------
    // Metadata.
    //

    /// Whether the record exists. A UDF can be called on a key that has no record yet, in which case this is
    /// false, every bin reads as absent, the metadata reads as 0 and [`save`](Record::save) with a bin staged
    /// creates the record. Reading creates nothing. Still true after [`delete`](Record::delete) for the rest
    /// of the call.
    #[must_use]
    pub fn exists(&self) -> bool {
        unsafe { raw_rec_exists() == 1 }
    }

    /// The record's generation: the server's count of the writes the record has taken, which is what a
    /// client's generation check compares. Zero on a key with no record.
    #[must_use]
    pub fn generation(&self) -> i64 {
        unsafe { gen() }
    }

    /// The record's remaining lifetime in seconds, counted from now, or `None` when it never expires. Also
    /// `None` on a key with no record, which has no TTL.
    #[must_use]
    pub fn ttl(&self) -> Option<u32> {
        match unsafe { raw_ttl() } {
            // Zero is how the server reports a record with no expiry.
            0 => None,
            value if value < 0 => None,
            value => u32::try_from(value).ok(),
        }
    }

    /// Set the record's time to live, in seconds from the write. It is the TTL of the write, so it takes
    /// effect when [`save`](Record::save) writes the record and does nothing without one.
    ///
    /// The value has a client write's meaning: `0` asks for the namespace's default TTL, `u32::MAX` (a
    /// client's `-1`) makes the record never expire, and anything else is that many seconds. The namespace's
    /// own rules on TTLs apply when the record is saved.
    ///
    /// A TTL is not a bin: on a key with no record, a `save` with only a TTL set creates nothing and fails
    /// the call with "key not found".
    ///
    /// `Err(Refused)` on a record that cannot be written: a stream UDF's.
    pub fn set_ttl(&mut self, seconds: u32) -> Result<(), Error> {
        status(unsafe { raw_set_ttl(seconds as i32) })
    }

    /// When the record was last written, in milliseconds since the Citrusleaf epoch, 2010-01-01T00:00:00Z,
    /// which is how the server stores it: Unix milliseconds minus 1 262 304 000 000. It is not on the same
    /// scale as [`now`], which counts from the Unix epoch. Zero on a key with no record.
    #[must_use]
    pub fn last_update_time(&self) -> i64 {
        unsafe { last_update() }
    }

    /// The record's size as the server stores it, in bytes. It is the stored record's, so writes staged in
    /// this call are not counted. Zero on a key with no record.
    #[must_use]
    pub fn size(&self) -> i64 {
        unsafe { record_size() }
    }

    /// The name of the set the record is in, copied into a buffer you own; the `&str` borrows `out`. A set
    /// name is at most 63 bytes, so a 64-byte buffer always fits. `Err(Absent)` when the record is in no set,
    /// `Err(TooSmall(n))` when `out` is shorter than the name.
    pub fn set_name<'b>(&self, out: &'b mut [u8]) -> Result<&'b str, Error> {
        let rc = unsafe { raw_set_name(out.as_mut_ptr(), out.len() as i32) };
        let written = bytes_result(rc, out.len())?;

        core::str::from_utf8(&out[..written]).map_err(|_| Error::Malformed)
    }

    /// The record's user key, when the record was written with the client's `send_key`: otherwise the server
    /// never stored it, and it cannot be recovered from the digest.
    ///
    /// An integer key comes back as [`Value::Int`] and `out` is not used. A string or blob key is copied into
    /// `out` and comes back borrowing it: as a [`Value::String`] when the bytes are valid UTF-8, as a
    /// [`Value::Blob`] when they are not. The key's bytes decide, not the type the client sent, so a blob key
    /// that happens to be valid UTF-8 arrives as a string.
    ///
    /// `Err(Absent)` when the record carries no key; `Err(TooSmall(n))` when `out` is shorter than the key.
    pub fn key<'b>(&self, out: &'b mut [u8]) -> Result<Value<'b>, Error> {
        match unsafe { key_type() } {
            2 => Ok(Value::Int(unsafe { key_int() })),
            4 | 5 => {
                let rc = unsafe { raw_key_bytes(out.as_mut_ptr(), out.len() as i32) };
                let written = bytes_result(rc, out.len())?;

                match core::str::from_utf8(&out[..written]) {
                    Ok(text) => Ok(Value::String(Text::Guest(text))),
                    Err(_) => Ok(Value::Blob(Blob::Guest(&out[..written]))),
                }
            }
            _ => Err(Error::Absent),
        }
    }

    /// The record's digest, copied into a buffer you own: 20 bytes, always present. What identifies a record
    /// a UDF wants to point at rather than ship - a top-k hit, say - since a client can fetch it by digest
    /// afterwards. `Err(TooSmall(20))` when `out` is shorter than 20 bytes.
    pub fn digest<'b>(&self, out: &'b mut [u8]) -> Result<&'b [u8], Error> {
        let rc = unsafe { raw_digest(out.as_mut_ptr(), out.len() as i32) };
        let written = bytes_result(rc, out.len())?;

        Ok(&out[..written])
    }

    //----------------------------------------------------------
    // Saving and deleting.
    //

    /// Write the staged bins to the server's record, creating the record when it does not exist yet.
    ///
    /// This is the one way a staged write takes effect: a UDF that returns without saving has changed
    /// nothing. A UDF that must not overwrite checks [`exists`](Record::exists) first; the record is locked
    /// for the whole call, so nothing can come between the check and the save.
    ///
    /// With nothing staged it is still a write: the server treats it as a touch, and the record's generation
    /// and last-update time move. On a key with no record, a save with a bin staged creates the record; one
    /// with no bin staged (nothing at all, or only a [`remove`](Record::remove) or a
    /// [`set_ttl`](Record::set_ttl)) makes nothing, since a record has to have a bin, and the call fails with
    /// "key not found".
    ///
    /// A save that succeeded is still part of the call: when the function then returns an `Err`, what it
    /// saved is undone, and the call has written nothing.
    ///
    /// `Err(Refused)` when the server does not take the write: the record is a stream UDF's, which is
    /// read-only, or the write breaks one of the server's own rules (a malformed GeoJson document, for one).
    /// After a refused save, a list, map or blob read from the record earlier in the call is no longer valid
    /// (see [`Value`]); the usual answer is to return the error.
    pub fn save(&mut self) -> Result<(), Error> {
        status(unsafe {
            if raw_rec_exists() == 1 {
                raw_rec_update()
            } else {
                raw_rec_create()
            }
        })
    }

    /// Delete the record. Not staged: it needs no [`save`](Record::save) after it, and whatever was staged
    /// goes with the record. The server carries it out when the call returns, so for the rest of the call
    /// [`exists`](Record::exists) answers as it did before. `Err(Refused)` when the server could not do it,
    /// when the key has no record to delete (nothing is created), or the record is a stream UDF's.
    ///
    /// ```ignore
    /// if rec.int_or("balance", 0)? == 0 {
    ///     rec.delete()?;
    /// }
    /// ```
    pub fn delete(&mut self) -> Result<(), Error> {
        status(unsafe { raw_rec_remove() })
    }
}

/// Reads the record of the call. `bin`, `generation` and `last_update_time` are [`Record`]'s own methods of
/// those names, and the typed readers come from the trait, so a helper written against `&impl ReadRecord`
/// takes a `&Record` and behaves exactly as the inherent methods do.
impl ReadRecord for Record {
    fn bin(&self, name: &str) -> Result<Value<'static>, Error> {
        Record::bin(self, name)
    }

    fn generation(&self) -> i64 {
        Record::generation(self)
    }

    fn last_update_time(&self) -> i64 {
        Record::last_update_time(self)
    }
}

fn status(rc: i32) -> Result<(), Error> {
    if rc == OK {
        Ok(())
    } else {
        Err(Error::from_status(rc))
    }
}

//==========================================================
// Building collections.
//

/// Start a list. It lives on the host; hand it to [`Record::set`], return it, or nest it in another.
///
/// `capacity` is a hint of how many elements to make room for, not a limit: the list grows as needed, and `0`
/// is fine. The [`List`] gives its handle back when it is dropped; what it was handed to keeps the list.
///
/// ```ignore
/// let recent = as_udf::list(3)?;
///
/// recent.push(42)?.push("text")?.push(rec.bin("last")?)?;
/// rec.set("recent", &recent)?;
/// rec.save()?;
/// ```
///
/// `Err(Refused)` when the host cannot make one: what a call holds on the host is bounded by its memory
/// budget, not by a count.
pub fn list(capacity: usize) -> Result<List, Error> {
    let handle = unsafe { list_new(capacity as i32) };

    if handle <= 0 {
        Err(Error::Refused)
    } else {
        Ok(List { handle })
    }
}

/// Start a map. Entries come out ordered by key, whatever order they go in: that is how it is stored,
/// returned and iterated.
///
/// `capacity` is a hint of how many entries to make room for, not a limit. The map lives on the host; hand
/// it to [`Record::set`], return it, or nest it in another. The [`Map`] gives its handle back when it is
/// dropped; what it was handed to keeps the map.
///
/// ```ignore
/// let summary = as_udf::map(2)?;
///
/// summary.insert("balance", balance)?.insert("owner", "alice")?;
///
/// Ok(summary.into())
/// ```
///
/// `Err(Refused)` when the host cannot make one: what a call holds on the host is bounded by its memory
/// budget, not by a count.
pub fn map(capacity: usize) -> Result<Map, Error> {
    let handle = unsafe { map_new(capacity as i32) };

    if handle <= 0 {
        Err(Error::Refused)
    } else {
        Ok(Map { handle })
    }
}

/// A list under construction, made by [`list`]. It lives on the host; this is its handle, and it owns it.
/// Elements are added with [`push`](List::push) and [`extend`](List::extend) through `&self`, and the list is
/// finished by handing it over: to [`Record::set`] or to another collection by reference (`rec.set("b",
/// &list)`, `outer.push(&list)`), or as the UDF's result by value (`-> Result<List, Error>`, or
/// `Ok(list.into())` into an [`Output`]). It can still be added to after it was handed over by reference, and
/// what it was handed to sees the additions, because the host shares the list rather than copying it.
///
/// **Dropping it gives the handle back**, so a loop that builds a list per iteration and hands each one over
/// by reference holds one at a time, and is bounded by the call's memory budget, not by a count. What it was
/// handed to keeps the list: the host holds its own reference. The only way to a [`Value`] of it is
/// borrowing (`&list`, [`as_value`](List::as_value)), so no value outlives the handle it names. A list
/// returned from a UDF is dropped by [`export!`] once the host has taken the result, which the host keeps by
/// its own reference: nothing a UDF writes holds a list's handle past its last use.
#[derive(Debug)]
pub struct List {
    handle: i32,
}

impl List {
    /// Append a value: a scalar, a guest string, blob or GeoJson, a built collection, or a value read from
    /// the record - which goes in by position, built by the server from its stored bytes without passing
    /// through the guest. Guest bytes are copied during the call. Returns the list, so pushes chain:
    /// `list.push(1)?.push("two")?`.
    ///
    /// A push holds nothing on the host but the element itself, whatever its kind, so a loop may push as many
    /// as the call's memory budget takes.
    ///
    /// `Err(WrongType)` for a value that cannot be an element: [`Value::Other`], an HLL made from guest
    /// bytes. `Err(Refused)` when the host does not take it: a list may not contain itself or nest more than
    /// 64 deep, what a call builds on the host is bounded, and a value read earlier may no longer be valid
    /// (see [`Value`]). There is no other error.
    pub fn push<'a>(&self, value: impl Into<Value<'a>>) -> Result<&Self, Error> {
        let raw = value.into().raw().ok_or(Error::WrongType)?;
        let rc = unsafe { list_push_values(self.handle, raw.as_ptr(), VALUE_SZ as i32) };

        Batch::taken(rc, 1).map(|()| self)
    }

    /// Append every item, 128 per import call instead of one each.
    ///
    /// An item is anything [`push`](List::push) takes, or a `Result<Value, Error>` as [`Coll::iter`] yields
    /// them, so `list.extend(stored.iter())` copies a stored list's elements by position in one call per
    /// window. The first `Err` item stops the extend with that error, a value that cannot be an element with
    /// `Err(WrongType)`, and one the host does not take with `Err(Refused)`, as in [`push`](List::push); the
    /// items before it are in.
    pub fn extend<'a, I>(&self, items: I) -> Result<&Self, Error>
    where
        I: IntoIterator,
        I::Item: Pushable<'a>,
    {
        let mut batch = Batch::new();

        for item in items {
            let value = match item.pushable() {
                Ok(value) => value,
                Err(e) => {
                    batch.flush_list(self.handle)?;
                    return Err(e);
                }
            };

            let Some(raw) = value.raw() else {
                batch.flush_list(self.handle)?;
                return Err(Error::WrongType);
            };

            if batch.add(raw) {
                batch.flush_list(self.handle)?;
            }
        }

        batch.flush_list(self.handle)?;

        Ok(self)
    }

    /// The list as a value borrowed from it, for [`Record::set`] or a push into another collection, which
    /// take their own reference to the list: the list still gives its handle back when dropped. `&list`
    /// converts the same way.
    #[must_use]
    pub fn as_value(&self) -> Value<'_> {
        Value::List(Coll::Built(self.handle))
    }
}

/// Gives the list's handle back to the host. What the list was set into or pushed into keeps it.
impl Drop for List {
    fn drop(&mut self) {
        unsafe { raw_cursor_close(self.handle) };
    }
}

/// A map under construction, made by [`map`], keyed by strings or integers. It lives on the host; this is its
/// handle, and it owns it. Entries are added with [`insert`](Map::insert) and [`extend`](Map::extend) through
/// `&self`, and the map is finished by handing it over: to [`Record::set`] or to another collection by
/// reference (`&map`), or as the UDF's result by value (`-> Result<Map, Error>`, or `Ok(map.into())` into an
/// [`Output`] or a [`Resolution`]). Whatever order entries go in, the map is ordered by key.
///
/// **Dropping it gives the handle back**, as for a [`List`]: a value of it is only ever borrowed (`&map`,
/// [`as_value`](Map::as_value)), and a map returned from a UDF is dropped once the host has taken the result.
#[derive(Debug)]
pub struct Map {
    handle: i32,
}

/// What a map is keyed by: a string (`&str`) or an integer (`i64`). [`Map::insert`], [`Map::contains_key`]
/// and [`Coll::get`] take either. Sealed: these two are the only key types.
pub trait MapKey: sealed::Sealed {
    #[doc(hidden)]
    fn insert_into(self, map: &Map, value: Value<'_>) -> Result<(), Error>;
    #[doc(hidden)]
    fn lookup_in(self, coll: &Coll) -> Result<Option<Value<'static>>, Error>;
    #[doc(hidden)]
    fn contained_in(self, map: &Map) -> bool;
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for &str {}
    impl Sealed for i64 {}
}

/// A string key: `map.insert("name", v)`, `coll.get("name")`. It matches a string key of the same bytes and
/// never an integer one. A key may be any length; nothing is cut.
impl MapKey for &str {
    fn insert_into(self, map: &Map, value: Value<'_>) -> Result<(), Error> {
        map.put(&Value::String(Text::Guest(self)), &value)
    }

    fn lookup_in(self, coll: &Coll) -> Result<Option<Value<'static>>, Error> {
        coll.get_str(self)
    }

    fn contained_in(self, map: &Map) -> bool {
        unsafe { raw_map_has(map.handle, self.as_ptr(), self.len() as i32) == 1 }
    }
}

/// An integer key: `map.insert(7, v)`, `coll.get(7)`. It matches an integer key of the same value and never a
/// string one. A literal needs to be an `i64`, which an unsuffixed one is inferred as here.
impl MapKey for i64 {
    fn insert_into(self, map: &Map, value: Value<'_>) -> Result<(), Error> {
        map.put(&Value::Int(self), &value)
    }

    fn lookup_in(self, coll: &Coll) -> Result<Option<Value<'static>>, Error> {
        coll.get_int(self)
    }

    fn contained_in(self, map: &Map) -> bool {
        unsafe { raw_map_has_i(map.handle, self) == 1 }
    }
}

impl Map {
    /// Insert a value under a key: a `&str` or an `i64`. The value is anything [`List::push`] takes, with the
    /// same errors: `Err(WrongType)` for a value that cannot be in a collection, `Err(Refused)` when the
    /// host does not take the entry, and no other. An entry already under the key is replaced. Returns the
    /// map, so inserts chain: `map.insert("a", 1)?.insert("b", 2)?`. The key's bytes, like a guest string's,
    /// are copied during the call.
    ///
    /// Like a push, an insert holds nothing on the host but the entry, under either kind of key, so a loop
    /// may insert as many as the call's memory budget takes.
    pub fn insert<'a>(&self, key: impl MapKey, value: impl Into<Value<'a>>) -> Result<&Self, Error> {
        key.insert_into(self, value.into())?;

        Ok(self)
    }

    /// Whether the map holds an entry under `key`. The map is asked where it is, on the host, in one import
    /// call, without packing it - which is what [`Coll::get`] on a built map would do. False when there is no
    /// such key.
    #[must_use]
    pub fn contains_key(&self, key: impl MapKey) -> bool {
        key.contained_in(self)
    }

    // One entry, by value: the batch import with a batch of one, which takes no host handle.
    fn put(&self, key: &Value<'_>, value: &Value<'_>) -> Result<(), Error> {
        let (Some(k), Some(v)) = (key.raw(), value.raw()) else {
            return Err(Error::WrongType);
        };
        let mut pair = [0u8; 2 * VALUE_SZ];

        pair[..VALUE_SZ].copy_from_slice(&k);
        pair[VALUE_SZ..].copy_from_slice(&v);

        Batch::taken(unsafe { map_put_entries(self.handle, pair.as_ptr(), (2 * VALUE_SZ) as i32) }, 1)
    }

    /// Insert every entry, 64 per import call instead of one each.
    ///
    /// An entry is a `(key, value)` pair of anything [`push`](List::push) takes - a string or integer key,
    /// in practice - or a `Result<(Value, Value), Error>` as [`Coll::entries`] yields them, so
    /// `map.extend(stored.entries())` copies a stored map by position. The first `Err` item stops the extend
    /// with that error, a key or value that cannot be in a collection with `Err(WrongType)`, and an entry
    /// the host does not take with `Err(Refused)`; the entries before it are in.
    pub fn extend<'a, I>(&self, entries: I) -> Result<&Self, Error>
    where
        I: IntoIterator,
        I::Item: PutEntry<'a>,
    {
        let mut batch = Batch::new();

        for entry in entries {
            let (key, value) = match entry.entry() {
                Ok(pair) => pair,
                Err(e) => {
                    batch.flush_map(self.handle)?;
                    return Err(e);
                }
            };

            let (Some(k), Some(v)) = (key.raw(), value.raw()) else {
                batch.flush_map(self.handle)?;
                return Err(Error::WrongType);
            };

            batch.add(k);

            if batch.add(v) {
                batch.flush_map(self.handle)?;
            }
        }

        batch.flush_map(self.handle)?;

        Ok(self)
    }

    /// The map as a value borrowed from it, for [`Record::set`] or a push into another collection, which
    /// take their own reference to the map: the map still gives its handle back when dropped. `&map`
    /// converts the same way.
    #[must_use]
    pub fn as_value(&self) -> Value<'_> {
        Value::Map(Coll::Built(self.handle))
    }
}

/// Gives the map's handle back to the host. What the map was set into or pushed into keeps it.
impl Drop for Map {
    fn drop(&mut self) {
        unsafe { raw_cursor_close(self.handle) };
    }
}

/// The list as a value borrowed from it, as [`List::as_value`] gives it: the list keeps its handle and gives
/// it back when dropped.
impl<'a> From<&'a List> for Value<'a> {
    fn from(value: &'a List) -> Self {
        value.as_value()
    }
}

/// The map as a value borrowed from it, as [`Map::as_value`] gives it.
impl<'a> From<&'a Map> for Value<'a> {
    fn from(value: &'a Map) -> Self {
        value.as_value()
    }
}

/// What [`List::extend`] takes per item: anything that converts to a [`Value`], or the `Result<Value, Error>`
/// a [`Coll::iter`] yields.
pub trait Pushable<'a> {
    #[doc(hidden)]
    fn pushable(self) -> Result<Value<'a>, Error>;
}

/// Lets an item of [`Coll::iter`] go straight into [`List::extend`]. An `Err` item stops the extend.
impl<'a> Pushable<'a> for Result<Value<'a>, Error> {
    fn pushable(self) -> Result<Value<'a>, Error> {
        self
    }
}

macro_rules! pushable {
    ($($t:ty),* $(,)?) => {
        $(
            impl<'a> Pushable<'a> for $t {
                fn pushable(self) -> Result<Value<'a>, Error> {
                    Ok(self.into())
                }
            }
        )*
    };
}

// By reference only for anything with bytes of the guest's: a batch keeps the address until it is flushed,
// so an owned string yielded by an iterator would be freed before the host read it. A built collection goes
// by reference only too, so that its handle is given back when it is dropped: one pushed by value would have
// nothing left to give it back.
pushable!(Value<'a>, i64, f64, bool, &'a str, &'a [u8], &'a List, &'a Map);

/// A [`Str`] by reference, pushed as a string that borrows its text.
impl<'a, const N: usize> Pushable<'a> for &'a Str<N> {
    fn pushable(self) -> Result<Value<'a>, Error> {
        Ok(self.into())
    }
}

/// A [`Bytes`] by reference, pushed as a blob that borrows its bytes.
impl<'a, const N: usize> Pushable<'a> for &'a Bytes<N> {
    fn pushable(self) -> Result<Value<'a>, Error> {
        Ok(self.into())
    }
}

/// What [`Map::extend`] takes per item: a `(key, value)` pair of [`Pushable`]s, or the
/// `Result<(Value, Value), Error>` a [`Coll::entries`] yields.
pub trait PutEntry<'a> {
    #[doc(hidden)]
    fn entry(self) -> Result<(Value<'a>, Value<'a>), Error>;
}

/// A `(key, value)` pair of anything [`List::push`] takes, as an entry for [`Map::extend`].
impl<'a, K: Pushable<'a>, V: Pushable<'a>> PutEntry<'a> for (K, V) {
    fn entry(self) -> Result<(Value<'a>, Value<'a>), Error> {
        Ok((self.0.pushable()?, self.1.pushable()?))
    }
}

/// Lets an item of [`Coll::entries`] go straight into [`Map::extend`]. An `Err` item stops the extend.
impl<'a> PutEntry<'a> for Result<(Value<'a>, Value<'a>), Error> {
    fn entry(self) -> Result<(Value<'a>, Value<'a>), Error> {
        self
    }
}

impl Value<'_> {
    // The 16-byte form the host takes in a batch: the same layout it hands out, plus the guest-only kinds
    // for bytes of the guest's (by address) and a built collection (by handle). None for a value the host
    // cannot take: a guest HLL, `Other`.
    fn raw(&self) -> Option<[u8; VALUE_SZ]> {
        fn stored(src: i32, at: u32) -> i64 {
            (i64::from(src) << 32) | i64::from(at)
        }

        let (kind, len, payload): (i32, u32, i64) = match self {
            Value::Nil => (0, 0, 0),
            Value::Bool(b) => (1, 0, i64::from(*b)),
            Value::Int(i) => (2, 0, *i),
            Value::Double(d) => (3, 0, d.to_bits() as i64),
            Value::String(Text::Stored { src, at, len }) => (4, *len as u32, stored(*src, *at)),
            Value::Blob(Blob::Stored { src, at, len }) => (5, *len as u32, stored(*src, *at)),
            Value::List(Coll::Stored { src, at, len }) => (6, *len as u32, stored(*src, *at)),
            Value::Map(Coll::Stored { src, at, len }) => (7, *len as u32, stored(*src, *at)),
            Value::GeoJson(Text::Stored { src, at, len }) => (9, *len as u32, stored(*src, *at)),
            Value::Hll(Blob::Stored { src, at, len }) => (10, *len as u32, stored(*src, *at)),
            Value::String(Text::Guest(s)) => (GUEST_STR, s.len() as u32, s.as_ptr() as usize as i64),
            Value::Blob(Blob::Guest(b)) => (GUEST_BLOB, b.len() as u32, b.as_ptr() as usize as i64),
            Value::GeoJson(Text::Guest(s)) => (GUEST_GEOJSON, s.len() as u32, s.as_ptr() as usize as i64),
            Value::List(Coll::Built(h)) | Value::Map(Coll::Built(h)) => (HANDLE, 0, i64::from(*h)),
            Value::Hll(Blob::Guest(_)) | Value::Other => return None,
        };

        let mut raw = [0u8; VALUE_SZ];

        raw[..4].copy_from_slice(&kind.to_le_bytes());
        raw[4..8].copy_from_slice(&len.to_le_bytes());
        raw[8..].copy_from_slice(&payload.to_le_bytes());

        Some(raw)
    }
}

// A window of values on their way to the host, 128 per import call.
struct Batch {
    raw: [[u8; VALUE_SZ]; WINDOW],
    n: usize,
}

impl Batch {
    fn new() -> Batch {
        Batch {
            raw: [[0; VALUE_SZ]; WINDOW],
            n: 0,
        }
    }

    // Add one value; true when the window is full and must be flushed.
    fn add(&mut self, raw: [u8; VALUE_SZ]) -> bool {
        self.raw[self.n] = raw;
        self.n += 1;

        self.n == WINDOW
    }

    fn flush_list(&mut self, handle: i32) -> Result<(), Error> {
        let n = core::mem::take(&mut self.n);

        if n == 0 {
            return Ok(());
        }

        let rc = unsafe { list_push_values(handle, self.raw.as_ptr().cast::<u8>(), (n * VALUE_SZ) as i32) };

        Batch::taken(rc, n)
    }

    fn flush_map(&mut self, handle: i32) -> Result<(), Error> {
        let n = core::mem::take(&mut self.n);

        if n == 0 {
            return Ok(());
        }

        let rc = unsafe { map_put_entries(handle, self.raw.as_ptr().cast::<u8>(), (n * VALUE_SZ) as i32) };

        Batch::taken(rc, n / 2)
    }

    // The host answers with how many it took. Fewer than sent means one of them it could not take, and a
    // status that it took none; either way the host refused, which is the one thing there is to tell a
    // caller whose value was of a kind a collection holds.
    fn taken(rc: i32, sent: usize) -> Result<(), Error> {
        if rc >= 0 && rc as usize == sent {
            Ok(())
        } else {
            Err(Error::Refused)
        }
    }
}

// Where a value read from the record sits, for the `*_at` natives; None for anything of the guest's.
fn stored_position(value: &Value<'_>) -> Option<(i32, u32)> {
    match value {
        Value::String(Text::Stored { src, at, .. })
        | Value::GeoJson(Text::Stored { src, at, .. })
        | Value::Blob(Blob::Stored { src, at, .. })
        | Value::Hll(Blob::Stored { src, at, .. })
        | Value::List(Coll::Stored { src, at, .. })
        | Value::Map(Coll::Stored { src, at, .. }) => Some((*src, *at)),
        _ => None,
    }
}

//==========================================================
// Results and failure.
//

/// Make a value the call's result. The [`export!`] macro does this with the `Ok` a UDF returns; it is public
/// for code that bypasses the macro.
///
/// The last call wins: a second result replaces the first. A guest string or blob is copied; a stored value
/// or built collection goes by position or handle. [`Value::Other`] is delivered as nil.
///
/// `Err(WrongType)` for an HLL made from guest bytes. A result the host cannot hold - one nesting collections
/// more than 64 deep, or larger than the call may build - fails the call with a message of the host's.
///
/// ```ignore
/// #[no_mangle]
/// pub extern "C" fn ping(_args: i32, _argc: i32) -> i32 {
///     match as_udf::result(&Value::from("pong")) {
///         Ok(()) => 0,
///         Err(e) => {
///             as_udf::fail(e.message());
///             1
///         }
///     }
/// }
/// ```
pub fn result(value: &Value<'_>) -> Result<(), Error> {
    let rc = unsafe {
        match value {
            Value::Nil | Value::Other => {
                raw_result_nil();
                OK
            }
            Value::Bool(b) => {
                raw_result_bool(i32::from(*b));
                OK
            }
            Value::Int(i) => {
                raw_result_int(*i);
                OK
            }
            Value::Double(d) => {
                raw_result_double(*d);
                OK
            }
            Value::String(Text::Guest(s)) => {
                raw_result_str(s.as_ptr(), s.len() as i32);
                OK
            }
            Value::GeoJson(Text::Guest(s)) => {
                raw_result_geojson(s.as_ptr(), s.len() as i32);
                OK
            }
            Value::Blob(Blob::Guest(b)) => {
                raw_result_blob(b.as_ptr(), b.len() as i32);
                OK
            }
            Value::String(Text::Stored { src, at, .. })
            | Value::GeoJson(Text::Stored { src, at, .. })
            | Value::Blob(Blob::Stored { src, at, .. })
            | Value::Hll(Blob::Stored { src, at, .. })
            | Value::List(Coll::Stored { src, at, .. })
            | Value::Map(Coll::Stored { src, at, .. }) => raw_result_at(*src, *at as i32),
            Value::List(Coll::Built(h)) | Value::Map(Coll::Built(h)) => raw_result_handle(*h),
            Value::Hll(Blob::Guest(_)) => return Err(Error::WrongType),
        }
    };

    status(rc)
}

/// What a UDF may return, as the `T` of its `Result<T, Error>` or on its own: a [`Value`], which is every kind
/// there is, or a string or blob the function built and owns - a [`Str`] or [`Bytes`], and with the `alloc`
/// feature a `String` or `Vec<u8>`. A `Value` only borrows a string, so it cannot carry one made inside the
/// function out of it; the owned types can, and the host copies the bytes before they are dropped.
///
/// ```ignore
/// export! {
///     fn greeting(name: String) -> Result<String, Error> {
///         Ok(format!("hello, {name}"))
///     }
///
///     fn version() -> String {
///         String::from("1.2")
///     }
/// }
/// ```
///
/// The [`export!`] and [`stream!`] macros use it to deliver what the function returned; `deliver` is what an
/// export written without them calls in place of [`result`].
pub trait Return {
    /// Make this the call's result, as [`result`] does for a [`Value`], or pass on the error. A function
    /// that returned `Err` delivers nothing: the error comes back for the export to [`fail`] the call with.
    fn deliver(self) -> Result<(), Error>;
}

/// Any value: what [`result`] takes.
impl Return for Value<'_> {
    fn deliver(self) -> Result<(), Error> {
        result(&self)
    }
}

/// A string the function built, of up to `N` bytes, returned as a string.
impl<const N: usize> Return for Str<N> {
    fn deliver(self) -> Result<(), Error> {
        result(&Value::from(&self))
    }
}

/// Bytes the function built, up to `N` of them, returned as a blob.
impl<const N: usize> Return for Bytes<N> {
    fn deliver(self) -> Result<(), Error> {
        result(&Value::from(&self))
    }
}

/// A string the function built, of any length, returned as a string. Needs the `alloc` feature.
#[cfg(feature = "alloc")]
impl Return for alloc::string::String {
    fn deliver(self) -> Result<(), Error> {
        result(&Value::from(self.as_str()))
    }
}

/// Bytes the function built, of any length, returned as a blob. Needs the `alloc` feature.
#[cfg(feature = "alloc")]
impl Return for alloc::vec::Vec<u8> {
    fn deliver(self) -> Result<(), Error> {
        result(&Value::from(self.as_slice()))
    }
}

/// The usual form: `Ok` delivers what it holds, `Err` is passed on and fails the call.
impl<T: Return> Return for Result<T, Error> {
    fn deliver(self) -> Result<(), Error> {
        self?.deliver()
    }
}

/// A list the function built, returned as a list. The host takes its own reference to it, and the list's
/// handle is given back as `deliver` returns, so the result holds nothing of the call's.
impl Return for List {
    fn deliver(self) -> Result<(), Error> {
        result(&self.as_value())
    }
}

/// A map the function built, returned as a map, with its handle given back as for a [`List`].
impl Return for Map {
    fn deliver(self) -> Result<(), Error> {
        result(&self.as_value())
    }
}

/// What a UDF returns when that is sometimes a collection it built and sometimes another value: a scalar, a
/// value read from the record, a list or a map. `.into()` makes one from a [`List`], a [`Map`] or anything
/// that converts to a [`Value`], so every branch reads `Ok(x.into())`.
///
/// ```ignore
/// export! {
///     fn recent(rec: &Record, n: i64) -> Result<Output, Error> {
///         if n == 0 {
///             return Ok(Value::Nil.into());
///         }
///
///         let out = as_udf::list(n as usize)?;
///
///         out.extend(rec.bin("history")?.list()?.iter().take(n as usize))?;
///
///         Ok(out.into())
///     }
/// }
/// ```
///
/// The list or map is owned, not named by a value: [`export!`] delivers it and drops it, which gives its
/// handle back once the host holds its own reference to the result. A function that only ever returns a
/// built collection declares `Result<List, Error>` or `Result<Map, Error>` instead.
pub enum Output<'a> {
    /// Any value: a scalar, guest bytes, a value read from the record.
    Value(Value<'a>),
    /// A list the function built.
    List(List),
    /// A map the function built.
    Map(Map),
}

/// The list, owned by the output, so it is dropped once delivered.
impl From<List> for Output<'_> {
    fn from(list: List) -> Self {
        Output::List(list)
    }
}

/// The map, owned by the output, so it is dropped once delivered.
impl From<Map> for Output<'_> {
    fn from(map: Map) -> Self {
        Output::Map(map)
    }
}

/// Anything that converts to a [`Value`]: `Value::Nil.into()`, `42.into()`, `rec.bin("n")?.into()`.
impl<'a, T: Into<Value<'a>>> From<T> for Output<'a> {
    fn from(value: T) -> Self {
        Output::Value(value.into())
    }
}

/// Delivers what it holds, and drops a built collection afterwards.
impl Return for Output<'_> {
    fn deliver(self) -> Result<(), Error> {
        match self {
            Output::Value(value) => value.deliver(),
            Output::List(list) => list.deliver(),
            Output::Map(map) => map.deliver(),
        }
    }
}

/// Fail the call with a message the client will see. The [`export!`] macro does this with the `Err` a UDF
/// returns, passing [`Error::message`]; call it directly only in an export written without the macro, and
/// return `1` from the export afterwards, as the macro does.
///
/// The message is copied; anything past 511 bytes is cut off. It does not end the function: code after it
/// still runs.
///
/// A call that fails writes nothing, even what it had already saved: a [`Record::save`] that succeeded, and
/// a [`Record::set_ints`] or [`Record::set_doubles`], are undone.
pub fn fail(message: &str) {
    unsafe { raw_fail(message.as_ptr(), message.len() as i32) };
}

//==========================================================
// Metadata.
//

/// The server's clock, in milliseconds since the Unix epoch. Available to record and stream UDFs.
///
/// `Err(Refused)` in an event handler, where a decision that reads the clock cannot converge across clusters;
/// a handler takes its times from the data.
///
/// It is not on the scale of [`Record::last_update_time`] and [`EventRecord::bin_last_update_time`], which
/// count milliseconds from the Citrusleaf epoch, 2010-01-01T00:00:00Z: add 1 262 304 000 000 to a last-update
/// time to compare it with this.
pub fn now() -> Result<i64, Error> {
    let t = unsafe { current_time() };

    if t < 0 {
        return Err(Error::Refused);
    }

    Ok(t)
}

/// Write a line to the server log, under the UDF context. For diagnosing a UDF, not for data: it returns
/// nothing and cannot fail.
///
/// ```ignore
/// as_udf::log(Level::Info, "withdraw: balance below the limit");
/// ```
///
/// The server meters it. A call gets 16 lines and a module 100 lines a second; lines over either limit are
/// dropped and counted, and the count is logged in their place. A line is cut at about 512 bytes, and control
/// characters in it are escaped, so a message cannot forge a log line. In an `xdr-merge` handler the line is
/// dropped: nothing is written.
pub fn log(level: Level, message: &str) {
    unsafe { raw_log(level as i32, message.as_ptr(), message.len() as i32) };
}

//==========================================================
// Arguments.
//

/// How many arguments a call may carry in all: 128. A call with more **fails** before the UDF runs, with a
/// message naming this limit and the count that came; no argument is ever dropped. It is also the size of an
/// [`Args`].
pub const MAX_ARGS: usize = 128;

/// The arguments of a call, decoded in one import call.
///
/// The [`export!`] macro turns these into the typed parameters of a UDF; a parameter of this type takes the
/// remaining arguments as they are, for a variadic function. It must be the last parameter.
///
/// ```ignore
/// export! {
///     fn add_all(numbers: Args) -> Result<Value, Error> {
///         let mut total = 0;
///
///         for n in numbers.iter() {
///             total += n.int()?;
///         }
///
///         Ok(Value::Int(total))
///     }
/// }
/// ```
///
/// Each argument is a [`Value`], valid for the call: a string or blob is fetched like one read from a bin, a
/// list or map is a [`Coll`]. `Args` is `Copy` and about 2 KB on the stack.
#[derive(Clone, Copy)]
pub struct Args {
    values: [[u8; VALUE_SZ]; MAX_ARGS],
    start: usize,
    len: usize,
}

impl Args {
    /// Decode the call's arguments from the two numbers the host passes an export: the handle of the argument
    /// list and the argument count. Used by [`export!`]; an export written without the macro calls it with
    /// its own two parameters, and fails the call when it answers `Err`. A call with no arguments gives an
    /// empty `Args`.
    ///
    /// `Err(Refused)` when the call carries more than [`MAX_ARGS`] arguments: they are not read, and none is
    /// dropped to make the rest fit. Any other `Err` is the host failing to hand the arguments over.
    pub fn from_handle(handle: i32, argc: i32) -> Result<Args, Error> {
        let mut args = Args {
            values: [[0; VALUE_SZ]; MAX_ARGS],
            start: 0,
            len: 0,
        };

        // A call with no arguments gets handle 0: nothing to fetch.
        if handle <= 0 || argc <= 0 {
            return Ok(args);
        }

        if argc as usize > MAX_ARGS {
            return Err(Error::Refused);
        }

        // One import for every argument: the host writes their values straight from the list it holds.
        let rc = unsafe {
            raw_args_values(handle, args.values.as_mut_ptr().cast::<u8>(), (MAX_ARGS * VALUE_SZ) as i32)
        };

        if rc < 0 {
            return Err(Error::from_status(rc));
        }

        args.len = (rc as usize).min(MAX_ARGS);

        Ok(args)
    }

    /// How many arguments there are (from this view's start).
    #[must_use]
    pub fn len(&self) -> usize {
        self.len - self.start
    }

    /// Whether there are no arguments (from this view's start).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The argument at `index`, counting from 0 at this view's start, or `None` past the end. An argument the
    /// client passed as nil is `Some(Value::Nil)`, not `None`.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<Value<'static>> {
        let i = self.start + index;

        if i < self.len {
            Some(Value::from_raw(&self.values[i]))
        } else {
            None
        }
    }

    /// Every argument, in order, as a [`Value`]. Unlike [`Coll::iter`] the items are not `Result`s: the
    /// arguments were already decoded, so the walk cannot fail and costs no import call.
    pub fn iter(&self) -> impl Iterator<Item = Value<'static>> + '_ {
        (0..self.len()).map(move |i| Value::from_raw(&self.values[self.start + i]))
    }

    /// The arguments from `index` on, as their own view, in which they count from 0 again. An `index` past
    /// the end gives an empty view.
    #[must_use]
    pub fn from(&self, index: usize) -> Args {
        let mut rest = *self;

        rest.start = (self.start + index).min(self.len);

        rest
    }
}

/// Why an argument could not become the parameter it was declared as. [`export!`] turns it into the
/// message the call fails with, worded the same in every SDK, with the argument counted from 1:
/// `<fn>: argument <N> (<param>) is missing`, `... is not <a kind>`, `... is <len> bytes, at most <max>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArgError {
    /// The call did not supply it.
    Missing,
    /// It is not the declared kind: what was wanted, as the message says it (`an int`, `a double`,
    /// `a bool`, `a string`, `a blob`, `a list or a map`; `a UTF-8 string` for a string whose bytes are not
    /// UTF-8, which only a `&str` or `String` parameter refuses).
    Expected(&'static str),
    /// A string or blob longer than the parameter allows: how long it is, and the most the parameter takes.
    TooLong { len: usize, max: usize },
    /// The host could not hand its bytes over.
    Unreadable,
}

/// How many bytes the buffer a `&str` or `&[u8]` parameter borrows holds. [`Str`] and [`Bytes`] declare
/// their own; `String` and `Vec<u8>` have no limit.
pub const ARG_BUF: usize = 256;

/// A type the [`export!`] macro can decode an argument into.
///
/// The constants describe the parameter for the signature export (see [`export!`]): what the type is called
/// there, the most bytes it takes when that is bounded, whether it may be absent, whether it takes the rest.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a type a UDF argument can have",
    note = "the record is not an argument: it is the first parameter, written `rec: &Record` or \
            `rec: &mut Record` exactly, and a stream UDF's `record` takes `&Record` and its `end` none"
)]
pub trait FromArg<'a>: Sized {
    /// What the signature calls this type: `int`, `double`, `bool`, `str`, `bytes`, `any`, `coll`.
    const NAME: &'static str;

    /// The most bytes a string or blob parameter takes, or 0 when unbounded or not a matter of bytes.
    const CAPACITY: usize = 0;

    /// Whether the parameter may be absent or nil.
    const OPTIONAL: bool = false;

    /// Whether this parameter consumes every remaining argument.
    const REST: bool = false;

    /// Decode argument `index`. `buf` is a buffer the macro declared for a string or blob parameter.
    fn from_arg(args: &Args, index: usize, buf: &'a mut [u8]) -> Result<Self, ArgError>;
}

/// An `int` parameter. Takes an integer argument only: a double is not converted.
impl<'a> FromArg<'a> for i64 {
    const NAME: &'static str = "int";

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::Int(i)) => Ok(i),
            Some(_) => Err(ArgError::Expected("an int")),
            None => Err(ArgError::Missing),
        }
    }
}

/// A `double` parameter. Takes a double argument only: an integer is not converted.
impl<'a> FromArg<'a> for f64 {
    const NAME: &'static str = "double";

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::Double(d)) => Ok(d),
            Some(_) => Err(ArgError::Expected("a double")),
            None => Err(ArgError::Missing),
        }
    }
}

/// A `bool` parameter. Takes a boolean argument only.
impl<'a> FromArg<'a> for bool {
    const NAME: &'static str = "bool";

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::Bool(b)) => Ok(b),
            Some(_) => Err(ArgError::Expected("a bool")),
            None => Err(ArgError::Missing),
        }
    }
}

/// A `str` parameter that borrows the buffer [`export!`] declares for it, [`ARG_BUF`] bytes. Takes a string
/// argument; a longer one is rejected as [`ArgError::TooLong`].
impl<'a> FromArg<'a> for &'a str {
    const NAME: &'static str = "str";
    const CAPACITY: usize = ARG_BUF;

    fn from_arg(args: &Args, index: usize, buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::String(Text::Stored { src, at, len })) => {
                if len > buf.len() {
                    return Err(ArgError::TooLong { len, max: buf.len() });
                }

                match fetch_bytes(src, at, buf) {
                    Ok(n) => core::str::from_utf8(&buf[..n])
                        .map_err(|_| ArgError::Expected("a UTF-8 string")),
                    Err(Error::TooSmall(n)) => Err(ArgError::TooLong { len: n, max: buf.len() }),
                    Err(_) => Err(ArgError::Unreadable),
                }
            }
            Some(_) => Err(ArgError::Expected("a string")),
            None => Err(ArgError::Missing),
        }
    }
}

/// A `bytes` parameter that borrows the buffer [`export!`] declares for it, [`ARG_BUF`] bytes. Takes a blob
/// argument, or a string for its bytes; a longer one is rejected as [`ArgError::TooLong`].
impl<'a> FromArg<'a> for &'a [u8] {
    const NAME: &'static str = "bytes";
    const CAPACITY: usize = ARG_BUF;

    fn from_arg(args: &Args, index: usize, buf: &'a mut [u8]) -> Result<Self, ArgError> {
        let (src, at, len) = match args.get(index) {
            Some(Value::Blob(Blob::Stored { src, at, len }))
            | Some(Value::String(Text::Stored { src, at, len })) => (src, at, len),
            Some(_) => return Err(ArgError::Expected("a blob")),
            None => return Err(ArgError::Missing),
        };

        if len > buf.len() {
            return Err(ArgError::TooLong { len, max: buf.len() });
        }

        match fetch_bytes(src, at, buf) {
            Ok(n) => Ok(&buf[..n]),
            Err(Error::TooSmall(n)) => Err(ArgError::TooLong { len: n, max: buf.len() }),
            Err(_) => Err(ArgError::Unreadable),
        }
    }
}

// Why a string or blob argument could not become the owned parameter: too long, not UTF-8, or unreadable.
fn owned_arg_error(e: Error, max: usize) -> ArgError {
    match e {
        Error::TooSmall(len) => ArgError::TooLong { len, max },
        Error::Malformed => ArgError::Expected("a UTF-8 string"),
        _ => ArgError::Unreadable,
    }
}

/// A `str` parameter of at most `N` bytes, copied into the [`Str`]. A longer string argument is rejected
/// as [`ArgError::TooLong`].
impl<'a, const N: usize> FromArg<'a> for Str<N> {
    const NAME: &'static str = "str";
    const CAPACITY: usize = N;

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::String(text)) => text.to_inline::<N>().map_err(|e| owned_arg_error(e, N)),
            Some(_) => Err(ArgError::Expected("a string")),
            None => Err(ArgError::Missing),
        }
    }
}

/// A `bytes` parameter of at most `N` bytes, copied into the [`Bytes`]. Takes a blob argument only; a
/// longer one is rejected as [`ArgError::TooLong`].
impl<'a, const N: usize> FromArg<'a> for Bytes<N> {
    const NAME: &'static str = "bytes";
    const CAPACITY: usize = N;

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::Blob(blob)) => blob.to_inline::<N>().map_err(|e| owned_arg_error(e, N)),
            Some(_) => Err(ArgError::Expected("a blob")),
            None => Err(ArgError::Missing),
        }
    }
}

/// A `str` parameter of any length, copied into a `String`. Needs the `alloc` feature.
#[cfg(feature = "alloc")]
impl<'a> FromArg<'a> for alloc::string::String {
    const NAME: &'static str = "str";

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::String(text)) => text.to_string().map_err(|e| owned_arg_error(e, usize::MAX)),
            Some(_) => Err(ArgError::Expected("a string")),
            None => Err(ArgError::Missing),
        }
    }
}

/// A `bytes` parameter of any length, copied into a `Vec<u8>`. Takes a blob argument only. Needs the
/// `alloc` feature.
#[cfg(feature = "alloc")]
impl<'a> FromArg<'a> for alloc::vec::Vec<u8> {
    const NAME: &'static str = "bytes";

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::Blob(blob)) => blob.to_vec().map_err(|e| owned_arg_error(e, usize::MAX)),
            Some(_) => Err(ArgError::Expected("a blob")),
            None => Err(ArgError::Missing),
        }
    }
}

/// An `any` parameter: the argument as it came, whatever its kind. Only a missing argument is an error.
impl<'a> FromArg<'a> for Value<'a> {
    const NAME: &'static str = "any";

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        args.get(index).ok_or(ArgError::Missing)
    }
}

/// A `coll` parameter. Takes a list or a map argument.
impl<'a> FromArg<'a> for Coll {
    const NAME: &'static str = "coll";

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::List(c)) | Some(Value::Map(c)) => Ok(c),
            Some(_) => Err(ArgError::Expected("a list or a map")),
            None => Err(ArgError::Missing),
        }
    }
}

/// An optional parameter: `None` when the call does not supply the argument or supplies nil, `Some` of the
/// inner type otherwise. An argument of the wrong kind is still rejected. In the signature export the
/// parameter's name carries a `?`.
impl<'a, T: FromArg<'a>> FromArg<'a> for Option<T> {
    const NAME: &'static str = T::NAME;
    const CAPACITY: usize = T::CAPACITY;
    const OPTIONAL: bool = true;

    fn from_arg(args: &Args, index: usize, buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            None | Some(Value::Nil) => Ok(None),
            Some(_) => T::from_arg(args, index, buf).map(Some),
        }
    }
}

/// The rest parameter: every argument from this position on, which may be none. Never fails.
impl<'a> FromArg<'a> for Args {
    const NAME: &'static str = "any";
    const REST: bool = true;

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        Ok(args.from(index))
    }
}

/// The most a failure message can carry: the host keeps 511 bytes of a call's failure, and anything longer
/// would be cut there.
const MESSAGE_CAP: usize = 511;

/// A fixed-size message buffer, so an error can name what went wrong without an allocator. It holds what the
/// host keeps of a failure, 511 bytes; a message longer than that ends in `...`, so a reader can see it was
/// cut. A cut never splits a character.
#[doc(hidden)]
pub struct Message {
    buf: [u8; MESSAGE_CAP],
    len: usize,
    cut: bool,
}

impl Message {
    /// An empty message.
    #[must_use]
    pub fn new() -> Message {
        Message {
            buf: [0; MESSAGE_CAP],
            len: 0,
            cut: false,
        }
    }

    /// The text written so far: 511 bytes at most, ending in `...` when more was written than fits.
    #[must_use]
    pub fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

/// An empty message, as [`Message::new`] gives one.
impl Default for Message {
    fn default() -> Self {
        Message::new()
    }
}

/// Appends text, so `write!` works on a message. Never fails: what does not fit is replaced by `...`.
impl core::fmt::Write for Message {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        if self.cut {
            return Ok(());
        }

        if s.len() <= self.buf.len() - self.len {
            self.buf[self.len..self.len + s.len()].copy_from_slice(s.as_bytes());
            self.len += s.len();

            return Ok(());
        }

        // Room for what fits, less three bytes for the mark, at a character boundary of `s`.
        let mut n = (self.buf.len() - self.len).saturating_sub(3);

        while n > 0 && !s.is_char_boundary(n) {
            n -= 1;
        }

        // When even the mark does not fit after what is there, it takes the last bytes, at a boundary.
        if self.len + n + 3 > self.buf.len() {
            let mut back = self.buf.len() - 3;

            while back > 0 && (self.buf[back] & 0xC0) == 0x80 {
                back -= 1;
            }

            self.len = back;
            n = 0;
        }

        self.buf[self.len..self.len + n].copy_from_slice(&s.as_bytes()[..n]);
        self.len += n;
        self.buf[self.len..self.len + 3].copy_from_slice(b"...");
        self.len += 3;
        self.cut = true;

        Ok(())
    }
}

/// One parameter as the signature export describes it. Used by [`export!`].
#[doc(hidden)]
pub struct Param {
    pub name: &'static str,
    pub ty: &'static str,
    pub capacity: usize,
    pub optional: bool,
    pub rest: bool,
}

/// The description of a parameter of type `T`. Used by [`export!`].
#[doc(hidden)]
#[must_use]
pub const fn param<'a, T: FromArg<'a>>(name: &'static str) -> Param {
    Param {
        name,
        ty: T::NAME,
        capacity: T::CAPACITY,
        optional: T::OPTIONAL,
        rest: T::REST,
    }
}

/// The text behind a `__udf_sig_<name>` export: written on the first call into a buffer of the module's own,
/// NUL-terminated, and handed to the host by address. Used by [`export!`] and [`stream!`].
///
/// 8 KiB: room for the 128 parameters a call can have, each with a long name and a capacity. A text that
/// still does not fit is not served cut, which would describe a function other than the one exported: the
/// export answers an empty signature, which the server reads as "this function declares nothing", and checks
/// nothing against it.
#[doc(hidden)]
pub struct Signature {
    buf: [u8; SIGNATURE_CAP],
    len: usize,
    overflow: bool,
}

const SIGNATURE_CAP: usize = 8192;

impl Signature {
    /// An empty signature, `const` so that it can initialise a `static`. The text is written by the first
    /// call that asks for it.
    #[must_use]
    pub const fn new() -> Signature {
        Signature {
            buf: [0; SIGNATURE_CAP],
            len: 0,
            overflow: false,
        }
    }

    /// `fn name(a: int, b: str[256], c?: bool, rest: any...) -> value`
    pub fn function(&mut self, name: &str, params: &[Param]) -> i32 {
        if self.len == 0 {
            let _ = write!(self, "fn {name}");
            self.params(params);
            let _ = self.write_str(" -> value");
        }

        self.address()
    }

    /// `stream name { record(bin: str[256]) end(bin: str[256]) }`
    pub fn stream(&mut self, name: &str, record: &[Param], end: &[Param]) -> i32 {
        if self.len == 0 {
            let _ = write!(self, "stream {name} {{ record");
            self.params(record);
            let _ = self.write_str(" end");
            self.params(end);
            let _ = self.write_str(" }");
        }

        self.address()
    }

    /// Serve `text` as the signature, whatever it says.
    pub fn literal(&mut self, text: &str) -> i32 {
        if self.len == 0 {
            let _ = write!(self, "{text}");
        }

        self.address()
    }

    fn params(&mut self, params: &[Param]) {
        let _ = self.write_str("(");

        for (i, p) in params.iter().enumerate() {
            let _ = write!(
                self,
                "{}{}{}: {}",
                if i == 0 { "" } else { ", " },
                p.name,
                if p.optional { "?" } else { "" },
                p.ty
            );

            if p.capacity != 0 {
                let _ = write!(self, "[{}]", p.capacity);
            }

            if p.rest {
                let _ = self.write_str("...");
            }
        }

        let _ = self.write_str(")");
    }

    // NUL-terminated, so the host reads it as a C string; the buffer keeps one byte of room for it.
    fn address(&mut self) -> i32 {
        let end = if self.overflow { 0 } else { self.len.min(self.buf.len() - 1) };

        self.buf[end] = 0;

        self.buf.as_ptr() as usize as i32
    }
}

/// An empty signature, as [`Signature::new`] gives one.
impl Default for Signature {
    fn default() -> Self {
        Signature::new()
    }
}

/// Appends text to the signature. Never fails: what does not fit in the 511 bytes the buffer has for
/// text is dropped.
impl core::fmt::Write for Signature {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let room = self.buf.len() - 1 - self.len;

        if self.overflow || s.len() > room {
            self.overflow = true;

            return Err(core::fmt::Error);
        }

        self.buf[self.len..self.len + s.len()].copy_from_slice(s.as_bytes());
        self.len += s.len();

        Ok(())
    }
}

/// Reject a call over one of its arguments, with a message naming the function, the position, the parameter
/// and what was wrong. `index` counts from 0, as [`Args::get`] does; the message counts from 1, as every
/// SDK's does. Used by [`export!`].
#[doc(hidden)]
pub fn reject_argument(function: &str, index: usize, parameter: &str, error: ArgError) -> i32 {
    let mut m = Message::new();
    let n = index + 1;

    let _ = match error {
        ArgError::Missing => write!(m, "{function}: argument {n} ({parameter}) is missing"),
        ArgError::Expected(wanted) => write!(m, "{function}: argument {n} ({parameter}) is not {wanted}"),
        ArgError::TooLong { len, max } => write!(m, "{function}: argument {n} ({parameter}) is {len} bytes, at most {max}"),
        ArgError::Unreadable => write!(m, "{function}: argument {n} ({parameter}) could not be read"),
    };

    fail(m.as_str());

    1
}

/// The arguments of a call, or the call failed: with more than [`MAX_ARGS`] arguments, by a message naming
/// the function, the limit and the count. Used by [`export!`] and [`stream!`].
#[doc(hidden)]
pub fn decode_args(function: &str, handle: i32, argc: i32) -> Result<Args, i32> {
    match Args::from_handle(handle, argc) {
        Ok(args) => Ok(args),
        Err(e) => {
            let mut m = Message::new();

            if argc > 0 && argc as usize > MAX_ARGS {
                let _ = write!(m, "{function} takes at most {MAX_ARGS} arguments, got {argc}");
            } else {
                let _ = write!(m, "{function}: the arguments could not be read: {}", e.message());
            }

            fail(m.as_str());

            Err(1)
        }
    }
}

/// Reject a call with more arguments than the function declares. Used by [`export!`].
#[doc(hidden)]
pub fn reject_arity(function: &str, declared: usize, got: i32) -> i32 {
    let mut m = Message::new();

    let _ = write!(m, "{function} takes at most {declared} arguments, got {got}");

    fail(m.as_str());

    1
}

//==========================================================
// Events.
//
// A UDF bound to a point in the server's own processing. There is one such point, `xdr-merge`: the
// handler is handed the record this node holds and the one XDR delivered, and gives back the final
// record or a verdict. What the server says about the merge is said of one record or the other, so it
// is read from them. See `event!`.
//

/// Reads one side of an event. `bin` is [`EventRecord::bin`], and the typed readers come from the trait, so a
/// helper written against `&impl ReadRecord` works on either side of a merge as it does on a [`Record`].
/// `generation` and `last_update_time` are zero on both sides of an `xdr-merge`.
impl ReadRecord for EventRecord {
    fn bin(&self, name: &str) -> Result<Value<'static>, Error> {
        EventRecord::bin(self, name)
    }

    fn generation(&self) -> i64 {
        self.generation
    }

    fn last_update_time(&self) -> i64 {
        self.last_update_time
    }
}

/// One side of an event: a read-only record with its metadata. An `xdr-merge` handler is given two, the
/// record this cluster holds (`current`) and the write that arrived over XDR (`incoming`); see [`event!`].
///
/// It reads like a [`Record`] - `bin`, `get`, `has` and the typed readers, with the same answers - and has
/// nothing that writes: a handler changes the record by what it returns ([`Resolution`]). Besides the bins it
/// answers what the server knows about its side of the merge: [`is_delete`](EventRecord::is_delete) and
/// [`is_partial`](EventRecord::is_partial) of the incoming write, [`is_tombstone`](EventRecord::is_tombstone)
/// of the current record, and per bin when and by which DC it was written.
///
/// It is `Copy`, and valid until the handler returns. Its bins are read through the event's own import, never
/// the ordinary record natives, which in an event answer "nothing": a `Record` has no place in a handler.
#[derive(Clone, Copy)]
pub struct EventRecord {
    which: i32,
    generation: i64,
    last_update_time: i64,
    ttl: i64,
    source_id: i64,
    bin_names: Coll,
    deleted_bins: Coll,
    bin_last_update_times: Option<Coll>,
    bin_source_ids: Option<Coll>,
    delete: bool,
    partial: bool,
    tombstone: bool,
    bin_convergence: bool,
    lost: Coll,
}

impl EventRecord {
    /// A bin of this side, as [`Record::bin`] reads one, in one import call: `Err(Absent)` when this side has
    /// no such bin, or `name` cannot be a bin name (1 to 15 bytes). A bin this side deleted is absent too;
    /// [`deleted_bins`](EventRecord::deleted_bins) tells the two apart. An incoming record delete has no
    /// bins.
    pub fn bin(&self, name: &str) -> Result<Value<'static>, Error> {
        let mut raw = [0u8; VALUE_SZ];
        let rc = unsafe {
            raw_event_bin_value(self.which, name.as_ptr(), name.len() as i32, raw.as_mut_ptr(), VALUE_SZ as i32)
        };

        if rc != OK {
            return Err(Error::from_status(rc));
        }

        Ok(Value::from_raw(&raw))
    }

    /// A bin that may be absent: `Ok(None)` when this side has no such bin, `Ok(Some(value))` otherwise,
    /// whatever kind it holds.
    pub fn get(&self, name: &str) -> Result<Option<Value<'static>>, Error> {
        ReadRecord::get(self, name)
    }

    /// Whether this side has the bin. False for a bin it deleted, and for a name that cannot be a bin name.
    #[must_use]
    pub fn has(&self, name: &str) -> bool {
        ReadRecord::has(self, name)
    }

    /// An integer bin, or `None` when this side does not have the bin. A bin of another kind is
    /// `Err(WrongType)`, a double included: nothing is converted.
    pub fn int(&self, name: &str) -> Result<Option<i64>, Error> {
        ReadRecord::int(self, name)
    }

    /// An integer bin, or `default` when this side does not have it. A bin of another kind is still
    /// `Err(WrongType)`.
    pub fn int_or(&self, name: &str, default: i64) -> Result<i64, Error> {
        ReadRecord::int_or(self, name, default)
    }

    /// A double bin, or `default` when this side does not have it. A bin of another kind, an integer
    /// included, is still `Err(WrongType)`.
    pub fn double_or(&self, name: &str, default: f64) -> Result<f64, Error> {
        ReadRecord::double_or(self, name, default)
    }

    /// A boolean bin, or `default` when this side does not have it. A bin of another kind is still
    /// `Err(WrongType)`.
    pub fn bool_or(&self, name: &str, default: bool) -> Result<bool, Error> {
        ReadRecord::bool_or(self, name, default)
    }

    /// A double bin, or `None` when this side does not have the bin. A bin of another kind is
    /// `Err(WrongType)`, an integer included.
    pub fn double(&self, name: &str) -> Result<Option<f64>, Error> {
        ReadRecord::double(self, name)
    }

    /// A boolean bin, or `None` when this side does not have the bin. A bin of another kind is
    /// `Err(WrongType)`.
    pub fn bool(&self, name: &str) -> Result<Option<bool>, Error> {
        ReadRecord::bool(self, name)
    }

    /// A string bin's text, or `None` when this side does not have the bin. A bin of another kind is
    /// `Err(WrongType)`, a GeoJson bin included: read that with `bin(name)?.geojson()`. Fetch the text with
    /// [`Text::as_str`] or [`Text::to_inline`].
    pub fn string(&self, name: &str) -> Result<Option<Text<'static>>, Error> {
        ReadRecord::string(self, name)
    }

    /// A blob bin's bytes, or `None` when this side does not have the bin. Strict: a bin of another kind is
    /// `Err(WrongType)`, a string, GeoJson or HLL bin included; [`bytes`](EventRecord::bytes) reads the raw
    /// bytes of any of those.
    pub fn blob(&self, name: &str) -> Result<Option<Blob<'static>>, Error> {
        ReadRecord::blob(self, name)
    }

    /// The raw bytes of a bin that has bytes - a string, blob, GeoJson or HLL bin - or `None` when this side
    /// does not have the bin. A bin of any other kind is `Err(WrongType)`. See [`Value::bytes`].
    pub fn bytes(&self, name: &str) -> Result<Option<Blob<'static>>, Error> {
        ReadRecord::bytes(self, name)
    }

    /// A list bin as a [`Coll`], or `None` when this side does not have the bin. A bin of another kind, a map
    /// included, is `Err(WrongType)`.
    pub fn list(&self, name: &str) -> Result<Option<Coll>, Error> {
        ReadRecord::list(self, name)
    }

    /// A map bin as a [`Coll`], or `None` when this side does not have the bin. A bin of another kind, a list
    /// included, is `Err(WrongType)`.
    pub fn map(&self, name: &str) -> Result<Option<Coll>, Error> {
        ReadRecord::map(self, name)
    }

    /// The names of the bins this side has, as a stored list of strings to iterate. Each is at most 15 bytes,
    /// so a 16-byte buffer reads any of them. Empty for an incoming record delete and for a tombstone. Bins
    /// this side deleted are not in it; see [`deleted_bins`](EventRecord::deleted_bins).
    ///
    /// ```ignore
    /// let mut buf = [0u8; 16];
    ///
    /// for name in incoming.bin_names().iter() {
    ///     let name = name?;
    ///     let name = name.as_str(&mut buf)?;
    ///     // ...
    /// }
    /// ```
    #[must_use]
    pub fn bin_names(&self) -> Coll {
        self.bin_names
    }

    /// The record's generation. Zero on both sides of an `xdr-merge`: each cluster sets its own, and a merge
    /// that read it would answer differently on each. Here only because [`ReadRecord`] has it.
    #[must_use]
    pub fn generation(&self) -> i64 {
        self.generation
    }

    /// When the record was last written. Zero on both sides of an `xdr-merge`, for the same reason as
    /// [`generation`](EventRecord::generation). The time a merge may use is per bin:
    /// [`bin_last_update_time`](EventRecord::bin_last_update_time).
    #[must_use]
    pub fn last_update_time(&self) -> i64 {
        self.last_update_time
    }

    /// This side's time to live, in seconds; 0 for a record that does not expire.
    #[must_use]
    pub fn ttl(&self) -> i64 {
        self.ttl
    }

    /// The XDR source id of the DC that wrote this version (`src-id` in that cluster's XDR configuration). It
    /// is the tie-break between two sides written at the same time; see [`Both::incoming_is_newer`].
    #[must_use]
    pub fn source_id(&self) -> i64 {
        self.source_id
    }

    /// The names of the bins this side deleted, as a stored list of strings: for the incoming write, its
    /// bin-delete ops; for the current record, its bin tombstones. Empty when there are none. A deleted bin
    /// reads as absent, so this is the only way to tell "never had it" from "removed it", and
    /// [`bin_last_update_time`](EventRecord::bin_last_update_time) says when.
    #[must_use]
    pub fn deleted_bins(&self) -> Coll {
        self.deleted_bins
    }

    /// Whether this side deleted any bin: whether [`deleted_bins`](EventRecord::deleted_bins) has anything in
    /// it.
    #[must_use]
    pub fn deleted_any(&self) -> bool {
        !self.deleted_bins.is_empty()
    }

    /// When this side last wrote the bin, where the server keeps a time per bin: under bin convergence
    /// ([`uses_bin_convergence`](EventRecord::uses_bin_convergence)). It is in milliseconds since the
    /// Citrusleaf epoch, 2010-01-01T00:00:00Z: Unix milliseconds minus 1 262 304 000 000. It covers a bin this
    /// side deleted as well. This is what decides a bin under the `bin-lut` policy, so a merge that orders the
    /// sides per bin agrees with the policy on the bins it does not merge.
    ///
    /// Where the server keeps no time for the bin, the answer is the record's
    /// [`last_update_time`](EventRecord::last_update_time), which in an `xdr-merge` is 0 on both sides: the
    /// two sides then tie on time, and [`Both::incoming_is_newer`] falls to the source id.
    #[must_use]
    pub fn bin_last_update_time(&self, name: &str) -> i64 {
        self.bin_last_update_times
            .and_then(|m| m.get(name).ok().flatten())
            .and_then(|v| v.int().ok())
            .unwrap_or(self.last_update_time)
    }

    /// The source id of the DC that wrote the bin, where the server keeps one per bin; this side's
    /// [`source_id`](EventRecord::source_id) otherwise.
    #[must_use]
    pub fn bin_source_id(&self, name: &str) -> i64 {
        self.bin_source_ids
            .and_then(|m| m.get(name).ok().flatten())
            .and_then(|v| v.int().ok())
            .unwrap_or(self.source_id)
    }

    /// Whether this is the incoming write and it is a record delete, in which case it has no bins. Always
    /// false of the current record. A handler that does not ask sees only an incoming record with no bins.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        self.delete
    }

    /// Whether this is the incoming write and it carries only the bins that changed (`bin-policy`), so a
    /// bin it lacks may well exist on the other DC. Always false of the current record.
    #[must_use]
    pub fn is_partial(&self) -> bool {
        self.partial
    }

    /// The names of the incoming bins that would lose under bin convergence, as a stored list of strings: the
    /// bins the policy would have kept the current value of. Empty for the current record, whose bins are not
    /// the ones being applied, and when no incoming bin would lose.
    #[must_use]
    pub fn lost_bins(&self) -> Coll {
        self.lost
    }

    /// Whether this is the current record and it is a tombstone: it exists, with no bins. Always false
    /// of the incoming write; see [`is_delete`](EventRecord::is_delete) for that side.
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        self.tombstone
    }

    /// Whether the policy the handler stands in for is bin convergence (`bin-lut`) rather than a plain
    /// apply of the incoming bins (`replace`). Only under bin convergence does the server keep a time
    /// per bin, so a time-based merge asks this before trusting
    /// [`bin_last_update_time`](EventRecord::bin_last_update_time). It is a fact about the merge, and both
    /// records give the same answer.
    #[must_use]
    pub fn uses_bin_convergence(&self) -> bool {
        self.bin_convergence
    }

    /// A new record built from this one's bins with `overrides` on top: every bin of this record goes in
    /// unless the map already has it. The bins travel by position, never through the guest. `overrides` is
    /// the map returned, filled in; it is what goes into [`Resolution::Record`].
    ///
    /// This prefers one side, so a handler that uses it on `current` is not symmetric: the two DCs would
    /// resolve the same pair to different records. [`Pair::with_newer`] is the symmetric form; use this one
    /// when the side is chosen by something both DCs agree on, such as the source id.
    ///
    /// ```ignore
    /// let winner = if incoming.source_id() > current.source_id() { incoming } else { current };
    /// let out = as_udf::map(1)?;
    ///
    /// out.insert("merged", true)?;
    ///
    /// Ok(Resolution::Record { bins: winner.with_bins(out)?, ttl: None })
    /// ```
    ///
    /// Only bins this record has go in. A bin it deleted is not one of them (see
    /// [`deleted_bins`](EventRecord::deleted_bins)), so nothing deleted comes back.
    pub fn with_bins(&self, overrides: Map) -> Result<Map, Error> {
        let mut buf = [0u8; 32];

        for name in self.bin_names.iter() {
            let name = name?;
            let name = name.as_str(&mut buf)?;

            if overrides.contains_key(name) {
                continue;
            }

            overrides.insert(name, self.bin(name)?)?;
        }

        Ok(overrides)
    }

    // Whether this side deleted the bin: whether `deleted_bins` names it. A name of any length is compared
    // whole, by length first, so none is cut to fit the buffer a bin name needs.
    fn deleted(&self, name: &str) -> Result<bool, Error> {
        let mut buf = [0u8; 16];

        if name.len() > buf.len() {
            return Ok(false);
        }

        for deleted in self.deleted_bins.iter() {
            if let Value::String(text) = deleted? {
                if text.len() == name.len() && text.bytes(&mut buf)? == name.as_bytes() {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

/// The two sides of a merge, read bin by bin into a [`Both`]. Made by [`Pair::of`].
///
/// A handler that merges some bins itself and wants the rest as the policy would have left them reads the
/// bins it knows through the pair and finishes with [`with_newer`](Pair::with_newer):
///
/// ```ignore
/// let pair = Pair::of(current, incoming);
/// let out = as_udf::map(2)?;
///
/// if let Some(visits) = pair.int("visits")?.max() {      // a counter that only grows
///     out.insert("visits", visits)?;
/// }
///
/// if let Some(total) = pair.int("total")?.sum() {       // each DC's own share, added
///     out.insert("total", total)?;
/// }
///
/// Ok(Resolution::Record { bins: pair.with_newer(out)?, ttl: None })
/// ```
///
/// Each reader is the [`EventRecord`] reader of the same name asked of both sides. `Pair` is `Copy`.
#[derive(Clone, Copy)]
pub struct Pair {
    current: EventRecord,
    incoming: EventRecord,
}

impl Pair {
    /// The current record and the incoming write side by side, for bin-wise merging. Each record knows
    /// which of the two it is, so the pair comes out the same if they are passed the other way round.
    #[must_use]
    pub fn of(current: EventRecord, incoming: EventRecord) -> Pair {
        if current.which <= incoming.which {
            Pair { current, incoming }
        } else {
            Pair { current: incoming, incoming: current }
        }
    }

    /// An integer bin on each side; a side that does not have the bin is `None`. `Err(WrongType)` when either
    /// side holds another kind, which fails the handler if passed on with `?`.
    pub fn int(&self, name: &str) -> Result<Both<i64>, Error> {
        Ok(self.both(name, self.current.int(name)?, self.incoming.int(name)?))
    }

    /// A double bin on each side; a side that does not have the bin is `None`. `Err(WrongType)` when either
    /// side holds another kind, an integer included.
    pub fn double(&self, name: &str) -> Result<Both<f64>, Error> {
        Ok(self.both(name, self.current.double(name)?, self.incoming.double(name)?))
    }

    /// A boolean bin on each side; a side that does not have the bin is `None`. `Err(WrongType)` when either
    /// side holds another kind.
    pub fn bool(&self, name: &str) -> Result<Both<bool>, Error> {
        Ok(self.both(name, self.current.bool(name)?, self.incoming.bool(name)?))
    }

    /// A string bin's text on each side; a side that does not have the bin is `None`. `Err(WrongType)` when
    /// either side holds another kind. A [`Text`] has no order of its own, so `max` and `min` are not offered
    /// on it: fetch the two texts and compare them, or take [`Both::newer`].
    pub fn string(&self, name: &str) -> Result<Both<Text<'static>>, Error> {
        Ok(self.both(name, self.current.string(name)?, self.incoming.string(name)?))
    }

    /// A blob bin's bytes on each side; a side that does not have the bin is `None`. `Err(WrongType)` when
    /// either side holds another kind.
    pub fn blob(&self, name: &str) -> Result<Both<Blob<'static>>, Error> {
        Ok(self.both(name, self.current.blob(name)?, self.incoming.blob(name)?))
    }

    /// A list bin on each side; a side that does not have the bin is `None`. `Err(WrongType)` when either
    /// side holds another kind.
    pub fn list(&self, name: &str) -> Result<Both<Coll>, Error> {
        Ok(self.both(name, self.current.list(name)?, self.incoming.list(name)?))
    }

    /// A map bin on each side; a side that does not have the bin is `None`. `Err(WrongType)` when either side
    /// holds another kind.
    pub fn map(&self, name: &str) -> Result<Both<Coll>, Error> {
        Ok(self.both(name, self.current.map(name)?, self.incoming.map(name)?))
    }

    /// The bin as a value on each side, whatever kind it holds; a side that does not have the bin is `None`.
    /// The form for a bin the handler passes on without looking inside: `pair.bin("profile")?.newer()`.
    pub fn bin(&self, name: &str) -> Result<Both<Value<'static>>, Error> {
        let side = |r: &EventRecord| match r.bin(name) {
            Ok(Value::Nil) | Err(Error::Absent) => Ok(None),
            Ok(v) => Ok(Some(v)),
            Err(e) => Err(e),
        };

        Ok(self.both(name, side(&self.current)?, side(&self.incoming)?))
    }

    /// Every bin either side has that `overrides` lacks, from the side that wrote it later - which is what
    /// the policy would have kept - so a handler merges the bins it knows about and leaves the rest as they
    /// would have been. Symmetric: both DCs order the two sides the same way.
    ///
    /// `overrides` is the map returned, filled in, and is what goes into [`Resolution::Record`]. A bin
    /// already in it is left as the handler set it. A bin that exists on one side only is taken from that
    /// side, whichever wrote later - **unless the side that wrote later deleted it**: a bin named in the
    /// newer side's [`deleted_bins`](EventRecord::deleted_bins) is left out of the map, which deletes it, so
    /// the older side's value does not bring a deleted bin back.
    pub fn with_newer(&self, overrides: Map) -> Result<Map, Error> {
        let mut buf = [0u8; 32];

        for side in [&self.current, &self.incoming] {
            for name in side.bin_names().iter() {
                let name = name?;
                let name = name.as_str(&mut buf)?;

                if overrides.contains_key(name) {
                    continue;
                }

                let newer = if self.incoming_is_newer(name) { &self.incoming } else { &self.current };

                // The later write to this bin was its removal: the bin stays removed.
                if newer.deleted(name)? {
                    continue;
                }

                if let Some(value) = self.bin(name)?.newer() {
                    overrides.insert(name, value)?;
                }
            }
        }

        Ok(overrides)
    }

    /// Whether the incoming side wrote the bin `name` later than the current side, per bin: the order the
    /// `bin-lut` policy uses. The later [`bin_last_update_time`](EventRecord::bin_last_update_time) wins, and
    /// on equal times the greater [`bin_source_id`](EventRecord::bin_source_id); see
    /// [`Both::incoming_is_newer`].
    #[must_use]
    pub fn incoming_is_newer(&self, name: &str) -> bool {
        let current = self.current.bin_last_update_time(name);
        let incoming = self.incoming.bin_last_update_time(name);

        incoming > current
            || (incoming == current && self.incoming.bin_source_id(name) > self.current.bin_source_id(name))
    }

    fn both<T>(&self, name: &str, current: Option<T>, incoming: Option<T>) -> Both<T> {
        Both {
            current,
            incoming,
            current_last_update_time: self.current.bin_last_update_time(name),
            incoming_last_update_time: self.incoming.bin_last_update_time(name),
            current_source_id: self.current.bin_source_id(name),
            incoming_source_id: self.incoming.bin_source_id(name),
        }
    }
}

/// A bin on both sides of a merge, either of which may be missing, with the merges a resolver reaches for.
/// Made by the readers of a [`Pair`]. `current` and `incoming` are the two values, `None` on a side that does
/// not have the bin; they are public for a merge that none of the methods covers.
///
/// ```ignore
/// let visits = pair.int("visits")?.max();             // Option<i64>: the larger
/// let total = pair.int("total")?.sum();               // Option<i64>: a missing side counts as 0
/// let name = pair.string("name")?.newer();            // Option<Text>: last writer wins
///
/// if let Some(visits) = visits {
///     out.insert("visits", visits)?;
/// }
/// ```
///
/// The methods take `self`: a `Both` is read once. The ones that return an `Option` give `None` only when
/// neither side has the bin.
pub struct Both<T> {
    pub current: Option<T>,
    pub incoming: Option<T>,
    current_last_update_time: i64,
    incoming_last_update_time: i64,
    current_source_id: i64,
    incoming_source_id: i64,
}

impl<T> Both<T> {
    /// Whether the incoming side wrote this bin later: by the bin's last update time where the server
    /// keeps one, the record's otherwise, and on a tie by source id - the order the server's own bin
    /// convergence uses, so "newer" means the same on both DCs even when the clocks agreed to the
    /// millisecond.
    #[must_use]
    pub fn incoming_is_newer(&self) -> bool {
        self.incoming_last_update_time > self.current_last_update_time
            || (self.incoming_last_update_time == self.current_last_update_time
                && self.incoming_source_id > self.current_source_id)
    }

    /// The side written later, by [`incoming_is_newer`](Both::incoming_is_newer); whichever exists when only
    /// one does, and `None` when neither does. Last writer wins, for one bin.
    pub fn newer(self) -> Option<T> {
        if self.incoming_is_newer() {
            self.incoming.or(self.current)
        } else {
            self.current.or(self.incoming)
        }
    }

    /// The side written earlier; whichever exists when only one does, and `None` when neither does. First
    /// writer wins, for one bin.
    pub fn older(self) -> Option<T> {
        if self.incoming_is_newer() {
            self.current.or(self.incoming)
        } else {
            self.incoming.or(self.current)
        }
    }

    /// Both sides, as `(older, newer)`: the order both DCs agree on, for a merge whose result depends on the
    /// order its inputs are taken in. A missing side stays `None` in its place.
    pub fn ordered(self) -> (Option<T>, Option<T>) {
        if self.incoming_is_newer() {
            (self.current, self.incoming)
        } else {
            (self.incoming, self.current)
        }
    }

    /// The current value when there is one, else the incoming. It prefers a side by which cluster is asking,
    /// so the two DCs can disagree: use it only where the other DC's answer does not have to match.
    pub fn current_or_incoming(self) -> Option<T> {
        self.current.or(self.incoming)
    }

    /// The incoming value when there is one, else the current. Like
    /// [`current_or_incoming`](Both::current_or_incoming), it is not symmetric between the two DCs.
    pub fn incoming_or_current(self) -> Option<T> {
        self.incoming.or(self.current)
    }

    /// Combine the two when both exist, as `f(current, incoming)`; the one that exists otherwise, without
    /// calling `f`; `None` when neither does. For the two DCs to agree, `f` must give the same answer with
    /// its arguments swapped.
    pub fn merge(self, f: impl FnOnce(T, T) -> T) -> Option<T> {
        match (self.current, self.incoming) {
            (Some(l), Some(r)) => Some(f(l, r)),
            (l, r) => l.or(r),
        }
    }
}

impl<T: PartialOrd> Both<T> {
    /// The larger of the two; the one that exists when only one does, and `None` when neither does. The merge
    /// of a value that only grows.
    pub fn max(self) -> Option<T> {
        self.merge(|l, r| if r > l { r } else { l })
    }

    /// The smaller of the two; the one that exists when only one does, and `None` when neither does.
    pub fn min(self) -> Option<T> {
        self.merge(|l, r| if r < l { r } else { l })
    }
}

impl Both<i64> {
    /// The sum; the one that exists when only one side has the bin, a missing side counting as zero, and
    /// `None` when neither does, as `max` and `min`. It wraps on overflow, as two's complement does, rather
    /// than trapping. Adding the two sides is right only when each holds its own DC's share; summing two
    /// copies of one counter counts twice.
    #[must_use]
    pub fn sum(self) -> Option<i64> {
        self.merge(i64::wrapping_add)
    }
}

impl Both<f64> {
    /// The sum; the one that exists when only one side has the bin, and `None` when neither does, as `max`
    /// and `min`. Right only when each side holds its own DC's share.
    #[must_use]
    pub fn sum(self) -> Option<f64> {
        self.merge(|l, r| l + r)
    }
}

/// What an `xdr-merge` handler answers: which record this cluster ends up with. A handler returns
/// `Ok(resolution)`; an `Err` fails the merge with the error's message.
///
/// ```ignore
/// if incoming.is_delete() {
///     return Ok(Resolution::TakeRemote);
/// }
///
/// let pair = Pair::of(current, incoming);
/// let out = as_udf::map(1)?;
///
/// out.insert("visits", pair.int("visits")?.max().unwrap_or(0))?;
///
/// Ok(Resolution::Record { bins: pair.with_newer(out)?, ttl: None })
/// ```
///
/// A [`Map`] of bins converts to `Resolution::Record` with `ttl: None`, so `Ok(bins.into())` is enough.
pub enum Resolution {
    /// Keep the current record; the incoming write is dropped.
    KeepLocal,
    /// Apply the incoming write whole, as if it had won every bin.
    TakeRemote,
    /// Let the namespace's policy decide, as it would have without the event.
    Policy,
    /// The final record is no record: it is removed on this cluster. The other cluster decides for itself,
    /// so a merge that deletes has to delete on both sides for the two to agree.
    Delete,
    /// The final record: these bins, whole - a bin not in the map, or set to nil, is deleted. **At least
    /// one bin must be left**: a record of no bins is the handler's failure, not a deletion, so that a
    /// merge which built nothing by mistake does not remove what it merged. Say [`Resolution::Delete`]
    /// to remove the record. With the bins comes
    /// optionally a TTL with a write's meaning: seconds, `0` for the namespace default,
    /// `u32::MAX` for never expire, subject to the namespace's rule on TTL reductions. `None` keeps the
    /// TTL the incoming write asked for.
    Record { bins: Map, ttl: Option<u32> },
}

/// A map of bins as the final record, with the TTL the incoming write asked for: [`Resolution::Record`]
/// with `ttl: None`.
impl From<Map> for Resolution {
    fn from(bins: Map) -> Self {
        Resolution::Record { bins, ttl: None }
    }
}

/// Decode an event call's inputs into the two records. Used by [`event!`].
///
/// The server sends one argument, a map describing the merge. Each fact in it is true of one record or
/// the other, and is attached to that one here; the binding's trigger and whether the call is a dry run
/// are not passed on, because a merge that read them would answer differently for the same two records.
#[doc(hidden)]
pub fn xdr_merge_inputs(args_handle: i32, argc: i32) -> Result<(EventRecord, EventRecord), Error> {
    let args = Args::from_handle(args_handle, argc)?;
    let merge = args.get(0).ok_or(Error::Custom("event: the merge description is missing"))?.map()?;

    // A key the server always sends, and one it sends only when it has something to say.
    let need = |m: &Coll, key: &str| m.get(key)?.ok_or(Error::Absent);
    let maybe = |m: &Coll, key: &str| m.get(key).ok().flatten();

    let mut kbuf = [0u8; 16];
    let bin_convergence = maybe(&merge, "policy").is_some_and(|v| v.as_str(&mut kbuf) == Ok("bin-lut"));
    // A stored list that is empty: the server always sends `empty` for this purpose.
    let empty = need(&merge, "empty")?.list()?;

    let side = |which: i32, key: &str| -> Result<EventRecord, Error> {
        let m = need(&merge, key)?.map()?;
        let bin_names = need(&m, "bins")?.list()?;
        let incoming = which == 1;
        let flag = |name: &str| matches!(m.get(name), Ok(Some(Value::Bool(true))));
        let int = |name: &str| maybe(&m, name).and_then(|v| v.int().ok()).unwrap_or(0);

        Ok(EventRecord {
            which,
            // Absent for an xdr-merge side: each cluster sets its own, and a merge must not read them.
            generation: int("gen"),
            last_update_time: int("lut"),
            ttl: int("ttl"),
            source_id: need(&m, "src-id")?.int()?,
            bin_names,
            deleted_bins: match maybe(&m, "deleted") {
                Some(v) => v.list()?,
                None => empty,
            },
            bin_last_update_times: maybe(&m, "bin-luts").and_then(|v| v.map().ok()),
            bin_source_ids: maybe(&m, "bin-srcs").and_then(|v| v.map().ok()),
            delete: incoming && flag("deleted-record"),
            partial: incoming && flag("partial"),
            tombstone: !incoming && flag("tombstone"),
            bin_convergence,
            lost: if incoming { need(&merge, "lost")?.list()? } else { empty },
        })
    };

    Ok((side(0, "current")?, side(1, "incoming")?))
}

/// Deliver an event handler's answer. Used by [`event!`].
#[doc(hidden)]
pub fn deliver_resolution(r: Result<Resolution, Error>) -> i32 {
    let outcome = match r {
        Ok(Resolution::KeepLocal) => result(&Value::from("keep-local")),
        Ok(Resolution::TakeRemote) => result(&Value::from("take-remote")),
        Ok(Resolution::Policy) => result(&Value::from("policy")),
        Ok(Resolution::Delete) => result(&Value::from("delete")),
        Ok(Resolution::Record { bins, ttl }) => (|| {
            let out = map(3)?;

            out.insert("verdict", "record")?.insert("bins", &bins)?;

            if let Some(ttl) = ttl {
                out.insert("ttl", i64::from(ttl))?;
            }

            result(&out.as_value())
        })(),
        Err(e) => Err(e),
    };

    match outcome {
        Ok(()) => 0,
        Err(e) => {
            fail(e.message());
            1
        }
    }
}

//==========================================================
// The export macros.
//

/// Export one or more functions as UDFs.
///
/// A UDF is an ordinary function with typed parameters, returning a `Result<Value, Error>`:
///
/// ```ignore
/// export! {
///     fn transfer(rec: &mut Record, amount: i64, to: &str, dry_run: Option<bool>) -> Result<Value, Error> {
///         ...
///         Ok(Value::Int(balance))
///     }
/// }
/// ```
///
/// # The record
///
/// A function that reads its record takes `rec: &Record` as its first parameter, and one that writes it
/// `rec: &mut Record`; see [`Record`]. It is written exactly so, with `Record` in scope under that name.
/// It is not an argument: a client does not pass it, it is not counted, and the signature export below
/// does not list it.
///
/// Each becomes a `(args: i32, argc: i32) -> i32` export, which is what the host calls. The macro decodes
/// the arguments into the parameters, checks their count and kinds, and rejects a mismatched call with a
/// message naming the parameter - before the body runs. `Ok(value)` becomes the result the client receives;
/// `Err(error)` fails the call with the error's message.
///
/// # What it returns
///
/// `Result<Value, Error>` is the general form: a [`Value`] is any kind of result. A function that builds a
/// string or blob returns it owned instead, because a `Value` only borrows one: `Result<String, Error>` or
/// `Result<Vec<u8>, Error>` with the `alloc` feature, `Result<Str<N>, Error>` or `Result<Bytes<N>, Error>`
/// without. A function that builds a list or map returns it owned too, `Result<List, Error>` or
/// `Result<Map, Error>`, or `Result<Output, Error>` when some branches return another value
/// (`Ok(list.into())` there): the macro gives the collection's handle back once the host has taken the
/// result. A function that cannot fail may leave the `Result` off and return any of these directly. The
/// return type is anything that is [`Return`], written with `Value`, `Error` and the rest in scope under
/// those names. The signature export says `-> value` for all of them.
///
/// # The parameters
///
/// Parameter types: `i64`, `f64`, `bool`; `&str` and `&[u8]` (copied into a 256-byte buffer of the
/// parameter's own); [`Str<N>`] and [`Bytes<N>`] for a string or blob of up to `N` bytes; `String` and
/// `Vec<u8>` for any length, with the `alloc` feature; [`Value`] for anything, [`Coll`] for a list or map,
/// `Option<T>` for an argument that may be absent or nil, and [`Args`] as the last parameter for every
/// remaining argument. A string or blob longer than its parameter allows is rejected from its reported
/// length, before any copy. A call with more arguments than parameters is rejected unless the last parameter
/// is [`Args`], and a call with more than [`MAX_ARGS`] (128) arguments in all is rejected whatever the
/// parameters are, with a message naming the limit and the count: no argument is dropped. The count is
/// checked first, before any argument is decoded.
///
/// The messages are word for word those of the other SDKs, with arguments counted from 1:
/// `add: argument 1 (by) is missing`, `add: argument 1 (by) is not an int` (the kinds are `an int`,
/// `a double`, `a bool`, `a string`, `a blob`, and `a list or a map` for a [`Coll`]),
/// `add: argument 2 (name) is 300 bytes, at most 256`, `add takes at most 2 arguments, got 3` and
/// `add_all takes at most 128 arguments, got 129`.
///
/// # The signature export
///
/// Beside each UDF the macro exports `__udf_sig_<name>`, which the host serves as a string result when a
/// client executes it like any other function: `fn add_all(name: str[256], numbers: any...) -> value`. A
/// parameter reads `name: type`, with `[N]` for the most bytes a string or blob parameter takes, `?` after
/// the name of one that may be absent, and `...` on one that takes the rest. The body never runs: a client
/// can read a module's contract to check a call before making it, or generate typed bindings.
///
/// Attributes are passed through, so doc comments on a UDF survive.
#[macro_export]
macro_rules! export {
    ($(
        $(#[$meta:meta])*
        fn $name:ident($($params:tt)*) -> $ret:ty $body:block
    )*) => {
        $(
            const _: () = {
                #[export_name = concat!("__udf_sig_", stringify!($name))]
                pub extern "C" fn signature() -> i32 {
                    static mut SIG: $crate::Signature = $crate::Signature::new();

                    // One writer: the host runs a module's exports one at a time per instance.
                    unsafe {
                        (*::core::ptr::addr_of_mut!(SIG)).function(
                            stringify!($name),
                            $crate::udf_params!(writable, $($params)*),
                        )
                    }
                }
            };

            $(#[$meta])*
            #[no_mangle]
            pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
                $crate::udf_call!(writable, $name, args_handle, argc, $body, [result $ret], $($params)*)
            }
        )*
    };
}

/// Export a stream (aggregation) UDF.
///
/// A stream UDF is driven by the host, one record at a time, and **one instance sees every record a query
/// thread reads** - so unlike a record UDF it can keep state between calls, in ordinary `static mut`. That is
/// where an accumulator belongs, and it is the reason aggregation is possible at all.
///
/// Each declaration produces the two exports the host looks for, derived from the name a client passes to its
/// query: `<name>_record`, called per record and returning `Result<(), Error>`, and `<name>_end`, called once
/// afterwards and returning `Result<Value, Error>` (or anything else [`export!`] lets a function return),
/// its `Ok` being the node's partial. Both take the call's
/// typed arguments like a record UDF does.
///
/// ```ignore
/// static mut TOTAL: i64 = 0;
///
/// stream! {
///     fn sum {
///         record(rec: &Record, bin: &str) -> Result<(), Error> {
///             unsafe { TOTAL += rec.bin(bin)?.int()? };
///             Ok(())
///         }
///         end(_bin: &str) -> Result<Value, Error> {
///             Ok(Value::Int(unsafe { TOTAL }))
///         }
///     }
/// }
/// ```
///
/// # One result per query thread
///
/// `end` runs once **per query thread per node** (a handful of times per query), so a client gets a few
/// partials and combines them itself.
///
/// The signature export is `__udf_sig_<name>`, reading `stream name { record(...) end(...) }`; see
/// [`export!`].
#[macro_export]
macro_rules! stream {
    ($(
        $(#[$meta:meta])*
        fn $name:ident {
            record($($rparams:tt)*) -> Result<(), Error> $rbody:block
            end($($earg:ident : $ety:ty),* $(,)?) -> $eret:ty $ebody:block
        }
    )*) => {
        $(
            const _: () = {
                #[export_name = concat!("__udf_sig_", stringify!($name))]
                pub extern "C" fn signature() -> i32 {
                    static mut SIG: $crate::Signature = $crate::Signature::new();

                    unsafe {
                        (*::core::ptr::addr_of_mut!(SIG)).stream(
                            stringify!($name),
                            $crate::udf_params!(readable, $($rparams)*),
                            &[$($crate::param::<$ety>(stringify!($earg))),*],
                        )
                    }
                }

                #[export_name = concat!(stringify!($name), "_record")]
                pub extern "C" fn record(args_handle: i32, argc: i32) -> i32 {
                    $crate::udf_call!(readable, $name, args_handle, argc, $rbody, [unit], $($rparams)*)
                }

                #[export_name = concat!(stringify!($name), "_end")]
                pub extern "C" fn end(args_handle: i32, argc: i32) -> i32 {
                    $crate::udf_body!($name, args_handle, argc, [], ($($earg : $ety),*), $ebody, [result $eret])
                }
            };
        )*
    };
}

/// Export an event handler.
///
/// The server has one event, `xdr-merge`, and the macro one arm for it, of exactly this shape:
///
/// ```ignore
/// event! {
///     on xdr_merge
///     fn resolve_account(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
///         let pair = Pair::of(current, incoming);
///         let out = as_udf::map(2)?;
///
///         if let Some(visits) = pair.int("visits")?.max() {
///             out.insert("visits", visits)?;
///         }
///
///         Ok(Resolution::Record { bins: pair.with_newer(out)?, ttl: None })
///     }
/// }
/// ```
///
/// The handler takes the record this cluster holds and the one that arrived over XDR, in that order, and
/// nothing else: what there is to know about the merge is asked of the record it is true of
/// ([`EventRecord::is_delete`], [`EventRecord::is_tombstone`] and their neighbours). The parameter types are
/// part of the arm, so a handler with another signature does not compile; `EventRecord`, `Resolution` and
/// `Error` must be in scope under those names. It returns the [`Resolution`], or an `Err` to fail the merge
/// with the error's message. Several handlers may follow one another in one `event!`.
///
/// The macro exports the handler under its name, which `event-set` refers to, and `__udf_sig_<name>` reading
/// `event xdr-merge/1 ...`, which `event-set` checks before binding: the number is the version of the
/// contract this SDK was written against, and a server that does not serve it refuses the binding.
///
/// A handler must be a pure function of its inputs - no writes, no clock - and symmetric in them: two DCs
/// that resolve the same pair in opposite order must produce the same record, or they diverge. There is no
/// clock in a handler ([`now`] answers `Err(Refused)`), a [`log`] line is dropped, and the record-level
/// [`generation`](EventRecord::generation) and [`last_update_time`](EventRecord::last_update_time) are 0 on
/// both sides. The dry run (`execute_udf(key,
/// "__event__", "xdr-merge", ...)`) reports whether a handler is: `symmetric`, `idempotent` and, given a
/// third record, `associative` - the three laws a convergent merge obeys.
#[macro_export]
macro_rules! event {
    () => {};

    // The signature export beside a handler.
    (@sig $name:ident, $text:expr) => {
        const _: () = {
            #[export_name = concat!("__udf_sig_", stringify!($name))]
            pub extern "C" fn signature() -> i32 {
                static mut SIG: $crate::Signature = $crate::Signature::new();

                unsafe { (*::core::ptr::addr_of_mut!(SIG)).literal($text) }
            }
        };
    };

    (
        $(#[$meta:meta])*
        on xdr_merge
        fn $name:ident($current:ident : EventRecord, $incoming:ident : EventRecord)
            -> Result<Resolution, Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event xdr-merge/1 ", stringify!($name),
            "(current: record, incoming: record) -> resolution"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let ($current, $incoming) = match $crate::xdr_merge_inputs(args_handle, argc) {
                Ok(inputs) => inputs,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<$crate::Resolution, $crate::Error> = (|| $body)();

            $crate::deliver_resolution(__r)
        }

        $crate::event!($($rest)*);
    };
}

/// The parameters of an export that a client passes: all of them but a leading record. Not for direct use.
///
/// `writable` is a record UDF, which may take `&mut Record`; `readable` is the per-record half of a stream
/// UDF, which may only read. A record parameter written any other way, or anywhere but first, is taken for
/// an argument and fails to compile as one, with a message that says so (see `FromArg`).
#[doc(hidden)]
#[macro_export]
macro_rules! udf_params {
    (writable, $rec:ident : &mut Record $(, $arg:ident : $ty:ty)* $(,)?) => {
        &[$($crate::param::<$ty>(stringify!($arg))),*]
    };
    ($access:ident, $rec:ident : &Record $(, $arg:ident : $ty:ty)* $(,)?) => {
        &[$($crate::param::<$ty>(stringify!($arg))),*]
    };
    ($access:ident, $($arg:ident : $ty:ty),* $(,)?) => {
        &[$($crate::param::<$ty>(stringify!($arg))),*]
    };
}

/// An export's body, with its record parameter, if it has one, bound first. Not for direct use.
#[doc(hidden)]
#[macro_export]
macro_rules! udf_call {
    (writable, $name:ident, $h:expr, $argc:expr, $body:block, $outcome:tt,
     $rec:ident : &mut Record $(, $arg:ident : $ty:ty)* $(,)?) => {
        $crate::udf_body!($name, $h, $argc, [mut $rec], ($($arg : $ty),*), $body, $outcome)
    };
    ($access:ident, $name:ident, $h:expr, $argc:expr, $body:block, $outcome:tt,
     $rec:ident : &Record $(, $arg:ident : $ty:ty)* $(,)?) => {
        $crate::udf_body!($name, $h, $argc, [ref $rec], ($($arg : $ty),*), $body, $outcome)
    };
    ($access:ident, $name:ident, $h:expr, $argc:expr, $body:block, $outcome:tt,
     $($arg:ident : $ty:ty),* $(,)?) => {
        $crate::udf_body!($name, $h, $argc, [], ($($arg : $ty),*), $body, $outcome)
    };
}

/// Bind the record parameter of an export. Not for direct use.
#[doc(hidden)]
#[macro_export]
macro_rules! udf_record {
    ([]) => {};
    // `Record` here is the name as the UDF's own source sees it, not `$crate::Record`: a function that
    // declares `rec: &mut Record` must have this crate's `Record` in scope under that name, as its
    // signature reads, and a different type of that name is a mismatch the compiler reports.
    ([mut $rec:ident]) => {
        let mut __record = $crate::Record::current();
        let $rec: &mut Record = &mut __record;
    };
    ([ref $rec:ident]) => {
        let __record = $crate::Record::current();
        let $rec: &Record = &__record;
    };
}

/// The body of an export: decode the arguments, run the block, deliver the outcome. Not for direct use.
#[doc(hidden)]
#[macro_export]
macro_rules! udf_body {
    ($name:ident, $args_handle:expr, $argc:expr, [$($record:tt)*], ($($arg:ident : $ty:ty),*), $body:block,
     $outcome:tt) => {{
        $crate::udf_record!([$($record)*]);

        // More arguments than the function declares fails the call before any is decoded, as in every SDK:
        // the most is the count of parameters, or MAX_ARGS when the last one takes the rest.
        let __most: usize = if false $(|| <$ty as $crate::FromArg>::REST)* {
            $crate::MAX_ARGS
        } else {
            0 $(+ { let _ = stringify!($arg); 1 })*
        };

        if $argc > 0 && $argc as usize > __most {
            return $crate::reject_arity(stringify!($name), __most, $argc);
        }

        let __args = match $crate::decode_args(stringify!($name), $args_handle, $argc) {
            Ok(args) => args,
            Err(failed) => return failed,
        };
        let mut __index: usize = 0;

        $(
            // A buffer named like the parameter, which the parameter then shadows: a string parameter
            // borrows it, every other kind ignores it.
            let mut $arg: [u8; $crate::ARG_BUF] = [0; $crate::ARG_BUF];
            let $arg: $ty = match <$ty as $crate::FromArg>::from_arg(&__args, __index, &mut $arg) {
                Ok(value) => value,
                Err(e) => return $crate::reject_argument(stringify!($name), __index, stringify!($arg), e),
            };

            __index += 1;
        )*

        let _ = __index;

        $crate::udf_outcome!($outcome, (|| $body)())
    }};
}

/// Deliver a UDF's outcome. Not for direct use.
#[doc(hidden)]
#[macro_export]
macro_rules! udf_outcome {
    // The type is the one the function declares, as its own source names it: anything that is `Return`.
    ([result $ret:ty], $r:expr) => {{
        let __r: $ret = $r;

        match $crate::Return::deliver(__r) {
            Ok(()) => 0,
            Err(e) => {
                $crate::fail(e.message());
                1
            }
        }
    }};
    ([unit], $r:expr) => {{
        let __r: ::core::result::Result<(), $crate::Error> = $r;

        match __r {
            Ok(()) => 0,
            Err(e) => {
                $crate::fail(e.message());
                1
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A message that fits is kept whole; one that does not ends in `...`, at most 511 bytes, and is still
    /// UTF-8 when the cut falls inside a character.
    #[test]
    fn a_long_message_is_marked_where_it_is_cut() {
        let mut m = Message::new();
        let _ = write!(m, "short");
        assert_eq!(m.as_str(), "short");

        let mut m = Message::new();
        let long = "x".repeat(600);
        let _ = write!(m, "{long}");
        assert_eq!(m.as_str().len(), 511);
        assert!(m.as_str().ends_with("..."));

        let mut m = Message::new();
        let _ = write!(m, "{}", "y".repeat(507));
        let _ = write!(m, "\u{e9}\u{e9}\u{e9}");
        assert!(m.as_str().ends_with("..."), "{}", m.as_str().len());
        assert!(m.as_str().len() <= 511);

        let mut m = Message::new();
        let _ = write!(m, "{}", "\u{e9}".repeat(300));
        assert!(m.as_str().ends_with("...") && m.as_str().len() <= 511, "{}", m.as_str().len());
    }

    /// A signature that fits is served; one that does not is served empty, never cut.
    #[test]
    fn a_signature_too_long_is_served_empty() {
        let params: [Param; 4] =
            core::array::from_fn(|_| Param { name: "a", ty: "int", capacity: 0, optional: false, rest: false });
        let mut sig = Signature::new();
        sig.function("f", &params);
        assert_eq!(&sig.buf[..sig.len], b"fn f(a: int, a: int, a: int, a: int) -> value");
        assert_eq!(sig.buf[sig.len], 0);

        let many: [Param; 200] = core::array::from_fn(|_| Param {
            name: "a_parameter_name_that_is_long",
            ty: "str",
            capacity: 65536,
            optional: true,
            rest: false,
        });
        let mut sig = Signature::new();
        sig.function("f", &many);
        assert!(sig.overflow);
        assert_eq!(sig.buf[0], 0, "an empty text, not a cut one");
    }

    /// The type codes are the ABI, so their values are the contract - the host sends them as integers.
    #[test]
    fn value_codes_match_the_host() {
        let raw = |kind: i32, len: u32, payload: i64| {
            let mut v = [0u8; VALUE_SZ];
            v[..4].copy_from_slice(&kind.to_le_bytes());
            v[4..8].copy_from_slice(&len.to_le_bytes());
            v[8..].copy_from_slice(&payload.to_le_bytes());
            v
        };

        assert_eq!(Value::from_raw(&raw(0, 0, 0)), Value::Nil);
        assert_eq!(Value::from_raw(&raw(1, 0, 1)), Value::Bool(true));
        assert_eq!(Value::from_raw(&raw(2, 0, -7)), Value::Int(-7));
        assert_eq!(Value::from_raw(&raw(3, 0, 2.5f64.to_bits() as i64)), Value::Double(2.5));
        assert_eq!(
            Value::from_raw(&raw(4, 3, (5i64 << 32) | 9)),
            Value::String(Text::Stored { src: 5, at: 9, len: 3 })
        );
        assert_eq!(
            Value::from_raw(&raw(6, 2, 5i64 << 32)),
            Value::List(Coll::Stored { src: 5, at: 0, len: 2 })
        );
        assert_eq!(
            Value::from_raw(&raw(9, 4, 1i64 << 32)),
            Value::GeoJson(Text::Stored { src: 1, at: 0, len: 4 })
        );
        assert_eq!(
            Value::from_raw(&raw(10, 8, 2i64 << 32)),
            Value::Hll(Blob::Stored { src: 2, at: 0, len: 8 })
        );
        assert_eq!(Value::from_raw(&raw(-1, 0, 0)), Value::Other);
        assert_eq!(Value::from_raw(&raw(99, 0, 0)), Value::Other);
    }

    /// The batch form is the host's layout plus the guest-only kinds, whose codes are the contract too.
    #[test]
    fn batch_values_carry_the_guest_kinds() {
        let kind = |raw: [u8; VALUE_SZ]| i32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
        let len = |raw: [u8; VALUE_SZ]| u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
        let payload = |raw: [u8; VALUE_SZ]| i64::from_le_bytes(raw[8..].try_into().unwrap());

        let text = "hello";
        let raw = Value::from(text).raw().unwrap();
        assert_eq!((kind(raw), len(raw)), (GUEST_STR, 5));
        assert_eq!(payload(raw), text.as_ptr() as usize as i64, "a guest string is its address");

        let raw = Value::GeoJson(Text::Guest("{}")).raw().unwrap();
        assert_eq!(kind(raw), GUEST_GEOJSON);

        let raw = Value::from(&b"ab"[..]).raw().unwrap();
        assert_eq!((kind(raw), len(raw)), (GUEST_BLOB, 2));

        let raw = Value::List(Coll::Built(7)).raw().unwrap();
        assert_eq!((kind(raw), payload(raw)), (HANDLE, 7));

        let raw = Value::String(Text::Stored { src: 5, at: 9, len: 3 }).raw().unwrap();
        assert_eq!((kind(raw), len(raw), payload(raw)), (4, 3, (5i64 << 32) | 9), "a stored value keeps its position");

        assert_eq!(Value::Int(-7).raw().map(payload), Some(-7));
        assert_eq!(Value::Hll(Blob::Guest(&[])).raw(), None, "a guest HLL cannot be made");

        let owned = Str::<8>::new("abc").unwrap();
        assert_eq!(owned, "abc");
        assert_eq!(Str::<2>::new("abc"), Err(Error::TooSmall(3)));
    }

    /// The signature text is a contract with whatever reads it, so its grammar is pinned here.
    #[test]
    fn a_signature_describes_every_parameter_kind() {
        fn text(sig: &Signature) -> &str {
            core::str::from_utf8(&sig.buf[..sig.len]).unwrap()
        }

        let mut sig = Signature::new();
        sig.function(
            "transfer",
            &[
                param::<i64>("amount"),
                param::<&str>("to"),
                param::<Str<8>>("tag"),
                param::<Option<bool>>("dry_run"),
                param::<Option<Bytes<16>>>("token"),
                param::<Coll>("items"),
                param::<Args>("rest"),
            ],
        );
        assert_eq!(
            text(&sig),
            "fn transfer(amount: int, to: str[256], tag: str[8], dry_run?: bool, token?: bytes[16], \
             items: coll, rest: any...) -> value"
        );

        let mut sig = Signature::new();
        sig.function("ping", &[]);
        assert_eq!(text(&sig), "fn ping() -> value");

        // Written once, however often it is asked for, and NUL-terminated for the host.
        let addr = sig.function("ping", &[]);
        assert_eq!(addr, sig.buf.as_ptr() as usize as i32);
        assert_eq!(sig.buf[sig.len], 0);

        let mut sig = Signature::new();
        sig.stream("sum", &[param::<&str>("bin")], &[param::<Value>("_bin")]);
        assert_eq!(text(&sig), "stream sum { record(bin: str[256]) end(_bin: any) }");
    }

    #[test]
    fn statuses_become_errors() {
        assert_eq!(Error::from_status(NOT_FOUND), Error::Absent);
        assert_eq!(Error::from_status(TYPE_MISMATCH), Error::WrongType);
        assert_eq!(Error::from_status(REFUSED), Error::Refused);
        assert_eq!(Error::from_status(NEEDED_BASE - 5), Error::TooSmall(5));
        assert_eq!(Error::from_status(-1), Error::Malformed);
    }

    #[test]
    fn a_message_never_overflows() {
        let mut m = Message::new();
        for _ in 0..100 {
            let _ = write!(m, "0123456789");
        }
        assert_eq!(m.as_str().len(), MESSAGE_CAP);
        assert!(m.as_str().ends_with("..."), "and the cut is marked");
    }
}
