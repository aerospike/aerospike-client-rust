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
//!   truncating. The `alloc` feature adds `String` and `Vec<u8>` as parameter types and as what
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
    fn val_int(value: i64) -> i32;
    fn val_double(value: f64) -> i32;
    fn val_bool(value: i32) -> i32;
    fn val_nil() -> i32;
    fn val_str(value: *const u8, value_len: i32) -> i32;
    fn val_blob(value: *const u8, value_len: i32) -> i32;
    fn val_geojson(value: *const u8, value_len: i32) -> i32;
    fn list_push(handle: i32, value: i32) -> i32;
    fn list_push_int(handle: i32, value: i64) -> i32;
    fn list_push_str(handle: i32, value: *const u8, value_len: i32) -> i32;
    fn map_put(handle: i32, key: i32, value: i32) -> i32;
    #[link_name = "map_has"]
    fn raw_map_has(handle: i32, key: *const u8, key_len: i32) -> i32;
    #[link_name = "map_has_i"]
    fn raw_map_has_i(handle: i32, key: i64) -> i32;
    fn map_put_str_int(handle: i32, key: *const u8, key_len: i32, value: i64) -> i32;
    fn map_put_str_str(
        handle: i32,
        key: *const u8,
        key_len: i32,
        value: *const u8,
        value_len: i32,
    ) -> i32;
    fn map_put_str_handle(handle: i32, key: *const u8, key_len: i32, value: i32) -> i32;
    // A value read from the record, into a built collection by position.
    fn list_push_at(handle: i32, src: i32, at: i32) -> i32;
    fn map_put_str_at(handle: i32, key: *const u8, key_len: i32, src: i32, at: i32) -> i32;
    fn map_put_i_at(handle: i32, key: i64, src: i32, at: i32) -> i32;
    // A batch of values into a built collection: 16-byte values, or 32-byte key-value pairs.
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// No such bin, key, element or argument.
    Absent,
    /// The value is not the kind asked for: `int()` on a string, `list()` on an integer.
    WrongType,
    /// The buffer was too small; this many bytes are needed.
    TooSmall(usize),
    /// The record is read-only here (an aggregation), the host rejected the write, or the per-call
    /// resource table is full.
    Refused,
    /// The host could not decode the stored bytes. Not something a UDF can act on.
    Malformed,
    /// A failure of the UDF's own, with the message the client will see.
    Custom(&'static str),
}

impl Error {
    /// The message a client sees when a UDF returns this.
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

/// How severe a log line is. `Warn` is the loudest.
#[derive(Clone, Copy, Debug)]
pub enum Level {
    Warn = 0,
    Info = 1,
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
/// opens a window onto the stored bytes when iterated. A value read from the record is valid for the rest of
/// the call, or until the bin it came from is written.
///
/// Values a UDF builds use the same type: `Value::Int(3)`, `"text".into()`, or a [`List`] / [`Map`] built
/// on the host. [`Value::List`] and [`Value::Map`] then hold a [`Coll::Built`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value<'a> {
    Nil,
    Bool(bool),
    Int(i64),
    Double(f64),
    String(Text<'a>),
    Blob(Blob<'a>),
    /// A GeoJson document, which reads and writes as its text. Distinct from [`Value::String`] on purpose: the
    /// particle type is what makes a bin geospatially indexable, and writing a document back as a string
    /// would silently make it unqueryable.
    GeoJson(Text<'a>),
    List(Coll),
    Map(Coll),
    /// An HLL: the bytes the server's HLL operations maintain. Readable, and writable back **by position**
    /// only - a UDF cannot make one from guest bytes, since the subtype that makes it an HLL is the
    /// server's to set.
    Hll(Blob<'a>),
    /// Something a UDF has no business interpreting.
    Other,
}

/// A string's bytes: in the record, or in the guest's own memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Text<'a> {
    /// `len` bytes at a position in a source the host holds. What a read yields.
    Stored { src: i32, at: u32, len: usize },
    /// A `&str` of the guest's. What a UDF builds; `"text".into()` makes one.
    Guest(&'a str),
}

/// A blob's bytes: in the record, or in the guest's own memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Blob<'a> {
    Stored { src: i32, at: u32, len: usize },
    Guest(&'a [u8]),
}

/// A list or map: `len` elements (or entries) somewhere the host can walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coll {
    /// In the record, or in the packed copy of an argument list: at a position in a source the host holds.
    Stored { src: i32, at: u32, len: usize },
    /// Built on the host by [`list`] / [`map`], identified by its handle.
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

    /// What kind of value this is, for messages.
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

    /// The bytes of an HLL.
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

    /// The text of a string or a GeoJson document.
    pub fn string(&self) -> Result<Text<'a>, Error> {
        match self {
            Value::String(t) | Value::GeoJson(t) => Ok(*t),
            _ => Err(Error::WrongType),
        }
    }

    /// The bytes of a blob, or `Err(WrongType)` for any other kind. An HLL is not a blob here: see
    /// [`Value::hll`].
    pub fn blob(&self) -> Result<Blob<'a>, Error> {
        match self {
            Value::Blob(b) => Ok(*b),
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

    /// A string's or GeoJson document's text, into a buffer you own. See [`Text::as_str`].
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

    /// Whether this is a string, blob, GeoJson document, HLL, list or map the host holds, as opposed to
    /// a scalar or something the guest built.
    #[must_use]
    pub fn is_stored(&self) -> bool {
        stored_position(self).is_some()
    }
}

impl Value<'_> {
    /// The value's bytes as the server stores them - its msgpack, header included - into `buf`, for a
    /// value read from the record (a string, blob, list or map). A short buffer reports the size needed.
    /// This is the encoding a convergent merge compares when nothing else tells two candidates apart.
    pub fn stored_bytes<'b>(&self, buf: &'b mut [u8]) -> Result<&'b [u8], Error> {
        let (src, at) = stored_position(self).ok_or(Error::WrongType)?;
        let n = bytes_result(unsafe { raw_coll_bytes_at(src, at as i32, buf.as_mut_ptr(), buf.len() as i32) }, buf.len())?;

        Ok(&buf[..n])
    }

    /// Compare two stored values byte by byte, as `memcmp` would on their stored encodings: the first byte
    /// that differs decides, a shorter prefix comes first. Neither value is copied into the guest. Equal
    /// content built the same way encodes the same, since the server stores lists in order and the maps
    /// a UDF builds key-ordered, so this is the last tie-break of a deterministic merge.
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

impl<'a> From<&'a str> for Value<'a> {
    fn from(value: &'a str) -> Self {
        Value::String(Text::Guest(value))
    }
}

impl<'a> From<&'a [u8]> for Value<'a> {
    fn from(value: &'a [u8]) -> Self {
        Value::Blob(Blob::Guest(value))
    }
}

impl<'a> From<&'a str> for Text<'a> {
    fn from(value: &'a str) -> Self {
        Text::Guest(value)
    }
}

impl Text<'_> {
    /// How many bytes the text has.
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

    /// The text, into a buffer you own. A guest string comes back as itself, nothing copied; a stored one is
    /// fetched in one import call. `Err(TooSmall(n))` says to try again with `n` bytes.
    pub fn as_str<'b>(&'b self, out: &'b mut [u8]) -> Result<&'b str, Error> {
        match self {
            Text::Guest(s) => Ok(s),
            Text::Stored { src, at, .. } => {
                let n = fetch_bytes(*src, *at, out)?;

                core::str::from_utf8(&out[..n]).map_err(|_| Error::Malformed)
            }
        }
    }

    /// The text's bytes, without the UTF-8 check.
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
    /// How many bytes the blob has.
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

    /// The bytes, into a buffer you own. See [`Text::as_str`].
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

/// A string in a buffer of its own, `N` bytes at most. What a UDF declares when it wants text to keep -
/// an argument, a bin's value - without an allocator; derefs to `str`.
///
/// As a parameter type, `N` is the longest argument the UDF accepts: a longer one is rejected before the
/// body runs, from its reported length, without a copy. `&str` is the same with a capacity of 256; with the
/// `alloc` feature, `String` takes any length.
#[derive(Clone, Copy)]
pub struct Str<const N: usize> {
    len: usize,
    buf: [u8; N],
}

impl<const N: usize> Str<N> {
    /// The most bytes this holds.
    pub const CAPACITY: usize = N;

    /// Copy a string in. `Err(TooSmall(len))` when it does not fit.
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

impl<'a, const N: usize> From<&'a Str<N>> for Value<'a> {
    fn from(value: &'a Str<N>) -> Self {
        Value::String(Text::Guest(value.as_str()))
    }
}

/// Bytes in a buffer of their own, `N` at most: [`Str`] for a blob. Derefs to `[u8]`.
#[derive(Clone, Copy)]
pub struct Bytes<const N: usize> {
    len: usize,
    buf: [u8; N],
}

impl<const N: usize> Bytes<N> {
    /// The most bytes this holds.
    pub const CAPACITY: usize = N;

    /// Copy bytes in. `Err(TooSmall(len))` when they do not fit.
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

impl<'a, const N: usize> From<&'a Bytes<N>> for Value<'a> {
    fn from(value: &'a Bytes<N>) -> Self {
        Value::Blob(Blob::Guest(value.as_bytes()))
    }
}

impl Text<'_> {
    /// The text in a [`Str`] of its own, in one import call. `Err(TooSmall(len))` when it is longer than `N`,
    /// known before anything is copied.
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

    /// The text as a `String` of any length, in one import call. Needs the `alloc` feature.
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
    /// The bytes in a [`Bytes`] of their own. See [`Text::to_inline`].
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

    /// The bytes as a `Vec<u8>` of any length. Needs the `alloc` feature.
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
    /// How many elements a list has, or entries a map has.
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

    /// Walk the collection: every element of a list, or every key and value of a map, alternately.
    ///
    /// The import calls are hidden. The iterator fetches 128 values per call into a window on the guest's
    /// stack, so a ten-thousand-element list is 79 imports, not ten thousand. Each item is a
    /// `Result<Value, Error>`: a malformed collection surfaces as an `Err` item rather than as a quiet end.
    ///
    /// A nested list or map comes out as a [`Value::List`] / [`Value::Map`] that iterates the same way, and
    /// the walk continues after it.
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

    /// The collection's bytes as stored, header included. See [`Value::stored_bytes`].
    pub fn bytes<'b>(&self, buf: &'b mut [u8]) -> Result<&'b [u8], Error> {
        let (src, at) = self.position()?;
        let n = bytes_result(unsafe { raw_coll_bytes_at(src, at as i32, buf.as_mut_ptr(), buf.len() as i32) }, buf.len())?;

        Ok(&buf[..n])
    }

    /// Compare two collections' stored bytes. See [`Value::cmp_stored`].
    pub fn cmp_bytes(&self, other: &Coll) -> Result<core::cmp::Ordering, Error> {
        let (a, at_a) = self.position()?;
        let (b, at_b) = other.position()?;
        let c = unsafe { raw_coll_cmp_at(a, at_a as i32, b, at_b as i32) };

        if c < -1 {
            return Err(Error::from_status(c));
        }

        Ok(c.cmp(&0))
    }

    /// The map's entries, paired.
    #[must_use]
    pub fn entries(&self) -> Entries {
        Entries { values: self.iter() }
    }

    /// The value under a key of a map, a string or an integer, in one import call. `Err(Absent)` for no
    /// such key.
    pub fn get(&self, key: impl MapKey) -> Result<Value<'static>, Error> {
        key.lookup_in(self)
    }

    /// The element at `index` of a list, in one import call: the host steps over the elements before it
    /// where they are stored, so nothing before it is fetched. `Err(Absent)` past the end.
    pub fn at(&self, index: usize) -> Result<Value<'static>, Error> {
        let (src, at) = self.position()?;
        let mut raw = [0u8; VALUE_SZ];
        let index = i32::try_from(index).map_err(|_| Error::Absent)?;
        let rc = unsafe { raw_list_get(src, at as i32, index, raw.as_mut_ptr(), VALUE_SZ as i32) };

        if rc != OK {
            return Err(Error::from_status(rc));
        }

        Ok(Value::from_raw(&raw))
    }

    fn get_str(&self, key: &str) -> Result<Value<'static>, Error> {
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

        if rc != OK {
            return Err(Error::from_status(rc));
        }

        Ok(Value::from_raw(&raw))
    }

    fn get_int(&self, key: i64) -> Result<Value<'static>, Error> {
        let (src, at) = self.position()?;
        let mut raw = [0u8; VALUE_SZ];
        let rc = unsafe { raw_coll_get_i(src, at as i32, key, raw.as_mut_ptr(), VALUE_SZ as i32) };

        if rc != OK {
            return Err(Error::from_status(rc));
        }

        Ok(Value::from_raw(&raw))
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

/// The values of a collection, 128 per import call. See [`Coll::iter`].
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
        // Give the cursor's slot back. Iterators end last-in-first-out in practice, which is exactly when
        // the host can reclaim it; any other order is harmless and merely keeps the slot until the call ends.
        if self.cursor > 0 {
            unsafe { raw_cursor_close(self.cursor) };
        }
    }
}

/// A map's entries, key and value together. See [`Coll::entries`].
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

/// What any record can be asked: the one a record UDF is called on ([`Record`]) and either side of an
/// event ([`EventRecord`]). A helper that takes `&impl ReadRecord` reads a bin the same way in both.
///
/// [`bin`](ReadRecord::bin) is the form for a bin that must be there, `Err(Absent)` when it is not. The
/// typed forms are for a bin that may not be: `None` when it is absent or nil, `Err(WrongType)` when it
/// holds another kind.
pub trait ReadRecord {
    /// A bin, whatever it holds. `Err(Absent)` when the record has no such bin.
    fn bin(&self, name: &str) -> Result<Value<'static>, Error>;

    /// The record's modification count.
    fn generation(&self) -> i64;

    /// When the record was last written, in milliseconds since the Aerospike epoch (2010-01-01 UTC).
    fn last_update_time(&self) -> i64;

    /// A bin that may be absent: `None` when it is absent or nil.
    fn get(&self, name: &str) -> Result<Option<Value<'static>>, Error> {
        match self.bin(name) {
            Ok(Value::Nil) | Err(Error::Absent) => Ok(None),
            Ok(v) => Ok(Some(v)),
            Err(e) => Err(e),
        }
    }

    /// Whether the record has the bin.
    fn has(&self, name: &str) -> bool {
        self.bin(name).is_ok()
    }

    /// An integer bin, or `None`.
    fn int(&self, name: &str) -> Result<Option<i64>, Error> {
        self.get(name)?.map(|v| v.int()).transpose()
    }

    /// An integer bin, or `default` when it is absent or nil. A bin of another kind is still
    /// `Err(WrongType)`.
    fn int_or(&self, name: &str, default: i64) -> Result<i64, Error> {
        Ok(self.int(name)?.unwrap_or(default))
    }

    /// A double bin, or `default` when it is absent or nil.
    fn double_or(&self, name: &str, default: f64) -> Result<f64, Error> {
        Ok(self.double(name)?.unwrap_or(default))
    }

    /// A boolean bin, or `default` when it is absent or nil.
    fn bool_or(&self, name: &str, default: bool) -> Result<bool, Error> {
        Ok(self.bool(name)?.unwrap_or(default))
    }

    /// A double bin, or `None`.
    fn double(&self, name: &str) -> Result<Option<f64>, Error> {
        self.get(name)?.map(|v| v.double()).transpose()
    }

    /// A boolean bin, or `None`.
    fn bool(&self, name: &str) -> Result<Option<bool>, Error> {
        self.get(name)?.map(|v| v.bool()).transpose()
    }

    /// A string bin's text, or `None`.
    fn string(&self, name: &str) -> Result<Option<Text<'static>>, Error> {
        self.get(name)?.map(|v| v.string()).transpose()
    }

    /// A blob bin's bytes, or `None`.
    fn blob(&self, name: &str) -> Result<Option<Blob<'static>>, Error> {
        self.get(name)?.map(|v| v.blob()).transpose()
    }

    /// A list bin, or `None`.
    fn list(&self, name: &str) -> Result<Option<Coll>, Error> {
        self.get(name)?.map(|v| v.list()).transpose()
    }

    /// A map bin, or `None`.
    fn map(&self, name: &str) -> Result<Option<Coll>, Error> {
        self.get(name)?.map(|v| v.map()).transpose()
    }
}

/// The record a UDF was called on.
///
/// A record UDF, and the per-record half of a stream UDF, takes it as its first parameter, before the
/// call's arguments. It is not one of them: a client does not pass it and the function's signature export
/// does not list it. A function that does not touch the record leaves the parameter out.
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
/// [`save`](Record::save). [`delete`](Record::delete) is the exception: it needs no save.
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

    /// A bin, whatever it holds, in one import call. `Err(Absent)` when the record has no such bin.
    pub fn bin(&self, name: &str) -> Result<Value<'static>, Error> {
        let mut raw = [0u8; VALUE_SZ];
        let rc = unsafe { raw_bin_value(name.as_ptr(), name.len() as i32, raw.as_mut_ptr(), VALUE_SZ as i32) };

        if rc != OK {
            return Err(Error::from_status(rc));
        }

        Ok(Value::from_raw(&raw))
    }

    /// A bin that may be absent: `None` when it is absent or nil.
    pub fn get(&self, name: &str) -> Result<Option<Value<'static>>, Error> {
        ReadRecord::get(self, name)
    }

    /// Whether the record has the bin.
    #[must_use]
    pub fn has(&self, name: &str) -> bool {
        ReadRecord::has(self, name)
    }

    /// An integer bin, or `None` when the bin is absent or nil. A bin of another kind is `Err(WrongType)`.
    pub fn int(&self, name: &str) -> Result<Option<i64>, Error> {
        ReadRecord::int(self, name)
    }

    /// An integer bin, or `default` when it is absent or nil. A bin of another kind is still
    /// `Err(WrongType)`.
    pub fn int_or(&self, name: &str, default: i64) -> Result<i64, Error> {
        ReadRecord::int_or(self, name, default)
    }

    /// A double bin, or `default` when it is absent or nil.
    pub fn double_or(&self, name: &str, default: f64) -> Result<f64, Error> {
        ReadRecord::double_or(self, name, default)
    }

    /// A boolean bin, or `default` when it is absent or nil.
    pub fn bool_or(&self, name: &str, default: bool) -> Result<bool, Error> {
        ReadRecord::bool_or(self, name, default)
    }

    /// A double bin, or `None` when absent or nil.
    pub fn double(&self, name: &str) -> Result<Option<f64>, Error> {
        ReadRecord::double(self, name)
    }

    /// A boolean bin, or `None` when absent or nil.
    pub fn bool(&self, name: &str) -> Result<Option<bool>, Error> {
        ReadRecord::bool(self, name)
    }

    /// A string bin's text, or `None` when absent or nil.
    pub fn string(&self, name: &str) -> Result<Option<Text<'static>>, Error> {
        ReadRecord::string(self, name)
    }

    /// A blob bin's bytes, or `None` when absent or nil.
    pub fn blob(&self, name: &str) -> Result<Option<Blob<'static>>, Error> {
        ReadRecord::blob(self, name)
    }

    /// A list bin, or `None` when absent or nil.
    pub fn list(&self, name: &str) -> Result<Option<Coll>, Error> {
        ReadRecord::list(self, name)
    }

    /// A map bin, or `None` when absent or nil.
    pub fn map(&self, name: &str) -> Result<Option<Coll>, Error> {
        ReadRecord::map(self, name)
    }

    /// How many bins the record has.
    #[must_use]
    pub fn bin_count(&self) -> i64 {
        unsafe { numbins() }
    }

    //----------------------------------------------------------
    // Writing bins.
    //

    /// Write a value to a bin. **Staged until [`save`](Record::save).**
    ///
    /// A value read from the record is written back from its stored bytes without the guest decoding it,
    /// which is how a sub-document is copied from one bin to another. A built [`List`] / [`Map`] is handed
    /// over whole. A string or blob of the guest's is copied during the call, so it may be any `&str` - a
    /// literal, a buffer of the UDF's, a `String` - not only a `'static` one.
    ///
    /// Writing [`Value::Nil`] removes the bin, as [`remove`](Record::remove) does: a bin cannot hold nil.
    pub fn set<'a>(&mut self, name: &str, value: impl Into<Value<'a>>) -> Result<(), Error> {
        self.set_value(name, &value.into())
    }

    /// [`set`](Record::set) for a value held by reference.
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

    /// Write bytes to a bin as a blob. Staged, like [`set`](Record::set), which does the same for a
    /// `&[u8]`; this form says the kind at the call.
    pub fn set_blob(&mut self, name: &str, value: &[u8]) -> Result<(), Error> {
        self.set_value(name, &Value::Blob(Blob::Guest(value)))
    }

    /// Write a GeoJson document to a bin from its text. Staged, like [`set`](Record::set). A document
    /// written with `set` from a `&str` would be a string, which no geospatial index reads.
    pub fn set_geojson(&mut self, name: &str, value: &str) -> Result<(), Error> {
        self.set_value(name, &Value::GeoJson(Text::Guest(value)))
    }

    /// Remove a bin. Staged, like [`set`](Record::set).
    pub fn remove(&mut self, name: &str) -> Result<(), Error> {
        status(unsafe { raw_bin_remove(name.as_ptr(), name.len() as i32) })
    }

    /// Write a slice of integers to a bin as a list, **without building a value per element**.
    ///
    /// The host packs the slice straight into a LIST particle and applies it as one op, which takes effect
    /// immediately rather than being staged: [`save`](Record::save) is not needed.
    pub fn set_ints(&mut self, name: &str, values: &[i64]) -> Result<(), Error> {
        let bytes = core::mem::size_of_val(values);

        status(unsafe {
            raw_set_list_ints(name.as_ptr(), name.len() as i32, values.as_ptr().cast::<u8>(), bytes as i32)
        })
    }

    /// The counterpart of [`set_ints`](Record::set_ints) for doubles.
    pub fn set_doubles(&mut self, name: &str, values: &[f64]) -> Result<(), Error> {
        let bytes = core::mem::size_of_val(values);

        status(unsafe {
            raw_set_list_doubles(name.as_ptr(), name.len() as i32, values.as_ptr().cast::<u8>(), bytes as i32)
        })
    }

    //----------------------------------------------------------
    // Metadata.
    //

    /// Whether the record exists. A UDF can be called on a key that has no record yet.
    #[must_use]
    pub fn exists(&self) -> bool {
        unsafe { raw_rec_exists() == 1 }
    }

    /// The record's modification count.
    #[must_use]
    pub fn generation(&self) -> i64 {
        unsafe { gen() }
    }

    /// The record's remaining lifetime in seconds, or `None` when it never expires.
    #[must_use]
    pub fn ttl(&self) -> Option<u32> {
        match unsafe { raw_ttl() } {
            // Zero is how the server reports a record with no expiry.
            0 => None,
            value if value < 0 => None,
            value => u32::try_from(value).ok(),
        }
    }

    /// Set the record's time to live, in seconds. It is the TTL of the write, so it takes effect when
    /// [`save`](Record::save) writes the record.
    pub fn set_ttl(&mut self, seconds: u32) -> Result<(), Error> {
        status(unsafe { raw_set_ttl(seconds as i32) })
    }

    /// When the record was last written, in milliseconds since the Aerospike epoch (2010-01-01 UTC), which
    /// is how the server stores it. It is not on the same scale as [`now`], which counts from the Unix
    /// epoch.
    #[must_use]
    pub fn last_update_time(&self) -> i64 {
        unsafe { last_update() }
    }

    /// The record's stored size, in bytes.
    #[must_use]
    pub fn size(&self) -> i64 {
        unsafe { record_size() }
    }

    /// The record's set name, into a buffer you own.
    pub fn set_name<'b>(&self, out: &'b mut [u8]) -> Result<&'b str, Error> {
        let rc = unsafe { raw_set_name(out.as_mut_ptr(), out.len() as i32) };
        let written = bytes_result(rc, out.len())?;

        core::str::from_utf8(&out[..written]).map_err(|_| Error::Malformed)
    }

    /// The record's user key, when it was written with `send_key`: an integer, or a string or blob whose
    /// bytes go into `out`. `Err(Absent)` when the record carries none.
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

    /// The record's digest, into a buffer you own: 20 bytes, always present. What identifies a record a
    /// UDF wants to point at rather than ship - a top-k hit, say - since a client can fetch it by digest
    /// afterwards.
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
    /// nothing. A UDF that must not overwrite checks [`exists`](Record::exists) first; the record is
    /// locked for the whole call, so nothing can come between the check and the save.
    ///
    /// With nothing staged it is still a write: the server treats it as a touch, and the record's
    /// generation and last-update time move. On a key with no record it makes nothing, since a record
    /// has to have a bin, and the call is answered "key not found".
    pub fn save(&mut self) -> Result<(), Error> {
        status(unsafe {
            if raw_rec_exists() == 1 {
                raw_rec_update()
            } else {
                raw_rec_create()
            }
        })
    }

    /// Delete the record. Not staged: it needs no [`save`](Record::save) after it, and whatever was
    /// staged goes with the record. The server carries it out when the call returns, so for the rest of
    /// the call [`exists`](Record::exists) answers as it did before. `Err(Refused)` when the server
    /// could not do it.
    pub fn delete(&mut self) -> Result<(), Error> {
        status(unsafe { raw_rec_remove() })
    }
}

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
pub fn list(capacity: usize) -> Result<List, Error> {
    let handle = unsafe { list_new(capacity as i32) };

    if handle <= 0 {
        Err(Error::Refused)
    } else {
        Ok(List { handle })
    }
}

/// Start a map. Entries come out ordered by key, whatever order they go in.
pub fn map(capacity: usize) -> Result<Map, Error> {
    let handle = unsafe { map_new(capacity as i32) };

    if handle <= 0 {
        Err(Error::Refused)
    } else {
        Ok(Map { handle })
    }
}

/// A list under construction.
#[derive(Clone, Copy, Debug)]
pub struct List {
    handle: i32,
}

impl List {
    /// Append a value: a scalar, a guest string, blob or GeoJson, a built collection, or a value read from
    /// the record - which goes in by position, built by the server from its stored bytes.
    pub fn push<'a>(&self, value: impl Into<Value<'a>>) -> Result<&Self, Error> {
        let value = value.into();
        let rc = unsafe {
            match &value {
                Value::Int(i) => list_push_int(self.handle, *i),
                Value::String(Text::Guest(s)) => list_push_str(self.handle, s.as_ptr(), s.len() as i32),
                Value::List(Coll::Built(h)) | Value::Map(Coll::Built(h)) => list_push(self.handle, *h),
                other => match stored_position(other) {
                    Some((src, at)) => list_push_at(self.handle, src, at as i32),
                    None => match val_of(other) {
                        Some(v) => list_push(self.handle, v),
                        None => return Err(Error::WrongType),
                    },
                },
            }
        };

        status(rc).map(|()| self)
    }

    /// Append every item, 128 per import call instead of one each.
    ///
    /// An item is anything [`push`](List::push) takes, or a `Result<Value, Error>` as [`Coll::iter`] yields
    /// them, so `list.extend(stored.iter())` copies a stored list's elements by position in one call per
    /// window. The first `Err` item, or a value the host cannot take, stops the extend with that error; the
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

    /// The finished list, as a value to return or store.
    #[must_use]
    pub fn into_value(self) -> Value<'static> {
        Value::List(Coll::Built(self.handle))
    }
}

/// A map under construction, keyed by strings.
#[derive(Clone, Copy, Debug)]
pub struct Map {
    handle: i32,
}

/// What a map is keyed by: a string or an integer. [`Map::insert`] and [`Coll::get`] take either.
pub trait MapKey: sealed::Sealed {
    #[doc(hidden)]
    fn insert_into(self, map: &Map, value: Value<'_>) -> Result<(), Error>;
    #[doc(hidden)]
    fn lookup_in(self, coll: &Coll) -> Result<Value<'static>, Error>;
    #[doc(hidden)]
    fn contained_in(self, map: &Map) -> bool;
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for &str {}
    impl Sealed for i64 {}
}

impl MapKey for &str {
    fn insert_into(self, map: &Map, value: Value<'_>) -> Result<(), Error> {
        map.insert_str(self, value).map(|_| ())
    }

    fn lookup_in(self, coll: &Coll) -> Result<Value<'static>, Error> {
        coll.get_str(self)
    }

    fn contained_in(self, map: &Map) -> bool {
        unsafe { raw_map_has(map.handle, self.as_ptr(), self.len() as i32) == 1 }
    }
}

impl MapKey for i64 {
    fn insert_into(self, map: &Map, value: Value<'_>) -> Result<(), Error> {
        map.insert_int(self, value).map(|_| ())
    }

    fn lookup_in(self, coll: &Coll) -> Result<Value<'static>, Error> {
        coll.get_int(self)
    }

    fn contained_in(self, map: &Map) -> bool {
        unsafe { raw_map_has_i(map.handle, self) == 1 }
    }
}

impl Map {
    /// Insert a value under a key: a string or an integer. The same values as [`List::push`]. An entry
    /// already under the key is replaced.
    pub fn insert<'a>(&self, key: impl MapKey, value: impl Into<Value<'a>>) -> Result<&Self, Error> {
        key.insert_into(self, value.into())?;

        Ok(self)
    }

    /// Whether the map holds an entry under `key`. The map is asked where it is, on the host.
    #[must_use]
    pub fn contains_key(&self, key: impl MapKey) -> bool {
        key.contained_in(self)
    }

    fn insert_str(&self, key: &str, value: Value<'_>) -> Result<&Self, Error> {
        let k = key.as_ptr();
        let kl = key.len() as i32;
        let rc = unsafe {
            match &value {
                Value::Int(i) => map_put_str_int(self.handle, k, kl, *i),
                Value::String(Text::Guest(s)) => map_put_str_str(self.handle, k, kl, s.as_ptr(), s.len() as i32),
                Value::List(Coll::Built(h)) | Value::Map(Coll::Built(h)) => {
                    map_put_str_handle(self.handle, k, kl, *h)
                }
                other => match stored_position(other) {
                    Some((src, at)) => map_put_str_at(self.handle, k, kl, src, at as i32),
                    None => match val_of(other) {
                        Some(v) => map_put(self.handle, val_str(k, kl), v),
                        None => return Err(Error::WrongType),
                    },
                },
            }
        };

        status(rc).map(|()| self)
    }

    fn insert_int(&self, key: i64, value: Value<'_>) -> Result<&Self, Error> {
        let rc = unsafe {
            match &value {
                Value::List(Coll::Built(h)) | Value::Map(Coll::Built(h)) => map_put(self.handle, val_int(key), *h),
                other => match stored_position(other) {
                    Some((src, at)) => map_put_i_at(self.handle, key, src, at as i32),
                    None => match val_of(other) {
                        Some(v) => map_put(self.handle, val_int(key), v),
                        None => return Err(Error::WrongType),
                    },
                },
            }
        };

        status(rc).map(|()| self)
    }

    /// Insert every entry, 64 per import call instead of one each.
    ///
    /// An entry is a `(key, value)` pair of anything [`push`](List::push) takes - a string or integer key,
    /// in practice - or a `Result<(Value, Value), Error>` as [`Coll::entries`] yields them, so
    /// `map.extend(stored.entries())` copies a stored map by position. The first `Err`, or a value the host
    /// cannot take, stops the extend with that error; the entries before it are in.
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

    /// The finished map, as a value to return or store.
    #[must_use]
    pub fn into_value(self) -> Value<'static> {
        Value::Map(Coll::Built(self.handle))
    }
}

/// The finished list as a value, as [`List::into_value`] gives it.
impl From<List> for Value<'_> {
    fn from(value: List) -> Self {
        value.into_value()
    }
}

/// The finished map as a value, as [`Map::into_value`] gives it.
impl From<Map> for Value<'_> {
    fn from(value: Map) -> Self {
        value.into_value()
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
// so an owned string yielded by an iterator would be freed before the host read it.
pushable!(Value<'a>, i64, f64, bool, &'a str, &'a [u8], List, Map);

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

    // The host answers with how many it took; fewer than sent means one of them it could not.
    fn taken(rc: i32, sent: usize) -> Result<(), Error> {
        if rc < 0 {
            Err(Error::from_status(rc))
        } else if rc as usize == sent {
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

// A standalone host value for the general push / put path, or None for a value the host cannot take.
unsafe fn val_of(value: &Value<'_>) -> Option<i32> {
    let h = match value {
        Value::Nil => val_nil(),
        Value::Bool(b) => val_bool(i32::from(*b)),
        Value::Int(i) => val_int(*i),
        Value::Double(d) => val_double(*d),
        Value::String(Text::Guest(s)) => val_str(s.as_ptr(), s.len() as i32),
        Value::Blob(Blob::Guest(b)) => val_blob(b.as_ptr(), b.len() as i32),
        Value::GeoJson(Text::Guest(s)) => val_geojson(s.as_ptr(), s.len() as i32),
        _ => return None,
    };

    if h <= 0 {
        None
    } else {
        Some(h)
    }
}

//==========================================================
// Results and failure.
//

/// Make a value the call's result. The [`export!`] macro does this with the `Ok` a UDF returns; it is public
/// for code that bypasses the macro.
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

/// Fail the call with a message the client will see. The [`export!`] macro does this with the `Err` a UDF
/// returns.
pub fn fail(message: &str) {
    unsafe { raw_fail(message.as_ptr(), message.len() as i32) };
}

//==========================================================
// Metadata.
//

/// The server's clock, in milliseconds since the Unix epoch. Refused in an event handler, where a decision
/// that reads the clock cannot converge across clusters; a handler takes its times from the data.
pub fn now() -> Result<i64, Error> {
    let t = unsafe { current_time() };

    if t < 0 {
        return Err(Error::Refused);
    }

    Ok(t)
}

/// Write a line to the server log, under the UDF context.
pub fn log(level: Level, message: &str) {
    unsafe { raw_log(level as i32, message.as_ptr(), message.len() as i32) };
}

//==========================================================
// Arguments.
//

/// How many arguments a call may carry. Beyond this the call is rejected before the UDF runs.
pub const MAX_ARGS: usize = 32;

/// The arguments of a call, decoded in one import call.
///
/// The [`export!`] macro turns these into the typed parameters of a UDF; a parameter of this type takes the
/// remaining arguments as they are, for a variadic function.
#[derive(Clone, Copy)]
pub struct Args {
    values: [[u8; VALUE_SZ]; MAX_ARGS],
    start: usize,
    len: usize,
}

impl Args {
    /// Decode the call's arguments from the handle the host passed. Used by [`export!`].
    #[must_use]
    pub fn from_handle(handle: i32, argc: i32) -> Args {
        let mut args = Args {
            values: [[0; VALUE_SZ]; MAX_ARGS],
            start: 0,
            len: 0,
        };

        // A call with no arguments gets handle 0: nothing to fetch.
        if handle <= 0 || argc <= 0 {
            return args;
        }

        // One import for every argument: the host writes their values straight from the list it holds.
        let rc = unsafe {
            raw_args_values(handle, args.values.as_mut_ptr().cast::<u8>(), (MAX_ARGS * VALUE_SZ) as i32)
        };

        if rc > 0 {
            args.len = rc as usize;
        }

        args
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

    /// The argument at `index`, or `None` past the end.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<Value<'static>> {
        let i = self.start + index;

        if i < self.len {
            Some(Value::from_raw(&self.values[i]))
        } else {
            None
        }
    }

    /// Every argument, in order.
    pub fn iter(&self) -> impl Iterator<Item = Value<'static>> + '_ {
        (0..self.len()).map(move |i| Value::from_raw(&self.values[self.start + i]))
    }

    /// The arguments from `index` on, as their own view.
    #[must_use]
    pub fn from(&self, index: usize) -> Args {
        let mut rest = *self;

        rest.start = (self.start + index).min(self.len);

        rest
    }
}

/// Why an argument could not become the parameter it was declared as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArgError {
    /// The call did not supply it.
    Missing,
    /// It is not the declared kind: what was wanted, and what came.
    Expected(&'static str, &'static str),
    /// A string or blob longer than the parameter allows: how long it is, and the most the parameter takes.
    TooLong { len: usize, max: usize },
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
            Some(other) => Err(ArgError::Expected("an integer", other.kind())),
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
            Some(other) => Err(ArgError::Expected("a double", other.kind())),
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
            Some(other) => Err(ArgError::Expected("a boolean", other.kind())),
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
                        .map_err(|_| ArgError::Expected("a utf-8 string", "a string")),
                    Err(Error::TooSmall(n)) => Err(ArgError::TooLong { len: n, max: buf.len() }),
                    Err(_) => Err(ArgError::Expected("a string", "a malformed value")),
                }
            }
            Some(other) => Err(ArgError::Expected("a string", other.kind())),
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
            Some(other) => return Err(ArgError::Expected("bytes", other.kind())),
            None => return Err(ArgError::Missing),
        };

        if len > buf.len() {
            return Err(ArgError::TooLong { len, max: buf.len() });
        }

        match fetch_bytes(src, at, buf) {
            Ok(n) => Ok(&buf[..n]),
            Err(Error::TooSmall(n)) => Err(ArgError::TooLong { len: n, max: buf.len() }),
            Err(_) => Err(ArgError::Expected("bytes", "a malformed value")),
        }
    }
}

// Why a string or blob argument could not become the owned parameter: too long, not UTF-8, or malformed.
fn owned_arg_error(e: Error, max: usize, wanted: &'static str) -> ArgError {
    match e {
        Error::TooSmall(len) => ArgError::TooLong { len, max },
        Error::Malformed => ArgError::Expected("a utf-8 string", wanted),
        _ => ArgError::Expected(wanted, "a malformed value"),
    }
}

/// A `str` parameter of at most `N` bytes, copied into the [`Str`]. A longer string argument is rejected
/// as [`ArgError::TooLong`].
impl<'a, const N: usize> FromArg<'a> for Str<N> {
    const NAME: &'static str = "str";
    const CAPACITY: usize = N;

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::String(text)) => text.to_inline::<N>().map_err(|e| owned_arg_error(e, N, "a string")),
            Some(other) => Err(ArgError::Expected("a string", other.kind())),
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
            Some(Value::Blob(blob)) => blob.to_inline::<N>().map_err(|e| owned_arg_error(e, N, "bytes")),
            Some(other) => Err(ArgError::Expected("bytes", other.kind())),
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
            Some(Value::String(text)) => text.to_string().map_err(|e| owned_arg_error(e, usize::MAX, "a string")),
            Some(other) => Err(ArgError::Expected("a string", other.kind())),
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
            Some(Value::Blob(blob)) => blob.to_vec().map_err(|e| owned_arg_error(e, usize::MAX, "bytes")),
            Some(other) => Err(ArgError::Expected("bytes", other.kind())),
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
            Some(other) => Err(ArgError::Expected("a list or a map", other.kind())),
            None => Err(ArgError::Missing),
        }
    }
}

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

/// A fixed-size message buffer, so an error can name what went wrong without an allocator.
#[doc(hidden)]
pub struct Message {
    buf: [u8; 256],
    len: usize,
}

impl Message {
    /// An empty message.
    #[must_use]
    pub fn new() -> Message {
        Message {
            buf: [0; 256],
            len: 0,
        }
    }

    /// The text written so far: 256 bytes at most, anything written past that having been dropped.
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

/// Appends text, so `write!` works on a message. Never fails: what does not fit in the 256 bytes is
/// dropped.
impl core::fmt::Write for Message {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let room = self.buf.len() - self.len;
        let n = s.len().min(room);

        self.buf[self.len..self.len + n].copy_from_slice(&s.as_bytes()[..n]);
        self.len += n;

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
#[doc(hidden)]
pub struct Signature {
    buf: [u8; 512],
    len: usize,
}

impl Signature {
    /// An empty signature, `const` so that it can initialise a `static`. The text is written by the first
    /// call that asks for it.
    #[must_use]
    pub const fn new() -> Signature {
        Signature { buf: [0; 512], len: 0 }
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
        let end = self.len.min(self.buf.len() - 1);

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
        let n = s.len().min(room);

        self.buf[self.len..self.len + n].copy_from_slice(&s.as_bytes()[..n]);
        self.len += n;

        Ok(())
    }
}

/// Reject a call over one of its arguments, with a message naming the function, the position, the parameter
/// and what was wrong. Used by [`export!`].
#[doc(hidden)]
pub fn reject_argument(function: &str, index: usize, parameter: &str, error: ArgError) -> i32 {
    let mut m = Message::new();

    let _ = match error {
        ArgError::Missing => write!(m, "{function}: argument {index} ({parameter}) is missing"),
        ArgError::Expected(wanted, got) => write!(
            m,
            "{function}: argument {index} ({parameter}): expected {wanted}, got {got}"
        ),
        ArgError::TooLong { len, max } => write!(
            m,
            "{function}: argument {index} ({parameter}) is {len} bytes, at most {max}"
        ),
    };

    fail(m.as_str());

    1
}

/// Reject a call with too many arguments. Used by [`export!`].
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

/// One side of an event: a read-only record with its metadata. Its bins are read through the event's own
/// import, never the ordinary record natives, which in an event answer "nothing".
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
    /// A bin, as [`Record::bin`] reads one: `Err(Absent)` when the record has no such bin.
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

    /// A bin that may be absent: `None` when it is absent or nil.
    pub fn get(&self, name: &str) -> Result<Option<Value<'static>>, Error> {
        ReadRecord::get(self, name)
    }

    /// Whether the record has the bin.
    #[must_use]
    pub fn has(&self, name: &str) -> bool {
        ReadRecord::has(self, name)
    }

    /// An integer bin, or `None` when the bin is absent or nil. A bin of another kind is `Err(WrongType)`.
    pub fn int(&self, name: &str) -> Result<Option<i64>, Error> {
        ReadRecord::int(self, name)
    }

    /// An integer bin, or `default` when it is absent or nil. A bin of another kind is still
    /// `Err(WrongType)`.
    pub fn int_or(&self, name: &str, default: i64) -> Result<i64, Error> {
        ReadRecord::int_or(self, name, default)
    }

    /// A double bin, or `default` when it is absent or nil.
    pub fn double_or(&self, name: &str, default: f64) -> Result<f64, Error> {
        ReadRecord::double_or(self, name, default)
    }

    /// A boolean bin, or `default` when it is absent or nil.
    pub fn bool_or(&self, name: &str, default: bool) -> Result<bool, Error> {
        ReadRecord::bool_or(self, name, default)
    }

    /// A double bin, or `None` when absent or nil.
    pub fn double(&self, name: &str) -> Result<Option<f64>, Error> {
        ReadRecord::double(self, name)
    }

    /// A boolean bin, or `None` when absent or nil.
    pub fn bool(&self, name: &str) -> Result<Option<bool>, Error> {
        ReadRecord::bool(self, name)
    }

    /// A string bin's text, or `None` when absent or nil.
    pub fn string(&self, name: &str) -> Result<Option<Text<'static>>, Error> {
        ReadRecord::string(self, name)
    }

    /// A blob bin's bytes, or `None` when absent or nil.
    pub fn blob(&self, name: &str) -> Result<Option<Blob<'static>>, Error> {
        ReadRecord::blob(self, name)
    }

    /// A list bin, or `None` when absent or nil.
    pub fn list(&self, name: &str) -> Result<Option<Coll>, Error> {
        ReadRecord::list(self, name)
    }

    /// A map bin, or `None` when absent or nil.
    pub fn map(&self, name: &str) -> Result<Option<Coll>, Error> {
        ReadRecord::map(self, name)
    }

    /// The names of this record's bins, as a stored list to iterate.
    #[must_use]
    pub fn bin_names(&self) -> Coll {
        self.bin_names
    }

    /// The record's generation. Zero on both sides of an `xdr-merge`: each cluster sets its own, and a merge
    /// must not read it.
    #[must_use]
    pub fn generation(&self) -> i64 {
        self.generation
    }

    /// When the record was last written. Zero on both sides of an `xdr-merge`, for the same reason as
    /// [`generation`](EventRecord::generation).
    #[must_use]
    pub fn last_update_time(&self) -> i64 {
        self.last_update_time
    }

    /// The time to live in seconds; 0 for a record that does not expire.
    #[must_use]
    pub fn ttl(&self) -> i64 {
        self.ttl
    }

    /// The XDR source id of the DC that wrote this version.
    #[must_use]
    pub fn source_id(&self) -> i64 {
        self.source_id
    }

    /// The names of the bins this side deleted: for the incoming write, its bin-delete ops; for the
    /// current record, its bin tombstones. A deleted bin reads as absent, so this is the only way to tell
    /// "never had it" from "removed it", and
    /// [`bin_last_update_time`](EventRecord::bin_last_update_time) says when.
    #[must_use]
    pub fn deleted_bins(&self) -> Coll {
        self.deleted_bins
    }

    /// Whether this side deleted any bin.
    #[must_use]
    pub fn deleted_any(&self) -> bool {
        !self.deleted_bins.is_empty()
    }

    /// When this side last wrote the bin, when the server knows per bin (bin convergence keeps a time per
    /// bin); the record's last update time otherwise. This is what decides a bin under the `bin-lut` policy, so a merge
    /// that orders the sides per bin agrees with the policy on the bins it does not merge.
    #[must_use]
    pub fn bin_last_update_time(&self, name: &str) -> i64 {
        self.bin_last_update_times
            .and_then(|m| m.get(name).ok())
            .and_then(|v| v.int().ok())
            .unwrap_or(self.last_update_time)
    }

    /// The source id that wrote the bin, per bin when known; the record's otherwise.
    #[must_use]
    pub fn bin_source_id(&self, name: &str) -> i64 {
        self.bin_source_ids
            .and_then(|m| m.get(name).ok())
            .and_then(|v| v.int().ok())
            .unwrap_or(self.source_id)
    }

    /// Whether this is the incoming write and it is a record delete, in which case it has no bins. Always
    /// false of the current record.
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

    /// The names of the incoming bins that would lose under bin convergence, as a stored list. Empty for
    /// the current record, whose bins are not the ones being applied.
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
    /// unless the map already has it. The bins travel by position, never through the guest.
    ///
    /// This prefers one side, so a handler that uses it on `current` is not symmetric: the two DCs would
    /// resolve the same pair to different records. [`Pair::with_newer`] is the symmetric form; use this one
    /// when the side is chosen by something both DCs agree on, such as the source id.
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
}

/// The two sides of a merge, read bin by bin into a [`Both`]. Made by [`Pair::of`].
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

    /// An integer bin on each side; a side where it is absent or nil is `None`. `Err(WrongType)` when either
    /// side holds another kind.
    pub fn int(&self, name: &str) -> Result<Both<i64>, Error> {
        Ok(self.both(name, self.current.int(name)?, self.incoming.int(name)?))
    }

    /// A double bin on each side; a side where it is absent or nil is `None`.
    pub fn double(&self, name: &str) -> Result<Both<f64>, Error> {
        Ok(self.both(name, self.current.double(name)?, self.incoming.double(name)?))
    }

    /// A boolean bin on each side; a side where it is absent or nil is `None`.
    pub fn bool(&self, name: &str) -> Result<Both<bool>, Error> {
        Ok(self.both(name, self.current.bool(name)?, self.incoming.bool(name)?))
    }

    /// A string bin's text on each side; a side where it is absent or nil is `None`.
    pub fn string(&self, name: &str) -> Result<Both<Text<'static>>, Error> {
        Ok(self.both(name, self.current.string(name)?, self.incoming.string(name)?))
    }

    /// A blob bin's bytes on each side; a side where it is absent or nil is `None`.
    pub fn blob(&self, name: &str) -> Result<Both<Blob<'static>>, Error> {
        Ok(self.both(name, self.current.blob(name)?, self.incoming.blob(name)?))
    }

    /// A list bin on each side; a side where it is absent or nil is `None`.
    pub fn list(&self, name: &str) -> Result<Both<Coll>, Error> {
        Ok(self.both(name, self.current.list(name)?, self.incoming.list(name)?))
    }

    /// A map bin on each side; a side where it is absent or nil is `None`.
    pub fn map(&self, name: &str) -> Result<Both<Coll>, Error> {
        Ok(self.both(name, self.current.map(name)?, self.incoming.map(name)?))
    }

    /// The bin as a value on each side; absent sides are `None`.
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
    pub fn with_newer(&self, overrides: Map) -> Result<Map, Error> {
        let mut buf = [0u8; 32];
        let have = overrides.into_value();
        let have = have.map()?;

        for side in [&self.current, &self.incoming] {
            for name in side.bin_names().iter() {
                let name = name?;
                let name = name.as_str(&mut buf)?;

                if have.get(name).is_ok() {
                    continue;
                }

                if let Some(value) = self.bin(name)?.newer() {
                    overrides.insert(name, value)?;
                }
            }
        }

        Ok(overrides)
    }

    /// Whether the incoming side wrote `name` later, per bin: the order the `bin-lut` policy uses.
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

    /// The side written later; whichever exists when only one does.
    pub fn newer(self) -> Option<T> {
        if self.incoming_is_newer() {
            self.incoming.or(self.current)
        } else {
            self.current.or(self.incoming)
        }
    }

    /// The side written earlier; whichever exists when only one does.
    pub fn older(self) -> Option<T> {
        if self.incoming_is_newer() {
            self.current.or(self.incoming)
        } else {
            self.incoming.or(self.current)
        }
    }

    /// Both sides, as `(older, newer)`: the order both DCs agree on, for a merge whose result depends on
    /// the order its inputs are taken in.
    pub fn ordered(self) -> (Option<T>, Option<T>) {
        if self.incoming_is_newer() {
            (self.current, self.incoming)
        } else {
            (self.incoming, self.current)
        }
    }

    /// The current value when there is one, else the incoming.
    pub fn current_or_incoming(self) -> Option<T> {
        self.current.or(self.incoming)
    }

    /// The incoming value when there is one, else the current.
    pub fn incoming_or_current(self) -> Option<T> {
        self.incoming.or(self.current)
    }

    /// Combine the two when both exist; the one that exists otherwise.
    pub fn merge(self, f: impl FnOnce(T, T) -> T) -> Option<T> {
        match (self.current, self.incoming) {
            (Some(l), Some(r)) => Some(f(l, r)),
            (l, r) => l.or(r),
        }
    }
}

impl<T: PartialOrd> Both<T> {
    /// The larger of the two; the one that exists when only one does.
    pub fn max(self) -> Option<T> {
        self.merge(|l, r| if r > l { r } else { l })
    }

    /// The smaller of the two; the one that exists when only one does.
    pub fn min(self) -> Option<T> {
        self.merge(|l, r| if r < l { r } else { l })
    }
}

impl Both<i64> {
    /// The sum, a missing side counting as zero.
    #[must_use]
    pub fn sum(self) -> i64 {
        self.current.unwrap_or(0) + self.incoming.unwrap_or(0)
    }
}

impl Both<f64> {
    /// The sum, a missing side counting as zero.
    #[must_use]
    pub fn sum(self) -> f64 {
        self.current.unwrap_or(0.0) + self.incoming.unwrap_or(0.0)
    }
}

/// What an `xdr-merge` handler answers.
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
    let args = Args::from_handle(args_handle, argc);
    let merge = args.get(0).ok_or(Error::Custom("event: the merge description is missing"))?.map()?;

    let mut kbuf = [0u8; 16];
    let bin_convergence = merge.get("policy").ok().is_some_and(|v| v.as_str(&mut kbuf) == Ok("bin-lut"));
    // A stored list that is empty: the server always sends `empty` for this purpose.
    let empty = merge.get("empty")?.list()?;

    let side = |which: i32, key: &str| -> Result<EventRecord, Error> {
        let m = merge.get(key)?.map()?;
        let bin_names = m.get("bins")?.list()?;
        let incoming = which == 1;
        let flag = |name: &str| matches!(m.get(name), Ok(Value::Bool(true)));

        Ok(EventRecord {
            which,
            // Absent for an xdr-merge side: each cluster sets its own, and a merge must not read them.
            generation: m.get("gen").ok().and_then(|v| v.int().ok()).unwrap_or(0),
            last_update_time: m.get("lut").ok().and_then(|v| v.int().ok()).unwrap_or(0),
            ttl: m.get("ttl").ok().and_then(|v| v.int().ok()).unwrap_or(0),
            source_id: m.get("src-id")?.int()?,
            bin_names,
            deleted_bins: match m.get("deleted") {
                Ok(v) => v.list()?,
                Err(_) => empty,
            },
            bin_last_update_times: m.get("bin-luts").ok().and_then(|v| v.map().ok()),
            bin_source_ids: m.get("bin-srcs").ok().and_then(|v| v.map().ok()),
            delete: incoming && flag("deleted-record"),
            partial: incoming && flag("partial"),
            tombstone: !incoming && flag("tombstone"),
            bin_convergence,
            lost: if incoming { merge.get("lost")?.list()? } else { empty },
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

            out.insert("verdict", "record")?.insert("bins", bins)?;

            if let Some(ttl) = ttl {
                out.insert("ttl", i64::from(ttl))?;
            }

            result(&out.into_value())
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
/// A UDF is an ordinary function with typed parameters and a `Result<Value, Error>` body:
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
/// Parameter types: `i64`, `f64`, `bool`; `&str` and `&[u8]` (copied into a 256-byte buffer of the
/// parameter's own); [`Str<N>`] and [`Bytes<N>`] for a string or blob of up to `N` bytes; `String` and
/// `Vec<u8>` for any length, with the `alloc` feature; [`Value`] for anything, [`Coll`] for a list or map,
/// `Option<T>` for an argument that may be absent or nil, and [`Args`] as the last parameter for every
/// remaining argument. A string or blob longer than its parameter allows is rejected from its reported
/// length, before any copy. A call with more arguments than parameters is rejected unless the last parameter
/// is [`Args`].
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
        fn $name:ident($($params:tt)*) -> Result<Value, Error> $body:block
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
                $crate::udf_call!(writable, $name, args_handle, argc, $body, result, $($params)*)
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
/// afterwards and returning `Result<Value, Error>`, its `Ok` being the node's partial. Both take the call's
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
            end($($earg:ident : $ety:ty),* $(,)?) -> Result<Value, Error> $ebody:block
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
                    $crate::udf_call!(readable, $name, args_handle, argc, $rbody, unit, $($rparams)*)
                }

                #[export_name = concat!(stringify!($name), "_end")]
                pub extern "C" fn end(args_handle: i32, argc: i32) -> i32 {
                    $crate::udf_body!($name, args_handle, argc, [], ($($earg : $ety),*), $ebody, result)
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
///         out.insert("visits", pair.int("visits")?.max())?;
///         Ok(Resolution::Record { bins: pair.with_newer(out)?, ttl: None })
///     }
/// }
/// ```
///
/// The handler takes the record this cluster holds and the one that arrived over XDR, in that order, and
/// nothing else: what there is to know about the merge is asked of the record it is true of
/// ([`EventRecord::is_delete`], [`EventRecord::is_tombstone`] and their neighbours). The parameter types
/// are part of the arm, so a handler with another signature does not compile. The macro
/// exports the handler under its name, which `event-set` refers to, and `__udf_sig_<name>` reading
/// `event xdr-merge/1 ...`, which `event-set` checks before binding: the number is the version of the
/// contract this SDK was written against, and a server that does not serve it refuses the binding.
///
/// A handler must be a pure function of its inputs - no writes, no clock - and symmetric in them: two DCs
/// that resolve the same pair in opposite order must produce the same record, or they diverge. The dry run
/// (`execute_udf(key, "__event__", "xdr-merge", ...)`) reports whether a handler is: `symmetric`,
/// `idempotent` and, given a third record, `associative` - the three laws a convergent merge obeys.
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
    (writable, $name:ident, $h:expr, $argc:expr, $body:block, $outcome:ident,
     $rec:ident : &mut Record $(, $arg:ident : $ty:ty)* $(,)?) => {
        $crate::udf_body!($name, $h, $argc, [mut $rec], ($($arg : $ty),*), $body, $outcome)
    };
    ($access:ident, $name:ident, $h:expr, $argc:expr, $body:block, $outcome:ident,
     $rec:ident : &Record $(, $arg:ident : $ty:ty)* $(,)?) => {
        $crate::udf_body!($name, $h, $argc, [ref $rec], ($($arg : $ty),*), $body, $outcome)
    };
    ($access:ident, $name:ident, $h:expr, $argc:expr, $body:block, $outcome:ident,
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
     $outcome:ident) => {{
        $crate::udf_record!([$($record)*]);

        let __args = $crate::Args::from_handle($args_handle, $argc);
        let mut __index: usize = 0;
        let mut __rest = false;

        $(
            // A buffer named like the parameter, which the parameter then shadows: a string parameter
            // borrows it, every other kind ignores it.
            let mut $arg: [u8; $crate::ARG_BUF] = [0; $crate::ARG_BUF];
            let $arg: $ty = match <$ty as $crate::FromArg>::from_arg(&__args, __index, &mut $arg) {
                Ok(value) => value,
                Err(e) => return $crate::reject_argument(stringify!($name), __index, stringify!($arg), e),
            };

            __rest |= <$ty as $crate::FromArg>::REST;
            __index += 1;
        )*

        if !__rest && __args.len() > __index {
            return $crate::reject_arity(stringify!($name), __index, $argc);
        }

        $crate::udf_outcome!($outcome, (|| $body)())
    }};
}

/// Deliver a UDF's outcome. Not for direct use.
#[doc(hidden)]
#[macro_export]
macro_rules! udf_outcome {
    (result, $r:expr) => {{
        let __r: ::core::result::Result<$crate::Value<'_>, $crate::Error> = $r;

        match __r {
            Ok(value) => match $crate::result(&value) {
                Ok(()) => 0,
                Err(e) => {
                    $crate::fail(e.message());
                    1
                }
            },
            Err(e) => {
                $crate::fail(e.message());
                1
            }
        }
    }};
    (unit, $r:expr) => {{
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
        assert_eq!(m.as_str().len(), 256);
    }
}
