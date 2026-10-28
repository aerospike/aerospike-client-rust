//! Writing an Aerospike WASM UDF for the WAMR engine.
//!
//! # The shape of a UDF
//!
//! ```ignore
//! use aerospike_udf_sdk_wasm::{self as as_udf, export, Error, Value};
//!
//! export! {
//!     /// Move `amount` out of this record's balance, unless `dry_run`.
//!     fn withdraw(amount: i64, dry_run: Option<bool>) -> Result<Value, Error> {
//!         let balance = as_udf::bin("balance")?.int()?;
//!
//!         if balance < amount {
//!             return Err(Error::Custom("insufficient funds"));
//!         }
//!
//!         if dry_run != Some(true) {
//!             as_udf::set("balance", balance - amount)?;
//!             as_udf::update()?;
//!         }
//!
//!         Ok(Value::Int(balance - amount))
//!     }
//! }
//! ```
//!
//! Three ideas carry the whole API:
//!
//! - **Every read is a [`Value`]**, the guest-side analogue of the client's `Value`: [`bin`] returns one, a
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

    // Events: a bin of one of the event's two records, 0 the local one and 1 the remote.
    #[link_name = "event_bin_value"]
    fn raw_event_bin_value(which: i32, name: *const u8, name_len: i32, out: *mut u8, out_cap: i32) -> i32;

    // A stored value's packed bytes, and two stored values compared byte by byte.
    #[link_name = "coll_bytes_at"]
    fn raw_coll_bytes_at(src: i32, at: i32, out: *mut u8, out_cap: i32) -> i32;
    #[link_name = "coll_cmp_at"]
    fn raw_coll_cmp_at(src1: i32, at1: i32, src2: i32, at2: i32) -> i32;

    // Timers on the record in hand.
    #[link_name = "timer_set"]
    fn raw_timer_set(name: *const u8, name_len: i32, delay_ms: i64) -> i32;
    #[link_name = "timer_cancel"]
    fn raw_timer_cancel(name: *const u8, name_len: i32) -> i32;
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
    Str(Text<'a>),
    Blob(Blob<'a>),
    /// A GeoJson document, which reads and writes as its text. Distinct from [`Value::Str`] on purpose: the
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
            4 => Value::Str(Text::Stored { src, at, len }),
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
            Value::Str(_) => "a string",
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

    pub fn int(&self) -> Result<i64, Error> {
        match self {
            Value::Int(i) => Ok(*i),
            _ => Err(Error::WrongType),
        }
    }

    pub fn double(&self) -> Result<f64, Error> {
        match self {
            Value::Double(d) => Ok(*d),
            _ => Err(Error::WrongType),
        }
    }

    pub fn bool(&self) -> Result<bool, Error> {
        match self {
            Value::Bool(b) => Ok(*b),
            _ => Err(Error::WrongType),
        }
    }

    /// The text of a string or a GeoJson document.
    pub fn text(&self) -> Result<Text<'a>, Error> {
        match self {
            Value::Str(t) | Value::GeoJson(t) => Ok(*t),
            _ => Err(Error::WrongType),
        }
    }

    pub fn blob(&self) -> Result<Blob<'a>, Error> {
        match self {
            Value::Blob(b) => Ok(*b),
            _ => Err(Error::WrongType),
        }
    }

    pub fn list(&self) -> Result<Coll, Error> {
        match self {
            Value::List(c) => Ok(*c),
            _ => Err(Error::WrongType),
        }
    }

    pub fn map(&self) -> Result<Coll, Error> {
        match self {
            Value::Map(c) => Ok(*c),
            _ => Err(Error::WrongType),
        }
    }

    /// A string's or GeoJson document's text, into a buffer you own. See [`Text::as_str`].
    pub fn as_str<'b>(&'b self, out: &'b mut [u8]) -> Result<&'b str, Error> {
        match self {
            Value::Str(t) | Value::GeoJson(t) => t.as_str(out),
            _ => Err(Error::WrongType),
        }
    }

    /// Whether this is [`Value::Nil`].
    #[must_use]
    pub fn is_nil(&self) -> bool {
        matches!(self, Value::Nil)
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

impl From<i64> for Value<'_> {
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}

impl From<f64> for Value<'_> {
    fn from(value: f64) -> Self {
        Value::Double(value)
    }
}

impl From<bool> for Value<'_> {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(value: &'a str) -> Self {
        Value::Str(Text::Guest(value))
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
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Blob::Stored { len, .. } => *len,
            Blob::Guest(b) => b.len(),
        }
    }

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

    #[must_use]
    pub fn as_str(&self) -> &str {
        // Only ever filled from a `&str`, or from bytes that passed the UTF-8 check on the way in.
        core::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

impl<const N: usize> core::ops::Deref for Str<N> {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl<const N: usize> AsRef<str> for Str<N> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<const N: usize> PartialEq for Str<N> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<const N: usize> PartialEq<str> for Str<N> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl<const N: usize> PartialEq<&str> for Str<N> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<const N: usize> core::fmt::Debug for Str<N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(self.as_str(), f)
    }
}

impl<const N: usize> core::fmt::Display for Str<N> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'a, const N: usize> From<&'a Str<N>> for Value<'a> {
    fn from(value: &'a Str<N>) -> Self {
        Value::Str(Text::Guest(value.as_str()))
    }
}

/// Bytes in a buffer of their own, `N` at most: [`Str`] for a blob. Derefs to `[u8]`.
#[derive(Clone, Copy)]
pub struct Bytes<const N: usize> {
    len: usize,
    buf: [u8; N],
}

impl<const N: usize> Bytes<N> {
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

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl<const N: usize> core::ops::Deref for Bytes<N> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<const N: usize> AsRef<[u8]> for Bytes<N> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<const N: usize> PartialEq for Bytes<N> {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl<const N: usize> PartialEq<[u8]> for Bytes<N> {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}

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

    /// The value under a string key of a map, in one import call. `Err(Absent)` for no such key.
    pub fn get(&self, key: &str) -> Result<Value<'static>, Error> {
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

    /// [`get`](Coll::get) for a map keyed by integers.
    pub fn get_i(&self, key: i64) -> Result<Value<'static>, Error> {
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
// Bins.
//

/// A bin, whatever it holds, in one import call. `Err(Absent)` when the record has no such bin.
pub fn bin(name: &str) -> Result<Value<'static>, Error> {
    let mut raw = [0u8; VALUE_SZ];
    let rc = unsafe { raw_bin_value(name.as_ptr(), name.len() as i32, raw.as_mut_ptr(), VALUE_SZ as i32) };

    if rc != OK {
        return Err(Error::from_status(rc));
    }

    Ok(Value::from_raw(&raw))
}

/// Write a value to a bin. **Staged until [`update`] or [`create`] commits it.**
///
/// A value read from the record is written back from its stored bytes without the guest decoding it, which
/// is how a sub-document is copied from one bin to another. A built [`List`] / [`Map`] is handed over whole.
/// A string or blob of the guest's is copied during the call, so it may be any `&str` - a literal, a buffer
/// of the UDF's, a `String` - not only a `'static` one.
pub fn set<'a>(name: &str, value: impl Into<Value<'a>>) -> Result<(), Error> {
    set_value(name, &value.into())
}

/// [`set`] for a value held by reference.
pub fn set_value(name: &str, value: &Value<'_>) -> Result<(), Error> {
    let n = name.as_ptr();
    let nl = name.len() as i32;
    let rc = unsafe {
        match value {
            Value::Nil => raw_set_nil(n, nl),
            Value::Bool(b) => raw_set_bool(n, nl, i32::from(*b)),
            Value::Int(i) => raw_set_int(n, nl, *i),
            Value::Double(d) => raw_set_double(n, nl, *d),
            Value::Str(Text::Guest(s)) => raw_set_str(n, nl, s.as_ptr(), s.len() as i32),
            Value::GeoJson(Text::Guest(s)) => raw_set_geojson(n, nl, s.as_ptr(), s.len() as i32),
            Value::Blob(Blob::Guest(b)) => raw_set_blob(n, nl, b.as_ptr(), b.len() as i32),
            Value::Str(Text::Stored { src, at, .. })
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

/// Delete a bin.
pub fn remove(name: &str) -> Result<(), Error> {
    status(unsafe { raw_bin_remove(name.as_ptr(), name.len() as i32) })
}

/// Write a slice of integers to a bin as a list, **without building a value per element**.
///
/// The host packs the slice straight into a LIST particle and applies it as one op, which takes effect
/// immediately rather than being staged: [`update`] is not needed. Refused in an aggregation.
pub fn set_list_ints(name: &str, values: &[i64]) -> Result<(), Error> {
    let bytes = core::mem::size_of_val(values);

    status(unsafe {
        raw_set_list_ints(name.as_ptr(), name.len() as i32, values.as_ptr().cast::<u8>(), bytes as i32)
    })
}

/// The counterpart of [`set_list_ints`] for doubles.
pub fn set_list_doubles(name: &str, values: &[f64]) -> Result<(), Error> {
    let bytes = core::mem::size_of_val(values);

    status(unsafe {
        raw_set_list_doubles(name.as_ptr(), name.len() as i32, values.as_ptr().cast::<u8>(), bytes as i32)
    })
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

/// Start a list. It lives on the host; hand it to [`set`], return it, or nest it in another.
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
                Value::Str(Text::Guest(s)) => list_push_str(self.handle, s.as_ptr(), s.len() as i32),
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

impl Map {
    /// Put a value under a string key. The same values as [`List::push`].
    pub fn put<'a>(&self, key: &str, value: impl Into<Value<'a>>) -> Result<&Self, Error> {
        let value = value.into();
        let k = key.as_ptr();
        let kl = key.len() as i32;
        let rc = unsafe {
            match &value {
                Value::Int(i) => map_put_str_int(self.handle, k, kl, *i),
                Value::Str(Text::Guest(s)) => map_put_str_str(self.handle, k, kl, s.as_ptr(), s.len() as i32),
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

    /// Put a value under an integer key.
    pub fn put_i<'a>(&self, key: i64, value: impl Into<Value<'a>>) -> Result<&Self, Error> {
        let value = value.into();
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

    /// Put every entry, 64 per import call instead of one each.
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

impl From<List> for Value<'_> {
    fn from(value: List) -> Self {
        value.into_value()
    }
}

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

impl<'a, const N: usize> Pushable<'a> for &'a Str<N> {
    fn pushable(self) -> Result<Value<'a>, Error> {
        Ok(self.into())
    }
}

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
            Value::Str(Text::Stored { src, at, len }) => (4, *len as u32, stored(*src, *at)),
            Value::Blob(Blob::Stored { src, at, len }) => (5, *len as u32, stored(*src, *at)),
            Value::List(Coll::Stored { src, at, len }) => (6, *len as u32, stored(*src, *at)),
            Value::Map(Coll::Stored { src, at, len }) => (7, *len as u32, stored(*src, *at)),
            Value::GeoJson(Text::Stored { src, at, len }) => (9, *len as u32, stored(*src, *at)),
            Value::Hll(Blob::Stored { src, at, len }) => (10, *len as u32, stored(*src, *at)),
            Value::Str(Text::Guest(s)) => (GUEST_STR, s.len() as u32, s.as_ptr() as usize as i64),
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
        Value::Str(Text::Stored { src, at, .. })
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
        Value::Str(Text::Guest(s)) => val_str(s.as_ptr(), s.len() as i32),
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
            Value::Str(Text::Guest(s)) => {
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
            Value::Str(Text::Stored { src, at, .. })
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

/// The record's modification count.
#[must_use]
pub fn generation() -> i64 {
    unsafe { gen() }
}

/// The record's remaining lifetime in seconds, or `None` when it never expires.
#[must_use]
pub fn time_to_live() -> Option<u32> {
    match unsafe { raw_ttl() } {
        // Zero is how the server reports a record with no expiry.
        0 => None,
        value if value < 0 => None,
        value => u32::try_from(value).ok(),
    }
}

pub fn set_time_to_live(seconds: u32) -> Result<(), Error> {
    status(unsafe { raw_set_ttl(seconds as i32) })
}

/// When the record was last written, in milliseconds since the Unix epoch.
#[must_use]
pub fn last_update_time() -> i64 {
    unsafe { last_update() }
}

#[must_use]
pub fn size() -> i64 {
    unsafe { record_size() }
}

#[must_use]
pub fn bin_count() -> i64 {
    unsafe { numbins() }
}

/// The server's clock, in milliseconds since the epoch. Refused in an event handler, where a decision
/// that reads the clock cannot converge across clusters; a handler takes its times from the data.
pub fn now() -> Result<i64, Error> {
    let t = unsafe { current_time() };

    if t < 0 {
        return Err(Error::Refused);
    }

    Ok(t)
}

/// The record's set name, into a buffer you own.
pub fn set_name(out: &mut [u8]) -> Result<&str, Error> {
    let rc = unsafe { raw_set_name(out.as_mut_ptr(), out.len() as i32) };
    let written = bytes_result(rc, out.len())?;

    core::str::from_utf8(&out[..written]).map_err(|_| Error::Malformed)
}

/// The record's user key, when it was written with `send_key`: an integer, or a string or blob whose bytes
/// go into `out`. `Err(Absent)` when the record carries none.
pub fn key(out: &mut [u8]) -> Result<Value<'_>, Error> {
    match unsafe { key_type() } {
        2 => Ok(Value::Int(unsafe { key_int() })),
        4 | 5 => {
            let rc = unsafe { raw_key_bytes(out.as_mut_ptr(), out.len() as i32) };
            let written = bytes_result(rc, out.len())?;

            match core::str::from_utf8(&out[..written]) {
                Ok(text) => Ok(Value::Str(Text::Guest(text))),
                Err(_) => Ok(Value::Blob(Blob::Guest(&out[..written]))),
            }
        }
        _ => Err(Error::Absent),
    }
}

/// The record's digest, into a buffer you own: 20 bytes, always present. What identifies a record a UDF
/// wants to point at rather than ship - a top-k hit, say - since a client can fetch it by digest afterwards.
pub fn digest(out: &mut [u8]) -> Result<&[u8], Error> {
    let rc = unsafe { raw_digest(out.as_mut_ptr(), out.len() as i32) };
    let written = bytes_result(rc, out.len())?;

    Ok(&out[..written])
}

/// Commit a record that does not exist yet.
pub fn create() -> Result<(), Error> {
    status(unsafe { raw_rec_create() })
}

/// Commit the staged writes to an existing record.
pub fn update() -> Result<(), Error> {
    status(unsafe { raw_rec_update() })
}

pub fn remove_record() -> Result<(), Error> {
    status(unsafe { raw_rec_remove() })
}

#[must_use]
pub fn exists() -> bool {
    unsafe { raw_rec_exists() == 1 }
}

/// Write a line to the server log, under the UDF context.
pub fn log_at(level: Level, message: &str) {
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

impl<'a> FromArg<'a> for &'a str {
    const NAME: &'static str = "str";
    const CAPACITY: usize = ARG_BUF;

    fn from_arg(args: &Args, index: usize, buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::Str(Text::Stored { src, at, len })) => {
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

impl<'a> FromArg<'a> for &'a [u8] {
    const NAME: &'static str = "bytes";
    const CAPACITY: usize = ARG_BUF;

    fn from_arg(args: &Args, index: usize, buf: &'a mut [u8]) -> Result<Self, ArgError> {
        let (src, at, len) = match args.get(index) {
            Some(Value::Blob(Blob::Stored { src, at, len }))
            | Some(Value::Str(Text::Stored { src, at, len })) => (src, at, len),
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

impl<'a, const N: usize> FromArg<'a> for Str<N> {
    const NAME: &'static str = "str";
    const CAPACITY: usize = N;

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::Str(text)) => text.to_inline::<N>().map_err(|e| owned_arg_error(e, N, "a string")),
            Some(other) => Err(ArgError::Expected("a string", other.kind())),
            None => Err(ArgError::Missing),
        }
    }
}

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

#[cfg(feature = "alloc")]
impl<'a> FromArg<'a> for alloc::string::String {
    const NAME: &'static str = "str";

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        match args.get(index) {
            Some(Value::Str(text)) => text.to_string().map_err(|e| owned_arg_error(e, usize::MAX, "a string")),
            Some(other) => Err(ArgError::Expected("a string", other.kind())),
            None => Err(ArgError::Missing),
        }
    }
}

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

impl<'a> FromArg<'a> for Value<'a> {
    const NAME: &'static str = "any";

    fn from_arg(args: &Args, index: usize, _buf: &'a mut [u8]) -> Result<Self, ArgError> {
        args.get(index).ok_or(ArgError::Missing)
    }
}

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
    #[must_use]
    pub fn new() -> Message {
        Message {
            buf: [0; 256],
            len: 0,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

impl Default for Message {
    fn default() -> Self {
        Message::new()
    }
}

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

    /// `event xdr-merge/1 name(local: record, remote: record, conflict: conflict) -> resolution`
    pub fn event(&mut self, kind: &str, name: &str) -> i32 {
        if self.len == 0 {
            let _ = write!(
                self,
                "event {kind} {name}(local: record, remote: record, conflict: conflict) -> resolution"
            );
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

impl Default for Signature {
    fn default() -> Self {
        Signature::new()
    }
}

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
// A UDF bound to a point in the server's own processing. `xdr-merge` hands the handler the record this
// node holds and the one XDR delivered, with a description of the merge, and takes back the final record
// or a verdict; the other kinds hand over one or two records and the operation's own description. See
// `event!`.
//

/// One side of an event: a read-only record with its metadata. Its bins are read through the event's own
/// import, never the ordinary record natives, which in an event answer "nothing".
#[derive(Clone, Copy)]
pub struct Record {
    which: i32,
    generation: i64,
    last_update_time: i64,
    ttl: i64,
    source_id: i64,
    bins: Coll,
    deleted: Coll,
    bin_luts: Option<Coll>,
    bin_srcs: Option<Coll>,
}

impl Record {
    /// A bin, as [`bin`](crate::bin) reads one: `Err(Absent)` when the record has no such bin.
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

    /// An integer bin, or `None` when the bin is absent or nil. A bin of another kind is `Err(WrongType)`.
    pub fn int(&self, name: &str) -> Result<Option<i64>, Error> {
        match self.bin(name) {
            Ok(Value::Nil) | Err(Error::Absent) => Ok(None),
            Ok(v) => v.int().map(Some),
            Err(e) => Err(e),
        }
    }

    /// A double bin, or `None` when absent or nil.
    pub fn double(&self, name: &str) -> Result<Option<f64>, Error> {
        match self.bin(name) {
            Ok(Value::Nil) | Err(Error::Absent) => Ok(None),
            Ok(v) => v.double().map(Some),
            Err(e) => Err(e),
        }
    }

    /// A string bin's text, or `None` when absent or nil.
    pub fn text(&self, name: &str) -> Result<Option<Text<'static>>, Error> {
        match self.bin(name) {
            Ok(Value::Nil) | Err(Error::Absent) => Ok(None),
            Ok(v) => v.text().map(Some),
            Err(e) => Err(e),
        }
    }

    /// A list bin, or `None` when absent or nil.
    pub fn list(&self, name: &str) -> Result<Option<Coll>, Error> {
        match self.bin(name) {
            Ok(Value::Nil) | Err(Error::Absent) => Ok(None),
            Ok(v) => v.list().map(Some),
            Err(e) => Err(e),
        }
    }

    /// A map bin, or `None` when absent or nil.
    pub fn map(&self, name: &str) -> Result<Option<Coll>, Error> {
        match self.bin(name) {
            Ok(Value::Nil) | Err(Error::Absent) => Ok(None),
            Ok(v) => v.map().map(Some),
            Err(e) => Err(e),
        }
    }

    /// The names of this record's bins, as a stored list to iterate.
    #[must_use]
    pub fn bins(&self) -> Coll {
        self.bins
    }

    #[must_use]
    pub fn generation(&self) -> i64 {
        self.generation
    }

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

    /// The names of the bins this side deleted: for the incoming write, its bin-delete ops; for the local
    /// record, its bin tombstones. A deleted bin reads as absent, so this is the only way to tell "never
    /// had it" from "removed it", and [`bin_lut`](Record::bin_lut) says when.
    #[must_use]
    pub fn deleted(&self) -> Coll {
        self.deleted
    }

    /// When this side last wrote the bin, when the server knows per bin (bin convergence keeps a LUT per
    /// bin); the record's LUT otherwise. This is what decides a bin under the `bin-lut` policy, so a merge
    /// that orders the sides per bin agrees with the policy on the bins it does not merge.
    #[must_use]
    pub fn bin_lut(&self, name: &str) -> i64 {
        self.bin_luts
            .and_then(|m| m.get(name).ok())
            .and_then(|v| v.int().ok())
            .unwrap_or(self.last_update_time)
    }

    /// The source id that wrote the bin, per bin when known; the record's otherwise.
    #[must_use]
    pub fn bin_source_id(&self, name: &str) -> i64 {
        self.bin_srcs
            .and_then(|m| m.get(name).ok())
            .and_then(|v| v.int().ok())
            .unwrap_or(self.source_id)
    }

    /// A new record built from this one's bins with `overrides` on top: every bin of this record goes in
    /// unless the map already has it. The bins travel by position, never through the guest.
    ///
    /// This prefers one side, so a handler that uses it on `local` is not symmetric: the two DCs would
    /// resolve the same pair to different records. [`Pair::with_newer`] is the symmetric form; use this one
    /// when the side is chosen by something both DCs agree on, such as the source id.
    pub fn with_bins(&self, overrides: Map) -> Result<Map, Error> {
        let mut buf = [0u8; 32];

        for name in self.bins.iter() {
            let name = name?;
            let name = name.as_str(&mut buf)?;

            if overrides.into_value().map()?.get(name).is_ok() {
                continue;
            }

            overrides.put(name, self.bin(name)?)?;
        }

        Ok(overrides)
    }
}

/// What the event is about: the kind of conflict, the policy that would have decided it, and the bins at
/// stake.
#[derive(Clone, Copy)]
pub struct Conflict {
    trigger: Trigger,
    dry_run: bool,
    bin_convergence: bool,
    lost: Coll,
    remote_partial: bool,
    remote_deleted_record: bool,
    local_tombstone: bool,
    local: Record,
    remote: Record,
}

/// What made the event fire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// Every incoming XDR write to a record that exists here (`trigger=always`, the default).
    Always,
    /// Only a write in which an incoming bin would lose under bin convergence (`trigger=conflict`).
    Conflict,
}

impl Conflict {
    #[must_use]
    pub fn trigger(&self) -> Trigger {
        self.trigger
    }

    /// A dry run from `__event__`: nothing will be written whatever the answer.
    #[must_use]
    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    /// Whether the policy the handler stands in for is bin convergence (`bin-lut`) rather than a plain
    /// apply of the incoming bins (`replace`).
    #[must_use]
    pub fn uses_bin_convergence(&self) -> bool {
        self.bin_convergence
    }

    /// The incoming write is a record delete; `remote()` has no bins.
    #[must_use]
    pub fn incoming_is_delete(&self) -> bool {
        self.remote_deleted_record
    }

    /// The current record is a tombstone: it exists, with no bins.
    #[must_use]
    pub fn current_is_tombstone(&self) -> bool {
        self.local_tombstone
    }

    /// Whether either side deleted a bin: the incoming write carries a bin delete, or the local record has
    /// a bin tombstone. Such a bin is absent on that side, and resurrecting it from the other side is the
    /// classic merge mistake; see [`Record::deleted`].
    #[must_use]
    pub fn involves_delete(&self) -> bool {
        !self.local.deleted.is_empty() || !self.remote.deleted.is_empty()
    }

    /// The names of the incoming bins that would lose, as a stored list.
    #[must_use]
    pub fn lost_bins(&self) -> Coll {
        self.lost
    }

    /// The names of bins the incoming write deletes; `remote().deleted()`.
    #[must_use]
    pub fn remote_deleted(&self) -> Coll {
        self.remote.deleted
    }

    #[must_use]
    pub fn local(&self) -> Record {
        self.local
    }

    #[must_use]
    pub fn remote(&self) -> Record {
        self.remote
    }

    /// Whether the incoming write carries only the bins that changed (`bin-policy`), so a bin it lacks may
    /// well exist on the other DC.
    #[must_use]
    pub fn remote_is_partial(&self) -> bool {
        self.remote_partial
    }

    /// The two records side by side, for bin-wise merging.
    #[must_use]
    pub fn pair(&self) -> Pair {
        Pair { local: self.local, remote: self.remote }
    }
}

/// The two sides of a conflict, read bin by bin into a [`Both`].
#[derive(Clone, Copy)]
pub struct Pair {
    local: Record,
    remote: Record,
}

impl Pair {
    pub fn int(&self, name: &str) -> Result<Both<i64>, Error> {
        Ok(self.both(name, self.local.int(name)?, self.remote.int(name)?))
    }

    pub fn double(&self, name: &str) -> Result<Both<f64>, Error> {
        Ok(self.both(name, self.local.double(name)?, self.remote.double(name)?))
    }

    pub fn text(&self, name: &str) -> Result<Both<Text<'static>>, Error> {
        Ok(self.both(name, self.local.text(name)?, self.remote.text(name)?))
    }

    pub fn list(&self, name: &str) -> Result<Both<Coll>, Error> {
        Ok(self.both(name, self.local.list(name)?, self.remote.list(name)?))
    }

    /// The bin as a value on each side; absent sides are `None`.
    pub fn bin(&self, name: &str) -> Result<Both<Value<'static>>, Error> {
        let side = |r: &Record| match r.bin(name) {
            Ok(Value::Nil) | Err(Error::Absent) => Ok(None),
            Ok(v) => Ok(Some(v)),
            Err(e) => Err(e),
        };

        Ok(self.both(name, side(&self.local)?, side(&self.remote)?))
    }

    /// Every bin either side has that `overrides` lacks, from the side that wrote it later - which is what
    /// the policy would have kept - so a handler merges the bins it knows about and leaves the rest as they
    /// would have been. Symmetric: both DCs order the two sides the same way.
    pub fn with_newer(&self, overrides: Map) -> Result<Map, Error> {
        let mut buf = [0u8; 32];
        let have = overrides.into_value();
        let have = have.map()?;

        for side in [&self.local, &self.remote] {
            for name in side.bins().iter() {
                let name = name?;
                let name = name.as_str(&mut buf)?;

                if have.get(name).is_ok() {
                    continue;
                }

                if let Some(value) = self.bin(name)?.newer() {
                    overrides.put(name, value)?;
                }
            }
        }

        Ok(overrides)
    }

    /// Whether the remote side wrote `name` later, per bin: the order the `bin-lut` policy uses.
    #[must_use]
    pub fn remote_is_newer(&self, name: &str) -> bool {
        let (ll, rl) = (self.local.bin_lut(name), self.remote.bin_lut(name));

        rl > ll || (rl == ll && self.remote.bin_source_id(name) > self.local.bin_source_id(name))
    }

    fn both<T>(&self, name: &str, local: Option<T>, remote: Option<T>) -> Both<T> {
        Both {
            local,
            remote,
            local_lut: self.local.bin_lut(name),
            remote_lut: self.remote.bin_lut(name),
            local_src: self.local.bin_source_id(name),
            remote_src: self.remote.bin_source_id(name),
        }
    }
}

/// A bin on both sides of a conflict, either of which may be missing, with the merges a resolver reaches for.
pub struct Both<T> {
    pub local: Option<T>,
    pub remote: Option<T>,
    local_lut: i64,
    remote_lut: i64,
    local_src: i64,
    remote_src: i64,
}

impl<T> Both<T> {
    /// Whether the remote side wrote this bin later: by the bin's LUT where the server keeps one, the
    /// record's otherwise, and on a tie by source id - the order the server's own bin convergence uses, so
    /// "newer" means the same on both DCs even when the clocks agreed to the millisecond.
    #[must_use]
    pub fn remote_is_newer(&self) -> bool {
        self.remote_lut > self.local_lut
            || (self.remote_lut == self.local_lut && self.remote_src > self.local_src)
    }

    /// The side written later; whichever exists when only one does.
    pub fn newer(self) -> Option<T> {
        if self.remote_is_newer() {
            self.remote.or(self.local)
        } else {
            self.local.or(self.remote)
        }
    }

    /// The side written earlier; whichever exists when only one does.
    pub fn older(self) -> Option<T> {
        if self.remote_is_newer() {
            self.local.or(self.remote)
        } else {
            self.remote.or(self.local)
        }
    }

    /// Both sides, as `(older, newer)`: the order both DCs agree on, for a merge whose result depends on
    /// the order its inputs are taken in.
    pub fn ordered(self) -> (Option<T>, Option<T>) {
        if self.remote_is_newer() {
            (self.local, self.remote)
        } else {
            (self.remote, self.local)
        }
    }

    /// The local value when there is one, else the remote.
    pub fn local_or_remote(self) -> Option<T> {
        self.local.or(self.remote)
    }

    /// The remote value when there is one, else the local.
    pub fn remote_or_local(self) -> Option<T> {
        self.remote.or(self.local)
    }

    /// Combine the two when both exist; the one that exists otherwise.
    pub fn merge(self, f: impl FnOnce(T, T) -> T) -> Option<T> {
        match (self.local, self.remote) {
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
        self.local.unwrap_or(0) + self.remote.unwrap_or(0)
    }
}

impl Both<f64> {
    #[must_use]
    pub fn sum(self) -> f64 {
        self.local.unwrap_or(0.0) + self.remote.unwrap_or(0.0)
    }
}

/// What an `xdr-merge` handler answers.
pub enum Resolution {
    /// Keep the local record; the incoming write is dropped.
    KeepLocal,
    /// Apply the incoming write whole, as if it had won every bin.
    TakeRemote,
    /// Let the namespace's policy decide, as it would have without the event.
    Policy,
    /// The final record: these bins, whole - a bin not in the map, or set to nil, is deleted - and
    /// optionally a TTL with a write's meaning: seconds, `0` for the namespace default,
    /// `u32::MAX` for never expire, subject to the namespace's rule on TTL reductions. `None` keeps the
    /// TTL the incoming write asked for.
    Record { bins: Map, ttl: Option<u32> },
}

impl From<Map> for Resolution {
    fn from(bins: Map) -> Self {
        Resolution::Record { bins, ttl: None }
    }
}

/// Decode an event call's inputs. Used by [`event!`].
#[doc(hidden)]
pub fn xdr_merge_inputs(args_handle: i32, argc: i32) -> Result<(Record, Record, Conflict), Error> {
    let args = Args::from_handle(args_handle, argc);
    let conflict = args.get(0).ok_or(Error::Custom("event: the conflict description is missing"))?.map()?;

    let side = |which: i32, key: &str| -> Result<Record, Error> {
        let m = conflict.get(key)?.map()?;
        let bins = m.get("bins")?.list()?;

        Ok(Record {
            which,
            // Absent for an xdr-merge side: each cluster sets its own, and a merge must not read them.
            generation: m.get("gen").ok().and_then(|v| v.int().ok()).unwrap_or(0),
            last_update_time: m.get("lut").ok().and_then(|v| v.int().ok()).unwrap_or(0),
            ttl: m.get("ttl").ok().and_then(|v| v.int().ok()).unwrap_or(0),
            source_id: m.get("src-id")?.int()?,
            bins,
            deleted: match m.get("deleted") {
                Ok(v) => v.list()?,
                Err(_) => empty_list(conflict)?,
            },
            bin_luts: m.get("bin-luts").ok().and_then(|v| v.map().ok()),
            bin_srcs: m.get("bin-srcs").ok().and_then(|v| v.map().ok()),
        })
    };

    let local = side(0, "current")?;
    let remote = side(1, "incoming")?;
    let remote_map = conflict.get("incoming")?.map()?;
    let current_map = conflict.get("current")?.map()?;

    let mut kbuf = [0u8; 16];
    let trigger = match conflict.get("trigger")?.as_str(&mut kbuf)? {
        "conflict" => Trigger::Conflict,
        _ => Trigger::Always,
    };
    let dry_run = conflict.get("mode").ok().is_some_and(|v| v.as_str(&mut kbuf) == Ok("dry-run"));
    let bin_convergence = conflict.get("policy").ok().is_some_and(|v| v.as_str(&mut kbuf) == Ok("bin-lut"));

    Ok((
        local,
        remote,
        Conflict {
            trigger,
            dry_run,
            bin_convergence,
            lost: conflict.get("lost")?.list()?,
            remote_partial: matches!(remote_map.get("partial"), Ok(Value::Bool(true))),
            remote_deleted_record: matches!(remote_map.get("deleted-record"), Ok(Value::Bool(true))),
            local_tombstone: matches!(current_map.get("tombstone"), Ok(Value::Bool(true))),
            local,
            remote,
        },
    ))
}

/// A stored list that is empty: the server always sends `empty` for this purpose.
fn empty_list(conflict: Coll) -> Result<Coll, Error> {
    conflict.get("empty")?.list()
}

/// Deliver an event handler's answer. Used by [`event!`].
#[doc(hidden)]
pub fn deliver_resolution(r: Result<Resolution, Error>) -> i32 {
    let outcome = match r {
        Ok(Resolution::KeepLocal) => result(&Value::from("keep-local")),
        Ok(Resolution::TakeRemote) => result(&Value::from("take-remote")),
        Ok(Resolution::Policy) => result(&Value::from("policy")),
        Ok(Resolution::Record { bins, ttl }) => (|| {
            let out = map(3)?;

            out.put("verdict", "record")?.put("bins", bins)?;

            if let Some(ttl) = ttl {
                out.put("ttl", i64::from(ttl))?;
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
// The other events: their inputs and answers.
//

/// Where a write or delete came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Client,
    Batch,
    /// A record UDF.
    Udf,
    /// A background operate job.
    Ops,
    Xdr,
    Other,
}

impl Source {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Client => "client",
            Source::Batch => "batch",
            Source::Udf => "udf",
            Source::Ops => "ops",
            Source::Xdr => "xdr",
            Source::Other => "other",
        }
    }

    fn parse(text: &str) -> Source {
        match text {
            "client" => Source::Client,
            "batch" => Source::Batch,
            "udf" => Source::Udf,
            "ops" => Source::Ops,
            "xdr" => Source::Xdr,
            _ => Source::Other,
        }
    }
}

/// The write a `before-write` handler is asked about.
#[derive(Clone, Copy)]
pub struct Write {
    source: Source,
    generation: i64,
    ttl: i64,
    create: bool,
    durable_delete: bool,
}

impl Write {
    #[must_use]
    pub fn source(&self) -> Source {
        self.source
    }

    /// The generation the client asked to match, 0 for none.
    #[must_use]
    pub fn generation(&self) -> i64 {
        self.generation
    }

    /// The TTL the write asked for, with a write's meaning.
    #[must_use]
    pub fn ttl(&self) -> i64 {
        self.ttl
    }

    /// Whether the record did not exist before this write.
    #[must_use]
    pub fn is_create(&self) -> bool {
        self.create
    }

    #[must_use]
    pub fn is_durable_delete(&self) -> bool {
        self.durable_delete
    }
}

/// The delete a `before-delete` handler is asked about.
#[derive(Clone, Copy)]
pub struct Delete {
    source: Source,
    durable: bool,
}

impl Delete {
    #[must_use]
    pub fn source(&self) -> Source {
        self.source
    }

    #[must_use]
    pub fn is_durable(&self) -> bool {
        self.durable
    }
}

/// The truncate a `before-truncate` or `after-truncate` handler sees.
#[derive(Clone, Copy)]
pub struct Truncate {
    namespace: Value<'static>,
    set: Value<'static>,
    lut: i64,
}

impl Truncate {
    /// The namespace name, read into `buf`.
    pub fn namespace<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.namespace.as_str(buf)
    }

    /// The set name, empty for the whole namespace.
    pub fn set<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.set.as_str(buf)
    }

    /// Records last updated before this time (ms since the epoch) go.
    #[must_use]
    pub fn last_update_time(&self) -> i64 {
        self.lut
    }
}

/// What a `before-write` handler answers.
pub enum Decision {
    /// Let the write proceed as it is.
    Accept,
    /// Fail the write with this result code (a server code 1..255; 22 is "forbidden") and message.
    Reject { code: i32, message: &'static str },
    /// Commit this record instead: these bins, whole - a bin not in the map, or set to nil, is deleted -
    /// and optionally a TTL with a write's meaning.
    Record { bins: Map, ttl: Option<u32> },
}

/// What a `before-delete` or `before-truncate` handler answers.
pub enum Gate {
    Allow,
    /// Refuse with this result code and message.
    Refuse { code: i32, message: &'static str },
}

/// The expiry a `before-expire` handler is asked about.
#[derive(Clone, Copy)]
pub struct Expire {
    void_time: i64,
    now: i64,
}

impl Expire {
    /// When the record was due, in seconds since the Aerospike epoch.
    #[must_use]
    pub fn void_time(&self) -> i64 {
        self.void_time
    }

    /// The supervisor's clock, same units.
    #[must_use]
    pub fn now(&self) -> i64 {
        self.now
    }
}

/// What a `before-expire` handler answers.
pub enum Expiry {
    Expire,
    /// Keep the record for `ttl` seconds more, or for the namespace's default TTL when `None` (for good if
    /// that is zero). The stored copy learns the new time on the record's next write.
    Keep { ttl: Option<u32> },
}

/// The eviction a `before-evict` handler is asked about.
#[derive(Clone, Copy)]
pub struct Evict {
    void_time: i64,
    now: i64,
    evict_void_time: i64,
}

impl Evict {
    #[must_use]
    pub fn void_time(&self) -> i64 {
        self.void_time
    }

    #[must_use]
    pub fn now(&self) -> i64 {
        self.now
    }

    /// Records due before this are being evicted.
    #[must_use]
    pub fn evict_void_time(&self) -> i64 {
        self.evict_void_time
    }
}

/// What a `before-evict` handler answers.
pub enum Eviction {
    Evict,
    Protect,
}

/// The read an `after-read` handler is asked about.
#[derive(Clone, Copy)]
pub struct Read {
    source: Source,
    all: bool,
}

impl Read {
    #[must_use]
    pub fn source(&self) -> Source {
        self.source
    }

    /// Whether the client asked for every bin rather than named ones.
    #[must_use]
    pub fn is_all_bins(&self) -> bool {
        self.all
    }
}

/// What an `after-read` handler answers.
pub enum ReadResult {
    Unchanged,
    /// Answer the read with this record instead: these bins, whole (a nil leaves a bin out).
    Record { bins: Map },
}

#[doc(hidden)]
pub fn event_expire(desc: Coll) -> Result<Expire, Error> {
    let m = desc.get("expire")?.map()?;

    Ok(Expire { void_time: m.get("void-time")?.int()?, now: m.get("now")?.int()? })
}

#[doc(hidden)]
pub fn event_evict(desc: Coll) -> Result<Evict, Error> {
    let m = desc.get("evict")?.map()?;

    Ok(Evict {
        void_time: m.get("void-time")?.int()?,
        now: m.get("now")?.int()?,
        evict_void_time: m.get("evict-void-time")?.int()?,
    })
}

#[doc(hidden)]
pub fn event_read(desc: Coll) -> Result<Read, Error> {
    let m = desc.get("read")?.map()?;
    let mut buf = [0u8; 16];

    Ok(Read {
        source: Source::parse(m.get("source")?.as_str(&mut buf)?),
        all: matches!(m.get("all"), Ok(Value::Bool(true))),
    })
}

#[doc(hidden)]
pub fn deliver_expiry(r: Result<Expiry, Error>) -> i32 {
    match r {
        Ok(Expiry::Expire) => deliver_verdict("expire", 0, "", None, None),
        Ok(Expiry::Keep { ttl }) => deliver_verdict("keep", 0, "", None, ttl),
        Err(e) => {
            fail(e.message());
            1
        }
    }
}

#[doc(hidden)]
pub fn deliver_eviction(r: Result<Eviction, Error>) -> i32 {
    match r {
        Ok(Eviction::Evict) => deliver_verdict("evict", 0, "", None, None),
        Ok(Eviction::Protect) => deliver_verdict("protect", 0, "", None, None),
        Err(e) => {
            fail(e.message());
            1
        }
    }
}

#[doc(hidden)]
pub fn deliver_read(r: Result<ReadResult, Error>) -> i32 {
    match r {
        Ok(ReadResult::Unchanged) => deliver_verdict("unchanged", 0, "", None, None),
        Ok(ReadResult::Record { bins }) => deliver_verdict("record", 0, "", Some(bins), None),
        Err(e) => {
            fail(e.message());
            1
        }
    }
}

/// A `node-lifecycle` phase: `service-ready`, `shutdown`, or `binding-deployed` (with the binding).
#[derive(Clone, Copy)]
pub struct Lifecycle {
    phase: Value<'static>,
    node: Value<'static>,
    binding: Option<Coll>,
}

impl Lifecycle {
    /// The phase name, read into `buf`.
    pub fn phase<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.phase.as_str(buf)
    }

    /// Whether this is the named phase.
    #[must_use]
    pub fn is(&self, phase: &str) -> bool {
        let mut buf = [0u8; 32];

        self.phase.as_str(&mut buf) == Ok(phase)
    }

    /// This node's id, in hex.
    pub fn node<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.node.as_str(buf)
    }

    /// For `binding-deployed`: the binding, as a map of `event`, `namespace`, `set`, `name`, `module`,
    /// `function`.
    #[must_use]
    pub fn binding(&self) -> Option<Coll> {
        self.binding
    }
}

/// A `cluster-change`: the cluster after a rebalance.
#[derive(Clone, Copy)]
pub struct Cluster {
    key: Value<'static>,
    size: i64,
    nodes: Coll,
    this_node: Value<'static>,
}

impl Cluster {
    /// The cluster key, in hex.
    pub fn key<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.key.as_str(buf)
    }

    #[must_use]
    pub fn size(&self) -> i64 {
        self.size
    }

    /// The node ids in succession order, as a stored list of hex strings.
    #[must_use]
    pub fn nodes(&self) -> Coll {
        self.nodes
    }

    pub fn this_node<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.this_node.as_str(buf)
    }
}

/// A `partition-transfer`: a partition arrived at or departed from this node.
#[derive(Clone, Copy)]
pub struct Transfer {
    namespace: Value<'static>,
    partition: i64,
    arrived: bool,
    peer: Value<'static>,
}

impl Transfer {
    pub fn namespace<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.namespace.as_str(buf)
    }

    #[must_use]
    pub fn partition(&self) -> i64 {
        self.partition
    }

    /// True when the partition arrived here, false when it left.
    #[must_use]
    pub fn arrived(&self) -> bool {
        self.arrived
    }

    /// The other node, in hex.
    pub fn peer<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.peer.as_str(buf)
    }
}

#[doc(hidden)]
pub fn event_lifecycle(desc: Coll) -> Result<Lifecycle, Error> {
    let m = desc.get("lifecycle")?.map()?;

    Ok(Lifecycle {
        phase: m.get("phase")?,
        node: m.get("node")?,
        binding: m.get("binding").ok().and_then(|v| v.map().ok()),
    })
}

#[doc(hidden)]
pub fn event_cluster(desc: Coll) -> Result<Cluster, Error> {
    let m = desc.get("cluster")?.map()?;

    Ok(Cluster {
        key: m.get("key")?,
        size: m.get("size")?.int()?,
        nodes: m.get("nodes")?.list()?,
        this_node: m.get("self")?,
    })
}

#[doc(hidden)]
pub fn event_transfer(desc: Coll) -> Result<Transfer, Error> {
    let m = desc.get("transfer")?.map()?;
    let mut buf = [0u8; 16];

    Ok(Transfer {
        namespace: m.get("namespace")?,
        partition: m.get("partition")?.int()?,
        arrived: m.get("direction")?.as_str(&mut buf)? == "arrived",
        peer: m.get("peer")?,
    })
}

/// A `timer` that came due on the record in hand. The handler runs as a record UDF: every record
/// function works, and [`timer_set`] re-arms it.
#[derive(Clone, Copy)]
pub struct Timer {
    name: Value<'static>,
    due: i64,
    now: i64,
}

impl Timer {
    pub fn name<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.name.as_str(buf)
    }

    /// Whether this is the timer named `name`.
    #[must_use]
    pub fn is(&self, name: &str) -> bool {
        let mut buf = [0u8; 64];

        self.name.as_str(&mut buf) == Ok(name)
    }

    /// When it was due and when it fired, in ms since the epoch.
    #[must_use]
    pub fn due(&self) -> i64 {
        self.due
    }

    #[must_use]
    pub fn now(&self) -> i64 {
        self.now
    }
}

/// From a record UDF or a timer handler: run this set's `timer` handler on this record `delay_ms` from
/// now, under `name`. A second set with the same name replaces the first. Refused outside a record UDF.
pub fn timer_set(name: &str, delay_ms: u64) -> Result<(), Error> {
    status(unsafe { raw_timer_set(name.as_ptr(), name.len() as i32, delay_ms as i64) })
}

/// Cancel a timer set on this record.
pub fn timer_cancel(name: &str) -> Result<(), Error> {
    status(unsafe { raw_timer_cancel(name.as_ptr(), name.len() as i32) })
}

/// The shipment a `before-ship` handler is asked about.
#[derive(Clone, Copy)]
pub struct Ship {
    dc: Value<'static>,
}

impl Ship {
    /// The destination DC's name.
    pub fn dc<'b>(&'b self, buf: &'b mut [u8]) -> Result<&'b str, Error> {
        self.dc.as_str(buf)
    }
}

/// What a `before-ship` handler answers.
pub enum ShipDecision {
    Ship,
    Skip,
    /// Ship this record instead: these bins, whole (a nil leaves a bin out).
    Record { bins: Map },
}

#[doc(hidden)]
pub fn event_timer(args_handle: i32, argc: i32) -> Result<Timer, Error> {
    let args = Args::from_handle(args_handle, argc);
    let m = args.get(0).ok_or(Error::Custom("timer: the description is missing"))?.map()?;

    Ok(Timer { name: m.get("name")?, due: m.get("due")?.int()?, now: m.get("now")?.int()? })
}

#[doc(hidden)]
pub fn event_ship(desc: Coll) -> Result<Ship, Error> {
    let m = desc.get("ship")?.map()?;

    Ok(Ship { dc: m.get("dc")? })
}

#[doc(hidden)]
pub fn deliver_ship(r: Result<ShipDecision, Error>) -> i32 {
    match r {
        Ok(ShipDecision::Ship) => deliver_verdict("ship", 0, "", None, None),
        Ok(ShipDecision::Skip) => deliver_verdict("skip", 0, "", None, None),
        Ok(ShipDecision::Record { bins }) => deliver_verdict("record", 0, "", Some(bins), None),
        Err(e) => {
            fail(e.message());
            1
        }
    }
}

/// The event call's description map. Used by [`event!`].
#[doc(hidden)]
pub fn event_description(args_handle: i32, argc: i32) -> Result<Coll, Error> {
    let args = Args::from_handle(args_handle, argc);

    args.get(0).ok_or(Error::Custom("event: the description is missing"))?.map()
}

/// One side of an event from its entry in the description. Used by [`event!`].
#[doc(hidden)]
pub fn event_record(desc: Coll, key: &str, which: i32) -> Result<Record, Error> {
    let m = desc.get(key)?.map()?;
    let bins = m.get("bins")?.list()?;

    Ok(Record {
        which,
        generation: m.get("gen")?.int()?,
        last_update_time: m.get("lut")?.int()?,
        ttl: m.get("ttl")?.int()?,
        source_id: m.get("src-id")?.int()?,
        bins,
        deleted: match m.get("deleted") {
            Ok(v) => v.list()?,
            Err(_) => empty_list(desc)?,
        },
        bin_luts: m.get("bin-luts").ok().and_then(|v| v.map().ok()),
        bin_srcs: m.get("bin-srcs").ok().and_then(|v| v.map().ok()),
    })
}

#[doc(hidden)]
pub fn event_write(desc: Coll) -> Result<Write, Error> {
    let m = desc.get("write")?.map()?;
    let mut buf = [0u8; 16];

    Ok(Write {
        source: Source::parse(m.get("source")?.as_str(&mut buf)?),
        generation: m.get("generation")?.int()?,
        ttl: m.get("ttl")?.int()?,
        create: matches!(m.get("create"), Ok(Value::Bool(true))),
        durable_delete: matches!(m.get("durable-delete"), Ok(Value::Bool(true))),
    })
}

#[doc(hidden)]
pub fn event_delete(desc: Coll) -> Result<Delete, Error> {
    let m = desc.get("delete")?.map()?;
    let mut buf = [0u8; 16];

    Ok(Delete {
        source: Source::parse(m.get("source")?.as_str(&mut buf)?),
        durable: matches!(m.get("durable"), Ok(Value::Bool(true))),
    })
}

#[doc(hidden)]
pub fn event_truncate(desc: Coll) -> Result<Truncate, Error> {
    let m = desc.get("truncate")?.map()?;

    Ok(Truncate {
        namespace: m.get("namespace")?,
        set: m.get("set")?,
        lut: m.get("lut")?.int()?,
    })
}

/// Deliver a `{verdict, code, message}` answer. Used by [`event!`].
#[doc(hidden)]
pub fn deliver_verdict(verdict: &str, code: i32, message: &str, bins: Option<Map>, ttl: Option<u32>) -> i32 {
    let outcome = (|| {
        if code == 0 && message.is_empty() && bins.is_none() && ttl.is_none() {
            return result(&Value::from(verdict));
        }

        let out = map(5)?;

        out.put("verdict", verdict)?;

        if code != 0 {
            out.put("code", i64::from(code))?;
        }

        if !message.is_empty() {
            out.put("message", message)?;
        }

        if let Some(bins) = bins {
            out.put("bins", bins)?;
        }

        if let Some(ttl) = ttl {
            out.put("ttl", i64::from(ttl))?;
        }

        result(&out.into_value())
    })();

    match outcome {
        Ok(()) => 0,
        Err(e) => {
            fail(e.message());
            1
        }
    }
}

#[doc(hidden)]
pub fn deliver_decision(r: Result<Decision, Error>) -> i32 {
    match r {
        Ok(Decision::Accept) => deliver_verdict("accept", 0, "", None, None),
        Ok(Decision::Reject { code, message }) => deliver_verdict("reject", code, message, None, None),
        Ok(Decision::Record { bins, ttl }) => deliver_verdict("record", 0, "", Some(bins), ttl),
        Err(e) => {
            fail(e.message());
            1
        }
    }
}

#[doc(hidden)]
pub fn deliver_gate(r: Result<Gate, Error>) -> i32 {
    match r {
        Ok(Gate::Allow) => deliver_verdict("allow", 0, "", None, None),
        Ok(Gate::Refuse { code, message }) => deliver_verdict("refuse", code, message, None, None),
        Err(e) => {
            fail(e.message());
            1
        }
    }
}

#[doc(hidden)]
pub fn deliver_done(r: Result<(), Error>) -> i32 {
    match r {
        Ok(()) => deliver_verdict("ok", 0, "", None, None),
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
///     fn transfer(amount: i64, to: &str, dry_run: Option<bool>) -> Result<Value, Error> {
///         ...
///         Ok(Value::Int(balance))
///     }
/// }
/// ```
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
        fn $name:ident($($arg:ident : $ty:ty),* $(,)?) -> Result<Value, Error> $body:block
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
                            &[$($crate::param::<$ty>(stringify!($arg))),*],
                        )
                    }
                }
            };

            $(#[$meta])*
            #[no_mangle]
            pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
                $crate::udf_body!($name, args_handle, argc, ($($arg : $ty),*), $body, result)
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
///         record(bin: &str) -> Result<(), Error> {
///             unsafe { TOTAL += as_udf::bin(bin)?.int()? };
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
            record($($rarg:ident : $rty:ty),* $(,)?) -> Result<(), Error> $rbody:block
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
                            &[$($crate::param::<$rty>(stringify!($rarg))),*],
                            &[$($crate::param::<$ety>(stringify!($earg))),*],
                        )
                    }
                }

                #[export_name = concat!(stringify!($name), "_record")]
                pub extern "C" fn record(args_handle: i32, argc: i32) -> i32 {
                    $crate::udf_body!($name, args_handle, argc, ($($rarg : $rty),*), $rbody, unit)
                }

                #[export_name = concat!(stringify!($name), "_end")]
                pub extern "C" fn end(args_handle: i32, argc: i32) -> i32 {
                    $crate::udf_body!($name, args_handle, argc, ($($earg : $ety),*), $ebody, result)
                }
            };
        )*
    };
}

/// Export an event handler.
///
/// One arm per kind, the parameter types part of the arm; the `xdr_merge` arm has exactly this shape:
///
/// ```ignore
/// event! {
///     on xdr_merge
///     fn resolve_account(local: Record, remote: Record, conflict: Conflict) -> Result<Resolution, Error> {
///         let out = as_udf::map(2)?;
///         out.put("visits", conflict.pair().int("visits")?.max())?;
///         Ok(Resolution::Record { bins: local.with_bins(out)?, ttl: None })
///     }
/// }
/// ```
///
/// The parameter types are part of the arm, so a handler with another signature does not compile. The macro
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
        fn $name:ident($local:ident : Record, $remote:ident : Record, $conflict:ident : Conflict)
            -> Result<Resolution, Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event xdr-merge/1 ", stringify!($name),
            "(current: record, incoming: record, conflict: conflict) -> resolution"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let ($local, $remote, $conflict) = match $crate::xdr_merge_inputs(args_handle, argc) {
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

    (
        $(#[$meta:meta])*
        on before_write
        fn $name:ident($before:ident : Record, $after:ident : Record, $write:ident : Write)
            -> Result<Decision, Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event before-write/1 ", stringify!($name),
            "(before: record, after: record, write: write) -> decision"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let __inputs = (|| -> ::core::result::Result<_, $crate::Error> {
                let desc = $crate::event_description(args_handle, argc)?;

                Ok(($crate::event_record(desc, "before", 0)?, $crate::event_record(desc, "after", 1)?,
                    $crate::event_write(desc)?))
            })();
            let ($before, $after, $write) = match __inputs {
                Ok(inputs) => inputs,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<$crate::Decision, $crate::Error> = (|| $body)();

            $crate::deliver_decision(__r)
        }

        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on after_write
        fn $name:ident($before:ident : Record, $after:ident : Record, $write:ident : Write)
            -> Result<(), Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event after-write/1 ", stringify!($name),
            "(before: record, after: record, write: write) -> ()"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let __inputs = (|| -> ::core::result::Result<_, $crate::Error> {
                let desc = $crate::event_description(args_handle, argc)?;

                Ok(($crate::event_record(desc, "before", 0)?, $crate::event_record(desc, "after", 1)?,
                    $crate::event_write(desc)?))
            })();
            let ($before, $after, $write) = match __inputs {
                Ok(inputs) => inputs,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<(), $crate::Error> = (|| $body)();

            $crate::deliver_done(__r)
        }

        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on after_delete
        fn $name:ident($record:ident : Record, $delete:ident : Delete) -> Result<(), Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event after-delete/1 ", stringify!($name),
            "(record: record, delete: delete) -> ()"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let __inputs = (|| -> ::core::result::Result<_, $crate::Error> {
                let desc = $crate::event_description(args_handle, argc)?;

                Ok(($crate::event_record(desc, "record", 0)?, $crate::event_delete(desc)?))
            })();
            let ($record, $delete) = match __inputs {
                Ok(inputs) => inputs,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<(), $crate::Error> = (|| $body)();

            $crate::deliver_done(__r)
        }

        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on before_delete
        fn $name:ident($record:ident : Record, $delete:ident : Delete) -> Result<Gate, Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event before-delete/1 ", stringify!($name),
            "(record: record, delete: delete) -> gate"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let __inputs = (|| -> ::core::result::Result<_, $crate::Error> {
                let desc = $crate::event_description(args_handle, argc)?;

                Ok(($crate::event_record(desc, "record", 0)?, $crate::event_delete(desc)?))
            })();
            let ($record, $delete) = match __inputs {
                Ok(inputs) => inputs,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<$crate::Gate, $crate::Error> = (|| $body)();

            $crate::deliver_gate(__r)
        }

        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on before_expire
        fn $name:ident($record:ident : Record, $expire:ident : Expire) -> Result<Expiry, Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event before-expire/1 ", stringify!($name),
            "(record: record, expire: expire) -> expiry"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let __inputs = (|| -> ::core::result::Result<_, $crate::Error> {
                let desc = $crate::event_description(args_handle, argc)?;

                Ok(($crate::event_record(desc, "record", 0)?, $crate::event_expire(desc)?))
            })();
            let ($record, $expire) = match __inputs {
                Ok(inputs) => inputs,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<$crate::Expiry, $crate::Error> = (|| $body)();

            $crate::deliver_expiry(__r)
        }

        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on before_evict
        fn $name:ident($record:ident : Record, $evict:ident : Evict) -> Result<Eviction, Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event before-evict/1 ", stringify!($name),
            "(record: record, evict: evict) -> eviction"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let __inputs = (|| -> ::core::result::Result<_, $crate::Error> {
                let desc = $crate::event_description(args_handle, argc)?;

                Ok(($crate::event_record(desc, "record", 0)?, $crate::event_evict(desc)?))
            })();
            let ($record, $evict) = match __inputs {
                Ok(inputs) => inputs,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<$crate::Eviction, $crate::Error> = (|| $body)();

            $crate::deliver_eviction(__r)
        }

        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on after_read
        fn $name:ident($record:ident : Record, $read:ident : Read) -> Result<ReadResult, Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event after-read/1 ", stringify!($name),
            "(record: record, read: read) -> read-result"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let __inputs = (|| -> ::core::result::Result<_, $crate::Error> {
                let desc = $crate::event_description(args_handle, argc)?;

                Ok(($crate::event_record(desc, "record", 0)?, $crate::event_read(desc)?))
            })();
            let ($record, $read) = match __inputs {
                Ok(inputs) => inputs,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<$crate::ReadResult, $crate::Error> = (|| $body)();

            $crate::deliver_read(__r)
        }

        $crate::event!($($rest)*);
    };

    // The once-per-node events share one shape: a description in, nothing out.
    (@once $kind:literal, $decode:ident, $(#[$meta:meta])* $name:ident, $arg:ident, $ty:ident, $body:block) => {
        $crate::event!(@sig $name, concat!("event ", $kind, "/1 ", stringify!($name), "(", stringify!($arg),
            ": ", stringify!($ty), ") -> ()"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let $arg = match $crate::event_description(args_handle, argc).and_then($crate::$decode) {
                Ok(t) => t,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<(), $crate::Error> = (|| $body)();

            $crate::deliver_done(__r)
        }
    };

    (
        $(#[$meta:meta])*
        on node_lifecycle
        fn $name:ident($arg:ident : Lifecycle) -> Result<(), Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@once "node-lifecycle", event_lifecycle, $(#[$meta])* $name, $arg, lifecycle, $body);
        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on cluster_change
        fn $name:ident($arg:ident : Cluster) -> Result<(), Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@once "cluster-change", event_cluster, $(#[$meta])* $name, $arg, cluster, $body);
        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on partition_transfer
        fn $name:ident($arg:ident : Transfer) -> Result<(), Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@once "partition-transfer", event_transfer, $(#[$meta])* $name, $arg, transfer, $body);
        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on timer
        fn $name:ident($timer:ident : Timer) -> Result<(), Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event timer/1 ", stringify!($name), "(timer: timer) -> ()"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let $timer = match $crate::event_timer(args_handle, argc) {
                Ok(t) => t,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<(), $crate::Error> = (|| $body)();

            $crate::deliver_done(__r)
        }

        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on before_ship
        fn $name:ident($record:ident : Record, $ship:ident : Ship) -> Result<ShipDecision, Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event before-ship/1 ", stringify!($name),
            "(record: record, ship: ship) -> ship-decision"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let __inputs = (|| -> ::core::result::Result<_, $crate::Error> {
                let desc = $crate::event_description(args_handle, argc)?;

                Ok(($crate::event_record(desc, "record", 0)?, $crate::event_ship(desc)?))
            })();
            let ($record, $ship) = match __inputs {
                Ok(inputs) => inputs,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<$crate::ShipDecision, $crate::Error> = (|| $body)();

            $crate::deliver_ship(__r)
        }

        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on before_truncate
        fn $name:ident($truncate:ident : Truncate) -> Result<Gate, Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event before-truncate/1 ", stringify!($name),
            "(truncate: truncate) -> gate"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let $truncate = match $crate::event_description(args_handle, argc)
                .and_then($crate::event_truncate)
            {
                Ok(t) => t,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<$crate::Gate, $crate::Error> = (|| $body)();

            $crate::deliver_gate(__r)
        }

        $crate::event!($($rest)*);
    };

    (
        $(#[$meta:meta])*
        on after_truncate
        fn $name:ident($truncate:ident : Truncate) -> Result<(), Error> $body:block
        $($rest:tt)*
    ) => {
        $crate::event!(@sig $name, concat!("event after-truncate/1 ", stringify!($name),
            "(truncate: truncate) -> ()"));

        $(#[$meta])*
        #[no_mangle]
        pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
            let $truncate = match $crate::event_description(args_handle, argc)
                .and_then($crate::event_truncate)
            {
                Ok(t) => t,
                Err(e) => {
                    $crate::fail(e.message());
                    return 1;
                }
            };
            let __r: ::core::result::Result<(), $crate::Error> = (|| $body)();

            $crate::deliver_done(__r)
        }

        $crate::event!($($rest)*);
    };
}

/// The body of an export: decode the arguments, run the block, deliver the outcome. Not for direct use.
#[doc(hidden)]
#[macro_export]
macro_rules! udf_body {
    ($name:ident, $args_handle:expr, $argc:expr, ($($arg:ident : $ty:ty),*), $body:block, $outcome:ident) => {{
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
            Value::Str(Text::Stored { src: 5, at: 9, len: 3 })
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

        let raw = Value::Str(Text::Stored { src: 5, at: 9, len: 3 }).raw().unwrap();
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
