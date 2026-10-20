//! Writing an Aerospike WASM UDF for the WAMR engine.
//!
//! # What makes this different from the wasmtime SDK
//!
//! There is no msgpack here, and no allocator. The host is C, holds `as_val` natively, and can therefore
//! answer a typed question with a typed answer: reading an integer bin is one import call returning an
//! `i64`, not an encode, a copy and a decode. Collections are walked **in place** through handles instead
//! of being serialised across.
//!
//! Consequences worth knowing:
//!
//! - **`no_std`, no `alloc`, no allocator of any kind.** Nothing here allocates, so a guest can be built
//!   without one. That removes what measurement showed to be the largest single cost in the wasmtime
//!   path - the guest's first heap allocation.
//! - **Buffers are yours.** Anything returning bytes takes a `&mut [u8]` you own, typically a `static mut`
//!   or a stack array. Nothing is ever truncated: a buffer that is too small reports the size needed.
//! - **A module built against this SDK does not run on the wasmtime engine**, and vice versa. The ABIs are
//!   different by design.
//!
//! # The shape of a UDF
//!
//! ```ignore
//! use aerospike_udf_sdk_wasm::{export, Args, Value};
//!
//! export! {
//!     fn increment(_args: Args) -> i32 {
//!         let n = aerospike_udf_sdk_wasm::bin_int("n");
//!         aerospike_udf_sdk_wasm::set_int("n", n + 1);
//!         aerospike_udf_sdk_wasm::result_int(n + 1);
//!         0
//!     }
//! }
//! ```

#![no_std]
#![allow(clippy::missing_safety_doc)]

//==========================================================
// The imports.
//

#[link(wasm_import_module = "aerospike")]
extern "C" {
    // Bins - reading.
    fn bin_type(name: *const u8, name_len: i32) -> i32;
    #[link_name = "bin_int"]
    fn raw_bin_int(name: *const u8, name_len: i32) -> i64;
    #[link_name = "bin_double"]
    fn raw_bin_double(name: *const u8, name_len: i32) -> f64;
    #[link_name = "bin_bool"]
    fn raw_bin_bool(name: *const u8, name_len: i32) -> i32;
    #[link_name = "bin_bytes"]
    fn raw_bin_bytes(name: *const u8, name_len: i32, out: *mut u8, out_cap: i32) -> i32;
    #[link_name = "bin_handle"]
    fn raw_bin_handle(name: *const u8, name_len: i32) -> i32;

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
    #[link_name = "bin_remove"]
    fn raw_bin_remove(name: *const u8, name_len: i32) -> i32;

    // Collections - reading.
    fn coll_size(handle: i32) -> i32;
    fn list_type(handle: i32, index: i32) -> i32;
    fn list_int(handle: i32, index: i32) -> i64;
    fn list_double(handle: i32, index: i32) -> f64;
    fn list_bool(handle: i32, index: i32) -> i32;
    fn list_bytes(handle: i32, index: i32, out: *mut u8, out_cap: i32) -> i32;
    fn list_handle(handle: i32, index: i32) -> i32;
    fn map_key_type(handle: i32, index: i32) -> i32;
    fn map_key_int(handle: i32, index: i32) -> i64;
    fn map_key_bytes(handle: i32, index: i32, out: *mut u8, out_cap: i32) -> i32;
    fn map_value_type(handle: i32, index: i32) -> i32;
    fn map_value_int(handle: i32, index: i32) -> i64;
    fn map_value_double(handle: i32, index: i32) -> f64;
    fn map_value_bytes(handle: i32, index: i32, out: *mut u8, out_cap: i32) -> i32;
    fn map_value_handle(handle: i32, index: i32) -> i32;
    fn map_get_type(handle: i32, key: *const u8, key_len: i32) -> i32;
    fn map_get_int(handle: i32, key: *const u8, key_len: i32) -> i64;
    fn map_get_double(handle: i32, key: *const u8, key_len: i32) -> f64;
    fn map_get_bytes(handle: i32, key: *const u8, key_len: i32, out: *mut u8, out_cap: i32) -> i32;
    fn map_get_handle(handle: i32, key: *const u8, key_len: i32) -> i32;
    fn list_ints(handle: i32, out: *mut u8, out_cap: i32) -> i32;

    // Collections - building.
    fn list_new(capacity: i32) -> i32;
    fn map_new(capacity: i32) -> i32;
    fn val_int(value: i64) -> i32;
    fn val_double(value: f64) -> i32;
    fn val_bool(value: i32) -> i32;
    fn val_nil() -> i32;
    // The host also exports `val_str`, which nothing here needs: a string reaches a list through
    // `list_push_str` and a string-keyed map through `map_put_str_str`, both one call instead of two.
    // It would be needed only to put a string into an *integer*-keyed map, which this SDK does not
    // expose - see `Map::put_int_key`.
    fn val_blob(value: *const u8, value_len: i32) -> i32;
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
    #[link_name = "result_handle"]
    fn raw_result_handle(handle: i32) -> i32;
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
// Status codes and types, mirroring mod_wamr_internal.h.
//

const OK: i32 = 0;
const NOT_FOUND: i32 = -2;
const NEEDED_BASE: i32 = -16;

/// What kind of value something is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    /// There is no such bin, or no such element.
    Absent,
    Nil,
    Bool,
    Int,
    Double,
    Str,
    Blob,
    List,
    Map,
    /// Something a UDF has no business interpreting.
    Other,
    /// A GeoJson document, which reads and writes as its text through [`Record::get_str`] and [`set_geojson`].
    ///
    /// Distinct from [`Type::Str`] on purpose: the particle type is the difference between a string bin and a
    /// geospatially indexable one, so writing a document back with [`set_str`] would silently make it unqueryable.
    GeoJson,
}

impl Type {
    fn from_raw(raw: i32) -> Type {
        match raw {
            0 => Type::Nil,
            1 => Type::Bool,
            2 => Type::Int,
            3 => Type::Double,
            4 => Type::Str,
            5 => Type::Blob,
            6 => Type::List,
            7 => Type::Map,
            8 => Type::Other,
            9 => Type::GeoJson,
            // Includes -1 and anything a newer server might add.
            _ => Type::Absent,
        }
    }
}

/// How severe a log line is. `Warn` is the loudest.
#[derive(Clone, Copy, Debug)]
pub enum Level {
    Warn = 0,
    Info = 1,
    Debug = 2,
}

/// Why a call that fills a buffer did not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadError {
    /// No such bin, element or key.
    Absent,
    /// The buffer was too small; this many bytes are needed.
    TooSmall(usize),
    /// The value is not one that has bytes.
    WrongType,
}

// Turn one of the (out, cap) conventions into a result.
fn bytes_result(rc: i32, len_of_buf: usize) -> Result<usize, ReadError> {
    if rc >= 0 {
        let written = rc as usize;

        // Cannot happen unless the host is wrong about its own buffer, and silently trusting it is how a
        // read walks off the end of a slice.
        if written > len_of_buf {
            return Err(ReadError::WrongType);
        }

        return Ok(written);
    }

    if rc <= NEEDED_BASE {
        return Err(ReadError::TooSmall((-rc - -NEEDED_BASE) as usize));
    }

    if rc == NOT_FOUND {
        return Err(ReadError::Absent);
    }

    Err(ReadError::WrongType)
}

//==========================================================
// Bins.
//

/// What kind of value a bin holds, or [`Type::Absent`].
#[must_use]
pub fn bin(name: &str) -> Type {
    Type::from_raw(unsafe { bin_type(name.as_ptr(), name.len() as i32) })
}

/// An integer bin, or 0 when it is absent or holds something else.
///
/// **One import call and no allocation** - the cheapest thing this SDK can do, and the reason the ABI is
/// shaped this way. Use [`bin`] first when 0 has to be told apart from "not there".
#[must_use]
pub fn bin_int(name: &str) -> i64 {
    unsafe { raw_bin_int(name.as_ptr(), name.len() as i32) }
}

/// An integer bin, distinguishing absent from zero. Costs a second call.
#[must_use]
pub fn bin_int_checked(name: &str) -> Option<i64> {
    if bin(name) == Type::Int {
        Some(bin_int(name))
    } else {
        None
    }
}

#[must_use]
pub fn bin_double(name: &str) -> f64 {
    unsafe { raw_bin_double(name.as_ptr(), name.len() as i32) }
}

#[must_use]
pub fn bin_bool(name: &str) -> bool {
    unsafe { raw_bin_bool(name.as_ptr(), name.len() as i32) != 0 }
}

/// A string or blob bin's bytes, into a buffer you own.
///
/// Returns how many bytes were written. `Err(TooSmall(n))` says to try again with `n` bytes.
pub fn bin_bytes<'a>(name: &str, out: &'a mut [u8]) -> Result<&'a [u8], ReadError> {
    let rc = unsafe {
        raw_bin_bytes(
            name.as_ptr(),
            name.len() as i32,
            out.as_mut_ptr(),
            out.len() as i32,
        )
    };

    let written = bytes_result(rc, out.len())?;

    Ok(&out[..written])
}

/// A string bin, as `&str`.
pub fn bin_str<'a>(name: &str, out: &'a mut [u8]) -> Result<&'a str, ReadError> {
    let bytes = bin_bytes(name, out)?;

    core::str::from_utf8(bytes).map_err(|_| ReadError::WrongType)
}

/// A list or map bin, as something to walk.
#[must_use]
pub fn bin_coll(name: &str) -> Option<Coll> {
    let handle = unsafe { raw_bin_handle(name.as_ptr(), name.len() as i32) };

    Coll::new(handle)
}

pub fn set_int(name: &str, value: i64) -> bool {
    unsafe { raw_set_int(name.as_ptr(), name.len() as i32, value) == OK }
}

pub fn set_double(name: &str, value: f64) -> bool {
    unsafe { raw_set_double(name.as_ptr(), name.len() as i32, value) == OK }
}

pub fn set_bool(name: &str, value: bool) -> bool {
    unsafe { raw_set_bool(name.as_ptr(), name.len() as i32, i32::from(value)) == OK }
}

pub fn set_str(name: &str, value: &str) -> bool {
    unsafe {
        raw_set_str(
            name.as_ptr(),
            name.len() as i32,
            value.as_ptr(),
            value.len() as i32,
        ) == OK
    }
}

pub fn set_blob(name: &str, value: &[u8]) -> bool {
    unsafe {
        raw_set_blob(
            name.as_ptr(),
            name.len() as i32,
            value.as_ptr(),
            value.len() as i32,
        ) == OK
    }
}

/// Write a GeoJson bin from its document text.
///
/// **Not the same as [`set_str`].** A geojson bin and a string bin differ by particle type, and that type is what makes
/// the bin geospatially indexable and queryable - so a document written with `set_str` keeps its text and quietly stops
/// being geospatial. That is why this exists rather than letting `set_str` serve both.
///
/// The document is not validated here. The server's geojson particle rejects a malformed one on write, which is one
/// implementation of the rule rather than two.
///
/// ```ignore
/// // Read a document, adjust it, write it back as geojson.
/// let mut buf = [0u8; 512];
/// let text = rec::get_str("loc", &mut buf)?;
/// set_geojson("loc", text);
/// ```
pub fn set_geojson(name: &str, value: &str) -> bool {
    unsafe {
        raw_set_geojson(
            name.as_ptr(),
            name.len() as i32,
            value.as_ptr(),
            value.len() as i32,
        ) == OK
    }
}

/// Write nil, which is how Aerospike deletes a bin.
pub fn set_nil(name: &str) -> bool {
    unsafe { raw_set_nil(name.as_ptr(), name.len() as i32) == OK }
}

/// Write a collection built with [`List`] or [`Map`].
pub fn set_coll(name: &str, value: &Builder) -> bool {
    unsafe { raw_set_handle(name.as_ptr(), name.len() as i32, value.handle) == OK }
}

pub fn remove(name: &str) -> bool {
    unsafe { raw_bin_remove(name.as_ptr(), name.len() as i32) == OK }
}

//==========================================================
// Reading collections.
//

/// A list or map somewhere in the record or the argument list, walked in place.
///
/// Nothing is copied to make one of these: it is a handle to the server's own value. Reading an element is
/// one import call.
#[derive(Clone, Copy)]
pub struct Coll {
    handle: i32,
}

impl Coll {
    fn new(handle: i32) -> Option<Coll> {
        // Zero is "no handle".
        if handle <= 0 {
            None
        } else {
            Some(Coll { handle })
        }
    }

    /// How many elements a list has, or how many entries a map has.
    #[must_use]
    pub fn len(&self) -> usize {
        let size = unsafe { coll_size(self.handle) };

        if size < 0 {
            0
        } else {
            size as usize
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // --- as a list ---

    #[must_use]
    pub fn type_at(&self, index: usize) -> Type {
        Type::from_raw(unsafe { list_type(self.handle, index as i32) })
    }

    #[must_use]
    pub fn int_at(&self, index: usize) -> i64 {
        unsafe { list_int(self.handle, index as i32) }
    }

    #[must_use]
    pub fn double_at(&self, index: usize) -> f64 {
        unsafe { list_double(self.handle, index as i32) }
    }

    #[must_use]
    pub fn bool_at(&self, index: usize) -> bool {
        unsafe { list_bool(self.handle, index as i32) != 0 }
    }

    pub fn bytes_at<'a>(&self, index: usize, out: &'a mut [u8]) -> Result<&'a [u8], ReadError> {
        let rc = unsafe {
            list_bytes(
                self.handle,
                index as i32,
                out.as_mut_ptr(),
                out.len() as i32,
            )
        };
        let written = bytes_result(rc, out.len())?;

        Ok(&out[..written])
    }

    pub fn str_at<'a>(&self, index: usize, out: &'a mut [u8]) -> Result<&'a str, ReadError> {
        core::str::from_utf8(self.bytes_at(index, out)?).map_err(|_| ReadError::WrongType)
    }

    /// A nested list or map.
    #[must_use]
    pub fn coll_at(&self, index: usize) -> Option<Coll> {
        Coll::new(unsafe { list_handle(self.handle, index as i32) })
    }

    // --- as a map ---
    //
    // By index rather than by key: the host flattens a map's entries once per handle, because `as_map` has
    // no indexed accessor and re-walking it per element would be quadratic. Look up by iterating.

    #[must_use]
    pub fn key_type_at(&self, index: usize) -> Type {
        Type::from_raw(unsafe { map_key_type(self.handle, index as i32) })
    }

    #[must_use]
    pub fn key_int_at(&self, index: usize) -> i64 {
        unsafe { map_key_int(self.handle, index as i32) }
    }

    pub fn key_bytes_at<'a>(&self, index: usize, out: &'a mut [u8]) -> Result<&'a [u8], ReadError> {
        let rc = unsafe {
            map_key_bytes(
                self.handle,
                index as i32,
                out.as_mut_ptr(),
                out.len() as i32,
            )
        };
        let written = bytes_result(rc, out.len())?;

        Ok(&out[..written])
    }

    pub fn key_str_at<'a>(&self, index: usize, out: &'a mut [u8]) -> Result<&'a str, ReadError> {
        core::str::from_utf8(self.key_bytes_at(index, out)?).map_err(|_| ReadError::WrongType)
    }

    #[must_use]
    pub fn value_type_at(&self, index: usize) -> Type {
        Type::from_raw(unsafe { map_value_type(self.handle, index as i32) })
    }

    #[must_use]
    pub fn value_int_at(&self, index: usize) -> i64 {
        unsafe { map_value_int(self.handle, index as i32) }
    }

    #[must_use]
    pub fn value_double_at(&self, index: usize) -> f64 {
        unsafe { map_value_double(self.handle, index as i32) }
    }

    pub fn value_bytes_at<'a>(
        &self,
        index: usize,
        out: &'a mut [u8],
    ) -> Result<&'a [u8], ReadError> {
        let rc = unsafe {
            map_value_bytes(
                self.handle,
                index as i32,
                out.as_mut_ptr(),
                out.len() as i32,
            )
        };
        let written = bytes_result(rc, out.len())?;

        Ok(&out[..written])
    }

    // --- as a map, by key ---
    //
    // **Prefer these over the indexed accessors.** Indexing a map makes the host flatten its entries into an
    // allocation, once per handle, and finding a key means comparing each one — which measured 1.48x slower
    // than Lua on a map-heavy aggregation. These are one `as_map_get` hash lookup: no allocation, no scan.
    //
    // String keys only. A map keyed by integers still needs the indexed accessors.

    /// What kind of value the map holds under `key`, or [`Type::Absent`].
    #[must_use]
    pub fn get_type(&self, key: &str) -> Type {
        Type::from_raw(unsafe { map_get_type(self.handle, key.as_ptr(), key.len() as i32) })
    }

    #[must_use]
    pub fn get_int(&self, key: &str) -> i64 {
        unsafe { map_get_int(self.handle, key.as_ptr(), key.len() as i32) }
    }

    #[must_use]
    pub fn get_double(&self, key: &str) -> f64 {
        unsafe { map_get_double(self.handle, key.as_ptr(), key.len() as i32) }
    }

    pub fn get_bytes<'a>(&self, key: &str, out: &'a mut [u8]) -> Result<&'a [u8], ReadError> {
        let rc = unsafe {
            map_get_bytes(
                self.handle,
                key.as_ptr(),
                key.len() as i32,
                out.as_mut_ptr(),
                out.len() as i32,
            )
        };
        let written = bytes_result(rc, out.len())?;

        Ok(&out[..written])
    }

    pub fn get_str<'a>(&self, key: &str, out: &'a mut [u8]) -> Result<&'a str, ReadError> {
        core::str::from_utf8(self.get_bytes(key, out)?).map_err(|_| ReadError::WrongType)
    }

    /// A nested list or map under `key`.
    #[must_use]
    pub fn get_coll(&self, key: &str) -> Option<Coll> {
        Coll::new(unsafe { map_get_handle(self.handle, key.as_ptr(), key.len() as i32) })
    }

    /// Every integer in this list, in **one** import call.
    ///
    /// Lists never needed a by-key accessor — indexing one is already direct — but reading n elements cost n
    /// calls. This is the same work in one, into a buffer you own. `Err(TooSmall(n))` reports the element
    /// count needed.
    pub fn ints_into<'a>(&self, out: &'a mut [i64]) -> Result<&'a [i64], ReadError> {
        let bytes = core::mem::size_of::<i64>() * out.len();
        let rc = unsafe { list_ints(self.handle, out.as_mut_ptr().cast::<u8>(), bytes as i32) };

        if rc >= 0 {
            let written = rc as usize;

            if written > out.len() {
                return Err(ReadError::WrongType);
            }

            return Ok(&out[..written]);
        }

        if rc <= NEEDED_BASE {
            // Elements, not bytes — the host reports this one in elements.
            return Err(ReadError::TooSmall((-rc - -NEEDED_BASE) as usize));
        }

        Err(ReadError::WrongType)
    }

    #[must_use]
    pub fn value_coll_at(&self, index: usize) -> Option<Coll> {
        Coll::new(unsafe { map_value_handle(self.handle, index as i32) })
    }
}

/// The UDF's arguments, which are just a list.
pub type Args = Coll;

//==========================================================
// Building collections.
//

/// A list or map being built, to write to a bin or return.
///
/// The value lives on the host side; this is a handle to it. It is released when the call ends, so there is
/// nothing to free and no way to leak one.
#[derive(Clone, Copy)]
pub struct Builder {
    handle: i32,
}

impl Builder {
    #[must_use]
    pub fn handle(&self) -> i32 {
        self.handle
    }
}

/// Start a list.
#[must_use]
pub fn list(capacity: usize) -> Option<List> {
    let handle = unsafe { list_new(capacity as i32) };

    if handle <= 0 {
        None
    } else {
        Some(List { handle })
    }
}

/// Start a map. Entries keep the order they are put in.
#[must_use]
pub fn map(capacity: usize) -> Option<Map> {
    let handle = unsafe { map_new(capacity as i32) };

    if handle <= 0 {
        None
    } else {
        Some(Map { handle })
    }
}

/// A list under construction.
#[derive(Clone, Copy)]
pub struct List {
    handle: i32,
}

impl List {
    pub fn push_int(&self, value: i64) -> &Self {
        unsafe { list_push_int(self.handle, value) };
        self
    }

    pub fn push_str(&self, value: &str) -> &Self {
        unsafe { list_push_str(self.handle, value.as_ptr(), value.len() as i32) };
        self
    }

    pub fn push_double(&self, value: f64) -> &Self {
        unsafe { list_push(self.handle, val_double(value)) };
        self
    }

    pub fn push_bool(&self, value: bool) -> &Self {
        unsafe { list_push(self.handle, val_bool(i32::from(value))) };
        self
    }

    pub fn push_nil(&self) -> &Self {
        unsafe { list_push(self.handle, val_nil()) };
        self
    }

    pub fn push_blob(&self, value: &[u8]) -> &Self {
        unsafe { list_push(self.handle, val_blob(value.as_ptr(), value.len() as i32)) };
        self
    }

    /// Nest a list or a map.
    pub fn push_list(&self, value: &List) -> &Self {
        unsafe { list_push(self.handle, value.handle) };
        self
    }

    pub fn push_map(&self, value: &Map) -> &Self {
        unsafe { list_push(self.handle, value.handle) };
        self
    }

    #[must_use]
    pub fn build(self) -> Builder {
        Builder {
            handle: self.handle,
        }
    }
}

/// A map under construction.
#[derive(Clone, Copy)]
pub struct Map {
    handle: i32,
}

impl Map {
    pub fn put_int(&self, key: &str, value: i64) -> &Self {
        unsafe { map_put_str_int(self.handle, key.as_ptr(), key.len() as i32, value) };
        self
    }

    pub fn put_str(&self, key: &str, value: &str) -> &Self {
        unsafe {
            map_put_str_str(
                self.handle,
                key.as_ptr(),
                key.len() as i32,
                value.as_ptr(),
                value.len() as i32,
            )
        };
        self
    }

    pub fn put_double(&self, key: &str, value: f64) -> &Self {
        unsafe {
            map_put_str_handle(
                self.handle,
                key.as_ptr(),
                key.len() as i32,
                val_double(value),
            )
        };
        self
    }

    pub fn put_bool(&self, key: &str, value: bool) -> &Self {
        unsafe {
            map_put_str_handle(
                self.handle,
                key.as_ptr(),
                key.len() as i32,
                val_bool(i32::from(value)),
            )
        };
        self
    }

    pub fn put_nil(&self, key: &str) -> &Self {
        unsafe { map_put_str_handle(self.handle, key.as_ptr(), key.len() as i32, val_nil()) };
        self
    }

    pub fn put_list(&self, key: &str, value: &List) -> &Self {
        unsafe { map_put_str_handle(self.handle, key.as_ptr(), key.len() as i32, value.handle) };
        self
    }

    pub fn put_map(&self, key: &str, value: &Map) -> &Self {
        unsafe { map_put_str_handle(self.handle, key.as_ptr(), key.len() as i32, value.handle) };
        self
    }

    /// An integer-keyed entry, for a map that is not keyed by strings.
    ///
    /// Only integer values, unlike the string-keyed methods above. The host's `map_put` takes any two
    /// values, so widening this is a matter of adding methods, not of ABI.
    pub fn put_int_key(&self, key: i64, value: i64) -> &Self {
        unsafe { map_put(self.handle, val_int(key), val_int(value)) };
        self
    }

    #[must_use]
    pub fn build(self) -> Builder {
        Builder {
            handle: self.handle,
        }
    }
}

//==========================================================
// Results.
//

pub fn result_nil() {
    unsafe { raw_result_nil() };
}

pub fn result_int(value: i64) {
    unsafe { raw_result_int(value) };
}

pub fn result_double(value: f64) {
    unsafe { raw_result_double(value) };
}

pub fn result_bool(value: bool) {
    unsafe { raw_result_bool(i32::from(value)) };
}

pub fn result_str(value: &str) {
    unsafe { raw_result_str(value.as_ptr(), value.len() as i32) };
}

pub fn result_blob(value: &[u8]) {
    unsafe { raw_result_blob(value.as_ptr(), value.len() as i32) };
}

/// Return a list or map built with [`list`] / [`map`].
pub fn result_coll(value: &Builder) -> bool {
    unsafe { raw_result_handle(value.handle) == OK }
}

/// Fail the UDF with a message the client will see.
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

pub fn set_time_to_live(seconds: u32) -> bool {
    unsafe { raw_set_ttl(seconds as i32) == OK }
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

#[must_use]
pub fn now() -> i64 {
    unsafe { current_time() }
}

pub fn set_name<'a>(out: &'a mut [u8]) -> Result<&'a str, ReadError> {
    let rc = unsafe { raw_set_name(out.as_mut_ptr(), out.len() as i32) };
    let written = bytes_result(rc, out.len())?;

    core::str::from_utf8(&out[..written]).map_err(|_| ReadError::WrongType)
}

/// What kind of key the record carries, or [`Type::Absent`] when it was written without `send_key`.
#[must_use]
pub fn key() -> Type {
    Type::from_raw(unsafe { key_type() })
}

#[must_use]
pub fn key_as_int() -> i64 {
    unsafe { key_int() }
}

pub fn key_as_bytes<'a>(out: &'a mut [u8]) -> Result<&'a [u8], ReadError> {
    let rc = unsafe { raw_key_bytes(out.as_mut_ptr(), out.len() as i32) };
    let written = bytes_result(rc, out.len())?;

    Ok(&out[..written])
}

pub fn create() -> bool {
    unsafe { raw_rec_create() == OK }
}

pub fn update() -> bool {
    unsafe { raw_rec_update() == OK }
}

pub fn remove_record() -> bool {
    unsafe { raw_rec_remove() == OK }
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
// The export macro.
//

/// Export one or more functions as UDFs.
///
/// Each becomes a `(args: i32, argc: i32) -> i32` export, which is what the host calls. Returning 0 means
/// success; anything else fails the UDF, and [`fail`] adds a message.
///
/// Attributes are passed through, so doc comments on a UDF survive.
#[macro_export]
macro_rules! export {
    ($(
        $(#[$meta:meta])*
        fn $name:ident($args:ident : $args_ty:ty) -> i32 $body:block
    )*) => {
        $(
            $(#[$meta])*
            #[no_mangle]
            pub extern "C" fn $name(args_handle: i32, argc: i32) -> i32 {
                // `argc` is the host's count; the handle carries it too. Taken as an argument so a guest
                // that only wants the count need not call back for it.
                let _ = argc;

                let $args: $args_ty = $crate::args_from(args_handle);

                $body
            }
        )*
    };
}

/// Export a stream (aggregation) UDF.
///
/// A stream UDF is driven by the host, one record at a time, and **the whole stream shares one instance** — so
/// unlike a record UDF it can keep state between calls, in ordinary `static mut`. That is where an accumulator
/// belongs, and it is the reason aggregation is possible at all.
///
/// Each declaration produces the two exports the host looks for, derived from the name a client passes to its
/// query: `<name>_record` and `<name>_end`. Both take the argument list and return 0 for success.
///
/// ```ignore
/// use aerospike_udf_sdk_wasm::{self as as_udf, stream, Args};
///
/// static mut TOTAL: i64 = 0;
///
/// stream! {
///     fn sum {
///         record(_args: Args) -> i32 {
///             unsafe { TOTAL += as_udf::bin_int("n") };
///             0
///         }
///         end(_args: Args) -> i32 {
///             as_udf::result_int(unsafe { TOTAL });
///             0
///         }
///     }
/// }
/// ```
///
/// # One result per node
///
/// `end` runs once **per node**, so a client gets one partial per node and combines them itself. Lua's
/// aggregation looks seamless only because the Aerospike clients run its final reduce for them; nothing does
/// that for WASM. Summing partials is usually trivial, but it is the caller's job and it is not automatic.
#[macro_export]
macro_rules! stream {
    ($(
        $(#[$meta:meta])*
        fn $name:ident {
            record($rec_args:ident : $rec_ty:ty) -> i32 $rec_body:block
            end($end_args:ident : $end_ty:ty) -> i32 $end_body:block
        }
    )*) => {
        $(
            $crate::stream_export!(
                concat!(stringify!($name), "_record"), $rec_args, $rec_ty, $rec_body);
            $crate::stream_export!(
                concat!(stringify!($name), "_end"), $end_args, $end_ty, $end_body);
        )*
    };
}

/// One half of a [`stream!`] declaration.
///
/// `#[export_name]` rather than pasting identifiers together: concatenating idents needs a proc macro or the
/// unstable `concat_idents!`, and this crate deliberately has no dependencies and no nightly features.
/// `export_name` takes a string, which `concat!` can build.
#[doc(hidden)]
#[macro_export]
macro_rules! stream_export {
    ($symbol:expr, $args:ident, $args_ty:ty, $body:block) => {
        const _: () = {
            #[export_name = $symbol]
            pub extern "C" fn exported(args_handle: i32, argc: i32) -> i32 {
                // The host passes the count as well as the handle, so a guest that only wants the count need
                // not call back for it. Unused here.
                let _ = argc;

                let $args: $args_ty = $crate::args_from(args_handle);

                $body
            }
        };
    };
}

/// Build an [`Args`] from the raw handle the host passed. Used by [`export!`]; not otherwise interesting.
#[must_use]
pub fn args_from(handle: i32) -> Args {
    // A UDF called with no arguments gets handle 0, which is "no collection" - so `len()` answers 0 and
    // every accessor answers absent, rather than the guest needing to special-case it.
    Coll { handle }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The type codes are the ABI, so their values are the contract - the host sends them as integers.
    #[test]
    fn type_codes_match_the_host() {
        assert_eq!(Type::from_raw(-1), Type::Absent);
        assert_eq!(Type::from_raw(0), Type::Nil);
        assert_eq!(Type::from_raw(1), Type::Bool);
        assert_eq!(Type::from_raw(2), Type::Int);
        assert_eq!(Type::from_raw(3), Type::Double);
        assert_eq!(Type::from_raw(4), Type::Str);
        assert_eq!(Type::from_raw(5), Type::Blob);
        assert_eq!(Type::from_raw(6), Type::List);
        assert_eq!(Type::from_raw(7), Type::Map);
        assert_eq!(Type::from_raw(8), Type::Other);
        assert_eq!(Type::from_raw(9), Type::GeoJson);

        // A code this SDK does not know reads as absent rather than panicking or mis-typing.
        assert_eq!(Type::from_raw(99), Type::Absent);
    }

    /// The (out, cap) convention, including the arithmetic that recovers a needed length. Getting the sign
    /// wrong here would turn "needs 40 bytes" into a nonsense size, and the guest would loop.
    #[test]
    fn a_needed_length_is_recovered_exactly() {
        assert_eq!(bytes_result(0, 8), Ok(0));
        assert_eq!(bytes_result(5, 8), Ok(5));
        assert_eq!(bytes_result(NOT_FOUND, 8), Err(ReadError::Absent));

        for needed in 0..500usize {
            let rc = NEEDED_BASE - needed as i32;

            assert_eq!(bytes_result(rc, 8), Err(ReadError::TooSmall(needed)));
        }
    }

    /// A host claiming to have written more than the buffer holds is refused, not trusted - it is the one
    /// report that, if wrong, would already have overrun the slice.
    #[test]
    fn an_impossible_written_length_is_refused() {
        assert_eq!(bytes_result(9, 8), Err(ReadError::WrongType));
    }
}
