// Copyright 2026 Aerospike, Inc.
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

//! The raw C ABI, transcribed from the server's `as_native.h`.
//!
//! Nothing here is meant to be used directly — the rest of the crate is the safe surface over it. It is
//! public because the generated code from [`crate::udfs!`] and [`crate::module!`] refers to it, and a macro
//! cannot reach a private module of the crate that defines it.
//!
//! # This is a compiled contract
//!
//! These declarations must match `as_native.h` exactly, field for field and in order. Unlike a Rust-to-Rust
//! interface there is no compiler checking the two halves against each other: a mismatch is silent at build
//! time and wrong at run time. The one automatic check that exists is [`ABI_VERSION`], which the host
//! compares against the manifest and refuses on mismatch — so a stale SDK fails to load rather than
//! corrupting a record.

use core::ffi::{c_char, c_void};

/// Bumped only for a change a compiled plugin could not survive.
///
/// **2** moved fields inside [`AggExport`] — `accumulate` gained a parameter and `heap_used`,
/// `from_partial` and `finalize` arrived — so a version 1 plugin would read a function pointer from the wrong
/// offset. That is the failure this number turns into a startup message.
pub const ABI_VERSION: u32 = 2;

pub const OK: i32 = 0;
pub const ERR: i32 = -1;
pub const NOT_FOUND: i32 = -2;
pub const TYPE_MISMATCH: i32 = -3;
pub const REFUSED: i32 = -4;

/// Not an error, which is why it is positive: "I took that record, and I am now at capacity."
pub const FULL: i32 = 1;

pub const TYPE_ABSENT: i32 = -1;
pub const TYPE_NIL: i32 = 0;
pub const TYPE_BOOL: i32 = 1;
pub const TYPE_INT: i32 = 2;
pub const TYPE_DOUBLE: i32 = 3;
pub const TYPE_STR: i32 = 4;
pub const TYPE_BLOB: i32 = 5;
pub const TYPE_LIST: i32 = 6;
pub const TYPE_MAP: i32 = 7;
pub const TYPE_OTHER: i32 = 8;
pub const TYPE_GEOJSON: i32 = 9;

pub const LOG_WARN: i32 = 0;
pub const LOG_INFO: i32 = 1;
pub const LOG_DEBUG: i32 = 2;

/// Opaque, host-owned.
#[repr(C)]
pub struct Rec {
    _opaque: [u8; 0],
}

/// Opaque, host-owned.
#[repr(C)]
pub struct Ctx {
    _opaque: [u8; 0],
}

/// Opaque, host-owned — where an aggregation's output goes.
#[repr(C)]
pub struct Emit {
    _opaque: [u8; 0],
}

/// A position inside a packed collection. **Passed by value, never allocated.**
///
/// Sixteen bytes: the packed bytes, their length, and the offset into them. Must match `as_native_cursor` exactly.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Cursor {
    pub buf: *const u8,
    pub buf_sz: u32,
    pub offset: u32,
}

impl Cursor {
    pub const EMPTY: Self = Self {
        buf: core::ptr::null(),
        buf_sz: 0,
        offset: 0,
    };
}

/// One value, 16 bytes, flat — and every pointer in it is **borrowed**.
///
/// `len` sits beside `kind` rather than inside the union, which is what keeps this to 16 bytes and lets a
/// collection carry its element count without a call to ask for it.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Value {
    pub kind: i32,
    pub len: u32,
    pub u: ValueUnion,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union ValueUnion {
    /// `TYPE_INT`, and `TYPE_BOOL` as 0 or 1 — bool has no member of its own, deliberately.
    pub i: i64,
    pub d: f64,
    /// `TYPE_STR` / `TYPE_BLOB` / `TYPE_GEOJSON`. Not NUL-terminated.
    pub p: *const u8,
    /// `TYPE_LIST` / `TYPE_MAP`.
    pub h: *const c_void,
}

impl Value {
    pub const NIL: Self = Self {
        kind: TYPE_NIL,
        len: 0,
        u: ValueUnion { i: 0 },
    };
}

pub type ListCb = unsafe extern "C" fn(index: u32, val: *const Value, udata: *mut c_void) -> bool;
pub type MapCb =
    unsafe extern "C" fn(key: *const Value, val: *const Value, udata: *mut c_void) -> bool;
pub type BinCb =
    unsafe extern "C" fn(name: *const c_char, name_len: u32, udata: *mut c_void) -> bool;

/// The host's services. Order and signatures must match `as_native_host` exactly.
#[repr(C)]
pub struct Host {
    pub abi_version: u32,
    pub size: u32,

    // Record - reading.
    pub rec_get:
        unsafe extern "C" fn(*const Rec, *const c_char, u32, *mut Value) -> i32,
    pub rec_get_int: unsafe extern "C" fn(*const Rec, *const c_char, u32, *mut i64) -> i32,
    pub rec_get_double: unsafe extern "C" fn(*const Rec, *const c_char, u32, *mut f64) -> i32,
    pub rec_get_bool: unsafe extern "C" fn(*const Rec, *const c_char, u32, *mut bool) -> i32,
    pub rec_get_bytes:
        unsafe extern "C" fn(*const Rec, *const c_char, u32, *mut *const u8, *mut u32) -> i32,
    pub rec_numbins: unsafe extern "C" fn(*const Rec) -> u32,
    pub rec_bin_names: unsafe extern "C" fn(*const Rec, BinCb, *mut c_void) -> i32,

    // Record - metadata.
    pub rec_ttl: unsafe extern "C" fn(*const Rec) -> u32,
    pub rec_gen: unsafe extern "C" fn(*const Rec) -> u16,
    pub rec_last_update_time: unsafe extern "C" fn(*const Rec) -> u64,
    pub rec_device_size: unsafe extern "C" fn(*const Rec) -> u32,
    pub rec_memory_size: unsafe extern "C" fn(*const Rec) -> u32,
    pub rec_setname: unsafe extern "C" fn(*const Rec) -> *const c_char,
    pub rec_digest: unsafe extern "C" fn(*const Rec) -> *const u8,
    pub rec_key: unsafe extern "C" fn(*const Rec, *mut Value) -> i32,

    // Record - writing.
    pub rec_set:
        unsafe extern "C" fn(*mut Ctx, *mut Rec, *const c_char, u32, *const Value) -> i32,
    pub rec_set_int: unsafe extern "C" fn(*mut Ctx, *mut Rec, *const c_char, u32, i64) -> i32,
    pub rec_set_double: unsafe extern "C" fn(*mut Ctx, *mut Rec, *const c_char, u32, f64) -> i32,
    pub rec_set_bool: unsafe extern "C" fn(*mut Ctx, *mut Rec, *const c_char, u32, bool) -> i32,
    pub rec_set_str:
        unsafe extern "C" fn(*mut Ctx, *mut Rec, *const c_char, u32, *const u8, u32) -> i32,
    pub rec_set_blob:
        unsafe extern "C" fn(*mut Ctx, *mut Rec, *const c_char, u32, *const u8, u32) -> i32,
    pub rec_remove_bin: unsafe extern "C" fn(*mut Ctx, *mut Rec, *const c_char, u32) -> i32,
    pub rec_set_ttl: unsafe extern "C" fn(*mut Rec, u32) -> i32,
    pub rec_update: unsafe extern "C" fn(*mut Ctx, *mut Rec) -> i32,
    pub rec_create: unsafe extern "C" fn(*mut Ctx, *mut Rec) -> i32,
    pub rec_delete: unsafe extern "C" fn(*mut Ctx, *mut Rec) -> i32,
    pub rec_exists: unsafe extern "C" fn(*const Rec) -> bool,

    // Collections - reading.
    pub list_size: unsafe extern "C" fn(*const c_void) -> u32,
    pub list_get: unsafe extern "C" fn(*const c_void, u32, *mut Value) -> i32,
    pub list_get_ints: unsafe extern "C" fn(*const c_void, u32, u32, *mut i64) -> i32,
    pub list_get_doubles: unsafe extern "C" fn(*const c_void, u32, u32, *mut f64) -> i32,
    pub list_foreach: unsafe extern "C" fn(*const c_void, ListCb, *mut c_void) -> i32,
    pub map_size: unsafe extern "C" fn(*const c_void) -> u32,
    pub map_get_str:
        unsafe extern "C" fn(*const c_void, *const c_char, u32, *mut Value) -> i32,
    pub map_get_int: unsafe extern "C" fn(*const c_void, i64, *mut Value) -> i32,
    pub map_foreach: unsafe extern "C" fn(*const c_void, MapCb, *mut c_void) -> i32,

    // Collections - building.
    pub list_new: unsafe extern "C" fn(*mut Ctx, u32, *mut Value) -> i32,
    pub list_append: unsafe extern "C" fn(*mut Ctx, *mut c_void, *const Value) -> i32,
    pub list_append_int: unsafe extern "C" fn(*mut Ctx, *mut c_void, i64) -> i32,
    pub list_append_double: unsafe extern "C" fn(*mut Ctx, *mut c_void, f64) -> i32,
    pub list_append_str: unsafe extern "C" fn(*mut Ctx, *mut c_void, *const u8, u32) -> i32,
    pub map_new: unsafe extern "C" fn(*mut Ctx, u32, *mut Value) -> i32,
    pub map_put:
        unsafe extern "C" fn(*mut Ctx, *mut c_void, *const Value, *const Value) -> i32,
    pub map_put_str_int:
        unsafe extern "C" fn(*mut Ctx, *mut c_void, *const c_char, u32, i64) -> i32,
    pub map_put_str_str:
        unsafe extern "C" fn(*mut Ctx, *mut c_void, *const c_char, u32, *const u8, u32) -> i32,
    pub map_put_str_double:
        unsafe extern "C" fn(*mut Ctx, *mut c_void, *const c_char, u32, f64) -> i32,
    pub map_put_int_int: unsafe extern "C" fn(*mut Ctx, *mut c_void, i64, i64) -> i32,

    // Emitting, from inside an aggregation.
    pub emit_record: unsafe extern "C" fn(*mut Ctx, *mut Emit, *const Rec) -> i32,
    pub emit_value: unsafe extern "C" fn(*mut Ctx, *mut Emit, *const Value) -> i32,

    // Diagnostics and control.
    pub fail: unsafe extern "C" fn(*mut Ctx, *const c_char, u32),
    pub log: unsafe extern "C" fn(*mut Ctx, i32, *const c_char, u32),
    pub deadline_exceeded: unsafe extern "C" fn(*const Ctx) -> bool,

    // ------------------------------------------------
    // Added after ABI 1.0. **Genuinely last, always.**
    //
    // A host older than this field simply has a smaller `size`, and `Record::supports_bytes_mut` is how a plugin
    // asks. Inserting a field anywhere but the end would silently rebind every call after it - which nearly
    // happened once in this SDK, when a pair of borrowed-read entries were added in the middle and moved
    // `deadline_exceeded` under them.
    /// Writable bytes for a plain blob bin, modified in place. See [`crate::Record::with_bytes_mut`].
    pub rec_get_bytes_mut: unsafe extern "C" fn(
        *mut Ctx,
        *mut Rec,
        *const c_char,
        u32,
        *mut *mut u8,
        *mut u32,
    ) -> i32,

    /// Borrow a blob bin's bytes with no copy at all. See [`crate::Record::with_bytes_ref`].
    pub rec_get_bytes_ref: unsafe extern "C" fn(
        *const Rec,
        *const c_char,
        u32,
        *mut *const u8,
        *mut u32,
    ) -> i32,

    /// Apply one typed op immediately. See [`crate::Record::op`].
    pub rec_op: unsafe extern "C" fn(
        *mut Ctx,
        *mut Rec,
        *const c_char,
        u32,
        u8,
        u8,
        *const u8,
        u32,
    ) -> i32,

    /// Elements in a list bin, read from its packed form. See [`crate::Record::list_len`].
    pub rec_list_count:
        unsafe extern "C" fn(*const Rec, *const c_char, u32, *mut u32) -> i32,
    /// Read integers straight out of a list bin's packed form. See [`crate::Record::list_ints`].
    pub rec_list_ints:
        unsafe extern "C" fn(*const Rec, *const c_char, u32, u32, u32, *mut i64) -> i32,
    /// Read doubles straight out of a list bin's packed form. See [`crate::Record::list_doubles`].
    pub rec_list_doubles:
        unsafe extern "C" fn(*const Rec, *const c_char, u32, u32, u32, *mut f64) -> i32,

    /// Entries in a map bin, from its packed form. See [`crate::Record::map_len`].
    pub rec_map_count: unsafe extern "C" fn(*const Rec, *const c_char, u32, *mut u32) -> i32,
    /// Look up an integer key in a map bin's packed form. See [`crate::Record::map_get_int`].
    pub rec_map_get_int:
        unsafe extern "C" fn(*const Rec, *const c_char, u32, i64, *mut Value) -> i32,
    /// Look up a string key in a map bin's packed form. See [`crate::Record::map_get_str`].
    pub rec_map_get_str:
        unsafe extern "C" fn(*const Rec, *const c_char, u32, *const c_char, u32, *mut Value) -> i32,

    /// Write an integer array to a bin as a packed list. See [`crate::Record::set_list_ints`].
    pub rec_set_list_ints:
        unsafe extern "C" fn(*mut Ctx, *mut Rec, *const c_char, u32, *const i64, u32) -> i32,
    /// Write a double array to a bin as a packed list. See [`crate::Record::set_list_doubles`].
    pub rec_set_list_doubles:
        unsafe extern "C" fn(*mut Ctx, *mut Rec, *const c_char, u32, *const f64, u32) -> i32,

    /// Open a cursor on a list or map bin. See [`crate::Record::cursor`].
    pub rec_cursor_open:
        unsafe extern "C" fn(*const Rec, *const c_char, u32, *mut Cursor, *mut Value) -> i32,
    /// Read the value at a cursor and advance. See [`crate::PackedCursor::next`].
    pub cursor_next: unsafe extern "C" fn(*mut Cursor, *mut Value, *mut Cursor) -> i32,
    /// Advance past values without reading them. See [`crate::PackedCursor::skip`].
    pub cursor_skip: unsafe extern "C" fn(*mut Cursor, u32) -> i32,
}

/// The op an immediate call applies.
///
/// **These are the server's own `AS_MSG_OP_*` codes**, not a parallel set: a plugin op and a client op are the same op
/// reaching the same function, so there is no second encoding to keep in step. The scalar ops carry a raw particle
/// payload and involve no msgpack at all - a collection op does, and for those the right answer is to reuse the
/// client's `Operation` packing rather than to grow a second encoder here.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Write = 2,
    Incr = 5,
    Append = 9,
    Prepend = 10,
}

/// The particle type of a value handed to an immediate op. The server's `AS_PARTICLE_TYPE_*`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticleType {
    Integer = 1,
    Double = 2,
    String = 3,
    Blob = 4,
}

/// The `size` a host must report for [`Host::rec_get_bytes_mut`] to be present.
///
/// Compared against the host's own `size` rather than constructed and measured: a zeroed `Host` would be undefined
/// behaviour, since its fields are non-null function pointers.
pub const BYTES_MUT_SIZE: usize = core::mem::size_of::<Host>();

/// The `size` a host must report for the immediate-op path to be present.
pub const IMMEDIATE_OPS_SIZE: usize =
    core::mem::size_of::<Host>() - (11 * core::mem::size_of::<usize>());

/// The `size` a host must report for the packed-list cursor to be present.
pub const PACKED_LIST_SIZE: usize =
    core::mem::size_of::<Host>() - (8 * core::mem::size_of::<usize>());

/// The `size` a host must report for the packed-map cursor to be present.
pub const PACKED_MAP_SIZE: usize =
    core::mem::size_of::<Host>() - (5 * core::mem::size_of::<usize>());

/// The `size` a host must report for the packed writers to be present.
pub const PACKED_WRITE_SIZE: usize =
    core::mem::size_of::<Host>() - (3 * core::mem::size_of::<usize>());

/// The `size` a host must report for the nested cursor to be present.
pub const CURSOR_SIZE: usize = core::mem::size_of::<Host>();


pub type RecordFn = unsafe extern "C" fn(
    ctx: *mut Ctx,
    rec: *mut Rec,
    args: *const Value,
    n_args: u32,
    result: *mut Value,
) -> i32;

#[repr(C)]
pub struct RecordExport {
    pub name: *const c_char,
    pub min_args: u32,
    pub max_args: u32,
    pub func: RecordFn,
}

/// The five operations of an aggregation, of which the server calls three.
///
/// `init`, `accumulate` and `to_partial` are the server's half. `from_partial`, `merge` and `finalize` belong
/// to whoever combines the partials — which is the client, running this same code linked as a library.
#[repr(C)]
pub struct AggExport {
    pub name: *const c_char,
    pub state_size: u32,
    pub state_align: u32,
    pub min_args: u32,
    pub max_args: u32,
    pub init: Option<
        unsafe extern "C" fn(*mut Ctx, *mut c_void, *const Value, u32) -> i32,
    >,
    pub accumulate: unsafe extern "C" fn(*mut Ctx, *mut c_void, *const Rec, *mut Emit) -> i32,
    pub heap_used: Option<unsafe extern "C" fn(*const c_void) -> u64>,
    pub to_partial: unsafe extern "C" fn(*mut Ctx, *mut c_void, *mut Value) -> i32,
    pub from_partial:
        Option<unsafe extern "C" fn(*mut Ctx, *mut c_void, *const Value) -> i32>,
    pub merge: Option<unsafe extern "C" fn(*mut Ctx, *mut c_void, *const c_void) -> i32>,
    pub finalize: Option<unsafe extern "C" fn(*mut Ctx, *mut c_void, *mut Value) -> i32>,
    pub destroy: Option<unsafe extern "C" fn(*mut c_void)>,
}

#[repr(C)]
pub struct Manifest {
    pub abi_version: u32,
    pub module_name: *const c_char,
    pub module_version: *const c_char,
    pub n_record_fns: u32,
    pub record_fns: *const RecordExport,
    pub n_agg_fns: u32,
    pub agg_fns: *const AggExport,
}

#[repr(C)]
pub struct Option_ {
    pub key: *const c_char,
    pub key_len: u32,
    pub value: *const c_char,
    pub value_len: u32,
}

#[repr(C)]
pub struct ModuleConfig {
    pub module_name: *const c_char,
    pub n_options: u32,
    pub options: *const Option_,
}

// SAFETY: these are immutable descriptors, written once at compile time and only ever read. The raw pointers
// they carry are to `static` C strings and `static` arrays in the plugin's own image, which live for the
// process. `static` items must be `Sync`, and there is nothing here to race on.
unsafe impl Sync for Manifest {}
unsafe impl Sync for RecordExport {}
unsafe impl Sync for AggExport {}

#[cfg(test)]
mod tests {
    use super::*;

    /// A value must stay 16 bytes.
    ///
    /// Not a style preference: the host writes these and the plugin reads them, so a layout disagreement is
    /// silent corruption. This is the cheapest half of that guarantee — the other half is `ABI_VERSION`.
    #[test]
    fn a_value_is_sixteen_bytes() {
        assert_eq!(core::mem::size_of::<Value>(), 16);
        assert_eq!(core::mem::align_of::<Value>(), 8);
    }

    /// The type codes are the ABI's own numbering, matching `as_native.h`.
    #[test]
    fn type_codes_match_the_host() {
        assert_eq!(TYPE_ABSENT, -1);
        assert_eq!(TYPE_NIL, 0);
        assert_eq!(TYPE_BOOL, 1);
        assert_eq!(TYPE_INT, 2);
        assert_eq!(TYPE_DOUBLE, 3);
        assert_eq!(TYPE_STR, 4);
        assert_eq!(TYPE_BLOB, 5);
        assert_eq!(TYPE_LIST, 6);
        assert_eq!(TYPE_MAP, 7);
        assert_eq!(TYPE_OTHER, 8);
        assert_eq!(TYPE_GEOJSON, 9);
    }

    /// Status codes match the WebAssembly ABI's, which is not a coincidence — a UDF author moving between
    /// them should not have to learn two sets.
    #[test]
    fn status_codes_are_shared_with_the_other_udf_abi() {
        assert_eq!(OK, 0);
        assert_eq!(ERR, -1);
        assert_eq!(NOT_FOUND, -2);
        assert_eq!(REFUSED, -4);
    }

    /// `FULL` must be **positive**, and that is not cosmetic.
    ///
    /// Every status check on the aggregation path reads `< OK` to mean failure. If `FULL` were negative — the
    /// obvious mistake, since every other non-zero status here is — a full accumulator would abort the query
    /// instead of being dumped, and the symptom would be a query that fails only on large data sets.
    #[test]
    fn full_is_not_an_error() {
        assert!(FULL > OK, "FULL must be positive so that `< OK` still means failure");
    }
}
