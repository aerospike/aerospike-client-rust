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

//! Values, and the collections they can be.
//!
//! Everything here **borrows**. A string is a pointer and a length into the record's own memory; a list or a
//! map is a handle the host owns and this walks in place. The lifetime parameter is what keeps that honest —
//! it comes from the [`Record`](crate::Record) or [`Ctx`](crate::Ctx) the value was obtained through, so no
//! value can outlive the call that produced it.

use core::ffi::c_void;
use core::marker::PhantomData;

use crate::{abi, check, host, Error, Result};

/// What a value is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Not a value at all — what a bin that does not exist reads as.
    Absent,
    /// Nil.
    Nil,
    /// A boolean.
    Bool,
    /// A 64-bit integer.
    Int,
    /// A double.
    Double,
    /// A string.
    Str,
    /// A byte blob.
    Blob,
    /// A list.
    List,
    /// A map.
    Map,
    /// GeoJSON.
    GeoJson,
    /// Something a UDF has no business interpreting.
    Other,
}

impl Kind {
    fn from_raw(kind: i32) -> Self {
        match kind {
            abi::TYPE_NIL => Self::Nil,
            abi::TYPE_BOOL => Self::Bool,
            abi::TYPE_INT => Self::Int,
            abi::TYPE_DOUBLE => Self::Double,
            abi::TYPE_STR => Self::Str,
            abi::TYPE_BLOB => Self::Blob,
            abi::TYPE_LIST => Self::List,
            abi::TYPE_MAP => Self::Map,
            abi::TYPE_GEOJSON => Self::GeoJson,
            abi::TYPE_OTHER => Self::Other,
            // Anything unrecognised, including TYPE_ABSENT, and including a code from a newer host than this
            // SDK knows about - which is a thing to treat as "not something I can read", not to panic on.
            _ => Self::Absent,
        }
    }
}

/// One value: a bin, an argument, an element, or something built to return.
#[derive(Clone, Copy)]
pub struct Value<'a> {
    raw: abi::Value,
    ctx: *mut abi::Ctx,
    _borrow: PhantomData<&'a ()>,
}

impl<'a> Value<'a> {
    pub(crate) fn from_raw(raw: abi::Value, ctx: *mut abi::Ctx) -> Self {
        Self {
            raw,
            ctx,
            _borrow: PhantomData,
        }
    }

    pub(crate) fn as_raw(&self) -> *const abi::Value {
        &self.raw
    }

    /// A nil value.
    pub fn nil() -> Self {
        Self::from_raw(abi::Value::NIL, core::ptr::null_mut())
    }

    /// An integer value.
    pub fn int(value: i64) -> Self {
        Self::from_raw(
            abi::Value {
                kind: abi::TYPE_INT,
                len: 0,
                u: abi::ValueUnion { i: value },
            },
            core::ptr::null_mut(),
        )
    }

    /// A double value.
    pub fn double(value: f64) -> Self {
        Self::from_raw(
            abi::Value {
                kind: abi::TYPE_DOUBLE,
                len: 0,
                u: abi::ValueUnion { d: value },
            },
            core::ptr::null_mut(),
        )
    }

    /// A boolean value.
    pub fn boolean(value: bool) -> Self {
        Self::from_raw(
            abi::Value {
                kind: abi::TYPE_BOOL,
                len: 0,
                u: abi::ValueUnion {
                    i: i64::from(value),
                },
            },
            core::ptr::null_mut(),
        )
    }

    /// A borrowed string as a value.
    ///
    /// The host copies a string when it stores or returns one, so the borrow only has to outlive the call it
    /// is handed to - which the lifetime says.
    pub fn string(value: &'a str) -> Self {
        Self::from_raw(
            abi::Value {
                kind: abi::TYPE_STR,
                len: value.len() as u32,
                u: abi::ValueUnion {
                    p: value.as_ptr(),
                },
            },
            core::ptr::null_mut(),
        )
    }

    /// Borrowed bytes as a value.
    pub fn blob(value: &'a [u8]) -> Self {
        Self::from_raw(
            abi::Value {
                kind: abi::TYPE_BLOB,
                len: value.len() as u32,
                u: abi::ValueUnion {
                    p: value.as_ptr(),
                },
            },
            core::ptr::null_mut(),
        )
    }

    /// What this is.
    pub fn kind(&self) -> Kind {
        Kind::from_raw(self.raw.kind)
    }

    /// Whether the thing asked for was not there.
    pub fn is_absent(&self) -> bool {
        self.raw.kind == abi::TYPE_ABSENT
    }

    /// Whether this is nil.
    pub fn is_nil(&self) -> bool {
        self.raw.kind == abi::TYPE_NIL
    }

    /// As an integer, if it is one.
    pub fn as_i64(&self) -> Option<i64> {
        if self.raw.kind == abi::TYPE_INT {
            // SAFETY: the tag says the union holds an integer.
            Some(unsafe { self.raw.u.i })
        } else {
            None
        }
    }

    /// As a number. An integer widens, because a client that wrote `1` and one that wrote `1.0` did not mean
    /// different things.
    pub fn as_f64(&self) -> Option<f64> {
        match self.raw.kind {
            // SAFETY: the tag selects the member in each arm.
            abi::TYPE_DOUBLE => Some(unsafe { self.raw.u.d }),
            abi::TYPE_INT => Some(unsafe { self.raw.u.i } as f64),
            _ => None,
        }
    }

    /// As a boolean, if it is one.
    pub fn as_bool(&self) -> Option<bool> {
        if self.raw.kind == abi::TYPE_BOOL {
            // SAFETY: bool rides in the integer member as 0 or 1.
            Some(unsafe { self.raw.u.i } != 0)
        } else {
            None
        }
    }

    /// The bytes of a string, blob or geojson value — borrowed, not copied.
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        match self.raw.kind {
            abi::TYPE_STR | abi::TYPE_BLOB | abi::TYPE_GEOJSON => {
                // SAFETY: the tag says the union holds a pointer, and `len` its length. Both describe memory
                // the host owns for at least the lifetime this value carries.
                let p = unsafe { self.raw.u.p };

                if p.is_null() {
                    return Some(&[]);
                }

                Some(unsafe { core::slice::from_raw_parts(p, self.raw.len as usize) })
            }
            _ => None,
        }
    }

    /// As a string, if it is one and is valid UTF-8.
    pub fn as_str(&self) -> Option<&'a str> {
        match self.raw.kind {
            abi::TYPE_STR | abi::TYPE_GEOJSON => {
                core::str::from_utf8(self.as_bytes()?).ok()
            }
            _ => None,
        }
    }

    /// As a list, if it is one. Walked in place; nothing is copied.
    pub fn as_list(&self) -> Option<List<'a>> {
        if self.raw.kind == abi::TYPE_LIST {
            Some(List::from_raw(self.raw, self.ctx))
        } else {
            None
        }
    }

    /// As a map, if it is one.
    pub fn as_map(&self) -> Option<Map<'a>> {
        if self.raw.kind == abi::TYPE_MAP {
            Some(Map::from_raw(self.raw, self.ctx))
        } else {
            None
        }
    }
}

impl core::fmt::Debug for Value<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.kind() {
            Kind::Int => write!(f, "Int({:?})", self.as_i64()),
            Kind::Double => write!(f, "Double({:?})", self.as_f64()),
            Kind::Bool => write!(f, "Bool({:?})", self.as_bool()),
            Kind::Str => write!(f, "Str({:?})", self.as_str()),
            Kind::Blob => write!(f, "Blob({} bytes)", self.raw.len),
            Kind::List => write!(f, "List({} elements)", self.raw.len),
            Kind::Map => write!(f, "Map({} entries)", self.raw.len),
            other => write!(f, "{other:?}"),
        }
    }
}

//==========================================================
// Arguments.
//

/// The arguments a client sent, already flat.
#[derive(Clone, Copy)]
pub struct Args<'a> {
    ptr: *const abi::Value,
    len: u32,
    ctx: *mut abi::Ctx,
    _borrow: PhantomData<&'a ()>,
}

impl<'a> Args<'a> {
    #[doc(hidden)]
    pub fn from_raw(ptr: *const abi::Value, len: u32, ctx: *mut abi::Ctx) -> Self {
        Self {
            ptr,
            len,
            ctx,
            _borrow: PhantomData,
        }
    }

    /// How many were sent.
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Whether none were.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The argument at `pos`, or an absent value if the client sent fewer.
    ///
    /// Absent rather than a panic: "the caller left it out" is a normal thing for a UDF to handle, and
    /// [`Option<T>`] as an argument type reads exactly that.
    pub fn get(&self, pos: u32) -> Value<'a> {
        if pos >= self.len || self.ptr.is_null() {
            return Value::from_raw(
                abi::Value {
                    kind: abi::TYPE_ABSENT,
                    len: 0,
                    u: abi::ValueUnion { i: 0 },
                },
                self.ctx,
            );
        }

        // SAFETY: pos is in range, and the host's array is valid for this call.
        Value::from_raw(unsafe { *self.ptr.add(pos as usize) }, self.ctx)
    }
}

//==========================================================
// Lists.
//

/// A list — one read from a bin, or one being built to return.
#[derive(Clone, Copy)]
pub struct List<'a> {
    raw: abi::Value,
    ctx: *mut abi::Ctx,
    _borrow: PhantomData<&'a ()>,
}

impl<'a> List<'a> {
    pub(crate) fn from_raw(raw: abi::Value, ctx: *mut abi::Ctx) -> Self {
        Self {
            raw,
            ctx,
            _borrow: PhantomData,
        }
    }

    fn handle(&self) -> *const c_void {
        // SAFETY: a List is only built from a value whose tag is LIST.
        unsafe { self.raw.u.h }
    }

    /// As a value, for returning or nesting.
    pub fn as_value(&self) -> Value<'a> {
        Value::from_raw(self.raw, self.ctx)
    }

    /// How many elements.
    pub fn len(&self) -> u32 {
        // SAFETY: a live handle.
        unsafe { (host().list_size)(self.handle()) }
    }

    /// Whether it is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// One element. Absent if the index is past the end.
    ///
    /// O(1) — a list bin deserialises to an array, so this is a range-checked index. But it is still **one
    /// host call per element**: prefer [`ints_into`](List::ints_into) for a whole list.
    pub fn get(&self, index: u32) -> Value<'a> {
        let mut raw = abi::Value::NIL;

        // SAFETY: the host always writes a value - ABSENT when out of range.
        unsafe {
            (host().list_get)(self.handle(), index, &mut raw);
        }

        Value::from_raw(raw, self.ctx)
    }

    /// Copy integers into `out`, starting at `from`, and report how many were written.
    ///
    /// **One host call for the whole window.** This is the accessor that matters: in the WebAssembly module,
    /// switching a summing loop from per-element reads to this shape is what took an aggregation from 1.48×
    /// slower than Lua to level with it.
    ///
    /// Errors with [`Error::TypeMismatch`] if anything in the window is not an integer — a short read with no
    /// way to say where it stopped would be worse than an error.
    pub fn ints_into(&self, from: u32, out: &mut [i64]) -> Result<usize> {
        // SAFETY: `out` is a live slice and its length is passed honestly.
        let rc = unsafe {
            (host().list_get_ints)(
                self.handle(),
                from,
                out.len() as u32,
                out.as_mut_ptr(),
            )
        };

        if rc < 0 {
            check(rc)?;
        }

        Ok(rc as usize)
    }

    /// As [`ints_into`](List::ints_into), for doubles. Integers widen.
    pub fn doubles_into(&self, from: u32, out: &mut [f64]) -> Result<usize> {
        // SAFETY: as above.
        let rc = unsafe {
            (host().list_get_doubles)(
                self.handle(),
                from,
                out.len() as u32,
                out.as_mut_ptr(),
            )
        };

        if rc < 0 {
            check(rc)?;
        }

        Ok(rc as usize)
    }

    /// Total every integer element, reading in bulk.
    ///
    /// The shape most summing UDFs want, so it is here rather than written out each time — one host call per
    /// 256 elements.
    pub fn sum_ints(&self) -> Result<i64> {
        let len = self.len();
        let mut buf = [0i64; 256];
        let mut total = 0i64;
        let mut from = 0u32;

        while from < len {
            let n = self.ints_into(from, &mut buf)?;

            if n == 0 {
                break;
            }

            for v in &buf[..n] {
                total = total.wrapping_add(*v);
            }

            from += n as u32;
        }

        Ok(total)
    }

    /// Visit every element. Return `false` from `f` to stop.
    pub fn for_each<F: FnMut(u32, Value<'a>) -> bool>(&self, mut f: F) {
        let mut visitor = Visitor {
            f: &mut f,
            ctx: self.ctx,
            _borrow: PhantomData::<&'a ()>,
        };

        // SAFETY: the trampoline matches the ABI, and `visitor` outlives the call.
        unsafe {
            (host().list_foreach)(
                self.handle(),
                list_trampoline::<'a, F>,
                (&mut visitor as *mut Visitor<'a, _>).cast(),
            );
        }
    }

    //------------------------------------------------
    // Building.
    //

    /// Append an integer.
    pub fn push_int(&self, value: i64) -> Result<()> {
        // SAFETY: appending to a list this call owns.
        check(unsafe { (host().list_append_int)(self.ctx, self.handle() as *mut c_void, value) })
    }

    /// Append a double.
    pub fn push_f64(&self, value: f64) -> Result<()> {
        // SAFETY: as above.
        check(unsafe { (host().list_append_double)(self.ctx, self.handle() as *mut c_void, value) })
    }

    /// Append a string. The host copies it.
    pub fn push_str(&self, value: &str) -> Result<()> {
        // SAFETY: as above; the host copies the bytes.
        check(unsafe {
            (host().list_append_str)(
                self.ctx,
                self.handle() as *mut c_void,
                value.as_ptr(),
                value.len() as u32,
            )
        })
    }

    /// Append any value, including a nested list or map.
    pub fn push(&self, value: &Value<'_>) -> Result<()> {
        // SAFETY: as above.
        check(unsafe {
            (host().list_append)(self.ctx, self.handle() as *mut c_void, value.as_raw())
        })
    }

    /// Append a nested list.
    pub fn push_list(&self, value: &List<'_>) -> Result<()> {
        self.push(&value.as_value())
    }

    /// Append a nested map.
    pub fn push_map(&self, value: &Map<'_>) -> Result<()> {
        self.push(&value.as_value())
    }
}

//==========================================================
// Maps.
//

/// A map — one read from a bin, or one being built to return.
#[derive(Clone, Copy)]
pub struct Map<'a> {
    raw: abi::Value,
    ctx: *mut abi::Ctx,
    _borrow: PhantomData<&'a ()>,
}

impl<'a> Map<'a> {
    pub(crate) fn from_raw(raw: abi::Value, ctx: *mut abi::Ctx) -> Self {
        Self {
            raw,
            ctx,
            _borrow: PhantomData,
        }
    }

    fn handle(&self) -> *const c_void {
        // SAFETY: a Map is only built from a value whose tag is MAP.
        unsafe { self.raw.u.h }
    }

    /// As a value, for returning or nesting.
    pub fn as_value(&self) -> Value<'a> {
        Value::from_raw(self.raw, self.ctx)
    }

    /// How many entries.
    pub fn len(&self) -> u32 {
        // SAFETY: a live handle.
        unsafe { (host().map_size)(self.handle()) }
    }

    /// Whether it is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The value under a string key. Absent if there is none.
    ///
    /// **One hash lookup, no allocation** — the key is built on the host's stack. Prefer this to scanning
    /// entries: doing the scan instead is what made an equivalent WebAssembly aggregation slower than Lua.
    pub fn get_str(&self, key: &str) -> Value<'a> {
        let mut raw = abi::Value::NIL;

        // SAFETY: the host always writes a value - ABSENT for a miss.
        unsafe {
            (host().map_get_str)(
                self.handle(),
                key.as_ptr().cast(),
                key.len() as u32,
                &mut raw,
            );
        }

        Value::from_raw(raw, self.ctx)
    }

    /// The value under an integer key.
    pub fn get_int(&self, key: i64) -> Value<'a> {
        let mut raw = abi::Value::NIL;

        // SAFETY: as above.
        unsafe {
            (host().map_get_int)(self.handle(), key, &mut raw);
        }

        Value::from_raw(raw, self.ctx)
    }

    /// Visit every entry. Return `false` from `f` to stop.
    ///
    /// Two values per entry and one call for the whole map — but if you know the key you want, ask for it
    /// instead.
    pub fn for_each<F: FnMut(Value<'a>, Value<'a>) -> bool>(&self, mut f: F) {
        let mut visitor = MapVisitor {
            f: &mut f,
            ctx: self.ctx,
            _borrow: PhantomData::<&'a ()>,
        };

        // SAFETY: the trampoline matches the ABI, and `visitor` outlives the call.
        unsafe {
            (host().map_foreach)(
                self.handle(),
                map_trampoline::<'a, F>,
                (&mut visitor as *mut MapVisitor<'a, _>).cast(),
            );
        }
    }

    //------------------------------------------------
    // Building.
    //

    /// Put an integer under a string key.
    pub fn put_str_int(&self, key: &str, value: i64) -> Result<()> {
        // SAFETY: putting into a map this call owns; the host copies the key.
        check(unsafe {
            (host().map_put_str_int)(
                self.ctx,
                self.handle() as *mut c_void,
                key.as_ptr().cast(),
                key.len() as u32,
                value,
            )
        })
    }

    /// Put a string under a string key.
    pub fn put_str_str(&self, key: &str, value: &str) -> Result<()> {
        // SAFETY: as above.
        check(unsafe {
            (host().map_put_str_str)(
                self.ctx,
                self.handle() as *mut c_void,
                key.as_ptr().cast(),
                key.len() as u32,
                value.as_ptr(),
                value.len() as u32,
            )
        })
    }

    /// Put a double under a string key.
    pub fn put_str_f64(&self, key: &str, value: f64) -> Result<()> {
        // SAFETY: as above.
        check(unsafe {
            (host().map_put_str_double)(
                self.ctx,
                self.handle() as *mut c_void,
                key.as_ptr().cast(),
                key.len() as u32,
                value,
            )
        })
    }

    /// Put an integer under an integer key.
    pub fn put_int_int(&self, key: i64, value: i64) -> Result<()> {
        // SAFETY: as above.
        check(unsafe {
            (host().map_put_int_int)(self.ctx, self.handle() as *mut c_void, key, value)
        })
    }

    /// Put any value under any key.
    pub fn put(&self, key: &Value<'_>, value: &Value<'_>) -> Result<()> {
        // SAFETY: as above.
        check(unsafe {
            (host().map_put)(
                self.ctx,
                self.handle() as *mut c_void,
                key.as_raw(),
                value.as_raw(),
            )
        })
    }

    /// Put a nested list under a string key.
    pub fn put_str_list(&self, key: &str, value: &List<'_>) -> Result<()> {
        let k = self.string_key(key);

        self.put(&k, &value.as_value())
    }

    /// Put a nested map under a string key.
    pub fn put_str_map(&self, key: &str, value: &Map<'_>) -> Result<()> {
        let k = self.string_key(key);

        self.put(&k, &value.as_value())
    }

    /// A borrowed string as a value, for the `put` path.
    ///
    /// The host copies a string when it stores one, so pointing at the caller's `&str` is valid for exactly
    /// as long as it needs to be — the duration of the `put` call.
    fn string_key<'k>(&self, key: &'k str) -> Value<'k> {
        Value::from_raw(
            abi::Value {
                kind: abi::TYPE_STR,
                len: key.len() as u32,
                u: abi::ValueUnion {
                    p: key.as_ptr(),
                },
            },
            self.ctx,
        )
    }
}

//==========================================================
// Closure trampolines.
//
// The host takes a C function pointer and a void*, so a Rust closure crosses as a pointer to a struct holding
// it. Nothing is allocated: the visitor lives on the caller's stack for the duration of the iteration.
//

struct Visitor<'a, F> {
    f: *mut F,
    ctx: *mut abi::Ctx,
    _borrow: PhantomData<&'a ()>,
}

unsafe extern "C" fn list_trampoline<'a, F>(
    index: u32,
    val: *const abi::Value,
    udata: *mut c_void,
) -> bool
where
    F: FnMut(u32, Value<'a>) -> bool,
{
    // SAFETY: udata is the Visitor the caller put on its stack, and val is the host's value for this element.
    let visitor = unsafe { &mut *udata.cast::<Visitor<'a, F>>() };
    let f = unsafe { &mut *visitor.f };

    f(index, Value::from_raw(unsafe { *val }, visitor.ctx))
}

struct MapVisitor<'a, F> {
    f: *mut F,
    ctx: *mut abi::Ctx,
    _borrow: PhantomData<&'a ()>,
}

unsafe extern "C" fn map_trampoline<'a, F>(
    key: *const abi::Value,
    val: *const abi::Value,
    udata: *mut c_void,
) -> bool
where
    F: FnMut(Value<'a>, Value<'a>) -> bool,
{
    // SAFETY: as above, for a key and a value.
    let visitor = unsafe { &mut *udata.cast::<MapVisitor<'a, F>>() };
    let f = unsafe { &mut *visitor.f };

    f(
        Value::from_raw(unsafe { *key }, visitor.ctx),
        Value::from_raw(unsafe { *val }, visitor.ctx),
    )
}

//==========================================================
// Owned bytes that have to outlive the call's return.
//

/// Keep `bytes` alive until the host has read the result, and describe them as a value.
///
/// This exists for one reason. The host reads the result slot **after** the entry point returns, so a value
/// pointing at a local `String` would be a dangling pointer by the time it is copied — a use-after-free that
/// would look like working code. Stashing the bytes in a per-thread buffer keeps them valid across exactly
/// that gap: the host copies them out before this thread can run another UDF.
///
/// One buffer per thread, reused, so a UDF returning a computed string does not allocate per call after the
/// first.
pub(crate) fn stash_bytes(bytes: &[u8], kind: i32) -> abi::Value {
    RESULT_BUF.with(|cell| {
        // SAFETY: the buffer is per-thread and not aliased - nothing else touches it between this write and
        // the host's read, because both happen on this thread with no UDF call in between.
        let buf = unsafe { &mut *cell.get() };

        buf.clear();
        buf.extend_from_slice(bytes);

        abi::Value {
            kind,
            len: buf.len() as u32,
            u: abi::ValueUnion { p: buf.as_ptr() },
        }
    })
}

thread_local! {
    static RESULT_BUF: core::cell::UnsafeCell<Vec<u8>> =
        const { core::cell::UnsafeCell::new(Vec::new()) };
}

/// Reported when a value cannot be what was asked for. Kept here so `value.rs` need not depend on the
/// formatting in `lib.rs`.
#[allow(dead_code)]
pub(crate) fn mismatch(wanted: &str, got: Kind) -> Error {
    Error::msg(format!("wanted {wanted}, got {got:?}"))
}
