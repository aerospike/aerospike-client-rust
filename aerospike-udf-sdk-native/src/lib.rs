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

//! Write Aerospike user-defined functions in Rust, compiled to a native shared object.
//!
//! ```ignore
//! use aerospike_udf_sdk_native::{module, udfs, Record, Result};
//!
//! udfs! {
//!     /// Add `by` to bin "n" and return the new value.
//!     fn increment(rec: &Record, by: i64) -> Result<i64> {
//!         let next = rec.get_int("n")?.unwrap_or(0) + by;
//!
//!         rec.set_int("n", next)?;
//!         rec.write()?;
//!
//!         Ok(next)
//!     }
//! }
//!
//! module! {
//!     name: "mymodule",
//!     version: "1.0.0",
//!     functions: [increment],
//! }
//! ```
//!
//! Build as a `cdylib` and name it in the server's configuration:
//!
//! ```text
//! mod-native {
//!     module mymodule {
//!         path /opt/aerospike/native/libmymodule.so
//!     }
//! }
//! ```
//!
//! # What you are signing up for
//!
//! A native UDF is **not sandboxed**. It runs in the server's address space with the server's privileges,
//! on transaction threads, with a record lock held. That is what makes it as fast as code written inside the
//! server, and it is also why only an operator can install one — there is no client-side registration and
//! there will not be.
//!
//! Three consequences, all of which this SDK can help with but none of which it can remove:
//!
//! - **Nothing will interrupt you.** Native code cannot be trapped mid-execution the way a WebAssembly guest
//!   can, so the transaction deadline is *cooperative*: [`Ctx::deadline_exceeded`] is the only form it takes.
//!   A loop that could run long must ask.
//! - **You are called concurrently.** Many transaction threads, the same function, at once. Anything mutable
//!   you keep outside what you are handed is yours to synchronize.
//! - **Do not block.** No network, no files, no unbounded compute. A record lock is held for the duration.
//!
//! # What it costs
//!
//! Nothing is serialized. A string bin is a pointer and a length into the record's own memory; a collection
//! is walked in place. So the thing that shows up in a profile is **the number of host calls**, not the
//! number of bytes — which is why the bulk and by-key accessors exist and why they are worth reaching for:
//!
//! | prefer | over | why |
//! |---|---|---|
//! | [`Map::get_str`], [`Map::get_int`] | scanning entries with `for_each` | one hash lookup, no allocation |
//! | [`List::ints_into`], [`List::sum_ints`] | [`List::get`] in a loop | one call instead of n |
//! | [`Record::get_int`] | [`Record::get`] then match | one call, one register, no `Value` |
//!
//! This is not a micro-optimization. In the WebAssembly module — same lesson, different engine — an
//! aggregation that scanned map keys by index ran 1.48× *slower* than the equivalent Lua, and the same
//! aggregation rewritten against the by-key and bulk calls came out level with it.
//!
//! # Safety, and where its edges are
//!
//! [`Value`], [`List`], [`Map`] and [`Args`] borrow from the record or from the call's arena, and the borrow
//! checker holds them to it: their lifetime comes from the `&Record` or `&Ctx` they were obtained through, so
//! none of them can outlive the call.
//!
//! [`Record`] and [`Ctx`] themselves are raw handles, constructible only by the generated entry points, and
//! are neither `Send` nor `Sync` — which closes the one misuse that would actually corrupt a server, namely
//! handing a record to another thread.
//!
//! Every generated entry point catches unwinding, because a panic crossing into C is undefined behaviour. A
//! panic becomes a UDF error, not a dead server. The landing pad costs nothing on the happy path.

#![warn(missing_docs)]

// Individual items are not documented here on purpose: this is a one-to-one transcription of as_native.h,
// and a doc comment per status code would be noise that obscures the one thing worth saying about the module,
// which is said at the top of it.
#[allow(missing_docs)]
pub mod abi;

#[cfg(feature = "client")]
pub mod client;

mod value;

pub use value::{Args, Kind, List, Map, Value};

use core::ffi::{c_char, c_void};
use core::sync::atomic::{AtomicPtr, Ordering};

/// What went wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// A bin, key or index that is not there.
    NotFound,
    /// It is there, but not of the type asked for.
    TypeMismatch,
    /// The host will not do this here — a write in an aggregation, where records are read-only.
    Refused,
    /// Anything else, with a message the client will see.
    Message(String),
}

impl Error {
    /// An error carrying a message back to the client.
    pub fn msg(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    /// The ABI status this maps to.
    fn code(&self) -> i32 {
        match self {
            Self::NotFound => abi::NOT_FOUND,
            Self::TypeMismatch => abi::TYPE_MISMATCH,
            Self::Refused => abi::REFUSED,
            Self::Message(_) => abi::ERR,
        }
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotFound => f.write_str("not found"),
            Self::TypeMismatch => f.write_str("wrong type"),
            Self::Refused => f.write_str("refused"),
            Self::Message(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {}

/// The result of a UDF.
pub type Result<T> = core::result::Result<T, Error>;

/// Turn a host status into a `Result`.
fn check(rc: i32) -> Result<()> {
    match rc {
        abi::OK => Ok(()),
        abi::NOT_FOUND => Err(Error::NotFound),
        abi::TYPE_MISMATCH => Err(Error::TypeMismatch),
        abi::REFUSED => Err(Error::Refused),
        _ => Err(Error::Message(format!("host call failed ({rc})"))),
    }
}

/// `Ok(None)` for a missing thing, an error for anything else.
fn optional(rc: i32) -> Result<bool> {
    match rc {
        abi::OK => Ok(true),
        abi::NOT_FOUND => Ok(false),
        other => check(other).map(|()| true),
    }
}

//==========================================================
// The host.
//

static HOST: AtomicPtr<abi::Host> = AtomicPtr::new(core::ptr::null_mut());

/// Record the host vtable. Called once, by the generated `as_native_module_init`.
///
/// # Safety
///
/// `host` must be the pointer the server passed, valid for the life of the process.
#[doc(hidden)]
pub unsafe fn set_host(host: *const abi::Host) -> bool {
    if host.is_null() || (*host).abi_version != abi::ABI_VERSION {
        return false;
    }

    HOST.store(host as *mut abi::Host, Ordering::Release);

    true
}

/// The host vtable.
///
/// A relaxed load of a hot global — a plain load on every architecture the server runs on, so reaching the
/// host costs nothing worth measuring.
#[inline(always)]
fn host() -> &'static abi::Host {
    // SAFETY: set_host runs during startup, before any UDF can be dispatched, and never changes after. A
    // null here would mean the server called a function before init, which it does not do.
    unsafe { &*HOST.load(Ordering::Relaxed) }
}

//==========================================================
// Context.
//

/// One invocation: where values are built, failures are reported, and the deadline is asked about.
///
/// Neither `Send` nor `Sync`, and only constructible by a generated entry point.
pub struct Ctx {
    ptr: *mut abi::Ctx,
}

impl Ctx {
    #[doc(hidden)]
    pub fn from_raw(ptr: *mut abi::Ctx) -> Self {
        Self { ptr }
    }

    /// Start a list. Its contents belong to this call.
    pub fn list(&self, capacity: u32) -> Result<List<'_>> {
        let mut raw = abi::Value::NIL;

        // SAFETY: the host writes a LIST value or reports failure.
        check(unsafe { (host().list_new)(self.ptr, capacity, &mut raw) })?;

        Ok(List::from_raw(raw, self.ptr))
    }

    /// Start a map.
    ///
    /// Note it will reach a client **ordered by key**, not in the order entries were put in: the server
    /// builds these as an ordered map. Deliberate — a deterministic order is worth more than insertion order.
    pub fn map(&self, capacity: u32) -> Result<Map<'_>> {
        let mut raw = abi::Value::NIL;

        // SAFETY: as above.
        check(unsafe { (host().map_new)(self.ptr, capacity, &mut raw) })?;

        Ok(Map::from_raw(raw, self.ptr))
    }

    /// The message the client sees when this call fails.
    ///
    /// Returning `Err` from a UDF does this for you; this is for adding detail before returning something
    /// else.
    pub fn fail(&self, message: &str) {
        // SAFETY: a borrowed string and its length; the host copies what it needs.
        unsafe {
            (host().fail)(self.ptr, message.as_ptr().cast(), message.len() as u32);
        }
    }

    /// Write to the server's log.
    pub fn log(&self, level: LogLevel, message: &str) {
        // SAFETY: as above.
        unsafe {
            (host().log)(
                self.ptr,
                level as i32,
                message.as_ptr().cast(),
                message.len() as u32,
            );
        }
    }

    /// Whether this transaction's deadline has passed.
    ///
    /// **Cooperative, and the only deadline a native UDF has.** Nothing will stop a function that does not
    /// ask. Any loop that could run long should check this and return.
    ///
    /// Always `false` in an aggregation: the server passes no timer down that path, so no stream UDF in any
    /// language can be interrupted, and reporting `false` is the truth rather than a guess.
    pub fn deadline_exceeded(&self) -> bool {
        // SAFETY: reads the context's deadline.
        unsafe { (host().deadline_exceeded)(self.ptr) }
    }
}

/// How loud a log line is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// Something went wrong.
    Warn = 0,
    /// Worth recording.
    Info = 1,
    /// Only while working on it.
    Debug = 2,
}

//==========================================================
// The record.
//

/// The record this call is about.
///
/// Reads borrow from the record itself — nothing is copied. Writes are staged and reach storage only when
/// [`write`](Record::write) is called, which mirrors Lua's `aerospike:update()` and is deliberately explicit:
/// forgetting it is the mistake that looks like it worked, since the function returns a plausible value and
/// the record does not change.
pub struct Record {
    rec: *mut abi::Rec,
    ctx: Ctx,
}

impl Record {
    #[doc(hidden)]
    pub fn from_raw(rec: *mut abi::Rec, ctx: *mut abi::Ctx) -> Self {
        Self {
            rec,
            ctx: Ctx::from_raw(ctx),
        }
    }

    /// This call's context, for building values and asking about the deadline.
    pub fn ctx(&self) -> &Ctx {
        &self.ctx
    }

    //------------------------------------------------
    // Reading.
    //

    /// An integer bin. `Ok(None)` if the bin is absent, an error if it holds something else.
    ///
    /// One host call returning a register — prefer this to [`get`](Record::get) when you know the type.
    pub fn get_int(&self, bin: &str) -> Result<Option<i64>> {
        let mut out = 0i64;

        // SAFETY: bin name and length; the host bounds-checks the name and writes only on success.
        let rc = unsafe {
            (host().rec_get_int)(self.rec, bin.as_ptr().cast(), bin.len() as u32, &mut out)
        };

        Ok(optional(rc)?.then_some(out))
    }

    /// A floating-point bin. An integer bin widens, because a client that wrote `1` and one that wrote `1.0`
    /// did not mean different things.
    pub fn get_f64(&self, bin: &str) -> Result<Option<f64>> {
        let mut out = 0f64;

        // SAFETY: as above.
        let rc = unsafe {
            (host().rec_get_double)(self.rec, bin.as_ptr().cast(), bin.len() as u32, &mut out)
        };

        Ok(optional(rc)?.then_some(out))
    }

    /// A boolean bin.
    pub fn get_bool(&self, bin: &str) -> Result<Option<bool>> {
        let mut out = false;

        // SAFETY: as above.
        let rc = unsafe {
            (host().rec_get_bool)(self.rec, bin.as_ptr().cast(), bin.len() as u32, &mut out)
        };

        Ok(optional(rc)?.then_some(out))
    }

    /// The bytes of a string, blob or geojson bin, borrowed from the record.
    pub fn get_bytes(&self, bin: &str) -> Result<Option<&[u8]>> {
        let mut p: *const u8 = core::ptr::null();
        let mut len = 0u32;

        // SAFETY: on success the host writes a pointer into a value the per-call arena owns, together with its
        // length.
        //
        // **That the arena owns it is the safety property**, not an implementation detail. The value outlives every
        // write staged during the call, so the returned slice stays valid across `set_*` and `write`. A version of
        // this that borrowed the *stored* particle instead was reverted for exactly that reason: every accessor here
        // takes `&self`, so nothing stops a caller reading a bin, writing it, and then using the earlier slice - and
        // with a borrowed particle that is a use-after-free the compiler cannot see.
        let rc = unsafe {
            (host().rec_get_bytes)(
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                &mut p,
                &mut len,
            )
        };

        if !optional(rc)? {
            return Ok(None);
        }

        // SAFETY: as above - p/len describe live record memory, and the returned slice borrows self.
        Ok(Some(unsafe { core::slice::from_raw_parts(p, len as usize) }))
    }

    /// Whether the host is new enough to offer in-place blob mutation.
    ///
    /// `rec_get_bytes_mut` was added after ABI 1.0, so a plugin built against this SDK can run on a host without it.
    /// Ask before relying on it and keep the copying path as a fallback; [`with_bytes_mut`](Record::with_bytes_mut)
    /// returns an error rather than guessing.
    pub fn supports_bytes_mut(&self) -> bool {
        host().size as usize >= abi::BYTES_MUT_SIZE
    }

    /// Modify a plain blob bin **in place**, without copying it out and back.
    ///
    /// ```ignore
    /// let n = rec.with_bytes_mut("ring", |bytes| { bytes[0] = 7; bytes.len() })?;
    /// rec.write()?;
    /// ```
    ///
    /// The bin is marked as changed when the pointer is handed out, so a later [`write`](Record::write) commits it.
    /// Nothing is committed if `write` is never called.
    ///
    /// # What this saves
    ///
    /// A read-modify-write through [`get_bytes`](Record::get_bytes) and [`set_blob`](Record::set_blob) copies the
    /// value **four** times: the host materialises the particle into a private value, the caller copies that into its
    /// own buffer, `set_blob` copies it back into a new value, and the write copies that into the new particle. This
    /// removes the middle two. The remaining two are the floor, not an oversight: the private copy is what makes the
    /// mutation safe, and building the new particle is what makes it durable.
    ///
    /// # Why `&mut self`, and why a closure
    ///
    /// Both are load-bearing rather than stylistic. `&mut self` is what stops a slice obtained earlier from
    /// `get_bytes` — which borrows `&self` — from still being alive and aliasing this one; the compiler enforces it,
    /// so it cannot be got wrong. The closure stops the mutable slice escaping to somewhere the record has moved on.
    ///
    /// **This is the distinction that the reverted borrowed *read* did not have.** That one handed out a slice into
    /// server-owned memory from an `&self` method, so nothing prevented a caller reading a bin, writing it, and then
    /// using the first slice — a use-after-free the compiler could not see. Here the memory is the call's own private
    /// copy and the borrow is exclusive.
    ///
    /// Returns `None` if the bin is absent. Errors if the bin is not a plain blob (a string, a geojson or an HLL is
    /// refused rather than reinterpreted), if the record is read-only as it is inside an aggregation, or if the host
    /// predates the call.
    pub fn with_bytes_mut<R>(
        &mut self,
        bin: &str,
        f: impl FnOnce(&mut [u8]) -> R,
    ) -> Result<Option<R>> {
        if !self.supports_bytes_mut() {
            return Err(Error::msg(
                "this host predates rec_get_bytes_mut - read with get_bytes and write with set_blob instead",
            ));
        }

        let mut p: *mut u8 = core::ptr::null_mut();
        let mut len = 0u32;

        // SAFETY: on success the host writes a pointer into the value *this call's* record cache owns, having first
        // marked the bin so the write path picks the bytes up. That value lives until the call returns and cannot be
        // replaced while `&mut self` is held, which is what makes the slice below sound.
        let rc = unsafe {
            (host().rec_get_bytes_mut)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                &mut p,
                &mut len,
            )
        };

        if !optional(rc)? {
            return Ok(None);
        }

        // A zero-length blob gives a null pointer, and `from_raw_parts_mut` may not be called with one.
        if p.is_null() || len == 0 {
            return Ok(Some(f(&mut [])));
        }

        // SAFETY: as above - p/len describe a live, uniquely borrowed buffer for the duration of the closure.
        let bytes = unsafe { core::slice::from_raw_parts_mut(p, len as usize) };

        Ok(Some(f(bytes)))
    }

    /// Whether the host offers the immediate-op path.
    pub fn supports_immediate_ops(&self) -> bool {
        host().size as usize >= abi::IMMEDIATE_OPS_SIZE
    }

    /// Read a blob bin with **no copy at all**.
    ///
    /// [`get_bytes`](Record::get_bytes) makes the host materialise the particle into an `as_val` first — a `malloc`
    /// and a `memcpy` of the whole value — and hands back a pointer into that copy. This hands back a pointer into
    /// the stored particle itself.
    ///
    /// Returns `Ok(None)` when the fast path does not apply and the caller should use `get_bytes` instead: the bin is
    /// absent, is not a plain blob, has a value staged by an earlier `set_*`, or the set is masked. **That is a
    /// fallback rather than an error** — all four are ordinary situations.
    ///
    /// # Why `&mut self` and a closure
    ///
    /// The pointer is into the record's own particle and dies the moment anything writes that bin. `&mut self` is what
    /// stops a staged write or an [`op`](Record::op) happening while the slice is alive, and the closure stops the
    /// slice escaping — the same reasoning as [`with_bytes_mut`](Record::with_bytes_mut), and the reason an earlier
    /// borrowed read that took `&self` had to be reverted.
    pub fn with_bytes_ref<R>(
        &mut self,
        bin: &str,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>> {
        if !self.supports_immediate_ops() {
            return Ok(None);
        }

        let mut p: *const u8 = core::ptr::null();
        let mut len = 0u32;

        // SAFETY: on success the host writes a pointer into the stored particle and its length. Nothing can replace
        // that particle while `&mut self` is held, which is what makes the slice below sound.
        let rc = unsafe {
            (host().rec_get_bytes_ref)(
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                &mut p,
                &mut len,
            )
        };

        if rc != abi::OK {
            return Ok(None);
        }

        if p.is_null() || len == 0 {
            return Ok(Some(f(&[])));
        }

        // SAFETY: as above.
        let bytes = unsafe { core::slice::from_raw_parts(p, len as usize) };

        Ok(Some(f(bytes)))
    }

    /// Apply one typed op to a bin, **immediately**.
    ///
    /// Nothing is staged and nothing is materialised: the value is a raw particle payload and the op goes to the same
    /// server function a client's op reaches. There is nothing to commit afterwards — the transaction commits once
    /// when the UDF returns, so [`write`](Record::write) is unnecessary here (and harmless).
    ///
    /// A failed op fails the whole transaction, exactly as a client's failing op does; the record's bins are restored
    /// from the snapshot the server took before the first write.
    pub fn op(
        &mut self,
        bin: &str,
        op: abi::Op,
        particle_type: abi::ParticleType,
        value: &[u8],
    ) -> Result<()> {
        if !self.supports_immediate_ops() {
            return Err(Error::msg(
                "this host predates the immediate-op path - stage with set_* and call write instead",
            ));
        }

        // SAFETY: the host copies what it needs out of `value` before returning.
        check(unsafe {
            (host().rec_op)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                op as u8,
                particle_type as u8,
                value.as_ptr(),
                value.len() as u32,
            )
        })
    }

    /// Add to an integer bin, immediately. Creates it if absent.
    ///
    /// The payload is the server's integer particle format — eight bytes, big-endian — so there is no encoding here
    /// beyond an endianness swap, and no msgpack.
    pub fn op_incr(&mut self, bin: &str, delta: i64) -> Result<()> {
        self.op(bin, abi::Op::Incr, abi::ParticleType::Integer, &delta.to_be_bytes())
    }

    /// Append bytes to a blob bin, immediately. Creates it if absent.
    pub fn op_append_blob(&mut self, bin: &str, value: &[u8]) -> Result<()> {
        self.op(bin, abi::Op::Append, abi::ParticleType::Blob, value)
    }

    /// Prepend bytes to a blob bin, immediately.
    pub fn op_prepend_blob(&mut self, bin: &str, value: &[u8]) -> Result<()> {
        self.op(bin, abi::Op::Prepend, abi::ParticleType::Blob, value)
    }

    /// Append to a string bin, immediately.
    pub fn op_append_str(&mut self, bin: &str, value: &str) -> Result<()> {
        self.op(bin, abi::Op::Append, abi::ParticleType::String, value.as_bytes())
    }

    /// Whether the host offers the packed-list cursor.
    pub fn supports_packed_lists(&self) -> bool {
        host().size as usize >= abi::PACKED_LIST_SIZE
    }

    /// How many elements a list bin holds, **without deserialising it**.
    ///
    /// Returns `None` when the fast path does not apply and [`get`](Record::get) is the answer instead: the bin is
    /// absent, is not a list, has a value staged by an earlier `set_*`, or the set is masked.
    pub fn list_len(&self, bin: &str) -> Result<Option<u32>> {
        if !self.supports_packed_lists() {
            return Ok(None);
        }

        let mut out = 0u32;

        // SAFETY: the host reads the bin's packed bytes and writes the element count.
        let rc = unsafe {
            (host().rec_list_count)(self.rec, bin.as_ptr().cast(), bin.len() as u32, &mut out)
        };

        Ok((rc == abi::OK).then_some(out))
    }

    /// Read integers out of a list bin's **packed** form, allocating nothing.
    ///
    /// [`get`](Record::get) on a list bin makes the host deserialise the whole list into an `as_arraylist` first — one
    /// heap allocation per element — before a single number is read. This walks the packed msgpack instead: measured
    /// at **5.2× on a ten-thousand element list**, where the per-element materialisation costs ~36 ns.
    ///
    /// Returns how many elements were read, which stops early at the first element that is not an integer. `None`
    /// means the fast path did not apply — see [`list_len`](Record::list_len).
    pub fn list_ints(&self, bin: &str, from: u32, out: &mut [i64]) -> Result<Option<usize>> {
        if !self.supports_packed_lists() {
            return Ok(None);
        }

        // SAFETY: out/len describe the caller's buffer; the host writes at most `len` elements.
        let rc = unsafe {
            (host().rec_list_ints)(
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                from,
                out.len() as u32,
                out.as_mut_ptr(),
            )
        };

        if rc < 0 {
            return Ok(None);
        }

        Ok(Some(rc as usize))
    }

    /// The same, for doubles.
    pub fn list_doubles(&self, bin: &str, from: u32, out: &mut [f64]) -> Result<Option<usize>> {
        if !self.supports_packed_lists() {
            return Ok(None);
        }

        // SAFETY: as above.
        let rc = unsafe {
            (host().rec_list_doubles)(
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                from,
                out.len() as u32,
                out.as_mut_ptr(),
            )
        };

        if rc < 0 {
            return Ok(None);
        }

        Ok(Some(rc as usize))
    }

    /// Whether the host offers the packed-map cursor.
    pub fn supports_packed_maps(&self) -> bool {
        host().size as usize >= abi::PACKED_MAP_SIZE
    }

    /// How many entries a map bin holds, **without deserialising it**.
    ///
    /// An ordered map's packed form begins with an order element carrying its flags and index; that is not one of your
    /// entries and is not counted here.
    pub fn map_len(&self, bin: &str) -> Result<Option<u32>> {
        if !self.supports_packed_maps() {
            return Ok(None);
        }

        let mut out = 0u32;

        // SAFETY: the host reads the bin's packed bytes and writes the entry count.
        let rc = unsafe {
            (host().rec_map_count)(self.rec, bin.as_ptr().cast(), bin.len() as u32, &mut out)
        };

        Ok((rc == abi::OK).then_some(out))
    }

    /// Look up an integer key in a map bin's **packed** form, allocating nothing.
    ///
    /// [`get`](Record::get) on a map bin makes the host deserialise every entry into an `as_orderedmap` first — one
    /// heap allocation per key and per value — before the lookup begins. This walks the packed entries instead,
    /// decoding keys in place to compare them.
    ///
    /// `Ok(None)` means the fast path did not apply *or* the key is absent — the two are told apart by the returned
    /// value's kind being [`Kind::Absent`]. A value that is itself a list or a map also yields `None`, because a cursor
    /// has no handle to give you for one: fall back to `get` for those.
    pub fn map_get_int(&self, bin: &str, key: i64) -> Result<Option<Value<'_>>> {
        if !self.supports_packed_maps() {
            return Ok(None);
        }

        let mut raw = abi::Value::NIL;

        // SAFETY: any pointer the host writes into `raw` is into the stored particle, which outlives `&self`.
        let rc = unsafe {
            (host().rec_map_get_int)(
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                key,
                &mut raw,
            )
        };

        if rc != abi::OK {
            return Ok(None);
        }

        Ok(Some(Value::from_raw(raw, core::ptr::null_mut())))
    }

    /// The same, for a string key.
    pub fn map_get_str(&self, bin: &str, key: &str) -> Result<Option<Value<'_>>> {
        if !self.supports_packed_maps() {
            return Ok(None);
        }

        let mut raw = abi::Value::NIL;

        // SAFETY: as above.
        let rc = unsafe {
            (host().rec_map_get_str)(
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                key.as_ptr().cast(),
                key.len() as u32,
                &mut raw,
            )
        };

        if rc != abi::OK {
            return Ok(None);
        }

        Ok(Some(Value::from_raw(raw, core::ptr::null_mut())))
    }

    /// Whether the host offers the packed writers.
    pub fn supports_packed_writes(&self) -> bool {
        host().size as usize >= abi::PACKED_WRITE_SIZE
    }

    /// Write a slice of integers to a bin as a list, **building no intermediate values**.
    ///
    /// The counterpart to [`list_ints`](Record::list_ints). Building a list through [`Ctx::list`] and `push_int` costs
    /// one heap allocation per element, and the commit then serialises the whole thing to msgpack anyway — measured at
    /// ~54 ns an element. This packs straight to msgpack once, on the stack for a small list, and hands the bytes to
    /// the same op a client's `put` of a list reaches. **At most one allocation, regardless of length.**
    ///
    /// Applied immediately, like [`op`](Record::op): there is nothing to commit afterwards.
    pub fn set_list_ints(&mut self, bin: &str, values: &[i64]) -> Result<()> {
        if !self.supports_packed_writes() {
            return Err(Error::msg(
                "this host predates the packed writers - build a list with ctx().list() instead",
            ));
        }

        // SAFETY: the host reads `values` and packs it before returning; nothing is retained.
        check(unsafe {
            (host().rec_set_list_ints)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                values.as_ptr(),
                values.len() as u32,
            )
        })
    }

    /// The same, for doubles.
    pub fn set_list_doubles(&mut self, bin: &str, values: &[f64]) -> Result<()> {
        if !self.supports_packed_writes() {
            return Err(Error::msg(
                "this host predates the packed writers - build a list with ctx().list() instead",
            ));
        }

        // SAFETY: as above.
        check(unsafe {
            (host().rec_set_list_doubles)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                values.as_ptr(),
                values.len() as u32,
            )
        })
    }

    /// Whether the host offers the nested cursor.
    pub fn supports_cursor(&self) -> bool {
        host().size as usize >= abi::CURSOR_SIZE
    }

    /// Open a cursor on a list or map bin, **deserialising nothing**.
    ///
    /// This is the general form of [`list_ints`](Record::list_ints) and [`map_get_str`](Record::map_get_str): it walks
    /// an arbitrary tree — heterogeneous elements, nested containers, any depth — straight out of the packed bytes.
    ///
    /// The reason it exists is that [`get`](Record::get) on a collection bin makes the host deserialise the **whole**
    /// document into one `as_val` per node before a plugin reads any of it. Measured on a 9,035-node document, that is
    /// 632 µs, and it is paid even by a query that matches nothing.
    ///
    /// `Ok(None)` means the fast path does not apply: the bin is absent, is neither a list nor a map, has a value
    /// staged by an earlier `set_*`, or the set is masked.
    pub fn cursor(&self, bin: &str) -> Result<Option<PackedCursor<'_>>> {
        if !self.supports_cursor() {
            return Ok(None);
        }

        let mut raw = abi::Cursor::EMPTY;
        let mut info = abi::Value::NIL;

        // SAFETY: on success the host writes a cursor borrowing the stored particle, which outlives `&self`.
        let rc = unsafe {
            (host().rec_cursor_open)(
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                &mut raw,
                &mut info,
            )
        };

        if rc != abi::OK {
            return Ok(None);
        }

        let kind = if info.kind == abi::TYPE_MAP { Kind::Map } else { Kind::List };

        Ok(Some(PackedCursor::new(raw, kind, info.len)))
    }

    /// A string bin, if it is valid UTF-8.
    pub fn get_str(&self, bin: &str) -> Result<Option<&str>> {
        match self.get_bytes(bin)? {
            None => Ok(None),
            Some(bytes) => core::str::from_utf8(bytes)
                .map(Some)
                .map_err(|_| Error::msg(format!("bin '{bin}' is not valid UTF-8"))),
        }
    }

    /// Any bin, whatever it holds.
    pub fn get(&self, bin: &str) -> Result<Value<'_>> {
        let mut raw = abi::Value::NIL;

        // SAFETY: the host always writes a value - ABSENT for a missing bin.
        let rc = unsafe {
            (host().rec_get)(self.rec, bin.as_ptr().cast(), bin.len() as u32, &mut raw)
        };

        if rc != abi::OK && rc != abi::NOT_FOUND {
            check(rc)?;
        }

        Ok(Value::from_raw(raw, self.ctx.ptr))
    }

    /// How many bins the record has.
    pub fn num_bins(&self) -> u32 {
        // SAFETY: a read of the record's bin count.
        unsafe { (host().rec_numbins)(self.rec) }
    }

    /// Every bin name, in one host call rather than one per bin.
    pub fn bin_names(&self) -> Vec<String> {
        unsafe extern "C" fn cb(name: *const c_char, name_len: u32, udata: *mut c_void) -> bool {
            // SAFETY: udata is the Vec below; name/name_len describe a live buffer the host owns.
            let names = unsafe { &mut *udata.cast::<Vec<String>>() };
            let bytes = unsafe { core::slice::from_raw_parts(name.cast::<u8>(), name_len as usize) };

            names.push(String::from_utf8_lossy(bytes).into_owned());

            true
        }

        let mut names = Vec::new();

        // SAFETY: the callback matches the ABI and udata outlives the call.
        unsafe {
            (host().rec_bin_names)(self.rec, cb, (&mut names as *mut Vec<String>).cast());
        }

        names
    }

    //------------------------------------------------
    // Metadata.
    //

    /// Seconds until the record expires, or 0 for never.
    pub fn ttl(&self) -> u32 {
        // SAFETY: a metadata read.
        unsafe { (host().rec_ttl)(self.rec) }
    }

    /// The generation. Zero means the record does not exist yet.
    pub fn generation(&self) -> u16 {
        // SAFETY: a metadata read.
        unsafe { (host().rec_gen)(self.rec) }
    }

    /// When the record was last written, in nanoseconds since the Aerospike epoch.
    pub fn last_update_time(&self) -> u64 {
        // SAFETY: a metadata read.
        unsafe { (host().rec_last_update_time)(self.rec) }
    }

    /// Bytes on device.
    pub fn device_size(&self) -> u32 {
        // SAFETY: a metadata read.
        unsafe { (host().rec_device_size)(self.rec) }
    }

    /// Bytes in memory.
    pub fn memory_size(&self) -> u32 {
        // SAFETY: a metadata read.
        unsafe { (host().rec_memory_size)(self.rec) }
    }

    /// The set name, or `None` for a record with no set.
    pub fn set_name(&self) -> Option<&str> {
        // SAFETY: the host returns a NUL-terminated string it owns, or null.
        let p = unsafe { (host().rec_setname)(self.rec) };

        if p.is_null() {
            return None;
        }

        // SAFETY: NUL-terminated and owned by the record, so valid for this borrow.
        unsafe { core::ffi::CStr::from_ptr(p) }.to_str().ok()
    }

    /// The record's 20-byte digest.
    pub fn digest(&self) -> Option<&[u8; 20]> {
        // SAFETY: the host returns a pointer to 20 bytes it owns, or null.
        let p = unsafe { (host().rec_digest)(self.rec) };

        if p.is_null() {
            return None;
        }

        // SAFETY: exactly 20 bytes, living as long as the record.
        Some(unsafe { &*p.cast::<[u8; 20]>() })
    }

    /// The primary key, if the client sent it.
    pub fn key(&self) -> Result<Value<'_>> {
        let mut raw = abi::Value::NIL;

        // SAFETY: the host always writes a value.
        let rc = unsafe { (host().rec_key)(self.rec, &mut raw) };

        if rc != abi::OK && rc != abi::NOT_FOUND {
            check(rc)?;
        }

        Ok(Value::from_raw(raw, self.ctx.ptr))
    }

    /// Whether the record exists yet.
    pub fn exists(&self) -> bool {
        // SAFETY: a metadata read.
        unsafe { (host().rec_exists)(self.rec) }
    }

    //------------------------------------------------
    // Writing. Staged until `write`.
    //

    /// Stage an integer bin.
    pub fn set_int(&self, bin: &str, value: i64) -> Result<()> {
        // SAFETY: bin name, length and a scalar; the host constructs and owns the stored value.
        check(unsafe {
            (host().rec_set_int)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                value,
            )
        })
    }

    /// Stage a floating-point bin.
    pub fn set_f64(&self, bin: &str, value: f64) -> Result<()> {
        // SAFETY: as above.
        check(unsafe {
            (host().rec_set_double)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                value,
            )
        })
    }

    /// Stage a boolean bin.
    pub fn set_bool(&self, bin: &str, value: bool) -> Result<()> {
        // SAFETY: as above.
        check(unsafe {
            (host().rec_set_bool)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                value,
            )
        })
    }

    /// Stage a string bin. The host copies it — the server must own what it stores.
    pub fn set_str(&self, bin: &str, value: &str) -> Result<()> {
        // SAFETY: two borrowed strings with lengths; the host copies.
        check(unsafe {
            (host().rec_set_str)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                value.as_ptr(),
                value.len() as u32,
            )
        })
    }

    /// Stage a blob bin. Copied, as for a string.
    pub fn set_blob(&self, bin: &str, value: &[u8]) -> Result<()> {
        // SAFETY: as above.
        check(unsafe {
            (host().rec_set_blob)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                value.as_ptr(),
                value.len() as u32,
            )
        })
    }

    /// Stage any value, including a list or map built through [`Ctx`].
    pub fn set(&self, bin: &str, value: &Value<'_>) -> Result<()> {
        // SAFETY: the host takes what it needs from the value and owns what it stores.
        check(unsafe {
            (host().rec_set)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
                value.as_raw(),
            )
        })
    }

    /// Stage the removal of a bin.
    pub fn remove(&self, bin: &str) -> Result<()> {
        // SAFETY: bin name and length.
        check(unsafe {
            (host().rec_remove_bin)(
                self.ctx.ptr,
                self.rec,
                bin.as_ptr().cast(),
                bin.len() as u32,
            )
        })
    }

    /// Set the record's time to live, in seconds.
    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        // SAFETY: a metadata write.
        check(unsafe { (host().rec_set_ttl)(self.rec, ttl) })
    }

    /// **Commit.** Creates the record if it does not exist, updates it if it does.
    ///
    /// Nothing staged above reaches storage without this. The existence check is one metadata read, which is
    /// a price worth paying to remove the choice — see [`update`](Record::update) and
    /// [`create`](Record::create) if you want to be strict about which one happens.
    pub fn write(&self) -> Result<()> {
        if self.exists() {
            self.update()
        } else {
            self.create()
        }
    }

    /// Commit, requiring the record to already exist.
    pub fn update(&self) -> Result<()> {
        // SAFETY: commits the staged bins.
        check(unsafe { (host().rec_update)(self.ctx.ptr, self.rec) })
    }

    /// Commit, requiring the record not to exist.
    pub fn create(&self) -> Result<()> {
        // SAFETY: as above.
        check(unsafe { (host().rec_create)(self.ctx.ptr, self.rec) })
    }

    /// Delete the record.
    pub fn delete(&self) -> Result<()> {
        // SAFETY: as above.
        check(unsafe { (host().rec_delete)(self.ctx.ptr, self.rec) })
    }
}

//==========================================================
// Aggregation.
//

/// Whether an accumulator has room for more.
///
/// The only difference between an aggregation that survives a billion records and one that does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fold {
    /// Keep going. What a fixed-size accumulator always answers.
    Continue,
    /// "I took that record, and I am now at capacity." The host writes this accumulator out as a partial,
    /// resets it — re-running [`configure`](Aggregation::configure) — and carries on.
    Full,
}

/// Where an aggregation's output goes.
///
/// Handed to [`accumulate`](Aggregation::accumulate) so a UDF can produce output *per record* instead of only
/// through its accumulator — which is what makes the cheapest shape expressible: a filter that returns
/// matching records and keeps no state at all.
///
/// **Only one of the two calls is free**, and it is worth knowing which.
pub struct Emit {
    ptr: *mut abi::Emit,
    ctx: *mut abi::Ctx,
}

impl Emit {
    #[doc(hidden)]
    pub fn from_raw(ptr: *mut abi::Emit, ctx: *mut abi::Ctx) -> Self {
        Self { ptr, ctx }
    }

    /// Pass the record through, exactly as a plain query would return it.
    ///
    /// **This is the free one.** The server already has the record laid out, so it goes into the response with
    /// no value built, nothing copied out of it, and no allocation anywhere. A filtering UDF costs the bin read
    /// and this call, and nothing else.
    ///
    /// The client reads these off the query stream as ordinary records — no [`client::Combiner`] involved,
    /// because there is nothing to combine.
    pub fn record(&self, rec: &Record) -> Result<()> {
        // SAFETY: both pointers belong to the call that produced this handle.
        check(unsafe { (host().emit_record)(self.ctx, self.ptr, rec.rec) })
    }

    /// Emit something derived.
    ///
    /// **Not free**: building `value` allocated in the server's allocator, as any constructed value does.
    /// Cheaper than accumulating it, and worth reaching for when the answer is per record — but it should not
    /// be mistaken for [`record`](Emit::record).
    pub fn value(&self, value: &Value<'_>) -> Result<()> {
        // SAFETY: as above; the host copies or reserves what it needs.
        check(unsafe { (host().emit_value)(self.ctx, self.ptr, value.as_raw()) })
    }
}

/// Turning an accumulator into a partial and back.
///
/// **Written for you by [`partial!`].** Rust cannot reflect over a struct's fields, so the accumulator is
/// declared through a macro that generates this pair — which is the whole reason an author of a native
/// aggregation writes no serialization code.
///
/// Implementing it by hand is allowed and occasionally right: an accumulator with a field the macro cannot
/// describe, or one with a partial format fixed by something outside this crate.
pub trait AsPartial: Sized {
    /// Serialize this accumulator. What this returns is what crosses the wire.
    fn to_partial<'c>(&self, ctx: &'c Ctx) -> Result<Value<'c>>;

    /// Read one back, into an accumulator [`configure`](Aggregation::configure) has already run on.
    fn from_partial(&mut self, partial: Value<'_>) -> Result<()>;
}

/// A map/reduce aggregation.
///
/// The shape here is what makes a native aggregation cheap, and it is worth knowing why:
///
/// - **One call per record.** [`accumulate`](Aggregation::accumulate) is the entire per-record cost. No value
///   is boxed, nothing is written to a stream, and there is no intermediate representation at all.
/// - **The host owns your state.** It allocates `size_of::<Self>()`, zeroed, so an accumulator of plain
///   fields makes an aggregation over millions of records perform **zero allocations**.
/// - **One accumulator per query thread**, kept across every partition slice that thread processes. Lua
///   finalizes at the end of each slice and so returns hundreds of partials from one node; this returns one
///   per thread.
///
/// # One `merge`, at every level
///
/// ```text
/// record  ->  accumulate(rec)              in memory, one indirect call
/// slice   ->  the same accumulator, kept   nothing at all
/// thread  ->  to_partial()  -- wire --     from_partial() on the client
/// client  ->  merge(&other)                in memory, this same code
/// answer  ->  finalize(ctx)                once, on everything
/// ```
///
/// There is no serialization on the server and none on the client. The only encoding is the wire hop, and it is
/// the convention that was already there: a partial is an ordinary value in the query result stream, msgpack
/// like a Lua partial. Nothing in the protocol changes.
///
/// # `finalize` may return something unmergeable, and that is the point
///
/// A partial is the **serialized accumulator**, not a summary of it — [`AsPartial`] round-trips it faithfully,
/// so two partials combine by definition, because that is what `merge` is. Mergeability therefore follows from
/// `merge` being correct rather than from anything you design.
///
/// Which leaves `finalize` free to return a mean, a ratio, or a formatted string. It runs once, on a fully
/// merged accumulator, so it is not the thing being combined. The old trap — emit a mean, then discover it
/// cannot be added to another node's mean — does not exist here.
///
/// # Three shapes, one trait
///
/// | shape | state | emits | partials | client |
/// |---|---|---|---|---|
/// | filter | zero-sized | per record, via [`Emit`] | none | reads the stream |
/// | chunked collect | grows | on [`Fold::Full`] | many | reads them in order |
/// | reduce | fixed | never | one per thread | [`client::Combiner`] merges them |
///
/// The differences are whether `accumulate` emits and whether it can answer [`Fold::Full`]. Which brings the
/// one rule the host cannot enforce: **emit output values or partials, not both.** They travel on the same
/// result stream, and a client receiving a mixture has no way to tell which is which. Each shape above picks
/// one, which is why none of them has to think about it.
pub trait Aggregation: Default + AsPartial + Sized {
    /// The name a client names in `set_aggregate_function`.
    ///
    /// A [`CStr`](core::ffi::CStr) rather than a `&str`, because the host reaches this through `strlen`: a
    /// Rust string is not NUL-terminated, so handing one over would read past its end. Write it as a C string
    /// literal - `const NAME: &CStr = c"sum";`
    const NAME: &'static core::ffi::CStr;

    /// Read the arguments. The default accepts and ignores them.
    ///
    /// `self` is already valid — [`Default`] ran first — so this configures rather than constructs, which is
    /// what lets the host hand out zeroed memory and lets a failure here be safe.
    ///
    /// **Runs again after every dump**, on a fresh accumulator, so anything configured here survives being
    /// full.
    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        let _ = args;

        Ok(())
    }

    /// Fold one record in. The whole per-record cost of the aggregation.
    ///
    /// Answer [`Fold::Full`] to be written out and reset. `out` is for producing output per record instead of
    /// accumulating it — see [`Emit`], and the rule below about not doing both.
    fn accumulate(&mut self, rec: &Record, out: &Emit) -> Result<Fold>;

    /// Fold another accumulator of the same aggregation into this one.
    ///
    /// The server does not need this — a thread keeps one accumulator, so there is nothing to fold — but the
    /// client combining partials does, and it is the same call. That is the point of it being here rather than
    /// in a client library.
    fn merge(&mut self, other: &Self) {
        let _ = other;
    }

    /// How much heap this accumulator owns, so an operator's `agg-memory-limit` can bound it as well as your
    /// own idea of full.
    ///
    /// Two independent brakes, deliberately: you know what your accumulator means, and an operator knows what
    /// the server can afford. The default of zero is the truth for an accumulator of plain fields.
    fn heap_used(&self) -> usize {
        0
    }

    /// The user's answer, from a fully merged accumulator.
    ///
    /// Runs on the **client**, once — never on the server, which only ever calls
    /// [`to_partial`](AsPartial::to_partial). Build it through `ctx` exactly as you would server-side: the SDK
    /// supplies a local host so this code cannot tell the difference.
    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>>;
}


//==========================================================
// What a partial! field has to know how to do.
//

/// One field of an accumulator, as [`partial!`] serializes it.
///
/// Three operations rather than a single "to a value", because a value carrying borrowed bytes cannot outlive
/// the thing it borrows from: a `String` field's value points into the `String`, so it has to be *written*
/// while that borrow is alive rather than returned. Hence a put and a push instead of a getter.
///
/// Implement this to use a type of your own as a `partial!` field. Note there is deliberately no `impl` for
/// `Vec<u8>`: it would collide with the blanket one for `Vec<T>`, and silently serializing a byte buffer as a
/// list of integers is worse than not compiling. A blob field is written [`Blob`] instead.
pub trait PartialField {
    /// Write this field into `map` under `key`.
    fn put_into(&self, ctx: &Ctx, map: &Map<'_>, key: &str) -> Result<()>;

    /// Append this field to `list`, for when it is an element rather than a field.
    fn push_into(&self, ctx: &Ctx, list: &List<'_>) -> Result<()>;

    /// Read it back. A value that is absent or of the wrong kind leaves the field alone — which is what makes a
    /// partial written by an older version of a plugin readable by a newer one.
    fn read(&mut self, value: Value<'_>) -> Result<()>;
}

macro_rules! partial_field_scalar {
    (
        $t:ty,
        put = |$s:ident, $map:ident, $key:ident| $put:expr,
        push = |$s2:ident, $list:ident| $push:expr,
        read = |$v:ident| $read:expr
    ) => {
        impl PartialField for $t {
            fn put_into(&self, _ctx: &Ctx, map: &Map<'_>, key: &str) -> Result<()> {
                let ($s, $map, $key) = (self, map, key);

                $put
            }

            fn push_into(&self, _ctx: &Ctx, list: &List<'_>) -> Result<()> {
                let ($s2, $list) = (self, list);

                $push
            }

            fn read(&mut self, value: Value<'_>) -> Result<()> {
                let $v = value;

                if let Some(v) = $read {
                    *self = v;
                }

                Ok(())
            }
        }
    };
}

partial_field_scalar! {
    i64,
    put = |s, map, key| map.put_str_int(key, *s),
    push = |s, list| list.push_int(*s),
    read = |v| v.as_i64()
}

partial_field_scalar! {
    f64,
    put = |s, map, key| map.put_str_f64(key, *s),
    push = |s, list| list.push_f64(*s),
    read = |v| v.as_f64()
}

partial_field_scalar! {
    bool,
    // As an integer, because that is how this ABI carries a bool - one less width for a binding in another
    // language to get wrong, and it costs nothing here.
    put = |s, map, key| map.put_str_int(key, i64::from(*s)),
    push = |s, list| list.push_int(i64::from(*s)),
    read = |v| v.as_bool().or_else(|| v.as_i64().map(|n| n != 0))
}

/// A byte buffer, for a [`partial!`] field that holds one.
///
/// **A newtype rather than an `impl` on `Vec<u8>`**, and that is forced rather than chosen: an
/// `impl PartialField for Vec<u8>` would collide with the blanket one for `Vec<T>`, and Rust cannot prove `u8`
/// will never implement `PartialField`. The alternative was serializing a byte buffer as a list of integers,
/// which is four times the size and would have been a silent trap.
///
/// So a blob field is written `id: Blob` instead of `id: Vec<u8>`. Deref makes it read like the `Vec` it is.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Blob(pub Vec<u8>);

impl core::ops::Deref for Blob {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl core::ops::DerefMut for Blob {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<u8>> for Blob {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for Blob {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl PartialField for Blob {
    fn put_into(&self, _ctx: &Ctx, map: &Map<'_>, key: &str) -> Result<()> {
        map.put(&Value::string(key), &Value::blob(&self.0))
    }

    fn push_into(&self, _ctx: &Ctx, list: &List<'_>) -> Result<()> {
        list.push(&Value::blob(&self.0))
    }

    fn read(&mut self, value: Value<'_>) -> Result<()> {
        if let Some(bytes) = value.as_bytes() {
            self.0.clear();
            self.0.extend_from_slice(bytes);
        }

        Ok(())
    }
}

impl PartialField for String {
    fn put_into(&self, _ctx: &Ctx, map: &Map<'_>, key: &str) -> Result<()> {
        map.put_str_str(key, self)
    }

    fn push_into(&self, _ctx: &Ctx, list: &List<'_>) -> Result<()> {
        list.push_str(self)
    }

    fn read(&mut self, value: Value<'_>) -> Result<()> {
        if let Some(s) = value.as_str() {
            self.clear();
            self.push_str(s);
        }

        Ok(())
    }
}

impl<T: PartialField + Default> PartialField for Vec<T> {
    fn put_into(&self, ctx: &Ctx, map: &Map<'_>, key: &str) -> Result<()> {
        let list = ctx.list(self.len() as u32)?;

        for item in self {
            item.push_into(ctx, &list)?;
        }

        map.put_str_list(key, &list)
    }

    fn push_into(&self, ctx: &Ctx, outer: &List<'_>) -> Result<()> {
        let list = ctx.list(self.len() as u32)?;

        for item in self {
            item.push_into(ctx, &list)?;
        }

        outer.push_list(&list)
    }

    fn read(&mut self, value: Value<'_>) -> Result<()> {
        self.clear();

        let Some(list) = value.as_list() else {
            return Ok(());
        };

        // Collected first because `for_each` cannot carry a `Result` out of the host's callback - the callback
        // answers only "keep going". One `Vec` of borrowed values, then the fallible part.
        let mut items = Vec::with_capacity(list.len() as usize);

        list.for_each(|_, item| {
            items.push(item);

            true
        });

        for item in items {
            let mut slot = T::default();

            slot.read(item)?;
            self.push(slot);
        }

        Ok(())
    }
}

//==========================================================
// Turning a Rust answer into a UDF result.
//

/// What a UDF may return.
///
/// Implemented for the scalars, for `()` (a nil answer), and for the borrowed types, so a function signature
/// can say what it means rather than assembling a `Value`.
pub trait IntoResult {
    /// Write this answer into the host's out-parameter.
    ///
    /// # Safety
    ///
    /// `out` must point at the result slot the host provided.
    unsafe fn into_result(self, out: *mut abi::Value);
}

macro_rules! into_result_scalar {
    ($($t:ty => |$v:ident| $body:expr),* $(,)?) => {
        $(impl IntoResult for $t {
            unsafe fn into_result(self, out: *mut abi::Value) {
                let $v = self;

                unsafe { *out = $body };
            }
        })*
    };
}

into_result_scalar! {
    i64 => |v| abi::Value { kind: abi::TYPE_INT, len: 0, u: abi::ValueUnion { i: v } },
    i32 => |v| abi::Value { kind: abi::TYPE_INT, len: 0, u: abi::ValueUnion { i: v as i64 } },
    u32 => |v| abi::Value { kind: abi::TYPE_INT, len: 0, u: abi::ValueUnion { i: v as i64 } },
    f64 => |v| abi::Value { kind: abi::TYPE_DOUBLE, len: 0, u: abi::ValueUnion { d: v } },
    bool => |v| abi::Value {
        kind: abi::TYPE_BOOL, len: 0, u: abi::ValueUnion { i: if v { 1 } else { 0 } }
    },
    () => |v| { let () = v; abi::Value::NIL },
}

impl IntoResult for &str {
    unsafe fn into_result(self, out: *mut abi::Value) {
        // The host copies these bytes when it builds the result, so pointing at the caller's memory is safe
        // for exactly as long as it needs to be.
        unsafe {
            *out = abi::Value {
                kind: abi::TYPE_STR,
                len: self.len() as u32,
                u: abi::ValueUnion {
                    p: self.as_ptr(),
                },
            };
        }
    }
}

impl IntoResult for &[u8] {
    unsafe fn into_result(self, out: *mut abi::Value) {
        unsafe {
            *out = abi::Value {
                kind: abi::TYPE_BLOB,
                len: self.len() as u32,
                u: abi::ValueUnion {
                    p: self.as_ptr(),
                },
            };
        }
    }
}

impl IntoResult for String {
    unsafe fn into_result(self, out: *mut abi::Value) {
        // **Not** `self.as_str().into_result(out)`. The host reads the result slot *after* this entry point
        // returns, so a pointer into this String's buffer would already be dangling when it is copied - a
        // use-after-free that would look like working code, because the bytes are usually still there.
        unsafe { *out = value::stash_bytes(self.as_bytes(), abi::TYPE_STR) };
    }
}

impl IntoResult for Vec<u8> {
    unsafe fn into_result(self, out: *mut abi::Value) {
        // As for String, and for the same reason.
        unsafe { *out = value::stash_bytes(&self, abi::TYPE_BLOB) };
    }
}

impl<T: IntoResult> IntoResult for Option<T> {
    unsafe fn into_result(self, out: *mut abi::Value) {
        match self {
            None => unsafe { *out = abi::Value::NIL },
            Some(v) => unsafe { v.into_result(out) },
        }
    }
}

impl IntoResult for Value<'_> {
    unsafe fn into_result(self, out: *mut abi::Value) {
        unsafe { *out = *self.as_raw() };
    }
}

impl IntoResult for List<'_> {
    unsafe fn into_result(self, out: *mut abi::Value) {
        unsafe { *out = *self.as_value().as_raw() };
    }
}

impl IntoResult for Map<'_> {
    unsafe fn into_result(self, out: *mut abi::Value) {
        unsafe { *out = *self.as_value().as_raw() };
    }
}

//==========================================================
// Pulling a typed argument out of the argument list.
//

/// A type a UDF can take as an argument.
///
/// The generated entry point extracts each argument through this before your function is entered, so a
/// signature of `by: i64` is one tag comparison and a union read — and a client sending the wrong type gets a
/// clean error instead of reaching code that has to check for itself.
pub trait FromArg<'a>: Sized {
    /// Extract from `value`, which is the argument at `pos`.
    fn from_arg(value: Value<'a>, pos: u32) -> Result<Self>;
}

fn wrong_arg(pos: u32, wanted: &str, got: Kind) -> Error {
    Error::msg(format!(
        "argument {pos} must be {wanted}, got {got:?}"
    ))
}

impl<'a> FromArg<'a> for i64 {
    fn from_arg(value: Value<'a>, pos: u32) -> Result<Self> {
        value
            .as_i64()
            .ok_or_else(|| wrong_arg(pos, "an integer", value.kind()))
    }
}

impl<'a> FromArg<'a> for f64 {
    fn from_arg(value: Value<'a>, pos: u32) -> Result<Self> {
        value
            .as_f64()
            .ok_or_else(|| wrong_arg(pos, "a number", value.kind()))
    }
}

impl<'a> FromArg<'a> for bool {
    fn from_arg(value: Value<'a>, pos: u32) -> Result<Self> {
        value
            .as_bool()
            .ok_or_else(|| wrong_arg(pos, "a boolean", value.kind()))
    }
}

impl<'a> FromArg<'a> for &'a str {
    fn from_arg(value: Value<'a>, pos: u32) -> Result<Self> {
        value
            .as_str()
            .ok_or_else(|| wrong_arg(pos, "a string", value.kind()))
    }
}

impl<'a> FromArg<'a> for &'a [u8] {
    fn from_arg(value: Value<'a>, pos: u32) -> Result<Self> {
        value
            .as_bytes()
            .ok_or_else(|| wrong_arg(pos, "bytes", value.kind()))
    }
}

impl<'a> FromArg<'a> for List<'a> {
    fn from_arg(value: Value<'a>, pos: u32) -> Result<Self> {
        value
            .as_list()
            .ok_or_else(|| wrong_arg(pos, "a list", value.kind()))
    }
}

impl<'a> FromArg<'a> for Map<'a> {
    fn from_arg(value: Value<'a>, pos: u32) -> Result<Self> {
        value
            .as_map()
            .ok_or_else(|| wrong_arg(pos, "a map", value.kind()))
    }
}

impl<'a> FromArg<'a> for Value<'a> {
    fn from_arg(value: Value<'a>, _pos: u32) -> Result<Self> {
        Ok(value)
    }
}

/// An argument that may be left out — `None` when the client sent fewer arguments than this.
impl<'a, T: FromArg<'a>> FromArg<'a> for Option<T> {
    fn from_arg(value: Value<'a>, pos: u32) -> Result<Self> {
        if value.kind() == Kind::Absent || value.kind() == Kind::Nil {
            return Ok(None);
        }

        T::from_arg(value, pos).map(Some)
    }
}

//==========================================================
// Plumbing the generated code needs.
//

/// Not public API. Used by the generated entry points.
#[doc(hidden)]
pub mod private {
    use super::*;

    pub use core::ffi::c_void;

    /// Run a UDF body, turning a panic or an `Err` into a host status.
    ///
    /// A panic unwinding into C is undefined behaviour, so this is not optional. The landing pad costs
    /// nothing when nothing panics, which is the only reason it can sit on every call.
    pub fn guard<T: IntoResult>(
        ctx: *mut abi::Ctx,
        out: *mut abi::Value,
        body: impl FnOnce() -> Result<T>,
    ) -> i32 {
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));

        match caught {
            Ok(Ok(value)) => {
                // SAFETY: `out` is the host's result slot, valid for this call.
                unsafe { value.into_result(out) };

                abi::OK
            }
            Ok(Err(e)) => {
                report(ctx, &e.to_string());

                e.code()
            }
            Err(payload) => {
                let what = panic_message(&payload);

                report(ctx, &format!("panic in native UDF: {what}"));

                abi::ERR
            }
        }
    }

    /// As `guard`, for a call that has no result to write.
    pub fn guard_unit(ctx: *mut abi::Ctx, body: impl FnOnce() -> Result<()>) -> i32 {
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));

        match caught {
            Ok(Ok(())) => abi::OK,
            Ok(Err(e)) => {
                report(ctx, &e.to_string());

                e.code()
            }
            Err(payload) => {
                let what = panic_message(&payload);

                report(ctx, &format!("panic in native UDF: {what}"));

                abi::ERR
            }
        }
    }

    fn report(ctx: *mut abi::Ctx, message: &str) {
        // SAFETY: a borrowed string and its length; the host copies what it needs.
        unsafe {
            (host().fail)(ctx, message.as_ptr().cast(), message.len() as u32);
        }
    }

    fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
        if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "(no message)".to_string()
        }
    }

    /// The argument at `pos`, or an absent value when the client sent fewer.
    ///
    /// `ctx` is threaded through because a list or map argument carries it: without it, appending to a
    /// collection reached through an argument would go through a null context.
    pub fn arg<'a>(
        ctx: *mut abi::Ctx,
        args: *const abi::Value,
        n_args: u32,
        pos: u32,
    ) -> Value<'a> {
        Args::from_raw(args, n_args, ctx).get(pos)
    }

    /// The entry point every aggregation's `init` becomes.
    ///
    /// Writes a valid `A` into the host's zeroed memory **before** configuring it, so that `destroy` always
    /// has something real to drop — including when `configure` reports an error, which is the case that would
    /// otherwise drop uninitialised memory.
    ///
    /// # Safety
    ///
    /// `state` must point at `size_of::<A>()` bytes the host owns, suitably aligned.
    pub unsafe extern "C" fn agg_init<A: Aggregation>(
        ctx: *mut abi::Ctx,
        state: *mut c_void,
        args: *const abi::Value,
        n_args: u32,
    ) -> i32 {
        let slot = state.cast::<A>();

        // SAFETY: the host guarantees the size and alignment; nothing has been written here yet.
        unsafe { slot.write(A::default()) };

        guard_unit(ctx, || {
            // SAFETY: just initialised above.
            let acc = unsafe { &mut *slot };

            acc.configure(Args::from_raw(args, n_args, ctx))
        })
    }

    /// The one entry point on the per-record path.
    ///
    /// Note it does **not** go through `guard_unit`: `Fold::Full` is a status the host acts on rather than an
    /// error, so this needs its own landing pad to map three outcomes instead of two.
    ///
    /// # Safety
    ///
    /// `state` must be an initialised `A`, and `rec` the record the host is offering.
    pub unsafe extern "C" fn agg_accumulate<A: Aggregation>(
        ctx: *mut abi::Ctx,
        state: *mut c_void,
        rec: *const abi::Rec,
        out: *mut abi::Emit,
    ) -> i32 {
        // SAFETY: agg_init ran first, so this is a valid A.
        let acc = unsafe { &mut *state.cast::<A>() };
        let record = Record::from_raw(rec as *mut abi::Rec, ctx);
        let emit = Emit::from_raw(out, ctx);

        let caught =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| acc.accumulate(&record, &emit)));

        match caught {
            Ok(Ok(Fold::Continue)) => abi::OK,
            Ok(Ok(Fold::Full)) => abi::FULL,
            Ok(Err(e)) => {
                report(ctx, &e.to_string());

                e.code()
            }
            Err(payload) => {
                report(ctx, &format!("panic in native UDF: {}", panic_message(&payload)));

                abi::ERR
            }
        }
    }

    /// # Safety
    ///
    /// `state` must be an initialised `A`.
    pub unsafe extern "C" fn agg_heap_used<A: Aggregation>(state: *const c_void) -> u64 {
        // SAFETY: initialised by agg_init.
        let acc = unsafe { &*state.cast::<A>() };

        // No `catch_unwind`: this is called once per record on a collecting aggregation, and an implementation
        // that panics while adding up its own field sizes is not a case worth paying for on that path. A panic
        // here is the plugin's bug, and `accumulate` - which surrounds it in the host's loop - is guarded.
        acc.heap_used() as u64
    }

    /// The server's half of the partial: serialize the accumulator.
    ///
    /// **Deliberately `AsPartial::to_partial` and not `Aggregation::finalize`.** What the server writes is the
    /// state, faithfully, so that partials can be merged; the user's answer is computed on the client after
    /// merging. Calling `finalize` here would put an unmergeable mean on the wire, which is the exact mistake
    /// this design exists to make impossible.
    ///
    /// # Safety
    ///
    /// `state` must be an initialised `A`; `out` the host's result slot.
    pub unsafe extern "C" fn agg_to_partial<A: Aggregation>(
        ctx: *mut abi::Ctx,
        state: *mut c_void,
        out: *mut abi::Value,
    ) -> i32 {
        // SAFETY: initialised by agg_init.
        let acc = unsafe { &*state.cast::<A>() };
        let context = Ctx::from_raw(ctx);

        guard(ctx, out, || acc.to_partial(&context))
    }

    /// # Safety
    ///
    /// `state` must be an initialised `A`; `partial` a value the host owns for this call.
    pub unsafe extern "C" fn agg_from_partial<A: Aggregation>(
        ctx: *mut abi::Ctx,
        state: *mut c_void,
        partial: *const abi::Value,
    ) -> i32 {
        // SAFETY: initialised by agg_init.
        let acc = unsafe { &mut *state.cast::<A>() };
        // SAFETY: the host's value slot, valid for this call.
        let value = Value::from_raw(unsafe { *partial }, ctx);

        guard_unit(ctx, || acc.from_partial(value))
    }

    /// # Safety
    ///
    /// Both pointers must be initialised accumulators of the same aggregation.
    pub unsafe extern "C" fn agg_merge<A: Aggregation>(
        ctx: *mut abi::Ctx,
        dst: *mut c_void,
        src: *const c_void,
    ) -> i32 {
        // SAFETY: both were initialised by agg_init.
        let into = unsafe { &mut *dst.cast::<A>() };
        let from = unsafe { &*src.cast::<A>() };

        guard_unit(ctx, || {
            into.merge(from);

            Ok(())
        })
    }

    /// # Safety
    ///
    /// `state` must be an initialised `A`; `out` the host's result slot.
    pub unsafe extern "C" fn agg_finalize<A: Aggregation>(
        ctx: *mut abi::Ctx,
        state: *mut c_void,
        out: *mut abi::Value,
    ) -> i32 {
        // SAFETY: initialised by agg_init.
        let acc = unsafe { &mut *state.cast::<A>() };
        let context = Ctx::from_raw(ctx);

        guard(ctx, out, || acc.finalize(&context))
    }

    /// # Safety
    ///
    /// `state` must be an initialised `A`, and not used afterwards.
    pub unsafe extern "C" fn agg_destroy<A: Aggregation>(state: *mut c_void) {
        // SAFETY: agg_init always leaves a valid A here, even when configure failed - which is why it writes
        // Default::default() before doing anything else.
        unsafe { core::ptr::drop_in_place(state.cast::<A>()) };
    }
}

//==========================================================
// Macros.
//

/// Define UDFs.
///
/// Each function becomes a module of the same name holding the function and the C entry point that wraps it,
/// which is what [`module!`] then refers to. Arguments are extracted and type-checked before your body runs;
/// panics and `Err` become UDF errors.
///
/// ```ignore
/// udfs! {
///     /// Add `by` to bin "n".
///     fn increment(rec: &Record, by: Option<i64>) -> Result<i64> {
///         let by = by.unwrap_or(1);
///         let next = rec.get_int("n")?.unwrap_or(0) + by;
///
///         rec.set_int("n", next)?;
///         rec.write()?;
///
///         Ok(next)
///     }
/// }
/// ```
#[macro_export]
macro_rules! udfs {
    (
        $(
            $(#[$meta:meta])*
            fn $name:ident $(< $( $lt:lifetime ),+ >)?
                ( $rec:ident : $rec_ty:ty $(, $arg:ident : $arg_ty:ty )* $(,)? )
                -> Result<$ret:ty>
            $body:block
        )*
    ) => {
        $(
            $(#[$meta])*
            #[doc(hidden)]
            #[allow(non_snake_case, unused_variables, unused_imports)]
            pub mod $name {
                use super::*;

                /// The function as written.
                ///
                /// The signature is passed through exactly as written, lifetimes included. A UDF that both
                /// takes and returns a borrowed value has to name a lifetime to relate the two - write
                /// `fn f<'a>(rec: &'a Record, v: Value<'a>) -> Result<Value<'a>>`. One dealing only in scalars
                /// never has to mention lifetimes at all.
                pub fn call $(< $($lt),+ >)? (
                    $rec: $rec_ty
                    $(, $arg: $arg_ty)*
                ) -> $crate::Result<$ret> $body

                /// The C entry point the host calls.
                ///
                /// # Safety
                ///
                /// Called only by the host, with its own pointers.
                pub unsafe extern "C" fn entry(
                    ctx: *mut $crate::abi::Ctx,
                    rec: *mut $crate::abi::Rec,
                    args: *const $crate::abi::Value,
                    n_args: u32,
                    result: *mut $crate::abi::Value,
                ) -> i32 {
                    // `mut` and `&mut` below, so a UDF *may* declare `rec: &mut Record` and reach
                    // `with_bytes_mut`. One declaring `rec: &Record` is unaffected: `&mut T` coerces to `&T`.
                    let mut record = $crate::Record::from_raw(rec, ctx);

                    $crate::private::guard(ctx, result, || {
                        let mut pos = 0u32;

                        // Deliberately **not** annotated with the declared type: the entry point does not
                        // declare the caller's lifetimes, so writing `Value<'a>` here would not resolve.
                        // Inference takes the type from `call`'s own signature instead, which is where it is
                        // written down anyway.
                        $(
                            let $arg = $crate::FromArg::from_arg(
                                $crate::private::arg(ctx, args, n_args, pos), pos
                            )?;

                            pos += 1;
                        )*

                        let _ = pos;

                        call(&mut record $(, $arg)*)
                    })
                }

                /// What the manifest advertises.
                pub const EXPORT: $crate::abi::RecordExport = $crate::abi::RecordExport {
                    name: concat!(stringify!($name), "\0").as_ptr().cast(),
                    // Zero, because optionality lives in the argument types: `Option<T>` accepts an absent
                    // argument and `T` refuses one, so the lower bound is enforced where the type is known
                    // rather than duplicated here where it is not.
                    min_args: 0,
                    max_args: $crate::udfs!(@count $($arg)*),
                    func: entry,
                };
            }
        )*
    };

    (@count) => { 0u32 };
    (@count $head:ident $($tail:ident)*) => { 1u32 + $crate::udfs!(@count $($tail)*) };
}

/// Declare the module: its name, its version, and what it exports.
///
/// Generates `as_native_module_init`, the one symbol the server looks for.
///
/// ```ignore
/// module! {
///     name: "stats",
///     version: "1.0.0",
///     functions: [increment, sum_list],
///     aggregations: [Sum, Stats],
/// }
/// ```
///
/// The name here is only for the startup log line — a client names the module by whatever an operator called
/// it in the configuration file, which is the right way round: the operator decides what is installed.
#[macro_export]
macro_rules! module {
    (
        name: $name:expr,
        version: $version:expr,
        $(functions: [ $($func:ident),* $(,)? ],)?
        $(aggregations: [ $($agg:ty),* $(,)? ],)?
    ) => {
        const _: () = {
            static RECORD_FNS: &[$crate::abi::RecordExport] = &[
                $($($func::EXPORT),*)?
            ];

            static AGG_FNS: &[$crate::abi::AggExport] = &[
                $($($crate::agg_export!($agg)),*)?
            ];

            static MANIFEST: $crate::abi::Manifest = $crate::abi::Manifest {
                abi_version: $crate::abi::ABI_VERSION,
                module_name: concat!($name, "\0").as_ptr().cast(),
                module_version: concat!($version, "\0").as_ptr().cast(),
                n_record_fns: RECORD_FNS.len() as u32,
                record_fns: RECORD_FNS.as_ptr(),
                n_agg_fns: AGG_FNS.len() as u32,
                agg_fns: AGG_FNS.as_ptr(),
            };

            #[no_mangle]
            pub unsafe extern "C" fn as_native_module_init(
                host: *const $crate::abi::Host,
                cfg: *const $crate::abi::ModuleConfig,
            ) -> *const $crate::abi::Manifest {
                let _ = cfg;

                // Refusing here fails server startup with this module named, which is what an operator needs
                // to see - better than a server that starts and answers wrongly.
                if !unsafe { $crate::set_host(host) } {
                    return core::ptr::null();
                }

                &MANIFEST
            }
        };
    };
}

/// Declare an accumulator, and get its serialization written for you.
///
/// Generates [`Default`] and [`AsPartial`] — a map of field name to value, built and read through the ordinary
/// [`Ctx`] builders. So the author of an aggregation writes the field list, `accumulate`, `merge` and
/// `finalize`, and never a line of serialization.
///
/// ```ignore
/// partial! {
///     pub struct Stats {
///         count: i64,
///         sum: i64,
///         bin: String,
///     }
/// }
/// ```
///
/// A `macro_rules!` rather than a derive, for the same reason [`udfs!`] and [`module!`] are: a proc macro would
/// mean a dependency, and this crate has none.
///
/// # What it derives, and why that list
///
/// `Default`, `Clone`, `Debug` and `PartialEq`. An accumulator is a plain data holder, and those four are what
/// make one testable: `PartialEq` lets a test assert a partial round-tripped unchanged, and `Debug` lets it say
/// what went wrong when it did not. Every [`PartialField`] type satisfies all four, so the list costs nothing.
///
/// `Ord` is deliberately absent — a partial with an `f64` field cannot derive it, and an accumulator that needs
/// ordering (a top-k heap, say) has a *meaning* for the comparison that a derive would get wrong.
///
/// # What a field may be
///
/// A position inside a packed collection, walked without deserialising it.
///
/// Obtained from [`Record::cursor`]. `Copy`, because it is sixteen bytes of borrowed pointer and offset — which is what
/// lets it be stored in a lazy tree node and descended into later without owning anything.
///
/// # Walking
///
/// ```ignore
/// let mut c = rec.cursor("doc")?.unwrap();
///
/// while let Some((value, child)) = c.next() {
///     match value.kind() {
///         Kind::List | Kind::Map => { /* descend into `child.unwrap()` */ }
///         _ => { /* a scalar; byte-shaped ones borrow the particle */ }
///     }
/// }
/// ```
///
/// A map iterates as alternating key and value, which is how msgpack stores one: [`len`](PackedCursor::len) entries
/// means `2 * len` calls to [`next`](PackedCursor::next).
#[derive(Clone, Copy)]
pub struct PackedCursor<'r> {
    raw: abi::Cursor,
    kind: Kind,
    len: u32,
    /// Values still to be read before this container ends.
    ///
    /// **A msgpack container is counted, not delimited** — there is no closing byte to stop at, and a nested cursor
    /// borrows the whole particle, so its `buf_sz` is the end of the *record's* value rather than of this container.
    /// Without this counter, iterating a child ran on into its siblings and then into everything after them: the first
    /// correctness test summed 8,472,156 where the truth was 588.
    remaining: u32,
    /// Ties the cursor to the record it borrows from, so it cannot outlive the particle.
    _rec: core::marker::PhantomData<&'r Record>,
}

impl<'r> PackedCursor<'r> {
    fn new(raw: abi::Cursor, kind: Kind, len: u32) -> Self {
        Self {
            raw,
            kind,
            len,
            // A map yields a key and a value per entry, which is how msgpack stores one.
            remaining: if kind == Kind::Map { len.saturating_mul(2) } else { len },
            _rec: core::marker::PhantomData,
        }
    }

    /// `Kind::List` or `Kind::Map`.
    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// Elements for a list, entries for a map — the order element a sorted map carries is not counted.
    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Read the value here and advance past it.
    ///
    /// A container yields a **child cursor** alongside the value rather than refusing it, which is what makes a tree
    /// walk possible. `None` means the cursor is exhausted.
    pub fn next(&mut self) -> Option<(Value<'r>, Option<PackedCursor<'r>>)> {
        if self.remaining == 0 {
            return None;
        }

        let mut out = abi::Value::NIL;
        let mut nested = abi::Cursor::EMPTY;

        // SAFETY: the host reads and advances `self.raw`, and any pointer it writes into `out` or `nested` borrows the
        // same particle this cursor already borrows.
        let rc = unsafe { (host().cursor_next)(&mut self.raw, &mut out, &mut nested) };

        if rc != abi::OK {
            return None;
        }

        self.remaining -= 1;

        let child = match out.kind {
            abi::TYPE_LIST => Some(PackedCursor::new(nested, Kind::List, out.len)),
            abi::TYPE_MAP => Some(PackedCursor::new(nested, Kind::Map, out.len)),
            _ => None,
        };

        Some((Value::from_raw(out, core::ptr::null_mut()), child))
    }

    /// Advance past `n` values without reading them, nested containers included.
    ///
    /// This is how a list is indexed: skip to the element you want rather than reading the ones before it.
    pub fn skip(&mut self, n: u32) -> Result<()> {
        let n = n.min(self.remaining);

        // SAFETY: the host advances `self.raw` within its own bounds.
        check(unsafe { (host().cursor_skip)(&mut self.raw, n) })?;

        self.remaining -= n;

        Ok(())
    }
}

/// Anything implementing [`PartialField`]: `i64`, `f64`, `bool`, `String`, [`Blob`] for bytes, and a `Vec` of any
/// of those or of another type `partial!` declared. Anything else needs [`AsPartial`] by hand — write
/// `partial! { @no_impl ... }` to get the struct and `Default` without it.
///
/// A field's **name is its wire key**, so renaming one changes the partial format. That is the trade for not
/// writing the format down: it is derived from the code, and the code is the only place it exists.
#[macro_export]
macro_rules! partial {
    // The struct and its serialization.
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$fmeta:meta])*
                $fvis:vis $field:ident : $fty:ty
            ),* $(,)?
        }
        $($rest:tt)*
    ) => {
        $(#[$meta])*
        #[derive(Default, Clone, Debug, PartialEq)]
        $vis struct $name {
            $(
                $(#[$fmeta])*
                $fvis $field : $fty,
            )*
        }

        impl $crate::AsPartial for $name {
            fn to_partial<'c>(&self, ctx: &'c $crate::Ctx) -> $crate::Result<$crate::Value<'c>> {
                let map = ctx.map($crate::partial!(@count $($field)*))?;

                $(
                    $crate::PartialField::put_into(
                        &self.$field, ctx, &map, stringify!($field)
                    )?;
                )*

                Ok(map.as_value())
            }

            fn from_partial(&mut self, partial: $crate::Value<'_>) -> $crate::Result<()> {
                let map = partial.as_map().ok_or_else(|| {
                    $crate::Error::msg(concat!(
                        stringify!($name), ": a partial must be a map"
                    ))
                })?;

                $(
                    $crate::PartialField::read(
                        &mut self.$field, map.get_str(stringify!($field))
                    )?;
                )*

                Ok(())
            }
        }

        // So one `partial!` struct can be a field - or a `Vec` element - of another. This is what lets a
        // collecting accumulator hold `Vec<Row>` and still have its serialization written for it.
        impl $crate::PartialField for $name {
            fn put_into(
                &self, ctx: &$crate::Ctx, map: &$crate::Map<'_>, key: &str
            ) -> $crate::Result<()> {
                let value = $crate::AsPartial::to_partial(self, ctx)?;

                map.put(&$crate::Value::string(key), &value)
            }

            fn push_into(
                &self, ctx: &$crate::Ctx, list: &$crate::List<'_>
            ) -> $crate::Result<()> {
                let value = $crate::AsPartial::to_partial(self, ctx)?;

                list.push(&value)
            }

            fn read(&mut self, value: $crate::Value<'_>) -> $crate::Result<()> {
                $crate::AsPartial::from_partial(self, value)
            }
        }

        $crate::partial! { $($rest)* }
    };

    // The struct and `Default` only, for an accumulator whose serialization is written by hand.
    (
        @no_impl
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$fmeta:meta])*
                $fvis:vis $field:ident : $fty:ty
            ),* $(,)?
        }
        $($rest:tt)*
    ) => {
        $(#[$meta])*
        #[derive(Default, Clone, Debug, PartialEq)]
        $vis struct $name {
            $(
                $(#[$fmeta])*
                $fvis $field : $fty,
            )*
        }

        $crate::partial! { $($rest)* }
    };

    // A unit struct - the filter shape, whose state is genuinely nothing.
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident;
        $($rest:tt)*
    ) => {
        $(#[$meta])*
        #[derive(Default, Clone, Debug, PartialEq)]
        $vis struct $name;

        impl $crate::AsPartial for $name {
            fn to_partial<'c>(&self, _ctx: &'c $crate::Ctx) -> $crate::Result<$crate::Value<'c>> {
                // Nothing was kept, so there is nothing to serialize. A stream carrying one nil per thread is
                // what a client sees, and it can ignore it - which is what a filter's client does anyway,
                // because it is reading records.
                Ok($crate::Value::nil())
            }

            fn from_partial(&mut self, _partial: $crate::Value<'_>) -> $crate::Result<()> {
                Ok(())
            }
        }

        $crate::partial! { $($rest)* }
    };

    () => {};

    (@count) => { 0u32 };
    (@count $head:ident $($tail:ident)*) => { 1u32 + $crate::partial!(@count $($tail)*) };
}

/// Build an [`abi::AggExport`] for a type implementing [`Aggregation`]. Used by [`module!`].
#[macro_export]
#[doc(hidden)]
macro_rules! agg_export {
    ($agg:ty) => {
        $crate::abi::AggExport {
            name: <$agg as $crate::Aggregation>::NAME.as_ptr(),
            state_size: core::mem::size_of::<$agg>() as u32,
            state_align: core::mem::align_of::<$agg>() as u32,
            min_args: 0,
            max_args: 16,
            init: Some($crate::private::agg_init::<$agg>),
            accumulate: $crate::private::agg_accumulate::<$agg>,
            heap_used: Some($crate::private::agg_heap_used::<$agg>),
            // The server's partial is the *serialized state*, not the user's answer - see agg_to_partial.
            to_partial: $crate::private::agg_to_partial::<$agg>,
            from_partial: Some($crate::private::agg_from_partial::<$agg>),
            merge: Some($crate::private::agg_merge::<$agg>),
            finalize: Some($crate::private::agg_finalize::<$agg>),
            destroy: Some($crate::private::agg_destroy::<$agg>),
        }
    };
}
