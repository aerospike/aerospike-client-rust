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

//! Combining partials on the client, with the plugin's own code.
//!
//! A node returns one partial per query thread, so something has to add them up — and the code that knows how
//! is the plugin's [`merge`](crate::Aggregation::merge), which is already written. This module runs it here.
//!
//! # The local host
//!
//! The one thing standing between the server and the client running identical code is that
//! [`finalize`](crate::Aggregation::finalize) builds its answer through [`Ctx`] — `ctx.map(4)`,
//! `map.put_str_int(..)` — and on a client there is no server to build through.
//!
//! So this module ships one: the same [`abi::Host`] vtable, implemented in Rust over ordinary Rust
//! collections. `finalize` cannot tell the difference, which is why it needs no client-side variant. **That is
//! the entire reason the host is a vtable** rather than a set of `extern` symbols.
//!
//! What the local host does *not* implement is anything about a record — reads, writes, metadata. A client has
//! no record, so those report [`Error::Refused`] rather than pretending. An aggregation's combining half never
//! touches them.
//!
//! # Two modes, because collecting is not reducing
//!
//! ```ignore
//! // Reducing: every partial into one accumulator, then the answer.
//! let mut combiner = Combiner::<Stats>::new(&[Value::from("v")])?;
//!
//! for partial in partials {
//!     combiner.absorb(&partial)?;
//! }
//!
//! let answer = combiner.finish()?;
//!
//! // Collecting: each partial is a chunk of the output, handed over as it arrives.
//! for partial in partials {
//!     let chunk = combiner.read(&partial)?;   // one accumulator's worth, not a billion rows
//!     present(&chunk);
//! }
//! ```
//!
//! Calling `absorb` in a loop over a collecting aggregation's partials would also work, and would rebuild in
//! memory exactly what the dumping existed to avoid. That is why there are two.

use core::cell::{Cell, RefCell};
use core::ffi::{c_char, c_void};
use core::sync::atomic::Ordering;

use aerospike_core::Value as CoreValue;

use crate::{abi, Aggregation, AsPartial, Ctx, Error, Result, Value, HOST};

//==========================================================
// The value tree the local host builds and reads.
//

/// One value, as the local host holds it.
///
/// Nested collections are held **by handle**, not by copy, so that appending to an inner list after nesting it
/// behaves the way it does on the server. Copying instead would have been simpler and would have made a plugin
/// that builds collections in an unusual order work in one place and not the other.
#[derive(Debug, Clone, PartialEq)]
enum Node {
    Nil,
    Bool(bool),
    Int(i64),
    Double(f64),
    Str(String),
    Blob(Vec<u8>),
    /// An index into the arena, resolved when the tree is read out.
    Coll(usize),
}

#[derive(Debug)]
enum Coll {
    List(Vec<Node>),
    Map(Vec<(Node, Node)>),
}

/// The local host's per-operation state — what a `*mut abi::Ctx` points at on this side of the wire.
struct LocalCtx {
    /// Stable under `push`, which is why handles can be indices.
    arena: Vec<Coll>,
    failure: RefCell<Option<String>>,
}

impl LocalCtx {
    fn new() -> Self {
        Self {
            arena: Vec::new(),
            failure: RefCell::new(None),
        }
    }

    fn push(&mut self, coll: Coll) -> usize {
        self.arena.push(coll);

        self.arena.len() - 1
    }
}

// Handles are `arena index + 1`, so that a handle is never null and never dereferenced. Legitimate rather than
// a trick: the ABI says a collection handle is opaque and only ever passed back to the host that made it, which
// is precisely so a host can represent one however it likes.
fn to_handle(index: usize) -> *const c_void {
    (index + 1) as *const c_void
}

fn from_handle(handle: *const c_void) -> Option<usize> {
    let raw = handle as usize;

    if raw == 0 {
        None
    } else {
        Some(raw - 1)
    }
}

// The context the reading calls reach through.
//
// A thread-local because the ABI's collection *readers* - `list_size`, `map_get_str` - take a handle and no
// context: on the server a handle is a real pointer and needs none. The local host has to find its arena
// somehow, and this is that somehow. Set for the duration of one `Combiner` call, on the thread making it.
thread_local! {
    static ACTIVE: Cell<*mut LocalCtx> = const { Cell::new(core::ptr::null_mut()) };
}

fn with_active<T>(f: impl FnOnce(&mut LocalCtx) -> T) -> Option<T> {
    let ptr = ACTIVE.get();

    if ptr.is_null() {
        return None;
    }

    // SAFETY: `ACTIVE` is set only by `Scope` below, to a live `LocalCtx` on this thread, and cleared before
    // that `LocalCtx` is dropped. Nothing else writes it and nothing crosses threads.
    Some(f(unsafe { &mut *ptr }))
}

/// Sets `ACTIVE` for a scope and restores it after, so a nested call cannot leave a dangling pointer behind.
struct Scope {
    previous: *mut LocalCtx,
}

impl Scope {
    fn enter(ctx: *mut LocalCtx) -> Self {
        let previous = ACTIVE.replace(ctx);

        Self { previous }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.set(self.previous);
    }
}

//==========================================================
// Reading a value out of the local arena.
//

fn node_to_core(local: &LocalCtx, node: &Node) -> CoreValue {
    match node {
        Node::Nil => CoreValue::Nil,
        Node::Bool(b) => CoreValue::Bool(*b),
        Node::Int(i) => CoreValue::Int(*i),
        Node::Double(d) => CoreValue::from(*d),
        Node::Str(s) => CoreValue::String(s.clone()),
        Node::Blob(b) => CoreValue::Blob(b.clone()),
        Node::Coll(index) => match local.arena.get(*index) {
            Some(Coll::List(items)) => {
                CoreValue::List(items.iter().map(|n| node_to_core(local, n)).collect())
            }
            Some(Coll::Map(pairs)) => CoreValue::HashMap(
                pairs
                    .iter()
                    .map(|(k, v)| (node_to_core(local, k), node_to_core(local, v)))
                    .collect(),
            ),
            // Not reachable through the safe surface: a handle only exists because the arena entry does.
            None => CoreValue::Nil,
        },
    }
}

fn core_to_node(local: &mut LocalCtx, value: &CoreValue) -> Node {
    match value {
        CoreValue::Nil => Node::Nil,
        CoreValue::Bool(b) => Node::Bool(*b),
        CoreValue::Int(i) => Node::Int(*i),
        CoreValue::Float(f) => Node::Double(f64::from(f.clone())),
        CoreValue::String(s) => Node::Str(s.clone()),
        CoreValue::GeoJson(s) => Node::Str(s.clone()),
        CoreValue::Blob(b) | CoreValue::HLL(b) => Node::Blob(b.clone()),
        CoreValue::List(items) | CoreValue::MultiResult(items) => {
            let nodes: Vec<Node> = items.iter().map(|v| core_to_node(local, v)).collect();

            Node::Coll(local.push(Coll::List(nodes)))
        }
        CoreValue::HashMap(m) => {
            let pairs: Vec<(Node, Node)> = m
                .iter()
                .map(|(k, v)| (core_to_node(local, k), core_to_node(local, v)))
                .collect();

            Node::Coll(local.push(Coll::Map(pairs)))
        }
        CoreValue::OrderedMap(m) => {
            let pairs: Vec<(Node, Node)> = m
                .iter()
                .map(|(k, v)| (core_to_node(local, k), core_to_node(local, v)))
                .collect();

            Node::Coll(local.push(Coll::Map(pairs)))
        }
        CoreValue::SortedMap(m) => {
            let pairs: Vec<(Node, Node)> = m
                .iter()
                .map(|(k, v)| (core_to_node(local, k), core_to_node(local, v)))
                .collect();

            Node::Coll(local.push(Coll::Map(pairs)))
        }
        CoreValue::KeyValueList(pairs) => {
            let nodes: Vec<(Node, Node)> = pairs
                .iter()
                .map(|(k, v)| (core_to_node(local, k), core_to_node(local, v)))
                .collect();

            Node::Coll(local.push(Coll::Map(nodes)))
        }
        // A particle type this client does not decode. Kept as its bytes, which is the only faithful thing to
        // do with it - and a partial containing one means the plugin wrote something this client's `Value` does
        // not know, which is worth preserving rather than flattening.
        CoreValue::Unknown(_, bytes) => Node::Blob(bytes.clone()),
        // Query-filter sentinels. They cannot reach a partial - the server never writes one into a result -
        // and there is nothing in this ABI to represent them as, so nil is the honest answer rather than a
        // guess that would compare equal to something.
        CoreValue::Infinity | CoreValue::Wildcard => Node::Nil,
    }
}

/// An `abi::Value` pointing at a node.
///
/// The borrowed bytes of a `Str` or `Blob` point into the arena, which is why nothing here may outlive the
/// operation that produced it — the same rule as on the server, for the same reason.
fn node_to_abi(node: &Node) -> abi::Value {
    match node {
        Node::Nil => abi::Value::NIL,
        Node::Bool(b) => abi::Value {
            kind: abi::TYPE_BOOL,
            len: 0,
            u: abi::ValueUnion { i: i64::from(*b) },
        },
        Node::Int(i) => abi::Value {
            kind: abi::TYPE_INT,
            len: 0,
            u: abi::ValueUnion { i: *i },
        },
        Node::Double(d) => abi::Value {
            kind: abi::TYPE_DOUBLE,
            len: 0,
            u: abi::ValueUnion { d: *d },
        },
        Node::Str(s) => abi::Value {
            kind: abi::TYPE_STR,
            len: s.len() as u32,
            u: abi::ValueUnion { p: s.as_ptr() },
        },
        Node::Blob(b) => abi::Value {
            kind: abi::TYPE_BLOB,
            len: b.len() as u32,
            u: abi::ValueUnion { p: b.as_ptr() },
        },
        Node::Coll(_) => abi::Value::NIL, // replaced by the caller, which knows the arena
    }
}

fn coll_to_abi(local: &LocalCtx, index: usize) -> abi::Value {
    let (kind, len) = match &local.arena[index] {
        Coll::List(items) => (abi::TYPE_LIST, items.len()),
        Coll::Map(pairs) => (abi::TYPE_MAP, pairs.len()),
    };

    abi::Value {
        kind,
        len: len as u32,
        u: abi::ValueUnion {
            h: to_handle(index),
        },
    }
}

fn any_to_abi(local: &LocalCtx, node: &Node) -> abi::Value {
    match node {
        Node::Coll(index) => coll_to_abi(local, *index),
        other => node_to_abi(other),
    }
}

fn abi_to_node(local: &mut LocalCtx, value: &abi::Value) -> Node {
    // SAFETY: reading the union member the kind names, which is the ABI's contract for this struct.
    unsafe {
        match value.kind {
            abi::TYPE_BOOL => Node::Bool(value.u.i != 0),
            abi::TYPE_INT => Node::Int(value.u.i),
            abi::TYPE_DOUBLE => Node::Double(value.u.d),
            abi::TYPE_STR | abi::TYPE_GEOJSON => Node::Str(
                core::str::from_utf8(core::slice::from_raw_parts(
                    value.u.p,
                    value.len as usize,
                ))
                .unwrap_or_default()
                .to_string(),
            ),
            abi::TYPE_BLOB => Node::Blob(
                core::slice::from_raw_parts(value.u.p, value.len as usize).to_vec(),
            ),
            abi::TYPE_LIST | abi::TYPE_MAP => match from_handle(value.u.h) {
                // Already in this arena: keep the handle rather than copying, so nesting stays by reference.
                Some(index) if index < local.arena.len() => Node::Coll(index),
                _ => Node::Nil,
            },
            _ => Node::Nil,
        }
    }
}

//==========================================================
// The local host's vtable.
//

/// Every call a client has no business making. One implementation, so the list of them stays a list rather
/// than becoming a page of near-identical stubs that might drift apart.
macro_rules! no_record {
    ($($name:ident: unsafe extern "C" fn($($arg:ty),*) -> $ret:ty = $default:expr;)*) => {
        $(
            unsafe extern "C" fn $name($(_: $arg),*) -> $ret {
                $default
            }
        )*
    };
}

no_record! {
    l_rec_get: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *mut abi::Value) -> i32 = abi::REFUSED;
    l_rec_get_int: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *mut i64) -> i32 = abi::REFUSED;
    l_rec_get_double: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *mut f64) -> i32 = abi::REFUSED;
    l_rec_get_bool: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *mut bool) -> i32 = abi::REFUSED;
    l_rec_get_bytes: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *mut *const u8, *mut u32) -> i32 = abi::REFUSED;
    // Refused, like every other record accessor here: the local host exists to run `merge` and `finalize` over
    // partials, where there is no record at all - let alone one whose bytes could be modified in place.
    l_rec_get_bytes_mut: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, *mut *mut u8, *mut u32) -> i32 = abi::REFUSED;
    l_rec_get_bytes_ref: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *mut *const u8, *mut u32) -> i32 = abi::REFUSED;
    l_rec_op: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, u8, u8, *const u8, u32) -> i32 = abi::REFUSED;
    l_rec_list_count: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *mut u32) -> i32 = abi::REFUSED;
    l_rec_list_ints: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, u32, u32, *mut i64) -> i32 = abi::REFUSED;
    l_rec_list_doubles: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, u32, u32, *mut f64) -> i32 = abi::REFUSED;
    l_rec_map_count: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *mut u32) -> i32 = abi::REFUSED;
    l_rec_map_get_int: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, i64, *mut abi::Value) -> i32 = abi::REFUSED;
    l_rec_map_get_str: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *const c_char, u32, *mut abi::Value) -> i32 = abi::REFUSED;
    l_rec_set_list_ints: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, *const i64, u32) -> i32 = abi::REFUSED;
    l_rec_set_list_doubles: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, *const f64, u32) -> i32 = abi::REFUSED;
    l_rec_cursor_open: unsafe extern "C" fn(*const abi::Rec, *const c_char, u32, *mut abi::Cursor, *mut abi::Value) -> i32 = abi::REFUSED;
    l_cursor_next: unsafe extern "C" fn(*mut abi::Cursor, *mut abi::Value, *mut abi::Cursor) -> i32 = abi::REFUSED;
    l_cursor_skip: unsafe extern "C" fn(*mut abi::Cursor, u32) -> i32 = abi::REFUSED;
    l_rec_numbins: unsafe extern "C" fn(*const abi::Rec) -> u32 = 0;
    l_rec_bin_names: unsafe extern "C" fn(*const abi::Rec, abi::BinCb, *mut c_void) -> i32 = abi::REFUSED;
    l_rec_ttl: unsafe extern "C" fn(*const abi::Rec) -> u32 = 0;
    l_rec_gen: unsafe extern "C" fn(*const abi::Rec) -> u16 = 0;
    l_rec_last_update_time: unsafe extern "C" fn(*const abi::Rec) -> u64 = 0;
    l_rec_device_size: unsafe extern "C" fn(*const abi::Rec) -> u32 = 0;
    l_rec_memory_size: unsafe extern "C" fn(*const abi::Rec) -> u32 = 0;
    l_rec_setname: unsafe extern "C" fn(*const abi::Rec) -> *const c_char = core::ptr::null();
    l_rec_digest: unsafe extern "C" fn(*const abi::Rec) -> *const u8 = core::ptr::null();
    l_rec_key: unsafe extern "C" fn(*const abi::Rec, *mut abi::Value) -> i32 = abi::REFUSED;
    l_rec_set: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, *const abi::Value) -> i32 = abi::REFUSED;
    l_rec_set_int: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, i64) -> i32 = abi::REFUSED;
    l_rec_set_double: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, f64) -> i32 = abi::REFUSED;
    l_rec_set_bool: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, bool) -> i32 = abi::REFUSED;
    l_rec_set_str: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, *const u8, u32) -> i32 = abi::REFUSED;
    l_rec_set_blob: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32, *const u8, u32) -> i32 = abi::REFUSED;
    l_rec_remove_bin: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec, *const c_char, u32) -> i32 = abi::REFUSED;
    l_rec_set_ttl: unsafe extern "C" fn(*mut abi::Rec, u32) -> i32 = abi::REFUSED;
    l_rec_update: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec) -> i32 = abi::REFUSED;
    l_rec_create: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec) -> i32 = abi::REFUSED;
    l_rec_delete: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Rec) -> i32 = abi::REFUSED;
    l_rec_exists: unsafe extern "C" fn(*const abi::Rec) -> bool = false;
    // A client has no output stream. An aggregation that emits is read off the query stream as records or
    // values, so its client half never reaches here.
    l_emit_record: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Emit, *const abi::Rec) -> i32 = abi::REFUSED;
    l_emit_value: unsafe extern "C" fn(*mut abi::Ctx, *mut abi::Emit, *const abi::Value) -> i32 = abi::REFUSED;
    // Nothing is running against a deadline here.
    l_deadline_exceeded: unsafe extern "C" fn(*const abi::Ctx) -> bool = false;
}

//------------------------------------------------
// Collections - reading.
//

unsafe extern "C" fn l_list_size(list: *const c_void) -> u32 {
    with_active(|local| match from_handle(list).and_then(|i| local.arena.get(i)) {
        Some(Coll::List(items)) => items.len() as u32,
        _ => 0,
    })
    .unwrap_or(0)
}

unsafe extern "C" fn l_list_get(list: *const c_void, index: u32, out: *mut abi::Value) -> i32 {
    with_active(|local| {
        let Some(Coll::List(items)) = from_handle(list).and_then(|i| local.arena.get(i)) else {
            return abi::TYPE_MISMATCH;
        };

        match items.get(index as usize) {
            // SAFETY: `out` is the caller's slot, valid for this call.
            Some(node) => {
                let value = any_to_abi(local, node);

                unsafe { *out = value };

                abi::OK
            }
            None => abi::NOT_FOUND,
        }
    })
    .unwrap_or(abi::ERR)
}

unsafe extern "C" fn l_list_get_ints(
    list: *const c_void,
    from: u32,
    n: u32,
    out: *mut i64,
) -> i32 {
    with_active(|local| {
        let Some(Coll::List(items)) = from_handle(list).and_then(|i| local.arena.get(i)) else {
            return abi::TYPE_MISMATCH;
        };

        let mut written = 0i32;

        for i in 0..n {
            let Some(node) = items.get((from + i) as usize) else {
                break;
            };

            let Node::Int(v) = node else {
                return abi::TYPE_MISMATCH;
            };

            // SAFETY: the caller promised `out` holds `n` slots.
            unsafe { *out.add(i as usize) = *v };

            written += 1;
        }

        written
    })
    .unwrap_or(abi::ERR)
}

unsafe extern "C" fn l_list_get_doubles(
    list: *const c_void,
    from: u32,
    n: u32,
    out: *mut f64,
) -> i32 {
    with_active(|local| {
        let Some(Coll::List(items)) = from_handle(list).and_then(|i| local.arena.get(i)) else {
            return abi::TYPE_MISMATCH;
        };

        let mut written = 0i32;

        for i in 0..n {
            let Some(node) = items.get((from + i) as usize) else {
                break;
            };

            let Node::Double(v) = node else {
                return abi::TYPE_MISMATCH;
            };

            // SAFETY: as above.
            unsafe { *out.add(i as usize) = *v };

            written += 1;
        }

        written
    })
    .unwrap_or(abi::ERR)
}

unsafe extern "C" fn l_list_foreach(
    list: *const c_void,
    cb: abi::ListCb,
    udata: *mut c_void,
) -> i32 {
    with_active(|local| {
        let Some(Coll::List(items)) = from_handle(list).and_then(|i| local.arena.get(i)) else {
            return abi::TYPE_MISMATCH;
        };

        // Snapshotted because the callback may build values, which appends to the arena this is borrowing.
        let values: Vec<abi::Value> = items.iter().map(|n| any_to_abi(local, n)).collect();

        for (i, value) in values.iter().enumerate() {
            // SAFETY: the callback belongs to the plugin and takes borrowed pointers.
            if !unsafe { cb(i as u32, value, udata) } {
                break;
            }
        }

        abi::OK
    })
    .unwrap_or(abi::ERR)
}

unsafe extern "C" fn l_map_size(map: *const c_void) -> u32 {
    with_active(|local| match from_handle(map).and_then(|i| local.arena.get(i)) {
        Some(Coll::Map(pairs)) => pairs.len() as u32,
        _ => 0,
    })
    .unwrap_or(0)
}

unsafe extern "C" fn l_map_get_str(
    map: *const c_void,
    key: *const c_char,
    key_len: u32,
    out: *mut abi::Value,
) -> i32 {
    // SAFETY: borrowed bytes and a length, as the ABI specifies. Not NUL-terminated.
    let wanted = unsafe {
        core::str::from_utf8(core::slice::from_raw_parts(key.cast::<u8>(), key_len as usize))
            .unwrap_or_default()
    };

    with_active(|local| {
        let Some(Coll::Map(pairs)) = from_handle(map).and_then(|i| local.arena.get(i)) else {
            return abi::TYPE_MISMATCH;
        };

        for (k, v) in pairs {
            if matches!(k, Node::Str(s) if s == wanted) {
                let value = any_to_abi(local, v);

                // SAFETY: the caller's slot.
                unsafe { *out = value };

                return abi::OK;
            }
        }

        abi::NOT_FOUND
    })
    .unwrap_or(abi::ERR)
}

unsafe extern "C" fn l_map_get_int(map: *const c_void, key: i64, out: *mut abi::Value) -> i32 {
    with_active(|local| {
        let Some(Coll::Map(pairs)) = from_handle(map).and_then(|i| local.arena.get(i)) else {
            return abi::TYPE_MISMATCH;
        };

        for (k, v) in pairs {
            if matches!(k, Node::Int(i) if *i == key) {
                let value = any_to_abi(local, v);

                // SAFETY: the caller's slot.
                unsafe { *out = value };

                return abi::OK;
            }
        }

        abi::NOT_FOUND
    })
    .unwrap_or(abi::ERR)
}

unsafe extern "C" fn l_map_foreach(
    map: *const c_void,
    cb: abi::MapCb,
    udata: *mut c_void,
) -> i32 {
    with_active(|local| {
        let Some(Coll::Map(pairs)) = from_handle(map).and_then(|i| local.arena.get(i)) else {
            return abi::TYPE_MISMATCH;
        };

        let entries: Vec<(abi::Value, abi::Value)> = pairs
            .iter()
            .map(|(k, v)| (any_to_abi(local, k), any_to_abi(local, v)))
            .collect();

        for (k, v) in &entries {
            // SAFETY: the plugin's callback, borrowed pointers.
            if !unsafe { cb(k, v, udata) } {
                break;
            }
        }

        abi::OK
    })
    .unwrap_or(abi::ERR)
}

//------------------------------------------------
// Collections - building.
//

unsafe extern "C" fn l_list_new(_ctx: *mut abi::Ctx, _capacity: u32, out: *mut abi::Value) -> i32 {
    with_active(|local| {
        let index = local.push(Coll::List(Vec::new()));

        // SAFETY: the caller's slot.
        unsafe {
            *out = abi::Value {
                kind: abi::TYPE_LIST,
                len: 0,
                u: abi::ValueUnion {
                    h: to_handle(index),
                },
            };
        }

        abi::OK
    })
    .unwrap_or(abi::ERR)
}

unsafe extern "C" fn l_map_new(_ctx: *mut abi::Ctx, _capacity: u32, out: *mut abi::Value) -> i32 {
    with_active(|local| {
        let index = local.push(Coll::Map(Vec::new()));

        // SAFETY: the caller's slot.
        unsafe {
            *out = abi::Value {
                kind: abi::TYPE_MAP,
                len: 0,
                u: abi::ValueUnion {
                    h: to_handle(index),
                },
            };
        }

        abi::OK
    })
    .unwrap_or(abi::ERR)
}

fn append(coll: *mut c_void, node: Node) -> i32 {
    with_active(|local| {
        let Some(index) = from_handle(coll) else {
            return abi::ERR;
        };

        match local.arena.get_mut(index) {
            Some(Coll::List(items)) => {
                items.push(node);

                abi::OK
            }
            _ => abi::TYPE_MISMATCH,
        }
    })
    .unwrap_or(abi::ERR)
}

fn insert(coll: *mut c_void, key: Node, value: Node) -> i32 {
    with_active(|local| {
        let Some(index) = from_handle(coll) else {
            return abi::ERR;
        };

        match local.arena.get_mut(index) {
            Some(Coll::Map(pairs)) => {
                // Last write wins, as a map does. Not a `HashMap`, because a partial's key set is tiny and a
                // linear scan over four entries beats hashing them.
                if let Some(slot) = pairs.iter_mut().find(|(k, _)| *k == key) {
                    slot.1 = value;
                } else {
                    pairs.push((key, value));
                }

                abi::OK
            }
            _ => abi::TYPE_MISMATCH,
        }
    })
    .unwrap_or(abi::ERR)
}

/// Borrowed bytes into an owned node — the local arena keeps what it is given, the way the server's does.
fn owned_str(ptr: *const u8, len: u32) -> Node {
    if ptr.is_null() {
        return Node::Str(String::new());
    }

    // SAFETY: borrowed bytes and a length, per the ABI.
    let bytes = unsafe { core::slice::from_raw_parts(ptr, len as usize) };

    Node::Str(String::from_utf8_lossy(bytes).into_owned())
}

unsafe extern "C" fn l_list_append(
    _ctx: *mut abi::Ctx,
    coll: *mut c_void,
    val: *const abi::Value,
) -> i32 {
    // SAFETY: the caller's value, valid for this call.
    let raw = unsafe { *val };

    match with_active(|local| abi_to_node(local, &raw)) {
        Some(node) => append(coll, node),
        None => abi::ERR,
    }
}

unsafe extern "C" fn l_list_append_int(_ctx: *mut abi::Ctx, coll: *mut c_void, val: i64) -> i32 {
    append(coll, Node::Int(val))
}

unsafe extern "C" fn l_list_append_double(
    _ctx: *mut abi::Ctx,
    coll: *mut c_void,
    val: f64,
) -> i32 {
    append(coll, Node::Double(val))
}

unsafe extern "C" fn l_list_append_str(
    _ctx: *mut abi::Ctx,
    coll: *mut c_void,
    val: *const u8,
    val_len: u32,
) -> i32 {
    append(coll, owned_str(val, val_len))
}

unsafe extern "C" fn l_map_put(
    _ctx: *mut abi::Ctx,
    coll: *mut c_void,
    key: *const abi::Value,
    val: *const abi::Value,
) -> i32 {
    // SAFETY: the caller's values, valid for this call.
    let (raw_key, raw_val) = unsafe { (*key, *val) };

    match with_active(|local| (abi_to_node(local, &raw_key), abi_to_node(local, &raw_val))) {
        Some((k, v)) => insert(coll, k, v),
        None => abi::ERR,
    }
}

unsafe extern "C" fn l_map_put_str_int(
    _ctx: *mut abi::Ctx,
    coll: *mut c_void,
    key: *const c_char,
    key_len: u32,
    val: i64,
) -> i32 {
    insert(coll, owned_str(key.cast(), key_len), Node::Int(val))
}

unsafe extern "C" fn l_map_put_str_str(
    _ctx: *mut abi::Ctx,
    coll: *mut c_void,
    key: *const c_char,
    key_len: u32,
    val: *const u8,
    val_len: u32,
) -> i32 {
    insert(coll, owned_str(key.cast(), key_len), owned_str(val, val_len))
}

unsafe extern "C" fn l_map_put_str_double(
    _ctx: *mut abi::Ctx,
    coll: *mut c_void,
    key: *const c_char,
    key_len: u32,
    val: f64,
) -> i32 {
    insert(coll, owned_str(key.cast(), key_len), Node::Double(val))
}

unsafe extern "C" fn l_map_put_int_int(
    _ctx: *mut abi::Ctx,
    coll: *mut c_void,
    key: i64,
    val: i64,
) -> i32 {
    insert(coll, Node::Int(key), Node::Int(val))
}

//------------------------------------------------
// Diagnostics.
//

unsafe extern "C" fn l_fail(_ctx: *mut abi::Ctx, msg: *const c_char, msg_len: u32) {
    let Node::Str(message) = owned_str(msg.cast(), msg_len) else {
        return;
    };

    with_active(|local| {
        *local.failure.borrow_mut() = Some(message);
    });
}

unsafe extern "C" fn l_log(_ctx: *mut abi::Ctx, _level: i32, msg: *const c_char, msg_len: u32) {
    if let Node::Str(message) = owned_str(msg.cast(), msg_len) {
        // A client's log is stderr. The alternative is swallowing it, and a plugin that logs while combining is
        // saying something worth reading.
        eprintln!("native udf: {message}");
    }
}

static LOCAL_HOST: abi::Host = abi::Host {
    abi_version: abi::ABI_VERSION,
    size: core::mem::size_of::<abi::Host>() as u32,

    rec_get: l_rec_get,
    rec_get_int: l_rec_get_int,
    rec_get_double: l_rec_get_double,
    rec_get_bool: l_rec_get_bool,
    rec_get_bytes: l_rec_get_bytes,
    rec_get_bytes_mut: l_rec_get_bytes_mut,
    rec_get_bytes_ref: l_rec_get_bytes_ref,
    rec_op: l_rec_op,
    rec_list_count: l_rec_list_count,
    rec_list_ints: l_rec_list_ints,
    rec_list_doubles: l_rec_list_doubles,
    rec_map_count: l_rec_map_count,
    rec_map_get_int: l_rec_map_get_int,
    rec_map_get_str: l_rec_map_get_str,
    rec_set_list_ints: l_rec_set_list_ints,
    rec_set_list_doubles: l_rec_set_list_doubles,
    rec_cursor_open: l_rec_cursor_open,
    cursor_next: l_cursor_next,
    cursor_skip: l_cursor_skip,
    rec_numbins: l_rec_numbins,
    rec_bin_names: l_rec_bin_names,

    rec_ttl: l_rec_ttl,
    rec_gen: l_rec_gen,
    rec_last_update_time: l_rec_last_update_time,
    rec_device_size: l_rec_device_size,
    rec_memory_size: l_rec_memory_size,
    rec_setname: l_rec_setname,
    rec_digest: l_rec_digest,
    rec_key: l_rec_key,

    rec_set: l_rec_set,
    rec_set_int: l_rec_set_int,
    rec_set_double: l_rec_set_double,
    rec_set_bool: l_rec_set_bool,
    rec_set_str: l_rec_set_str,
    rec_set_blob: l_rec_set_blob,
    rec_remove_bin: l_rec_remove_bin,
    rec_set_ttl: l_rec_set_ttl,
    rec_update: l_rec_update,
    rec_create: l_rec_create,
    rec_delete: l_rec_delete,
    rec_exists: l_rec_exists,

    list_size: l_list_size,
    list_get: l_list_get,
    list_get_ints: l_list_get_ints,
    list_get_doubles: l_list_get_doubles,
    list_foreach: l_list_foreach,
    map_size: l_map_size,
    map_get_str: l_map_get_str,
    map_get_int: l_map_get_int,
    map_foreach: l_map_foreach,

    list_new: l_list_new,
    list_append: l_list_append,
    list_append_int: l_list_append_int,
    list_append_double: l_list_append_double,
    list_append_str: l_list_append_str,
    map_new: l_map_new,
    map_put: l_map_put,
    map_put_str_int: l_map_put_str_int,
    map_put_str_str: l_map_put_str_str,
    map_put_str_double: l_map_put_str_double,
    map_put_int_int: l_map_put_int_int,

    emit_record: l_emit_record,
    emit_value: l_emit_value,

    fail: l_fail,
    log: l_log,
    deadline_exceeded: l_deadline_exceeded,
};

/// Install the local host, unless a real one is already there.
///
/// Idempotent, and safe to call from anywhere: a server process has set `HOST` to its own vtable during startup
/// and this must not overwrite it — a plugin loaded by `asd` has to keep talking to `asd`. Storing only into a
/// null slot is what makes the same crate usable on both sides.
fn install_local_host() {
    let _ = HOST.compare_exchange(
        core::ptr::null_mut(),
        (&raw const LOCAL_HOST).cast_mut(),
        Ordering::Release,
        Ordering::Relaxed,
    );
}

//==========================================================
// Combining.
//

/// Combines a query's partials using the plugin's own code.
///
/// `A` is the accumulator type the server ran — the same type, from the same crate, compiled once and linked
/// twice. Nothing here is a client-side reimplementation of anything.
pub struct Combiner<A: Aggregation> {
    /// The accumulator everything is merged into, configured exactly as the server's was.
    total: A,

    /// The aggregation's arguments, so a partial can be read into an accumulator that is already configured -
    /// which matters for anything that keeps a bin name or a threshold.
    args: Vec<CoreValue>,

    /// Whether anything has been absorbed. `finish` on an empty combiner is a real question with a real answer
    /// (the identity), and this distinguishes it from a bug.
    absorbed: usize,
}

impl<A: Aggregation> Combiner<A> {
    /// A combiner for an aggregation invoked with `args` — the same arguments given to
    /// `set_aggregate_function`.
    pub fn new(args: &[CoreValue]) -> Result<Self> {
        install_local_host();

        let mut local = LocalCtx::new();
        let mut total = A::default();

        configure(&mut local, &mut total, args)?;

        Ok(Self {
            total,
            args: args.to_vec(),
            absorbed: 0,
        })
    }

    /// Merge one partial in. **Reduce mode.**
    ///
    /// `from_partial` into a fresh accumulator, then the plugin's own `merge`. Every partial from every node,
    /// in any order — merging is what makes order irrelevant.
    pub fn absorb(&mut self, partial: &CoreValue) -> Result<()> {
        let mut local = LocalCtx::new();
        let mut one = A::default();

        configure(&mut local, &mut one, &self.args)?;
        read_partial(&mut local, &mut one, partial)?;

        self.total.merge(&one);
        self.absorbed += 1;

        Ok(())
    }

    /// Read one partial back into its own accumulator, without merging. **Collect mode.**
    ///
    /// For an aggregation that dumps: its partials *are* the output, in chunks, and handing each to the caller
    /// as it arrives is the whole reason it dumped. Absorbing them instead would rebuild in memory exactly what
    /// the dumping avoided — which works, and defeats the purpose.
    pub fn read(&self, partial: &CoreValue) -> Result<A> {
        let mut local = LocalCtx::new();
        let mut one = A::default();

        configure(&mut local, &mut one, &self.args)?;
        read_partial(&mut local, &mut one, partial)?;

        Ok(one)
    }

    /// The plugin's `finalize`, on everything absorbed so far.
    ///
    /// Runs the same code the server would have, through the local host, and hands back an ordinary
    /// [`aerospike_core::Value`].
    pub fn finish(&mut self) -> Result<CoreValue> {
        let mut local = LocalCtx::new();
        let ctx = Ctx::from_raw((&raw mut local).cast());

        let node = {
            let _scope = Scope::enter(&raw mut local);
            let value = self.total.finalize(&ctx)?;

            with_active(|l| abi_to_node(l, unsafe { &*value.as_raw() })).ok_or_else(|| {
                Error::msg("the local host was not active - this is an SDK bug")
            })?
        };

        if let Some(message) = local.failure.borrow().clone() {
            return Err(Error::msg(message));
        }

        Ok(node_to_core(&local, &node))
    }

    /// How many partials have been absorbed.
    pub fn absorbed(&self) -> usize {
        self.absorbed
    }

    /// The merged accumulator, for a caller that wants the fields rather than the finalized answer.
    pub fn accumulator(&self) -> &A {
        &self.total
    }
}

/// Serialize an accumulator into a partial, client-side.
///
/// The other direction from [`Combiner::read`], and the reason it is public: without it, `to_partial` is only
/// reachable from a running server, so a plugin author cannot test that their accumulator survives its own wire
/// format. It is also what a client needs in order to *send* an accumulator — a resumable aggregation, or a
/// partial cached between queries.
pub fn to_partial<A: Aggregation>(accumulator: &A) -> Result<CoreValue> {
    install_local_host();

    let mut local = LocalCtx::new();
    let ctx = Ctx::from_raw((&raw mut local).cast());

    let node = {
        let _scope = Scope::enter(&raw mut local);
        let value = accumulator.to_partial(&ctx)?;

        with_active(|l| abi_to_node(l, unsafe { &*value.as_raw() }))
            .ok_or_else(|| Error::msg("the local host was not active - this is an SDK bug"))?
    };

    if let Some(message) = local.failure.borrow().clone() {
        return Err(Error::msg(message));
    }

    Ok(node_to_core(&local, &node))
}

/// Run `configure` with the local host active, converting the arguments into it first.
fn configure<A: Aggregation>(
    local: &mut LocalCtx,
    acc: &mut A,
    args: &[CoreValue],
) -> Result<()> {
    let nodes: Vec<Node> = args.iter().map(|v| core_to_node(local, v)).collect();

    let _scope = Scope::enter(local);

    let raw: Vec<abi::Value> = nodes.iter().map(|n| any_to_abi(local, n)).collect();
    let arg_list = crate::Args::from_raw(raw.as_ptr(), raw.len() as u32, core::ptr::null_mut());

    acc.configure(arg_list)
}

/// Read a partial into `acc`, with the local host active.
fn read_partial<A: Aggregation>(
    local: &mut LocalCtx,
    acc: &mut A,
    partial: &CoreValue,
) -> Result<()> {
    let node = core_to_node(local, partial);

    let _scope = Scope::enter(local);

    let raw = any_to_abi(local, &node);
    let value = Value::from_raw(raw, core::ptr::null_mut());

    acc.from_partial(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::AsPartial;

    crate::partial! {
        /// A reducing accumulator, exactly as a plugin would declare it.
        pub struct Stats {
            count: i64,
            sum: i64,
            bin: String,
        }
    }

    impl Aggregation for Stats {
        const NAME: &'static core::ffi::CStr = c"stats";

        fn configure(&mut self, args: crate::Args<'_>) -> Result<()> {
            self.bin = args.get(0).as_str().unwrap_or("v").to_string();

            Ok(())
        }

        fn accumulate(&mut self, _rec: &crate::Record, _out: &crate::Emit) -> Result<crate::Fold> {
            unreachable!("not exercised on the client side")
        }

        fn merge(&mut self, other: &Self) {
            self.count += other.count;
            self.sum += other.sum;
        }

        fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
            let map = ctx.map(3)?;

            map.put_str_int("count", self.count)?;
            map.put_str_int("sum", self.sum)?;

            if self.count != 0 {
                // The unmergeable answer, which is only safe because it is not the partial.
                map.put_str_f64("mean", self.sum as f64 / self.count as f64)?;
            }

            Ok(map.as_value())
        }
    }

    /// A partial written by `to_partial` must read back through `from_partial` unchanged. Everything else here
    /// depends on that round trip being faithful, because that is what makes `merge` sufficient.
    #[test]
    fn a_partial_round_trips() {
        install_local_host();

        let mut local = LocalCtx::new();
        let source = Stats {
            count: 3,
            sum: 12,
            bin: "score".to_string(),
        };

        let node = {
            let _scope = Scope::enter(&raw mut local);
            let ctx = Ctx::from_raw(core::ptr::null_mut());
            let value = source.to_partial(&ctx).unwrap();

            with_active(|l| abi_to_node(l, unsafe { &*value.as_raw() })).unwrap()
        };

        let partial = node_to_core(&local, &node);
        let combiner = Combiner::<Stats>::new(&[CoreValue::from("score")]).unwrap();
        let read_back = combiner.read(&partial).unwrap();

        assert_eq!(read_back.count, 3);
        assert_eq!(read_back.sum, 12);
        assert_eq!(read_back.bin, "score", "a String field survives the round trip");
    }

    /// The client half end to end: several partials in, one answer out, and the answer is a mean — the thing
    /// that could not have been on the wire.
    #[test]
    fn partials_merge_into_one_answer() {
        let args = [CoreValue::from("v")];

        let make = |count: i64, sum: i64| {
            let mut m = std::collections::HashMap::new();

            m.insert(CoreValue::from("count"), CoreValue::from(count));
            m.insert(CoreValue::from("sum"), CoreValue::from(sum));
            m.insert(CoreValue::from("bin"), CoreValue::from("v"));

            CoreValue::HashMap(m)
        };

        let mut combiner = Combiner::<Stats>::new(&args).unwrap();

        // Three nodes' worth, in no particular order.
        combiner.absorb(&make(2, 10)).unwrap();
        combiner.absorb(&make(3, 5)).unwrap();
        combiner.absorb(&make(5, 35)).unwrap();

        assert_eq!(combiner.absorbed(), 3);
        assert_eq!(combiner.accumulator().count, 10);
        assert_eq!(combiner.accumulator().sum, 50);

        let answer = combiner.finish().unwrap();

        let CoreValue::HashMap(map) = &answer else {
            panic!("wanted a map, got {answer:?}");
        };

        assert_eq!(map.get(&CoreValue::from("count")), Some(&CoreValue::from(10)));
        assert_eq!(map.get(&CoreValue::from("sum")), Some(&CoreValue::from(50)));
        assert_eq!(
            map.get(&CoreValue::from("mean")),
            Some(&CoreValue::from(5.0)),
            "finalize computes the mean on the client, on fully merged data"
        );
    }

    crate::partial! {
        /// One row of a collecting accumulator's output.
        pub struct Row {
            id: i64,
            name: String,
        }
    }

    crate::partial! {
        /// A collecting accumulator - the shape that dumps.
        pub struct Rows {
            rows: Vec<Row>,
        }
    }

    impl Aggregation for Rows {
        const NAME: &'static core::ffi::CStr = c"rows";

        fn accumulate(&mut self, _rec: &crate::Record, _out: &crate::Emit) -> Result<crate::Fold> {
            unreachable!("not exercised on the client side")
        }

        fn merge(&mut self, other: &Self) {
            self.rows.extend(other.rows.iter().cloned());
        }

        fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
            let list = ctx.list(self.rows.len() as u32)?;

            for row in &self.rows {
                let m = ctx.map(2)?;

                m.put_str_int("id", row.id)?;
                m.put_str_str("name", &row.name)?;
                list.push_map(&m)?;
            }

            Ok(list.as_value())
        }
    }

    /// A `Vec` of nested `partial!` structs, which is what a collecting accumulator holds — so the macro has to
    /// serialize a list of maps and read it back.
    #[test]
    fn a_vec_of_nested_partials_round_trips() {
        install_local_host();

        let mut local = LocalCtx::new();
        let source = Rows {
            rows: vec![
                Row {
                    id: 1,
                    name: "one".to_string(),
                },
                Row {
                    id: 2,
                    name: "two".to_string(),
                },
            ],
        };

        let node = {
            let _scope = Scope::enter(&raw mut local);
            let ctx = Ctx::from_raw(core::ptr::null_mut());
            let value = source.to_partial(&ctx).unwrap();

            with_active(|l| abi_to_node(l, unsafe { &*value.as_raw() })).unwrap()
        };

        let partial = node_to_core(&local, &node);
        let combiner = Combiner::<Rows>::new(&[]).unwrap();
        let read_back = combiner.read(&partial).unwrap();

        assert_eq!(read_back.rows.len(), 2, "both rows came back");
        assert_eq!(read_back.rows[0].id, 1);
        assert_eq!(read_back.rows[0].name, "one");
        assert_eq!(read_back.rows[1].name, "two");
    }

    /// Chunks are read one at a time rather than merged, which is the point of having dumped them.
    #[test]
    fn chunks_are_read_one_at_a_time() {
        install_local_host();

        let combiner = Combiner::<Rows>::new(&[]).unwrap();

        let chunk = |ids: &[i64]| {
            let rows: Vec<CoreValue> = ids
                .iter()
                .map(|id| {
                    let mut m = std::collections::HashMap::new();

                    m.insert(CoreValue::from("id"), CoreValue::from(*id));
                    m.insert(CoreValue::from("name"), CoreValue::from("x"));

                    CoreValue::HashMap(m)
                })
                .collect();

            let mut outer = std::collections::HashMap::new();

            outer.insert(CoreValue::from("rows"), CoreValue::List(rows));

            CoreValue::HashMap(outer)
        };

        let mut seen = Vec::new();

        for partial in [chunk(&[1, 2]), chunk(&[3]), chunk(&[4, 5, 6])] {
            let rows = combiner.read(&partial).unwrap();

            // Never more than one chunk in hand - the property `read` exists for.
            seen.extend(rows.rows.iter().map(|r| r.id));
        }

        assert_eq!(seen, vec![1, 2, 3, 4, 5, 6]);
    }

    /// A client has no record, and the local host says so rather than inventing one.
    #[test]
    fn the_local_host_refuses_record_access() {
        install_local_host();

        // SAFETY: the local host's record calls ignore their arguments - refusing is all they do, which is
        // exactly what this is checking.
        let rc = unsafe { (LOCAL_HOST.rec_get_int)(core::ptr::null(), c"n".as_ptr(), 1, &mut 0i64) };

        assert_eq!(rc, abi::REFUSED, "a record read on the client must be refused");
    }
}
