// Copyright 2015-2020 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
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

//! Unique key map bin operations. Create map operations used by the client's `operate()` method.
//!
//! All maps maintain an index and a rank. The index is the item offset from the start of the map,
//! for both unordered and ordered maps. The rank is the sorted index of the value component.
//! Map supports negative indexing for index and rank.
//!
//! The default unique key map is unordered.
//!
//! Every operation that takes a map accepts any of the three map
//! collection types — `HashMap` (unordered), `IndexMap`
//! (insertion-ordered) or `BTreeMap` (key-sorted) — via the
//! [`MapLike`] trait. Maps returned by map operations
//! decode as [`Value::OrderedMap`] (or
//! [`Value::SortedMap`] for K-ordered
//! returns), preserving the pair order the server sent; the map
//! `Value` variants compare equal by content, so results can be
//! asserted against any representation.
//!
//! The [`MapOrder`] in a [`MapPolicy`] selects the storage/wire
//! representation, not the returned pair order: verified against
//! server 8.1, maps come back in canonical key order regardless of
//! order setting or creation path (plain bin write, CDT put with an
//! unordered policy, or explicit [`create`] with
//! [`MapOrder::Unordered`]) — insertion order is never preserved. The
//! observable difference is the decode variant (`OrderedMap` for
//! unordered maps, `SortedMap` for K-ordered) plus server-side
//! operation efficiency for ordered maps.
//!
//! K-ordered maps additionally support whole-map comparison in filter
//! expressions (server 6.3+): compare a K-ordered bin against a
//! `BTreeMap` literal via
//! [`expressions::map_val`](crate::expressions::map_val) with
//! `eq`/`lt`/etc.; ordering is canonical (length first, then
//! entry-wise). Unordered operands are only comparable on servers with
//! AER-6930 (8.1.2.3+).
//!
//! Key and value ordering (index positions in K-ordered maps, ranks,
//! range operations) follows the server's canonical value order —
//! keys sort `Int < String < Blob`, each type by its natural
//! (numeric / lexicographic / byte-wise) order; see
//! [`Value`]'s `Ord` implementation, which matches it.
//!
//! Index/Count examples:
//!
//! * Index 0: First item in map.
//! * Index 4: Fifth item in map.
//! * Index -1: Last item in map.
//! * Index -3: Third to last item in map.
//! * Index 1, Count 2: Second and third items in map.
//! * Index -3, Count 3: Last three items in map.
//! * Index -5, Count 4: Range between fifth to last item to second to last item inclusive.
//!
//! Rank examples:
//!
//! * Rank 0: Item with lowest value rank in map.
//! * Rank 4: Fifth lowest ranked item in map.
//! * Rank -1: Item with highest ranked value in map.
//! * Rank -3: Item with third highest ranked value in map.
//! * Rank 1 Count 2: Second and third lowest ranked items in map.
//! * Rank -3 Count 3: Top three ranked items in map.

use std::sync::Arc;

use crate::msgpack::encoder::{pack_cdt_create_op, pack_cdt_op};
use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::{CdtContext, DEFAULT_CTX};
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::value::MapLike;
use crate::Value;

#[derive(Debug, Clone, Copy)]
pub(crate) enum CdtMapOpType {
    SetType = 64,
    // 65 ADD, 66 ADD_ITEMS, 69 REPLACE and 70 REPLACE_ITEMS are the pre-4.3
    // write modes; the client sends PUT with write flags instead.
    Put = 67,
    PutItems = 68,
    Increment = 73,
    Decrement = 74,
    Clear = 75,
    RemoveByKey = 76,
    RemoveByIndex = 77,
    RemoveByRank = 79,
    RemoveKeyList = 81,
    RemoveByValue = 82,
    RemoveValueList = 83,
    RemoveByKeyInterval = 84,
    RemoveByIndexRange = 85,
    RemoveByValueInterval = 86,
    RemoveByRankRange = 87,
    RemoveByKeyRelIndexRange = 88,
    RemoveByValueRelRankRange = 89,
    Size = 96,
    GetByKey = 97,
    GetByIndex = 98,
    GetByRank = 100,
    GetByValue = 102,
    GetByKeyInterval = 103,
    GetByIndexRange = 104,
    GetByValueInterval = 105,
    GetByRankRange = 106,
    GetByKeyList = 107,
    GetByValueList = 108,
    GetByKeyRelIndexRange = 109,
    GetByValueRelRankRange = 110,
}
/// Map storage order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapOrder {
    /// Map is not ordered. This is the default.
    ///
    /// Note (verified against server 8.1): the order setting controls
    /// the map's storage/wire representation, NOT the returned pair
    /// order — the server returns the entries of an unordered map in
    /// canonical key order too, just without the K-ordered wire flag
    /// (so it decodes as [`Value::OrderedMap`]
    /// rather than [`Value::SortedMap`]).
    /// Insertion order is never preserved server-side.
    Unordered = 0,

    /// Order map by key. Returns carry the K-ordered wire flag and
    /// decode as [`Value::SortedMap`].
    KeyOrdered = 1,

    /// Order map by key, then value.
    KeyValueOrdered = 3,
}

impl MapOrder {
    pub(crate) const fn flag(self) -> u8 {
        match self {
            MapOrder::Unordered => 0x40,
            MapOrder::KeyOrdered => 0x80,
            MapOrder::KeyValueOrdered => 0xc0,
        }
    }
}

crate::flags::return_type! {
    /// What a map operation returns: one selector, optionally
    /// [`inverted`](Self::inverted).
    ///
    /// ```
    /// use aerospike::MapReturnType;
    ///
    /// let outside = MapReturnType::KEY_VALUE.inverted();
    /// assert!(outside.is_inverted());
    /// ```
    pub struct MapReturnType;
    /// Do not return a result.
    const NONE = 0;
    /// Return key index order.
    ///
    /// * 0 = first key
    /// * N = Nth key
    /// * -1 = last key
    const INDEX = 1;
    /// Return reverse key order.
    ///
    /// * 0 = last key
    /// * -1 = first key
    const REVERSE_INDEX = 2;
    /// Return value order.
    ///
    /// * 0 = smallest value
    /// * N = Nth smallest value
    /// * -1 = largest value
    const RANK = 3;
    /// Return reverse value order.
    ///
    /// * 0 = largest value
    /// * N = Nth largest value
    /// * -1 = smallest value
    const REVERSE_RANK = 4;
    /// Return count of items selected.
    const COUNT = 5;
    /// Return key for single key read and key list for range read.
    const KEY = 6;
    /// Return value for single key read and value list for range read.
    const VALUE = 7;
    /// Return key/value items. The possible return types are:
    ///
    /// * `Value::HashMap`: Returned for unordered maps
    /// * `Value::KeyValueList`: Returned for range results where range order needs to be preserved.
    const KEY_VALUE = 8;
    /// Returns true if count > 0.
    const EXISTS = 13;
    /// Returns an unordered map.
    const UNORDERED_MAP = 16;
    /// Returns an ordered map.
    const ORDERED_MAP = 17;
}

crate::flags::bit_flags! {
    /// Map write flags. Combine with `|`. Requires server version 4.3+.
    pub struct MapWriteFlags(u8);
    /// Default. Allow create or update.
    const DEFAULT = 0;
    /// If the key already exists, the item will be denied.
    /// If the key does not exist, a new item will be created.
    const CREATE_ONLY = 1;
    /// If the key already exists, the item will be overwritten.
    /// If the key does not exist, the item will be denied.
    const UPDATE_ONLY = 2;
    /// Do not raise error if a map item is denied due to write flag constraints.
    const NO_FAIL = 4;
    /// Allow other valid map items to be committed if a map item is denied due to
    /// write flag constraints.
    const PARTIAL = 8;
}

/// [`MapPolicy`] directives when creating a map and writing map items.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapPolicy {
    /// The Order of the Map
    pub order: MapOrder,
    /// The map write flags.
    pub flags: MapWriteFlags,
    /// Whether to persist the index for this map.
    pub persist_index: bool,
}

impl MapPolicy {
    /// A map policy with the given ordering and write flags (combine them
    /// with `|`; `MapWriteFlags::DEFAULT` allows create or update).
    pub const fn new(order: MapOrder, flags: MapWriteFlags) -> Self {
        MapPolicy {
            order,
            flags,
            persist_index: false,
        }
    }

    /// Like [`new`](Self::new), and the server also persists the map's
    /// index (the 0x10 bit of the order attribute), on servers that support
    /// persisted map indexes.
    pub const fn with_persisted_index(order: MapOrder, flags: MapWriteFlags) -> Self {
        MapPolicy {
            order,
            flags,
            persist_index: true,
        }
    }

    pub(crate) const fn order_attr(self) -> u8 {
        if self.persist_index {
            self.order as u8 | 0x10
        } else {
            self.order as u8
        }
    }
}

impl Default for MapPolicy {
    fn default() -> Self {
        MapPolicy::new(MapOrder::Unordered, MapWriteFlags::DEFAULT)
    }
}

/// Determines the correct operation to use when setting one or more map values, depending on the
/// map policy.
#[must_use]
pub fn create(bin: impl Into<String>, map_order: MapOrder, ctx: Vec<CdtContext>) -> Operation {
    if ctx.is_empty() {
        return set_order(bin, map_order);
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::SetType as u8,
        encoder: Arc::new(pack_cdt_create_op),
        args: vec![
            CdtArgument::Byte(map_order.flag()),
            CdtArgument::Byte(map_order as u8),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map create operation with a persisted index.
///
/// Server creates map at the top level with a persisted index. The persisted index flag (0x10)
/// is OR'd with the map order to signal the server to maintain a separate index data structure.
#[must_use]
pub fn create_with_index(bin: impl Into<String>, map_order: MapOrder) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::SetType as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(map_order as u8 | 0x10)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates set map policy operation. Server sets the map policy attributes.
/// Server does not return a result.
///
/// The required map policy attributes can be changed after the map has been created.
/// Supports optional CDT context for nested map operations.
#[must_use]
pub fn set_policy(policy: &MapPolicy, bin: impl Into<String>, ctx: Vec<CdtContext>) -> Operation {
    let mut attr = policy.order_attr();
    // If nested context, remove persist flag if present
    if !ctx.is_empty() {
        attr &= !0x10;
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::SetType as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(attr)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates set map policy operation. Server set the map policy attributes. Server does not
/// return a result.
///
/// The required map policy attributes can be changed after the map has been created.
#[must_use]
pub fn set_order(bin: impl Into<String>, map_order: MapOrder) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::SetType as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(map_order as u8)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map put operation. Server writes the key/value item to the map bin and returns the
/// map size.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
#[must_use]
pub fn put(policy: &MapPolicy, bin: impl Into<String>, key: Value, val: Value) -> Operation {
    let with_flags = policy.flags != MapWriteFlags::DEFAULT;
    let mut args = vec![CdtArgument::Value(key)];
    if with_flags || !val.is_nil() {
        args.push(CdtArgument::Value(val));
    }
    args.push(CdtArgument::Byte(policy.order_attr()));
    if with_flags {
        args.push(CdtArgument::Byte(policy.flags.bits()));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::Put as u8,
        encoder: Arc::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map put items operation. Server writes each map item to the map bin and returns the
/// map size.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
///
/// With an ordered map policy ([`MapOrder::KeyOrdered`] or [`MapOrder::KeyValueOrdered`]),
/// `HashMap` and `IndexMap` items are sorted client-side and sent with the key-ordered wire
/// header, so the server can merge them without re-sorting.
#[allow(clippy::implicit_hasher)]
pub fn put_items<M: MapLike<Value, Value>>(policy: &MapPolicy, bin: impl Into<String>, items: M) -> Operation {
    // With an ordered map policy the items are sent pre-sorted with the
    // K-ordered wire header (like Java packing a `TreeMap`), so the server
    // can merge them into the ordered map without re-sorting.
    let sort = !matches!(policy.order, MapOrder::Unordered);
    let items = match items.into_map() {
        crate::value::MapCollection::Hash(m) if sort => {
            CdtArgument::SortedMap(m.into_iter().collect())
        }
        crate::value::MapCollection::Ordered(m) if sort => {
            CdtArgument::SortedMap(m.into_iter().collect())
        }
        crate::value::MapCollection::Hash(m) => CdtArgument::Map(m),
        crate::value::MapCollection::Ordered(m) => CdtArgument::OrderedMap(m),
        crate::value::MapCollection::Sorted(m) => CdtArgument::SortedMap(m),
    };

    let mut args = vec![items, CdtArgument::Byte(policy.order_attr())];
    if policy.flags != MapWriteFlags::DEFAULT {
        args.push(CdtArgument::Byte(policy.flags.bits()));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::PutItems as u8,
        encoder: Arc::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map increment operation. Server increments values by `incr` for all items identified
/// by the key and returns the final result. Valid only for numbers.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
#[must_use]
pub fn increment_value(policy: &MapPolicy, bin: impl Into<String>, key: Value, incr: Value) -> Operation {
    let mut args = vec![CdtArgument::Value(key)];
    if !incr.is_nil() {
        args.push(CdtArgument::Value(incr));
    }
    args.push(CdtArgument::Byte(policy.order_attr()));
    let cdt_op = CdtOperation {
        op: CdtMapOpType::Increment as u8,
        encoder: Arc::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map decrement operation. Server decrements values by `decr` for all items identified
/// by the key and returns the final result. Valid only for numbers.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
#[must_use]
pub fn decrement_value(policy: &MapPolicy, bin: impl Into<String>, key: Value, decr: Value) -> Operation {
    let mut args = vec![CdtArgument::Value(key)];
    if !decr.is_nil() {
        args.push(CdtArgument::Value(decr));
    }
    args.push(CdtArgument::Byte(policy.order_attr()));
    let cdt_op = CdtOperation {
        op: CdtMapOpType::Decrement as u8,
        encoder: Arc::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map clear operation. Server removes all items in the map. Server does not return a
/// result.
#[must_use]
pub fn clear(bin: impl Into<String>) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::Clear as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes the map item identified by the key and returns
/// the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_key(
    bin: impl Into<String>,
    key: Value,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByKey as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(key),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes map items identified by keys and returns
/// removed data specified by `return_type`.
#[must_use]
pub fn remove_by_key_list(
    bin: impl Into<String>,
    keys: Vec<Value>,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveKeyList as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::List(keys),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation.
///
/// Server removes map items identified by the key range (`begin` inclusive, `end` exclusive).
/// If `begin` is `Value::Nil`, the range is less than `end`. If `end` is `Value::Nil`, the
/// range is greater than equal to `begin`. Server returns removed data specified by `return_type`.
#[must_use]
pub fn remove_by_key_range(
    bin: impl Into<String>,
    begin: Value,
    end: Value,
    return_type: MapReturnType,
) -> Operation {
    let mut args = vec![
        CdtArgument::Int(return_type.bits()),
        CdtArgument::Value(begin),
    ];
    if !end.is_nil() {
        args.push(CdtArgument::Value(end));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByKeyInterval as u8,
        encoder: Arc::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes the map items identified by value and returns
/// the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_value(
    bin: impl Into<String>,
    value: Value,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByValue as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(value),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes the map items identified by values and returns
/// the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_value_list(
    bin: impl Into<String>,
    values: Vec<Value>,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveValueList as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::List(values),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation.
///
/// Server removes map items identified by value range (`begin` inclusive, `end` exclusive).
/// If `begin` is `Value::Nil`, the range is less than `end`. If `end` is `Value::Nil`, the
/// range is greater than equal to `begin`. Server returns the removed data specified by
/// `return_type`.
#[must_use]
pub fn remove_by_value_range(
    bin: impl Into<String>,
    begin: Value,
    end: Value,
    return_type: MapReturnType,
) -> Operation {
    let mut args = vec![
        CdtArgument::Int(return_type.bits()),
        CdtArgument::Value(begin),
    ];
    if !end.is_nil() {
        args.push(CdtArgument::Value(end));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByValueInterval as u8,
        encoder: Arc::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes the map item identified by the index and return
/// the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_index(
    bin: impl Into<String>,
    index: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByIndex as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes `count` map items starting at the specified
/// index and returns the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_index_range(
    bin: impl Into<String>,
    index: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByIndexRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes the map items starting at the specified index
/// to the end of the map and returns the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_index_range_from(
    bin: impl Into<String>,
    index: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByIndexRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes the map item identified by rank and returns the
/// removed data specified by `return_type`.
#[must_use]
pub fn remove_by_rank(
    bin: impl Into<String>,
    rank: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByRank as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes `count` map items starting at the specified
/// rank and returns the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_rank_range(
    bin: impl Into<String>,
    rank: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByRankRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove operation. Server removes the map items starting at the specified rank to
/// the last ranked item and returns the removed data specified by `return_type`.
#[must_use]
pub fn remove_by_rank_range_from(
    bin: impl Into<String>,
    rank: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByRankRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map size operation. Server returns the size of the map.
#[must_use]
pub fn size(bin: impl Into<String>) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::Size as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get by key operation. Server selects the map item identified by the key and
/// returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_key(
    bin: impl Into<String>,
    key: Value,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKey as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(key),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get by key range operation.
///
/// Server selects the map items identified by the key range (`begin` inclusive, `end`
/// exclusive). If `begin` is `Value::Nil`, the range is less than `end`. If `end` is
/// `Value::Nil` the range is greater than equal to `begin`. Server returns the selected data
/// specified by `return_type`.
#[must_use]
pub fn get_by_key_range(
    bin: impl Into<String>,
    begin: Value,
    end: Value,
    return_type: MapReturnType,
) -> Operation {
    let mut args = vec![
        CdtArgument::Int(return_type.bits()),
        CdtArgument::Value(begin),
    ];
    if !end.is_nil() {
        args.push(CdtArgument::Value(end));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKeyInterval as u8,
        encoder: Arc::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get by value operation. Server selects the map items identified by value and
/// returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_value(
    bin: impl Into<String>,
    value: Value,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValue as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(value),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get by value range operation.
///
/// Server selects the map items identified by the value range (`begin` inclusive, `end`
/// exclusive). If `begin` is `Value::Nil`, the range is less than `end`. If `end` is
/// `Value::Nil`, the range is greater than equal to `begin`. Server returns the selected data
/// specified by `return_type`.
#[must_use]
pub fn get_by_value_range(
    bin: impl Into<String>,
    begin: Value,
    end: Value,
    return_type: MapReturnType,
) -> Operation {
    let mut args = vec![
        CdtArgument::Int(return_type.bits()),
        CdtArgument::Value(begin),
    ];
    if !end.is_nil() {
        args.push(CdtArgument::Value(end));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValueInterval as u8,
        encoder: Arc::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get by index operation. Server selects the map item identified by index and
/// returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_index(
    bin: impl Into<String>,
    index: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByIndex as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get by index range operation. Server selects `count` map items starting at the
/// specified index and returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_index_range(
    bin: impl Into<String>,
    index: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByIndexRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get by index range operation. Server selects the map items starting at the
/// specified index to the end of the map and returns the selected data specified by
/// `return_type`.
#[must_use]
pub fn get_by_index_range_from(
    bin: impl Into<String>,
    index: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByIndexRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get by rank operation. Server selects the map item identified by rank and
/// returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_rank(
    bin: impl Into<String>,
    rank: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByRank as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get rank range operation. Server selects `count` map items at the specified
/// rank and returns the selected data specified by `return_type`.
#[must_use]
pub fn get_by_rank_range(
    bin: impl Into<String>,
    rank: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByRankRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map get by rank range operation. Server selects the map items starting at the
/// specified rank to the last ranked item and returns the selected data specified by
/// `return_type`.
#[must_use]
pub fn get_by_rank_range_from(
    bin: impl Into<String>,
    rank: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByRankRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map remove by key relative to index range operation.
/// Server removes map items nearest to key and greater by index.
/// Server returns removed data specified by returnType.
///
/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (key,index) = [removed items]
/// (5,0) = [{5=15},{9=10}]
/// (5,1) = [{9=10}]
/// (5,-1) = [{4=2},{5=15},{9=10}]
/// (3,2) = [{9=10}]
/// (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
#[must_use]
pub fn remove_by_key_relative_index_range(
    bin: impl Into<String>,
    key: Value,
    index: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByKeyRelIndexRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(key),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates map remove by key relative to index range operation.
/// Server removes map items nearest to key and greater by index with a count limit.
/// Server returns removed data specified by returnType.
///
/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (key,index,count) = [removed items]
/// (5,0,1) = [{5=15}]
/// (5,1,2) = [{9=10}]
/// (5,-1,1) = [{4=2}]
/// (3,2,1) = [{9=10}]
/// (3,-2,2) = [{0=17}]
#[must_use]
pub fn remove_by_key_relative_index_range_count(
    bin: impl Into<String>,
    key: Value,
    index: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByKeyRelIndexRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(key),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// reates a map remove by value relative to rank range operation.
/// Server removes map items nearest to value and greater by relative rank.
/// Server returns removed data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank) = [removed items]
/// (11,1) = [{0=17}]
/// (11,-1) = [{9=10},{5=15},{0=17}]
#[must_use]
pub fn remove_by_value_relative_rank_range(
    bin: impl Into<String>,
    value: Value,
    rank: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByValueRelRankRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map remove by value relative to rank range operation.
///
/// Server removes map items nearest to value and greater by relative rank with a count limit.
/// Server returns removed data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank,count) = [removed items]
/// (11,1,1) = [{0=17}]
/// (11,-1,1) = [{9=10}]
#[must_use]
pub fn remove_by_value_relative_rank_range_count(
    bin: impl Into<String>,
    value: Value,
    rank: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByValueRelRankRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map get by key list operation.
/// Server selects map items identified by keys and returns selected data specified by returnType.
#[must_use]
pub fn get_by_key_list(
    bin: impl Into<String>,
    keys: Vec<Value>,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKeyList as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::List(keys),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map get by value list operation.
/// Server selects map items identified by values and returns selected data specified by returnType.
#[must_use]
pub fn get_by_value_list(
    bin: impl Into<String>,
    values: Vec<Value>,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValueList as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::List(values),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map get by key relative to index range operation.
/// Server selects map items nearest to key and greater by index.
/// Server returns selected data specified by returnType.
///
/// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (key,index) = [selected items]
/// (5,0) = [{5=15},{9=10}]
/// (5,1) = [{9=10}]
/// (5,-1) = [{4=2},{5=15},{9=10}]
/// (3,2) = [{9=10}]
/// (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
#[must_use]
pub fn get_by_key_relative_index_range(
    bin: impl Into<String>,
    key: Value,
    index: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKeyRelIndexRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(key),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map get by key relative to index range operation.
/// Server selects map items nearest to key and greater by index with a count limit.
/// Server returns selected data specified by returnType.
///
/// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (key,index,count) = [selected items]
/// (5,0,1) = [{5=15}]
/// (5,1,2) = [{9=10}]
/// (5,-1,1) = [{4=2}]
/// (3,2,1) = [{9=10}]
/// (3,-2,2) = [{0=17}]
#[must_use]
pub fn get_by_key_relative_index_range_count(
    bin: impl Into<String>,
    key: Value,
    index: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKeyRelIndexRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(key),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map get by value relative to rank range operation.
/// Server selects map items nearest to value and greater by relative rank.
/// Server returns selected data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank) = [selected items]
/// (11,1) = [{0=17}]
/// (11,-1) = [{9=10},{5=15},{0=17}]
#[must_use]
pub fn get_by_value_relative_rank_range(
    bin: impl Into<String>,
    value: Value,
    rank: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValueRelRankRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map get by value relative to rank range operation.
///
/// Server selects map items nearest to value and greater by relative rank with a count limit.
/// Server returns selected data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank,count) = [selected items]
/// (11,1,1) = [{0=17}]
/// (11,-1,1) = [{9=10}]
#[must_use]
pub fn get_by_value_relative_rank_range_count(
    bin: impl Into<String>,
    value: Value,
    rank: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValueRelRankRange as u8,
        encoder: Arc::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(return_type.bits()),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtMapOp(cdt_op),
    }
}
