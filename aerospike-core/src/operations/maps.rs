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
//! Map supports negative indexing for indexjkj and rank.
//!
//! The default unique key map is unordered.
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

use crate::msgpack::encoder::pack_cdt_op;
use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::DEFAULT_CTX;
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum CdtMapOpType {
    SetType = 64,
    Add = 65,
    AddItems = 66,
    Put = 67,
    PutItems = 68,
    Replace = 69,
    ReplaceItems = 70,
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
#[derive(Debug, Clone, Copy)]
pub enum MapOrder {
    /// Map is not ordered. This is the default.
    Unordered = 0,

    /// Order map by key.
    KeyOrdered = 1,

    /// Order map by key, then value.
    KeyValueOrdered = 3,
}

/// Map return type. Type of data to return when selecting or removing items from the map.
#[derive(Debug, Clone, Copy)]
pub enum MapReturnType {
    /// Do not return a result.
    None = 0,

    /// Return key index order.
    ///
    /// * 0 = first key
    /// * N = Nth key
    /// * -1 = last key
    Index = 1,

    /// Return reverse key order.
    ///
    /// * 0 = last key
    /// * -1 = first key
    ReverseIndex = 2,

    /// Return value order.
    ///
    /// * 0 = smallest value
    /// * N = Nth smallest value
    /// * -1 = largest value
    Rank = 3,

    /// Return reserve value order.
    ///
    /// * 0 = largest value
    /// * N = Nth largest value
    /// * -1 = smallest value
    ReverseRank = 4,

    /// Return count of items selected.
    Count = 5,

    /// Return key for single key read and key list for range read.
    Key = 6,

    /// Return value for single key read and value list for range read.
    Value = 7,

    /// Return key/value items. The possible return types are:
    ///
    /// * `Value::HashMap`: Returned for unordered maps
    /// * `Value::OrderedMap`: Returned for range results where range order needs to be preserved.
    KeyValue = 8,

    /// Invert meaning of map command and return values.
    /// With the INVERTED flag enabled, the keys outside of the specified key range will be removed and returned.
    Inverted = 0x10000,
}

/// Unique key map write type.
#[derive(Debug, Clone, Copy)]
pub enum MapWriteMode {
    /// If the key already exists, the item will be overwritten.
    /// If the key does not exist, a new item will be created.
    Update,

    /// If the key already exists, the item will be overwritten.
    /// If the key does not exist, the write will fail.
    UpdateOnly,

    /// If the key already exists, the write will fail.
    /// If the key does not exist, a new item will be created.
    CreateOnly,
}

/// `MapPolicy` directives when creating a map and writing map items.
#[derive(Debug, Clone, Copy)]
pub struct MapPolicy {
    /// The Order of the Map
    pub order: MapOrder,
    /// The Map Write Mode
    pub write_mode: MapWriteMode,
}

impl MapPolicy {
    /// Create a new map policy given the ordering for the map and the write mode.
    pub const fn new(order: MapOrder, write_mode: MapWriteMode) -> Self {
        MapPolicy { order, write_mode }
    }
}

impl Default for MapPolicy {
    fn default() -> Self {
        MapPolicy::new(MapOrder::Unordered, MapWriteMode::Update)
    }
}

/// Determines the correct operation to use when setting one or more map values, depending on the
/// map policy.
#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) const fn map_write_op(policy: &MapPolicy, multi: bool) -> CdtMapOpType {
    match policy.write_mode {
        MapWriteMode::Update => {
            if multi {
                CdtMapOpType::PutItems
            } else {
                CdtMapOpType::Put
            }
        }
        MapWriteMode::UpdateOnly => {
            if multi {
                CdtMapOpType::ReplaceItems
            } else {
                CdtMapOpType::Replace
            }
        }
        MapWriteMode::CreateOnly => {
            if multi {
                CdtMapOpType::AddItems
            } else {
                CdtMapOpType::Add
            }
        }
    }
}
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn map_order_arg(policy: &MapPolicy) -> Option<CdtArgument> {
    match policy.write_mode {
        MapWriteMode::UpdateOnly => None,
        _ => Some(CdtArgument::Byte(policy.order as u8)),
    }
}

#[doc(hidden)]
pub const fn map_order_flag(order: MapOrder) -> u8 {
    match order {
        MapOrder::KeyOrdered => 0x80,
        MapOrder::Unordered => 0x40,
        MapOrder::KeyValueOrdered => 0xc0,
    }
}

/// Create set map policy operation. Server set the map policy attributes. Server does not
/// return a result.
///
/// The required map policy attributes can be changed after the map has been created.
pub fn set_order(bin: &str, map_order: MapOrder) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::SetType as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(map_order as u8)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map put operation. Server writes the key/value item to the map bin and returns the
/// map size.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
pub fn put<'a>(
    policy: &'a MapPolicy,
    bin: &'a str,
    key: &'a Value,
    val: &'a Value,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Value(key)];
    if !val.is_nil() {
        args.push(CdtArgument::Value(val));
    }
    if let Some(arg) = map_order_arg(policy) {
        args.push(arg);
    }
    let cdt_op = CdtOperation {
        op: map_write_op(policy, false) as u8,
        encoder: Box::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map put items operation. Server writes each map item to the map bin and returns the
/// map size.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
#[allow(clippy::implicit_hasher)]
pub fn put_items<'a>(
    policy: &'a MapPolicy,
    bin: &'a str,
    items: &'a HashMap<Value, Value>,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Map(items)];
    if let Some(arg) = map_order_arg(policy) {
        args.push(arg);
    }
    let cdt_op = CdtOperation {
        op: map_write_op(policy, true) as u8,
        encoder: Box::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map increment operation. Server increments values by `incr` for all items identified
/// by the key and returns the final result. Valid only for numbers.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
pub fn increment_value<'a>(
    policy: &'a MapPolicy,
    bin: &'a str,
    key: &'a Value,
    incr: &'a Value,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Value(key)];
    if !incr.is_nil() {
        args.push(CdtArgument::Value(incr));
    }
    if let Some(arg) = map_order_arg(policy) {
        args.push(arg);
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::Increment as u8,
        encoder: Box::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map decrement operation. Server decrements values by `decr` for all items identified
/// by the key and returns the final result. Valid only for numbers.
///
/// The required map policy dictates the type of map to create when it does not exist. The map
/// policy also specifies the mode used when writing items to the map.
pub fn decrement_value<'a>(
    policy: &'a MapPolicy,
    bin: &'a str,
    key: &'a Value,
    decr: &'a Value,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Value(key)];
    if !decr.is_nil() {
        args.push(CdtArgument::Value(decr));
    }
    if let Some(arg) = map_order_arg(policy) {
        args.push(arg);
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::Decrement as u8,
        encoder: Box::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map clear operation. Server removes all items in the map. Server does not return a
/// result.
pub fn clear(bin: &str) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::Clear as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes the map item identified by the key and returns
/// the removed data specified by `return_type`.
pub fn remove_by_key<'a>(
    bin: &'a str,
    key: &'a Value,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByKey as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(key),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes map items identified by keys and returns
/// removed data specified by `return_type`.
pub fn remove_by_key_list<'a>(
    bin: &'a str,
    keys: &'a [Value],
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveKeyList as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::List(keys),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes map items identified by the key range
/// (`begin` inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is less than
/// `end`. If `end` is `Value::Nil`, the range is greater than equal to `begin`. Server returns
/// removed data specified by `return_type`.
pub fn remove_by_key_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: MapReturnType,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Byte(return_type as u8),
        CdtArgument::Value(begin),
    ];
    if !end.is_nil() {
        args.push(CdtArgument::Value(end));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByKeyInterval as u8,
        encoder: Box::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes the map items identified by value and returns
/// the removed data specified by `return_type`.
pub fn remove_by_value<'a>(
    bin: &'a str,
    value: &'a Value,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByValue as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes the map items identified by values and returns
/// the removed data specified by `return_type`.
pub fn remove_by_value_list<'a>(
    bin: &'a str,
    values: &'a [Value],
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveValueList as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::List(values),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes map items identified by value range (`begin`
/// inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is less than `end`. If
/// `end` is `Value::Nil`, the range is greater than equal to `begin`. Server returns the
/// removed data specified by `return_type`.
pub fn remove_by_value_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: MapReturnType,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Byte(return_type as u8),
        CdtArgument::Value(begin),
    ];
    if !end.is_nil() {
        args.push(CdtArgument::Value(end));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByValueInterval as u8,
        encoder: Box::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes the map item identified by the index and return
/// the removed data specified by `return_type`.
pub fn remove_by_index(bin: &str, index: i64, return_type: MapReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByIndex as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes `count` map items starting at the specified
/// index and returns the removed data specified by `return_type`.
pub fn remove_by_index_range(
    bin: &str,
    index: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByIndexRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes the map items starting at the specified index
/// to the end of the map and returns the removed data specified by `return_type`.
pub fn remove_by_index_range_from(bin: &str, index: i64, return_type: MapReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByIndexRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes the map item identified by rank and returns the
/// removed data specified by `return_type`.
pub fn remove_by_rank(bin: &str, rank: i64, return_type: MapReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByRank as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes `count` map items starting at the specified
/// rank and returns the removed data specified by `return_type`.
pub fn remove_by_rank_range(
    bin: &str,
    rank: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove operation. Server removes the map items starting at the specified rank to
/// the last ranked item and returns the removed data specified by `return_type`.
pub fn remove_by_rank_range_from(bin: &str, rank: i64, return_type: MapReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map size operation. Server returns the size of the map.
pub fn size(bin: &str) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::Size as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get by key operation. Server selects the map item identified by the key and
/// returns the selected data specified by `return_type`.
pub fn get_by_key<'a>(bin: &'a str, key: &'a Value, return_type: MapReturnType) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKey as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(key),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get by key range operation. Server selects the map items identified by the key
/// range (`begin` inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is less
/// than `end`. If `end` is `Value::Nil` the range is greater than equal to `begin`. Server
/// returns the selected data specified by `return_type`.
pub fn get_by_key_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: MapReturnType,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Byte(return_type as u8),
        CdtArgument::Value(begin),
    ];
    if !end.is_nil() {
        args.push(CdtArgument::Value(end));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKeyInterval as u8,
        encoder: Box::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get by value operation. Server selects the map items identified by value and
/// returns the selected data specified by `return_type`.
pub fn get_by_value<'a>(
    bin: &'a str,
    value: &'a Value,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValue as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get by value range operation. Server selects the map items identified by the
/// value range (`begin` inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is
/// less than `end`. If `end` is `Value::Nil`, the range is greater than equal to `begin`.
/// Server returns the selected data specified by `return_type`.
pub fn get_by_value_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: MapReturnType,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Byte(return_type as u8),
        CdtArgument::Value(begin),
    ];
    if !end.is_nil() {
        args.push(CdtArgument::Value(end));
    }
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValueInterval as u8,
        encoder: Box::new(pack_cdt_op),
        args,
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get by index operation. Server selects the map item identified by index and
/// returns the selected data specified by `return_type`.
pub fn get_by_index(bin: &str, index: i64, return_type: MapReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByIndex as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get by index range operation. Server selects `count` map items starting at the
/// specified index and returns the selected data specified by `return_type`.
pub fn get_by_index_range(
    bin: &str,
    index: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByIndexRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get by index range operation. Server selects the map items starting at the
/// specified index to the end of the map and returns the selected data specified by
/// `return_type`.
pub fn get_by_index_range_from(bin: &str, index: i64, return_type: MapReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByIndexRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get by rank operation. Server selects the map item identified by rank and
/// returns the selected data specified by `return_type`.
pub fn get_by_rank(bin: &str, rank: i64, return_type: MapReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByRank as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get rank range operation. Server selects `count` map items at the specified
/// rank and returns the selected data specified by `return_type`.
pub fn get_by_rank_range(
    bin: &str,
    rank: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map get by rank range operation. Server selects the map items starting at the
/// specified rank to the last ranked item and returns the selected data specified by
/// `return_type`.
pub fn get_by_rank_range_from(bin: &str, rank: i64, return_type: MapReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
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
pub fn remove_by_key_relative_index_range<'a>(
    bin: &'a str,
    key: &'a Value,
    index: i64,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByKeyRelIndexRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(key),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Create map remove by key relative to index range operation.
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
pub fn remove_by_key_relative_index_range_count<'a>(
    bin: &'a str,
    key: &'a Value,
    index: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByKeyRelIndexRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(key),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
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
pub fn remove_by_value_relative_rank_range<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByValueRelRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map remove by value relative to rank range operation.
/// Server removes map items nearest to value and greater by relative rank with a count limit.
/// Server returns removed data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank,count) = [removed items]
/// (11,1,1) = [{0=17}]
/// (11,-1,1) = [{9=10}]
pub fn remove_by_value_relative_rank_range_count<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::RemoveByValueRelRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map get by key list operation.
/// Server selects map items identified by keys and returns selected data specified by returnType.
pub fn get_by_key_list<'a>(
    bin: &'a str,
    keys: &'a [Value],
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKeyList as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::List(keys),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map get by value list operation.
/// Server selects map items identified by values and returns selected data specified by returnType.
pub fn get_by_value_list<'a>(
    bin: &'a str,
    values: &'a [Value],
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValueList as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::List(values),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
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
pub fn get_by_key_relative_index_range<'a>(
    bin: &'a str,
    key: &'a Value,
    index: i64,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKeyRelIndexRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(key),
            CdtArgument::Int(index),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
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
pub fn get_by_key_relative_index_range_count<'a>(
    bin: &'a str,
    key: &'a Value,
    index: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByKeyRelIndexRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(key),
            CdtArgument::Int(index),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
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
pub fn get_by_value_relative_rank_range<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValueRelRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}

/// Creates a map get by value relative to rank range operation.
/// Server selects map items nearest to value and greater by relative rank with a count limit.
/// Server returns selected data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// (value,rank,count) = [selected items]
/// (11,1,1) = [{0=17}]
/// (11,-1,1) = [{9=10}]
pub fn get_by_value_relative_rank_range_count<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    count: i64,
    return_type: MapReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtMapOpType::GetByValueRelRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(value),
            CdtArgument::Int(rank),
            CdtArgument::Int(count),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtMapOp(cdt_op),
    }
}
