// Copyright 2015-2017 Aerospike, Inc.
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

use std::collections::HashMap;

use errors::*;
use operations::*;
use Value;

// Map storage order.
#[derive(Clone, Copy)]
pub enum MapOrder {
    // Map is not ordered. This is the default.
    Unordered = 0,

    // Order map by key.
    KeyOrdered = 1,

    // Order map by key, then value.
    KeyValueOrdered = 3,
}


// Map return type. Type of data to return when selecting or removing items from the map.
#[derive(Clone)]
pub enum MapReturnType {
    // Do not return a result.
    None = 0,

    // Return key index order.
    //
    // 0 = first key
    // N = Nth key
    // -1 = last key
    Index = 1,

    // Return reverse key order.
    //
    // 0 = last key
    // -1 = first key
    ReverseIndex = 2,

    // Return value order.
    //
    // 0 = smallest value
    // N = Nth smallest value
    // -1 = largest value
    Rank = 3,

    // Return reserve value order.
    //
    // 0 = largest value
    // N = Nth largest value
    // -1 = smallest value
    ReverseRank = 4,

    // Return count of items selected.
    Count = 5,

    // Return key for single key read and key list for range read.
    Key = 6,

    // Return value for single key read and value list for range read.
    Value = 7,

    // Return key/value items. The possible return types are:
    //
    // Value::HashMap : Returned for unordered maps
    // Value::OrderedMap : Returned for range results where range order needs to be preserved.
    KeyValue = 8,
}

#[derive(Clone)]
pub enum MapWriteMode {
    // If the key already exists, the item will be overwritten.
    // If the key does not exist, a new item will be created.
    Update,

    // If the key already exists, the item will be overwritten.
    // If the key does not exist, the write will fail.
    UpdateOnly,

    // If the key already exists, the write will fail.
    // If the key does not exist, a new item will be created.
    CreateOnly,
}

// MapPolicy directives when creating a map and writing map items.
pub struct MapPolicy {
    order: MapOrder,
    write_mode: MapWriteMode,
}

impl MapPolicy {
    pub fn new(order: MapOrder, write_mode: MapWriteMode) -> Result<Self> {
        Ok(MapPolicy {
            order: order,
            write_mode: write_mode,
        })
    }
}

impl Default for MapPolicy {
    fn default() -> Self {
        MapPolicy::new(MapOrder::Unordered, MapWriteMode::Update).unwrap()
    }
}

fn map_write_op(policy: &MapPolicy, multi: bool) -> CdtOpType {
    match policy.write_mode {
        MapWriteMode::Update     => if multi { CdtOpType::MapPutItems     } else { CdtOpType::MapPut     },
        MapWriteMode::UpdateOnly => if multi { CdtOpType::MapReplaceItems } else { CdtOpType::MapReplace },
        MapWriteMode::CreateOnly => if multi { CdtOpType::MapAddItems     } else { CdtOpType::MapAdd     },
    }
}

fn map_order_arg(policy: &MapPolicy) -> Option<CdtArgument> {
    match policy.write_mode {
        MapWriteMode::UpdateOnly => None,
        _ => Some(CdtArgument::Byte(policy.order as u8))
    }
}

impl<'a> Operation<'a> {
    // Unique key map bin operations. Create map operations used by the client operate command.
    // The default unique key map is unordered.
    //
    // All maps maintain an index and a rank.  The index is the item offset from the start of the map,
    // for both unordered and ordered maps.  The rank is the sorted index of the value component.
    // Map supports negative indexing for index and rank.
    //
    // Index examples:
    //
    // Index 0: First item in map.
    // Index 4: Fifth item in map.
    // Index -1: Last item in map.
    // Index -3: Third to last item in map.
    // Index 1 Count 2: Second and third items in map.
    // Index -3 Count 3: Last three items in map.
    // Index -5 Count 4: Range between fifth to last item to second to last item inclusive.
    //
    // Rank examples:
    //
    // Rank 0: Item with lowest value rank in map.
    // Rank 4: Fifth lowest ranked item in map.
    // Rank -1: Item with highest ranked value in map.
    // Rank -3: Item with third highest ranked value in map.
    // Rank 1 Count 2: Second and third lowest ranked items in map.
    // Rank -3 Count 3: Top three ranked items in map.

    // MapSetPolicyOp creates set map policy operation.
    // Server sets map policy attributes.  Server returns null.
    pub fn map_set_order(bin: &'a str, map_order: MapOrder) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapSetType,
            args: vec![CdtArgument::Byte(map_order as u8)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_put_item(policy: &'a MapPolicy,
                        bin: &'a str,
                        key: &'a Value,
                        val: &'a Value)
                        -> Self {
        let mut args = vec![CdtArgument::Value(key)];
        if !val.is_nil() {
            args.push(CdtArgument::Value(val));
        }
        if let Some(arg) = map_order_arg(&policy) {
            args.push(arg);
        }
        let cdt_op = CdtOperation {
            op: map_write_op(&policy, false),
            args: args,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_put_items(policy: &'a MapPolicy,
                         bin: &'a str,
                         items: &'a HashMap<Value, Value>)
                         -> Self {
        let mut args = vec![CdtArgument::Map(items)];
        if let Some(arg) = map_order_arg(&policy) {
            args.push(arg);
        }
        let cdt_op = CdtOperation {
            op: map_write_op(&policy, true),
            args: args,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_increment_value(policy: &'a MapPolicy,
                               bin: &'a str,
                               key: &'a Value,
                               incr: &'a Value)
                               -> Self {
        let mut args = vec![CdtArgument::Value(key)];
        if !incr.is_nil() {
            args.push(CdtArgument::Value(incr));
        }
        if let Some(arg) = map_order_arg(&policy) {
            args.push(arg);
        }
        let cdt_op = CdtOperation {
            op: CdtOpType::MapIncrement,
            args: args,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_decrement_value(policy: &'a MapPolicy,
                               bin: &'a str,
                               key: &'a Value,
                               decr: &'a Value)
                               -> Self {
        let mut args = vec![CdtArgument::Value(key)];
        if !decr.is_nil() {
            args.push(CdtArgument::Value(decr));
        }
        if let Some(arg) = map_order_arg(&policy) {
            args.push(arg);
        }
        let cdt_op = CdtOperation {
            op: CdtOpType::MapDecrement,
            args: args,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_clear(_policy: &'a MapPolicy,
                     bin: &'a str)
                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapClear,
            args: vec![],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_key(_policy: &'a MapPolicy,
                             bin: &'a str,
                             key: &'a Value,
                             return_type: MapReturnType)
                             -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByKey,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Value(key)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_key_list(_policy: &'a MapPolicy,
                              bin: &'a str,
                              keys: &'a [Value],
                              return_type: MapReturnType)
                              -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByKeyList,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::List(keys)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_key_range(_policy: &'a MapPolicy,
                                   bin: &'a str,
                                   begin: &'a Value,
                                   end: &'a Value,
                                   return_type: MapReturnType)
                                   -> Self {
        let mut args = vec![CdtArgument::Byte(return_type as u8), CdtArgument::Value(begin)];
        if !end.is_nil() {
            args.push(CdtArgument::Value(end));
        }
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByKeyInterval,
            args: args,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_value(_policy: &'a MapPolicy,
                               bin: &'a str,
                               value: &'a Value,
                               return_type: MapReturnType)
                               -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByValue,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Value(value)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_value_list(_policy: &'a MapPolicy,
                                bin: &'a str,
                                values: &'a [Value],
                                return_type: MapReturnType)
                                -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByValueList,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::List(values)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_value_range(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: &'a Value,
                                     end: &'a Value,
                                     return_type: MapReturnType)
                                     -> Self {
        let mut args = vec![CdtArgument::Byte(return_type as u8), CdtArgument::Value(begin)];
        if !end.is_nil() {
            args.push(CdtArgument::Value(end));
        }
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByValueInterval,
            args: args,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_index(_policy: &'a MapPolicy,
                               bin: &'a str,
                               index: i64,
                               return_type: MapReturnType)
                               -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByIndex,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(index)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_index_range(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: i64,
                                     end: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByIndexRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(begin), CdtArgument::Int(end)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_index_range_from(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByIndexRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(begin)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_rank(_policy: &'a MapPolicy,
                               bin: &'a str,
                               rank: i64,
                               return_type: MapReturnType)
                               -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByRank,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_rank_range(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: i64,
                                     end: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByRankRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(begin), CdtArgument::Int(end)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_remove_by_rank_range_from(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByRankRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(begin)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_size(_policy: &'a MapPolicy,
                             bin: &'a str)
                             -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapSize,
            args: vec![],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_key(_policy: &'a MapPolicy,
                             bin: &'a str,
                             key: &'a Value,
                             return_type: MapReturnType)
                             -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByKey,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Value(key)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_key_range(_policy: &'a MapPolicy,
                                   bin: &'a str,
                                   begin: &'a Value,
                                   end: &'a Value,
                                   return_type: MapReturnType)
                                   -> Self {
        let mut args = vec![CdtArgument::Byte(return_type as u8), CdtArgument::Value(begin)];
        if !end.is_nil() {
            args.push(CdtArgument::Value(end));
        }
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByKeyInterval,
            args: args,
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_value(_policy: &'a MapPolicy,
                               bin: &'a str,
                               value: &'a Value,
                               return_type: MapReturnType)
                               -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByValue,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Value(value)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_value_range(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: &'a Value,
                                     end: &'a Value,
                                     return_type: MapReturnType)
                                     -> Self {
        let mut args = vec![CdtArgument::Byte(return_type as u8), CdtArgument::Value(begin)];
        if !end.is_nil() {
            args.push(CdtArgument::Value(end));
        }
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByValueInterval,
            args: args,
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_index(_policy: &'a MapPolicy,
                               bin: &'a str,
                               index: i64,
                               return_type: MapReturnType)
                               -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByIndex,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(index)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_index_range(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: i64,
                                     end: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByIndexRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(begin), CdtArgument::Int(end)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_index_range_from(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByIndexRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(begin)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_rank(_policy: &'a MapPolicy,
                               bin: &'a str,
                               rank: i64,
                               return_type: MapReturnType)
                               -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByRank,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_rank_range(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: i64,
                                     end: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByRankRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(begin), CdtArgument::Int(end)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    pub fn map_get_by_rank_range_from(_policy: &'a MapPolicy,
                                     bin: &'a str,
                                     begin: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByRankRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(begin)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }
}
