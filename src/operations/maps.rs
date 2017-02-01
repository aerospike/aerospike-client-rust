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

use Value;
use operations::{Operation, OperationType, OperationBin, OperationData,
                 CdtOperation, CdtOpType, CdtArgument};

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
#[derive(Debug, Clone)]
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
}

/// Unique key map write type.
#[derive(Debug, Clone)]
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

/// MapPolicy directives when creating a map and writing map items.
#[derive(Debug)]
pub struct MapPolicy {
    order: MapOrder,
    write_mode: MapWriteMode,
}

impl MapPolicy {
    pub fn new(order: MapOrder, write_mode: MapWriteMode) -> Self {
        MapPolicy {
            order: order,
            write_mode: write_mode,
        }
    }
}

impl Default for MapPolicy {
    fn default() -> Self {
        MapPolicy::new(MapOrder::Unordered, MapWriteMode::Update)
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

    /// Create set map policy operation. Server set the map policy attributes. Server does not
    /// return a result.
    ///
    /// The required map policy attributes can be changed after the map has been created.
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

    /// Create map put operation. Server writes the key/value item to the map bin and returns the
    /// map size.
    ///
    /// The required map policy dictates the type of map to create when it does not exist. The map
    /// policy also specifies the mode used when writing items to the map.
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

    /// Create map put items operation. Server writes each map item to the map bin and returns the
    /// map size.
    ///
    /// The required map policy dictates the type of map to create when it does not exist. The map
    /// policy also specifies the mode used when writing items to the map.
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

    /// Create map increment operation. Server increments values by `incr` for all items identified
    /// by the key and returns the final result. Valid only for numbers.
    ///
    /// The required map policy dictates the type of map to create when it does not exist. The map
    /// policy also specifies the mode used when writing items to the map.
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

    /// Create map decrement operation. Server decrements values by `decr` for all items identified
    /// by the key and returns the final result. Valid only for numbers.
    ///
    /// The required map policy dictates the type of map to create when it does not exist. The map
    /// policy also specifies the mode used when writing items to the map.
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

    /// Create map clear operation. Server removes all items in the map. Server does not return a
    /// result.
    pub fn map_clear(bin: &'a str) -> Self {
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

    /// Create map remove operation. Server removes the map item identified by the key and returns
    /// the removed data specified by `return_type`.
    pub fn map_remove_by_key(bin: &'a str,
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

    /// Create map remove operation. Server removes map items identified by keys and returns
    /// removed data specified by `return_type`.
    pub fn map_remove_by_key_list(bin: &'a str,
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

    /// Create map remove operation. Server removes map items identified by the key range
    /// (`begin` inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is less than
    /// `end`. If `end` is `Value::Nil`, the range is greater than equal to `begin`. Server returns
    /// removed data specified by `return_type`.
    pub fn map_remove_by_key_range(bin: &'a str,
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

    /// Create map remove operation. Server removes the map items identified by value and returns
    /// the removed data specified by `return_type`.
    pub fn map_remove_by_value(bin: &'a str,
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

    /// Create map remove operation. Server removes the map items identified by values and returns
    /// the removed data specified by `return_type`.
    pub fn map_remove_by_value_list(bin: &'a str,
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

    /// Create map remove operation. Server removes map items identified by value range (`begin`
    /// inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is less than `end`. If
    /// `end` is `Value::Nil`, the range is greater than equal to `begin`. Server returns the
    /// removed data specified by `return_type`.
    pub fn map_remove_by_value_range(bin: &'a str,
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

    /// Create map remove operation. Server removes the map item identified by the index and return
    /// the removed data specified by `return_type`.
    pub fn map_remove_by_index(bin: &'a str,
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

    /// Create map remove operation. Server removes `count` map items starting at the specified
    /// index and returns the removed data specified by `return_type`.
    pub fn map_remove_by_index_range(bin: &'a str,
                                     index: i64,
                                     count: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByIndexRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(index), CdtArgument::Int(count)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    /// Create map remove operation. Server removes the map items starting at the specified index
    /// to the end of the map and returns the removed data specified by `return_type`.
    pub fn map_remove_by_index_range_from(bin: &'a str,
                                     index: i64,
                                     return_type: MapReturnType)
                                     -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByIndexRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(index)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    /// Create map remove operation. Server removes the map item identified by rank and returns the
    /// removed data specified by `return_type`.
    pub fn map_remove_by_rank(bin: &'a str,
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

    /// Create map remove operation. Server removes `count` map items starting at the specified
    /// rank and returns the removed data specified by `return_type`.
    pub fn map_remove_by_rank_range(bin: &'a str,
                                    rank: i64,
                                    count: i64,
                                    return_type: MapReturnType)
                                    -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByRankRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank), CdtArgument::Int(count)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    /// Create map remove operation. Server removes the map items starting at the specified rank to
    /// the last ranked item and returns the removed data specified by `return_type`.
    pub fn map_remove_by_rank_range_from(bin: &'a str,
                                         rank: i64,
                                         return_type: MapReturnType)
                                         -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapRemoveByRankRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    /// Create map size operation. Server returns the size of the map.
    pub fn map_size(bin: &'a str) -> Self {
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

    /// Create map get by key operation. Server selects the map item idenfieid by the key and
    /// returns the selected data specified by `return_type`.
    pub fn map_get_by_key(bin: &'a str,
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

    /// Create map get by key range operation. Server selects the map items identified by the key
    /// range (`begin` inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is less
    /// than `end`. If `end` is `Value::Nil` the range is greater than equal to `begin`. Server
    /// returns the selected data specified by `return_type`.
    pub fn map_get_by_key_range(bin: &'a str,
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

    /// Create map get by value operation. Server selects the map items identified by value and
    /// returns the selected data specified by `return_type`.
    pub fn map_get_by_value(bin: &'a str,
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

    /// Create map get by value range operation. Server selects the map items identified by the
    /// value range (`begin` inclusive, `end` exclusive). If `begin` is `Value::Nil`, the range is
    /// less than `end`. If `end` is `Value::Nil`, the range is greater than equal to `begin`.
    /// Server returns the selected data specified by `return_type`.
    pub fn map_get_by_value_range(bin: &'a str,
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

    /// Create map get by index operation. Server selects the map item identified by index and
    /// returns the selected data specified by `return_type`.
    pub fn map_get_by_index(bin: &'a str,
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

    /// Create map get by index range operation. Server selects `count` map items starting at the
    /// specified index and returns the selected data specified by `return_type`.
    pub fn map_get_by_index_range(bin: &'a str,
                                  index: i64,
                                  count: i64,
                                  return_type: MapReturnType)
                                  -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByIndexRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(index), CdtArgument::Int(count)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    /// Create map get by index range operation. Server selects the map items starting at the
    /// specified index to the end of the map and returns the selected data specified by
    /// `return_type`.
    pub fn map_get_by_index_range_from(bin: &'a str,
                                       index: i64,
                                       return_type: MapReturnType)
                                       -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByIndexRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(index)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    /// Create map get by rank operation. Server selects the mamp item identified by rank and
    /// returns the selected data specified by `return_type`.
    pub fn map_get_by_rank(bin: &'a str,
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

    /// Create map get ranke range operation. Server selects `count` map items at the specified
    /// rank and returns the selected data specified by `return_type`.
    pub fn map_get_by_rank_range(bin: &'a str,
                                 rank: i64,
                                 count: i64,
                                 return_type: MapReturnType)
                                 -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByRankRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank), CdtArgument::Int(count)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }

    /// Create map get by rank range operation. Server selects the map items starting at the
    /// specified rank to the last ranked item and returns the selected data specified by
    /// `return_type`.
    pub fn map_get_by_rank_range_from(bin: &'a str,
                                      rank: i64,
                                      return_type: MapReturnType)
                                      -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::MapGetByRankRange,
            args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtMapOp(cdt_op),
        }
    }
}
