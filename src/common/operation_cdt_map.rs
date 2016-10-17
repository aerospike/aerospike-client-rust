// Copyright 2015-2016 Aerospike, Inc.
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

use value::Value;
use common::Bin;
use common::operation;
use common::operation::*;
use error::AerospikeResult;

const CDT_MAP_SET_TYPE: u8 = 64;
const CDT_MAP_ADD: u8 = 65;
const CDT_MAP_ADD_ITEMS: u8 = 66;
const CDT_MAP_PUT: u8 = 67;
const CDT_MAP_PUT_ITEMS: u8 = 68;
const CDT_MAP_REPLACE: u8 = 69;
const CDT_MAP_REPLACE_ITEMS: u8 = 70;
const CDT_MAP_INCREMENT: u8 = 73;
const CDT_MAP_DECREMENT: u8 = 74;
const CDT_MAP_CLEAR: u8 = 75;
const CDT_MAP_REMOVE_BY_KEY: u8 = 76;
const CDT_MAP_REMOVE_BY_INDEX: u8 = 77;
const CDT_MAP_REMOVE_BY_RANK: u8 = 79;
const CDT_MAP_REMOVE_KEY_LIST: u8 = 81;
const CDT_MAP_REMOVE_BY_VALUE: u8 = 82;
const CDT_MAP_REMOVE_VALUE_LIST: u8 = 83;
const CDT_MAP_REMOVE_BY_KEY_INTERVAL: u8 = 84;
const CDT_MAP_REMOVE_BY_INDEX_RANGE: u8 = 85;
const CDT_MAP_REMOVE_BY_VALUE_INTERVAL: u8 = 86;
const CDT_MAP_REMOVE_BY_RANK_RANGE: u8 = 87;
const CDT_MAP_SIZE: u8 = 96;
const CDT_MAP_GET_BY_KEY: u8 = 97;
const CDT_MAP_GET_BY_INDEX: u8 = 98;
const CDT_MAP_GET_BY_RANK: u8 = 100;
const CDT_MAP_GET_BY_VALUE: u8 = 102;
const CDT_MAP_GET_BY_KEY_INTERVAL: u8 = 103;
const CDT_MAP_GET_BY_INDEX_RANGE: u8 = 104;
const CDT_MAP_GET_BY_VALUE_INTERVAL: u8 = 105;
const CDT_MAP_GET_BY_RANK_RANGE: u8 = 106;

// Map storage order.
#[derive(Clone)]
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

impl MapWriteMode {
    fn mode_conf(&self) -> (u8, u8) {
        match self {
            &MapWriteMode::Update => (CDT_MAP_PUT, CDT_MAP_PUT_ITEMS),
            &MapWriteMode::UpdateOnly => (CDT_MAP_REPLACE, CDT_MAP_REPLACE_ITEMS),
            &MapWriteMode::CreateOnly => (CDT_MAP_ADD, CDT_MAP_ADD_ITEMS),
        }
    }
}

// MapPolicy directives when creating a map and writing map items.
pub struct MapPolicy {
    order: MapOrder,
    write_mode_item: u8,
    write_mode_items: u8,
}

impl MapPolicy {
    pub fn new(order: MapOrder, write_mode: MapWriteMode) -> AerospikeResult<Self> {
        let (item, items) = write_mode.mode_conf();
        Ok(MapPolicy {
            order: order,
            write_mode_item: item,
            write_mode_items: items,
        })
    }
}

impl Default for MapPolicy {
    fn default() -> Self {
        MapPolicy::new(MapOrder::Unordered, MapWriteMode::Update).unwrap()
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
    pub fn map_set_order(bin_name: &'a str, map_order: MapOrder) -> Self {
        Operation {
            op: operation::CDT_MAP_MODIFY,
            cdt_op: Some(CDT_MAP_SET_TYPE),
            cdt_args: Some(vec![Value::from(map_order as u8)]),
            cdt_list_values: None,
            cdt_map_entry: None,
            cdt_map_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn map_put_item(policy: &'a MapPolicy,
                        bin_name: &'a str,
                        key: &'a Value,
                        val: &'a Value)
                        -> Self {
        match policy.write_mode_item {
            CDT_MAP_REPLACE => {
                Operation {
                    op: operation::CDT_MAP_MODIFY,
                    cdt_op: Some(policy.write_mode_item),
                    cdt_args: None,
                    cdt_list_values: None,
                    cdt_map_entry: Some((key, val)),
                    cdt_map_values: None,
                    bin_name: bin_name,
                    bin_value: operation::NIL_VALUE,
                    header_only: false,
                }
            }
            _ => {
                Operation {
                    op: operation::CDT_MAP_MODIFY,
                    cdt_op: Some(policy.write_mode_item),
                    cdt_args: Some(vec![Value::from(policy.order.clone() as u8)]),
                    cdt_list_values: None,
                    cdt_map_entry: Some((key, val)),
                    cdt_map_values: None,
                    bin_name: bin_name,
                    bin_value: operation::NIL_VALUE,
                    header_only: false,
                }
            }
        }
    }

    pub fn map_put_items(policy: &'a MapPolicy,
                         bin_name: &'a str,
                         items: &'a HashMap<Value, Value>)
                         -> Self {
        match policy.write_mode_items {
            CDT_MAP_REPLACE_ITEMS => {
                Operation {
                    op: operation::CDT_MAP_MODIFY,
                    cdt_op: Some(policy.write_mode_items),
                    cdt_args: None,
                    cdt_list_values: None,
                    cdt_map_entry: None,
                    cdt_map_values: Some(items),
                    bin_name: bin_name,
                    bin_value: operation::NIL_VALUE,
                    header_only: false,
                }
            }
            _ => {
                Operation {
                    op: operation::CDT_MAP_MODIFY,
                    cdt_op: Some(policy.write_mode_items),
                    cdt_args: Some(vec![Value::from(policy.order.clone() as u8)]),
                    cdt_list_values: None,
                    cdt_map_entry: None,
                    cdt_map_values: Some(items),
                    bin_name: bin_name,
                    bin_value: operation::NIL_VALUE,
                    header_only: false,
                }
            }
        }
    }

    pub fn map_increment_value(policy: &'a MapPolicy,
                               bin_name: &'a str,
                               key: &'a Value,
                               incr: &'a Value)
                               -> Self {
        Operation {
            op: operation::CDT_MAP_MODIFY,
            cdt_op: Some(CDT_MAP_INCREMENT),
            cdt_args: Some(vec![Value::from(policy.order.clone() as u8)]),
            cdt_list_values: None,
            cdt_map_entry: Some((key, incr)),
            cdt_map_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn map_decrement_value(policy: &'a MapPolicy,
                               bin_name: &'a str,
                               key: &'a Value,
                               decr: &'a Value)
                               -> Self {
        Operation {
            op: operation::CDT_MAP_MODIFY,
            cdt_op: Some(CDT_MAP_DECREMENT),
            cdt_args: Some(vec![Value::from(policy.order.clone() as u8)]),
            cdt_list_values: None,
            cdt_map_entry: Some((key, decr)),
            cdt_map_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn map_clear(policy: &'a MapPolicy,
                     bin_name: &'a str,
                     key: &'a Value,
                     incr: &'a Value)
                     -> Self {
        Operation {
            op: operation::CDT_MAP_MODIFY,
            cdt_op: Some(CDT_MAP_CLEAR),
            cdt_args: None,
            cdt_list_values: None,
            cdt_map_entry: None,
            cdt_map_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn map_remove_by_key(policy: &'a MapPolicy,
                             bin_name: &'a str,
                             key: &'a Value,
                             return_type: MapReturnType)
                             -> Self {
        Operation {
            op: operation::CDT_MAP_MODIFY,
            cdt_op: Some(CDT_MAP_REMOVE_BY_KEY),
            cdt_args: Some(vec![Value::from(return_type as u8)]),
            cdt_list_values: None,
            cdt_map_entry: Some((key, operation::NIL_VALUE)),
            cdt_map_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn map_remove_by_keys(policy: &'a MapPolicy,
                              bin_name: &'a str,
                              keys: &'a [Value],
                              return_type: MapReturnType)
                              -> Self {
        Operation {
            op: operation::CDT_MAP_MODIFY,
            cdt_op: Some(CDT_MAP_REMOVE_KEY_LIST),
            cdt_args: Some(vec![Value::from(return_type as u8)]),
            cdt_list_values: Some(keys),
            cdt_map_entry: None,
            cdt_map_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn map_remove_by_key_range(policy: &'a MapPolicy,
                                   bin_name: &'a str,
                                   begin: &'a Value,
                                   end: &'a Value,
                                   return_type: MapReturnType)
                                   -> Self {
        if *end == Value::Nil {
            Operation {
                op: operation::CDT_MAP_MODIFY,
                cdt_op: Some(CDT_MAP_REMOVE_BY_KEY_INTERVAL),
                cdt_args: Some(vec![Value::from(return_type as u8)]),
                cdt_list_values: None,
                cdt_map_entry: Some((begin, operation::NIL_VALUE)),
                cdt_map_values: None,
                bin_name: bin_name,
                bin_value: operation::NIL_VALUE,
                header_only: false,
            }
        } else {
            Operation {
                op: operation::CDT_MAP_MODIFY,
                cdt_op: Some(CDT_MAP_REMOVE_BY_KEY_INTERVAL),
                cdt_args: Some(vec![Value::from(return_type as u8)]),
                cdt_list_values: None,
                cdt_map_entry: Some((begin, end)),
                cdt_map_values: None,
                bin_name: bin_name,
                bin_value: operation::NIL_VALUE,
                header_only: false,
            }
        }
    }

    pub fn map_remove_by_value(policy: &'a MapPolicy,
                               bin_name: &'a str,
                               value: &'a Value,
                               return_type: MapReturnType)
                               -> Self {
        Operation {
            op: operation::CDT_MAP_MODIFY,
            cdt_op: Some(CDT_MAP_REMOVE_BY_VALUE),
            cdt_args: Some(vec![Value::from(return_type as u8)]),
            cdt_list_values: None,
            cdt_map_entry: Some((value, operation::NIL_VALUE)),
            cdt_map_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn map_remove_by_values(policy: &'a MapPolicy,
                                bin_name: &'a str,
                                values: &'a [Value],
                                return_type: MapReturnType)
                                -> Self {
        Operation {
            op: operation::CDT_MAP_MODIFY,
            cdt_op: Some(CDT_MAP_REMOVE_VALUE_LIST),
            cdt_args: Some(vec![Value::from(return_type as u8)]),
            cdt_list_values: Some(values),
            cdt_map_entry: None,
            cdt_map_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn map_remove_by_value_range(policy: &'a MapPolicy,
                                     bin_name: &'a str,
                                     begin: &'a Value,
                                     end: &'a Value,
                                     return_type: MapReturnType)
                                     -> Self {
        if *end == Value::Nil {
            Operation {
                op: operation::CDT_MAP_MODIFY,
                cdt_op: Some(CDT_MAP_REMOVE_BY_VALUE_INTERVAL),
                cdt_args: Some(vec![Value::from(return_type as u8)]),
                cdt_list_values: None,
                cdt_map_entry: Some((begin, operation::NIL_VALUE)),
                cdt_map_values: None,
                bin_name: bin_name,
                bin_value: operation::NIL_VALUE,
                header_only: false,
            }
        } else {
            Operation {
                op: operation::CDT_MAP_MODIFY,
                cdt_op: Some(CDT_MAP_REMOVE_BY_VALUE_INTERVAL),
                cdt_args: Some(vec![Value::from(return_type as u8)]),
                cdt_list_values: None,
                cdt_map_entry: Some((begin, end)),
                cdt_map_values: None,
                bin_name: bin_name,
                bin_value: operation::NIL_VALUE,
                header_only: false,
            }
        }
    }
}
