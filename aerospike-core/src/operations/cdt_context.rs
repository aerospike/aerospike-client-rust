// Copyright 2015-2020 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Operation Context for nested Operations
use crate::operations::lists::{list_order_flag, ListOrderType};
use crate::operations::maps::map_order_flag;
use crate::operations::MapOrder;
use crate::Value;

#[doc(hidden)]
// Empty Context for scalar operations
pub const DEFAULT_CTX: &[CdtContext] = &[];

#[doc(hidden)]
pub enum CtxType {
    ListIndex = 0x10,
    ListRank = 0x11,
    ListValue = 0x13,
    MapIndex = 0x20,
    MapRank = 0x21,
    MapKey = 0x22,
    MapValue = 0x23,
}
/// `CdtContext` defines Nested CDT context. Identifies the location of nested list/map to apply the operation.
/// for the current level.
/// An array of CTX identifies location of the list/map on multiple
/// levels on nesting.
#[derive(Debug, Clone)]
pub struct CdtContext {
    /// Context Type
    pub id: u8,

    /// Flags
    pub flags: u8,

    /// Context Value
    pub value: Value,
}

/// Defines Lookup list by index offset.
/// If the index is negative, the resolved index starts backwards from end of list.
/// If an index is out of bounds, a parameter error will be returned.
/// Examples:
/// 0: First item.
/// 4: Fifth item.
/// -1: Last item.
/// -3: Third to last item.
pub const fn ctx_list_index(index: i64) -> CdtContext {
    CdtContext {
        id: CtxType::ListIndex as u8,
        flags: 0,
        value: Value::Int(index),
    }
}

/// list with given type at index offset, given an order and pad.
pub const fn ctx_list_index_create(index: i64, order: ListOrderType, pad: bool) -> CdtContext {
    CdtContext {
        id: CtxType::ListIndex as u8,
        flags: list_order_flag(order, pad),
        value: Value::Int(index),
    }
}

/// Defines Lookup list by rank.
/// 0 = smallest value
/// N = Nth smallest value
/// -1 = largest value
pub const fn ctx_list_rank(rank: i64) -> CdtContext {
    CdtContext {
        id: CtxType::ListRank as u8,
        flags: 0,
        value: Value::Int(rank),
    }
}

/// Defines Lookup list by value.
pub const fn ctx_list_value(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::ListValue as u8,
        flags: 0,
        value: key,
    }
}
/// Defines Lookup map by index offset.
/// If the index is negative, the resolved index starts backwards from end of list.
/// If an index is out of bounds, a parameter error will be returned.
/// Examples:
/// 0: First item.
/// 4: Fifth item.
/// -1: Last item.
/// -3: Third to last item.
pub const fn ctx_map_index(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::MapIndex as u8,
        flags: 0,
        value: key,
    }
}

/// Defines Lookup map by rank.
/// 0 = smallest value
/// N = Nth smallest value
/// -1 = largest value
pub const fn ctx_map_rank(rank: i64) -> CdtContext {
    CdtContext {
        id: CtxType::MapRank as u8,
        flags: 0,
        value: Value::Int(rank),
    }
}

/// Defines Lookup map by key.
pub const fn ctx_map_key(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::MapKey as u8,
        flags: 0,
        value: key,
    }
}

/// Create map with given type at map key.
pub const fn ctx_map_key_create(key: Value, order: MapOrder) -> CdtContext {
    CdtContext {
        id: CtxType::MapKey as u8,
        flags: map_order_flag(order),
        value: key,
    }
}

/// Defines Lookup map by value.
pub const fn ctx_map_value(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::MapValue as u8,
        flags: 0,
        value: key,
    }
}
