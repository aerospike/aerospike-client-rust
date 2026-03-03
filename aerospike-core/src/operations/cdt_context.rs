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
use crate::expressions::{self, Expression};
use crate::operations::lists::{list_order_flag, ListOrderType};
use crate::operations::MapOrder;
use crate::Value;

// Empty Context for scalar operations
pub(crate) const DEFAULT_CTX: Vec<CdtContext> = vec![];

pub(crate) enum CtxType {
    Expression = 0x04,
    ListIndex = 0x10,
    ListRank = 0x11,
    ListValue = 0x13,
    MapIndex = 0x20,
    MapRank = 0x21,
    MapKey = 0x22,
    MapValue = 0x23,
}

/// `CdtContext` defines Nested CDT context. Identifies the location of nested list/map to apply the operation.
///
/// for the current level.
/// An array of CTX identifies location of the list/map on multiple
/// levels on nesting.
#[derive(Debug, Clone, PartialEq)]
pub struct CdtContext {
    /// Context Type
    pub id: u8,

    /// Flags
    pub flags: u8,

    /// Context Value
    pub value: Value,

    /// Pre-packed expression bytes for expression-based contexts
    pub(crate) expression: Option<Expression>,
}

/// Defines Lookup list by index offset.
///
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
        expression: None,
    }
}

/// list with given type at index offset, given an order and pad.
pub const fn ctx_list_index_create(index: i64, order: ListOrderType, pad: bool) -> CdtContext {
    CdtContext {
        id: CtxType::ListIndex as u8,
        flags: list_order_flag(order, pad),
        value: Value::Int(index),
        expression: None,
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
        expression: None,
    }
}

/// Defines Lookup list by value.
pub const fn ctx_list_value(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::ListValue as u8,
        flags: 0,
        value: key,
        expression: None,
    }
}
/// Defines Lookup map by index offset.
///
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
        expression: None,
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
        expression: None,
    }
}

/// Defines Lookup map by key.
pub const fn ctx_map_key(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::MapKey as u8,
        flags: 0,
        value: key,
        expression: None,
    }
}

/// Create map with given type at map key.
pub const fn ctx_map_key_create(key: Value, order: MapOrder) -> CdtContext {
    CdtContext {
        id: CtxType::MapKey as u8,
        flags: order.flag(),
        value: key,
        expression: None,
    }
}

/// Defines Lookup map by value.
pub const fn ctx_map_value(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::MapValue as u8,
        flags: 0,
        value: key,
        expression: None,
    }
}

/// Creates a `CdtContext` for all children of the current collection that match the given
/// filter expression. Requires Aerospike Server version >= 8.1.1.
///
/// # Errors
///
/// Returns an error if the expression cannot be packed.
pub fn ctx_all_children_with_filter(exp: Expression) -> CdtContext {
    CdtContext {
        id: CtxType::Expression as u8,
        flags: 0,
        value: Value::Nil,
        expression: Some(exp),
    }
}

/// Selects all children (elements/entries) of the current collection context.
/// Requires Aerospike Server version >= 8.1.1.
pub fn ctx_all_children() -> CdtContext {
    CdtContext {
        id: CtxType::Expression as u8,
        flags: 0,
        value: Value::Nil,
        expression: Some(expressions::bool_val(true)),
    }
}
