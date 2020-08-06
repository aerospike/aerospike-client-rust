use crate::operations::lists::{list_order_flag, CdtListOrderType};
use crate::operations::MapOrder;
use crate::Value;
use std::borrow::Borrow;

/// CdtContext defines Nested CDT context. Identifies the location of nested list/map to apply the operation.
/// for the current level.
/// An array of CTX identifies location of the list/map on multiple
/// levels on nesting.
#[derive(Debug)]
pub struct CdtContext {
    id: u8,
    value: Value,
}

/// Defines Lookup list by index offset.
/// If the index is negative, the resolved index starts backwards from end of list.
/// If an index is out of bounds, a parameter error will be returned.
/// Examples:
/// 0: First item.
/// 4: Fifth item.
/// -1: Last item.
/// -3: Third to last item.
pub fn ctx_list_index<'a>(index: i64) -> CdtContext {
    CdtContext {
        id: 0x10,
        value: Value::Int(index),
    }
}

/// list with given type at index offset, given an order and pad.
pub fn ctx_list_index_create<'a>(index: i64, order: CdtListOrderType, pad: bool) -> CdtContext {
    CdtContext {
        id: 0x10 | list_order_flag(order, pad),
        value: Value::Int(index),
    }
}

/// Defines Lookup list by rank.
/// 0 = smallest value
/// N = Nth smallest value
/// -1 = largest value
pub fn ctx_list_rank<'a>(rank: i64) -> CdtContext {
    CdtContext {
        id: 0x11,
        value: Value::Int(rank),
    }
}

pub fn ctx_list_value(key: Value) -> CdtContext {
    CdtContext {
        id: 0x13,
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
pub fn ctx_map_index(key: Value) -> CdtContext {
    CdtContext {
        id: 0x20,
        value: key,
    }
}

/// Defines Lookup map by rank.
/// 0 = smallest value
/// N = Nth smallest value
/// -1 = largest value
pub fn ctx_map_rank<'a>(rank: i64) -> CdtContext {
    CdtContext {
        id: 0x21,
        value: Value::Int(rank),
    }
}

/// Defines Lookup map by key.
pub fn ctx_map_key(key: Value) -> CdtContext {
    CdtContext {
        id: 0x22,
        value: key,
    }
}

/// Create map with given type at map key.
pub fn ctx_map_key_create(key: Value, order: MapOrder) -> CdtContext {
    CdtContext {
        id: 0x22 | order as u8,
        value: key,
    }
}

/// CtxMapValue defines Lookup map by value.
pub fn ctx_map_value(key: Value) -> CdtContext {
    CdtContext {
        id: 0x23,
        value: key,
    }
}
