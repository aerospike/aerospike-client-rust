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

//! List bin operations. Create list operations used by the client's `operate()` method.
//!
//! List operations support negative indexing. If the index is negative, the resolved index starts
//! backwards from the end of the list.
//!
//! Index/Count examples:
//!
//! * Index 0: First item in list.
//! * Index 4: Fifth item in list.
//! * Index -1: Last item in list.
//! * Index -3: Third to last item in list.
//! * Index 1, Count 2: Second and third item in list.
//! * Index -3, Count 3: Last three items in list.
//! * Index -5, Count 4: Range between fifth to last item to second to last item inclusive.
//!
//! If an index is out of bounds, a parameter error will be returned. If a range is partially out of
//! bounds, the valid part of the range will be returned.

use crate::msgpack::encoder::pack_cdt_op;
use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::{CdtContext, DEFAULT_CTX};
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::Value;

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum CdtListOpType {
    SetType = 0,
    Append = 1,
    AppendItems = 2,
    Insert = 3,
    InsertItems = 4,
    Pop = 5,
    PopRange = 6,
    Remove = 7,
    RemoveRange = 8,
    Set = 9,
    Trim = 10,
    Clear = 11,
    Increment = 12,
    Sort = 13,
    Size = 16,
    Get = 17,
    GetRange = 18,
    GetByIndex = 19,
    GetByRank = 21,
    GetByValue = 22,
    GetByValueList = 23,
    GetByIndexRange = 24,
    GetByValueInterval = 25,
    GetByRankRange = 26,
    GetByValueRelRankRange = 27,
    RemoveByIndex = 32,
    RemoveByRank = 34,
    RemoveByValue = 35,
    RemoveByValueList = 36,
    RemoveByIndexRange = 37,
    RemoveByValueInterval = 38,
    RemoveByRankRange = 39,
    RemoveByValueRelRankRange = 40,
}

/// List storage order.
#[derive(Debug, Clone, Copy)]
pub enum ListOrderType {
    /// List is not ordered. This is the default.
    Unordered = 0,
    /// List is ordered.
    Ordered = 1,
}

/// `CdtListReturnType` determines the returned values in CDT List operations.
#[derive(Debug, Clone, Copy)]
pub enum ListReturnType {
    /// Do not return a result.
    None = 0,
    /// Return index offset order.
    /// 0 = first key
    /// N = Nth key
    /// -1 = last key
    Index = 1,
    /// Return reverse index offset order.
    /// 0 = last key
    /// -1 = first key
    ReverseIndex = 2,
    /// Return value order.
    /// 0 = smallest value
    /// N = Nth smallest value
    /// -1 = largest value
    Rank = 3,
    /// Return reserve value order.
    /// 0 = largest value
    /// N = Nth largest value
    /// -1 = smallest value
    ReverseRank = 4,
    /// Return count of items selected.
    Count = 5,
    /// Return value for single key read and value list for range read.
    Values = 7,
    /// Invert meaning of list command and return values.
    /// With the INVERTED flag enabled, the items outside of the specified index range will be returned.
    /// The meaning of the list command can also be inverted.
    /// With the INVERTED flag enabled, the items outside of the specified index range will be removed and returned.
    Inverted = 0x10000,
}

/// `CdtListSortFlags` determines sort flags for CDT lists
#[derive(Debug, Clone, Copy)]
pub enum ListSortFlags {
    /// Default is the default sort flag for CDT lists, and sort in Ascending order.
    Default = 0,
    /// Descending will sort the contents of the list in descending order.
    Descending = 1,
    /// DropDuplicates will drop duplicate values in the results of the CDT list operation.
    DropDuplicates = 2,
}

/// `CdtListWriteFlags` determines write flags for CDT lists
#[derive(Debug, Clone, Copy)]
pub enum ListWriteFlags {
    /// Default is the default behavior. It means:  Allow duplicate values and insertions at any index.
    Default = 0,
    /// AddUnique means: Only add unique values.
    AddUnique = 1,
    /// InsertBounded means: Enforce list boundaries when inserting.  Do not allow values to be inserted
    /// at index outside current list boundaries.
    InsertBounded = 2,
    /// NoFail means: do not raise error if a list item fails due to write flag constraints.
    NoFail = 4,
    /// Partial means: allow other valid list items to be committed if a list item fails due to
    /// write flag constraints.
    Partial = 8,
}

/// `ListPolicy` directives when creating a list and writing list items.
#[derive(Debug, Clone, Copy)]
pub struct ListPolicy {
    /// CdtListOrderType
    pub attributes: ListOrderType,
    /// CdtListWriteFlags
    pub flags: ListWriteFlags,
}

impl ListPolicy {
    /// Create unique key list with specified order when list does not exist.
    /// Use specified write mode when writing list items.
    pub const fn new(order: ListOrderType, write_flags: ListWriteFlags) -> Self {
        ListPolicy {
            attributes: order,
            flags: write_flags,
        }
    }
}

impl Default for ListPolicy {
    /// Returns the default policy for CDT list operations.
    fn default() -> Self {
        ListPolicy::new(ListOrderType::Unordered, ListWriteFlags::Default)
    }
}

#[doc(hidden)]
pub const fn list_order_flag(order: ListOrderType, pad: bool) -> u8 {
    if let ListOrderType::Ordered = order {
        return 0xc0;
    }
    if pad {
        return 0x80;
    }
    0x40
}

/// Creates list create operation.
/// Server creates list at given context level. The context is allowed to be beyond list
/// boundaries only if pad is set to true.  In that case, nil list entries will be inserted to
/// satisfy the context position.
pub fn create(bin: &str, list_order: ListOrderType, pad: bool) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::SetType as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(list_order_flag(list_order, pad)),
            CdtArgument::Byte(list_order as u8),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a set list order operation.
/// Server sets list order.  Server returns null.
pub fn set_order<'a>(
    bin: &'a str,
    list_order: ListOrderType,
    ctx: &'a [CdtContext],
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::SetType as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(list_order as u8)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}
/// Create list append operation. Server appends value to the end of list bin. Server returns
/// list size.
pub fn append<'a>(policy: &ListPolicy, bin: &'a str, value: &'a Value) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Append as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Value(value),
            CdtArgument::Byte(policy.attributes as u8),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list append items operation. Server appends each input list item to the end of list
/// bin. Server returns list size.
///
/// # Panics
/// Will panic if values is empty
pub fn append_items<'a>(policy: &ListPolicy, bin: &'a str, values: &'a [Value]) -> Operation<'a> {
    assert!(!values.is_empty());

    let cdt_op = CdtOperation {
        op: CdtListOpType::AppendItems as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::List(values),
            CdtArgument::Byte(policy.attributes as u8),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list insert operation. Server inserts value to the specified index of the list bin.
/// Server returns list size.
pub fn insert<'a>(
    policy: &ListPolicy,
    bin: &'a str,
    index: i64,
    value: &'a Value,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Insert as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(index),
            CdtArgument::Value(value),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list insert items operation. Server inserts each input list item starting at the
/// specified index of the list bin. Server returns list size.
///
/// # Panics
/// will panic if values is empty
pub fn insert_items<'a>(
    policy: &ListPolicy,
    bin: &'a str,
    index: i64,
    values: &'a [Value],
) -> Operation<'a> {
    assert!(!values.is_empty());

    let cdt_op = CdtOperation {
        op: CdtListOpType::InsertItems as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(index),
            CdtArgument::List(values),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list pop operation. Server returns the item at the specified index and removes the
/// item from the list bin.
pub fn pop(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Pop as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list pop range operation. Server returns `count` items starting at the specified
/// index and removes the items from the list bin.
pub fn pop_range(bin: &str, index: i64, count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::PopRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list pop range operation. Server returns the items starting at the specified index
/// to the end of the list and removes those items from the list bin.
pub fn pop_range_from(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::PopRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove operation. Server removes the item at the specified index from the list
/// bin. Server returns the number of items removed.
pub fn remove(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Remove as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove range operation. Server removes `count` items starting at the specified
/// index from the list bin. Server returns the number of items removed.
pub fn remove_range(bin: &str, index: i64, count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove range operation. Server removes the items starting at the specified
/// index to the end of the list. Server returns the number of items removed.
pub fn remove_range_from(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove value operation. Server removes all items that are equal to the
/// specified value. Server returns the number of items removed.
pub fn remove_by_value<'a>(
    bin: &'a str,
    value: &'a Value,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValue as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove by value list operation. Server removes all items that are equal to
/// one of the specified values. Server returns the number of items removed
pub fn remove_by_value_list<'a>(
    bin: &'a str,
    values: &'a [Value],
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueList as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
/// If valueBegin is nil, the range is less than valueEnd.
/// If valueEnd is nil, the range is greater than equal to valueBegin.
/// Server returns removed data specified by returnType
pub fn remove_by_value_range<'a>(
    bin: &'a str,
    return_type: ListReturnType,
    begin: &'a Value,
    end: &'a Value,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueInterval as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(begin),
            CdtArgument::Value(end),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove by value relative to rank range operation.
/// Server removes list items nearest to value and greater by relative rank.
/// Server returns removed data specified by returnType.
///
/// Examples for ordered list \[0, 4, 5, 9, 11, 15\]:
/// ```text
/// (value,rank) = [removed items]
/// (5,0) = [5,9,11,15]
/// (5,1) = [9,11,15]
/// (5,-1) = [4,5,9,11,15]
/// (3,0) = [4,5,9,11,15]
/// (3,3) = [11,15]
/// (3,-3) = [0,4,5,9,11,15]
/// ```
pub fn remove_by_value_relative_rank_range<'a>(
    bin: &'a str,
    return_type: ListReturnType,
    value: &'a Value,
    rank: i64,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueRelRankRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove by value relative to rank range operation.
/// Server removes list items nearest to value and greater by relative rank with a count limit.
/// Server returns removed data specified by returnType.
///
/// Examples for ordered list \[0, 4, 5, 9, 11, 15\]:
/// ```text
/// (value,rank,count) = [removed items]
/// (5,0,2) = [5,9]
/// (5,1,1) = [9]
/// (5,-1,2) = [4,5]
/// (3,0,1) = [4]
/// (3,3,7) = [11,15]
/// (3,-3,2) = []
/// ```
pub fn remove_by_value_relative_rank_range_count<'a>(
    bin: &'a str,
    return_type: ListReturnType,
    value: &'a Value,
    rank: i64,
    count: i64,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueRelRankRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list item identified by index and returns removed data specified by returnType.
pub fn remove_by_index(bin: &str, index: i64, return_type: ListReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByIndex as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list items starting at specified index to the end of list and returns removed
/// data specified by returnType.
pub fn remove_by_index_range(bin: &str, index: i64, return_type: ListReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByIndexRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes "count" list items starting at specified index and returns removed data specified by returnType.
pub fn remove_by_index_range_count(
    bin: &str,
    index: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByIndexRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list item identified by rank and returns removed data specified by returnType.
pub fn remove_by_rank(bin: &str, rank: i64, return_type: ListReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByRank as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes list items starting at specified rank to the last ranked item and returns removed
/// data specified by returnType.
pub fn remove_by_rank_range(bin: &str, rank: i64, return_type: ListReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list remove operation.
/// Server removes "count" list items starting at specified rank and returns removed data specified by returnType.
pub fn remove_by_rank_range_count(
    bin: &str,
    rank: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByRankRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list set operation. Server sets the item value at the specified index in the list
/// bin. Server does not return a result by default.
///
/// # Panics
/// Panics if value is empty
pub fn set<'a>(bin: &'a str, index: i64, value: &'a Value) -> Operation<'a> {
    assert!(!value.is_nil());

    let cdt_op = CdtOperation {
        op: CdtListOpType::Set as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index), CdtArgument::Value(value)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list trim operation. Server removes `count` items in the list bin that do not fall
/// into the range specified by `index` and `count`. If the range is out of bounds, then all
/// items will be removed. Server returns list size after trim.
pub fn trim(bin: &str, index: i64, count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Trim as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list clear operation. Server removes all items in the list bin. Server does not
/// return a result by default.
pub fn clear(bin: &str) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Clear as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list increment operation. Server increments the item value at the specified index by the
/// given amount and returns the final result.
pub fn increment<'a>(policy: &ListPolicy, bin: &'a str, index: i64, value: i64) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Increment as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Int(index),
            CdtArgument::Int(value),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list size operation. Server returns size of the list.
pub fn size(bin: &str) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Size as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list get operation. Server returns the item at the specified index in the list bin.
pub fn get(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Get as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list get range operation. Server returns `count` items starting at the specified
/// index in the list bin.
pub fn get_range(bin: &str, index: i64, count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list get range operation. Server returns items starting at the index to the end of
/// the list.
pub fn get_range_from(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by value operation.
/// Server selects list items identified by value and returns selected data specified by returnType.
pub fn get_by_value<'a>(
    bin: &'a str,
    value: &'a Value,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValue as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list get by value list operation.
/// Server selects list items identified by values and returns selected data specified by returnType.
pub fn get_by_value_list<'a>(
    bin: &'a str,
    values: &'a [Value],
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValueList as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by value range operation.
/// Server selects list items identified by value range (valueBegin inclusive, valueEnd exclusive)
/// If valueBegin is null, the range is less than valueEnd.
/// If valueEnd is null, the range is greater than equal to valueBegin.
/// Server returns selected data specified by returnType.
pub fn get_by_value_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValueInterval as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![
            CdtArgument::Byte(return_type as u8),
            CdtArgument::Value(begin),
            CdtArgument::Value(end),
        ],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list get by index operation.
/// Server selects list item identified by index and returns selected data specified by returnType
pub fn get_by_index(bin: &str, index: i64, return_type: ListReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByIndex as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list get by index range operation.
/// Server selects list items starting at specified index to the end of list and returns selected
/// data specified by returnType.
pub fn get_by_index_range(bin: &str, index: i64, return_type: ListReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByIndexRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list get by index range operation.
/// Server selects "count" list items starting at specified index and returns selected data specified
/// by returnType.
pub fn get_by_index_range_count(
    bin: &str,
    index: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByIndexRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by rank operation.
/// Server selects list item identified by rank and returns selected data specified by returnType.
pub fn get_by_rank(bin: &str, rank: i64, return_type: ListReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByRank as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by rank range operation.
/// Server selects list items starting at specified rank to the last ranked item and returns selected
/// data specified by returnType.
pub fn get_by_rank_range(bin: &str, rank: i64, return_type: ListReturnType) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByRankRange as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(return_type as u8), CdtArgument::Int(rank)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by rank range operation.
/// Server selects "count" list items starting at specified rank and returns selected data specified by returnType.
pub fn get_by_rank_range_count(
    bin: &str,
    rank: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByRankRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by value relative to rank range operation.
/// Server selects list items nearest to value and greater by relative rank.
/// Server returns selected data specified by returnType.
///
/// Examples for ordered list \[0, 4, 5, 9, 11, 15\]:
/// ```text
/// (value,rank) = [selected items]
/// (5,0) = [5,9,11,15]
/// (5,1) = [9,11,15]
/// (5,-1) = [4,5,9,11,15]
/// (3,0) = [4,5,9,11,15]
/// (3,3) = [11,15]
/// (3,-3) = [0,4,5,9,11,15]
/// ```
pub fn get_by_value_relative_rank_range<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValueRelRankRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates a list get by value relative to rank range operation.
/// Server selects list items nearest to value and greater by relative rank with a count limit.
/// Server returns selected data specified by returnType.
///
/// Examples for ordered list \[0, 4, 5, 9, 11, 15\]:
/// ```text
/// (value,rank,count) = [selected items]
/// (5,0,2) = [5,9]
/// (5,1,1) = [9]
/// (5,-1,2) = [4,5]
/// (3,0,1) = [4]
/// (3,3,7) = [11,15]
/// (3,-3,2) = []
/// ```
pub fn get_by_value_relative_rank_range_count<'a>(
    bin: &'a str,
    value: &'a Value,
    rank: i64,
    count: i64,
    return_type: ListReturnType,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetByValueRelRankRange as u8,
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
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Creates list sort operation.
/// Server sorts list according to sortFlags.
/// Server does not return a result by default.
pub fn sort(bin: &str, sort_flags: ListSortFlags) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Sort as u8,
        encoder: Box::new(pack_cdt_op),
        args: vec![CdtArgument::Byte(sort_flags as u8)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}
