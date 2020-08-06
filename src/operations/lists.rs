// Copyright 2015-2018 Aerospike, Inc.
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

use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::CdtContext;
use crate::operations::OperationData::CdtListOp;
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::Value;

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy)]
pub enum CdtListOrderType {
    Unordered = 0,
    Ordered = 1,
}

#[derive(Debug, Clone, Copy)]
pub enum CdtListReturnType {
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

#[derive(Debug, Clone, Copy)]
pub enum CdtListSortFlags {
    // Default is the default sort flag for CDT lists, and sort in Ascending order.
    Default = 0,
    // Descending will sort the contents of the list in descending order.
    Descending = 1,
    // DropDuplicates will drop duplicate values in the results of the CDT list operation.
    DropDuplicates = 2,
}

#[derive(Debug, Clone, Copy)]
pub enum CdtListWriteFlags {
    // Default is the default behavior. It means:  Allow duplicate values and insertions at any index.
    Default = 0,
    // AddUnique means: Only add unique values.
    AddUnique = 1,
    // InsertBounded means: Enforce list boundaries when inserting.  Do not allow values to be inserted
    // at index outside current list boundaries.
    InsertBounded = 2,
    // NoFail means: do not raise error if a list item fails due to write flag constraints.
    NoFail = 4,
    // Partial means: allow other valid list items to be committed if a list item fails due to
    // write flag constraints.
    Partial = 8,
}

pub struct ListPolicy {
    attributes: CdtListOrderType,
    flags: u8,
}

impl ListPolicy {
    /// Create a new map policy given the ordering for the map and the write mode.
    pub const fn new(order: CdtListOrderType, write_mode: CdtListWriteFlags) -> Self {
        ListPolicy {
            attributes: order,
            flags: write_mode as u8,
        }
    }
}

impl Default for ListPolicy {
    fn default() -> Self {
        ListPolicy::new(CdtListOrderType::Unordered, CdtListWriteFlags::Default)
    }
}
pub fn list_order_flag(order: CdtListOrderType, pad: bool) -> u8 {
    if order as u8 == 1 {
        return 0xc0;
    }
    if pad {
        return 0x80;
    }
    0x40
}
pub fn create<'a>(
    bin: &'a str,
    list_order: CdtListOrderType,
    pad: bool,
    ctx: &'a [CdtContext<'a>],
) -> Operation<'a> {
    if ctx.len() == 0 {
        return set_order(bin, list_order, None);
    }
    let cdt_op = CdtOperation {
        op: CdtListOpType::SetType as u8,
        args: vec![
            CdtArgument::Byte(list_order_flag(list_order, pad)),
            CdtArgument::Byte(list_order as u8),
        ],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx: Some(ctx),
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

pub fn set_order<'a>(
    bin: &'a str,
    list_order: CdtListOrderType,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::SetType as u8,
        args: vec![CdtArgument::Byte(list_order as u8)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}
/// Create list append operation. Server appends value to the end of list bin. Server returns
/// list size.
pub fn append<'a>(
    bin: &'a str,
    value: &'a Value,
    policy: Option<ListPolicy>,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Value(value)];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.attributes as u8));
        args.push(CdtArgument::Byte(policy.flags as u8));
    }
    let cdt_op = CdtOperation {
        op: CdtListOpType::Append as u8,
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list append items operation. Server appends each input list item to the end of list
/// bin. Server returns list size.
pub fn append_items<'a>(
    bin: &'a str,
    values: &'a [Value],
    policy: Option<ListPolicy>,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    assert!(!values.is_empty());

    let mut args = vec![CdtArgument::List(values)];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.attributes as u8));
        args.push(CdtArgument::Byte(policy.flags as u8));
    }

    let cdt_op = CdtOperation {
        op: CdtListOpType::AppendItems as u8,
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list insert operation. Server inserts value to the specified index of the list bin.
/// Server returns list size.
pub fn insert<'a>(
    bin: &'a str,
    index: i64,
    value: &'a Value,
    policy: Option<ListPolicy>,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Int(index), CdtArgument::Value(value)];
    if policy.is_some() {
        args.push(CdtArgument::Byte(policy.unwrap().flags as u8));
    }
    let cdt_op = CdtOperation {
        op: CdtListOpType::Insert as u8,
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list insert items operation. Server inserts each input list item starting at the
/// specified index of the list bin. Server returns list size.
pub fn insert_items<'a>(
    bin: &'a str,
    index: i64,
    values: &'a [Value],
    policy: Option<ListPolicy>,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    assert!(!values.is_empty());
    let mut args = vec![CdtArgument::Int(index), CdtArgument::List(values)];
    if policy.is_some() {
        args.push(CdtArgument::Byte(policy.unwrap().flags as u8));
    }
    let cdt_op = CdtOperation {
        op: CdtListOpType::InsertItems as u8,
        args: vec![CdtArgument::Int(index), CdtArgument::List(values)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list pop operation. Server returns the item at the specified index and removes the
/// item from the list bin.
pub fn pop<'a>(bin: &'a str, index: i64, ctx: Option<&'a [CdtContext<'a>]>) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Pop as u8,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list pop range operation. Server returns `count` items starting at the specified
/// index and removes the items from the list bin.
pub fn pop_range<'a>(
    bin: &'a str,
    index: i64,
    count: i64,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::PopRange as u8,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list pop range operation. Server returns the items starting at the specified index
/// to the end of the list and removes those items from the list bin.
pub fn pop_range_from<'a>(
    bin: &'a str,
    index: i64,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::PopRange as u8,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list remove operation. Server removes the item at the specified index from the list
/// bin. Server returns the number of items removed.
pub fn remove<'a>(bin: &'a str, index: i64, ctx: Option<&'a [CdtContext<'a>]>) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Remove as u8,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list remove range operation. Server removes `count` items starting at the specified
/// index from the list bin. Server returns the number of items removed.
pub fn remove_range<'a>(
    bin: &'a str,
    index: i64,
    count: i64,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveRange as u8,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list remove range operation. Server removes the items starting at the specified
/// index to the end of the list. Server returns the number of items removed.
pub fn remove_range_from<'a>(
    bin: &'a str,
    index: i64,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveRange as u8,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list remove value operation. Server removes all items that are equal to the
/// specified value. Server returns the number of items removed.
pub fn remove_by_value<'a>(
    bin: &'a str,
    value: &'a Value,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValue as u8,
        args: vec![CdtArgument::Value(value)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list remove by value list operation. Server removes all items that are equal to
/// one of the specified values. Server returns the number of items removed
pub fn remove_by_value_list<'a>(
    bin: &'a str,
    values: &'a [Value],
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueList as u8,
        args: vec![CdtArgument::List(values)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

pub fn remove_by_value_range<'a>(
    bin: &'a str,
    begin: &'a Value,
    end: &'a Value,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::RemoveByValueInterval as u8,
        args: vec![CdtArgument::Value(begin), CdtArgument::Value(end)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}
/// Create list set operation. Server sets the item value at the specified index in the list
/// bin. Server does not return a result by default.
pub fn set<'a>(
    bin: &'a str,
    index: i64,
    value: &'a Value,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    assert!(!value.is_nil());

    let cdt_op = CdtOperation {
        op: CdtListOpType::Set as u8,
        args: vec![CdtArgument::Int(index), CdtArgument::Value(value)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list trim operation. Server removes `count` items in the list bin that do not fall
/// into the range specified by `index` and `count`. If the range is out of bounds, then all
/// items will be removed. Server returns list size after trim.
pub fn trim<'a>(
    bin: &'a str,
    index: i64,
    count: i64,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Trim as u8,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list clear operation. Server removes all items in the list bin. Server does not
/// return a result by default.
pub fn clear<'a>(bin: &'a str, ctx: Option<&'a [CdtContext<'a>]>) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Clear as u8,
        args: vec![],
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list increment operation. Server increments the item value at the specified index by the
/// given amount and returns the final result.
pub fn increment<'a>(
    bin: &'a str,
    index: i64,
    value: i64,
    policy: Option<ListPolicy>,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Int(index), CdtArgument::Int(value)];
    if policy.is_some() {
        args.push(CdtArgument::Byte(policy.unwrap().flags as u8));
    }
    let cdt_op = CdtOperation {
        op: CdtListOpType::Increment as u8,
        args,
    };
    Operation {
        op: OperationType::CdtWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list size operation. Server returns size of the list.
pub fn size<'a>(bin: &'a str, ctx: Option<&'a [CdtContext<'a>]>) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Size as u8,
        args: vec![],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list get operation. Server returns the item at the specified index in the list bin.
pub fn get<'a>(bin: &'a str, index: i64, ctx: Option<&'a [CdtContext<'a>]>) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::Get as u8,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list get range operation. Server returns `count` items starting at the specified
/// index in the list bin.
pub fn get_range<'a>(
    bin: &'a str,
    index: i64,
    count: i64,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetRange as u8,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}

/// Create list get range operation. Server returns items starting at the index to the end of
/// the list.
pub fn get_range_from<'a>(
    bin: &'a str,
    index: i64,
    ctx: Option<&'a [CdtContext<'a>]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtListOpType::GetRange as u8,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtRead,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
        header_only: false,
    }
}
