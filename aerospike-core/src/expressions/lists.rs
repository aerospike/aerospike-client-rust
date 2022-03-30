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

//! List Cdt Aerospike Filter Expressions.

use crate::expressions::{nil, ExpOp, ExpType, ExpressionArgument, FilterExpression, MODIFY};
use crate::operations::cdt_context::{CdtContext, CtxType};
use crate::operations::lists::{CdtListOpType, ListPolicy, ListReturnType, ListSortFlags};
use crate::Value;

const MODULE: i64 = 0;
/// Create expression that appends value to end of list.
pub fn append(
    policy: ListPolicy,
    value: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Append as i64)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.attributes as u8)),
        ExpressionArgument::Value(Value::from(policy.flags as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that appends list items to end of list.
pub fn append_items(
    policy: ListPolicy,
    list: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::AppendItems as i64)),
        ExpressionArgument::FilterExpression(list),
        ExpressionArgument::Value(Value::from(policy.attributes as u8)),
        ExpressionArgument::Value(Value::from(policy.flags as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that inserts value to specified index of list.
pub fn insert(
    policy: ListPolicy,
    index: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Insert as i64)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.flags as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that inserts each input list item starting at specified index of list.
pub fn insert_items(
    policy: ListPolicy,
    index: FilterExpression,
    list: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::InsertItems as i64)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(list),
        ExpressionArgument::Value(Value::from(policy.flags as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that increments `list[index]` by value.
/// Value expression should resolve to a number.
pub fn increment(
    policy: ListPolicy,
    index: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Increment as i64)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.attributes as u8)),
        ExpressionArgument::Value(Value::from(policy.flags as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that sets item value at specified index in list.
pub fn set(
    policy: ListPolicy,
    index: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Set as i64)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.flags as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes all items in list.
pub fn clear(bin: FilterExpression, ctx: &[CdtContext]) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Clear as i64)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that sorts list according to sortFlags.
pub fn sort(
    sort_flags: ListSortFlags,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Sort as i64)),
        ExpressionArgument::Value(Value::from(sort_flags as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes list items identified by value.
pub fn remove_by_value(
    value: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValue as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes list items identified by values.
pub fn remove_by_value_list(
    values: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValueList as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(values),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
/// If valueBegin is null, the range is less than valueEnd. If valueEnd is null, the range is
/// greater than equal to valueBegin.
pub fn remove_by_value_range(
    value_begin: Option<FilterExpression>,
    value_end: Option<FilterExpression>,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValueInterval as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
    ];
    if let Some(val_beg) = value_begin {
        args.push(ExpressionArgument::FilterExpression(val_beg));
    } else {
        args.push(ExpressionArgument::FilterExpression(nil()));
    }
    if let Some(val_end) = value_end {
        args.push(ExpressionArgument::FilterExpression(val_end));
    }
    add_write(bin, ctx, args)
}

/// Create expression that removes list items nearest to value and greater by relative rank.
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
pub fn remove_by_value_relative_rank_range(
    value: FilterExpression,
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValueRelRankRange as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes list items nearest to value and greater by relative rank with a count limit.
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
pub fn remove_by_value_relative_rank_range_count(
    value: FilterExpression,
    rank: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValueRelRankRange as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes list item identified by index.
pub fn remove_by_index(
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByIndex as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes list items starting at specified index to the end of list.
pub fn remove_by_index_range(
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByIndexRange as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes "count" list items starting at specified index.
pub fn remove_by_index_range_count(
    index: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByIndexRange as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes list item identified by rank.
pub fn remove_by_rank(
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByRank as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes list items starting at specified rank to the last ranked item.
pub fn remove_by_rank_range(
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByRankRange as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes "count" list items starting at specified rank.
pub fn remove_by_rank_range_count(
    rank: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByRankRange as i64)),
        ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that returns list size.
///
/// ```
/// // List bin "a" size > 7
/// use aerospike::expressions::{gt, list_bin, int_val};
/// use aerospike::expressions::lists::size;
/// gt(size(list_bin("a".to_string()), &[]), int_val(7));
/// ```
pub fn size(bin: FilterExpression, ctx: &[CdtContext]) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Size as i64)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, ExpType::INT, args)
}

/// Create expression that selects list items identified by value and returns selected
/// data specified by returnType.
///
/// ```
/// // List bin "a" contains at least one item == "abc"
/// use aerospike::expressions::{gt, string_val, list_bin, int_val};
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::lists::get_by_value;
/// gt(
///   get_by_value(ListReturnType::Count, string_val("abc".to_string()), list_bin("a".to_string()), &[]),
///   int_val(0));
/// ```
///
pub fn get_by_value(
    return_type: ListReturnType,
    value: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValue as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects list items identified by value range and returns selected data
/// specified by returnType.
///
/// ```
/// // List bin "a" items >= 10 && items < 20
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::lists::get_by_value_range;
/// use aerospike::expressions::{int_val, list_bin};
///
/// get_by_value_range(ListReturnType::Values, Some(int_val(10)), Some(int_val(20)), list_bin("a".to_string()), &[]);
/// ```
pub fn get_by_value_range(
    return_type: ListReturnType,
    value_begin: Option<FilterExpression>,
    value_end: Option<FilterExpression>,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValueInterval as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
    ];
    if let Some(val_beg) = value_begin {
        args.push(ExpressionArgument::FilterExpression(val_beg));
    } else {
        args.push(ExpressionArgument::FilterExpression(nil()));
    }
    if let Some(val_end) = value_end {
        args.push(ExpressionArgument::FilterExpression(val_end));
    }
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects list items identified by values and returns selected data
/// specified by returnType.
pub fn get_by_value_list(
    return_type: ListReturnType,
    values: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValueList as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(values),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects list items nearest to value and greater by relative rank
/// and returns selected data specified by returnType.
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
pub fn get_by_value_relative_rank_range(
    return_type: ListReturnType,
    value: FilterExpression,
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValueRelRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects list items nearest to value and greater by relative rank with a count limit
/// and returns selected data specified by returnType.
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
pub fn get_by_value_relative_rank_range_count(
    return_type: ListReturnType,
    value: FilterExpression,
    rank: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValueRelRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects list item identified by index and returns
/// selected data specified by returnType.
///
/// ```
/// // a[3] == 5
/// use aerospike::expressions::{ExpType, eq, int_val, list_bin};
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::lists::get_by_index;
/// eq(
///   get_by_index(ListReturnType::Values, ExpType::INT, int_val(3), list_bin("a".to_string()), &[]),
///   int_val(5));
/// ```
///
pub fn get_by_index(
    return_type: ListReturnType,
    value_type: ExpType,
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByIndex as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Create expression that selects list items starting at specified index to the end of list
/// and returns selected data specified by returnType .
pub fn get_by_index_range(
    return_type: ListReturnType,
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByIndexRange as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects "count" list items starting at specified index
/// and returns selected data specified by returnType.
pub fn get_by_index_range_count(
    return_type: ListReturnType,
    index: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByIndexRange as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects list item identified by rank and returns selected
/// data specified by returnType.
///
/// ```
/// // Player with lowest score.
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::{ExpType, int_val, list_bin};
/// use aerospike::expressions::lists::get_by_rank;
/// get_by_rank(ListReturnType::Values, ExpType::STRING, int_val(0), list_bin("a".to_string()), &[]);
/// ```
pub fn get_by_rank(
    return_type: ListReturnType,
    value_type: ExpType,
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByRank as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Create expression that selects list items starting at specified rank to the last ranked item
/// and returns selected data specified by returnType.
pub fn get_by_rank_range(
    return_type: ListReturnType,
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects "count" list items starting at specified rank and returns
/// selected data specified by returnType.
pub fn get_by_rank_range_count(
    return_type: ListReturnType,
    rank: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

#[doc(hidden)]
fn add_read(
    bin: FilterExpression,
    return_type: ExpType,
    arguments: Vec<ExpressionArgument>,
) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(bin)),
        flags: Some(MODULE),
        module: Some(return_type),
        exps: None,
        arguments: Some(arguments),
    }
}

#[doc(hidden)]
fn add_write(
    bin: FilterExpression,
    ctx: &[CdtContext],
    arguments: Vec<ExpressionArgument>,
) -> FilterExpression {
    let return_type: ExpType;
    if ctx.is_empty() {
        return_type = ExpType::LIST;
    } else if (ctx[0].id & CtxType::ListIndex as u8) == 0 {
        return_type = ExpType::MAP;
    } else {
        return_type = ExpType::LIST;
    }

    FilterExpression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(bin)),
        flags: Some(MODULE | MODIFY),
        module: Some(return_type),
        exps: None,
        arguments: Some(arguments),
    }
}

#[doc(hidden)]
const fn get_value_type(return_type: ListReturnType) -> ExpType {
    if (return_type as u8 & !(ListReturnType::Inverted as u8)) == ListReturnType::Values as u8 {
        ExpType::LIST
    } else {
        ExpType::INT
    }
}
