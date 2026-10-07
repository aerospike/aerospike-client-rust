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

use crate::expressions::{nil, ExpOp, ExpType, Expression, ExpressionArgument, MODIFY};
use crate::operations::cdt_context::{CdtContext, CtxType};
use crate::operations::lists::{CdtListOpType, ListPolicy, ListReturnType, ListSortFlags};
use crate::Value;

const MODULE: i64 = 0;
/// Creates expression that appends value to end of list.
#[must_use]
pub fn append(
    policy: ListPolicy,
    value: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Append as i64)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.attributes as u8)),
        ExpressionArgument::Value(Value::from(policy.flags.bits())),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that appends list items to end of list.
#[must_use]
pub fn append_items(
    policy: ListPolicy,
    list: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::AppendItems as i64)),
        ExpressionArgument::FilterExpression(list),
        ExpressionArgument::Value(Value::from(policy.attributes as u8)),
        ExpressionArgument::Value(Value::from(policy.flags.bits())),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that inserts value to specified index of list.
#[must_use]
pub fn insert(
    policy: ListPolicy,
    index: Expression,
    value: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Insert as i64)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.flags.bits())),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that inserts each input list item starting at specified index of list.
#[must_use]
pub fn insert_items(
    policy: ListPolicy,
    index: Expression,
    list: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::InsertItems as i64)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(list),
        ExpressionArgument::Value(Value::from(policy.flags.bits())),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that increments `list[index]` by value.
/// Value expression should resolve to a number.
#[must_use]
pub fn increment(
    policy: ListPolicy,
    index: Expression,
    value: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Increment as i64)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.attributes as u8)),
        ExpressionArgument::Value(Value::from(policy.flags.bits())),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that sets item value at specified index in list.
#[must_use]
pub fn set(
    policy: ListPolicy,
    index: Expression,
    value: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Set as i64)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.flags.bits())),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes all items in list.
#[must_use]
pub fn clear(bin: Expression, ctx: &[CdtContext]) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Clear as i64)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that sorts list according to sortFlags.
#[must_use]
pub fn sort(sort_flags: ListSortFlags, bin: Expression, ctx: &[CdtContext]) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Sort as i64)),
        ExpressionArgument::Value(Value::from(sort_flags.bits())),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes list items identified by value.
#[must_use]
pub fn remove_by_value(
    return_type: ListReturnType,
    value: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValue as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes list items identified by values.
#[must_use]
pub fn remove_by_value_list(
    return_type: ListReturnType,
    values: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValueList as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(values),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
///
/// If valueBegin is null, the range is less than valueEnd. If valueEnd is null, the range is
/// greater than equal to valueBegin.
#[must_use]
pub fn remove_by_value_range(
    return_type: ListReturnType,
    value_begin: Option<Expression>,
    value_end: Option<Expression>,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValueInterval as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
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

/// Creates expression that removes list items nearest to value and greater by relative rank.
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
#[must_use]
pub fn remove_by_value_relative_rank_range(
    return_type: ListReturnType,
    value: Expression,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValueRelRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes list items nearest to value and greater by relative rank with a count limit.
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
#[must_use]
pub fn remove_by_value_relative_rank_range_count(
    return_type: ListReturnType,
    value: Expression,
    rank: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByValueRelRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes list item identified by index.
#[must_use]
pub fn remove_by_index(
    return_type: ListReturnType,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByIndex as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes list items starting at specified index to the end of list.
#[must_use]
pub fn remove_by_index_range(
    return_type: ListReturnType,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByIndexRange as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes "count" list items starting at specified index.
#[must_use]
pub fn remove_by_index_range_count(
    return_type: ListReturnType,
    index: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByIndexRange as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes list item identified by rank.
#[must_use]
pub fn remove_by_rank(
    return_type: ListReturnType,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByRank as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes list items starting at specified rank to the last ranked item.
#[must_use]
pub fn remove_by_rank_range(
    return_type: ListReturnType,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes "count" list items starting at specified rank.
#[must_use]
pub fn remove_by_rank_range_count(
    return_type: ListReturnType,
    rank: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::RemoveByRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that returns list size.
///
/// ```
/// // List bin "a" size > 7
/// use aerospike::expressions::{gt, list_bin, int_val};
/// use aerospike::expressions::lists::size;
/// let _ = gt(size(list_bin("a".to_string()), &[]), int_val(7));
/// ```
#[must_use]
pub fn size(bin: Expression, ctx: &[CdtContext]) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::Size as i64)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, ExpType::Int, args)
}

/// Creates an expression that concatenates the string items of `bin`. The list
/// must hold only strings. Requires Aerospike Server version 8.2.0 or later.
///
/// ```
/// // The list bin "a" joins to "onetwothree"
/// use aerospike::expressions::{eq, list_bin, string_val};
/// use aerospike::expressions::lists::join;
/// let _ = eq(join(list_bin("a".to_string()), &[]), string_val("onetwothree".to_string()));
/// ```
#[must_use]
pub fn join(bin: Expression, ctx: &[CdtContext]) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::StringJoin as i64)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, ExpType::String, args)
}

/// Creates an expression that joins the string items of `bin` with a separator.
///
/// The inverse of
/// [`expressions::string::split_by_separator`](crate::expressions::string::split_by_separator).
/// Requires Aerospike Server version 8.2.0 or later.
///
/// ```
/// // The list bin "a" joins to "one|two|three"
/// use aerospike::expressions::{eq, list_bin, string_val};
/// use aerospike::expressions::lists::join_by_separator;
/// let _ = eq(
///   join_by_separator(string_val("|".to_string()), list_bin("a".to_string()), &[]),
///   string_val("one|two|three".to_string()));
/// ```
#[must_use]
pub fn join_by_separator(separator: Expression, bin: Expression, ctx: &[CdtContext]) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::StringJoin as i64)),
        ExpressionArgument::FilterExpression(separator),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, ExpType::String, args)
}

/// Creates expression that selects list items identified by value and returns selected
/// data specified by returnType.
///
/// ```
/// // List bin "a" contains at least one item == "abc"
/// use aerospike::expressions::{gt, string_val, list_bin, int_val};
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::lists::get_by_value;
/// let _ = gt(
///   get_by_value(ListReturnType::COUNT, string_val("abc".to_string()), list_bin("a".to_string()), &[]),
///   int_val(0));
/// ```
///
#[must_use]
pub fn get_by_value(
    return_type: ListReturnType,
    value: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValue as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects list items identified by value range and returns selected data
/// specified by returnType.
///
/// ```
/// // List bin "a" items >= 10 && items < 20
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::lists::get_by_value_range;
/// use aerospike::expressions::{int_val, list_bin};
///
/// let _ = get_by_value_range(ListReturnType::VALUES, Some(int_val(10)), Some(int_val(20)), list_bin("a".to_string()), &[]);
/// ```
#[must_use]
pub fn get_by_value_range(
    return_type: ListReturnType,
    value_begin: Option<Expression>,
    value_end: Option<Expression>,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValueInterval as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
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

/// Creates expression that selects list items identified by values and returns selected data
/// specified by returnType.
#[must_use]
pub fn get_by_value_list(
    return_type: ListReturnType,
    values: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValueList as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(values),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects list items nearest to value and greater by relative rank
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
#[must_use]
pub fn get_by_value_relative_rank_range(
    return_type: ListReturnType,
    value: Expression,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValueRelRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects list items nearest to value and greater by relative rank with a count limit
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
#[must_use]
pub fn get_by_value_relative_rank_range_count(
    return_type: ListReturnType,
    value: Expression,
    rank: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByValueRelRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects list item identified by index and returns
/// selected data specified by returnType.
///
/// ```
/// // a[3] == 5
/// use aerospike::expressions::{ExpType, eq, int_val, list_bin};
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::lists::get_by_index;
/// let _ = eq(
///   get_by_index(ListReturnType::VALUES, ExpType::Int, int_val(3), list_bin("a".to_string()), &[]),
///   int_val(5));
/// ```
///
#[must_use]
pub fn get_by_index(
    return_type: ListReturnType,
    value_type: ExpType,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByIndex as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Creates expression that selects list items starting at specified index to the end of list
/// and returns selected data specified by returnType .
#[must_use]
pub fn get_by_index_range(
    return_type: ListReturnType,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByIndexRange as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects "count" list items starting at specified index
/// and returns selected data specified by returnType.
#[must_use]
pub fn get_by_index_range_count(
    return_type: ListReturnType,
    index: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByIndexRange as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects list item identified by rank and returns selected
/// data specified by returnType.
///
/// ```
/// // Player with lowest score.
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::{ExpType, int_val, list_bin};
/// use aerospike::expressions::lists::get_by_rank;
/// let _ = get_by_rank(ListReturnType::VALUES, ExpType::String, int_val(0), list_bin("a".to_string()), &[]);
/// ```
#[must_use]
pub fn get_by_rank(
    return_type: ListReturnType,
    value_type: ExpType,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByRank as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Creates expression that selects list items starting at specified rank to the last ranked item
/// and returns selected data specified by returnType.
#[must_use]
pub fn get_by_rank_range(
    return_type: ListReturnType,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects "count" list items starting at specified rank and returns
/// selected data specified by returnType.
#[must_use]
pub fn get_by_rank_range_count(
    return_type: ListReturnType,
    rank: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtListOpType::GetByRankRange as i64)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

pub(crate) fn add_read(
    bin: Expression,
    return_type: ExpType,
    arguments: Vec<ExpressionArgument>,
) -> Expression {
    Expression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(bin)),
        flags: Some(MODULE),
        module: Some(return_type),
        exps: None,
        arguments: Some(arguments),
        bytes: None,
    }
}

pub(crate) fn add_write(
    bin: Expression,
    ctx: &[CdtContext],
    arguments: Vec<ExpressionArgument>,
) -> Expression {
    let return_type: ExpType;
    if ctx.is_empty() {
        return_type = ExpType::List;
    } else if (ctx[0].id & CtxType::ListIndex as u16) == 0 {
        return_type = ExpType::Map;
    } else {
        return_type = ExpType::List;
    }

    Expression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(bin)),
        flags: Some(MODULE | MODIFY),
        module: Some(return_type),
        exps: None,
        arguments: Some(arguments),
        bytes: None,
    }
}

pub(crate) fn get_value_type(return_type: i64) -> ExpType {
    let t = return_type & !ListReturnType::INVERTED_BIT;

    match t {
        t if t == ListReturnType::INDEX.bits()
            || t == ListReturnType::REVERSE_INDEX.bits()
            || t == ListReturnType::RANK.bits()
            || t == ListReturnType::REVERSE_RANK.bits() =>
        {
            ExpType::List
        }

        t if t == ListReturnType::COUNT.bits() => ExpType::Int,

        t if t == ListReturnType::VALUES.bits() => ExpType::List,

        t if t == ListReturnType::EXISTS.bits() => ExpType::Bool,

        _ => panic!("Invalid ListReturnType: {return_type}"),
    }
}
