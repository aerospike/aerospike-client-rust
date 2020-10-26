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

use crate::exp::{ExpOp, ExpType, Expression, ExpressionArgument, FilterExpression, MODIFY};
use crate::operations::cdt_context::{CdtContext, CtxType};
use crate::operations::lists::{ListPolicy, ListReturnType, ListSortFlags};
use crate::Value;

/// List expression generator.
///
/// The bin expression argument in these methods can be a reference to a bin or the
/// result of another expression. Expressions that modify bin values are only used
/// for temporary expression evaluation and are not permanently applied to the bin.
///
/// List modify expressions return the bin's value. This value will be a list except
/// when the list is nested within a map. In that case, a map is returned for the
/// list modify expression.
///
/// List expressions support negative indexing. If the index is negative, the
/// resolved index starts backwards from end of list. If an index is out of bounds,
/// a parameter error will be returned. If a range is partially out of bounds, the
/// valid part of the range will be returned. Index/Range examples:
///
/// * Index 0: First item in list.
/// * Index 4: Fifth item in list.
/// * Index -1: Last item in list.
/// * Index -3: Third to last item in list.
/// * Index 1 Count 2: Second and third items in list.
/// * Index -3 Count 3: Last three items in list.
/// * Index -5 Count 4: Range between fifth to last item to second to last item inclusive.
///
/// Nested expressions are supported by optional CTX context arguments.
pub struct ListExpression {}

const MODULE: i64 = 0;

#[doc(hidden)]
pub enum ListExpOp {
    Append = 1,
    AppendItems = 2,
    Insert = 3,
    InsertItems = 4,
    Set = 9,
    Clear = 11,
    Increment = 12,
    Sort = 13,
    Size = 16,
    GetByIndex = 19,
    GetByRank = 21,
    GetByValue = 22, // GET_ALL_BY_VALUE on server.
    GetByValueList = 23,
    GetByIndexRange = 24,
    GetByValueRange = 25,
    GetByRankRange = 26,
    GetByValueRelRankRange = 27,
    RemoveByIndex = 32,
    RemoveByRank = 34,
    RemoveByValue = 35,
    RemoveByValueList = 36,
    RemoveByIndexRange = 37,
    RemoveByValueRange = 38,
    RemoveByRankRange = 39,
    RemoveByValueRelRankRange = 40,
}
impl ListExpression {
    /// Create expression that appends value to end of list.
    pub fn append(
        policy: ListPolicy,
        value: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::Append as i64)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.attributes as u8)),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that appends list items to end of list.
    pub fn append_items(
        policy: ListPolicy,
        list: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::AppendItems as i64)),
            ExpressionArgument::FilterExpression(list),
            ExpressionArgument::Value(Value::from(policy.attributes as u8)),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::Insert as i64)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::InsertItems as i64)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(list),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::Increment as i64)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.attributes as u8)),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::Set as i64)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes all items in list.
    pub fn clear(bin: FilterExpression, ctx: &[CdtContext]) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::Clear as i64)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that sorts list according to sortFlags.
    pub fn sort(
        sort_flags: ListSortFlags,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::Sort as i64)),
            ExpressionArgument::Value(Value::from(sort_flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes list items identified by value.
    pub fn remove_by_value(
        value: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValue as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes list items identified by values.
    pub fn remove_by_value_list(
        values: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValueList as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(values),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValueRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ];
        if let Some(val_beg) = value_begin {
            args.push(ExpressionArgument::FilterExpression(val_beg));
        } else {
            args.push(ExpressionArgument::FilterExpression(Expression::nil()));
        }
        if let Some(val_end) = value_end {
            args.push(ExpressionArgument::FilterExpression(val_end));
        }
        ListExpression::add_write(bin, ctx, args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValueRelRankRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValueRelRankRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes list item identified by index.
    pub fn remove_by_index(
        index: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByIndex as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes list items starting at specified index to the end of list.
    pub fn remove_by_index_range(
        index: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByIndexRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes "count" list items starting at specified index.
    pub fn remove_by_index_range_count(
        index: FilterExpression,
        count: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByIndexRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes list item identified by rank.
    pub fn remove_by_rank(
        rank: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByRank as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes list items starting at specified rank to the last ranked item.
    pub fn remove_by_rank_range(
        rank: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByRankRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes "count" list items starting at specified rank.
    pub fn remove_by_rank_range_count(
        rank: FilterExpression,
        count: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByRankRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    /// Create expression that returns list size.
    ///
    /// ```
    /// // List bin "a" size > 7
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::ListExpression;
    /// Expression::gt(ListExpression::size(Expression::list_bin("a".to_string()), &[]), Expression::int_val(7));
    /// ```
    pub fn size(bin: FilterExpression, ctx: &[CdtContext]) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::Size as i64)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ExpType::INT, args)
    }

    /// Create expression that selects list items identified by value and returns selected
    /// data specified by returnType.
    ///
    /// ```
    /// // List bin "a" contains at least one item == "abc"
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::ListExpression;
    /// use aerospike::operations::lists::ListReturnType;
    /// Expression::gt(
    ///   ListExpression::get_by_value(ListReturnType::Count, Expression::string_val("abc".to_string()), Expression::list_bin("a".to_string()), &[]),
    ///   Expression::int_val(0));
    /// ```
    ///
    pub fn get_by_value(
        return_type: ListReturnType,
        value: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValue as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects list items identified by value range and returns selected data
    /// specified by returnType.
    ///
    /// ```
    /// // List bin "a" items >= 10 && items < 20
    /// use aerospike::exp::ListExpression;
    /// use aerospike::operations::lists::ListReturnType;
    /// use aerospike::exp::Expression;
    /// ListExpression::get_by_value_range(ListReturnType::Values, Some(Expression::int_val(10)), Some(Expression::int_val(20)), Expression::list_bin("a".to_string()), &[]);
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
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValueRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
        ];
        if let Some(val_beg) = value_begin {
            args.push(ExpressionArgument::FilterExpression(val_beg));
        } else {
            args.push(ExpressionArgument::FilterExpression(Expression::nil()));
        }
        if let Some(val_end) = value_end {
            args.push(ExpressionArgument::FilterExpression(val_end));
        }
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValueList as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(values),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValueRelRankRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValueRelRankRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects list item identified by index and returns
    /// selected data specified by returnType.
    ///
    /// ```
    /// // a[3] == 5
    /// use aerospike::exp::{Expression, ExpType};
    /// use aerospike::exp::ListExpression;
    /// use aerospike::operations::lists::ListReturnType;
    /// Expression::eq(
    ///   ListExpression::get_by_index(ListReturnType::Values, ExpType::INT, Expression::int_val(3), Expression::list_bin("a".to_string()), &[]),
    ///   Expression::int_val(5));
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
            ExpressionArgument::Value(Value::from(ListExpOp::GetByIndex as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, value_type, args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::GetByIndexRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::GetByIndexRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects list item identified by rank and returns selected
    /// data specified by returnType.
    ///
    /// ```
    /// // Player with lowest score.
    /// use aerospike::exp::ListExpression;
    /// use aerospike::operations::lists::ListReturnType;
    /// use aerospike::exp::{ExpType, Expression};
    /// ListExpression::get_by_rank(ListReturnType::Values, ExpType::STRING, Expression::int_val(0), Expression::list_bin("a".to_string()), &[]);
    /// ```
    pub fn get_by_rank(
        return_type: ListReturnType,
        value_type: ExpType,
        rank: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByRank as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, value_type, args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::GetByRankRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(ListExpOp::GetByRankRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
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
            return_type = ExpType::LIST
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
    fn get_value_type(return_type: ListReturnType) -> ExpType {
        if (return_type as u8 & !(ListReturnType::Inverted as u8)) == ListReturnType::Values as u8 {
            ExpType::LIST
        } else {
            ExpType::INT
        }
    }
}
