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

//! Map Cdt Aerospike Filter Expressions.
use crate::expressions::{nil, ExpOp, ExpType, Expression, ExpressionArgument, MODIFY};
use crate::operations::cdt_context::{CdtContext, CtxType};
use crate::operations::maps::{map_write_op, CdtMapOpType};
use crate::{MapPolicy, MapReturnType, Value};

pub(crate) const MODULE: i64 = 0;

/// Creates expression that writes key/value item to map bin.
#[allow(clippy::trivially_copy_pass_by_ref)]
#[must_use]
pub fn put(
    policy: &MapPolicy,
    key: Expression,
    value: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let op = map_write_op(policy, false);
    let args: Vec<ExpressionArgument> = if op as u8 == CdtMapOpType::Replace as u8 {
        vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(op as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::FilterExpression(value),
        ]
    } else {
        vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(op as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.order as u8)),
        ]
    };
    add_write(bin, ctx, args)
}

/// Creates expression that writes each map item to map bin.
#[allow(clippy::trivially_copy_pass_by_ref)]
#[must_use]
pub fn put_items(
    policy: &MapPolicy,
    map: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let op = map_write_op(policy, true);
    let args: Vec<ExpressionArgument> = if op as u8 == CdtMapOpType::Replace as u8 {
        vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(op as u8)),
            ExpressionArgument::FilterExpression(map),
        ]
    } else {
        vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(op as u8)),
            ExpressionArgument::FilterExpression(map),
            ExpressionArgument::Value(Value::from(policy.order as u8)),
        ]
    };
    add_write(bin, ctx, args)
}

/// Creates expression that increments values by incr for all items identified by key.
/// Valid only for numbers.
#[allow(clippy::trivially_copy_pass_by_ref)]
#[must_use]
pub fn increment(
    policy: &MapPolicy,
    key: Expression,
    incr: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::Increment as u8)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(incr),
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(policy.order as u8)),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes all items in map.
#[must_use]
pub fn clear(bin: Expression, ctx: &[CdtContext]) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::Clear as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map item identified by key.
#[must_use]
pub fn remove_by_key(
    return_type: MapReturnType,
    key: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByKey as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items identified by keys.
#[must_use]
pub fn remove_by_key_list(
    return_type: MapReturnType,
    keys: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveKeyList as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(keys),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items identified by key range (keyBegin inclusive, keyEnd exclusive).
///
/// If keyBegin is null, the range is less than keyEnd.
/// If keyEnd is null, the range is greater than equal to keyBegin.
#[must_use]
pub fn remove_by_key_range(
    return_type: MapReturnType,
    key_begin: Option<Expression>,
    key_end: Option<Expression>,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByKeyInterval as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
    ];
    if let Some(val_beg) = key_begin {
        args.push(ExpressionArgument::FilterExpression(val_beg));
    } else {
        args.push(ExpressionArgument::FilterExpression(nil()));
    }
    if let Some(val_end) = key_end {
        args.push(ExpressionArgument::FilterExpression(val_end));
    }
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items nearest to key and greater by index.
///
/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
///
/// * (value,index) = [removed items]
/// * (5,0) = [{5=15},{9=10}]
/// * (5,1) = [{9=10}]
/// * (5,-1) = [{4=2},{5=15},{9=10}]
/// * (3,2) = [{9=10}]
/// * (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
#[must_use]
pub fn remove_by_key_relative_index_range(
    return_type: MapReturnType,
    key: Expression,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByKeyRelIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items nearest to key and greater by index with a count limit.
///
/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (value,index,count) = [removed items]
/// * (5,0,1) = [{5=15}]
/// * (5,1,2) = [{9=10}]
/// * (5,-1,1) = [{4=2}]
/// * (3,2,1) = [{9=10}]
/// * (3,-2,2) = [{0=17}]
#[must_use]
pub fn remove_by_key_relative_index_range_count(
    return_type: MapReturnType,
    key: Expression,
    index: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByKeyRelIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items identified by value.
#[must_use]
pub fn remove_by_value(
    return_type: MapReturnType,
    value: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByValue as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items identified by values.
#[must_use]
pub fn remove_by_value_list(
    return_type: MapReturnType,
    values: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveValueList as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(values),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items identified by value range (valueBegin inclusive, valueEnd exclusive).
///
/// If valueBegin is null, the range is less than valueEnd.
/// If valueEnd is null, the range is greater than equal to valueBegin.
#[must_use]
pub fn remove_by_value_range(
    return_type: MapReturnType,
    value_begin: Option<Expression>,
    value_end: Option<Expression>,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByValueInterval as u8)),
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

/// Creates expression that removes map items nearest to value and greater by relative rank.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// * (value,rank) = [removed items]
/// * (11,1) = [{0=17}]
/// * (11,-1) = [{9=10},{5=15},{0=17}]
#[must_use]
pub fn remove_by_value_relative_rank_range(
    return_type: MapReturnType,
    value: Expression,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByValueRelRankRange as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items nearest to value and greater by relative rank with a count limit.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// * (value,rank,count) = [removed items]
/// * (11,1,1) = [{0=17}]
/// * (11,-1,1) = [{9=10}]
#[must_use]
pub fn remove_by_value_relative_rank_range_count(
    return_type: MapReturnType,
    value: Expression,
    rank: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByValueRelRankRange as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map item identified by index.
#[must_use]
pub fn remove_by_index(
    return_type: MapReturnType,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByIndex as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items starting at specified index to the end of map.
#[must_use]
pub fn remove_by_index_range(
    return_type: MapReturnType,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes "count" map items starting at specified index.
#[must_use]
pub fn remove_by_index_range_count(
    return_type: MapReturnType,
    index: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map item identified by rank.
#[must_use]
pub fn remove_by_rank(
    return_type: MapReturnType,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByRank as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes map items starting at specified rank to the last ranked item.
#[must_use]
pub fn remove_by_rank_range(
    return_type: MapReturnType,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByRankRange as u8)),
        ExpressionArgument::Value(Value::from(return_type.bits())),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Creates expression that removes "count" map items starting at specified rank.
#[must_use]
pub fn remove_by_rank_range_count(
    return_type: MapReturnType,
    rank: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByRankRange as u8)),
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
/// // Map bin "a" size > 7
/// use aerospike::expressions::{gt, map_bin, int_val};
/// use aerospike::expressions::maps::size;
///
/// let _ = gt(size(map_bin("a".to_string()), &[]), int_val(7));
///
/// ```
#[must_use]
pub fn size(bin: Expression, ctx: &[CdtContext]) -> Expression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::Size as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, ExpType::Int, args)
}

/// Creates expression that selects map item identified by key and returns selected data
/// specified by returnType.
///
/// ```
/// // Map bin "a" contains key "B"
/// use aerospike::expressions::{ExpType, gt, string_val, map_bin, int_val};
/// use aerospike::MapReturnType;
/// use aerospike::expressions::maps::get_by_key;
///
/// let _ = gt(get_by_key(MapReturnType::COUNT, ExpType::Int, string_val("B".to_string()), map_bin("a".to_string()), &[]), int_val(0));
/// ```
///
#[must_use]
pub fn get_by_key(
    return_type: MapReturnType,
    value_type: ExpType,
    key: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKey as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Creates expression that selects map items identified by key range (keyBegin inclusive, keyEnd exclusive).
///
/// If keyBegin is null, the range is less than keyEnd.
/// If keyEnd is null, the range is greater than equal to keyBegin.
/// Expression returns selected data specified by returnType.
#[must_use]
pub fn get_by_key_range(
    return_type: MapReturnType,
    key_begin: Option<Expression>,
    key_end: Option<Expression>,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKeyInterval as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
    ];
    if let Some(val_beg) = key_begin {
        args.push(ExpressionArgument::FilterExpression(val_beg));
    } else {
        args.push(ExpressionArgument::FilterExpression(nil()));
    }
    if let Some(val_end) = key_end {
        args.push(ExpressionArgument::FilterExpression(val_end));
    }
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects map items identified by keys and returns selected data specified by returnType
#[must_use]
pub fn get_by_key_list(
    return_type: MapReturnType,
    keys: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKeyList as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(keys),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects map items nearest to key and greater by index.
/// Expression returns selected data specified by returnType.
///
/// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
///
/// * (value,index) = [selected items]
/// * (5,0) = [{5=15},{9=10}]
/// * (5,1) = [{9=10}]
/// * (5,-1) = [{4=2},{5=15},{9=10}]
/// * (3,2) = [{9=10}]
/// * (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
#[must_use]
pub fn get_by_key_relative_index_range(
    return_type: MapReturnType,
    key: Expression,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKeyRelIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects map items nearest to key and greater by index with a count limit.
/// Expression returns selected data specified by returnType.
///
/// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
///
/// * (value,index,count) = [selected items]
/// * (5,0,1) = [{5=15}]
/// * (5,1,2) = [{9=10}]
/// * (5,-1,1) = [{4=2}]
/// * (3,2,1) = [{9=10}]
/// * (3,-2,2) = [{0=17}]
#[must_use]
pub fn get_by_key_relative_index_range_count(
    return_type: MapReturnType,
    key: Expression,
    index: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKeyRelIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects map items identified by value and returns selected data
/// specified by returnType.
///
/// ```
/// // Map bin "a" contains value "BBB"
/// use aerospike::expressions::{gt, string_val, map_bin, int_val};
/// use aerospike::MapReturnType;
/// use aerospike::expressions::maps::get_by_value;
///
/// let _ = gt(get_by_value(MapReturnType::COUNT, string_val("BBB".to_string()), map_bin("a".to_string()), &[]), int_val(0));
/// ```
#[must_use]
pub fn get_by_value(
    return_type: MapReturnType,
    value: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValue as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects map items identified by value range (valueBegin inclusive, valueEnd exclusive).
///
/// If valueBegin is null, the range is less than valueEnd.
/// If valueEnd is null, the range is greater than equal to valueBegin.
///
/// Expression returns selected data specified by returnType.
#[must_use]
pub fn get_by_value_range(
    return_type: MapReturnType,
    value_begin: Option<Expression>,
    value_end: Option<Expression>,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValueInterval as u8)),
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

/// Creates expression that selects map items identified by values and returns selected data specified by returnType.
#[must_use]
pub fn get_by_value_list(
    return_type: MapReturnType,
    values: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValueList as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(values),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects map items nearest to value and greater by relative rank.
/// Expression returns selected data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// * (value,rank) = [selected items]
/// * (11,1) = [{0=17}]
/// * (11,-1) = [{9=10},{5=15},{0=17}]
#[must_use]
pub fn get_by_value_relative_rank_range(
    return_type: MapReturnType,
    value: Expression,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValueRelRankRange as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects map items nearest to value and greater by relative rank with a count limit.
/// Expression returns selected data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// * (value,rank,count) = [selected items]
/// * (11,1,1) = [{0=17}]
/// * (11,-1,1) = [{9=10}]
#[must_use]
pub fn get_by_value_relative_rank_range_count(
    return_type: MapReturnType,
    value: Expression,
    rank: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValueRelRankRange as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects map item identified by index and returns selected data specified by returnType.
#[must_use]
pub fn get_by_index(
    return_type: MapReturnType,
    value_type: ExpType,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByIndex as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Creates expression that selects map items starting at specified index to the end of map and returns selected
/// data specified by returnType.
#[must_use]
pub fn get_by_index_range(
    return_type: MapReturnType,
    index: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects "count" map items starting at specified index and returns selected data
/// specified by returnType.
#[must_use]
pub fn get_by_index_range_count(
    return_type: MapReturnType,
    index: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects map item identified by rank and returns selected data specified by returnType.
#[must_use]
pub fn get_by_rank(
    return_type: MapReturnType,
    value_type: ExpType,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByRank as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Creates expression that selects map items starting at specified rank to the last ranked item and
/// returns selected data specified by returnType.
#[must_use]
pub fn get_by_rank_range(
    return_type: MapReturnType,
    rank: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByRankRange as u8)),
        ExpressionArgument::Value(Value::from(return_type)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Creates expression that selects "count" map items starting at specified rank and returns selected
/// data specified by returnType.
#[must_use]
pub fn get_by_rank_range_count(
    return_type: MapReturnType,
    rank: Expression,
    count: Expression,
    bin: Expression,
    ctx: &[CdtContext],
) -> Expression {
    let return_type = return_type.bits();
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByRankRange as u8)),
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
    let return_type = if ctx.is_empty() || (ctx[0].id & CtxType::ListIndex as u16) == 0 {
        ExpType::Map
    } else {
        ExpType::List
    };

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
    let t = return_type & !MapReturnType::INVERTED_BIT;

    match t {
        t if t == MapReturnType::INDEX.bits()
            || t == MapReturnType::REVERSE_INDEX.bits()
            || t == MapReturnType::RANK.bits()
            || t == MapReturnType::REVERSE_RANK.bits() =>
        {
            ExpType::List
        }

        t if t == MapReturnType::COUNT.bits() => ExpType::Int,

        t if t == MapReturnType::KEY.bits() || t == MapReturnType::VALUE.bits() => ExpType::List,

        t if t == MapReturnType::KEY_VALUE.bits()
            || t == MapReturnType::ORDERED_MAP.bits()
            || t == MapReturnType::UNORDERED_MAP.bits() =>
        {
            ExpType::Map
        }

        t if t == MapReturnType::EXISTS.bits() => ExpType::Bool,

        _ => panic!("Invalid MapReturnType: {return_type}"),
    }
}
