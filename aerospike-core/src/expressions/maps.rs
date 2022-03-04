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
use crate::expressions::{nil, ExpOp, ExpType, ExpressionArgument, FilterExpression, MODIFY};
use crate::operations::cdt_context::{CdtContext, CtxType};
use crate::operations::maps::{map_write_op, CdtMapOpType};
use crate::{MapPolicy, MapReturnType, Value};

#[doc(hidden)]
const MODULE: i64 = 0;

/// Create expression that writes key/value item to map bin.
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn put(
    policy: &MapPolicy,
    key: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args: Vec<ExpressionArgument>;
    let op = map_write_op(policy, false);
    if op as u8 == CdtMapOpType::Replace as u8 {
        args = vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(op as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::FilterExpression(value),
        ];
    } else {
        args = vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(op as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.order as u8)),
        ];
    }
    add_write(bin, ctx, args)
}

/// Create expression that writes each map item to map bin.
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn put_items(
    policy: &MapPolicy,
    map: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args: Vec<ExpressionArgument>;
    let op = map_write_op(policy, true);
    if op as u8 == CdtMapOpType::Replace as u8 {
        args = vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(op as u8)),
            ExpressionArgument::FilterExpression(map),
        ];
    } else {
        args = vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(op as u8)),
            ExpressionArgument::FilterExpression(map),
            ExpressionArgument::Value(Value::from(policy.order as u8)),
        ];
    }
    add_write(bin, ctx, args)
}

/// Create expression that increments values by incr for all items identified by key.
/// Valid only for numbers.
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn increment(
    policy: &MapPolicy,
    key: FilterExpression,
    incr: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::Increment as u8)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(incr),
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(policy.order as u8)),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes all items in map.
pub fn clear(bin: FilterExpression, ctx: &[CdtContext]) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::Clear as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map item identified by key.
pub fn remove_by_key(
    key: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByKey as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map items identified by keys.
pub fn remove_by_key_list(
    keys: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveKeyList as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(keys),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map items identified by key range (keyBegin inclusive, keyEnd exclusive).
/// If keyBegin is null, the range is less than keyEnd.
/// If keyEnd is null, the range is greater than equal to keyBegin.
pub fn remove_by_key_range(
    key_begin: Option<FilterExpression>,
    key_end: Option<FilterExpression>,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByKeyInterval as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
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

/// Create expression that removes map items nearest to key and greater by index.
///
/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
///
/// * (value,index) = [removed items]
/// * (5,0) = [{5=15},{9=10}]
/// * (5,1) = [{9=10}]
/// * (5,-1) = [{4=2},{5=15},{9=10}]
/// * (3,2) = [{9=10}]
/// * (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
pub fn remove_by_key_relative_index_range(
    key: FilterExpression,
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByKeyRelIndexRange as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map items nearest to key and greater by index with a count limit.
///
/// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
///
/// (value,index,count) = [removed items]
/// * (5,0,1) = [{5=15}]
/// * (5,1,2) = [{9=10}]
/// * (5,-1,1) = [{4=2}]
/// * (3,2,1) = [{9=10}]
/// * (3,-2,2) = [{0=17}]
pub fn remove_by_key_relative_index_range_count(
    key: FilterExpression,
    index: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByKeyRelIndexRange as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map items identified by value.
pub fn remove_by_value(
    value: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByValue as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map items identified by values.
pub fn remove_by_value_list(
    values: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveValueList as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(values),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map items identified by value range (valueBegin inclusive, valueEnd exclusive).
/// If valueBegin is null, the range is less than valueEnd.
/// If valueEnd is null, the range is greater than equal to valueBegin.
pub fn remove_by_value_range(
    value_begin: Option<FilterExpression>,
    value_end: Option<FilterExpression>,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByValueInterval as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
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

/// Create expression that removes map items nearest to value and greater by relative rank.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// * (value,rank) = [removed items]
/// * (11,1) = [{0=17}]
/// * (11,-1) = [{9=10},{5=15},{0=17}]
pub fn remove_by_value_relative_rank_range(
    value: FilterExpression,
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByValueRelRankRange as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map items nearest to value and greater by relative rank with a count limit.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// * (value,rank,count) = [removed items]
/// * (11,1,1) = [{0=17}]
/// * (11,-1,1) = [{9=10}]
pub fn remove_by_value_relative_rank_range_count(
    value: FilterExpression,
    rank: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByValueRelRankRange as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map item identified by index.
pub fn remove_by_index(
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByIndex as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map items starting at specified index to the end of map.
pub fn remove_by_index_range(
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByIndexRange as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes "count" map items starting at specified index.
pub fn remove_by_index_range_count(
    index: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByIndexRange as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map item identified by rank.
pub fn remove_by_rank(
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByRank as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes map items starting at specified rank to the last ranked item.
pub fn remove_by_rank_range(
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByRankRange as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that removes "count" map items starting at specified rank.
pub fn remove_by_rank_range_count(
    rank: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::RemoveByRankRange as u8)),
        ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_write(bin, ctx, args)
}

/// Create expression that returns list size.
///
/// ```
/// // Map bin "a" size > 7
/// use aerospike::expressions::{gt, map_bin, int_val};
/// use aerospike::expressions::maps::size;
///
/// gt(size(map_bin("a".to_string()), &[]), int_val(7));
///
/// ```
pub fn size(bin: FilterExpression, ctx: &[CdtContext]) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::Size as u8)),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, ExpType::INT, args)
}

/// Create expression that selects map item identified by key and returns selected data
/// specified by returnType.
///
/// ```
/// // Map bin "a" contains key "B"
/// use aerospike::expressions::{ExpType, gt, string_val, map_bin, int_val};
/// use aerospike::MapReturnType;
/// use aerospike::expressions::maps::get_by_key;
///
/// gt(get_by_key(MapReturnType::Count, ExpType::INT, string_val("B".to_string()), map_bin("a".to_string()), &[]), int_val(0));
/// ```
///
pub fn get_by_key(
    return_type: MapReturnType,
    value_type: ExpType,
    key: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKey as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Create expression that selects map items identified by key range (keyBegin inclusive, keyEnd exclusive).
/// If keyBegin is null, the range is less than keyEnd.
/// If keyEnd is null, the range is greater than equal to keyBegin.
/// Expression returns selected data specified by returnType.
pub fn get_by_key_range(
    return_type: MapReturnType,
    key_begin: Option<FilterExpression>,
    key_end: Option<FilterExpression>,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKeyInterval as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
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

/// Create expression that selects map items identified by keys and returns selected data specified by returnType
pub fn get_by_key_list(
    return_type: MapReturnType,
    keys: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKeyList as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(keys),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects map items nearest to key and greater by index.
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
pub fn get_by_key_relative_index_range(
    return_type: MapReturnType,
    key: FilterExpression,
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKeyRelIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects map items nearest to key and greater by index with a count limit.
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
pub fn get_by_key_relative_index_range_count(
    return_type: MapReturnType,
    key: FilterExpression,
    index: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByKeyRelIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(key),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects map items identified by value and returns selected data
/// specified by returnType.
///
/// ```
/// // Map bin "a" contains value "BBB"
/// use aerospike::expressions::{gt, string_val, map_bin, int_val};
/// use aerospike::MapReturnType;
/// use aerospike::expressions::maps::get_by_value;
///
/// gt(get_by_value(MapReturnType::Count, string_val("BBB".to_string()), map_bin("a".to_string()), &[]), int_val(0));
/// ```
pub fn get_by_value(
    return_type: MapReturnType,
    value: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValue as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects map items identified by value range (valueBegin inclusive, valueEnd exclusive)
/// If valueBegin is null, the range is less than valueEnd.
/// If valueEnd is null, the range is greater than equal to valueBegin.
///
/// Expression returns selected data specified by returnType.
pub fn get_by_value_range(
    return_type: MapReturnType,
    value_begin: Option<FilterExpression>,
    value_end: Option<FilterExpression>,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let mut args = vec![
        ExpressionArgument::Context(ctx.to_vec()),
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValueInterval as u8)),
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

/// Create expression that selects map items identified by values and returns selected data specified by returnType.
pub fn get_by_value_list(
    return_type: MapReturnType,
    values: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValueList as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(values),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects map items nearest to value and greater by relative rank.
/// Expression returns selected data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// * (value,rank) = [selected items]
/// * (11,1) = [{0=17}]
/// * (11,-1) = [{9=10},{5=15},{0=17}]
pub fn get_by_value_relative_rank_range(
    return_type: MapReturnType,
    value: FilterExpression,
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValueRelRankRange as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects map items nearest to value and greater by relative rank with a count limit.
/// Expression returns selected data specified by returnType.
///
/// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
///
/// * (value,rank,count) = [selected items]
/// * (11,1,1) = [{0=17}]
/// * (11,-1,1) = [{9=10}]
pub fn get_by_value_relative_rank_range_count(
    return_type: MapReturnType,
    value: FilterExpression,
    rank: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByValueRelRankRange as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects map item identified by index and returns selected data specified by returnType.
pub fn get_by_index(
    return_type: MapReturnType,
    value_type: ExpType,
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByIndex as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Create expression that selects map items starting at specified index to the end of map and returns selected
/// data specified by returnType.
pub fn get_by_index_range(
    return_type: MapReturnType,
    index: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects "count" map items starting at specified index and returns selected data
/// specified by returnType.
pub fn get_by_index_range_count(
    return_type: MapReturnType,
    index: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByIndexRange as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(index),
        ExpressionArgument::FilterExpression(count),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects map item identified by rank and returns selected data specified by returnType.
pub fn get_by_rank(
    return_type: MapReturnType,
    value_type: ExpType,
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByRank as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, value_type, args)
}

/// Create expression that selects map items starting at specified rank to the last ranked item and
/// returns selected data specified by returnType.
pub fn get_by_rank_range(
    return_type: MapReturnType,
    rank: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByRankRange as u8)),
        ExpressionArgument::Value(Value::from(return_type as u8)),
        ExpressionArgument::FilterExpression(rank),
        ExpressionArgument::Context(ctx.to_vec()),
    ];
    add_read(bin, get_value_type(return_type), args)
}

/// Create expression that selects "count" map items starting at specified rank and returns selected
/// data specified by returnType.
pub fn get_by_rank_range_count(
    return_type: MapReturnType,
    rank: FilterExpression,
    count: FilterExpression,
    bin: FilterExpression,
    ctx: &[CdtContext],
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(CdtMapOpType::GetByRankRange as u8)),
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
    let return_type = if ctx.is_empty() || (ctx[0].id & CtxType::ListIndex as u8) == 0 {
        ExpType::MAP
    } else {
        ExpType::LIST
    };

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
const fn get_value_type(return_type: MapReturnType) -> ExpType {
    let t = return_type as u8 & !(MapReturnType::Inverted as u8);
    if t == MapReturnType::Key as u8 || t == MapReturnType::Value as u8 {
        ExpType::LIST
    } else if t == MapReturnType::KeyValue as u8 {
        ExpType::MAP
    } else {
        ExpType::INT
    }
}
