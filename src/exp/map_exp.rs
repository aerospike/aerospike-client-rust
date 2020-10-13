//! Map Cdt Aerospike Filter Expressions.
use crate::exp::exp::{ExpOp, ExpType, Expression, ExpressionArgument, FilterExpression, MODIFY};
use crate::operations::cdt_context::CdtContext;
use crate::operations::maps::CdtMapOpType;
use crate::operations::OperationData::CdtMapOp;
use crate::{MapPolicy, MapReturnType, MapWriteMode, Value};

/// Map Cdt Filter Expressions
pub struct MapExpression {}

#[doc(hidden)]
const MODULE: i64 = 0;

#[doc(hidden)]
pub enum MapExpOp {
    PUT = 67,
    PutItems = 68,
    REPLACE = 69,
    ReplaceItems = 70,
    INCREMENT = 73,
    CLEAR = 75,
    RemoveByKey = 76,
    RemoveByIndex = 77,
    RemoveByRank = 79,
    RemoveByKeyList = 81,
    RemoveByValue = 82,
    RemoveByValueList = 83,
    RemoveByKeyInterval = 84,
    RemoveByIndexRange = 85,
    RemoveByValueInterval = 86,
    RemoveByRankRange = 87,
    RemoveByKeyRelIndexRange = 88,
    RemoveByValueRelRankRange = 89,
    SIZE = 96,
    GetByKey = 97,
    GetByIndex = 98,
    GetByRank = 100,
    GetByValue = 102, // GET_ALL_BY_VALUE on server.
    GetByKeyInterval = 103,
    GetByIndexRange = 104,
    GetByValueInterval = 105,
    GetByRankRange = 106,
    GetByKeyList = 107,
    GetByValueList = 108,
    GetByKeyRelIndexRange = 109,
    GetByValueRelRankRange = 110,
}

impl MapExpression {
    /// Create expression that writes key/value item to map bin.
    pub fn put(
        policy: &MapPolicy,
        key: FilterExpression,
        value: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let mut args: Vec<ExpressionArgument>;
        let pol = MapExpression::get_policy_value(policy.write_mode, false);
        if pol == CdtMapOpType::Replace as u8 {
            args = vec![
                ExpressionArgument::Context(ctx.to_vec()),
                ExpressionArgument::Value(Value::from(pol)),
                ExpressionArgument::FilterExpression(key),
                ExpressionArgument::FilterExpression(value),
            ]
        } else {
            args = vec![
                ExpressionArgument::Context(ctx.to_vec()),
                ExpressionArgument::Value(Value::from(pol)),
                ExpressionArgument::FilterExpression(key),
                ExpressionArgument::FilterExpression(value),
                ExpressionArgument::Value(Value::from(policy.order as u8)),
            ]
        }
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that writes each map item to map bin.
    pub fn put_items(
        policy: &MapPolicy,
        map: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let mut args: Vec<ExpressionArgument>;
        let pol = MapExpression::get_policy_value(policy.write_mode, true);
        if pol == CdtMapOpType::Replace as u8 {
            args = vec![
                ExpressionArgument::Context(ctx.to_vec()),
                ExpressionArgument::Value(Value::from(pol)),
                ExpressionArgument::FilterExpression(map),
            ]
        } else {
            args = vec![
                ExpressionArgument::Context(ctx.to_vec()),
                ExpressionArgument::Value(Value::from(pol)),
                ExpressionArgument::FilterExpression(map),
                ExpressionArgument::Value(Value::from(policy.order as u8)),
            ]
        }
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that increments values by incr for all items identified by key.
    /// Valid only for numbers.
    pub fn increment(
        policy: &MapPolicy,
        key: FilterExpression,
        incr: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::INCREMENT as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::FilterExpression(incr),
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(policy.order as u8)),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes all items in map.
    pub fn clear(bin: FilterExpression, ctx: &[CdtContext]) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::CLEAR as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map item identified by key.
    pub fn remove_by_key(
        key: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByKey as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map items identified by keys.
    pub fn remove_by_key_list(
        keys: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByKeyList as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(keys),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByKeyInterval as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ];
        if let Some(val_beg) = key_begin {
            args.push(ExpressionArgument::FilterExpression(val_beg));
        } else {
            args.push(ExpressionArgument::FilterExpression(Expression::nil()));
        }
        if let Some(val_end) = key_end {
            args.push(ExpressionArgument::FilterExpression(val_end));
        }
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map items nearest to key and greater by index.
    ///
    /// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
    ///
    /// (value,index) = [removed items]
    /// (5,0) = [{5=15},{9=10}]
    /// (5,1) = [{9=10}]
    /// (5,-1) = [{4=2},{5=15},{9=10}]
    /// (3,2) = [{9=10}]</li>
    /// (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
    pub fn remove_by_key_relative_index_range(
        key: FilterExpression,
        index: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByKeyRelIndexRange as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map items nearest to key and greater by index with a count limit.
    ///
    /// Examples for map [{0=17},{4=2},{5=15},{9=10}]:
    ///
    /// (value,index,count) = [removed items]
    /// (5,0,1) = [{5=15}]
    /// (5,1,2) = [{9=10}]
    /// (5,-1,1) = [{4=2}]
    /// (3,2,1) = [{9=10}]
    /// (3,-2,2) = [{0=17}]
    pub fn remove_by_key_relative_index_range_count(
        key: FilterExpression,
        index: FilterExpression,
        count: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByKeyRelIndexRange as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map items identified by value.
    pub fn remove_by_value(
        value: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByValue as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map items identified by values.
    pub fn remove_by_value_list(
        values: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByValueList as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(values),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByValueInterval as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
        ];
        if let Some(val_beg) = value_begin {
            args.push(ExpressionArgument::FilterExpression(val_beg));
        } else {
            args.push(ExpressionArgument::FilterExpression(Expression::nil()));
        }
        if let Some(val_end) = value_end {
            args.push(ExpressionArgument::FilterExpression(val_end));
        }
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map items nearest to value and greater by relative rank.
    ///
    /// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
    ///
    /// (value,rank) = [removed items]
    /// (11,1) = [{0=17}]
    /// (11,-1) = [{9=10},{5=15},{0=17}]
    pub fn remove_by_value_relative_rank_range(
        value: FilterExpression,
        rank: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByValueRelRankRange as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map items nearest to value and greater by relative rank with a count limit.
    ///
    /// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
    ///
    /// (value,rank,count) = [removed items]
    /// (11,1,1) = [{0=17}]
    /// (11,-1,1) = [{9=10}]
    pub fn remove_by_value_relative_rank_range_count(
        value: FilterExpression,
        rank: FilterExpression,
        count: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByValueRelRankRange as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map item identified by index.
    pub fn remove_by_index(
        index: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByIndex as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map items starting at specified index to the end of map.
    pub fn remove_by_index_range(
        index: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByIndexRange as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes "count" map items starting at specified index.
    pub fn remove_by_index_range_count(
        index: FilterExpression,
        count: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByIndexRange as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map item identified by rank.
    pub fn remove_by_rank(
        rank: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByRank as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes map items starting at specified rank to the last ranked item.
    pub fn remove_by_rank_range(
        rank: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByRankRange as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that removes "count" map items starting at specified rank.
    pub fn remove_by_rank_range_count(
        rank: FilterExpression,
        count: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::RemoveByRank as u8)),
            ExpressionArgument::Value(Value::from(MapReturnType::None as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_write(bin, ctx, args)
    }

    /// Create expression that returns list size.
    ///
    /// ```
    /// // Map bin "a" size > 7
    /// use aerospike::exp::exp::Expression;
    /// use aerospike::exp::map_exp::MapExpression;
    ///
    /// Expression::gt(MapExpression::size(Expression::map_bin("a".to_string()), &[]), Expression::int_val(7));
    ///
    /// ```
    pub fn size(bin: FilterExpression, ctx: &[CdtContext]) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::SIZE as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, ExpType::INT, args)
    }

    /// Create expression that selects map item identified by key and returns selected data
    /// specified by returnType.
    ///
    /// ```
    /// // Map bin "a" contains key "B"
    /// use aerospike::exp::exp::{Expression, ExpType};
    /// use aerospike::exp::map_exp::MapExpression;
    /// use aerospike::MapReturnType;
    /// Expression::gt(MapExpression::get_by_key(MapReturnType::Count, ExpType::INT, Expression::string_val("B".to_string()), Expression::map_bin("a".to_string()), &[]), Expression::int_val(0));
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
            ExpressionArgument::Value(Value::from(MapExpOp::GetByKey as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, value_type, args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::GetByKeyInterval as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
        ];
        if let Some(val_beg) = key_begin {
            args.push(ExpressionArgument::FilterExpression(val_beg));
        } else {
            args.push(ExpressionArgument::FilterExpression(Expression::nil()));
        }
        if let Some(val_end) = key_end {
            args.push(ExpressionArgument::FilterExpression(val_end));
        }
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects map items identified by keys and returns selected data specified by returnType
    pub fn get_by_key_list(
        return_type: MapReturnType,
        keys: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::GetByKeyList as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(keys),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects map items nearest to key and greater by index.
    /// Expression returns selected data specified by returnType.
    ///
    /// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
    ///
    /// (value,index) = [selected items]
    /// (5,0) = [{5=15},{9=10}]
    /// (5,1) = [{9=10}]
    /// (5,-1) = [{4=2},{5=15},{9=10}]
    /// (3,2) = [{9=10}]
    /// (3,-2) = [{0=17},{4=2},{5=15},{9=10}]
    pub fn get_by_key_relative_index_range(
        return_type: MapReturnType,
        key: FilterExpression,
        index: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::GetByKeyRelIndexRange as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects map items nearest to key and greater by index with a count limit.
    /// Expression returns selected data specified by returnType.
    ///
    /// Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
    ///
    /// (value,index,count) = [selected items]
    /// (5,0,1) = [{5=15}]
    /// (5,1,2) = [{9=10}]
    /// (5,-1,1) = [{4=2}]
    /// (3,2,1) = [{9=10}]
    /// (3,-2,2) = [{0=17}]
    pub fn get_by_key_relative_index_range_count(
        return_type: MapReturnType,
        key: FilterExpression,
        index: FilterExpression,
        count: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::GetByKeyRelIndexRange as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(key),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects map items identified by value and returns selected data
    /// specified by returnType.
    ///
    /// ```
    /// // Map bin "a" contains value "BBB"
    /// use aerospike::exp::exp::Expression;
    /// use aerospike::exp::map_exp::MapExpression;
    /// use aerospike::MapReturnType;
    /// Expression::gt(MapExpression::get_by_value(MapReturnType::Count, Expression::string_val("BBB".to_string()), Expression::map_bin("a".to_string()), &[]), Expression::int_val(0));
    /// ```
    pub fn get_by_value(
        return_type: MapReturnType,
        value: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::GetByValue as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::GetByValueInterval as u8)),
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
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects map items identified by values and returns selected data specified by returnType.
    pub fn get_by_value_list(
        return_type: MapReturnType,
        values: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::GetByValueList as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(values),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects map items nearest to value and greater by relative rank.
    /// Expression returns selected data specified by returnType.
    ///
    /// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
    ///
    /// (value,rank) = [selected items]
    /// (11,1) = [{0=17}]
    /// (11,-1) = [{9=10},{5=15},{0=17}]
    pub fn get_by_value_relative_rank_range(
        return_type: MapReturnType,
        value: FilterExpression,
        rank: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::GetByValueRelRankRange as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
    }

    /// Create expression that selects map items nearest to value and greater by relative rank with a count limit.
    /// Expression returns selected data specified by returnType.
    ///
    /// Examples for map [{4=2},{9=10},{5=15},{0=17}]:
    ///
    /// (value,rank,count) = [selected items]
    /// (11,1,1) = [{0=17}]
    /// (11,-1,1) = [{9=10}]
    pub fn get_by_value_relative_rank_range_count(
        return_type: MapReturnType,
        value: FilterExpression,
        rank: FilterExpression,
        count: FilterExpression,
        bin: FilterExpression,
        ctx: &[CdtContext],
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(MapExpOp::GetByValueRelRankRange as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::GetByIndex as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, value_type, args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::GetByIndexRange as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::GetByIndexRange as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::GetByRank as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, value_type, args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::GetByRankRange as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
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
            ExpressionArgument::Value(Value::from(MapExpOp::GetByRankRange as u8)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterExpression(rank),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        MapExpression::add_read(bin, MapExpression::get_value_type(return_type), args)
    }

    #[doc(hidden)]
    pub fn add_read(
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
    pub fn add_write(
        bin: FilterExpression,
        ctx: &[CdtContext],
        arguments: Vec<ExpressionArgument>,
    ) -> FilterExpression {
        let return_type: ExpType;
        if ctx.is_empty() {
            return_type = ExpType::MAP
        } else {
            if (ctx[0].id & 0x10) == 0 {
                return_type = ExpType::MAP;
            } else {
                return_type = ExpType::LIST;
            }
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
    pub fn get_value_type(return_type: MapReturnType) -> ExpType {
        let t = (return_type as u8 & !(MapReturnType::Inverted as u8));
        if t <= MapReturnType::Count as u8 {
            ExpType::INT
        } else if t == MapReturnType::KeyValue as u8 {
            ExpType::MAP
        } else {
            ExpType::LIST
        }
    }

    #[doc(hidden)]
    pub fn get_policy_value(write_policy: MapWriteMode, multi: bool) -> u8 {
        match write_policy {
            MapWriteMode::Update => {
                if multi {
                    CdtMapOpType::PutItems as u8
                } else {
                    CdtMapOpType::Put as u8
                }
            }
            MapWriteMode::UpdateOnly => {
                if multi {
                    CdtMapOpType::ReplaceItems as u8
                } else {
                    CdtMapOpType::Replace as u8
                }
            }
            MapWriteMode::CreateOnly => {
                if multi {
                    CdtMapOpType::AddItems as u8
                } else {
                    CdtMapOpType::Add as u8
                }
            }
        }
    }
}
