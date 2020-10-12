use crate::exp::exp::{ExpOp, ExpType, ExpressionArgument, FilterCmd, MODIFY, Expression};
use crate::operations::cdt_context::CdtContext;
use crate::operations::lists::{ListPolicy, ListReturnType, ListSortFlags};
use crate::Value;

pub struct ListExpression {}

const MODULE: i64 = 0;

pub enum ListExpOp {
    APPEND = 1,
    AppendItems = 2,
    INSERT = 3,
    InsertItems = 4,
    SET = 9,
    CLEAR = 11,
    INCREMENT = 12,
    SORT = 13,
    SIZE = 16,
    GetByIndex = 19,
    GetByRank = 21,
    GetByValue = 22, // GET_ALL_BY_VALUE on server.
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
impl ListExpression {
    pub fn append(
        policy: ListPolicy,
        value: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::APPEND as i64)),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.attributes as u8)),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn append_items(
        policy: ListPolicy,
        list: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::AppendItems as i64)),
            ExpressionArgument::FilterCmd(list),
            ExpressionArgument::Value(Value::from(policy.attributes as u8)),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn insert(
        policy: ListPolicy,
        index: FilterCmd,
        value: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::INSERT as i64)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn insert_items(
        policy: ListPolicy,
        index: FilterCmd,
        list: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::InsertItems as i64)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::FilterCmd(list),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn increment(
        policy: ListPolicy,
        index: FilterCmd,
        value: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::INCREMENT as i64)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.attributes as u8)),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn set(
        policy: ListPolicy,
        index: FilterCmd,
        value: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::SET as i64)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn clear(bin: FilterCmd, ctx: &[CdtContext]) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::CLEAR as i64)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn sort(sort_flags: ListSortFlags, bin: FilterCmd, ctx: &[CdtContext]) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::SORT as i64)),
            ExpressionArgument::Value(Value::from(sort_flags as u8)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_value(value: FilterCmd, bin: FilterCmd, ctx: &[CdtContext]) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValue as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_value_list(
        values: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValueList as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(values),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_value_range(
        value_begin: Option<FilterCmd>,
        value_end: Option<FilterCmd>,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let mut args = vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValueInterval as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
        ];
        if let Some(val_beg) = value_begin {
            args.push(ExpressionArgument::FilterCmd(val_beg));
        }else{
            args.push(ExpressionArgument::FilterCmd(Expression::nil()));
        }
        if let Some(val_end) = value_end {
            args.push(ExpressionArgument::FilterCmd(val_end));
        }
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_value_relative_rank_range(
        value: FilterCmd,
        rank: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValueRelRankRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::FilterCmd(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_value_relative_rank_range_count(
        value_begin: FilterCmd,
        value_end: FilterCmd,
        count: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByValueRelRankRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(value_begin),
            ExpressionArgument::FilterCmd(value_end),
            ExpressionArgument::FilterCmd(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_index(index: FilterCmd, bin: FilterCmd, ctx: &[CdtContext]) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByIndex as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_index_range(
        index: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByIndexRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_index_range_count(
        index: FilterCmd,
        count: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByIndexRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::FilterCmd(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_rank(rank: FilterCmd, bin: FilterCmd, ctx: &[CdtContext]) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByRank as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_rank_range(rank: FilterCmd, bin: FilterCmd, ctx: &[CdtContext]) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByRankRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn remove_by_rank_range_count(
        rank: FilterCmd,
        count: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::RemoveByRankRange as i64)),
            ExpressionArgument::Value(Value::from(ListReturnType::None as u8)),
            ExpressionArgument::FilterCmd(rank),
            ExpressionArgument::FilterCmd(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_write(bin, ctx, args)
    }

    pub fn size(bin: FilterCmd, ctx: &[CdtContext]) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::SIZE as i64)),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ExpType::INT, args)
    }

    pub fn get_by_value(
        return_type: ListReturnType,
        value: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValue as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    pub fn get_by_value_range(
        return_type: ListReturnType,
        value_begin: Option<FilterCmd>,
        value_end: Option<FilterCmd>,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let mut args = vec![
            ExpressionArgument::Context(ctx.to_vec()),
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValueInterval as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
        ];
        if let Some(val_beg) = value_begin {
            args.push(ExpressionArgument::FilterCmd(val_beg));
        }else{
            args.push(ExpressionArgument::FilterCmd(Expression::nil()));
        }
        if let Some(val_end) = value_end {
            args.push(ExpressionArgument::FilterCmd(val_end));
        }
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    pub fn get_by_value_list(
        return_type: ListReturnType,
        values: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValueList as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(values),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    pub fn get_by_value_relative_rank_range(
        return_type: ListReturnType,
        value: FilterCmd,
        rank: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValueRelRankRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::FilterCmd(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    pub fn get_by_value_relative_rank_range_count(
        return_type: ListReturnType,
        value: FilterCmd,
        rank: FilterCmd,
        count: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByValueRelRankRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::FilterCmd(rank),
            ExpressionArgument::FilterCmd(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    pub fn get_by_index(
        return_type: ListReturnType,
        value_type: ExpType,
        index: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByIndex as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, value_type, args)
    }

    pub fn get_by_index_range(
        return_type: ListReturnType,
        index: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByIndexRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    pub fn get_by_index_range_count(
        return_type: ListReturnType,
        index: FilterCmd,
        count: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByIndexRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(index),
            ExpressionArgument::FilterCmd(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    pub fn get_by_rank(
        return_type: ListReturnType,
        value_type: ExpType,
        rank: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByRank as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, value_type, args)
    }

    pub fn get_by_rank_range(
        return_type: ListReturnType,
        rank: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByRankRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(rank),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    pub fn get_by_rank_range_count(
        return_type: ListReturnType,
        rank: FilterCmd,
        count: FilterCmd,
        bin: FilterCmd,
        ctx: &[CdtContext],
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(ListExpOp::GetByRankRange as i64)),
            ExpressionArgument::Value(Value::from(return_type as u8)),
            ExpressionArgument::FilterCmd(rank),
            ExpressionArgument::FilterCmd(count),
            ExpressionArgument::Context(ctx.to_vec()),
        ];
        ListExpression::add_read(bin, ListExpression::get_value_type(return_type), args)
    }

    pub fn add_read(
        bin: FilterCmd,
        return_type: ExpType,
        arguments: Vec<ExpressionArgument>,
    ) -> FilterCmd {
        FilterCmd {
            cmd: Some(ExpOp::Call),
            val: None,
            bin: Some(Box::new(bin)),
            flags: Some(MODULE),
            module: Some(return_type),
            exps: None,
            arguments: Some(arguments),
        }
    }

    pub fn add_write(
        bin: FilterCmd,
        ctx: &[CdtContext],
        arguments: Vec<ExpressionArgument>,
    ) -> FilterCmd {
        let mut return_type: ExpType;
        if ctx.is_empty() {
            return_type = ExpType::LIST
        } else {
            if (ctx[0].id & 0x10) == 0 {
                return_type = ExpType::MAP;
            } else {
                return_type = ExpType::LIST;
            }
        }

        FilterCmd {
            cmd: Some(ExpOp::Call),
            val: None,
            bin: Some(Box::new(bin)),
            flags: Some(MODULE | MODIFY),
            module: Some(return_type),
            exps: None,
            arguments: Some(arguments),
        }
    }

    pub fn get_value_type(return_type: ListReturnType) -> ExpType {
        if (return_type as u8 & !(ListReturnType::Inverted as u8)) == ListReturnType::Values as u8 {
            ExpType::LIST
        } else {
            ExpType::INT
        }
    }
}
