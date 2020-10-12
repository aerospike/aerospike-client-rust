use crate::exp::exp::{ExpOp, ExpType, ExpressionArgument, FilterCmd, MODIFY};
use crate::operations::bitwise::{BitPolicy, BitwiseResizeFlags};
use crate::Value;

const MODULE: i64 = 1;
const INT_FLAGS_SIGNED: i64 = 1;

pub enum BitExpOp {
    RESIZE = 0,
    INSERT = 1,
    REMOVE = 2,
    SET = 3,
    OR = 4,
    XOR = 5,
    AND = 6,
    NOT = 7,
    LSHIFT = 8,
    RSHIFT = 9,
    ADD = 10,
    SUBTRACT = 11,
    SetInt = 12,
    GET = 50,
    COUNT = 51,
    LSCAN = 52,
    RSCAN = 53,
    GetInt = 54,
}

pub struct BitExpression {}

impl BitExpression {
    pub fn resize(
        policy: &BitPolicy,
        byte_size: FilterCmd,
        resize_flags: BitwiseResizeFlags,
        bin: FilterCmd,
    ) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::RESIZE as i64)),
            ExpressionArgument::FilterCmd(byte_size),
            ExpressionArgument::Value(Value::from(policy.flags)),
            ExpressionArgument::Value(Value::from(resize_flags as u8))
        ];
        add_write(bin, args)
    }

    pub fn insert(policy: &BitPolicy, byte_offset: FilterCmd, value: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::INSERT as i64)),
            ExpressionArgument::FilterCmd(byte_offset),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    pub fn remove(policy: &BitPolicy, byte_offset: FilterCmd, byte_size: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::INSERT as i64)),
            ExpressionArgument::FilterCmd(byte_offset),
            ExpressionArgument::FilterCmd(byte_size),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    pub fn set(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, value: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::INSERT as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }
}

pub fn add_write(bin: FilterCmd, arguments: Vec<ExpressionArgument>) -> FilterCmd {
    FilterCmd {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(bin)),
        flags: Some(MODULE | MODIFY),
        module: Some(ExpType::BLOB),
        exps: None,
        arguments: Some(arguments),
    }
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
