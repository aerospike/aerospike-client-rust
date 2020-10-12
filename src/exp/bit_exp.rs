use crate::exp::exp::{ExpOp, ExpType, ExpressionArgument, FilterCmd, MODIFY};
use crate::operations::bitwise::{BitPolicy, BitwiseResizeFlags, BitwiseOverflowActions};
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

    pub fn or(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, value: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::OR as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    pub fn xor(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, value: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::XOR as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    pub fn and(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, value: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::AND as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    pub fn not(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::NOT as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    pub fn lshift(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, shift: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::LSHIFT as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(shift),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    pub fn rshift(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, shift: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::RSHIFT as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    pub fn add(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, value: FilterCmd, signed: bool, action: BitwiseOverflowActions, bin: FilterCmd) -> FilterCmd {
        let mut args =  vec![
            ExpressionArgument::Value(Value::from(BitExpOp::ADD as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        let mut flags = action as u8;
        if signed {
            flags |= INT_FLAGS_SIGNED;
        }
        args.push(ExpressionArgument::Value(Value::from(flags)));
        add_write(bin, args)
    }

    pub fn subtract(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, value: FilterCmd, signed: bool, action: BitwiseOverflowActions, bin: FilterCmd) -> FilterCmd {
        let mut args =  vec![
            ExpressionArgument::Value(Value::from(BitExpOp::SUBTRACT as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        let mut flags = action as u8;
        if signed {
            flags |= INT_FLAGS_SIGNED;
        }
        args.push(ExpressionArgument::Value(Value::from(flags)));
        add_write(bin, args)
    }

    pub fn set_int(policy: &BitPolicy, bit_offset: FilterCmd, bit_size: FilterCmd, value: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::SetInt as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    pub fn get( bit_offset: FilterCmd, bit_size: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::GET as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
        ];
        add_read(bin, ExpType::BLOB, args)
    }

    pub fn count( bit_offset: FilterCmd, bit_size: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::COUNT as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
        ];
        add_read(bin, ExpType::INT, args)
    }

    pub fn lscan( bit_offset: FilterCmd, bit_size: FilterCmd, value: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::LSCAN as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value)
        ];
        add_read(bin, ExpType::INT, args)
    }
    pub fn rscan( bit_offset: FilterCmd, bit_size: FilterCmd, value: FilterCmd, bin: FilterCmd) -> FilterCmd {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::RSCAN as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
            ExpressionArgument::FilterCmd(value)
        ];
        add_read(bin, ExpType::INT, args)
    }

    pub fn get_int( bit_offset: FilterCmd, bit_size: FilterCmd, signed: bool, bin: FilterCmd) -> FilterCmd {
        let mut args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::GetInt as i64)),
            ExpressionArgument::FilterCmd(bit_offset),
            ExpressionArgument::FilterCmd(bit_size),
        ];
        if signed {
            args.push(ExpressionArgument::Value(Value::from(INT_FLAGS_SIGNED)));
        }
        add_read(bin, ExpType::INT, args)
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
