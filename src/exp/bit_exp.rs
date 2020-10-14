//! Bitwise Aerospike Filter Expressions.
use crate::exp::{ExpOp, ExpType, ExpressionArgument, FilterExpression, MODIFY};
use crate::operations::bitwise::{BitPolicy, BitwiseOverflowActions, BitwiseResizeFlags};
use crate::Value;

const MODULE: i64 = 1;
const INT_FLAGS_SIGNED: i64 = 1;

#[doc(hidden)]
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

/// Bit expression generator.
///
/// The bin expression argument in these methods can be a reference to a bin or the
/// result of another expression. Expressions that modify bin values are only used
/// for temporary expression evaluation and are not permanently applied to the bin.
/// Bit modify expressions return the blob bin's value.
///
/// Offset orientation is left-to-right.  Negative offsets are supported.
/// If the offset is negative, the offset starts backwards from end of the bitmap.
/// If an offset is out of bounds, a parameter error will be returned.
pub struct BitExpression {}

impl BitExpression {
    /// Create expression that resizes byte[] to byteSize according to resizeFlags
    /// and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010]
    /// byteSize = 4
    /// resizeFlags = 0
    /// returns [0b00000001, 0b01000010, 0b00000000, 0b00000000]
    ///
    /// ```
    /// // Resize bin "a" and compare bit count
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::bit_exp::BitExpression;
    /// use aerospike::operations::bitwise::{BitPolicy, BitwiseResizeFlags};
    /// Expression::eq(
    ///   BitExpression::count(Expression::int_val(0), Expression::int_val(3),
    ///     BitExpression::resize(&BitPolicy::default(), Expression::int_val(4), BitwiseResizeFlags::Default, Expression::blob_bin("a".to_string()))),
    ///   Expression::int_val(2));
    /// ```
    pub fn resize(
        policy: &BitPolicy,
        byte_size: FilterExpression,
        resize_flags: BitwiseResizeFlags,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::RESIZE as i64)),
            ExpressionArgument::FilterExpression(byte_size),
            ExpressionArgument::Value(Value::from(policy.flags)),
            ExpressionArgument::Value(Value::from(resize_flags as u8)),
        ];
        add_write(bin, args)
    }

    /// Create expression that inserts value bytes into byte[] bin at byteOffset and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// byteOffset = 1
    /// value = [0b11111111, 0b11000111]
    /// bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    ///
    /// ```
    /// // Insert bytes into bin "a" and compare bit count
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::bit_exp::BitExpression;
    /// use aerospike::operations::bitwise::BitPolicy;
    /// let bytes: Vec<u8> = vec![];
    /// Expression::eq(
    ///   BitExpression::count(Expression::int_val(0), Expression::int_val(3),
    ///     BitExpression::insert(&BitPolicy::default(), Expression::int_val(1), Expression::blob_val(bytes), Expression::blob_bin("a".to_string()))),
    ///   Expression::int_val(2));
    /// ```
    pub fn insert(
        policy: &BitPolicy,
        byte_offset: FilterExpression,
        value: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::INSERT as i64)),
            ExpressionArgument::FilterExpression(byte_offset),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that removes bytes from byte[] bin at byteOffset for byteSize and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// byteOffset = 2
    /// byteSize = 3
    /// bin result = [0b00000001, 0b01000010]
    ///
    /// ```
    /// // Remove bytes from bin "a" and compare bit count
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::bit_exp::BitExpression;
    /// use aerospike::operations::bitwise::BitPolicy;
    /// Expression::eq(
    ///   BitExpression::count(Expression::int_val(0), Expression::int_val(3),
    ///     BitExpression::remove(&BitPolicy::default(), Expression::int_val(2), Expression::int_val(3), Expression::blob_bin("a".to_string()))),
    ///   Expression::int_val(2));
    /// ```
    pub fn remove(
        policy: &BitPolicy,
        byte_offset: FilterExpression,
        byte_size: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::INSERT as i64)),
            ExpressionArgument::FilterExpression(byte_offset),
            ExpressionArgument::FilterExpression(byte_size),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that sets value on byte[] bin at bitOffset for bitSize and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 13
    /// bitSize = 3
    /// value = [0b11100000]
    /// bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]
    ///
    /// ```
    /// // Set bytes in bin "a" and compare bit count
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::bit_exp::BitExpression;
    /// use aerospike::operations::bitwise::BitPolicy;
    /// let bytes: Vec<u8> = vec![];
    /// Expression::eq(
    ///   BitExpression::count(Expression::int_val(0), Expression::int_val(3),
    ///     BitExpression::set(&BitPolicy::default(), Expression::int_val(13), Expression::int_val(3), Expression::blob_val(bytes), Expression::blob_bin("a".to_string()))),
    ///   Expression::int_val(2));
    /// ```
    pub fn set(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        value: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::INSERT as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that performs bitwise "or" on value and byte[] bin at bitOffset for bitSize
    /// and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 17
    /// bitSize = 6
    /// value = [0b10101000]
    /// bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]
    ///
    pub fn or(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        value: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::OR as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that performs bitwise "xor" on value and byte[] bin at bitOffset for bitSize
    /// and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 17
    /// bitSize = 6
    /// value = [0b10101100]
    /// bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]
    ///
    pub fn xor(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        value: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::XOR as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that performs bitwise "and" on value and byte[] bin at bitOffset for bitSize
    /// and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 23
    /// bitSize = 9
    /// value = [0b00111100, 0b10000000]
    /// bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]
    ///
    pub fn and(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        value: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::AND as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that negates byte[] bin starting at bitOffset for bitSize and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 25
    /// bitSize = 6
    /// bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]
    ///
    pub fn not(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::NOT as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that shifts left byte[] bin starting at bitOffset for bitSize and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 32
    /// bitSize = 8
    /// shift = 3
    /// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]
    ///
    pub fn lshift(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        shift: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::LSHIFT as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(shift),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that shifts right byte[] bin starting at bitOffset for bitSize and returns byte[].
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 0
    /// bitSize = 9
    /// shift = 1
    /// bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]
    ///
    pub fn rshift(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        shift: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::RSHIFT as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(shift),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that adds value to byte[] bin starting at bitOffset for bitSize and returns byte[].
    /// `BitSize` must be <= 64. Signed indicates if bits should be treated as a signed number.
    /// If add overflows/underflows, `BitwiseOverflowActions` is used.
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 24
    /// bitSize = 16
    /// value = 128
    /// signed = false
    /// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]
    ///
    pub fn add(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        value: FilterExpression,
        signed: bool,
        action: BitwiseOverflowActions,
        bin: FilterExpression,
    ) -> FilterExpression {
        let mut args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::ADD as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        let mut flags = action as u8;
        if signed {
            flags |= INT_FLAGS_SIGNED as u8;
        }
        args.push(ExpressionArgument::Value(Value::from(flags)));
        add_write(bin, args)
    }

    /// Create expression that subtracts value from byte[] bin starting at bitOffset for bitSize and returns byte[].
    /// `BitSize` must be <= 64. Signed indicates if bits should be treated as a signed number.
    /// If add overflows/underflows, `BitwiseOverflowActions` is used.
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 24
    /// bitSize = 16
    /// value = 128
    /// signed = false
    /// bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]
    ///
    pub fn subtract(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        value: FilterExpression,
        signed: bool,
        action: BitwiseOverflowActions,
        bin: FilterExpression,
    ) -> FilterExpression {
        let mut args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::SUBTRACT as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        let mut flags = action as u8;
        if signed {
            flags |= INT_FLAGS_SIGNED as u8;
        }
        args.push(ExpressionArgument::Value(Value::from(flags)));
        add_write(bin, args)
    }

    /// Create expression that sets value to byte[] bin starting at bitOffset for bitSize and returns byte[].
    /// `BitSize` must be <= 64.
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 1
    /// bitSize = 8
    /// value = 127
    /// bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]
    ///
    pub fn set_int(
        policy: &BitPolicy,
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        value: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::SetInt as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::from(policy.flags)),
        ];
        add_write(bin, args)
    }

    /// Create expression that returns bits from byte[] bin starting at bitOffset for bitSize.
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 9
    /// bitSize = 5
    /// returns [0b10000000]
    ///
    /// ```
    /// // Bin "a" bits = [0b10000000]
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::bit_exp::BitExpression;
    /// Expression::eq(
    ///   BitExpression::get(Expression::int_val(9), Expression::int_val(5), Expression::blob_bin("a".to_string())),
    ///   Expression::blob_val(vec![0b10000000]));
    /// ```
    pub fn get(
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::GET as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
        ];
        add_read(bin, ExpType::BLOB, args)
    }

    /// Create expression that returns integer count of set bits from byte[] bin starting at
    /// bitOffset for bitSize.
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 20
    /// bitSize = 4
    /// returns 2
    ///
    /// ```
    /// // Bin "a" bit count <= 2
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::bit_exp::BitExpression;
    /// Expression::le(BitExpression::count(Expression::int_val(0), Expression::int_val(5), Expression::blob_bin("a".to_string())), Expression::int_val(2));
    /// ```
    pub fn count(
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::COUNT as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
        ];
        add_read(bin, ExpType::INT, args)
    }

    /// Create expression that returns integer bit offset of the first specified value bit in byte[] bin
    /// starting at bitOffset for bitSize.
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 24
    /// bitSize = 8
    /// value = true
    /// returns 5
    ///
    /// ```
    /// // lscan(a) == 5
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::bit_exp::BitExpression;
    /// Expression::eq(BitExpression::lscan(Expression::int_val(24), Expression::int_val(8), Expression::int_val(1), Expression::blob_bin("a".to_string())), Expression::int_val(5));
    /// ```
    ///
    pub fn lscan(
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        value: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::LSCAN as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(value),
        ];
        add_read(bin, ExpType::INT, args)
    }

    /// Create expression that returns integer bit offset of the last specified value bit in byte[] bin
    /// starting at bitOffset for bitSize.
    /// Example:
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 32
    /// bitSize = 8
    /// value = true
    /// returns 7
    ///
    /// ```
    /// // rscan(a) == 7
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::bit_exp::BitExpression;
    /// Expression::eq(BitExpression::rscan(Expression::int_val(32), Expression::int_val(8), Expression::int_val(1), Expression::blob_bin("a".to_string())), Expression::int_val(7));
    /// ```
    ///
    pub fn rscan(
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        value: FilterExpression,
        bin: FilterExpression,
    ) -> FilterExpression {
        let args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::RSCAN as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
            ExpressionArgument::FilterExpression(value),
        ];
        add_read(bin, ExpType::INT, args)
    }

    /// Create expression that returns integer from byte[] bin starting at bitOffset for bitSize.
    /// Signed indicates if bits should be treated as a signed number.
    ///
    /// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
    /// bitOffset = 8
    /// bitSize = 16
    /// signed = false
    /// returns 16899
    ///
    /// ```
    /// // getInt(a) == 16899
    /// use aerospike::exp::Expression;
    /// use aerospike::exp::bit_exp::BitExpression;
    /// Expression::eq(BitExpression::get_int(Expression::int_val(8), Expression::int_val(16), false, Expression::blob_bin("a".to_string())), Expression::int_val(16899));
    /// ```
    pub fn get_int(
        bit_offset: FilterExpression,
        bit_size: FilterExpression,
        signed: bool,
        bin: FilterExpression,
    ) -> FilterExpression {
        let mut args = vec![
            ExpressionArgument::Value(Value::from(BitExpOp::GetInt as i64)),
            ExpressionArgument::FilterExpression(bit_offset),
            ExpressionArgument::FilterExpression(bit_size),
        ];
        if signed {
            args.push(ExpressionArgument::Value(Value::from(INT_FLAGS_SIGNED)));
        }
        add_read(bin, ExpType::INT, args)
    }
}

#[doc(hidden)]
pub fn add_write(bin: FilterExpression, arguments: Vec<ExpressionArgument>) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(bin)),
        flags: Some(MODULE | MODIFY),
        module: Some(ExpType::BLOB),
        exps: None,
        arguments: Some(arguments),
    }
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