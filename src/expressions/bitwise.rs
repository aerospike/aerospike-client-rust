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

//! Bitwise Aerospike Filter Expressions.
use crate::expressions::{ExpOp, ExpType, ExpressionArgument, FilterExpression, MODIFY};
use crate::operations::bitwise::{BitPolicy, BitwiseOverflowActions, BitwiseResizeFlags};
use crate::Value;

const MODULE: i64 = 1;
const INT_FLAGS_SIGNED: i64 = 1;

#[doc(hidden)]
pub enum BitExpOp {
    Resize = 0,
    Insert = 1,
    Remove = 2,
    Set = 3,
    Or = 4,
    Xor = 5,
    And = 6,
    Not = 7,
    LShift = 8,
    RShift = 9,
    Add = 10,
    Subtract = 11,
    SetInt = 12,
    Get = 50,
    Count = 51,
    LScan = 52,
    RScan = 53,
    GetInt = 54,
}

/// Create expression that resizes byte[] to byteSize according to resizeFlags
/// and returns byte[].
///
/// ```
/// // Resize bin "a" and compare bit count
/// // bin = [0b00000001, 0b01000010]
/// // byteSize = 4
/// // resizeFlags = 0
/// // returns [0b00000001, 0b01000010, 0b00000000, 0b00000000]
/// use aerospike::operations::bitwise::{BitPolicy, BitwiseResizeFlags};
/// use aerospike::expressions::{eq, int_val, blob_bin};
/// use aerospike::expressions::bitwise::{count, resize};
/// eq(
///   count(int_val(0), int_val(3),
///     resize(&BitPolicy::default(), int_val(4), BitwiseResizeFlags::Default, blob_bin("a".to_string()))),
///   int_val(2));
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn resize(
    policy: &BitPolicy,
    byte_size: FilterExpression,
    resize_flags: BitwiseResizeFlags,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Resize as i64)),
        ExpressionArgument::FilterExpression(byte_size),
        ExpressionArgument::Value(Value::from(policy.flags)),
        ExpressionArgument::Value(Value::from(resize_flags as u8)),
    ];
    add_write(bin, args)
}

/// Create expression that inserts value bytes into byte[] bin at byteOffset and returns byte[].
///
///
/// ```
/// // Insert bytes into bin "a" and compare bit count
/// // bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// // byteOffset = 1
/// // value = [0b11111111, 0b11000111]
/// // bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// use aerospike::operations::bitwise::BitPolicy;
/// use aerospike::expressions::{eq, int_val, blob_val, blob_bin};
/// use aerospike::expressions::bitwise::{count, insert};
/// let bytes: Vec<u8> = vec![];
/// eq(
///   count(int_val(0), int_val(3),
///     insert(&BitPolicy::default(), int_val(1), blob_val(bytes), blob_bin("a".to_string()))),
///   int_val(2));
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn insert(
    policy: &BitPolicy,
    byte_offset: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Insert as i64)),
        ExpressionArgument::FilterExpression(byte_offset),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.flags)),
    ];
    add_write(bin, args)
}

/// Create expression that removes bytes from byte[] bin at byteOffset for byteSize and returns byte[].
///
/// ```
/// // Remove bytes from bin "a" and compare bit count
/// // bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// // byteOffset = 2
/// // byteSize = 3
/// // bin result = [0b00000001, 0b01000010]
/// use aerospike::expressions::{eq, int_val, blob_bin};
/// use aerospike::operations::bitwise::BitPolicy;
/// use aerospike::expressions::bitwise::{count, remove};
/// eq(
///   count(int_val(0), int_val(3),
///     remove(&BitPolicy::default(), int_val(2), int_val(3), blob_bin("a".to_string()))),
///   int_val(2));
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn remove(
    policy: &BitPolicy,
    byte_offset: FilterExpression,
    byte_size: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Remove as i64)),
        ExpressionArgument::FilterExpression(byte_offset),
        ExpressionArgument::FilterExpression(byte_size),
        ExpressionArgument::Value(Value::from(policy.flags)),
    ];
    add_write(bin, args)
}

/// Create expression that sets value on byte[] bin at bitOffset for bitSize and returns byte[].
///
/// ```
/// // Set bytes in bin "a" and compare bit count
/// // bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// // bitOffset = 13
/// // bitSize = 3
/// // value = [0b11100000]
/// // bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]
/// use aerospike::operations::bitwise::BitPolicy;
/// use aerospike::expressions::{eq, int_val, blob_val, blob_bin};
/// use aerospike::expressions::bitwise::{count, set};
/// let bytes: Vec<u8> = vec![];
/// eq(
///   count(int_val(0), int_val(3),
///     set(&BitPolicy::default(), int_val(13), int_val(3), blob_val(bytes), blob_bin("a".to_string()))),
///   int_val(2));
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn set(
    policy: &BitPolicy,
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Set as i64)),
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
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 17
/// bitSize = 6
/// value = [0b10101000]
/// bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn or(
    policy: &BitPolicy,
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Or as i64)),
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
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 17
/// bitSize = 6
/// value = [0b10101100]
/// bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn xor(
    policy: &BitPolicy,
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Xor as i64)),
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
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 23
/// bitSize = 9
/// value = [0b00111100, 0b10000000]
/// bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn and(
    policy: &BitPolicy,
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::And as i64)),
        ExpressionArgument::FilterExpression(bit_offset),
        ExpressionArgument::FilterExpression(bit_size),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.flags)),
    ];
    add_write(bin, args)
}

/// Create expression that negates byte[] bin starting at bitOffset for bitSize and returns byte[].
///
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 25
/// bitSize = 6
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn not(
    policy: &BitPolicy,
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Not as i64)),
        ExpressionArgument::FilterExpression(bit_offset),
        ExpressionArgument::FilterExpression(bit_size),
        ExpressionArgument::Value(Value::from(policy.flags)),
    ];
    add_write(bin, args)
}

/// Create expression that shifts left byte[] bin starting at bitOffset for bitSize and returns byte[].
///
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 32
/// bitSize = 8
/// shift = 3
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn lshift(
    policy: &BitPolicy,
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    shift: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::LShift as i64)),
        ExpressionArgument::FilterExpression(bit_offset),
        ExpressionArgument::FilterExpression(bit_size),
        ExpressionArgument::FilterExpression(shift),
        ExpressionArgument::Value(Value::from(policy.flags)),
    ];
    add_write(bin, args)
}

/// Create expression that shifts right byte[] bin starting at bitOffset for bitSize and returns byte[].
///
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 0
/// bitSize = 9
/// shift = 1
/// bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn rshift(
    policy: &BitPolicy,
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    shift: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::RShift as i64)),
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
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 24
/// bitSize = 16
/// value = 128
/// signed = false
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn add(
    policy: &BitPolicy,
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    value: FilterExpression,
    signed: bool,
    action: BitwiseOverflowActions,
    bin: FilterExpression,
) -> FilterExpression {
    let mut flags = action as u8;
    if signed {
        flags |= INT_FLAGS_SIGNED as u8;
    }
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Add as i64)),
        ExpressionArgument::FilterExpression(bit_offset),
        ExpressionArgument::FilterExpression(bit_size),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.flags)),
        ExpressionArgument::Value(Value::from(flags)),
    ];
    add_write(bin, args)
}

/// Create expression that subtracts value from byte[] bin starting at bitOffset for bitSize and returns byte[].
/// `BitSize` must be <= 64. Signed indicates if bits should be treated as a signed number.
/// If add overflows/underflows, `BitwiseOverflowActions` is used.
///
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 24
/// bitSize = 16
/// value = 128
/// signed = false
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
pub fn subtract(
    policy: &BitPolicy,
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    value: FilterExpression,
    signed: bool,
    action: BitwiseOverflowActions,
    bin: FilterExpression,
) -> FilterExpression {
    let mut flags = action as u8;
    if signed {
        flags |= INT_FLAGS_SIGNED as u8;
    }
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Subtract as i64)),
        ExpressionArgument::FilterExpression(bit_offset),
        ExpressionArgument::FilterExpression(bit_size),
        ExpressionArgument::FilterExpression(value),
        ExpressionArgument::Value(Value::from(policy.flags)),
        ExpressionArgument::Value(Value::from(flags)),
    ];
    add_write(bin, args)
}

/// Create expression that sets value to byte[] bin starting at bitOffset for bitSize and returns byte[].
/// `BitSize` must be <= 64.
///
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 1
/// bitSize = 8
/// value = 127
/// bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]
/// ```
#[allow(clippy::trivially_copy_pass_by_ref)]
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
/// ```
/// // Bin "a" bits = [0b10000000]
/// // bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// // bitOffset = 9
/// // bitSize = 5
/// // returns [0b10000000]
///
/// use aerospike::expressions::{eq, int_val, blob_bin, blob_val};
/// use aerospike::expressions::bitwise::get;
/// eq(
///   get(int_val(9), int_val(5), blob_bin("a".to_string())),
///   blob_val(vec![0b10000000]));
/// ```
pub fn get(
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Get as i64)),
        ExpressionArgument::FilterExpression(bit_offset),
        ExpressionArgument::FilterExpression(bit_size),
    ];
    add_read(bin, ExpType::BLOB, args)
}

/// Create expression that returns integer count of set bits from byte[] bin starting at
/// bitOffset for bitSize.
///
/// ```
/// // Bin "a" bit count <= 2
/// // bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// // bitOffset = 20
/// // bitSize = 4
/// // returns 2
///
/// use aerospike::expressions::{le, int_val, blob_bin};
/// use aerospike::expressions::bitwise::count;
/// le(count(int_val(0), int_val(5), blob_bin("a".to_string())), int_val(2));
/// ```
pub fn count(
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::Count as i64)),
        ExpressionArgument::FilterExpression(bit_offset),
        ExpressionArgument::FilterExpression(bit_size),
    ];
    add_read(bin, ExpType::INT, args)
}

/// Create expression that returns integer bit offset of the first specified value bit in byte[] bin
/// starting at bitOffset for bitSize.
///
/// ```
/// // lscan(a) == 5
/// // bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// // bitOffset = 24
/// // bitSize = 8
/// // value = true
/// // returns 5
/// use aerospike::expressions::{eq, int_val, blob_bin};
/// use aerospike::expressions::bitwise::lscan;
/// eq(lscan(int_val(24), int_val(8), int_val(1), blob_bin("a".to_string())), int_val(5));
/// ```
///
pub fn lscan(
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::LScan as i64)),
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
/// ```
/// // rscan(a) == 7
/// // bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// // bitOffset = 32
/// // bitSize = 8
/// // value = true
/// // returns 7
///
/// use aerospike::expressions::{eq, int_val, blob_bin};
/// use aerospike::expressions::bitwise::rscan;
/// eq(rscan(int_val(32), int_val(8), int_val(1), blob_bin("a".to_string())), int_val(7));
/// ```
///
pub fn rscan(
    bit_offset: FilterExpression,
    bit_size: FilterExpression,
    value: FilterExpression,
    bin: FilterExpression,
) -> FilterExpression {
    let args = vec![
        ExpressionArgument::Value(Value::from(BitExpOp::RScan as i64)),
        ExpressionArgument::FilterExpression(bit_offset),
        ExpressionArgument::FilterExpression(bit_size),
        ExpressionArgument::FilterExpression(value),
    ];
    add_read(bin, ExpType::INT, args)
}

/// Create expression that returns integer from byte[] bin starting at bitOffset for bitSize.
/// Signed indicates if bits should be treated as a signed number.
///
/// ```
/// // getInt(a) == 16899
/// // bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// // bitOffset = 8
/// // bitSize = 16
/// // signed = false
/// // returns 16899
/// use aerospike::expressions::{eq, int_val, blob_bin};
/// use aerospike::expressions::bitwise::get_int;
/// eq(get_int(int_val(8), int_val(16), false, blob_bin("a".to_string())), int_val(16899));
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

#[doc(hidden)]
fn add_write(bin: FilterExpression, arguments: Vec<ExpressionArgument>) -> FilterExpression {
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
