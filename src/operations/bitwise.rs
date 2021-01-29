// Copyright 2015-2020 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bit operations. Create bit operations used by client operate command.
//! Offset orientation is left-to-right.  Negative offsets are supported.
//! If the offset is negative, the offset starts backwards from end of the bitmap.
//! If an offset is out of bounds, a parameter error will be returned.
//!
//! Nested CDT operations are supported by optional CTX context arguments. Example:
//!
//! ```
//! use aerospike::operations::bitwise::{resize, BitwiseResizeFlags, BitPolicy};
//! // bin = [[0b00000001, 0b01000010], [0b01011010]]
//! // Resize first bitmap (in a list of bitmaps) to 3 bytes.
//! resize("bin", 3, Some(BitwiseResizeFlags::Default), &BitPolicy::default());
//! // bin result = [[0b00000001, 0b01000010, 0b00000000], [0b01011010]]
//! ```

use crate::msgpack::encoder::pack_cdt_bit_op;
use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::DEFAULT_CTX;
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::Value;

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum CdtBitwiseOpType {
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

/// `CdtBitwiseResizeFlags` specifies the bitwise operation flags for resize.
#[derive(Debug, Clone)]
pub enum BitwiseResizeFlags {
    /// Default specifies the defalt flag.
    Default = 0,
    /// FromFront Adds/removes bytes from the beginning instead of the end.
    FromFront = 1,
    /// GrowOnly will only allow the byte[] size to increase.
    GrowOnly = 2,
    /// ShrinkOnly will only allow the byte[] size to decrease.
    ShrinkOnly = 4,
}

/// `CdtBitwiseWriteFlags` specify bitwise operation policy write flags.
#[derive(Debug, Clone)]
pub enum BitwiseWriteFlags {
    /// Default allows create or update.
    Default = 0,
    /// CreateOnly specifies that:
    /// If the bin already exists, the operation will be denied.
    /// If the bin does not exist, a new bin will be created.
    CreateOnly = 1,
    /// UpdateOnly specifies that:
    /// If the bin already exists, the bin will be overwritten.
    /// If the bin does not exist, the operation will be denied.
    UpdateOnly = 2,
    /// NoFail specifies not to raise error if operation is denied.
    NoFail = 4,
    /// Partial allows other valid operations to be committed if this operations is
    /// denied due to flag constraints.
    Partial = 8,
}

/// `CdtBitwiseOverflowActions` specifies the action to take when bitwise add/subtract results in overflow/underflow.
#[derive(Debug, Clone)]
pub enum BitwiseOverflowActions {
    /// Fail specifies to fail operation with error.
    Fail = 0,
    /// Saturate specifies that in add/subtract overflows/underflows, set to max/min value.
    /// Example: MAXINT + 1 = MAXINT
    Saturate = 2,
    /// Wrap specifies that in add/subtract overflows/underflows, wrap the value.
    /// Example: MAXINT + 1 = -1
    Wrap = 4,
}
/// `BitPolicy` determines the Bit operation policy.
#[derive(Debug, Clone, Copy)]
pub struct BitPolicy {
    /// The flags determined by CdtBitwiseWriteFlags
    pub flags: u8,
}

impl BitPolicy {
    /// Creates a new `BitPolicy` with defined `CdtBitwiseWriteFlags`
    pub const fn new(flags: u8) -> Self {
        BitPolicy { flags }
    }
}

impl Default for BitPolicy {
    /// Returns the default `BitPolicy`
    fn default() -> Self {
        BitPolicy::new(BitwiseWriteFlags::Default as u8)
    }
}

/// Creates byte "resize" operation.
/// Server resizes byte[] to byteSize according to resizeFlags.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010]
/// byteSize = 4
/// resizeFlags = 0
/// bin result = [0b00000001, 0b01000010, 0b00000000, 0b00000000]
/// ```
pub fn resize<'a>(
    bin: &'a str,
    byte_size: i64,
    resize_flags: Option<BitwiseResizeFlags>,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Int(byte_size),
        CdtArgument::Byte(policy.flags as u8),
    ];
    if let Some(resize_flags) = resize_flags {
        args.push(CdtArgument::Byte(resize_flags as u8));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Resize as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args,
    };
    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates byte "insert" operation.
/// Server inserts value bytes into byte[] bin at byteOffset.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// byteOffset = 1
/// value = [0b11111111, 0b11000111]
/// bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// ```
pub fn insert<'a>(
    bin: &'a str,
    byte_offset: i64,
    value: &'a Value,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Insert as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(byte_offset),
            CdtArgument::Value(value),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates byte "remove" operation.
/// Server removes bytes from byte[] bin at byteOffset for byteSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// byteOffset = 2
/// byteSize = 3
/// bin result = [0b00000001, 0b01000010]
/// ```
pub fn remove<'a>(
    bin: &'a str,
    byte_offset: i64,
    byte_size: i64,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Remove as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(byte_offset),
            CdtArgument::Int(byte_size),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "set" operation.
/// Server sets value on byte[] bin at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 13
/// bitSize = 3
/// value = [0b11100000]
/// bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]
/// ```
pub fn set<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Set as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Value(value),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "or" operation.
/// Server performs bitwise "or" on value and byte[] bin at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 17
/// bitSize = 6
/// value = [0b10101000]
/// bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]
/// ```
pub fn or<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Or as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Value(value),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "exclusive or" operation.
/// Server performs bitwise "xor" on value and byte[] bin at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 17
/// bitSize = 6
/// value = [0b10101100]
/// bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]
/// ```
pub fn xor<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Xor as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Value(value),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "and" operation.
/// Server performs bitwise "and" on value and byte[] bin at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 23
/// bitSize = 9
/// value = [0b00111100, 0b10000000]
/// bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]
/// ```
pub fn and<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::And as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Value(value),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "not" operation.
/// Server negates byte[] bin starting at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 25
/// bitSize = 6
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]
/// ```
pub fn not<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Not as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "left shift" operation.
/// Server shifts left byte[] bin starting at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 32
/// bitSize = 8
/// shift = 3
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]
/// ```
pub fn lshift<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    shift: i64,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::LShift as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Int(shift),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "right shift" operation.
/// Server shifts right byte[] bin starting at bitOffset for bitSize.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 0
/// bitSize = 9
/// shift = 1
/// bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]
/// ```
pub fn rshift<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    shift: i64,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        encoder: Box::new(pack_cdt_bit_op),
        op: CdtBitwiseOpType::RShift as u8,
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Int(shift),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "add" operation.
/// Server adds value to byte[] bin starting at bitOffset for bitSize. `BitSize` must be <= 64.
/// Signed indicates if bits should be treated as a signed number.
/// If add overflows/underflows, `CdtBitwiseOverflowAction` is used.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 24
/// bitSize = 16
/// value = 128
/// signed = false
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]
/// ```
pub fn add<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: i64,
    signed: bool,
    action: BitwiseOverflowActions,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let mut action_flags = action as u8;
    if signed {
        action_flags |= 1;
    }

    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Add as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Int(value),
            CdtArgument::Byte(policy.flags as u8),
            CdtArgument::Byte(action_flags),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "subtract" operation.
/// Server subtracts value from byte[] bin starting at bitOffset for bitSize. `bit_size` must be <= 64.
/// Signed indicates if bits should be treated as a signed number.
/// If add overflows/underflows, `CdtBitwiseOverflowAction` is used.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 24
/// bitSize = 16
/// value = 128
/// signed = false
/// bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]
/// ```
pub fn subtract<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: i64,
    signed: bool,
    action: BitwiseOverflowActions,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let mut action_flags = action as u8;
    if signed {
        action_flags |= 1;
    }

    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Subtract as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Int(value),
            CdtArgument::Byte(policy.flags as u8),
            CdtArgument::Byte(action_flags),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "setInt" operation.
/// Server sets value to byte[] bin starting at bitOffset for bitSize. Size must be <= 64.
/// Server does not return a value.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 1
/// bitSize = 8
/// value = 127
/// bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]
/// ```
pub fn set_int<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: i64,
    policy: &'a BitPolicy,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::SetInt as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Int(value),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };

    Operation {
        op: OperationType::BitWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "get" operation.
/// Server returns bits from byte[] bin starting at bitOffset for bitSize.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 9
/// bitSize = 5
/// returns [0b1000000]
/// ```
pub fn get(bin: &str, bit_offset: i64, bit_size: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Get as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![CdtArgument::Int(bit_offset), CdtArgument::Int(bit_size)],
    };

    Operation {
        op: OperationType::BitRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "count" operation.
/// Server returns integer count of set bits from byte[] bin starting at bitOffset for bitSize.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 20
/// bitSize = 4
/// returns 2
/// ```
pub fn count(bin: &str, bit_offset: i64, bit_size: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Count as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![CdtArgument::Int(bit_offset), CdtArgument::Int(bit_size)],
    };

    Operation {
        op: OperationType::BitRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "left scan" operation.
/// Server returns integer bit offset of the first specified value bit in byte[] bin
/// starting at bitOffset for bitSize.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 24
/// bitSize = 8
/// value = true
/// returns 5
/// ```
pub fn lscan(bin: &str, bit_offset: i64, bit_size: i64, value: bool) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::LScan as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Bool(value),
        ],
    };

    Operation {
        op: OperationType::BitRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "right scan" operation.
/// Server returns integer bit offset of the last specified value bit in byte[] bin
/// starting at bitOffset for bitSize.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 32
/// bitSize = 8
/// value = true
/// returns 7
/// ```
pub fn rscan(bin: &str, bit_offset: i64, bit_size: i64, value: bool) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::RScan as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Bool(value),
        ],
    };

    Operation {
        op: OperationType::BitRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}

/// Creates bit "get integer" operation.
/// Server returns integer from byte[] bin starting at bitOffset for bitSize.
/// Signed indicates if bits should be treated as a signed number.
///
/// Example:
/// ```text
/// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
/// bitOffset = 8
/// bitSize = 16
/// signed = false
/// returns 16899
/// ```
pub fn get_int(bin: &str, bit_offset: i64, bit_size: i64, signed: bool) -> Operation {
    let mut args = vec![CdtArgument::Int(bit_offset), CdtArgument::Int(bit_size)];
    if signed {
        args.push(CdtArgument::Byte(1));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::GetInt as u8,
        encoder: Box::new(pack_cdt_bit_op),
        args,
    };

    Operation {
        op: OperationType::BitRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
    }
}
