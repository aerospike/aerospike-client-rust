// Bit operations. Create bit operations used by client operate command.
// Offset orientation is left-to-right.  Negative offsets are supported.
// If the offset is negative, the offset starts backwards from end of the bitmap.
// If an offset is out of bounds, a parameter error will be returned.
//
// Nested CDT operations are supported by optional CTX context arguments.  Example:
// bin = [[0b00000001, 0b01000010],[0b01011010]]
// Resize first bitmap (in a list of bitmaps) to 3 bytes.
// BitOperation.resize("bin", 3, BitResizeFlags.DEFAULT, CTX.listIndex(0))
// bin result = [[0b00000001, 0b01000010, 0b00000000],[0b01011010]]
//

use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::CdtContext;
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::Value;

// Bit operations. Create bit operations used by client operate command.
// Offset orientation is left-to-right.  Negative offsets are supported.
// If the offset is negative, the offset starts backwards from end of the bitmap.
// If an offset is out of bounds, a parameter error will be returned.
//
// Nested CDT operations are supported by optional CTX context arguments.  Example:
// bin = [[0b00000001, 0b01000010],[0b01011010]]
// Resize first bitmap (in a list of bitmaps) to 3 bytes.
// bin result = [[0b00000001, 0b01000010, 0b00000000],[0b01011010]]
//
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

pub enum CdtBitwiseResizeFlags {
    Default = 0,
    FromFront = 1,
    GrowOnly = 2,
    ShrinkOnly = 4,
}

pub enum CdtBitwiseWriteFlags {
    Default = 0,
    CreateOnly = 1,
    UpdateOnly = 2,
    NoFail = 4,
    Partial = 8,
}

pub enum CdtBitwiseOverflowActions {
    Fail = 0,
    Saturate = 2,
    Wrap = 4,
}
pub struct BitPolicy {
    flags: u8,
}

impl BitPolicy {
    pub const fn new(flags: u8) -> Self {
        BitPolicy { flags }
    }
}

impl Default for BitPolicy {
    fn default() -> Self {
        BitPolicy::new(CdtBitwiseWriteFlags::Default as u8)
    }
}

// Creates byte "resize" operation.
// Server resizes byte[] to byteSize according to resizeFlags.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010]
// byteSize = 4
// resizeFlags = 0
// bin result = [0b00000001, 0b01000010, 0b00000000, 0b00000000]
pub fn resize<'a>(
    bin: &'a str,
    byte_size: i64,
    resize_flags: Option<u8>,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Int(byte_size)];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    if resize_flags.is_some() {
        let flags = resize_flags.unwrap();
        args.push(CdtArgument::Byte(flags));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Resize as u8,
        args,
    };
    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates byte "insert" operation.
// Server inserts value bytes into byte[] bin at byteOffset.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// byteOffset = 1
// value = [0b11111111, 0b11000111]
// bin result = [0b00000001, 0b11111111, 0b11000111, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
pub fn insert<'a>(
    bin: &'a str,
    byte_offset: i64,
    value: &'a Value,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Int(byte_offset), CdtArgument::Value(value)];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }

    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Insert as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates byte "remove" operation.
// Server removes bytes from byte[] bin at byteOffset for byteSize.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// byteOffset = 2
// byteSize = 3
// bin result = [0b00000001, 0b01000010]
pub fn remove<'a>(
    bin: &'a str,
    byte_offset: i64,
    byte_size: i64,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Int(byte_offset), CdtArgument::Int(byte_size)];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Remove as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "set" operation.
// Server sets value on byte[] bin at bitOffset for bitSize.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 13
// bitSize = 3
// value = [0b11100000]
// bin result = [0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101]
pub fn set<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Int(bit_offset),
        CdtArgument::Int(bit_size),
        CdtArgument::Value(value),
    ];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Set as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "or" operation.
// Server performs bitwise "or" on value and byte[] bin at bitOffset for bitSize.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 17
// bitSize = 6
// value = [0b10101000]
// bin result = [0b00000001, 0b01000010, 0b01010111, 0b00000100, 0b00000101]
pub fn or<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Int(bit_offset),
        CdtArgument::Int(bit_size),
        CdtArgument::Value(value),
    ];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Or as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "exclusive or" operation.
// Server performs bitwise "xor" on value and byte[] bin at bitOffset for bitSize.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 17
// bitSize = 6
// value = [0b10101100]
// bin result = [0b00000001, 0b01000010, 0b01010101, 0b00000100, 0b00000101]
pub fn xor<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Int(bit_offset),
        CdtArgument::Int(bit_size),
        CdtArgument::Value(value),
    ];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Xor as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "and" operation.
// Server performs bitwise "and" on value and byte[] bin at bitOffset for bitSize.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 23
// bitSize = 9
// value = [0b00111100, 0b10000000]
// bin result = [0b00000001, 0b01000010, 0b00000010, 0b00000000, 0b00000101]
pub fn and<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: &'a Value,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Int(bit_offset),
        CdtArgument::Int(bit_size),
        CdtArgument::Value(value),
    ];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::And as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "not" operation.
// Server negates byte[] bin starting at bitOffset for bitSize.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 25
// bitSize = 6
// bin result = [0b00000001, 0b01000010, 0b00000011, 0b01111010, 0b00000101]
pub fn not<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Int(bit_offset), CdtArgument::Int(bit_size)];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Not as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "left shift" operation.
// Server shifts left byte[] bin starting at bitOffset for bitSize.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 32
// bitSize = 8
// shift = 3
// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00101000]
pub fn lshift<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    shift: i64,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Int(bit_offset),
        CdtArgument::Int(bit_size),
        CdtArgument::Int(shift),
    ];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::LShift as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "right shift" operation.
// Server shifts right byte[] bin starting at bitOffset for bitSize.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 0
// bitSize = 9
// shift = 1
// bin result = [0b00000000, 0b11000010, 0b00000011, 0b00000100, 0b00000101]
pub fn rshift<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    shift: i64,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Int(bit_offset),
        CdtArgument::Int(bit_size),
        CdtArgument::Int(shift),
    ];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::RShift as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "add" operation.
// Server adds value to byte[] bin starting at bitOffset for bitSize. BitSize must be <= 64.
// Signed indicates if bits should be treated as a signed number.
// If add overflows/underflows, {@link BitOverflowAction} is used.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 24
// bitSize = 16
// value = 128
// signed = false
// bin result = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b10000101]
pub fn add<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: i64,
    signed: bool,
    action: CdtBitwiseOverflowActions,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut action_flags = action as u8;
    if signed {
        action_flags |= 1;
    }
    let mut args = vec![
        CdtArgument::Int(bit_offset),
        CdtArgument::Int(bit_size),
        CdtArgument::Int(value),
    ];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    args.push(CdtArgument::Byte(action_flags));

    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Add as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "subtract" operation.
// Server subtracts value from byte[] bin starting at bitOffset for bitSize. BitSize must be <= 64.
// Signed indicates if bits should be treated as a signed number.
// If add overflows/underflows, {@link BitOverflowAction} is used.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 24
// bitSize = 16
// value = 128
// signed = false
// bin result = [0b00000001, 0b01000010, 0b00000011, 0b0000011, 0b10000101]
pub fn subtract<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: i64,
    signed: bool,
    action: CdtBitwiseOverflowActions,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut action_flags = action as u8;
    if signed {
        action_flags |= 1;
    }
    let mut args = vec![
        CdtArgument::Int(bit_offset),
        CdtArgument::Int(bit_size),
        CdtArgument::Int(value),
    ];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }
    args.push(CdtArgument::Byte(action_flags));

    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Subtract as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "setInt" operation.
// Server sets value to byte[] bin starting at bitOffset for bitSize. Size must be <= 64.
// Server does not return a value.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 1
// bitSize = 8
// value = 127
// bin result = [0b00111111, 0b11000010, 0b00000011, 0b0000100, 0b00000101]
pub fn set_int<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: i64,
    policy: Option<BitPolicy>,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![
        CdtArgument::Int(bit_offset),
        CdtArgument::Int(bit_size),
        CdtArgument::Int(value),
    ];
    if policy.is_some() {
        let policy = policy.unwrap();
        args.push(CdtArgument::Byte(policy.flags));
    }

    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::SetInt as u8,
        args,
    };

    Operation {
        op: OperationType::BitWrite,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "get" operation.
// Server returns bits from byte[] bin starting at bitOffset for bitSize.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 9
// bitSize = 5
// returns [0b1000000]
pub fn get<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Get as u8,
        args: vec![CdtArgument::Int(bit_offset), CdtArgument::Int(bit_size)],
    };

    Operation {
        op: OperationType::BitRead,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "count" operation.
// Server returns integer count of set bits from byte[] bin starting at bitOffset for bitSize.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 20
// bitSize = 4
// returns 2
pub fn count<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::Count as u8,
        args: vec![CdtArgument::Int(bit_offset), CdtArgument::Int(bit_size)],
    };

    Operation {
        op: OperationType::BitRead,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "left scan" operation.
// Server returns integer bit offset of the first specified value bit in byte[] bin
// starting at bitOffset for bitSize.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 24
// bitSize = 8
// value = true
// returns 5
pub fn lscan<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: bool,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::LScan as u8,
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Bool(value),
        ],
    };

    Operation {
        op: OperationType::BitRead,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "right scan" operation.
// Server returns integer bit offset of the last specified value bit in byte[] bin
// starting at bitOffset for bitSize.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 32
// bitSize = 8
// value = true
// returns 7
pub fn rscan<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    value: bool,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::RScan as u8,
        args: vec![
            CdtArgument::Int(bit_offset),
            CdtArgument::Int(bit_size),
            CdtArgument::Bool(value),
        ],
    };

    Operation {
        op: OperationType::BitRead,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}

// Creates bit "get integer" operation.
// Server returns integer from byte[] bin starting at bitOffset for bitSize.
// Signed indicates if bits should be treated as a signed number.
// Example:
// bin = [0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101]
// bitOffset = 8
// bitSize = 16
// signed = false
// returns 16899
pub fn get_int<'a>(
    bin: &'a str,
    bit_offset: i64,
    bit_size: i64,
    signed: bool,
    ctx: Option<&'a [CdtContext]>,
) -> Operation<'a> {
    let mut args = vec![CdtArgument::Int(bit_offset), CdtArgument::Int(bit_size)];
    if signed {
        args.push(CdtArgument::Byte(1));
    }
    let cdt_op = CdtOperation {
        op: CdtBitwiseOpType::GetInt as u8,
        args,
    };

    Operation {
        op: OperationType::BitRead,
        ctx,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtBitOp(cdt_op),
        header_only: false,
    }
}
