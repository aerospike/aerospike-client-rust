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

//! Functions used for Filter Expressions. This module requires Aerospike Server version >= 5.2

pub mod bitwise;
pub mod hll;
pub mod lists;
pub mod maps;
pub mod regex_flag;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

use crate::commands::buffer::Buffer;
use crate::msgpack::encoder::{pack_array_begin, pack_integer, pack_raw_string, pack_value};
use crate::operations::cdt_context::CdtContext;
use crate::value::MapLike;
use crate::{Error, Result};
use crate::{ParticleType, Value};
use std::fmt::Debug;

/// Expression data types for use in filter expressions on Map and List operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpType {
    /// NIL Expression Type
    NIL = 0,
    /// BOOLEAN Expression Type
    BOOL = 1,
    /// INTEGER Expression Type
    INT = 2,
    /// STRING Expression Type
    STRING = 3,
    /// LIST Expression Type
    LIST = 4,
    /// MAP Expression Type
    MAP = 5,
    /// BLOB Expression Type
    BLOB = 6,
    /// FLOAT Expression Type
    FLOAT = 7,
    /// GEO String Expression Type
    GEO = 8,
    /// HLL Expression Type
    HLL = 9,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ExpOp {
    Unknown = 0,
    EQ = 1,
    NE = 2,
    GT = 3,
    GE = 4,
    LT = 5,
    LE = 6,
    Regex = 7,
    Geo = 8,
    And = 16,
    Or = 17,
    Not = 18,
    Xor = 19,
    Add = 20,
    Sub = 21,
    Mul = 22,
    Div = 23,
    Pow = 24,
    Log = 25,
    Mod = 26,
    Abs = 27,
    Floor = 28,
    Ceil = 29,
    ToInt = 30,
    ToFloat = 31,
    IntAnd = 32,
    IntOr = 33,
    IntXor = 34,
    IntNot = 35,
    IntLshift = 36,
    IntRshift = 37,
    IntARshift = 38,
    IntCount = 39,
    IntLscan = 40,
    IntRscan = 41,
    Min = 50,
    Max = 51,
    DigestModulo = 64,
    DeviceSize = 65,
    LastUpdate = 66,
    SinceUpdate = 67,
    VoidTime = 68,
    TTL = 69,
    SetName = 70,
    KeyExists = 71,
    IsTombstone = 72,
    MemorySize = 73,
    RecordSize = 74,
    Key = 80,
    Bin = 81,
    BinType = 82,
    ResultRemove = 100,
    VarBuiltIn = 122,
    Cond = 123,
    Var = 124,
    Let = 125,
    Quoted = 126,
    Call = 127,
}

pub(crate) const MODIFY: i64 = 0x40;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ExpressionArgument {
    Value(Value),
    FilterExpression(Expression),
    Context(Vec<CdtContext>),
    CdtSelectPathArg(crate::operations::path::SelectFlag, Vec<CdtContext>),
    CdtModifyPathArg(
        crate::operations::path::ModifyFlag,
        Expression,
        Expression,
        Vec<CdtContext>,
    ),
}

/// Identifies which element of a loop variable to use in path expressions.
/// Requires Aerospike Server version >= 8.1.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopVarPart(pub i64);

impl LoopVarPart {
    /// Map key part of the loop variable.
    pub const MAP_KEY: LoopVarPart = LoopVarPart(0);
    /// Value part of the loop variable (list element or map value).
    pub const VALUE: LoopVarPart = LoopVarPart(1);
    /// Index part of the loop variable (list index).
    pub const INDEX: LoopVarPart = LoopVarPart(2);
}

/// Filter expression, which can be applied to most commands to control which records are affected.
///
/// Filter expressions are created using the functions in the
/// [expressions](crate::expressions) module and its submodules.
#[derive(Debug, Clone, PartialEq)]
pub struct Expression {
    /// The Operation code
    cmd: Option<ExpOp>,
    /// The Primary Value of the Operation
    val: Option<Value>,
    /// The Bin to use it on (REGEX for example)
    bin: Option<Box<Expression>>,
    /// The additional flags for the Operation (REGEX or `return_type` of Module for example)
    flags: Option<i64>,
    /// The optional Module flag for Module operations or Bin Types
    module: Option<ExpType>,
    /// Sub commands for the `CmdExp` operation
    exps: Option<Vec<Expression>>,
    /// Optional Arguments (CDT)
    arguments: Option<Vec<ExpressionArgument>>,
    /// Pre-packed expression bytes (used by [`from_base64`]).
    bytes: Option<Vec<u8>>,
}

impl Expression {
    pub(crate) fn new(
        cmd: Option<ExpOp>,
        val: Option<Value>,
        bin: Option<Expression>,
        flags: Option<i64>,
        module: Option<ExpType>,
        exps: Option<Vec<Expression>>,
    ) -> Expression {
        let bin = bin.map(Box::new);
        Expression {
            cmd,
            val,
            bin,
            flags,
            module,
            exps,
            arguments: None,
            bytes: None,
        }
    }

    fn pack_expression(&self, exps: &[Expression], buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        if let Some(val) = &self.val {
            // DEF expression
            size += pack_raw_string(buf, &val.to_string());
            size += exps[0].pack(buf)?;
        } else {
            // Normal Expressions
            match self.cmd.unwrap() {
                ExpOp::Let => {
                    // Let wire format: LET <defname1>, <defexp1>, <defname2>, <defexp2>, ..., <scope exp>
                    let count = (exps.len() - 1) * 2 + 2;
                    size += pack_array_begin(buf, count);
                }
                _ => {
                    size += pack_array_begin(buf, exps.len() + 1);
                }
            }
            size += pack_integer(buf, self.cmd.unwrap() as i64);
            for exp in exps {
                size += exp.pack(buf)?;
            }
        }
        Ok(size)
    }

    fn pack_command(&self, cmd: ExpOp, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;

        match cmd {
            ExpOp::Regex => {
                size += pack_array_begin(buf, 4);
                // The Operation
                size += pack_integer(buf, cmd as i64);
                // Regex Flags
                size += pack_integer(buf, self.flags.unwrap());
                // Raw String is needed instead of the msgpack String that the pack_value method would use.
                size += pack_raw_string(buf, &self.val.clone().unwrap().to_string());
                // The Bin
                size += self.bin.clone().unwrap().pack(buf)?;
            }
            ExpOp::Call => {
                // Packing logic for Module
                size += pack_array_begin(buf, 5);
                // The Operation
                size += pack_integer(buf, cmd as i64);
                // The Module Operation
                size += pack_integer(buf, self.module.unwrap() as i64);
                // The Module (List/Map or Bitwise)
                size += pack_integer(buf, self.flags.unwrap());
                // Encoding the Arguments
                // bin_from_arg is set when CdtModifyPathArg is encountered; its bin_exp
                // is written as the Call's bin instead of self.bin (which is None for that case).
                let mut bin_from_arg: Option<&Expression> = None;
                if let Some(args) = &self.arguments {
                    let mut len = 0;
                    for arg in args {
                        // First match to estimate the Size and write the Context
                        match arg {
                            ExpressionArgument::Value(_)
                            | ExpressionArgument::FilterExpression(_) => len += 1,
                            // Path args are written as direct elements: [0xfe, flat_ctx, flag, ...]
                            ExpressionArgument::CdtSelectPathArg(_, _) => len += 3,
                            ExpressionArgument::CdtModifyPathArg(_, _, _, _) => len += 4,
                            ExpressionArgument::Context(ctx) => {
                                if !ctx.is_empty() {
                                    size += pack_array_begin(buf, 3);
                                    size += pack_integer(buf, 0xff);
                                    size += pack_array_begin(buf, ctx.len() * 2);

                                    for c in ctx {
                                        size += pack_integer(buf, i64::from(c.id));
                                        if let Some(ref exp) = c.expression {
                                            size += exp.pack_binary(buf)?;
                                        } else {
                                            size += pack_value(buf, &c.value)?;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    size += pack_array_begin(buf, len);
                    // Second match to write the real values
                    for arg in args {
                        match arg {
                            ExpressionArgument::Value(val) => {
                                size += pack_value(buf, val)?;
                            }
                            ExpressionArgument::FilterExpression(cmd) => {
                                size += cmd.pack(buf)?;
                            }
                            ExpressionArgument::CdtSelectPathArg(flag, ctx) => {
                                // Write [0xfe, flat_ctx, flag] as 3 direct args
                                size += pack_integer(buf, 0xfe);
                                size += pack_flat_ctx(buf, ctx)?;
                                size += pack_integer(buf, flag.0);
                            }
                            ExpressionArgument::CdtModifyPathArg(
                                flag,
                                bin_exp,
                                modify_exp,
                                ctx,
                            ) => {
                                // Write [0xfe, flat_ctx, flag|0x04, modify_exp] as 4 direct args
                                size += pack_integer(buf, 0xfe);
                                size += pack_flat_ctx(buf, ctx)?;
                                size += pack_integer(buf, flag.0 | 0x04);
                                size += modify_exp.pack(buf)?;
                                bin_from_arg = Some(bin_exp);
                            }
                            ExpressionArgument::Context(_) => {}
                        }
                    }
                } else {
                    // No Arguments
                    size += pack_value(buf, &self.val.clone().unwrap())?;
                }
                // Write the Bin (5th element of the Call array)
                if let Some(bin) = bin_from_arg {
                    size += bin.pack(buf)?;
                } else {
                    size += self.bin.clone().unwrap().pack(buf)?;
                }
            }
            ExpOp::VarBuiltIn => {
                // Loop variable built-in: array[3]: [122, ExpType, LoopVarPart]
                size += pack_array_begin(buf, 3);
                size += pack_integer(buf, cmd as i64);
                size += pack_integer(buf, self.flags.unwrap()); // ExpType value
                size += pack_value(buf, self.val.as_ref().unwrap())?; // LoopVarPart
            }
            ExpOp::Bin => {
                // Bin Encoder
                size += pack_array_begin(buf, 3);
                // The Bin Operation
                size += pack_integer(buf, cmd as i64);
                // The Bin Type (INT/String etc.)
                size += pack_integer(buf, self.module.unwrap() as i64);
                // The name - Raw String is needed instead of the msgpack String that the pack_value method would use.
                size += pack_raw_string(buf, &self.val.clone().unwrap().to_string());
            }
            ExpOp::BinType | ExpOp::Var => {
                // BinType/Var encoder
                size += pack_array_begin(buf, 2);
                // BinType/Var Operation
                size += pack_integer(buf, cmd as i64);
                // The name - Raw String is needed instead of the msgpack String that the pack_value method would use.
                size += pack_raw_string(buf, &self.val.clone().unwrap().to_string());
            }
            _ => {
                // Packing logic for all other Ops
                if let Some(value) = &self.val {
                    // Operation has a Value
                    size += pack_array_begin(buf, 2);
                    // Write the Operation
                    size += pack_integer(buf, cmd as i64);
                    // Write the Value
                    size += pack_value(buf, value)?;
                } else {
                    // Operation has no Value
                    size += pack_array_begin(buf, 1);
                    // Write the Operation
                    size += pack_integer(buf, cmd as i64);
                }
            }
        }

        Ok(size)
    }

    fn pack_value(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        // Packing logic for Value based Ops
        pack_value(buf, &self.val.clone().unwrap())
    }

    /// Returns the packed size of the expression.
    pub(crate) fn size(&self) -> Result<usize> {
        self.pack(&mut None)
    }

    /// Packs the expression.
    pub(crate) fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        if let Some(bytes) = &self.bytes {
            if let Some(buf) = buf {
                return Ok(buf.write_bytes(bytes));
            }
            return Ok(bytes.len());
        }

        let mut size = 0;
        if let Some(exps) = &self.exps {
            size += self.pack_expression(exps, buf)?;
        } else if let Some(cmd) = self.cmd {
            size += self.pack_command(cmd, buf)?;
        } else {
            size += self.pack_value(buf)?;
        }

        Ok(size)
    }

    /// Encode the expression to a base64 string.
    pub fn base64(&self) -> Result<String> {
        let sz = self.size()?;
        let mut buf = Buffer::new(sz);
        buf.resize_buffer(sz)?;
        self.pack(&mut Some(&mut buf))?;
        Ok(BASE64.encode(&buf.data_buffer[..buf.data_offset]))
    }

    /// Packs this expression wrapped in a msgpack binary value (bin8/bin16/bin32 format).
    /// Used when an expression context needs to be encoded as binary bytes within CDT operations.
    /// Calls `size()` to determine the expression size without pre-allocating, then writes
    /// the binary header and the expression bytes lazily.
    #[must_use]
    pub(crate) fn pack_binary(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let exp_size = self.size()?;
        let header_size = if exp_size < 256 {
            if let Some(ref mut b) = *buf {
                b.write_u8(0xc4);
                b.write_u8(exp_size as u8);
            }
            2
        } else if exp_size < 65536 {
            if let Some(ref mut b) = *buf {
                b.write_u8(0xc5);
                b.write_u16(exp_size as u16);
            }
            3
        } else {
            if let Some(ref mut b) = *buf {
                b.write_u8(0xc6);
                b.write_u32(exp_size as u32);
            }
            5
        };
        self.pack(buf)?;
        Ok(header_size + exp_size)
    }
}

/// Creates a record key expression of specified type.
/// ```
/// use aerospike::expressions::{ExpType, ge, int_val, key};
/// // Integer record key >= 100000
/// ge(key(ExpType::INT), int_val(10000));
/// ```
pub fn key(exp_type: ExpType) -> Expression {
    Expression::new(
        Some(ExpOp::Key),
        Some(Value::from(exp_type as i64)),
        None,
        None,
        None,
        None,
    )
}

/// Creates an expression from a base64-encoded expression string.
pub fn from_base64(b64: &str) -> Result<Expression> {
    let bytes = BASE64
        .decode(b64)
        .map_err(|e| Error::BadResponse(format!("Invalid base64 expression: {e}")))?;
    Ok(Expression {
        cmd: None,
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: None,
        arguments: None,
        bytes: Some(bytes),
    })
}

/// Creates an expression that returns whether the primary key is stored in the record meta data.
///
/// This would occur when [`WritePolicy::send_key`](crate::WritePolicy::send_key) is true on record write.
/// ```
/// // Key exists in record meta data
/// use aerospike::expressions::key_exists;
/// key_exists();
/// ```
pub fn key_exists() -> Expression {
    Expression::new(Some(ExpOp::KeyExists), None, None, None, None, None)
}

/// Creates 64 bit int bin expression.
/// ```
/// // Integer bin "a" == 500
/// use aerospike::expressions::{int_bin, int_val, eq};
/// eq(int_bin("a".to_string()), int_val(500));
/// ```
pub fn int_bin(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::INT),
        None,
    )
}

/// Creates boolean bin expression.
/// ```
/// // Boolean bin "a" == true
/// use aerospike::expressions::{bool_bin, bool_val, eq};
/// eq(bool_bin("a".to_string()), bool_val(true));
/// ```
pub fn bool_bin(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::BOOL),
        None,
    )
}

/// Creates string bin expression.
/// ```
/// // String bin "a" == "views"
/// use aerospike::expressions::{eq, string_bin, string_val};
/// eq(string_bin("a".to_string()), string_val("views".to_string()));
/// ```
pub fn string_bin(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::STRING),
        None,
    )
}

/// Creates blob bin expression.
/// ```
/// // String bin "a" == [1,2,3]
/// use aerospike::expressions::{eq, blob_bin, blob_val};
/// let blob: Vec<u8> = vec![1,2,3];
/// eq(blob_bin("a".to_string()), blob_val(blob));
/// ```
pub fn blob_bin(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::BLOB),
        None,
    )
}

/// Creates 64 bit float bin expression.
/// ```
/// use aerospike::expressions::{float_val, float_bin, eq};
/// // Integer bin "a" == 500.5
/// eq(float_bin("a".to_string()), float_val(500.5));
/// ```
pub fn float_bin(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::FLOAT),
        None,
    )
}

/// Creates geo bin expression.
/// ```
/// // String bin "a" == region
/// use aerospike::expressions::{eq, geo_bin, string_val};
/// let region = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-122.0, 37.5], 50000.0] }";
/// eq(geo_bin("a".to_string()), string_val(region.to_string()));
/// ```
pub fn geo_bin(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::GEO),
        None,
    )
}

/// Creates list bin expression.
/// ```
/// use aerospike::expressions::{ExpType, eq, int_val, list_bin};
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::lists::get_by_index;
/// // String bin a[2] == 3
/// eq(get_by_index(ListReturnType::Values, ExpType::INT, int_val(2), list_bin("a".to_string()), &[]), int_val(3));
/// ```
pub fn list_bin(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::LIST),
        None,
    )
}

/// Creates map bin expression.
///
/// ```
/// // Bin a["key"] == "value"
/// use aerospike::expressions::{ExpType, string_val, map_bin, eq};
/// use aerospike::MapReturnType;
/// use aerospike::expressions::maps::get_by_key;
///
/// eq(
///     get_by_key(MapReturnType::Value, ExpType::STRING, string_val("key".to_string()), map_bin("a".to_string()), &[]),
///     string_val("value".to_string()));
/// ```
pub fn map_bin(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::MAP),
        None,
    )
}

/// Creates an HLL bin expression.
///
/// ```
/// use aerospike::expressions::{gt, list_val, hll_bin, int_val};
/// use aerospike::operations::hll::HLLPolicy;
/// use aerospike::Value;
/// use aerospike::expressions::hll::add;
///
/// // Add values to HLL bin "a" and check count > 7
/// let list = vec![Value::from(1)];
/// gt(add(HLLPolicy::default(), list_val(list), hll_bin("a".to_string())), int_val(7));
/// ```
pub fn hll_bin(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::HLL),
        None,
    )
}

/// Creates an expression that returns if bin of specified name exists.
/// ```
/// // Bin "a" exists in record
/// use aerospike::expressions::bin_exists;
/// bin_exists("a".to_string());
/// ```
pub fn bin_exists(name: String) -> Expression {
    ne(bin_type(name), int_val(ParticleType::NULL as i64))
}

/// Creates an expression that returns bin's integer particle type.
/// ```
/// use aerospike::ParticleType;
/// use aerospike::expressions::{eq, bin_type, int_val};
/// // Bin "a" particle type is a list
/// eq(bin_type("a".to_string()), int_val(ParticleType::LIST as i64));
/// ```
pub fn bin_type(name: String) -> Expression {
    Expression::new(
        Some(ExpOp::BinType),
        Some(Value::from(name)),
        None,
        None,
        None,
        None,
    )
}

/// Creates an expression that returns record set name string.
/// ```
/// use aerospike::expressions::{eq, set_name, string_val};
/// // Record set name == "myset
/// eq(set_name(), string_val("myset".to_string()));
/// ```
pub fn set_name() -> Expression {
    Expression::new(Some(ExpOp::SetName), None, None, None, None, None)
}

/// Creates expression that returns the record size. This expression usually evaluates
/// quickly because record meta data is cached in memory.
///
/// Requires server version 7.0+. This expression replaces [`device_size()`](device_size) and
/// [`memory_size()`](memory_size) since those older expressions are equivalent on server version 7.0+.
///
/// ```
/// use aerospike::expressions::{ge, record_size, int_val};
/// // Record device size >= 100 KB
/// ge(record_size(), int_val(100*1024));
/// ```
pub fn record_size() -> Expression {
    Expression::new(Some(ExpOp::RecordSize), None, None, None, None, None)
}

/// Creates an expression that returns record size on disk.
/// If server storage-engine is memory, then zero is returned.
///
/// Deprecated: `memory_size` has been deprecated since server version 8.1. Use [`record_size()`].
/// ```
/// #  #![deny(warnings)]
/// # #![allow(deprecated)]
/// use aerospike::expressions::{ge, device_size, int_val};
/// // Record device size >= 100 KB
/// ge(device_size(), int_val(100*1024));
/// ```
#[deprecated]
pub fn device_size() -> Expression {
    Expression::new(Some(ExpOp::DeviceSize), None, None, None, None, None)
}

/// Creates expression that returns record size in memory.
///
/// If server storage-engine is not memory nor data-in-memory, then zero is returned.
/// This expression usually evaluates quickly because record meta data is cached in memory.
///
/// Requires server version between 5.3 inclusive and 7.0 exclusive.
/// Use [`record_size()`](record_size) for server version 7.0+.
///
/// Deprecated: `memory_size` has been deprecated since server version 8.1. Use [`record_size()`].
/// ```
/// # #![deny(warnings)]
/// # #![allow(deprecated)]
/// use aerospike::expressions::{ge, memory_size, int_val};
/// // Record device size >= 100 KB
/// ge(memory_size(), int_val(100*1024));
/// ```
#[deprecated]
pub fn memory_size() -> Expression {
    Expression::new(Some(ExpOp::MemorySize), None, None, None, None, None)
}

/// Creates an expression that returns record last update time expressed as 64 bit integer
/// nanoseconds since 1970-01-01 epoch.
/// ```
/// // Record last update time >=2020-08-01
/// use aerospike::expressions::{ge, last_update, float_val};
/// ge(last_update(), float_val(1.5962E+18));
/// ```
pub fn last_update() -> Expression {
    Expression::new(Some(ExpOp::LastUpdate), None, None, None, None, None)
}

/// Creates expression that returns milliseconds since the record was last updated.
/// This expression usually evaluates quickly because record meta data is cached in memory.
///
/// ```
/// // Record last updated more than 2 hours ago
/// use aerospike::expressions::{gt, int_val, since_update};
/// gt(since_update(), int_val(2 * 60 * 60 * 1000));
/// ```
pub fn since_update() -> Expression {
    Expression::new(Some(ExpOp::SinceUpdate), None, None, None, None, None)
}

/// Creates an expression that returns record expiration time expressed as 64 bit integer
/// nanoseconds since 1970-01-01 epoch.
/// ```
/// // Expires on 2020-08-01
/// use aerospike::expressions::{and, ge, last_update, float_val, lt};
/// and(vec![ge(last_update(), float_val(1.5962E+18)), lt(last_update(), float_val(1.5963E+18))]);
/// ```
pub fn void_time() -> Expression {
    Expression::new(Some(ExpOp::VoidTime), None, None, None, None, None)
}

/// Creates an expression that returns record expiration time (time to live) in integer seconds.
/// ```
/// // Record expires in less than 1 hour
/// use aerospike::expressions::{lt, ttl, int_val};
/// lt(ttl(), int_val(60*60));
/// ```
pub fn ttl() -> Expression {
    Expression::new(Some(ExpOp::TTL), None, None, None, None, None)
}

/// Creates expression that returns if record has been deleted and is still in tombstone state.
/// This expression usually evaluates quickly because record meta data is cached in memory.
///
/// ```
/// // Deleted records that are in tombstone state.
/// use aerospike::expressions::{is_tombstone};
/// is_tombstone();
/// ```
pub fn is_tombstone() -> Expression {
    Expression::new(Some(ExpOp::IsTombstone), None, None, None, None, None)
}
/// Creates an expression that returns record digest modulo as integer.
/// ```
/// // Records that have digest(key) % 3 == 1
/// use aerospike::expressions::{int_val, eq, digest_modulo};
/// eq(digest_modulo(3), int_val(1));
/// ```
pub fn digest_modulo(modulo: i64) -> Expression {
    Expression::new(
        Some(ExpOp::DigestModulo),
        Some(Value::from(modulo)),
        None,
        None,
        None,
        None,
    )
}

/// Creates a regular expression string comparison expression.
/// ```
/// use aerospike::RegexFlag;
/// use aerospike::expressions::{regex_compare, string_bin};
/// // Select string bin "a" that starts with "prefix" and ends with "suffix".
/// // Ignore case and do not match newline.
/// regex_compare("prefix.*suffix".to_string(), RegexFlag::ICASE as i64 | RegexFlag::NEWLINE as i64, string_bin("a".to_string()));
/// ```
pub fn regex_compare(regex: String, flags: i64, bin: Expression) -> Expression {
    Expression::new(
        Some(ExpOp::Regex),
        Some(Value::from(regex)),
        Some(bin),
        Some(flags),
        None,
        None,
    )
}

/// Creates a geospatial comparison expression.
/// ```
/// use aerospike::expressions::{geo_compare, geo_bin, geo_val};
/// // Query region within coordinates.
/// let region = "{\"type\": \"Polygon\", \"coordinates\": [ [[-122.500000, 37.000000],[-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000, 38.080000], [-122.500000, 37.000000]] ] }";
/// geo_compare(geo_bin("a".to_string()), geo_val(region.to_string()));
/// ```
pub fn geo_compare(left: Expression, right: Expression) -> Expression {
    Expression::new(
        Some(ExpOp::Geo),
        None,
        None,
        None,
        None,
        Some(vec![left, right]),
    )
}

/// Creates a 64-bit integer value.
pub fn int_val(val: i64) -> Expression {
    Expression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Creates a boolean value.
pub fn bool_val(val: bool) -> Expression {
    Expression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Creates a string bin value.
pub fn string_val(val: String) -> Expression {
    Expression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Creates a 64-bit float bin value.
pub fn float_val(val: f64) -> Expression {
    Expression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Creates a blob bin value.
pub fn blob_val(val: Vec<u8>) -> Expression {
    Expression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Creates a list bin value.
pub fn list_val(val: Vec<Value>) -> Expression {
    Expression::new(
        Some(ExpOp::Quoted),
        Some(Value::from(val)),
        None,
        None,
        None,
        None,
    )
}

/// Creates a map bin value.
#[allow(clippy::implicit_hasher)]
pub fn map_val<M: MapLike<Value, Value>>(val: M) -> Expression {
    let val = match val.value() {
        (Some(m), None) => Value::HashMap(m),
        (None, Some(m)) => Value::OrderedMap(m),
        _ => unreachable!(),
    };
    Expression::new(None, Some(val), None, None, None, None)
}

/// Creates a geospatial JSON string value.
pub fn geo_val(val: String) -> Expression {
    Expression::new(None, Some(Value::GeoJSON(val)), None, None, None, None)
}

/// Creates a nil value.
pub fn nil() -> Expression {
    Expression::new(None, Some(Value::Nil), None, None, None, None)
}

/// Creates an infinity value.
pub fn infinity() -> Expression {
    Expression::new(None, Some(Value::Infinity), None, None, None, None)
}

/// Creates a wildcard value.
pub fn wildcard() -> Expression {
    Expression::new(None, Some(Value::Wildcard), None, None, None, None)
}

/// Creates "not" operator expression.
/// ```
/// // ! (a == 0 || a == 10)
/// use aerospike::expressions::{not, or, eq, int_bin, int_val};
/// not(or(vec![eq(int_bin("a".to_string()), int_val(0)), eq(int_bin("a".to_string()), int_val(10))]));
/// ```
pub fn not(exp: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::Not),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![exp]),
        arguments: None,
        bytes: None,
    }
}

/// Creates "and" (&&) operator that applies to a variable number of expressions.
/// ```
/// // (a > 5 || a == 0) && b < 3
/// use aerospike::expressions::{and, or, gt, int_bin, int_val, eq, lt};
/// and(vec![or(vec![gt(int_bin("a".to_string()), int_val(5)), eq(int_bin("a".to_string()), int_val(0))]), lt(int_bin("b".to_string()), int_val(3))]);
/// ```
pub const fn and(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::And),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates "or" (||) operator that applies to a variable number of expressions.
/// ```
/// // a == 0 || b == 0
/// use aerospike::expressions::{or, eq, int_bin, int_val};
/// or(vec![eq(int_bin("a".to_string()), int_val(0)), eq(int_bin("b".to_string()), int_val(0))]);
/// ```
pub const fn or(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Or),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates "xor" (^) operator that applies to a variable number of expressions.
/// ```
/// // a == 0 ^ b == 0
/// use aerospike::expressions::{xor, eq, int_bin, int_val};
/// xor(vec![eq(int_bin("a".to_string()), int_val(0)), eq(int_bin("b".to_string()), int_val(0))]);
/// ```
pub const fn xor(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Xor),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates equal (==) expression.
/// ```
/// // a == 11
/// use aerospike::expressions::{eq, int_bin, int_val};
/// eq(int_bin("a".to_string()), int_val(11));
/// ```
pub fn eq(left: Expression, right: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::EQ),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
        bytes: None,
    }
}

/// Creates not equal (!=) expression
/// ```
/// // a != 13
/// use aerospike::expressions::{ne, int_bin, int_val};
/// ne(int_bin("a".to_string()), int_val(13));
/// ```
pub fn ne(left: Expression, right: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::NE),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
        bytes: None,
    }
}

/// Creates greater than (>) operation.
/// ```
/// // a > 8
/// use aerospike::expressions::{gt, int_bin, int_val};
/// gt(int_bin("a".to_string()), int_val(8));
/// ```
pub fn gt(left: Expression, right: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::GT),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
        bytes: None,
    }
}

/// Creates greater than or equal (>=) operation.
/// ```
/// use aerospike::expressions::{ge, int_bin, int_val};
/// // a >= 88
/// ge(int_bin("a".to_string()), int_val(88));
/// ```
pub fn ge(left: Expression, right: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::GE),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
        bytes: None,
    }
}

/// Creates less than (<) operation.
/// ```
/// // a < 1000
/// use aerospike::expressions::{lt, int_bin, int_val};
/// lt(int_bin("a".to_string()), int_val(1000));
/// ```
pub fn lt(left: Expression, right: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::LT),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
        bytes: None,
    }
}

/// Creates less than or equals (<=) operation.
/// ```
/// use aerospike::expressions::{le, int_bin, int_val};
/// // a <= 1
/// le(int_bin("a".to_string()), int_val(1));
/// ```
pub fn le(left: Expression, right: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::LE),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
        bytes: None,
    }
}

/// Creates "add" (+) operator that applies to a variable number of expressions.
///
/// Return sum of all `FilterExpressions` given. All arguments must resolve to the same type (integer or float).
/// Requires server version 5.6.0+.
/// ```
/// use aerospike::expressions::{eq, num_add, int_bin, int_val};
/// // a + b + c == 10
/// eq(num_add(vec![int_bin("a".to_string()), int_bin("b".to_string()), int_bin("c".to_string())]), int_val(10));
/// ```
pub const fn num_add(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Add),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates "subtract" (-) operator that applies to a variable number of expressions.
///
/// If only one `FilterExpressions` is provided, return the negation of that argument.
/// Otherwise, return the sum of the 2nd to Nth `FilterExpressions` subtracted from the 1st
/// `FilterExpressions`. All `FilterExpressions` must resolve to the same type (integer or float).
/// Requires server version 5.6.0+.
/// ```
/// use aerospike::expressions::{gt, num_sub, int_bin, int_val};
/// // a - b - c > 10
/// gt(num_sub(vec![int_bin("a".to_string()), int_bin("b".to_string()), int_bin("c".to_string())]), int_val(10));
/// ```
pub const fn num_sub(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Sub),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates "multiply" (*) operator that applies to a variable number of expressions.
///
/// Return the product of all `FilterExpressions`. If only one `FilterExpressions` is supplied, return
/// that `FilterExpressions`. All `FilterExpressions` must resolve to the same type (integer or float).
/// Requires server version 5.6.0+.
/// ```
/// use aerospike::expressions::{lt, num_mul, int_val, int_bin};
/// // a * b * c < 100
/// lt(num_mul(vec![int_bin("a".to_string()), int_bin("b".to_string()), int_bin("c".to_string())]), int_val(100));
/// ```
pub const fn num_mul(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Mul),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates "divide" (/) operator that applies to a variable number of expressions.
///
/// If there is only one `FilterExpressions`, returns the reciprocal for that `FilterExpressions`.
/// Otherwise, return the first `FilterExpressions` divided by the product of the rest.
/// All `FilterExpressions` must resolve to the same type (integer or float).
/// Requires server version 5.6.0+.
/// ```
/// use aerospike::expressions::{lt, int_val, int_bin, num_div};
/// // a / b / c > 1
/// lt(num_div(vec![int_bin("a".to_string()), int_bin("b".to_string()), int_bin("c".to_string())]), int_val(1));
/// ```
pub const fn num_div(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Div),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates "power" operator that raises a "base" to the "exponent" power.
/// All arguments must resolve to floats.
/// Requires server version 5.6.0+.
/// ```
/// // pow(a, 2.0) == 4.0
/// use aerospike::expressions::{eq, num_pow, float_bin, float_val};
/// eq(num_pow(float_bin("a".to_string()), float_val(2.0)), float_val(4.0));
/// ```
pub fn num_pow(base: Expression, exponent: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::Pow),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![base, exponent]),
        arguments: None,
        bytes: None,
    }
}

/// Creates "log" operator for logarithm of "num" with base "base".
/// All arguments must resolve to floats.
/// Requires server version 5.6.0+.
/// ```
/// // log(a, 2.0) == 4.0
/// use aerospike::expressions::{eq, float_bin, float_val, num_log};
/// eq(num_log(float_bin("a".to_string()), float_val(2.0)), float_val(4.0));
/// ```
pub fn num_log(num: Expression, base: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::Log),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![num, base]),
        arguments: None,
        bytes: None,
    }
}

/// Creates "modulo" (%) operator that determines the remainder of "numerator"
/// divided by "denominator". All arguments must resolve to integers.
/// Requires server version 5.6.0+.
/// ```
/// // a % 10 == 0
/// use aerospike::expressions::{eq, num_mod, int_val, int_bin};
/// eq(num_mod(int_bin("a".to_string()), int_val(10)), int_val(0));
/// ```
pub fn num_mod(numerator: Expression, denominator: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::Mod),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![numerator, denominator]),
        arguments: None,
        bytes: None,
    }
}

/// Creates operator that returns absolute value of a number.
/// All arguments must resolve to integer or float.
/// Requires server version 5.6.0+.
/// ```
/// // abs(a) == 1
/// use aerospike::expressions::{eq, int_val, int_bin, num_abs};
/// eq(num_abs(int_bin("a".to_string())), int_val(1));
/// ```
pub fn num_abs(value: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::Abs),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![value]),
        arguments: None,
        bytes: None,
    }
}

/// Creates expression that rounds a floating point number down to the closest integer value.
/// The return type is float.
// Requires server version 5.6.0+.
/// ```
/// // floor(2.95) == 2.0
/// use aerospike::expressions::{eq, num_floor, float_val};
/// eq(num_floor(float_val(2.95)), float_val(2.0));
/// ```
pub fn num_floor(num: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::Floor),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![num]),
        arguments: None,
        bytes: None,
    }
}

/// Creates expression that rounds a floating point number up to the closest integer value.
/// The return type is float.
/// Requires server version 5.6.0+.
/// ```
/// // ceil(2.15) == 3.0
/// use aerospike::expressions::{float_val, num_ceil, ge};
/// ge(num_ceil(float_val(2.15)), float_val(3.0));
/// ```
pub fn num_ceil(num: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::Ceil),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![num]),
        arguments: None,
        bytes: None,
    }
}

/// Creates expression that converts an integer to a float.
/// Requires server version 5.6.0+.
/// ```
/// // int(2.5) == 2
/// use aerospike::expressions::{float_val, eq, to_int, int_val};
/// eq(to_int(float_val(2.5)), int_val(2));
/// ```
pub fn to_int(num: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::ToInt),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![num]),
        arguments: None,
        bytes: None,
    }
}

/// Creates expression that converts a float to an integer.
/// Requires server version 5.6.0+.
/// ```
/// // float(2) == 2.0
/// use aerospike::expressions::{float_val, eq, to_float, int_val};
/// eq(to_float(int_val(2)), float_val(2.0));
/// ```
pub fn to_float(num: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::ToFloat),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![num]),
        arguments: None,
        bytes: None,
    }
}

/// Creates integer "and" (&) operator that is applied to two or more integers.
/// All arguments must resolve to integers.
/// Requires server version 5.6.0+.
/// ```
/// // a & 0xff == 0x11
/// use aerospike::expressions::{eq, int_val, int_and, int_bin};
/// eq(int_and(vec![int_bin("a".to_string()), int_val(0xff)]), int_val(0x11));
/// ```
pub const fn int_and(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntAnd),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates integer "or" (|) operator that is applied to two or more integers.
/// All arguments must resolve to integers.
/// Requires server version 5.6.0+.
/// ```
/// // a | 0xff == 0xff
/// use aerospike::expressions::{eq, int_val, int_or, int_bin};
/// eq(int_or(vec![int_bin("a".to_string()), int_val(0xFF)]), int_val(0xFF));
/// ```
pub const fn int_or(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntOr),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates integer "xor" (^) operator that is applied to two or more integers.
/// All arguments must resolve to integers.
/// Requires server version 5.6.0+.
/// ```
/// // a ^ b == 16
/// use aerospike::expressions::{eq, int_val, int_xor, int_bin};
/// eq(int_xor(vec![int_bin("a".to_string()), int_bin("b".to_string())]), int_val(16));
/// ```
pub const fn int_xor(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntXor),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates integer "not" (~) operator.
/// Requires server version 5.6.0+.
/// ```
/// // ~a == 7
/// use aerospike::expressions::{eq, int_val, int_not, int_bin};
/// eq(int_not(int_bin("a".to_string())), int_val(7));
/// ```
pub fn int_not(exp: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntNot),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![exp]),
        arguments: None,
        bytes: None,
    }
}

/// Creates integer "left shift" (<<) operator.
/// Requires server version 5.6.0+.
/// ```
/// // a << 8 > 0xff
/// use aerospike::expressions::{int_val, int_bin, gt, int_lshift};
/// gt(int_lshift(int_bin("a".to_string()), int_val(8)), int_val(0xff));
/// ```
pub fn int_lshift(value: Expression, shift: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntLshift),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![value, shift]),
        arguments: None,
        bytes: None,
    }
}

/// Creates integer "logical right shift" (>>>) operator.
/// Requires server version 5.6.0+.
/// ```
/// // a >> 8 > 0xff
/// use aerospike::expressions::{int_val, int_bin, gt, int_rshift};
/// gt(int_rshift(int_bin("a".to_string()), int_val(8)), int_val(0xff));
/// ```
pub fn int_rshift(value: Expression, shift: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntRshift),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![value, shift]),
        arguments: None,
        bytes: None,
    }
}

/// Creates integer "arithmetic right shift" (>>) operator.
/// The sign bit is preserved and not shifted.
/// Requires server version 5.6.0+.
/// ```
/// // a >>> 8 > 0xff
/// use aerospike::expressions::{int_val, int_bin, gt, int_arshift};
/// gt(int_arshift(int_bin("a".to_string()), int_val(8)), int_val(0xff));
/// ```
pub fn int_arshift(value: Expression, shift: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntARshift),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![value, shift]),
        arguments: None,
        bytes: None,
    }
}

/// Creates expression that returns count of integer bits that are set to 1.
/// Requires server version 5.6.0+.
/// ```
/// // count(a) == 4
/// use aerospike::expressions::{int_val, int_bin, int_count, eq};
/// eq(int_count(int_bin("a".to_string())), int_val(4));
/// ```
pub fn int_count(exp: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntCount),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![exp]),
        arguments: None,
        bytes: None,
    }
}

/// Creates expression that scans integer bits from left (most significant bit) to right, looking for a search bit value.
///
/// When the search value is found, the index of that bit (where the most significant bit is
/// index 0) is returned. If "search" is true, the scan will search for the bit
/// value 1. If "search" is false it will search for bit value 0.
/// Requires server version 5.6.0+.
/// ```
/// // lscan(a, true) == 4
/// use aerospike::expressions::{int_val, int_bin, eq, int_lscan, bool_val};
/// eq(int_lscan(int_bin("a".to_string()), bool_val(true)), int_val(4));
/// ```
pub fn int_lscan(value: Expression, search: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntLscan),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![value, search]),
        arguments: None,
        bytes: None,
    }
}

/// Creates expression that scans integer bits from right (least significant bit) to left, looking for a search bit value.
///
/// When the search value is found, the index of that bit (where the most significant bit is
/// index 0) is returned. If "search" is true, the scan will search for the bit
/// value 1. If "search" is false it will search for bit value 0.
/// Requires server version 5.6.0+.
/// ```
/// // rscan(a, true) == 4
/// use aerospike::expressions::{int_val, int_bin, eq, int_rscan, bool_val};
/// eq(int_rscan(int_bin("a".to_string()), bool_val(true)), int_val(4));
/// ```
pub fn int_rscan(value: Expression, search: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::IntRscan),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![value, search]),
        arguments: None,
        bytes: None,
    }
}

/// Creates expression that returns the minimum value in a variable number of expressions.
/// All arguments must be the same type (integer or float).
/// Requires server version 5.6.0+.
/// ```
/// // min(a, b, c) > 0
/// use aerospike::expressions::{int_val, int_bin, gt, min};
/// gt(min(vec![int_bin("a".to_string()),int_bin("b".to_string()),int_bin("c".to_string())]), int_val(0));
/// ```
pub const fn min(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Min),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Creates expression that returns the maximum value in a variable number of expressions.
/// All arguments must be the same type (integer or float).
/// Requires server version 5.6.0+.
/// ```
/// // max(a, b, c) > 100
/// use aerospike::expressions::{int_val, int_bin, gt, max};
/// gt(max(vec![int_bin("a".to_string()),int_bin("b".to_string()),int_bin("c".to_string())]), int_val(100));
/// ```
pub const fn max(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Max),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

//--------------------------------------------------
// Variables
//--------------------------------------------------

/// Conditionally select an expression from a variable number of expression pairs
/// followed by default expression action.
/// Requires server version 5.6.0+.
/// ```
/// // Args Format: bool exp1, action exp1, bool exp2, action exp2, ..., action-default
/// // Apply operator based on type.
///
/// use aerospike::expressions::{cond, int_bin, eq, int_val, num_add, num_sub, num_mul};
/// cond(
///   vec![
///     eq(int_bin("type".to_string()), int_val(0)), num_add(vec![int_bin("val1".to_string()), int_bin("val2".to_string())]),
///     eq(int_bin("type".to_string()), int_val(1)), num_sub(vec![int_bin("val1".to_string()), int_bin("val2".to_string())]),
///     eq(int_bin("type".to_string()), int_val(2)), num_mul(vec![int_bin("val1".to_string()), int_bin("val2".to_string())]),
///     int_val(-1)
///   ]
/// );
/// ```
pub const fn cond(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Cond),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Define variables and expressions in scope.
/// Requires server version 5.6.0+.
/// ```
/// // 5 < a < 10
/// use aerospike::expressions::{exp_let, def, int_bin, and, lt, int_val, var};
/// exp_let(
///   vec![
///     def("x".to_string(), int_bin("a".to_string())),
///     and(vec![
///       lt(int_val(5), var("x".to_string()),),
///       lt(var("x".to_string()), int_val(10))
///     ])
///   ]
/// );
/// ```
pub const fn exp_let(exps: Vec<Expression>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Let),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
        bytes: None,
    }
}

/// Assign variable to an expression that can be accessed later.
/// Requires server version 5.6.0+.
/// ```
/// // 5 < a < 10
/// use aerospike::expressions::{exp_let, def, int_bin, and, lt, int_val, var};
/// exp_let(
///   vec![
///     def("x".to_string(), int_bin("a".to_string())),
///     and(vec![
///       lt(int_val(5), var("x".to_string()),),
///       lt(var("x".to_string()), int_val(10))
///     ])
///   ]
/// );
/// ```
pub fn def(name: String, value: Expression) -> Expression {
    Expression {
        cmd: None,
        val: Some(Value::from(name)),
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![value]),
        arguments: None,
        bytes: None,
    }
}

/// Retrieve expression value from a variable.
/// Requires server version 5.6.0+.
pub fn var(name: String) -> Expression {
    Expression {
        cmd: Some(ExpOp::Var),
        val: Some(Value::from(name)),
        bin: None,
        flags: None,
        module: None,
        exps: None,
        arguments: None,
        bytes: None,
    }
}

/// Creates unknown value. Used to intentionally fail an expression.
///
/// The failure can be ignored with [`ExpWriteFlags::EvalNoFail`](crate::operations::exp::ExpWriteFlags::EvalNoFail)
/// or [`ExpReadFlags::EvalNoFail`](crate::operations::exp::ExpReadFlags::EvalNoFail).
/// Requires server version 5.6.0+.
///
/// ```
/// // double v = balance - 100.0;
/// // return (v > 0.0)? v : unknown;
/// use aerospike::expressions::{exp_let, def, num_sub, float_bin, float_val, cond, ge, var, unknown};
/// exp_let(
///     vec![
///         def("v".to_string(), num_sub(vec![float_bin("balance".to_string()), float_val(100.0)])),
///         cond(vec![ge(var("v".to_string()), float_val(0.0)), var("v".to_string())]),
///         unknown()
///     ]
/// );
/// ```
pub const fn unknown() -> Expression {
    Expression {
        cmd: Some(ExpOp::Unknown),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: None,
        arguments: None,
        bytes: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_roundtrip_int_eq() {
        let expr = eq(int_bin("bin".to_string()), int_val(42));
        let b64 = expr.base64().unwrap();
        assert!(!b64.is_empty());

        let decoded = from_base64(&b64).unwrap();
        let re_encoded = decoded.base64().unwrap();
        assert_eq!(b64, re_encoded);
    }

    #[test]
    fn base64_roundtrip_string_compare() {
        let expr = eq(
            string_bin("name".to_string()),
            string_val("hello".to_string()),
        );
        let b64 = expr.base64().unwrap();

        let decoded = from_base64(&b64).unwrap();
        let re_encoded = decoded.base64().unwrap();
        assert_eq!(b64, re_encoded);
    }

    #[test]
    fn base64_roundtrip_complex_expression() {
        let expr = and(vec![
            gt(int_bin("age".to_string()), int_val(18)),
            lt(int_bin("age".to_string()), int_val(65)),
            eq(
                string_bin("status".to_string()),
                string_val("active".to_string()),
            ),
        ]);
        let b64 = expr.base64().unwrap();

        let decoded = from_base64(&b64).unwrap();
        let re_encoded = decoded.base64().unwrap();
        assert_eq!(b64, re_encoded);
    }

    #[test]
    fn from_base64_invalid_input() {
        let result = from_base64("not-valid-base64!!!");
        assert!(result.is_err());
    }

    #[test]
    fn base64_roundtrip_bool_and_float() {
        let expr = or(vec![
            eq(float_bin("score".to_string()), float_val(3.14)),
            bool_val(true),
        ]);
        let b64 = expr.base64().unwrap();

        let decoded = from_base64(&b64).unwrap();
        let re_encoded = decoded.base64().unwrap();
        assert_eq!(b64, re_encoded);
    }
}

// ===== Path Expression helpers =====

/// Pack CDT context in "flat" format used by path-based operations.
/// Expression values are written directly (no binary wrapper).
pub(crate) fn pack_flat_ctx(buf: &mut Option<&mut Buffer>, ctx: &[CdtContext]) -> Result<usize> {
    let mut size = 0;
    size += pack_array_begin(buf, ctx.len() * 2);
    for c in ctx {
        size += pack_integer(buf, i64::from(c.id));
        if let Some(ref exp) = c.expression {
            size += exp.pack(buf)?;
        } else {
            size += pack_value(buf, &c.value)?;
        }
    }
    Ok(size)
}

/// Pack the CDT path select bytes: array[3]: [0xfe, flat_ctx, flag]
pub(crate) fn pack_path_select(
    buf: &mut Option<&mut Buffer>,
    ctx: &[CdtContext],
    flag: i64,
) -> Result<usize> {
    let mut size = 0;
    size += pack_array_begin(buf, 3);
    size += pack_integer(buf, 0xfe);
    size += pack_flat_ctx(buf, ctx)?;
    size += pack_integer(buf, flag);
    Ok(size)
}

/// Pack CDT path modify content: array[4]: [0xfe, flat_ctx, flag|0x04, exp]
/// The 0x04 bit (EXP_PATH_MODIFY_APPLY) is always OR'd into the flag.
/// The expression is packed directly (no binary wrapper).
pub(crate) fn pack_path_modify_exp(
    buf: &mut Option<&mut Buffer>,
    ctx: &[CdtContext],
    flag: i64,
    exp: &Expression,
) -> Result<usize> {
    let mut size = 0;
    size += pack_array_begin(buf, 4);
    size += pack_integer(buf, 0xfe);
    size += pack_flat_ctx(buf, ctx)?;
    size += pack_integer(buf, flag | 0x04);
    size += exp.pack(buf)?;
    Ok(size)
}

// ===== Loop Variable Expressions =====

/// Retrieve the boolean part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_bool_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::BOOL as i64),
        None,
        None,
    )
}

/// Retrieve the integer part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_int_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::INT as i64),
        None,
        None,
    )
}

/// Retrieve the float part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_float_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::FLOAT as i64),
        None,
        None,
    )
}

/// Retrieve the string part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_string_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::STRING as i64),
        None,
        None,
    )
}

/// Retrieve the list part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_list_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::LIST as i64),
        None,
        None,
    )
}

/// Retrieve the map part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_map_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::MAP as i64),
        None,
        None,
    )
}

/// Retrieve the blob part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_blob_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::BLOB as i64),
        None,
        None,
    )
}

/// Retrieve the hll part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_hll_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::HLL as i64),
        None,
        None,
    )
}

/// Retrieve the nil part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_nil_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::NIL as i64),
        None,
        None,
    )
}

/// Retrieve the GeoJSON part of a loop variable.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_geo_json_loop_var(part: LoopVarPart) -> Expression {
    Expression::new(
        Some(ExpOp::VarBuiltIn),
        Some(Value::Int(part.0)),
        None,
        Some(ExpType::GEO as i64),
        None,
        None,
    )
}

/// Remove the expression result from the return value.
/// Requires Aerospike Server version >= 8.1.1.
pub fn exp_remove_result() -> Expression {
    Expression::new(Some(ExpOp::ResultRemove), None, None, None, None, None)
}

// ===== Path-based Expression Operations =====

/// Create an expression that selects from a CDT bin using a path context.
/// Requires Aerospike Server version >= 8.1.1.
///
/// # Errors
///
/// Returns an error if the path bytes cannot be packed.
pub fn exp_select_by_path(
    return_type: ExpType,
    flag: crate::operations::path::SelectFlag,
    bin_exp: Expression,
    ctx: &[CdtContext],
) -> Expression {
    Expression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(bin_exp)),
        flags: Some(0),
        module: Some(return_type),
        exps: None,
        arguments: Some(vec![ExpressionArgument::CdtSelectPathArg(
            flag,
            ctx.to_vec(),
        )]),
        bytes: None,
    }
}

/// Create an expression that modifies a CDT bin using a path context.
/// Requires Aerospike Server version >= 8.1.1.
///
/// # Errors
///
/// Returns an error if the path bytes cannot be packed.
pub fn exp_modify_by_path(
    return_type: ExpType,
    flag: crate::operations::path::ModifyFlag,
    bin_exp: Expression,
    modify_exp: Expression,
    ctx: &[CdtContext],
) -> Expression {
    Expression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: None, // bin_exp is stored in CdtModifyPathArg and written from there
        flags: Some(MODIFY),
        module: Some(return_type),
        exps: None,
        arguments: Some(vec![ExpressionArgument::CdtModifyPathArg(
            flag,
            bin_exp,
            modify_exp,
            ctx.to_vec(),
        )]),
        bytes: None,
    }
}
