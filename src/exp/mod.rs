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

//! Functions used for Filter Expressions. This Module requires Aerospike Server Version >= 5.2

pub mod bit_exp;
pub mod list_exp;
pub mod map_exp;

use crate::commands::buffer::Buffer;
use crate::errors::Result;
use crate::msgpack::encoder::{pack_array_begin, pack_integer, pack_raw_string, pack_value};
use crate::operations::cdt_context::CdtContext;
use crate::Value;
use std::collections::HashMap;
use std::fmt::Debug;

/// Expression Data Types for usage in some `FilterExpressions` on for example Map and List
#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum ExpOp {
    EQ = 1,
    NE = 2,
    GT = 3,
    GE = 4,
    LT = 5,
    LE = 6,
    REGEX = 7,
    GEO = 8,
    AND = 16,
    OR = 17,
    NOT = 18,
    DigestModulo = 64,
    DeviceSize = 65,
    LastUpdate = 66,
    SinceUpdate = 67,
    VoidTime = 68,
    TTL = 69,
    SetName = 70,
    KeyExists = 71,
    IsTombstone = 72,
    KEY = 80,
    BIN = 81,
    BinType = 82,
    Quoted = 126,
    Call = 127,
}

#[doc(hidden)]
pub const MODIFY: i64 = 0x40;

// Const for Calendar Exps. Not implemented yet!
// const NANOS_PER_MILLIS: i64 = 1000000;

#[doc(hidden)]
pub const SERIAL_VERSION_ID: i64 = 1;

#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum ExpressionArgument {
    Value(Value),
    FilterExpression(FilterExpression),
    Context(Vec<CdtContext>),
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct FilterExpression {
    /// The Operation code
    pub cmd: Option<ExpOp>,
    /// The Primary Value of the Operation
    pub val: Option<Value>,
    /// The Bin to use it on (REGEX for example)
    pub bin: Option<Box<FilterExpression>>,
    /// The additional flags for the Operation (REGEX or return_type of Module for example)
    pub flags: Option<i64>,
    /// The optional Module flag for Module operations or Bin Types
    pub module: Option<ExpType>,
    /// Sub commands for the CmdExp operation
    pub exps: Option<Vec<FilterExpression>>,

    pub arguments: Option<Vec<ExpressionArgument>>,
}

#[doc(hidden)]
impl FilterExpression {
    pub fn new(
        cmd: Option<ExpOp>,
        val: Option<Value>,
        bin: Option<FilterExpression>,
        flags: Option<i64>,
        module: Option<ExpType>,
        exps: Option<Vec<FilterExpression>>,
    ) -> FilterExpression {
        if let Some(bin) = bin {
            FilterExpression {
                cmd,
                val,
                bin: Some(Box::new(bin)),
                flags,
                module,
                exps,
                arguments: None,
            }
        } else {
            FilterExpression {
                cmd,
                val,
                bin: None,
                flags,
                module,
                exps,
                arguments: None,
            }
        }
    }

    pub fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        if let Some(exps) = &self.exps {
            size += pack_array_begin(buf, exps.len() + 1)?;
            size += pack_integer(buf, self.cmd.unwrap() as i64)?;
            for exp in exps {
                size += exp.pack(buf)?;
            }
        } else if let Some(cmd) = self.cmd {
            if cmd as i64 == ExpOp::REGEX as i64 {
                // Packing logic for Regex
                size += pack_array_begin(buf, 4)?;
                // The Operation
                size += pack_integer(buf, cmd as i64)?;
                // Regex Flags
                size += pack_integer(buf, self.flags.unwrap())?;
                // Raw String is needed instead of the msgpack String that the pack_value method would use.
                size += pack_raw_string(buf, &self.val.clone().unwrap().to_string())?;
                // The Bin
                size += self.bin.clone().unwrap().pack(buf)?;
            } else if cmd as i64 == ExpOp::Call as i64 {
                // Packing logic for Module
                size += pack_array_begin(buf, 5)?;
                // The Operation
                size += pack_integer(buf, cmd as i64)?;
                // The Module Operation
                size += pack_integer(buf, self.module.unwrap() as i64)?;
                // The Module (List/Map or Bitwise)
                size += pack_integer(buf, self.flags.unwrap())?;
                // Encoding the Arguments
                if let Some(args) = &self.arguments {
                    let mut len = 0;
                    for arg in args {
                        // First match to estimate the Size and write the Context
                        match arg {
                            ExpressionArgument::Value(_)
                            | ExpressionArgument::FilterExpression(_) => len += 1,
                            ExpressionArgument::Context(ctx) => {
                                if !ctx.is_empty() {
                                    pack_array_begin(buf, 3)?;
                                    pack_integer(buf, 0xff)?;
                                    pack_array_begin(buf, ctx.len() * 2)?;

                                    for c in ctx {
                                        pack_integer(buf, i64::from(c.id))?;
                                        pack_value(buf, &c.value)?;
                                    }
                                }
                            }
                        }
                    }
                    size += pack_array_begin(buf, len)?;
                    // Second match to write the real values
                    for arg in args {
                        match arg {
                            ExpressionArgument::Value(val) => {
                                size += pack_value(buf, val)?;
                            }
                            ExpressionArgument::FilterExpression(cmd) => {
                                size += cmd.pack(buf)?;
                            }
                            _ => {}
                        }
                    }
                } else {
                    // No Arguments
                    size += pack_value(buf, &self.val.clone().unwrap())?;
                }
                // Write the Bin
                size += self.bin.clone().unwrap().pack(buf)?;
            } else if cmd as i64 == ExpOp::BIN as i64 {
                // Bin Encoder
                size += pack_array_begin(buf, 3)?;
                // The Bin Operation
                size += pack_integer(buf, cmd as i64)?;
                // The Bin Type (INT/String etc.)
                size += pack_integer(buf, self.module.unwrap() as i64)?;
                // The name - Raw String is needed instead of the msgpack String that the pack_value method would use.
                size += pack_raw_string(buf, &self.val.clone().unwrap().to_string())?;
            } else if cmd as i64 == ExpOp::BinType as i64 {
                // BinType encoder
                size += pack_array_begin(buf, 2)?;
                // BinType Operation
                size += pack_integer(buf, cmd as i64)?;
                // The name - Raw String is needed instead of the msgpack String that the pack_value method would use.
                size += pack_raw_string(buf, &self.val.clone().unwrap().to_string())?;
            } else {
                // Packing logic for all other Ops
                if let Some(value) = &self.val {
                    // Operation has a Value
                    size += pack_array_begin(buf, 2)?;
                    // Write the Operation
                    size += pack_integer(buf, cmd as i64)?;
                    // Write the Value
                    size += pack_value(buf, value)?;
                } else {
                    // Operation has no Value
                    size += pack_array_begin(buf, 1)?;
                    // Write the Operation
                    size += pack_integer(buf, cmd as i64)?;
                }
            }
        } else {
            // Packing logic for Value based Ops
            size += pack_value(buf, &self.val.clone().unwrap())?;
        }

        Ok(size)
    }
}

/// Basic Expression functions and single Bin interaction
pub struct Expression {}

impl Expression {
    /// Create a record key expression of specified type.
    /// ```
    /// use aerospike::exp::{Expression, ExpType};
    /// // Integer record key >= 100000
    /// Expression::ge(Expression::key(ExpType::INT), Expression::int_val(10000));
    /// ```
    pub fn key(exp_type: ExpType) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::KEY),
            Some(Value::from(exp_type as i64)),
            None,
            None,
            None,
            None,
        )
    }

    /// Create function that returns if the primary key is stored in the record meta data
    /// as a boolean expression. This would occur when `send_key` is true on record write.
    /// ```
    /// use aerospike::exp::Expression;
    /// // Key exists in record meta data
    /// Expression::key_exists();
    /// ```
    pub fn key_exists() -> FilterExpression {
        FilterExpression::new(Some(ExpOp::KeyExists), None, None, None, None, None)
    }

    /// Create bin expression of specified type.
    /// ```
    /// use aerospike::exp::{Expression, ExpType};
    /// // String bin "a" == "views"
    /// Expression::eq(Expression::bin("a".to_string(), ExpType::STRING), Expression::string_val("views".to_string()));
    /// ```
    pub fn bin(name: String, exp_type: ExpType) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(exp_type),
            None,
        )
    }

    /// Create 64 bit int bin expression.
    /// ```
    /// use aerospike::exp::Expression;
    /// // Integer bin "a" == 500
    /// Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(500));
    /// ```
    pub fn int_bin(name: String) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::INT),
            None,
        )
    }

    /// Create string bin expression.
    /// ```
    /// use aerospike::exp::Expression;
    /// // String bin "a" == "views"
    /// Expression::eq(Expression::string_bin("a".to_string()), Expression::string_val("views".to_string()));
    /// ```
    pub fn string_bin(name: String) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::STRING),
            None,
        )
    }

    /// Create blob bin expression.
    /// ```
    /// use aerospike::exp::Expression;
    /// // String bin "a" == [1,2,3]
    /// let blob: Vec<u8> = vec![1,2,3];
    /// Expression::eq(Expression::blob_bin("a".to_string()), Expression::blob_val(blob));
    /// ```
    pub fn blob_bin(name: String) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::BLOB),
            None,
        )
    }

    /// Create 64 bit float bin expression.
    /// ```
    /// use aerospike::exp::Expression;
    /// // Integer bin "a" == 500.5
    /// Expression::eq(Expression::float_bin("a".to_string()), Expression::float_val(500.5));
    /// ```
    pub fn float_bin(name: String) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::FLOAT),
            None,
        )
    }

    /// Create geo bin expression.
    /// ```
    /// use aerospike::exp::Expression;
    /// // String bin "a" == region
    /// let region = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-122.0, 37.5], 50000.0] }";
    /// Expression::eq(Expression::geo_bin("a".to_string()), Expression::string_val(region.to_string()));
    /// ```
    pub fn geo_bin(name: String) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::GEO),
            None,
        )
    }

    /// Create list bin expression.
    /// ```
    /// use aerospike::exp::{Expression, ExpType};
    /// use aerospike::operations::lists::ListReturnType;
    /// use aerospike::exp::list_exp::ListExpression;
    /// // String bin a[2] == 3
    /// Expression::eq(ListExpression::get_by_index(ListReturnType::Values, ExpType::INT, Expression::int_val(2), Expression::list_bin("a".to_string()), &[]), Expression::int_val(3));
    /// ```
    pub fn list_bin(name: String) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::LIST),
            None,
        )
    }

    /// Create map bin expression.
    ///
    /// ```
    /// // Bin a["key"] == "value"
    /// use aerospike::exp::{Expression, ExpType};
    /// use aerospike::exp::map_exp::MapExpression;
    /// use aerospike::MapReturnType;
    /// Expression::eq(
    ///     MapExpression::get_by_key(MapReturnType::Value, ExpType::STRING, Expression::string_val("key".to_string()), Expression::map_bin("a".to_string()), &[]),
    ///     Expression::string_val("value".to_string()));
    /// ```
    pub fn map_bin(name: String) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::MAP),
            None,
        )
    }

    /*pub fn hll_bin(name: String) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::HLL),
            None,
        )
    }*/

    /// Create function that returns if bin of specified name exists.
    /// ```
    /// use aerospike::exp::Expression;
    /// // Bin "a" exists in record
    /// Expression::bin_exists("a".to_string());
    /// ```
    pub fn bin_exists(name: String) -> FilterExpression {
        Expression::ne(Expression::bin_type(name), Expression::int_val(0))
    }

    /// Create function that returns bin's integer particle type.
    /// ```
    /// use aerospike::exp::Expression;
    /// use aerospike::ParticleType;
    /// // Bin "a" particle type is a list
    /// Expression::eq(Expression::bin_type("a".to_string()), Expression::int_val(ParticleType::LIST as i64));
    /// ```
    pub fn bin_type(name: String) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::BinType),
            Some(Value::from(name)),
            None,
            None,
            None,
            None,
        )
    }

    /// Create function that returns record set name string.
    /// ```
    /// use aerospike::exp::Expression;
    /// // Record set name == "myset
    /// Expression::eq(Expression::set_name(), Expression::string_val("myset".to_string()));
    /// ```
    pub fn set_name() -> FilterExpression {
        FilterExpression::new(Some(ExpOp::SetName), None, None, None, None, None)
    }

    /// Create function that returns record size on disk.
    /// If server storage-engine is memory, then zero is returned.
    /// ```
    /// use aerospike::exp::Expression;
    /// // Record device size >= 100 KB
    /// Expression::ge(Expression::device_size(), Expression::int_val(100*1024));
    /// ```
    pub fn device_size() -> FilterExpression {
        FilterExpression::new(Some(ExpOp::DeviceSize), None, None, None, None, None)
    }

    /// Create function that returns record last update time expressed as 64 bit integer
    /// nanoseconds since 1970-01-01 epoch.
    /// ```
    /// use aerospike::exp::Expression;
    /// // Record last update time >=2020-08-01
    /// Expression::ge(Expression::last_update(), Expression::float_val(1.5962E+18));
    /// ```
    pub fn last_update() -> FilterExpression {
        FilterExpression::new(Some(ExpOp::LastUpdate), None, None, None, None, None)
    }

    /// Create expression that returns milliseconds since the record was last updated.
    /// This expression usually evaluates quickly because record meta data is cached in memory.
    ///
    /// ```
    /// // Record last updated more than 2 hours ago
    /// use aerospike::exp::Expression;
    /// Expression::gt(Expression::since_update(), Expression::int_val(2 * 60 * 60 * 1000));
    /// ```
    pub fn since_update() -> FilterExpression {
        FilterExpression::new(Some(ExpOp::SinceUpdate), None, None, None, None, None)
    }

    /// Create function that returns record expiration time expressed as 64 bit integer
    /// nanoseconds since 1970-01-01 epoch.
    /// ```
    /// use aerospike::exp::Expression;
    /// // Expires on 2020-08-01
    /// Expression::and(vec![Expression::ge(Expression::last_update(), Expression::float_val(1.5962E+18)), Expression::lt(Expression::last_update(), Expression::float_val(1.5963E+18))]);
    /// ```
    pub fn void_time() -> FilterExpression {
        FilterExpression::new(Some(ExpOp::VoidTime), None, None, None, None, None)
    }

    /// Create function that returns record expiration time (time to live) in integer seconds.
    /// ```
    /// // Record expires in less than 1 hour
    /// use aerospike::exp::Expression;
    /// Expression::lt(Expression::ttl(), Expression::int_val(60*60));
    /// ```
    pub fn ttl() -> FilterExpression {
        FilterExpression::new(Some(ExpOp::TTL), None, None, None, None, None)
    }

    /// Create expression that returns if record has been deleted and is still in tombstone state.
    /// This expression usually evaluates quickly because record meta data is cached in memory.
    ///
    /// ```
    /// // Deleted records that are in tombstone state.
    /// use aerospike::exp::Expression;
    /// Expression::is_tombstone();
    /// ```
    pub fn is_tombstone() -> FilterExpression {
        FilterExpression::new(Some(ExpOp::IsTombstone), None, None, None, None, None)
    }
    /// Create function that returns record digest modulo as integer.
    /// ```
    /// // Records that have digest(key) % 3 == 1
    /// use aerospike::exp::Expression;
    /// Expression::eq(Expression::digest_modulo(3), Expression::int_val(1));
    /// ```
    pub fn digest_modulo(modulo: i64) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::DigestModulo),
            Some(Value::from(modulo)),
            None,
            None,
            None,
            None,
        )
    }

    /// Create function like regular expression string operation.
    /// ```
    /// use aerospike::exp::Expression;
    /// use aerospike::RegexFlag;
    /// // Select string bin "a" that starts with "prefix" and ends with "suffix".
    /// // Ignore case and do not match newline.
    /// Expression::regex_compare("prefix.*suffix".to_string(), RegexFlag::ICASE as i64 | RegexFlag::NEWLINE as i64, Expression::string_bin("a".to_string()));
    /// ```
    pub fn regex_compare(regex: String, flags: i64, bin: FilterExpression) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::REGEX),
            Some(Value::from(regex)),
            Some(bin),
            Some(flags),
            None,
            None,
        )
    }

    /// Create compare geospatial operation.
    /// ```
    /// use aerospike::exp::Expression;
    /// // Query region within coordinates.
    /// let region = "{\"type\": \"Polygon\", \"coordinates\": [ [[-122.500000, 37.000000],[-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000, 38.080000], [-122.500000, 37.000000]] ] }";
    /// Expression::geo_compare(Expression::geo_bin("a".to_string()), Expression::geo_val(region.to_string()));
    /// ```
    pub fn geo_compare(left: FilterExpression, right: FilterExpression) -> FilterExpression {
        FilterExpression::new(
            Some(ExpOp::GEO),
            None,
            None,
            None,
            None,
            Some(vec![left, right]),
        )
    }

    /// Creates 64 bit integer value
    pub fn int_val(val: i64) -> FilterExpression {
        FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Creates String bin value
    pub fn string_val(val: String) -> FilterExpression {
        FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Creates 64 bit float bin value
    pub fn float_val(val: f64) -> FilterExpression {
        FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Creates Blob bin value
    pub fn blob_val(val: Vec<u8>) -> FilterExpression {
        FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Create List bin Value
    pub fn list_val(val: Vec<Value>) -> FilterExpression {
        FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Create Map bin Value
    pub fn map_val(val: HashMap<Value, Value>) -> FilterExpression {
        FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Create geospatial json string value.
    pub fn geo_val(val: String) -> FilterExpression {
        FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Create a Nil Value
    pub fn nil() -> FilterExpression {
        FilterExpression::new(None, Some(Value::Nil), None, None, None, None)
    }
    /// Create "not" operator expression.
    /// ```
    /// use aerospike::exp::Expression;
    /// // ! (a == 0 || a == 10)
    /// Expression::not(Expression::or(vec![Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(0)), Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(10))]));
    /// ```
    pub fn not(exp: FilterExpression) -> FilterExpression {
        FilterExpression {
            cmd: Some(ExpOp::NOT),
            val: None,
            bin: None,
            flags: None,
            module: None,
            exps: Some(vec![exp]),
            arguments: None,
        }
    }

    /// Create "and" (&&) operator that applies to a variable number of expressions.
    /// ```
    /// use aerospike::exp::Expression;
    /// // (a > 5 || a == 0) && b < 3
    /// Expression::and(vec![Expression::or(vec![Expression::gt(Expression::int_bin("a".to_string()), Expression::int_val(5)), Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(0))]), Expression::lt(Expression::int_bin("b".to_string()), Expression::int_val(3))]);
    /// ```
    pub fn and(exps: Vec<FilterExpression>) -> FilterExpression {
        FilterExpression {
            cmd: Some(ExpOp::AND),
            val: None,
            bin: None,
            flags: None,
            module: None,
            exps: Some(exps),
            arguments: None,
        }
    }

    /// Create "or" (||) operator that applies to a variable number of expressions.
    /// ```
    /// use aerospike::exp::Expression;
    /// // a == 0 || b == 0
    /// Expression::or(vec![Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(0)), Expression::eq(Expression::int_bin("b".to_string()), Expression::int_val(0))]);
    /// ```
    pub fn or(exps: Vec<FilterExpression>) -> FilterExpression {
        FilterExpression {
            cmd: Some(ExpOp::OR),
            val: None,
            bin: None,
            flags: None,
            module: None,
            exps: Some(exps),
            arguments: None,
        }
    }

    /// Create equal (==) expression.
    /// ```
    /// use aerospike::exp::Expression;
    /// // a == 11
    /// Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(11));
    /// ```
    pub fn eq(left: FilterExpression, right: FilterExpression) -> FilterExpression {
        FilterExpression {
            cmd: Some(ExpOp::EQ),
            val: None,
            bin: None,
            flags: None,
            module: None,
            exps: Some(vec![left, right]),
            arguments: None,
        }
    }

    /// Create not equal (!=) expression
    /// ```
    /// use aerospike::exp::Expression;
    /// // a != 13
    /// Expression::ne(Expression::int_bin("a".to_string()), Expression::int_val(13));
    /// ```
    pub fn ne(left: FilterExpression, right: FilterExpression) -> FilterExpression {
        FilterExpression {
            cmd: Some(ExpOp::NE),
            val: None,
            bin: None,
            flags: None,
            module: None,
            exps: Some(vec![left, right]),
            arguments: None,
        }
    }

    /// Create greater than (>) operation.
    /// ```
    /// use aerospike::exp::Expression;
    /// // a > 8
    /// Expression::gt(Expression::int_bin("a".to_string()), Expression::int_val(8));
    /// ```
    pub fn gt(left: FilterExpression, right: FilterExpression) -> FilterExpression {
        FilterExpression {
            cmd: Some(ExpOp::GT),
            val: None,
            bin: None,
            flags: None,
            module: None,
            exps: Some(vec![left, right]),
            arguments: None,
        }
    }

    /// Create greater than or equal (>=) operation.
    /// ```
    /// use aerospike::exp::Expression;
    /// // a >= 88
    /// Expression::ge(Expression::int_bin("a".to_string()), Expression::int_val(88));
    /// ```
    pub fn ge(left: FilterExpression, right: FilterExpression) -> FilterExpression {
        FilterExpression {
            cmd: Some(ExpOp::GE),
            val: None,
            bin: None,
            flags: None,
            module: None,
            exps: Some(vec![left, right]),
            arguments: None,
        }
    }

    /// Create less than (<) operation.
    /// ```
    /// use aerospike::exp::Expression;
    /// // a < 1000
    /// Expression::lt(Expression::int_bin("a".to_string()), Expression::int_val(1000));
    /// ```
    pub fn lt(left: FilterExpression, right: FilterExpression) -> FilterExpression {
        FilterExpression {
            cmd: Some(ExpOp::LT),
            val: None,
            bin: None,
            flags: None,
            module: None,
            exps: Some(vec![left, right]),
            arguments: None,
        }
    }

    /// Create less than or equals (<=) operation.
    /// ```
    /// use aerospike::exp::Expression;
    /// // a <= 1
    /// Expression::le(Expression::int_bin("a".to_string()), Expression::int_val(1));
    /// ```
    pub fn le(left: FilterExpression, right: FilterExpression) -> FilterExpression {
        FilterExpression {
            cmd: Some(ExpOp::LE),
            val: None,
            bin: None,
            flags: None,
            module: None,
            exps: Some(vec![left, right]),
            arguments: None,
        }
    }
}
// ----------------------------------------------
