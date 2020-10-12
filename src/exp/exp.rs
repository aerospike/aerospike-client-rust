//! Aerospike Filter Expressions
//! This feature requires Aerospike Server Version >= 5.2
use crate::commands::buffer::Buffer;
use crate::errors::Result;
use crate::msgpack::encoder::{pack_array_begin, pack_integer, pack_value};
use crate::Value;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum ExpType {
    NIL = 0,
    BOOL = 1,
    INT = 2,
    STRING = 3,
    LIST = 4,
    MAP = 5,
    BLOB = 6,
    FLOAT = 7,
    GEO = 8,
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
#[doc(hidden)]
const NANOS_PER_MILLIS: i64 = 1000000;
#[doc(hidden)]
pub const SERIAL_VERSION_ID: i64 = 1;

#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum ExpressionArgument {
    Value(Value),
    FilterCmd(FilterCmd),
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct FilterCmd {
    /// The Operation code
    pub cmd: Option<ExpOp>,
    /// The Primary Value of the Operation
    pub val: Option<Value>,
    /// The Bin to use it on (REGEX for example)
    pub bin: Option<Box<FilterCmd>>,
    /// The additional flags for the Operation (REGEX or return_type of Module for example)
    pub flags: Option<i64>,
    /// The optional Module flag for Module operations or Bin Types
    pub module: Option<ExpType>,
    /// Sub commands for the CmdExp operation
    pub exps: Option<Vec<FilterCmd>>,

    pub arguments: Option<Vec<ExpressionArgument>>,
}

#[doc(hidden)]
impl FilterCmd {
    pub fn new(
        cmd: Option<ExpOp>,
        val: Option<Value>,
        bin: Option<FilterCmd>,
        flags: Option<i64>,
        module: Option<ExpType>,
        exps: Option<Vec<FilterCmd>>,
    ) -> FilterCmd {
        if let Some(bin) = bin {
            FilterCmd {
                cmd,
                val,
                bin: Some(Box::new(bin)),
                flags,
                module,
                exps,
                arguments: None,
            }
        } else {
            FilterCmd {
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
        } else {
            if let Some(cmd) = self.cmd {
                if cmd as i64 == ExpOp::REGEX as i64 {
                    // Packing logic for Regex
                    size += pack_array_begin(buf, 4)?;
                    size += pack_integer(buf, cmd as i64)?;
                    size += pack_integer(buf, self.flags.unwrap())?;
                    size += pack_value(buf, &self.val.clone().unwrap())?;
                    size += self.bin.clone().unwrap().pack(buf)?;
                } else if cmd as i64 == ExpOp::Call as i64 {
                    // Packing logic for Module
                    size += pack_array_begin(buf, 5)?;
                    size += pack_integer(buf, cmd as i64)?;
                    size += pack_integer(buf, self.flags.unwrap())?;
                    size += pack_integer(buf, self.module.unwrap() as i64)?;
                    if let Some(args) = &self.arguments {
                        size += pack_array_begin(buf, args.len())?;
                        for arg in args {
                            match arg {
                                ExpressionArgument::Value(val) => {
                                    size += pack_value(buf, val)?;
                                }
                                ExpressionArgument::FilterCmd(cmd) => {
                                    size += cmd.pack(buf)?;
                                }
                            }
                        }
                    } else {
                        size += pack_value(buf, &self.val.clone().unwrap())?;
                    }
                    size += self.bin.clone().unwrap().pack(buf)?;
                } else if cmd as i64 == ExpOp::BIN as i64 {
                    size += pack_array_begin(buf, 3)?;
                    size += pack_integer(buf, cmd as i64)?;
                    size += pack_integer(buf, self.module.unwrap() as i64)?;
                    size += pack_value(buf, &self.val.clone().unwrap())?;
                } else {
                    // Packing logic for all other Ops
                    if let Some(value) = &self.val {
                        size += pack_array_begin(buf, 2)?;
                        size += pack_integer(buf, cmd as i64)?;
                        size += pack_value(buf, value)?;
                    } else {
                        size += pack_array_begin(buf, 1)?;
                        size += pack_integer(buf, cmd as i64)?;
                    }
                }
            } else {
                // Packing logic for Value based Ops
                size += pack_value(buf, &self.val.clone().unwrap())?;
            }
        }

        Ok(size)
    }
}

/// Basic Expression functions and single Bin interaction
pub struct Expression {}

impl Expression {
    /// Create a record key expression of specified type.
    /// ```
    /// use aerospike::exp::exp::{Expression, ExpType};
    /// // Integer record key >= 100000
    /// Expression::ge(Expression::key(ExpType::INT), Expression::int_val(10000));
    /// ```
    pub fn key(exp_type: ExpType) -> FilterCmd {
        FilterCmd::new(
            Some(ExpOp::KEY),
            Some(Value::from(exp_type as i64)),
            None,
            None,
            None,
            None,
        )
    }

    /// Create function that returns if the primary key is stored in the record meta data
    /// as a boolean expression. This would occur when send_key is true on record write.
    /// ```
    /// use aerospike::exp::exp::Expression;
    /// // Key exists in record meta data
    /// Expression::key_exists();
    /// ```
    pub fn key_exists() -> FilterCmd {
        FilterCmd::new(Some(ExpOp::KeyExists), None, None, None, None, None)
    }

    /// Create bin expression of specified type.
    /// ```
    /// use aerospike::exp::exp::{Expression, ExpType};
    /// // String bin "a" == "views"
    /// Expression::eq(Expression::bin("a".to_string(), ExpType::STRING), Expression::string_val("views".to_string()));
    /// ```
    pub fn bin(name: String, exp_type: ExpType) -> FilterCmd {
        FilterCmd::new(
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
    /// use aerospike::exp::exp::Expression;
    /// // Integer bin "a" == 500
    /// Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(500));
    /// ```
    pub fn int_bin(name: String) -> FilterCmd {
        FilterCmd::new(
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
    /// use aerospike::exp::exp::Expression;
    /// // String bin "a" == "views"
    /// Expression::eq(Expression::string_bin("a".to_string()), Expression::string_val("views".to_string()));
    /// ```
    pub fn string_bin(name: String) -> FilterCmd {
        FilterCmd::new(
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
    /// use aerospike::exp::exp::Expression;
    /// // String bin "a" == [1,2,3]
    /// let blob: Vec<u8> = vec![1,2,3];
    /// Expression::eq(Expression::blob_bin("a".to_string()), Expression::blob_val(blob));
    /// ```
    pub fn blob_bin(name: String) -> FilterCmd {
        FilterCmd::new(
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
    /// use aerospike::exp::exp::Expression;
    /// // Integer bin "a" == 500.5
    /// Expression::eq(Expression::float_bin("a".to_string()), Expression::float_val(500.5));
    /// ```
    pub fn float_bin(name: String) -> FilterCmd {
        FilterCmd::new(
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
    /// use aerospike::exp::exp::Expression;
    /// // String bin "a" == region
    /// let region = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-122.0, 37.5], 50000.0] }";
    /// Expression::eq(Expression::geo_bin("a".to_string()), Expression::string_val(region.to_string()));
    /// ```
    pub fn geo_bin(name: String) -> FilterCmd {
        FilterCmd::new(
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
    /// use aerospike::exp::exp::{Expression, ExpType};
    /// use aerospike::operations::lists::ListReturnType;
    /// // String bin a[2] == 3
    /// Expression::eq(list_exp::get_by_index(2, ListReturnType::Values, ExpType::INT, Expression::list_bin("a".to_string())), Expression::int_val(3));
    /// ```
    pub fn list_bin(name: String) -> FilterCmd {
        FilterCmd::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::LIST),
            None,
        )
    }

    pub fn map_bin(name: String) -> FilterCmd {
        FilterCmd::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::MAP),
            None,
        )
    }

    pub fn hll_bin(name: String) -> FilterCmd {
        FilterCmd::new(
            Some(ExpOp::BIN),
            Some(Value::from(name)),
            None,
            None,
            Some(ExpType::HLL),
            None,
        )
    }

    /// Create function that returns if bin of specified name exists.
    /// ```
    /// use aerospike::exp::exp::Expression;
    /// // Bin "a" exists in record
    /// Expression::bin_exists("a".to_string());
    /// ```
    pub fn bin_exists(name: String) -> FilterCmd {
        Expression::ne(Expression::bin_type(name), Expression::int_val(0))
    }

    /// Create function that returns bin's integer particle type.
    /// ```
    /// use aerospike::exp::exp::Expression;
    /// use aerospike::ParticleType;
    /// // Bin "a" particle type is a list
    /// Expression::eq(Expression::bin_type("a".to_string()), Expression::int_val(ParticleType::LIST as i64));
    /// ```
    pub fn bin_type(name: String) -> FilterCmd {
        FilterCmd::new(
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
    /// use aerospike::exp::exp::Expression;
    /// // Record set name == "myset
    /// Expression::eq(Expression::set_name(), Expression::string_val("myset".to_string()));
    /// ```
    pub fn set_name() -> FilterCmd {
        FilterCmd::new(Some(ExpOp::SetName), None, None, None, None, None)
    }

    /// Create function that returns record size on disk.
    /// If server storage-engine is memory, then zero is returned.
    /// ```
    /// use aerospike::exp::exp::Expression;
    /// // Record device size >= 100 KB
    /// Expression::ge(Expression::device_size(), Expression::int_val(100*1024));
    /// ```
    pub fn device_size() -> FilterCmd {
        FilterCmd::new(Some(ExpOp::DeviceSize), None, None, None, None, None)
    }

    /// Create function that returns record last update time expressed as 64 bit integer
    /// nanoseconds since 1970-01-01 epoch.
    /// ```
    /// use aerospike::exp::exp::Expression;
    /// // Record last update time >=2020-08-01
    /// Expression::ge(Expression::last_update(), Expression::float_val(1.5962E+18));
    /// ```
    pub fn last_update() -> FilterCmd {
        FilterCmd::new(Some(ExpOp::LastUpdate), None, None, None, None, None)
    }

    pub fn since_update() -> FilterCmd {
        FilterCmd::new(Some(ExpOp::SinceUpdate), None, None, None, None, None)
    }

    /// Create function that returns record expiration time expressed as 64 bit integer
    /// nanoseconds since 1970-01-01 epoch.
    /// ```
    /// use aerospike::exp::exp::Expression;
    /// // Expires on 2020-08-01
    /// Expression::and(vec![Expression::ge(Expression::last_update(), Expression::float_val(1.5962E+18)), Expression::lt(Expression::last_update(), Expression::float_val(1.5963E+18))]);
    /// ```
    pub fn void_time() -> FilterCmd {
        FilterCmd::new(Some(ExpOp::VoidTime), None, None, None, None, None)
    }

    /// Create function that returns record expiration time (time to live) in integer seconds.
    /// ```
    /// // Record expires in less than 1 hour
    /// use aerospike::exp::exp::Expression;
    /// Expression::lt(Expression::ttl(), Expression::int_val(60*60));
    /// ```
    pub fn ttl() -> FilterCmd {
        FilterCmd::new(Some(ExpOp::TTL), None, None, None, None, None)
    }

    pub fn is_tombstone() -> FilterCmd {
        FilterCmd::new(Some(ExpOp::IsTombstone), None, None, None, None, None)
    }
    /// Create function that returns record digest modulo as integer.
    /// ```
    /// // Records that have digest(key) % 3 == 1
    /// use aerospike::exp::exp::Expression;
    /// Expression::eq(Expression::digest_modulo(3), Expression::int_val(1));
    /// ```
    pub fn digest_modulo(modulo: i64) -> FilterCmd {
        FilterCmd::new(
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
    /// use aerospike::exp::exp::Expression;
    /// use aerospike::RegexFlag;
    /// // Select string bin "a" that starts with "prefix" and ends with "suffix".
    /// // Ignore case and do not match newline.
    /// Expression::regex_compare("prefix.*suffix".to_string(), RegexFlag::ICASE | RegexFlag::NEWLINE, Expression::string_bin("a".to_string()));
    /// ```
    pub fn regex_compare(regex: String, flags: i64, bin: FilterCmd) -> FilterCmd {
        FilterCmd::new(
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
    /// use aerospike::exp::exp::Expression;
    /// // Query region within coordinates.
    /// let region = "{\"type\": \"Polygon\", \"coordinates\": [ [[-122.500000, 37.000000],[-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000, 38.080000], [-122.500000, 37.000000]] ] }";
    /// Expression::geo_compare(Expression::geo_bin("a".to_string()), Expression::geo_val(region.to_string()));
    /// ```
    pub fn geo_compare(left: FilterCmd, right: FilterCmd) -> FilterCmd {
        FilterCmd::new(
            Some(ExpOp::GEO),
            None,
            None,
            None,
            None,
            Some(vec![left, right]),
        )
    }

    /// Creates 64 bit integer value
    pub fn int_val(val: i64) -> FilterCmd {
        FilterCmd::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Creates String bin value
    pub fn string_val(val: String) -> FilterCmd {
        FilterCmd::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Creates 64 bit float bin value
    pub fn float_val(val: f64) -> FilterCmd {
        FilterCmd::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Creates Blob bin value
    pub fn blob_val(val: Vec<u8>) -> FilterCmd {
        FilterCmd::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Create List bin Value
    pub fn list_val(val: Vec<Value>) -> FilterCmd {
        FilterCmd::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Create Map bin Value
    pub fn map_val(val: HashMap<Value, Value>) -> FilterCmd {
        FilterCmd::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Create geospatial json string value.
    pub fn geo_val(val: String) -> FilterCmd {
        FilterCmd::new(None, Some(Value::from(val)), None, None, None, None)
    }

    /// Create a Nil Value
    pub fn nil() -> FilterCmd {
        FilterCmd::new(None, Some(Value::Nil), None, None, None, None)
    }
    /// Create "not" operator expression.
    /// ```
    /// use aerospike::exp::exp::Expression;
    /// // ! (a == 0 || a == 10)
    /// Expression::not(Expression::or(vec![Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(0)), Expression::eq(int_bin("a".to_string()), Expression::int_val(10))]));
    /// ```
    pub fn not(exp: FilterCmd) -> FilterCmd {
        FilterCmd {
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
    /// use aerospike::exp::exp::Expression;
    /// // (a > 5 || a == 0) && b < 3
    /// Expression::and(vec![Expression::or(vec![Expression::gt(Expression::int_bin("a".to_string()), Expression::int_val(5)), Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(0))]), Expression::lt(Expression::int_bin("b".to_string()), Expression::int_val(3))]);
    /// ```
    pub fn and(exps: Vec<FilterCmd>) -> FilterCmd {
        FilterCmd {
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
    /// use aerospike::exp::exp::Expression;
    /// // a == 0 || b == 0
    /// Expression::or(vec![Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(0)), Expression::eq(Expression::int_bin("b".to_string()), Expression::int_val(0))]);
    /// ```
    pub fn or(exps: Vec<FilterCmd>) -> FilterCmd {
        FilterCmd {
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
    /// use aerospike::exp::exp::Expression;
    /// // a == 11
    /// Expression::eq(Expression::int_bin("a".to_string()), Expression::int_val(11));
    /// ```
    pub fn eq(left: FilterCmd, right: FilterCmd) -> FilterCmd {
        FilterCmd {
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
    /// use aerospike::exp::exp::Expression;
    /// // a != 13
    /// Expression::ne(Expression::int_bin("a".to_string()), Expression::int_val(13));
    /// ```
    pub fn ne(left: FilterCmd, right: FilterCmd) -> FilterCmd {
        FilterCmd {
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
    /// use aerospike::exp::exp::Expression;
    /// // a > 8
    /// Expression::gt(Expression::int_bin("a".to_string()), Expression::int_val(8));
    /// ```
    pub fn gt(left: FilterCmd, right: FilterCmd) -> FilterCmd {
        FilterCmd {
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
    /// use aerospike::exp::exp::Expression;
    /// // a >= 88
    /// Expression::ge(Expression::int_bin("a".to_string()), Expression::int_val(88));
    /// ```
    pub fn ge(left: FilterCmd, right: FilterCmd) -> FilterCmd {
        FilterCmd {
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
    /// use aerospike::exp::exp::Expression;
    /// // a < 1000
    /// Expression::lt(Expression::int_bin("a".to_string()), Expression::int_val(1000));
    /// ```
    pub fn lt(left: FilterCmd, right: FilterCmd) -> FilterCmd {
        FilterCmd {
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
    /// use aerospike::exp::exp::Expression;
    /// // a <= 1
    /// Expression::le(Expression::int_bin("a".to_string()), Expression::int_val(1));
    /// ```
    pub fn le(left: FilterCmd, right: FilterCmd) -> FilterCmd {
        FilterCmd {
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
