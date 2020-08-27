use crate::commands::buffer::Buffer;
use crate::errors::Result;
use crate::msgpack::encoder::{
    pack_array, pack_array_begin, pack_blob, pack_f64, pack_integer, pack_string,
};
use crate::Value;

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

pub const _EQ: i64 = 1;
pub const _NE: i64 = 2;
pub const _GT: i64 = 3;
pub const _GE: i64 = 4;
pub const _LT: i64 = 5;
pub const _LE: i64 = 6;
pub const _REGEX: i64 = 7;
pub const _GEO: i64 = 8;
pub const _AND: i64 = 16;
pub const _OR: i64 = 17;
pub const _NOT: i64 = 18;
pub const _DIGEST_MODULO: i64 = 64;
pub const _DEVICE_SIZE: i64 = 65;
pub const _LAST_UPDATE: i64 = 66;
pub const _VOID_TIME: i64 = 67;
pub const _TTL: i64 = 68;
pub const _SET_NAME: i64 = 69;
pub const _KEY_EXISTS: i64 = 70;
pub const _KEY: i64 = 80;
pub const _BIN: i64 = 81;
pub const _BIN_TYPE: i64 = 82;
pub const _LOCAL: i64 = 126;
pub const _CALL: i64 = 127;
pub const _MODIFY: i64 = 0x40;
pub const _NANOS_PER_MILLIS: i64 = 1000000;

pub type ASExp = Box<dyn Exp>;
pub trait Exp {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize>;

    fn write_to(&self, buf: &mut Buffer) -> Result<usize> {
        self.pack(&mut Some(buf))
    }

    fn estimate_size(&self) -> Result<usize> {
        self.pack(&mut None)
    }
}

fn get_serial_version_uid() -> i64 {
    1
}

/// Create a record key expression of specified type.
/// ```
/// use aerospike::operations::exp::{ExpType, key, ge, val};
/// // Integer record key >= 100000
/// ge(key(ExpType::INT), val(10000));
/// ```
pub fn key(exp_type: ExpType) -> ASExp {
    CmdInt::new(_KEY, exp_type as i64)
}

/// Create function that returns if the primary key is stored in the record meta data
/// as a boolean expression. This would occur when send_key is true on record write.
/// ```
/// use aerospike::operations::exp::{key_exists};
/// // Key exists in record meta data
/// key_exists();
/// ```
pub fn key_exists() -> ASExp {
    Cmd::new(_KEY_EXISTS)
}

/// Create bin expression of specified type.
/// ```
/// use aerospike::operations::exp::{eq, bin, ExpType, string_val};
/// // String bin "a" == "views"
/// eq(bin("a".to_string(), ExpType::STRING), string_val("views".to_string()));
/// ```
pub fn bin(name: String, exp_type: ExpType) -> ASExp {
    Bin::new(name, exp_type)
}

/// Create 64 bit int bin expression.
/// ```
/// use aerospike::operations::exp::{eq, int_bin, int_val};
/// // Integer bin "a" == 500
/// eq(int_bin("a".to_string()), int_val(500));
/// ```
pub fn int_bin(name: String) -> ASExp {
    Bin::new(name, ExpType::INT)
}

/// Create string bin expression.
/// ```
/// use aerospike::operations::exp::{eq, string_bin, string_val};
/// // String bin "a" == "views"
/// eq(string_bin("a".to_string()), string_val("views".to_string()));
/// ```
pub fn string_bin(name: String) -> ASExp {
    Bin::new(name, ExpType::STRING)
}

/// Create blob bin expression.
/// ```
/// use aerospike::operations::exp::{eq, blob_bin, blob_val};
/// // String bin "a" == [1,2,3]
/// let blob: Vec<u8> = vec![1,2,3];
/// eq(blob_bin("a".to_string()), blob_val(blob));
/// ```
pub fn blob_bin(name: String) -> ASExp {
    Bin::new(name, ExpType::BLOB)
}

/// Create 64 bit float bin expression.
/// ```
/// use aerospike::operations::exp::{eq, float_bin, float_val};
/// // Integer bin "a" == 500.5
/// eq(float_bin("a".to_string()), float_val(500.5));
/// ```
pub fn float_bin(name: String) -> ASExp {
    Bin::new(name, ExpType::FLOAT)
}

/// Create geo bin expression.
/// ```
/// use aerospike::operations::exp::{eq, geo_bin, string_val};
/// // String bin "a" == region
/// let region = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-122.0, 37.5], 50000.0] }";
/// eq(geo_bin("a".to_string()), string_val(region.to_string()));
/// ```
pub fn geo_bin(name: String) -> ASExp {
    Bin::new(name, ExpType::GEO)
}

/// Create list bin expression.
/// ```
/// use aerospike::operations::exp::{eq, list_bin, ExpType, int_val};
/// use aerospike::operations::lists::ListReturnType;
/// // String bin a[2] == 3
/// //eq(list_exp::get_by_index(2, ListReturnType::Values, ExpType::INT, list_bin("a".to_string())), int_val(3));
/// ```
pub fn list_bin(name: String) -> ASExp {
    Bin::new(name, ExpType::LIST)
}

pub fn map_bin(name: String) -> ASExp {
    Bin::new(name, ExpType::MAP)
}

pub fn hll_bin(name: String) -> ASExp {
    Bin::new(name, ExpType::HLL)
}

/// Create function that returns if bin of specified name exists.
/// ```
/// use aerospike::operations::exp::bin_exists;
/// // Bin "a" exists in record
/// bin_exists("a".to_string());
/// ```
pub fn bin_exists(name: String) -> ASExp {
    ne(bin_type(name), int_val(0))
}

/// Create function that returns bin's integer particle type.
/// ```
/// use aerospike::operations::exp::{eq, bin_type, int_val};
/// use aerospike::ParticleType;
/// // Bin "a" particle type is a list
/// eq(bin_type("a".to_string()), int_val(ParticleType::LIST as i64));
/// ```
pub fn bin_type(name: String) -> ASExp {
    CmdStr::new(_BIN_TYPE, name)
}

/// Create function that returns record set name string.
/// ```
/// use aerospike::operations::exp::{set_name, eq, string_val};
/// // Record set name == "myset
/// eq(set_name(), string_val("myset".to_string()));
/// ```
pub fn set_name() -> ASExp {
    Cmd::new(_SET_NAME)
}

/// Create function that returns record size on disk.
/// If server storage-engine is memory, then zero is returned.
/// ```
/// use aerospike::operations::exp::{ge, device_size, int_val};
/// // Record device size >= 100 KB
/// ge(device_size(), int_val(100*1024));
/// ```
pub fn device_size() -> ASExp {
    Cmd::new(_DEVICE_SIZE)
}

/// Create function that returns record last update time expressed as 64 bit integer
/// nanoseconds since 1970-01-01 epoch.
/// ```
/// use aerospike::operations::exp::{ge, last_update, float_val};
/// // Record last update time >=2020-08-01
/// ge(last_update(), float_val(1.5962E+18));
/// ```
pub fn last_update() -> ASExp {
    Cmd::new(_LAST_UPDATE)
}

/// Create function that returns record expiration time expressed as 64 bit integer
/// nanoseconds since 1970-01-01 epoch.
/// ```
/// use aerospike::operations::exp::{ge, last_update, float_val, and, lt};
/// // Expires on 2020-08-01
/// and(vec![ge(last_update(), float_val(1.5962E+18)), lt(last_update(), float_val(1.5963E+18))]);
/// ```
pub fn void_time() -> ASExp {
    Cmd::new(_VOID_TIME)
}

/// Create function that returns record expiration time (time to live) in integer seconds.
/// ```
/// // Record expires in less than 1 hour
/// use aerospike::operations::exp::{lt, ttl, int_val};
/// lt(ttl(), int_val(60*60));
/// ```
pub fn ttl() -> ASExp {
    Cmd::new(_TTL)
}

/// Create function that returns record digest modulo as integer.
/// ```
/// // Records that have digest(key) % 3 == 1
/// use aerospike::operations::exp::{eq, digest_modulo, int_val};
/// eq(digest_modulo(3), int_val(1));
/// ```
pub fn digest_modulo(modulo: i64) -> ASExp {
    CmdInt::new(_DIGEST_MODULO, modulo)
}

/// Create function like regular expression string operation.
/// ```
/// use aerospike::operations::exp::{like, string_bin};
/// use aerospike::RegexFlag;
/// // Select string bin "a" that starts with "prefix" and ends with "suffix".
/// // Ignore case and do not match newline.
/// like("prefix.*suffix".to_string(), RegexFlag::ICASE | RegexFlag::NEWLINE, string_bin("a".to_string()));
/// ```
pub fn like(regex: String, flags: i64, bin: ASExp) -> ASExp {
    Regex::new(bin, regex, flags)
}

/// Create compare geospatial operation.
/// ```
/// use aerospike::operations::exp::{geo_compare, geo_bin, geo};
/// // Query region within coordinates.
/// let region = "{\"type\": \"Polygon\", \"coordinates\": [ [[-122.500000, 37.000000],[-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000, 38.080000], [-122.500000, 37.000000]] ] }";
/// geo_compare(geo_bin("a".to_string()), geo(region.to_string()));
/// ```
pub fn geo_compare(left: ASExp, right: ASExp) -> ASExp {
    CmdExp::new(_GEO, vec![left, right])
}

/// Creates 64 bit integer value
pub fn int_val(val: i64) -> ASExp {
    Int::new(val)
}

/// Creates String bin value
pub fn string_val(val: String) -> ASExp {
    Str::new(val)
}

/// Creates 64 bit float bin value
pub fn float_val(val: f64) -> ASExp {
    Float::new(val)
}

/// Creates Blob bin value
pub fn blob_val(val: Vec<u8>) -> ASExp {
    Blob::new(val)
}

/// Expose list expression methods.
pub fn list_val(val: Vec<Value>) -> ASExp {
    ListVal::new(val)
}

/// Create geospatial json string value.
pub fn geo(val: String) -> ASExp {
    Geo::new(val)
}

/// Create "not" operator expression.
/// ```
/// use aerospike::operations::exp::{not, or, eq, int_bin, int_val};
/// // ! (a == 0 || a == 10)
/// not(or(vec![eq(int_bin("a".to_string()), int_val(0)), eq(int_bin("a".to_string()), int_val(10))]));
/// ```
pub fn not(exp: ASExp) -> ASExp {
    CmdExp::new(_NOT, vec![exp])
}

/// Create "and" (&&) operator that applies to a variable number of expressions.
/// ```
/// use aerospike::operations::exp::{and, or, gt, int_bin, int_val, eq, lt};
/// // (a > 5 || a == 0) && b < 3
/// and(vec![or(vec![gt(int_bin("a".to_string()), int_val(5)), eq(int_bin("a".to_string()), int_val(0))]), lt(int_bin("b".to_string()), int_val(3))]);
/// ```
pub fn and(exps: Vec<ASExp>) -> ASExp {
    CmdExp::new(_AND, exps)
}

/// Create "or" (||) operator that applies to a variable number of expressions.
/// ```
/// use aerospike::operations::exp::{or, eq, int_bin, int_val};
/// // a == 0 || b == 0
/// or(vec![eq(int_bin("a".to_string()), int_val(0)), eq(int_bin("b".to_string()), int_val(0))]);
/// ```
pub fn or(exps: Vec<ASExp>) -> ASExp {
    CmdExp::new(_OR, exps)
}

/// Create equal (==) expression.
/// ```
/// use aerospike::operations::exp::{eq, int_bin, int_val};
/// // a == 11
/// eq(int_bin("a".to_string()), int_val(11));
/// ```
pub fn eq(left: ASExp, right: ASExp) -> ASExp {
    CmdExp::new(_EQ, vec![left, right])
}

/// Create not equal (!=) expression
/// ```
/// use aerospike::operations::exp::{ne, int_bin, int_val};
/// // a != 13
/// ne(int_bin("a".to_string()), int_val(13));
/// ```
pub fn ne(left: ASExp, right: ASExp) -> ASExp {
    CmdExp::new(_NE, vec![left, right])
}

/// Create greater than (>) operation.
/// ```
/// use aerospike::operations::exp::{gt, int_bin, int_val};
/// // a > 8
/// gt(int_bin("a".to_string()), int_val(8));
/// ```
pub fn gt(left: ASExp, right: ASExp) -> ASExp {
    CmdExp::new(_GT, vec![left, right])
}

/// Create greater than or equal (>=) operation.
/// ```
/// use aerospike::operations::exp::{ge, int_bin, int_val};
/// // a >= 88
/// ge(int_bin("a".to_string()), int_val(88));
/// ```
pub fn ge(left: ASExp, right: ASExp) -> ASExp {
    CmdExp::new(_GE, vec![left, right])
}

/// Create less than (<) operation.
/// ```
/// use aerospike::operations::exp::{lt, int_bin, int_val};
/// // a < 1000
/// lt(int_bin("a".to_string()), int_val(1000));
/// ```
pub fn lt(left: ASExp, right: ASExp) -> ASExp {
    CmdExp::new(_LT, vec![left, right])
}

/// Create less than or equals (<=) operation.
/// ```
/// use aerospike::operations::exp::{le, int_bin, int_val};
/// // a <= 1
/// le(int_bin("a".to_string()), int_val(1));
/// ```
pub fn le(left: ASExp, right: ASExp) -> ASExp {
    CmdExp::new(_LE, vec![left, right])
}
// ----------------------------------------------

pub struct Module {
    pub bin: ASExp,
    pub bytes: Vec<u8>,
    pub return_type: i64,
    pub module: i64,
}

impl Module {
    pub fn new(bin: ASExp, bytes: Vec<u8>, return_type: i64, module: i64) -> ASExp {
        Box::new(Module {
            bin,
            bytes,
            return_type,
            module,
        })
    }
}
impl Exp for Module {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_array_begin(buf, 5)?;
        size += pack_integer(buf, _CALL)?;
        size += pack_integer(buf, self.return_type)?;
        size += pack_integer(buf, self.module)?;
        size += pack_blob(buf, self.bytes.as_slice())?;
        size += self.bin.pack(buf)?;
        Ok(size)
    }
}
pub struct Bin {
    pub name: String,
    pub exp_type: ExpType,
}

impl Bin {
    pub fn new(name: String, exp_type: ExpType) -> ASExp {
        Box::new(Bin { name, exp_type })
    }
}

impl Exp for Bin {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_array_begin(buf, 3)?;
        size += pack_integer(buf, _BIN)?;
        size += pack_integer(buf, self.exp_type as i64)?;
        size += pack_string(buf, &self.name)?;
        Ok(size)
    }
}

pub struct Regex {
    pub bin: ASExp,
    pub regex: String,
    pub flags: i64,
}

impl Regex {
    pub fn new(bin: ASExp, regex: String, flags: i64) -> ASExp {
        Box::new(Regex { bin, regex, flags })
    }
}

impl Exp for Regex {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_array_begin(buf, 4)?;
        size += pack_integer(buf, _REGEX)?;
        size += pack_integer(buf, self.flags)?;
        size += pack_string(buf, &self.regex)?;
        size += self.bin.pack(buf)?;
        Ok(size)
    }
}

pub struct CmdExp {
    pub exps: Vec<ASExp>,
    pub cmd: i64,
}

impl CmdExp {
    pub fn new(cmd: i64, exps: Vec<ASExp>) -> ASExp {
        Box::new(CmdExp { exps, cmd })
    }
}

impl Exp for CmdExp {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_array_begin(buf, self.exps.len() + 1)?;
        size += pack_integer(buf, self.cmd)?;
        for exp in &self.exps {
            size += exp.pack(buf)?;
        }
        Ok(size)
    }
}

pub struct CmdInt {
    pub cmd: i64,
    pub val: i64,
}

impl CmdInt {
    pub fn new(cmd: i64, val: i64) -> ASExp {
        Box::new(CmdInt { cmd, val })
    }
}

impl Exp for CmdInt {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_array_begin(buf, 2)?;
        size += pack_integer(buf, self.cmd)?;
        size += pack_integer(buf, self.val)?;
        Ok(size)
    }
}

pub struct CmdStr {
    pub cmd: i64,
    pub val: String,
}

impl CmdStr {
    pub fn new(cmd: i64, val: String) -> ASExp {
        Box::new(CmdStr { cmd, val })
    }
}

impl Exp for CmdStr {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_array_begin(buf, 2)?;
        size += pack_integer(buf, self.cmd)?;
        size += pack_string(buf, &self.val)?;
        Ok(size)
    }
}

pub struct Cmd {
    pub cmd: i64,
}

impl Cmd {
    pub fn new(cmd: i64) -> ASExp {
        Box::new(Cmd { cmd })
    }
}

impl Exp for Cmd {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_array_begin(buf, 1)?;
        size += pack_integer(buf, self.cmd)?;
        Ok(size)
    }
}

pub struct Int {
    pub val: i64,
}

impl Int {
    pub fn new(val: i64) -> ASExp {
        Box::new(Int { val })
    }
}

impl Exp for Int {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_integer(buf, self.val)?;
        Ok(size)
    }
}

pub struct Str {
    pub val: String,
}

impl Str {
    pub fn new(val: String) -> ASExp {
        Box::new(Str { val })
    }
}

impl Exp for Str {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_string(buf, &self.val)?;
        Ok(size)
    }
}

pub struct Float {
    pub val: f64,
}

impl Float {
    pub fn new(val: f64) -> ASExp {
        Box::new(Float { val })
    }
}

impl Exp for Float {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_f64(buf, self.val)?;
        Ok(size)
    }
}

pub struct Blob {
    pub val: Vec<u8>,
}

impl Blob {
    pub fn new(val: Vec<u8>) -> ASExp {
        Box::new(Blob { val })
    }
}

impl Exp for Blob {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_blob(buf, self.val.as_slice())?;
        Ok(size)
    }
}

pub struct Geo {
    pub val: String,
}

impl Geo {
    pub fn new(val: String) -> ASExp {
        Box::new(Geo { val })
    }
}

impl Exp for Geo {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_string(buf, &self.val)?;
        Ok(size)
    }
}

pub struct ListVal {
    pub list: Vec<Value>,
}

impl ListVal {
    pub fn new(list: Vec<Value>) -> ASExp {
        Box::new(ListVal { list })
    }
}

impl Exp for ListVal {
    fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        size += pack_array_begin(buf, 2)?;
        size += pack_integer(buf, _LOCAL)?;
        size += pack_array(buf, self.list.as_slice())?;
        Ok(size)
    }
}
