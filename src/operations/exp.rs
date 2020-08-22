use crate::operations::expression::Expression;
use crate::Value;
use crate::commands::buffer::Buffer;

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
    // HLL = 9    NOT YET IMPLEMENTED
}

pub const _EQ: i64 = 1;
pub const _NE: i64 = 2;
pub const _GT: i64 = 3;
pub const _GE: i64 = 4:
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
pub const _MODIFY: i64 = 0x40;
pub const _NANOS_PER_MILLIS: i64 = 1000000;

pub type ASExp = Box<dyn Exp>;
pub trait Exp {
    fn build(exp: ASExp) -> &Expression {
        Expression::new(exp)
    }

    fn pack(buf: &mut Option<&mut Buffer>);
}

pub struct Module{
    pub bin: ASExp,
    pub bytes: Vec<u8>,
    pub return_type: i64,
    pub module: i64
}

impl Module {
    pub fn new(bin: ASExp, bytes: Vec<u8>, return_type: i64, module: i64) -> Self {
        Module{
            bin,
            bytes,
            return_type,
            module
        }
    }
}
impl Exp for Module {
    fn pack(buf: &mut Option<&mut Buffer>) {
        buf.pack
    }
}
pub struct Bin {
    pub name: String,
    pub exp_type: ExpType
}

impl Bin {
    pub fn new(name: String, exp_type: ExpType) -> Self {
        Bin{
            name,
            exp_type
        }
    }
}

pub struct Regex {
    pub bin: ASExp,
    pub regex: String,
    pub flags: i64
}

impl Regex {
    pub fn new(bin: ASExp, regex: String, flags: i64) -> Self {
        Regex{
            bin,
            regex,
            flags
        }
    }
}

pub struct CmdExp {
    pub exps: Vec<ASExp>,
    pub cmd: i64
}

impl CmdExp {
    pub fn new(cmd: i64, exps: Vec<ASExp>) -> Self {
        CmdExp{ exps, cmd }
    }
}

pub struct CmdInt {
    pub cmd: i64,
    pub val: i64
}

impl CmdInt {
    pub fn new(cmd: i64, val: i64) -> Self {
        CmdInt {
            cmd,
            val
        }
    }
}

pub struct CmdStr {
    pub cmd: i64,
    pub val: String
}

impl CmdStr {
    pub fn new(cmd: i64, val: String) -> Self{
        CmdStr{ cmd, val }
    }
}

pub struct Cmd {
    pub cmd: i64
}

impl Cmd {
    pub fn new(cmd: i64) -> Self {
        Cmd {cmd}
    }
}

pub struct Int {
    pub val: i64
}

impl Int {
    pub fn new(val: i64) -> Self{
        Int { val }
    }
}

pub struct Str {
    pub val: String
}

impl Str {
    pub fn new(val: String) -> Self {
        Str{ val }
    }
}

pub struct Float {
    pub val: f64
}

impl Float {
    pub fn new(val: f64) -> Self {
        Float {
            val
        }
    }
}

pub struct Blob {
    pub val: Vec<u8>
}

impl Blob {
    pub fn new(val: Vec<u8>) -> Self {
        Blob {
            val
        }
    }
}

pub struct Geo {
    pub val: String
}

impl Geo {
    pub fn new(val: String) -> Self {
        Geo { val }
    }
}

pub struct ListVal {
    pub list: Vec<Value>
}

impl ListVal {
    pub fn new(list: Vec<Value>) -> Self {
        ListVal {
            list
        }
    }
}