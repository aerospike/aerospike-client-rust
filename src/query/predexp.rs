const AS_PREDEXP_UNKNOWN_BIN: u16 = std::u16::MAX;

use crate::commands::buffer::Buffer;
use crate::errors::Result;

const AS_PREDEXP_AND: u16 = 1;
const AS_PREDEXP_OR:  u16 = 2;
const AS_PREDEXP_NOT: u16 = 3;

const AS_PREDEXP_INTEGER_VALUE: u16 = 10;
const AS_PREDEXP_STRING_VALUE:  u16 = 11;
const AS_PREDEXP_GEOJSON_VALUE: u16 = 12;

const AS_PREDEXP_INTEGER_BIN: u16 = 100;
const AS_PREDEXP_STRING_BIN:  u16 = 101;
const AS_PREDEXP_GEOJSON_BIN: u16 = 102;
const AS_PREDEXP_LIST_BIN:    u16 = 103;
const AS_PREDEXP_MAP_BIN:     u16 = 104;

const AS_PREDEXP_INTEGER_VAR: u16 = 120;
const AS_PREDEXP_STRING_VAR:  u16 = 121;
const AS_PREDEXP_GEOJSON_VAR: u16 = 122;

const AS_PREDEXP_REC_DEVICE_SIZE:   u16 = 150;
const AS_PREDEXP_REC_LAST_UPDATE:   u16 = 151;
const AS_PREDEXP_REC_VOID_TIME:     u16 = 152;
const AS_PREDEXP_REC_DIGEST_MODULO: u16 = 153;

const AS_PREDEXP_INTEGER_EQUAL:     u16 = 200;
const AS_PREDEXP_INTEGER_UNEQUAL:   u16 = 201;
const AS_PREDEXP_INTEGER_GREATER:   u16 = 202;
const AS_PREDEXP_INTEGER_GREATEREQ: u16 = 203;
const AS_PREDEXP_INTEGER_LESS:      u16 = 204;
const AS_PREDEXP_INTEGER_LESSEQ:    u16 = 205;

const AS_PREDEXP_STRING_EQUAL:   u16 = 210;
const AS_PREDEXP_STRING_UNEQUAL: u16 = 211;
const AS_PREDEXP_STRING_REGEX:   u16 = 212;

const AS_PREDEXP_GEOJSON_WITHIN:   u16 = 220;
const AS_PREDEXP_GEOJSON_CONTAINS: u16 = 221;

const AS_PREDEXP_LIST_ITERATE_OR:    u16 = 250;
const AS_PREDEXP_MAPKEY_ITERATE_OR:  u16 = 251;
const AS_PREDEXP_MAPVAL_ITERATE_OR:  u16 = 252;
const AS_PREDEXP_LIST_ITERATE_AND:   u16 = 253;
const AS_PREDEXP_MAPKEY_ITERATE_AND: u16 = 254;
const AS_PREDEXP_MAPVAL_ITERATE_AND: u16 = 255;

pub trait PredExp: Send + Sync{
    fn pred_string(&self) -> String;
    fn marshaled_size(&self) -> u32;
    fn write(&self, buffer: &mut Buffer) -> Result<()>;
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct PredExpBase {}

impl PredExpBase {
    #[doc(hidden)]
    fn default_size(&self) -> u32 {
        return 2+4; // size of TAG + size of LEN
    }

    #[doc(hidden)]
    fn write(&self, buffer: &mut Buffer, tag: u16, len: u32) -> Result<()> {
        buffer.write_u16(tag)?;
        buffer.write_u32(len)?;
        Ok(())
    }
}

// ------------------------------------- PredExpAnd

/// Predicate for And
#[derive(Debug, Clone)]
pub struct PredExpAnd {
    pred_exp_base: PredExpBase,
    #[doc(hidden)]
    pub nexpr: u16
}

impl PredExp for PredExpAnd {
    fn pred_string(&self) -> String {
        String::from("AND")
    }

    fn marshaled_size(&self) -> u32 {
        self.pred_exp_base.default_size() + 2
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.pred_exp_base.write(buffer, AS_PREDEXP_AND, 2)?;
        buffer.write_u16(self.nexpr)?;
        Ok(())
    }
}

/// Create "AND" Predicate
#[macro_export]
macro_rules! as_pred_and {
    ($nexpr:expr) => {{
        $crate::query::predexp::PredExpAnd{
            pred_exp_base: $crate::query::predexp::PredExpBase{},
            nexpr: $nexpr
        }
    }};
}

// ------------------------------------- PredExpOr

/// Predicate for Or
#[derive(Debug, Clone)]
pub struct PredExpOr {
    pred_exp_base: PredExpBase,
    #[doc(hidden)]
    pub nexpr: u16
}

impl PredExp for PredExpOr {
    fn pred_string(&self) -> String {
        String::from("OR")
    }

    fn marshaled_size(&self) -> u32 {
        self.pred_exp_base.default_size() + 2
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.pred_exp_base.write(buffer, AS_PREDEXP_OR, 2)?;
        buffer.write_u16(self.nexpr)?;
        Ok(())
    }
}

/// Create "OR" Predicate
#[macro_export]
macro_rules! as_pred_or {
    ($nexpr:expr) => {{
        $crate::query::predexp::PredExpOr{
            pred_exp_base: $crate::query::predexp::PredExpBase{},
            nexpr: $nexpr
        }
    }};
}

// ------------------------------------- PredExpNot

/// Predicate for Negation
#[derive(Debug, Clone)]
pub struct PredExpNot {
    pred_exp_base: PredExpBase,
}

impl PredExp for PredExpNot {
    fn pred_string(&self) -> String {
        String::from("NOT")
    }

    fn marshaled_size(&self) -> u32 {
        self.pred_exp_base.default_size()
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.pred_exp_base.write(buffer, AS_PREDEXP_NOT, 0)?;
        Ok(())
    }
}

/// Create "NOT" Predicate
#[macro_export]
macro_rules! as_pred_not {
    () => {{
        $crate::query::predexp::PredExpNot{
            pred_exp_base: $crate::query::predexp::PredExpBase{}
        }
    }};
}

// ------------------------------------- PredExpIntegerValue



#[cfg(test)]
mod tests {
    use crate::query::predexp::PredExp;

    #[test]
    fn predicate_macros() {
        let pred_and = as_pred_and!(2);
        assert_eq!(pred_and.pred_string(), "AND");
        assert_eq!(pred_and.nexpr, 2);

        let pred_or = as_pred_or!(2);
        assert_eq!(pred_or.pred_string(), "OR");
        assert_eq!(pred_or.nexpr, 2);

        let pred_not = as_pred_not!();
        assert_eq!(pred_not.pred_string(), "NOT");
    }
}
