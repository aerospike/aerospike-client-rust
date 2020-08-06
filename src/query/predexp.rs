use crate::commands::buffer::Buffer;
use crate::errors::Result;

#[doc(hidden)]
pub const _AS_PREDEXP_UNKNOWN_BIN: u16 = u16::max_value();
#[doc(hidden)]
pub const _AS_PREDEXP_AND: u16 = 1;
#[doc(hidden)]
pub const _AS_PREDEXP_OR: u16 = 2;
#[doc(hidden)]
pub const _AS_PREDEXP_NOT: u16 = 3;
#[doc(hidden)]
pub const _AS_PREDEXP_INTEGER_VALUE: u16 = 10;
#[doc(hidden)]
pub const _AS_PREDEXP_STRING_VALUE: u16 = 11;
#[doc(hidden)]
pub const _AS_PREDEXP_GEOJSON_VALUE: u16 = 12;
#[doc(hidden)]
pub const _AS_PREDEXP_INTEGER_BIN: u16 = 100;
#[doc(hidden)]
pub const _AS_PREDEXP_STRING_BIN: u16 = 101;
#[doc(hidden)]
pub const _AS_PREDEXP_GEOJSON_BIN: u16 = 102;
#[doc(hidden)]
pub const _AS_PREDEXP_LIST_BIN: u16 = 103;
#[doc(hidden)]
pub const _AS_PREDEXP_MAP_BIN: u16 = 104;
#[doc(hidden)]
pub const _AS_PREDEXP_INTEGER_VAR: u16 = 120;
#[doc(hidden)]
pub const _AS_PREDEXP_STRING_VAR: u16 = 121;
#[doc(hidden)]
pub const _AS_PREDEXP_GEOJSON_VAR: u16 = 122;
#[doc(hidden)]
pub const _AS_PREDEXP_REC_DEVICE_SIZE: u16 = 150;
#[doc(hidden)]
pub const _AS_PREDEXP_REC_LAST_UPDATE: u16 = 151;
#[doc(hidden)]
pub const _AS_PREDEXP_REC_VOID_TIME: u16 = 152;
#[doc(hidden)]
pub const _AS_PREDEXP_REC_DIGEST_MODULO: u16 = 153;
#[doc(hidden)]
pub const _AS_PREDEXP_INTEGER_EQUAL: u16 = 200;
#[doc(hidden)]
pub const _AS_PREDEXP_INTEGER_UNEQUAL: u16 = 201;
#[doc(hidden)]
pub const _AS_PREDEXP_INTEGER_GREATER: u16 = 202;
#[doc(hidden)]
pub const _AS_PREDEXP_INTEGER_GREATEREQ: u16 = 203;
#[doc(hidden)]
pub const _AS_PREDEXP_INTEGER_LESS: u16 = 204;
#[doc(hidden)]
pub const _AS_PREDEXP_INTEGER_LESSEQ: u16 = 205;
#[doc(hidden)]
pub const _AS_PREDEXP_STRING_EQUAL: u16 = 210;
#[doc(hidden)]
pub const _AS_PREDEXP_STRING_UNEQUAL: u16 = 211;
#[doc(hidden)]
pub const _AS_PREDEXP_STRING_REGEX: u16 = 212;
#[doc(hidden)]
pub const _AS_PREDEXP_GEOJSON_WITHIN: u16 = 220;
#[doc(hidden)]
pub const _AS_PREDEXP_GEOJSON_CONTAINS: u16 = 221;
#[doc(hidden)]
pub const _AS_PREDEXP_LIST_ITERATE_OR: u16 = 250;
#[doc(hidden)]
pub const _AS_PREDEXP_MAPKEY_ITERATE_OR: u16 = 251;
#[doc(hidden)]
pub const _AS_PREDEXP_MAPVAL_ITERATE_OR: u16 = 252;
#[doc(hidden)]
pub const _AS_PREDEXP_LIST_ITERATE_AND: u16 = 253;
#[doc(hidden)]
pub const _AS_PREDEXP_MAPKEY_ITERATE_AND: u16 = 254;
#[doc(hidden)]
pub const _AS_PREDEXP_MAPVAL_ITERATE_AND: u16 = 255;

#[doc(hidden)]
pub trait PredExp: Send + Sync {
    // Returns String Value of the Predicate action
    fn pred_string(&self) -> String;
    // Returns the absolute size of the Predicate (default_size + additional-size)
    fn marshaled_size(&self) -> usize;
    // Writes the PredExp to the Command Buffer
    fn write(&self, buffer: &mut Buffer) -> Result<()>;

    // Default Header Size
    #[doc(hidden)]
    fn default_size(&self) -> usize {
        2 + 4 // size of TAG + size of LEN
    }

    // Write tag und len to buffer
    #[doc(hidden)]
    fn write_head(&self, buffer: &mut Buffer, tag: u16, len: u32) -> Result<()> {
        buffer.write_u16(tag)?;
        buffer.write_u32(len)?;
        Ok(())
    }
}

// ------------------------------------- PredExpAnd

/// Predicate for And
#[derive(Debug, Clone)]
pub struct PredExpAnd {
    /// Number of Predicates
    pub nexpr: u16,
}

impl PredExp for PredExpAnd {
    fn pred_string(&self) -> String {
        String::from("AND")
    }

    fn marshaled_size(&self) -> usize {
        self.default_size() + 2
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, _AS_PREDEXP_AND, 2)?;
        buffer.write_u16(self.nexpr)?;
        Ok(())
    }
}

/// Create "AND" Predicate
#[macro_export]
macro_rules! as_predexp_and {
    ($nexpr:expr) => {{
        $crate::query::predexp::PredExpAnd { nexpr: $nexpr }
    }};
}

// ------------------------------------- PredExpOr

/// Predicate for Or
#[derive(Debug, Clone)]
pub struct PredExpOr {
    /// Number of Predicates
    pub nexpr: u16,
}

impl PredExp for PredExpOr {
    fn pred_string(&self) -> String {
        String::from("OR")
    }

    fn marshaled_size(&self) -> usize {
        self.default_size() + 2
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, _AS_PREDEXP_OR, 2)?;
        buffer.write_u16(self.nexpr)?;
        Ok(())
    }
}

/// Create "OR" Predicate
#[macro_export]
macro_rules! as_predexp_or {
    ($nexpr:expr) => {{
        $crate::query::predexp::PredExpOr { nexpr: $nexpr }
    }};
}

// ------------------------------------- PredExpNot

/// Predicate for Negation
#[derive(Debug, Clone)]
pub struct PredExpNot {}

impl PredExp for PredExpNot {
    fn pred_string(&self) -> String {
        String::from("NOT")
    }

    fn marshaled_size(&self) -> usize {
        self.default_size()
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, _AS_PREDEXP_NOT, 0)?;
        Ok(())
    }
}

/// Create "NOT" Predicate
#[macro_export]
macro_rules! as_predexp_not {
    () => {{
        $crate::query::predexp::PredExpNot {}
    }};
}

// ------------------------------------- PredExpIntegerValue

/// Predicate for Integer Values
#[derive(Debug, Clone)]
pub struct PredExpIntegerValue {
    /// Value
    pub val: i64,
}

impl PredExp for PredExpIntegerValue {
    fn pred_string(&self) -> String {
        self.val.to_string()
    }

    fn marshaled_size(&self) -> usize {
        self.default_size() + 8
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, _AS_PREDEXP_INTEGER_VALUE, 8)?;
        buffer.write_i64(self.val)?;
        Ok(())
    }
}

/// Create Integer Value Predicate
#[macro_export]
macro_rules! as_predexp_integer_value {
    ($val:expr) => {{
        $crate::query::predexp::PredExpIntegerValue { val: $val }
    }};
}

// ------------------------------------- PredExpStringValue

/// Predicate for Integer Values
#[derive(Debug, Clone)]
pub struct PredExpStringValue {
    /// Value
    pub val: String,
}

impl PredExp for PredExpStringValue {
    fn pred_string(&self) -> String {
        self.val.clone()
    }

    fn marshaled_size(&self) -> usize {
        self.default_size() + self.val.len()
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, _AS_PREDEXP_STRING_VALUE, self.val.len() as u32)?;
        buffer.write_str(&self.val)?;
        Ok(())
    }
}

/// Create String Value Predicate
#[macro_export]
macro_rules! as_predexp_string_value {
    ($val:expr) => {{
        $crate::query::predexp::PredExpStringValue { val: $val }
    }};
}

// ------------------------------------- PredExpGeoJSONValue

/// Predicate for `GeoJSON` Values
#[derive(Debug, Clone)]
pub struct PredExpGeoJSONValue {
    /// Value
    pub val: String,
}

impl PredExp for PredExpGeoJSONValue {
    fn pred_string(&self) -> String {
        self.val.clone()
    }

    fn marshaled_size(&self) -> usize {
        self.default_size()
            + 1 // flags
            + 2 // ncells
            + self.val.len()
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(
            buffer,
            _AS_PREDEXP_GEOJSON_VALUE,
            (1 + 2 + self.val.len()) as u32,
        )?;
        buffer.write_u8(0u8)?;
        buffer.write_u16(0)?;
        buffer.write_str(&self.val)?;
        Ok(())
    }
}

/// Create GeoJSON Value Predicate
#[macro_export]
macro_rules! as_predexp_geojson_value {
    ($val:expr) => {{
        $crate::query::predexp::PredExpGeoJSONValue { val: $val }
    }};
}

// ------------------------------------- PredExpBin

/// Predicate for Bins
#[derive(Debug, Clone)]
pub struct PredExpBin {
    /// Bin Name
    pub name: String,
    /// Bin Type
    pub tag: u16,
}

impl PredExp for PredExpBin {
    fn pred_string(&self) -> String {
        self.name.clone()
    }

    fn marshaled_size(&self) -> usize {
        self.default_size() + self.name.len()
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, self.tag, (self.name.len()) as u32)?;
        buffer.write_str(&self.name)?;
        Ok(())
    }
}

/// Create Unknown Bin Predicate
#[macro_export]
macro_rules! as_predexp_unknown_bin {
    ($name:expr) => {{
        $crate::query::predexp::PredExpBin {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_UNKNOWN_BIN,
        }
    }};
}

/// Create Integer Bin Predicate
#[macro_export]
macro_rules! as_predexp_integer_bin {
    ($name:expr) => {{
        $crate::query::predexp::PredExpBin {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_INTEGER_BIN,
        }
    }};
}

/// Create String Bin Predicate
#[macro_export]
macro_rules! as_predexp_string_bin {
    ($name:expr) => {{
        $crate::query::predexp::PredExpBin {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_STRING_BIN,
        }
    }};
}

/// Create GeoJSON Bin Predicate
#[macro_export]
macro_rules! as_predexp_geojson_bin {
    ($name:expr) => {{
        $crate::query::predexp::PredExpBin {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_GEOJSON_BIN,
        }
    }};
}

/// Create List Bin Predicate
#[macro_export]
macro_rules! as_predexp_list_bin {
    ($name:expr) => {{
        $crate::query::predexp::PredExpBin {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_LIST_BIN,
        }
    }};
}

/// Create Map Bin Predicate
#[macro_export]
macro_rules! as_predexp_map_bin {
    ($name:expr) => {{
        $crate::query::predexp::PredExpBin {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_MAP_BIN,
        }
    }};
}

// ------------------------------------- PredExpVar

/// Predicate for Vars
#[derive(Debug, Clone)]
pub struct PredExpVar {
    /// Var Name
    pub name: String,
    /// Var Type
    pub tag: u16,
}

impl PredExp for PredExpVar {
    fn pred_string(&self) -> String {
        self.name.clone()
    }

    fn marshaled_size(&self) -> usize {
        self.default_size() + self.name.len()
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, self.tag, (self.name.len()) as u32)?;
        buffer.write_str(&self.name)?;
        Ok(())
    }
}

/// Create 64Bit Integer Var used in list/map iterations
#[macro_export]
macro_rules! as_predexp_integer_var {
    ($name:expr) => {{
        $crate::query::predexp::PredExpVar {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_INTEGER_VAR,
        }
    }};
}

/// Create String Var used in list/map iterations
#[macro_export]
macro_rules! as_predexp_string_var {
    ($name:expr) => {{
        $crate::query::predexp::PredExpVar {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_STRING_VAR,
        }
    }};
}

/// Create String Var used in list/map iterations
#[macro_export]
macro_rules! as_predexp_geojson_var {
    ($name:expr) => {{
        $crate::query::predexp::PredExpVar {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_GEOJSON_VAR,
        }
    }};
}

// ------------------------------------- PredExpMD

/// Predicate for `MetaData` (`RecDeviceSize`, `RecLastUpdate`, `RecVoidTime`)
#[derive(Debug, Clone)]
pub struct PredExpMD {
    /// Predicate Type
    pub tag: u16, // not marshaled
}

impl PredExp for PredExpMD {
    fn pred_string(&self) -> String {
        match self.tag {
            _AS_PREDEXP_REC_DEVICE_SIZE => String::from("rec.DeviceSize"),
            _AS_PREDEXP_REC_LAST_UPDATE => String::from("rec.LastUpdate"),
            _AS_PREDEXP_REC_VOID_TIME => String::from("rec.Expiration"),
            _AS_PREDEXP_REC_DIGEST_MODULO => String::from("rec.DigestModulo"),
            _ => panic!("Invalid Metadata tag."),
        }
    }

    fn marshaled_size(&self) -> usize {
        self.default_size()
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, self.tag, 0)?;
        Ok(())
    }
}

/// Create Record Size on Disk Predicate
#[macro_export]
macro_rules! as_predexp_rec_device_size {
    () => {{
        $crate::query::predexp::PredExpMD {
            tag: $crate::query::predexp::_AS_PREDEXP_REC_DEVICE_SIZE,
        }
    }};
}

/// Create Last Update Predicate
#[macro_export]
macro_rules! as_predexp_rec_last_update {
    () => {{
        $crate::query::predexp::PredExpMD {
            tag: $crate::query::predexp::_AS_PREDEXP_REC_LAST_UPDATE,
        }
    }};
}

/// Create Record Expiration Time Predicate in nanoseconds since 1970-01-01 epoch as 64 bit integer
#[macro_export]
macro_rules! as_predexp_rec_void_time {
    () => {{
        $crate::query::predexp::PredExpMD {
            tag: $crate::query::predexp::_AS_PREDEXP_REC_VOID_TIME,
        }
    }};
}

// ------------------------------------- PredExpMDDigestModulo

/// Predicate Digest Modulo Metadata Value
/// The digest modulo expression assumes the value of 4 bytes of the
// record's key digest modulo as its argument.
// This predicate is available in Aerospike server versions 3.12.1+
#[derive(Debug, Clone)]
pub struct PredExpMDDigestModulo {
    /// Modulo
    pub modulo: i32, // not marshaled
}

impl PredExp for PredExpMDDigestModulo {
    fn pred_string(&self) -> String {
        String::from("rec.DigestModulo")
    }

    fn marshaled_size(&self) -> usize {
        self.default_size() + 4
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, _AS_PREDEXP_REC_DIGEST_MODULO, 4)?;
        buffer.write_i32(self.modulo)?;
        Ok(())
    }
}

/// Creates a digest modulo record metadata value predicate expression.
#[macro_export]
macro_rules! as_predexp_rec_digest_modulo {
    ($modulo:expr) => {{
        $crate::query::predexp::PredExpMDDigestModulo { modulo: $modulo }
    }};
}

// ------------------------------------- PredExpCompare

/// Predicate for comparing
#[derive(Debug, Clone)]
pub struct PredExpCompare {
    /// Compare Type
    pub tag: u16, // not marshaled
}

impl PredExp for PredExpCompare {
    fn pred_string(&self) -> String {
        match self.tag {
            _AS_PREDEXP_INTEGER_EQUAL | _AS_PREDEXP_STRING_EQUAL => String::from("="),
            _AS_PREDEXP_INTEGER_UNEQUAL | _AS_PREDEXP_STRING_UNEQUAL => String::from("!="),
            _AS_PREDEXP_INTEGER_GREATER => String::from(">"),
            _AS_PREDEXP_INTEGER_GREATEREQ => String::from(">="),
            _AS_PREDEXP_INTEGER_LESS => String::from("<"),
            _AS_PREDEXP_INTEGER_LESSEQ => String::from("<="),
            _AS_PREDEXP_STRING_REGEX => String::from("~="),
            _AS_PREDEXP_GEOJSON_CONTAINS => String::from("CONTAINS"),
            _AS_PREDEXP_GEOJSON_WITHIN => String::from("WITHIN"),
            _ => panic!("unexpected predicate tag"),
        }
    }

    fn marshaled_size(&self) -> usize {
        self.default_size()
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, self.tag, 0)?;
        Ok(())
    }
}

/// Creates Equal predicate for integer values
#[macro_export]
macro_rules! as_predexp_integer_equal {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_INTEGER_EQUAL,
        }
    }};
}

/// Creates NotEqual predicate for integer values
#[macro_export]
macro_rules! as_predexp_integer_unequal {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_INTEGER_UNEQUAL,
        }
    }};
}

/// Creates Greater Than predicate for integer values
#[macro_export]
macro_rules! as_predexp_integer_greater {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_INTEGER_GREATER,
        }
    }};
}

/// Creates Greater Than Or Equal predicate for integer values
#[macro_export]
macro_rules! as_predexp_integer_greatereq {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_INTEGER_GREATEREQ,
        }
    }};
}

/// Creates Less Than predicate for integer values
#[macro_export]
macro_rules! as_predexp_integer_less {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_INTEGER_LESS,
        }
    }};
}

/// Creates Less Than Or Equal predicate for integer values
#[macro_export]
macro_rules! as_predexp_integer_lesseq {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_INTEGER_LESSEQ,
        }
    }};
}

/// Creates Equal predicate for string values
#[macro_export]
macro_rules! as_predexp_string_equal {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_STRING_EQUAL,
        }
    }};
}

/// Creates Not Equal predicate for string values
#[macro_export]
macro_rules! as_predexp_string_unequal {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_STRING_UNEQUAL,
        }
    }};
}

/// Creates Within Region predicate for `GeoJSON` values
#[macro_export]
macro_rules! as_predexp_geojson_within {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_GEOJSON_WITHIN,
        }
    }};
}

/// Creates Region Contains predicate for GeoJSON values
#[macro_export]
macro_rules! as_predexp_geojson_contains {
    () => {{
        $crate::query::predexp::PredExpCompare {
            tag: $crate::query::predexp::_AS_PREDEXP_GEOJSON_CONTAINS,
        }
    }};
}

// ------------------------------------- PredExpStringRegex

/// Predicate for String Regex
#[derive(Debug, Clone)]
pub struct PredExpStringRegex {
    /// Flags
    pub cflags: u32, // not marshaled
}

impl PredExp for PredExpStringRegex {
    fn pred_string(&self) -> String {
        String::from("regex:")
    }

    fn marshaled_size(&self) -> usize {
        self.default_size() + 4
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, _AS_PREDEXP_STRING_REGEX, 4)?;
        buffer.write_u32(self.cflags)?;
        Ok(())
    }
}

/// Creates a Regex predicate
#[macro_export]
macro_rules! as_predexp_string_regex {
    ($cflags:expr) => {{
        $crate::query::predexp::PredExpStringRegex { cflags: $cflags }
    }};
}

// ------------------------------------- PredExp???Iterate???

/// Predicate for Iterators
#[derive(Debug, Clone)]
pub struct PredExpIter {
    /// Name
    pub name: String,
    /// Iter Type
    pub tag: u16, // not marshaled
}

impl PredExp for PredExpIter {
    fn pred_string(&self) -> String {
        match self.tag {
            _AS_PREDEXP_LIST_ITERATE_OR => {
                let mut tagname = String::from("list_iterate_or using \"");
                tagname.push_str(&self.name);
                tagname.push_str("\":");
                tagname
            }
            _AS_PREDEXP_MAPKEY_ITERATE_OR => {
                let mut tagname = String::from("mapkey_iterate_or using \"");
                tagname.push_str(&self.name);
                tagname.push_str("\":");
                tagname
            }
            _AS_PREDEXP_MAPVAL_ITERATE_OR => {
                let mut tagname = String::from("mapval_iterate_or using \"");
                tagname.push_str(&self.name);
                tagname.push_str("\":");
                tagname
            }
            _AS_PREDEXP_LIST_ITERATE_AND => {
                let mut tagname = String::from("list_iterate_and using \"");
                tagname.push_str(&self.name);
                tagname.push_str("\":");
                tagname
            }
            _AS_PREDEXP_MAPKEY_ITERATE_AND => {
                let mut tagname = String::from("mapkey_iterate_and using \"");
                tagname.push_str(&self.name);
                tagname.push_str("\":");
                tagname
            }
            _AS_PREDEXP_MAPVAL_ITERATE_AND => {
                let mut tagname = String::from("mapvalue_iterate_and using \"");
                tagname.push_str(&self.name);
                tagname.push_str("\":");
                tagname
            }
            _ => panic!("Invalid Metadata tag."),
        }
    }

    fn marshaled_size(&self) -> usize {
        self.default_size() + self.name.len()
    }

    fn write(&self, buffer: &mut Buffer) -> Result<()> {
        self.write_head(buffer, _AS_PREDEXP_STRING_REGEX, self.name.len() as u32)?;
        buffer.write_str(&self.name)?;
        Ok(())
    }
}

/// Creates an Or iterator predicate for list items
#[macro_export]
macro_rules! as_predexp_list_iterate_or {
    ($name:expr) => {{
        $crate::query::predexp::PredExpIter {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_LIST_ITERATE_OR,
        }
    }};
}

/// Creates an And iterator predicate for list items
#[macro_export]
macro_rules! as_predexp_list_iterate_and {
    ($name:expr) => {{
        $crate::query::predexp::PredExpIter {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_LIST_ITERATE_AND,
        }
    }};
}

/// Creates an Or iterator predicate on map keys
#[macro_export]
macro_rules! as_predexp_mapkey_iterate_or {
    ($name:expr) => {{
        $crate::query::predexp::PredExpIter {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_MAPKEY_ITERATE_OR,
        }
    }};
}

/// Creates an And iterator predicate on map keys
#[macro_export]
macro_rules! as_predexp_mapkey_iterate_and {
    ($name:expr) => {{
        $crate::query::predexp::PredExpIter {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_MAPKEY_ITERATE_AND,
        }
    }};
}

/// Creates an Or iterator predicate on map values
#[macro_export]
macro_rules! as_predexp_mapval_iterate_or {
    ($name:expr) => {{
        $crate::query::predexp::PredExpIter {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_MAPVAL_ITERATE_OR,
        }
    }};
}

/// Creates an And iterator predicate on map values
#[macro_export]
macro_rules! as_predexp_mapval_iterate_and {
    ($name:expr) => {{
        $crate::query::predexp::PredExpIter {
            name: $name,
            tag: $crate::query::predexp::_AS_PREDEXP_MAPVAL_ITERATE_AND,
        }
    }};
}

#[cfg(test)]
mod tests {
    use crate::query::predexp::*;

    #[test]
    fn predicate_macros() {
        let pred_and = as_predexp_and!(2);
        assert_eq!(pred_and.pred_string(), "AND");
        assert_eq!(pred_and.nexpr, 2);

        let pred_or = as_predexp_or!(2);
        assert_eq!(pred_or.pred_string(), "OR");
        assert_eq!(pred_or.nexpr, 2);

        let pred_not = as_predexp_not!();
        assert_eq!(pred_not.pred_string(), "NOT");

        let pred_int_val = as_predexp_integer_value!(2);
        assert_eq!(pred_int_val.pred_string(), "2");
        assert_eq!(pred_int_val.val, 2);

        let pred_str_val = as_predexp_string_value!(String::from("test"));
        assert_eq!(pred_str_val.pred_string(), "test");
        assert_eq!(pred_str_val.val, "test");

        let pred_geo_val = as_predexp_geojson_value!(String::from("test"));
        assert_eq!(pred_geo_val.pred_string(), "test");
        assert_eq!(pred_geo_val.val, "test");

        let bin_unknown = as_predexp_unknown_bin!(String::from("test"));
        assert_eq!(bin_unknown.pred_string(), "test");
        assert_eq!(bin_unknown.tag, _AS_PREDEXP_UNKNOWN_BIN);

        let int_bin = as_predexp_integer_bin!(String::from("test"));
        assert_eq!(int_bin.pred_string(), "test");
        assert_eq!(int_bin.tag, _AS_PREDEXP_INTEGER_BIN);

        let str_bin = as_predexp_string_bin!(String::from("test"));
        assert_eq!(str_bin.pred_string(), "test");
        assert_eq!(str_bin.tag, _AS_PREDEXP_STRING_BIN);

        let geo_bin = as_predexp_geojson_bin!(String::from("test"));
        assert_eq!(geo_bin.pred_string(), "test");
        assert_eq!(geo_bin.tag, _AS_PREDEXP_GEOJSON_BIN);

        let list_bin = as_predexp_list_bin!(String::from("test"));
        assert_eq!(list_bin.pred_string(), "test");
        assert_eq!(list_bin.tag, _AS_PREDEXP_LIST_BIN);

        let map_bin = as_predexp_map_bin!(String::from("test"));
        assert_eq!(map_bin.pred_string(), "test");
        assert_eq!(map_bin.tag, _AS_PREDEXP_MAP_BIN);

        let int_var = as_predexp_integer_var!(String::from("test"));
        assert_eq!(int_var.pred_string(), "test");
        assert_eq!(int_var.tag, _AS_PREDEXP_INTEGER_VAR);

        let str_var = as_predexp_string_var!(String::from("test"));
        assert_eq!(str_var.pred_string(), "test");
        assert_eq!(str_var.tag, _AS_PREDEXP_STRING_VAR);

        let geo_var = as_predexp_geojson_var!(String::from("test"));
        assert_eq!(geo_var.pred_string(), "test");
        assert_eq!(geo_var.tag, _AS_PREDEXP_GEOJSON_VAR);

        let dev_size = as_predexp_rec_device_size!();
        assert_eq!(dev_size.tag, _AS_PREDEXP_REC_DEVICE_SIZE);

        let last_update = as_predexp_rec_last_update!();
        assert_eq!(last_update.tag, _AS_PREDEXP_REC_LAST_UPDATE);

        let void_time = as_predexp_rec_void_time!();
        assert_eq!(void_time.tag, _AS_PREDEXP_REC_VOID_TIME);

        let digest_modulo = as_predexp_rec_digest_modulo!(10);
        assert_eq!(digest_modulo.modulo, 10);

        let int_eq = as_predexp_integer_equal!();
        assert_eq!(int_eq.tag, _AS_PREDEXP_INTEGER_EQUAL);

        let int_uneq = as_predexp_integer_unequal!();
        assert_eq!(int_uneq.tag, _AS_PREDEXP_INTEGER_UNEQUAL);

        let int_gt = as_predexp_integer_greater!();
        assert_eq!(int_gt.tag, _AS_PREDEXP_INTEGER_GREATER);

        let int_gteq = as_predexp_integer_greatereq!();
        assert_eq!(int_gteq.tag, _AS_PREDEXP_INTEGER_GREATEREQ);

        let int_lt = as_predexp_integer_less!();
        assert_eq!(int_lt.tag, _AS_PREDEXP_INTEGER_LESS);

        let int_lteq = as_predexp_integer_lesseq!();
        assert_eq!(int_lteq.tag, _AS_PREDEXP_INTEGER_LESSEQ);

        let str_eq = as_predexp_string_equal!();
        assert_eq!(str_eq.tag, _AS_PREDEXP_STRING_EQUAL);

        let str_uneq = as_predexp_string_unequal!();
        assert_eq!(str_uneq.tag, _AS_PREDEXP_STRING_UNEQUAL);

        let geo_within = as_predexp_geojson_within!();
        assert_eq!(geo_within.tag, _AS_PREDEXP_GEOJSON_WITHIN);

        let geo_contains = as_predexp_geojson_contains!();
        assert_eq!(geo_contains.tag, _AS_PREDEXP_GEOJSON_CONTAINS);

        let string_reg = as_predexp_string_regex!(5);
        assert_eq!(string_reg.cflags, 5);
    }
}
