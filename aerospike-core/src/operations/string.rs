// Copyright 2015-2026 Aerospike, Inc.
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

//! String operations. Build operations to be passed to the client's `operate()`
//! call for inspecting and modifying string bins.
//!
//! Index orientation is left-to-right with codepoint addressing. Negative
//! indexes count from the end of the string (-1 = last codepoint). Out-of-bounds
//! indexes are clamped to the valid range; no error is returned.
//!
//! String operations require Aerospike Server version 8.1.3 or later. A non-empty
//! [`CdtContext`] argument navigates into a string nested inside a list or map
//! bin; with an empty context, the operation targets the bin itself. The leaf
//! that the context resolves to must already be an Aerospike string —
//! operations on non-string leaves return `BIN_TYPE_ERROR`.

use crate::commands::buffer::Buffer;
use crate::msgpack::encoder::{pack_array_begin, pack_integer, pack_value};
use crate::operations::cdt_context::DEFAULT_CTX;
use crate::operations::{CdtContext, Operation, OperationBin, OperationData, OperationType};
use crate::{Result, Value};

// Read op codes (server-side subop ids).
const STR_OP_STRLEN: u8 = 0;
const STR_OP_SUBSTR: u8 = 1;
const STR_OP_CHAR_AT: u8 = 2;
const STR_OP_FIND: u8 = 3;
const STR_OP_CONTAINS: u8 = 4;
const STR_OP_STARTS_WITH: u8 = 5;
const STR_OP_ENDS_WITH: u8 = 6;
const STR_OP_TO_INTEGER: u8 = 7;
const STR_OP_TO_DOUBLE: u8 = 8;
const STR_OP_BYTE_LENGTH: u8 = 9;
const STR_OP_IS_NUMERIC: u8 = 10;
const STR_OP_IS_UPPER: u8 = 11;
const STR_OP_IS_LOWER: u8 = 12;
const STR_OP_TO_BLOB: u8 = 13;
const STR_OP_SPLIT: u8 = 14;
const STR_OP_B64_DECODE: u8 = 15;
const STR_OP_REGEX_COMPARE: u8 = 16;

// Modify op codes (server-side subop ids).
const STR_OP_INSERT: u8 = 50;
const STR_OP_OVERWRITE: u8 = 51;
const STR_OP_CONCAT: u8 = 52;
const STR_OP_SNIP: u8 = 53;
const STR_OP_REPLACE: u8 = 54;
const STR_OP_REPLACE_ALL: u8 = 55;
const STR_OP_UPPER: u8 = 56;
const STR_OP_LOWER: u8 = 57;
const STR_OP_CASE_FOLD: u8 = 58;
const STR_OP_NORMALIZE_NFC: u8 = 59;
const STR_OP_TRIM_START: u8 = 60;
const STR_OP_TRIM_END: u8 = 61;
const STR_OP_TRIM: u8 = 62;
const STR_OP_PAD_START: u8 = 63;
const STR_OP_PAD_END: u8 = 64;
const STR_OP_REPEAT: u8 = 65;
const STR_OP_REGEX_REPLACE: u8 = 66;
const STR_OP_APPEND: u8 = 67;
const STR_OP_PREPEND: u8 = 68;

/// Numeric type filter used by [`is_numeric_typed`]. Combine with the
/// `is_numeric` sub-op to restrict validation to integers or floats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringNumericType {
    /// Match either an integer or a floating-point number.
    Any = 0,
    /// Match only integers.
    Int = 1,
    /// Match only floating-point numbers.
    Float = 2,
}

/// Regex behavior flags for [`regex_compare_with_flags`] and [`regex_replace`].
/// Combine values with bitwise OR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringRegexFlags(pub i64);

impl StringRegexFlags {
    /// Default ICU regex flags (no options).
    pub const DEFAULT: StringRegexFlags = StringRegexFlags(0);
    /// Case-insensitive matching.
    pub const CASE_INSENSITIVE: StringRegexFlags = StringRegexFlags(1 << 0);
    /// Multi-line: `^` and `$` match the start and end of any line.
    pub const MULTILINE: StringRegexFlags = StringRegexFlags(1 << 1);
    /// `.` matches any character including line terminators.
    pub const DOT_ALL: StringRegexFlags = StringRegexFlags(1 << 2);
    /// Only `\n` is treated as a line terminator (Unix-style line endings).
    pub const UNIX_LINES: StringRegexFlags = StringRegexFlags(1 << 3);
    /// Replace every match. Only meaningful for regex replace operations.
    pub const GLOBAL: StringRegexFlags = StringRegexFlags(1 << 4);
}

impl std::ops::BitOr for StringRegexFlags {
    type Output = StringRegexFlags;
    fn bitor(self, rhs: StringRegexFlags) -> StringRegexFlags {
        StringRegexFlags(self.0 | rhs.0)
    }
}

/// Write semantics for string modify operations. Combine values with bitwise OR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringWriteFlags(pub i64);

impl StringWriteFlags {
    /// Allow create or update.
    pub const DEFAULT: StringWriteFlags = StringWriteFlags(0);
    /// Suppress the error if the operation cannot be applied (e.g. missing bin).
    /// The bin is left unchanged and a nil result is returned for that op.
    pub const NO_FAIL: StringWriteFlags = StringWriteFlags(4);
}

impl std::ops::BitOr for StringWriteFlags {
    type Output = StringWriteFlags;
    fn bitor(self, rhs: StringWriteFlags) -> StringWriteFlags {
        StringWriteFlags(self.0 | rhs.0)
    }
}

/// Per-operation policy carrying [`StringWriteFlags`]. Passed inline to each
/// modify builder; it is not part of the client's dynamic configuration.
/// Mirrors how [`BitPolicy`](crate::operations::bitwise::BitPolicy) is scoped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringPolicy {
    flags: i64,
}

impl StringPolicy {
    /// Construct a new policy with the supplied write flags.
    pub const fn new(flags: StringWriteFlags) -> Self {
        StringPolicy { flags: flags.0 }
    }

    /// Returns the raw write-flags bitmask carried by this policy.
    pub(crate) const fn flags(&self) -> i64 {
        self.flags
    }
}

impl Default for StringPolicy {
    fn default() -> Self {
        StringPolicy::new(StringWriteFlags::DEFAULT)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StringOp {
    sub_op: u8,
    args: Vec<Value>,
}

impl StringOp {
    pub(crate) fn estimate_size(&self, ctx: &[CdtContext]) -> Result<usize> {
        pack_string_op(&mut None, self.sub_op, &self.args, ctx)
    }

    pub(crate) fn write_to(&self, buf: &mut Buffer, ctx: &[CdtContext]) -> Result<usize> {
        pack_string_op(&mut Some(buf), self.sub_op, &self.args, ctx)
    }
}

fn read_op(sub_op: u8, bin: &str, args: Vec<Value>) -> Operation {
    Operation {
        op: OperationType::StringRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::StringOp(StringOp { sub_op, args }),
    }
}

fn modify_op(sub_op: u8, bin: &str, args: Vec<Value>) -> Operation {
    Operation {
        op: OperationType::StringModify,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::StringOp(StringOp { sub_op, args }),
    }
}

// Build the msgpack payload for a string op. The layout matches the server's
// particle_string.c `string_state_init`:
//   - No CTX:   [SUBOP, args...]
//   - With CTX: [0xFF, [ctx_id_1, ctx_value_1, ...], SUBOP, args...]
// SUBOP and args sit at the outer level alongside the 0xFF sentinel — there is
// no nested array around them (this is different from list/map/bitwise ops).
fn pack_string_op(
    buf: &mut Option<&mut Buffer>,
    sub_op: u8,
    args: &[Value],
    ctx: &[CdtContext],
) -> Result<usize> {
    let mut size: usize = 0;
    let has_ctx = !ctx.is_empty();
    let inner_count = 1 + args.len();
    let outer_count = if has_ctx {
        2 + inner_count
    } else {
        inner_count
    };

    size += pack_array_begin(buf, outer_count);

    if has_ctx {
        size += pack_integer(buf, 0xff);
        size += pack_array_begin(buf, ctx.len() * 2);
        for c in ctx {
            if c.id == 0 {
                size += pack_integer(buf, i64::from(c.id));
            } else {
                size += pack_integer(buf, i64::from(c.id | u16::from(c.flags)));
            }
            if let Some(ref exp) = c.expression {
                size += exp.pack_binary(buf)?;
            } else {
                size += pack_value(buf, &c.value)?;
            }
        }
    }

    size += pack_integer(buf, i64::from(sub_op));
    for a in args {
        size += pack_value(buf, a)?;
    }

    Ok(size)
}

// -----------------------------------------------------------------
// Read operations
// -----------------------------------------------------------------

/// `strlen` operation. The server returns the number of Unicode codepoints in
/// the string bin as an `i64`. This is the codepoint count — not the grapheme
/// cluster count and not the UTF-8 byte length. Use [`byte_length`] for the
/// byte length.
pub fn strlen(bin: &str) -> Operation {
    read_op(STR_OP_STRLEN, bin, vec![])
}

/// `substr` operation that reads from codepoint `start` to the end of the
/// string. Negative `start` counts from the end.
pub fn substr_from(bin: &str, start: i64) -> Operation {
    read_op(STR_OP_SUBSTR, bin, vec![Value::Int(start)])
}

/// `substr` operation that reads `length` codepoints starting at codepoint
/// `start`. Negative indexes count from the end. `length` is clamped to the
/// remaining string length.
pub fn substr(bin: &str, start: i64, length: i64) -> Operation {
    read_op(
        STR_OP_SUBSTR,
        bin,
        vec![Value::Int(start), Value::Int(length)],
    )
}

/// `charAt` operation. Returns the codepoint at `index` as a one-codepoint
/// string. Negative indexes count from the end.
pub fn char_at(bin: &str, index: i64) -> Operation {
    read_op(STR_OP_CHAR_AT, bin, vec![Value::Int(index)])
}

/// `find` operation. Returns the codepoint index of the first occurrence of
/// `needle`, or -1 if not found.
pub fn find(bin: &str, needle: &str) -> Operation {
    read_op(STR_OP_FIND, bin, vec![Value::from(needle)])
}

/// `find` operation that locates a specific `occurrence` of `needle` (1 = first
/// match, -1 = last match). Returns the codepoint index of that match, or -1
/// if not found.
pub fn find_nth(bin: &str, needle: &str, occurrence: i64) -> Operation {
    read_op(
        STR_OP_FIND,
        bin,
        vec![Value::from(needle), Value::Int(occurrence)],
    )
}

/// `contains` operation. Returns true if the bin contains `needle` as a
/// substring, false otherwise.
pub fn contains(bin: &str, needle: &str) -> Operation {
    read_op(STR_OP_CONTAINS, bin, vec![Value::from(needle)])
}

/// `startsWith` operation. Returns true if the bin begins with `prefix`,
/// false otherwise.
pub fn starts_with(bin: &str, prefix: &str) -> Operation {
    read_op(STR_OP_STARTS_WITH, bin, vec![Value::from(prefix)])
}

/// `endsWith` operation. Returns true if the bin ends with `suffix`,
/// false otherwise.
pub fn ends_with(bin: &str, suffix: &str) -> Operation {
    read_op(STR_OP_ENDS_WITH, bin, vec![Value::from(suffix)])
}

/// `toInteger` operation. The server parses the string as an `i64`. Returns
/// `PARAMETER_ERROR` if the bin cannot be parsed as an integer.
pub fn to_integer(bin: &str) -> Operation {
    read_op(STR_OP_TO_INTEGER, bin, vec![])
}

/// `toDouble` operation. The server parses the string as a 64-bit float.
/// Returns `PARAMETER_ERROR` if the bin cannot be parsed as a double.
pub fn to_double(bin: &str) -> Operation {
    read_op(STR_OP_TO_DOUBLE, bin, vec![])
}

/// `byteLength` operation. Returns the number of UTF-8 bytes in the string
/// (`i64`). Differs from [`strlen`] for non-ASCII content where one codepoint
/// can encode to multiple bytes.
pub fn byte_length(bin: &str) -> Operation {
    read_op(STR_OP_BYTE_LENGTH, bin, vec![])
}

/// `isNumeric` operation. Returns true if the bin contains a valid integer or
/// float, false otherwise.
pub fn is_numeric(bin: &str) -> Operation {
    read_op(STR_OP_IS_NUMERIC, bin, vec![])
}

/// `isNumeric` operation that filters by [`StringNumericType`] (e.g. restrict
/// to integer-only or float-only validation).
pub fn is_numeric_typed(bin: &str, numeric_type: StringNumericType) -> Operation {
    read_op(
        STR_OP_IS_NUMERIC,
        bin,
        vec![Value::Int(numeric_type as i64)],
    )
}

/// `isUpper` operation. Returns true if every cased codepoint in the bin is
/// uppercase, false otherwise.
pub fn is_upper(bin: &str) -> Operation {
    read_op(STR_OP_IS_UPPER, bin, vec![])
}

/// `isLower` operation. Returns true if every cased codepoint in the bin is
/// lowercase, false otherwise.
pub fn is_lower(bin: &str) -> Operation {
    read_op(STR_OP_IS_LOWER, bin, vec![])
}

/// `toBlob` operation. Returns the UTF-8 bytes of the string as a blob.
pub fn to_blob(bin: &str) -> Operation {
    read_op(STR_OP_TO_BLOB, bin, vec![])
}

/// `split` operation that splits by Unicode codepoint — each codepoint becomes
/// its own element of the returned list.
pub fn split(bin: &str) -> Operation {
    read_op(STR_OP_SPLIT, bin, vec![])
}

/// `split` operation that splits the bin by the `separator` substring. If the
/// separator is absent the result is a singleton list containing the whole
/// string.
pub fn split_by_separator(bin: &str, separator: &str) -> Operation {
    read_op(STR_OP_SPLIT, bin, vec![Value::from(separator)])
}

/// `b64Decode` operation. The server treats the bin as base64-encoded text and
/// returns the decoded bytes as a blob.
pub fn b64_decode(bin: &str) -> Operation {
    read_op(STR_OP_B64_DECODE, bin, vec![])
}

/// `regexCompare` operation. The server matches `pattern` (ICU regex syntax)
/// against the bin and returns true on match, false otherwise.
pub fn regex_compare(bin: &str, pattern: &str) -> Operation {
    read_op(STR_OP_REGEX_COMPARE, bin, vec![Value::from(pattern)])
}

/// `regexCompare` operation that honors [`StringRegexFlags`] (e.g.
/// `CASE_INSENSITIVE`). Flag values may be combined with bitwise OR.
pub fn regex_compare_with_flags(
    bin: &str,
    pattern: &str,
    regex_flags: StringRegexFlags,
) -> Operation {
    read_op(
        STR_OP_REGEX_COMPARE,
        bin,
        vec![Value::from(pattern), Value::Int(regex_flags.0)],
    )
}

// -----------------------------------------------------------------
// Modify operations
// -----------------------------------------------------------------

/// `insert` operation that splices `value` into the bin at codepoint `index`.
/// Negative indexes count from the end of the string.
pub fn insert(policy: &StringPolicy, bin: &str, index: i64, value: &str) -> Operation {
    modify_op(
        STR_OP_INSERT,
        bin,
        vec![
            Value::Int(index),
            Value::from(value),
            Value::Int(policy.flags),
        ],
    )
}

/// `overwrite` operation that overwrites codepoints starting at codepoint
/// `index` with `value`. The result may grow beyond the original length when
/// `value` extends past the end.
pub fn overwrite(policy: &StringPolicy, bin: &str, index: i64, value: &str) -> Operation {
    modify_op(
        STR_OP_OVERWRITE,
        bin,
        vec![
            Value::Int(index),
            Value::from(value),
            Value::Int(policy.flags),
        ],
    )
}

/// `concat` operation that appends `value` to the bin.
pub fn concat(policy: &StringPolicy, bin: &str, value: &str) -> Operation {
    modify_op(
        STR_OP_CONCAT,
        bin,
        vec![
            Value::List(vec![Value::from(value)]),
            Value::Int(policy.flags),
        ],
    )
}

/// `concat` operation that appends each element of `values` to the bin in order.
pub fn concat_list(policy: &StringPolicy, bin: &str, values: &[&str]) -> Operation {
    let list: Vec<Value> = values.iter().map(|s| Value::from(*s)).collect();
    modify_op(
        STR_OP_CONCAT,
        bin,
        vec![Value::List(list), Value::Int(policy.flags)],
    )
}

/// `append` operation that appends `value` to the end of the bin. This is the
/// single-value form backed by the server's dedicated `APPEND` sub-op; unlike
/// [`concat`], it takes a lone string rather than a list.
pub fn append(policy: &StringPolicy, bin: &str, value: &str) -> Operation {
    modify_op(
        STR_OP_APPEND,
        bin,
        vec![Value::from(value), Value::Int(policy.flags)],
    )
}

/// `prepend` operation that prepends `value` to the start of the bin. Distinct
/// from `insert(0, value)` — this is the server's dedicated `PREPEND` sub-op.
pub fn prepend(policy: &StringPolicy, bin: &str, value: &str) -> Operation {
    modify_op(
        STR_OP_PREPEND,
        bin,
        vec![Value::from(value), Value::Int(policy.flags)],
    )
}

/// `snip` operation that removes codepoints starting at codepoint `start`
/// through the end of the string.
pub fn snip_from(policy: &StringPolicy, bin: &str, start: i64) -> Operation {
    modify_op(
        STR_OP_SNIP,
        bin,
        vec![Value::Int(start), Value::Int(policy.flags)],
    )
}

/// `snip` operation that removes the half-open codepoint range
/// `[start, end)` from the bin.
pub fn snip(policy: &StringPolicy, bin: &str, start: i64, end: i64) -> Operation {
    modify_op(
        STR_OP_SNIP,
        bin,
        vec![Value::Int(start), Value::Int(end), Value::Int(policy.flags)],
    )
}

/// `replace` operation that replaces the first occurrence of `needle` with
/// `replacement`.
pub fn replace(policy: &StringPolicy, bin: &str, needle: &str, replacement: &str) -> Operation {
    modify_op(
        STR_OP_REPLACE,
        bin,
        vec![
            Value::List(vec![Value::from(needle), Value::from(replacement)]),
            Value::Int(policy.flags),
        ],
    )
}

/// `replaceAll` operation that replaces every occurrence of `needle` with
/// `replacement`.
pub fn replace_all(policy: &StringPolicy, bin: &str, needle: &str, replacement: &str) -> Operation {
    modify_op(
        STR_OP_REPLACE_ALL,
        bin,
        vec![
            Value::List(vec![Value::from(needle), Value::from(replacement)]),
            Value::Int(policy.flags),
        ],
    )
}

/// `upper` operation that uppercases the bin in place.
pub fn upper(policy: &StringPolicy, bin: &str) -> Operation {
    modify_op(STR_OP_UPPER, bin, vec![Value::Int(policy.flags)])
}

/// `lower` operation that lowercases the bin in place.
pub fn lower(policy: &StringPolicy, bin: &str) -> Operation {
    modify_op(STR_OP_LOWER, bin, vec![Value::Int(policy.flags)])
}

/// `caseFold` operation that applies a locale-independent case fold
/// (lowercase) to the bin. Useful for normalized comparison keys.
pub fn case_fold(policy: &StringPolicy, bin: &str) -> Operation {
    modify_op(STR_OP_CASE_FOLD, bin, vec![Value::Int(policy.flags)])
}

/// `normalizeNFC` operation that normalizes the bin to Unicode NFC form.
/// Already-normalized strings are unchanged.
pub fn normalize_nfc(policy: &StringPolicy, bin: &str) -> Operation {
    modify_op(STR_OP_NORMALIZE_NFC, bin, vec![Value::Int(policy.flags)])
}

/// `trimStart` operation that removes whitespace from the start of the bin.
pub fn trim_start(policy: &StringPolicy, bin: &str) -> Operation {
    modify_op(STR_OP_TRIM_START, bin, vec![Value::Int(policy.flags)])
}

/// `trimEnd` operation that removes whitespace from the end of the bin.
pub fn trim_end(policy: &StringPolicy, bin: &str) -> Operation {
    modify_op(STR_OP_TRIM_END, bin, vec![Value::Int(policy.flags)])
}

/// `trim` operation that removes whitespace from both ends of the bin.
pub fn trim(policy: &StringPolicy, bin: &str) -> Operation {
    modify_op(STR_OP_TRIM, bin, vec![Value::Int(policy.flags)])
}

/// `padStart` operation that prepends `pad_string` repeatedly until the bin
/// reaches `target_length` codepoints. No-op when the bin is already at or
/// above the target length.
pub fn pad_start(
    policy: &StringPolicy,
    bin: &str,
    target_length: i64,
    pad_string: &str,
) -> Operation {
    modify_op(
        STR_OP_PAD_START,
        bin,
        vec![
            Value::Int(target_length),
            Value::from(pad_string),
            Value::Int(policy.flags),
        ],
    )
}

/// `padEnd` operation that appends `pad_string` repeatedly until the bin
/// reaches `target_length` codepoints. No-op when the bin is already at or
/// above the target length.
pub fn pad_end(
    policy: &StringPolicy,
    bin: &str,
    target_length: i64,
    pad_string: &str,
) -> Operation {
    modify_op(
        STR_OP_PAD_END,
        bin,
        vec![
            Value::Int(target_length),
            Value::from(pad_string),
            Value::Int(policy.flags),
        ],
    )
}

/// `repeat` operation that repeats the bin contents `count` times. `count`
/// must be non-negative.
pub fn repeat(policy: &StringPolicy, bin: &str, count: i64) -> Operation {
    modify_op(
        STR_OP_REPEAT,
        bin,
        vec![Value::Int(count), Value::Int(policy.flags)],
    )
}

/// `regexReplace` operation that replaces the first match of `pattern` with
/// `replacement`. Pass [`StringRegexFlags::GLOBAL`] to replace every match.
/// Flag values may be combined with bitwise OR.
///
/// The server's `regex_replace` op table does not accept policy write flags,
/// so `policy` is accepted only for API symmetry with the other modify ops
/// and is ignored.
pub fn regex_replace(
    _policy: &StringPolicy,
    bin: &str,
    pattern: &str,
    replacement: &str,
    regex_flags: StringRegexFlags,
) -> Operation {
    modify_op(
        STR_OP_REGEX_REPLACE,
        bin,
        vec![
            Value::List(vec![Value::from(pattern), Value::from(replacement)]),
            Value::Int(regex_flags.0),
        ],
    )
}

// -----------------------------------------------------------------
// Type conversion
// -----------------------------------------------------------------

/// `toString` operation that converts an integer, float, string, or blob bin
/// to its string representation. Returns `BIN_TYPE_ERROR` for any other bin
/// type.
///
/// Unlike the other builders in this module, `to_string` does not accept a
/// [`CdtContext`]. The other string operations are sent as `STRING_READ` /
/// `STRING_MODIFY` wire ops carrying a msgpack payload (with the optional CTX
/// envelope). `to_string` is a separate top-level wire op (`TO_STRING`,
/// code 19) that carries no payload at all — the bin is referenced solely by
/// the operation header, and the server-side handler never inspects an op
/// payload.
///
/// To convert a value nested inside a list or map, extract the leaf with
/// [`crate::operations::lists::get_by_index`] or
/// [`crate::operations::maps::get_by_key`] (using the appropriate
/// [`CdtContext`]) and convert it client-side.
pub fn to_string(bin: &str) -> Operation {
    Operation {
        op: OperationType::ToString,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::None,
    }
}
