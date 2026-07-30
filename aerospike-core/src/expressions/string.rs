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

//! String filter expressions. Produce [`Expression`] nodes that read or
//! transform string values inside another expression. Mirrors the operations
//! exposed by [`crate::operations::string`], but composes inside expressions
//! instead of being sent as standalone operate ops.
//!
//! Each builder takes a source [`Expression`] that yields the string to
//! operate on. Common sources:
//! - [`crate::expressions::string_bin`] — read a string bin.
//! - [`crate::expressions::string_val`] — a string literal.
//! - Another `expressions::string` expression — chains read/transform ops.
//!
//! Modify-style expressions (e.g. [`upper`], [`replace`]) return the
//! **modified string value**; they do not mutate the underlying bin.
//!
//! Index orientation is left-to-right with codepoint addressing. Negative
//! indexes count from the end of the string (-1 = last codepoint).
//! Out-of-bounds indexes are clamped; no error is returned.
//!
//! Unlike [`crate::operations::string`], these builders do **not** accept a
//! [`crate::operations::cdt_context::CdtContext`]. To apply a string
//! expression to a value nested inside a list or map, compose with
//! [`crate::expressions::lists::get_by_index`] or
//! [`crate::expressions::maps::get_by_key`] to extract the leaf first.
//!
//! Requires Aerospike Server version 8.1.3 or later.

use crate::expressions::{ExpOp, ExpType, Expression, ExpressionArgument, MODIFY};
use crate::operations::string::{StringPolicy, StringRegexFlags};
use crate::Value;

// CALL module ids on the server.
const MODULE: i64 = 3; // CALL_STRING

// Read sub-op ids.
const STRLEN: i64 = 0;
const SUBSTR: i64 = 1;
const CHAR_AT: i64 = 2;
const FIND: i64 = 3;
const CONTAINS: i64 = 4;
const STARTS_WITH: i64 = 5;
const ENDS_WITH: i64 = 6;
const TO_INTEGER: i64 = 7;
const TO_DOUBLE: i64 = 8;
const BYTE_LENGTH: i64 = 9;
const IS_NUMERIC: i64 = 10;
const IS_UPPER: i64 = 11;
const IS_LOWER: i64 = 12;
const TO_BLOB: i64 = 13;
const SPLIT: i64 = 14;
const B64_DECODE: i64 = 15;
const REGEX_COMPARE: i64 = 16;

// Modify sub-op ids.
const INSERT: i64 = 50;
const OVERWRITE: i64 = 51;
const CONCAT: i64 = 52;
const SNIP: i64 = 53;
const REPLACE: i64 = 54;
const REPLACE_ALL: i64 = 55;
const UPPER: i64 = 56;
const LOWER: i64 = 57;
const CASE_FOLD: i64 = 58;
const NORMALIZE_NFC: i64 = 59;
const TRIM_START: i64 = 60;
const TRIM_END: i64 = 61;
const TRIM: i64 = 62;
const PAD_START: i64 = 63;
const PAD_END: i64 = 64;
const REPEAT: i64 = 65;
const REGEX_REPLACE: i64 = 66;
const APPEND: i64 = 67;
const PREPEND: i64 = 68;

use crate::operations::string::StringNumericType;

// -----------------------------------------------------------------
// Read expressions
// -----------------------------------------------------------------

/// Expression that returns the number of Unicode codepoints in `src` as an
/// `i64`. For UTF-8 byte length, use [`byte_length`].
pub fn strlen(src: Expression) -> Expression {
    add_read(src, ExpType::INT, vec![sub(STRLEN)])
}

/// Expression that returns the substring of `src` from codepoint `start` to
/// the end. Negative `start` counts from the end of the string.
pub fn substr(start: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::STRING,
        vec![sub(SUBSTR), ExpressionArgument::FilterExpression(start)],
    )
}

/// Expression that returns the substring of `src` in the half-open codepoint
/// range `[start, end)` — `start` inclusive, `end` exclusive. Negative
/// indexes count from the end. If, after negative-index normalization,
/// `start >= end`, the result is the empty string.
pub fn substr_range(start: Expression, end: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::STRING,
        vec![
            sub(SUBSTR),
            ExpressionArgument::FilterExpression(start),
            ExpressionArgument::FilterExpression(end),
        ],
    )
}

/// Expression that returns the codepoint at `index` of `src` as a
/// one-codepoint string. Negative indexes count from the end.
pub fn char_at(index: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::STRING,
        vec![sub(CHAR_AT), ExpressionArgument::FilterExpression(index)],
    )
}

/// Expression that returns the codepoint index of the first occurrence of
/// `needle` in `src`, or -1 if not found.
pub fn find(needle: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::INT,
        vec![sub(FIND), ExpressionArgument::FilterExpression(needle)],
    )
}

/// Expression that returns the codepoint index of the `occurrence`-th match
/// of `needle` (1 = first, -1 = last), or -1 if not found.
pub fn find_nth(needle: Expression, occurrence: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::INT,
        vec![
            sub(FIND),
            ExpressionArgument::FilterExpression(needle),
            ExpressionArgument::FilterExpression(occurrence),
        ],
    )
}

/// Expression that tests whether `src` contains `needle` as a substring.
pub fn contains(needle: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::BOOL,
        vec![sub(CONTAINS), ExpressionArgument::FilterExpression(needle)],
    )
}

/// Expression that tests whether `src` begins with `prefix`.
pub fn starts_with(prefix: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::BOOL,
        vec![
            sub(STARTS_WITH),
            ExpressionArgument::FilterExpression(prefix),
        ],
    )
}

/// Expression that tests whether `src` ends with `suffix`.
pub fn ends_with(suffix: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::BOOL,
        vec![sub(ENDS_WITH), ExpressionArgument::FilterExpression(suffix)],
    )
}

/// Expression that parses `src` as an `i64`. Returns an error at evaluation
/// time if the source cannot be parsed as an integer.
pub fn to_integer(src: Expression) -> Expression {
    add_read(src, ExpType::INT, vec![sub(TO_INTEGER)])
}

/// Expression that parses `src` as a 64-bit float. Returns an error at
/// evaluation time if the source cannot be parsed as a double.
pub fn to_double(src: Expression) -> Expression {
    add_read(src, ExpType::FLOAT, vec![sub(TO_DOUBLE)])
}

/// Expression that returns the UTF-8 byte length of `src` as an `i64`.
/// Differs from [`strlen`] for non-ASCII content.
pub fn byte_length(src: Expression) -> Expression {
    add_read(src, ExpType::INT, vec![sub(BYTE_LENGTH)])
}

/// Expression that tests whether `src` contains a valid integer or float.
pub fn is_numeric(src: Expression) -> Expression {
    add_read(src, ExpType::BOOL, vec![sub(IS_NUMERIC)])
}

/// Expression that tests whether `src` parses as a number of the requested
/// [`StringNumericType`].
pub fn is_numeric_typed(numeric_type: StringNumericType, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::BOOL,
        vec![
            sub(IS_NUMERIC),
            ExpressionArgument::Value(Value::Int(numeric_type as i64)),
        ],
    )
}

/// Expression that tests whether every cased codepoint in `src` is uppercase.
pub fn is_upper(src: Expression) -> Expression {
    add_read(src, ExpType::BOOL, vec![sub(IS_UPPER)])
}

/// Expression that tests whether every cased codepoint in `src` is lowercase.
pub fn is_lower(src: Expression) -> Expression {
    add_read(src, ExpType::BOOL, vec![sub(IS_LOWER)])
}

/// Expression that returns the UTF-8 bytes of `src` as a blob.
pub fn to_blob(src: Expression) -> Expression {
    add_read(src, ExpType::BLOB, vec![sub(TO_BLOB)])
}

/// Expression that splits `src` by Unicode codepoint — each codepoint becomes
/// its own list element.
pub fn split(src: Expression) -> Expression {
    add_read(src, ExpType::LIST, vec![sub(SPLIT)])
}

/// Expression that splits `src` by the `separator` substring. If the
/// separator is absent, the result is a singleton list containing the whole
/// source.
pub fn split_by_separator(separator: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::LIST,
        vec![sub(SPLIT), ExpressionArgument::FilterExpression(separator)],
    )
}

/// Expression that base64-decodes `src` and returns the decoded bytes as a
/// blob.
pub fn b64_decode(src: Expression) -> Expression {
    add_read(src, ExpType::BLOB, vec![sub(B64_DECODE)])
}

/// Expression that tests whether `pattern` (ICU regex syntax) matches `src`.
pub fn regex_compare(pattern: Expression, src: Expression) -> Expression {
    add_read(
        src,
        ExpType::BOOL,
        vec![
            sub(REGEX_COMPARE),
            ExpressionArgument::FilterExpression(pattern),
        ],
    )
}

/// Expression that tests whether `pattern` matches `src` under the supplied
/// [`StringRegexFlags`]. Flags can be combined with bitwise OR.
pub fn regex_compare_with_flags(
    pattern: Expression,
    regex_flags: StringRegexFlags,
    src: Expression,
) -> Expression {
    add_read(
        src,
        ExpType::BOOL,
        vec![
            sub(REGEX_COMPARE),
            ExpressionArgument::FilterExpression(pattern),
            ExpressionArgument::Value(Value::Int(regex_flags.0)),
        ],
    )
}

// -----------------------------------------------------------------
// Modify expressions
// -----------------------------------------------------------------

/// Expression that splices `value` into `src` at codepoint `index` and
/// returns the resulting string. Does not modify the underlying bin.
pub fn insert(
    policy: &StringPolicy,
    index: Expression,
    value: Expression,
    src: Expression,
) -> Expression {
    add_modify(
        src,
        vec![
            sub(INSERT),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that overwrites codepoints in `src` starting at codepoint
/// `index` with `value`, returning the resulting string.
pub fn overwrite(
    policy: &StringPolicy,
    index: Expression,
    value: Expression,
    src: Expression,
) -> Expression {
    add_modify(
        src,
        vec![
            sub(OVERWRITE),
            ExpressionArgument::FilterExpression(index),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that concatenates `values` (a list of strings) onto `src` in
/// order, returning the resulting string.
pub fn concat(policy: &StringPolicy, values: Expression, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(CONCAT),
            ExpressionArgument::FilterExpression(values),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that appends `value` to the end of `src` and returns the
/// resulting string. Unicode/DBCS-aware counterpart to the legacy byte-level
/// append; does not modify the underlying bin.
pub fn append(policy: &StringPolicy, value: Expression, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(APPEND),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that prepends `value` to the start of `src` and returns the
/// resulting string. Unicode/DBCS-aware counterpart to the legacy byte-level
/// prepend; does not modify the underlying bin.
pub fn prepend(policy: &StringPolicy, value: Expression, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(PREPEND),
            ExpressionArgument::FilterExpression(value),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that removes the half-open codepoint range `[start, end)` from
/// `src` and returns the resulting string.
pub fn snip(
    policy: &StringPolicy,
    start: Expression,
    end: Expression,
    src: Expression,
) -> Expression {
    add_modify(
        src,
        vec![
            sub(SNIP),
            ExpressionArgument::FilterExpression(start),
            ExpressionArgument::FilterExpression(end),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that replaces the first occurrence of `needle` in `src` with
/// `replacement` and returns the resulting string.
pub fn replace(
    policy: &StringPolicy,
    needle: Expression,
    replacement: Expression,
    src: Expression,
) -> Expression {
    add_modify(
        src,
        vec![
            sub(REPLACE),
            ExpressionArgument::QuotedExpressions(vec![needle, replacement]),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that replaces every occurrence of `needle` in `src` with
/// `replacement` and returns the resulting string.
pub fn replace_all(
    policy: &StringPolicy,
    needle: Expression,
    replacement: Expression,
    src: Expression,
) -> Expression {
    add_modify(
        src,
        vec![
            sub(REPLACE_ALL),
            ExpressionArgument::QuotedExpressions(vec![needle, replacement]),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that returns `src` uppercased.
pub fn upper(policy: &StringPolicy, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(UPPER),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that returns `src` lowercased.
pub fn lower(policy: &StringPolicy, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(LOWER),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that returns `src` case-folded (locale-independent lowercase).
pub fn case_fold(policy: &StringPolicy, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(CASE_FOLD),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that returns `src` normalized to Unicode NFC form.
pub fn normalize_nfc(policy: &StringPolicy, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(NORMALIZE_NFC),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that returns `src` with whitespace removed from the start.
pub fn trim_start(policy: &StringPolicy, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(TRIM_START),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that returns `src` with whitespace removed from the end.
pub fn trim_end(policy: &StringPolicy, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(TRIM_END),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that returns `src` with whitespace removed from both ends.
pub fn trim(policy: &StringPolicy, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(TRIM),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that prepends `pad_string` to `src` repeatedly until the result
/// reaches `target_length` codepoints.
pub fn pad_start(
    policy: &StringPolicy,
    target_length: Expression,
    pad_string: Expression,
    src: Expression,
) -> Expression {
    add_modify(
        src,
        vec![
            sub(PAD_START),
            ExpressionArgument::FilterExpression(target_length),
            ExpressionArgument::FilterExpression(pad_string),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that appends `pad_string` to `src` repeatedly until the result
/// reaches `target_length` codepoints.
pub fn pad_end(
    policy: &StringPolicy,
    target_length: Expression,
    pad_string: Expression,
    src: Expression,
) -> Expression {
    add_modify(
        src,
        vec![
            sub(PAD_END),
            ExpressionArgument::FilterExpression(target_length),
            ExpressionArgument::FilterExpression(pad_string),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that returns `src` repeated `count` times.
pub fn repeat(policy: &StringPolicy, count: Expression, src: Expression) -> Expression {
    add_modify(
        src,
        vec![
            sub(REPEAT),
            ExpressionArgument::FilterExpression(count),
            ExpressionArgument::Value(Value::Int(policy_flags(policy))),
        ],
    )
}

/// Expression that replaces matches of `pattern` (ICU regex syntax) in `src`
/// with `replacement` and returns the resulting string. Pass
/// [`StringRegexFlags::GLOBAL`] to replace every match.
///
/// The `policy` parameter is kept for API symmetry with the other modify
/// builders and is ignored — the server's `regex_replace` op table does not
/// accept policy write flags.
pub fn regex_replace(
    _policy: &StringPolicy,
    pattern: Expression,
    replacement: Expression,
    regex_flags: StringRegexFlags,
    src: Expression,
) -> Expression {
    add_modify(
        src,
        vec![
            sub(REGEX_REPLACE),
            ExpressionArgument::QuotedExpressions(vec![pattern, replacement]),
            ExpressionArgument::Value(Value::Int(regex_flags.0)),
        ],
    )
}

// -----------------------------------------------------------------
// Type conversion expression
// -----------------------------------------------------------------

/// Expression that returns the string representation of `src`, where `src`
/// may be any expression yielding an integer, float, string, or blob value.
/// Returns an error for any other source type.
///
/// Unlike the other builders in this module, `to_string` is not a CALL
/// sub-op — it is the dedicated unary `TO_STRING` expression (opcode 99),
/// packed as `[99, src]` like the `to_int`/`to_float` conversions.
pub fn to_string(src: Expression) -> Expression {
    Expression {
        cmd: Some(ExpOp::ToString),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![src]),
        arguments: None,
        bytes: None,
    }
}

// -----------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------

fn sub(sub_op: i64) -> ExpressionArgument {
    ExpressionArgument::Value(Value::Int(sub_op))
}

const fn policy_flags(policy: &StringPolicy) -> i64 {
    policy.flags()
}

fn add_read(
    src: Expression,
    return_type: ExpType,
    arguments: Vec<ExpressionArgument>,
) -> Expression {
    Expression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(src)),
        flags: Some(MODULE),
        module: Some(return_type),
        exps: None,
        arguments: Some(arguments),
        bytes: None,
    }
}

fn add_modify(src: Expression, arguments: Vec<ExpressionArgument>) -> Expression {
    Expression {
        cmd: Some(ExpOp::Call),
        val: None,
        bin: Some(Box::new(src)),
        flags: Some(MODULE | MODIFY),
        module: Some(ExpType::STRING),
        exps: None,
        arguments: Some(arguments),
        bytes: None,
    }
}
