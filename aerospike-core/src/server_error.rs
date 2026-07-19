// Copyright 2015-2026 Aerospike, Inc.
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

//! Extended server-supplied error detail.
//!
//! When extended error detail is requested via
//! [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity),
//! the server may attach a numeric subcode, a human-readable message, and (at
//! verbosity 3, on expression build failures) a structured expression trace to a
//! failure response. The detail is surfaced on
//! [`ErrorKind::Server`](crate::ErrorKind::Server) as a
//! [`ServerErrorDetail`] and via the [`Error::server_error_detail`],
//! [`Error::sub_code`] and [`Error::server_message`] accessors.
//!
//! Requires Aerospike server version 8.1.3 or later; older servers ignore the
//! request flags.
//!
//! [`Error::server_error_detail`]: crate::errors::Error::server_error_detail
//! [`Error::sub_code`]: crate::errors::Error::sub_code
//! [`Error::server_message`]: crate::errors::Error::server_message

use std::fmt;

/// Server error detail subcodes.
///
/// When extended error detail is requested (see
/// [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity)),
/// the server may attach a numeric subcode to a failure response. The subcode is
/// surfaced on [`ServerErrorDetail::sub_code`].
///
/// Match on the ([`ResultCode`](crate::ResultCode), subcode) pair. Subcode
/// integer values are scoped to their parent result code and are NOT globally
/// unique — the value 1, for example, recurs under every parent status. A
/// subcode is only meaningful when interpreted together with the result code.
/// Always check the result code first.
///
/// [`NONE`](sub_code::NONE) (0) means "no subcode" — it is reserved universally
/// and is the value returned when the server did not send a subcode (verbosity
/// disabled, or the failing branch had no dispatchable subcode).
///
/// This catalogue mirrors the server's per-status enums and is
/// server-version-specific. It is append-only: published values are immutable
/// and are never renumbered or reused. New failure modes get new values appended
/// to their group. Treat any subcode value not declared here as an opaque
/// integer rather than assuming it is absent.
pub mod sub_code {
    /// Returned when the server did not supply a subcode.
    pub const NONE: u32 = 0;

    // -------------------------------------------------------
    // Pairs with ResultCode::ParameterError (4)
    // -------------------------------------------------------

    /// Per-record TTL exceeds the namespace's max-ttl.
    pub const PARAM_TTL_INVALID: u32 = 1;
    /// Bit op offset lands past the blob (or above the proto cap).
    pub const PARAM_BITS_OFFSET_OUT_OF_RANGE: u32 = 2;
    /// Bit op size is out of range (e.g. zero, or too large).
    pub const PARAM_BITS_SIZE_OUT_OF_RANGE: u32 = 3;
    /// Blob resize would exceed the maximum blob size.
    pub const PARAM_BITS_RESIZE_EXCEEDED: u32 = 4;
    /// Write would exceed the per-record bin-count limit (write path).
    pub const PARAM_BIN_COUNT_TOO_LARGE: u32 = 5;
    /// String op wire/expression args malformed or out of range.
    pub const PARAM_STRING_OP_PARAMS_INVALID: u32 = 6;
    /// String op code or modifier/read class mismatch on the wire path.
    pub const PARAM_STRING_OP_INVALID: u32 = 7;
    /// String context-eval path malformed.
    pub const PARAM_STRING_CTX_NOT_APPLICABLE: u32 = 8;
    /// String modify/read index or code-point range out of bounds.
    pub const PARAM_STRING_INDEX_OUT_OF_BOUNDS: u32 = 9;
    /// String regex pattern invalid (compile / ICU failure).
    pub const PARAM_STRING_REGEX_INVALID: u32 = 10;
    /// String or string op argument is not valid UTF-8.
    pub const PARAM_STRING_UTF8_INVALID: u32 = 11;

    // -------------------------------------------------------
    // Pairs with ResultCode::PartitionUnavailable (11)
    // -------------------------------------------------------

    /// Cluster is still resolving initial partition balance at startup.
    pub const UNAVAIL_INITIAL_BALANCE_UNRESOLVED: u32 = 1;
    /// A needed replica is unavailable (likely a partition split).
    pub const UNAVAIL_REPLICA_UNAVAILABLE: u32 = 2;

    // -------------------------------------------------------
    // Pairs with ResultCode::UnsupportedFeature (16)
    // -------------------------------------------------------

    /// MRT attempted against a non-SC (AP) namespace.
    pub const UNSUPP_FEAT_MRT_REQUIRES_STRONG_CONSISTENCY: u32 = 1;
    /// Requested feature is unsupported in this context (generic).
    pub const UNSUPP_FEAT_GENERIC: u32 = 2;

    // -------------------------------------------------------
    // Pairs with ResultCode::BinNotFound (17)
    // -------------------------------------------------------

    /// HLL op needs an existing bin and can't auto-create one.
    pub const BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP: u32 = 1;
    /// String modify on a missing bin (non-NO_FAIL path).
    pub const BIN_NOT_FOUND_STRING_VALUE_NOT_FOUND: u32 = 2;

    // -------------------------------------------------------
    // Pairs with ResultCode::BinNameTooLong (21)
    // -------------------------------------------------------

    /// Write would exceed the per-record bin-count limit (UDF path).
    pub const BIN_NAME_COUNT_TOO_LARGE: u32 = 1;

    // -------------------------------------------------------
    // Pairs with ResultCode::FailForbidden (22)
    // -------------------------------------------------------

    /// Write bounced by an XDR ship filter at the destination.
    pub const FORBID_XDR_FILTER_BLOCKED: u32 = 1;
    /// Set-level record-count stop-writes limit reached.
    pub const FORBID_SET_COUNT_STOP_WRITES: u32 = 2;
    /// Set-level size stop-writes limit reached.
    pub const FORBID_SET_SIZE_STOP_WRITES: u32 = 3;
    /// Writes stopped due to cluster clock skew.
    pub const FORBID_CLOCK_SKEW_STOP_WRITES: u32 = 4;
    /// `REPLACE` / `CREATE_OR_REPLACE` forbidden while resolving conflicts.
    pub const FORBID_REPLACE_CONFLICT_RESOLVING: u32 = 5;
    /// Write forbidden because the set/namespace is mid-truncate.
    pub const FORBID_TRUNCATED: u32 = 6;
    // Server subcodes 7 and 9 in this family are retired (masking violations
    // return ROLE_VIOLATION, not FORBIDDEN) and are intentionally not declared.
    /// Non-durable delete forbidden (would violate durability).
    pub const FORBID_DURABILITY_VIOLATION: u32 = 8;

    // -------------------------------------------------------
    // Pairs with ResultCode::OpNotApplicable (26)
    // -------------------------------------------------------

    /// List index is outside the current element range.
    pub const OPNOT_CDT_INDEX_OUT_OF_BOUNDS: u32 = 1;
    /// Requested rank is past the current population.
    pub const OPNOT_CDT_RANK_OUT_OF_BOUNDS: u32 = 2;
    /// Insert would exceed an ordered+bounded list's cap.
    pub const OPNOT_CDT_BOUNDED_LIST_OVERFLOW: u32 = 3;
    /// HLL op needs `index_bits` but the sketch has none set.
    pub const OPNOT_HLL_INDEX_BITS_UNSET: u32 = 4;
    /// Union needs to reduce `index_bits` but folding isn't allowed.
    pub const OPNOT_HLL_CANNOT_REDUCE_INDEX_BITS: u32 = 5;
    /// As above, for the minhash dimension.
    pub const OPNOT_HLL_CANNOT_REDUCE_MINHASH_BITS: u32 = 6;
    /// Fold blocked because the sketch carries minhash bits.
    pub const OPNOT_HLL_CANNOT_FOLD_MINHASH: u32 = 7;
    /// Fold target `index_bits` >= current (fold can only reduce).
    pub const OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE: u32 = 8;
    /// Intersect inputs have mismatched minhash parameters.
    pub const OPNOT_HLL_INTERSECT_MINHASH_MISMATCH: u32 = 9;
    /// String to numeric conversion failed.
    pub const OPNOT_STRING_CONVERSION_FAILED: u32 = 10;
    /// Source blob/string is not valid UTF-8 for an `OPNOT_APPLICABLE` path.
    pub const OPNOT_STRING_UTF8_INVALID: u32 = 11;

    // -------------------------------------------------------
    // Pairs with ResultCode::FilteredOut (27)
    // -------------------------------------------------------

    /// Record filtered out by a metadata-only filter expression.
    pub const FILTERED_META: u32 = 1;
    /// Record filtered out by a bin-reading filter expression.
    pub const FILTERED_BINS: u32 = 2;

    // -------------------------------------------------------
    // Pairs with ResultCode::MrtBlocked (120)
    // -------------------------------------------------------

    /// Record is provisionally locked by another MRT.
    pub const MRT_BLOCKED_RECORD_LOCKED: u32 = 1;
    /// Op belongs to a different MRT than the one holding the lock.
    pub const MRT_BLOCKED_ID_MISMATCH: u32 = 2;
}

/// The expression build failed.
pub const EXP_TRACE_PHASE_BUILD: u32 = 1;
/// Expression evaluation failed (reserved for a future server branch).
pub const EXP_TRACE_PHASE_EVAL: u32 = 2;

/// The msgpack source language (the implied default when the lang key is absent).
pub const EXP_TRACE_LANG_MSGPACK: u32 = 1;
/// The AEL DSL source language (reserved for a future server branch).
pub const EXP_TRACE_LANG_AEL: u32 = 2;

/// The `"..."` sentinel the server splices into [`ExpressionTrace::path`] when
/// the true nesting depth exceeds the path-frame cap. [`ExpressionTrace::depth`]
/// still reports the true count.
pub const EXP_TRACE_PATH_TRUNCATION_SENTINEL: &str = "...";

// Wire keys of the top-level error-detail map (field 45). Append-only.
const DETAIL_KEY_SUBCODE: u32 = 1;
const DETAIL_KEY_MESSAGE: u32 = 2;
const DETAIL_KEY_EXP_TRACE: u32 = 3;

// Wire keys of the nested expression-trace map. Append-only.
const TRACE_KEY_PHASE: u32 = 1;
const TRACE_KEY_BYTE_OFFSET: u32 = 2;
const TRACE_KEY_OP: u32 = 3;
const TRACE_KEY_DEPTH: u32 = 4;
const TRACE_KEY_PATH: u32 = 5;
const TRACE_KEY_SNIPPET: u32 = 6;
const TRACE_KEY_LANG: u32 = 8;
const TRACE_KEY_AEL_OFFSET: u32 = 9;
const TRACE_KEY_AEL_SPAN: u32 = 10;

/// A structured expression build/eval trace surfaced at error-detail verbosity 3.
///
/// When extended error detail is requested at verbosity 3 (see
/// [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity))
/// and the server fails to build an expression — a metadata/predicate filter
/// (`filter_expression`) or an `exp_read`/`exp_write` operation — it attaches
/// this trace to the error detail. It is surfaced on
/// [`ServerErrorDetail::exp_trace`].
///
/// Expression build failures carry
/// [`ResultCode::ParameterError`](crate::ResultCode::ParameterError) and no
/// subcode; the contextual message is on the error. The trace is purely
/// additive diagnostic detail — it never changes the result code, subcode, or
/// message-string format.
///
/// Every field is optional. The server caps the whole error-detail payload and
/// drops `snippet` first, then `path`, when the budget is tight, so those may
/// be absent even within a present trace. Never require any field.
///
/// Two coordinate spaces — do not conflate them. `byte_offset` is a byte offset
/// into the msgpack expression payload the client sent. The `ael_offset` /
/// `ael_span` pair are offsets into AEL source text — a different coordinate
/// space, reserved for a future server branch and absent on today's msgpack
/// build traces.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExpressionTrace {
    /// Phase that failed: [`EXP_TRACE_PHASE_BUILD`] or [`EXP_TRACE_PHASE_EVAL`];
    /// `None` when absent. Today the server emits build traces only.
    pub phase: Option<u32>,

    /// Byte offset into the msgpack expression payload of the failing element,
    /// or `None` when absent. This is a coordinate into the wire payload the
    /// client sent — not into AEL source text (see `ael_offset`).
    pub byte_offset: Option<u32>,

    /// The failing op name (pre-rendered server-side), or `None` when absent.
    pub op: Option<String>,

    /// True nesting depth of the fault, or `None` when absent. Reports the
    /// true count even when `path` was truncated to the frame cap.
    pub depth: Option<u32>,

    /// Op-name chain from root to fault, or `None` when absent. May contain an
    /// [`EXP_TRACE_PATH_TRUNCATION_SENTINEL`] (`"..."`) element mid-list when
    /// the true nesting exceeded the server's path-frame cap; `depth` still
    /// reports the true count.
    pub path: Option<Vec<String>>,

    /// Human-only rendered snippet of the failing element, or `None` when
    /// absent (it is the first field the server drops under a tight byte
    /// budget).
    pub snippet: Option<String>,

    /// Source language: [`EXP_TRACE_LANG_MSGPACK`] or [`EXP_TRACE_LANG_AEL`].
    /// An absent lang key means msgpack (the default), so this reads as
    /// `None` only before parsing; the parser fills [`EXP_TRACE_LANG_MSGPACK`]
    /// when the server omitted it.
    pub lang: Option<u32>,

    /// Char offset into the AEL source text, or `None` when absent. Reserved
    /// for the AEL DSL branch; absent on today's msgpack build traces. A
    /// different coordinate space from `byte_offset`.
    pub ael_offset: Option<u32>,

    /// Byte width of the offending AEL source region, or `None` when absent.
    /// Reserved for the AEL DSL branch.
    pub ael_span: Option<u32>,
}

impl fmt::Display for ExpressionTrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExpressionTrace[phase={}", self.phase.unwrap_or(0))?;
        if let Some(off) = self.byte_offset {
            write!(f, ", byte_offset={off}")?;
        }
        if let Some(op) = &self.op {
            write!(f, ", op={op}")?;
        }
        if let Some(depth) = self.depth {
            write!(f, ", depth={depth}")?;
        }
        if let Some(path) = &self.path {
            write!(f, ", path=[{}]", path.join(" "))?;
        }
        if let Some(snippet) = &self.snippet {
            write!(f, ", snippet={snippet}")?;
        }
        if let Some(lang) = self.lang {
            if lang != EXP_TRACE_LANG_MSGPACK {
                write!(f, ", lang={lang}")?;
            }
        }
        if let Some(off) = self.ael_offset {
            write!(f, ", ael_offset={off}")?;
        }
        if let Some(span) = self.ael_span {
            write!(f, ", ael_span={span}")?;
        }
        write!(f, "]")
    }
}

/// Extended error detail the server attached to a failure response.
///
/// Populated only when
/// [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity)
/// is greater than zero and the failing branch dispatched a detail. Requires
/// server version 8.1.3+.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServerErrorDetail {
    /// The server-supplied error subcode (see the [`sub_code`] constants).
    /// [`sub_code::NONE`] (0) when the server sent a message without a subcode.
    ///
    /// A subcode is only meaningful when interpreted together with the result
    /// code: subcode integer values are scoped to their parent result code and
    /// are NOT globally unique. Dispatch on the (result code, subcode) pair.
    pub sub_code: u32,

    /// The formatted server-supplied error detail (human-readable message
    /// and/or subcode tag). Empty when the server sent a subcode without a
    /// message at verbosity 1.
    pub message: String,

    /// The server-supplied expression build trace, or `None` when absent.
    ///
    /// Populated only at error-detail verbosity 3 on an expression build
    /// failure. `None` on every other failure (including non-expression
    /// failures at verbosity 3).
    pub exp_trace: Option<ExpressionTrace>,
}

impl fmt::Display for ServerErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        if let Some(trace) = &self.exp_trace {
            write!(f, ", {trace}")?;
        }
        Ok(())
    }
}

/// Best-effort bounded cursor over the raw msgpack error-detail bytes.
///
/// The detail payload is diagnostic only, so parsing must never fail a
/// command: any malformed or truncated element terminates the walk and
/// whatever was decoded so far is kept.
#[derive(Clone, Copy)]
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }

    fn be_uint(&mut self, n: usize) -> Option<u64> {
        let bytes = self.take(n)?;
        Some(bytes.iter().fold(0u64, |acc, &b| (acc << 8) | u64::from(b)))
    }

    /// Msgpack map header (fixmap / map16 / map32): number of entries.
    fn map_header(&mut self) -> Option<usize> {
        match self.byte()? {
            b if b & 0xF0 == 0x80 => Some((b & 0x0F) as usize),
            0xDE => self.be_uint(2).map(|n| n as usize),
            0xDF => self.be_uint(4).map(|n| n as usize),
            _ => None,
        }
    }

    /// Msgpack map key: positive fixint or uint8. Anything else is `None`.
    fn key(&mut self) -> Option<u32> {
        match self.byte()? {
            b if b <= 0x7F => Some(u32::from(b)),
            0xCC => self.byte().map(u32::from),
            _ => None,
        }
    }

    /// Decode an unsigned msgpack integer value, consuming it on success only.
    fn uint(&mut self) -> Option<u64> {
        let mut probe = *self;
        let v = match probe.byte()? {
            b if b <= 0x7F => u64::from(b),
            0xCC => probe.be_uint(1)?,
            0xCD => probe.be_uint(2)?,
            0xCE => probe.be_uint(4)?,
            0xCF => probe.be_uint(8)?,
            _ => return None,
        };
        *self = probe;
        Some(v)
    }

    /// Decode a msgpack string value, consuming it on success only.
    fn str(&mut self) -> Option<String> {
        let mut probe = *self;
        let len = match probe.byte()? {
            b if b & 0xE0 == 0xA0 => usize::from(b & 0x1F),
            0xD9 => probe.be_uint(1)? as usize,
            0xDA => probe.be_uint(2)? as usize,
            0xDB => probe.be_uint(4)? as usize,
            _ => return None,
        };
        let s = String::from_utf8_lossy(probe.take(len)?).into_owned();
        *self = probe;
        Some(s)
    }

    /// Decode a msgpack array of strings, consuming it on success only.
    /// Non-string elements are skipped.
    fn str_array(&mut self) -> Option<Vec<String>> {
        let mut probe = *self;
        let count = match probe.byte()? {
            b if b & 0xF0 == 0x90 => usize::from(b & 0x0F),
            0xDC => probe.be_uint(2)? as usize,
            0xDD => probe.be_uint(4)? as usize,
            _ => return None,
        };
        let mut items = Vec::new();
        for _ in 0..count {
            if let Some(s) = probe.str() {
                items.push(s);
            } else if !probe.skip_value() {
                break;
            }
        }
        *self = probe;
        Some(items)
    }

    /// Advance past a single msgpack value. Returns `false` when the value is
    /// truncated or of an unhandled type, in which case the cursor is
    /// exhausted so the caller's walk terminates.
    fn skip_value(&mut self) -> bool {
        let Some(b) = self.byte() else {
            return false;
        };

        // positive / negative fixint
        if b <= 0x7F || b >= 0xE0 {
            return true;
        }
        // fixstr
        if b & 0xE0 == 0xA0 {
            return self.take(usize::from(b & 0x1F)).is_some();
        }
        // fixmap / fixarray
        if b & 0xF0 == 0x80 || b & 0xF0 == 0x90 {
            let mult = if b & 0xF0 == 0x80 { 2 } else { 1 };
            let count = usize::from(b & 0x0F) * mult;
            return (0..count).all(|_| self.skip_value());
        }

        let skipped = match b {
            0xC0 | 0xC2 | 0xC3 => Some(0),                      // nil, false, true
            0xCC | 0xD0 => Some(1),                             // uint8, int8
            0xCD | 0xD1 => Some(2),                             // uint16, int16
            0xCE | 0xD2 | 0xCA => Some(4),                      // uint32, int32, float32
            0xCF | 0xD3 | 0xCB => Some(8),                      // uint64, int64, float64
            0xD9 | 0xC4 => self.be_uint(1).map(|n| n as usize), // str8, bin8
            0xDA | 0xC5 => self.be_uint(2).map(|n| n as usize), // str16, bin16
            0xDB | 0xC6 => self.be_uint(4).map(|n| n as usize), // str32, bin32
            0xDC..=0xDF => {
                // array16, map16, array32, map32
                let mult = if b == 0xDE || b == 0xDF { 2 } else { 1 };
                let width = if b == 0xDC || b == 0xDE { 2 } else { 4 };
                let Some(count) = self.be_uint(width) else {
                    self.pos = self.buf.len();
                    return false;
                };
                return (0..count as usize * mult).all(|_| self.skip_value());
            }
            _ => None, // ext / unknown: cannot size it
        };

        if let Some(n) = skipped {
            self.take(n).is_some()
        } else {
            self.pos = self.buf.len();
            false
        }
    }
}

/// Decode the msgpack error-detail map the server attaches (wire field 45)
/// when error-detail verbosity > 0. Map keys: 1 = subcode (uint), 2 = message
/// (string), 3 = nested expression-trace map (verbosity 3).
///
/// Returns `None` when the payload is empty, malformed, or carries none of the
/// known keys. Best-effort: a truncated or unknown element terminates the walk
/// and whatever was decoded before it is kept.
pub(crate) fn parse_error_detail(buf: &[u8]) -> Option<ServerErrorDetail> {
    let mut cur = Cursor { buf, pos: 0 };
    let count = cur.map_header()?;

    let mut sub_code: Option<u64> = None;
    let mut message: Option<String> = None;
    let mut exp_trace: Option<ExpressionTrace> = None;

    for _ in 0..count {
        let Some(key) = cur.key() else {
            break;
        };
        match key {
            DETAIL_KEY_SUBCODE => {
                if let Some(v) = cur.uint() {
                    sub_code = Some(v);
                } else if !cur.skip_value() {
                    break;
                }
            }
            DETAIL_KEY_MESSAGE => {
                if let Some(s) = cur.str() {
                    message = Some(s);
                } else if !cur.skip_value() {
                    break;
                }
            }
            DETAIL_KEY_EXP_TRACE => {
                // Parse from a probe; always advance past the nested value.
                exp_trace = parse_exp_trace(cur);
                if !cur.skip_value() {
                    break;
                }
            }
            _ => {
                if !cur.skip_value() {
                    break;
                }
            }
        }
    }

    // The server only serializes subcodes >= 1 (NONE = 0 is never sent), and
    // pre-formats the display message the same way the Go client does.
    let formatted = match (&message, sub_code) {
        (Some(m), Some(sc)) => format!("{m} (subcode={sc})"),
        (None, Some(sc)) => format!("error subcode={sc}"),
        (Some(m), None) => m.clone(),
        (None, None) => {
            exp_trace.as_ref()?;
            String::new()
        }
    };

    Some(ServerErrorDetail {
        sub_code: sub_code.map_or(sub_code::NONE, |v| v as u32),
        message: formatted,
        exp_trace,
    })
}

/// Decode the nested expression-trace map (detail key 3, verbosity 3).
/// Operates on a copy of the cursor; the caller advances past the value.
fn parse_exp_trace(mut cur: Cursor) -> Option<ExpressionTrace> {
    let count = cur.map_header()?;
    if count == 0 {
        return None;
    }

    let mut t = ExpressionTrace {
        // An absent lang key means msgpack (the implied default).
        lang: Some(EXP_TRACE_LANG_MSGPACK),
        ..ExpressionTrace::default()
    };

    // The typed decoders consume the value on success only, so each arm
    // reports whether it consumed; unconsumed values (type mismatch, unknown
    // or reserved keys such as outcome / the AEL fields) are skipped whole.
    for _ in 0..count {
        let Some(key) = cur.key() else {
            break;
        };
        let consumed = match key {
            TRACE_KEY_PHASE => set_u32(&mut t.phase, cur.uint()),
            TRACE_KEY_BYTE_OFFSET => set_u32(&mut t.byte_offset, cur.uint()),
            TRACE_KEY_OP => set_opt(&mut t.op, cur.str()),
            TRACE_KEY_DEPTH => set_u32(&mut t.depth, cur.uint()),
            TRACE_KEY_PATH => set_opt(&mut t.path, cur.str_array()),
            TRACE_KEY_SNIPPET => set_opt(&mut t.snippet, cur.str()),
            TRACE_KEY_LANG => set_u32(&mut t.lang, cur.uint()),
            TRACE_KEY_AEL_OFFSET => set_u32(&mut t.ael_offset, cur.uint()),
            TRACE_KEY_AEL_SPAN => set_u32(&mut t.ael_span, cur.uint()),
            _ => false,
        };
        if !consumed && !cur.skip_value() {
            break;
        }
    }

    Some(t)
}

/// Store a decoded value and report whether the decoder consumed it.
fn set_opt<T>(slot: &mut Option<T>, decoded: Option<T>) -> bool {
    decoded.is_some_and(|v| {
        *slot = Some(v);
        true
    })
}

/// [`set_opt`] with the u64 → u32 narrowing every trace integer needs.
fn set_u32(slot: &mut Option<u32>, decoded: Option<u64>) -> bool {
    set_opt(slot, decoded.map(|v| v as u32))
}

/// Msgpack-detail parser unit tests. Ported from the Go client's
/// `error_detail_parser_test.go` (CLIENT-4975). The Go tests drive the parser
/// through `recordParser.parseFieldsError`; here the msgpack-decoding cases
/// call [`parse_error_detail`] directly (the field-walk that wraps it is
/// covered by `commands::buffer::tests`).
#[cfg(test)]
mod tests {
    use super::*;

    // ---- msgpack builder helpers (mirror the Go test helpers) ----

    /// Positive fixint / fixmap int key (0..=0x7F).
    fn fixint(v: u8) -> Vec<u8> {
        assert!(v <= 0x7F);
        vec![v]
    }

    fn int_key(v: u8) -> Vec<u8> {
        fixint(v)
    }

    fn fixstr(s: &str) -> Vec<u8> {
        assert!(s.len() <= 0x1F);
        let mut v = vec![0xA0 | s.len() as u8];
        v.extend_from_slice(s.as_bytes());
        v
    }

    fn str8(s: &str) -> Vec<u8> {
        let mut v = vec![0xD9, s.len() as u8];
        v.extend_from_slice(s.as_bytes());
        v
    }

    fn str16(s: &str) -> Vec<u8> {
        let mut v = vec![0xDA];
        v.extend_from_slice(&(s.len() as u16).to_be_bytes());
        v.extend_from_slice(s.as_bytes());
        v
    }

    fn str32(s: &str) -> Vec<u8> {
        let mut v = vec![0xDB];
        v.extend_from_slice(&(s.len() as u32).to_be_bytes());
        v.extend_from_slice(s.as_bytes());
        v
    }

    fn uint8(v: u8) -> Vec<u8> {
        vec![0xCC, v]
    }

    fn uint16(v: u16) -> Vec<u8> {
        let mut out = vec![0xCD];
        out.extend_from_slice(&v.to_be_bytes());
        out
    }

    fn uint32(v: u32) -> Vec<u8> {
        let mut out = vec![0xCE];
        out.extend_from_slice(&v.to_be_bytes());
        out
    }

    fn uint64(v: u64) -> Vec<u8> {
        let mut out = vec![0xCF];
        out.extend_from_slice(&v.to_be_bytes());
        out
    }

    fn pair(k: &[u8], v: &[u8]) -> Vec<u8> {
        let mut out = k.to_vec();
        out.extend_from_slice(v);
        out
    }

    /// fixmap (0..=15 pairs).
    fn fixmap(pairs: &[Vec<u8>]) -> Vec<u8> {
        assert!(pairs.len() <= 15);
        let mut out = vec![0x80 | pairs.len() as u8];
        for p in pairs {
            out.extend_from_slice(p);
        }
        out
    }

    /// fixarray (0..=15 elements).
    fn fixarray(elems: &[Vec<u8>]) -> Vec<u8> {
        assert!(elems.len() <= 15);
        let mut out = vec![0x90 | elems.len() as u8];
        for e in elems {
            out.extend_from_slice(e);
        }
        out
    }

    // ============================================================
    // fixmap baseline
    // ============================================================

    #[test]
    fn parses_fixmap_with_subcode_and_message() {
        let detail = fixmap(&[
            pair(&int_key(1), &fixint(99)),
            pair(&int_key(2), &fixstr("cannot append")),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "cannot append (subcode=99)");
        assert_eq!(d.sub_code, 99);
    }

    #[test]
    fn parses_fixmap_with_subcode_only() {
        let detail = fixmap(&[pair(&int_key(1), &fixint(42))]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "error subcode=42");
        assert_eq!(d.sub_code, 42);
    }

    #[test]
    fn parses_fixmap_with_message_only() {
        let detail = fixmap(&[pair(&int_key(2), &fixstr("oops"))]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "oops");
        assert_eq!(d.sub_code, sub_code::NONE);
    }

    #[test]
    fn parses_keys_in_reverse_order() {
        let detail = fixmap(&[
            pair(&int_key(2), &fixstr("swap")),
            pair(&int_key(1), &fixint(7)),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "swap (subcode=7)");
        assert_eq!(d.sub_code, 7);
    }

    #[test]
    fn parses_multi_byte_utf8_message() {
        let multibyte = "αβγ · 测试 · 🚀";
        let detail = fixmap(&[
            pair(&int_key(1), &fixint(1)),
            pair(&int_key(2), &str8(multibyte)),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, format!("{multibyte} (subcode=1)"));
    }

    // ============================================================
    // msgpack header / value variants
    // ============================================================

    #[test]
    fn parses_map16_header() {
        let mut payload = vec![0xDE, 0x00, 16];
        payload.extend(pair(&int_key(1), &fixint(7)));
        payload.extend(pair(&int_key(2), &fixstr("boom")));
        for i in 0..14u8 {
            // unknown key (uint8), nil value
            payload.extend([0xCC, 100 + i, 0xC0]);
        }
        let d = parse_error_detail(&payload).unwrap();
        assert_eq!(d.message, "boom (subcode=7)");
    }

    #[test]
    fn parses_map32_header() {
        let mut payload = vec![0xDF, 0x00, 0x00, 0x00, 0x02];
        payload.extend(pair(&int_key(1), &fixint(9)));
        payload.extend(pair(&int_key(2), &fixstr("m32")));
        let d = parse_error_detail(&payload).unwrap();
        assert_eq!(d.message, "m32 (subcode=9)");
    }

    #[test]
    fn parses_str32_message() {
        let big = "x".repeat(100);
        let mut payload = vec![0x82];
        payload.extend(pair(&int_key(1), &fixint(5)));
        payload.extend(int_key(2));
        payload.extend(str32(&big));
        let d = parse_error_detail(&payload).unwrap();
        assert_eq!(d.message, format!("{big} (subcode=5)"));
    }

    #[test]
    fn parses_subcode_as_fixint() {
        let detail = fixmap(&[
            pair(&int_key(1), &fixint(127)),
            pair(&int_key(2), &fixstr("fx")),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "fx (subcode=127)");
    }

    #[test]
    fn parses_subcode_as_uint8() {
        let detail = fixmap(&[
            pair(&int_key(1), &uint8(200)),
            pair(&int_key(2), &fixstr("u8")),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "u8 (subcode=200)");
        assert_eq!(d.sub_code, 200);
    }

    #[test]
    fn parses_subcode_as_uint16() {
        let detail = fixmap(&[
            pair(&int_key(1), &uint16(1100)),
            pair(&int_key(2), &fixstr("hi")),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "hi (subcode=1100)");
    }

    #[test]
    fn parses_subcode_as_uint32() {
        let detail = fixmap(&[
            pair(&int_key(1), &uint32(70000)),
            pair(&int_key(2), &fixstr("x")),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "x (subcode=70000)");
    }

    #[test]
    fn parses_subcode_as_uint64() {
        let value: u64 = 5_000_000_000;
        let detail = fixmap(&[
            pair(&int_key(1), &uint64(value)),
            pair(&int_key(2), &fixstr("u64")),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert!(d.message.starts_with("u64 (subcode="));
        assert_eq!(u64::from(d.sub_code), value as u32 as u64);
    }

    #[test]
    fn parses_message_as_str8() {
        let msg = "string8";
        let detail = fixmap(&[pair(&int_key(1), &fixint(3)), pair(&int_key(2), &str8(msg))]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, format!("{msg} (subcode=3)"));
    }

    #[test]
    fn parses_message_as_str16() {
        let msg = "string16";
        let detail = fixmap(&[
            pair(&int_key(1), &fixint(4)),
            pair(&int_key(2), &str16(msg)),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, format!("{msg} (subcode=4)"));
    }

    // ============================================================
    // defensive edge cases
    // ============================================================

    #[test]
    fn empty_map_produces_no_detail() {
        // Go asserts an empty message; our parser reports None (no keys → no
        // detail worth surfacing), which the field-walk treats the same way.
        assert!(parse_error_detail(&[0x80]).is_none());
    }

    #[test]
    fn truncated_value_does_not_panic() {
        // {1: <uint16 header with no payload>}
        let d = parse_error_detail(&[0x81, 0x01, 0xCD]);
        assert!(d.is_none() || d.unwrap().message.is_empty());
    }

    #[test]
    fn truncated_map_header_returns_no_detail() {
        assert!(parse_error_detail(&[0xDE]).is_none());
    }

    #[test]
    fn unknown_keys_are_skipped_not_fatal() {
        let payload = fixmap(&[
            pair(&int_key(50), &fixint(0)), // unknown
            pair(&int_key(1), &fixint(7)),
            pair(&int_key(51), &[0xC0]), // unknown, nil value
            pair(&int_key(2), &fixstr("z")),
        ]);
        let d = parse_error_detail(&payload).unwrap();
        assert_eq!(d.message, "z (subcode=7)");
    }

    #[test]
    fn empty_and_malformed_payloads_yield_none() {
        assert!(parse_error_detail(&[]).is_none());
        assert!(parse_error_detail(&[0x80]).is_none()); // {}
        assert!(parse_error_detail(&[0xA3, b'a']).is_none()); // not a map
        assert!(parse_error_detail(&[0xDE, 0x00]).is_none()); // truncated map16
    }

    #[test]
    fn truncation_keeps_earlier_fields() {
        // {1: 4, 2: <str8 header promising more bytes than present>}
        let buf = [0x82, 0x01, 0x04, 0x02, 0xD9, 0x20, b'x'];
        let d = parse_error_detail(&buf).unwrap();
        assert_eq!(d.sub_code, 4);
        assert_eq!(d.message, "error subcode=4");
    }

    // ============================================================
    // verbosity-3 expression trace (nested key-3 map)
    // ============================================================

    #[test]
    fn parses_a_full_expression_trace_alongside_the_message() {
        let trace = fixmap(&[
            pair(&int_key(1), &fixint(EXP_TRACE_PHASE_BUILD as u8)),
            pair(&int_key(2), &fixint(7)),
            pair(&int_key(3), &fixstr("cmp_eq")),
            pair(&int_key(4), &fixint(3)),
            pair(
                &int_key(5),
                &fixarray(&[fixstr("and"), fixstr("eq"), fixstr("cmp_eq")]),
            ),
            pair(&int_key(6), &fixstr("eq(int,float)")),
        ]);
        let detail = fixmap(&[
            pair(&int_key(2), &fixstr("bad exp")),
            pair(&int_key(3), &trace),
        ]);
        let d = parse_error_detail(&detail).unwrap();

        // Message still surfaces unchanged; subcode absent (no key 1).
        assert_eq!(d.message, "bad exp");
        assert_eq!(d.sub_code, sub_code::NONE);

        let t = d.exp_trace.unwrap();
        assert_eq!(t.phase, Some(EXP_TRACE_PHASE_BUILD));
        assert_eq!(t.byte_offset, Some(7));
        assert_eq!(t.op.as_deref(), Some("cmp_eq"));
        assert_eq!(t.depth, Some(3));
        assert_eq!(t.snippet.as_deref(), Some("eq(int,float)"));
        assert_eq!(
            t.path,
            Some(vec!["and".into(), "eq".into(), "cmp_eq".into()])
        );
        assert_eq!(t.lang, Some(EXP_TRACE_LANG_MSGPACK));
    }

    #[test]
    fn keeps_path_truncation_sentinel_and_reports_true_depth() {
        let trace = fixmap(&[
            pair(&int_key(1), &fixint(EXP_TRACE_PHASE_BUILD as u8)),
            pair(&int_key(4), &fixint(20)),
            pair(
                &int_key(5),
                &fixarray(&[fixstr("and"), fixstr("or"), fixstr("..."), fixstr("cmp_eq")]),
            ),
        ]);
        let detail = fixmap(&[pair(&int_key(3), &trace)]);
        let t = parse_error_detail(&detail).unwrap().exp_trace.unwrap();

        // depth reports the TRUE count, not the truncated path length.
        assert_eq!(t.depth, Some(20));
        let path = t.path.unwrap();
        assert_eq!(path.len(), 4);
        assert_eq!(path[2], EXP_TRACE_PATH_TRUNCATION_SENTINEL);
        assert_eq!(path[2], "...");
        assert_eq!(path[0], "and");
        assert_eq!(path[3], "cmp_eq");
    }

    #[test]
    fn tolerates_snippet_and_path_absent_within_a_present_trace() {
        let trace = fixmap(&[
            pair(&int_key(1), &fixint(EXP_TRACE_PHASE_BUILD as u8)),
            pair(&int_key(2), &fixint(12)),
            pair(&int_key(3), &fixstr("add")),
            pair(&int_key(4), &fixint(2)),
        ]);
        let detail = fixmap(&[pair(&int_key(3), &trace)]);
        let t = parse_error_detail(&detail).unwrap().exp_trace.unwrap();

        assert_eq!(t.phase, Some(EXP_TRACE_PHASE_BUILD));
        assert_eq!(t.byte_offset, Some(12));
        assert_eq!(t.op.as_deref(), Some("add"));
        assert_eq!(t.depth, Some(2));
        assert_eq!(t.snippet, None); // snippet absent
        assert_eq!(t.path, None); // path absent
    }

    #[test]
    fn skips_unknown_reserved_trace_keys_without_corrupting_known_fields() {
        let trace = fixmap(&[
            pair(&int_key(7), &fixint(5)), // outcome, reserved
            pair(&int_key(1), &fixint(EXP_TRACE_PHASE_BUILD as u8)),
            pair(&int_key(11), &fixint(9)), // ael_line, reserved
            pair(&int_key(2), &fixint(4)),
            pair(&int_key(12), &fixint(2)), // ael_col, reserved
            pair(&int_key(99), &fixstr("ignored")), // wholly unknown
        ]);
        let detail = fixmap(&[pair(&int_key(3), &trace)]);
        let t = parse_error_detail(&detail).unwrap().exp_trace.unwrap();

        assert_eq!(t.phase, Some(EXP_TRACE_PHASE_BUILD));
        assert_eq!(t.byte_offset, Some(4));
        // unknown keys did not corrupt absent fields.
        assert_eq!(t.op, None);
        assert_eq!(t.depth, None);
    }

    #[test]
    fn treats_an_absent_lang_as_msgpack() {
        let trace = fixmap(&[
            pair(&int_key(1), &fixint(EXP_TRACE_PHASE_BUILD as u8)),
            pair(&int_key(2), &fixint(1)),
        ]);
        let detail = fixmap(&[pair(&int_key(3), &trace)]);
        let t = parse_error_detail(&detail).unwrap().exp_trace.unwrap();

        assert_eq!(t.lang, Some(EXP_TRACE_LANG_MSGPACK));
        assert_eq!(t.ael_offset, None);
        assert_eq!(t.ael_span, None);
    }

    #[test]
    fn exposes_lang_ael_with_its_offsets_when_present() {
        let trace = fixmap(&[
            pair(&int_key(1), &fixint(EXP_TRACE_PHASE_BUILD as u8)),
            pair(&int_key(8), &fixint(EXP_TRACE_LANG_AEL as u8)),
            pair(&int_key(9), &fixint(42)),
            pair(&int_key(10), &fixint(6)),
        ]);
        let detail = fixmap(&[pair(&int_key(3), &trace)]);
        let t = parse_error_detail(&detail).unwrap().exp_trace.unwrap();

        assert_eq!(t.lang, Some(EXP_TRACE_LANG_AEL));
        assert_eq!(t.ael_offset, Some(42));
        assert_eq!(t.ael_span, Some(6));
    }

    #[test]
    fn leaves_exp_trace_none_for_a_plain_subcode_message_response() {
        let detail = fixmap(&[
            pair(&int_key(1), &fixint(4)),
            pair(&int_key(2), &fixstr("plain")),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "plain (subcode=4)");
        assert!(d.exp_trace.is_none()); // no key 3 => no expression trace
    }

    #[test]
    fn surfaces_the_message_even_when_key3_precedes_key2() {
        let trace = fixmap(&[
            pair(&int_key(1), &fixint(EXP_TRACE_PHASE_BUILD as u8)),
            pair(&int_key(3), &fixstr("eq")),
        ]);
        let detail = fixmap(&[
            pair(&int_key(3), &trace),
            pair(&int_key(2), &fixstr("bad exp")),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "bad exp");
        let t = d.exp_trace.unwrap();
        assert_eq!(t.op.as_deref(), Some("eq"));
    }

    #[test]
    fn treats_a_present_but_empty_nested_map_as_no_trace() {
        // An empty nested trace map yields no trace, and with no subcode or
        // message either, there is nothing to surface: the detail is absent.
        let detail = fixmap(&[pair(&int_key(3), &fixmap(&[]))]);
        assert!(parse_error_detail(&detail).is_none());

        // But a present message with an empty trace map still surfaces the
        // message and simply carries no trace.
        let detail = fixmap(&[
            pair(&int_key(2), &fixstr("msg")),
            pair(&int_key(3), &fixmap(&[])),
        ]);
        let d = parse_error_detail(&detail).unwrap();
        assert_eq!(d.message, "msg");
        assert!(d.exp_trace.is_none());
    }

    #[test]
    fn ael_only_offsets_and_ael_span_wide_encodings() {
        // ael offsets carried as uint16 exercise the wide-int decode paths.
        let trace = fixmap(&[
            pair(&int_key(1), &fixint(EXP_TRACE_PHASE_BUILD as u8)),
            pair(&int_key(8), &fixint(EXP_TRACE_LANG_AEL as u8)),
            pair(&int_key(9), &uint16(300)),
            pair(&int_key(10), &uint16(12)),
        ]);
        let detail = fixmap(&[pair(&int_key(3), &trace)]);
        let t = parse_error_detail(&detail).unwrap().exp_trace.unwrap();
        assert_eq!(t.ael_offset, Some(300));
        assert_eq!(t.ael_span, Some(12));
    }

    // ============================================================
    // Display
    // ============================================================

    #[test]
    fn detail_and_trace_display() {
        let d = ServerErrorDetail {
            sub_code: 2,
            message: "boom (subcode=2)".into(),
            exp_trace: Some(ExpressionTrace {
                phase: Some(EXP_TRACE_PHASE_BUILD),
                byte_offset: Some(7),
                op: Some("add".into()),
                depth: Some(1),
                path: Some(vec!["and".into(), "add".into()]),
                snippet: None,
                lang: Some(EXP_TRACE_LANG_MSGPACK),
                ael_offset: None,
                ael_span: None,
            }),
        };
        let s = d.to_string();
        assert!(s.starts_with("boom (subcode=2), ExpressionTrace[phase=1"));
        assert!(s.contains("path=[and add]"));
        assert!(!s.contains("lang=")); // msgpack default elided
    }
}
