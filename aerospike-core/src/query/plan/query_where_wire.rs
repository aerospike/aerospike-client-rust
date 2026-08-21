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

use crate::errors::{Error, Result};

/// Bit 0 continuation bit — `1` = more flag bytes follow; `0` = last flag byte.
///
/// On a single-byte prefix with bit 0 clear, wire layout matches v1 encoding (bit 0 was
/// documented as an unused encoding selector).
pub const FLAG_ENC_VARINT: u8 = 1 << 0;

/// Explain phase — server runs index planner only.
pub const FLAG_EXPLAIN: u8 = 1 << 1;

/// Optional: reject PI fallback on explain when set with [`FLAG_EXPLAIN`].
pub const FLAG_REQUIRE_INDEX: u8 = 1 << 2;

/// Explain-only: require field `21` index name hint.
pub const FLAG_HARD_HINT: u8 = 1 << 3;

/// Supported flag bits for field `44` WHERE (v1).
pub const FLAG_KNOWN: u8 = FLAG_ENC_VARINT | FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT;

/// Explain-only flags cleared when building field `44` for execute.
const EXPLAIN_ONLY_FLAGS: u8 = FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT;

/// Semantic flag bits carried in bits 1–7 of each prefix byte.
const FLAG_SEMANTIC_MASK: u8 = 0xFE;

/// Maximum varInt-style flag prefix length (guards malformed payloads).
const MAX_FLAG_PREFIX_LEN: usize = 4;

/// Encodes field `44` (`WHERE`) payloads for internal server query explain/execute.
///
/// Wire shape: `[flag-byte 0][flag-byte 1]…[flag-byte N][AEL source UTF-8…]`.
///
/// Each flag byte uses varInt-style continuation: bit 0 = continuation, bits 1–7 = semantic
/// flags OR'd across bytes. The default encode path emits a single byte with bit 0 clear when
/// all semantic flags fit in bits 1–7 (backward compatible with v1 clients).
pub struct QueryWhereWire;

impl QueryWhereWire {
    /// Field `44` body for phase 1 (explain) with default flags ([`FLAG_EXPLAIN`] only).
    pub fn for_explain(ael: &str) -> Result<Vec<u8>> {
        Self::for_explain_with_flags(FLAG_EXPLAIN, ael)
    }

    /// Field `44` body for phase 1 (explain) with the given flag mask (must include
    /// [`FLAG_EXPLAIN`]; may include [`FLAG_REQUIRE_INDEX`] / [`FLAG_HARD_HINT`]).
    pub fn for_explain_with_flags(flags: u8, ael: &str) -> Result<Vec<u8>> {
        if flags & FLAG_EXPLAIN == 0 {
            return Err(Error::invalid_argument("explain WHERE flags must include EXPLAIN"));
        }
        Self::encode(flags, ael)
    }

    /// Field `44` body for phase 2 (execute) — same AEL, explain-only flags cleared.
    pub fn for_execute(ael: &str) -> Result<Vec<u8>> {
        Self::encode(0, ael)
    }

    /// Validates AEL before field `44` encoding.
    pub fn require_ael(ael: &str) -> Result<()> {
        if ael.trim().is_empty() {
            return Err(Error::invalid_argument("WHERE AEL must not be null or blank"));
        }
        Ok(())
    }

    /// Returns the decoded semantic flags from a WHERE payload.
    pub fn flags(payload: &[u8]) -> Result<u8> {
        Self::decode_flag_prefix(payload).map(|(flags, _)| flags)
    }

    /// Clears explain-only flags on a phase-1 payload and re-encodes the flag prefix.
    ///
    /// A multi-byte prefix may shrink to a single byte after clearing explain flags.
    pub(crate) fn clear_explain_in_place(payload: &mut Vec<u8>) -> Result<()> {
        if payload.is_empty() {
            return Ok(());
        }
        let (mut flags, ael_offset) = Self::decode_flag_prefix(payload)?;
        flags &= !EXPLAIN_ONLY_FLAGS;
        let prefix = Self::encode_flag_prefix(flags);
        let ael = payload[ael_offset..].to_vec();
        payload.clear();
        payload.extend(prefix);
        payload.extend(ael);
        Ok(())
    }

    /// Returns the AEL source text from a WHERE payload.
    pub fn ael(payload: &[u8]) -> Result<String> {
        let (_, ael_offset) = Self::decode_flag_prefix(payload)?;
        if ael_offset >= payload.len() {
            return Err(Error::client_error("missing WHERE AEL"));
        }
        std::str::from_utf8(&payload[ael_offset..])
            .map(str::to_owned)
            .map_err(|e| Error::client_error(format!("invalid WHERE AEL UTF-8: {e}")))
    }

    fn encode(flags: u8, ael: &str) -> Result<Vec<u8>> {
        Self::require_ael(ael)?;
        Self::validate_flags(flags)?;
        let mut payload = Vec::with_capacity(1 + ael.len());
        payload.extend(Self::encode_flag_prefix(flags));
        payload.extend_from_slice(ael.as_bytes());
        Ok(payload)
    }

    fn validate_flags(flags: u8) -> Result<()> {
        if flags & !FLAG_KNOWN != 0 {
            return Err(Error::invalid_argument(format!(
                "unknown WHERE flags 0x{flags:02x}"
            )));
        }
        if flags & FLAG_ENC_VARINT != 0 {
            return Err(Error::invalid_argument(
                "WHERE flag bit 0 is reserved for wire continuation, not a semantic flag",
            ));
        }
        Ok(())
    }

    /// Decodes a varInt-style flag prefix and returns `(semantic_flags, ael_offset)`.
    fn decode_flag_prefix(payload: &[u8]) -> Result<(u8, usize)> {
        if payload.is_empty() {
            return Err(Error::client_error("missing WHERE payload"));
        }

        let mut offset = 0usize;
        let mut decoded = 0u8;
        loop {
            if offset >= payload.len() {
                return Err(Error::client_error("truncated WHERE flag prefix"));
            }
            if offset >= MAX_FLAG_PREFIX_LEN {
                return Err(Error::client_error("WHERE flag prefix too long"));
            }

            let byte = payload[offset];
            offset += 1;
            decoded |= byte & FLAG_SEMANTIC_MASK;
            if byte & FLAG_ENC_VARINT == 0 {
                break;
            }
        }

        Ok((decoded, offset))
    }

    /// Encodes semantic flags into a varInt-style prefix.
    ///
    /// Current flags always fit in one byte with continuation clear (v1-compatible wire).
    fn encode_flag_prefix(semantic: u8) -> Vec<u8> {
        vec![semantic & FLAG_SEMANTIC_MASK]
    }

    /// Formats Tier-D policy flags for debug logs.
    pub(crate) fn format_policy_flags(flags: u8) -> String {
        let policy_flags = flags & (FLAG_REQUIRE_INDEX | FLAG_HARD_HINT);
        if policy_flags == 0 {
            return "none".into();
        }

        let mut out = String::new();
        if policy_flags & FLAG_REQUIRE_INDEX != 0 {
            out.push_str("REQUIRE_INDEX|");
        }
        if policy_flags & FLAG_HARD_HINT != 0 {
            out.push_str("HARD_HINT|");
        }
        out.pop();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIMPLE_AEL: &str = "$.age > 30";
    const COMPOUND_AEL: &str = "$.age > 30 and $.country == 'US'";

    fn expected_payload(flags: u8, ael: &str) -> Vec<u8> {
        let mut payload = vec![flags];
        payload.extend_from_slice(ael.as_bytes());
        payload
    }

    fn multi_byte_prefix_payload(prefix: &[u8], ael: &str) -> Vec<u8> {
        let mut payload = prefix.to_vec();
        payload.extend_from_slice(ael.as_bytes());
        payload
    }

    #[test]
    fn for_explain_encodes_explain_flag_and_ael() {
        let payload = QueryWhereWire::for_explain(SIMPLE_AEL).unwrap();
        assert_eq!(payload[0], FLAG_EXPLAIN);
        assert_eq!(QueryWhereWire::ael(&payload).unwrap(), SIMPLE_AEL);
        assert_eq!(payload, expected_payload(FLAG_EXPLAIN, SIMPLE_AEL));
    }

    #[test]
    fn for_explain_with_flags_require_index() {
        let flags = FLAG_EXPLAIN | FLAG_REQUIRE_INDEX;
        let payload = QueryWhereWire::for_explain_with_flags(flags, SIMPLE_AEL).unwrap();
        assert_eq!(QueryWhereWire::flags(&payload).unwrap(), flags);
    }

    #[test]
    fn for_explain_with_flags_hard_hint() {
        let flags = FLAG_EXPLAIN | FLAG_HARD_HINT;
        let payload = QueryWhereWire::for_explain_with_flags(flags, SIMPLE_AEL).unwrap();
        assert_eq!(QueryWhereWire::flags(&payload).unwrap(), flags);
    }

    #[test]
    fn for_explain_with_flags_require_index_and_hard_hint() {
        let flags = FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT;
        let payload = QueryWhereWire::for_explain_with_flags(flags, SIMPLE_AEL).unwrap();
        assert_eq!(QueryWhereWire::flags(&payload).unwrap(), flags);
    }

    #[test]
    fn for_explain_with_flags_requires_explain_bit() {
        assert!(QueryWhereWire::for_explain_with_flags(FLAG_REQUIRE_INDEX, SIMPLE_AEL).is_err());
    }

    #[test]
    fn for_execute_clears_explain_flag() {
        let payload = QueryWhereWire::for_execute(SIMPLE_AEL).unwrap();
        assert_eq!(payload[0], 0);
        assert_eq!(QueryWhereWire::ael(&payload).unwrap(), SIMPLE_AEL);
        assert_eq!(payload, expected_payload(0, SIMPLE_AEL));
    }

    #[test]
    fn encode_supports_compound_ael() {
        let payload = QueryWhereWire::for_explain(COMPOUND_AEL).unwrap();
        assert_eq!(QueryWhereWire::ael(&payload).unwrap(), COMPOUND_AEL);
    }

    #[test]
    fn clear_explain_clears_all_explain_only_flags() {
        let mut explain =
            QueryWhereWire::for_explain_with_flags(FLAG_EXPLAIN | FLAG_REQUIRE_INDEX, COMPOUND_AEL)
                .unwrap();
        QueryWhereWire::clear_explain_in_place(&mut explain).unwrap();
        assert_eq!(QueryWhereWire::flags(&explain).unwrap(), 0);
        assert_eq!(QueryWhereWire::ael(&explain).unwrap(), COMPOUND_AEL);
        assert_eq!(explain, QueryWhereWire::for_execute(COMPOUND_AEL).unwrap());
    }

    #[test]
    fn clear_explain_on_default_explain_payload() {
        let mut explain = QueryWhereWire::for_explain(SIMPLE_AEL).unwrap();
        QueryWhereWire::clear_explain_in_place(&mut explain).unwrap();
        assert_eq!(explain, QueryWhereWire::for_execute(SIMPLE_AEL).unwrap());
    }

    #[test]
    fn rejects_empty_ael() {
        assert!(QueryWhereWire::for_explain("").is_err());
        assert!(QueryWhereWire::require_ael("").is_err());
        assert!(QueryWhereWire::require_ael("   ").is_err());
    }

    #[test]
    fn rejects_unknown_flags() {
        assert!(QueryWhereWire::for_explain_with_flags(1 << 4, SIMPLE_AEL).is_err());
    }

    #[test]
    fn rejects_continuation_bit_in_semantic_flags() {
        assert!(QueryWhereWire::for_explain_with_flags(FLAG_ENC_VARINT, SIMPLE_AEL).is_err());
    }

    #[test]
    fn decode_single_byte_prefix_matches_v1() {
        let payload = expected_payload(FLAG_EXPLAIN | FLAG_REQUIRE_INDEX, SIMPLE_AEL);
        assert_eq!(
            QueryWhereWire::flags(&payload).unwrap(),
            FLAG_EXPLAIN | FLAG_REQUIRE_INDEX
        );
        assert_eq!(QueryWhereWire::ael(&payload).unwrap(), SIMPLE_AEL);
    }

    #[test]
    fn decode_multi_byte_prefix_or_semantic_flags() {
        // Byte 0: CONT + EXPLAIN|REQUIRE_INDEX; byte 1: HARD_HINT only.
        let prefix = [FLAG_ENC_VARINT | FLAG_EXPLAIN | FLAG_REQUIRE_INDEX, FLAG_HARD_HINT];
        let payload = multi_byte_prefix_payload(&prefix, SIMPLE_AEL);
        assert_eq!(
            QueryWhereWire::flags(&payload).unwrap(),
            FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT
        );
        assert_eq!(QueryWhereWire::ael(&payload).unwrap(), SIMPLE_AEL);
    }

    #[test]
    fn clear_explain_collapses_multi_byte_prefix_to_single_byte() {
        let prefix = [FLAG_ENC_VARINT | FLAG_EXPLAIN | FLAG_REQUIRE_INDEX, FLAG_HARD_HINT];
        let mut payload = multi_byte_prefix_payload(&prefix, COMPOUND_AEL);
        QueryWhereWire::clear_explain_in_place(&mut payload).unwrap();
        assert_eq!(payload, QueryWhereWire::for_execute(COMPOUND_AEL).unwrap());
    }

    #[test]
    fn rejects_truncated_multi_byte_prefix() {
        let payload = vec![FLAG_ENC_VARINT | FLAG_EXPLAIN];
        assert!(QueryWhereWire::flags(&payload).is_err());
        assert!(QueryWhereWire::ael(&payload).is_err());
    }

    #[test]
    fn rejects_overlong_flag_prefix() {
        let payload = vec![
            FLAG_ENC_VARINT,
            FLAG_ENC_VARINT,
            FLAG_ENC_VARINT,
            FLAG_ENC_VARINT,
            FLAG_ENC_VARINT,
        ];
        assert!(QueryWhereWire::flags(&payload).is_err());
    }
}
