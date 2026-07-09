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

/// Bit 0 encoding selector — `0` = single-byte flags (v1); `1` = varInt (future).
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

/// Encodes field `44` (`WHERE`) payloads for internal server query explain/execute.
///
/// Wire shape: `[flags: u8][AEL source UTF-8...]`.
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
            return Err(Error::InvalidArgument(
                "explain WHERE flags must include EXPLAIN".into(),
            ));
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
            return Err(Error::InvalidArgument(
                "WHERE AEL must not be null or blank".into(),
            ));
        }
        Ok(())
    }

    /// Returns the flags byte from a WHERE payload built by this module.
    pub fn flags(payload: &[u8]) -> Result<u8> {
        if payload.is_empty() {
            return Err(Error::ClientError("missing WHERE payload".into()));
        }
        Ok(payload[0])
    }

    /// Clears explain-only flags on a phase-1 payload built by this module.
    pub(crate) fn clear_explain_in_place(payload: &mut [u8]) {
        if !payload.is_empty() {
            payload[0] &= !EXPLAIN_ONLY_FLAGS;
        }
    }

    /// Returns the AEL source text from a WHERE payload built by this module.
    pub fn ael(payload: &[u8]) -> Result<String> {
        if payload.len() < 2 {
            return Err(Error::ClientError("missing WHERE payload".into()));
        }
        std::str::from_utf8(&payload[1..])
            .map(str::to_owned)
            .map_err(|e| Error::ClientError(format!("invalid WHERE AEL UTF-8: {e}")))
    }

    fn encode(flags: u8, ael: &str) -> Result<Vec<u8>> {
        Self::require_ael(ael)?;
        Self::validate_flags(flags)?;
        let mut payload = Vec::with_capacity(1 + ael.len());
        payload.push(flags);
        payload.extend_from_slice(ael.as_bytes());
        Ok(payload)
    }

    fn validate_flags(flags: u8) -> Result<()> {
        if flags & !FLAG_KNOWN != 0 {
            return Err(Error::InvalidArgument(format!(
                "unknown WHERE flags 0x{flags:02x}"
            )));
        }
        if flags & FLAG_ENC_VARINT != 0 {
            return Err(Error::InvalidArgument(
                "varInt WHERE flags encoding is not supported".into(),
            ));
        }
        Ok(())
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
        QueryWhereWire::clear_explain_in_place(&mut explain);
        assert_eq!(QueryWhereWire::flags(&explain).unwrap(), 0);
        assert_eq!(QueryWhereWire::ael(&explain).unwrap(), COMPOUND_AEL);
        assert_eq!(explain, QueryWhereWire::for_execute(COMPOUND_AEL).unwrap());
    }

    #[test]
    fn clear_explain_on_default_explain_payload() {
        let mut explain = QueryWhereWire::for_explain(SIMPLE_AEL).unwrap();
        QueryWhereWire::clear_explain_in_place(&mut explain);
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
    fn rejects_varint_encoding_flag() {
        assert!(QueryWhereWire::for_explain_with_flags(FLAG_ENC_VARINT, SIMPLE_AEL).is_err());
    }
}
