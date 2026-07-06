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

const FLAG_KNOWN: u8 = FLAG_ENC_VARINT | FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT;

/// Encodes field `44` (`WHERE`) payloads for server query explain/execute.
///
/// Wire shape: `[flags: u8][AEL source UTF-8...]`.
pub struct QueryWhereWire;

impl QueryWhereWire {
    /// Field `44` body for phase 1 (explain).
    pub fn for_explain(ael: &str) -> Result<Vec<u8>> {
        Self::encode(FLAG_EXPLAIN, ael)
    }

    /// Field `44` body for phase 2 (execute) — same AEL, EXPLAIN cleared.
    pub fn for_execute(ael: &str) -> Result<Vec<u8>> {
        Self::encode(0, ael)
    }

    /// Encodes a WHERE field value: `[flags][AEL UTF-8]`.
    pub fn encode(flags: u8, ael: &str) -> Result<Vec<u8>> {
        Self::validate_flags(flags)?;
        let ael_bytes = Self::ael_bytes(ael)?;
        let mut payload = Vec::with_capacity(1 + ael_bytes.len());
        payload.push(flags);
        payload.extend_from_slice(&ael_bytes);
        Ok(payload)
    }

    /// Rebuilds execute payload from an explain payload (clears [`FLAG_EXPLAIN`]).
    pub fn clear_explain(explain_payload: &[u8]) -> Result<Vec<u8>> {
        if explain_payload.len() < 2 {
            return Err(Error::InvalidArgument(
                "explain WHERE payload must include flags and AEL".into(),
            ));
        }
        let flags = explain_payload[0];
        Self::validate_flags(flags)?;
        if flags & FLAG_EXPLAIN == 0 {
            return Err(Error::InvalidArgument(
                "explain WHERE payload must have EXPLAIN flag set".into(),
            ));
        }
        let execute_flags = flags & !FLAG_EXPLAIN;
        let mut payload = Vec::with_capacity(explain_payload.len());
        payload.push(execute_flags);
        payload.extend_from_slice(&explain_payload[1..]);
        Ok(payload)
    }

    /// Returns the flags byte from a WHERE payload.
    pub fn flags(payload: &[u8]) -> Result<u8> {
        payload.first().copied().ok_or_else(|| {
            Error::InvalidArgument("WHERE payload must not be null or empty".into())
        })
    }

    /// Returns the AEL source text from a WHERE payload.
    pub fn ael(payload: &[u8]) -> Result<String> {
        if payload.len() < 2 {
            return Err(Error::InvalidArgument(
                "WHERE payload must include flags and AEL".into(),
            ));
        }
        std::str::from_utf8(&payload[1..])
            .map(str::to_owned)
            .map_err(|e| Error::InvalidArgument(format!("invalid WHERE AEL UTF-8: {e}")))
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

    fn ael_bytes(ael: &str) -> Result<Vec<u8>> {
        if ael.is_empty() {
            return Err(Error::InvalidArgument("WHERE AEL must not be empty".into()));
        }
        Ok(ael.as_bytes().to_vec())
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
        assert_eq!(QueryWhereWire::flags(&payload).unwrap(), FLAG_EXPLAIN);
        assert_eq!(QueryWhereWire::ael(&payload).unwrap(), SIMPLE_AEL);
        assert_eq!(payload, expected_payload(FLAG_EXPLAIN, SIMPLE_AEL));
    }

    #[test]
    fn for_execute_clears_explain_flag() {
        let payload = QueryWhereWire::for_execute(SIMPLE_AEL).unwrap();
        assert_eq!(QueryWhereWire::flags(&payload).unwrap(), 0);
        assert_eq!(QueryWhereWire::ael(&payload).unwrap(), SIMPLE_AEL);
        assert_eq!(payload, expected_payload(0, SIMPLE_AEL));
    }

    #[test]
    fn encode_supports_compound_ael() {
        let payload = QueryWhereWire::for_explain(COMPOUND_AEL).unwrap();
        assert_eq!(QueryWhereWire::ael(&payload).unwrap(), COMPOUND_AEL);
    }

    #[test]
    fn clear_explain_rebuilds_execute_payload() {
        let explain = QueryWhereWire::encode(FLAG_EXPLAIN | FLAG_REQUIRE_INDEX, COMPOUND_AEL)
            .unwrap();
        let execute = QueryWhereWire::clear_explain(&explain).unwrap();
        assert_eq!(QueryWhereWire::flags(&execute).unwrap(), FLAG_REQUIRE_INDEX);
        assert_eq!(QueryWhereWire::ael(&execute).unwrap(), COMPOUND_AEL);
    }

    #[test]
    fn rejects_empty_ael() {
        assert!(QueryWhereWire::for_explain("").is_err());
    }

    #[test]
    fn rejects_unknown_flags() {
        assert!(QueryWhereWire::encode(1 << 4, SIMPLE_AEL).is_err());
    }

    #[test]
    fn rejects_varint_encoding_flag() {
        assert!(QueryWhereWire::encode(FLAG_ENC_VARINT, SIMPLE_AEL).is_err());
    }

    #[test]
    fn clear_explain_requires_explain_flag() {
        let execute_shape = QueryWhereWire::for_execute(SIMPLE_AEL).unwrap();
        assert!(QueryWhereWire::clear_explain(&execute_shape).is_err());
    }
}
