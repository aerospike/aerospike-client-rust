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

/// Encodes field `44` (`WHERE`) payloads for internal server query explain/execute.
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

    /// Clears [`FLAG_EXPLAIN`] on a phase-1 payload built by this module.
    pub(crate) fn clear_explain_in_place(payload: &mut [u8]) {
        debug_assert!(
            payload.len() >= 2 && payload[0] & FLAG_EXPLAIN != 0,
            "clear_explain_in_place expects a client-built explain WHERE payload"
        );
        if !payload.is_empty() {
            payload[0] &= !FLAG_EXPLAIN;
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
        if ael.is_empty() {
            return Err(Error::InvalidArgument("Empty WHERE clause".into()));
        }
        let mut payload = Vec::with_capacity(1 + ael.len());
        payload.push(flags);
        payload.extend_from_slice(ael.as_bytes());
        Ok(payload)
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
    fn clear_explain_rebuilds_execute_payload() {
        let mut explain = QueryWhereWire::for_explain(COMPOUND_AEL).unwrap();
        explain[0] |= FLAG_REQUIRE_INDEX;
        QueryWhereWire::clear_explain_in_place(&mut explain);
        assert_eq!(explain[0], FLAG_REQUIRE_INDEX);
        assert_eq!(QueryWhereWire::ael(&explain).unwrap(), COMPOUND_AEL);
    }

    #[test]
    fn rejects_empty_ael() {
        assert!(QueryWhereWire::for_explain("").is_err());
    }
}
