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

/// Internal transform of explain field `22` (`INDEX_RANGE`) for phase-2 execute.
pub(crate) struct IndexRangeWire;

impl IndexRangeWire {
    /// Converts explain INDEX_RANGE bytes to the execute shape used with field `21`.
    pub(crate) fn for_execute_with_index_name(probe_range_bytes: &[u8]) -> Result<Vec<u8>> {
        if probe_range_bytes.is_empty() {
            return Err(Error::BadResponse(
                "empty INDEX_RANGE field body".into(),
            ));
        }

        let mut offset = 0usize;
        let n_ranges = probe_range_bytes[offset];
        offset += 1;
        if n_ranges != 1 {
            return Err(Error::BadResponse(format!(
                "INDEX_RANGE field must contain a single range, found {n_ranges}"
            )));
        }
        if offset >= probe_range_bytes.len() {
            return Err(Error::BadResponse(
                "truncated INDEX_RANGE field body".into(),
            ));
        }

        let bin_name_len = probe_range_bytes[offset] as usize;
        offset += 1;
        if bin_name_len == 0 {
            return Ok(probe_range_bytes.to_vec());
        }
        if offset + bin_name_len > probe_range_bytes.len() {
            return Err(Error::BadResponse(
                "truncated INDEX_RANGE field body".into(),
            ));
        }

        offset += bin_name_len;
        let tail_len = probe_range_bytes.len() - offset;
        if tail_len == 0 {
            return Err(Error::BadResponse(
                "truncated INDEX_RANGE field body".into(),
            ));
        }

        let mut execute = Vec::with_capacity(2 + tail_len);
        execute.push(1);
        execute.push(0);
        execute.extend_from_slice(&probe_range_bytes[offset..]);
        Ok(execute)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_bin_name_for_execute_with_index_name() {
        let probe = vec![1, 3, b'a', b'g', b'e', 1, 9, 10, 11];
        let execute = IndexRangeWire::for_execute_with_index_name(&probe).unwrap();
        assert_eq!(execute, vec![1, 0, 1, 9, 10, 11]);
    }

    #[test]
    fn no_op_when_bin_name_len_already_zero() {
        let probe = vec![1, 0, 3, 0, 0, 0, 0, 4];
        let execute = IndexRangeWire::for_execute_with_index_name(&probe).unwrap();
        assert_eq!(execute, probe);
    }

    #[test]
    fn rejects_empty_payload() {
        let err = IndexRangeWire::for_execute_with_index_name(&[]).unwrap_err();
        assert!(matches!(err, Error::BadResponse(_)));
    }

    #[test]
    fn rejects_multiple_ranges() {
        let probe = vec![2, 3, b'a', b'g', b'e'];
        let err = IndexRangeWire::for_execute_with_index_name(&probe).unwrap_err();
        assert!(matches!(err, Error::BadResponse(_)));
    }

    #[test]
    fn rejects_truncated_bin_name() {
        let probe = vec![1, 3, b'a', b'g'];
        let err = IndexRangeWire::for_execute_with_index_name(&probe).unwrap_err();
        assert!(matches!(err, Error::BadResponse(_)));
    }
}
