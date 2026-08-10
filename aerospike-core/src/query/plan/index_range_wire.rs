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

use crate::commands::particle_type::ParticleType;

/// Internal transform of explain field `22` (`INDEX_RANGE`) for phase-2 execute.
pub(crate) struct IndexRangeWire;

impl IndexRangeWire {
    /// Converts explain INDEX_RANGE bytes to the execute shape used with field `21`.
    pub(crate) fn for_execute_with_index_name(probe_range_bytes: &[u8]) -> Result<Vec<u8>> {
        if probe_range_bytes.is_empty() {
            return Err(Error::bad_response(
                "empty INDEX_RANGE field body",
            ));
        }

        let mut offset = 0usize;
        let n_ranges = probe_range_bytes[offset];
        offset += 1;
        if n_ranges != 1 {
            return Err(Error::bad_response(format!(
                "INDEX_RANGE field must contain a single range, found {n_ranges}"
            )));
        }
        if offset >= probe_range_bytes.len() {
            return Err(Error::bad_response(
                "truncated INDEX_RANGE field body",
            ));
        }

        let bin_name_len = probe_range_bytes[offset] as usize;
        offset += 1;
        if bin_name_len == 0 {
            return Ok(probe_range_bytes.to_vec());
        }
        if offset + bin_name_len > probe_range_bytes.len() {
            return Err(Error::bad_response(
                "truncated INDEX_RANGE field body",
            ));
        }

        offset += bin_name_len;
        let tail_len = probe_range_bytes.len() - offset;
        if tail_len == 0 {
            return Err(Error::bad_response(
                "truncated INDEX_RANGE field body",
            ));
        }

        let mut execute = Vec::with_capacity(2 + tail_len);
        execute.push(1);
        execute.push(0);
        execute.extend_from_slice(&probe_range_bytes[offset..]);
        Ok(execute)
    }

    /// Human-readable explain `INDEX_RANGE` for debug logs.
    pub(crate) fn describe_probe_range(probe_range_bytes: Option<&[u8]>) -> Option<String> {
        let probe_range_bytes = probe_range_bytes?;
        if probe_range_bytes.len() < 2 {
            return Some("invalid(truncated)".into());
        }

        let bin_name_len = probe_range_bytes[1] as usize;
        let mut offset = 2usize;
        if probe_range_bytes.len() < offset + bin_name_len + 1 {
            return Some("invalid(truncated)".into());
        }

        let bin_name = if bin_name_len > 0 {
            std::str::from_utf8(&probe_range_bytes[offset..offset + bin_name_len])
                .unwrap_or("<invalid utf-8>")
                .to_owned()
        } else {
            String::new()
        };
        offset += bin_name_len;
        let ktype = probe_range_bytes[offset];
        offset += 1;

        match ktype {
            x if x == ParticleType::INTEGER as u8 => {
                let begin = read_integer_bound(probe_range_bytes, offset)?;
                let end = read_integer_bound(probe_range_bytes, begin.next_offset)?;
                Some(format!(
                    "bin={bin_name} range=[{},{}]",
                    begin.value, end.value
                ))
            }
            x if x == ParticleType::STRING as u8 => {
                let value = read_bytes_bound(probe_range_bytes, offset)?;
                let text = std::str::from_utf8(&value.bytes).unwrap_or("<invalid utf-8>");
                Some(format!(
                    "bin={bin_name} value={text} len={}",
                    value.bytes.len()
                ))
            }
            x if x == ParticleType::BLOB as u8 => {
                let value = read_bytes_bound(probe_range_bytes, offset)?;
                Some(format!(
                    "bin={bin_name} value=x'{}' len={}",
                    bytes_to_hex(&value.bytes),
                    value.bytes.len()
                ))
            }
            _ => Some(format!(
                "bin={bin_name} ktype={ktype} hex={}",
                bytes_to_hex(probe_range_bytes)
            )),
        }
    }
}

struct BoundLong {
    value: i64,
    next_offset: usize,
}

struct BoundBytes {
    bytes: Vec<u8>,
    next_offset: usize,
}

fn read_integer_bound(bytes: &[u8], offset: usize) -> Option<BoundLong> {
    let raw = read_bytes_bound(bytes, offset)?;
    if raw.bytes.len() != 8 {
        return None;
    }
    let value = i64::from_be_bytes(raw.bytes.try_into().ok()?);
    Some(BoundLong {
        value,
        next_offset: raw.next_offset,
    })
}

fn read_bytes_bound(bytes: &[u8], mut offset: usize) -> Option<BoundBytes> {
    if bytes.len() < offset + 4 {
        return None;
    }
    let len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().ok()?) as usize;
    offset += 4;
    if bytes.len() < offset + len {
        return None;
    }
    Some(BoundBytes {
        bytes: bytes[offset..offset + len].to_vec(),
        next_offset: offset + len,
    })
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer_probe_range(bin_name: &str, begin: i64, end: i64) -> Vec<u8> {
        let mut probe = Vec::with_capacity(2 + bin_name.len() + 1 + 8 + 8 + 8 + 8);
        probe.push(1);
        probe.push(bin_name.len() as u8);
        probe.extend_from_slice(bin_name.as_bytes());
        probe.push(ParticleType::INTEGER as u8);
        probe.extend_from_slice(&8u32.to_be_bytes());
        probe.extend_from_slice(&begin.to_be_bytes());
        probe.extend_from_slice(&8u32.to_be_bytes());
        probe.extend_from_slice(&end.to_be_bytes());
        probe
    }

    fn string_probe_range(bin_name: &str, value: &str) -> Vec<u8> {
        let value_bytes = value.as_bytes();
        let mut probe = Vec::with_capacity(2 + bin_name.len() + 1 + 8 + value_bytes.len());
        probe.push(1);
        probe.push(bin_name.len() as u8);
        probe.extend_from_slice(bin_name.as_bytes());
        probe.push(ParticleType::STRING as u8);
        probe.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
        probe.extend_from_slice(value_bytes);
        probe
    }

    #[test]
    fn describe_integer_range() {
        let probe = integer_probe_range("age", 101, i64::MAX);
        assert_eq!(
            IndexRangeWire::describe_probe_range(Some(&probe)).unwrap(),
            "bin=age range=[101,9223372036854775807]"
        );
    }

    #[test]
    fn describe_integer_equality() {
        let probe = integer_probe_range("age", 30, 30);
        assert_eq!(
            IndexRangeWire::describe_probe_range(Some(&probe)).unwrap(),
            "bin=age range=[30,30]"
        );
    }

    #[test]
    fn describe_string_equality() {
        let probe = string_probe_range("ka", "k1");
        assert_eq!(
            IndexRangeWire::describe_probe_range(Some(&probe)).unwrap(),
            "bin=ka value=k1 len=2"
        );
    }

    #[test]
    fn describe_null_range() {
        assert!(IndexRangeWire::describe_probe_range(None).is_none());
    }

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
        assert!(matches!(err.kind(), crate::ErrorKind::BadResponse));
    }

    #[test]
    fn rejects_multiple_ranges() {
        let probe = vec![2, 3, b'a', b'g', b'e'];
        let err = IndexRangeWire::for_execute_with_index_name(&probe).unwrap_err();
        assert!(matches!(err.kind(), crate::ErrorKind::BadResponse));
    }

    #[test]
    fn rejects_truncated_bin_name() {
        let probe = vec![1, 3, b'a', b'g'];
        let err = IndexRangeWire::for_execute_with_index_name(&probe).unwrap_err();
        assert!(matches!(err.kind(), crate::ErrorKind::BadResponse));
    }
}
