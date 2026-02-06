// Copyright 2015-2018 Aerospike, Inc.
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

use aerospike::Value;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DBObjectType {
    Integer,
    Bytes,
    String,
    Timestamp,
    Random,
}

impl DBObjectType {
    fn parse_type(t: char) -> Result<Self, String> {
        match t {
            'I' => Ok(DBObjectType::Integer),
            'S' => Ok(DBObjectType::String),
            'B' => Ok(DBObjectType::Bytes),
            'D' => Ok(DBObjectType::Timestamp),
            'R' => Ok(DBObjectType::Random),
            _other => Err(format!("Invalid types: {_other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DBObjectSpec {
    bin_type: DBObjectType,
    size: usize,
    pub rand_pct: u32,
}

impl Default for DBObjectSpec {
    fn default() -> Self {
        DBObjectSpec {
            bin_type: DBObjectType::Integer,
            size: 8,
            rand_pct: 0,
        }
    }
}

impl FromStr for DBObjectSpec {
    type Err = String;

    fn from_str(bin_spec: &str) -> Result<Self, Self::Err> {
        let bin_spec_trimmed = bin_spec.trim();
        if bin_spec_trimmed.is_empty() {
            return Err("Empty bin spec".into());
        }

        let args: Vec<&str> = bin_spec_trimmed.split(':').collect();
        let type_str = args
            .get(0)
            .ok_or_else(|| "Empty object spec".to_string())?
            .trim();

        let t = type_str
            .chars()
            .next()
            .ok_or_else(|| "Empty object spec type".to_string())?;

        let bin_type = DBObjectType::parse_type(t)?;
        match bin_type {
            DBObjectType::Timestamp | DBObjectType::Integer => Ok(DBObjectSpec {
                bin_type,
                size: 8,
                rand_pct: 0,
            }),
            DBObjectType::Bytes | DBObjectType::String => {
                let size = args
                    .get(1)
                    .ok_or_else(|| "Missing size for object spec".to_string())
                    .and_then(|s| parse_size(s))?;

                Ok(DBObjectSpec {
                    bin_type,
                    size,
                    rand_pct: 0,
                })
            }
            DBObjectType::Random => {
                let total_bytes = args
                    .get(1)
                    .ok_or_else(|| "Missing size for object spec".to_string())
                    .and_then(|s| parse_size(s))?;

                let rand_pct: usize = args
                    .get(2)
                    .ok_or_else(|| "Missing randPct for random object spec".to_string())
                    .and_then(|s| parse_size(s))?;

                let chunks = total_bytes / 8;
                Ok(DBObjectSpec {
                    bin_type,
                    size: chunks as usize,
                    rand_pct: rand_pct as u32,
                })
            }
        }
    }
}

impl DBObjectSpec {
    pub fn gen_value<R: Rng + ?Sized>(&self, rng: &mut R, key_seed: Option<i64>) -> Value {
        match self.bin_type {
            DBObjectType::Integer => match key_seed {
                Some(seed) => Value::from(seed),
                None => Value::Int(rng.gen_range(0..i64::MAX)),
            },
            DBObjectType::Bytes => {
                let mut buf = vec![0u8; self.size];
                rng.fill_bytes(&mut buf);
                Value::Blob(buf)
            }
            DBObjectType::String => {
                let str_value = (0..self.size)
                    .map(|_| rng.sample(Alphanumeric) as char)
                    .collect();
                Value::String(str_value)
            }
            DBObjectType::Timestamp => Value::from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            ),
            DBObjectType::Random => {
                let mut bytes = vec![0u8; self.size.saturating_mul(8)];

                let rand_pct = self.rand_pct.min(100);
                if self.size == 0 {
                    return Value::Blob(bytes);
                }

                if rand_pct < 100 {
                    let n_zeros = (self.size * (100 - rand_pct as usize)) / 100;
                    let n_rands = self.size - n_zeros;

                    let mut offset = 0usize;
                    for _ in 0..n_zeros {
                        offset = write_bytes(0, &mut bytes, offset);
                    }
                    for _ in 0..n_rands {
                        let l = rng.gen::<i64>() as u64;
                        offset = write_bytes(l, &mut bytes, offset);
                    }
                } else {
                    let mut offset = 0usize;
                    while offset < bytes.len() {
                        let l = rng.gen::<i64>() as u64;
                        offset = write_bytes(l, &mut bytes, offset);
                    }
                }
                Value::Blob(bytes)
            }
        }
    }
}

fn write_bytes(mut l: u64, bytes: &mut [u8], offset: usize) -> usize {
    let end = offset + 8;
    if end > bytes.len() {
        return bytes.len();
    }
    for i in (offset..end).rev() {
        bytes[i] = (l & 0xFF) as u8;
        l >>= 8
    }
    end
}

fn parse_size(s: &str) -> Result<usize, String> {
    let value: u64 = s
        .trim()
        .parse()
        .map_err(|e| format!("Invalid number '{}': {}", s, e))?;

    if value == 0 {
        Err(format!("Invalid number '{}': must be > 0", s))
    } else {
        Ok(value as usize)
    }
}

pub fn parse_object_spec_list(s: &str) -> Result<Vec<DBObjectSpec>, String> {
    let mut parsed_specs = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        parsed_specs.push(DBObjectSpec::from_str(part)?);
    }
    if parsed_specs.is_empty() {
        return Err("Object spec is empty".into());
    }
    Ok(parsed_specs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aerospike::Value;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn seeded_rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    #[test]
    fn gen_value_integer_with_key_seed() {
        let spec: DBObjectSpec = "I".parse().unwrap();
        let mut rng = seeded_rng();
        let v = spec.gen_value(&mut rng, Some(12345));
        assert_eq!(v, Value::Int(12345));
    }

    #[test]
    fn gen_value_integer_without_key_seed() {
        let spec: DBObjectSpec = "I".parse().unwrap();
        let mut rng = seeded_rng();
        let v = spec.gen_value(&mut rng, None);
        if let Value::Int(n) = v {
            assert!(n >= 0 && n <= i64::MAX, "integer should be in 0..=i64::MAX");
        } else {
            panic!("expected Value::Int, got {:?}", v);
        }
    }

    #[test]
    fn gen_value_bytes_length() {
        let spec: DBObjectSpec = "B:10".parse().unwrap();
        let mut rng = seeded_rng();
        let v = spec.gen_value(&mut rng, None);
        if let Value::Blob(b) = v {
            assert_eq!(b.len(), 10);
        } else {
            panic!("expected Value::Blob, got {:?}", v);
        }
    }

    #[test]
    fn gen_value_bytes_deterministic_with_seed() {
        let spec: DBObjectSpec = "B:4".parse().unwrap();
        let mut rng1 = StdRng::seed_from_u64(1);
        let mut rng2 = StdRng::seed_from_u64(1);
        let v1 = spec.gen_value(&mut rng1, None);
        let v2 = spec.gen_value(&mut rng2, None);
        assert_eq!(v1, v2);
    }

    #[test]
    fn gen_value_string_length_and_printable() {
        let spec: DBObjectSpec = "S:10".parse().unwrap();
        let mut rng = seeded_rng();
        let v = spec.gen_value(&mut rng, None);
        if let Value::String(s) = v {
            assert_eq!(s.len(), 10);
            for c in s.chars() {
                let code = c as u32;
                assert!(
                    code >= 33 && code <= 126,
                    "char {:?} not in printable ASCII",
                    c
                );
            }
        } else {
            panic!("expected Value::String, got {:?}", v);
        }
    }

    #[test]
    fn gen_value_timestamp_is_int_in_reasonable_range() {
        let spec: DBObjectSpec = "D".parse().unwrap();
        let mut rng = seeded_rng();
        let v = spec.gen_value(&mut rng, None);
        if let Value::Int(ms) = v {
            // Roughly 2020–2040 in ms
            assert!(ms >= 1_570_000_000_000 && ms <= 2_500_000_000_000);
        } else {
            panic!("expected Value::Int (timestamp), got {:?}", v);
        }
    }

    #[test]
    fn gen_value_random_zero_rand_pct() {
        // R:16:0 -> 16 bytes total, 0% random -> all zeros
        let spec: DBObjectSpec = "R:16:0".parse().unwrap();
        let mut rng = seeded_rng();
        let v = spec.gen_value(&mut rng, None);
        if let Value::Blob(b) = v {
            assert_eq!(b.len(), 16);
            assert!(b.iter().all(|&x| x == 0), "expected all zeros");
        } else {
            panic!("expected Value::Blob, got {:?}", v);
        }
    }

    #[test]
    fn gen_value_random_full_rand_deterministic() {
        let spec: DBObjectSpec = "R:8:100".parse().unwrap();
        let mut rng1 = StdRng::seed_from_u64(99);
        let mut rng2 = StdRng::seed_from_u64(99);
        let v1 = spec.gen_value(&mut rng1, None);
        let v2 = spec.gen_value(&mut rng2, None);
        assert_eq!(v1, v2);
        if let Value::Blob(b) = v1 {
            assert_eq!(b.len(), 64); // 8 chunks * 8 bytes
        }
    }

    #[test]
    fn gen_value_random_mixed_rand_pct_size() {
        let spec: DBObjectSpec = "R:24:50".parse().unwrap();
        let mut rng = seeded_rng();
        let v = spec.gen_value(&mut rng, None);
        if let Value::Blob(b) = v {
            assert_eq!(b.len(), 24);
        } else {
            panic!("expected Value::Blob, got {:?}", v);
        }
    }
}
