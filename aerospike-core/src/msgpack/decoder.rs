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

use std::collections::BTreeMap;

use indexmap::IndexMap;
use std::vec::Vec;

use crate::commands::buffer::Buffer;
use crate::commands::ParticleType;
use crate::errors::{Error, Result};
use crate::operations::MapOrder;
use crate::value::Value;

pub fn unpack_value_list(buf: &mut Buffer) -> Result<Value> {
    if buf.data_buffer.is_empty() {
        return Ok(Value::List(vec![]));
    }

    buf.ensure(1)?;
    let ltype: u8 = buf.read_u8(None);

    let count: usize = match ltype {
        0x90..=0x9f => (ltype & 0x0f) as usize,
        0xdc => {
            buf.ensure(2)?;
            buf.read_u16(None) as usize
        }
        0xdd => {
            buf.ensure(4)?;
            buf.read_u32(None) as usize
        }
        other => {
            return Err(Error::bad_response(format!(
                "expected a list marker, found {other:#04x}"
            )))
        }
    };

    unpack_list(buf, count)
}

pub fn unpack_value_map(buf: &mut Buffer) -> Result<Value> {
    if buf.data_buffer.is_empty() {
        return Ok(Value::OrderedMap(IndexMap::new()));
    }

    buf.ensure(1)?;
    let ltype: u8 = buf.read_u8(None);

    let count: usize = match ltype {
        0x80..=0x8f => (ltype & 0x0f) as usize,
        0xde => {
            buf.ensure(2)?;
            buf.read_u16(None) as usize
        }
        0xdf => {
            buf.ensure(4)?;
            buf.read_u32(None) as usize
        }
        other => {
            return Err(Error::bad_response(format!(
                "expected a map marker, found {other:#04x}"
            )))
        }
    };

    unpack_map(buf, count)
}

/// A capacity hint that a hostile count cannot turn into a giant allocation:
/// every element takes at least one byte, so never reserve more slots than
/// there are bytes left.
fn capacity_hint(buf: &Buffer, count: usize) -> usize {
    count.min(buf.remaining())
}

fn unpack_list(buf: &mut Buffer, mut count: usize) -> Result<Value> {
    if count > 0 && buf.remaining() > 0 && is_ext(buf.peek()) {
        unpack_value(buf)?;
        count -= 1;
    }

    let mut list: Vec<Value> = Vec::with_capacity(capacity_hint(buf, count));
    for _ in 0..count {
        let val = unpack_value(buf)?;
        list.push(val);
    }

    Ok(Value::from(list))
}

fn map_order(buf: &Buffer) -> MapOrder {
    if buf.remaining() < 3 {
        return MapOrder::Unordered;
    }
    let map_type = buf.peek();

    if map_type == 0xc7 {
        let extension_type = buf.peek_n(1);
        if extension_type == 0 {
            let map_bits = buf.peek_n(2);

            if map_bits & 0x08 != 0 {
                return MapOrder::KeyValueOrdered;
            } else if map_bits & 0x01 != 0 {
                return MapOrder::KeyOrdered;
            }
        }
    }
    MapOrder::Unordered
}

/// The server only ever keys maps by integers, strings and bytes (plus the
/// nil placeholder); anything else in key position is a corrupt reply. The
/// `Hash` and `Ord` impls on `Value` reject the other variants by panicking,
/// so this check has to come first.
fn check_map_key(key: &Value) -> Result<()> {
    match key {
        Value::Nil | Value::Int(_) | Value::String(_) | Value::Blob(_) => Ok(()),
        other => Err(Error::bad_response(format!(
            "map key of type {} is not valid",
            other.type_label()
        ))),
    }
}

fn unpack_map(buf: &mut Buffer, mut count: usize) -> Result<Value> {
    let mut order = MapOrder::Unordered;
    if count > 0 && buf.remaining() > 0 && is_ext(buf.peek()) {
        order = map_order(buf);

        unpack_value(buf)?;
        unpack_value(buf)?;
        count -= 1;
    }

    match order {
        MapOrder::Unordered => {
            // Decode into an insertion-ordered map so the pair order the
            // server sent is what the caller sees.
            let mut map: IndexMap<Value, Value> =
                IndexMap::with_capacity(capacity_hint(buf, count));
            for _ in 0..count {
                let key = unpack_value(buf)?;
                check_map_key(&key)?;
                let val = unpack_value(buf)?;
                map.insert(key, val);
            }

            Ok(Value::OrderedMap(map))
        }
        MapOrder::KeyOrdered => {
            let mut map: BTreeMap<Value, Value> = BTreeMap::new();
            for _ in 0..count {
                let key = unpack_value(buf)?;
                check_map_key(&key)?;
                let val = unpack_value(buf)?;
                map.insert(key, val);
            }

            Ok(Value::SortedMap(map))
        }
        MapOrder::KeyValueOrdered => {
            let mut list: Vec<(Value, Value)> = Vec::with_capacity(capacity_hint(buf, count));
            for _ in 0..count {
                let key = unpack_value(buf)?;
                let val = unpack_value(buf)?;
                list.push((key, val));
            }

            Ok(Value::KeyValueList(list))
        }
    }
}

fn unpack_blob(buf: &mut Buffer, count: usize) -> Result<Value> {
    // The first byte is the particle type; the rest is the payload.
    let count = count
        .checked_sub(1)
        .ok_or_else(|| Error::bad_response("empty blob particle"))?;
    buf.ensure(count + 1)?;
    let vtype = buf.read_u8(None);

    match ParticleType::try_from_u8(vtype) {
        Some(ParticleType::STRING) => {
            let val = buf.read_str(count)?;
            Ok(Value::String(val))
        }

        Some(ParticleType::BLOB) => Ok(Value::Blob(buf.read_blob(count)?)),
        Some(ParticleType::HLL) => Ok(Value::Hll(buf.read_blob(count)?)),

        Some(ParticleType::GEOJSON) => {
            let val = buf.read_str(count)?;
            Ok(Value::GeoJson(val))
        }

        _ => Ok(Value::Unknown(vtype, buf.read_blob(count)?)),
    }
}

/// Skip an ext value of `len` payload bytes plus its one-byte type.
fn skip_ext(buf: &mut Buffer, len: usize) -> Result<Value> {
    let count = len + 1;
    buf.ensure(count)?;
    buf.skip_bytes(count);
    Ok(Value::Nil)
}

pub fn unpack_value(buf: &mut Buffer) -> Result<Value> {
    buf.ensure(1)?;
    let obj_type = buf.read_u8(None);

    match obj_type {
        0x00..=0x7f => Ok(Value::from(obj_type)),
        0x80..=0x8f => unpack_map(buf, (obj_type & 0x0f) as usize),
        0x90..=0x9f => unpack_list(buf, (obj_type & 0x0f) as usize),
        0xa0..=0xbf => unpack_blob(buf, (obj_type & 0x1f) as usize),
        0xc0 => Ok(Value::Nil),
        0xc2 => Ok(Value::from(false)),
        0xc3 => Ok(Value::from(true)),
        0xc4 | 0xd9 => {
            buf.ensure(1)?;
            let count = buf.read_u8(None);
            unpack_blob(buf, count as usize)
        }
        0xc5 | 0xda => {
            buf.ensure(2)?;
            let count = buf.read_u16(None);
            unpack_blob(buf, count as usize)
        }
        0xc6 | 0xdb => {
            buf.ensure(4)?;
            let count = buf.read_u32(None);
            unpack_blob(buf, count as usize)
        }
        0xc7 => {
            buf.ensure(1)?;
            let len = usize::from(buf.read_u8(None));
            skip_ext(buf, len)
        }
        0xc8 => {
            buf.ensure(2)?;
            let len = usize::from(buf.read_u16(None));
            skip_ext(buf, len)
        }
        0xc9 => {
            buf.ensure(4)?;
            let len = buf.read_u32(None) as usize;
            skip_ext(buf, len)
        }
        0xca => {
            buf.ensure(4)?;
            Ok(Value::from(buf.read_f32(None)))
        }
        0xcb => {
            buf.ensure(8)?;
            Ok(Value::from(buf.read_f64(None)))
        }
        0xcc => {
            buf.ensure(1)?;
            Ok(Value::from(buf.read_u8(None)))
        }
        0xcd => {
            buf.ensure(2)?;
            Ok(Value::from(buf.read_u16(None)))
        }
        0xce => {
            buf.ensure(4)?;
            Ok(Value::from(buf.read_u32(None)))
        }
        0xcf => {
            buf.ensure(8)?;
            Ok(buf.read_u64_value(None))
        }
        0xd0 => {
            buf.ensure(1)?;
            Ok(Value::from(buf.read_i8(None)))
        }
        0xd1 => {
            buf.ensure(2)?;
            Ok(Value::from(buf.read_i16(None)))
        }
        0xd2 => {
            buf.ensure(4)?;
            Ok(Value::from(buf.read_i32(None)))
        }
        0xd3 => {
            buf.ensure(8)?;
            Ok(Value::from(buf.read_i64(None)))
        }
        0xd4 => skip_ext(buf, 1),
        0xd5 => skip_ext(buf, 2),
        0xd6 => skip_ext(buf, 4),
        0xd7 => skip_ext(buf, 8),
        0xd8 => skip_ext(buf, 16),
        0xdc => {
            buf.ensure(2)?;
            let count = buf.read_u16(None);
            unpack_list(buf, count as usize)
        }
        0xdd => {
            buf.ensure(4)?;
            let count = buf.read_u32(None);
            unpack_list(buf, count as usize)
        }
        0xde => {
            buf.ensure(2)?;
            let count = buf.read_u16(None);
            unpack_map(buf, count as usize)
        }
        0xdf => {
            buf.ensure(4)?;
            let count = buf.read_u32(None);
            unpack_map(buf, count as usize)
        }
        0xe0..=0xff => {
            let value = i16::from(obj_type) - 0xe0 - 32;
            Ok(Value::from(value))
        }
        _ => Err(Error::bad_response(format!(
            "Error unpacking value of type '{obj_type:x}'"
        ))),
    }
}

const fn is_ext(byte: u8) -> bool {
    matches!(byte, 0xc7 | 0xc8 | 0xc9 | 0xd4 | 0xd5 | 0xd6 | 0xd7 | 0xd8)
}

#[cfg(test)]
mod tests {
    //! Hostile or truncated server data must come back as `BadResponse`,
    //! never as a panic or a giant allocation.
    use super::*;
    use crate::ErrorKind;

    fn buf(bytes: &[u8]) -> Buffer {
        let mut b = Buffer::new(0);
        b.data_buffer = bytes.to_vec();
        b.data_offset = 0;
        b
    }

    fn is_bad_response(r: &Result<Value>) -> bool {
        matches!(r, Err(e) if matches!(e.kind(), ErrorKind::BadResponse))
    }

    #[test]
    fn truncated_scalars_are_errors() {
        // Marker promises 8 bytes of f64, none follow.
        assert!(is_bad_response(&unpack_value(&mut buf(&[0xcb]))));
        // uint32 with two of four bytes.
        assert!(is_bad_response(&unpack_value(&mut buf(&[0xce, 0x00, 0x01]))));
        // Empty input.
        assert!(is_bad_response(&unpack_value(&mut buf(&[]))));
    }

    #[test]
    fn truncated_strings_and_blobs_are_errors() {
        // fixstr of 5 bytes (particle type + 4) with only the type byte.
        assert!(is_bad_response(&unpack_value(&mut buf(&[0xa5, 0x03]))));
        // str32 declaring 4 GiB.
        assert!(is_bad_response(&unpack_value(&mut buf(&[
            0xdb, 0xff, 0xff, 0xff, 0xff, 0x03
        ]))));
        // A zero-length blob has no room for its particle type byte.
        assert!(is_bad_response(&unpack_value(&mut buf(&[0xc4, 0x00]))));
    }

    #[test]
    fn huge_declared_counts_do_not_allocate() {
        // array32 with 4 billion elements and no payload: the capacity hint
        // is bounded by the bytes left, and the first element read fails.
        assert!(is_bad_response(&unpack_value(&mut buf(&[
            0xdd, 0xff, 0xff, 0xff, 0xff
        ]))));
        assert!(is_bad_response(&unpack_value(&mut buf(&[
            0xdf, 0xff, 0xff, 0xff, 0xff
        ]))));
        assert!(is_bad_response(&unpack_value_list(&mut buf(&[
            0xdd, 0xff, 0xff, 0xff, 0xff
        ]))));
    }

    #[test]
    fn unexpected_top_level_markers_are_errors() {
        assert!(is_bad_response(&unpack_value_list(&mut buf(&[0xc0]))));
        assert!(is_bad_response(&unpack_value_map(&mut buf(&[0x01]))));
    }

    #[test]
    fn ext_lengths_at_the_type_maximum_do_not_overflow() {
        // ext8 with length 255: 1 + 255 bytes must be present; they are not.
        assert!(is_bad_response(&unpack_value(&mut buf(&[0xc7, 0xff, 0x00]))));
        // ext16 with length 65535.
        assert!(is_bad_response(&unpack_value(&mut buf(&[0xc8, 0xff, 0xff, 0x00]))));
        // ext32 with length u32::MAX.
        assert!(is_bad_response(&unpack_value(&mut buf(&[
            0xc9, 0xff, 0xff, 0xff, 0xff, 0x00
        ]))));
    }

    #[test]
    fn map_keys_of_unsupported_types_are_rejected() {
        // fixmap(1) { 1.5f64 => 1 }: a float key would panic in `Hash`.
        let mut bytes = vec![0x81, 0xcb];
        bytes.extend_from_slice(&1.5f64.to_be_bytes());
        bytes.push(0x01);
        assert!(is_bad_response(&unpack_value(&mut buf(&bytes))));
        // fixmap(1) { true => 1 }
        assert!(is_bad_response(&unpack_value(&mut buf(&[0x81, 0xc3, 0x01]))));
        // The same key in a key-ordered map: fixmap(2) whose first pair is
        // the ext8 order marker (len 1, type 0, bits 0x01) with a nil value.
        assert!(is_bad_response(&unpack_value(&mut buf(&[
            0x82, 0xc7, 0x01, 0x00, 0x01, 0xc0, 0xc3, 0x01
        ]))));
    }

    #[test]
    fn well_formed_values_still_decode() {
        // fixmap(2) { 1 => "a"(particle 3), 2 => [1, 2] }
        let v = unpack_value(&mut buf(&[
            0x82, 0x01, 0xa2, 0x03, b'a', 0x02, 0x92, 0x01, 0x02,
        ]))
        .unwrap();
        let Value::OrderedMap(m) = v else {
            panic!("expected an ordered map, got {v:?}")
        };
        assert_eq!(m.get(&Value::Int(1)), Some(&Value::String("a".into())));
        assert_eq!(
            m.get(&Value::Int(2)),
            Some(&Value::List(vec![Value::Int(1), Value::Int(2)]))
        );
    }
}
