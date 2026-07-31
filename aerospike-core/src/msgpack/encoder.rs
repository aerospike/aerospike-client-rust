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

use std::collections::{BTreeMap, HashMap};
use std::num::Wrapping;

use crate::commands::buffer::Buffer;
use crate::commands::ParticleType;
use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::CdtContext;
use crate::operations::maps::MapOrder;
use crate::value::{FloatValue, Value};
use crate::vector::Vector;
use crate::{Error, Result};

pub fn pack_value(buf: &mut Option<&mut Buffer>, val: &Value) -> Result<usize> {
    let res = match *val {
        Value::Nil => pack_nil(buf),
        Value::Int(ref val) => pack_integer(buf, *val),
        Value::Bool(ref val) => pack_bool(buf, *val),
        Value::String(ref val) => pack_string(buf, val),
        Value::Float(ref val) => match *val {
            FloatValue::F64(_) => pack_f64(buf, f64::from(val)),
            FloatValue::F32(_) => pack_f32(buf, f32::from(val)),
        },
        Value::Blob(ref val) => pack_blob(buf, val),
        Value::HLL(ref val) => pack_hll(buf, val),
        Value::Vector(ref val) => pack_vector(buf, val),
        Value::List(ref val) => pack_array(buf, val)?,
        Value::HashMap(ref val) => pack_map(buf, val)?,
        Value::OrderedMap(ref val) => pack_index_map(buf, val)?,
        Value::SortedMap(ref val) => pack_ordered_map(buf, val)?,
        Value::MultiResult(_) => {
            return Err(Error::invalid_argument(
                "Multi results are not supported in this encoder.",
            ))
        }
        Value::KeyValueList(_) => {
            return Err(Error::invalid_argument(
                "KeyValue lists are not supported in this encoder.",
            ))
        }
        Value::Unknown(particle_type, _) => {
            return Err(Error::invalid_argument(format!(
                "Unknown values (particle type {particle_type}) hold data this client \
                 cannot interpret and cannot be written back to the server."
            )))
        }
        Value::GeoJSON(ref val) => pack_geo_json(buf, val),
        Value::Infinity => pack_infinity(buf),
        Value::Wildcard => pack_wildcard(buf),
    };

    Ok(res)
}

/// Packs a value like [`pack_value`], but any unordered map (`HashMap` /
/// `OrderedMap`) is packed with its entries sorted in the server's canonical
/// key order and a plain (no order-flag ext header) map header. Servers with
/// AER-6930 (8.1.2.3+) require map value literals in expressions to be in
/// canonical form; without it whole-map comparisons silently fail. Lists and
/// sorted maps recurse so nested unordered maps are canonicalized too
/// (mirrors Java `Packer.sortMaps`).
pub fn pack_value_canonical(buf: &mut Option<&mut Buffer>, val: &Value) -> Result<usize> {
    match *val {
        Value::List(ref list) => {
            let mut size = pack_array_begin(buf, list.len());
            for item in list {
                size += pack_value_canonical(buf, item)?;
            }
            Ok(size)
        }
        Value::HashMap(ref map) => pack_entries_canonical(buf, map.iter()),
        Value::OrderedMap(ref map) => pack_entries_canonical(buf, map.iter()),
        Value::SortedMap(ref map) => {
            let mut size = pack_map_begin(buf, map.len(), MapOrder::KeyOrdered);
            for (key, value) in map {
                size += pack_value_canonical(buf, key)?;
                size += pack_value_canonical(buf, value)?;
            }
            Ok(size)
        }
        _ => pack_value(buf, val),
    }
}

fn pack_entries_canonical<'a>(
    buf: &mut Option<&mut Buffer>,
    entries: impl Iterator<Item = (&'a Value, &'a Value)>,
) -> Result<usize> {
    let mut entries: Vec<(&Value, &Value)> = entries.collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let mut size = pack_map_header(buf, entries.len());
    for (key, value) in entries {
        size += pack_value_canonical(buf, key)?;
        size += pack_value_canonical(buf, value)?;
    }
    Ok(size)
}

pub fn pack_empty_args_array(buf: &mut Option<&mut Buffer>) -> usize {
    let mut size = 0;
    size += pack_array_begin(buf, 0);

    size
}

pub fn pack_ctx_for_index(buf: &mut Option<&mut Buffer>, ctx: &[CdtContext]) -> Result<usize> {
    let mut size: usize = 0;
    size += pack_array_begin(buf, ctx.len() * 2);

    for c in ctx {
        size += pack_integer(buf, i64::from(c.id));
        if let Some(ref exp) = c.expression {
            size += exp.pack_binary(buf)?;
        } else {
            size += pack_value(buf, &c.value)?;
        }
    }

    Ok(size)
}

pub fn pack_cdt_op(
    buf: &mut Option<&mut Buffer>,
    cdt_op: &CdtOperation,
    ctx: &[CdtContext],
) -> Result<usize> {
    let mut size: usize = 0;
    if !ctx.is_empty() {
        size += pack_array_begin(buf, 3);
        size += pack_integer(buf, 0xff);
        size += pack_array_begin(buf, ctx.len() * 2);

        for c in ctx {
            if c.id == 0 {
                size += pack_integer(buf, i64::from(c.id));
            } else {
                size += pack_integer(buf, i64::from(c.id | u16::from(c.flags)));
            }
            if let Some(ref exp) = c.expression {
                size += exp.pack_binary(buf)?;
            } else {
                size += pack_value(buf, &c.value)?;
            }
        }
    }

    size += pack_array_begin(buf, cdt_op.args.len() + 1);
    size += pack_integer(buf, i64::from(cdt_op.op));

    if !cdt_op.args.is_empty() {
        for arg in &cdt_op.args {
            size += match arg {
                CdtArgument::Byte(byte) => pack_value(buf, &Value::from(byte)),
                CdtArgument::Int(int) => pack_value(buf, &Value::from(int)),
                CdtArgument::Value(value) => pack_value(buf, value),
                CdtArgument::List(list) => pack_array(buf, list),
                CdtArgument::Map(map) => pack_map(buf, map),
                CdtArgument::OrderedMap(map) => pack_index_map(buf, map),
                CdtArgument::SortedMap(map) => pack_ordered_map(buf, map),
                CdtArgument::Bool(bool_val) => pack_value(buf, &Value::from(bool_val)),
            }?;
        }
    }

    Ok(size)
}

/// Encoder for CDT `SET_TYPE` create operations. The arguments must be
/// `[CdtArgument::Byte(order_flag), CdtArgument::Byte(attributes)]`. The
/// order flag is not sent as an op argument but OR'd into the last context
/// element's id, which is how the server is told to create the container at
/// that context position with the given order. With an empty context the
/// flag is dropped and the encoding degenerates to a plain top-level
/// set-order op. The persisted-index bit (0x10) on the attributes byte is
/// stripped when a context is present, since persisted indexes only apply
/// to top-level containers.
pub fn pack_cdt_create_op(
    buf: &mut Option<&mut Buffer>,
    cdt_op: &CdtOperation,
    ctx: &[CdtContext],
) -> Result<usize> {
    let (order_flag, args) = match cdt_op.args.split_first() {
        Some((&CdtArgument::Byte(flag), rest)) => (flag, rest),
        _ => {
            return Err(Error::invalid_argument(
                "CDT create op requires a leading order-flag byte argument",
            ))
        }
    };

    let mut size: usize = 0;
    if !ctx.is_empty() {
        size += pack_array_begin(buf, 3);
        size += pack_integer(buf, 0xff);
        size += pack_array_begin(buf, ctx.len() * 2);

        let last = ctx.len() - 1;
        for (i, c) in ctx.iter().enumerate() {
            let mut flags = c.flags;
            if i == last {
                flags |= order_flag;
            }
            if c.id == 0 && flags == 0 {
                size += pack_integer(buf, i64::from(c.id));
            } else {
                size += pack_integer(buf, i64::from(c.id | u16::from(flags)));
            }
            if let Some(ref exp) = c.expression {
                size += exp.pack_binary(buf)?;
            } else {
                size += pack_value(buf, &c.value)?;
            }
        }
    }

    size += pack_array_begin(buf, args.len() + 1);
    size += pack_integer(buf, i64::from(cdt_op.op));

    for (i, arg) in args.iter().enumerate() {
        size += match arg {
            CdtArgument::Byte(byte) => {
                let byte = if i == 0 && !ctx.is_empty() {
                    *byte & !0x10
                } else {
                    *byte
                };
                pack_value(buf, &Value::from(byte))
            }
            CdtArgument::Int(int) => pack_value(buf, &Value::from(int)),
            CdtArgument::Value(value) => pack_value(buf, value),
            CdtArgument::List(list) => pack_array(buf, list),
            CdtArgument::Map(map) => pack_map(buf, map),
            CdtArgument::OrderedMap(map) => pack_index_map(buf, map),
            CdtArgument::SortedMap(map) => pack_ordered_map(buf, map),
            CdtArgument::Bool(bool_val) => pack_value(buf, &Value::from(bool_val)),
        }?;
    }

    Ok(size)
}

pub fn pack_hll_op(
    buf: &mut Option<&mut Buffer>,
    hll_op: &CdtOperation,
    _ctx: &[CdtContext],
) -> Result<usize> {
    let mut size: usize = 0;
    size += pack_array_begin(buf, hll_op.args.len() + 1);
    size += pack_integer(buf, i64::from(hll_op.op));
    if !hll_op.args.is_empty() {
        for arg in &hll_op.args {
            size += match arg {
                CdtArgument::Byte(byte) => pack_value(buf, &Value::from(byte)),
                CdtArgument::Int(int) => pack_value(buf, &Value::from(int)),
                CdtArgument::Value(value) => pack_value(buf, value),
                CdtArgument::List(list) => pack_array(buf, list),
                CdtArgument::Map(map) => pack_map(buf, map),
                CdtArgument::OrderedMap(map) => pack_index_map(buf, map),
                CdtArgument::SortedMap(map) => pack_ordered_map(buf, map),
                CdtArgument::Bool(bool_val) => pack_value(buf, &Value::from(bool_val)),
            }?;
        }
    }
    Ok(size)
}

pub fn pack_cdt_bit_op(
    buf: &mut Option<&mut Buffer>,
    cdt_op: &CdtOperation,
    ctx: &[CdtContext],
) -> Result<usize> {
    let mut size: usize = 0;
    if !ctx.is_empty() {
        size += pack_array_begin(buf, 3);
        size += pack_integer(buf, 0xff);
        size += pack_array_begin(buf, ctx.len() * 2);

        for c in ctx {
            if c.id == 0 {
                size += pack_integer(buf, i64::from(c.id));
            } else {
                size += pack_integer(buf, i64::from(c.id | u16::from(c.flags)));
            }
            if let Some(ref exp) = c.expression {
                size += exp.pack_binary(buf)?;
            } else {
                size += pack_value(buf, &c.value)?;
            }
        }
    }

    size += pack_array_begin(buf, cdt_op.args.len() + 1);
    size += pack_integer(buf, i64::from(cdt_op.op));

    if !cdt_op.args.is_empty() {
        for arg in &cdt_op.args {
            size += match arg {
                CdtArgument::Byte(byte) => pack_value(buf, &Value::from(byte)),
                CdtArgument::Int(int) => pack_value(buf, &Value::from(int)),
                CdtArgument::Value(value) => pack_value(buf, value),
                CdtArgument::List(list) => pack_array(buf, list),
                CdtArgument::Map(map) => pack_map(buf, map),
                CdtArgument::OrderedMap(map) => pack_index_map(buf, map),
                CdtArgument::SortedMap(map) => pack_ordered_map(buf, map),
                CdtArgument::Bool(bool_val) => pack_value(buf, &Value::from(bool_val)),
            }?;
        }
    }
    Ok(size)
}

pub fn pack_array(buf: &mut Option<&mut Buffer>, values: &[Value]) -> Result<usize> {
    let mut size = 0;

    size += pack_array_begin(buf, values.len());
    for val in values {
        size += pack_value(buf, val)?;
    }

    Ok(size)
}

pub fn pack_map(buf: &mut Option<&mut Buffer>, map: &HashMap<Value, Value>) -> Result<usize> {
    let mut size = 0;

    size += pack_map_begin(buf, map.len(), MapOrder::Unordered);
    for (key, val) in map {
        size += pack_value(buf, key)?;
        size += pack_value(buf, val)?;
    }

    Ok(size)
}

/// Pack an insertion-ordered map. The wire representation is an
/// *unordered* Aerospike map (the server has no insertion-ordered map
/// type), but the pairs are written in the map's insertion order, which
/// makes the encoding deterministic.
pub fn pack_index_map(
    buf: &mut Option<&mut Buffer>,
    map: &indexmap::IndexMap<Value, Value>,
) -> Result<usize> {
    let mut size = 0;

    size += pack_map_begin(buf, map.len(), MapOrder::Unordered);
    for (key, val) in map {
        size += pack_value(buf, key)?;
        size += pack_value(buf, val)?;
    }

    Ok(size)
}

pub fn pack_ordered_map(
    buf: &mut Option<&mut Buffer>,
    map: &BTreeMap<Value, Value>,
) -> Result<usize> {
    let mut size = 0;

    size += pack_map_begin(buf, map.len(), MapOrder::KeyOrdered);
    for (key, val) in map {
        size += pack_value(buf, key)?;
        size += pack_value(buf, val)?;
    }

    Ok(size)
}

pub fn pack_infinity(buf: &mut Option<&mut Buffer>) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(0xd4);
        buf.write_u8(0xff);
        buf.write_u8(0x01);
    }
    3
}

pub fn pack_wildcard(buf: &mut Option<&mut Buffer>) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(0xd4);
        buf.write_u8(0xff);
        buf.write_u8(0x00);
    }
    3
}

const MSGPACK_MARKER_NIL: u8 = 0xc0;
const MSGPACK_MARKER_BOOL_TRUE: u8 = 0xc3;
const MSGPACK_MARKER_BOOL_FALSE: u8 = 0xc2;

const MSGPACK_MARKER_U8: u8 = 0xcc;
const MSGPACK_MARKER_U16: u8 = 0xcd;
const MSGPACK_MARKER_U32: u8 = 0xce;
const MSGPACK_MARKER_U64: u8 = 0xcf;

const MSGPACK_MARKER_I8: u8 = 0xd0;
const MSGPACK_MARKER_I16: u8 = 0xd1;
const MSGPACK_MARKER_I32: u8 = 0xd2;
const MSGPACK_MARKER_I64: u8 = 0xd3;

pub fn pack_half_byte(buf: &mut Option<&mut Buffer>, value: u8) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(value);
    }
    1
}

pub fn pack_byte(buf: &mut Option<&mut Buffer>, value: u8) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(value);
    }
    1
}

pub fn pack_nil(buf: &mut Option<&mut Buffer>) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(MSGPACK_MARKER_NIL);
    }
    1
}

pub fn pack_bool(buf: &mut Option<&mut Buffer>, value: bool) -> usize {
    if let Some(ref mut buf) = *buf {
        if value {
            buf.write_u8(MSGPACK_MARKER_BOOL_TRUE);
        } else {
            buf.write_u8(MSGPACK_MARKER_BOOL_FALSE);
        }
    }
    1
}

pub fn pack_map_begin(buf: &mut Option<&mut Buffer>, length: usize, order: MapOrder) -> usize {
    match order {
        MapOrder::Unordered => pack_map_header(buf, length),
        MapOrder::KeyOrdered => {
            let mut size = pack_map_header(buf, length + 1);
            size += pack_byte(buf, 0xc7);
            size += pack_byte(buf, 0);
            size += pack_byte(buf, order as u8);
            size += pack_byte(buf, 0xc0);
            size
        }
        MapOrder::KeyValueOrdered => unreachable!(),
    }
}

pub fn pack_map_header(buf: &mut Option<&mut Buffer>, length: usize) -> usize {
    if length < 16 {
        pack_half_byte(buf, 0x80 | (length as u8))
    } else if length < 1 << 16 {
        pack_type_u16(buf, 0xde, length as u16)
    } else {
        pack_type_u32(buf, 0xdf, length as u32)
    }
}

pub fn pack_array_begin(buf: &mut Option<&mut Buffer>, length: usize) -> usize {
    if length < 16 {
        pack_half_byte(buf, 0x90 | (length as u8))
    } else if length < 1 << 16 {
        pack_type_u16(buf, 0xdc, length as u16)
    } else {
        pack_type_u32(buf, 0xdd, length as u32)
    }
}

pub fn pack_string_begin(buf: &mut Option<&mut Buffer>, length: usize) -> usize {
    if length < 32 {
        pack_half_byte(buf, 0xa0 | (length as u8))
    } else if length < 256 {
        pack_type_u8(buf, 0xd9, length as u8)
    } else if length < 1 << 16 {
        pack_type_u16(buf, 0xda, length as u16)
    } else {
        pack_type_u32(buf, 0xdb, length as u32)
    }
}

pub fn pack_blob(buf: &mut Option<&mut Buffer>, value: &[u8]) -> usize {
    let mut size = value.len() + 1;

    size += pack_string_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_u8(ParticleType::BLOB as u8);
        buf.write_bytes(value);
    }

    size
}

pub fn pack_hll(buf: &mut Option<&mut Buffer>, value: &[u8]) -> usize {
    let mut size = value.len() + 1;

    size += pack_string_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_u8(ParticleType::HLL as u8);
        buf.write_bytes(value);
    }

    size
}

/// Pack a vector nested in a CDT or expression: a msgpack byte string tagged
/// with the `VECTOR` particle type (like [`pack_hll`]), payload = its wire form.
pub fn pack_vector(buf: &mut Option<&mut Buffer>, value: &Vector) -> usize {
    let mut size = value.wire_size() + 1;

    size += pack_string_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_u8(ParticleType::VECTOR as u8);
        value.write_to(buf);
    }

    size
}

pub fn pack_string(buf: &mut Option<&mut Buffer>, value: &str) -> usize {
    let mut size = value.len() + 1;

    size += pack_string_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_u8(ParticleType::STRING as u8);
        buf.write_str(value);
    }

    size
}

pub fn pack_raw_string(buf: &mut Option<&mut Buffer>, value: &str) -> usize {
    let mut size = value.len();

    size += pack_string_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_str(value);
    }

    size
}

pub fn pack_geo_json(buf: &mut Option<&mut Buffer>, value: &str) -> usize {
    let mut size = value.len() + 1;

    size += pack_string_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_u8(ParticleType::GEOJSON as u8);
        buf.write_str(value);
    }

    size
}

pub fn pack_integer(buf: &mut Option<&mut Buffer>, value: i64) -> usize {
    if value >= 0 {
        pack_u64(buf, value as u64)
    } else if value >= -32 {
        pack_half_byte(buf, 0xe0 | ((Wrapping(value as u8) + Wrapping(32)).0))
    } else if value >= i64::from(i8::MIN) {
        if let Some(ref mut buf) = *buf {
            buf.write_u8(MSGPACK_MARKER_I8);
            buf.write_i8(value as i8);
        }
        2
    } else if value >= i64::from(i16::MIN) {
        if let Some(ref mut buf) = *buf {
            buf.write_u8(MSGPACK_MARKER_I16);
            buf.write_i16(value as i16);
        }
        3
    } else if value >= i64::from(i32::MIN) {
        if let Some(ref mut buf) = *buf {
            buf.write_u8(MSGPACK_MARKER_I32);
            buf.write_i32(value as i32);
        }
        5
    } else {
        if let Some(ref mut buf) = *buf {
            buf.write_u8(MSGPACK_MARKER_I64);
            buf.write_i64(value);
        }
        9
    }
}

pub fn pack_type_u8(buf: &mut Option<&mut Buffer>, marker: u8, value: u8) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(marker);
        buf.write_u8(value);
    }
    2
}

pub fn pack_type_u16(buf: &mut Option<&mut Buffer>, marker: u8, value: u16) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(marker);
        buf.write_u16(value);
    }
    3
}

pub fn pack_type_u32(buf: &mut Option<&mut Buffer>, marker: u8, value: u32) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(marker);
        buf.write_u32(value);
    }
    5
}

pub fn pack_u64(buf: &mut Option<&mut Buffer>, value: u64) -> usize {
    if value < (1 << 7) {
        pack_half_byte(buf, value as u8)
    } else if value <= u64::from(u8::MAX) {
        if let Some(ref mut buf) = *buf {
            buf.write_u8(MSGPACK_MARKER_U8);
            buf.write_u8(value as u8);
        }
        2
    } else if value <= u64::from(u16::MAX) {
        pack_type_u16(buf, MSGPACK_MARKER_U16, value as u16)
    } else if value <= u64::from(u32::MAX) {
        pack_type_u32(buf, MSGPACK_MARKER_U32, value as u32)
    } else {
        if let Some(ref mut buf) = *buf {
            buf.write_u8(MSGPACK_MARKER_U64);
            buf.write_u64(value);
        }
        9
    }
}

pub fn pack_f32(buf: &mut Option<&mut Buffer>, value: f32) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(0xca);
        buf.write_f32(value);
    }
    5
}

pub fn pack_f64(buf: &mut Option<&mut Buffer>, value: f64) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(0xcb);
        buf.write_f64(value);
    }
    9
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::operations::cdt_context::{ctx_list_index, ctx_map_key};
    use crate::operations::lists::CdtListOpType;
    use crate::operations::maps::CdtMapOpType;

    // Packs a CDT create op both in estimate mode (no buffer) and write
    // mode, asserts the two sizes agree, and returns the written bytes.
    fn pack_create(op: &CdtOperation, ctx: &[CdtContext]) -> Vec<u8> {
        let estimated = pack_cdt_create_op(&mut None, op, ctx).unwrap();
        let mut buffer = Buffer::new(usize::MAX);
        buffer.resize_buffer(estimated).unwrap();
        buffer.data_offset = 0;
        let written = pack_cdt_create_op(&mut Some(&mut buffer), op, ctx).unwrap();
        assert_eq!(estimated, written);
        buffer.data_buffer[..written].to_vec()
    }

    fn create_op(cmd: u8, flag: u8, attributes: u8) -> CdtOperation {
        CdtOperation {
            op: cmd,
            encoder: Arc::new(pack_cdt_create_op),
            args: vec![CdtArgument::Byte(flag), CdtArgument::Byte(attributes)],
        }
    }

    // Java: CDT.init(packer, ctx, SET_TYPE, 1, order.flag); packInt(order.attributes)
    // KeyOrdered under mapKey("k"): last ctx id = 0x22 | 0x80 = 0xa2,
    // single attributes argument.
    #[test]
    fn map_create_flag_merges_into_last_ctx_element() {
        let op = create_op(CdtMapOpType::SetType as u8, 0x80, 1);
        let ctx = [ctx_map_key(Value::from("k"))];
        assert_eq!(
            pack_create(&op, &ctx),
            vec![
                0x93, // array(3): CONTEXT_EVAL wrapper
                0xcc, 0xff, // CONTEXT_EVAL opcode
                0x92, // array(2): one ctx pair
                0xcc, 0xa2, // MapKey (0x22) | KeyOrdered create flag (0x80)
                0xa2, 0x03, 0x6b, // "k" as Aerospike string (particle prefix)
                0x92, // array(2): command + 1 arg
                0x40, // map SET_TYPE (64)
                0x01, // attributes: KeyOrdered
            ]
        );
    }

    // With no ctx the flag argument is dropped: plain top-level set-order.
    #[test]
    fn map_create_without_ctx_degenerates_to_set_order() {
        let op = create_op(CdtMapOpType::SetType as u8, 0x80, 1);
        assert_eq!(pack_create(&op, &[]), vec![0x92, 0x40, 0x01]);
    }

    // Nested list create: flag merges into ctx, persist-index bit (0x10)
    // is stripped from the attributes since it only applies at top level.
    #[test]
    fn list_create_nested_strips_persist_index_bit() {
        let op = create_op(CdtListOpType::SetType as u8, 0xc0, 1 | 0x10);
        let ctx = [ctx_list_index(3)];
        assert_eq!(
            pack_create(&op, &ctx),
            vec![
                0x93, // array(3): CONTEXT_EVAL wrapper
                0xcc, 0xff, // CONTEXT_EVAL opcode
                0x92, // array(2): one ctx pair
                0xcc, 0xd0, // ListIndex (0x10) | Ordered create flag (0xc0)
                0x03, // index 3
                0x92, // array(2): command + 1 arg
                0x00, // list SET_TYPE (0)
                0x01, // attributes: Ordered, persist bit stripped
            ]
        );
    }

    // Top-level list create keeps the persist-index bit.
    #[test]
    fn list_create_top_level_keeps_persist_index_bit() {
        let op = create_op(CdtListOpType::SetType as u8, 0xc0, 1 | 0x10);
        assert_eq!(pack_create(&op, &[]), vec![0x92, 0x00, 0x11]);
    }

    fn pack_canonical(val: &Value) -> Vec<u8> {
        let estimated = pack_value_canonical(&mut None, val).unwrap();
        let mut buffer = Buffer::new(usize::MAX);
        buffer.resize_buffer(estimated).unwrap();
        buffer.data_offset = 0;
        let written = pack_value_canonical(&mut Some(&mut buffer), val).unwrap();
        assert_eq!(estimated, written);
        buffer.data_buffer[..written].to_vec()
    }

    // {b:2, a:1} packed canonically: entries key-sorted, PLAIN map header
    // (no order-flag ext), matching Java Packer.sortMaps / AER-6930.
    #[test]
    fn canonical_pack_sorts_unordered_maps_without_ext_header() {
        let expected = vec![
            0x82, // map(2), plain header
            0xa2, 0x03, 0x61, 0x01, // "a": 1
            0xa2, 0x03, 0x62, 0x02, // "b": 2
        ];

        let mut hash = std::collections::HashMap::new();
        hash.insert(Value::from("b"), Value::from(2));
        hash.insert(Value::from("a"), Value::from(1));
        assert_eq!(pack_canonical(&Value::HashMap(hash)), expected);

        // IndexMap in reverse-insertion order canonicalizes identically.
        let mut index = indexmap::IndexMap::new();
        index.insert(Value::from("b"), Value::from(2));
        index.insert(Value::from("a"), Value::from(1));
        assert_eq!(pack_canonical(&Value::OrderedMap(index)), expected);
    }

    // Unordered maps nested inside lists and sorted maps canonicalize too;
    // the sorted-map wrapper keeps its K-ordered ext header.
    #[test]
    fn canonical_pack_recurses_into_nested_containers() {
        let mut inner = std::collections::HashMap::new();
        inner.insert(Value::from("b"), Value::from(2));
        inner.insert(Value::from("a"), Value::from(1));
        let listed = Value::List(vec![Value::HashMap(inner.clone())]);
        assert_eq!(
            pack_canonical(&listed),
            vec![0x91, 0x82, 0xa2, 0x03, 0x61, 0x01, 0xa2, 0x03, 0x62, 0x02]
        );

        let mut sorted = BTreeMap::new();
        sorted.insert(Value::from("k"), Value::HashMap(inner));
        assert_eq!(
            pack_canonical(&Value::SortedMap(sorted)),
            vec![
                0x82, // map(1+1): K-ordered header
                0xc7, 0x00, 0x01, 0xc0, // order-flag ext pair
                0xa2, 0x03, 0x6b, // "k"
                0x82, 0xa2, 0x03, 0x61, 0x01, 0xa2, 0x03, 0x62, 0x02, // canonical inner
            ]
        );
    }
}
