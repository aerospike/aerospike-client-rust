// Copyright 2015-2020 Aerospike, Inc.
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

use std::collections::HashMap;
use std::num::Wrapping;
use std::{i16, i32, i64, i8};

use crate::commands::buffer::Buffer;
use crate::commands::ParticleType;
use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::CdtContext;
use crate::value::{FloatValue, Value};

#[doc(hidden)]
pub fn pack_value(buf: &mut Option<&mut Buffer>, val: &Value) -> usize {
    match *val {
        Value::Nil => pack_nil(buf),
        Value::Int(ref val) => pack_integer(buf, *val),
        Value::UInt(ref val) => pack_u64(buf, *val),
        Value::Bool(ref val) => pack_bool(buf, *val),
        Value::String(ref val) => pack_string(buf, val),
        Value::Float(ref val) => match *val {
            FloatValue::F64(_) => pack_f64(buf, f64::from(val)),
            FloatValue::F32(_) => pack_f32(buf, f32::from(val)),
        },
        Value::Blob(ref val) | Value::HLL(ref val) => pack_blob(buf, val),
        Value::List(ref val) => pack_array(buf, val),
        Value::HashMap(ref val) => pack_map(buf, val),
        Value::OrderedMap(_) => panic!("Ordered maps are not supported in this encoder."),
        Value::GeoJSON(ref val) => pack_geo_json(buf, val),
    }
}

#[doc(hidden)]
pub fn pack_empty_args_array(buf: &mut Option<&mut Buffer>) -> usize {
    let mut size = 0;
    size += pack_array_begin(buf, 0);

    size
}

#[doc(hidden)]
pub fn pack_cdt_op(
    buf: &mut Option<&mut Buffer>,
    cdt_op: &CdtOperation,
    ctx: &[CdtContext],
) -> usize {
    let mut size: usize = 0;
    if ctx.is_empty() {
        size += pack_raw_u16(buf, u16::from(cdt_op.op));
        if !cdt_op.args.is_empty() {
            size += pack_array_begin(buf, cdt_op.args.len());
        }
    } else {
        size += pack_array_begin(buf, 3);
        size += pack_integer(buf, 0xff);
        size += pack_array_begin(buf, ctx.len() * 2);

        for c in ctx {
            if c.id == 0 {
                size += pack_integer(buf, i64::from(c.id));
            } else {
                size += pack_integer(buf, i64::from(c.id | c.flags));
            }
            size += pack_value(buf, &c.value);
        }

        size += pack_array_begin(buf, cdt_op.args.len() + 1);
        size += pack_integer(buf, i64::from(cdt_op.op));
    }

    if !cdt_op.args.is_empty() {
        for arg in &cdt_op.args {
            size += match *arg {
                CdtArgument::Byte(byte) => pack_value(buf, &Value::from(byte)),
                CdtArgument::Int(int) => pack_value(buf, &Value::from(int)),
                CdtArgument::Value(value) => pack_value(buf, value),
                CdtArgument::List(list) => pack_array(buf, list),
                CdtArgument::Map(map) => pack_map(buf, map),
                CdtArgument::Bool(bool_val) => pack_value(buf, &Value::from(bool_val)),
            }
        }
    }

    size
}

#[doc(hidden)]
pub fn pack_hll_op(
    buf: &mut Option<&mut Buffer>,
    hll_op: &CdtOperation,
    _ctx: &[CdtContext],
) -> usize {
    let mut size: usize = 0;
    size += pack_array_begin(buf, hll_op.args.len() + 1);
    size += pack_integer(buf, i64::from(hll_op.op));
    if !hll_op.args.is_empty() {
        for arg in &hll_op.args {
            size += match *arg {
                CdtArgument::Byte(byte) => pack_value(buf, &Value::from(byte)),
                CdtArgument::Int(int) => pack_value(buf, &Value::from(int)),
                CdtArgument::Value(value) => pack_value(buf, value),
                CdtArgument::List(list) => pack_array(buf, list),
                CdtArgument::Map(map) => pack_map(buf, map),
                CdtArgument::Bool(bool_val) => pack_value(buf, &Value::from(bool_val)),
            }
        }
    }
    size
}

#[doc(hidden)]
pub fn pack_cdt_bit_op(
    buf: &mut Option<&mut Buffer>,
    cdt_op: &CdtOperation,
    ctx: &[CdtContext],
) -> usize {
    let mut size: usize = 0;
    if !ctx.is_empty() {
        size += pack_array_begin(buf, 3);
        size += pack_integer(buf, 0xff);
        size += pack_array_begin(buf, ctx.len() * 2);

        for c in ctx {
            if c.id == 0 {
                size += pack_integer(buf, i64::from(c.id));
            } else {
                size += pack_integer(buf, i64::from(c.id | c.flags));
            }
            size += pack_value(buf, &c.value);
        }
    }

    size += pack_array_begin(buf, cdt_op.args.len() + 1);
    size += pack_integer(buf, i64::from(cdt_op.op));

    if !cdt_op.args.is_empty() {
        for arg in &cdt_op.args {
            size += match *arg {
                CdtArgument::Byte(byte) => pack_value(buf, &Value::from(byte)),
                CdtArgument::Int(int) => pack_value(buf, &Value::from(int)),
                CdtArgument::Value(value) => pack_value(buf, value),
                CdtArgument::List(list) => pack_array(buf, list),
                CdtArgument::Map(map) => pack_map(buf, map),
                CdtArgument::Bool(bool_val) => pack_value(buf, &Value::from(bool_val)),
            }
        }
    }
    size
}

#[doc(hidden)]
pub fn pack_array(buf: &mut Option<&mut Buffer>, values: &[Value]) -> usize {
    let mut size = 0;

    size += pack_array_begin(buf, values.len());
    for val in values {
        size += pack_value(buf, val);
    }

    size
}

#[doc(hidden)]
pub fn pack_map(buf: &mut Option<&mut Buffer>, map: &HashMap<Value, Value>) -> usize {
    let mut size = 0;

    size += pack_map_begin(buf, map.len());
    for (key, val) in map.iter() {
        size += pack_value(buf, key);
        size += pack_value(buf, val);
    }

    size
}

/// ///////////////////////////////////////////////////////////////////

const MSGPACK_MARKER_NIL: u8 = 0xc0;
const MSGPACK_MARKER_BOOL_TRUE: u8 = 0xc3;
const MSGPACK_MARKER_BOOL_FALSE: u8 = 0xc2;

const MSGPACK_MARKER_I8: u8 = 0xcc;
const MSGPACK_MARKER_I16: u8 = 0xcd;
const MSGPACK_MARKER_I32: u8 = 0xce;
// const MSGPACK_MARKER_I64: u8 = 0xd3;

const MSGPACK_MARKER_NI8: u8 = 0xd0;
const MSGPACK_MARKER_NI16: u8 = 0xd1;
const MSGPACK_MARKER_NI32: u8 = 0xd2;
const MSGPACK_MARKER_NI64: u8 = 0xd3;

// This method is not compatible with MsgPack specs and is only used by aerospike client<->server
// for wire transfer only
#[doc(hidden)]
pub fn pack_raw_u16(buf: &mut Option<&mut Buffer>, value: u16) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u16(value);
    }
    2
}

#[doc(hidden)]
pub fn pack_half_byte(buf: &mut Option<&mut Buffer>, value: u8) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(value);
    }
    1
}

#[doc(hidden)]
pub fn pack_byte(buf: &mut Option<&mut Buffer>, marker: u8, value: u8) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(marker);
        buf.write_u8(value);
    }
    2
}

#[doc(hidden)]
pub fn pack_nil(buf: &mut Option<&mut Buffer>) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(MSGPACK_MARKER_NIL);
    }
    1
}

#[doc(hidden)]
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

#[doc(hidden)]
pub fn pack_map_begin(buf: &mut Option<&mut Buffer>, length: usize) -> usize {
    match length {
        val if val < 16 => pack_half_byte(buf, 0x80 | (length as u8)),
        val if (16..(1 << 16)).contains(&val) => pack_i16(buf, 0xde, length as i16),
        _ => pack_i32(buf, 0xdf, length as i32),
    }
}

#[doc(hidden)]
pub fn pack_array_begin(buf: &mut Option<&mut Buffer>, length: usize) -> usize {
    match length {
        val if val < 16 => pack_half_byte(buf, 0x90 | (length as u8)),
        val if (16..(1 << 16)).contains(&val) => pack_i16(buf, 0xdc, length as i16),
        _ => pack_i32(buf, 0xdd, length as i32),
    }
}

#[doc(hidden)]
pub fn pack_byte_array_begin(buf: &mut Option<&mut Buffer>, length: usize) -> usize {
    match length {
        val if val < 32 => pack_half_byte(buf, 0xa0 | (length as u8)),
        val if (32..(1 << 16)).contains(&val) => pack_i16(buf, 0xda, length as i16),
        _ => pack_i32(buf, 0xdb, length as i32),
    }
}

#[doc(hidden)]
pub fn pack_blob(buf: &mut Option<&mut Buffer>, value: &[u8]) -> usize {
    let mut size = value.len() + 1;

    size += pack_byte_array_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_u8(ParticleType::BLOB as u8);
        buf.write_bytes(value);
    }

    size
}

#[doc(hidden)]
pub fn pack_string(buf: &mut Option<&mut Buffer>, value: &str) -> usize {
    let mut size = value.len() + 1;

    size += pack_byte_array_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_u8(ParticleType::STRING as u8);
        buf.write_str(value);
    }

    size
}

#[doc(hidden)]
pub fn pack_raw_string(buf: &mut Option<&mut Buffer>, value: &str) -> usize {
    let mut size = value.len();

    size += pack_byte_array_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_str(value);
    }

    size
}

#[doc(hidden)]
fn pack_geo_json(buf: &mut Option<&mut Buffer>, value: &str) -> usize {
    let mut size = value.len() + 1;

    size += pack_byte_array_begin(buf, size);
    if let Some(ref mut buf) = *buf {
        buf.write_u8(ParticleType::GEOJSON as u8);
        buf.write_str(value);
    }

    size
}

#[doc(hidden)]
pub fn pack_integer(buf: &mut Option<&mut Buffer>, val: i64) -> usize {
    match val {
        val if (0..(1 << 7)).contains(&val) => pack_half_byte(buf, val as u8),
        val if val >= 1 << 7 && val < i64::from(i8::max_value()) => {
            pack_byte(buf, MSGPACK_MARKER_I8, val as u8)
        }
        val if val >= i64::from(i8::max_value()) && val < i64::from(i16::max_value()) => {
            pack_i16(buf, MSGPACK_MARKER_I16, val as i16)
        }
        val if val >= i64::from(i16::max_value()) && val < i64::from(i32::max_value()) => {
            pack_i32(buf, MSGPACK_MARKER_I32, val as i32)
        }
        val if val >= i64::from(i32::max_value()) => pack_i64(buf, MSGPACK_MARKER_I32, val),

        // Negative values
        val if val >= -32 && val < 0 => {
            pack_half_byte(buf, 0xe0 | ((Wrapping(val as u8) + Wrapping(32)).0))
        }
        val if val >= i64::from(i8::min_value()) && val < -32 => {
            pack_byte(buf, MSGPACK_MARKER_NI8, val as u8)
        }
        val if val >= i64::from(i16::min_value()) && val < i64::from(i8::min_value()) => {
            pack_i16(buf, MSGPACK_MARKER_NI16, val as i16)
        }
        val if val >= i64::from(i32::min_value()) && val < i64::from(i16::min_value()) => {
            pack_i32(buf, MSGPACK_MARKER_NI32, val as i32)
        }
        val if val < i64::from(i32::min_value()) => pack_i64(buf, MSGPACK_MARKER_NI64, val),
        _ => unreachable!(),
    }
}

#[doc(hidden)]
pub fn pack_i16(buf: &mut Option<&mut Buffer>, marker: u8, value: i16) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(marker);
        buf.write_i16(value);
    }
    3
}

#[doc(hidden)]
pub fn pack_i32(buf: &mut Option<&mut Buffer>, marker: u8, value: i32) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(marker);
        buf.write_i32(value);
    }
    5
}

#[doc(hidden)]
pub fn pack_i64(buf: &mut Option<&mut Buffer>, marker: u8, value: i64) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(marker);
        buf.write_i64(value);
    }
    9
}

#[doc(hidden)]
pub fn pack_u64(buf: &mut Option<&mut Buffer>, value: u64) -> usize {
    if value <= i64::max_value() as u64 {
        return pack_integer(buf, value as i64);
    }

    if let Some(ref mut buf) = *buf {
        buf.write_u8(0xcf);
        buf.write_u64(value);
    }
    9
}

#[doc(hidden)]
pub fn pack_f32(buf: &mut Option<&mut Buffer>, value: f32) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(0xca);
        buf.write_f32(value);
    }
    5
}

#[doc(hidden)]
pub fn pack_f64(buf: &mut Option<&mut Buffer>, value: f64) -> usize {
    if let Some(ref mut buf) = *buf {
        buf.write_u8(0xcb);
        buf.write_f64(value);
    }
    9
}
