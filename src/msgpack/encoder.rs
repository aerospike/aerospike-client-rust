// Copyright 2015-2016 Aerospike, Inc.
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

extern crate core;

use std::{i8, i16, i32, i64};
use std::collections::HashMap;
use std::num::Wrapping;
use std::vec::Vec;

use common::ParticleType;
use common::operation;
use error::AerospikeResult;
use command::buffer::Buffer;
use value::*;

pub fn pack_value(buf: Option<&mut Buffer>, val: &Value) -> AerospikeResult<usize> {
    match val {
        &Value::Nil => pack_nil(buf),
        &Value::Int(ref val) => pack_integer(buf, *val),
        &Value::UInt(ref val) => pack_u64(buf, *val),
        &Value::Bool(ref val) => pack_bool(buf, *val),
        &Value::String(ref val) => pack_string(buf, val),
        &Value::Float(ref val) => {
            match val {
                &FloatValue::F64(_) => pack_f64(buf, f64::from(val)),
                &FloatValue::F32(_) => pack_f32(buf, f32::from(val)),
            }
        }
        &Value::Blob(ref val) => pack_blob(buf, val),
        &Value::List(ref val) => pack_array(buf, val),
        &Value::HashMap(ref val) => pack_map(buf, val),
        &Value::OrderedMap(_) => panic!("Ordered maps are not supported in this encoder."),
        &Value::GeoJSON(ref val) => pack_geo_json(buf, val),
    }
}

pub fn pack_empty_args_array(buf: Option<&mut Buffer>) -> AerospikeResult<usize> {
    let mut size = 0;
    if let Some(buf) = buf {
        size += try!(pack_array_begin(Some(buf), 0));
    } else {
        size += try!(pack_array_begin(None, 0));
    }

    Ok(size)
}

pub fn pack_cdt_list_args(buf: Option<&mut Buffer>,
                          cdt_op: u8,
                          meta_args: &Option<Vec<Value>>,
                          args: &Option<&[Value]>,
                          single_value: &Value)
                          -> AerospikeResult<usize> {

    // arguments will always be either in args or single value
    // never in both
    assert_eq!(args.is_some() && (single_value != operation::NIL_VALUE),
               false);

    let mut args_len: usize = 0;
    if let &Some(ref meta_args) = meta_args {
        args_len += meta_args.len();
    };

    // all args are considered ONE argument
    if args.is_some() || single_value != operation::NIL_VALUE {
        args_len += 1;
    }

    let mut size: usize = 0;

    if let Some(buf) = buf {
        size += try!(pack_raw_u16(Some(buf), cdt_op as u16));

        if args_len > 0 {
            size += try!(pack_array_begin(Some(buf), args_len));
        }

        if let &Some(ref meta_args) = meta_args {
            for value in meta_args {
                size += try!(pack_value(Some(buf), &value));
            }
        }

        if let &Some(args) = args {
            size += try!(pack_array(Some(buf), args));
        } else if single_value != operation::NIL_VALUE {
            size += try!(pack_value(Some(buf), single_value));
        }
    } else {
        size += try!(pack_raw_u16(None, cdt_op as u16));

        if args_len > 0 {
            size += try!(pack_array_begin(None, args_len));
        }

        if let &Some(ref meta_args) = meta_args {
            for value in meta_args {
                size += try!(pack_value(None, &value));
            }
        }

        if let &Some(args) = args {
            size += try!(pack_array(None, args));
        } else if single_value != operation::NIL_VALUE {
            size += try!(pack_value(None, single_value));
        }
    }

    Ok(size)
}

pub fn pack_cdt_map_args(buf: Option<&mut Buffer>,
                         cdt_op: u8,
                         meta_args: &Option<Vec<Value>>,
                         lists: &Option<&[Value]>,
                         args: &Option<&HashMap<Value, Value>>,
                         single_entry: &Option<(&Value, &Value)>)
                         -> AerospikeResult<usize> {

    let mut args_len: usize = 0;
    if let &Some(ref meta_args) = meta_args {
        args_len += meta_args.len();
    };

    // all args are considered ONE argument
    if args.is_some() || lists.is_some() {
        args_len += 1;
    }

    if let &Some((_, v)) = single_entry {
        args_len += 1;
        if v != operation::NIL_VALUE {
            args_len += 1;
        }
    }

    let mut size: usize = 0;

    if let Some(buf) = buf {
        size += try!(pack_raw_u16(Some(buf), cdt_op as u16));

        if args_len > 0 {
            size += try!(pack_array_begin(Some(buf), args_len));
        }

        if let &Some(args) = args {
            size += try!(pack_map(Some(buf), args));
        } else if let &Some((key, val)) = single_entry {
            size += try!(pack_value(Some(buf), key));
            if val != operation::NIL_VALUE {
                size += try!(pack_value(Some(buf), val));
            }
        } else if let &Some(items) = lists {
            size += try!(pack_array(Some(buf), items));
        }

        if let &Some(ref meta_args) = meta_args {
            for value in meta_args {
                size += try!(pack_value(Some(buf), &value));
            }
        }
    } else {
        size += try!(pack_raw_u16(None, cdt_op as u16));

        if args_len > 0 {
            size += try!(pack_array_begin(None, args_len));
        }

        if let &Some(args) = args {
            size += try!(pack_map(None, args));
        } else if let &Some((key, val)) = single_entry {
            size += try!(pack_value(None, key));
            if val != operation::NIL_VALUE {
                size += try!(pack_value(None, val));
            }
        } else if let &Some(items) = lists {
            size += try!(pack_array(None, items));
        }

        if let &Some(ref meta_args) = meta_args {
            for value in meta_args {
                size += try!(pack_value(None, &value));
            }
        }
    }

    Ok(size)
}


pub fn pack_array(buf: Option<&mut Buffer>, values: &[Value]) -> AerospikeResult<usize> {
    let mut size = 0;

    if let Some(buf) = buf {
        size += try!(pack_array_begin(Some(buf), values.len()));
        for val in values {
            size += try!(pack_value(Some(buf), val));
        }
    } else {
        size += try!(pack_array_begin(None, values.len()));
        for val in values {
            size += try!(pack_value(None, val));
        }
    }

    Ok(size)
}

fn pack_map(buf: Option<&mut Buffer>, map: &HashMap<Value, Value>) -> AerospikeResult<usize> {
    let mut size = 0;

    if let Some(buf) = buf {
        size += try!(pack_map_begin(Some(buf), map.len()));
        for (key, val) in map.iter() {
            size += try!(pack_value(Some(buf), key));
            size += try!(pack_value(Some(buf), val));
        }
    } else {
        size += try!(pack_map_begin(None, map.len()));
        for (key, val) in map {
            size += try!(pack_value(None, key));
            size += try!(pack_value(None, val));
        }
    }

    Ok(size)
}

/// ///////////////////////////////////////////////////////////////////

const MSGPACK_MARKER_NIL: u8 = 0xc0;
const MSGPACK_MARKER_BOOL_TRUE: u8 = 0xc3;
const MSGPACK_MARKER_BOOL_FALSE: u8 = 0xc2;

const MSGPACK_MARKER_I8: u8 = 0xcc;
const MSGPACK_MARKER_I16: u8 = 0xcd;
const MSGPACK_MARKER_I32: u8 = 0xce;
const MSGPACK_MARKER_I64: u8 = 0xd3;

const MSGPACK_MARKER_NI8: u8 = 0xd0;
const MSGPACK_MARKER_NI16: u8 = 0xd1;
const MSGPACK_MARKER_NI32: u8 = 0xd2;
const MSGPACK_MARKER_NI64: u8 = 0xd3;

// This method is not compatible with MsgPack specs and is only used by aerospike client<->server
// for wire transfer only
fn pack_raw_u16(buf: Option<&mut Buffer>, val: u16) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        try!(buf.write_u16(val));
    }
    Ok(2)
}

fn pack_half_byte(buf: Option<&mut Buffer>, val: u8) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        try!(buf.write_u8(val));
    }
    Ok(1)
}

fn pack_byte(buf: Option<&mut Buffer>, marker: u8, val: u8) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        try!(buf.write_u8(marker));
        try!(buf.write_u8(val));
    }
    Ok(2)
}

fn pack_nil(buf: Option<&mut Buffer>) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        try!(buf.write_u8(MSGPACK_MARKER_NIL));
    }
    Ok(1)
}

fn pack_bool(buf: Option<&mut Buffer>, val: bool) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        if val {
            try!(buf.write_u8(MSGPACK_MARKER_BOOL_TRUE));
        } else {
            try!(buf.write_u8(MSGPACK_MARKER_BOOL_FALSE));
        }
    }
    Ok(1)
}

fn pack_map_begin(buf: Option<&mut Buffer>, length: usize) -> AerospikeResult<usize> {
    match length {
        val if val < 16 => pack_half_byte(buf, 0x80 | (length as u8)),
        val if val >= 16 && val < 2 ^ 16 => pack_i16(buf, 0xde, length as i16),
        _ => pack_i32(buf, 0xdf, length as i32),
    }
}

fn pack_array_begin(buf: Option<&mut Buffer>, length: usize) -> AerospikeResult<usize> {
    match length {
        val if val < 16 => pack_half_byte(buf, 0x90 | (length as u8)),
        val if val >= 16 && val < 2 ^ 16 => pack_i16(buf, 0xdc, length as i16),
        _ => pack_i32(buf, 0xdd, length as i32),
    }
}

fn pack_byte_array_begin(buf: Option<&mut Buffer>, length: usize) -> AerospikeResult<usize> {
    match length {
        val if val < 32 => pack_half_byte(buf, 0xa0 | (length as u8)),
        val if val >= 32 && val < 2 ^ 16 => pack_i16(buf, 0xda, length as i16),
        _ => pack_i32(buf, 0xdb, length as i32),
    }
}

fn pack_blob(buf: Option<&mut Buffer>, val: &[u8]) -> AerospikeResult<usize> {
    let mut size = val.len() + 1;

    if let Some(buf) = buf {
        size += try!(pack_byte_array_begin(Some(buf), size));
        try!(buf.write_u8(ParticleType::BLOB as u8));
        try!(buf.write_bytes(val));
    } else {
        size += try!(pack_byte_array_begin(None, size));
    }

    Ok(size)
}

fn pack_string(buf: Option<&mut Buffer>, val: &str) -> AerospikeResult<usize> {
    let mut size = val.len() + 1;

    if let Some(buf) = buf {
        size += try!(pack_byte_array_begin(Some(buf), size));
        try!(buf.write_u8(ParticleType::STRING as u8));
        try!(buf.write_str(val));
    } else {
        size += try!(pack_byte_array_begin(None, size));
    }

    Ok(size)
}

fn pack_geo_json(buf: Option<&mut Buffer>, val: &str) -> AerospikeResult<usize> {
    let mut size = val.len() + 1;

    if let Some(buf) = buf {
        size += try!(pack_byte_array_begin(Some(buf), size));
        try!(buf.write_u8(ParticleType::GEOJSON as u8));
        try!(buf.write_str(val));
    } else {
        size += try!(pack_byte_array_begin(None, size));
    }

    Ok(size)
}

fn pack_integer(buf: Option<&mut Buffer>, val: i64) -> AerospikeResult<usize> {
    match val {
        val if val >= 0 && val < 2 ^ 7 => pack_half_byte(buf, val as u8),
        val if val >= 2 ^ 7 && val < i8::MAX as i64 => pack_byte(buf, MSGPACK_MARKER_I8, val as u8),
        val if val >= i8::MAX as i64 && val < i16::MAX as i64 => {
            pack_i16(buf, MSGPACK_MARKER_I16, val as i16)
        }
        val if val >= i16::MAX as i64 && val < i32::MAX as i64 => {
            pack_i32(buf, MSGPACK_MARKER_I32, val as i32)
        }
        val if val >= i32::MAX as i64 => pack_i64(buf, MSGPACK_MARKER_I32, val),

        // Negative values
        val if val >= -32 && val < 0 => {
            pack_half_byte(buf, 0xe0 | ((Wrapping(val as u8) + Wrapping(32)).0))
        }
        val if val >= i8::MIN as i64 && val < -32 => pack_byte(buf, MSGPACK_MARKER_NI8, val as u8),
        val if val >= i16::MIN as i64 && val < i8::MIN as i64 => {
            pack_i16(buf, MSGPACK_MARKER_NI16, val as i16)
        }
        val if val >= i32::MIN as i64 && val < i16::MIN as i64 => {
            pack_i32(buf, MSGPACK_MARKER_NI32, val as i32)
        }
        val if val < i32::MIN as i64 => pack_i64(buf, MSGPACK_MARKER_NI64, val),
        _ => unreachable!(),
    }
}

fn pack_i16(buf: Option<&mut Buffer>, marker: u8, val: i16) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        try!(buf.write_u8(marker));
        try!(buf.write_i16(val));
    }
    Ok(3)
}

fn pack_i32(buf: Option<&mut Buffer>, marker: u8, val: i32) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        try!(buf.write_u8(marker));
        try!(buf.write_i32(val));
    }
    Ok(5)
}

fn pack_i64(buf: Option<&mut Buffer>, marker: u8, val: i64) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        try!(buf.write_u8(marker));
        try!(buf.write_i64(val));
    }
    Ok(9)
}

fn pack_u64(buf: Option<&mut Buffer>, val: u64) -> AerospikeResult<usize> {
    if val <= i64::MAX as u64 {
        return pack_integer(buf, val as i64);
    }

    if let Some(buf) = buf {
        try!(buf.write_u8(0xcf));
        try!(buf.write_u64(val));
    }
    Ok(9)
}

fn pack_f32(buf: Option<&mut Buffer>, val: f32) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        try!(buf.write_u8(0xca));
        try!(buf.write_f32(val));
    }
    Ok(5)
}

fn pack_f64(buf: Option<&mut Buffer>, val: f64) -> AerospikeResult<usize> {
    if let Some(buf) = buf {
        try!(buf.write_u8(0xcb));
        try!(buf.write_f64(val));
    }
    Ok(9)
}
