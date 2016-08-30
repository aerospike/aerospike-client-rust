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
use std::str;
use std::fmt;
use std::io::Write;
use std::collections::HashMap;

use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt, ByteOrder};

use crypto::ripemd160::Ripemd160;
use crypto::digest::Digest;

use std::vec::Vec;

use common::ParticleType;
use error::{AerospikeResult, ResultCode, AerospikeError};
use command::buffer::Buffer;
use value::*;

pub fn unpack_value_list(buf: &mut Buffer) -> AerospikeResult<Value> {
    if buf.data_buffer.len() == 0 {
        return Ok(Value::List(vec![]));
    }

    let ltype: u8 = try!(buf.read_u8(None)) & 0xff;

    let count: usize = if (ltype & 0xf0) == 0x90 { (ltype & 0x0f) as usize }
    else if ltype == 0xdc { try!(buf.read_u16(None)) as usize }
    else if ltype == 0xdd { try!(buf.read_u32(None)) as usize }
    else { unreachable!() };

    unpack_list(buf, count)
}

pub fn unpack_value_map(buf: &mut Buffer) -> AerospikeResult<Value> {
    if buf.data_buffer.len() == 0 {
        return Ok(Value::from(HashMap::with_capacity(0)));
    }

    let ltype: u8 = try!(buf.read_u8(None)) & 0xff;

    let count: usize = if (ltype & 0xf0) == 0x80 { (ltype & 0x0f) as usize }
    else if ltype == 0xde { try!(buf.read_u16(None)) as usize }
    else if ltype == 0xdf { try!(buf.read_u32(None)) as usize }
    else { unreachable!() };

    unpack_map(buf, count)
}

fn unpack_list(buf: &mut Buffer, count: usize) -> AerospikeResult<Value> {
    let mut list: Vec<Value> = Vec::with_capacity(count);
    for i in 0..count {
        let val = try!(unpack_value(buf));
        list.push(val);
    }

    Ok(Value::from(list))
}

fn unpack_map(buf: &mut Buffer, count: usize) -> AerospikeResult<Value> {
    let mut map: HashMap<Value, Value> = HashMap::with_capacity(count);
    for i in 0..count {
        let key = try!(unpack_value(buf));
        let val = try!(unpack_value(buf));
        map.insert(key, val);
    }

    Ok(Value::from(map))
}

fn unpack_blob(buf: &mut Buffer, count: usize) -> AerospikeResult<Value> {
    let vtype = try!(buf.read_u8(None));
    let count = count - 1;

    match ParticleType::from(vtype) {
        ParticleType::STRING => {
            let val = try!(buf.read_str(count));
            Ok(Value::String(val))
        },

        ParticleType::BLOB => {
            Ok(Value::Blob(try!(buf.read_blob(count))))
        },

        ParticleType::GEOJSON => {
                let val = try!(buf.read_str(count));
                Ok(Value::GeoJSON(val))
        },

    _ =>
        Err(AerospikeError::new(ResultCode::SERIALIZE_ERROR, Some(format!("Error while unpacking BLOB. Type-header with code `{}` not recognized.", vtype)))),
    }
}

fn unpack_value(buf: &mut Buffer) -> AerospikeResult<Value> {
    let obj_type: u8 = try!(buf.read_u8(None)) & 0xff;

    match obj_type {
        0xc0 => return Ok(Value::Nil),

        0xc3 => return Ok(Value::from(true)),

        0xc2 => return Ok(Value::from(false)),

        0xca => return Ok(Value::from(try!(buf.read_f32(None)))),

        0xcb => return Ok(Value::from(try!(buf.read_f64(None)))),

        0xcc => return Ok(Value::from(try!(buf.read_u8(None)))),

        0xcd => return Ok(Value::from(try!(buf.read_u16(None)))),

        0xce => return Ok(Value::from(try!(buf.read_u32(None)))),

        0xcf => return Ok(Value::from(try!(buf.read_u64(None)))),

        0xd0 => return Ok(Value::from(try!(buf.read_i8(None)))),

        0xd1 => return Ok(Value::from(try!(buf.read_i16(None)))),

        0xd2 => return Ok(Value::from(try!(buf.read_i32(None)))),

        0xd3 => return Ok(Value::from(try!(buf.read_i64(None)))),

        0xc4 | 0xd9 => {
                let count = try!(buf.read_u8(None));
                return Ok(Value::from(try!(unpack_blob(buf, count as usize))));
            },

        0xc5 | 0xda => {
                let count = try!(buf.read_u16(None));
                return Ok(Value::from(try!(unpack_blob(buf, count as usize))));
            },

        0xc6 | 0xdb => {
                let count = try!(buf.read_u32(None));
                return Ok(Value::from(try!(unpack_blob(buf, count as usize))));
            },

        0xdc => {
                let count = try!(buf.read_u16(None));
                return unpack_list(buf, count as usize);
            },

        0xdd => {
                let count = try!(buf.read_u32(None));
                return unpack_list(buf, count as usize);
            },

        0xde => {
                let count = try!(buf.read_u16(None));
                return unpack_map(buf, count as usize);
            },

        0xdf => {
                let count = try!(buf.read_u32(None));
                return unpack_map(buf, count as usize);
            },

        0xd4 => {
                let count = (1 + 1) as usize;
                try!(buf.skip_bytes(count)); // Skip over type extension with 1 byte
            },
        0xd5 => {
                let count = (1 + 2) as usize;
                try!(buf.skip_bytes(count)); // Skip over type extension with 2 bytes
            },
        0xd6 => {
                let count = (1 + 4) as usize;
                try!(buf.skip_bytes(count)); // Skip over type extension with 4 bytes
            },
        0xd7 => {
                let count = (1 + 8) as usize;
                try!(buf.skip_bytes(count)); // Skip over type extension with 8 bytes
            },
        0xd8 => {
                let count = (1 + 16) as usize;
                try!(buf.skip_bytes(count)); // Skip over type extension with 16 bytes
            },
        0xc7 => {
                let count = 1 + try!(buf.read_u8(None));
                try!(buf.skip_bytes(count as usize)); // Skip over type extension with 8 bit header and bytes
            },
        0xc8 => {
                let count = 1 + try!(buf.read_u16(None));
                try!(buf.skip_bytes(count as usize)); // Skip over type extension with 16 bit header and bytes
            },
        0xc9 => {
                let count = 1 + try!(buf.read_u32(None));
                try!(buf.skip_bytes(count as usize)); // Skip over type extension with 32 bit header and bytes
            },
        _ => {
                if (obj_type & 0xe0) == 0xa0 {
                    return unpack_blob(buf, (obj_type & 0x1f) as usize)
                }

                if (obj_type & 0xf0) == 0x80 {
                    return unpack_map(buf, (obj_type & 0x0f) as usize)
                }

                if (obj_type & 0xf0) == 0x90 {
                    let count = (obj_type & 0x0f) as usize;
                    return unpack_list(buf, count)
                }

                if obj_type < 0x80 {
                    return Ok(Value::from(obj_type))
                }

                if obj_type >= 0xe0 {
                    return Ok(Value::from((obj_type as i8) - 0xe0 - 32))
                }
            },
    }

    Err(AerospikeError::ErrSerialize())
}
