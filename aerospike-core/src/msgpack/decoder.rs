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

use std::collections::HashMap;
use std::vec::Vec;

use crate::commands::buffer::Buffer;
use crate::commands::ParticleType;
use crate::errors::{ErrorKind, Result};
use crate::value::Value;

pub fn unpack_value_list(buf: &mut Buffer) -> Result<Value> {
    if buf.data_buffer.is_empty() {
        return Ok(Value::List(vec![]));
    }

    let ltype: u8 = buf.read_u8(None);

    let count: usize = match ltype {
        0x90..=0x9f => (ltype & 0x0f) as usize,
        0xdc => buf.read_u16(None) as usize,
        0xdd => buf.read_u32(None) as usize,
        _ => unreachable!(),
    };

    unpack_list(buf, count)
}

pub fn unpack_value_map(buf: &mut Buffer) -> Result<Value> {
    if buf.data_buffer.is_empty() {
        return Ok(Value::from(HashMap::with_capacity(0)));
    }

    let ltype: u8 = buf.read_u8(None);

    let count: usize = match ltype {
        0x80..=0x8f => (ltype & 0x0f) as usize,
        0xde => buf.read_u16(None) as usize,
        0xdf => buf.read_u32(None) as usize,
        _ => unreachable!(),
    };

    unpack_map(buf, count)
}

fn unpack_list(buf: &mut Buffer, mut count: usize) -> Result<Value> {
    if count > 0 && is_ext(buf.peek()) {
        let _uv = unpack_value(buf);
        count -= 1;
    }

    let mut list: Vec<Value> = Vec::with_capacity(count);
    for _ in 0..count {
        let val = unpack_value(buf)?;
        list.push(val);
    }

    Ok(Value::from(list))
}

fn unpack_map(buf: &mut Buffer, mut count: usize) -> Result<Value> {
    if count > 0 && is_ext(buf.peek()) {
        let _uv = unpack_value(buf);
        let _uv = unpack_value(buf);
        count -= 1;
    }

    let mut map: HashMap<Value, Value> = HashMap::with_capacity(count);
    for _ in 0..count {
        let key = unpack_value(buf)?;
        let val = unpack_value(buf)?;
        map.insert(key, val);
    }

    Ok(Value::from(map))
}

fn unpack_blob(buf: &mut Buffer, count: usize) -> Result<Value> {
    let vtype = buf.read_u8(None);
    let count = count - 1;

    match ParticleType::from(vtype) {
        ParticleType::STRING => {
            let val = buf.read_str(count)?;
            Ok(Value::String(val))
        }

        ParticleType::BLOB => Ok(Value::Blob(buf.read_blob(count))),

        ParticleType::GEOJSON => {
            let val = buf.read_str(count)?;
            Ok(Value::GeoJSON(val))
        }

        _ => bail!(
            "Error while unpacking BLOB. Type-header with code `{}` not recognized.",
            vtype
        ),
    }
}

fn unpack_value(buf: &mut Buffer) -> Result<Value> {
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
            let count = buf.read_u8(None);
            Ok(unpack_blob(buf, count as usize)?)
        }
        0xc5 | 0xda => {
            let count = buf.read_u16(None);
            Ok(unpack_blob(buf, count as usize)?)
        }
        0xc6 | 0xdb => {
            let count = buf.read_u32(None);
            Ok(unpack_blob(buf, count as usize)?)
        }
        0xc7 => {
            warn!("Skipping over type extension with 8 bit header and bytes");
            let count = 1 + buf.read_u8(None);
            buf.skip_bytes(count as usize);
            Ok(Value::Nil)
        }
        0xc8 => {
            warn!("Skipping over type extension with 16 bit header and bytes");
            let count = 1 + buf.read_u16(None);
            buf.skip_bytes(count as usize);
            Ok(Value::Nil)
        }
        0xc9 => {
            warn!("Skipping over type extension with 32 bit header and bytes");
            let count = 1 + buf.read_u32(None);
            buf.skip_bytes(count as usize);
            Ok(Value::Nil)
        }
        0xca => Ok(Value::from(buf.read_f32(None))),
        0xcb => Ok(Value::from(buf.read_f64(None))),
        0xcc => Ok(Value::from(buf.read_u8(None))),
        0xcd => Ok(Value::from(buf.read_u16(None))),
        0xce => Ok(Value::from(buf.read_u32(None))),
        0xcf => Ok(Value::from(buf.read_u64(None))),
        0xd0 => Ok(Value::from(buf.read_i8(None))),
        0xd1 => Ok(Value::from(buf.read_i16(None))),
        0xd2 => Ok(Value::from(buf.read_i32(None))),
        0xd3 => Ok(Value::from(buf.read_i64(None))),
        0xd4 => {
            warn!("Skipping over type extension with 1 byte");
            let count = (1 + 1) as usize;
            buf.skip_bytes(count);
            Ok(Value::Nil)
        }
        0xd5 => {
            warn!("Skipping over type extension with 2 bytes");
            let count = (1 + 2) as usize;
            buf.skip_bytes(count);
            Ok(Value::Nil)
        }
        0xd6 => {
            warn!("Skipping over type extension with 4 bytes");
            let count = (1 + 4) as usize;
            buf.skip_bytes(count);
            Ok(Value::Nil)
        }
        0xd7 => {
            warn!("Skipping over type extension with 8 bytes");
            let count = (1 + 8) as usize;
            buf.skip_bytes(count);
            Ok(Value::Nil)
        }
        0xd8 => {
            warn!("Skipping over type extension with 16 bytes");
            let count = (1 + 16) as usize;
            buf.skip_bytes(count);
            Ok(Value::Nil)
        }
        0xdc => {
            let count = buf.read_u16(None);
            unpack_list(buf, count as usize)
        }
        0xdd => {
            let count = buf.read_u32(None);
            unpack_list(buf, count as usize)
        }
        0xde => {
            let count = buf.read_u16(None);
            unpack_map(buf, count as usize)
        }
        0xdf => {
            let count = buf.read_u32(None);
            unpack_map(buf, count as usize)
        }
        0xe0..=0xff => {
            let value = i16::from(obj_type) - 0xe0 - 32;
            Ok(Value::from(value))
        }
        _ => Err(
            ErrorKind::BadResponse(format!("Error unpacking value of type '{:x}'", obj_type))
                .into(),
        ),
    }
}

const fn is_ext(byte: u8) -> bool {
    matches!(byte, 0xc7 | 0xc8 | 0xc9 | 0xd4 | 0xd5 | 0xd6 | 0xd7 | 0xd8)
}
