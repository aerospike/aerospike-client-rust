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

use std::str;
use std::fmt;
use std::io::Write;

use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt, ByteOrder};

use std::vec::Vec;

use common::ParticleType;
use error::{AerospikeResult, ResultCode, AerospikeError};
use command::buffer::Buffer;

pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn particle_type(&self) -> ParticleType {
        match self {
            &Value::Int(_) => ParticleType::INTEGER,
            &Value::Float(_) => ParticleType::FLOAT,
            &Value::String(_) => ParticleType::STRING,
            &Value::Blob(_) => ParticleType::BLOB,
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            &Value::Int(val) => format!("{}", val),
            &Value::Float(val) => format!("{}", val),
            &Value::String(ref val) => format!("{}", val),
            &Value::Blob(ref val) => format!("{:?}", val),
        }
    }

    pub fn estimate_size(&self) -> usize {
        match self {
            &Value::Int(_) => 8,
            &Value::Float(_) => 8,
            &Value::String(ref s) => s.len(),
            &Value::Blob(ref s) => s.len(),
        }
    }

    pub fn write_to(&self, buffer: &mut Buffer) -> AerospikeResult<()> {
        match self {
            &Value::Int(val) => buffer.write_i64(val),
            &Value::Float(val) => buffer.write_f64(val),
            &Value::String(ref val) => buffer.write_str(val),
            &Value::Blob(ref val) => buffer.write_bytes(val),
        }
        Ok(())
    }

    pub fn key_bytes(&self) -> AerospikeResult<Vec<u8>> {
        match self {
            &Value::Int(val) => {
                let mut buf = Vec::with_capacity(8);
                buf.resize(8, 0);
                NetworkEndian::write_i64(&mut buf, val);
                Ok(buf)
            },
            &Value::Float(val) => {
                let mut buf = Vec::with_capacity(8);
                buf.resize(8, 0);
                NetworkEndian::write_f64(&mut buf, val);
                Ok(buf)
            },
            &Value::String(ref val) => {
                Ok(val.as_bytes().to_vec())
            },
            _ => panic!("Data type is not supported as Key value.")
        }
    }
}

impl core::fmt::Display for Value {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        try!(self.as_string().fmt(f));
        Ok(())
    }
}

impl From<i8> for Value {
    fn from(val: i8) -> Value {
        Value::Int(val as i64)
    }
}

impl From<u8> for Value {
    fn from(val: u8) -> Value {
        Value::Int(val as i64)
    }
}

impl From<i16> for Value {
    fn from(val: i16) -> Value {
        Value::Int(val as i64)
    }
}

impl From<u16> for Value {
    fn from(val: u16) -> Value {
        Value::Int(val as i64)
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Value {
        Value::Int(val as i64)
    }
}

impl From<u32> for Value {
    fn from(val: u32) -> Value {
        Value::Int(val as i64)
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Value {
        Value::Int(val)
    }
}

impl From<String> for Value {
    fn from(val: String) -> Value {
        Value::String(val)
    }
}

impl From<Vec<u8>> for Value {
    fn from(val: Vec<u8>) -> Value {
        Value::Blob(val)
    }
}

impl<'a> From<&'a i8> for Value {
    fn from(val: &'a i8) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a u8> for Value {
    fn from(val: &'a u8) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a i16> for Value {
    fn from(val: &'a i16) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a u16> for Value {
    fn from(val: &'a u16) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a i32> for Value {
    fn from(val: &'a i32) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a u32> for Value {
    fn from(val: &'a u32) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a i64> for Value {
    fn from(val: &'a i64) -> Value {
        Value::Int(*val)
    }
}

impl From<f32> for Value {
    fn from(val: f32) -> Value {
        Value::Float(val as f64)
    }
}

impl From<f64> for Value {
    fn from(val: f64) -> Value {
        Value::Float(val)
    }
}

impl<'a> From<&'a f32> for Value {
    fn from(val: &'a f32) -> Value {
        Value::Float(*val as f64)
    }
}

impl<'a> From<&'a f64> for Value {
    fn from(val: &'a f64) -> Value {
        Value::Float(*val)
    }
}

impl<'a> From<&'a String> for Value {
    fn from(val: &'a String) -> Value {
        Value::String(val.clone())
    }
}

impl<'a> From<&'a str> for Value {
    fn from(val: &'a str) -> Value {
        Value::String(val.to_string())
    }
}

impl<'a> From<&'a Vec<u8>> for Value {
    fn from(val: &'a Vec<u8>) -> Value {
        Value::Blob(val.clone())
    }
}

impl<'a> From<&'a [u8]> for Value {
    fn from(val: &'a [u8]) -> Value {
        Value::Blob(val.to_vec())
    }
}


pub fn bytes_to_particle(ptype: u8, buf: &[u8]) -> AerospikeResult<Option<Value>> {
    match ptype {
        x if x == ParticleType::NULL as u8 => {
            Ok(None)
        },
        x if x == ParticleType::INTEGER as u8 => {
            let val = NetworkEndian::read_i64(&buf);
            Ok(Some(Value::Int(val)))
        },
        x if x == ParticleType::FLOAT as u8 => {
            let val = NetworkEndian::read_f64(&buf);
            Ok(Some(Value::Float(val)))
        },
        x if x == ParticleType::STRING as u8 => {
            let val = try!(String::from_utf8(buf.to_vec()));
            Ok(Some(Value::String(val)))
        },
        x if x == ParticleType::BLOB as u8 => {
            Ok(Some(Value::Blob(buf.to_vec())))
        },
        _ => unreachable!(),
    }
}

// #[macro_export]
// macro_rules! val {
//     (None) => {{
//         None
//     }};
//     ($x:expr) => {{
//         let temp_val: &Value = &$x;
//         Some(Box::new(temp_val))
//     }}
// }

// #[macro_export]
// macro_rules! key_val {
//     (None) => {{
//         None
//     }};
//     ($x:expr) => {{
//         let temp_val: &KeyValue = $x;
//         Some(Box::new(temp_val))
//     }};
// }
