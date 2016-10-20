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

use std::{str, f32, f64};
use std::fmt;
use std::mem;
use std::io::Write;
use std::collections::HashMap;
use std::hash::{Hash, Hasher, SipHasher};

use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt, ByteOrder};

use crypto::ripemd160::Ripemd160;
use crypto::digest::Digest;

use std::vec::Vec;

use common::ParticleType;
use error::{AerospikeResult, ResultCode, AerospikeError};
use command::buffer::Buffer;
use msgpack::encoder::pack_value;
use msgpack::decoder::*;

/// ////////////////////////////////////////////////////////////////////////
#[derive(Debug,Clone,PartialEq,Eq,Hash)]
pub enum FloatValue {
    F32(u32),
    F64(u64),
}

impl From<FloatValue> for f64 {
    fn from(val: FloatValue) -> f64 {
        match val {
            FloatValue::F32(_) => {
                panic!("This library does not automatically convert f32 -> f64 to be used in keys \
                        or bins.")
            }
            FloatValue::F64(val) => unsafe { mem::transmute(val) },
        }
    }
}

impl<'a> From<&'a FloatValue> for f64 {
    fn from(val: &FloatValue) -> f64 {
        match val {
            &FloatValue::F32(_) => {
                panic!("This library does not automatically convert f32 -> f64 to be used in keys \
                        or bins.")
            }
            &FloatValue::F64(val) => unsafe { mem::transmute(val) },
        }
    }
}

impl From<f64> for FloatValue {
    fn from(val: f64) -> FloatValue {
        let mut val = val;
        if val.is_nan() {
            val = f64::NAN
        } // make all NaNs have the same representation
        unsafe { FloatValue::F64(mem::transmute(val)) }
    }
}

impl<'a> From<&'a f64> for FloatValue {
    fn from(val: &f64) -> FloatValue {
        let mut val = *val;
        if val.is_nan() {
            val = f64::NAN
        } // make all NaNs have the same representation
        unsafe { FloatValue::F64(mem::transmute(val)) }
    }
}

impl From<FloatValue> for f32 {
    fn from(val: FloatValue) -> f32 {
        match val {
            FloatValue::F32(val) => unsafe { mem::transmute(val) },
            FloatValue::F64(val) => unsafe { mem::transmute(val as u32) },
        }
    }
}

impl<'a> From<&'a FloatValue> for f32 {
    fn from(val: &FloatValue) -> f32 {
        match val {
            &FloatValue::F32(val) => unsafe { mem::transmute(val) },
            &FloatValue::F64(val) => unsafe { mem::transmute(val as u32) },
        }
    }
}

impl From<f32> for FloatValue {
    fn from(val: f32) -> FloatValue {
        let mut val = val;
        if val.is_nan() {
            val = f32::NAN
        } // make all NaNs have the same representation
        unsafe { FloatValue::F32(mem::transmute(val)) }
    }
}

impl<'a> From<&'a f32> for FloatValue {
    fn from(val: &f32) -> FloatValue {
        let mut val = *val;
        if val.is_nan() {
            val = f32::NAN
        } // make all NaNs have the same representation
        unsafe { FloatValue::F32(mem::transmute(val)) }
    }
}

impl core::fmt::Display for FloatValue {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        match self {
            &FloatValue::F32(val) => {
                let val: f32 = unsafe { mem::transmute(val) };
                write!(f, "{}", val)
            }
            &FloatValue::F64(val) => {
                let val: f64 = unsafe { mem::transmute(val) };
                write!(f, "{}", val)
            }
        }
    }
}

/// ////////////////////////////////////////////////////////////////////////
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Value {
    Nil,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(FloatValue),
    String(String),
    Blob(Vec<u8>),
    List(Vec<Value>),
    HashMap(HashMap<Value, Value>),
    OrderedMap(Vec<(Value, Value)>),
    GeoJSON(String),
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            &Value::Nil => {
                let v: Option<u8> = None;
                v.hash(state)
            }
            &Value::Bool(ref val) => val.hash(state),
            &Value::Int(ref val) => val.hash(state),
            &Value::UInt(ref val) => val.hash(state),
            &Value::Float(ref val) => val.hash(state),
            &Value::String(ref val) => val.hash(state),
            &Value::Blob(ref val) => val.hash(state),
            &Value::List(ref val) => val.hash(state),
            &Value::HashMap(_) => panic!("HashMaps cannot be used as map keys."),
            &Value::OrderedMap(_) => panic!("OrderedMaps cannot be used as map keys."),
            &Value::GeoJSON(ref val) => val.hash(state),
        }
    }
}

impl Value {
    pub fn particle_type(&self) -> ParticleType {
        match self {
            &Value::Nil => ParticleType::NULL,
            &Value::Int(_) => ParticleType::INTEGER,
            &Value::UInt(_) => {
                panic!("Aerospike does not support u64 natively on server-side. Use casting to \
                        store and retrieve u64 values.")
            }
            &Value::Bool(_) => ParticleType::INTEGER,
            &Value::Float(_) => ParticleType::FLOAT,
            &Value::String(_) => ParticleType::STRING,
            &Value::Blob(_) => ParticleType::BLOB,
            &Value::List(_) => ParticleType::LIST,
            &Value::HashMap(_) => ParticleType::MAP,
            &Value::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            &Value::GeoJSON(_) => ParticleType::GEOJSON,
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            &Value::Nil => "<null>".to_string(),
            &Value::Int(ref val) => format!("{}", val),
            &Value::UInt(ref val) => format!("{}", val),
            &Value::Bool(ref val) => format!("{}", val),
            &Value::Float(ref val) => format!("{}", f64::from(val)),
            &Value::String(ref val) => format!("{}", val),
            &Value::Blob(ref val) => format!("{:?}", val),
            &Value::List(ref val) => format!("{:?}", val),
            &Value::HashMap(ref val) => format!("{:?}", val),
            &Value::OrderedMap(ref val) => format!("{:?}", val),
            &Value::GeoJSON(ref val) => format!("{}", val),
        }
    }

    pub fn estimate_size(&self) -> AerospikeResult<usize> {
        match self {
            &Value::Nil => Ok(0),
            &Value::Int(_) => Ok(8),
            &Value::UInt(_) => {
                panic!("Aerospike does not support u64 natively on server-side. Use casting to \
                        store and retrieve u64 values.")
            }
            &Value::Bool(_) => Ok(8),
            &Value::Float(_) => Ok(8),
            &Value::String(ref s) => Ok(s.len()),
            &Value::Blob(ref b) => Ok(b.len()),
            &Value::List(_) => pack_value(None, self),
            &Value::HashMap(_) => pack_value(None, self),
            &Value::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            &Value::GeoJSON(ref s) => Ok(1 + 2 + s.len()), // flags + ncells + jsonstr
        }
    }

    pub fn write_to(&self, buf: &mut Buffer) -> AerospikeResult<usize> {
        match self {
            &Value::Nil => Ok(0),
            &Value::Int(ref val) => buf.write_i64(*val),
            &Value::UInt(_) => {
                panic!("Aerospike does not support u64 natively on server-side. Use casting to \
                        store and retrieve u64 values.")
            }
            &Value::Bool(ref val) => buf.write_bool(*val),
            &Value::Float(ref val) => buf.write_f64(f64::from(val)),
            &Value::String(ref val) => buf.write_str(val),
            &Value::Blob(ref val) => buf.write_bytes(val),
            &Value::List(_) => pack_value(Some(buf), self),
            &Value::HashMap(_) => pack_value(Some(buf), self),
            &Value::OrderedMap(_) => panic!("The library never passes ordered maps to the server."),
            &Value::GeoJSON(ref val) => buf.write_geo(val),
        }
    }

    pub fn write_key_bytes(&self, h: &mut Ripemd160) -> AerospikeResult<()> {
        match self {
            &Value::Int(ref val) => {
                let mut buf = [0; 8];
                NetworkEndian::write_i64(&mut buf, *val);
                h.input(&buf);
                Ok(())
            }
            &Value::Float(ref val) => {
                let mut buf = [0; 8];
                NetworkEndian::write_f64(&mut buf, f64::from(val));
                h.input(&buf);
                Ok(())
            }
            &Value::String(ref val) => {
                h.input(val.as_bytes());
                Ok(())
            }
            _ => panic!("Data type is not supported as Key value."),
        }
    }
}

impl core::fmt::Display for Value {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        write!(f, "{}", self.as_string())
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

impl From<Vec<Value>> for Value {
    fn from(val: Vec<Value>) -> Value {
        Value::List(val)
    }
}

impl From<HashMap<Value, Value>> for Value {
    fn from(val: HashMap<Value, Value>) -> Value {
        Value::HashMap(val)
    }
}

impl From<f32> for Value {
    fn from(val: f32) -> Value {
        Value::Float(FloatValue::from(val))
    }
}

impl From<f64> for Value {
    fn from(val: f64) -> Value {
        Value::Float(FloatValue::from(val))
    }
}

impl<'a> From<&'a f32> for Value {
    fn from(val: &'a f32) -> Value {
        Value::Float(FloatValue::from(*val))
    }
}

impl<'a> From<&'a f64> for Value {
    fn from(val: &'a f64) -> Value {
        Value::Float(FloatValue::from(*val))
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

impl From<bool> for Value {
    fn from(val: bool) -> Value {
        Value::Bool(val)
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

impl From<u64> for Value {
    fn from(val: u64) -> Value {
        Value::UInt(val)
    }
}

impl From<isize> for Value {
    fn from(val: isize) -> Value {
        Value::Int(val as i64)
    }
}

impl From<usize> for Value {
    fn from(val: usize) -> Value {
        Value::UInt(val as u64)
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

impl<'a> From<&'a u64> for Value {
    fn from(val: &'a u64) -> Value {
        Value::UInt(*val)
    }
}

impl<'a> From<&'a isize> for Value {
    fn from(val: &'a isize) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a usize> for Value {
    fn from(val: &'a usize) -> Value {
        Value::UInt(*val as u64)
    }
}


impl<'a> From<&'a bool> for Value {
    fn from(val: &'a bool) -> Value {
        Value::Bool(*val)
    }
}

impl From<Value> for i64 {
    fn from(val: Value) -> i64 {
        match val {
            Value::Int(val) => val,
            Value::UInt(val) => val as i64,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for i64 {
    fn from(val: &'a Value) -> i64 {
        match val {
            &Value::Int(val) => val,
            &Value::UInt(val) => val as i64,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}



pub fn bytes_to_particle(ptype: u8, buf: &mut Buffer, len: usize) -> AerospikeResult<Value> {
    match ParticleType::from(ptype) {
        ParticleType::NULL => Ok(Value::Nil),
        ParticleType::INTEGER => {
            let val = try!(buf.read_i64(None));
            Ok(Value::Int(val))
        }
        ParticleType::FLOAT => {
            let val = try!(buf.read_f64(None));
            Ok(Value::Float(FloatValue::from(val)))
        }
        ParticleType::STRING => {
            let val = try!(buf.read_str(len));
            Ok(Value::String(val))
        }
        ParticleType::GEOJSON => {
            try!(buf.skip(1));
            let ncells = try!(buf.read_i16(None)) as usize;
            let header_size: usize = ncells * 8;

            try!(buf.skip(header_size));
            let val = try!(buf.read_str(len - header_size - 3));
            Ok(Value::String(val))
        }
        ParticleType::BLOB => Ok(Value::Blob(try!(buf.read_blob(len)))),
        ParticleType::LIST => {
            let val = try!(unpack_value_list(buf));
            Ok(val)
        }
        ParticleType::MAP => {
            let val = try!(unpack_value_map(buf));
            Ok(val)
        }
        ParticleType::DIGEST => Ok(Value::from("A DIGEST, NOT IMPLEMENTED YET!")),
        ParticleType::LDT => Ok(Value::from("A LDT, NOT IMPLEMENTED YET!")),
    }
}

#[macro_export]
macro_rules! as_val {
    ($val:expr) => {{ Value::from($val) }}
}

#[macro_export]
macro_rules! as_geo {
    ($val:expr) => {{ Value::GeoJSON($val) }}
}

#[macro_export]
macro_rules! as_blob {
    ($val:expr) => {{ Value::Blob($val) }}
}

#[macro_export]
macro_rules! as_list {
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec: Vec<Value> = Vec::new();
            $(
                temp_vec.push(Value::from($v));
            )*
            Value::List(temp_vec)
        }
    };
}

#[macro_export]
macro_rules! as_values {
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec: Vec<Value> = Vec::new();
            $(
                temp_vec.push(Value::from($v));
            )*
            temp_vec
        }
    };
}

#[macro_export]
macro_rules! as_map {
    ( $( $k:expr => $v:expr),* ) => {
        {
            let mut temp_map: HashMap<Value, Value> = HashMap::new();
            $(
                temp_map.insert(Value::from($k), Value::from($v));
            )*
            Value::HashMap(temp_map)
        }
    };
}
