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

use std::cmp::{Ordering, PartialOrd};
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::result::Result as StdResult;

use byteorder::{ByteOrder, NetworkEndian};

use ripemd::digest::Digest;
use ripemd::Ripemd160;

use std::vec::Vec;

use crate::commands::buffer::Buffer;
use crate::commands::ParticleType;
use crate::errors::Result;
use crate::msgpack::{decoder, encoder};

#[cfg(feature = "serialization")]
use serde::ser::{SerializeMap, SerializeSeq};
#[cfg(feature = "serialization")]
use serde::{Serialize, Serializer};

/// Container for floating point bin values stored in the Aerospike database.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FloatValue {
    /// Container for single precision float values.
    F32(u32),
    /// Container for double precision float values.
    F64(u64),
}

impl From<FloatValue> for f64 {
    fn from(val: FloatValue) -> f64 {
        match val {
            FloatValue::F32(_) => panic!(
                "This library does not automatically convert f32 -> f64 to be used in keys \
                 or bins."
            ),
            FloatValue::F64(val) => f64::from_bits(val),
        }
    }
}

impl<'a> From<&'a FloatValue> for f64 {
    fn from(val: &FloatValue) -> f64 {
        match *val {
            FloatValue::F32(_) => panic!(
                "This library does not automatically convert f32 -> f64 to be used in keys \
                 or bins."
            ),
            FloatValue::F64(val) => f64::from_bits(val),
        }
    }
}

impl From<f64> for FloatValue {
    fn from(val: f64) -> FloatValue {
        let mut val = val;
        if val.is_nan() {
            val = f64::NAN;
        } // make all NaNs have the same representation
        FloatValue::F64(val.to_bits())
    }
}

impl<'a> From<&'a f64> for FloatValue {
    fn from(val: &f64) -> FloatValue {
        let mut val = *val;
        if val.is_nan() {
            val = f64::NAN;
        } // make all NaNs have the same representation
        FloatValue::F64(val.to_bits())
    }
}

impl From<FloatValue> for f32 {
    fn from(val: FloatValue) -> f32 {
        match val {
            FloatValue::F32(val) => f32::from_bits(val),
            FloatValue::F64(val) => f32::from_bits(val as u32),
        }
    }
}

impl<'a> From<&'a FloatValue> for f32 {
    fn from(val: &FloatValue) -> f32 {
        match *val {
            FloatValue::F32(val) => f32::from_bits(val),
            FloatValue::F64(val) => f32::from_bits(val as u32),
        }
    }
}

impl From<f32> for FloatValue {
    fn from(val: f32) -> FloatValue {
        let mut val = val;
        if val.is_nan() {
            val = f32::NAN;
        } // make all NaNs have the same representation
        FloatValue::F32(val.to_bits())
    }
}

impl<'a> From<&'a f32> for FloatValue {
    fn from(val: &f32) -> FloatValue {
        let mut val = *val;
        if val.is_nan() {
            val = f32::NAN;
        } // make all NaNs have the same representation
        FloatValue::F32(val.to_bits())
    }
}

impl fmt::Display for FloatValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        match *self {
            FloatValue::F32(val) => {
                let val: f32 = f32::from_bits(val);
                write!(f, "{}", val)
            }
            FloatValue::F64(val) => {
                let val: f64 = f64::from_bits(val);
                write!(f, "{}", val)
            }
        }
    }
}

/// Container for bin values stored in the Aerospike database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Empty value.
    Nil,

    /// Boolean value.
    Bool(bool),

    /// Integer value. All integers are represented as 64-bit numerics in Aerospike.
    Int(i64),

    /// Floating point value. All floating point values are stored in 64-bit IEEE-754 format in
    /// Aerospike. Aerospike server v3.6.0 and later support double data type.
    Float(FloatValue),

    /// String value.
    String(String),

    /// Byte array value.
    Blob(Vec<u8>),

    /// List data type is an ordered collection of values. Lists can contain values of any
    /// supported data type. List data order is maintained on writes and reads.
    List(Vec<Value>),

    /// Returned in cases where the server executes multiple operations for the same bin.
    /// This value is only sent from the server to the client, and can't be sent from the
    /// client to the server.
    MultiResult(Vec<Value>),

    /// Map data type is a collection of key-value pairs. Each key can only appear once in a
    /// collection and is associated with a value. Map values can be any supported data
    /// type.
    /// Map keys can only be of type String, Bytes, Integer, and that this will be enforced by the client and server.
    HashMap(HashMap<Value, Value>),

    /// OrderedMap data type where the map entries are sorted based key ordering (K-ordered maps).
    /// Each key can only appear once in a collection and is associated with a value.
    /// Map values can be any supported data type.
    /// Map keys can only be of type String, Bytes, Integer, and that this will be enforced by the client and server.
    OrderedMap(BTreeMap<Value, Value>),

    /// Result of any map operation in which the server returns a
    /// map requested with [MapReturnType::KeyValue].
    KeyValueList(Vec<(Value, Value)>),

    /// GeoJSON data type are JSON formatted strings to encode geo-spatial information.
    GeoJSON(String),

    /// HLL value
    HLL(Vec<u8>),

    /// Infinity Value
    Infinity,

    /// Infinity Value
    Wildcard,
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self {
            Value::Nil => {
                let v: Option<u8> = None;
                v.hash(state);
            }
            Value::Bool(_) => panic!("Booleans cannot be used as map keys."),
            Value::Int(ref val) => val.hash(state),
            Value::Float(_) => panic!("Floats cannot be used as map keys."),
            Value::String(ref val) => val.hash(state),
            Value::GeoJSON(_) => panic!("GeoJson cannot be used as map keys."),
            Value::Blob(ref val) => val.hash(state),
            Value::HLL(_) => panic!("HLL cannot be used as map keys."),
            Value::MultiResult(_) => panic!("MultiValues cannot be used as map keys."),
            Value::List(_) => panic!("Lists cannot be used as map keys."),
            Value::HashMap(_) => panic!("HashMaps cannot be used as map keys."),
            Value::OrderedMap(_) => panic!("OrderedMaps cannot be used as map keys."),
            Value::KeyValueList(_) => panic!("OrderedMaps cannot be used as map keys."),
            Value::Infinity => panic!("Infinity cannot be used as map keys."),
            Value::Wildcard => panic!("Wildcard cannot be used as map keys."),
        }
    }
}

impl Value {
    /// Returns true if this value is the empty value (nil).
    pub const fn is_nil(&self) -> bool {
        matches!(*self, Value::Nil)
    }

    /// Return the particle type for the value used in the wire protocol.
    /// For internal use only.
    pub fn particle_type(&self) -> ParticleType {
        match *self {
            Value::Nil => ParticleType::NULL,
            Value::Int(_) => ParticleType::INTEGER,
            Value::Float(_) => ParticleType::FLOAT,
            Value::String(_) => ParticleType::STRING,
            Value::Blob(_) => ParticleType::BLOB,
            Value::Bool(_) => ParticleType::BOOL,
            Value::MultiResult(_) => ParticleType::LIST,
            Value::List(_) => ParticleType::LIST,
            Value::HashMap(_) => ParticleType::MAP,
            Value::OrderedMap(_) => ParticleType::MAP,
            Value::KeyValueList(_) => ParticleType::MAP,
            Value::GeoJSON(_) => ParticleType::GEOJSON,
            Value::HLL(_) => ParticleType::HLL,
            Value::Infinity => unreachable!(),
            Value::Wildcard => unreachable!(),
        }
    }

    /// Returns a string representation of the value.
    pub fn as_string(&self) -> String {
        match *self {
            Value::Nil => "<null>".to_string(),
            Value::Int(ref val) => val.to_string(),
            Value::Bool(ref val) => val.to_string(),
            Value::Float(ref val) => val.to_string(),
            Value::String(ref val) | Value::GeoJSON(ref val) => val.to_string(),
            Value::Blob(ref val) | Value::HLL(ref val) => format!("{:?}", val),
            Value::MultiResult(ref val) => format!("{:?}", val),
            Value::List(ref val) => format!("{:?}", val),
            Value::HashMap(ref val) => format!("{:?}", val),
            Value::OrderedMap(ref val) => format!("{:?}", val),
            Value::KeyValueList(ref val) => format!("{:?}", val),
            Value::Infinity => "INF".into(),
            Value::Wildcard => "*".into(),
        }
    }

    /// Calculate the size in bytes that the representation on wire for this value will require.
    /// For internal use only.
    pub(crate) fn estimate_size(&self) -> usize {
        match *self {
            Value::Nil => 0,
            Value::Int(_) | Value::Float(_) => 8,
            Value::String(ref s) => s.len(),
            Value::Blob(ref b) => b.len(),
            Value::Bool(_) => 1,
            Value::MultiResult(_) => {
                panic!("MultiValues are only returned as results from the server and never from the client.")
            }
            Value::List(_) | Value::HashMap(_) | Value::OrderedMap(_) => {
                encoder::pack_value(&mut None, self)
            }
            Value::KeyValueList(_) => {
                panic!("The library never passes ordered maps to the server.")
            }
            Value::GeoJSON(ref s) => 1 + 2 + s.len(), // flags + ncells + jsonstr
            Value::HLL(ref h) => h.len(),
            Value::Infinity => 0,
            Value::Wildcard => 0,
        }
    }

    /// Serialize the value into the given buffer.
    /// For internal use only.
    pub(crate) fn write_to(&self, buf: &mut Buffer) -> usize {
        match *self {
            Value::Nil => 0,
            Value::Int(ref val) => buf.write_i64(*val),
            Value::Bool(ref val) => buf.write_bool(*val),
            Value::Float(ref val) => buf.write_f64(f64::from(val)),
            Value::String(ref val) => buf.write_str(val),
            Value::Blob(ref val) | Value::HLL(ref val) => buf.write_bytes(val),
            Value::MultiResult(_) => {
                panic!("MultiValues are only returned as results from the server and never from the client.")
            }
            Value::List(_) | Value::HashMap(_) | Value::OrderedMap(_) => {
                encoder::pack_value(&mut Some(buf), self)
            }
            Value::KeyValueList(_) => {
                panic!("The library never passes ordered maps to the server.")
            }
            Value::GeoJSON(ref val) => buf.write_geo(val),
            Value::Infinity => encoder::pack_infinity(&mut Some(buf)),
            Value::Wildcard => encoder::pack_wildcard(&mut Some(buf)),
        }
    }

    /// Serialize the value as a record key.
    /// For internal use only.
    pub(crate) fn write_key_bytes(&self, h: &mut Ripemd160) -> Result<()> {
        match *self {
            Value::Int(ref val) => {
                let mut buf = [0; 8];
                NetworkEndian::write_i64(&mut buf, *val);
                h.update(&buf);
                Ok(())
            }
            Value::String(ref val) => {
                h.update(val.as_bytes());
                Ok(())
            }
            Value::Blob(ref val) => {
                h.update(val);
                Ok(())
            }
            _ => panic!("Data type is not supported as Key value."),
        }
    }

    /// Order for Value types.
    pub(crate) fn value_type_order(&self) -> u8 {
        match self {
            Value::Nil => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::String(_) => 3,
            Value::List(_) => 4,
            Value::HashMap(_) => 5,
            Value::OrderedMap(_) => 6,
            Value::Blob(_) => 7,
            Value::HLL(_) => 8,
            Value::Float(_) => 9,
            Value::GeoJSON(_) => 10,
            // Just here for completion's sake
            Value::Infinity => 11,
            Value::Wildcard => 12,
            Value::MultiResult(_) => 13,
            Value::KeyValueList(_) => 14,
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.value_type_order().cmp(&other.value_type_order()) {
            Ordering::Equal => {
                // Same type, compare by value
                match (self, other) {
                    (Value::Int(a_val), Value::Int(b_val)) => a_val.cmp(b_val),
                    (Value::String(a_val), Value::String(b_val)) => a_val.cmp(b_val),
                    (Value::GeoJSON(a_val), Value::GeoJSON(b_val)) => a_val.cmp(b_val),
                    (Value::HLL(a_val), Value::HLL(b_val)) => a_val.cmp(b_val),
                    (Value::Blob(a_val), Value::Blob(b_val)) => a_val.cmp(b_val),
                    (Value::Bool(a_val), Value::Bool(b_val)) => a_val.cmp(b_val),
                    (Value::HashMap(ref a_val), Value::HashMap(ref b_val)) => {
                        a_val.len().cmp(&b_val.len())
                    }
                    (Value::OrderedMap(ref a_val), Value::OrderedMap(ref b_val)) => {
                        a_val.len().cmp(&b_val.len())
                    }
                    (Value::KeyValueList(ref a_val), Value::KeyValueList(ref b_val)) => {
                        a_val.len().cmp(&b_val.len())
                    }
                    (Value::Float(a_val), Value::Float(b_val)) => {
                        // Compare float bits for deterministic ordering
                        let a_bits = match a_val {
                            FloatValue::F32(bits) => u64::from(*bits),
                            FloatValue::F64(bits) => *bits,
                        };

                        let b_bits = match b_val {
                            FloatValue::F32(bits) => u64::from(*bits),
                            FloatValue::F64(bits) => *bits,
                        };

                        a_bits.cmp(&b_bits)
                    }
                    _ => Ordering::Greater,
                }
            }

            ord => ord,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.value_type_order().cmp(&other.value_type_order()) {
            Ordering::Equal => {
                // Same type, compare by value
                match (self, other) {
                    (Value::Int(a_val), Value::Int(b_val)) => Some(a_val.cmp(b_val)),
                    (Value::String(a_val), Value::String(b_val)) => Some(a_val.cmp(b_val)),
                    (Value::GeoJSON(a_val), Value::GeoJSON(b_val)) => Some(a_val.cmp(b_val)),
                    (Value::HLL(a_val), Value::HLL(b_val)) => Some(a_val.cmp(b_val)),
                    (Value::Blob(a_val), Value::Blob(b_val)) => Some(a_val.cmp(b_val)),
                    (Value::Bool(a_val), Value::Bool(b_val)) => Some(a_val.cmp(b_val)),
                    (Value::Float(a_val), Value::Float(b_val)) => {
                        // Compare float bits for deterministic ordering
                        let a_bits = match a_val {
                            FloatValue::F32(bits) => u64::from(*bits),
                            FloatValue::F64(bits) => *bits,
                        };

                        let b_bits = match b_val {
                            FloatValue::F32(bits) => u64::from(*bits),
                            FloatValue::F64(bits) => *bits,
                        };

                        Some(a_bits.cmp(&b_bits))
                    }
                    _ => None,
                }
            }

            ord => Some(ord),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
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

impl From<BTreeMap<Value, Value>> for Value {
    fn from(val: BTreeMap<Value, Value>) -> Value {
        Value::OrderedMap(val)
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
        Value::Int(i64::from(val))
    }
}

impl From<u8> for Value {
    fn from(val: u8) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<i16> for Value {
    fn from(val: i16) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<u16> for Value {
    fn from(val: u16) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<u32> for Value {
    fn from(val: u32) -> Value {
        Value::Int(i64::from(val))
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Value {
        Value::Int(val)
    }
}

impl From<u64> for Value {
    fn from(val: u64) -> Value {
        Value::Int(val as i64)
    }
}

impl From<isize> for Value {
    fn from(val: isize) -> Value {
        Value::Int(val as i64)
    }
}

impl From<usize> for Value {
    fn from(val: usize) -> Value {
        Value::Int(val as i64)
    }
}

impl<'a> From<&'a i8> for Value {
    fn from(val: &'a i8) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a u8> for Value {
    fn from(val: &'a u8) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a i16> for Value {
    fn from(val: &'a i16) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a u16> for Value {
    fn from(val: &'a u16) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a i32> for Value {
    fn from(val: &'a i32) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a u32> for Value {
    fn from(val: &'a u32) -> Value {
        Value::Int(i64::from(*val))
    }
}

impl<'a> From<&'a i64> for Value {
    fn from(val: &'a i64) -> Value {
        Value::Int(*val)
    }
}

impl<'a> From<&'a u64> for Value {
    fn from(val: &'a u64) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a isize> for Value {
    fn from(val: &'a isize) -> Value {
        Value::Int(*val as i64)
    }
}

impl<'a> From<&'a usize> for Value {
    fn from(val: &'a usize) -> Value {
        Value::Int(*val as i64)
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
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl<'a> From<&'a Value> for i64 {
    fn from(val: &'a Value) -> i64 {
        match *val {
            Value::Int(val) => val,
            _ => panic!("Value is not an integer to convert."),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::String(v) => Ok(v),
            Value::GeoJSON(v) => Ok(v),
            _ => {
                return Err(format!(
                    "Invalid type conversion from Value::{} to {}",
                    val.particle_type(),
                    std::any::type_name::<Self>()
                ))
            }
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::Blob(v) => Ok(v),
            Value::HLL(v) => Ok(v),
            _ => {
                return Err(format!(
                    "Invalid type conversion from Value::{} to {}",
                    val.particle_type(),
                    std::any::type_name::<Self>()
                ))
            }
        }
    }
}

impl TryFrom<Value> for Vec<Value> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::List(v) => Ok(v),
            Value::MultiResult(v) => Ok(v),
            _ => {
                return Err(format!(
                    "Invalid type conversion from Value::{} to {}",
                    val.particle_type(),
                    std::any::type_name::<Self>()
                ))
            }
        }
    }
}

impl TryFrom<Value> for HashMap<Value, Value> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::HashMap(v) => Ok(v),
            _ => {
                return Err(format!(
                    "Invalid type conversion from Value::{} to {}",
                    val.particle_type(),
                    std::any::type_name::<Self>()
                ))
            }
        }
    }
}

impl TryFrom<Value> for BTreeMap<Value, Value> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::OrderedMap(v) => Ok(v),
            _ => {
                return Err(format!(
                    "Invalid type conversion from Value::{} to {}",
                    val.particle_type(),
                    std::any::type_name::<Self>()
                ))
            }
        }
    }
}

impl TryFrom<Value> for Vec<(Value, Value)> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::KeyValueList(v) => Ok(v),
            _ => {
                return Err(format!(
                    "Invalid type conversion from Value::{} to {}",
                    val.particle_type(),
                    std::any::type_name::<Self>()
                ))
            }
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::Float(v) => Ok(f64::from(v)),
            _ => {
                return Err(format!(
                    "Invalid type conversion from Value::{} to {}",
                    val.particle_type(),
                    std::any::type_name::<Self>()
                ))
            }
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::Bool(v) => Ok(v),
            _ => return Err("Invalid type bool".into()),
        }
    }
}

pub(crate) fn bytes_to_particle(ptype: u8, buf: &mut Buffer, len: usize) -> Result<Value> {
    match ParticleType::from(ptype) {
        ParticleType::NULL => Ok(Value::Nil),
        ParticleType::INTEGER => {
            let val = buf.read_i64(None);
            Ok(Value::Int(val))
        }
        ParticleType::FLOAT => {
            let val = buf.read_f64(None);
            Ok(Value::Float(FloatValue::from(val)))
        }
        ParticleType::STRING => {
            let val = buf.read_str(len)?;
            Ok(Value::String(val))
        }
        ParticleType::GEOJSON => {
            buf.skip(1);
            let ncells = buf.read_i16(None) as usize;
            let header_size: usize = ncells * 8;

            buf.skip(header_size);
            let val = buf.read_str(len - header_size - 3)?;
            Ok(Value::GeoJSON(val))
        }
        ParticleType::BLOB => Ok(Value::Blob(buf.read_blob(len))),
        ParticleType::LIST => {
            let val = decoder::unpack_value_list(buf)?;
            Ok(val)
        }
        ParticleType::MAP => {
            let val = decoder::unpack_value_map(buf)?;
            Ok(val)
        }
        ParticleType::DIGEST => Ok(Value::from("A DIGEST, NOT IMPLEMENTED YET!")),
        ParticleType::LDT => Ok(Value::from("A LDT, NOT IMPLEMENTED YET!")),
        ParticleType::HLL => Ok(Value::HLL(buf.read_blob(len))),
        ParticleType::BOOL => Ok(Value::Bool(buf.read_bool(len))),
    }
}

/// Constructs a new Value from one of the supported native data types.
#[macro_export]
macro_rules! as_val {
    ($val:expr) => {{
        $crate::Value::from($val)
    }};
}

/// Constructs a new `GeoJSON` Value from one of the supported native data types.
#[macro_export]
macro_rules! as_geo {
    ($val:expr) => {{
        $crate::Value::GeoJSON($val.to_owned())
    }};
}

/// Constructs a new Blob Value from one of the supported native data types.
#[macro_export]
macro_rules! as_blob {
    ($val:expr) => {{
        $crate::Value::Blob($val)
    }};
}

/// Constructs a new List Value from a list of one or more native data types.
///
/// # Examples
///
/// Write a list value to a record bin.
///
/// ```rust,edition2018
/// # use aerospike::*;
/// # use std::vec::Vec;
/// # async fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
/// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
/// # let key = as_key!("test", "test", "mykey");
/// let list = as_list!("a", "b", "c");
/// let bin = as_bin!("list", list);
/// client.put(&WritePolicy::default(), &key, &vec![bin]).await.unwrap();
/// # }
/// ```
#[macro_export]
macro_rules! as_list {
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push(as_val!($v));
            )*
            $crate::Value::List(temp_vec)
        }
    };
}

/// Constructs a vector of Values from a list of one or more native data types.
///
/// # Examples
///
/// Execute a user-defined function (UDF) with some arguments.
///
/// ```rust,should_panic,edition2018
/// # use aerospike::*;
/// # use std::vec::Vec;
/// # async fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
/// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
/// # let key = as_key!("test", "test", "mykey");
/// let module = "myUDF";
/// let func = "myFunction";
/// let args = as_values!("a", "b", "c");
/// client.execute_udf(&WritePolicy::default(), &key,
///     &module, &func, Some(&args)).await.unwrap();
/// # }
/// ```
#[macro_export]
macro_rules! as_values {
    ( $( $v:expr),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push(as_val!($v));
            )*
            temp_vec
        }
    };
}

/// Constructs a Map Value from a list of key/value pairs.
///
/// # Examples
///
/// Write a map value to a record bin.
///
/// ```rust,edition2018
/// # use aerospike::*;
/// # async fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
/// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
/// # let key = as_key!("test", "test", "mykey");
/// let map = as_map!("a" => 1, "b" => 2);
/// let bin = as_bin!("map", map);
/// client.put(&WritePolicy::default(), &key, &vec![bin]).await.unwrap();
/// # }
/// ```
#[macro_export]
macro_rules! as_map {
    ( $( $k:expr => $v:expr),* ) => {
        {
            let mut temp_map = std::collections::HashMap::new();
            $(
                temp_map.insert(as_val!($k), as_val!($v));
            )*
            $crate::Value::HashMap(temp_map)
        }
    };
}

/// Constructs an Ordered Map Value from a list of key/value pairs.
///
/// # Examples
///
/// Write a map value to a record bin.
///
/// ```rust,edition2018
/// # use aerospike::*;
/// # async fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
/// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
/// # let key = as_key!("test", "test", "mykey");
/// let map = as_ord_map!("a" => 1, "b" => 2);
/// let bin = as_bin!("map", map);
/// client.put(&WritePolicy::default(), &key, &vec![bin]).await.unwrap();
/// # }
/// ```
#[macro_export]
macro_rules! as_ord_map {
    ( $( $k:expr => $v:expr),* ) => {
        {
            let mut temp_map = std::collections::BTreeMap::new();
            $(
                temp_map.insert(as_val!($k), as_val!($v));
            )*
            $crate::Value::OrderedMap(temp_map)
        }
    };
}

#[cfg(feature = "serialization")]
impl Serialize for Value {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match &self {
            Value::Nil => serializer.serialize_none(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Int(i) => serializer.serialize_i64(*i),
            Value::Float(f) => match f {
                FloatValue::F32(u) => serializer.serialize_f32(f32::from_bits(*u)),
                FloatValue::F64(u) => serializer.serialize_f64(f64::from_bits(*u)),
            },
            Value::String(s) | Value::GeoJSON(s) => serializer.serialize_str(s),
            Value::Blob(b) | Value::HLL(b) => serializer.serialize_bytes(&b[..]),
            Value::List(l) => {
                let mut seq = serializer.serialize_seq(Some(l.len()))?;
                for elem in l {
                    seq.serialize_element(&elem)?;
                }
                seq.end()
            }
            Value::HashMap(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (key, value) in m {
                    map.serialize_entry(&key, &value)?;
                }
                map.end()
            }
            Value::OrderedMap(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (key, value) in m {
                    map.serialize_entry(&key, &value)?;
                }
                map.end()
            }
            Value::KeyValueList(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (key, value) in m {
                    map.serialize_entry(&key, &value)?;
                }
                map.end()
            }
            Value::Infinity => panic!("Infinity cannot be serialized"),
            Value::Wildcard => panic!("Wildcard cannot be serialized"),
            Value::MultiResult(_) => panic!("MultiValue cannot be serialized"),
        }
    }
}

/// Allows either a HashMap or BTreeMap to be passed as arguments to certain methods.
pub trait MapLike<K: Eq, V> {
    fn value(self) -> (Option<HashMap<K, V>>, Option<BTreeMap<K, V>>);
    fn value_as_ref(&self) -> (Option<&HashMap<K, V>>, Option<&BTreeMap<K, V>>);
}

impl<K: Eq + Ord, V> MapLike<K, V> for BTreeMap<K, V> {
    fn value(self) -> (Option<HashMap<K, V>>, Option<BTreeMap<K, V>>) {
        (None, Some(self))
    }

    fn value_as_ref(&self) -> (Option<&HashMap<K, V>>, Option<&BTreeMap<K, V>>) {
        (None, Some(self))
    }
}

impl<K: Eq + Hash, V> MapLike<K, V> for HashMap<K, V> {
    fn value(self) -> (Option<HashMap<K, V>>, Option<BTreeMap<K, V>>) {
        (Some(self), None)
    }

    fn value_as_ref(&self) -> (Option<&HashMap<K, V>>, Option<&BTreeMap<K, V>>) {
        (Some(self), None)
    }
}

#[cfg(test)]
mod tests {
    use super::Value;
    use std::collections::{BTreeMap, HashMap};
    use std::convert::TryInto;

    #[test]
    fn try_into() {
        let _: i64 = Value::Int(42).try_into().unwrap();
        let _: f64 = Value::from(42.1).try_into().unwrap();
        let _: String = Value::String("hello".into()).try_into().unwrap();
        let _: String = Value::GeoJSON(r#"{"type":"Point"}"#.into())
            .try_into()
            .unwrap();
        let _: Vec<u8> = Value::Blob("hello!".into()).try_into().unwrap();
        let _: Vec<u8> = Value::HLL("hello!".into()).try_into().unwrap();
        let _: bool = Value::Bool(false).try_into().unwrap();
        let _: HashMap<Value, Value> = Value::HashMap(HashMap::new()).try_into().unwrap();
        let _: BTreeMap<Value, Value> = Value::OrderedMap(BTreeMap::new()).try_into().unwrap();
        let _: Vec<(Value, Value)> = Value::KeyValueList(Vec::new()).try_into().unwrap();
    }

    #[test]
    fn as_string() {
        assert_eq!(Value::Nil.as_string(), String::from("<null>"));
        assert_eq!(Value::Int(42).as_string(), String::from("42"));
        assert_eq!(Value::Bool(true).as_string(), String::from("true"));
        assert_eq!(Value::from(4.1416).as_string(), String::from("4.1416"));
        assert_eq!(
            as_geo!(r#"{"type":"Point"}"#).as_string(),
            String::from(r#"{"type":"Point"}"#)
        );
    }

    #[test]
    fn as_geo() {
        let string = String::from(r#"{"type":"Point"}"#);
        let str = r#"{"type":"Point"}"#;
        assert_eq!(as_geo!(string), as_geo!(str));
    }

    #[test]
    #[cfg(feature = "serialization")]
    fn serializer() {
        let val: Value = as_list!(
            Value::Nil,
            "0",
            9,
            8,
            7,
            1,
            2.1f64,
            -1,
            as_list!(5, 6, 7, 8, "asd"),
            true,
            false
        );
        let json = serde_json::to_string(&val);
        assert_eq!(
            json.unwrap(),
            "[null,\"0\",9,8,7,1,4611911198408756429,-1,[5,6,7,8,\"asd\"],true,false]",
            "List Serialization failed"
        );

        let val: Value =
            as_map!("a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5, "f" => as_map!("test"=>123));
        let json = serde_json::to_string(&val);
        // We only check for the len of the String because HashMap serialization does not keep the key order. Comparing like the list above is not possible.
        assert_eq!(json.unwrap().len(), 48, "Map Serialization failed");
    }
}
