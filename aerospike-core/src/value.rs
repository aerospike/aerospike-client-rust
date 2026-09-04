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
use crate::errors::{Error, Result};
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

// `f32 -> f64` is lossless and is what the wire already does for an F32 bin,
// so the conversion widens rather than refusing: the public `From`, the
// public `TryFrom<Value>`, and the particle writer all agree on `as_f64`.
impl From<FloatValue> for f64 {
    fn from(val: FloatValue) -> f64 {
        val.as_f64()
    }
}

impl From<&FloatValue> for f64 {
    fn from(val: &FloatValue) -> f64 {
        val.as_f64()
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

impl From<&f64> for FloatValue {
    fn from(val: &f64) -> FloatValue {
        let mut val = *val;
        if val.is_nan() {
            val = f64::NAN;
        } // make all NaNs have the same representation
        FloatValue::F64(val.to_bits())
    }
}

impl FloatValue {
    /// Widen to `f64` for the wire, where every float is an 8-byte double
    /// particle. `f32 -> f64` is lossless (matches the Java client, which
    /// stores both Float and Double bins as doubles).
    pub(crate) const fn as_f64(&self) -> f64 {
        match *self {
            FloatValue::F32(bits) => f32::from_bits(bits) as f64,
            FloatValue::F64(bits) => f64::from_bits(bits),
        }
    }
}

// Narrowing an F64 is a numeric cast of the value, not of its bit pattern:
// `f32::from_bits(bits as u32)` kept the low 32 bits of the double's encoding
// and reinterpreted them as a float, which is unrelated to the number stored.
impl From<FloatValue> for f32 {
    fn from(val: FloatValue) -> f32 {
        match val {
            FloatValue::F32(bits) => f32::from_bits(bits),
            FloatValue::F64(bits) => f64::from_bits(bits) as f32,
        }
    }
}

impl From<&FloatValue> for f32 {
    fn from(val: &FloatValue) -> f32 {
        match *val {
            FloatValue::F32(bits) => f32::from_bits(bits),
            FloatValue::F64(bits) => f64::from_bits(bits) as f32,
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

impl From<&f32> for FloatValue {
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
                write!(f, "{val}")
            }
            FloatValue::F64(val) => {
                let val: f64 = f64::from_bits(val);
                write!(f, "{val}")
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

    /// `OrderedMap` data type where the map entries are sorted based key ordering (K-ordered maps).
    /// Each key can only appear once in a collection and is associated with a value.
    /// Map values can be any supported data type.
    /// Map keys can only be of type String, Bytes, Integer, and that this will be enforced by the client and server.
    OrderedMap(BTreeMap<Value, Value>),

    /// Result of any map operation in which the server returns a
    /// map requested with [`MapReturnType::KeyValue`].
    KeyValueList(Vec<(Value, Value)>),

    /// `GeoJSON` data type are JSON formatted strings to encode geo-spatial information.
    GeoJSON(String),

    /// HLL value
    HLL(Vec<u8>),

    /// Infinity Value
    Infinity,

    /// Infinity Value
    Wildcard,
}

#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match *self {
            #[allow(clippy::collection_is_never_read)]
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
            Value::OrderedMap(_) | Value::KeyValueList(_) => {
                panic!("OrderedMaps cannot be used as map keys.")
            }
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
    /// # Errors
    ///
    /// [`Infinity`](Value::Infinity) and [`Wildcard`](Value::Wildcard) have no
    /// particle type: they exist only inside msgpack payloads, as CDT and
    /// expression bounds, where the encoder writes them directly and never asks
    /// for a particle code. Reaching here means one was handed to the client as
    /// an ordinary bin value or record key, which is a caller mistake — Java
    /// reports the same case as `PARAMETER_ERROR` from `Value.getType()`.
    ///
    /// This used to be `unreachable!()`, so either value killed the process.
    /// The panic is the return path here: there is no `ParticleType` that
    /// safely means "has no particle type", and a placeholder would trade a
    /// loud abort for a silent buffer-size mismatch, since `estimate_size`
    /// reports 0 for these while `write_to` packs real bytes. Failing in
    /// `particle_type`, which every particle path calls before `write_to`, is
    /// what keeps that unreachable.
    pub(crate) fn particle_type(&self) -> Result<ParticleType> {
        let ptype = match *self {
            Value::Nil => ParticleType::NULL,
            Value::Int(_) => ParticleType::INTEGER,
            Value::Float(_) => ParticleType::FLOAT,
            Value::String(_) => ParticleType::STRING,
            Value::Blob(_) => ParticleType::BLOB,
            Value::Bool(_) => ParticleType::BOOL,
            Value::MultiResult(_) | Value::List(_) => ParticleType::LIST,
            Value::HashMap(_) | Value::OrderedMap(_) | Value::KeyValueList(_) => ParticleType::MAP,
            Value::GeoJSON(_) => ParticleType::GEOJSON,
            Value::HLL(_) => ParticleType::HLL,
            Value::Infinity => {
                return Err(Error::InvalidArgument(
                    "Invalid particle type: INF. Infinity is only valid inside a \
                     collection or expression bound, not as a bin value or key."
                        .to_string(),
                ))
            }
            Value::Wildcard => {
                return Err(Error::InvalidArgument(
                    "Invalid particle type: wildcard. A wildcard is only valid inside \
                     a collection or expression bound, not as a bin value or key."
                        .to_string(),
                ))
            }
        };

        Ok(ptype)
    }

    /// Short label naming this value's type, for diagnostics.
    ///
    /// Unlike [`particle_type`](Self::particle_type) this never fails, so error
    /// messages about an unexpected value can name it even when the value is one
    /// that has no particle type at all.
    pub(crate) const fn type_label(&self) -> &'static str {
        match *self {
            Value::Nil => "nil",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::String(_) => "string",
            Value::Blob(_) => "blob",
            Value::List(_) => "list",
            Value::MultiResult(_) => "multi-result",
            Value::HashMap(_) => "map",
            Value::OrderedMap(_) => "ordered map",
            Value::KeyValueList(_) => "key-value list",
            Value::GeoJSON(_) => "geo-json",
            Value::HLL(_) => "hll",
            Value::Infinity => "INF",
            Value::Wildcard => "wildcard",
        }
    }

    /// Returns a string representation of the value.
    pub fn as_string(&self) -> String {
        match *self {
            Value::Nil => "<null>".to_string(),
            Value::Int(ref val) => val.to_string(),
            Value::Bool(ref val) => val.to_string(),
            Value::Float(ref val) => val.to_string(),
            Value::String(ref val) | Value::GeoJSON(ref val) => val.clone(),
            Value::Blob(ref val) | Value::HLL(ref val) => format!("{val:?}"),
            Value::MultiResult(ref val) | Value::List(ref val) => format!("{val:?}"),
            Value::HashMap(ref val) => format!("{val:?}"),
            Value::OrderedMap(ref val) => format!("{val:?}"),
            Value::KeyValueList(ref val) => format!("{val:?}"),
            Value::Infinity => "INF".into(),
            Value::Wildcard => "*".into(),
        }
    }

    /// Calculate the size in bytes that the representation on wire for this value will require.
    /// For internal use only.
    pub(crate) fn estimate_size(&self) -> Result<usize> {
        let res = match *self {
            Value::Int(_) | Value::Float(_) => 8,
            Value::String(ref s) => s.len(),
            Value::Blob(ref b) => b.len(),
            Value::Bool(_) => 1,
            Value::MultiResult(_) => {
                return Err(Error::InvalidArgument("MultiValues are only returned as results from the server and never from the client.".into()));
            }
            Value::List(_) | Value::HashMap(_) | Value::OrderedMap(_) => {
                encoder::pack_value(&mut None, self)?
            }
            Value::KeyValueList(_) => {
                return Err(Error::InvalidArgument(
                    "The library never passes ordered maps to the server.".into(),
                ));
            }
            Value::GeoJSON(ref s) => 1 + 2 + s.len(), // flags + ncells + jsonstr
            Value::HLL(ref h) => h.len(),
            Value::Nil | Value::Infinity | Value::Wildcard => 0,
        };

        Ok(res)
    }

    /// Serialize the value into the given buffer.
    /// For internal use only.
    pub(crate) fn write_to(&self, buf: &mut Buffer) -> Result<usize> {
        let res = match *self {
            Value::Nil => 0,
            Value::Int(ref val) => buf.write_i64(*val),
            Value::Bool(ref val) => buf.write_bool(*val),
            Value::Float(ref val) => buf.write_f64(val.as_f64()),
            Value::String(ref val) => buf.write_str(val),
            Value::Blob(ref val) | Value::HLL(ref val) => buf.write_bytes(val),
            Value::MultiResult(_) => {
                return Err(Error::InvalidArgument("MultiValues are only returned as results from the server and never from the client.".into()));
            }
            Value::List(_) | Value::HashMap(_) | Value::OrderedMap(_) => {
                encoder::pack_value(&mut Some(buf), self)?
            }
            Value::KeyValueList(_) => {
                return Err(Error::InvalidArgument(
                    "The library never passes ordered maps to the server.".into(),
                ));
            }
            Value::GeoJSON(ref val) => buf.write_geo(val),
            Value::Infinity => encoder::pack_infinity(&mut Some(buf)),
            Value::Wildcard => encoder::pack_wildcard(&mut Some(buf)),
        };

        Ok(res)
    }

    /// Serialize the value as a record key.
    /// For internal use only.
    pub(crate) fn write_key_bytes(&self, h: &mut Ripemd160) -> Result<()> {
        match *self {
            Value::Int(ref val) => {
                let mut buf = [0; 8];
                NetworkEndian::write_i64(&mut buf, *val);
                h.update(buf);
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
            _ => Err(Error::InvalidArgument(
                "Data type is not supported as Key value.".into(),
            )),
        }
    }

    /// Order for Value types.
    pub(crate) const fn value_type_order(&self) -> u8 {
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
                    (Value::String(a_val), Value::String(b_val))
                    | (Value::GeoJSON(a_val), Value::GeoJSON(b_val)) => a_val.cmp(b_val),
                    (Value::HLL(a_val), Value::HLL(b_val))
                    | (Value::Blob(a_val), Value::Blob(b_val)) => a_val.cmp(b_val),
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
        Some(self.cmp(other))
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

/// The server has no native u64 type: values that fit in an `i64` are stored
/// losslessly as `INTEGER`, but a value past `i64::MAX` would silently wrap to a
/// negative integer (`u64::MAX` becomes `-1`), corrupting a key or bin. Reject
/// that case rather than store the wrong value.
fn u64_to_int_value(val: u64) -> Value {
    assert!(
        val <= i64::MAX as u64,
        // Edition 2018: `assert!` does not capture `{val}`, so pass it positionally.
        "Aerospike does not support u64 natively on server-side. Values up to \
         i64::MAX store as INTEGER; {} is larger, so cast it explicitly or use a \
         string/blob representation.",
        val
    );
    Value::Int(val as i64)
}

impl From<u64> for Value {
    fn from(val: u64) -> Value {
        u64_to_int_value(val)
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
        u64_to_int_value(*val)
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
            Value::String(v) | Value::GeoJSON(v) => Ok(v),
            _ => Err(format!(
                "Invalid type conversion from Value::{} to {}",
                val.type_label(),
                std::any::type_name::<Self>()
            )),
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::Blob(v) | Value::HLL(v) => Ok(v),
            _ => Err(format!(
                "Invalid type conversion from Value::{} to {}",
                val.type_label(),
                std::any::type_name::<Self>()
            )),
        }
    }
}

impl TryFrom<Value> for Vec<Value> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::List(v) | Value::MultiResult(v) => Ok(v),
            _ => Err(format!(
                "Invalid type conversion from Value::{} to {}",
                val.type_label(),
                std::any::type_name::<Self>()
            )),
        }
    }
}

#[allow(clippy::implicit_hasher)]
impl TryFrom<Value> for HashMap<Value, Value> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::HashMap(v) => Ok(v),
            _ => Err(format!(
                "Invalid type conversion from Value::{} to {}",
                val.type_label(),
                std::any::type_name::<Self>()
            )),
        }
    }
}

impl TryFrom<Value> for BTreeMap<Value, Value> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::OrderedMap(v) => Ok(v),
            _ => Err(format!(
                "Invalid type conversion from Value::{} to {}",
                val.type_label(),
                std::any::type_name::<Self>()
            )),
        }
    }
}

impl TryFrom<Value> for Vec<(Value, Value)> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::KeyValueList(v) => Ok(v),
            _ => Err(format!(
                "Invalid type conversion from Value::{} to {}",
                val.type_label(),
                std::any::type_name::<Self>()
            )),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::Float(v) => Ok(v.as_f64()),
            _ => Err(format!(
                "Invalid type conversion from Value::{} to {}",
                val.type_label(),
                std::any::type_name::<Self>()
            )),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::Bool(v) => Ok(v),
            _ => Err("Invalid type bool".into()),
        }
    }
}

pub fn bytes_to_particle(ptype: u8, buf: &mut Buffer, len: usize) -> Result<Value> {
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
/// # #[tokio::main]
/// # async fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
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
///  # #[tokio::main]
/// # async fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
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
/// # #[tokio::main]
/// # async fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
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
/// # #[tokio::main]
/// # async fn main() {
/// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
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

/// Allows either a `HashMap` or `BTreeMap` to be passed as arguments to certain methods.
#[allow(clippy::type_complexity)]
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

    /// Narrowing an F64 to f32 must cast the number, not its bit pattern. The
    /// old `f32::from_bits(bits as u32)` kept the low 32 bits of the double's
    /// encoding: for 2.25 (0x4002_0000_0000_0000) those are all zero, so it
    /// returned 0.0 for a value that is exactly representable in f32.
    #[test]
    fn f64_float_values_narrow_to_f32_by_value_not_by_bits() {
        let exact = crate::value::FloatValue::from(2.25f64);
        assert_eq!(f32::from(exact.clone()), 2.25f32);
        assert_eq!(f32::from(&exact), 2.25f32);
        // The bit-truncation the old code performed, shown to be a different answer.
        assert_eq!(f32::from_bits(2.25f64.to_bits() as u32), 0.0f32);

        // A value that is not exact in f32 rounds the way `as f32` does.
        let inexact = crate::value::FloatValue::from(0.1f64);
        assert_eq!(f32::from(&inexact), 0.1f64 as f32);

        // F32 -> f32 is untouched.
        let single = crate::value::FloatValue::from(1.5f32);
        assert_eq!(f32::from(single.clone()), 1.5f32);
        assert_eq!(f32::from(&single), 1.5f32);
    }

    /// `f32 -> f64` is lossless, and an F32 bin is already written to the
    /// server as a double, so every public conversion must widen the same
    /// way. The `From` impls used to panic on F32 while `TryFrom<Value>` and
    /// the wire path widened.
    #[test]
    fn f32_float_values_widen_to_f64_on_every_path() {
        let f32_val = crate::value::FloatValue::from(1.5f32);
        let f64_val = crate::value::FloatValue::from(2.25f64);

        assert_eq!(f64::from(f32_val.clone()), 1.5);
        assert_eq!(f64::from(&f32_val), 1.5);
        assert_eq!(f64::from(f64_val.clone()), 2.25);
        assert_eq!(f64::from(&f64_val), 2.25);

        // ...and agree with the fallible Value conversion and the wire helper.
        assert_eq!(f64::try_from(crate::Value::Float(f32_val.clone())), Ok(1.5));
        assert_eq!(f64::from(&f32_val), f32_val.as_f64());

        // Widening is exact: the f32's value round-trips bit-for-bit through f64.
        let x = 0.1f32;
        assert_eq!(f64::from(crate::value::FloatValue::from(x)), f64::from(x));
    }
    use super::Value;
    use crate::commands::ParticleType;
    use crate::errors::Error;
    use std::collections::{BTreeMap, HashMap};
    use std::convert::{TryFrom, TryInto};

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
            "[null,\"0\",9,8,7,1,2.1,-1,[5,6,7,8,\"asd\"],true,false]",
            "List Serialization failed"
        );

        let val: Value =
            as_map!("a" => 1, "b" => 2, "c" => 3, "d" => 4, "e" => 5, "f" => as_map!("test"=>123));
        let json = serde_json::to_string(&val);
        // We only check for the len of the String because HashMap serialization does not keep the key order. Comparing like the list above is not possible.
        assert_eq!(json.unwrap().len(), 48, "Map Serialization failed");
    }

    #[test]
    fn particle_type_rejects_infinity_and_wildcard() {
        // These have no particle type: they are msgpack-only bounds. Asking for
        // one used to abort the process through `unreachable!()`.
        for value in [Value::Infinity, Value::Wildcard] {
            let err = value
                .particle_type()
                .expect_err("INF/wildcard have no particle type");
            assert!(
                matches!(err, Error::InvalidArgument(_)),
                "expected InvalidArgument for {:?}, got {:?}",
                value,
                err
            );
        }
    }

    #[test]
    fn particle_type_still_answers_for_every_storable_value() {
        // The guard must not have swallowed the ordinary cases.
        for (value, want) in [
            (Value::Nil, ParticleType::NULL),
            (Value::Bool(true), ParticleType::BOOL),
            (Value::Int(1), ParticleType::INTEGER),
            (Value::from(1.5_f64), ParticleType::FLOAT),
            (Value::from("s"), ParticleType::STRING),
            (Value::Blob(vec![1]), ParticleType::BLOB),
            (Value::List(vec![Value::Int(1)]), ParticleType::LIST),
            (Value::HLL(vec![1]), ParticleType::HLL),
            (Value::GeoJSON("{}".to_string()), ParticleType::GEOJSON),
        ] {
            assert_eq!(
                value.particle_type().unwrap() as u8,
                want as u8,
                "wrong particle type for {:?}",
                value
            );
        }

        let map: Value = as_map!("a" => 1);
        assert_eq!(map.particle_type().unwrap() as u8, ParticleType::MAP as u8);
    }

    #[test]
    fn type_label_names_every_variant_including_the_typeless_ones() {
        // Used by the TryFrom error messages, which must be able to name a value
        // that has no particle type at all.
        assert_eq!(Value::Infinity.type_label(), "INF");
        assert_eq!(Value::Wildcard.type_label(), "wildcard");
        assert_eq!(Value::Int(1).type_label(), "int");
        assert_eq!(Value::Nil.type_label(), "nil");

        // And the messages themselves still render.
        let err = String::try_from(Value::Infinity).expect_err("INF is not a string");
        assert!(err.contains("INF"), "unhelpful message: {}", err);
    }
}
