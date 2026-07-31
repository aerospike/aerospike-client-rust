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

use indexmap::IndexMap;
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
use crate::vector::Vector;

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

impl From<&FloatValue> for f64 {
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
    pub(crate) fn as_f64(&self) -> f64 {
        match *self {
            FloatValue::F32(bits) => f64::from(f32::from_bits(bits)),
            FloatValue::F64(bits) => f64::from_bits(bits),
        }
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

impl From<&FloatValue> for f32 {
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
///
/// # Ordering
///
/// [`Value`] implements a total order that matches the server's
/// canonical value ordering (verified empirically against server 8.1
/// via the list [`sort`](crate::operations::lists::sort) operation and
/// K-ordered map key placement). Types rank
/// `Nil < Bool < Int < String < List < Map < Bytes < Float < GeoJSON`;
/// see the [`Ord`] impl for the within-type rules. Sorting a
/// `Vec<Value>` client-side therefore produces the same order the
/// server uses for list sort, ranks, and K-ordered map keys.
///
/// # Map variants
///
/// Three variants represent maps, differing only in the client-side
/// collection (and the wire order flag for [`Value::SortedMap`]):
/// [`Value::HashMap`] (unordered), [`Value::OrderedMap`]
/// (insertion-ordered), and [`Value::SortedMap`] (key-sorted,
/// K-ordered on the server). The variants compare equal by *content*
/// ([`PartialEq`]), every map-taking API accepts all three (see
/// [`MapLike`](crate::MapLike)), and maps returned by the server decode
/// as `OrderedMap` preserving the exact pair order the server sent.
#[derive(Debug, Clone)]
pub enum Value {
    /// Empty value.
    Nil,

    /// Boolean value.
    Bool(bool),

    /// Integer value. All integers are represented as 64-bit numerics in Aerospike.
    Int(i64),

    /// Floating point value. All floating point values are stored in 64-bit IEEE-754 format in
    /// Aerospike. Aerospike server v3.6.0 and later support double data type.
    ///
    /// In the server's canonical value ordering, floats are a separate
    /// type ranked AFTER byte blobs: `Int(2)` and `Float(2.0)` never
    /// interleave when sorted.
    Float(FloatValue),

    /// String value.
    String(String),

    /// Byte array value.
    Blob(Vec<u8>),

    /// List data type is an ordered collection of values. Lists can contain values of any
    /// supported data type. List data order is maintained on writes and reads.
    ///
    /// In the server's canonical value ordering (list sort, ranks),
    /// lists compare element-wise with a shorter prefix ordering first
    /// (`[] < [0,9] < [1] < [1,2]`) — the same order `Vec<Value>`'s
    /// `Ord` produces.
    List(Vec<Value>),

    /// Returned in cases where the server executes multiple operations for the same bin.
    /// This value is only sent from the server to the client, and can't be sent from the
    /// client to the server.
    MultiResult(Vec<Value>),

    /// Unordered map: a collection of key-value pairs with no defined
    /// entry order. Each key can only appear once; values can be any
    /// supported data type. Map keys can only be of type String, Bytes
    /// or Integer (keys sort `Int < String < Blob` on the server), and
    /// this is enforced by the client and server.
    ///
    /// Written to the wire as an unordered map in arbitrary pair order.
    /// The server never returns this variant — maps decode as
    /// [`Value::OrderedMap`] (or [`Value::SortedMap`] for K-ordered
    /// returns) — but it compares content-equal with both.
    HashMap(HashMap<Value, Value>),

    /// Insertion-ordered map: entries keep the order in which they were
    /// inserted. Each key can only appear once; values can be any
    /// supported data type. Map keys can only be of type String, Bytes
    /// or Integer, and this is enforced by the client and server.
    ///
    /// The server has no insertion-ordered map type: on the wire this is
    /// an *unordered* map whose pairs are written in insertion order
    /// (deterministic encoding). This is also the variant every
    /// server-returned non-K-ordered map decodes into, preserving the
    /// exact pair order the server sent. Note that the server (verified
    /// on 8.1) returns map entries in canonical key order regardless of
    /// creation path — plain bin writes AND CDT maps created with an
    /// explicitly unordered [`MapOrder`](crate::operations::MapOrder)
    /// alike — so a written insertion order never survives a
    /// round-trip; an unordered map merely comes back without the
    /// K-ordered wire flag (this variant instead of
    /// [`Value::SortedMap`]).
    OrderedMap(IndexMap<Value, Value>),

    /// Key-sorted map (K-ordered on the server): entries are sorted by
    /// key in the server's canonical key order. Each key can only appear
    /// once; values can be any supported data type. Map keys can only be
    /// of type String, Bytes or Integer, and this is enforced by the
    /// client and server.
    ///
    /// Written to the wire with the K-ordered flag; K-ordered map
    /// returns from the server decode as this variant.
    ///
    /// K-ordered maps are the comparable form for whole-map filter
    /// expressions (server 6.3+): a K-ordered bin can be compared
    /// against a `SortedMap` literal with `eq`/`lt`/etc., following the
    /// canonical map order (length first, then entry-wise). Unordered
    /// operands on either side are not comparable before server
    /// AER-6930 (8.1.2.3).
    SortedMap(BTreeMap<Value, Value>),

    /// Result of any map operation in which the server returns a
    /// map requested with [`MapReturnType::KeyValue`].
    KeyValueList(Vec<(Value, Value)>),

    /// `GeoJSON` data type are JSON formatted strings to encode geo-spatial information.
    GeoJSON(String),

    /// HLL value
    HLL(Vec<u8>),

    /// A dense numeric vector for vector similarity search. Encoded with the
    /// `VECTOR` particle type; see [`Vector`](crate::Vector).
    Vector(Vector),

    /// Infinity Value
    Infinity,

    /// Infinity Value
    Wildcard,

    /// Unknown Value signifies values whose wire particle type this client
    /// does not interpret (e.g. legacy language-specific serializations
    /// like Java/C#/Python blobs). Carries the raw particle-type code and
    /// the raw payload bytes, uninterpreted.
    ///
    /// Strictly read-only: it is rejected on every path that would send it
    /// to the server — as a bin value, inside lists/maps/CDT arguments, as
    /// a record key ([`Key::new`](crate::Key::new) fails), in query
    /// [`Filter`](crate::query::Filter)s (the filter-value conversion
    /// panics, like other non-indexable types), and in expression literals
    /// (packing the expression fails).
    Unknown(u8, Vec<u8>),
}

/// The three map variants ([`Value::HashMap`], [`Value::OrderedMap`],
/// [`Value::SortedMap`]) compare equal by *content*, regardless of which
/// collection carries them — the server may return any representation
/// (unordered wire maps decode as `OrderedMap` to preserve return
/// order), and two maps with the same entries are the same map. All
/// other variants compare structurally, like the previous derived impl.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        fn entries_eq<'a, A>(len: usize, entries: A, other: &Value) -> bool
        where
            A: Iterator<Item = (&'a Value, &'a Value)>,
        {
            let other_len = match other {
                Value::HashMap(m) => m.len(),
                Value::OrderedMap(m) => m.len(),
                Value::SortedMap(m) => m.len(),
                _ => return false,
            };
            if len != other_len {
                return false;
            }
            let get = |k: &Value| -> Option<&Value> {
                match other {
                    Value::HashMap(m) => m.get(k),
                    Value::OrderedMap(m) => m.get(k),
                    Value::SortedMap(m) => m.get(k),
                    _ => None,
                }
            };
            entries.into_iter().all(|(k, v)| get(k) == Some(v))
        }

        match (self, other) {
            (Value::Nil, Value::Nil)
            | (Value::Infinity, Value::Infinity)
            | (Value::Wildcard, Value::Wildcard) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) | (Value::GeoJSON(a), Value::GeoJSON(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) | (Value::HLL(a), Value::HLL(b)) => a == b,
            (Value::List(a), Value::List(b)) | (Value::MultiResult(a), Value::MultiResult(b)) => {
                a == b
            }
            (Value::KeyValueList(a), Value::KeyValueList(b)) => a == b,
            (Value::Vector(a), Value::Vector(b)) => a == b,
            (Value::Unknown(t1, b1), Value::Unknown(t2, b2)) => t1 == t2 && b1 == b2,
            (Value::HashMap(a), b) => entries_eq(a.len(), a.iter(), b),
            (Value::OrderedMap(a), b) => entries_eq(a.len(), a.iter(), b),
            (Value::SortedMap(a), b) => entries_eq(a.len(), a.iter(), b),
            _ => false,
        }
    }
}

impl Eq for Value {}

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
            Value::Vector(_) => panic!("Vectors cannot be used as map keys."),
            Value::MultiResult(_) => panic!("MultiValues cannot be used as map keys."),
            Value::List(_) => panic!("Lists cannot be used as map keys."),
            Value::HashMap(_) => panic!("HashMaps cannot be used as map keys."),
            Value::OrderedMap(_) => panic!("OrderedMaps cannot be used as map keys."),
            Value::SortedMap(_) | Value::KeyValueList(_) => {
                panic!("SortedMaps cannot be used as map keys.")
            }
            Value::Infinity => panic!("Infinity cannot be used as map keys."),
            Value::Wildcard => panic!("Wildcard cannot be used as map keys."),
            Value::Unknown(..) => panic!("Unknown values cannot be used as map keys."),
        }
    }
}

impl Value {
    /// Returns true if this value is the empty value (nil).
    pub const fn is_nil(&self) -> bool {
        matches!(*self, Value::Nil)
    }

    /// Return the wire particle-type code for the value. Returns the raw
    /// `u8` so that [`Value::Unknown`] can carry codes the client does not
    /// interpret. For internal use only.
    ///
    /// # Errors
    ///
    /// [`Infinity`](Value::Infinity) and [`Wildcard`](Value::Wildcard) have no
    /// particle type: they exist only inside msgpack payloads, as CDT and
    /// expression bounds, where [`msgpack::encoder::pack_value`] writes them
    /// directly and never asks for a particle code. Reaching here means one was
    /// handed to the client as an ordinary bin value or record key, which is a
    /// caller mistake — Java reports the same case as `PARAMETER_ERROR` from
    /// `Value.getType()`, and so does this.
    pub fn particle_type(&self) -> Result<u8> {
        let code = match *self {
            Value::Nil => ParticleType::NULL as u8,
            Value::Int(_) => ParticleType::INTEGER as u8,
            Value::Float(_) => ParticleType::FLOAT as u8,
            Value::String(_) => ParticleType::STRING as u8,
            Value::Blob(_) => ParticleType::BLOB as u8,
            Value::Bool(_) => ParticleType::BOOL as u8,
            Value::MultiResult(_) | Value::List(_) => ParticleType::LIST as u8,
            Value::HashMap(_)
            | Value::OrderedMap(_)
            | Value::SortedMap(_)
            | Value::KeyValueList(_) => ParticleType::MAP as u8,
            Value::GeoJSON(_) => ParticleType::GEOJSON as u8,
            Value::HLL(_) => ParticleType::HLL as u8,
            Value::Vector(_) => ParticleType::VECTOR as u8,
            Value::Unknown(code, _) => code,
            Value::Infinity => {
                return Err(Error::invalid_argument(
                    "Invalid particle type: INF. Infinity is only valid inside a \
                     collection or expression bound, not as a bin value or key.",
                ))
            }
            Value::Wildcard => {
                return Err(Error::invalid_argument(
                    "Invalid particle type: wildcard. A wildcard is only valid inside \
                     a collection or expression bound, not as a bin value or key.",
                ))
            }
        };

        Ok(code)
    }

    /// Short label naming this value's type, for diagnostics.
    ///
    /// Unlike [`particle_type`](Self::particle_type) this never fails, so
    /// error messages about an unexpected value can name it even when the
    /// value is one that has no particle type at all.
    pub(crate) fn type_label(&self) -> String {
        match *self {
            Value::Nil => "nil".to_string(),
            Value::Bool(_) => "bool".to_string(),
            Value::Int(_) => "int".to_string(),
            Value::Float(_) => "float".to_string(),
            Value::String(_) => "string".to_string(),
            Value::Blob(_) => "blob".to_string(),
            Value::List(_) => "list".to_string(),
            Value::MultiResult(_) => "multi-result".to_string(),
            Value::HashMap(_) => "map".to_string(),
            Value::OrderedMap(_) => "ordered map".to_string(),
            Value::SortedMap(_) => "sorted map".to_string(),
            Value::KeyValueList(_) => "key-value list".to_string(),
            Value::GeoJSON(_) => "geo-json".to_string(),
            Value::HLL(_) => "hll".to_string(),
            Value::Vector(_) => "vector".to_string(),
            Value::Infinity => "INF".to_string(),
            Value::Wildcard => "wildcard".to_string(),
            Value::Unknown(code, _) => {
                format!("unknown particle {}({code})", ParticleType::name_of(code))
            }
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
            Value::SortedMap(ref val) => format!("{val:?}"),
            Value::KeyValueList(ref val) => format!("{val:?}"),
            Value::Vector(ref val) => val.to_string(),
            Value::Infinity => "INF".into(),
            Value::Wildcard => "*".into(),
            Value::Unknown(code, ref bytes) => {
                format!(
                    "<unknown particle {}({code}), {} bytes>",
                    ParticleType::name_of(code),
                    bytes.len()
                )
            }
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
                return Err(Error::invalid_argument("MultiValues are only returned as results from the server and never from the client."));
            }
            Value::List(_) | Value::HashMap(_) | Value::OrderedMap(_) | Value::SortedMap(_) => {
                encoder::pack_value(&mut None, self)?
            }
            Value::KeyValueList(_) => {
                return Err(Error::invalid_argument(
                    "The library never passes ordered maps to the server.",
                ));
            }
            Value::GeoJSON(ref s) => 1 + 2 + s.len(), // flags + ncells + jsonstr
            Value::HLL(ref h) => h.len(),
            Value::Vector(ref v) => v.wire_size(),
            Value::Nil | Value::Infinity | Value::Wildcard => 0,
            Value::Unknown(code, _) => {
                return Err(Error::invalid_argument(format!(
                    "Unknown values (particle type {}({code})) hold data this client \
                     cannot interpret and cannot be written back to the server.",
                    ParticleType::name_of(code)
                )));
            }
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
                return Err(Error::invalid_argument("MultiValues are only returned as results from the server and never from the client."));
            }
            Value::List(_) | Value::HashMap(_) | Value::OrderedMap(_) | Value::SortedMap(_) => {
                encoder::pack_value(&mut Some(buf), self)?
            }
            Value::KeyValueList(_) => {
                return Err(Error::invalid_argument(
                    "The library never passes ordered maps to the server.",
                ));
            }
            Value::GeoJSON(ref val) => buf.write_geo(val),
            Value::Vector(ref val) => val.write_to(buf),
            Value::Infinity => encoder::pack_infinity(&mut Some(buf)),
            Value::Wildcard => encoder::pack_wildcard(&mut Some(buf)),
            Value::Unknown(code, _) => {
                return Err(Error::invalid_argument(format!(
                    "Unknown values (particle type {}({code})) hold data this client \
                     cannot interpret and cannot be written back to the server.",
                    ParticleType::name_of(code)
                )));
            }
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
            _ => Err(Error::invalid_argument(
                "Data type is not supported as Key value.",
            )),
        }
    }

    /// Order rank for Value types, matching the server's canonical
    /// (msgpack) type ordering as determined empirically against server
    /// 8.1 (list `sort` operation and K-ordered map key order):
    /// `Nil < Bool < Int < String < List < Map < Bytes < Float < GeoJSON`.
    /// All three map variants share one rank — the server has a single
    /// MAP type (this also keeps `Ord` consistent with the content-based
    /// map equality in `PartialEq`). Variants the server never orders
    /// (HLL, Infinity, Wildcard, `MultiResult`, `KeyValueList`) rank
    /// after the probed types.
    pub(crate) const fn value_type_order(&self) -> u8 {
        match self {
            Value::Nil => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::String(_) => 3,
            Value::List(_) => 4,
            Value::HashMap(_) | Value::OrderedMap(_) | Value::SortedMap(_) => 5,
            Value::Blob(_) => 6,
            Value::HLL(_) => 7,
            Value::Float(_) => 8,
            Value::GeoJSON(_) => 9,
            Value::Infinity => 10,
            Value::Wildcard => 11,
            Value::MultiResult(_) => 12,
            Value::KeyValueList(_) => 13,
            Value::Unknown(..) => 14,
            Value::Vector(_) => 15,
        }
    }
}

/// Total ordering matching the server's canonical value ordering,
/// verified empirically against server 8.1 via the list `sort`
/// operation and K-ordered map key placement:
///
/// - types rank `Nil < Bool < Int < String < List < Map < Bytes <
///   Float < GeoJSON` (floats are a separate type AFTER bytes — the
///   server does not interleave `2` and `2.0`);
/// - bools order `false < true`; ints and floats numerically; strings,
///   blobs and `GeoJSON` bytewise;
/// - lists compare element-wise, a shorter prefix ordering first
///   (`[] < [0,9] < [1] < [1,2]`);
/// - maps compare by LENGTH first, then entry-wise over key-ordered
///   pairs (`{} < {a:1} < {b:0} < {a:1,b:2}`), identically for all
///   three map variants.
impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        /// Map entries in key order, for any map variant.
        fn sorted_entries(v: &Value) -> Vec<(&Value, &Value)> {
            let mut entries: Vec<(&Value, &Value)> = match v {
                Value::HashMap(m) => m.iter().collect(),
                Value::OrderedMap(m) => m.iter().collect(),
                Value::SortedMap(m) => m.iter().collect(),
                _ => Vec::new(),
            };
            entries.sort_by(|a, b| a.0.cmp(b.0));
            entries
        }

        const fn is_map(v: &Value) -> bool {
            matches!(
                v,
                Value::HashMap(_) | Value::OrderedMap(_) | Value::SortedMap(_)
            )
        }

        match self.value_type_order().cmp(&other.value_type_order()) {
            Ordering::Equal => {
                // Same server type: compare by value.
                match (self, other) {
                    (Value::Int(a_val), Value::Int(b_val)) => a_val.cmp(b_val),
                    (Value::String(a_val), Value::String(b_val))
                    | (Value::GeoJSON(a_val), Value::GeoJSON(b_val)) => a_val.cmp(b_val),
                    (Value::HLL(a_val), Value::HLL(b_val))
                    | (Value::Blob(a_val), Value::Blob(b_val)) => a_val.cmp(b_val),
                    (Value::Bool(a_val), Value::Bool(b_val)) => a_val.cmp(b_val),
                    // Element-wise, like the server (Vec's lexicographic
                    // Ord: shorter prefix first).
                    (Value::List(a_val), Value::List(b_val))
                    | (Value::MultiResult(a_val), Value::MultiResult(b_val)) => a_val.cmp(b_val),
                    (Value::KeyValueList(a_val), Value::KeyValueList(b_val)) => a_val.cmp(b_val),
                    (Value::Vector(a_val), Value::Vector(b_val)) => a_val.cmp(b_val),
                    // Numeric float ordering (total order over f64), like
                    // the server — NOT raw bit order, which would sort
                    // every negative float above the positives.
                    (Value::Float(a_val), Value::Float(b_val)) => {
                        let a_num = match a_val {
                            FloatValue::F32(bits) => f64::from(f32::from_bits(*bits)),
                            FloatValue::F64(bits) => f64::from_bits(*bits),
                        };
                        let b_num = match b_val {
                            FloatValue::F32(bits) => f64::from(f32::from_bits(*bits)),
                            FloatValue::F64(bits) => f64::from_bits(*bits),
                        };
                        a_num.total_cmp(&b_num)
                    }
                    // Any combination of the three map variants: length
                    // first, then entry-wise in key order.
                    (a, b) if is_map(a) && is_map(b) => {
                        let entries_a = sorted_entries(a);
                        let entries_b = sorted_entries(b);
                        entries_a
                            .len()
                            .cmp(&entries_b.len())
                            .then_with(|| entries_a.cmp(&entries_b))
                    }
                    (Value::Unknown(a_type, a_bytes), Value::Unknown(b_type, b_bytes)) => {
                        a_type.cmp(b_type).then_with(|| a_bytes.cmp(b_bytes))
                    }
                    // Equal ranks with no value comparison left
                    // (Nil/Infinity/Wildcard).
                    _ => Ordering::Equal,
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

impl From<Vector> for Value {
    fn from(val: Vector) -> Value {
        Value::Vector(val)
    }
}

impl From<HashMap<Value, Value>> for Value {
    fn from(val: HashMap<Value, Value>) -> Value {
        Value::HashMap(val)
    }
}

impl From<BTreeMap<Value, Value>> for Value {
    fn from(val: BTreeMap<Value, Value>) -> Value {
        Value::SortedMap(val)
    }
}

impl From<IndexMap<Value, Value>> for Value {
    fn from(val: IndexMap<Value, Value>) -> Value {
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
        // Aerospike's wire protocol carries signed 64-bit ints only.
        // Silently casting `u64::MAX` to `i64` produces `-1`, which is
        // exactly the kind of confusing truncation we want to catch — so
        // refuse instead.
        assert!(
            val <= i64::MAX as u64,
            "{}",
            "Aerospike does not support u64 natively on server-side. \
             Value {val} exceeds i64::MAX. Cast explicitly to i64 if \
             the truncation is intentional."
        );
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
        Value::from(*val)
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
            Value::OrderedMap(v) => Ok(v.into_iter().collect()),
            Value::SortedMap(v) => Ok(v.into_iter().collect()),
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
            Value::SortedMap(v) => Ok(v),
            Value::HashMap(v) => Ok(v.into_iter().collect()),
            Value::OrderedMap(v) => Ok(v.into_iter().collect()),
            _ => Err(format!(
                "Invalid type conversion from Value::{} to {}",
                val.type_label(),
                std::any::type_name::<Self>()
            )),
        }
    }
}

impl TryFrom<Value> for IndexMap<Value, Value> {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::OrderedMap(v) => Ok(v),
            Value::HashMap(v) => Ok(v.into_iter().collect()),
            Value::SortedMap(v) => Ok(v.into_iter().collect()),
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

impl TryFrom<Value> for Vector {
    type Error = String;
    fn try_from(val: Value) -> std::result::Result<Self, Self::Error> {
        match val {
            Value::Vector(v) => Ok(v),
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
    let Some(particle_type) = ParticleType::try_from_u8(ptype) else {
        // A particle type this client does not interpret (legacy
        // language-specific serializations, unknown future types): return
        // the raw bytes tagged with their wire code instead of failing the
        // whole record. These values are read-only — they are rejected on
        // every write path.
        return Ok(Value::Unknown(ptype, buf.read_blob(len)));
    };
    match particle_type {
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
        ParticleType::HLL => Ok(Value::HLL(buf.read_blob(len))),
        ParticleType::VECTOR => Ok(Value::Vector(Vector::from_bytes(buf, len)?)),
        ParticleType::BOOL => Ok(Value::Bool(buf.read_bool(len))),
        // Retired server types the client does not interpret: same
        // treatment as unrecognized codes above.
        ParticleType::DIGEST | ParticleType::LDT => Ok(Value::Unknown(ptype, buf.read_blob(len))),
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

/// Constructs an `OrderedMap` Value (entries keep their insertion order)
/// from a list of key/value pairs.
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
            let mut temp_map = $crate::IndexMap::new();
            $(
                temp_map.insert(as_val!($k), as_val!($v));
            )*
            $crate::Value::OrderedMap(temp_map)
        }
    };
}

/// Constructs a `SortedMap` Value (entries sorted by key, K-ordered on the
/// server) from a list of key/value pairs.
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
/// let map = as_sorted_map!("a" => 1, "b" => 2);
/// let bin = as_bin!("map", map);
/// client.put(&WritePolicy::default(), &key, &vec![bin]).await.unwrap();
/// # }
/// ```
#[macro_export]
macro_rules! as_sorted_map {
    ( $( $k:expr => $v:expr),* ) => {
        {
            let mut temp_map = std::collections::BTreeMap::new();
            $(
                temp_map.insert(as_val!($k), as_val!($v));
            )*
            $crate::Value::SortedMap(temp_map)
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
            Value::SortedMap(m) => {
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
            Value::Vector(v) => match v.data() {
                crate::vector::VectorData::Float16(d) => {
                    let mut seq = serializer.serialize_seq(Some(d.len()))?;
                    for e in d {
                        seq.serialize_element(e)?;
                    }
                    seq.end()
                }
                crate::vector::VectorData::Int32(d) => {
                    let mut seq = serializer.serialize_seq(Some(d.len()))?;
                    for e in d {
                        seq.serialize_element(e)?;
                    }
                    seq.end()
                }
                crate::vector::VectorData::Float32(d) => {
                    let mut seq = serializer.serialize_seq(Some(d.len()))?;
                    for e in d {
                        seq.serialize_element(e)?;
                    }
                    seq.end()
                }
                crate::vector::VectorData::Float64(d) => {
                    let mut seq = serializer.serialize_seq(Some(d.len()))?;
                    for e in d {
                        seq.serialize_element(e)?;
                    }
                    seq.end()
                }
            },
            Value::Infinity => panic!("Infinity cannot be serialized"),
            Value::Wildcard => panic!("Wildcard cannot be serialized"),
            Value::MultiResult(_) => panic!("MultiValue cannot be serialized"),
            // Serialize the raw payload; the particle type is not
            // representable in most formats and the bytes are opaque anyway.
            Value::Unknown(_, b) => serializer.serialize_bytes(&b[..]),
        }
    }
}

/// One of the three map collection types accepted by every map-taking API:
/// unordered [`HashMap`], insertion-ordered [`IndexMap`], or key-sorted
/// [`BTreeMap`].
pub enum MapCollection<K: Eq, V> {
    /// Unordered map ([`Value::HashMap`]).
    Hash(HashMap<K, V>),
    /// Insertion-ordered map ([`Value::OrderedMap`]).
    Ordered(IndexMap<K, V>),
    /// Key-sorted map ([`Value::SortedMap`], K-ordered on the server).
    Sorted(BTreeMap<K, V>),
}

/// Allows a `HashMap`, `IndexMap` or `BTreeMap` to be passed as the map
/// argument to any map-taking method.
pub trait MapLike<K: Eq, V> {
    /// Convert into the map-collection sum type.
    fn into_map(self) -> MapCollection<K, V>;
}

#[allow(clippy::implicit_hasher)]
impl<K: Eq + Hash, V> MapLike<K, V> for HashMap<K, V> {
    fn into_map(self) -> MapCollection<K, V> {
        MapCollection::Hash(self)
    }
}

impl<K: Eq + Hash, V> MapLike<K, V> for IndexMap<K, V> {
    fn into_map(self) -> MapCollection<K, V> {
        MapCollection::Ordered(self)
    }
}

impl<K: Eq + Ord, V> MapLike<K, V> for BTreeMap<K, V> {
    fn into_map(self) -> MapCollection<K, V> {
        MapCollection::Sorted(self)
    }
}

impl From<MapCollection<Value, Value>> for Value {
    fn from(map: MapCollection<Value, Value>) -> Value {
        match map {
            MapCollection::Hash(m) => Value::HashMap(m),
            MapCollection::Ordered(m) => Value::OrderedMap(m),
            MapCollection::Sorted(m) => Value::SortedMap(m),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{bytes_to_particle, MapCollection, MapLike, Value};
    use crate::commands::buffer::Buffer;
    use crate::commands::ParticleType;
    use crate::ResultCode;
    use indexmap::IndexMap;
    use ripemd::digest::Digest;
    use ripemd::Ripemd160;
    use std::collections::{BTreeMap, HashMap};
    use std::convert::TryInto;

    #[test]
    fn ordered_map_round_trips_through_value() {
        let mut m = IndexMap::new();
        m.insert(Value::from("z"), Value::from(1));
        m.insert(Value::from("a"), Value::from(2));

        let val = Value::from(m.clone());
        assert!(matches!(val, Value::OrderedMap(_)));
        assert_eq!(val.particle_type().unwrap(), ParticleType::MAP as u8);

        // Insertion order is preserved by the container.
        let back: IndexMap<Value, Value> = val.try_into().unwrap();
        let keys: Vec<&Value> = back.keys().collect();
        assert_eq!(keys, vec![&Value::from("z"), &Value::from("a")]);
    }

    #[test]
    fn particle_type_rejects_infinity_and_wildcard() {
        // These have no particle type: they are msgpack-only bounds. Asking
        // for one used to abort the process; Java answers PARAMETER_ERROR.
        for value in [Value::Infinity, Value::Wildcard] {
            let err = value
                .particle_type()
                .expect_err("INF/wildcard have no particle type");
            assert_eq!(
                err.result_code(),
                i32::from(u8::from(ResultCode::ParameterError)),
                "expected PARAMETER_ERROR for {value:?}"
            );
        }
    }

    #[test]
    fn particle_type_still_answers_for_every_storable_value() {
        // The guard must not have swallowed the ordinary cases.
        assert_eq!(
            Value::Nil.particle_type().unwrap(),
            ParticleType::NULL as u8
        );
        assert_eq!(
            Value::from(1).particle_type().unwrap(),
            ParticleType::INTEGER as u8
        );
        assert_eq!(
            Value::from("s").particle_type().unwrap(),
            ParticleType::STRING as u8
        );
        assert_eq!(
            Value::from(vec![1_u8]).particle_type().unwrap(),
            ParticleType::BLOB as u8
        );
        // `Unknown` still carries its uninterpreted code through.
        assert_eq!(Value::Unknown(99, vec![]).particle_type().unwrap(), 99);
    }

    #[test]
    fn type_label_names_every_variant_without_failing() {
        // Diagnostics have to work for the values that have no particle type —
        // the TryFrom error messages used to reach for `particle_type` here and
        // panicked while reporting a different failure.
        assert_eq!(Value::Infinity.type_label(), "INF");
        assert_eq!(Value::Wildcard.type_label(), "wildcard");
        assert_eq!(Value::from(1).type_label(), "int");

        let err = String::try_from(Value::Infinity)
            .expect_err("INF is not a string")
            .to_string();
        assert!(err.contains("INF"), "message should name the type: {err}");
    }

    #[test]
    fn ord_matches_server_semantics() {
        use std::cmp::Ordering;

        // Type ranks (server-verified): Nil < Bool < Int < String < List
        // < Map < Bytes < Float < GeoJSON.
        let ranked = [
            Value::Nil,
            Value::Bool(false),
            Value::Int(9),
            Value::from("zzz"),
            Value::List(vec![Value::Int(1)]),
            as_map!("k" => 1),
            Value::Blob(vec![0]),
            Value::from(-1.5),
            Value::GeoJSON("{}".into()),
        ];
        for pair in ranked.windows(2) {
            assert_eq!(
                pair[0].cmp(&pair[1]),
                Ordering::Less,
                "{:?} must order before {:?}",
                pair[0],
                pair[1]
            );
        }

        // Floats order numerically (bit order would put -0.5 last).
        assert!(Value::from(-0.5) < Value::from(2.0));
        assert!(Value::from(2.0) < Value::from(2.5));
        // Ints and floats do not interleave: every int < every float.
        assert!(Value::from(1000) < Value::from(-99.0));

        // Lists: element-wise, shorter prefix first.
        let list = |v: &[i64]| Value::List(v.iter().map(|i| Value::from(*i)).collect());
        assert!(list(&[]) < list(&[0, 9]));
        assert!(list(&[0, 9]) < list(&[1]));
        assert!(list(&[1]) < list(&[1, 2]));

        // Maps: length first, then entry-wise in key order — identically
        // across variants.
        assert!(as_map!() < as_map!("a" => 1));
        assert!(as_map!("a" => 1) < as_map!("b" => 0));
        assert!(as_map!("b" => 0) < as_map!("a" => 1, "b" => 2));
        assert!(as_ord_map!("b" => 0) < as_sorted_map!("a" => 1, "b" => 2));
        // Content-equal maps compare Equal across variants (consistent
        // with PartialEq).
        assert_eq!(
            as_map!("a" => 1).cmp(&as_ord_map!("a" => 1)),
            Ordering::Equal
        );

        // Blobs: byte-wise.
        assert!(Value::Blob(vec![1, 2]) < Value::Blob(vec![9]));
    }

    #[test]
    fn map_variants_compare_by_content() {
        let hash = as_map!("a" => 1, "b" => 2);
        let ordered = as_ord_map!("b" => 2, "a" => 1); // different order
        let sorted = as_sorted_map!("a" => 1, "b" => 2);

        // Same entries: equal across all three representations.
        assert_eq!(hash, ordered);
        assert_eq!(hash, sorted);
        assert_eq!(ordered, sorted);
        assert_eq!(ordered, hash); // symmetry

        // Different content is not equal.
        assert_ne!(hash, as_ord_map!("a" => 1));
        assert_ne!(hash, as_ord_map!("a" => 1, "b" => 3));
        // Maps never equal non-maps.
        assert_ne!(hash, Value::List(vec![Value::from("a")]));
        assert_ne!(hash, Value::Nil);
    }

    #[test]
    fn map_try_from_accepts_any_variant() {
        let ordered = as_ord_map!("k" => 1);
        let as_hash: HashMap<Value, Value> = ordered.clone().try_into().unwrap();
        assert_eq!(as_hash.len(), 1);
        let as_sorted: BTreeMap<Value, Value> = ordered.clone().try_into().unwrap();
        assert_eq!(as_sorted.len(), 1);
        let as_index: IndexMap<Value, Value> = as_map!("k" => 1).try_into().unwrap();
        assert_eq!(as_index.len(), 1);
    }

    #[test]
    fn map_macros_produce_their_variants() {
        assert!(matches!(as_map!("a" => 1), Value::HashMap(_)));
        assert!(matches!(as_ord_map!("a" => 1), Value::OrderedMap(_)));
        assert!(matches!(as_sorted_map!("a" => 1), Value::SortedMap(_)));
    }

    #[test]
    fn map_like_covers_all_three_collections() {
        let hash: HashMap<Value, Value> = HashMap::new();
        let ordered: IndexMap<Value, Value> = IndexMap::new();
        let sorted: BTreeMap<Value, Value> = BTreeMap::new();
        assert!(matches!(Value::from(hash.into_map()), Value::HashMap(_)));
        assert!(matches!(
            Value::from(ordered.into_map()),
            Value::OrderedMap(_)
        ));
        assert!(matches!(
            Value::from(sorted.into_map()),
            Value::SortedMap(_)
        ));
        // MapCollection maps 1:1 onto the Value variants.
        assert!(matches!(
            MapCollection::Ordered(IndexMap::<Value, Value>::new()),
            MapCollection::Ordered(_)
        ));
    }

    #[test]
    fn ordered_map_estimates_and_packs() {
        let mut m = IndexMap::new();
        m.insert(Value::from("k"), Value::from(7));
        let val = Value::OrderedMap(m);
        // Packs like an (unordered) wire map; must not error.
        assert!(val.estimate_size().unwrap() > 0);
    }

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
        let _: BTreeMap<Value, Value> = Value::SortedMap(BTreeMap::new()).try_into().unwrap();
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
        // Floats serialize as proper JSON numbers (e.g. `2.1`), not as the
        // raw `f64::to_bits()` integer pattern an earlier draft of this
        // test was pinned to.
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

    // Any particle type the client does not interpret decodes as
    // Value::Unknown carrying the raw code and payload — never a panic,
    // never a lost record.
    #[test]
    fn foreign_particle_types_decode_as_unknown() {
        let payload = [0xDEu8, 0xAD, 0xBE, 0xEF];

        // JBLOB(7) and the language blobs (8-12), retired DIGEST(6) and
        // LDT(21), and codes the client knows nothing about (99) all
        // surface as Unknown with the raw payload.
        for code in [6u8, 7, 8, 9, 10, 11, 12, 21, 99] {
            let mut buf = Buffer::new(0);
            buf.resize_buffer(payload.len()).unwrap();
            buf.data_buffer[..payload.len()].copy_from_slice(&payload);
            buf.data_offset = 0;

            let value = bytes_to_particle(code, &mut buf, payload.len()).unwrap();
            let Value::Unknown(particle_code, bytes) = value else {
                panic!("code {code}: expected Value::Unknown, got {value:?}");
            };
            assert_eq!(particle_code, code);
            assert_eq!(bytes, payload);
        }
    }

    // Full write -> read cycle through the bin particle path used by put/get.
    #[test]
    fn vector_value_round_trips_through_particle() {
        use crate::Vector;

        let value = Value::Vector(Vector::float32(vec![0.5, -1.5, 2.0]));
        assert_eq!(value.particle_type().unwrap(), ParticleType::VECTOR as u8);

        let size = value.estimate_size().unwrap();
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(size).unwrap();
        buf.data_offset = 0;
        let written = value.write_to(&mut buf).unwrap();
        assert_eq!(written, size);

        buf.data_offset = 0;
        let decoded = bytes_to_particle(ParticleType::VECTOR as u8, &mut buf, size).unwrap();
        assert_eq!(decoded, value);
    }

    // A vector nested in a list round-trips through the msgpack (CDT) path.
    #[test]
    fn vector_round_trips_nested_in_list() {
        use crate::msgpack::{decoder, encoder};
        use crate::Vector;

        let list = Value::List(vec![
            Value::from(1),
            Value::Vector(Vector::int32(vec![1, 2, 3])),
        ]);

        let size = encoder::pack_value(&mut None, &list).unwrap();
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(size).unwrap();
        buf.data_offset = 0;
        encoder::pack_value(&mut Some(&mut buf), &list).unwrap();

        buf.data_offset = 0;
        let decoded = decoder::unpack_value_list(&mut buf).unwrap();
        assert_eq!(decoded, list);
    }

    // A vector nested as a map value round-trips through the msgpack path.
    #[test]
    fn vector_round_trips_as_map_value() {
        use crate::msgpack::{decoder, encoder};
        use crate::Vector;

        let mut map = HashMap::new();
        map.insert(
            Value::from("k"),
            Value::Vector(Vector::float32(vec![1.5, -2.5])),
        );
        let value = Value::HashMap(map);

        let size = encoder::pack_value(&mut None, &value).unwrap();
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(size).unwrap();
        buf.data_offset = 0;
        encoder::pack_value(&mut Some(&mut buf), &value).unwrap();

        buf.data_offset = 0;
        let decoded = decoder::unpack_value_map(&mut buf).unwrap();
        assert_eq!(decoded, value);
    }

    // A vector buried in a list-of-map round-trips (deep CDT nesting).
    #[test]
    fn vector_round_trips_deeply_nested() {
        use crate::msgpack::{decoder, encoder};
        use crate::Vector;

        let mut inner = HashMap::new();
        inner.insert(
            Value::from("v"),
            Value::Vector(Vector::float64(vec![1.5, -2.5, 3.0])),
        );
        let list = Value::List(vec![Value::from(1), Value::HashMap(inner)]);

        let size = encoder::pack_value(&mut None, &list).unwrap();
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(size).unwrap();
        buf.data_offset = 0;
        encoder::pack_value(&mut Some(&mut buf), &list).unwrap();

        buf.data_offset = 0;
        let decoded = decoder::unpack_value_list(&mut buf).unwrap();
        assert_eq!(decoded, list);
    }

    // Value wrapping: estimate_size mirrors the vector wire size, and `as_bin!`
    // produces a Value::Vector bin.
    #[test]
    fn vector_value_size_and_bin_wrapping() {
        use crate::Vector;

        let v = Vector::float32(vec![1.0, 2.0, 3.0]);
        let value = Value::Vector(v.clone());
        assert_eq!(value.estimate_size().unwrap(), v.wire_size());

        let bin = crate::as_bin!("embedding", v.clone());
        assert_eq!(bin.name, "embedding");
        assert_eq!(bin.value, Value::Vector(v));
    }

    // A vector may not be used as a record key (parity with Java's
    // VectorValue.validateKeyType): both the digest path and Key::new reject it.
    #[test]
    fn vector_cannot_be_used_as_key() {
        use crate::Vector;

        let value = Value::Vector(Vector::int32(vec![1, 2, 3]));

        let mut hasher = Ripemd160::new();
        assert!(value.write_key_bytes(&mut hasher).is_err());
        assert!(crate::Key::new("ns", "set", value).is_err());
    }

    // Unknown values are read-only: every send path rejects them.
    #[test]
    fn unknown_values_cannot_be_sent() {
        let value = Value::Unknown(9, vec![1, 2, 3]); // 9 = PYTHON_BLOB

        assert!(value.estimate_size().is_err());

        let mut buf = Buffer::new(0);
        buf.resize_buffer(64).unwrap();
        buf.data_offset = 0;
        assert!(value.write_to(&mut buf).is_err());

        // Rejected inside CDT/list/map payloads too.
        assert!(crate::msgpack::encoder::pack_value(&mut None, &value).is_err());

        // And as a key: digest computation fails, so Key::new errors.
        let mut hasher = Ripemd160::new();
        assert!(value.write_key_bytes(&mut hasher).is_err());
        assert!(crate::Key::new("ns", "set", value).is_err());
    }

    // Unknown values cannot appear in expression literals: packing the
    // expression fails, whether nested in a list or as a map value.
    #[test]
    fn unknown_values_rejected_in_expressions() {
        let unknown = Value::Unknown(9, vec![1, 2, 3]);

        let exp = crate::expressions::list_val(vec![unknown.clone()]);
        assert!(exp.base64().is_err());

        let mut map = HashMap::new();
        map.insert(Value::from("k"), unknown);
        let exp = crate::expressions::map_val(map);
        assert!(exp.base64().is_err());
    }

    // Unknown values are not indexable: the filter-value conversion
    // rejects them like every other non-Int/String/Blob type.
    #[test]
    #[should_panic(expected = "must be integer, string, or blob")]
    fn unknown_values_rejected_in_filters() {
        use crate::query::filter::EqFilterValue;
        let _ = Value::Unknown(9, vec![1, 2, 3]).into_filter_value();
    }
}
