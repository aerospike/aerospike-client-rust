// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! serde ↔ [`Value`] bridge (the `serde_json::value` analog): serialize
//! any `T: Serialize` into a [`Value`] tree and deserialize any
//! `T: Deserialize` back out. This powers the `#[record(serde)]` engine
//! of the `RecordMapper` derive and is usable standalone.
//!
//! Encoding notes (differences from the bespoke derive engine — the
//! engine choice is part of your schema, don't switch it on populated
//! data):
//! - `Vec<u8>` serializes as a **List of integers** (serde has no
//!   specialization); use `serde_bytes` for [`Value::Blob`].
//! - Enums use serde's externally-tagged representation: unit variants
//!   become strings, data variants become single-entry maps.
//! - `None` maps to [`Value::Nil`] (deleting the bin at write time when
//!   used at the top level of a record).

use std::collections::HashMap;
use std::fmt;

use serde::de::{
    self, DeserializeOwned, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
use serde::{ser, Deserialize, Serialize};

use crate::errors::{Error, Result};
use crate::{FloatValue, IndexMap, Value};

/// Serialize `T` into a [`Value`].
///
/// # Errors
/// Values the encoding cannot represent (e.g. a `u64` above
/// `i64::MAX`).
pub fn to_value<T: Serialize + ?Sized>(value: &T) -> Result<Value> {
    value
        .serialize(ValueSerializer)
        .map_err(|e| Error::invalid_argument(e.0))
}

/// Deserialize `T` from a [`Value`].
///
/// # Errors
/// Shape or type mismatches between the value tree and `T`.
pub fn from_value<'de, T: Deserialize<'de>>(value: &'de Value) -> Result<T> {
    T::deserialize(ValueDeserializer(value)).map_err(|e| Error::invalid_argument(e.0))
}

/// Serialize a struct into record bins: `T` must encode as a map with
/// string keys (a struct or a `HashMap<String, _>`).
///
/// # Errors
/// Non-map encodings and unrepresentable values.
pub fn to_bins<T: Serialize + ?Sized>(value: &T) -> Result<IndexMap<String, Value>> {
    match to_value(value)? {
        Value::HashMap(map) => map
            .into_iter()
            .map(|(k, v)| match k {
                Value::String(name) => Ok((name, v)),
                other => Err(Error::invalid_argument(format!(
                    "record bins need string names, got {other:?}"
                ))),
            })
            .collect(),
        other => Err(Error::invalid_argument(format!(
            "expected the entity to serialize as a map, got {other:?}"
        ))),
    }
}

/// Deserialize a struct from record bins.
///
/// # Errors
/// Shape or type mismatches between the bins and `T`.
pub fn from_bins<T: DeserializeOwned>(bins: &IndexMap<String, Value>) -> Result<T> {
    let mut map: IndexMap<Value, Value> = IndexMap::default();
    for (name, value) in bins {
        map.insert(Value::String(name.clone()), value.clone());
    }
    let value = Value::OrderedMap(map);
    from_value(&value)
}

// ===== error shim ===========================================================

#[derive(Debug)]
pub struct SerdeError(String);

impl fmt::Display for SerdeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for SerdeError {}

impl ser::Error for SerdeError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SerdeError(msg.to_string())
    }
}

impl de::Error for SerdeError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        SerdeError(msg.to_string())
    }
}

// ===== Serializer ===========================================================

/// Magic newtype-struct names that signal Aerospike particle types the
/// serde data model cannot express (the `bson::ObjectId` trick). Emitted
/// by the [`super::serde_helpers`] modules and the wrapper types'
/// `Serialize` impls; any other serializer treats the newtype
/// transparently.
pub(crate) const GEO_JSON_TOKEN: &str = "$__aerospike_geo_json";
pub(crate) const HLL_TOKEN: &str = "$__aerospike_hll";

struct ValueSerializer;

fn variant_map(variant: &str, value: Value) -> Value {
    let mut map = HashMap::with_capacity(1);
    map.insert(Value::String(variant.to_string()), value);
    Value::HashMap(map)
}

impl ser::Serializer for ValueSerializer {
    type Ok = Value;
    type Error = SerdeError;
    type SerializeSeq = SeqSerializer;
    type SerializeTuple = SeqSerializer;
    type SerializeTupleStruct = SeqSerializer;
    type SerializeTupleVariant = VariantSeqSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = MapSerializer;
    type SerializeStructVariant = VariantMapSerializer;

    fn serialize_bool(self, v: bool) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Bool(v))
    }

    fn serialize_i8(self, v: i8) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Int(i64::from(v)))
    }

    fn serialize_i16(self, v: i16) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Int(i64::from(v)))
    }

    fn serialize_i32(self, v: i32) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Int(i64::from(v)))
    }

    fn serialize_i64(self, v: i64) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Int(v))
    }

    fn serialize_u8(self, v: u8) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Int(i64::from(v)))
    }

    fn serialize_u16(self, v: u16) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Int(i64::from(v)))
    }

    fn serialize_u32(self, v: u32) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Int(i64::from(v)))
    }

    fn serialize_u64(self, v: u64) -> std::result::Result<Value, SerdeError> {
        i64::try_from(v)
            .map(Value::Int)
            .map_err(|_| SerdeError(format!("u64 value {v} exceeds i64::MAX")))
    }

    fn serialize_f32(self, v: f32) -> std::result::Result<Value, SerdeError> {
        Ok(Value::from(v))
    }

    fn serialize_f64(self, v: f64) -> std::result::Result<Value, SerdeError> {
        Ok(Value::from(v))
    }

    fn serialize_char(self, v: char) -> std::result::Result<Value, SerdeError> {
        Ok(Value::String(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> std::result::Result<Value, SerdeError> {
        Ok(Value::String(v.to_string()))
    }

    fn serialize_bytes(self, v: &[u8]) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Blob(v.to_vec()))
    }

    fn serialize_none(self) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Nil)
    }

    fn serialize_some<T: Serialize + ?Sized>(
        self,
        value: &T,
    ) -> std::result::Result<Value, SerdeError> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Nil)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> std::result::Result<Value, SerdeError> {
        Ok(Value::Nil)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> std::result::Result<Value, SerdeError> {
        Ok(Value::String(variant.to_string()))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> std::result::Result<Value, SerdeError> {
        match name {
            GEO_JSON_TOKEN => match value.serialize(ValueSerializer)? {
                Value::String(s) => Ok(Value::GeoJSON(s)),
                other => Err(SerdeError(format!(
                    "the GeoJSON helper expects a string, got {other:?}"
                ))),
            },
            HLL_TOKEN => match value.serialize(ValueSerializer)? {
                Value::Blob(bytes) => Ok(Value::HLL(bytes)),
                other => Err(SerdeError(format!(
                    "the HLL helper expects bytes, got {other:?}"
                ))),
            },
            _ => value.serialize(self),
        }
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> std::result::Result<Value, SerdeError> {
        Ok(variant_map(variant, value.serialize(ValueSerializer)?))
    }

    fn serialize_seq(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, SerdeError> {
        Ok(SeqSerializer {
            items: Vec::with_capacity(len.unwrap_or(0)),
        })
    }

    fn serialize_tuple(self, len: usize) -> std::result::Result<Self::SerializeTuple, SerdeError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, SerdeError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, SerdeError> {
        Ok(VariantSeqSerializer {
            variant,
            items: Vec::with_capacity(len),
        })
    }

    fn serialize_map(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, SerdeError> {
        Ok(MapSerializer {
            entries: HashMap::with_capacity(len.unwrap_or(0)),
            pending_key: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStruct, SerdeError> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, SerdeError> {
        Ok(VariantMapSerializer {
            variant,
            entries: HashMap::with_capacity(len),
        })
    }
}

struct SeqSerializer {
    items: Vec<Value>,
}

impl ser::SerializeSeq for SeqSerializer {
    type Ok = Value;
    type Error = SerdeError;

    fn serialize_element<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerdeError> {
        self.items.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> std::result::Result<Value, SerdeError> {
        Ok(Value::List(self.items))
    }
}

impl ser::SerializeTuple for SeqSerializer {
    type Ok = Value;
    type Error = SerdeError;

    fn serialize_element<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerdeError> {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> std::result::Result<Value, SerdeError> {
        ser::SerializeSeq::end(self)
    }
}

impl ser::SerializeTupleStruct for SeqSerializer {
    type Ok = Value;
    type Error = SerdeError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerdeError> {
        ser::SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> std::result::Result<Value, SerdeError> {
        ser::SerializeSeq::end(self)
    }
}

struct VariantSeqSerializer {
    variant: &'static str,
    items: Vec<Value>,
}

impl ser::SerializeTupleVariant for VariantSeqSerializer {
    type Ok = Value;
    type Error = SerdeError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerdeError> {
        self.items.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> std::result::Result<Value, SerdeError> {
        Ok(variant_map(self.variant, Value::List(self.items)))
    }
}

struct MapSerializer {
    entries: HashMap<Value, Value>,
    pending_key: Option<Value>,
}

impl ser::SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = SerdeError;

    fn serialize_key<T: Serialize + ?Sized>(
        &mut self,
        key: &T,
    ) -> std::result::Result<(), SerdeError> {
        self.pending_key = Some(key.serialize(ValueSerializer)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), SerdeError> {
        let key = self
            .pending_key
            .take()
            .ok_or_else(|| SerdeError("serialize_value called before serialize_key".into()))?;
        self.entries.insert(key, value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> std::result::Result<Value, SerdeError> {
        Ok(Value::HashMap(self.entries))
    }
}

impl ser::SerializeStruct for MapSerializer {
    type Ok = Value;
    type Error = SerdeError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), SerdeError> {
        self.entries.insert(
            Value::String(key.to_string()),
            value.serialize(ValueSerializer)?,
        );
        Ok(())
    }

    fn end(self) -> std::result::Result<Value, SerdeError> {
        Ok(Value::HashMap(self.entries))
    }
}

struct VariantMapSerializer {
    variant: &'static str,
    entries: HashMap<Value, Value>,
}

impl ser::SerializeStructVariant for VariantMapSerializer {
    type Ok = Value;
    type Error = SerdeError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), SerdeError> {
        self.entries.insert(
            Value::String(key.to_string()),
            value.serialize(ValueSerializer)?,
        );
        Ok(())
    }

    fn end(self) -> std::result::Result<Value, SerdeError> {
        Ok(variant_map(self.variant, Value::HashMap(self.entries)))
    }
}

// ===== Deserializer =========================================================

#[derive(Clone, Copy)]
struct ValueDeserializer<'de>(&'de Value);

fn map_iter<'de>(
    value: &'de Value,
) -> Option<Box<dyn Iterator<Item = (&'de Value, &'de Value)> + 'de>> {
    match value {
        Value::HashMap(m) => Some(Box::new(m.iter())),
        Value::OrderedMap(m) => Some(Box::new(m.iter())),
        Value::SortedMap(m) => Some(Box::new(m.iter())),
        _ => None,
    }
}

impl<'de> de::Deserializer<'de> for ValueDeserializer<'de> {
    type Error = SerdeError;

    fn deserialize_any<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerdeError> {
        match self.0 {
            Value::Nil => visitor.visit_unit(),
            Value::Bool(b) => visitor.visit_bool(*b),
            Value::Int(n) => visitor.visit_i64(*n),
            Value::Float(FloatValue::F32(bits)) => visitor.visit_f32(f32::from_bits(*bits)),
            Value::Float(FloatValue::F64(bits)) => visitor.visit_f64(f64::from_bits(*bits)),
            Value::String(s) | Value::GeoJSON(s) => visitor.visit_borrowed_str(s),
            Value::Blob(bytes) | Value::HLL(bytes) => visitor.visit_borrowed_bytes(bytes),
            Value::List(items) => visitor.visit_seq(SeqDeserializer { iter: items.iter() }),
            map @ (Value::HashMap(_) | Value::OrderedMap(_) | Value::SortedMap(_)) => visitor
                .visit_map(MapDeserializer {
                    iter: map_iter(map).expect("map variant"),
                    pending_value: None,
                }),
            other => Err(SerdeError(format!("cannot deserialize from {other:?}"))),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerdeError> {
        match self.0 {
            Value::Nil => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, SerdeError> {
        match self.0 {
            // Unit variant, encoded as its name.
            Value::String(_) => visitor.visit_enum(EnumDeserializer {
                variant: self.0,
                value: None,
            }),
            map @ (Value::HashMap(_) | Value::OrderedMap(_) | Value::SortedMap(_)) => {
                let mut iter = map_iter(map).expect("map variant");
                let (variant, value) = iter.next().ok_or_else(|| {
                    SerdeError("expected a single-entry map for an enum variant".into())
                })?;
                if iter.next().is_some() {
                    return Err(SerdeError(
                        "expected a single-entry map for an enum variant".into(),
                    ));
                }
                visitor.visit_enum(EnumDeserializer {
                    variant,
                    value: Some(value),
                })
            }
            other => Err(SerdeError(format!(
                "cannot deserialize an enum from {other:?}"
            ))),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, SerdeError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_unit<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, SerdeError> {
        match self.0 {
            Value::Nil => visitor.visit_unit(),
            other => Err(SerdeError(format!("expected Nil, got {other:?}"))),
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit_struct seq tuple tuple_struct map struct
        identifier ignored_any
    }
}

struct SeqDeserializer<'de> {
    iter: std::slice::Iter<'de, Value>,
}

impl<'de> SeqAccess<'de> for SeqDeserializer<'de> {
    type Error = SerdeError;

    fn next_element_seed<T: de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> std::result::Result<Option<T::Value>, SerdeError> {
        match self.iter.next() {
            Some(item) => seed.deserialize(ValueDeserializer(item)).map(Some),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.iter.len())
    }
}

struct MapDeserializer<'de> {
    iter: Box<dyn Iterator<Item = (&'de Value, &'de Value)> + 'de>,
    pending_value: Option<&'de Value>,
}

impl<'de> MapAccess<'de> for MapDeserializer<'de> {
    type Error = SerdeError;

    fn next_key_seed<K: de::DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> std::result::Result<Option<K::Value>, SerdeError> {
        match self.iter.next() {
            Some((key, value)) => {
                self.pending_value = Some(value);
                seed.deserialize(ValueDeserializer(key)).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: de::DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> std::result::Result<V::Value, SerdeError> {
        let value = self
            .pending_value
            .take()
            .ok_or_else(|| SerdeError("next_value called before next_key".into()))?;
        seed.deserialize(ValueDeserializer(value))
    }
}

struct EnumDeserializer<'de> {
    variant: &'de Value,
    value: Option<&'de Value>,
}

impl<'de> EnumAccess<'de> for EnumDeserializer<'de> {
    type Error = SerdeError;
    type Variant = VariantDeserializer<'de>;

    fn variant_seed<V: de::DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> std::result::Result<(V::Value, Self::Variant), SerdeError> {
        let variant = seed.deserialize(ValueDeserializer(self.variant))?;
        Ok((variant, VariantDeserializer { value: self.value }))
    }
}

struct VariantDeserializer<'de> {
    value: Option<&'de Value>,
}

impl<'de> VariantAccess<'de> for VariantDeserializer<'de> {
    type Error = SerdeError;

    fn unit_variant(self) -> std::result::Result<(), SerdeError> {
        match self.value {
            None => Ok(()),
            Some(other) => Err(SerdeError(format!(
                "expected a unit variant, got data: {other:?}"
            ))),
        }
    }

    fn newtype_variant_seed<T: de::DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> std::result::Result<T::Value, SerdeError> {
        match self.value {
            Some(value) => seed.deserialize(ValueDeserializer(value)),
            None => Err(SerdeError("expected data for a newtype variant".into())),
        }
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, SerdeError> {
        match self.value {
            Some(value) => de::Deserializer::deserialize_any(ValueDeserializer(value), visitor),
            None => Err(SerdeError("expected data for a tuple variant".into())),
        }
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, SerdeError> {
        match self.value {
            Some(value) => de::Deserializer::deserialize_any(ValueDeserializer(value), visitor),
            None => Err(SerdeError("expected data for a struct variant".into())),
        }
    }
}

// IntoDeserializer lets callers plug values into serde combinators.
impl<'de> IntoDeserializer<'de, SerdeError> for ValueDeserializer<'de> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Inner {
        label: String,
        weight: f64,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum Payment {
        Cash,
        Card { last4: String },
        Wire(String),
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Outer {
        id: i64,
        name: String,
        maybe: Option<i64>,
        list: Vec<i64>,
        inner: Inner,
        payment: Payment,
        #[serde(with = "serde_bytes")]
        blob: Vec<u8>,
    }

    fn sample() -> Outer {
        Outer {
            id: 42,
            name: "x".to_string(),
            maybe: None,
            list: vec![1, 2, 3],
            inner: Inner {
                label: "in".to_string(),
                weight: 2.5,
            },
            payment: Payment::Card {
                last4: "1234".to_string(),
            },
            blob: vec![9, 8, 7],
        }
    }

    #[test]
    fn round_trips_struct_tree() {
        let outer = sample();
        let value = to_value(&outer).unwrap();
        assert!(matches!(value, Value::HashMap(_)));
        let back: Outer = from_value(&value).unwrap();
        assert_eq!(back, outer);
    }

    #[test]
    fn enum_representations() {
        assert_eq!(
            to_value(&Payment::Cash).unwrap(),
            Value::String("Cash".to_string())
        );
        let card = to_value(&Payment::Card {
            last4: "1234".to_string(),
        })
        .unwrap();
        assert!(matches!(card, Value::HashMap(_)));
        let back: Payment = from_value(&card).unwrap();
        assert_eq!(
            back,
            Payment::Card {
                last4: "1234".to_string()
            }
        );
        let wire = to_value(&Payment::Wire("DE99".to_string())).unwrap();
        let back: Payment = from_value(&wire).unwrap();
        assert_eq!(back, Payment::Wire("DE99".to_string()));
    }

    #[test]
    fn serde_bytes_maps_to_blob() {
        let value = to_value(&sample()).unwrap();
        let Value::HashMap(map) = &value else {
            panic!("expected map")
        };
        assert_eq!(
            map.get(&Value::String("blob".to_string())),
            Some(&Value::Blob(vec![9, 8, 7]))
        );
        // Plain Vec<u8> without serde_bytes is a List.
        let plain: Vec<u8> = vec![1, 2];
        assert_eq!(
            to_value(&plain).unwrap(),
            Value::List(vec![Value::Int(1), Value::Int(2)])
        );
    }

    #[test]
    fn bins_bridge() {
        let bins = to_bins(&sample()).unwrap();
        assert!(bins.get("name").is_some());
        let back: Outer = from_bins(&bins).unwrap();
        assert_eq!(back, sample());

        // Non-map encodings are rejected.
        assert!(to_bins(&42i64).is_err());
    }

    #[test]
    fn reads_all_server_map_variants() {
        let mut ordered: IndexMap<Value, Value> = IndexMap::default();
        ordered.insert(Value::String("label".to_string()), Value::from("in"));
        ordered.insert(Value::String("weight".to_string()), Value::from(2.5));
        let back: Inner = from_value(&Value::OrderedMap(ordered)).unwrap();
        assert_eq!(back.label, "in");

        let mut sorted = std::collections::BTreeMap::new();
        sorted.insert(Value::String("label".to_string()), Value::from("s"));
        sorted.insert(Value::String("weight".to_string()), Value::from(1.0));
        let back: Inner = from_value(&Value::SortedMap(sorted)).unwrap();
        assert_eq!(back.label, "s");
    }

    #[test]
    fn u64_overflow_is_reported() {
        assert!(to_value(&u64::MAX).is_err());
        assert_eq!(to_value(&7u64).unwrap(), Value::Int(7));
    }
}
