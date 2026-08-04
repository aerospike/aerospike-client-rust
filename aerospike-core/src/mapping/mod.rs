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

//! Object mapping — the contract for converting between application
//! types and Aerospike records (port of the Java SDK's `RecordMapper`).
//!
//! Implement [`RecordMapper`] for an entity type to store it as a record
//! and read it back:
//!
//! ```
//! use aerospike::mapping::RecordMapper;
//! use aerospike::{IndexMap, Key, Value};
//!
//! struct Customer {
//!     id: i64,
//!     name: String,
//!     age: i64,
//! }
//!
//! impl RecordMapper for Customer {
//!     fn to_bins(&self) -> aerospike::Result<IndexMap<String, Value>> {
//!         let mut bins = IndexMap::default();
//!         bins.insert("name".to_string(), Value::from(self.name.as_str()));
//!         bins.insert("age".to_string(), Value::from(self.age));
//!         Ok(bins)
//!     }
//!
//!     fn from_record(
//!         bins: &IndexMap<String, Value>,
//!         key: &Key,
//!         _generation: u32,
//!     ) -> aerospike::Result<Customer> {
//!         let get_str = |bin: &str| match bins.get(bin) {
//!             Some(Value::String(s)) => Ok(s.clone()),
//!             other => Err(aerospike::Error::invalid_argument(format!(
//!                 "bin '{bin}': expected a string, got {other:?}"
//!             ))),
//!         };
//!         let get_int = |bin: &str| match bins.get(bin) {
//!             Some(Value::Int(n)) => Ok(*n),
//!             other => Err(aerospike::Error::invalid_argument(format!(
//!                 "bin '{bin}': expected an integer, got {other:?}"
//!             ))),
//!         };
//!         Ok(Customer {
//!             id: match key.user_key {
//!                 Some(Value::Int(id)) => id,
//!                 _ => 0,
//!             },
//!             name: get_str("name")?,
//!             age: get_int("age")?,
//!         })
//!     }
//!
//!     fn id(&self) -> Value {
//!         Value::from(self.id)
//!     }
//! }
//! ```
//!
//! For types you don't own (the orphan rule prevents implementing this
//! trait for them), wrap them in a newtype and implement the mapper on
//! the wrapper — the Rust idiom replacing the Java SDK's external
//! mapper objects and `RecordMappingFactory` registry, which exist
//! because Java resolves mappers at runtime; Rust resolves the
//! implementation at compile time.

#[cfg(feature = "serialization")]
pub mod serde;
#[cfg(feature = "serialization")]
pub mod serde_helpers;

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::errors::{Error, Result};
use crate::{FloatValue, IndexMap, Key, Value};

/// Conversion between an application type and an Aerospike record.
///
/// - [`to_bins`](Self::to_bins) produces the bins to write;
/// - [`from_record`](Self::from_record) rebuilds the value from a read
///   record's bins, key, and generation;
/// - [`id`](Self::id) extracts the record's user key, letting typed
///   write builders derive the full [`Key`] from a dataset.
///
/// The key field itself is conventionally *not* stored as a bin: `id()`
/// carries it into the key on writes and `from_record` recovers it from
/// [`Key::user_key`] on reads (send-key policies control whether the
/// server retains it).
pub trait RecordMapper: Sized {
    /// The bins representing this value, in write order.
    ///
    /// # Errors
    /// Implementations report values that cannot be represented.
    fn to_bins(&self) -> Result<IndexMap<String, Value>>;

    /// Rebuild a value from a read record.
    ///
    /// # Errors
    /// Implementations report missing bins or type mismatches.
    fn from_record(bins: &IndexMap<String, Value>, key: &Key, generation: u32) -> Result<Self>;

    /// The user key identifying this value within its dataset.
    fn id(&self) -> Value;
}

// ===== Field-level conversions ==============================================

/// Conversion of one field into a [`Value`] (the building block used by
/// the `RecordMapper` derive for each struct field, recursively for
/// nested collections).
///
/// Notable encodings:
/// - `Vec<u8>` maps to [`Value::Blob`] (the database-natural byte
///   encoding); consequently `u8` itself does not implement this trait —
///   use a wider integer for scalar byte fields.
/// - `Option<T>`: `None` maps to [`Value::Nil`], which deletes the bin
///   on write.
pub trait ToValue {
    /// Convert to a [`Value`].
    ///
    /// # Errors
    /// Values that cannot be represented (e.g. a `u64` above
    /// `i64::MAX`).
    fn to_value(&self) -> Result<Value>;
}

/// Conversion of a [`Value`] back into a field (the read-side twin of
/// [`ToValue`]).
pub trait FromValue: Sized {
    /// Convert from a [`Value`].
    ///
    /// # Errors
    /// Type or range mismatches.
    fn from_value(value: &Value) -> Result<Self>;

    /// The value to use when the bin is absent from the record. The
    /// default errors; `Option<T>` yields `None`.
    ///
    /// # Errors
    /// By default, absence is an error.
    fn from_missing() -> Result<Self> {
        Err(Error::invalid_argument("bin is missing"))
    }
}

fn type_mismatch<T>(expected: &str, got: &Value) -> Result<T> {
    Err(Error::invalid_argument(format!(
        "expected {expected}, got {got:?}"
    )))
}

// -- scalars -----------------------------------------------------------------

macro_rules! int_to_value {
    ($($ty:ty),+) => {$(
        impl ToValue for $ty {
            fn to_value(&self) -> Result<Value> {
                Ok(Value::Int(i64::from(*self)))
            }
        }
        impl FromValue for $ty {
            fn from_value(value: &Value) -> Result<Self> {
                match value {
                    Value::Int(n) => <$ty>::try_from(*n).map_err(|_| {
                        Error::invalid_argument(format!(
                            "integer {n} out of range for {}",
                            stringify!($ty)
                        ))
                    }),
                    other => type_mismatch("an integer", other),
                }
            }
        }
    )+};
}

// `u8` is deliberately absent: `Vec<u8>` must encode as a Blob, which a
// blanket `Vec<T: ToValue>` impl would otherwise claim as a List.
int_to_value!(i8, i16, i32, i64, u16, u32);

impl ToValue for u64 {
    fn to_value(&self) -> Result<Value> {
        i64::try_from(*self)
            .map(Value::Int)
            .map_err(|_| Error::invalid_argument(format!("u64 value {self} exceeds i64::MAX")))
    }
}

impl FromValue for u64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Int(n) => u64::try_from(*n)
                .map_err(|_| Error::invalid_argument(format!("integer {n} is negative"))),
            other => type_mismatch("an integer", other),
        }
    }
}

impl ToValue for bool {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::Bool(*self))
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Bool(b) => Ok(*b),
            other => type_mismatch("a boolean", other),
        }
    }
}

impl ToValue for f64 {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::from(*self))
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Float(FloatValue::F64(bits)) => Ok(f64::from_bits(*bits)),
            Value::Float(FloatValue::F32(bits)) => Ok(f64::from(f32::from_bits(*bits))),
            other => type_mismatch("a float", other),
        }
    }
}

impl ToValue for f32 {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::from(*self))
    }
}

impl FromValue for f32 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Float(FloatValue::F32(bits)) => Ok(f32::from_bits(*bits)),
            Value::Float(FloatValue::F64(bits)) => Ok(f64::from_bits(*bits) as f32),
            other => type_mismatch("a float", other),
        }
    }
}

impl ToValue for String {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::String(self.clone()))
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::String(s) => Ok(s.clone()),
            other => type_mismatch("a string", other),
        }
    }
}

impl ToValue for str {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::String(self.to_string()))
    }
}

impl ToValue for Value {
    fn to_value(&self) -> Result<Value> {
        Ok(self.clone())
    }
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Result<Self> {
        Ok(value.clone())
    }
}

impl<T: ToValue + ?Sized> ToValue for &T {
    fn to_value(&self) -> Result<Value> {
        (*self).to_value()
    }
}

impl<T: ToValue + ?Sized> ToValue for Box<T> {
    fn to_value(&self) -> Result<Value> {
        (**self).to_value()
    }
}

impl<T: FromValue> FromValue for Box<T> {
    fn from_value(value: &Value) -> Result<Self> {
        T::from_value(value).map(Box::new)
    }
}

// -- blobs -------------------------------------------------------------------

impl ToValue for Vec<u8> {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::Blob(self.clone()))
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Blob(bytes) | Value::HLL(bytes) => Ok(bytes.clone()),
            other => type_mismatch("a blob", other),
        }
    }
}

impl ToValue for [u8] {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::Blob(self.to_vec()))
    }
}

// -- Option ------------------------------------------------------------------

impl<T: ToValue> ToValue for Option<T> {
    fn to_value(&self) -> Result<Value> {
        match self {
            Some(inner) => inner.to_value(),
            // Writing Nil deletes the bin.
            None => Ok(Value::Nil),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Nil => Ok(None),
            other => T::from_value(other).map(Some),
        }
    }

    fn from_missing() -> Result<Self> {
        Ok(None)
    }
}

// -- collections ---------------------------------------------------------------

impl<T: ToValue> ToValue for Vec<T> {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::List(
            self.iter().map(ToValue::to_value).collect::<Result<_>>()?,
        ))
    }
}

impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::List(items) => items.iter().map(T::from_value).collect(),
            other => type_mismatch("a list", other),
        }
    }
}

impl<K: ToValue, V: ToValue> ToValue for HashMap<K, V> {
    fn to_value(&self) -> Result<Value> {
        let mut out: HashMap<Value, Value> = HashMap::with_capacity(self.len());
        for (k, v) in self {
            out.insert(k.to_value()?, v.to_value()?);
        }
        Ok(Value::HashMap(out))
    }
}

impl<K, V: FromValue> FromValue for HashMap<K, V>
where
    K: FromValue + Eq + std::hash::Hash,
{
    fn from_value(value: &Value) -> Result<Self> {
        map_entries(value)?
            .map(|(k, v)| Ok((K::from_value(k)?, V::from_value(v)?)))
            .collect()
    }
}

impl<K: ToValue, V: ToValue> ToValue for BTreeMap<K, V> {
    fn to_value(&self) -> Result<Value> {
        let mut out: HashMap<Value, Value> = HashMap::with_capacity(self.len());
        for (k, v) in self {
            out.insert(k.to_value()?, v.to_value()?);
        }
        Ok(Value::HashMap(out))
    }
}

impl<K, V: FromValue> FromValue for BTreeMap<K, V>
where
    K: FromValue + Ord,
{
    fn from_value(value: &Value) -> Result<Self> {
        map_entries(value)?
            .map(|(k, v)| Ok((K::from_value(k)?, V::from_value(v)?)))
            .collect()
    }
}

// -- Aerospike-specific wrapper types ------------------------------------------

/// A field stored as the server's native GeoJSON particle
/// ([`Value::GeoJSON`]) instead of a plain string — queryable with
/// geospatial filters. Works in both derive engines; under serde-based
/// serializers other than Aerospike's (JSON, YAML, ...) it encodes
/// transparently as the inner string.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GeoJson(pub String);

impl GeoJson {
    /// Wrap a GeoJSON document string.
    #[must_use]
    pub fn new(geo_json: impl Into<String>) -> GeoJson {
        GeoJson(geo_json.into())
    }

    /// The GeoJSON document string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Unwrap into the document string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl ToValue for GeoJson {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::GeoJSON(self.0.clone()))
    }
}

impl FromValue for GeoJson {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::GeoJSON(s) | Value::String(s) => Ok(GeoJson(s.clone())),
            other => type_mismatch("a GeoJSON document", other),
        }
    }
}

/// A field stored as the server's HyperLogLog particle ([`Value::HLL`])
/// instead of a plain blob — as written by HLL operations or backup
/// restores. Works in both derive engines; under serde-based serializers
/// other than Aerospike's it encodes transparently as bytes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Hll(pub Vec<u8>);

impl Hll {
    /// Wrap raw HLL bytes.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Hll {
        Hll(bytes.into())
    }

    /// The raw HLL bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Unwrap into the raw bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl ToValue for Hll {
    fn to_value(&self) -> Result<Value> {
        Ok(Value::HLL(self.0.clone()))
    }
}

impl FromValue for Hll {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::HLL(bytes) | Value::Blob(bytes) => Ok(Hll(bytes.clone())),
            other => type_mismatch("an HLL value", other),
        }
    }
}

// -- std::time ------------------------------------------------------------------

/// [`SystemTime`] encodes as epoch **milliseconds** (matching the Java
/// client's `Date.getTime()` bins); pre-epoch times are negative. Use
/// the `serde_helpers` time modules for other resolutions under the
/// serde engine.
impl ToValue for SystemTime {
    fn to_value(&self) -> Result<Value> {
        let millis = match self.duration_since(UNIX_EPOCH) {
            Ok(after) => i64::try_from(after.as_millis()),
            Err(err) => i64::try_from(err.duration().as_millis()).map(|m| -m),
        }
        .map_err(|_| Error::invalid_argument("timestamp out of range for epoch milliseconds"))?;
        Ok(Value::Int(millis))
    }
}

impl FromValue for SystemTime {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Int(millis) => {
                let magnitude = Duration::from_millis(millis.unsigned_abs());
                let time = if *millis >= 0 {
                    UNIX_EPOCH.checked_add(magnitude)
                } else {
                    UNIX_EPOCH.checked_sub(magnitude)
                };
                time.ok_or_else(|| {
                    Error::invalid_argument(format!("timestamp {millis}ms is unrepresentable"))
                })
            }
            other => type_mismatch("an epoch-milliseconds integer", other),
        }
    }
}

/// [`Duration`] encodes as **nanoseconds** (full precision; errors above
/// `i64::MAX` nanoseconds ≈ 292 years). Use the `serde_helpers` time
/// modules for a milliseconds encoding under the serde engine.
impl ToValue for Duration {
    fn to_value(&self) -> Result<Value> {
        i64::try_from(self.as_nanos())
            .map(Value::Int)
            .map_err(|_| Error::invalid_argument("duration exceeds i64::MAX nanoseconds"))
    }
}

impl FromValue for Duration {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Int(nanos) if *nanos >= 0 => Ok(Duration::from_nanos(*nanos as u64)),
            Value::Int(nanos) => Err(Error::invalid_argument(format!(
                "duration cannot be negative ({nanos}ns)"
            ))),
            other => type_mismatch("a nanoseconds integer", other),
        }
    }
}

/// Iterate any map variant's entries.
fn map_entries(value: &Value) -> Result<Box<dyn Iterator<Item = (&Value, &Value)> + '_>> {
    match value {
        Value::HashMap(m) => Ok(Box::new(m.iter())),
        Value::OrderedMap(m) => Ok(Box::new(m.iter())),
        Value::SortedMap(m) => Ok(Box::new(m.iter())),
        other => type_mismatch("a map", other),
    }
}

/// Everything the `RecordMapper` derive's generated code references,
/// under one stable path (`<crate>::mapping::__derive::...`) so the
/// macro works from `aerospike_core`, the `aerospike` facade, and
/// `aerospike_sdk` alike.
#[doc(hidden)]
pub mod __derive {
    pub use super::{FromValue, RecordMapper, ToValue};
    pub use crate::errors::{Error, Result};
    pub use crate::{IndexMap, Key, Value};

    /// Referenced by the derive's `#[record(serde)]` engine; requires
    /// the `serialization` feature.
    #[cfg(feature = "serialization")]
    pub use super::serde as serde_support;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_round_trips() {
        assert_eq!(42i64.to_value().unwrap(), Value::Int(42));
        assert_eq!(i64::from_value(&Value::Int(42)).unwrap(), 42);
        assert_eq!(i16::from_value(&Value::Int(7)).unwrap(), 7i16);
        assert!(i16::from_value(&Value::Int(1 << 20)).is_err()); // range
        assert!(i64::from_value(&Value::from("x")).is_err()); // type

        assert_eq!(42u64.to_value().unwrap(), Value::Int(42));
        assert!(u64::MAX.to_value().is_err());

        assert_eq!(true.to_value().unwrap(), Value::Bool(true));
        assert_eq!(2.5f64.to_value().unwrap(), Value::from(2.5));
        assert_eq!(f64::from_value(&Value::from(2.5)).unwrap(), 2.5);
        assert_eq!(f64::from_value(&Value::from(1.5f32)).unwrap(), 1.5);
        assert_eq!(
            String::from_value(&Value::from("hi")).unwrap(),
            "hi".to_string()
        );
    }

    #[test]
    fn blob_vs_list() {
        // Vec<u8> is a Blob, not a List.
        let bytes: Vec<u8> = vec![1, 2, 3];
        assert_eq!(bytes.to_value().unwrap(), Value::Blob(vec![1, 2, 3]));
        assert_eq!(
            Vec::<u8>::from_value(&Value::Blob(vec![1, 2, 3])).unwrap(),
            vec![1, 2, 3]
        );
        // HLL reads back as bytes too.
        assert_eq!(
            Vec::<u8>::from_value(&Value::HLL(vec![9])).unwrap(),
            vec![9]
        );
        // Other integer vecs are lists.
        let ints: Vec<i64> = vec![1, 2];
        assert_eq!(
            ints.to_value().unwrap(),
            Value::List(vec![Value::Int(1), Value::Int(2)])
        );
    }

    #[test]
    fn option_nil_and_missing() {
        assert_eq!(Some(5i64).to_value().unwrap(), Value::Int(5));
        assert_eq!(Option::<i64>::None.to_value().unwrap(), Value::Nil);
        assert_eq!(Option::<i64>::from_value(&Value::Nil).unwrap(), None);
        assert_eq!(Option::<i64>::from_value(&Value::Int(5)).unwrap(), Some(5));
        assert_eq!(Option::<i64>::from_missing().unwrap(), None);
        assert!(i64::from_missing().is_err());
    }

    #[test]
    fn nested_collections() {
        let nested: Vec<Vec<i64>> = vec![vec![1], vec![2, 3]];
        let value = nested.to_value().unwrap();
        assert_eq!(Vec::<Vec<i64>>::from_value(&value).unwrap(), nested);

        let mut map: HashMap<String, Vec<i64>> = HashMap::new();
        map.insert("a".to_string(), vec![1, 2]);
        let value = map.to_value().unwrap();
        let back: HashMap<String, Vec<i64>> = HashMap::from_value(&value).unwrap();
        assert_eq!(back, map);

        // All three server map variants read back.
        let mut ordered = IndexMap::default();
        ordered.insert(Value::from("k"), Value::Int(1));
        let back: HashMap<String, i64> = HashMap::from_value(&Value::OrderedMap(ordered)).unwrap();
        assert_eq!(back.get("k"), Some(&1));

        let mut btree: BTreeMap<String, i64> = BTreeMap::new();
        btree.insert("z".to_string(), 26);
        let round = BTreeMap::<String, i64>::from_value(&btree.to_value().unwrap()).unwrap();
        assert_eq!(round, btree);
    }
}
