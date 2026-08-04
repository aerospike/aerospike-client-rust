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

//! `#[serde(with = "...")]` helper modules for the `#[record(serde)]`
//! engine, covering representations the serde data model cannot express
//! or defaults it gets wrong (the `bson::serde_helpers` analog):
//!
//! ```
//! use aerospike::mapping::serde_helpers;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Poi {
//!     name: String,
//!     #[serde(with = "serde_helpers::geo_json")]
//!     location: String, // stored as the queryable GeoJSON particle
//!     #[serde(with = "serde_helpers::blob")]
//!     thumbnail: Vec<u8>, // stored as a Blob, not a List of ints
//!     #[serde(with = "serde_helpers::system_time_as_millis")]
//!     created: std::time::SystemTime, // epoch milliseconds
//! }
//! ```
//!
//! Every helper's encoding matches the bespoke engine's `ToValue` /
//! `FromValue` impl for the same type, so the two engines stay
//! byte-compatible for these fields. Under serde-based serializers other
//! than Aerospike's, the helpers degrade gracefully (GeoJSON to a plain
//! string, HLL/blob to the serializer's byte representation).

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::serde::{GEO_JSON_TOKEN, HLL_TOKEN};
use super::{GeoJson, Hll};

/// Serializes `&[u8]` through `serialize_bytes` (what `serde_bytes`
/// does), so the Aerospike serializer produces a Blob.
struct BytesShim<'a>(&'a [u8]);

impl Serialize for BytesShim<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// Accepts bytes (the Aerospike deserializer) or an integer sequence
/// (JSON and friends).
struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bytes or a sequence of bytes")
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
        Ok(v.to_vec())
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
        Ok(v)
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
        let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(byte) = seq.next_element::<u8>()? {
            out.push(byte);
        }
        Ok(out)
    }
}

// ===== GeoJson / Hll wrapper (de)serialization ================================

impl Serialize for GeoJson {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(GEO_JSON_TOKEN, self.0.as_str())
    }
}

impl<'de> Deserialize<'de> for GeoJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<GeoJson, D::Error> {
        String::deserialize(deserializer).map(GeoJson)
    }
}

impl Serialize for Hll {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(HLL_TOKEN, &BytesShim(&self.0))
    }
}

impl<'de> Deserialize<'de> for Hll {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Hll, D::Error> {
        deserializer.deserialize_byte_buf(BytesVisitor).map(Hll)
    }
}

// ===== field helper modules ===================================================

/// Store a `String` field as the server's queryable GeoJSON particle:
/// `#[serde(with = "serde_helpers::geo_json")]`.
pub mod geo_json {
    use super::{Deserialize, Deserializer, Serializer, GEO_JSON_TOKEN};

    /// Emit the GeoJSON marker around the document string.
    ///
    /// # Errors
    /// Errors of the underlying serializer.
    pub fn serialize<S: Serializer>(value: &str, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(GEO_JSON_TOKEN, value)
    }

    /// Read the document back as a string.
    ///
    /// # Errors
    /// Errors of the underlying deserializer.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
        String::deserialize(deserializer)
    }
}

/// Store a `Vec<u8>` field as the server's HyperLogLog particle:
/// `#[serde(with = "serde_helpers::hll")]`.
pub mod hll {
    use super::{BytesShim, BytesVisitor, Deserializer, Serializer, HLL_TOKEN};

    /// Emit the HLL marker around the raw bytes.
    ///
    /// # Errors
    /// Errors of the underlying serializer.
    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_newtype_struct(HLL_TOKEN, &BytesShim(value))
    }

    /// Read the raw bytes back.
    ///
    /// # Errors
    /// Errors of the underlying deserializer.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_byte_buf(BytesVisitor)
    }
}

/// Store a `Vec<u8>` field as a Blob instead of a List of integers:
/// `#[serde(with = "serde_helpers::blob")]` (equivalent to depending on
/// `serde_bytes`).
pub mod blob {
    use super::{BytesVisitor, Deserializer, Serializer};

    /// Serialize through `serialize_bytes`.
    ///
    /// # Errors
    /// Errors of the underlying serializer.
    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(value)
    }

    /// Read the bytes back (accepts blobs and integer sequences).
    ///
    /// # Errors
    /// Errors of the underlying deserializer.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_byte_buf(BytesVisitor)
    }
}

fn system_time_to_millis<E: serde::ser::Error>(time: &SystemTime) -> Result<i64, E> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_millis()),
        Err(err) => i64::try_from(err.duration().as_millis()).map(|m| -m),
    }
    .map_err(|_| E::custom("timestamp out of range for epoch milliseconds"))
}

fn system_time_to_nanos<E: serde::ser::Error>(time: &SystemTime) -> Result<i64, E> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_nanos()),
        Err(err) => i64::try_from(err.duration().as_nanos()).map(|n| -n),
    }
    .map_err(|_| E::custom("timestamp out of range for epoch nanoseconds"))
}

fn millis_to_system_time<E: de::Error>(millis: i64) -> Result<SystemTime, E> {
    let magnitude = Duration::from_millis(millis.unsigned_abs());
    let time = if millis >= 0 {
        UNIX_EPOCH.checked_add(magnitude)
    } else {
        UNIX_EPOCH.checked_sub(magnitude)
    };
    time.ok_or_else(|| E::custom(format!("timestamp {millis}ms is unrepresentable")))
}

fn nanos_to_system_time<E: de::Error>(nanos: i64) -> Result<SystemTime, E> {
    let magnitude = Duration::from_nanos(nanos.unsigned_abs());
    let time = if nanos >= 0 {
        UNIX_EPOCH.checked_add(magnitude)
    } else {
        UNIX_EPOCH.checked_sub(magnitude)
    };
    time.ok_or_else(|| E::custom(format!("timestamp {nanos}ns is unrepresentable")))
}

/// Store a [`SystemTime`] field as epoch **milliseconds** (the Java
/// client's `Date.getTime()` encoding, and the bespoke engine's
/// `ToValue` encoding): `#[serde(with =
/// "serde_helpers::system_time_as_millis")]`.
pub mod system_time_as_millis {
    use super::{
        millis_to_system_time, system_time_to_millis, Deserialize, Deserializer, Serializer,
        SystemTime,
    };

    /// Serialize as an epoch-milliseconds integer.
    ///
    /// # Errors
    /// Timestamps out of `i64` millisecond range.
    pub fn serialize<S: Serializer>(value: &SystemTime, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(system_time_to_millis(value)?)
    }

    /// Read an epoch-milliseconds integer back.
    ///
    /// # Errors
    /// Unrepresentable timestamps.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SystemTime, D::Error> {
        millis_to_system_time(i64::deserialize(deserializer)?)
    }
}

/// Store a [`SystemTime`] field as epoch **nanoseconds** (full
/// precision, representable for years ~1678–2261): `#[serde(with =
/// "serde_helpers::system_time_as_nanos")]`.
pub mod system_time_as_nanos {
    use super::{
        nanos_to_system_time, system_time_to_nanos, Deserialize, Deserializer, Serializer,
        SystemTime,
    };

    /// Serialize as an epoch-nanoseconds integer.
    ///
    /// # Errors
    /// Timestamps out of `i64` nanosecond range.
    pub fn serialize<S: Serializer>(value: &SystemTime, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(system_time_to_nanos(value)?)
    }

    /// Read an epoch-nanoseconds integer back.
    ///
    /// # Errors
    /// Unrepresentable timestamps.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SystemTime, D::Error> {
        nanos_to_system_time(i64::deserialize(deserializer)?)
    }
}

/// Store a [`Duration`] field as **nanoseconds** (the bespoke engine's
/// `ToValue` encoding; errors above `i64::MAX` nanoseconds):
/// `#[serde(with = "serde_helpers::duration_as_nanos")]`.
pub mod duration_as_nanos {
    use super::{de, Deserialize, Deserializer, Duration, Serializer};

    /// Serialize as a nanoseconds integer.
    ///
    /// # Errors
    /// Durations above `i64::MAX` nanoseconds.
    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        let nanos = i64::try_from(value.as_nanos())
            .map_err(|_| serde::ser::Error::custom("duration exceeds i64::MAX nanoseconds"))?;
        serializer.serialize_i64(nanos)
    }

    /// Read a nanoseconds integer back.
    ///
    /// # Errors
    /// Negative values.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let nanos = i64::deserialize(deserializer)?;
        u64::try_from(nanos)
            .map(Duration::from_nanos)
            .map_err(|_| de::Error::custom(format!("duration cannot be negative ({nanos}ns)")))
    }
}

/// Store a [`Duration`] field as **milliseconds** (sub-millisecond
/// precision truncates): `#[serde(with =
/// "serde_helpers::duration_as_millis")]`.
pub mod duration_as_millis {
    use super::{de, Deserialize, Deserializer, Duration, Serializer};

    /// Serialize as a milliseconds integer.
    ///
    /// # Errors
    /// Durations above `i64::MAX` milliseconds.
    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        let millis = i64::try_from(value.as_millis())
            .map_err(|_| serde::ser::Error::custom("duration exceeds i64::MAX milliseconds"))?;
        serializer.serialize_i64(millis)
    }

    /// Read a milliseconds integer back.
    ///
    /// # Errors
    /// Negative values.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let millis = i64::deserialize(deserializer)?;
        u64::try_from(millis)
            .map(Duration::from_millis)
            .map_err(|_| de::Error::custom(format!("duration cannot be negative ({millis}ms)")))
    }
}

#[cfg(test)]
mod tests {
    use super::super::serde::{from_value, to_value};
    use super::*;
    use crate::Value;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Poi {
        name: String,
        #[serde(with = "super::geo_json")]
        location: String,
        #[serde(with = "super::blob")]
        thumbnail: Vec<u8>,
        #[serde(with = "super::hll")]
        sketch: Vec<u8>,
        #[serde(with = "super::system_time_as_millis")]
        created: SystemTime,
        #[serde(with = "super::duration_as_nanos")]
        ttl: Duration,
        geo_wrapped: GeoJson,
        hll_wrapped: Hll,
    }

    fn sample() -> Poi {
        Poi {
            name: "hq".to_string(),
            location: r#"{"type":"Point","coordinates":[1.0,2.0]}"#.to_string(),
            thumbnail: vec![1, 2, 3],
            sketch: vec![9, 9],
            created: UNIX_EPOCH + Duration::from_millis(1_700_000_000_123),
            ttl: Duration::from_nanos(1_500_000),
            geo_wrapped: GeoJson::new(r#"{"type":"Point","coordinates":[3.0,4.0]}"#),
            hll_wrapped: Hll::new(vec![7, 7, 7]),
        }
    }

    fn field<'v>(value: &'v Value, name: &str) -> &'v Value {
        let Value::HashMap(map) = value else {
            panic!("expected map")
        };
        map.get(&Value::String(name.to_string())).expect(name)
    }

    #[test]
    fn helpers_emit_native_particles() {
        let value = to_value(&sample()).unwrap();
        assert!(matches!(field(&value, "location"), Value::GeoJSON(_)));
        assert!(matches!(field(&value, "geo_wrapped"), Value::GeoJSON(_)));
        assert_eq!(field(&value, "thumbnail"), &Value::Blob(vec![1, 2, 3]));
        assert_eq!(field(&value, "sketch"), &Value::HLL(vec![9, 9]));
        assert_eq!(field(&value, "hll_wrapped"), &Value::HLL(vec![7, 7, 7]));
        assert_eq!(field(&value, "created"), &Value::Int(1_700_000_000_123));
        assert_eq!(field(&value, "ttl"), &Value::Int(1_500_000));
    }

    #[test]
    fn helpers_round_trip() {
        let poi = sample();
        let value = to_value(&poi).unwrap();
        let back: Poi = from_value(&value).unwrap();
        assert_eq!(back, poi);
    }

    #[test]
    fn engines_agree_on_helper_encodings() {
        use super::super::ToValue;

        // GeoJson / Hll wrappers and time types encode identically
        // through ToValue (bespoke engine) and serde helpers.
        let geo = GeoJson::new("{}");
        assert_eq!(geo.to_value().unwrap(), to_value(&geo).unwrap());
        let hll = Hll::new(vec![1]);
        assert_eq!(hll.to_value().unwrap(), to_value(&hll).unwrap());

        let time = UNIX_EPOCH + Duration::from_millis(123_456);
        let bespoke = time.to_value().unwrap();
        // Newtype structs serialize transparently as their inner value.
        #[derive(Serialize)]
        struct T(#[serde(with = "super::system_time_as_millis")] SystemTime);
        assert_eq!(bespoke, to_value(&T(time)).unwrap());
    }

    #[test]
    fn pre_epoch_and_range_errors() {
        // Pre-epoch times are negative and round-trip (newtype structs
        // encode transparently as the inner value).
        let before = UNIX_EPOCH - Duration::from_millis(500);
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct T(#[serde(with = "super::system_time_as_millis")] SystemTime);
        let value = to_value(&T(before)).unwrap();
        assert_eq!(value, Value::Int(-500));
        let back: T = from_value(&value).unwrap();
        assert_eq!(back.0, before);

        // Negative durations are rejected on read.
        #[derive(Deserialize, Debug)]
        struct D(#[serde(with = "super::duration_as_nanos")] Duration);
        assert!(from_value::<D>(&Value::Int(-1)).is_err());
    }

    #[test]
    fn wrappers_degrade_gracefully_under_json() {
        // Under serde_json the markers vanish: GeoJSON becomes a plain
        // string, HLL a byte array.
        let json = serde_json::to_string(&GeoJson::new("{}")).unwrap();
        assert_eq!(json, "\"{}\"");
        let json = serde_json::to_string(&Hll::new(vec![1, 2])).unwrap();
        assert_eq!(json, "[1,2]");
    }
}
