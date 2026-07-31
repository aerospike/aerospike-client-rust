// Copyright 2015-2026 Aerospike, Inc.
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

//! Dense numeric vectors for vector similarity search.
//!
//! A [`Vector`] is the public, user-facing type. When stored in a bin it is
//! carried as a [`Value::Vector`](crate::Value::Vector) and encoded with the
//! `VECTOR` particle type (wire code 16), matching the Java and C clients.
//!
//! # Wire format
//!
//! ```text
//! Offset  Size (bytes)  Field         Description
//! 0       1             version       Vector format version (currently 1).
//! 1       1             element_type  See [`VectorElementType`].
//! 2       4             dimensions    Element count, little-endian.
//! 6       2             reserved      Zero padding (8-byte alignment).
//! 8       variable      data          Contiguous little-endian elements.
//! ```
//!
//! The whole payload, header included, is little-endian (unlike the rest of the
//! Aerospike wire protocol).

use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt;

use crate::commands::buffer::Buffer;
use crate::errors::{Error, Result};

/// Current vector wire-format version.
pub const VECTOR_VERSION: u8 = 1;

/// Size in bytes of the fixed vector header (`version`, `element_type`,
/// `dimensions` and `reserved` padding).
pub const VECTOR_HEADER_SIZE: usize = 8;

/// Element type of a [`Vector`], identifying how each element is encoded on the
/// wire. The discriminant is the wire code the server uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorElementType {
    /// IEEE 754 half precision, carried as raw 16-bit patterns (Rust has no `f16`).
    Float16 = 0x01,
    /// 32-bit signed integer.
    Int32 = 0x02,
    /// 32-bit IEEE 754 float.
    Float32 = 0x03,
    /// 64-bit IEEE 754 double.
    Float64 = 0x04,
}

impl VectorElementType {
    /// Wire-protocol code for this element type.
    pub const fn code(self) -> u8 {
        self as u8
    }

    /// Number of bytes used to encode a single element of this type.
    pub const fn byte_size(self) -> usize {
        match self {
            VectorElementType::Float16 => 2,
            VectorElementType::Int32 | VectorElementType::Float32 => 4,
            VectorElementType::Float64 => 8,
        }
    }

    /// Look up an element type from its wire-protocol code, returning `None`
    /// for codes this client does not interpret.
    const fn try_from_code(code: u8) -> Option<Self> {
        match code {
            0x01 => Some(VectorElementType::Float16),
            0x02 => Some(VectorElementType::Int32),
            0x03 => Some(VectorElementType::Float32),
            0x04 => Some(VectorElementType::Float64),
            _ => None,
        }
    }
}

impl fmt::Display for VectorElementType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match self {
            VectorElementType::Float16 => "float16",
            VectorElementType::Int32 => "int32",
            VectorElementType::Float32 => "float32",
            VectorElementType::Float64 => "float64",
        };
        f.write_str(name)
    }
}

/// Distance metric for a vector distance expression
/// ([`expressions::vector::distance`](crate::expressions::vector::distance)).
///
/// The discriminant is the wire code.
///
/// # Work in progress
///
/// The distance expression this feeds is not finalized and cannot be sent to
/// the server yet; this enum is provisional.
///
// TODO(vector-exp-metric-semantics): metric semantics are not finalized
// upstream. A pending server contract may redefine these as L2-squared and
// cosine *distance* (1 - similarity), flipping the "nearest" sort direction for
// cosine. Revisit once that lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorDistanceMetric {
    /// Euclidean (L2) distance; smaller is closer.
    Euclidean = 0,
    /// Dot product; larger is more similar.
    DotProduct = 1,
    /// Cosine similarity; larger is closer.
    Cosine = 2,
}

impl VectorDistanceMetric {
    /// Wire-protocol code for this metric.
    pub const fn code(self) -> i64 {
        self as i64
    }
}

/// Element data of a [`Vector`]: one variant per element type. Elements are
/// held in host order and converted to/from the little-endian wire format on
/// write/read.
#[derive(Debug, Clone)]
pub enum VectorData {
    /// `float16` elements as raw 16-bit patterns (see [`VectorElementType::Float16`]).
    Float16(Vec<u16>),
    /// `int32` elements.
    Int32(Vec<i32>),
    /// `float` (fp32) elements.
    Float32(Vec<f32>),
    /// `double` (fp64) elements.
    Float64(Vec<f64>),
}

impl VectorData {
    /// The element type of this data.
    pub const fn element_type(&self) -> VectorElementType {
        match self {
            VectorData::Float16(_) => VectorElementType::Float16,
            VectorData::Int32(_) => VectorElementType::Int32,
            VectorData::Float32(_) => VectorElementType::Float32,
            VectorData::Float64(_) => VectorElementType::Float64,
        }
    }

    /// Number of elements.
    pub const fn dimensions(&self) -> usize {
        match self {
            VectorData::Float16(d) => d.len(),
            VectorData::Int32(d) => d.len(),
            VectorData::Float32(d) => d.len(),
            VectorData::Float64(d) => d.len(),
        }
    }
}

/// A dense vector of numeric elements, used for vector similarity search.
///
/// Build one with the element-type constructors ([`Vector::float32`] and
/// friends); the [`data`](Vector::data) is held in host order and converted
/// to/from the little-endian wire format on write/read.
///
/// A `Vector` converts into a [`Value`](crate::Value) via [`From`], so it can
/// be stored directly in a bin:
///
/// ```
/// use aerospike::{Vector, as_bin};
///
/// let embedding = Vector::float32(vec![0.12, 0.98, -0.34]);
/// let bin = as_bin!("embedding", embedding);
/// ```
#[derive(Debug, Clone)]
pub struct Vector {
    version: u8,
    data: VectorData,
}

impl Vector {
    /// Create a vector of raw `float16` elements (IEEE 754 half precision).
    /// Rust has no native `f16`, so each element is passed as its raw 16-bit
    /// bit pattern.
    pub const fn float16(data: Vec<u16>) -> Self {
        Self::current(VectorData::Float16(data))
    }

    /// Create a vector of `int32` elements.
    pub const fn int32(data: Vec<i32>) -> Self {
        Self::current(VectorData::Int32(data))
    }

    /// Create a vector of `float` (fp32) elements.
    pub const fn float32(data: Vec<f32>) -> Self {
        Self::current(VectorData::Float32(data))
    }

    /// Create a vector of `double` (fp64) elements.
    pub const fn float64(data: Vec<f64>) -> Self {
        Self::current(VectorData::Float64(data))
    }

    /// Wrap element data with the current wire-format version.
    const fn current(data: VectorData) -> Self {
        Vector {
            version: VECTOR_VERSION,
            data,
        }
    }

    /// The wire-format version. Vectors you construct carry the current
    /// [`VECTOR_VERSION`]; a vector decoded from the server carries whatever
    /// version it sent, so a newer server format is observable here (and is
    /// preserved if the vector is written back unchanged).
    pub const fn version(&self) -> u8 {
        self.version
    }

    /// The element data.
    pub const fn data(&self) -> &VectorData {
        &self.data
    }

    /// The element type of this vector.
    pub const fn element_type(&self) -> VectorElementType {
        self.data.element_type()
    }

    /// Number of dimensions (elements) in this vector.
    pub const fn dimensions(&self) -> usize {
        self.data.dimensions()
    }

    /// Returns `true` if this vector has no elements.
    pub const fn is_empty(&self) -> bool {
        self.dimensions() == 0
    }

    /// Number of bytes this vector occupies on the wire (header plus element
    /// data). For internal use only.
    pub(crate) const fn wire_size(&self) -> usize {
        VECTOR_HEADER_SIZE + self.dimensions() * self.element_type().byte_size()
    }

    /// Serialize this vector into `buf` in the little-endian wire format,
    /// returning the number of bytes written (equal to [`Self::wire_size`]).
    /// The stored [`version`](Self::version) is emitted, preserving a value
    /// read back from the server. For internal use only.
    pub(crate) fn write_to(&self, buf: &mut Buffer) -> usize {
        buf.write_u8(self.version);
        buf.write_u8(self.element_type().code());
        buf.write_u32_little_endian(self.dimensions() as u32);
        // 2 reserved header bytes (8-byte alignment). Not yet defined by the
        // server contract; may become vector flags, so we emit zeros for now.
        buf.write_u8(0);
        buf.write_u8(0);

        match &self.data {
            VectorData::Float16(d) => {
                for &x in d {
                    buf.write_u16_little_endian(x);
                }
            }
            VectorData::Int32(d) => {
                for &x in d {
                    buf.write_u32_little_endian(x as u32);
                }
            }
            VectorData::Float32(d) => {
                for &x in d {
                    buf.write_u32_little_endian(x.to_bits());
                }
            }
            VectorData::Float64(d) => {
                for &x in d {
                    buf.write_u64_little_endian(x.to_bits());
                }
            }
        }

        self.wire_size()
    }

    /// Raw element bytes in little-endian order, without the header. This is
    /// the query-vector form a vector distance expression sends.
    ///
    // TODO(vector-exp-envelope): the frozen server contract will switch the
    // expression query argument to the full wire value (header + elements);
    // until that server change ships, headerless elements match current
    // behavior.
    pub(crate) fn element_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.dimensions() * self.element_type().byte_size());
        match &self.data {
            VectorData::Float16(d) => d.iter().for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
            VectorData::Int32(d) => d.iter().for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
            VectorData::Float32(d) => d.iter().for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
            VectorData::Float64(d) => d.iter().for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
        }
        out
    }

    /// Deserialize a vector from the wire format at the buffer's current
    /// offset. `len` is the number of bytes available for this particle. The
    /// on-wire version is preserved in [`version`](Self::version). For internal
    /// use only.
    ///
    /// # Errors
    ///
    /// Returns [`Error::bad_response`] if the payload is too short for its
    /// header, carries an unknown element type, or declares more dimensions
    /// than `len` can hold.
    pub(crate) fn from_bytes(buf: &mut Buffer, len: usize) -> Result<Self> {
        if len < VECTOR_HEADER_SIZE {
            return Err(Error::bad_response(format!(
                "invalid vector length: {len}, need at least {VECTOR_HEADER_SIZE}"
            )));
        }

        let version = buf.read_u8(None);
        let type_code = buf.read_u8(None);
        let Some(element_type) = VectorElementType::try_from_code(type_code) else {
            return Err(Error::bad_response(format!(
                "unknown vector element type code: {type_code}"
            )));
        };
        let dimensions = buf.read_u32_little_endian(None) as usize;
        // Skip the 2 reserved header bytes. Not yet defined by the server
        // contract; if they later carry vector flags, read them into a field
        // here instead of discarding.
        buf.skip(2);

        // Checked so a huge dimension count can't overflow past the bounds check.
        let data_size = dimensions
            .checked_mul(element_type.byte_size())
            .ok_or_else(|| Error::bad_response("vector dimensions overflow"))?;

        if len < VECTOR_HEADER_SIZE + data_size {
            return Err(Error::bad_response(format!(
                "invalid vector length: {len}, expected at least {}",
                VECTOR_HEADER_SIZE + data_size
            )));
        }

        let bytes = buf.read_blob(data_size);
        let data = match element_type {
            VectorElementType::Float16 => VectorData::Float16(
                bytes
                    .chunks_exact(2)
                    .map(|c| u16::from_le_bytes([c[0], c[1]]))
                    .collect(),
            ),
            VectorElementType::Int32 => VectorData::Int32(
                bytes
                    .chunks_exact(4)
                    .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
                    .collect(),
            ),
            VectorElementType::Float32 => VectorData::Float32(
                bytes
                    .chunks_exact(4)
                    .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
                    .collect(),
            ),
            VectorElementType::Float64 => VectorData::Float64(
                bytes
                    .chunks_exact(8)
                    .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
                    .collect(),
            ),
        };

        // Skip any trailing bytes so the offset lands exactly past this particle.
        let consumed = VECTOR_HEADER_SIZE + data_size;
        if consumed < len {
            buf.skip(len - consumed);
        }

        Ok(Vector { version, data })
    }
}

/// Float elements compare by IEEE 754 bit pattern (like
/// [`FloatValue`](crate::FloatValue)), keeping [`Eq`] reflexive.
impl PartialEq for VectorData {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (VectorData::Float16(a), VectorData::Float16(b)) => a == b,
            (VectorData::Int32(a), VectorData::Int32(b)) => a == b,
            (VectorData::Float32(a), VectorData::Float32(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
            }
            (VectorData::Float64(a), VectorData::Float64(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
            }
            _ => false,
        }
    }
}

impl Eq for VectorData {}

/// Total order by element-type code, then element-wise (shorter prefix first),
/// floats via [`f32::total_cmp`]/[`f64::total_cmp`].
impl Ord for VectorData {
    fn cmp(&self, other: &Self) -> Ordering {
        fn cmp_by<T, F>(a: &[T], b: &[T], mut f: F) -> Ordering
        where
            F: FnMut(&T, &T) -> Ordering,
        {
            for (x, y) in a.iter().zip(b.iter()) {
                match f(x, y) {
                    Ordering::Equal => {}
                    non_eq => return non_eq,
                }
            }
            a.len().cmp(&b.len())
        }

        self.element_type()
            .code()
            .cmp(&other.element_type().code())
            .then_with(|| match (self, other) {
                (VectorData::Float16(a), VectorData::Float16(b)) => a.cmp(b),
                (VectorData::Int32(a), VectorData::Int32(b)) => a.cmp(b),
                (VectorData::Float32(a), VectorData::Float32(b)) => cmp_by(a, b, f32::total_cmp),
                (VectorData::Float64(a), VectorData::Float64(b)) => cmp_by(a, b, f64::total_cmp),
                // Unreachable: equal type codes imply the same variant.
                _ => Ordering::Equal,
            })
    }
}

impl PartialOrd for VectorData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Two vectors are equal when both their [`version`](Vector::version) and their
/// element data match.
impl PartialEq for Vector {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version && self.data == other.data
    }
}

impl Eq for Vector {}

/// Orders by version, then element data. Only exists to give
/// [`Value`](crate::Value) a total order; the server does not order vector bins.
impl Ord for Vector {
    fn cmp(&self, other: &Self) -> Ordering {
        self.version
            .cmp(&other.version)
            .then_with(|| self.data.cmp(&other.data))
    }
}

impl PartialOrd for Vector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Vector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Vector::{}(", self.element_type())?;
        match &self.data {
            VectorData::Float16(d) => write!(f, "{d:?}")?,
            VectorData::Int32(d) => write!(f, "{d:?}")?,
            VectorData::Float32(d) => write!(f, "{d:?}")?,
            VectorData::Float64(d) => write!(f, "{d:?}")?,
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::buffer::Buffer;

    /// Write `vector` into a fresh buffer sized to (at least) `capacity` and
    /// reset the offset to the start, ready to read back.
    fn encode(vector: &Vector, capacity: usize) -> Buffer {
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(capacity).unwrap();
        buf.data_offset = 0;
        let written = vector.write_to(&mut buf);
        assert_eq!(
            written,
            vector.wire_size(),
            "write_to must write exactly wire_size bytes"
        );
        buf.data_offset = 0;
        buf
    }

    /// Serialize then deserialize a vector through the wire format.
    fn round_trip(vector: &Vector) -> Vector {
        let size = vector.wire_size();
        let mut buf = encode(vector, size);
        Vector::from_bytes(&mut buf, size).unwrap()
    }

    /// Build a buffer holding a hand-crafted vector header + body, offset at 0.
    fn craft(version: u8, type_code: u8, dimensions: u32, reserved: [u8; 2], body: &[u8]) -> Buffer {
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(VECTOR_HEADER_SIZE + body.len()).unwrap();
        buf.data_offset = 0;
        buf.write_u8(version);
        buf.write_u8(type_code);
        buf.write_u32_little_endian(dimensions);
        buf.write_u8(reserved[0]);
        buf.write_u8(reserved[1]);
        for &b in body {
            buf.write_u8(b);
        }
        buf.data_offset = 0;
        buf
    }

    #[test]
    fn element_type_codes_and_byte_sizes() {
        assert_eq!(VectorElementType::Float16.code(), 0x01);
        assert_eq!(VectorElementType::Int32.code(), 0x02);
        assert_eq!(VectorElementType::Float32.code(), 0x03);
        assert_eq!(VectorElementType::Float64.code(), 0x04);

        assert_eq!(VectorElementType::Float16.byte_size(), 2);
        assert_eq!(VectorElementType::Int32.byte_size(), 4);
        assert_eq!(VectorElementType::Float32.byte_size(), 4);
        assert_eq!(VectorElementType::Float64.byte_size(), 8);
    }

    #[test]
    fn distance_metric_codes() {
        assert_eq!(VectorDistanceMetric::Euclidean.code(), 0);
        assert_eq!(VectorDistanceMetric::DotProduct.code(), 1);
        assert_eq!(VectorDistanceMetric::Cosine.code(), 2);
    }

    #[test]
    fn wire_size_matches_header_plus_elements() {
        assert_eq!(Vector::float32(vec![1.0, 2.0, 3.0]).wire_size(), 8 + 3 * 4);
        assert_eq!(Vector::float64(vec![1.0, 2.0]).wire_size(), 8 + 2 * 8);
        assert_eq!(Vector::int32(vec![1, 2, 3, 4]).wire_size(), 8 + 4 * 4);
        assert_eq!(Vector::float16(vec![0, 1]).wire_size(), 8 + 2 * 2);
        assert_eq!(Vector::float32(vec![]).wire_size(), 8);
    }

    #[test]
    fn header_is_little_endian_with_version_and_type() {
        let vector = Vector::float32(vec![1.5]);
        let buf = encode(&vector, vector.wire_size());

        assert_eq!(buf.data_buffer[0], VECTOR_VERSION);
        assert_eq!(buf.data_buffer[1], VectorElementType::Float32.code());
        // dimensions = 1, little-endian
        assert_eq!(&buf.data_buffer[2..6], &[1, 0, 0, 0]);
        assert_eq!(&buf.data_buffer[6..8], &[0, 0]); // reserved
        // the single float, little-endian
        assert_eq!(&buf.data_buffer[8..12], &1.5f32.to_le_bytes());
    }

    #[test]
    fn int32_and_float64_elements_are_little_endian() {
        let v = Vector::int32(vec![1, -2]);
        let buf = encode(&v, v.wire_size());
        assert_eq!(&buf.data_buffer[8..12], &1i32.to_le_bytes());
        assert_eq!(&buf.data_buffer[12..16], &(-2i32).to_le_bytes());

        let v = Vector::float64(vec![1.5]);
        let buf = encode(&v, v.wire_size());
        assert_eq!(&buf.data_buffer[8..16], &1.5f64.to_le_bytes());
    }

    #[test]
    fn round_trips_every_element_type() {
        assert_eq!(
            round_trip(&Vector::float16(vec![0x3c00, 0x4000, 0xbc00])),
            Vector::float16(vec![0x3c00, 0x4000, 0xbc00])
        );
        assert_eq!(
            round_trip(&Vector::int32(vec![-5, 0, 7, i32::MIN, i32::MAX])),
            Vector::int32(vec![-5, 0, 7, i32::MIN, i32::MAX])
        );
        assert_eq!(
            round_trip(&Vector::float32(vec![0.1, -2.5, 3.14159])),
            Vector::float32(vec![0.1, -2.5, 3.14159])
        );
        assert_eq!(
            round_trip(&Vector::float64(vec![0.1, -2.5, f64::MAX])),
            Vector::float64(vec![0.1, -2.5, f64::MAX])
        );
    }

    #[test]
    fn round_trips_empty_vector() {
        assert_eq!(round_trip(&Vector::float16(vec![])), Vector::float16(vec![]));
        assert_eq!(round_trip(&Vector::int32(vec![])), Vector::int32(vec![]));
        assert_eq!(round_trip(&Vector::float32(vec![])), Vector::float32(vec![]));
        assert_eq!(round_trip(&Vector::float64(vec![])), Vector::float64(vec![]));
        // An empty vector is header-only on the wire.
        assert_eq!(Vector::float64(vec![]).wire_size(), VECTOR_HEADER_SIZE);
    }

    #[test]
    fn large_vector_round_trips() {
        let dims = 4096;
        let data: Vec<f32> = (0..dims).map(|i| i as f32 * 0.5).collect();
        let v = Vector::float32(data.clone());
        assert_eq!(round_trip(&v), v);
        assert_eq!(round_trip(&v).data(), &VectorData::Float32(data));
    }

    #[test]
    fn round_trips_special_float_values() {
        let v = Vector::float32(vec![
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            -0.0,
            f32::MIN_POSITIVE,
        ]);
        assert_eq!(round_trip(&v), v, "float32 special values must survive bit-for-bit");

        let v = Vector::float64(vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            -0.0,
            f64::MIN_POSITIVE,
        ]);
        assert_eq!(round_trip(&v), v, "float64 special values must survive bit-for-bit");
    }

    #[test]
    fn negative_zero_differs_from_positive_zero() {
        // Bit-pattern equality (matches FloatValue): -0.0 and 0.0 are distinct.
        assert_ne!(Vector::float32(vec![-0.0]), Vector::float32(vec![0.0]));
        assert_ne!(Vector::float64(vec![-0.0]), Vector::float64(vec![0.0]));
    }

    #[test]
    fn equality_requires_matching_type_and_version() {
        // Same numeric values, different element type: not equal.
        assert_ne!(Vector::float32(vec![1.0]), Vector::float64(vec![1.0]));

        // Same data, different version: not equal.
        let constructed = Vector::float32(vec![1.0, 2.0]);
        let body: Vec<u8> = [1.0f32, 2.0]
            .iter()
            .flat_map(|x| x.to_bits().to_le_bytes())
            .collect();
        let mut buf = craft(2, VectorElementType::Float32.code(), 2, [0, 0], &body);
        let decoded = Vector::from_bytes(&mut buf, VECTOR_HEADER_SIZE + body.len()).unwrap();
        assert_eq!(decoded.data(), constructed.data());
        assert_ne!(decoded, constructed, "version must participate in equality");
    }

    #[test]
    fn ordering_by_element_type_then_elements() {
        // Element-type code ordering dominates (Float16 < Int32 < Float32 < Float64).
        let f16 = Vector::float16(vec![0xffff]);
        let i32v = Vector::int32(vec![i32::MIN]);
        let f32v = Vector::float32(vec![f32::INFINITY]);
        let f64v = Vector::float64(vec![f64::NEG_INFINITY]);
        assert!(f16 < i32v);
        assert!(i32v < f32v);
        assert!(f32v < f64v);

        // Within a type: shorter prefix sorts first, then element-wise.
        assert!(Vector::float32(vec![1.0]) < Vector::float32(vec![1.0, 0.0]));
        assert!(Vector::float32(vec![1.0, 0.0]) < Vector::float32(vec![1.0, 2.0]));
    }

    #[test]
    fn ordering_uses_total_cmp_for_floats() {
        // total_cmp orders: -inf < finite < +inf < NaN.
        let neg_inf = Vector::float64(vec![f64::NEG_INFINITY]);
        let finite = Vector::float64(vec![0.0]);
        let pos_inf = Vector::float64(vec![f64::INFINITY]);
        let nan = Vector::float64(vec![f64::NAN]);
        assert!(neg_inf < finite);
        assert!(finite < pos_inf);
        assert!(pos_inf < nan);
    }

    #[test]
    fn element_bytes_match_wire_body_for_all_types() {
        // For every element type, element_bytes() is exactly the wire payload
        // with the 8-byte header stripped (the form a distance expression sends).
        for v in [
            Vector::float16(vec![0x3c00, 0xbc00, 0x4000]),
            Vector::int32(vec![-1, 0, 1, 12345]),
            Vector::float32(vec![1.5, -2.25, 3.14159]),
            Vector::float64(vec![1.5, -2.25, 3.14159]),
        ] {
            let eb = v.element_bytes();
            assert_eq!(eb.len(), v.dimensions() * v.element_type().byte_size());
            let buf = encode(&v, v.wire_size());
            assert_eq!(&eb[..], &buf.data_buffer[VECTOR_HEADER_SIZE..v.wire_size()]);
        }
    }

    #[test]
    fn preserves_and_reemits_nonstandard_version() {
        let body: Vec<u8> = [1.0f32, 2.0]
            .iter()
            .flat_map(|x| x.to_bits().to_le_bytes())
            .collect();
        let len = VECTOR_HEADER_SIZE + body.len();
        let mut buf = craft(2, VectorElementType::Float32.code(), 2, [0, 0], &body);

        let v = Vector::from_bytes(&mut buf, len).unwrap();
        assert_eq!(v.version(), 2, "on-wire version must be preserved");
        assert_eq!(v.data(), &VectorData::Float32(vec![1.0, 2.0]));

        // Writing it back re-emits the same (non-default) version.
        let out = encode(&v, v.wire_size());
        assert_eq!(out.data_buffer[0], 2);
    }

    #[test]
    fn from_bytes_rejects_short_header() {
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(4).unwrap();
        buf.data_offset = 0;
        assert!(
            Vector::from_bytes(&mut buf, 4).is_err(),
            "too short for the 8-byte header"
        );
    }

    #[test]
    fn from_bytes_rejects_unknown_element_type() {
        let mut buf = craft(VECTOR_VERSION, 0x09, 0, [0, 0], &[]);
        assert!(
            Vector::from_bytes(&mut buf, VECTOR_HEADER_SIZE).is_err(),
            "unknown element-type code must be rejected"
        );
    }

    #[test]
    fn from_bytes_rejects_truncated_body() {
        // Header claims 4 float32 elements (16 body bytes) but only 8 are given.
        let body = [0u8; 8];
        let len = VECTOR_HEADER_SIZE + body.len();
        let mut buf = craft(VECTOR_VERSION, VectorElementType::Float32.code(), 4, [0, 0], &body);
        assert!(
            Vector::from_bytes(&mut buf, len).is_err(),
            "declared dimensions exceed available bytes"
        );
    }

    #[test]
    fn from_bytes_ignores_reserved_and_skips_trailing_bytes() {
        // Non-zero reserved bytes are tolerated; trailing bytes past the vector
        // are skipped so the offset lands exactly at start + len.
        let body: Vec<u8> = 7.0f32.to_bits().to_le_bytes().to_vec();
        let trailing = [0xAAu8, 0xBB, 0xCC];
        let mut full = body.clone();
        full.extend_from_slice(&trailing);
        let len = VECTOR_HEADER_SIZE + full.len();

        let mut buf = craft(VECTOR_VERSION, VectorElementType::Float32.code(), 1, [0xAB, 0xCD], &full);
        let v = Vector::from_bytes(&mut buf, len).unwrap();
        assert_eq!(v, Vector::float32(vec![7.0]));
        assert_eq!(buf.data_offset, len, "offset must advance past the whole particle");
    }

    #[test]
    fn float16_special_bit_patterns_round_trip() {
        // Raw half-precision patterns: +Inf, -Inf, NaN, +0, -0, smallest
        // subnormal. Float16 is opaque to us, so the raw u16s must survive.
        let v = Vector::float16(vec![0x7c00, 0xfc00, 0x7e00, 0x0000, 0x8000, 0x0001]);
        let buf = encode(&v, v.wire_size());
        assert_eq!(&buf.data_buffer[8..10], &0x7c00u16.to_le_bytes());
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn metadata_accessors() {
        let v = Vector::float64(vec![1.0, 2.0, 3.0]);
        assert_eq!(v.element_type(), VectorElementType::Float64);
        assert_eq!(v.dimensions(), 3);
        assert_eq!(v.version(), VECTOR_VERSION);
        assert_eq!(v.data(), &VectorData::Float64(vec![1.0, 2.0, 3.0]));
        assert!(!v.is_empty());
        assert!(Vector::int32(vec![]).is_empty());
    }

    #[test]
    fn display_shows_type_and_elements() {
        let s = Vector::float32(vec![1.0, 2.0]).to_string();
        assert!(s.contains("float32"), "got: {s}");
        assert!(s.contains("1.0") && s.contains("2.0"), "got: {s}");
    }

    #[test]
    fn different_data_of_same_type_is_not_equal() {
        assert_ne!(
            Vector::float32(vec![1.0, 2.0, 3.0]),
            Vector::float32(vec![1.0, 2.0, 4.0])
        );
        assert_ne!(Vector::int32(vec![1, 2, 3]), Vector::int32(vec![1, 2, 4]));
        // Differing length is also unequal.
        assert_ne!(Vector::int32(vec![1, 2]), Vector::int32(vec![1, 2, 3]));
    }

    #[test]
    fn element_type_try_from_code_round_trips() {
        for t in [
            VectorElementType::Float16,
            VectorElementType::Int32,
            VectorElementType::Float32,
            VectorElementType::Float64,
        ] {
            assert_eq!(VectorElementType::try_from_code(t.code()), Some(t));
        }
        assert_eq!(VectorElementType::try_from_code(0x00), None);
        assert_eq!(VectorElementType::try_from_code(0x7f), None);
    }

    #[test]
    fn from_bytes_rejects_oversized_dimensions_without_allocating() {
        // A header claiming u32::MAX elements (or a "negative" dimension count,
        // which is the same bit pattern) must be rejected by the bounds check
        // rather than attempting a huge allocation.
        let mut buf = craft(
            VECTOR_VERSION,
            VectorElementType::Float32.code(),
            u32::MAX,
            [0, 0],
            &[],
        );
        assert!(Vector::from_bytes(&mut buf, VECTOR_HEADER_SIZE).is_err());
    }

    #[test]
    fn write_and_read_respect_buffer_offset() {
        let v = Vector::int32(vec![7, 8, 9]);
        let size = v.wire_size();
        let lead = 4;

        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(lead + size).unwrap();
        // Sentinel leading bytes that write_to must not touch.
        for b in &mut buf.data_buffer[..lead] {
            *b = 0xEE;
        }

        buf.data_offset = lead;
        let written = v.write_to(&mut buf);
        assert_eq!(written, size);
        assert_eq!(&buf.data_buffer[..lead], &[0xEE; 4], "leading bytes untouched");
        assert_eq!(buf.data_buffer[lead], VECTOR_VERSION);

        buf.data_offset = lead;
        assert_eq!(Vector::from_bytes(&mut buf, size).unwrap(), v);
    }
}
