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

//! Dense numeric vectors encoded as `VECTOR` particles (wire type 16).
//!
//! # Wire format
//!
//! ```text
//! Offset  Size (bytes)  Field         Description
//! 0       1             version       Vector format version.
//! 1       1             element_type  See [`VectorElementType`].
//! 2       4             dimensions    Element count, little-endian.
//! 6       2             reserved      Reserved header field.
//! 8       variable      data          Contiguous little-endian elements.
//! ```
//!
//! [`Vector::wire_bytes`] returns the complete value used by vector-distance
//! expressions.

use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt;

use crate::commands::buffer::Buffer;
use crate::errors::{Error, Result};

/// Current vector wire-format version.
pub const VECTOR_VERSION: u8 = 1;

/// Size in bytes of the fixed vector header (`version`, `element_type`,
/// `dimensions`, and `reserved`).
pub const VECTOR_HEADER_SIZE: usize = 8;

/// Maximum size in bytes of a vector's element array, mirroring the server's
/// `VECTOR_MAX_ELEMENTS_BYTES`. The per-type dimension cap derives from this.
pub const VECTOR_MAX_ELEMENTS_BYTES: usize = 1 << 18;

/// Wire encoding of [`Vector`] elements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorElementType {
    /// IEEE 754 half precision as raw bits.
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

    /// Largest dimension count the server accepts for this element type
    /// (`VECTOR_MAX_ELEMENTS_BYTES / byte_size`).
    pub const fn max_dimensions(self) -> usize {
        VECTOR_MAX_ELEMENTS_BYTES / self.byte_size()
    }

    /// Returns the element type for a supported wire code.
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

/// Vector-distance metric.
///
/// Use the named builders in [`crate::expressions::vector`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorDistanceMetric {
    /// Squared L2 distance; smaller is closer.
    EuclideanSquared = 0,
    /// Dot product; larger is more similar.
    DotProduct = 1,
    /// Cosine similarity; larger is closer.
    CosineSimilarity = 2,
}

impl VectorDistanceMetric {
    /// Wire-protocol expression opcode for this metric.
    pub const fn code(self) -> i64 {
        match self {
            VectorDistanceMetric::EuclideanSquared => 52,
            VectorDistanceMetric::DotProduct => 53,
            VectorDistanceMetric::CosineSimilarity => 54,
        }
    }
}

/// Element data held in host order.
#[derive(Debug, Clone)]
pub enum VectorData {
    /// `float16` elements as raw 16-bit patterns.
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

/// A dense numeric vector.
///
/// Converts into [`Value`](crate::Value) and can be written directly to a bin:
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
    reserved: u16,
    data: VectorData,
}

impl Vector {
    /// Creates a `float16` vector from raw IEEE 754 bit patterns.
    pub fn float16(data: Vec<u16>) -> Self {
        Self::try_float16(data).expect("invalid vector data")
    }

    /// Creates an `int32` vector.
    pub fn int32(data: Vec<i32>) -> Self {
        Self::try_int32(data).expect("invalid vector data")
    }

    /// Creates a `float32` vector.
    pub fn float32(data: Vec<f32>) -> Self {
        Self::try_float32(data).expect("invalid vector data")
    }

    /// Creates a `float64` vector.
    pub fn float64(data: Vec<f64>) -> Self {
        Self::try_float64(data).expect("invalid vector data")
    }

    /// Creates a validated `float16` vector.
    pub fn try_float16(data: Vec<u16>) -> Result<Self> {
        Self::current(VectorData::Float16(data))
    }

    /// Creates a validated `int32` vector.
    pub fn try_int32(data: Vec<i32>) -> Result<Self> {
        Self::current(VectorData::Int32(data))
    }

    /// Creates a validated `float32` vector.
    pub fn try_float32(data: Vec<f32>) -> Result<Self> {
        Self::current(VectorData::Float32(data))
    }

    /// Creates a validated `float64` vector.
    pub fn try_float64(data: Vec<f64>) -> Result<Self> {
        Self::current(VectorData::Float64(data))
    }

    /// Validates the dimension count.
    fn current(data: VectorData) -> Result<Self> {
        let dimensions = data.dimensions();

        if dimensions == 0 {
            return Err(Error::invalid_argument(
                "vector must have at least 1 dimension",
            ));
        }

        let max = data.element_type().max_dimensions();

        if dimensions > max {
            return Err(Error::invalid_argument(format!(
                "vector dimensions {dimensions} exceeds max {max} for element type {}",
                data.element_type()
            )));
        }

        Ok(Vector {
            version: VECTOR_VERSION,
            reserved: 0,
            data,
        })
    }

    /// The wire-format version.
    pub const fn version(&self) -> u8 {
        self.version
    }

    /// Raw bits of the header's `reserved` field.
    pub const fn reserved(&self) -> u16 {
        self.reserved
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

    /// Wire size, including the header.
    pub(crate) const fn wire_size(&self) -> usize {
        VECTOR_HEADER_SIZE + self.dimensions() * self.element_type().byte_size()
    }

    /// Serializes this vector in the little-endian wire format.
    pub(crate) fn write_to(&self, buf: &mut Buffer) -> usize {
        buf.write_u8(self.version);
        buf.write_u8(self.element_type().code());
        buf.write_u32_little_endian(self.dimensions() as u32);
        buf.write_u16_little_endian(self.reserved);

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

    /// Returns the complete little-endian vector wire value, including its
    /// 8-byte header. This is the literal form consumed by vector-distance
    /// expressions.
    pub fn wire_bytes(&self) -> Vec<u8> {
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(self.wire_size())
            .expect("validated vector wire size must fit the buffer");
        buf.data_offset = 0;
        self.write_to(&mut buf);
        buf.data_buffer.clone()
    }

    /// Returns the little-endian element bytes without the 8-byte header.
    pub fn element_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.dimensions() * self.element_type().byte_size());
        match &self.data {
            VectorData::Float16(d) => d
                .iter()
                .for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
            VectorData::Int32(d) => d
                .iter()
                .for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
            VectorData::Float32(d) => d
                .iter()
                .for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
            VectorData::Float64(d) => d
                .iter()
                .for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
        }
        out
    }

    /// Decodes a vector particle at the buffer's current offset.
    ///
    /// # Errors
    ///
    /// Rejects short payloads and unknown element types; preserves version and `reserved`.
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
        let reserved = u16::from_le_bytes([buf.read_u8(None), buf.read_u8(None)]);

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

        let vector = Vector {
            version,
            reserved,
            data,
        };
        let consumed = VECTOR_HEADER_SIZE + data_size;
        if consumed < len {
            buf.skip(len - consumed);
        }

        Ok(vector)
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

/// Two vectors are equal when their version, `reserved` bits, and element data match.
impl PartialEq for Vector {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version && self.reserved == other.reserved && self.data == other.data
    }
}

impl Eq for Vector {}

/// Orders by version, `reserved`, then element data.
impl Ord for Vector {
    fn cmp(&self, other: &Self) -> Ordering {
        self.version
            .cmp(&other.version)
            .then_with(|| self.reserved.cmp(&other.reserved))
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

    /// Write `vector` into a fresh buffer of exactly `capacity` bytes and reset
    /// the offset to the start, ready to read back.
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
    fn craft(
        version: u8,
        type_code: u8,
        dimensions: u32,
        reserved: [u8; 2],
        body: &[u8],
    ) -> Buffer {
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
        assert_eq!(VectorDistanceMetric::EuclideanSquared.code(), 52);
        assert_eq!(VectorDistanceMetric::DotProduct.code(), 53);
        assert_eq!(VectorDistanceMetric::CosineSimilarity.code(), 54);
    }

    #[test]
    fn wire_size_matches_header_plus_elements() {
        assert_eq!(Vector::float32(vec![1.0, 2.0, 3.0]).wire_size(), 8 + 3 * 4);
        assert_eq!(Vector::float64(vec![1.0, 2.0]).wire_size(), 8 + 2 * 8);
        assert_eq!(Vector::int32(vec![1, 2, 3, 4]).wire_size(), 8 + 4 * 4);
        assert_eq!(Vector::float16(vec![0, 1]).wire_size(), 8 + 2 * 2);
    }

    #[test]
    fn header_is_little_endian_with_version_and_type() {
        let vector = Vector::float32(vec![1.5]);
        let buf = encode(&vector, vector.wire_size());

        assert_eq!(buf.data_buffer[0], VECTOR_VERSION);
        assert_eq!(buf.data_buffer[1], VectorElementType::Float32.code());
        // dimensions = 1, little-endian
        assert_eq!(&buf.data_buffer[2..6], &1u32.to_le_bytes());
        assert_eq!(&buf.data_buffer[6..8], &[0, 0]); // reserved
                                                     // the single float, little-endian
        assert_eq!(&buf.data_buffer[8..12], &1.5f32.to_le_bytes());
        assert_eq!(
            vector.wire_bytes(),
            [
                VECTOR_VERSION,
                VectorElementType::Float32.code(),
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0xc0,
                0x3f,
            ]
        );
    }

    #[test]
    fn client_always_emits_zero_reserved() {
        let vector = Vector::float32(vec![1.5]);
        let buf = encode(&vector, vector.wire_size());

        assert_eq!(&buf.data_buffer[6..8], &[0, 0]);
        assert_eq!(vector.reserved(), 0);
        assert_eq!(round_trip(&vector).reserved(), 0);
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
    fn empty_vectors_are_rejected_at_construction() {
        assert!(Vector::try_float16(vec![]).is_err());
        assert!(Vector::try_int32(vec![]).is_err());
        assert!(Vector::try_float32(vec![]).is_err());
        assert!(Vector::try_float64(vec![]).is_err());
    }

    #[test]
    fn dimensions_above_element_type_max_are_rejected() {
        // Per-type cap = VECTOR_MAX_ELEMENTS_BYTES / element size.
        assert_eq!(VectorElementType::Float16.max_dimensions(), 131_072);
        assert_eq!(VectorElementType::Int32.max_dimensions(), 65_536);
        assert_eq!(VectorElementType::Float32.max_dimensions(), 65_536);
        assert_eq!(VectorElementType::Float64.max_dimensions(), 32_768);

        let max = VectorElementType::Float64.max_dimensions();
        assert!(Vector::try_float64(vec![0.0; max]).is_ok());
        assert!(Vector::try_float64(vec![0.0; max + 1]).is_err());
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
    fn non_finite_float_elements_round_trip() {
        let f32_vector = Vector::float32(vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY]);
        assert_eq!(round_trip(&f32_vector), f32_vector);

        let f64_vector = Vector::float64(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY]);
        assert_eq!(round_trip(&f64_vector), f64_vector);

        let f16_vector = Vector::float16(vec![0x7c00, 0xfc00, 0x7e00]);
        assert_eq!(round_trip(&f16_vector), f16_vector);
    }

    #[test]
    fn negative_zero_differs_from_positive_zero() {
        // Bit-pattern equality (matches FloatValue): -0.0 and 0.0 are distinct.
        assert_ne!(Vector::float32(vec![-0.0]), Vector::float32(vec![0.0]));
        assert_ne!(Vector::float64(vec![-0.0]), Vector::float64(vec![0.0]));
    }

    #[test]
    fn equality_requires_matching_type_and_data() {
        // Same numeric values, different element type: not equal.
        assert_ne!(Vector::float32(vec![1.0]), Vector::float64(vec![1.0]));

        // Same type, different data: not equal.
        assert_ne!(
            Vector::float32(vec![1.0, 2.0]),
            Vector::float32(vec![1.0, 3.0])
        );

        // Same type and data: equal.
        assert_eq!(
            Vector::float32(vec![1.0, 2.0]),
            Vector::float32(vec![1.0, 2.0])
        );
    }

    #[test]
    fn ordering_by_element_type_then_elements() {
        // Element-type code ordering dominates (Float16 < Int32 < Float32 < Float64).
        let f16 = Vector::float16(vec![0x7bff]);
        let i32v = Vector::int32(vec![i32::MIN]);
        let f32v = Vector::float32(vec![f32::MAX]);
        let f64v = Vector::float64(vec![-f64::MAX]);
        assert!(f16 < i32v);
        assert!(i32v < f32v);
        assert!(f32v < f64v);

        // Within a type: shorter prefix sorts first, then element-wise.
        assert!(Vector::float32(vec![1.0]) < Vector::float32(vec![1.0, 0.0]));
        assert!(Vector::float32(vec![1.0, 0.0]) < Vector::float32(vec![1.0, 2.0]));
    }

    #[test]
    fn ordering_uses_total_cmp_for_finite_floats() {
        assert!(Vector::float64(vec![-1.0]) < Vector::float64(vec![0.0]));
        assert!(Vector::float64(vec![0.0]) < Vector::float64(vec![1.0]));
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
    fn from_bytes_preserves_nonstandard_version() {
        let body: Vec<u8> = [1.0f32, 2.0]
            .iter()
            .flat_map(|x| x.to_bits().to_le_bytes())
            .collect();
        let len = VECTOR_HEADER_SIZE + body.len();
        let mut buf = craft(2, VectorElementType::Float32.code(), 2, [0, 0], &body);

        let vector = Vector::from_bytes(&mut buf, len).unwrap();
        assert_eq!(vector.version(), 2);
        assert_eq!(encode(&vector, vector.wire_size()).data_buffer[0], 2);
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
    fn from_bytes_decodes_empty_body() {
        let mut buf = craft(
            VECTOR_VERSION,
            VectorElementType::Float32.code(),
            0,
            [0, 0],
            &[],
        );
        let vector = Vector::from_bytes(&mut buf, VECTOR_HEADER_SIZE).unwrap();
        assert_eq!(vector.element_type(), VectorElementType::Float32);
        assert_eq!(vector.dimensions(), 0);
    }

    #[test]
    fn from_bytes_rejects_truncated_body() {
        // Header claims 4 float32 elements (16 body bytes) but only 8 are given.
        let body = [0u8; 8];
        let len = VECTOR_HEADER_SIZE + body.len();
        let mut buf = craft(
            VECTOR_VERSION,
            VectorElementType::Float32.code(),
            4,
            [0, 0],
            &body,
        );
        assert!(
            Vector::from_bytes(&mut buf, len).is_err(),
            "declared dimensions exceed available bytes"
        );
    }

    #[test]
    fn from_bytes_preserves_reserved_bits_and_skips_trailing_bytes() {
        let body: Vec<u8> = 7.0f32.to_bits().to_le_bytes().to_vec();
        let trailing = [0xAAu8, 0xBB, 0xCC];
        let mut full = body.clone();
        full.extend_from_slice(&trailing);
        let len = VECTOR_HEADER_SIZE + full.len();

        let mut buf = craft(
            VECTOR_VERSION,
            VectorElementType::Float32.code(),
            1,
            [0xAB, 0xCD],
            &full,
        );
        let vector = Vector::from_bytes(&mut buf, len).unwrap();
        assert_eq!(vector.reserved(), u16::from_le_bytes([0xAB, 0xCD]));
        assert_eq!(vector.data(), &VectorData::Float32(vec![7.0]));
        assert_eq!(buf.data_offset, len);
    }

    #[test]
    fn float16_special_bit_patterns_round_trip() {
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
        assert_eq!(v.reserved(), 0);
        assert_eq!(round_trip(&v), v);
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
        // Avoid a huge allocation.
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
        assert_eq!(
            &buf.data_buffer[..lead],
            &[0xEE; 4],
            "leading bytes untouched"
        );
        assert_eq!(buf.data_buffer[lead], VECTOR_VERSION);

        buf.data_offset = lead;
        assert_eq!(Vector::from_bytes(&mut buf, size).unwrap(), v);
    }
}
