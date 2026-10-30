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
//! 0       1             version       Vector format version, currently 1.
//! 1       1             element_type  See [`VectorElementType`].
//! 2       4             dimensions    Element count, little-endian, at least 1.
//! 6       variable      data          Contiguous little-endian elements.
//! ```
//!
//! A vector has at least one element, and a floating-point element is finite:
//! the constructors refuse NaN and infinities, and the decoder refuses an
//! unknown version, a zero dimension count and a length that does not match
//! the header exactly.
//!
use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt;

use crate::commands::buffer::Buffer;
use crate::errors::{Error, Result};

/// Current vector wire-format version.
pub const VECTOR_VERSION: u8 = 1;

/// Size in bytes of the fixed vector header (`version`, `element_type` and
/// `dimensions`).
pub const VECTOR_HEADER_SIZE: usize = 6;

/// A `float16` whose exponent bits are all set is an infinity or a NaN.
const FLOAT16_EXPONENT_MASK: u16 = 0x7c00;

/// Maximum size in bytes of a vector's element array.
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

    /// Largest supported dimension count for this element type.
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

    /// The first element that is an infinity or a NaN, as its index and its
    /// printed form; `None` when every element is finite. Integers always are.
    fn first_non_finite(&self) -> Option<(usize, String)> {
        match self {
            VectorData::Float16(d) => d
                .iter()
                .position(|&x| x & FLOAT16_EXPONENT_MASK == FLOAT16_EXPONENT_MASK)
                .map(|i| (i, format!("{:#06x}", d[i]))),
            VectorData::Int32(_) => None,
            VectorData::Float32(d) => d
                .iter()
                .position(|x| !x.is_finite())
                .map(|i| (i, d[i].to_string())),
            VectorData::Float64(d) => d
                .iter()
                .position(|x| !x.is_finite())
                .map(|i| (i, d[i].to_string())),
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
    data: VectorData,
}

impl Vector {
    /// Creates a `float16` vector from raw IEEE 754 bit patterns.
    ///
    /// # Panics
    ///
    /// When the data is empty, too long, or holds an infinity or a NaN; see
    /// [`Self::try_float16`].
    pub fn float16(data: Vec<u16>) -> Self {
        Self::try_float16(data).expect("invalid vector data")
    }

    /// Creates an `int32` vector.
    ///
    /// # Panics
    ///
    /// When the data is empty or too long; see [`Self::try_int32`].
    pub fn int32(data: Vec<i32>) -> Self {
        Self::try_int32(data).expect("invalid vector data")
    }

    /// Creates a `float32` vector.
    ///
    /// # Panics
    ///
    /// When the data is empty, too long, or holds an infinity or a NaN; see
    /// [`Self::try_float32`].
    pub fn float32(data: Vec<f32>) -> Self {
        Self::try_float32(data).expect("invalid vector data")
    }

    /// Creates a `float64` vector.
    ///
    /// # Panics
    ///
    /// When the data is empty, too long, or holds an infinity or a NaN; see
    /// [`Self::try_float64`].
    pub fn float64(data: Vec<f64>) -> Self {
        Self::try_float64(data).expect("invalid vector data")
    }

    /// Creates a validated `float16` vector.
    ///
    /// # Errors
    ///
    /// An empty vector, more elements than the type allows, or an element
    /// whose exponent bits are all set (an infinity or a NaN).
    pub fn try_float16(data: Vec<u16>) -> Result<Self> {
        Self::current(VectorData::Float16(data))
    }

    /// Creates a validated `int32` vector.
    ///
    /// # Errors
    ///
    /// An empty vector, or more elements than the type allows.
    pub fn try_int32(data: Vec<i32>) -> Result<Self> {
        Self::current(VectorData::Int32(data))
    }

    /// Creates a validated `float32` vector.
    ///
    /// # Errors
    ///
    /// An empty vector, more elements than the type allows, or a non-finite
    /// element.
    pub fn try_float32(data: Vec<f32>) -> Result<Self> {
        Self::current(VectorData::Float32(data))
    }

    /// Creates a validated `float64` vector.
    ///
    /// # Errors
    ///
    /// An empty vector, more elements than the type allows, or a non-finite
    /// element.
    pub fn try_float64(data: Vec<f64>) -> Result<Self> {
        Self::current(VectorData::Float64(data))
    }

    fn current(data: VectorData) -> Result<Self> {
        let dimensions = data.dimensions();

        if dimensions == 0 {
            return Err(Error::invalid_argument(
                "vector must have at least 1 dimension".to_string(),
            ));
        }

        let max = data.element_type().max_dimensions();

        if dimensions > max {
            return Err(Error::invalid_argument(format!(
                "vector dimensions {dimensions} exceeds max {max} for element type {}",
                data.element_type()
            )));
        }

        if let Some((index, value)) = data.first_non_finite() {
            return Err(Error::invalid_argument(format!(
                "vector element {index} is not finite: {value}"
            )));
        }

        Ok(Vector { data })
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

    /// Wire size, including the header.
    pub(crate) const fn wire_size(&self) -> usize {
        VECTOR_HEADER_SIZE + self.dimensions() * self.element_type().byte_size()
    }

    /// Serializes this vector in the little-endian wire format.
    pub(crate) fn write_to(&self, buf: &mut Buffer) -> usize {
        buf.mark_vector();
        buf.write_u8(VECTOR_VERSION);
        buf.write_u8(self.element_type().code());
        buf.write_u32_little_endian(self.dimensions() as u32);

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
    /// 6-byte header: the form the server stores, vector-distance expressions
    /// consume and client-side Lua receives. [`from_wire_bytes`](Self::from_wire_bytes)
    /// is the inverse.
    #[must_use]
    // Resizing a fresh, unbounded buffer to a validated size cannot fail, so
    // the `expect` below is not a panic the caller can reach.
    #[allow(clippy::missing_panics_doc)]
    pub fn wire_bytes(&self) -> Vec<u8> {
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(self.wire_size())
            .expect("validated vector wire size must fit the buffer");
        buf.data_offset = 0;
        self.write_to(&mut buf);
        buf.data_buffer.clone()
    }

    /// Decodes a vector particle at the buffer's current offset.
    ///
    /// # Errors
    ///
    /// Rejects an unknown format version, an unknown element type, a zero or
    /// oversized dimension count, and a length that does not match the header
    /// exactly. Each is a parse error: the bytes did not come from a vector
    /// this client understands.
    pub(crate) fn from_bytes(buf: &mut Buffer, len: usize) -> Result<Self> {
        if len < VECTOR_HEADER_SIZE {
            return Err(Error::bad_response(format!(
                "invalid vector length: {len}, need at least {VECTOR_HEADER_SIZE}"
            )));
        }

        let version = buf.read_u8(None);
        if version != VECTOR_VERSION {
            return Err(Error::bad_response(format!(
                "unsupported vector format version {version}; this client reads version \
                 {VECTOR_VERSION}"
            )));
        }
        let type_code = buf.read_u8(None);
        let Some(element_type) = VectorElementType::try_from_code(type_code) else {
            return Err(Error::bad_response(format!(
                "unknown vector element type code: {type_code}"
            )));
        };
        let dimensions = buf.read_u32_little_endian(None) as usize;

        if dimensions == 0 {
            return Err(Error::bad_response(
                "vector must have at least 1 dimension".to_string(),
            ));
        }

        if dimensions > element_type.max_dimensions() {
            return Err(Error::bad_response(format!(
                "vector dimensions {dimensions} exceeds maximum {}",
                element_type.max_dimensions()
            )));
        }

        let data_size = dimensions
            .checked_mul(element_type.byte_size())
            .ok_or_else(|| Error::bad_response("vector dimensions overflow"))?;

        if len != VECTOR_HEADER_SIZE + data_size {
            return Err(Error::bad_response(format!(
                "invalid vector length: {len}, expected {} for {dimensions} {element_type} \
                 elements",
                VECTOR_HEADER_SIZE + data_size
            )));
        }

        let bytes = buf.read_blob(data_size)?;
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

        Ok(Vector { data })
    }

    /// Decodes a complete vector wire value: the 6-byte header followed by
    /// the little-endian elements, as the server stores it and as client-side
    /// Lua receives it.
    ///
    /// # Errors
    ///
    /// Rejects an unknown format version, an unknown element type, a zero or
    /// oversized dimension count, and a length that does not match the header
    /// exactly, each as [`ErrorKind::InvalidArgument`](crate::ErrorKind::InvalidArgument):
    /// the bytes are the caller's, not a server reply.
    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self> {
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(bytes.len())?;
        buf.data_buffer[..bytes.len()].copy_from_slice(bytes);
        buf.data_offset = 0;
        Self::from_bytes(&mut buf, bytes.len())
            .map_err(|e| Error::invalid_argument(e.message().unwrap_or_default()))
    }
}

/// Float elements compare by IEEE 754 bit pattern, keeping [`Eq`] reflexive.
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
                _ => Ordering::Equal,
            })
    }
}

impl PartialOrd for VectorData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Two vectors are equal when their element data match.
impl PartialEq for Vector {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for Vector {}

/// Orders by element data.
impl Ord for Vector {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
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

    fn round_trip(vector: &Vector) -> Vector {
        let size = vector.wire_size();
        let mut buf = encode(vector, size);
        Vector::from_bytes(&mut buf, size).unwrap()
    }

    fn craft(version: u8, type_code: u8, dimensions: u32, body: &[u8]) -> Buffer {
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(VECTOR_HEADER_SIZE + body.len()).unwrap();
        buf.data_offset = 0;
        buf.write_u8(version);
        buf.write_u8(type_code);
        buf.write_u32_little_endian(dimensions);
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
    fn wire_size_matches_header_plus_elements() {
        assert_eq!(Vector::float32(vec![1.0, 2.0, 3.0]).wire_size(), 6 + 3 * 4);
        assert_eq!(Vector::float64(vec![1.0, 2.0]).wire_size(), 6 + 2 * 8);
        assert_eq!(Vector::int32(vec![1, 2, 3, 4]).wire_size(), 6 + 4 * 4);
        assert_eq!(Vector::float16(vec![0, 1]).wire_size(), 6 + 2 * 2);
    }

    #[test]
    fn header_is_little_endian_with_version_and_type() {
        let vector = Vector::float32(vec![1.5]);
        let buf = encode(&vector, vector.wire_size());

        assert_eq!(buf.data_buffer[0], VECTOR_VERSION);
        assert_eq!(buf.data_buffer[1], VectorElementType::Float32.code());
        assert_eq!(&buf.data_buffer[2..6], &1u32.to_le_bytes());
        assert_eq!(&buf.data_buffer[6..10], &1.5f32.to_le_bytes());
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
                0xc0,
                0x3f,
            ]
        );
    }

    #[test]
    fn int32_and_float64_elements_are_little_endian() {
        let v = Vector::int32(vec![1, -2]);
        let buf = encode(&v, v.wire_size());
        assert_eq!(&buf.data_buffer[6..10], &1i32.to_le_bytes());
        assert_eq!(&buf.data_buffer[10..14], &(-2i32).to_le_bytes());

        let v = Vector::float64(vec![1.5]);
        let buf = encode(&v, v.wire_size());
        assert_eq!(&buf.data_buffer[6..14], &1.5f64.to_le_bytes());
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
            round_trip(&Vector::float32(vec![0.1, -2.5, 3.5])),
            Vector::float32(vec![0.1, -2.5, 3.5])
        );
        assert_eq!(
            round_trip(&Vector::float64(vec![0.1, -2.5, f64::MAX])),
            Vector::float64(vec![0.1, -2.5, f64::MAX])
        );
    }

    #[test]
    fn empty_vectors_are_rejected() {
        for result in [
            Vector::try_float16(vec![]),
            Vector::try_int32(vec![]),
            Vector::try_float32(vec![]),
            Vector::try_float64(vec![]),
        ] {
            let err = result.expect_err("an empty vector must be refused");
            assert!(
                err.to_string().contains("at least 1 dimension"),
                "unexpected message: {err}"
            );
        }
    }

    #[test]
    fn non_finite_float_elements_are_rejected() {
        for data in [
            vec![1.0, f32::NAN],
            vec![f32::INFINITY],
            vec![0.0, 0.0, f32::NEG_INFINITY],
        ] {
            let err = Vector::try_float32(data).expect_err("non-finite f32 must be refused");
            assert!(err.to_string().contains("not finite"), "{err}");
        }
        for data in [vec![f64::NAN], vec![f64::INFINITY], vec![f64::NEG_INFINITY]] {
            let err = Vector::try_float64(data).expect_err("non-finite f64 must be refused");
            assert!(err.to_string().contains("not finite"), "{err}");
        }
        // +inf, -inf, quiet NaN and signalling NaN, as float16 bit patterns.
        for pattern in [0x7c00u16, 0xfc00, 0x7e00, 0x7c01] {
            let err = Vector::try_float16(vec![0x3c00, pattern])
                .expect_err("non-finite f16 must be refused");
            assert!(err.to_string().contains("element 1"), "{err}");
        }
        // Integers have no non-finite values; the extremes are fine.
        assert!(Vector::try_int32(vec![i32::MIN, i32::MAX]).is_ok());
        // The largest finite float16 (65504) is accepted.
        assert!(Vector::try_float16(vec![0x7bff, 0xfbff]).is_ok());
    }

    #[test]
    fn dimensions_above_element_type_max_are_rejected() {
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
    fn float_extremes_round_trip() {
        let f32_vector = Vector::float32(vec![f32::MAX, f32::MIN, f32::MIN_POSITIVE, -0.0]);
        assert_eq!(round_trip(&f32_vector), f32_vector);

        let f64_vector = Vector::float64(vec![f64::MAX, f64::MIN, f64::MIN_POSITIVE, -0.0]);
        assert_eq!(round_trip(&f64_vector), f64_vector);
    }

    #[test]
    fn negative_zero_differs_from_positive_zero() {
        assert_ne!(Vector::float32(vec![-0.0]), Vector::float32(vec![0.0]));
        assert_ne!(Vector::float64(vec![-0.0]), Vector::float64(vec![0.0]));
    }

    #[test]
    fn equality_requires_matching_type_and_data() {
        assert_ne!(Vector::float32(vec![1.0]), Vector::float64(vec![1.0]));

        assert_ne!(
            Vector::float32(vec![1.0, 2.0]),
            Vector::float32(vec![1.0, 3.0])
        );

        assert_eq!(
            Vector::float32(vec![1.0, 2.0]),
            Vector::float32(vec![1.0, 2.0])
        );
    }

    #[test]
    fn ordering_by_element_type_then_elements() {
        let f16 = Vector::float16(vec![0x7bff]);
        let i32v = Vector::int32(vec![i32::MIN]);
        let f32v = Vector::float32(vec![f32::MAX]);
        let f64v = Vector::float64(vec![-f64::MAX]);
        assert!(f16 < i32v);
        assert!(i32v < f32v);
        assert!(f32v < f64v);

        assert!(Vector::float32(vec![1.0]) < Vector::float32(vec![1.0, 0.0]));
        assert!(Vector::float32(vec![1.0, 0.0]) < Vector::float32(vec![1.0, 2.0]));
    }

    #[test]
    fn ordering_uses_total_cmp_for_finite_floats() {
        assert!(Vector::float64(vec![-1.0]) < Vector::float64(vec![0.0]));
        assert!(Vector::float64(vec![0.0]) < Vector::float64(vec![1.0]));
    }

    #[test]
    fn from_bytes_rejects_unknown_version() {
        let body = [1.0f32.to_le_bytes(), 2.0f32.to_le_bytes()].concat();
        for version in [0u8, 2, 0xff] {
            let mut buf = craft(version, VectorElementType::Float32.code(), 2, &body);
            let err = Vector::from_bytes(&mut buf, VECTOR_HEADER_SIZE + body.len())
                .expect_err("an unknown version must be a parse error");
            assert!(err.to_string().contains("version"), "{err}");
        }
    }

    #[test]
    fn from_bytes_rejects_short_header() {
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(4).unwrap();
        buf.data_offset = 0;
        assert!(
            Vector::from_bytes(&mut buf, 4).is_err(),
            "too short for the 6-byte header"
        );
    }

    #[test]
    fn from_bytes_rejects_unknown_element_type() {
        let mut buf = craft(VECTOR_VERSION, 0x09, 1, &[0, 0, 0, 0]);
        assert!(
            Vector::from_bytes(&mut buf, VECTOR_HEADER_SIZE + 4).is_err(),
            "unknown element-type code must be rejected"
        );
    }

    #[test]
    fn from_bytes_rejects_zero_dimensions() {
        let mut buf = craft(VECTOR_VERSION, VectorElementType::Float32.code(), 0, &[]);
        let err = Vector::from_bytes(&mut buf, VECTOR_HEADER_SIZE)
            .expect_err("a zero dimension count must be rejected");
        assert!(err.to_string().contains("at least 1 dimension"), "{err}");
    }

    #[test]
    fn from_bytes_rejects_truncated_body() {
        let body = [0u8; 8];
        let len = VECTOR_HEADER_SIZE + body.len();
        let mut buf = craft(VECTOR_VERSION, VectorElementType::Float32.code(), 4, &body);
        assert!(
            Vector::from_bytes(&mut buf, len).is_err(),
            "declared dimensions exceed available bytes"
        );
    }

    #[test]
    fn from_bytes_rejects_trailing_bytes() {
        let body: Vec<u8> = 7.0f32.to_bits().to_le_bytes().to_vec();
        let trailing = [0xAAu8, 0xBB, 0xCC];
        let mut full = body;
        full.extend_from_slice(&trailing);
        let len = VECTOR_HEADER_SIZE + full.len();

        let mut buf = craft(VECTOR_VERSION, VectorElementType::Float32.code(), 1, &full);
        let err = Vector::from_bytes(&mut buf, len)
            .expect_err("the length must match the header exactly");
        assert!(err.to_string().contains("expected 10"), "{err}");
    }

    #[test]
    fn from_wire_bytes_round_trips_and_checks_the_length() {
        let v = Vector::float64(vec![1.0, -2.0]);
        assert_eq!(Vector::from_wire_bytes(&v.wire_bytes()).unwrap(), v);

        let mut longer = v.wire_bytes();
        longer.push(0);
        assert!(Vector::from_wire_bytes(&longer).is_err());
        assert!(Vector::from_wire_bytes(&v.wire_bytes()[..5]).is_err());
    }

    // The bytes are the caller's, so a malformed value is an invalid
    // argument, not a bad server response, and the text stays bare.
    #[test]
    fn from_wire_bytes_rejects_bad_input_as_an_invalid_argument() {
        let mut bytes = Vector::float32(vec![1.0]).wire_bytes();
        bytes[0] = VECTOR_VERSION + 1;
        let err = Vector::from_wire_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err.kind(), crate::ErrorKind::InvalidArgument),
            "{err}"
        );
        assert_eq!(
            err.message(),
            Some("unsupported vector format version 2; this client reads version 1")
        );
        assert_eq!(
            err.base_message(),
            "Invalid argument: unsupported vector format version 2; this client reads version 1"
        );
    }

    #[test]
    fn float16_finite_bit_patterns_round_trip() {
        // Largest finite, smallest subnormal, signed zeros, largest subnormal.
        let v = Vector::float16(vec![0x7bff, 0x0001, 0x0000, 0x8000, 0x03ff, 0xfbff]);
        let buf = encode(&v, v.wire_size());
        assert_eq!(&buf.data_buffer[6..8], &0x7bffu16.to_le_bytes());
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn metadata_accessors() {
        let v = Vector::float64(vec![1.0, 2.0, 3.0]);
        assert_eq!(v.element_type(), VectorElementType::Float64);
        assert_eq!(v.dimensions(), 3);
        assert_eq!(v.data(), &VectorData::Float64(vec![1.0, 2.0, 3.0]));
        assert_eq!(round_trip(&v), v);
        assert_eq!(v.wire_bytes()[0], VECTOR_VERSION);
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
        let mut buf = craft(VECTOR_VERSION, VectorElementType::Float32.code(), u32::MAX, &[]);
        assert!(Vector::from_bytes(&mut buf, VECTOR_HEADER_SIZE).is_err());
    }

    #[test]
    fn write_and_read_respect_buffer_offset() {
        let v = Vector::int32(vec![7, 8, 9]);
        let size = v.wire_size();
        let lead = 4;

        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(lead + size).unwrap();
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
