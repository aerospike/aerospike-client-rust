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

/// A dense vector of numeric elements, used for vector similarity search.
///
/// The variant selects the element type; elements are held in host order and
/// converted to/from the little-endian wire format on write/read.
///
/// A `Vector` converts into a [`Value`](crate::Value) via [`From`], so it can
/// be stored directly in a bin:
///
/// ```
/// use aerospike::{Vector, as_bin};
///
/// let embedding = Vector::Float32(vec![0.12, 0.98, -0.34]);
/// let bin = as_bin!("embedding", embedding);
/// ```
#[derive(Debug, Clone)]
pub enum Vector {
    /// `float16` elements as raw 16-bit patterns (see [`VectorElementType::Float16`]).
    Float16(Vec<u16>),
    /// `int32` elements.
    Int32(Vec<i32>),
    /// `float` (fp32) elements.
    Float32(Vec<f32>),
    /// `double` (fp64) elements.
    Float64(Vec<f64>),
}

impl Vector {
    /// The element type of this vector.
    pub const fn element_type(&self) -> VectorElementType {
        match self {
            Vector::Float16(_) => VectorElementType::Float16,
            Vector::Int32(_) => VectorElementType::Int32,
            Vector::Float32(_) => VectorElementType::Float32,
            Vector::Float64(_) => VectorElementType::Float64,
        }
    }

    /// Number of dimensions (elements) in this vector.
    pub const fn dimensions(&self) -> usize {
        match self {
            Vector::Float16(d) => d.len(),
            Vector::Int32(d) => d.len(),
            Vector::Float32(d) => d.len(),
            Vector::Float64(d) => d.len(),
        }
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
    /// For internal use only.
    pub(crate) fn write_to(&self, buf: &mut Buffer) -> usize {
        buf.write_u8(VECTOR_VERSION);
        buf.write_u8(self.element_type().code());
        buf.write_u32_little_endian(self.dimensions() as u32);
        buf.write_u8(0); // reserved
        buf.write_u8(0); // reserved

        match self {
            Vector::Float16(d) => {
                for &x in d {
                    buf.write_u16_little_endian(x);
                }
            }
            Vector::Int32(d) => {
                for &x in d {
                    buf.write_u32_little_endian(x as u32);
                }
            }
            Vector::Float32(d) => {
                for &x in d {
                    buf.write_u32_little_endian(x.to_bits());
                }
            }
            Vector::Float64(d) => {
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
        match self {
            Vector::Float16(d) => d.iter().for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
            Vector::Int32(d) => d.iter().for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
            Vector::Float32(d) => d.iter().for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
            Vector::Float64(d) => d.iter().for_each(|x| out.extend_from_slice(&x.to_le_bytes())),
        }
        out
    }

    /// Deserialize a vector from the wire format at the buffer's current
    /// offset. `len` is the number of bytes available for this particle. For
    /// internal use only.
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

        let _version = buf.read_u8(None);
        let type_code = buf.read_u8(None);
        let Some(element_type) = VectorElementType::try_from_code(type_code) else {
            return Err(Error::bad_response(format!(
                "unknown vector element type code: {type_code}"
            )));
        };
        let dimensions = buf.read_u32_little_endian(None) as usize;
        buf.skip(2); // reserved

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
        let vector = match element_type {
            VectorElementType::Float16 => Vector::Float16(
                bytes
                    .chunks_exact(2)
                    .map(|c| u16::from_le_bytes([c[0], c[1]]))
                    .collect(),
            ),
            VectorElementType::Int32 => Vector::Int32(
                bytes
                    .chunks_exact(4)
                    .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
                    .collect(),
            ),
            VectorElementType::Float32 => Vector::Float32(
                bytes
                    .chunks_exact(4)
                    .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
                    .collect(),
            ),
            VectorElementType::Float64 => Vector::Float64(
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

        Ok(vector)
    }
}

/// Float elements compare by IEEE 754 bit pattern (like
/// [`FloatValue`](crate::FloatValue)), keeping [`Eq`] reflexive.
impl PartialEq for Vector {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Vector::Float16(a), Vector::Float16(b)) => a == b,
            (Vector::Int32(a), Vector::Int32(b)) => a == b,
            (Vector::Float32(a), Vector::Float32(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
            }
            (Vector::Float64(a), Vector::Float64(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
            }
            _ => false,
        }
    }
}

impl Eq for Vector {}

/// Total order by element-type code, then element-wise (shorter prefix first),
/// floats via [`f32::total_cmp`]/[`f64::total_cmp`]. Only exists to give
/// [`Value`](crate::Value) a total order; the server does not order vector bins.
impl Ord for Vector {
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
                (Vector::Float16(a), Vector::Float16(b)) => a.cmp(b),
                (Vector::Int32(a), Vector::Int32(b)) => a.cmp(b),
                (Vector::Float32(a), Vector::Float32(b)) => cmp_by(a, b, f32::total_cmp),
                (Vector::Float64(a), Vector::Float64(b)) => cmp_by(a, b, f64::total_cmp),
                // Unreachable: equal type codes imply the same variant.
                _ => Ordering::Equal,
            })
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
        match self {
            Vector::Float16(d) => write!(f, "{d:?}")?,
            Vector::Int32(d) => write!(f, "{d:?}")?,
            Vector::Float32(d) => write!(f, "{d:?}")?,
            Vector::Float64(d) => write!(f, "{d:?}")?,
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::buffer::Buffer;

    fn round_trip(vector: &Vector) -> Vector {
        // Estimate/write agree, then decode the payload back.
        let size = vector.wire_size();

        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(size).unwrap();
        buf.data_offset = 0;
        let written = vector.write_to(&mut buf);
        assert_eq!(written, size, "write_to must write exactly wire_size bytes");

        buf.data_offset = 0;
        Vector::from_bytes(&mut buf, size).unwrap()
    }

    #[test]
    fn wire_size_matches_header_plus_elements() {
        assert_eq!(Vector::Float32(vec![1.0, 2.0, 3.0]).wire_size(), 8 + 3 * 4);
        assert_eq!(Vector::Float64(vec![1.0, 2.0]).wire_size(), 8 + 2 * 8);
        assert_eq!(Vector::Int32(vec![1, 2, 3, 4]).wire_size(), 8 + 4 * 4);
        assert_eq!(Vector::Float16(vec![0, 1]).wire_size(), 8 + 2 * 2);
    }

    #[test]
    fn header_is_little_endian_with_version_and_type() {
        let vector = Vector::Float32(vec![1.5]);
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(vector.wire_size()).unwrap();
        buf.data_offset = 0;
        vector.write_to(&mut buf);

        assert_eq!(buf.data_buffer[0], VECTOR_VERSION);
        assert_eq!(buf.data_buffer[1], VectorElementType::Float32.code());
        // dimensions = 1, little-endian
        assert_eq!(&buf.data_buffer[2..6], &[1, 0, 0, 0]);
        assert_eq!(&buf.data_buffer[6..8], &[0, 0]); // reserved
        // the single float, little-endian
        assert_eq!(&buf.data_buffer[8..12], &1.5f32.to_le_bytes());
    }

    #[test]
    fn round_trips_every_element_type() {
        assert_eq!(
            round_trip(&Vector::Float16(vec![0x3c00, 0x4000, 0xbc00])),
            Vector::Float16(vec![0x3c00, 0x4000, 0xbc00])
        );
        assert_eq!(
            round_trip(&Vector::Int32(vec![-5, 0, 7, i32::MIN, i32::MAX])),
            Vector::Int32(vec![-5, 0, 7, i32::MIN, i32::MAX])
        );
        assert_eq!(
            round_trip(&Vector::Float32(vec![0.1, -2.5, 3.14159])),
            Vector::Float32(vec![0.1, -2.5, 3.14159])
        );
        assert_eq!(
            round_trip(&Vector::Float64(vec![0.1, -2.5, f64::MAX])),
            Vector::Float64(vec![0.1, -2.5, f64::MAX])
        );
    }

    #[test]
    fn round_trips_empty_vector() {
        assert_eq!(round_trip(&Vector::Float32(vec![])), Vector::Float32(vec![]));
    }

    #[test]
    fn from_bytes_rejects_short_and_unknown() {
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(4).unwrap();
        buf.data_offset = 0;
        assert!(Vector::from_bytes(&mut buf, 4).is_err(), "too short for header");

        // Valid header length but an unknown element-type code (0x09).
        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(8).unwrap();
        buf.data_offset = 0;
        buf.write_u8(VECTOR_VERSION);
        buf.write_u8(0x09);
        buf.write_u32_little_endian(0);
        buf.write_u8(0);
        buf.write_u8(0);
        buf.data_offset = 0;
        assert!(Vector::from_bytes(&mut buf, 8).is_err(), "unknown element type");
    }

    #[test]
    fn metadata_accessors() {
        let v = Vector::Float64(vec![1.0, 2.0, 3.0]);
        assert_eq!(v.element_type(), VectorElementType::Float64);
        assert_eq!(v.dimensions(), 3);
        assert!(!v.is_empty());
        assert!(Vector::Int32(vec![]).is_empty());
    }
}
