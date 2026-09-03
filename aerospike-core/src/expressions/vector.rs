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

//! Vector-distance filter expressions.
//!
//! Query vectors use the complete little-endian VECTOR wire value.
//! Incomparable vector operands evaluate as unknown. For an expression read,
//! use [`ExpReadFlags::EvalNoFail`](crate::operations::exp::ExpReadFlags::EvalNoFail)
//! to receive that as an absent result instead of `OpNotApplicable`.

use crate::expressions::{ExpOp, Expression};
use crate::vector::Vector;
use crate::Value;

/// Creates a squared-Euclidean-distance expression (not square-rooted). Smaller is closer.
///
/// If the bin is not a VECTOR or its element type or dimensions differ from
/// `query`, the server evaluates the expression as unknown.
pub fn euclidean_squared_distance(query: &Vector, bin: Expression) -> Expression {
    build_distance(ExpOp::VectorEuclideanDistance, query, bin)
}

/// Creates a dot-product expression. Larger is more similar.
///
/// If the bin is not a VECTOR or its element type or dimensions differ from
/// `query`, the server evaluates the expression as unknown.
pub fn dot_product(query: &Vector, bin: Expression) -> Expression {
    build_distance(ExpOp::VectorDotProduct, query, bin)
}

/// Creates a cosine-similarity expression. Larger is more similar.
///
/// If the bin is not a VECTOR or its element type or dimensions differ from
/// `query`, the server evaluates the expression as unknown.
pub fn cosine_similarity(query: &Vector, bin: Expression) -> Expression {
    build_distance(ExpOp::VectorCosineSimilarity, query, bin)
}

/// Builds the three-element vector-distance expression form.
fn build_distance(opcode: ExpOp, query: &Vector, bin: Expression) -> Expression {
    Expression::new(
        Some(opcode),
        Some(Value::Blob(query.wire_bytes())),
        Some(bin),
        None,
        None,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::buffer::Buffer;
    use crate::expressions::vector_bin;

    #[test]
    fn distance_builders_can_be_packed() {
        let query = Vector::float32(vec![1.0]);
        for exp in [
            euclidean_squared_distance(&query, vector_bin("v".to_string())),
            dot_product(&query, vector_bin("v".to_string())),
            cosine_similarity(&query, vector_bin("v".to_string())),
        ] {
            exp.size().expect("vector distance expr should pack");
        }
    }

    #[test]
    fn distance_builders_use_distinct_opcodes_and_full_vector_literals() {
        let query = Vector::float32(vec![1.0]);
        for (exp, opcode) in [
            (
                euclidean_squared_distance(&query, vector_bin("v".to_string())),
                0x34,
            ),
            (dot_product(&query, vector_bin("v".to_string())), 0x35),
            (cosine_similarity(&query, vector_bin("v".to_string())), 0x36),
        ] {
            let expected = [
                0x93, // array(3)
                opcode, 0x93, 0x51, 0x0a, 0xa1, 0x76, // vector bin "v"
                0xad, 0x04, // BLOB particle, 12-byte complete VECTOR wire value
                0x01, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // VECTOR header
                0x00, 0x00, 0x80, 0x3f, // float32 1.0, little-endian
            ];

            let mut buf = Buffer::new(usize::MAX);
            buf.resize_buffer(expected.len()).unwrap();
            buf.data_offset = 0;
            exp.pack(&mut Some(&mut buf))
                .expect("vector distance expr should pack");
            assert_eq!(&buf.data_buffer[..expected.len()], &expected);
        }
    }
}
