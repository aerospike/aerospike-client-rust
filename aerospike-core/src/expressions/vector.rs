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

/// Creates a squared-Euclidean-distance expression. Smaller is closer.
///
/// Incompatible vectors evaluate as unknown.
pub fn euclidean_squared_distance(query: &Vector, bin: Expression) -> Expression {
    build_distance(ExpOp::VectorEuclideanDistance, query, bin)
}

/// Creates a dot-product expression. Larger is more similar.
///
/// Incompatible vectors evaluate as unknown.
pub fn dot_product(query: &Vector, bin: Expression) -> Expression {
    build_distance(ExpOp::VectorDotProduct, query, bin)
}

/// Creates a cosine-similarity expression. Larger is more similar.
///
/// Incompatible vectors evaluate as unknown.
pub fn cosine_similarity(query: &Vector, bin: Expression) -> Expression {
    build_distance(ExpOp::VectorCosineSimilarity, query, bin)
}

/// Builds the three-element vector-distance expression form.
///
/// The query travels as a `Value::Vector` sharing the caller's element data,
/// so cloning the expression (a batch does it once per key) is a refcount
/// and comparing two clones is a pointer test. The packer writes it in the
/// BLOB form the server expects.
fn build_distance(opcode: ExpOp, query: &Vector, bin: Expression) -> Expression {
    Expression::new(
        Some(opcode),
        Some(Value::Vector(query.clone())),
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
    fn packing_vector_expressions_flags_the_buffer() {
        use crate::expressions::{float_val, ge, int_bin, int_val, list_val};

        fn packed_has_vector(exp: &Expression) -> bool {
            let size = exp.size().expect("expr should size");
            let mut buf = Buffer::new(usize::MAX);
            buf.resize_buffer(size).unwrap();
            buf.data_offset = 0;
            exp.pack(&mut Some(&mut buf)).expect("expr should pack");
            buf.contains_vector()
        }

        let query = Vector::float32(vec![1.0]);

        // Vector-distance queries are encoded as BLOB literals.
        let dist = euclidean_squared_distance(&query, vector_bin("v".to_string()));
        assert!(packed_has_vector(&dist));
        assert!(packed_has_vector(&ge(dist, float_val(0.5))));

        assert!(packed_has_vector(&vector_bin("v".to_string())));
        assert!(packed_has_vector(&list_val(vec![Value::Vector(query)])));
        assert!(!packed_has_vector(&ge(
            int_bin("a".to_string()),
            int_val(7)
        )));
    }

    // A hand-built expression carrying the query as a BLOB of its wire bytes
    // packs to the same bytes as the builder's shared `Value::Vector`.
    #[test]
    fn a_blob_query_operand_packs_like_a_vector_one() {
        let query = Vector::float32(vec![1.0, -2.5]);
        let built = euclidean_squared_distance(&query, vector_bin("v".to_string()));
        let by_hand = Expression::new(
            Some(ExpOp::VectorEuclideanDistance),
            Some(Value::Blob(query.wire_bytes())),
            Some(vector_bin("v".to_string())),
            None,
            None,
            None,
        );
        let pack = |exp: &Expression| {
            let size = exp.size().unwrap();
            let mut buf = Buffer::new(usize::MAX);
            buf.resize_buffer(size).unwrap();
            buf.data_offset = 0;
            exp.pack(&mut Some(&mut buf)).unwrap();
            buf.data_buffer[..size].to_vec()
        };
        assert_eq!(pack(&built), pack(&by_hand));

        // Clones of the builder's expression share the query and compare
        // equal without reading it; the by-hand one compares by content.
        assert_eq!(built, built.clone());
        assert_ne!(built, by_hand);
    }

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
                0xab, 0x04, // BLOB particle, 10-byte complete VECTOR wire value
                0x01, 0x03, 0x01, 0x00, 0x00, 0x00, // VECTOR header: version, type, dimensions
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

    fn packed_has_vector(exp: &Expression) -> bool {
        let size = exp.size().expect("expr should size");
        let mut buf = Buffer::new(size);
        buf.resize_buffer(size).unwrap();
        buf.data_offset = 0;
        exp.pack(&mut Some(&mut buf)).expect("expr should pack");
        buf.contains_vector()
    }

    #[test]
    fn every_distance_op_flags_the_buffer() {
        let query = Vector::float32(vec![1.0]);
        for exp in [
            euclidean_squared_distance(&query, vector_bin("v".to_string())),
            dot_product(&query, vector_bin("v".to_string())),
            cosine_similarity(&query, vector_bin("v".to_string())),
        ] {
            assert!(packed_has_vector(&exp));
        }
    }

    // Prepacked/base64 expressions are opaque bytes and are intentionally not
    // parsed for vectors — this pins that escape-hatch behavior.
    #[test]
    fn prepacked_vector_expression_is_not_flagged() {
        use crate::expressions::from_base64;
        let query = Vector::float32(vec![1.0]);
        let original = euclidean_squared_distance(&query, vector_bin("v".to_string()));
        assert!(packed_has_vector(&original));

        let opaque = from_base64(&original.base64().unwrap()).unwrap();
        assert!(!packed_has_vector(&opaque));
    }
}
