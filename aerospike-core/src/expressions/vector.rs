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

//! Vector Aerospike filter expressions.
//!
//! TODO(vector-exp-envelope, vector-exp-metric-semantics): the query-vector
//! envelope and each metric's semantics ([`l2_squared_distance`],
//! [`dot_product`], [`cosine_similarity`]) are named for their decided
//! target behavior, but this has not been double-checked against current
//! server code (the available `aerospike-server` checkout may be outdated),
//! and there are no integration tests against a real server yet. Verify and
//! add integration tests before relying on this in production.

use crate::expressions::{ExpOp, Expression};
use crate::vector::{Vector, VectorDistanceMetric};
use crate::Value;

/// Creates an expression that returns the squared L2 (squared Euclidean)
/// distance between a stored vector bin and `query` as a 64-bit float.
/// Smaller is closer.
///
/// The query vector's element type and dimension count must match the stored
/// vector; otherwise the expression evaluates to unknown. `bin` is typically
/// [`vector_bin`](crate::expressions::vector_bin).
///
/// See the [module docs](self): the wire contract is not yet double-checked
/// against current server code.
///
/// ```
/// use aerospike::expressions::{lt, float_val, vector_bin};
/// use aerospike::expressions::vector::l2_squared_distance;
/// use aerospike::Vector;
///
/// let query = Vector::float32(vec![0.12, 0.98, -0.34]);
/// let _exp = lt(
///     l2_squared_distance(&query, vector_bin("embedding".to_string())),
///     float_val(0.1),
/// );
/// ```
pub fn l2_squared_distance(query: &Vector, bin: Expression) -> Expression {
    build_distance(VectorDistanceMetric::L2Squared, query, bin)
}

/// Creates an expression that returns the dot product between a stored
/// vector bin and `query` as a 64-bit float. Larger is more similar.
///
/// The query vector's element type and dimension count must match the stored
/// vector; otherwise the expression evaluates to unknown. `bin` is typically
/// [`vector_bin`](crate::expressions::vector_bin).
///
/// See the [module docs](self): the wire contract is not yet double-checked
/// against current server code.
///
/// ```
/// use aerospike::expressions::{gt, float_val, vector_bin};
/// use aerospike::expressions::vector::dot_product;
/// use aerospike::Vector;
///
/// let query = Vector::float32(vec![0.12, 0.98, -0.34]);
/// let _exp = gt(
///     dot_product(&query, vector_bin("embedding".to_string())),
///     float_val(0.8),
/// );
/// ```
pub fn dot_product(query: &Vector, bin: Expression) -> Expression {
    build_distance(VectorDistanceMetric::DotProduct, query, bin)
}

/// Creates an expression that returns the cosine similarity between a stored
/// vector bin and `query` as a 64-bit float. Larger is more similar.
///
/// The query vector's element type and dimension count must match the stored
/// vector; otherwise the expression evaluates to unknown. `bin` is typically
/// [`vector_bin`](crate::expressions::vector_bin).
///
/// See the [module docs](self): the wire contract is not yet double-checked
/// against current server code.
///
/// ```
/// use aerospike::expressions::{gt, float_val, vector_bin};
/// use aerospike::expressions::vector::cosine_similarity;
/// use aerospike::Vector;
///
/// let query = Vector::float32(vec![0.12, 0.98, -0.34]);
/// let _exp = gt(
///     cosine_similarity(&query, vector_bin("embedding".to_string())),
///     float_val(0.8),
/// );
/// ```
pub fn cosine_similarity(query: &Vector, bin: Expression) -> Expression {
    build_distance(VectorDistanceMetric::Cosine, query, bin)
}

/// Shared builder behind [`l2_squared_distance`], [`dot_product`], and
/// [`cosine_similarity`]. Kept private: the metric is a compile-time choice
/// of *which named function* to call, not a runtime parameter callers should
/// pick from a bare [`VectorDistanceMetric`] — that keeps the public API
/// aligned with the PRD-specified per-metric builder shape (mirroring Java's
/// planned `VectorExp.cosineSimilarity`/`dotProduct`/`l2SquaredDistance`).
fn build_distance(metric: VectorDistanceMetric, query: &Vector, bin: Expression) -> Expression {
    Expression::new(
        Some(ExpOp::VectorDist),
        Some(Value::Blob(query.element_bytes())),
        Some(bin),
        Some(metric.code()),
        None,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::buffer::Buffer;
    use crate::expressions::vector_bin;

    // All three vector distance builders can now be packed/sent. This does not
    // confirm the server accepts or correctly interprets the payload -- see
    // the module-level TODO about double-checking against current server code
    // and adding integration tests.
    #[test]
    fn distance_builders_can_be_packed() {
        let query = Vector::float32(vec![1.0]);
        for exp in [
            l2_squared_distance(&query, vector_bin("v".to_string())),
            dot_product(&query, vector_bin("v".to_string())),
            cosine_similarity(&query, vector_bin("v".to_string())),
        ] {
            exp.size().expect("vector distance expr should pack");
        }
    }

    // Pins the wire form. Matches the server's `EXP_VECTOR_DIST` (opcode 52)
    // layout: array[4] = [VECTOR_DIST(52), metric, query blob, bin].
    #[test]
    fn cosine_similarity_wire_bytes() {
        let query = Vector::float32(vec![1.0]);
        let exp = cosine_similarity(&query, vector_bin("v".to_string()));

        let expected = [
            0x94, // array(4)
            0x34, // VECTOR_DIST (52)
            0x02, // metric = Cosine (2)
            0xa5, 0x04, 0x00, 0x00, 0x80, 0x3f, // BLOB of 1.0f32, little-endian
            0x93, 0x51, 0x06, 0xa1, 0x76, // bin "v": [Bin(81), BLOB type(6), "v"]
        ];

        let mut buf = Buffer::new(usize::MAX);
        buf.resize_buffer(expected.len()).unwrap();
        buf.data_offset = 0;
        exp.pack(&mut Some(&mut buf))
            .expect("vector distance expr should pack");
        assert_eq!(&buf.data_buffer[..expected.len()], &expected);
    }
}
