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

//! WIP vector-distance filter expressions.
//!
//! Requires `EXP_VECTOR_DIST`. Query payloads contain raw little-endian
//! elements, excluding the vector header.

use crate::expressions::{ExpOp, Expression};
use crate::vector::{Vector, VectorDistanceMetric};
use crate::Value;

/// Creates a squared-Euclidean-distance expression (not square-rooted). Smaller is closer.
///
/// WIP: requires `EXP_VECTOR_DIST`; query type and dimensions must match `bin`.
pub fn euclidean_squared_distance(query: &Vector, bin: Expression) -> Expression {
    build_distance(VectorDistanceMetric::EuclideanSquared, query, bin)
}

/// Creates a dot-product expression. Larger is more similar.
///
/// WIP: requires `EXP_VECTOR_DIST`; query type and dimensions must match `bin`.
pub fn dot_product(query: &Vector, bin: Expression) -> Expression {
    build_distance(VectorDistanceMetric::DotProduct, query, bin)
}

/// Creates a cosine-similarity expression. Larger is more similar.
///
/// WIP: requires `EXP_VECTOR_DIST`; query type and dimensions must match `bin`.
pub fn cosine_similarity(query: &Vector, bin: Expression) -> Expression {
    build_distance(VectorDistanceMetric::CosineSimilarity, query, bin)
}

/// Builds the shared vector-distance expression form.
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

    // WIP wire form: [VECTOR_DIST, metric, query blob, bin].
    #[test]
    fn cosine_similarity_wire_bytes() {
        let query = Vector::float32(vec![1.0]);
        let exp = cosine_similarity(&query, vector_bin("v".to_string()));

        let expected = [
            0x94, // array(4)
            0x34, // VECTOR_DIST (52)
            0x02, // metric = CosineSimilarity (2)
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
