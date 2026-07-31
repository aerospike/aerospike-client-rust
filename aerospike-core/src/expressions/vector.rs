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
//! # Work in progress — not usable yet
//!
//! The vector distance expression is **incomplete**. Its server wire contract
//! is not finalized (the query-vector envelope and the distance-metric
//! semantics are still in flux upstream — see the `vector-exp-envelope` and
//! `vector-exp-metric-semantics` TODOs). [`distance`] can be *constructed*, but
//! attaching it to a command and evaluating it returns an error at pack time
//! rather than sending a provisional payload the server may reject or
//! misinterpret. Do not depend on this API until it is finalized.

use crate::expressions::{ExpOp, Expression};
use crate::vector::{Vector, VectorDistanceMetric};
use crate::Value;

/// Creates an expression that returns the distance between a stored vector bin
/// and `query` as a 64-bit float, using the given metric.
///
/// The query vector's element type and dimension count must match the stored
/// vector; otherwise the expression evaluates to unknown. `bin` is typically
/// [`vector_bin`](crate::expressions::vector_bin).
///
/// # Work in progress
///
/// **This is not usable yet.** The server wire contract is not finalized, so a
/// constructed expression cannot be sent: packing it (which happens when it is
/// attached to a command) returns an
/// [`Error`](crate::Error). It exists only so callers can compile against the
/// eventual API. See the [module docs](self) for details.
///
/// ```
/// use aerospike::expressions::{gt, float_val, vector_bin};
/// use aerospike::expressions::vector::distance;
/// use aerospike::{Vector, VectorDistanceMetric};
///
/// // Builds, but cannot be sent to the server yet (see "Work in progress").
/// let query = Vector::Float32(vec![0.12, 0.98, -0.34]);
/// let _wip = gt(
///     distance(VectorDistanceMetric::Cosine, &query, vector_bin("embedding".to_string())),
///     float_val(0.8),
/// );
/// ```
pub fn distance(metric: VectorDistanceMetric, query: &Vector, bin: Expression) -> Expression {
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

    // WIP guard: size estimation (the first thing a real command does) errors,
    // so a vector distance expression can never be sent.
    #[test]
    fn distance_cannot_be_sent_yet() {
        let query = Vector::Float32(vec![1.0]);
        let exp = distance(
            VectorDistanceMetric::Cosine,
            &query,
            vector_bin("v".to_string()),
        );

        let err = exp.size().expect_err("vector distance expr must not pack yet");
        assert!(
            err.to_string().contains("work in progress"),
            "error should flag the WIP status: {err}"
        );
    }

    // Pins the provisional wire form so it stays correct until finalized. The
    // encoding runs and writes into the buffer, then packing returns the WIP
    // error; here we inspect the bytes it wrote and confirm it still errors.
    // Matches Java's VectorExp.distance / Exp.VectorDist:
    // array[4] = [VECTOR_DIST(52), metric, query blob, bin].
    #[test]
    fn distance_provisional_wire_bytes() {
        let query = Vector::Float32(vec![1.0]);
        let exp = distance(
            VectorDistanceMetric::Cosine,
            &query,
            vector_bin("v".to_string()),
        );

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
        // Encoding writes the provisional bytes, then reports the WIP error.
        assert!(exp.pack(&mut Some(&mut buf)).is_err());
        assert_eq!(&buf.data_buffer[..expected.len()], &expected);
    }
}
