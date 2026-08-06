// Copyright 2026 Aerospike, Inc.
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

//! Approximate distinct count, in 1 KB whatever the cardinality.
//!
//! HyperLogLog, at 2¹⁰ registers of one byte: about **1.6% standard error**, constant state, and a merge that is a
//! per-register maximum — associative, commutative, and idempotent, so combining per-thread partials is exact even
//! though the estimate is not.
//!
//! # Why implement one rather than use the server's
//!
//! Aerospike has an HLL particle type with its own operations, but those are client-side `operate()` commands: a UDF
//! cannot reach them, in any language. Counting distinct values *while* aggregating therefore needs the sketch inside
//! the plugin — and it is a hundred lines, which is the whole argument for the native ABI being cheap enough to make
//! that reasonable.
//!
//! One consequence worth stating: this is **not** interchangeable with an Aerospike HLL bin. The register layout is
//! this module's own, so a sketch built here cannot be unioned with one built by `operate()`.
//!
//! # Hashing a float
//!
//! `splitmix64`'s finaliser over the value's bit pattern. Deterministic, no dependency, and good enough that the
//! register distribution is uniform — which is the only property the estimate rests on. `-0.0` is normalised to
//! `0.0` first, or the same logical value would land in two registers.

use aerospike_udf_sdk_native::{partial, Blob};

/// log₂ of the register count.
const PRECISION: u32 = 10;
const REGISTERS: usize = 1 << PRECISION;

/// The largest rank the `push` path can produce.
///
/// The index takes the top `PRECISION` bits and a sentinel bit is ORed into the remainder, so at most
/// `64 - PRECISION + 1` leading zeros are countable. Nothing this module computes exceeds it — but a partial arrives
/// from the network as an opaque blob, and `estimate` shifts by whatever is in it. `1u64 << 200` panics in a debug
/// build and silently computes `1u64 << 8` in a release one, because Rust masks the shift amount, so a register from
/// another process is clamped rather than trusted.
const MAX_RANK: u8 = (64 - PRECISION + 1) as u8;

partial! {
    /// A HyperLogLog sketch.
    ///
    /// The registers travel as a [`Blob`] rather than a `Vec<i64>`: the SDK has no `PartialField` for `Vec<u8>` — it
    /// would collide with the blanket one for `Vec<T>` — and serialising 1,024 bytes as a list of integers would be
    /// several times the size for no gain.
    pub struct Hll {
        regs: Blob,
    }
}

impl Hll {
    pub fn new() -> Self {
        Self {
            regs: Blob(vec![0u8; REGISTERS]),
        }
    }

    /// The registers, length-normalised.
    ///
    /// # This is on the per-record path, and it once forgot that
    ///
    /// An earlier version of this method also clamped **every register value** here, to guarantee the shift in
    /// `estimate` was in range. It did guarantee that, and it turned an O(1) `push` into a 1024-byte scan per record:
    /// the benchmark's HyperLogLog stage went from **+7.8 ns/record to +258**, a 33× regression, shipped as a safety
    /// fix on reasoning alone without re-measuring.
    ///
    /// The clamp belongs where the untrusted value is *used*, not where it is reached. The only arithmetic that can
    /// fault on a hostile register is the `1u64 << r` in [`estimate`](Self::estimate), which runs once per query, and
    /// [`merge`](Self::merge) clamps the side it does not own. `push` only compares and stores, so it needs nothing.
    /// The length check stays: it is one comparison, and it is what keeps every other method free of them.
    fn slots(&mut self) -> &mut Vec<u8> {
        // A partial read back from the wire could be any length, and a default-constructed one is empty.
        if self.regs.0.len() != REGISTERS {
            self.regs.0.resize(REGISTERS, 0);
        }

        &mut self.regs.0
    }

    /// Overwrite the registers, for the test that feeds this module bytes no real sketch produces.
    #[doc(hidden)]
    pub fn set_registers_for_test(&mut self, regs: Blob) {
        self.regs = regs;
    }

    pub fn push(&mut self, v: f64) {
        let value = if v == 0.0 { 0.0 } else { v };
        let hash = mix(value.to_bits());

        // Top PRECISION bits choose the register; the rest supplies the run of leading zeros.
        let index = (hash >> (64 - PRECISION)) as usize;
        let remaining = (hash << PRECISION) | (1 << (PRECISION - 1));
        let rank = remaining.leading_zeros() as u8 + 1;

        let slots = self.slots();

        if rank > slots[index] {
            slots[index] = rank;
        }
    }

    /// Per-register maximum — which is why merging is exact even though counting is not.
    pub fn merge(&mut self, other: &Self) {
        if other.regs.0.len() != REGISTERS {
            return;
        }

        let slots = self.slots();

        for i in 0..REGISTERS {
            // The other side's registers are clamped as they are taken, so a corrupt partial cannot raise this sketch
            // above what its own arithmetic can represent.
            let r = other.regs.0[i].min(MAX_RANK);

            if r > slots[i] {
                slots[i] = r;
            }
        }
    }

    /// The estimated number of distinct values.
    pub fn estimate(&self) -> f64 {
        let regs = if self.regs.0.len() == REGISTERS {
            &self.regs.0[..]
        } else {
            return 0.0;
        };

        let m = REGISTERS as f64;
        let mut sum = 0.0;
        let mut zeros = 0u32;

        for &r in regs {
            // Clamped again here, not only in `slots`: this method takes `&self`, so it can be reached on a partial
            // that was deserialised and never normalised. One line, and it is the difference between a wrong answer and
            // a panic on input another process chose.
            let r = r.min(MAX_RANK);

            sum += 1.0 / ((1u64 << r) as f64);

            if r == 0 {
                zeros += 1;
            }
        }

        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / sum;

        // **Linear counting for small cardinalities.** The raw estimator is badly biased when most registers are
        // still empty, and at that end the number of empty registers is itself a good estimator. Without this the
        // sketch reports something like 700 for 100 distinct values, which looks like a working implementation and
        // is not.
        if raw <= 2.5 * m && zeros > 0 {
            return m * (m / zeros as f64).ln();
        }

        raw
    }

    /// The sketch's standard error, so a caller can put a band on the estimate rather than reading it as exact.
    pub fn relative_error() -> f64 {
        1.04 / (REGISTERS as f64).sqrt()
    }
}

/// `splitmix64`'s finaliser: cheap, dependency-free, and uniform enough for register selection.
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);

    z ^ (z >> 31)
}
