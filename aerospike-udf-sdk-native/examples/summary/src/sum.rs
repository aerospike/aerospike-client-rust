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

//! The sum, exactly — **two accumulators, combined only at the end**.
//!
//! An integer bin is an `i64` on the device. Widening it to `f64` on the way in throws bits away before any arithmetic
//! happens at all: past 2⁵³ consecutive integers stop being representable, so `(1<<53) + 1` *becomes* `1<<53` at the
//! cast and no later care can recover it. Timestamps in nanoseconds, byte counters and snowflake ids all live up
//! there.
//!
//! So integers are summed as integers, in `i128`, and doubles are summed as doubles, compensated. Neither one ever
//! sees the other's values, and the two totals meet exactly once — in [`ExactSum::total`].
//!
//! # Why they must not meet sooner
//!
//! Folding the integer running total into a float accumulator per record would round **once per record**. Keeping
//! them apart rounds **once per query**, at the end, and only because the answer has to leave as a single Aerospike
//! double. That is the whole reason for the split, and it is why `int_n == 0` or `flt_n == 0` — a bin with one
//! consistent type, which is the normal case — comes out with no rounding at all.
//!
//! # The integer side provably cannot overflow
//!
//! Every value is an `i64`, so `|v| ≤ 2⁶³`, and the count is an `i64`, so `n ≤ 2⁶³ − 1`. The largest reachable total
//! is therefore `(2⁶³ − 1) · 2⁶³ = 2¹²⁶ − 2⁶³`, against `i128::MAX = 2¹²⁷ − 1` — **a factor of two in hand**, and the
//! bound holds even if every record stored `i64::MIN` and the record count itself saturated. There is no dataset that
//! overflows this. `the_integer_accumulator_provably_cannot_overflow` asserts the arithmetic of that claim.
//!
//! It is still checked, because a partial is not a dataset. [`from_partial`] will accept whatever two `i64`s arrive,
//! so the accumulator can be *handed* a total near `i128::MAX` by a malformed or hostile partial — the same lesson the
//! HyperLogLog register bytes taught. Overflow saturates and sets a flag that `finalize` reports, rather than panicking
//! in debug and wrapping in release.
//!
//! [`from_partial`]: aerospike_udf_sdk_native::AsPartial
//!
//! # Why the wire form is two `i64`s
//!
//! There is no 128-bit numeric anywhere in the path: not in `PartialField`, not in the native ABI, not in Aerospike.
//! So the `i128` is carried as its high and low halves and reassembled on read — exact, two fields, and a plain
//! `i128` add to merge.
//!
//! # Why the float side is Neumaier rather than exact
//!
//! Measured, over 100,000 values around 1e8 whose true sum is known exactly because each is a multiple of 2⁻¹⁰:
//!
//! | summing | ns/value | error |
//! |---|---|---|
//! | plain `f64` loop | 1.04 | 34 ulps |
//! | `mean · n`, which this replaces | 5.44 | 18 ulps |
//! | **Neumaier compensated** | **1.25** | **0 ulps** |
//! | Shewchuk expansion, exact for any length | 4.83 | 0 ulps |
//!
//! Neumaier costs 0.2 ns/value over the naive loop, keeps the rounding error of every add in a second `f64`, and
//! bounds the total error at O(ε) instead of O(n·ε). Shewchuk is *provably* exact rather than merely exact on this
//! data, and costs 4× and a `Vec` per accumulator to close a gap between "one ulp" and "zero ulps" that no caller can
//! observe — and that sits twelve orders of magnitude below the 1% error this module already reports on its
//! percentiles.

use aerospike_udf_sdk_native::partial;

partial! {
    /// Two running totals for one bin: integers in `i128`, doubles compensated in `f64`.
    ///
    /// The `int_hi`/`int_lo` pair is one `i128` — see the module note on why it is spelled as two fields.
    pub struct ExactSum {
        /// High 64 bits of the integer total.
        int_hi: i64,
        /// Low 64 bits of the integer total, as a bit pattern rather than a number.
        int_lo: i64,
        int_n: i64,
        /// Neumaier running total of the double-typed values.
        flt: f64,
        /// The rounding error `flt` has accumulated, kept exactly. Folded in once, at the end.
        flt_c: f64,
        flt_n: i64,
        /// Set when the integer total had to saturate, which only a malformed partial can cause.
        int_saturated: bool,
        /// Set when a partial arrived carrying a non-finite float total, which was refused rather than absorbed.
        flt_refused: bool,
    }
}

impl ExactSum {
    /// The integer total, reassembled from its halves.
    pub fn int(&self) -> i128 {
        ((self.int_hi as i128) << 64) | (self.int_lo as u64 as i128)
    }

    fn set_int(&mut self, v: i128) {
        self.int_hi = (v >> 64) as i64;
        self.int_lo = v as u64 as i64;
    }

    /// One integer observation, as the `i64` it was stored as — **not** widened to `f64` first.
    pub fn push_int(&mut self, v: i64) {
        self.add_int(v as i128);
        self.int_n = self.int_n.saturating_add(1);
    }

    /// One double observation. `NaN` and infinities are the caller's problem and never reach here.
    pub fn push_f64(&mut self, v: f64) {
        self.add_f64(v);
        self.flt_n = self.flt_n.saturating_add(1);
    }

    /// Unreachable from any dataset — see the module note — so this exists for partials, and saturates visibly.
    fn add_int(&mut self, v: i128) {
        match self.int().checked_add(v) {
            Some(t) => self.set_int(t),
            None => {
                self.set_int(if v > 0 { i128::MAX } else { i128::MIN });
                self.int_saturated = true;
            }
        }
    }

    /// Neumaier's compensated add.
    ///
    /// `s + v` rounds, and the amount it rounded by is recoverable *exactly* with one subtraction — which term to
    /// subtract from depends on which operand is larger, and that branch is the difference between Neumaier's version
    /// and Kahan's. Kahan's loses the correction when `v` is the bigger of the two, which is exactly what happens when
    /// a large value arrives late into a small running total.
    fn add_f64(&mut self, v: f64) {
        let t = self.flt + v;

        self.flt_c += if self.flt.abs() >= v.abs() {
            (self.flt - t) + v
        } else {
            (v - t) + self.flt
        };

        self.flt = t;
    }

    /// Combine another accumulator's totals into this one.
    ///
    /// The integer side is an `i128` add, so it is **exactly** associative and commutative. The float side merges the
    /// other's sum *and* its compensation, both through the compensated add, so a merge is no less accurate than the
    /// accumulation was.
    pub fn merge(&mut self, other: &Self) {
        self.add_int(other.int());
        self.int_n = self.int_n.saturating_add(other.int_n);
        self.int_saturated |= other.int_saturated;

        // A partial can carry anything. Absorbing an infinity here would poison every subsequent record's
        // contribution, and the record that arrives after a `NaN` deserves better than to be silently discarded.
        if other.flt.is_finite() && other.flt_c.is_finite() {
            self.add_f64(other.flt);
            self.add_f64(other.flt_c);
            self.flt_n = self.flt_n.saturating_add(other.flt_n);
        } else {
            // Its count is dropped along with its total, deliberately: a real total over an inflated count is a
            // plausible wrong mean, and this module's whole stance is that an absent or flagged answer beats one.
            self.flt_refused = true;
        }

        self.flt_refused |= other.flt_refused;
    }

    /// Values accumulated, both sides.
    pub fn count(&self) -> i64 {
        self.int_n.saturating_add(self.flt_n)
    }

    pub fn int_count(&self) -> i64 {
        self.int_n
    }

    pub fn double_count(&self) -> i64 {
        self.flt_n
    }

    pub fn saturated(&self) -> bool {
        self.int_saturated
    }

    pub fn refused_a_partial(&self) -> bool {
        self.flt_refused
    }

    /// The double side's total, with its compensation folded in.
    pub fn double_total(&self) -> f64 {
        self.flt + self.flt_c
    }

    /// Both sides as one `f64`, which is the only shape an Aerospike double can carry.
    ///
    /// This is the one place the two totals meet, and the one rounding the design cannot avoid. When the bin held only
    /// integers the addition is `int as f64 + 0.0` and the *only* loss is the `i128 → f64` conversion — which is why
    /// [`exact`](Self::exact) exists to sidestep even that.
    pub fn total(&self) -> Option<f64> {
        if self.count() == 0 {
            return None;
        }

        let v = self.int() as f64 + self.double_total();

        v.is_finite().then_some(v)
    }

    /// The total as an exact integer, when it is one.
    ///
    /// `Some` only when no double-typed value was ever seen, so the `i128` holds the answer with every bit — and not
    /// when the integer side had to saturate, because then it does not.
    pub fn exact(&self) -> Option<i128> {
        (self.flt_n == 0 && self.int_n > 0 && !self.int_saturated).then(|| self.int())
    }

    /// The mean, from the total rather than from Welford's running one.
    ///
    /// Welford's mean drifts by O(n·ε) — 18 ulps over 100,000 values in the benchmark above. Dividing an accurate
    /// total by the count instead costs at most one rounding, so this is the better of the two and it is what
    /// `finalize` reports.
    pub fn mean(&self) -> Option<f64> {
        let n = self.count();

        if n <= 0 {
            return None;
        }

        let m = self.total()? / n as f64;

        m.is_finite().then_some(m)
    }

    /// Put the integer total somewhere no dataset can reach, to test that arriving there is survivable.
    ///
    /// The real route is `from_partial`, which will accept whatever two `i64`s a partial carries — but reaching it
    /// needs a host, and this is arithmetic. Also how the hi/lo split's round trip is tested.
    pub fn set_int_for_test(&mut self, v: i128) {
        self.set_int(v);
    }

    /// Likewise for the double side, where the value a partial can arrive with is a non-finite one.
    pub fn set_double_for_test(&mut self, flt: f64, flt_c: f64, n: i64) {
        self.flt = flt;
        self.flt_c = flt_c;
        self.flt_n = n;
    }
}
