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

//! Summary statistics over a numeric bin, in **one pass and bounded state**.
//!
//! ```text
//! summary("latency_ms")                    everything below, over every record
//! summary("latency_ms", 0.005)             tighter percentiles: 0.5% relative error
//! summary("latency_ms", 0.01, 256)         and a wider frequency table for the mode
//! ```
//!
//! | statistic | how | exact? |
//! |---|---|---|
//! | count, min, max | running | **exact** |
//! | sum | two accumulators, `i128` and compensated `f64` | **exact** for an integer bin, to O(ε) for a double one |
//! | mean | that sum over the count | one rounding |
//! | variance, stddev, CV | Welford | **exact** |
//! | skewness, kurtosis | Welford, 3rd and 4th moments | **exact** |
//! | geometric, harmonic mean | Σ log, Σ 1/x | **exact** |
//! | median, p25, p75, p90, p95, p99, IQR | relative-error sketch | within **ε**, default 1% |
//! | mode, top-k | Space-Saving | bounded, and it says when it is certain |
//! | distinct | HyperLogLog, 1 KB | ~1.6% standard error |
//!
//! # The two classes, which is the whole design
//!
//! Everything in the first group collapses into a handful of numbers regardless of record count, and merges exactly.
//! Nothing in the second can: an exact median, mode or distinct count needs state proportional to the data. That is
//! information-theoretic, not a shortcoming to engineer away.
//!
//! So the second group is approximate **with a stated, provable error**, and every one of them reports enough for a
//! caller to know how much to trust it — `percentile_error`, `mode_is_certain`, `distinct_error`. A number that might
//! be wrong and does not say so is worse than no number.
//!
//! # What it costs
//!
//! State is a few hundred bytes plus the sketch's live buckets plus 1 KB of registers — flat in the record count. One
//! partial per query thread, so four per node, not one per partition slice.
//!
//! # Reading the bin
//!
//! Integer and double bins both, and the result reports how many of each it saw — so a bin that is an integer in most
//! records and a double in a few is *visible* rather than surprising. Records where the bin is absent, non-numeric or
//! `NaN` are counted and skipped, never silently ignored.
//!
//! The **type is carried all the way to the sum**: an integer bin is summed as an `i64` into an `i128` and never
//! widened, because widening to `f64` loses bits above 2⁵³ before any arithmetic happens. Every other statistic here
//! is a float by nature and takes the widening. See [`sum`].

#![allow(clippy::needless_range_loop)]

use core::ffi::CStr;

use aerospike_udf_sdk_native::{
    module, partial, udfs, Aggregation, Args, Ctx, Emit, Error, Fold, Kind, Record, Result, Value,
};

pub mod heavy;
pub mod hll;
pub mod moments;
pub mod sketch;
pub mod sum;

#[cfg(test)]
mod tests;

use heavy::Heavy;
use hll::Hll;
use moments::Moments;
use sketch::Sketch;
use sum::ExactSum;

partial! {
    /// The accumulators for the multiplicative means, kept apart from [`Moments`] because their domains differ.
    ///
    /// A geometric mean is undefined for a non-positive value and a harmonic mean for zero, so each keeps its own
    /// count of the values it could actually use — and `finalize` reports the answer as absent rather than inventing
    /// one when the data did not support it.
    pub struct Means {
        /// Σ ln(x) over x > 0, which is how a geometric mean stays representable: the product itself overflows
        /// `f64` after a few hundred values.
        ln_sum: f64,
        ln_n: i64,
        recip_sum: f64,
        recip_n: i64,
        /// Values the geometric mean had to skip, reported so its absence is explained.
        non_positive: i64,
    }
}

impl Means {
    /// Public so the benchmark can attribute what it costs. `ln` and a division are not free, and a component the
    /// attribution table cannot reach is a component nobody notices getting slower.
    pub fn push(&mut self, x: f64) {
        if x > 0.0 {
            self.ln_sum += x.ln();
            self.ln_n += 1;
        } else {
            self.non_positive += 1;
        }

        if x != 0.0 {
            self.recip_sum += 1.0 / x;
            self.recip_n += 1;
        }
    }

    fn merge(&mut self, other: &Self) {
        self.ln_sum += other.ln_sum;
        self.ln_n += other.ln_n;
        self.recip_sum += other.recip_sum;
        self.recip_n += other.recip_n;
        self.non_positive += other.non_positive;
    }

    fn geometric(&self) -> Option<f64> {
        (self.ln_n > 0).then(|| (self.ln_sum / self.ln_n as f64).exp())
    }

    fn harmonic(&self) -> Option<f64> {
        if self.recip_n == 0 || self.recip_sum == 0.0 {
            return None;
        }

        Some(self.recip_n as f64 / self.recip_sum)
    }
}

partial! {
    /// Everything, for one bin.
    ///
    /// Composed of `partial!` structs rather than a flat field list: each piece serialises itself, so the wire form
    /// is written once per component and this struct only says which components exist.
    pub struct Summary {
        bin: String,
        /// The sum, kept apart from [`Moments`] and in two pieces — see [`sum`] for why.
        sums: ExactSum,
        moments: Moments,
        means: Means,
        sketch: Sketch,
        heavy: Heavy,
        distinct: Hll,
        skipped_absent: i64,
        skipped_type: i64,
        skipped_nan: i64,
        n_int: i64,
        n_double: i64,
    }
}

impl Aggregation for Summary {
    const NAME: &'static CStr = c"summary";

    /// `summary(bin, [epsilon], [mode_capacity])`.
    ///
    /// Both optional arguments have defaults that are right for most data, so the common call names a bin and nothing
    /// else. They are resolved **once per query thread**, here, and never looked at again per record.
    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("the first argument must be the bin name"))?
            .to_string();

        let eps = args.get(1).as_f64().unwrap_or(sketch::DEFAULT_EPSILON);

        if !(0.0..1.0).contains(&eps) || eps == 0.0 {
            return Err(Error::msg(
                "epsilon must be greater than 0 and less than 1 - it is a relative error, so 0.01 is 1%",
            ));
        }

        let cap = args.get(2).as_i64().unwrap_or(heavy::DEFAULT_CAPACITY);

        if cap <= 0 {
            return Err(Error::msg("the mode capacity must be positive"));
        }

        self.sketch = Sketch::with_epsilon(eps);
        self.heavy = Heavy::with_capacity(cap);
        self.distinct = Hll::new();

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        // `get_f64` widens an integer bin, which is what makes one code path serve both - but it cannot say *which*
        // it was, and a bin that changes type between records is worth being able to see. So the type is asked for
        // first, and it is one host call either way.
        let raw = rec.get(&self.bin)?;

        // `stored` keeps the integer as the `i64` it actually was, because the sum is accumulated from that rather
        // than from its `f64` widening - past 2^53 the two are different numbers. `value` is the widening, which is
        // what every other statistic here needs and cannot avoid.
        let mut stored: Option<i64> = None;

        let value = match raw.kind() {
            Kind::Int => {
                let i = raw.as_i64().unwrap_or(0);

                stored = Some(i);
                self.n_int += 1;

                i as f64
            }
            Kind::Double => {
                self.n_double += 1;
                raw.as_f64().unwrap_or(0.0)
            }
            Kind::Absent | Kind::Nil => {
                self.skipped_absent += 1;
                return Ok(Fold::Continue);
            }
            _ => {
                self.skipped_type += 1;
                return Ok(Fold::Continue);
            }
        };

        // NaN would poison the mean, make min/max meaningless and can never be a frequency-table key, so it is
        // counted out here rather than defended against in four places.
        if !value.is_finite() {
            self.skipped_nan += 1;
            return Ok(Fold::Continue);
        }

        // **The two sums.** Integers to the integer accumulator, doubles to the compensated float one, and they meet
        // only in `finalize`. See [`sum`].
        match stored {
            Some(i) => self.sums.push_int(i),
            None => self.sums.push_f64(value),
        }

        self.moments.push(value);
        self.means.push(value);
        self.sketch.push(value);
        self.heavy.push(value);
        self.distinct.push(value);

        // Fixed state, so it never fills: this is a reducing aggregation and one partial per thread is the whole
        // output. `Fold::Full` exists for the collecting shape, which this is not.
        Ok(Fold::Continue)
    }

    fn merge(&mut self, other: &Self) {
        if self.bin.is_empty() {
            self.bin = other.bin.clone();
        }

        self.sums.merge(&other.sums);
        self.moments.merge(&other.moments);
        self.means.merge(&other.means);
        self.sketch.merge(&other.sketch);
        self.heavy.merge(&other.heavy);
        self.distinct.merge(&other.distinct);

        self.skipped_absent += other.skipped_absent;
        self.skipped_type += other.skipped_type;
        self.skipped_nan += other.skipped_nan;
        self.n_int += other.n_int;
        self.n_double += other.n_double;
    }

    /// The answer, as a map.
    ///
    /// **This is not what gets merged** — `to_partial`, which `partial!` wrote, is. That split is what lets a mean, a
    /// median and a standard deviation appear here at all: none of them add, so none of them could have gone on the
    /// wire.
    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let map = ctx.map(32)?;
        let n = self.moments.count();

        map.put_str_str("bin", &self.bin)?;
        map.put_str_int("count", n)?;

        // Read counts, so a mixed-type or partly-empty bin is visible in the answer.
        map.put_str_int("read_int", self.n_int)?;
        map.put_str_int("read_double", self.n_double)?;
        map.put_str_int("skipped_absent", self.skipped_absent)?;
        map.put_str_int("skipped_wrong_type", self.skipped_type)?;
        map.put_str_int("skipped_not_finite", self.skipped_nan)?;

        if n == 0 {
            // Nothing to describe. Reporting the skip counts and stopping beats emitting a map full of zeros that
            // reads like a distribution centred on nothing.
            return Ok(map.as_value());
        }

        put_opt(&map, "sum", self.sums.total())?;

        // The total as an exact integer, when there is one: the bin held only integers, so nothing was ever widened
        // and the `i128` has every bit. `sum` above is that same number rounded into a double, which is lossy past
        // 2^53 — and being lossy past 2^53 is the entire reason this key exists.
        if let Some(exact) = self.sums.exact() {
            match i64::try_from(exact) {
                Ok(v) => map.put_str_int("sum_exact", v)?,
                // Beyond `i64` there is no Aerospike integer to hold it, and a double would defeat the purpose. A
                // decimal string is the only lossless shape left, and it is better than dropping the answer.
                Err(_) => map.put_str_str("sum_exact_str", &exact.to_string())?,
            }
        }

        // Neither is reachable from a dataset - both mean a partial arrived malformed. Reported rather than swallowed,
        // for the same reason as `percentile_collapsed`.
        if self.sums.saturated() {
            map.put_str_int("sum_saturated", 1)?;
        }

        if self.sums.refused_a_partial() {
            map.put_str_int("sum_refused_a_partial", 1)?;
        }

        put_opt(&map, "min", self.moments.min())?;
        put_opt(&map, "max", self.moments.max())?;
        put_opt(&map, "range", self.moments.range())?;

        // From the total over the count, not from Welford's running mean, which drifts by O(n·ε). `cv` below still
        // divides by Welford's, so that the variance and the mean it was computed about stay each other's - the two
        // differ in the last ulp or two.
        put_opt(&map, "mean", self.sums.mean())?;
        put_opt(&map, "variance", self.moments.variance())?;
        put_opt(&map, "stddev", self.moments.stddev())?;
        put_opt(&map, "cv", self.moments.cv())?;
        put_opt(&map, "skewness", self.moments.skewness())?;
        put_opt(&map, "kurtosis_excess", self.moments.kurtosis())?;
        put_opt(&map, "geometric_mean", self.means.geometric())?;
        put_opt(&map, "harmonic_mean", self.means.harmonic())?;

        if self.means.non_positive > 0 {
            map.put_str_int("geometric_mean_skipped", self.means.non_positive)?;
        }

        // --- order statistics, approximate and labelled as such ---

        put_opt(&map, "p25", self.sketch.quantile(0.25))?;
        put_opt(&map, "median", self.sketch.quantile(0.5))?;
        put_opt(&map, "p75", self.sketch.quantile(0.75))?;
        put_opt(&map, "p90", self.sketch.quantile(0.90))?;
        put_opt(&map, "p95", self.sketch.quantile(0.95))?;
        put_opt(&map, "p99", self.sketch.quantile(0.99))?;

        if let (Some(p75), Some(p25)) = (self.sketch.quantile(0.75), self.sketch.quantile(0.25)) {
            map.put_str_f64("iqr", p75 - p25)?;
        }

        map.put_str_f64("percentile_error", self.sketch.epsilon())?;
        map.put_str_int("sketch_buckets", self.sketch.buckets() as i64)?;

        // Only present when they have something to say, so a clean result stays readable.
        if self.sketch.collapsed() > 0 {
            map.put_str_int("percentile_collapsed", self.sketch.collapsed())?;
        }

        if self.sketch.mismatched() {
            map.put_str_int("percentile_epsilon_mismatch", 1)?;
        }

        // --- mode ---

        if let Some((value, count, err)) = self.heavy.mode() {
            map.put_str_f64("mode", value)?;
            map.put_str_int("mode_count", count)?;

            if err > 0 {
                map.put_str_int("mode_count_error", err)?;
            }

            map.put_str_int("mode_is_certain", i64::from(self.heavy.mode_is_certain(n)))?;
        }

        map.put_str_int("distinct_values_tracked", self.heavy.distinct_seen())?;

        // --- distinct ---

        map.put_str_f64("distinct_estimate", self.distinct.estimate())?;
        map.put_str_f64("distinct_error", Hll::relative_error())?;

        Ok(map.as_value())
    }
}

/// Write a statistic, or leave the key out entirely when the data could not support it.
///
/// Absent rather than zero or `NaN`: the standard deviation of one record and a standard deviation of zero are
/// different facts, and a map that cannot tell them apart makes the caller guess.
fn put_opt(map: &aerospike_udf_sdk_native::Map<'_>, key: &str, value: Option<f64>) -> Result<()> {
    match value {
        Some(v) if v.is_finite() => map.put_str_f64(key, v),
        _ => Ok(()),
    }
}

/// The largest `k` `top_values` will honour.
///
/// A caller supplies `k`, and it used to reach `Heavy::with_capacity(k * 4)` and `ctx.list(k as u32)` unchecked — so
/// `i64::MAX` overflowed the multiplication (a panic in debug, a wrapped value in release) and truncated the list
/// capacity. Clamping is the whole fix; the bound just has to exceed any sensible request while keeping the table
/// bounded.
pub const MAX_TOP_K: usize = 1024;

/// Resolve the `k` argument: default 5, at least 1, never above [`MAX_TOP_K`].
pub fn clamp_k(k: Option<i64>) -> usize {
    match k {
        None => 5,
        Some(v) if v < 1 => 1,
        Some(v) => (v as u64).min(MAX_TOP_K as u64) as usize,
    }
}

udfs! {
    /// The top `k` most frequent values of a bin, for one record's worth of a list bin.
    ///
    /// A record function rather than an aggregation, so it answers about *this* record: pass a bin holding a list of
    /// numbers and get the modal values within it. Useful for the shape of data where each record already carries a
    /// sample.
    fn top_values<'a>(rec: &'a Record, bin: &str, k: Option<i64>) -> Result<Value<'a>> {
        // Clamped, so neither the capacity arithmetic below nor the list size can be driven out of range by an
        // argument. See `MAX_TOP_K`.
        let k = clamp_k(k);

        let list = rec
            .get(bin)?
            .as_list()
            .ok_or_else(|| Error::msg("that bin does not hold a list"))?;

        let n = list.len() as usize;
        let mut table = Heavy::with_capacity((k as i64).saturating_mul(4));
        let mut values = vec![0.0f64; n];

        // One bulk host call rather than n - the same reason the vector example reads a packed blob.
        let got = list.doubles_into(0, &mut values)?;

        for &v in &values[..got] {
            if v.is_finite() {
                table.push(v);
            }
        }

        let out = rec.ctx().list(k as u32)?;

        for (value, count, _) in table.top(k) {
            let row = rec.ctx().map(2)?;

            row.put_str_f64("value", value)?;
            row.put_str_int("count", count)?;
            out.push(&row.as_value())?;
        }

        Ok(out.as_value())
    }
}

//==========================================================
// Stage probes.
//
// **Each does strictly less than the next**, so the differences between their timings attribute the per-record cost to
// one component. Nothing here is a guess about what the aggregation is doing: it is the same machinery with one piece
// removed at a time.
//
// Separate aggregations rather than one with a mode argument, and each holding only the state it needs. A mode
// argument would put a branch on the per-record path and leave the unused accumulators in the partial, distorting
// both halves of what is being measured.
//

partial! {
    /// The query and the UDF dispatch, and nothing else. Does not even read the bin.
    pub struct ProbeNothing {
        bin: String,
        n: i64,
    }

    /// Reads the bin and classifies it. The step up from `ProbeNothing` is the bin read.
    pub struct ProbeRead {
        bin: String,
        n: i64,
    }

    /// Plus the moments — count, sum, min, max, variance, skewness, kurtosis.
    pub struct ProbeMoments {
        bin: String,
        moments: Moments,
    }

    /// Plus the quantile sketch.
    pub struct ProbeSketch {
        bin: String,
        moments: Moments,
        sketch: Sketch,
    }

    /// Plus Space-Saving. The step from here to `summary` is HyperLogLog.
    pub struct ProbeHeavy {
        bin: String,
        moments: Moments,
        sketch: Sketch,
        heavy: Heavy,
    }
}

/// Read the configured bin as a finite `f64`, or `None`.
///
/// Shared by every probe so that the thing being measured is the *addition* at each step and not an incidental
/// difference in how the value was obtained.
fn numeric(rec: &Record, bin: &str) -> Result<Option<f64>> {
    let raw = rec.get(bin)?;

    let v = match raw.kind() {
        Kind::Int => raw.as_i64().unwrap_or(0) as f64,
        Kind::Double => raw.as_f64().unwrap_or(0.0),
        _ => return Ok(None),
    };

    Ok(v.is_finite().then_some(v))
}

macro_rules! probe_boilerplate {
    ($t:ty, $name:literal) => {
        impl Aggregation for $t {
            const NAME: &'static CStr = $name;

            fn configure(&mut self, args: Args<'_>) -> Result<()> {
                self.bin = args
                    .get(0)
                    .as_str()
                    .ok_or_else(|| Error::msg("the first argument must be the bin name"))?
                    .to_string();

                Ok(())
            }

            fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
                self.take(rec)
            }

            fn merge(&mut self, other: &Self) {
                self.absorb(other);
            }

            fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
                let map = ctx.map(1)?;

                map.put_str_int("count", self.total())?;

                Ok(map.as_value())
            }
        }
    };
}

impl Aggregation for ProbeNothing {
    const NAME: &'static CStr = c"probe_nothing";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.bin = args.get(0).as_str().unwrap_or("v").to_string();

        Ok(())
    }

    fn accumulate(&mut self, _rec: &Record, _out: &Emit) -> Result<Fold> {
        self.n += 1;

        Ok(Fold::Continue)
    }

    fn merge(&mut self, other: &Self) {
        self.n += other.n;
    }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let map = ctx.map(1)?;

        map.put_str_int("count", self.n)?;

        Ok(map.as_value())
    }
}

impl ProbeRead {
    fn absorb(&mut self, other: &Self) {
        self.n += other.n;
    }

    fn total(&self) -> i64 {
        self.n
    }

    fn take(&mut self, rec: &Record) -> Result<Fold> {
        if numeric(rec, &self.bin)?.is_some() {
            self.n += 1;
        }

        Ok(Fold::Continue)
    }
}

probe_boilerplate!(ProbeRead, c"probe_read");

impl ProbeMoments {
    fn absorb(&mut self, other: &Self) {
        self.moments.merge(&other.moments);
    }

    fn total(&self) -> i64 {
        self.moments.count()
    }

    fn take(&mut self, rec: &Record) -> Result<Fold> {
        if let Some(v) = numeric(rec, &self.bin)? {
            self.moments.push(v);
        }

        Ok(Fold::Continue)
    }
}

probe_boilerplate!(ProbeMoments, c"probe_moments");

impl ProbeSketch {
    fn absorb(&mut self, other: &Self) {
        self.moments.merge(&other.moments);
        self.sketch.merge(&other.sketch);
    }

    fn total(&self) -> i64 {
        self.moments.count()
    }

    fn take(&mut self, rec: &Record) -> Result<Fold> {
        if let Some(v) = numeric(rec, &self.bin)? {
            self.moments.push(v);
            self.sketch.push(v);
        }

        Ok(Fold::Continue)
    }
}

probe_boilerplate!(ProbeSketch, c"probe_sketch");

impl ProbeHeavy {
    fn absorb(&mut self, other: &Self) {
        self.moments.merge(&other.moments);
        self.sketch.merge(&other.sketch);
        self.heavy.merge(&other.heavy);
    }

    fn total(&self) -> i64 {
        self.moments.count()
    }

    fn take(&mut self, rec: &Record) -> Result<Fold> {
        if let Some(v) = numeric(rec, &self.bin)? {
            self.moments.push(v);
            self.sketch.push(v);
            self.heavy.push(v);
        }

        Ok(Fold::Continue)
    }
}

probe_boilerplate!(ProbeHeavy, c"probe_heavy");

module! {
    name: "summary",
    version: "1.0.0",
    functions: [top_values],
    aggregations: [
        Summary, ProbeNothing, ProbeRead, ProbeMoments, ProbeSketch, ProbeHeavy
    ],
}
