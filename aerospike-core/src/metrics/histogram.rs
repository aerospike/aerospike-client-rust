// Copyright 2014-2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Latency/size histograms used by the metrics subsystem.
//!
//! All values are `u64`. A [`SyncHistogram`] is a thread-safe wrapper around the
//! bucketed data.
//!
//! # Bucket layout
//!
//! Every histogram uses the Aerospike client **range layout** (the one
//! `asadm`, `asloglatency` and the prior Java client's `latencyColumns` /
//! `latencyShift` share). With `columns` buckets and a `shift` of *s*
//! (multiplier `m = 2^s`):
//!
//! | bucket        | values recorded                       |
//! | ------------- | ------------------------------------- |
//! | 0             | `v <= 1`                              |
//! | 1             | `1 < v <= m`                          |
//! | *i*           | `m^(i-1) < v <= m^i`                  |
//! | `columns - 1` | `v > m^(columns-2)` (overflow bucket) |
//!
//! Ranges are **upper-closed**: a value equal to a boundary lands in the
//! lower bucket. With the default `shift = 1` and 7 columns that is
//! `<=1, >1, >2, >4, >8, >16, >32`, i.e. bucket `ceil(log2 v)`. A larger shift
//! skips powers of two (`shift = 3`: `<=1, >1, >8, >64, ...`).

use std::sync::Mutex;

#[cfg(feature = "serialization")]
use serde::ser::SerializeStruct;
#[cfg(feature = "serialization")]
use serde::{Serialize, Serializer};

/// Largest usable shift: `2^63` is the biggest power-of-two boundary a `u64`
/// value can exceed, so anything above is clamped here.
const MAX_SHIFT: u32 = 63;

/// Inner, non-synchronized histogram state.
#[derive(Debug, Clone)]
struct HistogramInner {
    shift: u32,
    buckets: Vec<u64>,
    min: u64,
    max: u64,
    sum: f64,
    count: u64,
}

/// Normalizes a `(shift, columns)` pair so bucket arithmetic is always
/// defined: at least one column, a shift between 1 and [`MAX_SHIFT`].
fn normalize(shift: u32, columns: usize) -> (u32, usize) {
    (shift.clamp(1, MAX_SHIFT), columns.max(1))
}

/// Bucket index for value `v` in the range layout described in the module
/// docs. `shift` and `columns` must already be normalized.
fn bucket_index(v: u64, shift: u32, columns: usize) -> usize {
    if v <= 1 {
        return 0;
    }
    // `ceil(log2 v)` for v >= 2: the bit length of `v - 1`.
    let log2 = 64 - (v - 1).leading_zeros();
    // Each bucket spans `shift` powers of two; round up so a value equal to a
    // boundary stays in the lower bucket (upper-closed ranges).
    let bucket = log2.div_ceil(shift) as usize;
    bucket.min(columns - 1)
}

impl HistogramInner {
    fn new(shift: u32, columns: usize) -> Self {
        let (shift, columns) = normalize(shift, columns);
        HistogramInner {
            shift,
            buckets: vec![0; columns],
            min: 0,
            max: 0,
            sum: 0.0,
            count: 0,
        }
    }

    fn reset(&mut self) {
        for b in &mut self.buckets {
            *b = 0;
        }
        self.min = 0;
        self.max = 0;
        self.sum = 0.0;
        self.count = 0;
    }

    fn reshape(&mut self, shift: u32, columns: usize) {
        let (shift, columns) = normalize(shift, columns);
        if self.shift == shift && self.buckets.len() == columns {
            return;
        }
        self.shift = shift;
        self.buckets = vec![0; columns];
        self.min = 0;
        self.max = 0;
        self.sum = 0.0;
        self.count = 0;
    }

    fn add(&mut self, v: u64) {
        if self.count == 0 {
            self.max = v;
            self.min = v;
        } else if v > self.max {
            self.max = v;
        } else if v < self.min {
            self.min = v;
        }

        self.sum += v as f64;
        self.count += 1;

        let idx = bucket_index(v, self.shift, self.buckets.len());
        self.buckets[idx] += 1;
    }

    fn merge(&mut self, other: &HistogramInner) {
        // Mismatched histograms are silently skipped.
        if self.shift != other.shift || self.buckets.len() != other.buckets.len() {
            return;
        }

        if other.min < self.min || self.min == 0 {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        self.sum += other.sum;
        self.count += other.count;

        for (dst, src) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *dst += *src;
        }
    }
}

/// Thread-safe histogram of `u64` values.
#[derive(Debug)]
pub struct SyncHistogram {
    inner: Mutex<HistogramInner>,
}

impl SyncHistogram {
    /// Creates a new, empty histogram with `columns` buckets whose boundaries
    /// multiply by `2^shift` (see the module docs for the layout). A `shift`
    /// of 0 is treated as 1 and `columns` of 0 as 1.
    #[must_use]
    pub fn new(shift: u32, columns: usize) -> Self {
        SyncHistogram {
            inner: Mutex::new(HistogramInner::new(shift, columns)),
        }
    }

    /// Records a single value into the appropriate bucket.
    pub fn add(&self, v: u64) {
        self.inner.lock().unwrap().add(v);
    }

    /// Merges the contents of `other` into `self`. Histograms with a different
    /// shape are ignored.
    pub fn merge(&self, other: &SyncHistogram) {
        let snapshot = other.inner.lock().unwrap().clone();
        self.inner.lock().unwrap().merge(&snapshot);
    }

    /// Returns a deep copy of this histogram.
    #[must_use]
    pub fn clone_histogram(&self) -> SyncHistogram {
        SyncHistogram {
            inner: Mutex::new(self.inner.lock().unwrap().clone()),
        }
    }

    /// Returns a deep copy of this histogram and resets the original to empty.
    #[must_use]
    pub fn clone_and_reset(&self) -> SyncHistogram {
        let mut guard = self.inner.lock().unwrap();
        let copy = guard.clone();
        guard.reset();
        SyncHistogram {
            inner: Mutex::new(copy),
        }
    }

    /// Changes the histogram's layout, resetting its contents if the layout
    /// actually changed.
    pub fn reshape(&self, shift: u32, columns: usize) {
        self.inner.lock().unwrap().reshape(shift, columns);
    }

    /// Discards everything recorded so far, keeping the layout.
    ///
    /// Used when the recorded values stop being comparable with the ones
    /// already in the buckets — a [`LatencyUnit`](crate::metrics::LatencyUnit)
    /// change reshapes nothing but makes every existing sample meaningless.
    pub(crate) fn reset(&self) {
        self.inner.lock().unwrap().reset();
    }

    /// Number of values recorded.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.inner.lock().unwrap().count
    }

    /// Smallest value recorded (0 if empty).
    #[must_use]
    pub fn min(&self) -> u64 {
        self.inner.lock().unwrap().min
    }

    /// Largest value recorded (0 if empty).
    #[must_use]
    pub fn max(&self) -> u64 {
        self.inner.lock().unwrap().max
    }

    /// Sum of all recorded values.
    #[must_use]
    pub fn sum(&self) -> f64 {
        self.inner.lock().unwrap().sum
    }

    /// Snapshot of the bucket counts.
    #[must_use]
    pub fn buckets(&self) -> Vec<u64> {
        self.inner.lock().unwrap().buckets.clone()
    }

    /// The boundary spacing exponent this histogram was built with.
    #[must_use]
    pub fn shift(&self) -> u32 {
        self.inner.lock().unwrap().shift
    }

    /// Mean of all recorded values (0 if empty).
    #[must_use]
    pub fn average(&self) -> f64 {
        let g = self.inner.lock().unwrap();
        if g.count > 0 {
            g.sum / g.count as f64
        } else {
            0.0
        }
    }
}

impl Clone for SyncHistogram {
    fn clone(&self) -> Self {
        self.clone_histogram()
    }
}

#[cfg(feature = "serialization")]
impl Serialize for SyncHistogram {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Only the data fields are serialized; the layout field (`shift`) is
        // intentionally omitted — it is reported once per snapshot.
        let g = self.inner.lock().unwrap();
        let mut state = serializer.serialize_struct("histogram", 5)?;
        state.serialize_field("buckets", &g.buckets)?;
        state.serialize_field("min", &g.min)?;
        state.serialize_field("max", &g.max)?;
        state.serialize_field("sum", &g.sum)?;
        state.serialize_field("count", &g.count)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The spec's default layout (`metrics.md` §5.3): 7 columns, shift 1 —
    /// `<=1, >1, >2, >4, >8, >16, >32`, every range closed at the top.
    #[test]
    fn default_layout_is_upper_closed_powers_of_two() {
        let h = SyncHistogram::new(1, 7);
        let expect = [
            (0u64, 0usize),
            (1, 0),
            (2, 1),
            (3, 2),
            (4, 2),
            (5, 3),
            (8, 3),
            (9, 4),
            (16, 4),
            (17, 5),
            (32, 5),
            (33, 6),
            (1_000_000, 6),
            (u64::MAX, 6),
        ];
        for (v, bucket) in expect {
            assert_eq!(
                bucket_index(v, 1, 7),
                bucket,
                "value {v} should land in bucket {bucket}"
            );
            h.add(v);
        }
        assert_eq!(h.buckets(), vec![2, 1, 2, 2, 2, 2, 3]);
        assert_eq!(h.count(), 14);
        assert_eq!(h.min(), 0);
        assert_eq!(h.max(), u64::MAX);
    }

    /// `shift = 3` multiplies each boundary by 8: `<=1, >1, >8, >64, >512`.
    #[test]
    fn shift_skips_powers_of_two() {
        let h = SyncHistogram::new(3, 5);
        for v in [1u64, 2, 8, 9, 64, 65, 512, 513, 100_000] {
            h.add(v);
        }
        // 1 -> 0; 2,8 -> 1; 9,64 -> 2; 65,512 -> 3; 513,100000 -> 4
        assert_eq!(h.buckets(), vec![1, 2, 2, 2, 2]);
        assert_eq!(h.shift(), 3);
    }

    #[test]
    fn degenerate_shapes_are_normalized() {
        // shift 0 behaves as shift 1; zero columns become one.
        let h = SyncHistogram::new(0, 7);
        h.add(3);
        assert_eq!(h.buckets()[2], 1);
        assert_eq!(h.shift(), 1);

        let one = SyncHistogram::new(1, 0);
        one.add(1_000);
        assert_eq!(one.buckets(), vec![1]);

        // An absurd shift is clamped rather than overflowing the boundary math:
        // bucket 1 is then `(1, 2^63]` and anything above overflows.
        let wide = SyncHistogram::new(500, 3);
        wide.add(1 << 63);
        wide.add(u64::MAX);
        assert_eq!(wide.shift(), MAX_SHIFT);
        assert_eq!(wide.buckets(), vec![0, 1, 1]);
    }

    #[test]
    fn merge_combines_counts() {
        let a = SyncHistogram::new(1, 4);
        let b = SyncHistogram::new(1, 4);
        a.add(1);
        a.add(5);
        b.add(5);
        b.add(100);
        a.merge(&b);
        assert_eq!(a.count(), 4);
        assert_eq!(a.min(), 1);
        assert_eq!(a.max(), 100);
        let total: u64 = a.buckets().iter().sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn clone_and_reset_empties_original() {
        let h = SyncHistogram::new(1, 4);
        h.add(10);
        h.add(20);
        let snap = h.clone_and_reset();
        assert_eq!(snap.count(), 2);
        assert_eq!(h.count(), 0);
        assert_eq!(h.buckets().iter().sum::<u64>(), 0);
    }

    #[test]
    fn reshape_resets_only_on_change() {
        let h = SyncHistogram::new(1, 4);
        h.add(10);
        // identical layout -> no reset
        h.reshape(1, 4);
        assert_eq!(h.count(), 1);
        // different layout -> reset
        h.reshape(2, 6);
        assert_eq!(h.count(), 0);
        assert_eq!(h.buckets().len(), 6);
        assert_eq!(h.shift(), 2);
    }

    #[test]
    fn zero_value_and_average() {
        let h = SyncHistogram::new(1, 4);
        h.add(0);
        h.add(0);
        assert_eq!(h.count(), 2);
        assert_eq!(h.min(), 0);
        assert_eq!(h.max(), 0);
        assert_eq!(h.buckets()[0], 2); // zero values land in bucket 0
        assert_eq!(h.average(), 0.0);

        let h2 = SyncHistogram::new(1, 4);
        h2.add(10);
        h2.add(30);
        assert_eq!(h2.average(), 20.0);
        // empty histogram average is 0, not NaN
        assert_eq!(SyncHistogram::new(1, 4).average(), 0.0);
    }

    #[test]
    fn merge_rejects_mismatched_shape() {
        let a = SyncHistogram::new(1, 4);
        let b = SyncHistogram::new(2, 4); // different shift
        a.add(5);
        b.add(5);
        a.merge(&b); // silently ignored — counts unchanged
        assert_eq!(a.count(), 1);
        let c = SyncHistogram::new(1, 5); // different column count
        c.add(5);
        a.merge(&c);
        assert_eq!(a.count(), 1);
    }

    #[cfg(feature = "serialization")]
    #[test]
    fn serializes_only_data_fields() {
        let h = SyncHistogram::new(1, 4);
        h.add(3);
        let v = serde_json::to_value(&h).unwrap();
        // The layout field (shift) is intentionally omitted.
        assert!(v.get("buckets").unwrap().is_array());
        assert_eq!(v["count"], 1);
        assert_eq!(v["min"], 3);
        assert_eq!(v["max"], 3);
        assert_eq!(v["sum"], 3.0);
        assert!(v.get("shift").is_none());
        assert!(v.get("base").is_none());
    }
}
