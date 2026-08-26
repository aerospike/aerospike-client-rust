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

use std::sync::Mutex;

#[cfg(feature = "serialization")]
use serde::ser::SerializeStruct;
#[cfg(feature = "serialization")]
use serde::{Serialize, Serializer};

/// Bucket layout of a histogram (`Linear` = 0, `Logarithmic` = 1).
///
/// Crate-internal: latency histograms are always the [`Logarithmic`] range
/// layout (the same shape as `asadm` / `asloglatency`). `Linear` is retained
/// for the bucketing code path only and is not reachable from the policy,
/// the config file, or the public API.
///
/// [`Logarithmic`]: HistogramType::Logarithmic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum HistogramType {
    /// Buckets are `<base <base*2 <base*3 ... >=base*(columns-1)`.
    Linear,
    /// Logarithmic range layout: `<= 1`, `> 1`, then each subsequent boundary
    /// multiplied by `base` (`2^latency_shift`). Matches the Java client /
    /// `asadm` histogram, not `floor(log_base(v))`.
    #[default]
    Logarithmic,
}

/// Index of the range-layout bucket that holds `v`.
///
/// `base` is the per-step multiplier (`2^latency_shift`). The first two
/// buckets are always `<= 1` and `> 1`; later limits grow by `base` each
/// step. Overflow lands in the last column.
#[must_use]
pub(crate) fn range_bucket_index(v: u64, base: u64, columns: usize) -> usize {
    let columns = columns.max(1);
    let base = base.max(1);
    let last = columns - 1;
    let mut limit = 1u64;
    for i in 0..last {
        if v <= limit {
            return i;
        }
        limit = limit.saturating_mul(base);
    }
    last
}

/// Inner, non-synchronized histogram state.
#[derive(Debug, Clone)]
struct HistogramInner {
    htype: HistogramType,
    base: u64,
    buckets: Vec<u64>,
    min: u64,
    max: u64,
    sum: f64,
    count: u64,
}

impl HistogramInner {
    fn new(htype: HistogramType, base: u64, columns: usize) -> Self {
        // Guard against a zero-column or zero-base configuration which would
        // make bucket indexing/arithmetic undefined.
        let columns = columns.max(1);
        let base = base.max(1);
        HistogramInner {
            htype,
            base,
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

    fn reshape(&mut self, htype: HistogramType, base: u64, columns: usize) {
        let columns = columns.max(1);
        let base = base.max(1);
        if self.htype == htype && self.base == base && self.buckets.len() == columns {
            return;
        }
        self.htype = htype;
        self.base = base;
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

        let idx = match self.htype {
            HistogramType::Linear => {
                // Integer division == floor for non-negative operands.
                let slot = if v == 0 { 0 } else { (v / self.base) as usize };
                slot.min(self.buckets.len() - 1)
            }
            HistogramType::Logarithmic => range_bucket_index(v, self.base, self.buckets.len()),
        };
        self.buckets[idx] += 1;
    }

    fn merge(&mut self, other: &HistogramInner) {
        // Mismatched histograms are silently skipped.
        if self.base != other.base
            || self.htype != other.htype
            || self.buckets.len() != other.buckets.len()
        {
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
    /// Creates a new, empty histogram with the given layout.
    #[must_use]
    pub(crate) fn new(htype: HistogramType, base: u64, columns: usize) -> Self {
        SyncHistogram {
            inner: Mutex::new(HistogramInner::new(htype, base, columns)),
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
    pub fn reshape(&self, htype: HistogramType, base: u64, columns: usize) {
        self.inner.lock().unwrap().reshape(htype, base, columns);
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
        // Only the data fields are serialized; the layout fields (`htype`,
        // `base`) are intentionally omitted.
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

    #[test]
    fn range_layout_default_shift1_columns7() {
        // Spec default: <=1, >1, >2, >4, >8, >16, >32 (multiplier 2).
        let h = SyncHistogram::new(HistogramType::Logarithmic, 2, 7);
        for v in [0u64, 1] {
            h.add(v);
        }
        h.add(2);
        for v in [3u64, 4] {
            h.add(v);
        }
        for v in [5u64, 8] {
            h.add(v);
        }
        for v in [9u64, 16] {
            h.add(v);
        }
        for v in [17u64, 32] {
            h.add(v);
        }
        for v in [33u64, 1_000] {
            h.add(v);
        }
        assert_eq!(h.buckets(), vec![2, 1, 2, 2, 2, 2, 2]);
        assert_eq!(range_bucket_index(1, 2, 7), 0);
        assert_eq!(range_bucket_index(2, 2, 7), 1);
        assert_eq!(range_bucket_index(3, 2, 7), 2);
        assert_eq!(range_bucket_index(32, 2, 7), 5);
        assert_eq!(range_bucket_index(33, 2, 7), 6);
    }

    #[test]
    fn range_layout_shift3_columns5() {
        // Spec: <=1, >1 (<=8), >8 (<=64), >64 (<=512), >512. multiplier = 8.
        let h = SyncHistogram::new(HistogramType::Logarithmic, 8, 5);
        h.add(1); // 0
        h.add(7); // 1
        h.add(8); // 1
        h.add(9); // 2
        h.add(64); // 2
        h.add(65); // 3
        h.add(512); // 3
        h.add(513); // 4
        assert_eq!(h.buckets(), vec![1, 2, 2, 2, 1]);
        assert_eq!(range_bucket_index(1, 8, 5), 0);
        assert_eq!(range_bucket_index(8, 8, 5), 1);
        assert_eq!(range_bucket_index(64, 8, 5), 2);
        assert_eq!(range_bucket_index(512, 8, 5), 3);
        assert_eq!(range_bucket_index(513, 8, 5), 4);
    }

    #[test]
    fn linear_bucketing() {
        // base=15, columns=5 => <15 <30 <45 <60 >=60
        let h = SyncHistogram::new(HistogramType::Linear, 15, 5);
        for v in [0u64, 14, 15, 29, 30, 44, 45, 59, 60, 200] {
            h.add(v);
        }
        assert_eq!(h.buckets(), vec![2, 2, 2, 2, 2]);
        assert_eq!(h.count(), 10);
        assert_eq!(h.min(), 0);
        assert_eq!(h.max(), 200);
    }

    #[test]
    fn merge_combines_counts() {
        let a = SyncHistogram::new(HistogramType::Logarithmic, 2, 4);
        let b = SyncHistogram::new(HistogramType::Logarithmic, 2, 4);
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
        let h = SyncHistogram::new(HistogramType::Logarithmic, 2, 4);
        h.add(10);
        h.add(20);
        let snap = h.clone_and_reset();
        assert_eq!(snap.count(), 2);
        assert_eq!(h.count(), 0);
        assert_eq!(h.buckets().iter().sum::<u64>(), 0);
    }

    #[test]
    fn reshape_resets_only_on_change() {
        let h = SyncHistogram::new(HistogramType::Logarithmic, 2, 4);
        h.add(10);
        // identical layout -> no reset
        h.reshape(HistogramType::Logarithmic, 2, 4);
        assert_eq!(h.count(), 1);
        // different layout -> reset
        h.reshape(HistogramType::Linear, 5, 6);
        assert_eq!(h.count(), 0);
        assert_eq!(h.buckets().len(), 6);
    }

    #[test]
    fn zero_value_and_average() {
        let h = SyncHistogram::new(HistogramType::Logarithmic, 2, 4);
        h.add(0);
        h.add(0);
        assert_eq!(h.count(), 2);
        assert_eq!(h.min(), 0);
        assert_eq!(h.max(), 0);
        assert_eq!(h.buckets()[0], 2); // zero values land in bucket 0
        assert_eq!(h.average(), 0.0);

        let h2 = SyncHistogram::new(HistogramType::Linear, 10, 4);
        h2.add(10);
        h2.add(30);
        assert_eq!(h2.average(), 20.0);
        // empty histogram average is 0, not NaN
        assert_eq!(
            SyncHistogram::new(HistogramType::Linear, 10, 4).average(),
            0.0
        );
    }

    #[test]
    fn merge_rejects_mismatched_shape() {
        let a = SyncHistogram::new(HistogramType::Logarithmic, 2, 4);
        let b = SyncHistogram::new(HistogramType::Linear, 2, 4); // different type
        a.add(5);
        b.add(5);
        a.merge(&b); // silently ignored — counts unchanged
        assert_eq!(a.count(), 1);
    }

    #[cfg(feature = "serialization")]
    #[test]
    fn serializes_only_data_fields() {
        let h = SyncHistogram::new(HistogramType::Logarithmic, 2, 4);
        h.add(3);
        let v = serde_json::to_value(&h).unwrap();
        // Layout fields (htype/base) are intentionally omitted.
        assert!(v.get("buckets").unwrap().is_array());
        assert_eq!(v["count"], 1);
        assert_eq!(v["min"], 3);
        assert_eq!(v["max"], 3);
        assert_eq!(v["sum"], 3.0);
        assert!(v.get("htype").is_none());
        assert!(v.get("base").is_none());
    }
}
