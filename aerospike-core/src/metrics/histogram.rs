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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HistogramType {
    /// Buckets are `<base <base*2 <base*3 ... >=base*(columns-1)`.
    Linear,
    /// Buckets are `<base^1 <base^2 <base^3 ... >=base^(columns-1)`.
    #[default]
    Logarithmic,
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

        let mut slot: i64 = 0;
        if v > 0 {
            slot = match self.htype {
                // Integer division == floor for non-negative operands.
                HistogramType::Linear => (v / self.base) as i64,
                HistogramType::Logarithmic => {
                    ((v as f64).ln() / (self.base as f64).ln()).floor() as i64
                }
            };
        }

        let len = self.buckets.len();
        let idx = if slot < 0 {
            0
        } else if slot as usize >= len {
            len - 1
        } else {
            slot as usize
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
    pub fn new(htype: HistogramType, base: u64, columns: usize) -> Self {
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
    fn logarithmic_bucketing() {
        // base=8, columns=5 => <8 <64 <512 <4096 >=4096
        let h = SyncHistogram::new(HistogramType::Logarithmic, 8, 5);
        for v in [1u64, 7, 8, 63, 64, 511, 512, 4095, 4096, 100_000] {
            h.add(v);
        }
        // 1,7 -> bucket 0; 8,63 -> 1; 64,511 -> 2; 512,4095 -> 3; 4096,100000 -> 4
        assert_eq!(h.buckets(), vec![2, 2, 2, 2, 2]);
        assert_eq!(h.count(), 10);
        assert_eq!(h.min(), 1);
        assert_eq!(h.max(), 100_000);
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
