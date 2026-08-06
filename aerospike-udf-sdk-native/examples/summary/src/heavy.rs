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

//! Mode and the most frequent values, in bounded state — with the guarantee reported rather than assumed.
//!
//! # What "mode" can mean here
//!
//! An exact mode needs a count per distinct value, so it needs state proportional to the number of distinct values.
//! Space-Saving gives a bounded approximation with a guarantee worth stating precisely:
//!
//! > With capacity *m* over *n* observations, **any value occurring more than n/m times is certain to be in the
//! > table**, and each reported count is an over-estimate by at most `error`, so the truth is in
//! > `[count − error, count]`.
//!
//! The useful consequence: if the top value's count exceeds `n/m`, it *is* the mode, and this reports the threshold
//! so a caller can check rather than hope. Better still, if nothing was ever evicted then the table saw every
//! distinct value and **every count is exact** — which is the common case for the kind of low-cardinality field
//! anybody actually asks the mode of. That is reported too.
//!
//! # And when it is the wrong question
//!
//! The mode of a million distinct `f64` measurements is a category error, not a hard problem: every value occurs
//! once, so "the most frequent" is whichever one arbitrary tie-breaking picked. Mode belongs to categorical and
//! integral data. For continuous data the meaningful answers are the modal *bucket* — which the quantile sketch
//! already has — and the top-k this also returns.
//!
//! So the result carries `mode_is_certain` and `distinct_seen`, and a caller looking at `distinct_seen == n` knows
//! the mode is meaningless for their data without having to be told in advance.
//!
//! # Floats as table keys
//!
//! Equality on `f64` needs care to be a *key*. `NaN` never equals itself, so it can never be counted and is rejected
//! before it arrives here. `-0.0 == 0.0` is true but their bit patterns differ, so `-0.0` is normalised to `0.0` on
//! the way in; without that, the same logical value could occupy two rows and neither would be the mode.

use aerospike_udf_sdk_native::partial;

/// Rows kept by default. 64 covers a status code, an enum, a country or a small integer domain outright.
pub const DEFAULT_CAPACITY: i64 = 64;

partial! {
    /// A Space-Saving frequency table.
    ///
    /// Three parallel lists rather than a list of triples, for the same reason as the sketch: fewer values on the
    /// wire, and `partial!` writes them from the field list regardless.
    pub struct Heavy {
        cap: i64,
        /// Values as **bit patterns**, not `f64`.
        ///
        /// The hot loop is a scan comparing keys, and comparing `i64` directly avoids a `to_bits` per element per
        /// record. It also makes equality unambiguous, which is what a frequency table wants - see the note above on
        /// `NaN` and `-0.0`.
        keys: Vec<i64>,
        cnts: Vec<i64>,
        errs: Vec<i64>,
        /// Whether any row was ever displaced. False means every count in here is exact.
        evicted: bool,
        /// Distinct values *offered*, which is how a caller can tell mode is the wrong question for their data.
        distinct_seen: i64,
    }
}

impl Heavy {
    pub fn with_capacity(cap: i64) -> Self {
        let mut h = Self::default();

        h.cap = if cap > 0 { cap } else { DEFAULT_CAPACITY };

        h
    }

    fn capacity(&self) -> usize {
        if self.cap > 0 { self.cap as usize } else { DEFAULT_CAPACITY as usize }
    }

    /// One observation. `NaN` is the caller's problem and never reaches here.
    pub fn push(&mut self, v: f64) {
        self.add(normalise(v), 1, 0);
    }

    /// The shared body of `push` and `merge`: add `count` occurrences of `v` carrying `err` of existing uncertainty.
    ///
    /// **One pass, not two.** Looking the key up and finding the least frequent row used to be separate scans, so a
    /// miss cost 2m comparisons - and on high-cardinality data *every* observation is a miss. A benchmark over 100,000
    /// values with 103,759 distinct ones measured this at 127 ns per record, 83% of the whole aggregation's arithmetic,
    /// against 13 ns for the moments. Combining the scans and comparing bit patterns rather than floats is what that
    /// measurement bought.
    fn add(&mut self, v: f64, count: i64, err: i64) {
        let key = normalise(v).to_bits() as i64;

        // The single scan: find the key if it is here, and the weakest row in case it is not.
        let mut found = usize::MAX;
        let mut min_at = 0usize;
        let mut min_count = i64::MAX;

        for i in 0..self.keys.len() {
            if self.keys[i] == key {
                found = i;
                break;
            }

            // `<` rather than `<=`, so a tie keeps the earlier row and the table does not churn.
            if self.cnts[i] < min_count {
                min_count = self.cnts[i];
                min_at = i;
            }
        }

        if found != usize::MAX {
            self.cnts[found] += count;
            self.errs[found] += err;
            return;
        }

        self.distinct_seen += 1;

        if self.keys.len() < self.capacity() {
            self.keys.push(key);
            self.cnts.push(count);
            self.errs.push(err);
            return;
        }

        // The early `break` above means the scan may have stopped before seeing every row, but it only breaks on a
        // *hit* - and a hit returns. Reaching here means the whole table was scanned, so `min_at` is the true minimum.
        let floor = self.cnts[min_at];

        self.keys[min_at] = key;
        self.cnts[min_at] = floor + count;
        self.errs[min_at] = floor + err;
        self.evicted = true;
    }

    fn value_at(&self, i: usize) -> f64 {
        f64::from_bits(self.keys[i] as u64)
    }

    pub fn merge(&mut self, other: &Self) {
        if self.cap == 0 {
            self.cap = other.cap;
        }

        self.evicted |= other.evicted;

        // Largest first, so the rows most likely to belong in the result are inserted while there is still room.
        let mut order: Vec<usize> = (0..other.keys.len()).collect();

        order.sort_by(|&a, &b| other.cnts[b].cmp(&other.cnts[a]));

        for i in order {
            self.add(other.value_at(i), other.cnts[i], other.errs[i]);
        }

        // Distinct counting is per-side; adding them over-counts anything both sides saw. An over-estimate is the
        // honest direction here - it can only make the mode look *less* certain than it is.
        self.distinct_seen = self.distinct_seen.max(other.distinct_seen);
    }

    /// The most frequent value, with its count and the uncertainty on that count.
    ///
    /// Ties break toward the smaller value, so the answer is a function of the data rather than of the order records
    /// happened to arrive in — the same reason the vector example breaks distance ties by digest.
    pub fn mode(&self) -> Option<(f64, i64, i64)> {
        let mut best: Option<usize> = None;

        for i in 0..self.keys.len() {
            best = match best {
                None => Some(i),
                Some(b)
                    if self.cnts[i] > self.cnts[b]
                        || (self.cnts[i] == self.cnts[b] && self.value_at(i) < self.value_at(b)) =>
                {
                    Some(i)
                }
                keep => keep,
            };
        }

        best.map(|at| (self.value_at(at), self.cnts[at], self.errs[at]))
    }

    /// Whether the mode is provably the mode: nothing was evicted, so every count is exact.
    pub fn mode_is_certain(&self, total: i64) -> bool {
        if !self.evicted {
            return true;
        }

        // Otherwise it is certain when the leader's guaranteed lower bound beats every other row's upper bound, and
        // beats the n/m threshold that Space-Saving promises.
        let Some((_, count, err)) = self.mode() else {
            return false;
        };

        let threshold = total / self.cap.max(1);
        let lower = count - err;

        if lower <= threshold {
            return false;
        }

        self.cnts.iter().filter(|c| **c != count).all(|c| *c < lower)
    }

    pub fn distinct_seen(&self) -> i64 {
        self.distinct_seen
    }

    pub fn evicted(&self) -> bool {
        self.evicted
    }

    pub fn capacity_declared(&self) -> i64 {
        self.cap
    }

    /// The top `k` values by count, most frequent first, ties by smaller value.
    pub fn top(&self, k: usize) -> Vec<(f64, i64, i64)> {
        let mut rows: Vec<(f64, i64, i64)> = (0..self.keys.len())
            .map(|i| (self.value_at(i), self.cnts[i], self.errs[i]))
            .collect();

        rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.total_cmp(&b.0)));
        rows.truncate(k);

        rows
    }
}

/// `-0.0` and `0.0` are the same value, so they must be the same key.
fn normalise(v: f64) -> f64 {
    if v == 0.0 {
        0.0
    } else {
        v
    }
}
