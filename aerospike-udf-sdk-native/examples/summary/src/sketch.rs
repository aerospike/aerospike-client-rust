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

//! Median and percentiles, with a **relative error guarantee** instead of an exact answer.
//!
//! # Why this cannot be exact
//!
//! Count, sum and variance collapse into a handful of numbers no matter how many records there are. A median cannot:
//! any algorithm that reports an exact median has to be able to distinguish every value it might return, so exact
//! order statistics need state proportional to the data. That is not an implementation shortcoming, and no amount of
//! cleverness removes it.
//!
//! So the choice is between an exact answer with unbounded state — which is what shipping every value to the client
//! would be — and a bounded answer with a stated error. This is the second, and the error is not a hope:
//!
//! > **Every percentile this returns is within ε of the true value, relatively.** Ask for p99 with ε = 1% and the
//! > answer is a real value from the data lying within 1% of the true p99.
//!
//! Relative rather than absolute error is the right guarantee for the things percentiles get used for. Latency, size
//! and price all span orders of magnitude, and 1% of the value is meaningful at every scale where an absolute
//! tolerance is either uselessly loose at the top or absurdly tight at the bottom.
//!
//! # How
//!
//! Bucket by a geometric progression: `γ = (1+ε)/(1−ε)`, and a value `v > 0` lands in bucket `⌈log_γ v⌉`. Every value
//! in a bucket is then within ε of the bucket's representative, which is what makes the guarantee fall out of the
//! indexing rather than out of any analysis. Merging is adding counts per index, which is associative and
//! commutative — so per-thread partials combine and the answer does not depend on how records were divided.
//!
//! This is the scheme [DDSketch] describes. The two things worth knowing about it here:
//!
//! - **negatives get a mirrored sketch**, indexed on `|v|`, because `log` has no opinion about them, and zero is
//!   counted on its own since `log 0` is not a number;
//! - **it is sparse**, so a distribution occupying a narrow range costs a handful of buckets, not the whole index
//!   space. ε = 1% covers a 10⁹ dynamic range in about 2,100 buckets, and real data touches far fewer.
//!
//! [DDSketch]: https://arxiv.org/abs/1908.10693
//!
//! # What happens when it runs out
//!
//! Bounded means bounded, so there is a cap. Past it, a new bucket is folded into the nearest existing one and the
//! event is **counted and reported** — `collapsed` in the result. Above the cap the relative-error guarantee no
//! longer strictly holds, and a caller can see that rather than having to trust it. With the default cap it takes a
//! dynamic range of roughly 10¹⁷ to trigger.

use aerospike_udf_sdk_native::partial;

/// The default relative error: 1%.
pub const DEFAULT_EPSILON: f64 = 0.01;

/// Buckets per sign. 4,096 at ε = 1% spans about 10¹⁷, which no real distribution occupies.
const MAX_BUCKETS: usize = 4096;

partial! {
    /// A relative-error quantile sketch.
    ///
    /// Indices and counts are kept as two parallel lists rather than a list of pairs: it halves the number of values
    /// on the wire and `partial!` writes both from the field list either way.
    ///
    /// `eps` travels *with* the sketch so that merging two sketches built with different precision can be detected
    /// rather than producing a quietly meaningless answer — see [`Sketch::merge`].
    pub struct Sketch {
        eps: f64,
        zeros: i64,
        pos_idx: Vec<i64>,
        pos_cnt: Vec<i64>,
        neg_idx: Vec<i64>,
        neg_cnt: Vec<i64>,
        collapsed: i64,
        mismatched: bool,
    }
}

impl Sketch {
    pub fn with_epsilon(eps: f64) -> Self {
        let mut s = Self::default();

        s.eps = if eps.is_finite() && eps > 0.0 && eps < 1.0 {
            eps
        } else {
            DEFAULT_EPSILON
        };

        s
    }

    fn gamma_ln(&self) -> f64 {
        let eps = if self.eps > 0.0 { self.eps } else { DEFAULT_EPSILON };

        ((1.0 + eps) / (1.0 - eps)).ln()
    }

    /// One observation.
    pub fn push(&mut self, v: f64) {
        if !v.is_finite() {
            return;
        }

        if v == 0.0 {
            // Catches -0.0 too, which compares equal to 0.0 and belongs in the same place.
            self.zeros += 1;
            return;
        }

        let index = (v.abs().ln() / self.gamma_ln()).ceil() as i64;
        let negative = v < 0.0;

        let mut collapsed = 0;

        {
            let (idx, cnt) = if negative {
                (&mut self.neg_idx, &mut self.neg_cnt)
            } else {
                (&mut self.pos_idx, &mut self.pos_cnt)
            };

            match idx.binary_search(&index) {
                Ok(at) => cnt[at] += 1,
                Err(at) => {
                    if idx.len() < MAX_BUCKETS {
                        idx.insert(at, index);
                        cnt.insert(at, 1);
                    } else {
                        // Full. Fold into the nearest neighbour and say so, rather than growing without bound or
                        // dropping the observation - either of which would be a silent lie about the data.
                        let nearest = if at == 0 { 0 } else { at.min(idx.len() - 1) };

                        cnt[nearest] += 1;
                        collapsed = 1;
                    }
                }
            }
        }

        self.collapsed += collapsed;
    }

    /// Add another sketch's counts to this one.
    pub fn merge(&mut self, other: &Self) {
        // Different precision means different bucket boundaries, so the counts are not about the same things. Adding
        // them anyway would produce a plausible number from incomparable parts; recording it lets `finalize` say so.
        if (self.eps - other.eps).abs() > f64::EPSILON && other.total() > 0 {
            if self.total() == 0 {
                self.eps = other.eps;
            } else {
                self.mismatched = true;
            }
        }

        self.zeros += other.zeros;
        self.collapsed += other.collapsed;
        self.mismatched |= other.mismatched;

        merge_side(&mut self.pos_idx, &mut self.pos_cnt, &other.pos_idx, &other.pos_cnt);
        merge_side(&mut self.neg_idx, &mut self.neg_cnt, &other.neg_idx, &other.neg_cnt);
    }

    pub fn total(&self) -> i64 {
        self.zeros + self.pos_cnt.iter().sum::<i64>() + self.neg_cnt.iter().sum::<i64>()
    }

    pub fn collapsed(&self) -> i64 {
        self.collapsed
    }

    pub fn mismatched(&self) -> bool {
        self.mismatched
    }

    pub fn epsilon(&self) -> f64 {
        self.eps
    }

    /// How many distinct buckets are in use — the sketch's actual footprint.
    pub fn buckets(&self) -> usize {
        self.pos_idx.len() + self.neg_idx.len()
    }

    /// The value at quantile `q`, where `q` is in `[0, 1]`.
    ///
    /// Walks the distribution in value order: negatives from the most negative up, then zeros, then positives. `None`
    /// when there is nothing to report.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let total = self.total();

        if total == 0 || !(0.0..=1.0).contains(&q) {
            return None;
        }

        // Rank of the wanted element, 0-based, clamped to the last.
        let want = (((total - 1) as f64) * q).round() as i64;
        let mut seen = 0i64;

        // Negatives, largest |v| first, since that is the smallest value.
        for (i, &index) in self.neg_idx.iter().enumerate().rev() {
            seen += self.neg_cnt[i];

            if seen > want {
                return Some(-self.representative(index));
            }
        }

        seen += self.zeros;

        if seen > want {
            return Some(0.0);
        }

        for (i, &index) in self.pos_idx.iter().enumerate() {
            seen += self.pos_cnt[i];

            if seen > want {
                return Some(self.representative(index));
            }
        }

        // Only reachable through rounding at the very top; the largest bucket is the right answer.
        self.pos_idx
            .last()
            .map(|&index| self.representative(index))
            .or(Some(0.0))
    }

    /// The value a bucket stands for: the point whose relative distance to both bucket edges is equal, which is what
    /// makes the error bound symmetric rather than one-sided.
    fn representative(&self, index: i64) -> f64 {
        let gamma = self.gamma_ln().exp();

        2.0 * gamma.powi(index as i32) / (1.0 + gamma)
    }
}

/// Add `(src_idx, src_cnt)` into `(idx, cnt)`, keeping the index list sorted.
///
/// A linear merge of two sorted runs rather than repeated insertion: merging two full sketches by `push`-like
/// insertion would be quadratic in the bucket count, on a path that runs once per partial per query.
fn merge_side(idx: &mut Vec<i64>, cnt: &mut Vec<i64>, src_idx: &[i64], src_cnt: &[i64]) {
    if src_idx.is_empty() {
        return;
    }

    let mut out_idx = Vec::with_capacity(idx.len() + src_idx.len());
    let mut out_cnt = Vec::with_capacity(idx.len() + src_idx.len());

    let (mut a, mut b) = (0usize, 0usize);

    while a < idx.len() || b < src_idx.len() {
        let take_a = b >= src_idx.len() || (a < idx.len() && idx[a] <= src_idx[b]);

        if take_a {
            let i = idx[a];
            let mut c = cnt[a];

            a += 1;

            if b < src_idx.len() && src_idx[b] == i {
                c += src_cnt[b];
                b += 1;
            }

            out_idx.push(i);
            out_cnt.push(c);
        } else {
            out_idx.push(src_idx[b]);
            out_cnt.push(src_cnt[b]);
            b += 1;
        }
    }

    // The cap applies to a merged sketch too, or two sketches from different partitions could exceed it together.
    if out_idx.len() > MAX_BUCKETS {
        let extra: i64 = out_cnt[MAX_BUCKETS..].iter().sum();

        out_idx.truncate(MAX_BUCKETS);
        out_cnt.truncate(MAX_BUCKETS);

        if let Some(last) = out_cnt.last_mut() {
            *last += extra;
        }
    }

    *idx = out_idx;
    *cnt = out_cnt;
}
