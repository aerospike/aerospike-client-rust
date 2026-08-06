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

//! Count, sum, min, max, mean, variance, skewness and kurtosis — **exactly**, in fixed state.
//!
//! # Why not `sum(x)` and `sum(x²)`
//!
//! Because the textbook formula for variance is unusable on real data:
//!
//! ```text
//! var = (Σx² − n·x̄²) / n
//! ```
//!
//! Take values near a billion that vary by one. `Σx²` and `n·x̄²` are both around 10¹⁸, `f64` carries about sixteen
//! significant digits, and the difference between them — the answer — is *entirely* below the last retained digit. It
//! returns nonsense, and often a **negative** variance, which makes the standard deviation `NaN`.
//!
//! That failure is not exotic: timestamps, prices in cents, sequence numbers and sensor readings all look like it. So
//! this keeps [Welford]'s running form instead, which never subtracts two large nearly-equal numbers. A unit test
//! pins the case that breaks the naive version.
//!
//! [Welford]: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
//!
//! # Why the merge formulas look like that
//!
//! An accumulator per query thread means partials have to combine, so the *parallel* form of Welford is not optional
//! here — it is the whole reason this is usable in a distributed aggregation. Combining `(n, mean, M2, M3, M4)` pairs
//! is Chan/Pébay's pairwise update, and it is **associative and commutative**, which is what makes the answer
//! independent of how records were divided between threads, nodes or dumps.
//!
//! A test asserts that by splitting a dataset three ways and merging in two different orders.

use aerospike_udf_sdk_native::partial;

partial! {
    /// Running moments, plus min and max.
    ///
    /// `partial!` writes the serialization, so nothing here is about the wire — and because a `partial!` struct is
    /// itself a `PartialField`, this nests inside the top-level accumulator as one field.
    pub struct Moments {
        n: i64,
        mean: f64,
        m2: f64,
        m3: f64,
        m4: f64,
        min: f64,
        max: f64,
        seen: bool,
    }
}

impl Moments {
    /// One observation.
    pub fn push(&mut self, x: f64) {
        // Welford, extended to the third and fourth moments. The `delta / n` scaling is what keeps every term the
        // size of the data rather than the size of the data squared.
        let n1 = self.n as f64;

        self.n += 1;

        let n = self.n as f64;
        let delta = x - self.mean;
        let delta_n = delta / n;
        let delta_n2 = delta_n * delta_n;
        let term = delta * delta_n * n1;

        self.mean += delta_n;
        self.m4 += term * delta_n2 * (n * n - 3.0 * n + 3.0)
            + 6.0 * delta_n2 * self.m2
            - 4.0 * delta_n * self.m3;
        self.m3 += term * delta_n * (n - 2.0) - 3.0 * delta_n * self.m2;
        self.m2 += term;

        if !self.seen {
            self.seen = true;
            self.min = x;
            self.max = x;
        } else {
            if x < self.min {
                self.min = x;
            }

            if x > self.max {
                self.max = x;
            }
        }
    }

    /// Combine another accumulator's moments into this one.
    ///
    /// Associative and commutative — see the module note. The `n_a == 0` shortcut is not just an optimisation: the
    /// general formulas divide by `n`, so an empty side has to be handled before them.
    pub fn merge(&mut self, other: &Self) {
        if other.n == 0 {
            return;
        }

        if self.n == 0 {
            *self = other.clone();
            return;
        }

        let na = self.n as f64;
        let nb = other.n as f64;
        let n = na + nb;

        let delta = other.mean - self.mean;
        let d2 = delta * delta;
        let d3 = d2 * delta;
        let d4 = d2 * d2;

        let m2 = self.m2 + other.m2 + d2 * na * nb / n;

        let m3 = self.m3
            + other.m3
            + d3 * na * nb * (na - nb) / (n * n)
            + 3.0 * delta * (na * other.m2 - nb * self.m2) / n;

        let m4 = self.m4
            + other.m4
            + d4 * na * nb * (na * na - na * nb + nb * nb) / (n * n * n)
            + 6.0 * d2 * (na * na * other.m2 + nb * nb * self.m2) / (n * n)
            + 4.0 * delta * (na * other.m3 - nb * self.m3) / n;

        self.mean += delta * nb / n;
        self.m2 = m2;
        self.m3 = m3;
        self.m4 = m4;
        self.n = self.n + other.n;

        if !self.seen {
            self.seen = other.seen;
            self.min = other.min;
            self.max = other.max;
        } else if other.seen {
            if other.min < self.min {
                self.min = other.min;
            }

            if other.max > self.max {
                self.max = other.max;
            }
        }
    }

    pub fn count(&self) -> i64 {
        self.n
    }

    pub fn seen(&self) -> bool {
        self.seen
    }

    pub fn mean(&self) -> Option<f64> {
        (self.n > 0).then_some(self.mean)
    }

    /// The sum, reconstructed from the mean — **superseded by [`crate::sum::ExactSum`]**, which is what `finalize`
    /// reports.
    ///
    /// This was the sum, on the reasoning that `mean · n` is the same number to within the rounding Welford already
    /// accepts. It is not: Welford's mean drifts by O(n·ε) and multiplying by `n` rounds twice more, which measured
    /// **18 ulps** over 100,000 values around 1e8 — against 0 for a compensated running total costing 1.25 ns/value.
    /// An 18-ulp sum is a fine sum; the problem was that this module's own table called it exact.
    ///
    /// Kept because the stage probes measure [`Moments`] in isolation and because it is the honest reference for what
    /// the reconstruction costs.
    pub fn sum(&self) -> Option<f64> {
        (self.n > 0).then(|| self.mean * self.n as f64)
    }

    pub fn min(&self) -> Option<f64> {
        self.seen.then_some(self.min)
    }

    pub fn max(&self) -> Option<f64> {
        self.seen.then_some(self.max)
    }

    /// Sample variance, dividing by `n − 1`.
    ///
    /// **Sample, not population**, because a query over records is nearly always a sample of a process rather than
    /// the entire population — and because getting this wrong silently is exactly the kind of thing that makes two
    /// tools disagree by a factor of `n/(n−1)` and nobody knows which is right. `population_variance` is there for
    /// when the whole population really is in the set.
    pub fn variance(&self) -> Option<f64> {
        (self.n > 1).then(|| self.m2 / (self.n as f64 - 1.0))
    }

    pub fn population_variance(&self) -> Option<f64> {
        (self.n > 0).then(|| self.m2 / self.n as f64)
    }

    pub fn stddev(&self) -> Option<f64> {
        self.variance().map(f64::sqrt)
    }

    /// Coefficient of variation — the standard deviation as a fraction of the mean.
    ///
    /// `None` when the mean is zero, because the answer is then unbounded rather than large.
    pub fn cv(&self) -> Option<f64> {
        let (sd, mean) = (self.stddev()?, self.mean()?);

        (mean != 0.0).then_some(sd / mean)
    }

    /// Fisher-Pearson skewness. `None` when there is no spread to be skewed about.
    pub fn skewness(&self) -> Option<f64> {
        if self.n < 3 || self.m2 <= 0.0 {
            return None;
        }

        Some((self.n as f64).sqrt() * self.m3 / self.m2.powf(1.5))
    }

    /// **Excess** kurtosis: 0 for a normal distribution, not 3.
    ///
    /// Which convention is in use is worth stating rather than leaving to be discovered, since the two differ by
    /// exactly 3 and both are called "kurtosis".
    pub fn kurtosis(&self) -> Option<f64> {
        if self.n < 4 || self.m2 <= 0.0 {
            return None;
        }

        Some(self.n as f64 * self.m4 / (self.m2 * self.m2) - 3.0)
    }

    pub fn range(&self) -> Option<f64> {
        Some(self.max()? - self.min()?)
    }
}
