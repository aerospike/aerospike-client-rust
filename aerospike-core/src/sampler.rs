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

//! Probability sampling for the metrics subsystem.
//!
//! A [`Sampler`] is a small `Copy` value (no trait, no dynamic dispatch) that
//! decides, per command, whether the command is recorded. It samples when a
//! value drawn from a [`XorShift`] generator falls under `threshold` within
//! `range`:
//!
//! - `range == threshold` → **always** sample.
//! - `threshold == 0`     → **never** sample (nothing is recorded).
//! - otherwise            → sample with probability `threshold / range`.
//!
//! The default sampler in [`MetricsPolicy`](crate::metrics::MetricsPolicy) is
//! [`Sampler::all`], so enabling metrics records every command.

use crate::xor_shift::XorShift;

/// A probability sampler.
///
/// Samples when `rng.next_u64() % range < threshold`. Construct with
/// [`Sampler::new`], [`Sampler::all`], or [`Sampler::probability`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sampler {
    /// Denominator of the sampling fraction. Always `>= 1`.
    pub range: u64,
    /// Numerator of the sampling fraction. A drawn value reduced modulo `range`
    /// that is `< threshold` is sampled. Always `<= range`.
    pub threshold: u64,
}

impl Sampler {
    /// Creates a sampler that keeps `threshold` out of every `range` events.
    ///
    /// `range` is forced to at least 1 and `threshold` is clamped to `range`
    /// (so `range == threshold` samples everything).
    #[must_use]
    pub fn new(range: u64, threshold: u64) -> Self {
        let range = range.max(1);
        Sampler {
            range,
            threshold: threshold.min(range),
        }
    }

    /// A sampler that always samples (`range == threshold`).
    #[must_use]
    pub const fn all() -> Self {
        Sampler {
            range: 1,
            threshold: 1,
        }
    }

    /// A sampler that never samples (`threshold == 0`). With this sampler the
    /// per-command metrics record nothing; connection/tend lifecycle counters
    /// (gated only on metrics being enabled) still record.
    #[must_use]
    pub const fn never() -> Self {
        Sampler {
            range: 1,
            threshold: 0,
        }
    }

    /// Creates a sampler from a probability in `[0.0, 1.0]` (clamped), using a
    /// fixed one-in-a-million granularity.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // RANGE = 1e6 is exact in f64
    pub fn probability(p: f64) -> Self {
        const RANGE: u64 = 1_000_000;
        let p = p.clamp(0.0, 1.0);
        Sampler::new(RANGE, (p * RANGE as f64) as u64)
    }

    /// Returns `true` if the event should be sampled, drawing from `rand`.
    #[must_use]
    pub const fn should_sample(&self, rand: &mut XorShift) -> bool {
        // `range` is always >= 1 via the constructors; guard anyway so a
        // hand-built `Sampler { range: 0, .. }` can't divide by zero.
        if self.range == 0 {
            return false;
        }
        rand.next_u64() % self.range < self.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_range_and_threshold_always_samples() {
        let mut rng = XorShift::with_seed(1, 2);
        let s = Sampler::all();
        assert_eq!(
            s,
            Sampler {
                range: 1,
                threshold: 1
            }
        );
        for _ in 0..1000 {
            assert!(s.should_sample(&mut rng));
        }
        // An explicit equal range/threshold behaves the same.
        let s2 = Sampler::new(7, 7);
        for _ in 0..1000 {
            assert!(s2.should_sample(&mut rng));
        }
    }

    #[test]
    fn zero_threshold_never_samples() {
        let mut rng = XorShift::with_seed(3, 4);
        let s = Sampler::new(100, 0);
        for _ in 0..1000 {
            assert!(!s.should_sample(&mut rng));
        }
    }

    #[test]
    fn new_clamps_inputs() {
        assert_eq!(
            Sampler::new(0, 5),
            Sampler {
                range: 1,
                threshold: 1
            }
        );
        assert_eq!(
            Sampler::new(10, 99),
            Sampler {
                range: 10,
                threshold: 10
            }
        );
    }

    #[test]
    fn probability_bounds() {
        assert_eq!(
            Sampler::probability(1.0).range,
            Sampler::probability(1.0).threshold
        );
        assert_eq!(
            Sampler::probability(2.0).range,
            Sampler::probability(2.0).threshold
        );
        assert_eq!(Sampler::probability(0.0).threshold, 0);
        assert_eq!(Sampler::probability(-1.0).threshold, 0);
    }

    #[test]
    fn probability_half_is_roughly_balanced() {
        let mut rng = XorShift::with_seed(0x1234_5678, 0x9abc_def0);
        let s = Sampler::probability(0.5);
        let n = 100_000;
        let hits = (0..n).filter(|_| s.should_sample(&mut rng)).count();
        let ratio = hits as f64 / f64::from(n);
        assert!(
            (0.47..0.53).contains(&ratio),
            "sampling ratio {ratio} far from 0.5"
        );
    }

    #[test]
    fn deterministic_for_same_seed() {
        let s = Sampler::new(10, 3);
        let mut a = XorShift::with_seed(7, 11);
        let mut b = XorShift::with_seed(7, 11);
        for _ in 0..500 {
            assert_eq!(s.should_sample(&mut a), s.should_sample(&mut b));
        }
    }
}
