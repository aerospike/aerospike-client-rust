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

//! Unit tests for the arithmetic, which is all of it that needs no record.
//!
//! Two of these exist because the obvious implementation is wrong in a way that looks right:
//! `the_naive_variance_formula_would_have_failed_here` and `merging_gives_the_same_answer_as_one_pass`.

use crate::heavy::Heavy;
use crate::hll::Hll;
use crate::moments::Moments;
use crate::sketch::Sketch;
use crate::sum::ExactSum;

fn close(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() <= tol * b.abs().max(1.0)
}

#[test]
fn moments_match_a_hand_computed_answer() {
    let mut m = Moments::default();

    for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
        m.push(x);
    }

    assert_eq!(m.count(), 8);
    assert_eq!(m.sum(), Some(40.0));
    assert_eq!(m.mean(), Some(5.0));
    assert_eq!(m.min(), Some(2.0));
    assert_eq!(m.max(), Some(9.0));
    assert_eq!(m.range(), Some(7.0));

    // Population variance of that classic set is 4; the sample variance is 32/7.
    assert!(close(m.population_variance().unwrap(), 4.0, 1e-12));
    assert!(close(m.variance().unwrap(), 32.0 / 7.0, 1e-12));
    assert!(close(m.stddev().unwrap(), (32.0f64 / 7.0).sqrt(), 1e-12));
}

/// **The reason Welford is here rather than `Σx²`.**
///
/// Values near a billion varying by one: both terms of the textbook formula are ~1e18, `f64` keeps ~16 digits, and
/// the answer lives below the last one. The naive form is computed alongside so the failure is demonstrated rather
/// than asserted - it typically returns 0 or a negative variance, and a negative variance means `NaN` stddev.
#[test]
fn the_naive_variance_formula_would_have_failed_here() {
    let base = 1.0e9;
    let values: Vec<f64> = (0..1000).map(|i| base + (i % 3) as f64).collect();

    let mut m = Moments::default();
    let mut sum = 0.0f64;
    let mut sum_sq = 0.0f64;

    for &x in &values {
        m.push(x);
        sum += x;
        sum_sq += x * x;
    }

    let n = values.len() as f64;
    let naive = (sum_sq - n * (sum / n) * (sum / n)) / (n - 1.0);
    let welford = m.variance().unwrap();

    // The exact answer, computed by hand rather than guessed: `i % 3` over 0..1000 gives 334 zeros, 333 ones and
    // 333 twos, so the mean is 0.999 and Σ(x−x̄)² is 666.999, making the sample variance 666.999/999.
    //
    // Worth writing out because the first version of this test asserted 2/3 - the value for an even three-way split -
    // and the failure looked like a bug in Welford when it was a bug in the expectation.
    let exact = 666.999 / 999.0;

    assert!(
        close(welford, exact, 1e-9),
        "Welford should be right: got {welford}, exact {exact}"
    );

    // And the naive form should be visibly wrong on the same data - either far off or impossible.
    let naive_is_broken = !close(naive, exact, 1e-3) || naive < 0.0;

    assert!(
        naive_is_broken,
        "the naive formula was expected to break here but produced {naive} against an exact {exact}; if this ever \
         passes, the test data no longer demonstrates the hazard and needs a larger base"
    );
}

/// **Merging must give the same answer as one pass**, or a per-thread accumulator is an approximation.
///
/// Split three ways and merged in two orders, which is what associativity and commutativity mean in practice.
#[test]
fn merging_gives_the_same_answer_as_one_pass() {
    let values: Vec<f64> = (1..=300).map(|i| (i as f64) * 1.7 - 40.0).collect();

    let mut whole = Moments::default();

    for &x in &values {
        whole.push(x);
    }

    let mut parts = [Moments::default(), Moments::default(), Moments::default()];

    for (i, &x) in values.iter().enumerate() {
        parts[i % 3].push(x);
    }

    let mut forward = parts[0].clone();

    forward.merge(&parts[1]);
    forward.merge(&parts[2]);

    let mut backward = parts[2].clone();

    backward.merge(&parts[1]);
    backward.merge(&parts[0]);

    for (name, merged) in [("forward", &forward), ("backward", &backward)] {
        assert_eq!(merged.count(), whole.count(), "{name}: count");
        assert!(close(merged.mean().unwrap(), whole.mean().unwrap(), 1e-12), "{name}: mean");
        assert!(
            close(merged.variance().unwrap(), whole.variance().unwrap(), 1e-9),
            "{name}: variance"
        );
        assert!(
            close(merged.skewness().unwrap(), whole.skewness().unwrap(), 1e-6),
            "{name}: skewness {} vs {}",
            merged.skewness().unwrap(),
            whole.skewness().unwrap()
        );
        assert!(
            close(merged.kurtosis().unwrap(), whole.kurtosis().unwrap(), 1e-6),
            "{name}: kurtosis"
        );
        assert_eq!(merged.min(), whole.min(), "{name}: min");
        assert_eq!(merged.max(), whole.max(), "{name}: max");
    }
}

/// Skewness and kurtosis have known values for a symmetric set and for a uniform one.
#[test]
fn higher_moments_have_the_right_sign_and_scale() {
    // Symmetric: skewness 0.
    let mut sym = Moments::default();

    for x in [-3.0, -1.0, 0.0, 1.0, 3.0] {
        sym.push(x);
    }

    assert!(sym.skewness().unwrap().abs() < 1e-12, "a symmetric set is not skewed");

    // Right-tailed: positive skewness.
    let mut right = Moments::default();

    for x in [1.0, 1.0, 1.0, 1.0, 10.0] {
        right.push(x);
    }

    assert!(right.skewness().unwrap() > 0.0, "a long right tail is positively skewed");

    // A large uniform sample has excess kurtosis near -1.2.
    let mut uniform = Moments::default();

    for i in 0..10_000 {
        uniform.push(i as f64 / 10_000.0);
    }

    assert!(
        close(uniform.kurtosis().unwrap(), -1.2, 0.01),
        "uniform excess kurtosis should be about -1.2, got {}",
        uniform.kurtosis().unwrap()
    );
}

/// Fewer than 2, 3 or 4 observations mean variance, skewness and kurtosis are not defined - absent, not zero.
#[test]
fn undefined_statistics_are_absent_rather_than_zero() {
    let mut m = Moments::default();

    assert_eq!(m.mean(), None);
    assert_eq!(m.variance(), None);

    m.push(5.0);

    assert_eq!(m.mean(), Some(5.0));
    assert_eq!(m.variance(), None, "one observation has no sample variance");
    assert_eq!(m.skewness(), None);
    assert_eq!(m.kurtosis(), None);

    m.push(7.0);

    assert!(m.variance().is_some());
    assert_eq!(m.skewness(), None, "two observations have no skewness");
}

/// **The sketch's promise, checked against the truth**: every percentile within ε, relatively.
#[test]
fn every_percentile_is_within_the_stated_relative_error() {
    let eps = 0.01;
    let mut s = Sketch::with_epsilon(eps);

    // A wide, skewed spread - the shape percentiles are actually used on.
    let mut values: Vec<f64> = Vec::new();

    for i in 1..=10_000 {
        let v = (i as f64).powf(1.5) / 100.0;

        values.push(v);
        s.push(v);
    }

    values.sort_by(f64::total_cmp);

    for q in [0.01, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0] {
        let want = values[(((values.len() - 1) as f64) * q).round() as usize];
        let got = s.quantile(q).unwrap();

        assert!(
            (got - want).abs() <= eps * want.abs(),
            "q={q}: got {got}, true {want}, outside {}%",
            eps * 100.0
        );
    }

    assert_eq!(s.total(), 10_000);
    assert_eq!(s.collapsed(), 0, "this range should not exhaust the buckets");
}

/// Negatives, zero and a mixture, since `log` has nothing to say about any of them.
#[test]
fn the_sketch_handles_negatives_and_zero() {
    let mut s = Sketch::with_epsilon(0.01);

    for v in [-100.0, -1.0, 0.0, 0.0, 1.0, 100.0] {
        s.push(v);
    }

    assert_eq!(s.total(), 6);

    let min = s.quantile(0.0).unwrap();
    let max = s.quantile(1.0).unwrap();

    assert!(min < -50.0, "the smallest value is the most negative one, got {min}");
    assert!(max > 50.0, "the largest is the most positive, got {max}");
    assert_eq!(s.quantile(0.5), Some(0.0), "the median of that set is zero");
}

/// Sketch merging is adding counts, so it must be order-independent and match one pass.
#[test]
fn sketches_merge_to_the_same_distribution() {
    let values: Vec<f64> = (1..=500).map(|i| i as f64 * 3.0).collect();

    let mut whole = Sketch::with_epsilon(0.01);

    for &v in &values {
        whole.push(v);
    }

    let mut a = Sketch::with_epsilon(0.01);
    let mut b = Sketch::with_epsilon(0.01);

    for (i, &v) in values.iter().enumerate() {
        if i % 2 == 0 {
            a.push(v);
        } else {
            b.push(v);
        }
    }

    let mut ab = a.clone();
    ab.merge(&b);

    let mut ba = b.clone();
    ba.merge(&a);

    assert_eq!(ab.total(), whole.total());

    for q in [0.0, 0.1, 0.5, 0.9, 1.0] {
        assert_eq!(ab.quantile(q), whole.quantile(q), "q={q}: merged should equal one pass");
        assert_eq!(ba.quantile(q), ab.quantile(q), "q={q}: merge order must not matter");
    }
}

/// Mixing two precisions is detected rather than producing a plausible number from incomparable parts.
#[test]
fn merging_different_precisions_is_flagged() {
    let mut coarse = Sketch::with_epsilon(0.10);
    let mut fine = Sketch::with_epsilon(0.001);

    for i in 1..100 {
        coarse.push(i as f64);
        fine.push(i as f64);
    }

    assert!(!coarse.mismatched());

    coarse.merge(&fine);

    assert!(coarse.mismatched(), "a precision mismatch must be visible in the result");
}

/// Below capacity, Space-Saving is exact - and it says so.
#[test]
fn the_mode_is_exact_while_the_table_fits() {
    let mut h = Heavy::with_capacity(8);

    for v in [1.0, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0] {
        h.push(v);
    }

    let (value, count, err) = h.mode().unwrap();

    assert_eq!(value, 3.0);
    assert_eq!(count, 3);
    assert_eq!(err, 0, "nothing was evicted, so the count is exact");
    assert!(!h.evicted());
    assert!(h.mode_is_certain(7));
    assert_eq!(h.distinct_seen(), 4);
}

/// Over capacity it stays honest: the dominant value is still found, and the counts admit their uncertainty.
#[test]
fn a_dominant_value_survives_an_overflowing_table() {
    let mut h = Heavy::with_capacity(4);

    // 100 of one value, plus 200 singletons - far more distinct values than rows.
    for _ in 0..100 {
        h.push(7.0);
    }

    for i in 0..200 {
        h.push(1000.0 + i as f64);
    }

    let (value, count, _) = h.mode().unwrap();

    assert_eq!(value, 7.0, "a value occurring far more than n/m must be found");
    assert!(count >= 100, "its count is an over-estimate, never an under-estimate");
    assert!(h.evicted(), "the table overflowed, and says so");
    assert!(
        h.distinct_seen() > 4,
        "distinct_seen reports what was offered, not what was kept"
    );
}

/// Ties break toward the smaller value, so the answer does not depend on arrival order.
#[test]
fn mode_ties_break_deterministically() {
    let mut forward = Heavy::with_capacity(8);
    let mut backward = Heavy::with_capacity(8);

    for v in [5.0, 5.0, 2.0, 2.0] {
        forward.push(v);
    }

    for v in [2.0, 2.0, 5.0, 5.0] {
        backward.push(v);
    }

    assert_eq!(forward.mode().unwrap().0, 2.0);
    assert_eq!(backward.mode().unwrap().0, 2.0, "arrival order must not decide a tie");
}

/// `-0.0` and `0.0` are one value, so they must be one row.
#[test]
fn negative_zero_is_the_same_key_as_zero() {
    let mut h = Heavy::with_capacity(8);

    h.push(0.0);
    h.push(-0.0);
    h.push(1.0);

    assert_eq!(h.mode().unwrap(), (0.0, 2, 0));
    assert_eq!(h.distinct_seen(), 2, "zero and negative zero are not two distinct values");
}

#[test]
fn heavy_tables_merge() {
    let mut a = Heavy::with_capacity(16);
    let mut b = Heavy::with_capacity(16);

    for _ in 0..5 {
        a.push(1.0);
    }

    for _ in 0..9 {
        b.push(2.0);
    }

    a.merge(&b);

    assert_eq!(a.mode().unwrap().0, 2.0);
    assert_eq!(a.top(2)[0], (2.0, 9, 0));
    assert_eq!(a.top(2)[1], (1.0, 5, 0));
}

/// The distinct estimate should be within a few standard errors, at both ends of the range where the estimator
/// switches from linear counting to the raw form.
#[test]
fn the_distinct_estimate_is_close_at_both_ends() {
    for n in [10usize, 100, 1_000, 50_000] {
        let mut h = Hll::new();

        for i in 0..n {
            h.push(i as f64 * 1.5);
        }

        let got = h.estimate();
        let tolerance = 5.0 * Hll::relative_error() * n as f64 + 2.0;

        assert!(
            (got - n as f64).abs() <= tolerance,
            "n={n}: estimated {got}, tolerance {tolerance}"
        );
    }
}

/// Repeats must not move it, which is the property that makes it a *distinct* count.
#[test]
fn repeats_do_not_change_the_distinct_estimate() {
    let mut h = Hll::new();

    for i in 0..500 {
        h.push(i as f64);
    }

    let before = h.estimate();

    for _ in 0..20 {
        for i in 0..500 {
            h.push(i as f64);
        }
    }

    assert_eq!(h.estimate(), before, "seeing the same values again is not new information");
}

/// Merging is a per-register maximum, so it is exact and order-independent.
#[test]
fn distinct_sketches_merge_without_double_counting() {
    let mut a = Hll::new();
    let mut b = Hll::new();

    // Overlapping halves: 0..600 and 400..1000, so 1000 distinct in total.
    for i in 0..600 {
        a.push(i as f64);
    }

    for i in 400..1000 {
        b.push(i as f64);
    }

    let mut ab = a.clone();
    ab.merge(&b);

    let mut ba = b.clone();
    ba.merge(&a);

    assert_eq!(ab.estimate(), ba.estimate(), "merge order must not matter");

    let tolerance = 5.0 * Hll::relative_error() * 1000.0 + 2.0;

    assert!(
        (ab.estimate() - 1000.0).abs() <= tolerance,
        "overlap must not be double counted: got {}",
        ab.estimate()
    );
}

//==========================================================
// Overflow, and input another process chose.
//

/// **A register byte from the wire must not reach a shift unchecked.**
///
/// `estimate` computes `1u64 << r` per register. On the *write* path `r` is bounded by construction — the index takes
/// the top 10 bits and a sentinel bit is ORed into the remainder, so the rank cannot exceed 55 — but a partial arrives
/// from the network as an opaque blob, and `from_partial` stored it verbatim. A byte of 200 then meant `1u64 << 200`,
/// which panics in a debug build and silently computes `1u64 << 8` in a release one, because Rust masks the shift
/// amount.
///
/// Neither is acceptable for a value another process supplied, so registers are clamped on the way in and again in
/// `estimate`. This feeds it the bytes that used to break it.
#[test]
fn a_hostile_register_byte_cannot_overflow_the_shift() {
    use aerospike_udf_sdk_native::Blob;

    let mut h = Hll::new();

    h.set_registers_for_test(Blob(vec![200u8; 1024]));

    let estimate = h.estimate();

    assert!(
        estimate.is_finite() && estimate >= 0.0,
        "a corrupt sketch should still produce a finite number, got {estimate}"
    );

    // A short blob, which a truncated or older partial could be.
    let mut short = Hll::new();

    short.set_registers_for_test(Blob(vec![255u8; 3]));

    assert!(short.estimate().is_finite(), "a short register array must not panic");

    // And merging a corrupt sketch must not poison a good one.
    let mut good = Hll::new();

    for i in 0..1000 {
        good.push(i as f64);
    }

    let mut corrupt = Hll::new();

    corrupt.set_registers_for_test(Blob(vec![200u8; 1024]));
    good.merge(&corrupt);

    assert!(good.estimate().is_finite(), "merging a corrupt sketch must not poison the estimate");
}

/// A hostile `k` must not overflow the arithmetic that sizes the frequency table.
#[test]
fn a_hostile_k_is_clamped() {
    for k in [1i64, 5, 1_000, i64::MAX, i64::MIN, -1] {
        let clamped = crate::clamp_k(Some(k));

        assert!(
            (1..=crate::MAX_TOP_K).contains(&clamped),
            "k={k} clamped to {clamped}, outside 1..={}",
            crate::MAX_TOP_K
        );

        // The multiplication that used to overflow i64 for a large k.
        assert!(
            (clamped as i64).saturating_mul(4) > 0,
            "k={k}: the capacity arithmetic must stay positive"
        );
    }

    assert_eq!(crate::clamp_k(None), 5, "the default stays 5");
}

/// Values large enough to overflow the fourth moment yield an *absent* statistic, not a plausible wrong one.
#[test]
fn moments_that_overflow_are_reported_as_absent() {
    let mut m = Moments::default();

    // x⁴ for these exceeds f64::MAX, so m4 goes infinite.
    for x in [1e90, -1e90, 2e90, -2e90, 1.5e90] {
        m.push(x);
    }

    // What can still be represented is still right.
    assert_eq!(m.count(), 5);
    assert!(m.min().unwrap() <= -2e90);
    assert!(m.max().unwrap() >= 2e90);

    // And what overflowed is not a finite number - which is what `put_opt` filters, so the result map omits the key
    // rather than reporting `inf` as though it were a measurement.
    let kurtosis = m.kurtosis();

    assert!(
        kurtosis.is_none() || !kurtosis.unwrap().is_finite(),
        "an overflowed kurtosis must never look like a real value: {kurtosis:?}"
    );
}

//==========================================================
// The two sums.
//

/// **The claim that lets the integer accumulator skip a per-record overflow check**, asserted as arithmetic.
///
/// The worst dataset that can exist: every record stores the largest-magnitude `i64` there is, and there are as many
/// records as an `i64` count can express.
#[test]
fn the_integer_accumulator_provably_cannot_overflow() {
    // i64::MIN has the larger magnitude of the two extremes, and negating it needs the wider type.
    let widest = -(i64::MIN as i128);
    let most = i64::MAX as i128;

    assert_eq!(widest, 1i128 << 63, "the widest value is 2^63, not i64::MAX");

    let bound = widest
        .checked_mul(most)
        .expect("the bound itself must be representable, or this test proves nothing");

    // 2^126 − 2^63 against i128::MAX = 2^127 − 1: a whole factor of two spare, which is why `add_int` can add without
    // asking, and why a saturation flag ever firing means a partial was malformed rather than that data was large.
    assert!(
        bound < i128::MAX / 2,
        "an i128 must hold the largest total any dataset can produce, with room: {bound}"
    );
}

#[test]
fn the_hi_lo_split_survives_every_extreme() {
    let extremes = [
        0i128,
        1,
        -1,
        i64::MAX as i128,
        i64::MIN as i128,
        1i128 << 64,
        -(1i128 << 64),
        (1i128 << 100) + 12_345,
        i128::MAX,
        i128::MIN,
    ];

    for v in extremes {
        let mut s = ExactSum::default();

        s.set_int_for_test(v);

        // Two i64 fields on the wire, one i128 in memory - and no value in between them that does not survive the
        // trip. `-1` and `i128::MIN` are the two that catch a split written with the wrong shift or the wrong cast.
        assert_eq!(s.int(), v, "the hi/lo split must round trip {v}");
    }
}

/// **The reason integers are not widened to `f64` on the way in.**
///
/// Past 2⁵³ consecutive integers stop being representable, so the loss happens at the cast, before any summing. The
/// test asserts its own premise first — that `2⁵³ + 1` does not survive an `f64` — so it cannot pass for the wrong
/// reason on some future target.
#[test]
fn an_integer_sum_above_two_to_the_53_is_exact() {
    let base = 1i64 << 53;

    assert_ne!(
        (base + 1) as f64 as i64,
        base + 1,
        "the premise: an f64 cannot hold 2^53 + 1"
    );

    let mut s = ExactSum::default();
    let mut m = Moments::default();
    let mut truth: i128 = 0;

    for k in 0..1000i64 {
        let v = base + k;

        s.push_int(v);
        m.push(v as f64);
        truth += v as i128;
    }

    assert_eq!(s.count(), 1000);
    assert_eq!(s.exact(), Some(truth), "the integer total must be exact");
    assert!(!s.saturated());

    // And the route this replaced is off by a visible amount. Deterministic, not flaky: the same values every run.
    let widened = m.sum().unwrap() as i128;

    assert_ne!(
        widened, truth,
        "if the f64 route ever agrees, this test has stopped demonstrating anything"
    );

    // The i64 the result map reports, since this total still fits one.
    assert!(i64::try_from(truth).is_ok(), "and it is reportable as an Aerospike integer");
}

/// A total past `i64` still has an exact answer, and a decimal string is the only lossless way to hand it over.
#[test]
fn a_total_beyond_i64_is_still_exact() {
    let mut s = ExactSum::default();

    s.push_int(i64::MAX);
    s.push_int(i64::MAX);

    let exact = s.exact().expect("two integers, no doubles, no saturation");

    assert_eq!(exact, 2 * (i64::MAX as i128));
    assert!(i64::try_from(exact).is_err(), "which is why finalize falls back to a string");
    assert_eq!(exact.to_string(), "18446744073709551614");

    // `sum` still reports something, and it is the correctly rounded double - useful, and lossy, and that is exactly
    // the pair of facts `sum_exact_str` exists to separate.
    assert_eq!(s.total(), Some(2.0 * (i64::MAX as f64)));
}

/// The double side gets the correctly rounded total where a plain loop does not.
///
/// # The reference cannot be an `f64`, and that is not a detail
///
/// Two earlier versions of this test tried to hold the true answer in an `f64` and failed on their own premise, both
/// times because those two requirements are secretly the **same requirement**: a total an `f64` can represent exactly
/// is a total the naive loop *computes* exactly. Scaling the values onto a 2⁻¹⁰ grid does not help — it moves both
/// thresholds by the same octave. There is no gap to aim at.
///
/// So the reference is an `i128` and the comparison happens there. The values are integers near 10¹² (each exact in an
/// `f64`, being under 2⁵³) and there are enough of them that the running total passes 2⁵³ and rounding becomes
/// unavoidable. Both totals are then integer-valued `f64`s, so `as i128` converts them without loss and the two errors
/// are exact integers.
///
/// # What is actually asserted
///
/// Not "compensated is exact" — at this magnitude no `f64` can be, since one ulp is 16. The claim is the sharper one:
/// **compensated lands within one ulp of the truth, which is the best a double can do, and a plain loop does not.**
#[test]
fn the_double_sum_is_compensated() {
    let mut s = ExactSum::default();
    let mut naive = 0.0f64;
    let mut truth: i128 = 0;

    for i in 0..100_000i128 {
        let v = 1_000_000_000_000i128 + (i % 977) * 7919 + (i * 37) % 1024;

        assert_eq!(v as f64 as i128, v, "each value must itself be exact in an f64");

        s.push_f64(v as f64);
        naive += v as f64;
        truth += v;
    }

    assert!(
        truth > (1i128 << 53),
        "the premise: the running total has to pass 2^53 or nothing rounds at all"
    );

    let one_ulp = (truth as f64 * f64::EPSILON).ceil() as i128;
    let naive_err = (naive as i128 - truth).abs();
    let comp_err = (s.double_total() as i128 - truth).abs();

    assert!(
        comp_err <= one_ulp,
        "compensated must land within one ulp ({one_ulp}), the best a double can do: off by {comp_err}"
    );
    assert!(
        naive_err > one_ulp,
        "the premise: a plain loop must drift past one ulp, or there is nothing to fix ({naive_err})"
    );

    assert_eq!(s.exact(), None, "doubles were seen, so there is no exact integer answer");
}

/// Integers to one accumulator, doubles to the other, and nothing crosses until the end.
#[test]
fn the_two_sums_only_meet_at_the_end() {
    let mut s = ExactSum::default();

    for i in 1..=1000i64 {
        s.push_int(i);
    }

    for i in 1..=1000i64 {
        s.push_f64(i as f64 / 4.0);
    }

    assert_eq!(s.int_count(), 1000);
    assert_eq!(s.double_count(), 1000);
    assert_eq!(s.count(), 2000);

    // Σ1..1000 = 500500, and a quarter of it on the double side.
    assert_eq!(s.int(), 500_500);
    assert_eq!(s.double_total(), 125_125.0);
    assert_eq!(s.total(), Some(625_625.0));
    assert_eq!(s.mean(), Some(625_625.0 / 2000.0));

    assert_eq!(
        s.exact(),
        None,
        "a double was seen, so the total is not an exact integer and must not claim to be"
    );
}

/// Merging is `i128` addition, so it is exact and order-independent — not close, and not nearly.
#[test]
fn sums_merge_to_the_same_answer() {
    let values: Vec<i64> = (0..3000).map(|i| (1i64 << 53) + i * 7).collect();

    let mut whole = ExactSum::default();

    for &v in &values {
        whole.push_int(v);
    }

    let mut thirds = [ExactSum::default(), ExactSum::default(), ExactSum::default()];

    for (i, &v) in values.iter().enumerate() {
        thirds[i % 3].push_int(v);
    }

    let mut forwards = thirds[0].clone();

    forwards.merge(&thirds[1]);
    forwards.merge(&thirds[2]);

    let mut backwards = thirds[2].clone();

    backwards.merge(&thirds[1]);
    backwards.merge(&thirds[0]);

    assert_eq!(forwards.count(), 3000);
    assert_eq!(forwards.exact(), whole.exact());
    assert_eq!(backwards.exact(), whole.exact(), "and in the other order too");
}

/// **A partial is input from another process** — the lesson the HyperLogLog registers taught, in its arithmetic form.
///
/// No dataset reaches `i128::MAX`; `the_integer_accumulator_provably_cannot_overflow` says so. A malformed partial
/// hands it over directly, and the next `push_int` would panic in debug and wrap to a negative total in release.
#[test]
fn a_hostile_partial_cannot_overflow_the_integer_sum() {
    let mut s = ExactSum::default();

    s.set_int_for_test(i128::MAX);

    assert!(!s.saturated(), "arriving there is not itself the overflow");

    s.push_int(i64::MAX);

    assert!(s.saturated(), "the overflow has to be visible rather than a panic or a wrap");
    assert_eq!(s.int(), i128::MAX, "and saturate rather than wrap to a negative total");
    assert_eq!(s.exact(), None, "a saturated total is not an exact one and must not be offered as one");

    // The other end behaves the same way.
    let mut t = ExactSum::default();

    t.set_int_for_test(i128::MIN);
    t.push_int(i64::MIN);

    assert!(t.saturated());
    assert_eq!(t.int(), i128::MIN);
}

/// An infinity in a partial would poison every record that came before it. It is refused, and the refusal is reported.
#[test]
fn a_non_finite_partial_is_refused_rather_than_absorbed() {
    let mut good = ExactSum::default();

    for i in 1..=100i64 {
        good.push_f64(i as f64);
    }

    let mut poison = ExactSum::default();

    poison.set_double_for_test(f64::INFINITY, 0.0, 7);

    good.merge(&poison);

    assert_eq!(good.double_total(), 5050.0, "the good total must survive intact");
    assert_eq!(good.double_count(), 100, "and the bad side's count goes with its total");
    assert!(good.refused_a_partial(), "and the refusal is reported rather than silent");
    assert_eq!(good.mean(), Some(50.5), "so the mean stays the mean of what was accumulated");

    // A NaN compensation term is the subtler version of the same thing.
    let mut also = ExactSum::default();

    also.push_f64(1.0);

    let mut nan = ExactSum::default();

    nan.set_double_for_test(1.0, f64::NAN, 1);
    also.merge(&nan);

    assert_eq!(also.double_total(), 1.0);
    assert!(also.refused_a_partial());
}
