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

//! Millions of samples, checked against a reference that is **obviously** correct rather than clever.
//!
//! ```sh
//! cargo test --release --test scale                 # the default scale, a few seconds
//! TS_SCALE=10000000 cargo test --release --test scale -- --nocapture
//! ```
//!
//! # Why these are separate from the unit tests
//!
//! `src/tests.rs` checks *rules* on a handful of samples: this bucket combines, that one is refused. None of it would
//! notice a defect that only shows up after the ring has lapped four hundred times, or after a mean has accumulated a
//! million additions, or on the one bucket per year where a calendar month is 28 days long.
//!
//! # The reference is written twice, on purpose
//!
//! Every expectation here is recomputed by a second implementation that shares no code with the ring — no
//! `ring::combine`, no `Shape::bucket_of`, no slot arithmetic. A test that asserts the ring agrees with itself asserts
//! nothing. Where the ring says "the mean of this hour is x", the reference divides a sum it kept itself by a count it
//! kept itself, and the two have to match **bit for bit**.
//!
//! That is affordable because the sample values are integers: a sum of 31 million values under 1,000 is a shade over
//! 3 × 10¹⁰, exactly representable in an `f64`, so a logic error cannot hide behind a rounding difference. Rounding gets
//! its own test, with values chosen so that it cannot be exact.

use std::collections::BTreeMap;
use std::env;
use std::time::Instant;

use aerospike_udf_example_timeseries::calendar::{self, MS_PER_DAY, MS_PER_HOUR, MS_PER_MIN, MS_PER_SEC};
use aerospike_udf_example_timeseries::config::{
    default_archives, Agg, Archive, Config, Kind, Mode, Resolution, Shape,
};
use aerospike_udf_example_timeseries::ring::{self, CounterEvent, Num, Outcome, RingMut};

/// How many samples the heavy tests push. Overridable, because "millions" is a spectrum.
///
/// The default is chosen to be a few seconds in `--release` and tolerable in debug. Ten million is the number these were
/// developed against.
fn scale() -> i64 {
    env::var("TS_SCALE")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1_000_000)
}

/// 2026-01-01T00:00:00Z, a Thursday, in a non-leap year — so a run of a few million seconds crosses month boundaries of
/// 31, 28 and 30 days and a week boundary that is not a multiple of anything.
fn origin() -> i64 {
    calendar::days_from_civil(2026, 1, 1) * MS_PER_DAY
}

/// The sample at index `i`. **Integers**, so every sum below is exact in an `f64`.
///
/// 997 is prime, so the value cycle shares no factor with 60, 3,600 or 86,400 and therefore never lines up with a bucket
/// boundary — which is what stops a per-bucket aggregate from being accidentally right.
fn value_at(i: i64) -> f64 {
    (i % 997) as f64
}

/// A config with one archive, so a test can drive a single resolution at a chosen aggregation.
fn one_archive(resolution: Resolution, agg: Agg, slots: i64) -> Config {
    let mut cfg = Config::new(MS_PER_SEC, 3_600, 0).expect("a valid base shape");

    cfg.set_archives(&[Archive::new(resolution, agg, slots)])
        .expect("one archive is a valid set");

    cfg
}

fn shape_of(cfg: &Config, resolution: Resolution) -> Shape {
    Shape::at_resolution(cfg, cfg.fingerprint_of(), resolution).expect("that archive exists")
}

//==========================================================
// The reference: an independent accumulator, one bucket at a time.
//

/// What a bucket should hold, accumulated without touching any of the ring's code.
#[derive(Clone, Copy, Debug)]
struct Expected {
    first: f64,
    last: f64,
    min: f64,
    max: f64,
    sum: f64,
    count: i64,
}

impl Expected {
    fn new(v: f64) -> Self {
        Self { first: v, last: v, min: v, max: v, sum: v, count: 1 }
    }

    fn add(&mut self, v: f64) {
        self.last = v;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.sum += v;
        self.count += 1;
    }

    /// The value the ring should report, per aggregation. Deliberately a separate `match` from `ring::combine`.
    fn value(&self, agg: Agg) -> f64 {
        match agg {
            Agg::First => self.first,
            Agg::Last => self.last,
            Agg::Min => self.min,
            Agg::Max => self.max,
            Agg::Sum => self.sum,
            Agg::Count => self.count as f64,
            Agg::Mean => self.sum / self.count as f64,
        }
    }
}

/// The bucket a timestamp falls in, computed **without** `Resolution::bucket_of`.
///
/// Second through day divide; week counts days from a Monday found by weekday arithmetic; month and year come from the
/// calendar module, which the unit tests verify against a table of dates and a two-century round trip. Sharing the
/// calendar is unavoidable — reimplementing Gregorian arithmetic in a test would be testing the copy — but the
/// *bucketing* is independent.
fn expected_bucket(res: Resolution, ms: i64) -> i64 {
    let days = ms.div_euclid(MS_PER_DAY);

    match res {
        Resolution::Second => ms.div_euclid(MS_PER_SEC),
        Resolution::Minute => ms.div_euclid(MS_PER_MIN),
        Resolution::Hour => ms.div_euclid(MS_PER_HOUR),
        Resolution::Day => days,
        // 1970-01-01 was a Thursday, so day 0 is weekday 3 counting Monday as 0.
        Resolution::Week => (days + 3).div_euclid(7),
        Resolution::Month => {
            let (y, m, _) = calendar::civil_from_days(days);

            (y - 1970) * 12 + (m as i64 - 1)
        }
        Resolution::Year => {
            let (y, _, _) = calendar::civil_from_days(days);

            y - 1970
        }
    }
}

/// Push `n` one-second samples into a ring and return how long it took.
fn fill(ring: &mut RingMut<'_>, n: i64, from_ms: i64) -> f64 {
    let started = Instant::now();

    for i in 0..n {
        let applied = ring
            .append(from_ms + i * MS_PER_SEC, Num::F(value_at(i)))
            .unwrap_or_else(|e| panic!("sample {i}: {e}"));

        // Monotone input into a ring wider than one bucket: every sample either starts a bucket or joins the newest.
        // Anything else means the placement logic drifted, and finding that out at sample 4,000,000 is the point.
        assert!(
            matches!(applied.outcome, Outcome::Placed | Outcome::Combined),
            "sample {i} at {} was {:?}",
            from_ms + i * MS_PER_SEC,
            applied.outcome
        );
    }

    started.elapsed().as_secs_f64()
}

/// Walk the same samples again and assert every **live** bucket matches the reference.
///
/// Returns how many buckets were checked, so a test can fail if it silently checked none — which is how a scale test
/// passes while measuring nothing.
fn verify(ring: &RingMut<'_>, shape: &Shape, n: i64, from_ms: i64, agg: Agg) -> i64 {
    let (first, last) = ring.window().expect("a filled ring has a window");
    let mut checked = 0i64;
    let mut current: Option<(i64, Expected)> = None;

    // O(1) memory: the input is monotone, so a bucket is complete as soon as the next one starts.
    let mut flush = |bucket: i64, exp: &Expected| {
        if bucket < first || bucket > last {
            return;
        }

        let got = ring
            .at(bucket)
            .unwrap_or_else(|| panic!("bucket {bucket} is live and holds nothing"));

        assert_eq!(
            got.as_f64(),
            exp.value(agg),
            "bucket {bucket} ({}, {}): ring says {:?}, reference says {exp:?}",
            shape.resolution().map_or("base", Resolution::name),
            agg.name(),
            got
        );

        // A mean has to carry the right sample count too, or `xff` is judging on a number that drifted.
        if agg == Agg::Mean {
            assert_eq!(got.samples(), exp.count, "bucket {bucket} sample count");
        }

        checked += 1;
    };

    for i in 0..n {
        let ms = from_ms + i * MS_PER_SEC;
        let bucket = expected_bucket(shape.resolution().expect("an archive"), ms);
        let v = value_at(i);

        match &mut current {
            Some((b, exp)) if *b == bucket => exp.add(v),
            Some((b, exp)) => {
                flush(*b, exp);
                current = Some((bucket, Expected::new(v)));
            }
            None => current = Some((bucket, Expected::new(v))),
        }
    }

    if let Some((b, exp)) = &current {
        flush(*b, exp);
    }

    // Just below the window must read as nothing, whatever bytes are still in the slot.
    if first > i64::MIN + 1 {
        assert_eq!(ring.at(first - 1), None, "the bucket below the window must be unknown");
    }

    checked
}

//==========================================================

/// **The headline test**: millions of one-second samples through every resolution, each checked bucket by bucket.
#[test]
fn millions_of_samples_land_where_the_arithmetic_says_they_should() {
    let n = scale();
    let from = origin();

    println!(
        "\n  {n} one-second samples = {:.1} days, from 2026-01-01\n",
        (n * MS_PER_SEC) as f64 / MS_PER_DAY as f64
    );
    println!(
        "  {:>9} {:>7} {:>9} {:>10} {:>12} {:>9}",
        "archive", "slots", "live", "checked", "append s", "ns/sample"
    );

    let mut total_checked = 0i64;

    for archive in default_archives() {
        let cfg = one_archive(archive.resolution, archive.agg, archive.slots);
        let shape = shape_of(&cfg, archive.resolution);
        let mut bytes = ring::create(&shape);
        let mut ring = RingMut::open(&mut bytes, &shape).expect("its own bytes open");

        let secs = fill(&mut ring, n, from);
        let checked = verify(&ring, &shape, n, from, archive.agg);
        let (first, last) = ring.window().unwrap();

        println!(
            "  {:>9} {:>7} {:>9} {:>10} {:>11.3}s {:>9.0}",
            archive.resolution.name(),
            archive.slots,
            last - first + 1,
            checked,
            secs,
            secs * 1e9 / n as f64
        );

        assert!(checked > 0, "{} checked no buckets at all", archive.resolution.name());
        assert_eq!(ring.appends(), n, "{}: every append counted", archive.resolution.name());

        total_checked += checked;
    }

    println!();

    assert!(total_checked > 0);
}

/// Every aggregation, at a resolution coarse enough that each bucket holds thousands of samples.
#[test]
fn every_aggregation_is_exact_over_thousands_of_samples_per_bucket() {
    let n = scale().min(2_000_000);
    let from = origin();

    for agg in [Agg::Last, Agg::First, Agg::Min, Agg::Max, Agg::Sum, Agg::Count, Agg::Mean] {
        // Hourly buckets over one-second samples: 3,600 samples each, so `first` and `last` differ, `min` and `max` are
        // not the endpoints, and `sum` is far from any single value.
        let cfg = one_archive(Resolution::Hour, agg, 1_000);
        let shape = shape_of(&cfg, Resolution::Hour);
        let mut bytes = ring::create(&shape);
        let mut ring = RingMut::open(&mut bytes, &shape).expect("open");

        fill(&mut ring, n, from);

        let checked = verify(&ring, &shape, n, from, agg);

        assert!(checked > 100, "{}: only {checked} buckets checked", agg.name());
    }
}

/// **Lapping.** A small ring that goes round hundreds of thousands of times must never report a retired lap's value.
#[test]
fn lapping_a_small_ring_a_hundred_thousand_times_leaks_nothing() {
    let slots = 16i64;
    let n = scale();
    let from = origin();

    let cfg = one_archive(Resolution::Second, Agg::Last, slots);
    let shape = shape_of(&cfg, Resolution::Second);
    let mut bytes = ring::create(&shape);
    let mut ring = RingMut::open(&mut bytes, &shape).expect("open");

    // Checked repeatedly along the way rather than only at the end: a leak that self-corrects would otherwise pass.
    let checkpoints = 64i64;
    let every = (n / checkpoints).max(1);

    for i in 0..n {
        ring.append(from + i * MS_PER_SEC, Num::F(value_at(i))).unwrap();

        if i % every == 0 || i == n - 1 {
            let (first, last) = ring.window().unwrap();
            let newest = expected_bucket(Resolution::Second, from + i * MS_PER_SEC);

            assert_eq!(last, newest, "at sample {i}");
            assert_eq!(last - first + 1, slots, "the window is always exactly the ring");

            // Every bucket that has been *written* holds its own sample. Before the ring has filled once there are
            // fewer of those than there are slots — the first checkpoint is at sample 0, where exactly one bucket has a
            // value and the other fifteen are inside the window and legitimately unknown.
            let written = (i + 1).min(slots);

            for back in 0..written {
                let want = value_at(i - back);

                assert_eq!(
                    ring.at(last - back).map(Num::as_f64),
                    Some(want),
                    "at sample {i}, {back} buckets back"
                );
            }

            // And anything inside the window that was never written reads as unknown rather than as a stale lap.
            for back in written..slots {
                assert_eq!(
                    ring.at(last - back),
                    None,
                    "at sample {i}, {back} buckets back was never written"
                );
            }

            assert_eq!(ring.at(first - 1), None, "at sample {i}, one past the window");
        }
    }

    assert_eq!(ring.appends(), n);
    println!("\n  {n} samples through a {slots}-slot ring: {} laps\n", n / slots);
}

/// **Precision.** A mean is a running `f64` sum, so millions of additions accumulate error. How much?
#[test]
fn a_mean_over_millions_of_inexact_samples_stays_close() {
    let from = origin();

    // **One bucket for everything**, so the running sum takes every addition. A yearly archive fed one-second samples is
    // exactly this case in production, and the cap below is that case at its worst: 2026 is not a leap year, so one year
    // of per-second samples is 31,536,000 additions into a single slot and there cannot be more.
    //
    // The cap is why it is here. Asking for 40,000,000 spills into 2027's bucket, and the count assertion then failed
    // reporting 31,536,000 - the ring was right about the calendar and the test was wrong about its own premise.
    let year_seconds = (calendar::days_from_civil(2027, 1, 1) - calendar::days_from_civil(2026, 1, 1)) * 86_400;

    assert_eq!(year_seconds, 31_536_000, "2026 is a 365-day year");

    let n = scale().min(year_seconds);

    let cfg = one_archive(Resolution::Year, Agg::Mean, 25);
    let shape = shape_of(&cfg, Resolution::Year);

    println!("\n  {n} samples, all in one bucket\n");
    println!("  {:>26} {:>22} {:>22} {:>12}", "values", "ring mean", "compensated mean", "rel. error");

    for (label, f) in [
        // 0.1 is the classic: no exact binary representation, so every addition rounds.
        ("0.1 each", (|_| 0.1) as fn(i64) -> f64),
        // A ramp, so the running sum grows large and later additions lose more of each addend's low bits.
        ("i * 0.1", |i: i64| i as f64 * 0.1),
        // Alternating magnitudes: the case naive summation is worst at.
        ("1e9 and 1e-9", |i: i64| if i % 2 == 0 { 1e9 } else { 1e-9 }),
    ] {
        let mut bytes = ring::create(&shape);
        let mut ring = RingMut::open(&mut bytes, &shape).expect("open");

        // Neumaier compensated summation as the reference - the same algorithm the `summary` example's exact sum uses.
        let mut sum = 0.0f64;
        let mut comp = 0.0f64;

        for i in 0..n {
            let v = f(i);

            ring.append(from + i * MS_PER_SEC, Num::F(v)).unwrap();

            let t = sum + v;

            comp += if sum.abs() >= v.abs() {
                (sum - t) + v
            } else {
                (v - t) + sum
            };
            sum = t;
        }

        let exact = (sum + comp) / n as f64;
        let got = ring.at(shape.bucket_of(from)).expect("one bucket").as_f64();
        let rel = ((got - exact) / exact).abs();

        println!("  {label:>26} {got:>22.12e} {exact:>22.12e} {rel:>12.3e}");

        // A naive `f64` sum of n terms has a worst case around n·ε, and ε is 2.2e-16. Asserting a bound two orders
        // above that is generous enough not to be flaky and tight enough to catch a real regression - like a slot that
        // stopped accumulating, or a count that drifted from the sum.
        assert!(
            rel < 1e-9,
            "{label}: relative error {rel:.3e} is larger than a naive sum should manage"
        );
        assert_eq!(ring.at(shape.bucket_of(from)).unwrap().samples(), n, "{label}: sample count");
    }

    println!(
        "\n  A running f64 sum, so this is naive-summation error and it grows with the sample count. Exact for\n  \
         integer-valued samples, which is why every other test here uses them.\n"
    );
}

/// A counter driven through millions of readings, wraps and resets — the deltas have to add up.
#[test]
fn a_counter_over_millions_of_readings_adds_up() {
    let n = scale().min(2_000_000);
    let from = origin();

    let mut cfg = Config::new(MS_PER_SEC, 3_600, 0).unwrap();

    cfg.kind = Kind::I64;
    cfg.mode = Mode::Counter;
    cfg.set_archives(&[Archive::new(Resolution::Second, Agg::Sum, 3_600)])
        .unwrap();

    let shape = shape_of(&cfg, Resolution::Second);
    let mut bytes = ring::create(&shape);
    let mut ring = RingMut::open(&mut bytes, &shape).expect("open");

    // A counter that climbs by a varying amount and restarts from zero every so often. Both are ordinary: a process
    // restarts, and its counter begins again.
    let restart_every = 100_000i64;
    let mut reading = 0i64;
    let mut events = [0i64; 5];
    let mut expected_resets = 0i64;
    // The deltas the ring should hold, summed by the test itself.
    let mut expected_total = 0i64;
    let live_from = n - 3_600;

    for i in 0..n {
        if i % restart_every == 0 && i > 0 {
            reading = 0;
            expected_resets += 1;
        } else {
            reading += (i % 7) + 1;
        }

        let applied = ring.append(from + i * MS_PER_SEC, Num::I(reading)).unwrap();
        let event = applied.counter.expect("a counter ring reports an event");

        events[match event {
            CounterEvent::Baseline => 0,
            CounterEvent::Delta => 1,
            CounterEvent::Wrapped => 2,
            CounterEvent::Reset => 3,
            CounterEvent::Stale => 4,
        }] += 1;

        // Accumulate what the live window should contain.
        if i >= live_from {
            if let Some(Num::I(d)) = applied.stored {
                expected_total += d;
            }
        }
    }

    println!(
        "\n  {n} counter readings, restarting every {restart_every}: \
         baseline={} delta={} wrapped={} reset={} stale={}\n",
        events[0], events[1], events[2], events[3], events[4]
    );

    assert_eq!(events[0], 1, "exactly one baseline, at the first reading");
    assert_eq!(events[2], 0, "no wrap was declared, so nothing may be called one");
    assert_eq!(events[4], 0, "no heartbeat was set, so nothing is stale");
    assert_eq!(events[3], expected_resets, "one reset per restart");
    assert_eq!(events[1], n - 1 - expected_resets, "everything else is a delta");
    assert_eq!(ring.resets() as i64, expected_resets, "and the ring counted them");

    // Sum the live window out of the ring and compare with what the test accumulated.
    let (first, last) = ring.window().unwrap();
    let mut got = 0i64;

    for bucket in first..=last {
        if let Some(Num::I(v)) = ring.at(bucket) {
            got += v;
        }
    }

    assert_eq!(got, expected_total, "the live window's deltas");
    assert!(!ring.saturated(), "nothing here comes close to i64::MAX");
}

/// A declared 32-bit counter wrapping thousands of times: every wrap reconstructed, nothing called a reset.
#[test]
fn a_wrapping_counter_reconstructs_every_lap() {
    let n = scale().min(500_000);
    let from = origin();
    let modulus = 1i64 << 32;

    let mut cfg = Config::new(MS_PER_SEC, 3_600, 0).unwrap();

    cfg.kind = Kind::I64;
    cfg.mode = Mode::Counter;
    cfg.wrap_bits = 32;
    cfg.set_archives(&[Archive::new(Resolution::Second, Agg::Sum, 3_600)])
        .unwrap();

    let shape = shape_of(&cfg, Resolution::Second);
    let mut bytes = ring::create(&shape);
    let mut ring = RingMut::open(&mut bytes, &shape).expect("open");

    // A step that is a large fraction of the width, so a wrap happens every few readings - but under half the width, so
    // every one of them is a *plausible* wrap. Above half, the reconstruction is deliberately refused as a reset.
    let step = modulus / 3;
    let mut raw = 0i64;
    let mut wraps = 0i64;

    for i in 0..n {
        let next = (raw + step) % modulus;
        let wrapped = next < raw;

        raw = next;

        let applied = ring.append(from + i * MS_PER_SEC, Num::I(raw)).unwrap();

        match applied.counter {
            Some(CounterEvent::Wrapped) => {
                wraps += 1;

                assert!(wrapped, "reading {i} was called a wrap and did not decrease");
                assert_eq!(applied.stored, Some(Num::I(step)), "a reconstructed wrap is still one step");
            }
            Some(CounterEvent::Delta) => {
                assert!(!wrapped, "reading {i} decreased and was called an ordinary delta");
                assert_eq!(applied.stored, Some(Num::I(step)));
            }
            Some(CounterEvent::Baseline) => assert_eq!(i, 0),
            // A reset here would mean a lap the reconstruction refused, which is the failure this test exists to catch.
            other => panic!("reading {i} ({raw}) came back as {other:?}"),
        }
    }

    println!("\n  {n} readings stepping {step} through a 32-bit counter: {wraps} wraps reconstructed\n");

    assert!(wraps > n / 4, "a step of a third of the width should wrap about a third of the time");
    assert_eq!(ring.resets(), 0, "not one of those laps was recorded as a reset");
}

/// **Calendar correctness at scale**: three years of hourly samples, every month and week bucket verified.
///
/// Three years so it spans a leap year, and hourly rather than per-second so three years is affordable.
#[test]
fn three_years_of_samples_bucket_by_the_calendar() {
    // 2024 is a leap year, so this run contains a 29-day February.
    //
    // The span is **exactly** three calendar years — 1,096 days, not 3 × 366 — so "thirty-six months" and "three years"
    // become real assertions about the calendar closing. The first version used 3 × 366 days, which overshoots into
    // January 2027 and produced 37 months; the ring was right and the claim was wrong.
    let first_day = calendar::days_from_civil(2024, 1, 1);
    let days = calendar::days_from_civil(2027, 1, 1) - first_day;
    let from = first_day * MS_PER_DAY;
    let hours = days * 24;

    assert_eq!(days, 1_096, "2024 is a leap year, so three years from 2024-01-01 is 366 + 365 + 365");

    for (res, agg, slots) in [
        (Resolution::Day, Agg::Mean, 1_200),
        (Resolution::Week, Agg::Mean, 200),
        (Resolution::Month, Agg::Mean, 48),
        (Resolution::Year, Agg::Mean, 5),
    ] {
        let cfg = one_archive(res, agg, slots);
        let shape = shape_of(&cfg, res);
        let mut bytes = ring::create(&shape);
        let mut ring = RingMut::open(&mut bytes, &shape).expect("open");

        // An independent accumulator keyed by the reference bucket. Small enough to hold outright at this resolution,
        // so this one does not depend on the input being monotone.
        let mut reference: BTreeMap<i64, Expected> = BTreeMap::new();

        for h in 0..hours {
            let ms = from + h * MS_PER_HOUR;
            let v = value_at(h);

            ring.append(ms, Num::F(v)).unwrap();

            reference
                .entry(expected_bucket(res, ms))
                .and_modify(|e| e.add(v))
                .or_insert_with(|| Expected::new(v));
        }

        let (first, last) = ring.window().unwrap();
        let mut checked = 0i64;

        for (bucket, exp) in &reference {
            if *bucket < first || *bucket > last {
                continue;
            }

            let got = ring.at(*bucket).unwrap_or_else(|| panic!("{} bucket {bucket} empty", res.name()));

            assert_eq!(got.as_f64(), exp.value(agg), "{} bucket {bucket}", res.name());
            assert_eq!(got.samples(), exp.count, "{} bucket {bucket} count", res.name());

            checked += 1;
        }

        // The bucket count is itself a claim about the calendar: 36 months, 3 or 4 years, and 1,096 days for a span
        // containing one leap year.
        let live: Vec<i64> = reference.keys().copied().filter(|b| *b >= first && *b <= last).collect();

        assert_eq!(checked, live.len() as i64);
        assert!(checked > 0, "{} checked nothing", res.name());

        match res {
            // Thirty-six months over 1,096 days: the months are 28 to 31 days long and they still close exactly, which
            // a fixed 30-day step could not manage.
            Resolution::Month => assert_eq!(checked, 36, "three calendar years is thirty-six months"),
            Resolution::Day => assert_eq!(checked, days, "one bucket per day of the span"),
            Resolution::Year => assert_eq!(checked, 3, "2024, 2025 and 2026"),
            _ => {}
        }

        // Every bucket's start must be the containing period's start, and consecutive buckets must be adjacent - which
        // is what a fixed 30-day month would break.
        for pair in live.windows(2) {
            assert_eq!(pair[1], pair[0] + 1, "{} buckets must be consecutive", res.name());

            let gap = shape.bucket_start_ms(pair[1]) - shape.bucket_start_ms(pair[0]);

            match res {
                Resolution::Day => assert_eq!(gap, MS_PER_DAY),
                Resolution::Week => assert_eq!(gap, 7 * MS_PER_DAY),
                // The whole point: a month is 28 to 31 days and each one is exactly its own length.
                Resolution::Month => assert!(
                    (28..=31).contains(&(gap / MS_PER_DAY)),
                    "month gap was {} days",
                    gap / MS_PER_DAY
                ),
                Resolution::Year => assert!(
                    matches!(gap / MS_PER_DAY, 365 | 366),
                    "year gap was {} days",
                    gap / MS_PER_DAY
                ),
                _ => {}
            }
        }

        println!("  {:>6}: {checked} buckets verified", res.name());
    }

    println!();
}

/// **Order independence.** Shuffling the samples inside a window must not change an order-free aggregation's answer.
#[test]
fn out_of_order_arrival_reaches_the_same_answer() {
    let n = 200_000i64;
    let from = origin();

    for agg in [Agg::Min, Agg::Max, Agg::Sum, Agg::Count, Agg::Mean] {
        let cfg = one_archive(Resolution::Hour, agg, 100);
        let shape = shape_of(&cfg, Resolution::Hour);

        // In order.
        let mut ordered_bytes = ring::create(&shape);
        let mut ordered = RingMut::open(&mut ordered_bytes, &shape).expect("open");

        for i in 0..n {
            ordered.append(from + i * MS_PER_SEC, Num::F(value_at(i))).unwrap();
        }

        // Backfilled: walk the hours forward but scatter the samples inside each hour, so every append is either a new
        // bucket or a backfill into a live one. A deterministic scramble - no RNG, so a failure reproduces.
        let mut scrambled_bytes = ring::create(&shape);
        let mut scrambled = RingMut::open(&mut scrambled_bytes, &shape).expect("open");
        let per_hour = 3_600i64;
        let hours = n / per_hour;

        for h in 0..hours {
            for k in 0..per_hour {
                // A stride coprime with 3,600 visits every second of the hour in a scattered order.
                let i = h * per_hour + (k * 1_009) % per_hour;

                scrambled
                    .append(from + i * MS_PER_SEC, Num::F(value_at(i)))
                    .unwrap();
            }
        }

        // Compare only the hours the scrambled run actually covered.
        let (first, last) = scrambled.window().unwrap();
        let mut compared = 0i64;

        for bucket in first..=last {
            let a = ordered.at(bucket).map(Num::as_f64);
            let b = scrambled.at(bucket).map(Num::as_f64);

            if b.is_none() {
                continue;
            }

            assert_eq!(a, b, "{}: bucket {bucket} differs by arrival order", agg.name());

            compared += 1;
        }

        assert!(compared > 10, "{}: only {compared} buckets compared", agg.name());
    }
}

/// `xff` at scale: a bucket crosses the threshold exactly when its sample count does.
#[test]
fn xff_crosses_exactly_at_the_threshold() {
    let from = origin();
    let mut hourly = Archive::new(Resolution::Hour, Agg::Mean, 24);

    hourly.xff_pct = 50;

    let mut cfg = Config::new(MS_PER_SEC, 3_600, 0).unwrap();

    // The finest archive steps in seconds, so an hourly bucket could hold 3,600 and half of that is 1,800.
    cfg.set_archives(&[Archive::new(Resolution::Second, Agg::Last, 3_600), hourly])
        .unwrap();

    let shape = shape_of(&cfg, Resolution::Hour);

    assert_eq!(shape.expected_samples(), 3_600);

    let mut bytes = ring::create(&shape);
    let mut ring = RingMut::open(&mut bytes, &shape).expect("open");
    let bucket = shape.bucket_of(from);

    for i in 0..3_600i64 {
        ring.append(from + i * MS_PER_SEC, Num::F(1.0)).unwrap();

        let reported = ring.at(bucket).is_some();
        let stored = ring.at_unfiltered(bucket).expect("something is always stored");

        assert_eq!(stored.samples(), i + 1, "sample count after {} appends", i + 1);
        assert_eq!(
            reported,
            i + 1 >= 1_800,
            "after {} of 3600 samples, xff=50 should {} report",
            i + 1,
            if i + 1 >= 1_800 { "" } else { "not" }
        );
    }
}

/// Every slot of a large ring, written and read — so no slot is unreachable and none aliases another.
#[test]
fn every_slot_of_a_large_ring_is_reachable_and_distinct() {
    let slots = 65_536i64;
    let cfg = one_archive(Resolution::Second, Agg::Last, slots);
    let shape = shape_of(&cfg, Resolution::Second);
    let mut bytes = ring::create(&shape);
    let mut ring = RingMut::open(&mut bytes, &shape).expect("open");
    let from = origin();

    // A distinct value per bucket, so aliasing shows up as a wrong value rather than as nothing.
    for i in 0..slots {
        ring.append(from + i * MS_PER_SEC, Num::F(i as f64)).unwrap();
    }

    assert_eq!(ring.window(), Some((shape.bucket_of(from), shape.bucket_of(from) + slots - 1)));

    for i in 0..slots {
        assert_eq!(
            ring.at(shape.bucket_of(from) + i).map(Num::as_f64),
            Some(i as f64),
            "slot for bucket offset {i}"
        );
    }

    // One more lap, replacing every slot exactly once.
    for i in slots..2 * slots {
        ring.append(from + i * MS_PER_SEC, Num::F(-(i as f64))).unwrap();
    }

    for i in slots..2 * slots {
        assert_eq!(
            ring.at(shape.bucket_of(from) + i).map(Num::as_f64),
            Some(-(i as f64)),
            "second lap, bucket offset {i}"
        );
    }

    // And nothing from the first lap survived.
    for i in 0..slots {
        assert_eq!(ring.at(shape.bucket_of(from) + i), None, "first lap bucket {i} must be gone");
    }
}

/// An integer ring summing into saturation, and saying so.
#[test]
fn an_integer_sum_saturates_visibly_rather_than_wrapping() {
    let cfg = {
        let mut c = Config::new(MS_PER_SEC, 3_600, 0).unwrap();

        c.kind = Kind::I64;
        c.set_archives(&[Archive::new(Resolution::Year, Agg::Sum, 5)]).unwrap();

        c
    };

    let shape = shape_of(&cfg, Resolution::Year);
    let mut bytes = ring::create(&shape);
    let mut ring = RingMut::open(&mut bytes, &shape).expect("open");
    let from = origin();
    let big = i64::MAX / 3;
    let bucket = shape.bucket_of(from);

    // **`i64::MAX / 3` truncates**, so three of them come to `i64::MAX - 1` and fit. This assertion first expected
    // saturation on the third append and the ring correctly reported `MAX - 1`; the test's arithmetic was the wrong part.
    assert_eq!(3 * big, i64::MAX - 1);

    for i in 0..3i64 {
        ring.append(from + i * MS_PER_SEC, Num::I(big)).unwrap();
    }

    assert_eq!(ring.at(bucket), Some(Num::I(i64::MAX - 1)), "exact, right up to the ceiling");
    assert!(!ring.saturated(), "one short of overflowing is not overflowing");

    // The fourth is what does not fit.
    ring.append(from + 3 * MS_PER_SEC, Num::I(big)).unwrap();

    assert_eq!(
        ring.at(bucket),
        Some(Num::I(i64::MAX)),
        "clamped at the ceiling, not wrapped to a negative number"
    );
    assert!(ring.saturated(), "and the ring records that it happened");

    // Once saturated it stays saturated rather than drifting further.
    for i in 4..12i64 {
        ring.append(from + i * MS_PER_SEC, Num::I(big)).unwrap();
    }

    assert_eq!(ring.at(bucket), Some(Num::I(i64::MAX)));
}
