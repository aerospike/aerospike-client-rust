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

//! The arithmetic and the layouts, which is everything that needs no record.
//!
//! `ts_report` and `ts_stats` build result maps through the host, so they are tested against a real server instead.
//! What is here is the part where being wrong is silent: slot arithmetic, the retire-on-advance, counter resets, and
//! the four refusals that protect a ring from a config that changed underneath it.

use crate::config::{Agg, Config, Kind, Mode, Shape, MAX_SLOTS};
use crate::ring::{self, CounterEvent, Num, Outcome, RingMut, RingView, NO_BUCKET};

const STEP: i64 = 60_000;
const SLOTS: i64 = 10;

fn cfg() -> Config {
    Config::new(STEP, SLOTS, 0).expect("a valid default shape")
}

/// The single-ring shape of a config, which is what most of these tests exercise.
fn base(cfg: &Config) -> Shape {
    Shape::base(cfg, cfg.fingerprint_of())
}

/// A ring's bytes, owned by the test so it has something to lend.
///
/// In a UDF these bytes belong to the bin and `RingMut` borrows them in place; a unit test has no bin, so it holds the
/// buffer itself. The borrowing is the same either way, which is the point of `RingMut` taking a slice.
fn bytes_of(cfg: &Config) -> Vec<u8> {
    ring::create(&base(cfg))
}

fn ring_in<'a>(bytes: &'a mut [u8], cfg: &Config) -> RingMut<'a> {
    RingMut::open(bytes, &base(cfg)).expect("its own bytes must open")
}

fn at(ring: &RingMut<'_>, bucket: i64) -> Option<f64> {
    ring.at(bucket).map(Num::as_f64)
}

//==========================================================
// The config bin.
//

#[test]
fn a_config_survives_the_round_trip_with_every_field_set() {
    let original = Config {
        agg: Agg::Max,
        kind: Kind::I64,
        mode: Mode::Counter,
        step_ms: 1_000,
        slots: 3_600,
        created_ms: 1_767_225_600_000,
        allow_backfill: false,
        allow_wipe: true,
        wrap_bits: 32,
        archive_table: [crate::config::Archive::EMPTY; crate::config::MAX_ARCHIVES],
        n_archives: 0,
        heartbeat_ms: 0,
    };

    let decoded = Config::decode(&original.encode()).expect("its own bytes must decode");

    assert_eq!(decoded, original);
}

#[test]
fn a_config_bin_that_is_not_one_is_refused() {
    assert!(Config::decode(&[]).is_err(), "empty");
    assert!(Config::decode(&[0u8; 40]).is_err(), "all zeroes has no magic");

    let mut bytes = cfg().encode();

    bytes[0] = b'X';
    assert!(Config::decode(&bytes).is_err(), "wrong magic");

    let mut bytes = cfg().encode();

    bytes[4] = 99;
    assert!(Config::decode(&bytes).is_err(), "a layout version this build does not know");

    let mut bytes = cfg().encode();

    bytes[8..16].copy_from_slice(&0i64.to_le_bytes());
    assert!(Config::decode(&bytes).is_err(), "a zero step would divide by zero");

    let mut bytes = cfg().encode();

    bytes[16..24].copy_from_slice(&(MAX_SLOTS + 1).to_le_bytes());
    assert!(Config::decode(&bytes).is_err(), "slots past the ceiling");

    let mut bytes = cfg().encode();

    bytes[33] = 17;
    assert!(Config::decode(&bytes).is_err(), "a wrap width that is not 0, 32 or 64");
}

/// **The check that makes an edited config an error rather than a reinterpretation.**
#[test]
fn changing_any_field_changes_the_fingerprint() {
    let base = cfg();
    let baseline = base.fingerprint_of();

    let mut faster = base;
    faster.step_ms = 1_000;

    let mut wider = base;
    wider.slots = 20;

    let mut summed = base;
    summed.agg = Agg::Sum;

    for (label, other) in [("step", faster), ("slots", wider), ("agg", summed)] {
        assert_ne!(
            other.fingerprint_of(),
            baseline,
            "changing {label} has to change the fingerprint, or a ring cannot tell it happened"
        );
    }
}

#[test]
fn a_ring_refuses_a_config_it_was_not_built_against() {
    let original = cfg();
    let bytes = bytes_of(&original);

    // Same slot count and same aggregation, so the length matches and every byte still decodes - only the fingerprint
    // disagrees. This is the case that would otherwise be silent.
    let mut edited = original;
    edited.allow_backfill = !original.allow_backfill;

    let err = RingView::open(&bytes, &base(&edited))
        .expect_err("a ring built against a different config must be refused");

    assert!(
        format!("{err}").contains("fingerprint"),
        "the error should say what is wrong: {err}"
    );

    // A config whose slots changed fails on length first, with its own message.
    let mut resized = original;
    resized.slots = 20;

    let err = RingView::open(&bytes, &base(&resized)).expect_err("length");

    assert!(format!("{err}").contains("slots"), "{err}");

    // And one whose aggregation changed is caught by the ring's own header, before the fingerprint is even reached -
    // which is what makes a ring self-describing rather than dependent on the caller getting the config right.
    let mut reaggregated = original;
    reaggregated.agg = Agg::Max;

    let err = RingView::open(&bytes, &base(&reaggregated)).expect_err("aggregation");

    assert!(format!("{err}").contains("aggregates with"), "{err}");

    // A bin holding something else entirely.
    assert!(RingView::open(&[0u8; 120], &base(&original)).is_err());
}

//==========================================================
// Slot arithmetic.
//

/// A timestamp before 1970 has to floor, not truncate toward zero.
#[test]
fn buckets_floor_rather_than_truncate() {
    let mut ring_bytes = bytes_of(&cfg());
    let ring = ring_in(&mut ring_bytes, &cfg());

    assert_eq!(ring.bucket_of(0), 0);
    assert_eq!(ring.bucket_of(STEP - 1), 0);
    assert_eq!(ring.bucket_of(STEP), 1);

    // `-1 / 60000` truncates to 0, which would put a pre-epoch instant in the epoch's own bucket.
    assert_eq!(ring.bucket_of(-1), -1, "truncation would give 0 here");
    assert_eq!(ring.bucket_of(-STEP), -1);
    assert_eq!(ring.bucket_of(-STEP - 1), -2);

    // And the slot index must stay non-negative for a negative bucket.
    let mut ring_bytes = bytes_of(&cfg());
    let mut ring = ring_in(&mut ring_bytes, &cfg());

    ring.append(-STEP - 1, Num::F(7.0)).expect("a pre-epoch sample is still a sample");

    assert_eq!(at(&ring, -2), Some(7.0));
}

#[test]
fn a_fresh_ring_is_entirely_unknown() {
    let mut ring_bytes = bytes_of(&cfg());
    let ring = ring_in(&mut ring_bytes, &cfg());

    assert_eq!(ring.last_bucket(), NO_BUCKET);
    assert_eq!(ring.window(), None, "an empty ring has no window, not a window of zeroes");
    assert_eq!(ring.appends(), 0);

    for bucket in -5..15 {
        assert_eq!(ring.at(bucket), None, "bucket {bucket}");
    }
}

//==========================================================
// Placement.
//

#[test]
fn samples_land_in_the_bucket_their_timestamp_names() {
    let mut ring_bytes = bytes_of(&cfg());
    let mut ring = ring_in(&mut ring_bytes, &cfg());

    for i in 0..5i64 {
        let applied = ring.append(i * STEP, Num::F(i as f64)).unwrap();

        assert_eq!(applied.outcome, Outcome::Placed, "sample {i}");
        assert_eq!(applied.bucket, i);
    }

    assert_eq!(ring.last_bucket(), 4);
    assert_eq!(ring.window(), Some((-5, 4)));
    assert_eq!(ring.appends(), 5);

    for i in 0..5i64 {
        assert_eq!(at(&ring, i), Some(i as f64));
    }
}

#[test]
fn a_second_sample_in_the_same_bucket_combines() {
    let mut ring_bytes = bytes_of(&cfg());
    let mut ring = ring_in(&mut ring_bytes, &cfg());

    ring.append(0, Num::F(1.0)).unwrap();

    let applied = ring.append(STEP - 1, Num::F(9.0)).unwrap();

    assert_eq!(applied.outcome, Outcome::Combined);
    assert_eq!(at(&ring, 0), Some(9.0), "the default agg is last");
    assert_eq!(ring.last_bucket(), 0, "combining does not move the position");
}

#[test]
fn advancing_retires_exactly_the_buckets_it_passed() {
    let mut ring_bytes = bytes_of(&cfg());
    let mut ring = ring_in(&mut ring_bytes, &cfg());

    for i in 0..5i64 {
        ring.append(i * STEP, Num::F(10.0 + i as f64)).unwrap();
    }

    // Jump from bucket 4 to bucket 8: buckets 5, 6 and 7 are retired, and 8 is written.
    let applied = ring.append(8 * STEP, Num::F(99.0)).unwrap();

    assert_eq!(applied.outcome, Outcome::Placed);
    assert_eq!(applied.cleared, 3, "buckets 5, 6, 7");
    assert_eq!(at(&ring, 8), Some(99.0));

    for retired in 5..8 {
        assert_eq!(at(&ring, retired), None, "bucket {retired} was skipped, so it is unknown");
    }

    // The older samples that are still inside the window survived.
    for i in 0..5i64 {
        assert_eq!(at(&ring, i), Some(10.0 + i as f64), "bucket {i} should be untouched");
    }
}

/// A slot recycled by a full lap must not still hold the previous lap's value.
#[test]
fn a_recycled_slot_does_not_leak_the_previous_lap() {
    let mut ring_bytes = bytes_of(&cfg());
    let mut ring = ring_in(&mut ring_bytes, &cfg());

    ring.append(0, Num::F(1.0)).unwrap();

    // Bucket 0 and bucket SLOTS share a slot. Walk there one bucket at a time so nothing is wiped.
    for b in 1..=SLOTS {
        ring.append(b * STEP, Num::F(100.0 + b as f64)).unwrap();
    }

    assert_eq!(at(&ring, SLOTS), Some(100.0 + SLOTS as f64));
    assert_eq!(
        ring.at(0),
        None,
        "bucket 0 is outside the window now and must not report the value still in its slot"
    );
}

#[test]
fn a_sample_older_than_the_window_is_refused_and_changes_nothing() {
    let mut ring_bytes = bytes_of(&cfg());
    let mut ring = ring_in(&mut ring_bytes, &cfg());

    for b in 0..=SLOTS {
        ring.append(b * STEP, Num::F(b as f64)).unwrap();
    }

    let before = ring.bytes().to_vec();
    let applied = ring.append(0, Num::F(1234.0)).unwrap();

    assert_eq!(applied.outcome, Outcome::TooOld);
    assert!(!applied.outcome.accepted());
    assert_eq!(ring.bytes(), &before[..], "a refused append must not touch a single byte");
}

#[test]
fn backfill_merges_into_an_older_live_bucket() {
    let mut ring_bytes = bytes_of(&cfg());
    let mut ring = ring_in(&mut ring_bytes, &cfg());

    ring.append(0, Num::F(1.0)).unwrap();
    ring.append(3 * STEP, Num::F(4.0)).unwrap();

    let applied = ring.append(STEP, Num::F(2.0)).unwrap();

    assert_eq!(applied.outcome, Outcome::Backfilled);
    assert_eq!(at(&ring, 1), Some(2.0));
    assert_eq!(ring.last_bucket(), 3, "backfill does not move the position backwards");
}

#[test]
fn backfill_can_be_switched_off() {
    let mut shape = cfg();
    shape.allow_backfill = false;

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(3 * STEP, Num::F(4.0)).unwrap();

    let before = ring.bytes().to_vec();
    let applied = ring.append(STEP, Num::F(2.0)).unwrap();

    assert_eq!(applied.outcome, Outcome::BackfillRefused);
    assert_eq!(ring.bytes(), &before[..]);
}

/// **The seconds-versus-milliseconds guard.**
#[test]
fn an_append_that_would_erase_everything_is_refused_by_default() {
    let mut ring_bytes = bytes_of(&cfg());
    let mut ring = ring_in(&mut ring_bytes, &cfg());

    for b in 0..5i64 {
        ring.append(b * STEP, Num::F(b as f64)).unwrap();
    }

    let before = ring.bytes().to_vec();

    // A timestamp in microseconds instead of milliseconds: a gap of a thousand windows.
    let applied = ring.append(4 * STEP * 1000, Num::F(7.0)).unwrap();

    assert_eq!(applied.outcome, Outcome::WouldWipe);
    assert_eq!(
        &ring_bytes[..],
        &before[..],
        "the whole point is that the data is still there to be salvaged"
    );

    // The same append against a ring that opted in.
    let mut permissive = cfg();
    permissive.allow_wipe = true;

    let mut ring_bytes = bytes_of(&permissive);
    let mut ring = ring_in(&mut ring_bytes, &permissive);

    for b in 0..5i64 {
        ring.append(b * STEP, Num::F(b as f64)).unwrap();
    }

    let applied = ring.append(4 * STEP * 1000, Num::F(7.0)).unwrap();

    assert_eq!(applied.outcome, Outcome::Placed);
    assert_eq!(applied.cleared, SLOTS - 1, "clearing is capped at the ring size");
    assert_eq!(ring.window(), Some((4000 - SLOTS + 1, 4000)));

    for b in 0..5i64 {
        assert_eq!(ring.at(b), None, "everything old is gone, as advertised");
    }
}

//==========================================================
// Aggregation within a bucket.
//

#[test]
fn every_aggregation_mode_does_what_it_says() {
    let cases: [(Agg, f64); 6] = [
        (Agg::Last, 3.0),
        (Agg::First, 5.0),
        (Agg::Min, 1.0),
        (Agg::Max, 5.0),
        (Agg::Sum, 9.0),
        (Agg::Count, 3.0),
    ];

    for (agg, expected) in cases {
        let mut shape = cfg();
        shape.agg = agg;

        let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

        // Same bucket, three times, in this order.
        for v in [5.0, 1.0, 3.0] {
            ring.append(0, Num::F(v)).unwrap();
        }

        assert_eq!(at(&ring, 0), Some(expected), "agg {}", agg.name());
    }
}

#[test]
fn count_downsamples_by_summing() {
    // Counting counts again would say "two buckets", when what is wanted is "seven samples".
    assert_eq!(Agg::Count.downsample_with(), Agg::Sum);

    for agg in [Agg::Last, Agg::First, Agg::Min, Agg::Max, Agg::Sum] {
        assert_eq!(agg.downsample_with(), agg, "{} combines with itself", agg.name());
    }
}

#[test]
fn an_integer_ring_stays_exact_and_saturates_visibly() {
    let mut shape = cfg();

    shape.kind = Kind::I64;
    shape.agg = Agg::Sum;

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    // Above 2^53, so an f64 ring would already have lost the low bit of each of these.
    let base = (1i64 << 53) + 1;

    ring.append(0, Num::I(base)).unwrap();
    ring.append(0, Num::I(base)).unwrap();

    assert_eq!(ring.at(0), Some(Num::I(2 * base)), "exact, not nearly");
    assert!(!ring.saturated());

    ring.append(0, Num::I(i64::MAX)).unwrap();

    assert_eq!(ring.at(0), Some(Num::I(i64::MAX)), "saturated rather than wrapped negative");
    assert!(ring.saturated(), "and it says so");
}

#[test]
fn a_ring_refuses_the_wrong_sample_width() {
    let mut shape = cfg();
    shape.kind = Kind::I64;

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    assert!(
        ring.append(0, Num::F(1.5)).is_err(),
        "truncating 1.5 into an integer ring silently is exactly what not to do"
    );
}

//==========================================================
// Counters.
//

#[test]
fn a_counters_first_reading_is_a_baseline_and_not_a_rate() {
    let mut shape = cfg();

    shape.kind = Kind::I64;
    shape.mode = Mode::Counter;
    shape.agg = Agg::Sum;

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);
    let applied = ring.append(0, Num::I(1_000)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Baseline));
    assert_eq!(applied.outcome, Outcome::Placed);
    assert_eq!(ring.at(0), None, "one reading cannot be a delta, so the slot stays unknown");

    let applied = ring.append(STEP, Num::I(1_250)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Delta));
    assert_eq!(ring.at(1), Some(Num::I(250)));
}

#[test]
fn a_counter_going_backwards_is_a_reset_rather_than_a_huge_rate() {
    let mut shape = cfg();

    shape.kind = Kind::I64;
    shape.mode = Mode::Counter;

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(0, Num::I(5_000)).unwrap();
    ring.append(STEP, Num::I(5_500)).unwrap();

    assert_eq!(ring.at(1), Some(Num::I(500)));

    // The process restarted and the counter began again at 12.
    let applied = ring.append(2 * STEP, Num::I(12)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Reset));
    assert_eq!(ring.at(2), None, "unknown, not -5488 and not 12");
    assert_eq!(ring.resets(), 1, "and the reset is counted rather than smoothed away");

    // The reading after a reset measures from the new baseline.
    let applied = ring.append(3 * STEP, Num::I(112)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Delta));
    assert_eq!(ring.at(3), Some(Num::I(100)));
}

#[test]
fn a_declared_wrap_width_reconstructs_the_delta() {
    let mut shape = cfg();

    shape.kind = Kind::I64;
    shape.mode = Mode::Counter;
    shape.wrap_bits = 32;

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);
    let ceiling = 1i64 << 32;

    ring.append(0, Num::I(ceiling - 100)).unwrap();

    let applied = ring.append(STEP, Num::I(50)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Wrapped));
    assert_eq!(ring.at(1), Some(Num::I(150)), "100 to the ceiling and 50 past it");
    assert_eq!(ring.resets(), 0, "a wrap is not a reset");

    // A decrease between values that do *not* both fit 32 bits is not a 32-bit wrap.
    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(0, Num::I(ceiling + 1_000)).unwrap();

    let applied = ring.append(STEP, Num::I(5)).unwrap();

    assert_eq!(
        applied.counter,
        Some(CounterEvent::Reset),
        "these readings are too big to be a 32-bit counter, so the decrease is not a wrap"
    );
}

/// **A declared wrap width must not turn every reset into a four-billion spike.**
///
/// The first version of `counter_delta` accepted any decrease between two 32-bit-representable readings as a wrap, so
/// a process restarting from 1,250 to 7 produced a delta of 4,294,966,053 — the exact spurious rate the counter mode
/// exists to avoid, and declaring a width made it *worse* than leaving it at zero. A live test caught it.
#[test]
fn a_low_counter_restarting_is_a_reset_and_not_a_wrap() {
    let mut shape = cfg();

    shape.kind = Kind::I64;
    shape.mode = Mode::Counter;
    shape.wrap_bits = 32;

    let ceiling = 1i64 << 32;

    // A restart: both readings are small, so the "wrapped" delta would be nearly the whole width.
    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(0, Num::I(1_250)).unwrap();

    let applied = ring.append(STEP, Num::I(7)).unwrap();

    assert_eq!(
        applied.counter,
        Some(CounterEvent::Reset),
        "1250 -> 7 is a restart; calling it a wrap would store a delta of {}",
        7 + ceiling - 1_250
    );
    assert_eq!(ring.at(1), None, "and unknown is the honest value");
    assert_eq!(ring.resets(), 1);

    // A real wrap: near the ceiling, rolling just past zero, so the delta is small.
    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(0, Num::I(ceiling - 3)).unwrap();

    let applied = ring.append(STEP, Num::I(4)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Wrapped));
    assert_eq!(ring.at(1), Some(Num::I(7)), "3 to the ceiling and 4 past it");

    // The dividing line, stated: exactly half the width is not plausible, just under it is.
    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(0, Num::I(ceiling - 1)).unwrap();

    let applied = ring.append(STEP, Num::I(ceiling / 2 - 2)).unwrap();

    assert_eq!(
        applied.counter,
        Some(CounterEvent::Wrapped),
        "a delta just under half the width is still credible"
    );

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(0, Num::I(ceiling - 1)).unwrap();

    let applied = ring.append(STEP, Num::I(ceiling / 2 + 2)).unwrap();

    assert_eq!(
        applied.counter,
        Some(CounterEvent::Reset),
        "and one just over it is not"
    );
}

/// A reset arriving mid-bucket must not delete the delta already recorded in that bucket.
#[test]
fn an_unknown_never_erases_a_known_value() {
    let mut shape = cfg();

    shape.kind = Kind::I64;
    shape.mode = Mode::Counter;
    shape.agg = Agg::Sum;

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(0, Num::I(100)).unwrap();
    ring.append(STEP, Num::I(150)).unwrap();

    assert_eq!(ring.at(1), Some(Num::I(50)));

    // Still bucket 1, and the counter restarted.
    let applied = ring.append(STEP + 1, Num::I(3)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Reset));
    assert_eq!(
        ring.at(1),
        Some(Num::I(50)),
        "the delta already measured in this bucket is real and stays"
    );

    // And the combining rule that produces it, stated directly.
    assert_eq!(ring::combine(Agg::Sum, Some(Num::I(50)), None).0, Some(Num::I(50)));
    assert_eq!(ring::combine(Agg::Sum, None, Some(Num::I(7))).0, Some(Num::I(7)));
}

#[test]
fn a_float_counter_has_no_wrap_only_resets() {
    let mut shape = cfg();

    shape.mode = Mode::Counter;
    shape.wrap_bits = 32;

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(0, Num::F(10.5)).unwrap();

    let applied = ring.append(STEP, Num::F(12.0)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Delta));
    assert_eq!(at(&ring, 1), Some(1.5));

    let applied = ring.append(2 * STEP, Num::F(1.0)).unwrap();

    assert_eq!(
        applied.counter,
        Some(CounterEvent::Reset),
        "wrap_bits is documented as ignored for f64 rather than doing something arbitrary"
    );
}

/// A refused append must not move a counter's baseline, or the next good reading measures from a lie.
#[test]
fn a_refused_append_does_not_move_the_counter_baseline() {
    let mut shape = cfg();

    shape.kind = Kind::I64;
    shape.mode = Mode::Counter;

    let mut ring_bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut ring_bytes, &shape);

    ring.append(0, Num::I(1_000)).unwrap();
    ring.append(STEP, Num::I(1_100)).unwrap();

    assert_eq!(ring.at(1), Some(Num::I(100)));

    // Too old: rejected, and it must leave the baseline at 1,100.
    for b in 2..=SLOTS + 1 {
        ring.append(b * STEP, Num::I(1_100 + b * 10)).unwrap();
    }

    let baseline_before = ring.bytes().to_vec();

    assert_eq!(ring.append(0, Num::I(999_999)).unwrap().outcome, Outcome::TooOld);
    assert_eq!(
        &ring_bytes[..],
        &baseline_before[..],
        "a rejected reading is not a reading, so the baseline it would have set is not set"
    );
}

//==========================================================
// Mean archives.
//

#[test]
fn a_mean_archive_averages_the_samples_in_a_bucket() {
    let mut shape = cfg();

    shape.agg = Agg::Mean;

    let mut bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut bytes, &shape);

    // 16-byte slots, so the ring is twice as long as the same slot count would be otherwise.
    assert_eq!(shape.ring_len(), crate::ring::HEADER + SLOTS as usize * 16);

    for v in [10.0f64, 20.0, 30.0, 40.0] {
        ring.append(0, Num::F(v)).unwrap();
    }

    // The mean, not the last and not the sum.
    assert_eq!(at(&ring, 0), Some(25.0));

    // And the sum and count are both retained, which is what makes consolidation exact.
    assert_eq!(ring.at(0), Some(Num::Mean { sum: 100.0, count: 4 }));
}

#[test]
fn an_empty_mean_slot_needs_no_sentinel() {
    let mut shape = cfg();

    shape.agg = Agg::Mean;

    let mut bytes = bytes_of(&shape);
    let ring = ring_in(&mut bytes, &shape);

    // A count of zero *is* the unknown state, so a fresh mean ring is empty at zero with no NaN written anywhere.
    for bucket in -3..12 {
        assert_eq!(ring.at(bucket), None, "bucket {bucket}");
    }
}

/// **The reason a mean slot stores its count.**
///
/// Averaging two buckets' averages is only the average of their samples when both held the same number of them. Across
/// archives they never do, so a mean-of-means would be quietly wrong every time sample density varied.
#[test]
fn combining_means_is_exact_rather_than_an_average_of_averages() {
    // One bucket saw three samples averaging 10; another saw one sample of 100.
    let heavy = Num::Mean { sum: 30.0, count: 3 };
    let light = Num::Mean { sum: 100.0, count: 1 };

    let (combined, _) = ring::combine(Agg::Mean, Some(heavy), Some(light));

    // The true mean of those four samples is 130/4 = 32.5.
    assert_eq!(combined, Some(Num::Mean { sum: 130.0, count: 4 }));
    assert_eq!(combined.unwrap().as_f64(), 32.5);

    // The naive answer - averaging 10 and 100 - would have been 55, which is what storing only the mean would give.
    assert_ne!(combined.unwrap().as_f64(), 55.0);
}

#[test]
fn a_mean_reports_how_many_samples_are_behind_it() {
    assert_eq!(Num::Mean { sum: 9.0, count: 3 }.samples(), 3);

    // Anything else is one sample of itself, so `xff` can treat every archive the same way.
    assert_eq!(Num::F(1.0).samples(), 1);
    assert_eq!(Num::I(1).samples(), 1);
}

//==========================================================
// Resolutions.
//

#[test]
fn fixed_resolutions_bucket_by_division_and_calendar_ones_do_not() {
    use crate::calendar::{self, MS_PER_DAY, MS_PER_HOUR, MS_PER_MIN, MS_PER_SEC, MS_PER_WEEK};
    use crate::config::Resolution;

    // Every fixed unit reports a step and buckets by dividing it.
    for (res, step) in [
        (Resolution::Second, MS_PER_SEC),
        (Resolution::Minute, MS_PER_MIN),
        (Resolution::Hour, MS_PER_HOUR),
        (Resolution::Day, MS_PER_DAY),
        (Resolution::Week, MS_PER_WEEK),
    ] {
        assert_eq!(res.step_ms(), Some(step), "{}", res.name());
    }

    // **The two that have no duration say so** rather than returning an approximation a caller might store against.
    assert_eq!(Resolution::Month.step_ms(), None);
    assert_eq!(Resolution::Year.step_ms(), None);

    // A month bucket is a calendar month however long that month is: January has 31 days, February 28, and each is one
    // bucket. A 30-day step would have split January and merged part of February.
    let jan = calendar::days_from_civil(2026, 1, 1) * MS_PER_DAY;
    let feb = calendar::days_from_civil(2026, 2, 1) * MS_PER_DAY;
    let mar = calendar::days_from_civil(2026, 3, 1) * MS_PER_DAY;

    assert_eq!(Resolution::Month.bucket_of(jan) + 1, Resolution::Month.bucket_of(feb));
    assert_eq!(Resolution::Month.bucket_of(feb) + 1, Resolution::Month.bucket_of(mar));
    assert_eq!(Resolution::Month.bucket_of(jan + 30 * MS_PER_DAY), Resolution::Month.bucket_of(jan));

    // And every bucket start round trips.
    for res in Resolution::ALL {
        let b = res.bucket_of(jan);

        assert_eq!(res.bucket_of(res.bucket_start_ms(b)), b, "{} round trip", res.name());
    }
}

#[test]
fn every_resolution_parses_and_round_trips_its_code() {
    use crate::config::Resolution;

    for res in Resolution::ALL {
        assert_eq!(Resolution::from_u8(res.as_u8()).unwrap(), res);
        assert_eq!(Resolution::parse(res.name()).unwrap(), res);
    }

    // The abbreviations, including the pair that would collide if `m` meant both.
    assert_eq!(Resolution::parse("m").unwrap(), Resolution::Minute);
    assert_eq!(Resolution::parse("mo").unwrap(), Resolution::Month);
    assert!(Resolution::parse("M").is_err(), "case is not a distinction to rely on");
}

//==========================================================
// Config v2: the archive table.
//

#[test]
fn a_config_with_archives_survives_the_round_trip() {
    use crate::config::{default_archives, Resolution, ARCHIVE_LEN, LEN};

    let mut shape = cfg();

    shape.set_archives(&default_archives()).expect("the default set is valid");
    shape.heartbeat_ms = 300_000;

    let bytes = shape.encode();

    // Header plus one descriptor each - nothing is inferred from the length.
    assert_eq!(bytes.len(), LEN + 7 * ARCHIVE_LEN);

    let back = Config::decode(&bytes).expect("its own bytes must decode");

    assert_eq!(back, shape);
    assert_eq!(back.archives().len(), 7);
    assert_eq!(back.heartbeat_ms, 300_000);

    // Finest first, and every resolution present exactly once.
    let got: Vec<Resolution> = back.archives().iter().map(|a| a.resolution).collect();

    assert_eq!(got, Resolution::ALL.to_vec());
}

/// **The compatibility promise**: a v1 config still reads, and means "no archives".
///
/// The bytes a v1 config left reserved are not guaranteed zero, so the archive count and the heartbeat are read only
/// when the version says they mean something. Without that, an old series could come back claiming archives it never had.
#[test]
fn a_version_1_config_still_reads_as_a_single_ring() {
    use crate::config::{VERSION, VERSION_V1};

    let shape = cfg();
    let mut bytes = shape.encode();

    assert_eq!(bytes[4], VERSION, "this build writes v2");

    // Forge a v1 header, and put rubbish in the bytes v1 left reserved.
    bytes.truncate(crate::config::LEN);
    bytes[4] = VERSION_V1;
    bytes[34] = 0xFF;
    bytes[35] = 0xFF;
    bytes[36] = 0xFF;
    bytes[37] = 0xFF;

    let back = Config::decode(&bytes).expect("a v1 config must still decode");

    assert!(back.archives().is_empty(), "v1 has no archive table");
    assert_eq!(back.heartbeat_ms, 0, "v1 has no heartbeat");
    assert_eq!(back.step_ms, shape.step_ms);
    assert_eq!(back.slots, shape.slots);
}

#[test]
fn a_malformed_archive_table_is_refused() {
    use crate::config::{default_archives, Archive, Resolution, ARCHIVE_LEN, LEN, MAX_ARCHIVES};

    let mut shape = cfg();

    shape.set_archives(&default_archives()).unwrap();

    // Claiming more archives than the bin can hold.
    let mut bytes = shape.encode();

    bytes.truncate(LEN + 3 * ARCHIVE_LEN);
    assert!(
        Config::decode(&bytes).is_err(),
        "a table shorter than its own count must be refused, not read as far as it goes"
    );

    // Claiming more than are possible.
    let mut bytes = shape.encode();

    bytes[34] = (MAX_ARCHIVES + 1) as u8;
    assert!(Config::decode(&bytes).is_err(), "past MAX_ARCHIVES");

    // Out of order: coarse before fine would make archive selection pick the wrong one.
    let mut shape = cfg();

    assert!(
        shape
            .set_archives(&[
                Archive::new(Resolution::Hour, Agg::Mean, 10),
                Archive::new(Resolution::Minute, Agg::Mean, 10),
            ])
            .is_err(),
        "archives must be finest first"
    );

    // The same resolution twice would mean two rings competing for one bin.
    let mut shape = cfg();

    assert!(
        shape
            .set_archives(&[
                Archive::new(Resolution::Hour, Agg::Mean, 10),
                Archive::new(Resolution::Hour, Agg::Last, 10),
            ])
            .is_err(),
        "a repeated resolution must be refused"
    );
}

/// The size table in `default_archives`, asserted rather than left as a comment that can rot.
#[test]
fn the_default_archive_set_is_the_size_it_claims() {
    use crate::config::{default_archives, Resolution};

    let archives = default_archives();
    let total: usize = archives.iter().map(|a| a.ring_len()).sum();

    // 60.8 KiB per metric, so roughly sixteen share a 1 MB record.
    //
    // The number is asserted because getting it wrong is easy and invisible. Twice, now: the figure first written into
    // the design notes was 62,136, which had the week archive at 1,720 bytes instead of 104 x 16 + 40 = 1,704 - and then
    // the ring header grew from 40 bytes to 56 to carry the resolution, which added 8 x 7 = 112 and made the week
    // archive genuinely 1,720. An arithmetic slip and a real change landing on the same number is exactly why this is a
    // test and not a comment.
    assert_eq!(total, 62_232, "the documented per-metric size");
    assert!(total * 16 < 1_048_576, "sixteen metrics must fit a 1 MB record");
    assert!(total * 17 > 1_048_576 - 65_536, "and seventeen must be tight");

    // Retention, in the units a reader would check.
    let span = |r: Resolution| archives.iter().find(|a| a.resolution == r).unwrap().approx_span_ms();

    assert_eq!(span(Resolution::Second), 3_600 * 1_000, "1 hour of seconds");
    assert_eq!(span(Resolution::Minute), 1_440 * 60_000, "1 day of minutes");
    assert_eq!(span(Resolution::Hour), 336 * 3_600_000, "14 days of hours");

    // The year archive has to reach 25 years, which is the claim that matters most and the one a fixed 365-day step
    // would have got wrong.
    assert!(
        span(Resolution::Year) >= 25 * 31_556_952_000 - 1,
        "25 years of years"
    );

    // The second archive is `last`, not `mean` - it is the largest and a one-second bucket holds one sample.
    assert_eq!(archives[0].agg, Agg::Last);
    assert_eq!(archives[0].agg.slot_width(), 8);
    assert_eq!(archives[1].agg, Agg::Mean);
    assert_eq!(archives[1].agg.slot_width(), 16);
}

//==========================================================
// Archive rings.
//

use crate::config::{Archive, Resolution};

/// A config with an archive hierarchy, and a shape for one of its archives.
fn archived(archives: &[Archive]) -> Config {
    let mut c = cfg();

    c.set_archives(archives).expect("a valid archive set");

    c
}

fn shape_at(cfg: &Config, resolution: Resolution) -> Shape {
    Shape::at_resolution(cfg, cfg.fingerprint_of(), resolution).expect("that archive exists")
}

fn archive_bytes(shape: &Shape) -> Vec<u8> {
    ring::create(shape)
}

fn archive_ring<'a>(bytes: &'a mut [u8], shape: &Shape) -> RingMut<'a> {
    RingMut::open(bytes, shape).expect("its own bytes must open")
}

/// **The check that keeps one archive from being read as another.**
///
/// Two archives of one series share a config and therefore a fingerprint. Before the resolution was in the ring's own
/// header, opening the hourly ring against the minute archive's shape passed every check there was — same magic, same
/// fingerprint, same length whenever the slot counts happened to agree — and reported values an hour from where they
/// belonged.
#[test]
fn a_ring_says_which_archive_it_is() {
    let cfg = archived(&[
        Archive::new(Resolution::Minute, Agg::Mean, 60),
        // Same slot count and same aggregation as the minute archive, so length and agg cannot tell them apart.
        Archive::new(Resolution::Hour, Agg::Mean, 60),
    ]);

    let minute = shape_at(&cfg, Resolution::Minute);
    let hour = shape_at(&cfg, Resolution::Hour);

    assert_eq!(minute.ring_len(), hour.ring_len(), "the premise: identical lengths");
    assert_eq!(minute.fingerprint(), hour.fingerprint(), "and one config, so one fingerprint");

    let hour_bytes = archive_bytes(&hour);
    let err = RingView::open(&hour_bytes, &minute)
        .expect_err("the hourly ring must not open as the minute archive");

    assert!(
        format!("{err}").contains("hour") && format!("{err}").contains("minute"),
        "the error has to name both: {err}"
    );

    // And a base ring is not an archive ring either, in both directions.
    let base_bytes = bytes_of(&cfg);

    assert!(RingView::open(&base_bytes, &minute).is_err(), "base read as an archive");
    assert!(
        RingView::open(&archive_bytes(&minute), &base(&cfg)).is_err(),
        "archive read as a base ring"
    );
}

#[test]
fn an_archive_buckets_by_its_own_resolution() {
    use crate::calendar::{days_from_civil, MS_PER_DAY, MS_PER_MIN};

    let cfg = archived(&crate::config::default_archives());

    // 2026-08-06 13:47:30, to the millisecond.
    let base_day = days_from_civil(2026, 8, 6) * MS_PER_DAY;
    let ts = base_day + 13 * 3_600_000 + 47 * MS_PER_MIN + 30_000;

    // Every archive puts it in a different bucket, and each bucket's start is the containing period's start.
    let expectations: [(Resolution, i64); 5] = [
        (Resolution::Second, base_day + 13 * 3_600_000 + 47 * MS_PER_MIN + 30_000),
        (Resolution::Minute, base_day + 13 * 3_600_000 + 47 * MS_PER_MIN),
        (Resolution::Hour, base_day + 13 * 3_600_000),
        (Resolution::Day, base_day),
        // 2026-08-06 is a Thursday, so its week began on Monday the 3rd.
        (Resolution::Week, days_from_civil(2026, 8, 3) * MS_PER_DAY),
    ];

    for (res, start) in expectations {
        let shape = shape_at(&cfg, res);

        assert_eq!(
            shape.bucket_start_ms(shape.bucket_of(ts)),
            start,
            "{} bucket start",
            res.name()
        );
    }

    // The two calendar ones, which is where a fixed step would drift.
    let month = shape_at(&cfg, Resolution::Month);
    let year = shape_at(&cfg, Resolution::Year);

    assert_eq!(
        month.bucket_start_ms(month.bucket_of(ts)),
        days_from_civil(2026, 8, 1) * MS_PER_DAY
    );
    assert_eq!(
        year.bucket_start_ms(year.bucket_of(ts)),
        days_from_civil(2026, 1, 1) * MS_PER_DAY
    );
    assert_eq!(month.step_ms(), None, "a month has no fixed width");
}

/// **Why every archive is fed the raw sample.**
///
/// RRDtool feeds a coarse archive the fine archive's already-consolidated values, so an hourly mean is a mean of
/// per-minute means — which equals the true mean only when every minute held the same number of samples. Here each
/// archive buckets the raw sample by its own resolution, so a coarse mean is exact.
#[test]
fn a_coarse_mean_is_the_true_mean_of_the_raw_samples() {
    let cfg = archived(&[
        Archive::new(Resolution::Minute, Agg::Mean, 120),
        Archive::new(Resolution::Hour, Agg::Mean, 24),
    ]);

    let minute = shape_at(&cfg, Resolution::Minute);
    let hour = shape_at(&cfg, Resolution::Hour);

    let mut minute_bytes = archive_bytes(&minute);
    let mut hour_bytes = archive_bytes(&hour);
    let mut minute_ring = archive_ring(&mut minute_bytes, &minute);
    let mut hour_ring = archive_ring(&mut hour_bytes, &hour);

    // An hour of deliberately lopsided density: minute 0 gets 10 samples of 100, minute 1 gets one sample of 0, and
    // nothing else reports. A mean of means would say (100 + 0) / 2 = 50.
    let mut expected_sum = 0.0f64;
    let mut expected_count = 0i64;

    for i in 0..10 {
        let ts = i * 1_000; // ten samples inside minute 0

        minute_ring.append(ts, Num::F(100.0)).unwrap();
        hour_ring.append(ts, Num::F(100.0)).unwrap();

        expected_sum += 100.0;
        expected_count += 1;
    }

    minute_ring.append(60_000, Num::F(0.0)).unwrap();
    hour_ring.append(60_000, Num::F(0.0)).unwrap();
    expected_count += 1;

    let true_mean = expected_sum / expected_count as f64;

    assert_eq!(true_mean, 1_000.0 / 11.0);

    let hour_bucket = hour.bucket_of(0);

    assert_eq!(
        hour_ring.at(hour_bucket),
        Some(Num::Mean { sum: expected_sum, count: expected_count }),
        "the hourly bucket holds every raw sample's sum and count"
    );
    assert_eq!(hour_ring.at(hour_bucket).unwrap().as_f64(), true_mean);
    assert_ne!(
        hour_ring.at(hour_bucket).unwrap().as_f64(),
        50.0,
        "a mean of the two minute means would have been 50, and it would have been wrong"
    );
}

/// `xff` suppresses a bucket too sparse to trust — and does it on the way *out*, not on the way in.
#[test]
fn xff_suppresses_a_sparse_bucket_without_losing_it() {
    let mut hourly = Archive::new(Resolution::Hour, Agg::Mean, 24);

    hourly.xff_pct = 50;

    let cfg = archived(&[Archive::new(Resolution::Minute, Agg::Mean, 60), hourly]);
    let shape = shape_at(&cfg, Resolution::Hour);

    // The finest archive steps in minutes, so an hourly bucket could hold 60 of them and half of that is 30.
    assert_eq!(shape.expected_samples(), 60);
    assert!(!shape.passes_xff(29));
    assert!(shape.passes_xff(30));

    let mut bytes = archive_bytes(&shape);
    let mut ring = archive_ring(&mut bytes, &shape);
    let bucket = shape.bucket_of(0);

    for i in 0..29i64 {
        ring.append(i * 60_000, Num::F(7.0)).unwrap();
    }

    assert_eq!(ring.at(bucket), None, "29 of a possible 60 is below half");
    assert_eq!(
        ring.at_unfiltered(bucket).map(Num::as_f64),
        Some(7.0),
        "and the value is still there - suppressed for reporting, not discarded"
    );

    // **The reason xff cannot filter on the way in.** The 30th sample has to combine with a bucket that does not
    // currently report, or a suppressed bucket could never become unsuppressed.
    ring.append(29 * 60_000, Num::F(7.0)).unwrap();

    assert_eq!(ring.at(bucket).map(Num::as_f64), Some(7.0), "30 of 60 passes");
    assert_eq!(ring.at(bucket).unwrap().samples(), 30);

    // xff = 0 reports whatever is there, which is why it is the default.
    let open = archived(&[
        Archive::new(Resolution::Minute, Agg::Mean, 60),
        Archive::new(Resolution::Hour, Agg::Mean, 24),
    ]);
    let lenient = shape_at(&open, Resolution::Hour);
    let mut bytes = archive_bytes(&lenient);
    let mut ring = archive_ring(&mut bytes, &lenient);

    ring.append(0, Num::F(7.0)).unwrap();

    assert_eq!(ring.at(lenient.bucket_of(0)).map(Num::as_f64), Some(7.0));
}

/// **The heartbeat**: a counter reading that arrives too late is a new baseline, not three hours of traffic in one bucket.
#[test]
fn a_late_counter_reading_becomes_a_baseline_rather_than_a_spike() {
    let mut shape = cfg();

    shape.kind = Kind::I64;
    shape.mode = Mode::Counter;
    shape.heartbeat_ms = 5 * STEP;

    let mut bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut bytes, &shape);

    ring.append(0, Num::I(1_000)).unwrap();

    // Inside the heartbeat: an ordinary delta.
    let applied = ring.append(STEP, Num::I(1_100)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Delta));
    assert_eq!(ring.at(1), Some(Num::I(100)));

    // Past it. Without a heartbeat this would store 8,900 in one bucket, as though the whole gap were one interval.
    let applied = ring.append(STEP + 6 * STEP, Num::I(10_000)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Stale));
    assert_eq!(ring.at(7), None, "unknown, not 8900");
    assert_eq!(ring.resets(), 0, "a stale reading is not a counter reset");

    // And the reading after it measures from the new baseline, so one gap costs one bucket.
    let applied = ring.append(8 * STEP, Num::I(10_050)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Delta));
    assert_eq!(ring.at(8), Some(Num::I(50)));
}

#[test]
fn no_heartbeat_means_no_limit() {
    let mut shape = cfg();

    shape.kind = Kind::I64;
    shape.mode = Mode::Counter;

    assert_eq!(shape.heartbeat_ms, 0, "the default, so existing series keep their behaviour");

    let mut bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut bytes, &shape);

    ring.append(0, Num::I(1_000)).unwrap();

    let applied = ring.append(9 * STEP, Num::I(10_000)).unwrap();

    assert_eq!(applied.counter, Some(CounterEvent::Delta));
    assert_eq!(ring.at(9), Some(Num::I(9_000)));
}

//==========================================================
// Naming, parsing and selection.
//

#[test]
fn archive_bins_are_named_after_their_resolution() {
    use crate::config::{archive_bin, check_base_name, default_archives, BIN_NAME_LIMIT};

    assert_eq!(archive_bin("cpu", Resolution::Second), "cpu~s");
    assert_eq!(archive_bin("cpu", Resolution::Hour), "cpu~h");
    // Not "~m": that is the minute.
    assert_eq!(archive_bin("cpu", Resolution::Month), "cpu~mo");

    // Every suffix has to be something `parse` accepts, or the two directions are separate tables that can drift.
    for res in Resolution::ALL {
        assert_eq!(Resolution::parse(res.suffix()).unwrap(), res, "{}", res.name());
    }

    let archives = default_archives();

    check_base_name("cpu", &archives).expect("short names are fine");
    check_base_name("a".repeat(BIN_NAME_LIMIT - 3).as_str(), &archives)
        .expect("twelve characters plus '~mo' is exactly the limit");

    // Thirteen plus "~mo" is sixteen, which no bin name may be - and it is refused at init rather than discovered when
    // six of seven rings already exist.
    let err = check_base_name("a".repeat(BIN_NAME_LIMIT - 2).as_str(), &archives)
        .expect_err("one character too long");

    assert!(format!("{err}").contains("characters"), "{err}");

    // A base name containing the separator would make its own archive bins ambiguous.
    assert!(check_base_name("cpu~h", &archives).is_err());
    assert!(check_base_name("", &archives).is_err());
}

#[test]
fn archive_specs_parse_and_default_sensibly() {
    let a = Archive::parse("h:336").expect("resolution and slots are enough");

    assert_eq!(a.resolution, Resolution::Hour);
    assert_eq!(a.slots, 336);
    // RRDtool's default consolidation, and the right one for any archive coarser than the sampling rate.
    assert_eq!(a.agg, Agg::Mean);
    assert_eq!(a.xff_pct, 0);

    let a = Archive::parse("s:3600:last:50").unwrap();

    assert_eq!((a.resolution, a.slots, a.agg, a.xff_pct), (Resolution::Second, 3_600, Agg::Last, 50));

    let list = Archive::parse_list("s:3600:last;m:1440;h:336").unwrap();

    assert_eq!(list.len(), 3);
    assert_eq!(list[1].resolution, Resolution::Minute);

    // Each of these is a way to get a series that is not the one the caller described.
    assert!(Archive::parse("h").is_err(), "no slot count");
    assert!(Archive::parse("h:").is_err(), "an empty slot count");
    assert!(Archive::parse("h:many").is_err(), "a slot count that is not a number");
    assert!(Archive::parse("hour:336:maxx").is_err(), "a misspelt aggregation");
    assert!(Archive::parse("nanosecond:10").is_err(), "a resolution that does not exist");
    assert!(Archive::parse("h:0").is_err(), "an empty archive");
    assert!(Archive::parse("h:336:mean:150").is_err(), "an xff over 100");
    assert!(Archive::parse("h:336:mean:50:extra").is_err(), "a fifth field");
    assert!(Archive::parse_list("").is_err(), "a list describing nothing");
    assert!(Archive::parse_list(" ; ").is_err(), "or only separators");
}

/// Selection takes the **finest** archive that reaches back far enough, and says when none does.
#[test]
fn archive_selection_prefers_the_finest_that_reaches() {
    use crate::config::default_archives;

    let cfg = archived(&default_archives());
    let now = 1_767_225_600_000i64;
    let minute = 60_000i64;
    let hour = 60 * minute;
    let day = 24 * hour;

    // (how far back, which archive)
    let cases: [(i64, Resolution); 6] = [
        (10 * minute, Resolution::Second), // the second archive covers one hour
        (2 * hour, Resolution::Minute),    // the minute archive covers one day
        (3 * day, Resolution::Hour),       // the hour archive covers fourteen days
        (30 * day, Resolution::Day),       // the day archive covers about three months
        (200 * day, Resolution::Week),     // the week archive covers two years
        (1_000 * day, Resolution::Month),  // the month archive covers five years
    ];

    for (back, expected) in cases {
        let (index, why, reached) = crate::select_archive(&cfg, Some(now - back), now);

        assert_eq!(
            cfg.archives()[index].resolution,
            expected,
            "{back} ms back should come from the {} archive, not the {} one ({why})",
            expected.name(),
            cfg.archives()[index].resolution.name()
        );
        assert!(reached, "and it reaches: {why}");
    }

    // No start given means the finest, because "recent" is what a bare report asks for.
    let (index, _, reached) = crate::select_archive(&cfg, None, now);

    assert_eq!(index, 0);
    assert!(reached);

    // Nothing reaches back fifty years, so the coarsest answers and **says it did not reach** - a returned fact, not a
    // string a caller has to match on.
    let (index, _, reached) = crate::select_archive(&cfg, Some(now - 50 * 365 * day), now);

    assert_eq!(cfg.archives()[index].resolution, Resolution::Year);
    assert!(!reached, "the report calls this 'truncated' rather than 'ok'");
}

#[test]
fn the_archives_option_builds_the_hierarchy() {
    use crate::config::default_archives;

    let mut shape = cfg();

    crate::apply_opts(&mut shape, "archives=default").unwrap();
    assert_eq!(shape.archives(), default_archives().as_slice());

    let mut shape = cfg();

    crate::apply_opts(&mut shape, "archives=none").unwrap();
    assert!(!shape.has_archives(), "the default, so an existing series is untouched");

    let mut shape = cfg();

    crate::apply_opts(&mut shape, "kind=i64,archives=m:1440;h:336,heartbeat=300000").unwrap();
    assert_eq!(shape.kind, Kind::I64);
    assert_eq!(shape.archives().len(), 2);
    assert_eq!(shape.heartbeat_ms, 300_000);

    // **`xff` applies whichever side of `archives` it is written on.** An options string whose meaning depends on key
    // order is a trap, and this one has no reason to be ordered.
    for opts in ["archives=default,xff=50", "xff=50,archives=default"] {
        let mut shape = cfg();

        crate::apply_opts(&mut shape, opts).unwrap();

        assert!(
            shape.archives().iter().all(|a| a.xff_pct == 50),
            "'{opts}' should set every archive's xff"
        );
    }

    // And an xff with nothing to apply to is a mistake worth reporting rather than ignoring.
    let mut shape = cfg();

    assert!(crate::apply_opts(&mut shape, "xff=50").is_err());

    let mut shape = cfg();

    assert!(crate::apply_opts(&mut shape, "archives=h:0").is_err(), "an empty archive");
    assert!(crate::apply_opts(&mut shape, "heartbeat=soon").is_err());
}

/// A shape reports the **archive's** aggregation and slot count, not the config's.
#[test]
fn a_shape_describes_one_ring_rather_than_the_series() {
    let cfg = archived(&[
        Archive::new(Resolution::Minute, Agg::Last, 60),
        Archive::new(Resolution::Day, Agg::Mean, 30),
    ]);

    assert_eq!(cfg.agg, Agg::Last, "the config's own agg, which the base ring would use");
    assert_eq!(cfg.slots, SLOTS);

    let day = shape_at(&cfg, Resolution::Day);

    assert_eq!(day.agg(), Agg::Mean, "the archive's, not the config's");
    assert_eq!(day.slots(), 30);
    assert_eq!(day.slot_width(), 16, "because a mean needs a count");
    assert_eq!(day.ring_len(), ring::HEADER + 30 * 16);
    assert_eq!(day.bin_name("cpu"), "cpu~d");

    // Reaching for an archive the series does not keep names what it does keep, rather than substituting a neighbour.
    let err = Shape::at_resolution(&cfg, cfg.fingerprint_of(), Resolution::Year).expect_err("absent");

    assert!(format!("{err}").contains("minute, day"), "{err}");

    // And a series with no archives says so instead of inventing one.
    let plain = self::cfg();
    let err = crate::shape_of(&plain, plain.fingerprint_of(), Some(Resolution::Hour))
        .expect_err("no hierarchy");

    assert!(format!("{err}").contains("single ring"), "{err}");
}

//==========================================================
// Extremes.
//
// These run in debug, where an arithmetic overflow panics instead of wrapping. That is the point: every one of them is
// a subtraction on numbers a caller supplies, and in release a wrap would silently produce a plausible wrong answer.
//

/// A ring stepped at **one millisecond** has the least headroom there is: a bucket index *is* the timestamp, so any
/// difference of two buckets can be as large as the whole `i64` range.
#[test]
fn a_one_millisecond_ring_survives_the_ends_of_time() {
    let shape = Config::new(1, 10, 0).expect("a 1ms step is legal");
    let mut bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut bytes, &shape);

    // The far future first, so `last_bucket` is as large as it can be.
    let applied = ring.append(i64::MAX, Num::F(1.0)).expect("the last millisecond is still a time");

    assert_eq!(applied.outcome, Outcome::Placed);
    assert_eq!(applied.bucket, i64::MAX);

    // `window()` is `last - slots + 1`, which must not wrap.
    assert_eq!(ring.window(), Some((i64::MAX - 9, i64::MAX)));
    assert_eq!(ring.at(i64::MAX - 10), None, "just outside the window");
    assert_eq!(at(&ring, i64::MAX), Some(1.0));

    // Now the far past. `last - bucket` spans nearly the whole range, and it has to come out as "too old" rather than
    // as whatever a wrapped subtraction happens to give.
    let before = ring.bytes().to_vec();
    let applied = ring.append(i64::MIN + 1, Num::F(2.0)).expect("and the first is too");

    assert_eq!(applied.outcome, Outcome::TooOld);
    assert_eq!(ring.bytes(), &before[..], "a refusal at the ends of time is still a refusal");

    // And the other order: a fresh ring anchored in the far past, then a sample in the far future. `bucket - last`
    // is the same span the other way, and the answer has to be `would_wipe` rather than a wrap.
    let mut bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut bytes, &shape);

    ring.append(i64::MIN + 1, Num::F(1.0)).unwrap();

    let applied = ring.append(i64::MAX, Num::F(2.0)).unwrap();

    assert_eq!(
        applied.outcome,
        Outcome::WouldWipe,
        "a gap of the whole i64 range erases every slot, so the default refuses it"
    );
}

/// **`i64::MIN` as a bucket collides with the "never written" sentinel**, and the only honest thing is to refuse it.
///
/// `NO_BUCKET` is `i64::MIN`, so a ring whose newest bucket were genuinely `i64::MIN` would be indistinguishable from an
/// empty one — it would report no window and every sample in it as unknown. Reachable only at a 1 ms step with a
/// timestamp of `i64::MIN` exactly, which is not a time anybody means, but "not a time anybody means" is not a reason
/// for a write to be silently discarded.
#[test]
fn the_one_timestamp_that_collides_with_the_empty_sentinel_is_refused() {
    let shape = Config::new(1, 10, 0).unwrap();
    let mut bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut bytes, &shape);

    let err = ring
        .append(i64::MIN, Num::F(1.0))
        .expect_err("bucket i64::MIN cannot be represented as a written position");

    assert!(format!("{err}").contains("i64::MIN"), "{err}");
    assert_eq!(ring.last_bucket(), NO_BUCKET, "and the ring is still empty");
    assert_eq!(ring.window(), None);

    // One millisecond later is fine, which is what makes the refusal a boundary rather than a region.
    ring.append(i64::MIN + 1, Num::F(1.0)).expect("i64::MIN + 1 is an ordinary time");

    // The window's lower end **saturates** at `i64::MIN` rather than wrapping - there are no buckets below it to
    // include. The literal `i64::MIN + 1 - 9` is what this assertion said first, and the compiler rejected it for
    // overflowing, which is exactly the arithmetic `window()` was doing.
    assert_eq!(ring.window(), Some((i64::MIN, i64::MIN + 1)));
    assert_eq!(at(&ring, i64::MIN + 1), Some(1.0));
}

/// Every resolution places and labels a bucket at both ends of `i64` without overflowing.
#[test]
fn every_resolution_survives_the_ends_of_time() {
    use crate::config::{default_archives, Resolution};

    let cfg = archived(&default_archives());

    for res in Resolution::ALL {
        let shape = shape_at(&cfg, res);

        for ts in [i64::MIN + 1, i64::MIN / 2, -1, 0, 1, i64::MAX / 2, i64::MAX] {
            let bucket = shape.bucket_of(ts);
            let start = shape.bucket_start_ms(bucket);

            // The bucket's start is at or before the instant in it. Saturating arithmetic can clamp the *label* at the
            // extremes, which is why this is an inequality rather than a round trip - but it must never come out ahead.
            assert!(
                start <= ts || start == i64::MIN || start == i64::MAX,
                "{}: bucket_of({ts}) = {bucket}, whose start {start} is after it",
                res.name()
            );

            // And a ring can actually take a sample there.
            let mut bytes = ring::create(&shape);
            let mut ring = RingMut::open(&mut bytes, &shape).unwrap();
            let applied = ring
                .append(ts, Num::F(1.0))
                .unwrap_or_else(|e| panic!("{} at {ts}: {e}", res.name()));

            assert_eq!(applied.outcome, Outcome::Placed, "{} at {ts}", res.name());
            assert_eq!(ring.at(bucket).map(Num::as_f64), Some(1.0), "{} at {ts}", res.name());
        }
    }
}

/// A ring one slot wide is a legal degenerate case, and every path has to cope with it.
#[test]
fn a_single_slot_ring_works() {
    let shape = Config::new(STEP, 1, 0).expect("one slot is legal");
    let mut bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut bytes, &shape);

    ring.append(0, Num::F(1.0)).unwrap();

    assert_eq!(ring.window(), Some((0, 0)));
    assert_eq!(at(&ring, 0), Some(1.0));

    // The next bucket replaces it: `gap - 1 = 0` slots to clear, and the store overwrites.
    let applied = ring.append(STEP, Num::F(2.0)).unwrap();

    assert_eq!(applied.outcome, Outcome::Placed);
    assert_eq!(applied.cleared, 0, "there is nothing between adjacent buckets");
    assert_eq!(at(&ring, 1), Some(2.0));
    assert_eq!(ring.at(0), None, "bucket 0 is outside a one-slot window now");

    // **A one-slot ring is exempt from the wipe guard**, and it has to be. Its window is one bucket, so every forward
    // advance erases the whole window and `gap >= slots` is always true — the guard would refuse every append after the
    // first unless `wipe=1`, which would make a shape `ts_init` happily creates unusable.
    //
    // This is the sharp edge the test found. Before the exemption, the assertion above failed with `WouldWipe`.
    let applied = ring.append(3 * STEP, Num::F(3.0)).unwrap();

    assert_eq!(applied.outcome, Outcome::Placed, "a one-slot ring may always advance");
    assert_eq!(applied.cleared, 0, "clearing is capped at slots - 1, which is zero here");
    assert_eq!(at(&ring, 3), Some(3.0));
    assert_eq!(ring.window(), Some((3, 3)));

    // Two slots is where the guard starts applying, and there a gap of two really does discard both.
    let two = Config::new(STEP, 2, 0).unwrap();
    let mut bytes = bytes_of(&two);
    let mut ring = ring_in(&mut bytes, &two);

    ring.append(0, Num::F(1.0)).unwrap();

    assert_eq!(ring.append(STEP, Num::F(2.0)).unwrap().outcome, Outcome::Placed, "one bucket on");
    assert_eq!(
        ring.append(4 * STEP, Num::F(3.0)).unwrap().outcome,
        Outcome::WouldWipe,
        "three buckets on, from a two-slot window"
    );
}

/// **A non-finite sample is refused**, because `NaN` is how a slot is marked unknown.
///
/// Storing one would report a successful write and read back as "nothing was ever here" — the one outcome a caller
/// cannot detect. An infinity is the opposite failure: every aggregation treats it as a real value, so one of them
/// poisons a `sum` or `mean` bucket permanently.
#[test]
fn a_non_finite_sample_is_refused() {
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let err = crate::finite(bad).expect_err("{bad} is not a sample");

        assert!(format!("{err}").contains("finite"), "{bad}: {err}");
    }

    // Everything finite goes through untouched, including the extremes and a negative zero.
    for good in [0.0, -0.0, 1.5, -1.5, f64::MIN, f64::MAX, f64::MIN_POSITIVE] {
        assert_eq!(crate::finite(good).unwrap(), Num::F(good), "{good}");
    }
}

/// The largest ring the config allows, built and used.
#[test]
fn a_max_slots_ring_is_usable() {
    let shape = Config::new(1_000, MAX_SLOTS, 0).expect("the ceiling is legal");

    assert_eq!(shape.ring_len(), ring::HEADER + MAX_SLOTS as usize * 8);

    let mut bytes = bytes_of(&shape);
    let mut ring = ring_in(&mut bytes, &shape);

    // The two ends of the window and one in the middle, so the slot arithmetic is exercised at the extremes of the ring
    // rather than only near zero.
    for bucket in [0i64, MAX_SLOTS / 2, MAX_SLOTS - 1] {
        ring.append(bucket * 1_000, Num::F(bucket as f64)).unwrap();
    }

    assert_eq!(ring.window(), Some((0, MAX_SLOTS - 1)));

    for bucket in [0i64, MAX_SLOTS / 2, MAX_SLOTS - 1] {
        assert_eq!(at(&ring, bucket), Some(bucket as f64), "bucket {bucket}");
    }

    // One more bucket retires exactly bucket 0 and nothing else.
    ring.append(MAX_SLOTS * 1_000, Num::F(-1.0)).unwrap();

    assert_eq!(ring.at(0), None);
    assert_eq!(at(&ring, MAX_SLOTS), Some(-1.0));
    assert_eq!(at(&ring, MAX_SLOTS / 2), Some((MAX_SLOTS / 2) as f64), "the middle survived");
}
