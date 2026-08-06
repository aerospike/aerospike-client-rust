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

//! Calendar arithmetic, because **months and years are not durations**.
//!
//! Every other resolution is a fixed multiple of a millisecond, so its bucket is `t / step` and nothing here is
//! needed. A month is 28 to 31 days and a year is 365 or 366, so there is no `step_ms` for either: their buckets have
//! to be counted in calendar units.
//!
//! The alternative was approximating them as 30 and 365 days. That keeps the division and drifts: twelve 30-day
//! "months" land five days short of a year, so a bucket labelled *March* would wander through the calendar and end up
//! covering most of April. A chart with the wrong month on it is worse than no month archive.
//!
//! # Where this comes from
//!
//! `civil_from_days` and `days_from_civil` are Howard Hinnant's algorithms, which are the standard ones and are exact
//! for the whole proleptic Gregorian calendar rather than only for dates near now. They are about twenty lines of
//! integer arithmetic and need **no dependency**, which matters: this example's argument is that it has none.
//!
//! # Divisions are floored, not truncated
//!
//! `div_euclid` throughout, for the same reason the ring uses it: a pre-1970 timestamp is negative, and truncating
//! toward zero would put 1969-12-31 in the same day bucket as 1970-01-01.

pub const MS_PER_SEC: i64 = 1_000;
pub const MS_PER_MIN: i64 = 60 * MS_PER_SEC;
pub const MS_PER_HOUR: i64 = 60 * MS_PER_MIN;
pub const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;
pub const MS_PER_WEEK: i64 = 7 * MS_PER_DAY;

/// Days from 1970-01-01 to the day containing `ms`.
pub fn days_from_ms(ms: i64) -> i64 {
    ms.div_euclid(MS_PER_DAY)
}

/// The civil date of a day count, as `(year, month, day)` with month in `1..=12`.
///
/// Hinnant's `civil_from_days`. Shifts the epoch to 0000-03-01 so leap days land at the end of the era, which is what
/// removes the special cases.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]

    (y + i64::from(m <= 2), m as u32, d as u32)
}

/// The day count of a civil date. The exact inverse of [`civil_from_days`].
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let m = month as i64;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]

    era * 146_097 + doe - 719_468
}

/// Months since 1970-01, which is the bucket index of a month archive.
pub fn month_bucket(ms: i64) -> i64 {
    let (y, m, _) = civil_from_days(days_from_ms(ms));

    (y - 1970) * 12 + (m as i64 - 1)
}

/// The first millisecond of a month bucket.
pub fn month_bucket_start_ms(bucket: i64) -> i64 {
    let y = 1970 + bucket.div_euclid(12);
    let m = bucket.rem_euclid(12) + 1;

    days_from_civil(y, m as u32, 1).saturating_mul(MS_PER_DAY)
}

/// Years since 1970, which is the bucket index of a year archive.
pub fn year_bucket(ms: i64) -> i64 {
    let (y, _, _) = civil_from_days(days_from_ms(ms));

    y - 1970
}

/// The first millisecond of a year bucket.
pub fn year_bucket_start_ms(bucket: i64) -> i64 {
    days_from_civil(1970 + bucket, 1, 1).saturating_mul(MS_PER_DAY)
}

/// Days to subtract from an epoch day count so that weeks begin on Monday.
///
/// **1970-01-01 was a Thursday**, so `ms / MS_PER_WEEK` produces weeks running Thursday to Wednesday - which is almost
/// never what anyone means. Shifting by three days puts the boundary on Monday: day −3 is 1969-12-29, a Monday, and it
/// is the start of week 0.
pub const MONDAY_SHIFT: i64 = 3;

/// Weeks since the Monday before the epoch, which is the bucket index of a week archive.
pub fn week_bucket(ms: i64) -> i64 {
    (days_from_ms(ms) + MONDAY_SHIFT).div_euclid(7)
}

/// The first millisecond of a week bucket - a Monday.
pub fn week_bucket_start_ms(bucket: i64) -> i64 {
    (bucket * 7 - MONDAY_SHIFT).saturating_mul(MS_PER_DAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Dates whose day numbers are known independently, including the awkward ones.
    #[test]
    fn known_dates_convert_both_ways() {
        // (year, month, day, days from epoch)
        let cases: [(i64, u32, u32, i64); 12] = [
            (1970, 1, 1, 0),
            (1969, 12, 31, -1),
            (1969, 12, 29, -3),   // the Monday that starts week 0
            (1970, 12, 31, 364),
            (1972, 2, 29, 789),   // the first leap day after the epoch
            (2000, 1, 1, 10_957),
            (2000, 2, 29, 11_016), // a leap day in a century divisible by 400
            (1900, 3, 1, -25_508), // 1900 was *not* a leap year: the /100 rule
            (2026, 1, 1, 20_454),
            (2026, 8, 6, 20_671),
            (1, 1, 1, -719_162),   // far outside any plausible timestamp, still exact
            (2400, 2, 29, 157_113),
        ];

        for (y, m, d, days) in cases {
            assert_eq!(
                days_from_civil(y, m, d),
                days,
                "days_from_civil({y}, {m}, {d})"
            );
            assert_eq!(civil_from_days(days), (y, m, d), "civil_from_days({days})");
        }
    }

    /// Every day across two centuries round trips, which is the property the known dates only sample.
    #[test]
    fn every_day_round_trips_across_two_centuries() {
        for days in days_from_civil(1900, 1, 1)..days_from_civil(2100, 1, 1) {
            let (y, m, d) = civil_from_days(days);

            assert_eq!(days_from_civil(y, m, d), days, "round trip at day {days}");
            assert!((1..=12).contains(&m), "month out of range at day {days}: {m}");
            assert!((1..=31).contains(&d), "day out of range at day {days}: {d}");
        }
    }

    #[test]
    fn month_buckets_count_calendar_months() {
        let jan_1970 = month_bucket(0);

        assert_eq!(jan_1970, 0);

        // 2026-01-01 is 672 months after 1970-01.
        let jan_2026 = days_from_civil(2026, 1, 1) * MS_PER_DAY;

        assert_eq!(month_bucket(jan_2026), (2026 - 1970) * 12);

        // Every day of a month shares one bucket, and the next month is exactly one more - which is the property a
        // fixed 30-day step cannot have.
        for month in 1..=12u32 {
            let first = days_from_civil(2026, month, 1) * MS_PER_DAY;
            let bucket = month_bucket(first);

            assert_eq!(month_bucket_start_ms(bucket), first, "month {month} start");

            // The last millisecond before the next month is still this month.
            let next = if month == 12 {
                days_from_civil(2027, 1, 1)
            } else {
                days_from_civil(2026, month + 1, 1)
            } * MS_PER_DAY;

            assert_eq!(month_bucket(next - 1), bucket, "end of month {month}");
            assert_eq!(month_bucket(next), bucket + 1, "start of month {month} + 1");
        }
    }

    #[test]
    fn year_buckets_count_calendar_years() {
        assert_eq!(year_bucket(0), 0);
        assert_eq!(year_bucket_start_ms(0), 0);

        for year in 1970..2030i64 {
            let first = days_from_civil(year, 1, 1) * MS_PER_DAY;
            let bucket = year_bucket(first);

            assert_eq!(bucket, year - 1970);
            assert_eq!(year_bucket_start_ms(bucket), first, "year {year} start");

            let next = days_from_civil(year + 1, 1, 1) * MS_PER_DAY;

            // A leap year is 366 days and this still holds, which is the point of not using a fixed 365-day step.
            assert_eq!(year_bucket(next - 1), bucket, "end of {year}");
            assert_eq!(year_bucket(next), bucket + 1, "start of {}", year + 1);
        }
    }

    /// **The Thursday trap.** Epoch weeks start on a Thursday; these have to start on Monday.
    #[test]
    fn weeks_begin_on_monday() {
        // 2026-08-06 is a Thursday. Its week must begin on Monday 2026-08-03.
        let thursday = days_from_civil(2026, 8, 6) * MS_PER_DAY;
        let monday = days_from_civil(2026, 8, 3) * MS_PER_DAY;

        assert_eq!(
            week_bucket_start_ms(week_bucket(thursday)),
            monday,
            "the week containing Thursday must start on the Monday before it"
        );

        // Every day from that Monday to the Sunday after shares one bucket; the next Monday does not.
        let bucket = week_bucket(monday);

        for offset in 0..7 {
            assert_eq!(
                week_bucket(monday + offset * MS_PER_DAY),
                bucket,
                "day {offset} of the week"
            );
        }

        assert_eq!(week_bucket(monday + 7 * MS_PER_DAY), bucket + 1);

        // And the start of every bucket is a Monday, checked by weekday rather than by construction. 1970-01-01 was a
        // Thursday, so day 0 is weekday 3 counting Monday as 0.
        for b in -600..3_000i64 {
            let days = week_bucket_start_ms(b) / MS_PER_DAY;

            assert_eq!(
                (days + 3).rem_euclid(7),
                0,
                "week {b} starts on weekday {}",
                (days + 3).rem_euclid(7)
            );
        }
    }

    /// Pre-epoch instants floor rather than truncating, in every unit.
    #[test]
    fn pre_epoch_timestamps_floor() {
        assert_eq!(days_from_ms(-1), -1);
        assert_eq!(month_bucket(-1), -1, "1969-12 is one month before 1970-01");
        assert_eq!(year_bucket(-1), -1, "1969 is one year before 1970");

        let dec_1969 = days_from_civil(1969, 12, 1) * MS_PER_DAY;

        assert_eq!(month_bucket_start_ms(-1), dec_1969);
        assert_eq!(year_bucket_start_ms(-1), days_from_civil(1969, 1, 1) * MS_PER_DAY);
    }
}
