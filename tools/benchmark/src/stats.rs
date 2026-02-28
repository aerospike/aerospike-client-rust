// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
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

use std::f64;
use std::time::{Duration, Instant};

use chrono::Utc;
use tokio::sync::mpsc::Receiver;

/// Read and write histograms sent by workers each collection interval.
pub type StatsPacket = (Histogram, Histogram);

use crate::workers::Status;

// Number of buckets for latency histogram, e.g.
// 6 buckets => "<1ms", "<2ms", "<4ms", "<8ms", "<16ms", ">=16ms"
const HIST_BUCKETS: usize = 6;

/// Upper bound (μs) of each bucket for asbench-style latency output.
const BUCKET_UPPER_US: [u128; HIST_BUCKETS] = [1_000, 2_000, 4_000, 8_000, 16_000, 32_000];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportStyle {
    Pretty,
    Asbench,
}

lazy_static! {
    // How frequently histogram is printed
    pub static ref REPORT_MS: Duration = Duration::from_secs(1);
}

#[derive(Debug)]
pub struct Collector {
    receiver: Receiver<StatsPacket>,
    read_histogram: Histogram,
    write_histogram: Histogram,
    report_style: ReportStyle,
}

impl Collector {
    pub fn new(recv: Receiver<StatsPacket>, report: ReportStyle) -> Self {
        Collector {
            receiver: recv,
            read_histogram: Histogram::new(),
            write_histogram: Histogram::new(),
            report_style: report,
        }
    }

    fn drain_and_merge(&mut self) {
        while let Ok((read_hist, write_hist)) = self.receiver.try_recv() {
            self.read_histogram.merge(read_hist);
            self.write_histogram.merge(write_hist);
        }
    }

    pub async fn collect(mut self) {
        let mut report_interval = tokio::time::interval(*REPORT_MS);
        report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        report_interval.tick().await;

        loop {
            tokio::select! {
                msg = self.receiver.recv() => {
                    match msg {
                        Some((read_hist, write_hist)) => {
                            self.read_histogram.merge(read_hist);
                            self.write_histogram.merge(write_hist);
                        }
                        None => {
                            break;
                        }
                    }
                }
                _ = report_interval.tick() => {
                    self.drain_and_merge();
                    self.report();
                    self.read_histogram.reset();
                    self.write_histogram.reset();
                }
            }
        }
        self.drain_and_merge();
        self.report();
        self.read_histogram.reset();
        self.write_histogram.reset();
        self.summary();
    }

    fn report(&self) {
        match self.report_style {
            ReportStyle::Pretty => {
                Self::report_section("READ", &self.read_histogram);
                Self::report_section("WRITE", &self.write_histogram);
            }
            ReportStyle::Asbench => {
                self.report_asbench_tps();
                self.report_asbench_latency();
            }
        }
    }

    // Asbench benchmark style: write(tps=N timeouts=N errors=N) read(...) total(...)
    fn report_asbench_tps(&self) {
        let r = &self.read_histogram;
        let w = &self.write_histogram;
        let write_tps = w.tps() as i64;
        let read_tps = r.tps() as i64;
        let total_tps = write_tps + read_tps;
        let total_timeouts = w.timeouts() + r.timeouts();
        let total_errors = w.errors() + r.errors();
        println!(
            "write(tps={} timeouts={} errors={}) read(tps={} timeouts={} errors={}) total(tps={} timeouts={} errors={})",
            write_tps,
            w.timeouts(),
            w.errors(),
            read_tps,
            r.timeouts(),
            r.errors(),
            total_tps,
            total_timeouts,
            total_errors
        );
    }

    // Asbench latency format: "dr: <op> <UTC> <period>, <total_count>, <bucket1>, <bucket2>, ..."
    fn report_asbench_latency(&self) {
        let period_sec = self.write_histogram.total_elapsed_as_secs() as i64;
        let utc = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        Self::report_asbench_latency_section("write", &self.write_histogram, &utc, period_sec);
        Self::report_asbench_latency_section("read", &self.read_histogram, &utc, period_sec);
    }

    fn report_asbench_latency_section(name: &str, hist: &Histogram, utc: &str, period_sec: i64) {
        let total_cnt = hist.count();
        let (min_us, max_us) = if total_cnt > 0 {
            let min_ns = hist.min();
            let max_ns = hist.max();

            let min_us = if min_ns == u128::max_value() {
                0.001_f64
            } else {
                min_ns as f64 / 1_000.0
            };
            let max_us = max_ns as f64 / 1_000.0;
            (min_us, max_us)
        } else {
            (0.0, 0.0)
        };

        print!(
            "HG: {} {} {}, {}, {}, {}",
            name,
            utc,
            period_sec,
            total_cnt,
            min_us as i64,
            max_us as i64,
        );
        println!();
    }

    fn report_section(label: &str, hist: &Histogram) {
        println!(
            "--- {} ---\n  TPS: {:>8.0},   TOTAL_OPS: {:>8},   Timeouts: {:>8},   Errors: {:>8}",
            label,
            hist.tps(),
            hist.count(),
            hist.timeouts(),
            hist.errors()
        );
        if hist.count() > 0 {
            let bkt = hist.latencies();
            let min_ms = if hist.min() == u128::max_value() {
                0.001_f64
            } else {
                hist.min() as f64 / 1_000_000.0
            };
            println!(
                "  Latency:     min      avg      max    |        < 1 ms        < 2 ms        < 4 \
                 ms        < 8 ms       < 16 ms      >= 16 ms"
            );
            println!(
                "         {:>8.3} {:>8.3} {:>8.3} ms | {:>7}/{:>4.1}% {:>7}/{:>4.1}% \
                 {:>7}/{:>4.1}% {:>7}/{:>4.1}% {:>7}/{:>4.1}% {:>7}/{:>4.1}%",
                min_ms,
                hist.avg() as f64 / 1_000_000.0,
                hist.max() as f64 / 1_000_000.0,
                bkt[0].0,
                bkt[0].1,
                bkt[1].0,
                bkt[1].1,
                bkt[2].0,
                bkt[2].1,
                bkt[3].0,
                bkt[3].1,
                bkt[4].0,
                bkt[4].1,
                bkt[5].0,
                bkt[5].1
            );
        } else {
            println!("  Latency: (no ops)");
        }
        println!();
    }

    fn summary(&self) {
        let read = self.read_histogram;
        let write = self.write_histogram;
        println!(
            "\nTotal read requests: {},   Total write requests: {}",
            read.total(),
            write.total()
        );
        // Both histograms share the same start time (Collector creation), so elapsed is identical
        println!("Elapsed time: {:.1}s", write.total_elapsed_as_secs());
        println!("Total TPS: {:.0}", read.total_tps() + write.total_tps());
        println!(
            "Total timeouts: {},   Total errors: {}",
            read.total_timeouts() + write.total_timeouts(),
            read.total_errors() + write.total_errors()
        );
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Histogram {
    buckets: [u128; HIST_BUCKETS],
    /// Min latency in nanoseconds (avoids truncation from as_micros() for sub-µs ops)
    min: u128,
    max: u128,
    sum: u128,
    count: u128,
    timeouts: u128,
    errors: u128,
    interval: Instant,
    start: Instant,
    total: u128,
    total_timeouts: u128,
    total_errors: u128,
}

impl Histogram {
    pub fn new() -> Self {
        let now = Instant::now();
        Histogram {
            buckets: [0; HIST_BUCKETS],
            min: u128::max_value(),
            max: u128::min_value(),
            sum: 0,
            count: 0,
            timeouts: 0,
            errors: 0,
            interval: now,
            start: now,
            total: 0,
            total_timeouts: 0,
            total_errors: 0,
        }
    }

    pub fn min(&self) -> u128 {
        self.min
    }

    pub fn max(&self) -> u128 {
        self.max
    }

    pub fn avg(&self) -> u128 {
        self.sum / self.count
    }

    pub fn tps(&self) -> f64 {
        self.count as f64 / self.interval_as_secs()
    }

    pub fn count(&self) -> u128 {
        self.count
    }

    pub fn timeouts(&self) -> u128 {
        self.timeouts
    }

    pub fn errors(&self) -> u128 {
        self.errors
    }

    pub fn latencies(&self) -> Vec<(u128, f64)> {
        self.buckets
            .iter()
            .map(|&c| {
                let pct = c as f64 / self.count as f64 * 100.0;
                (c, pct)
            })
            .collect()
    }

    // fn percentile(&self, p: f64) -> u128 {
    //     if self.count == 0 {
    //         return 0;
    //     }
    //     let target = p / 100.0 * self.count as f64;
    //     // Upper bound (μs) of each bucket: <1ms, <2ms, <4ms, <8ms, <16ms, >=16ms
    //     let bucket_upper_us: [u128; HIST_BUCKETS] = [1_000, 2_000, 4_000, 8_000, 16_000, 32_000];
    //     let mut cum = 0u128;
    //     for (i, &c) in self.buckets.iter().enumerate() {
    //         cum += c;
    //         if cum as f64 >= target {
    //             return bucket_upper_us[i];
    //         }
    //     }
    //     bucket_upper_us[HIST_BUCKETS - 1]
    // }

    pub fn total(&self) -> u128 {
        self.total
    }

    pub fn total_timeouts(&self) -> u128 {
        self.total_timeouts
    }

    pub fn total_errors(&self) -> u128 {
        self.total_errors
    }

    pub fn total_elapsed_as_secs(&self) -> f64 {
        let elapsed = self.start.elapsed();
        elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1_000_000_000.0
    }

    pub fn total_tps(&self) -> f64 {
        self.total as f64 / self.total_elapsed_as_secs()
    }

    pub fn add(&mut self, latency: Duration, status: Status) {
        let nanos = latency.as_nanos();
        let micros = latency.as_micros();

        if nanos > 0 && nanos < self.min {
            self.min = nanos;
        }

        if nanos > self.max {
            self.max = nanos;
        }

        self.count += 1;
        self.sum += nanos;

        let mut upper = 1_000u128;
        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            if (micros < upper) || (i == HIST_BUCKETS - 1) {
                *bucket += 1;
                break;
            }
            upper <<= 1;
        }

        match status {
            Status::Timeout => self.timeouts += 1,
            Status::Error => self.errors += 1,
            _ => {}
        }
    }

    pub fn merge(&mut self, other: Histogram) {
        if other.min != u128::MAX && other.min != 0 {
            self.min = if self.min == u128::MAX {
                other.min
            } else {
                self.min.min(other.min)
            };
        }

        self.max = if self.max > other.max {
            self.max
        } else {
            other.max
        };

        self.count += other.count;
        self.sum += other.sum;

        for (s, o) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *s += *o
        }

        self.timeouts += other.timeouts;
        self.errors += other.errors;
    }

    pub fn reset(&mut self) {
        for bucket in &mut self.buckets {
            *bucket = 0;
        }
        self.total += self.count;
        self.total_timeouts += self.timeouts;
        self.total_errors += self.errors;
        self.min = u128::max_value();
        self.max = u128::min_value();
        self.sum = 0;
        self.count = 0;
        self.timeouts = 0;
        self.errors = 0;
        self.interval = Instant::now();
    }

    fn interval_as_secs(&self) -> f64 {
        let elapsed = self.interval.elapsed();
        elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1_000_000_000.0
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_histogram_add() {
        let mut hist = Histogram::new();
        for i in 0..10 {
            let status = match i % 3 {
                0 => Status::Success,
                1 => Status::Error,
                2 => Status::Timeout,
                _ => unreachable!(),
            };
            hist.add(Duration::from_millis(i), status);
        }
        assert_eq!(hist.buckets, [1, 1, 2, 4, 2, 0]);
        assert_eq!(hist.min, 1_000_000); // 0 is ignored (clock artifact)
        assert_eq!(hist.max, 9_000_000); // 9 ms in nanos
        assert_eq!(hist.sum, 45_000_000_000); // 0+1+..+9 ms in nanos
        assert_eq!(hist.count, 10);
        assert_eq!(hist.errors, 3);
        assert_eq!(hist.timeouts, 3);

        hist.add(Duration::from_millis(42), Status::Success);
        assert_eq!(hist.buckets, [1, 1, 2, 4, 2, 1]);
    }

    #[test]
    fn test_histogram_merge() {
        let mut hist1 = Histogram::new();
        for i in 0..8 {
            let status = if i < 5 {
                Status::Success
            } else {
                Status::Timeout
            };
            hist1.add(Duration::from_millis(i), status);
        }

        let mut hist2 = Histogram::new();
        for i in 2..10 {
            let status = if i < 8 {
                Status::Success
            } else {
                Status::Error
            };
            hist2.add(Duration::from_millis(i), status);
        }

        hist1.merge(hist2);
        assert_eq!(hist1.buckets, [1, 1, 4, 8, 2, 0]);
        assert_eq!(hist1.min, 1_000_000); // 0 ignored; hist1 had 1..7, hist2 had 2..9
        assert_eq!(hist1.max, 9_000_000); // 9 ms in nanos
        assert_eq!(hist1.timeouts, 3);
        assert_eq!(hist1.errors, 2);
    }

    #[test]
    fn test_histogram_min_value() {
        let mut hist = Histogram::new();
        hist.add(Duration::ZERO, Status::Success);
        assert_eq!(hist.min(), u128::MAX);
        assert_eq!(hist.count(), 1);

        // Merge with histogram that has a zero min
        let mut hist2 = Histogram::new();
        hist2.add(Duration::ZERO, Status::Success);
        hist2.merge(hist);
        assert_eq!(hist.min(), u128::MAX, "when no sample recorded, expected would be MAX Bound value");

        // Merge with histogram that has a non-zero min: 0 should not overwrite.
        let mut hist3 = Histogram::new();
        hist3.add(Duration::from_millis(5), Status::Success);
        hist.merge(hist3);
        assert_eq!(hist.min(), 5000000, "after merge with 0 and 5ms, min should stay 5ms");
    }
}
