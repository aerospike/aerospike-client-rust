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
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use workers::Status;

// Number of buckets for latency histogram, e.g.
// 6 buckets => "<1ms", "<2ms", "<4ms", "<8ms", "<16ms", ">=16ms"
const HIST_BUCKETS: usize = 6;

lazy_static! {
    // How frequently histogram is printed
    pub static ref REPORT_MS: Duration = Duration::from_secs(1);
}

#[derive(Debug)]
pub struct Collector {
    receiver: Receiver<Histogram>,
    histogram: Histogram,
}

impl Collector {
    pub fn new(recv: Receiver<Histogram>) -> Self {
        Collector {
            receiver: recv,
            histogram: Histogram::new(),
        }
    }

    pub fn collect(mut self) {
        let mut last_report = Instant::now();
        for hist in self.receiver.iter() {
            self.histogram.merge(hist);
            if last_report.elapsed() > *REPORT_MS {
                self.report();
                last_report = Instant::now();
                self.histogram.reset();
            }
        }
        self.report();
        self.histogram.reset();
        self.summary();
    }

    fn report(&self) {
        let hist = self.histogram;
        let bkt = hist.latencies();
        println!(
            "TPS: {:>8.0},   Success: {:>8},   Timeouts: {:>8},   Errors: {:>8}",
            hist.tps(),
            hist.count(),
            hist.timeouts(),
            hist.errors()
        );
        println!(
            "Latency:    min      avg      max    |        < 1 ms        < 2 ms        < 4 \
             ms        < 8 ms       < 16 ms      >= 16 ms"
        );
        println!(
            "       {:>8.0} {:>8.0} {:>8.0} μs | {:>7}/{:>4.1}% {:>7}/{:>4.1}% \
             {:>7}/{:>4.1}% {:>7}/{:>4.1}% {:>7}/{:>4.1}% {:>7}/{:>4.1}%",
            hist.min(),
            hist.avg(),
            hist.max(),
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
    }

    fn summary(&self) {
        let hist = self.histogram;
        println!(
            "Total requests: {},   Elapsed time: {:.1}s,    TPS: {:.0}",
            hist.total(),
            hist.total_elapsed_as_secs(),
            hist.total_tps()
        )
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Histogram {
    buckets: [u128; HIST_BUCKETS],
    min: u128,
    max: u128,
    sum: u128,
    count: u128,
    timeouts: u128,
    errors: u128,
    interval: Instant,
    start: Instant,
    total: u128,
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

    pub fn total(&self) -> u128 {
        self.total
    }

    pub fn total_elapsed_as_secs(&self) -> f64 {
        let elapsed = self.start.elapsed();
        elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1_000_000_000.0
    }

    pub fn total_tps(&self) -> f64 {
        self.total as f64 / self.total_elapsed_as_secs()
    }

    pub fn add(&mut self, latency: Duration, status: Status) {
        let micros = latency.as_micros();
        if micros < self.min {
            self.min = micros;
        }

        if micros > self.max {
            self.max = micros;
        }

        self.count += 1;
        self.sum += micros;

        let mut upper = 1_000;
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
        self.min = if self.min < other.min {
            self.min
        } else {
            other.min
        };

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
        assert_eq!(hist.min, 0);
        assert_eq!(hist.max, 9);
        assert_eq!(hist.sum, 45);
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
        assert_eq!(hist1.min, 0);
        assert_eq!(hist1.max, 9);
        assert_eq!(hist1.timeouts, 3);
        assert_eq!(hist1.errors, 2);
    }
}
