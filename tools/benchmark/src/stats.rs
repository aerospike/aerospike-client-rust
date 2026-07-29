// Copyright 2015-2026 Aerospike, Inc.
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

//! Latency histograms and periodic reporting.
//!
//! Latency formats mirror the Java benchmark's `-latency` option:
//! the default is a fixed 6-bucket table, `<columns>,<shift>[,us|ms]`
//! prints cumulative percentages per power-of-two bucket (Aerospike
//! format), `alt,<columns>,<shift>[,us|ms]` prints counts and
//! percentages, and `ycsb[,<warmup>]` prints avg/min/max with running
//! 95th/99th percentiles.

use std::str::FromStr;
use std::time::{Duration, Instant};

use chrono::Local;
use tokio::sync::mpsc::Receiver;

use crate::workers::Status;

// Number of buckets for the default latency histogram, e.g.
// 6 buckets => "<1ms", "<2ms", "<4ms", "<8ms", "<16ms", ">=16ms"
const DEFAULT_HIST_BUCKETS: usize = 6;

// YCSB mode: 1 ms resolution buckets plus one overflow bucket.
const YCSB_BUCKETS: usize = 1001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportStyle {
    Pretty,
    Asbench,
}

/// Latency histogram layout/format, from `-l/--latency`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyMode {
    /// Fixed 6-bucket power-of-two milliseconds table.
    Default,
    /// Aerospike format: `columns` buckets, each threshold `shift` powers
    /// of two above the previous, printed as cumulative percentages.
    Aerospike {
        cols: usize,
        shift: u32,
        micros: bool,
    },
    /// Like `Aerospike` but prints `count:percent` per bucket plus a total.
    Alternate {
        cols: usize,
        shift: u32,
        micros: bool,
    },
    /// YCSB format: avg/min/max microseconds and running 95th/99th
    /// percentile milliseconds. `warmup` initial samples are excluded
    /// from the latency statistics.
    Ycsb { warmup: u64 },
}

impl LatencyMode {
    pub fn warmup(&self) -> u64 {
        match self {
            LatencyMode::Ycsb { warmup } => *warmup,
            _ => 0,
        }
    }

    fn bucket_count(&self) -> usize {
        match self {
            LatencyMode::Default => DEFAULT_HIST_BUCKETS,
            LatencyMode::Aerospike { cols, .. } | LatencyMode::Alternate { cols, .. } => *cols,
            LatencyMode::Ycsb { .. } => YCSB_BUCKETS,
        }
    }

    /// Bucket index for a latency sample.
    fn bucket_index(&self, latency: Duration) -> usize {
        match self {
            LatencyMode::Default => {
                let micros = latency.as_micros();
                let mut upper = 1_000u128;
                for i in 0..DEFAULT_HIST_BUCKETS {
                    if micros < upper || i == DEFAULT_HIST_BUCKETS - 1 {
                        return i;
                    }
                    upper <<= 1;
                }
                DEFAULT_HIST_BUCKETS - 1
            }
            LatencyMode::Aerospike { cols, shift, micros }
            | LatencyMode::Alternate { cols, shift, micros } => {
                let value = if *micros {
                    latency.as_micros()
                } else {
                    latency.as_millis()
                };
                // Bucket 0 = <= 1 unit; bucket i covers (limit_{i-1}, limit_i]
                // where limit_i = 1 << (i * shift); last bucket unbounded.
                let mut limit = 1u128;
                for i in 0..*cols {
                    if value <= limit || i == cols - 1 {
                        return i;
                    }
                    limit <<= shift;
                }
                cols - 1
            }
            LatencyMode::Ycsb { .. } => {
                (latency.as_millis() as usize).min(YCSB_BUCKETS - 1)
            }
        }
    }

    /// Column headers for the pretty/latency block.
    fn headers(&self) -> Vec<String> {
        match self {
            LatencyMode::Default => vec![
                "< 1 ms".into(),
                "< 2 ms".into(),
                "< 4 ms".into(),
                "< 8 ms".into(),
                "< 16 ms".into(),
                ">= 16 ms".into(),
            ],
            LatencyMode::Aerospike { cols, shift, micros }
            | LatencyMode::Alternate { cols, shift, micros } => {
                let unit = if *micros { "us" } else { "ms" };
                let mut headers = vec![format!("<={}{}", 1, unit)];
                let mut limit = 1u64;
                for _ in 1..*cols {
                    headers.push(format!(">{limit}{unit}"));
                    limit <<= shift;
                }
                headers
            }
            LatencyMode::Ycsb { .. } => vec![],
        }
    }
}

impl FromStr for LatencyMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split(',').map(str::trim).collect();
        match parts.first() {
            Some(&"ycsb") => {
                let warmup = match parts.get(1) {
                    Some(w) => w
                        .parse()
                        .map_err(|e| format!("Invalid ycsb warmup count `{w}`: {e}"))?,
                    None => 0,
                };
                if parts.len() > 2 {
                    return Err("latency: too many arguments for ycsb format".to_string());
                }
                Ok(LatencyMode::Ycsb { warmup })
            }
            Some(&"alt") => parse_columns_shift(&parts[1..], true),
            Some(_) => parse_columns_shift(&parts, false),
            None => Err("empty latency specification".to_string()),
        }
    }
}

fn parse_columns_shift(parts: &[&str], alternate: bool) -> Result<LatencyMode, String> {
    if parts.len() < 2 || parts.len() > 3 {
        return Err(
            "latency format: 'ycsb[,<warmup>]' or '[alt,]<columns>,<shift>[,us|ms]'".to_string(),
        );
    }
    let cols: usize = parts[0]
        .parse()
        .map_err(|e| format!("Invalid latency columns `{}`: {e}", parts[0]))?;
    let shift: u32 = parts[1]
        .parse()
        .map_err(|e| format!("Invalid latency shift `{}`: {e}", parts[1]))?;
    if !(2..=32).contains(&cols) {
        return Err("latency columns must be between 2 and 32".to_string());
    }
    if shift == 0 || shift > 8 {
        return Err("latency shift must be between 1 and 8".to_string());
    }
    let micros = match parts.get(2) {
        Some(&"us") => true,
        Some(&"ms") | None => false,
        Some(other) => return Err(format!("Invalid latency unit `{other}` (us or ms)")),
    };
    Ok(if alternate {
        LatencyMode::Alternate { cols, shift, micros }
    } else {
        LatencyMode::Aerospike { cols, shift, micros }
    })
}

/// Read, write and transaction histograms sent by workers each collection
/// interval. The `txn` histogram carries whole-transaction samples for the
/// TXN and MRT workloads.
#[derive(Debug, Clone)]
pub struct StatsPacket {
    pub read: Histogram,
    pub write: Histogram,
    pub txn: Histogram,
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
    txn_histogram: Histogram,
    report_style: ReportStyle,
    report_not_found: bool,
}

impl Collector {
    pub fn new(
        recv: Receiver<StatsPacket>,
        report: ReportStyle,
        latency_mode: LatencyMode,
        report_not_found: bool,
    ) -> Self {
        Collector {
            receiver: recv,
            read_histogram: Histogram::new(latency_mode),
            write_histogram: Histogram::new(latency_mode),
            txn_histogram: Histogram::new(latency_mode),
            report_style: report,
            report_not_found,
        }
    }

    fn merge(&mut self, packet: StatsPacket) {
        self.read_histogram.merge(&packet.read);
        self.write_histogram.merge(&packet.write);
        self.txn_histogram.merge(&packet.txn);
    }

    fn drain_and_merge(&mut self) {
        while let Ok(packet) = self.receiver.try_recv() {
            self.merge(packet);
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
                        Some(packet) => self.merge(packet),
                        None => break,
                    }
                }
                _ = report_interval.tick() => {
                    self.drain_and_merge();
                    self.report();
                    self.read_histogram.reset();
                    self.write_histogram.reset();
                    self.txn_histogram.reset();
                }
            }
        }
        self.drain_and_merge();
        self.read_histogram.reset();
        self.write_histogram.reset();
        self.txn_histogram.reset();
        self.summary();
    }

    fn report(&self) {
        match self.report_style {
            ReportStyle::Pretty => {
                Self::report_section("READ", &self.read_histogram, self.report_not_found);
                Self::report_section("WRITE", &self.write_histogram, false);
                if self.txn_histogram.count() > 0 {
                    Self::report_section("TXN", &self.txn_histogram, false);
                }
            }
            ReportStyle::Asbench => {
                self.report_asbench_tps();
                self.report_asbench_latency();
            }
        }
    }

    // Asbench/Java benchmark style:
    // <timestamp> write(tps=N timeouts=N errors=N) read(tps=N ... [nf=N]) [txns(...)] total(...)
    fn report_asbench_tps(&self) {
        let r = &self.read_histogram;
        let w = &self.write_histogram;
        let t = &self.txn_histogram;
        let write_tps = w.tps() as i64;
        let read_tps = r.tps() as i64;
        let total_tps = write_tps + read_tps;
        let total_timeouts = w.timeouts() + r.timeouts();
        let total_errors = w.errors() + r.errors();
        let nf = if self.report_not_found {
            format!(" nf={}", r.not_found())
        } else {
            String::new()
        };
        let txns = if t.count() > 0 {
            format!(
                " txns(tps={} timeouts={} errors={})",
                t.tps() as i64,
                t.timeouts(),
                t.errors()
            )
        } else {
            String::new()
        };
        println!(
            "{} write(tps={} timeouts={} errors={}) read(tps={} timeouts={} errors={}{}){} total(tps={} timeouts={} errors={})",
            Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            write_tps,
            w.timeouts(),
            w.errors(),
            read_tps,
            r.timeouts(),
            r.errors(),
            nf,
            txns,
            total_tps,
            total_timeouts,
            total_errors
        );
    }

    // Asbench-style latency line (C benchmark compatible): "HG: <op> <UTC> <period_sec>, <total_count>, <min_us>, <max_us>"
    fn report_asbench_latency(&self) {
        let period_sec = self.write_histogram.total_elapsed_as_secs() as i64;
        let utc = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        Self::report_asbench_latency_section("write", &self.write_histogram, &utc, period_sec);
        Self::report_asbench_latency_section("read", &self.read_histogram, &utc, period_sec);
    }

    fn report_asbench_latency_section(name: &str, hist: &Histogram, utc: &str, period_sec: i64) {
        let total_cnt = hist.count();
        let (min_us, max_us) = if total_cnt > 0 {
            let min_ns = hist.min();
            let max_ns = hist.max();

            let min_us = if min_ns == u128::MAX {
                0.001_f64
            } else {
                min_ns as f64 / 1_000.0
            };
            let max_us = max_ns as f64 / 1_000.0;
            (min_us, max_us)
        } else {
            (0.0, 0.0)
        };

        println!(
            "HG: {} {} {}, {}, {}, {}",
            name,
            utc,
            period_sec,
            total_cnt,
            (min_us + 0.5) as i64, // rounding to nearest microseconds
            (max_us + 0.5) as i64,
        );
    }

    fn report_section(label: &str, hist: &Histogram, show_not_found: bool) {
        let nf = if show_not_found {
            format!(",   NotFound: {:>8}", hist.not_found())
        } else {
            String::new()
        };
        println!(
            "--- {} ---\n  TPS: {:>8.0},   TOTAL_OPS: {:>8},   Timeouts: {:>8},   Errors: {:>8}{}",
            label,
            hist.tps(),
            hist.count(),
            hist.timeouts(),
            hist.errors(),
            nf
        );
        if hist.count() > 0 {
            hist.print_latency_block();
        } else {
            println!("  Latency: (no ops)");
        }
        println!();
    }

    fn summary(&self) {
        let read = &self.read_histogram;
        let write = &self.write_histogram;
        let txn = &self.txn_histogram;
        println!(
            "\nTotal read requests: {},   Total write requests: {}",
            read.total(),
            write.total()
        );
        if txn.total() > 0 {
            println!("Total transactions: {}", txn.total());
        }
        if self.report_not_found {
            println!("Total reads not found: {}", read.total_not_found());
        }
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

#[derive(Debug, Clone)]
pub struct Histogram {
    mode: LatencyMode,
    buckets: Vec<u64>,
    /// Cumulative buckets over the whole run (never reset); used for the
    /// YCSB running percentiles.
    run_buckets: Vec<u64>,
    min: u128,
    max: u128,
    sum: u128,
    count: u128,
    timeouts: u128,
    errors: u128,
    not_found: u128,
    interval: Instant,
    start: Instant,
    total: u128,
    total_timeouts: u128,
    total_errors: u128,
    total_not_found: u128,
    run_sum: u128,
}

impl Histogram {
    pub fn new(mode: LatencyMode) -> Self {
        let now = Instant::now();
        let n = mode.bucket_count();
        Histogram {
            mode,
            buckets: vec![0; n],
            run_buckets: vec![0; n],
            min: u128::MAX,
            max: u128::MIN,
            sum: 0,
            count: 0,
            timeouts: 0,
            errors: 0,
            not_found: 0,
            interval: now,
            start: now,
            total: 0,
            total_timeouts: 0,
            total_errors: 0,
            total_not_found: 0,
            run_sum: 0,
        }
    }

    pub fn min(&self) -> u128 {
        self.min
    }

    pub fn max(&self) -> u128 {
        self.max
    }

    pub fn avg(&self) -> u128 {
        if self.count == 0 {
            return 0;
        }
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

    pub fn not_found(&self) -> u128 {
        self.not_found
    }

    pub fn total(&self) -> u128 {
        self.total
    }

    pub fn total_timeouts(&self) -> u128 {
        self.total_timeouts
    }

    pub fn total_errors(&self) -> u128 {
        self.total_errors
    }

    pub fn total_not_found(&self) -> u128 {
        self.total_not_found
    }

    pub fn total_elapsed_as_secs(&self) -> f64 {
        let elapsed = self.start.elapsed();
        elapsed.as_secs() as f64 + f64::from(elapsed.subsec_nanos()) / 1_000_000_000.0
    }

    pub fn total_tps(&self) -> f64 {
        self.total as f64 / self.total_elapsed_as_secs()
    }

    /// Record a completed operation. `record_latency` is false while the
    /// YCSB warmup is active: the op still counts for throughput, but its
    /// latency is not sampled.
    pub fn add_sample(&mut self, latency: Duration, status: Status, record_latency: bool) {
        let nanos = latency.as_nanos();

        // min/max track every op with a measured latency, including
        // timeouts and errors.
        if record_latency {
            if nanos > 0 && nanos < self.min {
                self.min = nanos;
            }
            if nanos > self.max {
                self.max = nanos;
            }
        }

        match status {
            Status::Timeout => {
                self.timeouts += 1;
                return;
            }
            Status::Error => {
                self.errors += 1;
                return;
            }
            Status::NotFound => {
                self.not_found += 1;
                self.count += 1;
            }
            Status::Success => {
                self.count += 1;
            }
        }

        if !record_latency {
            return;
        }

        self.sum += nanos;
        self.run_sum += nanos;
        let idx = self.mode.bucket_index(latency);
        self.buckets[idx] += 1;
        self.run_buckets[idx] += 1;
    }

    #[cfg(test)]
    pub fn add(&mut self, latency: Duration, status: Status) {
        self.add_sample(latency, status, true);
    }

    // Merges interval counts from `other` into `self`.
    // Intended for interval histograms only
    pub fn merge(&mut self, other: &Histogram) {
        if other.min != u128::MAX && other.min != 0 {
            self.min = if self.min == u128::MAX {
                other.min
            } else {
                self.min.min(other.min)
            };
        }

        self.max = self.max.max(other.max);
        self.count += other.count;
        self.sum += other.sum;
        self.run_sum += other.sum;

        for (s, o) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *s += *o;
        }
        for (s, o) in self.run_buckets.iter_mut().zip(other.buckets.iter()) {
            *s += *o;
        }

        self.timeouts += other.timeouts;
        self.errors += other.errors;
        self.not_found += other.not_found;
    }

    pub fn reset(&mut self) {
        for bucket in &mut self.buckets {
            *bucket = 0;
        }
        self.total += self.count;
        self.total_timeouts += self.timeouts;
        self.total_errors += self.errors;
        self.total_not_found += self.not_found;
        self.min = u128::MAX;
        self.max = u128::MIN;
        self.sum = 0;
        self.count = 0;
        self.timeouts = 0;
        self.errors = 0;
        self.not_found = 0;
        self.interval = Instant::now();
    }

    fn interval_as_secs(&self) -> f64 {
        let elapsed = self.interval.elapsed();
        elapsed.as_secs() as f64 + f64::from(elapsed.subsec_nanos()) / 1_000_000_000.0
    }

    /// Running percentile (over the whole run) in bucket units.
    fn run_percentile(&self, pct: f64) -> usize {
        let total: u64 = self.run_buckets.iter().sum();
        if total == 0 {
            return 0;
        }
        let threshold = (total as f64 * pct / 100.0).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, c) in self.run_buckets.iter().enumerate() {
            cumulative += c;
            if cumulative >= threshold {
                return i;
            }
        }
        self.run_buckets.len() - 1
    }

    fn print_latency_block(&self) {
        let sampled: u64 = self.buckets.iter().sum();
        match self.mode {
            LatencyMode::Default | LatencyMode::Aerospike { .. } => {
                let headers = self.mode.headers();
                let min_ms = if self.min == u128::MAX {
                    0.001_f64
                } else {
                    self.min as f64 / 1_000_000.0
                };
                let header_row: Vec<String> =
                    headers.iter().map(|h| format!("{h:>13}")).collect();
                println!(
                    "  Latency:     min      avg      max    | {}",
                    header_row.join(" ")
                );
                let cells: Vec<String> = match self.mode {
                    // Default: per-bucket count/percent.
                    LatencyMode::Default => self
                        .buckets
                        .iter()
                        .map(|&c| {
                            let pct = if sampled > 0 {
                                c as f64 / sampled as f64 * 100.0
                            } else {
                                0.0
                            };
                            format!("{:>7}/{:>4.1}%", c, pct)
                        })
                        .collect(),
                    // Aerospike: column 0 is the <=1 unit share; every other
                    // column is the cumulative percentage over its threshold.
                    _ => (0..self.buckets.len())
                        .map(|i| {
                            let c: u64 = if i == 0 {
                                self.buckets[0]
                            } else {
                                self.buckets[i..].iter().sum()
                            };
                            let pct = if sampled > 0 {
                                c as f64 / sampled as f64 * 100.0
                            } else {
                                0.0
                            };
                            format!("{pct:>12.2}%")
                        })
                        .collect(),
                };
                println!(
                    "         {:>8.3} {:>8.3} {:>8.3} ms | {}",
                    min_ms,
                    self.avg() as f64 / 1_000_000.0,
                    self.max as f64 / 1_000_000.0,
                    cells.join(" ")
                );
            }
            LatencyMode::Alternate { .. } => {
                let headers = self.mode.headers();
                let cells: Vec<String> = self
                    .buckets
                    .iter()
                    .zip(headers.iter())
                    .map(|(&c, h)| {
                        let pct = if sampled > 0 {
                            c as f64 / sampled as f64 * 100.0
                        } else {
                            0.0
                        };
                        format!("{h}({c}:{pct:.2}%)")
                    })
                    .collect();
                println!("  Latency: {} total({sampled})", cells.join(" "));
            }
            LatencyMode::Ycsb { .. } => {
                let min_us = if self.min == u128::MAX {
                    0
                } else {
                    (self.min / 1_000) as u64
                };
                println!(
                    "  Latency: avg={}us min={}us max={}us | run p95={}ms p99={}ms",
                    self.avg() / 1_000,
                    min_us,
                    self.max / 1_000,
                    self.run_percentile(95.0),
                    self.run_percentile(99.0),
                );
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_histogram_add() {
        let mut hist = Histogram::new(LatencyMode::Default);
        for i in 0..10 {
            let status = match i % 3 {
                0 => Status::Success,
                1 => Status::Error,
                2 => Status::Timeout,
                _ => unreachable!(),
            };
            hist.add(Duration::from_millis(i), status);
        }
        assert_eq!(hist.buckets, [1, 0, 1, 1, 1, 0]);
        assert_eq!(hist.min, 1_000_000); // 0 is ignored (clock artifact)
        assert_eq!(hist.max, 9_000_000); // 9 ms in nanos
        assert_eq!(hist.sum, 18000000); // 0+1+..+9 ms in nanos
        assert_eq!(hist.count, 4);
        assert_eq!(hist.errors, 3);
        assert_eq!(hist.timeouts, 3);

        hist.add(Duration::from_millis(42), Status::Success);
        assert_eq!(hist.buckets, [1, 0, 1, 1, 1, 1]);
    }

    #[test]
    fn test_histogram_merge() {
        let mut hist1 = Histogram::new(LatencyMode::Default);
        for i in 0..8 {
            let status = if i < 5 {
                Status::Success
            } else {
                Status::Timeout
            };
            hist1.add(Duration::from_millis(i), status);
        }

        let mut hist2 = Histogram::new(LatencyMode::Default);
        for i in 2..10 {
            let status = if i < 8 {
                Status::Success
            } else {
                Status::Error
            };
            hist2.add(Duration::from_millis(i), status);
        }

        hist1.merge(&hist2);
        assert_eq!(hist1.buckets, [1, 1, 4, 5, 0, 0]);
        assert_eq!(hist1.min, 1_000_000); // 0 ignored; hist1 had 1..7, hist2 had 2..9
        assert_eq!(hist1.max, 9_000_000); // 9 ms in nanos
        assert_eq!(hist1.timeouts, 3);
        assert_eq!(hist1.errors, 2);
    }

    #[test]
    fn not_found_counts_separately_and_as_success() {
        let mut hist = Histogram::new(LatencyMode::Default);
        hist.add(Duration::from_millis(1), Status::NotFound);
        hist.add(Duration::from_millis(1), Status::Success);
        assert_eq!(hist.count(), 2);
        assert_eq!(hist.not_found(), 1);
        assert_eq!(hist.errors(), 0);
    }

    #[test]
    fn latency_mode_parsing() {
        assert_eq!(
            LatencyMode::from_str("7,1").unwrap(),
            LatencyMode::Aerospike {
                cols: 7,
                shift: 1,
                micros: false
            }
        );
        assert_eq!(
            LatencyMode::from_str("alt,4,3,us").unwrap(),
            LatencyMode::Alternate {
                cols: 4,
                shift: 3,
                micros: true
            }
        );
        assert_eq!(
            LatencyMode::from_str("ycsb").unwrap(),
            LatencyMode::Ycsb { warmup: 0 }
        );
        assert_eq!(
            LatencyMode::from_str("ycsb,5000").unwrap(),
            LatencyMode::Ycsb { warmup: 5000 }
        );
        assert!(LatencyMode::from_str("1,1").is_err());
        assert!(LatencyMode::from_str("7,0").is_err());
        assert!(LatencyMode::from_str("7,1,parsecs").is_err());
    }

    #[test]
    fn aerospike_buckets() {
        // 4 cols, shift 3 (ms): thresholds 1, 8, 64.
        let mode = LatencyMode::Aerospike {
            cols: 4,
            shift: 3,
            micros: false,
        };
        assert_eq!(mode.bucket_index(Duration::from_millis(1)), 0);
        assert_eq!(mode.bucket_index(Duration::from_millis(2)), 1);
        assert_eq!(mode.bucket_index(Duration::from_millis(8)), 1);
        assert_eq!(mode.bucket_index(Duration::from_millis(9)), 2);
        assert_eq!(mode.bucket_index(Duration::from_millis(64)), 2);
        assert_eq!(mode.bucket_index(Duration::from_millis(65)), 3);
        assert_eq!(mode.bucket_index(Duration::from_secs(100)), 3);
        assert_eq!(
            mode.headers(),
            vec!["<=1ms", ">1ms", ">8ms", ">64ms"]
        );
    }

    #[test]
    fn ycsb_percentiles() {
        let mut hist = Histogram::new(LatencyMode::Ycsb { warmup: 0 });
        for i in 1..=100u64 {
            hist.add(Duration::from_millis(i), Status::Success);
        }
        assert_eq!(hist.run_percentile(95.0), 95);
        assert_eq!(hist.run_percentile(99.0), 99);
        assert_eq!(hist.run_percentile(50.0), 50);
    }

    #[test]
    fn warmup_skips_latency_but_counts_op() {
        let mut hist = Histogram::new(LatencyMode::Ycsb { warmup: 10 });
        hist.add_sample(Duration::from_millis(5), Status::Success, false);
        assert_eq!(hist.count(), 1);
        assert_eq!(hist.sum, 0);
        let sampled: u64 = hist.buckets.iter().sum();
        assert_eq!(sampled, 0);
    }
}
