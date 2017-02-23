use std::time::{Duration, Instant};
use std::sync::mpsc::Receiver;
use std::f64;

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
                self.histogram.reset()
            }
        }
        self.report();
    }

    fn report(&self) {
        let hist = self.histogram;
        let count = hist.count;
        let bkt = hist.buckets;
        let pct: Vec<f64> = bkt.iter().map(|&b| b as f64 / count as f64 * 100.0).collect();
        println!("TPS: {:.0}, Timeouts: {}, Errors: {}",
                 hist.tps(),
                 hist.timeouts,
                 hist.errors);
        println!("{:>8.3} {:>8.3} {:>8.3} | {:>7}/{:>4.1}% {:>7}/{:>4.1}% {:>7}/{:>4.1}% \
                  {:>7}/{:>4.1}% {:>7}/{:>4.1}% {:>7}/{:>4.1}%",
                 hist.min(),
                 hist.avg(),
                 hist.max(),
                 bkt[0],
                 pct[0],
                 bkt[1],
                 pct[1],
                 bkt[2],
                 pct[2],
                 bkt[3],
                 pct[3],
                 bkt[4],
                 pct[4],
                 bkt[5],
                 pct[5]);
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Histogram {
    pub buckets: [u64; HIST_BUCKETS],
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub count: u64,
    pub timeouts: u64,
    pub errors: u64,
    start: Instant,
}

impl Histogram {
    pub fn new() -> Self {
        Histogram {
            buckets: [0; HIST_BUCKETS],
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
            count: 0,
            timeouts: 0,
            errors: 0,
            start: Instant::now(),
        }
    }

    pub fn add(&mut self, latency: Duration, status: Status) {
        let millis = latency.as_millis();
        if millis < self.min {
            self.min = millis;
        }

        if millis > self.max {
            self.max = millis;
        }

        self.count += 1;
        self.sum += millis;

        let mut upper = 1;
        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            if ((millis as u64) < upper) || (i == HIST_BUCKETS - 1) {
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
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        self.sum = 0.0;
        self.count = 0;
        self.timeouts = 0;
        self.errors = 0;
        self.start = Instant::now();
    }

    pub fn tps(&self) -> f64 {
        self.count as f64 / self.elapsed_as_secs()
    }

    pub fn min(&self) -> f64 {
        self.min
    }

    pub fn max(&self) -> f64 {
        self.max
    }

    pub fn avg(&self) -> f64 {
        self.sum / self.count as f64
    }

    fn elapsed_as_secs(&self) -> f64 {
        let elapsed = self.start.elapsed();
        elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1_000_000_000.0
    }
}

trait DurationExt {
    fn as_millis(&self) -> f64;
}

impl DurationExt for Duration {
    fn as_millis(&self) -> f64 {
        let secs = self.as_secs() as f64;
        let nanos = self.subsec_nanos() as f64;
        secs * 1_000.0 + nanos / 1_000_000.0
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
        assert_eq!(hist.min, 0.0);
        assert_eq!(hist.max, 9.0);
        assert_eq!(hist.sum, 45.0);
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
        assert_eq!(hist1.min, 0.0);
        assert_eq!(hist1.max, 9.0);
        assert_eq!(hist1.timeouts, 3);
        assert_eq!(hist1.errors, 2);
    }
}
