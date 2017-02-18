use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use aerospike::Result as asResult;
use aerospike::Error as asError;
use aerospike::{ErrorKind, ResultCode};

#[derive(Debug)]
pub struct Counters {
    count: AtomicUsize,
    errors: AtomicUsize,
    timeouts: AtomicUsize,
    latency: AtomicUsize,
    start: Instant,
}

impl Counters {
    pub fn new() -> Self {
        Counters {
            count: AtomicUsize::new(0),
            errors: AtomicUsize::new(0),
            timeouts: AtomicUsize::new(0),
            latency: AtomicUsize::new(0),
            start: Instant::now(),
        }
    }

    fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    fn elapsed_millis(&self) -> f64 {
        let elapsed = self.elapsed();
        elapsed.as_secs() as f64 * 1_000.0 + elapsed.subsec_nanos() as f64 / 1_000_000.0
    }

    fn inc_count(&self) -> usize {
        self.count.fetch_add(1, Ordering::Relaxed)
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    fn inc_errors(&self) -> usize {
        self.errors.fetch_add(1, Ordering::Relaxed)
    }

    fn errors(&self) -> usize {
        self.errors.load(Ordering::Relaxed)
    }

    fn inc_timeouts(&self) -> usize {
        self.timeouts.fetch_add(1, Ordering::Relaxed)
    }

    fn timeouts(&self) -> usize {
        self.timeouts.load(Ordering::Relaxed)
    }

    fn add_latency(&self, elapsed: Duration) {
        let nanos = elapsed.as_secs() * 1_000_000_000 + elapsed.subsec_nanos() as u64;
        self.latency.fetch_add(nanos as usize, Ordering::Relaxed);
    }

    fn total_latency_millis(&self) -> f64 {
        self.latency.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    fn avg_latency_millis(&self) -> f64 {
        self.total_latency_millis() / self.count() as f64
    }

    pub fn track_request<F>(&self, req: F)
        where F: Fn() -> asResult<()>
    {
        let now = Instant::now();
        let res = req();
        self.add_latency(now.elapsed());
        self.inc_count();
        match res {
            Err(asError(ErrorKind::ServerError(ResultCode::Timeout), _)) => {
                self.inc_timeouts();
                ()
            }
            Err(_) => {
                self.inc_errors();
                ()
            }
            Ok(_) => {}
        }
    }
}

impl fmt::Display for Counters {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let count = self.count();
        let timeouts = self.timeouts();
        let errors = self.errors();
        let avg_ms = self.avg_latency_millis();
        let elapsed_ms = self.elapsed_millis();
        write!(f,
               "Commands: {}, Timeouts: {}, Errors: {}, Avg(ms): {}, Total elapsed time: {}",
               count,
               timeouts,
               errors,
               avg_ms,
               elapsed_ms)
    }
}
