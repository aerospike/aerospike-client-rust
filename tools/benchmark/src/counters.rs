use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct Counters {
    count: AtomicUsize,
    elapsed: AtomicUsize,
}

impl Counters {
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn inc_count(&self) -> usize {
        self.count.fetch_add(1, Ordering::Relaxed)
    }

    pub fn elapsed(&self) -> usize {
        self.elapsed.load(Ordering::Relaxed)
    }

    pub fn elapsed_as_millis(&self) -> usize {
        self.elapsed() / 1_000_000
    }

    pub fn add_elapsed(&self, elapsed: Duration) {
        let nanos = elapsed.as_secs() * 1_000_000_000 + elapsed.subsec_nanos() as u64;
        self.elapsed.fetch_add(nanos as usize, Ordering::Relaxed);
    }
}
