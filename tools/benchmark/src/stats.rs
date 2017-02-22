use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub struct Collector {
    receiver: Receiver<Histogram>,
    sender: Sender<Histogram>,
    histogram: Histogram,
}

impl Collector {
    pub fn new() -> Self {
        let (send, recv) = mpsc::channel();
        Collector {
            receiver: recv,
            sender: send,
            histogram: Histogram::new(),
        }
    }

    pub fn sender(&self) -> Sender<Histogram> {
        self.sender.clone()
    }

    pub fn collect(mut self) -> Histogram {
        drop(self.sender);
        for hist in self.receiver.iter() {
            self.histogram.merge(hist);
        }
        self.histogram
    }
}

const HIST_BUCKETS: usize = 6;

#[derive(Debug, Copy, Clone)]
pub struct Histogram {
    pub buckets: [u64; HIST_BUCKETS],
    pub min: u64,
    pub max: u64,
    pub timeouts: u64,
    pub errors: u64,
}

impl Histogram {
    pub fn new() -> Self {
        Histogram {
            buckets: [0; HIST_BUCKETS],
            min: u64::max_value(),
            max: u64::min_value(),
            timeouts: 0,
            errors: 0,
        }
    }

    pub fn add(&mut self, val: u64) {
        if val < self.min {
            self.min = val;
        }

        if val > self.max {
            self.max = val;
        }

        let mut l = 1;
        for bucket in &mut self.buckets {
            if val < l {
                *bucket += 1;
                return;
            }
            l <<= 1;
        }

        if let Some(last) = self.buckets.last_mut() {
            *last += 1;
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
        self.min = u64::max_value();
        self.max = u64::min_value();
        self.timeouts = 0;
        self.errors = 0;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_histogram_add() {
        let mut hist = Histogram::new();
        for i in 0..10 {
            hist.add(i);
        }
        assert_eq!(hist.buckets, [1, 1, 2, 4, 2, 0]);
        assert_eq!(hist.min, 0);
        assert_eq!(hist.max, 9);

        hist.add(42);
        assert_eq!(hist.buckets, [1, 1, 2, 4, 2, 1]);
    }

    #[test]
    fn test_histogram_merge() {
        let mut hist1 = Histogram::new();
        for i in 0..8 {
            hist1.add(i);
        }
        hist1.timeouts = 1;
        hist1.errors = 2;

        let mut hist2 = Histogram::new();
        for i in 2..10 {
            hist2.add(i);
        }
        hist2.timeouts = 3;
        hist2.errors = 4;

        hist1.merge(hist2);
        assert_eq!(hist1.buckets, [1, 1, 4, 8, 2, 0]);
        assert_eq!(hist1.min, 0);
        assert_eq!(hist1.max, 9);
        assert_eq!(hist1.timeouts, 4);
        assert_eq!(hist1.errors, 6);
    }
}
