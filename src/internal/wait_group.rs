
use std::sync::{Condvar, Mutex};

pub struct WaitGroup {
    count: Mutex<usize>,
    condv: Condvar,
}

impl WaitGroup {
    pub fn new(n: usize) -> WaitGroup {
        WaitGroup {
            count: Mutex::new(n),
            condv: Condvar::new(),
        }
    }

    pub fn add(&self, delta: usize) {
        let mut count = self.count.lock().unwrap();
        *count += delta;
        self.cond_notify(*count)
    }

    pub fn wait(&self) {
        let mut count = self.count.lock().unwrap();
        while *count > 0 {
            count = self.condv.wait(count).unwrap();
        }
    }

    pub fn done(&self) {
        let mut count = self.count.lock().unwrap();
        *count -= 1;
        self.cond_notify(*count)
    }

    fn cond_notify(&self, count: usize) {
        if count <= 0 {
            self.condv.notify_all();
        }
    }
}
