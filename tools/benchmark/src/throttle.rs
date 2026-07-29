// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0

//! Run-wide pacing and stop conditions shared by all workers.
//!
//! Mirrors the Java benchmark: `-g/--throughput` caps the aggregate
//! transactions per second (workers that exceed the current one-second
//! period's budget sleep until the next period), and
//! `--transactions` stops the whole run after a total number of
//! transactions.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct RunControl {
    /// Aggregate target transactions/second; 0 = unlimited.
    target_tps: u64,
    /// Total transactions to run; 0 = unlimited.
    txn_limit: u64,
    /// Transactions counted this one-second period (throttling).
    period_count: AtomicU64,
    /// Index of the current one-second period since `epoch`.
    period: AtomicU64,
    /// Total transactions across the whole run (limit enforcement).
    total_count: AtomicU64,
    stop: AtomicBool,
    epoch: Instant,
}

impl RunControl {
    pub fn new(target_tps: u64, txn_limit: u64) -> Arc<Self> {
        Arc::new(RunControl {
            target_tps,
            txn_limit,
            period_count: AtomicU64::new(0),
            period: AtomicU64::new(0),
            total_count: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            epoch: Instant::now(),
        })
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// Record `n` completed transactions. Enforces the total-transaction
    /// limit and, when a target TPS is set, sleeps out the remainder of the
    /// current one-second period once the aggregate budget is spent.
    pub async fn pace(&self, n: u64) {
        if self.txn_limit > 0
            && self.total_count.fetch_add(n, Ordering::Relaxed) + n >= self.txn_limit
        {
            self.stop();
            return;
        }

        if self.target_tps == 0 {
            return;
        }

        let now_ms = self.epoch.elapsed().as_millis() as u64;
        let current_period = now_ms / 1000;
        let seen_period = self.period.load(Ordering::Relaxed);
        if current_period != seen_period {
            // First worker to cross the boundary resets the period budget.
            if self
                .period
                .compare_exchange(
                    seen_period,
                    current_period,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                self.period_count.store(0, Ordering::Relaxed);
            }
        }

        let used = self.period_count.fetch_add(n, Ordering::Relaxed) + n;
        if used >= self.target_tps {
            // Budget spent: sleep until the next period starts.
            let next_period_ms = (current_period + 1) * 1000;
            let remaining = next_period_ms.saturating_sub(self.epoch.elapsed().as_millis() as u64);
            if remaining > 0 {
                tokio::time::sleep(Duration::from_millis(remaining)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn txn_limit_stops_the_run() {
        let ctl = RunControl::new(0, 10);
        for _ in 0..9 {
            ctl.pace(1).await;
        }
        assert!(!ctl.stopped());
        ctl.pace(1).await;
        assert!(ctl.stopped());
    }

    #[tokio::test]
    async fn unlimited_never_stops_or_sleeps() {
        let ctl = RunControl::new(0, 0);
        let start = Instant::now();
        for _ in 0..1000 {
            ctl.pace(1).await;
        }
        assert!(!ctl.stopped());
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn throttle_paces_to_target() {
        // 100 TPS target: 250 ops must take at least ~2 seconds.
        let ctl = RunControl::new(100, 0);
        let start = Instant::now();
        for _ in 0..250 {
            ctl.pace(1).await;
        }
        assert!(
            start.elapsed() >= Duration::from_millis(1900),
            "elapsed: {:?}",
            start.elapsed()
        );
    }
}
