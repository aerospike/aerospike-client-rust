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

//! Per-cluster tiered buffer pool for large command buffers.
//!
//! Port of the Go client's `types/pool.TieredBufferPool`: buffers are kept
//! in power-of-two size classes between [`DEFAULT_MIN_POOLED_SIZE`] and
//! [`DEFAULT_MAX_POOLED_SIZE`] (sizing configurable per client through
//! `ClientPolicy::buffer_pool_*`); requests above the maximum are freshly
//! allocated and never retained, and only exact power-of-two capacities
//! are accepted back. Each `Cluster` owns its own pool (shared by all of
//! that client's connections), so clients are fully isolated from each
//! other.
//!
//! Go gets eviction for free: `sync.Pool` contents age out across garbage
//! collector cycles, so big idle buffers are reclaimed automatically. Rust
//! has no GC, so this pool replicates the same *victim cache* mechanism
//! explicitly: each tier holds a `current` and a `victim` generation, and
//! [`age_if_due`] — driven by the cluster tend loop — periodically drops
//! the victim generation and demotes `current` into it. A buffer that
//! keeps being reused migrates back into `current` and lives on; a buffer
//! from a one-off burst survives at most one aging interval after its last
//! use. Unlike `sync.Pool`, retention is strictly bounded: each tier caps
//! its slot count, giving a hard worst-case footprint regardless of load.
//! When the owning cluster is dropped, its pool (and everything retained
//! in it) is freed with it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::policy::ClientPolicy;

/// Default smallest pooled buffer (the Go client's `MinBufferSize`).
/// Configurable via `ClientPolicy::buffer_pool_min_size`.
pub const DEFAULT_MIN_POOLED_SIZE: usize = 8 * 1024;

/// Default largest pooled buffer (the Go client's `PoolCutOffBufferSize`);
/// larger buffers are allocated fresh and dropped on return. Configurable
/// via `ClientPolicy::buffer_pool_max_size`.
pub const DEFAULT_MAX_POOLED_SIZE: usize = 1024 * 1024;

/// Default per-tier retention budget in bytes; each tier keeps at most
/// `tier_bytes / tier_size` buffers (clamped to [2, 64]). Configurable via
/// `ClientPolicy::buffer_pool_tier_bytes`.
pub const DEFAULT_PER_TIER_BYTES: usize = 4 * 1024 * 1024;

/// How often the victim generation is dropped. Two intervals of disuse
/// therefore reclaim a buffer completely.
const AGE_INTERVAL: Duration = Duration::from_secs(30);

/// One power-of-two size class with its two generations.
struct Tier {
    size: usize,
    max_slots: usize,
    generations: Mutex<Generations>,
}

#[derive(Default)]
struct Generations {
    /// Buffers used (or returned) since the last aging pass.
    current: Vec<Vec<u8>>,
    /// Buffers that were `current` one aging pass ago; freed on the next.
    victim: Vec<Vec<u8>>,
}

impl std::fmt::Debug for TieredBufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredBufferPool")
            .field("min", &self.min)
            .field("max", &self.max)
            .field("tiers", &self.tiers.len())
            .field("retained_bytes", &self.retained_bytes())
            .finish()
    }
}

pub struct TieredBufferPool {
    min: usize,
    max: usize,
    tiers: Vec<Tier>,
    /// Aging-throttle state: creation instant and the last aging pass
    /// (milliseconds since `epoch`).
    epoch: Instant,
    last_age_ms: AtomicU64,
}

impl TieredBufferPool {
    /// The owning cluster's pool, sized from its client policy; `None`
    /// when pooling is disabled.
    pub fn from_policy(policy: &ClientPolicy) -> Option<Arc<Self>> {
        policy.use_buffer_pool.then(|| {
            Arc::new(Self::new(
                policy.buffer_pool_min_size,
                policy.buffer_pool_max_size,
                policy.buffer_pool_tier_bytes,
            ))
        })
    }

    fn new(min: usize, max: usize, tier_bytes: usize) -> Self {
        assert!(min.is_power_of_two() && max.is_power_of_two() && min <= max && tier_bytes > 0);
        let mut tiers = Vec::new();
        let mut size = min;
        while size <= max {
            tiers.push(Tier {
                size,
                max_slots: (tier_bytes / size).clamp(2, 64),
                generations: Mutex::new(Generations::default()),
            });
            size <<= 1;
        }
        TieredBufferPool {
            min,
            max,
            tiers,
            epoch: Instant::now(),
            last_age_ms: AtomicU64::new(0),
        }
    }

    /// Tier index for a buffer of `size` bytes, or None outside the range.
    fn tier_index(&self, size: usize) -> Option<usize> {
        if size > self.max {
            return None;
        }
        let class = size.max(self.min).next_power_of_two();
        Some((class.ilog2() - self.min.ilog2()) as usize)
    }

    /// Return a buffer with `len == 0` and `capacity >= size`. Pooled
    /// buffers always have exact power-of-two capacities; requests above
    /// the pooled maximum get a fresh, never-retained allocation.
    pub(crate) fn get(&self, size: usize) -> Vec<u8> {
        let Some(idx) = self.tier_index(size) else {
            return Vec::with_capacity(size);
        };
        let tier = &self.tiers[idx];
        let reused = {
            let mut generations = tier
                .generations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            generations
                .current
                .pop()
                .or_else(|| generations.victim.pop())
        };
        reused.unwrap_or_else(|| Vec::with_capacity(tier.size))
    }

    /// Give a buffer back. Only exact power-of-two capacities within the
    /// pooled range are retained (and only while the tier has free slots);
    /// everything else is simply dropped, like the Go implementation.
    pub(crate) fn put(&self, mut buf: Vec<u8>) {
        let capacity = buf.capacity();
        if !capacity.is_power_of_two() || capacity < self.min || capacity > self.max {
            return;
        }
        let Some(idx) = self.tier_index(capacity) else {
            return;
        };
        buf.clear();
        let tier = &self.tiers[idx];
        let mut generations = tier
            .generations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if generations.current.len() + generations.victim.len() < tier.max_slots {
            generations.current.push(buf);
        }
    }

    /// Run an aging pass if [`AGE_INTERVAL`] has elapsed since the last
    /// one. Called from the owning cluster's tend loop; the CAS throttle
    /// keeps the cadence independent of the tend frequency.
    pub fn age_if_due(&self) {
        let now_ms = self.epoch.elapsed().as_millis() as u64;
        let last = self.last_age_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < AGE_INTERVAL.as_millis() as u64 {
            return;
        }
        if self
            .last_age_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            self.age();
        }
    }

    /// One aging pass: free the victim generation, demote `current`.
    pub(crate) fn age(&self) {
        for tier in &self.tiers {
            let mut generations = tier
                .generations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            generations.victim = std::mem::take(&mut generations.current);
        }
    }

    /// Bytes currently retained across all tiers (diagnostics/tests).
    #[allow(dead_code)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.tiers
            .iter()
            .map(|t| {
                let generations = t
                    .generations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (generations.current.len() + generations.victim.len()) * t.size
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> TieredBufferPool {
        TieredBufferPool::new(8 * 1024, 1024 * 1024, DEFAULT_PER_TIER_BYTES)
    }

    #[test]
    fn size_classes() {
        let p = pool();
        assert_eq!(p.get(1).capacity(), 8 * 1024); // clamped up to min
        assert_eq!(p.get(8 * 1024).capacity(), 8 * 1024);
        assert_eq!(p.get(8 * 1024 + 1).capacity(), 16 * 1024);
        assert_eq!(p.get(9_000).capacity(), 16 * 1024);
        assert_eq!(p.get(1024 * 1024).capacity(), 1024 * 1024);
        // Above the cutoff: exact-size fresh allocation.
        assert_eq!(p.get(2 * 1024 * 1024).capacity(), 2 * 1024 * 1024);
    }

    #[test]
    fn reuses_returned_buffers() {
        let p = pool();
        let buf = p.get(100_000);
        assert_eq!(buf.capacity(), 128 * 1024);
        let ptr = buf.as_ptr();
        p.put(buf);
        let again = p.get(100_000);
        assert_eq!(again.as_ptr(), ptr, "expected the same allocation back");
        assert_eq!(again.len(), 0, "pooled buffers come back empty");
    }

    #[test]
    fn oversized_and_odd_capacities_are_dropped() {
        let p = pool();
        // Over the cutoff: never retained. (No pointer check — the
        // allocator may legitimately hand the same block back for the
        // fresh allocation.)
        let big = p.get(2 * 1024 * 1024);
        p.put(big);
        assert_eq!(p.retained_bytes(), 0);

        // Non-power-of-two capacity: dropped.
        p.put(Vec::with_capacity(10_000));
        assert_eq!(p.retained_bytes(), 0);

        // Below the minimum: dropped.
        p.put(Vec::with_capacity(1024));
        assert_eq!(p.retained_bytes(), 0);
    }

    #[test]
    fn tier_capacity_is_bounded() {
        let p = pool();
        // 1 MiB tier keeps at most PER_TIER_BYTES / 1 MiB = 4 buffers.
        for _ in 0..10 {
            p.put(Vec::with_capacity(1024 * 1024));
        }
        assert_eq!(p.retained_bytes(), 4 * 1024 * 1024);
    }

    #[test]
    fn aging_uses_a_victim_generation() {
        let p = pool();
        let buf = p.get(64 * 1024);
        let ptr = buf.as_ptr();
        p.put(buf);

        // One aging pass: buffer moves to the victim generation but is
        // still reusable.
        p.age();
        let survivor = p.get(64 * 1024);
        assert_eq!(survivor.as_ptr(), ptr);
        p.put(survivor);

        // Two consecutive passes without use: freed.
        p.age();
        p.age();
        assert_eq!(p.retained_bytes(), 0);
    }

    #[test]
    fn custom_tier_budget_bounds_slots() {
        // 64 KiB budget per tier.
        let p = TieredBufferPool::new(8 * 1024, 64 * 1024, 64 * 1024);
        // 64K tier: budget/size = 1, clamped up to 2 slots.
        for _ in 0..5 {
            p.put(Vec::with_capacity(64 * 1024));
        }
        assert_eq!(p.retained_bytes(), 2 * 64 * 1024);
        // 8K tier: budget/size = 8 slots.
        for _ in 0..20 {
            p.put(Vec::with_capacity(8 * 1024));
        }
        assert_eq!(p.retained_bytes(), 2 * 64 * 1024 + 8 * 8 * 1024);
    }

    #[test]
    fn age_if_due_is_throttled() {
        // A fresh pool ages at most once within the interval: repeated
        // calls must not run additional passes.
        let p = pool();
        let buf = p.get(64 * 1024);
        p.put(buf);
        p.age_if_due(); // may or may not run a pass, depending on timing
        let retained_after_first = p.retained_bytes();
        for _ in 0..100 {
            p.age_if_due();
        }
        // Without the throttle, 100 passes would have freed everything
        // through the victim generation.
        assert_eq!(p.retained_bytes(), retained_after_first);
    }

    #[test]
    fn from_policy_respects_the_toggle() {
        let mut policy = ClientPolicy::default();
        assert!(TieredBufferPool::from_policy(&policy).is_some());
        policy.use_buffer_pool = false;
        assert!(TieredBufferPool::from_policy(&policy).is_none());
    }
}
