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

//! Peers that failed validation, retried off the tend.
//!
//! A peer's first validation runs in the tend. Once it fails, later attempts
//! run as background tasks, a fresh one each tend, so an attempt stuck on a
//! connection that is never answered does not hold back the tend or the
//! attempt after it. The tend takes up a result on its next pass; node
//! additions stay in the tend.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aerospike_rt::time::{Duration, Instant};
use futures::channel::oneshot;
use futures::future::{self, Either, FutureExt, Shared};

use super::node_validator::NodeValidator;
use crate::locks::lock;
use crate::net::Host;

/// Attempts one peer may have in flight. An attempt lives until its connect
/// or info timeout, so a peer that accepts connections and never answers
/// would otherwise gather one more attempt every tend.
const MAX_IN_FLIGHT: usize = 4;

/// A peer host that validated, with its validator.
pub type Found = (Host, NodeValidator);

/// What the tend does with a peer this pass.
pub enum Retry {
    /// The peer has not failed: validate it in the tend.
    First,
    /// A background attempt validated the peer.
    Found(Box<Found>),
    /// The peer is still being retried in the background.
    Pending,
}

/// One background attempt against a peer. Dropping it frees its in-flight
/// slot.
pub struct Attempt {
    in_flight: Arc<AtomicUsize>,
    found: Arc<Mutex<Option<Found>>>,
    stop: Shared<oneshot::Receiver<()>>,
}

impl Attempt {
    /// Runs `validate` until it finishes or the peer stops being retried,
    /// keeping what it found unless an earlier attempt already validated the
    /// peer. Returns whether the result was kept.
    pub async fn run(self, validate: impl Future<Output = Option<Found>>) -> bool {
        let stop = self.stop.clone();
        match future::select(std::pin::pin!(validate), stop).await {
            Either::Left((Some(found), _)) => {
                let mut slot = lock(&self.found);
                if slot.is_some() {
                    return false;
                }
                *slot = Some(found);
                true
            }
            _ => false,
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Entry {
    /// The peer was materialized since the last prune.
    seen: bool,
    /// When the last attempt started, or the first validation failed.
    started: Instant,
    in_flight: Arc<AtomicUsize>,
    found: Arc<Mutex<Option<Found>>>,
    /// Dropped with the entry, which stops every attempt in flight.
    _stop: oneshot::Sender<()>,
    stopped: Shared<oneshot::Receiver<()>>,
}

/// The peers being retried in the background, by node name.
#[derive(Default)]
pub struct PeerRetries {
    entries: Mutex<HashMap<String, Entry>>,
}

impl fmt::Debug for PeerRetries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let peers: Vec<String> = lock(&self.entries).keys().cloned().collect();
        f.debug_struct("PeerRetries")
            .field("peers", &peers)
            .finish()
    }
}

impl PeerRetries {
    /// The state of `peer` for this pass. When the peer is still pending and
    /// its last attempt started at least `interval` ago, `spawn` starts the
    /// next one.
    pub fn next(&self, peer: &str, interval: Duration, spawn: impl FnOnce(Attempt)) -> Retry {
        let mut entries = lock(&self.entries);
        let Some(entry) = entries.get_mut(peer) else {
            return Retry::First;
        };
        entry.seen = true;
        let found = lock(&entry.found).take();
        if let Some(found) = found {
            entries.remove(peer);
            return Retry::Found(Box::new(found));
        }
        if entry.started.elapsed() >= interval
            && entry.in_flight.load(Ordering::Relaxed) < MAX_IN_FLIGHT
        {
            entry.in_flight.fetch_add(1, Ordering::Relaxed);
            entry.started = Instant::now();
            spawn(Attempt {
                in_flight: entry.in_flight.clone(),
                found: entry.found.clone(),
                stop: entry.stopped.clone(),
            });
        }
        Retry::Pending
    }

    /// Records that `peer`'s validation in the tend failed, so its next
    /// attempt runs in the background.
    pub fn failed(&self, peer: &str) {
        lock(&self.entries)
            .entry(peer.to_string())
            .or_insert_with(|| {
                let (stop, stopped) = oneshot::channel();
                Entry {
                    seen: true,
                    started: Instant::now(),
                    in_flight: Arc::new(AtomicUsize::new(0)),
                    found: Arc::new(Mutex::new(None)),
                    _stop: stop,
                    stopped: stopped.shared(),
                }
            });
    }

    /// Drops the peers not materialized since the last prune: no node
    /// advertises them any more, or they joined another way.
    pub fn prune(&self) {
        lock(&self.entries).retain(|_, entry| std::mem::take(&mut entry.seen));
    }

    /// Stops every attempt in flight.
    pub fn clear(&self) {
        lock(&self.entries).clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn found() -> Found {
        (
            Host::new("127.0.0.1", 3000),
            NodeValidator::new(crate::ClientPolicy::default()),
        )
    }

    #[aerospike_macro::test]
    async fn a_peer_that_never_failed_is_validated_in_the_tend() {
        let retries = PeerRetries::default();
        let retry = retries.next("BB9A", Duration::ZERO, |_| panic!("nothing to retry"));
        assert!(matches!(retry, Retry::First));
    }

    #[aerospike_macro::test]
    async fn a_failed_peer_is_retried_at_most_once_per_interval() {
        let retries = PeerRetries::default();
        retries.failed("BB9A");
        let mut attempts = Vec::new();
        let retry = retries.next("BB9A", Duration::ZERO, |a| attempts.push(a));
        assert!(matches!(retry, Retry::Pending));
        retries.next("BB9A", Duration::from_secs(60), |a| attempts.push(a));
        assert_eq!(attempts.len(), 1, "the second pass is inside the interval");
    }

    #[aerospike_macro::test]
    async fn attempts_in_flight_are_bounded() {
        let retries = PeerRetries::default();
        retries.failed("BB9A");
        let mut attempts = Vec::new();
        for _ in 0..MAX_IN_FLIGHT + 2 {
            retries.next("BB9A", Duration::ZERO, |a| attempts.push(a));
        }
        assert_eq!(attempts.len(), MAX_IN_FLIGHT);
        attempts.pop();
        retries.next("BB9A", Duration::ZERO, |a| attempts.push(a));
        assert_eq!(
            attempts.len(),
            MAX_IN_FLIGHT,
            "a finished attempt frees its slot"
        );
    }

    #[aerospike_macro::test]
    async fn the_first_result_is_kept_and_taken_once() {
        let retries = PeerRetries::default();
        retries.failed("BB9A");
        let mut attempts = Vec::new();
        retries.next("BB9A", Duration::ZERO, |a| attempts.push(a));
        retries.next("BB9A", Duration::ZERO, |a| attempts.push(a));
        let second = attempts.pop().unwrap();
        let first = attempts.pop().unwrap();
        assert!(first.run(async { Some(found()) }).await);
        assert!(
            !second.run(async { Some(found()) }).await,
            "the peer already validated"
        );

        let retry = retries.next("BB9A", Duration::ZERO, |_| panic!("the peer validated"));
        assert!(matches!(retry, Retry::Found(_)));
        let retry = retries.next("BB9A", Duration::ZERO, |_| panic!("the peer validated"));
        assert!(matches!(retry, Retry::First), "the result was taken up");
    }

    #[aerospike_macro::test]
    async fn a_later_attempt_that_validates_stops_a_stuck_earlier_one() {
        let retries = PeerRetries::default();
        retries.failed("BB9A");
        let mut attempts = Vec::new();
        retries.next("BB9A", Duration::ZERO, |a| attempts.push(a));
        retries.next("BB9A", Duration::ZERO, |a| attempts.push(a));
        let later = attempts.pop().unwrap();
        let stuck = attempts.pop().unwrap().run(future::pending());
        assert!(later.run(async { Some(found()) }).await);

        let retry = retries.next("BB9A", Duration::ZERO, |_| panic!("the peer validated"));
        assert!(matches!(retry, Retry::Found(_)));
        assert!(!stuck.await, "taking the result stopped the stuck attempt");
    }

    #[aerospike_macro::test]
    async fn a_pruned_peer_stops_its_attempts() {
        let retries = PeerRetries::default();
        retries.failed("BB9A");
        let mut attempts = Vec::new();
        retries.next("BB9A", Duration::ZERO, |a| attempts.push(a));
        retries.prune();
        retries.prune();
        let stuck = attempts.pop().unwrap();
        assert!(!stuck.run(future::pending()).await, "the attempt stopped");
    }
}
