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

//! Tasks that run a command until stopped and record every outcome, for the
//! fault-injection rows.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aerospike::{Error, ErrorKind};

/// Errors kept in full. A row under fault injection can fail tens of
/// thousands of commands per second, and every error carries its retry
/// chain; the rest keep their kind only.
const KEPT_ERRORS: usize = 1000;

/// Outcomes kept at all. Past this the counts go on and the list stops: a
/// command that fails at once, with no node to send it to, completes at the
/// rate the CPU allows, and recording each one is what runs out of memory.
const KEPT_OUTCOMES: usize = 200_000;

/// One command run by a [`Load`]: when it started, how long it took, the
/// kind of error it raised (`ok` on success) and, for the first
/// `KEPT_ERRORS` failures, the error itself.
#[derive(Clone)]
pub struct Outcome {
    pub start: Instant,
    pub elapsed_ms: u64,
    kind: String,
    timeout: bool,
    socket_timeout: bool,
    max_error_rate: bool,
    pub error: Option<Error>,
}

impl Outcome {
    fn new(start: Instant, elapsed_ms: u64, error: Option<Error>, keep: bool) -> Self {
        let kind = match &error {
            Some(e) => format!("{}({})", e.kind().name(), e.result_code()),
            None => "ok".to_string(),
        };
        let timeout = error.as_ref().is_some_and(|e| matches!(e.kind(), ErrorKind::Timeout));
        let socket_timeout = error.as_ref().is_some_and(waited_out_a_timeout);
        let max_error_rate = error
            .as_ref()
            .is_some_and(|e| matches!(e.kind(), ErrorKind::MaxErrorRate));
        Outcome {
            start,
            elapsed_ms,
            kind,
            timeout,
            socket_timeout,
            max_error_rate,
            error: if keep { error } else { None },
        }
    }

    /// The error's kind and result code, as a sort key for counting.
    pub fn kind(&self) -> String {
        self.kind.clone()
    }

    pub fn is_error(&self) -> bool {
        self.kind != "ok"
    }

    /// The error, or a cause, is a client timeout.
    pub fn is_timeout(&self) -> bool {
        self.timeout
    }

    /// The command waited out a socket or total timeout, as opposed to
    /// exhausting its retries on connection errors, which the client also
    /// reports under the `Timeout` kind ("Timeout after N tries").
    pub fn is_socket_timeout(&self) -> bool {
        self.socket_timeout
    }

    pub fn is_max_error_rate(&self) -> bool {
        self.max_error_rate
    }
}

/// Tasks that run a command until stopped.
pub struct Load {
    name: String,
    outcomes: Arc<Mutex<Vec<Outcome>>>,
    /// Running counts, so a poll never walks the outcome list under its lock.
    counts: Arc<Counts>,
    stopped: Arc<AtomicBool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

#[derive(Default)]
struct Counts {
    total: AtomicUsize,
    errors: AtomicUsize,
    kept_errors: AtomicUsize,
}

impl Load {
    /// Starts `tasks` tasks, each calling `command(task, iteration)` in a
    /// loop until [`stop`](Self::stop).
    pub fn new<F, Fut>(name: &str, tasks: usize, command: F) -> Self
    where
        F: Fn(usize, usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = aerospike::Result<()>> + Send + 'static,
    {
        let outcomes: Arc<Mutex<Vec<Outcome>>> = Arc::new(Mutex::new(Vec::new()));
        let counts = Arc::new(Counts::default());
        let stopped = Arc::new(AtomicBool::new(false));
        let command = Arc::new(command);
        let handles = (0..tasks)
            .map(|task| {
                let outcomes = outcomes.clone();
                let counts = counts.clone();
                let stopped = stopped.clone();
                let command = command.clone();
                tokio::spawn(async move {
                    let mut iteration = 0;
                    while !stopped.load(Ordering::Relaxed) {
                        let start = Instant::now();
                        let error = command(task, iteration).await.err();
                        let elapsed_ms = start.elapsed().as_millis() as u64;
                        let total = counts.total.fetch_add(1, Ordering::Relaxed);
                        let mut keep = false;
                        if error.is_some() {
                            counts.errors.fetch_add(1, Ordering::Relaxed);
                            keep = counts.kept_errors.fetch_add(1, Ordering::Relaxed) < KEPT_ERRORS;
                        }
                        if total < KEPT_OUTCOMES {
                            outcomes
                                .lock()
                                .unwrap()
                                .push(Outcome::new(start, elapsed_ms, error, keep));
                        }
                        iteration += 1;
                        // A command that completes without ever pending, for
                        // example one refused because no node owns its
                        // partition, would otherwise keep this task on its
                        // worker for good and starve the row's own timers.
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();
        Load {
            name: name.to_string(),
            outcomes,
            counts,
            stopped,
            handles,
        }
    }

    /// Stops the tasks and returns whether every one of them returned within
    /// `bound_ms`. A task still running past the bound is aborted and named.
    pub async fn stop(&mut self, bound_ms: u64) -> bool {
        self.stopped.store(true, Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_millis(bound_ms);
        let mut done = true;
        for (i, handle) in self.handles.drain(..).enumerate() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if tokio::time::timeout(remaining, handle).await.is_err() {
                println!("L2-BLOCKED {}-{i} still running {bound_ms}ms after stop", self.name);
                done = false;
            }
        }
        done
    }

    pub fn outcomes(&self) -> Vec<Outcome> {
        self.outcomes.lock().unwrap().clone()
    }

    pub fn errors(&self) -> Vec<Outcome> {
        self.outcomes().into_iter().filter(Outcome::is_error).collect()
    }

    pub fn successes(&self) -> usize {
        self.counts.total.load(Ordering::Relaxed) - self.counts.errors.load(Ordering::Relaxed)
    }

    pub fn error_count(&self) -> usize {
        self.counts.errors.load(Ordering::Relaxed)
    }

    pub fn total(&self) -> usize {
        self.counts.total.load(Ordering::Relaxed)
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// Whether an error in the chain is a client timeout other than the
/// retries-exhausted wrapper.
fn waited_out_a_timeout(error: &Error) -> bool {
    let mut current = Some(error);
    while let Some(e) = current {
        if matches!(e.kind(), ErrorKind::Timeout)
            && e.client_result_code() != Some(aerospike::ClientResultCode::MaxRetriesExceeded)
        {
            return true;
        }
        current = e.cause();
    }
    false
}

/// The number of outcomes of each error kind.
pub fn kinds(outcomes: &[Outcome]) -> BTreeMap<String, usize> {
    let mut map = BTreeMap::new();
    for outcome in outcomes {
        *map.entry(outcome.kind()).or_insert(0) += 1;
    }
    map
}
