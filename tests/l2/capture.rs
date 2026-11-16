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

//! The client's warnings and errors, captured for the rows that assert on
//! what the client reports: translation misses, collisions, nodes without an
//! alternate address. Installed once per test binary as the `log` logger.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use log::{Level, LevelFilter, Log, Metadata, Record};

struct Capture {
    lines: Mutex<Vec<(Level, String)>>,
    active: AtomicBool,
}

static CAPTURE: Capture = Capture {
    lines: Mutex::new(Vec::new()),
    active: AtomicBool::new(false),
};

impl Log for Capture {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Warn
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) || !self.active.load(Ordering::Relaxed) {
            return;
        }
        let message = record.args().to_string();
        println!("CLIENT-LOG {} {message}", record.level());
        self.lines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((record.level(), message));
    }

    fn flush(&self) {}
}

/// Installs the capture as the process logger and starts recording.
pub fn install() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        log::set_logger(&CAPTURE).expect("the L2 capture is the only logger in this binary");
        log::set_max_level(LevelFilter::Warn);
    });
    clear();
    CAPTURE.active.store(true, Ordering::Relaxed);
}

/// Stops recording between rows.
pub fn stop() {
    CAPTURE.active.store(false, Ordering::Relaxed);
}

/// Forgets what was captured so far.
pub fn clear() {
    CAPTURE
        .lines
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

fn lines(level: Level) -> Vec<String> {
    CAPTURE
        .lines
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|(l, _)| *l == level)
        .map(|(_, m)| m.clone())
        .collect()
}

/// Every warning the client logged since the last `clear`.
pub fn warnings() -> Vec<String> {
    lines(Level::Warn)
}

/// Every error the client logged since the last `clear`.
pub fn errors() -> Vec<String> {
    lines(Level::Error)
}

/// Warnings and errors containing every one of `parts`.
pub fn messages_containing(parts: &[&str]) -> Vec<String> {
    warnings()
        .into_iter()
        .chain(errors())
        .filter(|m| parts.iter().all(|p| m.contains(p)))
        .collect()
}
