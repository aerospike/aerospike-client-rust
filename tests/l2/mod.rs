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

//! The L2 discovery harness and rows. `topology` reads the environment and
//! talks to the listeners, Toxiproxy and the node server; `capture` records
//! the client's log; `load` runs commands from several tasks; the other
//! modules are the rows, one file per Java test class.

#![allow(dead_code)]

pub mod capture;
pub mod load;
pub mod topology;

pub mod cn_only;
pub mod fault_injection;
pub mod idle_reset;
pub mod low_mtu;
pub mod pass_through;
pub mod port_map_range;
pub mod reachable;
pub mod rows;
pub mod source_port_range;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Mutex;

use futures::FutureExt;

/// The runtime every row runs on; the client's tend task lives on it too.
pub fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

/// The rows share Toxiproxy and the cluster: one at a time, whatever
/// `--test-threads` says.
static SERIAL: Mutex<()> = Mutex::new(());

/// Runs one row: skips it without the topology, installs the log capture,
/// runs `body` on the shared runtime and restores whatever the row changed
/// in the topology, whether it passed or panicked.
pub fn run<F>(row: &str, body: impl FnOnce() -> F)
where
    F: Future<Output = ()>,
{
    let _serial = SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if topology::topology().is_none() {
        println!(
            "L2-SKIP {row}: DISCOVERY_ENDPOINT_HOSTNAME is not set. Run tests/discovery/run_l2.sh \
             or the discovery-l2 workflow"
        );
        return;
    }
    capture::install();
    let result = runtime().block_on(async {
        let outcome = AssertUnwindSafe(body()).catch_unwind().await;
        topology::restore_all().await;
        outcome
    });
    capture::stop();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

/// Prints the reason and returns `false` when `condition` does not hold, so
/// a row can `return` instead of running without its profile.
pub fn skip_unless(condition: bool, reason: &str) -> bool {
    if !condition {
        println!("L2-SKIP {reason}");
    }
    condition
}
