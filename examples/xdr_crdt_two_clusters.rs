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

//! The device map CRDT between two live clusters that ship to each other.
//!
//! The device map is step 5 of the CRDT tutorial in the UDF book: a list of
//! devices per subscriber, changed only through the module's write UDFs and
//! merged by its `xdr_merge` handler on the data alone. This example:
//!
//! 1. registers the module on both clusters and binds its handler to a new
//!    set on both (`event-set`);
//! 2. changes random devices of random subscribers on both clusters at once,
//!    through `update_device`, `remove_device` and `purge_devices`, so that
//!    each cluster merges the other's writes into records it changed itself;
//! 3. waits for XDR to ship everything, then compares every record of the
//!    set on the two clusters by value, with the comparison
//!    `xdr_compare_records` uses;
//! 4. shows the binding's counters, then removes the bindings, the records
//!    and the module.
//!
//! The clusters need the event system (an Enterprise server whose
//! `event-kinds` lists `xdr-merge`), a distinct non-zero XDR `src-id` each,
//! and XDR shipping the namespace each way; the event chapter's
//! "Deployment" section has the rest.
//!
//! Settings, from the environment:
//!
//! - `AEROSPIKE_XDR_DC1`, `AEROSPIKE_XDR_DC2`: the two clusters, `host:port`
//!   each. Without them, or without the event system, the example skips.
//! - `AEROSPIKE_XDR_NAMESPACE`: the namespace, `test` by default.
//! - `XDR_CRDT_ROUNDS` (default 30): rounds of changes, one on each cluster
//!   per round.
//! - `XDR_CRDT_SEED`: the seed, printed by every run, to run one again.
//! - `AEROSPIKE_USE_SERVICES_ALTERNATE`, as for every example.
//!
//! ```bash
//! AEROSPIKE_XDR_DC1=127.0.0.1:3400 AEROSPIKE_XDR_DC2=127.0.0.1:3500 \
//!   cargo run --example xdr_crdt_two_clusters
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use aerospike::{Client, Error, Key, Result, Value, WritePolicy};

#[path = "xdr/cluster.rs"]
mod cluster;
#[path = "xdr/compare.rs"]
mod compare;

use cluster::Rng;

/// The device map, built from `aerospike-udf-sdk/wasm-rust/examples/crdt`.
const CRDT: &[u8] = include_bytes!("../tests/fixtures/wasm/crdt.wasm");

const SUBSCRIBERS: u64 = 5;
const DEVICES: &[&str] = &["device-A", "device-B", "device-C", "device-D"];
const STATES: &[&str] = &["ACTIVE", "SUSPENDED", "LOST"];

/// How long XDR may take to ship everything and the two clusters to agree.
const SETTLE: Duration = Duration::from_secs(60);

/// The application's clock: the write UDFs never read the server's. It starts here and moves on by up
/// to 12 hours a round, so that a long run purges markers older than the module's week.
const START_MS: i64 = 1_790_000_000_000;
const HOURS_12_MS: u64 = 12 * 60 * 60 * 1000;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

/// One change an application makes to a subscriber's devices.
enum Change {
    Update { device: &'static str, state: &'static str },
    Remove { device: &'static str },
    Purge,
}

impl Change {
    fn random(rng: &mut Rng) -> Self {
        match rng.below(10) {
            0..=6 => Change::Update { device: rng.pick(DEVICES), state: rng.pick(STATES) },
            7 | 8 => Change::Remove { device: rng.pick(DEVICES) },
            _ => Change::Purge,
        }
    }

    /// Makes the change through the module's write UDF: each is one atomic write of the value, its
    /// version and its removal marker together.
    async fn apply(&self, client: &Client, key: &Key, package: &str, now: i64) -> Result<()> {
        let policy = write_policy();
        let (function, args) = match self {
            Change::Update { device, state } => {
                let changes = HashMap::from([(Value::from("state"), Value::from(*state))]);

                ("update_device", vec![Value::from(*device), Value::from(changes)])
            }
            Change::Remove { device } => ("remove_device", vec![Value::from(*device), Value::from(now)]),
            Change::Purge => ("purge_devices", vec![Value::from(now)]),
        };

        client.execute_udf(&policy, key, package, function, &args).await?;
        Ok(())
    }

    fn describe(&self) -> String {
        match self {
            Change::Update { device, state } => format!("update {device} {state}"),
            Change::Remove { device } => format!("remove {device}"),
            Change::Purge => String::from("purge"),
        }
    }
}

/// A merge holds the record while it runs; give a write behind it time.
fn write_policy() -> WritePolicy {
    let mut policy = WritePolicy::default();

    policy.base_policy.total_timeout = 5000;
    policy.base_policy.socket_timeout = 5000;
    policy
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`). An error says the
/// clusters did not converge, a merge failed, or a step could not be done.
pub async fn run() -> Result<()> {
    let Some((dc1, dc2)) = cluster::two_clusters() else {
        println!("skipped: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2 to the two clusters (host:port each)");
        return Ok(());
    };
    let c1 = cluster::connect(&dc1).await?;
    let c2 = cluster::connect(&dc2).await?;

    for (c, name) in [(&c1, "dc1"), (&c2, "dc2")] {
        if let Some(reason) = cluster::events_unavailable(c).await? {
            println!("skipped: {name}: {reason}");
            return Ok(());
        }
    }

    let namespace = cluster::namespace();
    let suffix = cluster::unique_suffix();
    let set = format!("subscribers_{suffix}");
    let package = format!("crdt_{suffix}");
    let keys: Vec<Key> = (0..SUBSCRIBERS)
        .map(|i| Key::new(namespace.as_str(), set.as_str(), Value::from(format!("subscriber-{i}"))))
        .collect::<Result<_>>()?;

    // The same module, bound to the same set, on both clusters.
    for c in [&c1, &c2] {
        cluster::register(c, CRDT, &package).await?;
        cluster::bind(c, &namespace, &set, &package, "xdr_merge").await?;
    }
    println!("bound {package}.xdr_merge to {namespace}.{set} on both clusters");

    let outcome = exercise(&c1, &c2, &namespace, &set, &package, &keys).await;

    // Clean up whatever happened: the bindings first, so that the deletes are not merged.
    for c in [&c1, &c2] {
        let _ = cluster::unbind(c, &namespace, &set).await;

        for key in &keys {
            let _ = c.delete(&write_policy(), key).await;
        }

        let _ = cluster::remove(c, &package).await;
    }

    c1.close().await?;
    c2.close().await?;
    outcome
}

/// The changes, the wait, the comparison and the counters.
async fn exercise(c1: &Client, c2: &Client, namespace: &str, set: &str, package: &str, keys: &[Key]) -> Result<()> {
    let mut rng = Rng::seeded("XDR_CRDT_SEED");
    let rounds = cluster::number("XDR_CRDT_ROUNDS", 30);
    let mut now = START_MS;

    // Each round, one change on each cluster at the same moment, to the same subscriber half the time.
    for round in 0..rounds {
        now += rng.below(HOURS_12_MS) as i64;

        let i1 = rng.below(SUBSCRIBERS) as usize;
        let i2 = if rng.below(2) == 0 { i1 } else { rng.below(SUBSCRIBERS) as usize };
        let (change1, change2) = (Change::random(&mut rng), Change::random(&mut rng));

        println!(
            "round {round:>3}: dc1 subscriber-{i1} {:<26} dc2 subscriber-{i2} {}",
            change1.describe(),
            change2.describe()
        );

        let (r1, r2) = futures::join!(
            change1.apply(c1, &keys[i1], package, now),
            change2.apply(c2, &keys[i2], package, now),
        );
        r1?;
        r2?;
    }

    // Wait until neither cluster has anything left to ship and both hold the same records.
    let deadline = Instant::now() + SETTLE;
    let comparison = loop {
        let quiet = cluster::shipped(c1, namespace).await? && cluster::shipped(c2, namespace).await?;

        if quiet {
            let comparison = compare::compare_set(c1, c2, namespace, set).await?;

            if comparison.matches() || Instant::now() >= deadline {
                break comparison;
            }
        } else if Instant::now() >= deadline {
            break compare::compare_set(c1, c2, namespace, set).await?;
        }

        aerospike_rt::sleep(Duration::from_millis(500)).await;
    };

    print!("{}", comparison.report());

    // What the handler did on each side. Every incoming write to an existing subscriber is a call
    // (`fired`); `combined` counts the collisions it merged, `fallback` the calls that failed.
    let mut failed = 0;

    for (c, name) in [(c1, "dc1"), (c2, "dc2")] {
        let counters = cluster::counters(c, namespace, set).await?;
        let count = |n: &str| counters.get(n).copied().unwrap_or(0);

        println!(
            "{name}: fired {}, combined {}, took-incoming {}, keep-local {}, skipped {}, fallback {}",
            count("fired"),
            count("combined"),
            count("took-incoming"),
            count("keep-local"),
            count("skipped"),
            count("fallback")
        );
        failed += count("fallback");
    }

    if !comparison.matches() {
        return Err(Error::client_error(format!(
            "the clusters did not converge: {} subscribers differ",
            comparison.mismatches.len()
        )));
    }

    if failed > 0 {
        return Err(Error::client_error(format!("{failed} merges failed")));
    }

    println!("both clusters hold the same {} subscribers", comparison.on_dc1);
    Ok(())
}
