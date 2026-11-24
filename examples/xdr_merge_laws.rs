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

//! Checks that an `xdr-merge` handler is a CRDT merge, on random inputs, on
//! one cluster.
//!
//! Two clusters converge only if their merge is commutative (each side
//! merges the mirror pair), idempotent (a write can be delivered twice) and
//! associative (writes reach a third site in any order). The server's dry
//! run checks all three on the real bytes of the results:
//! `execute_udf(key, "__event__", "xdr-merge", [incoming bins, incoming
//! metadata, other bins])` runs the set's bound handler against the record at
//! `key` and an incoming record, writes nothing, and answers `symmetric`,
//! `idempotent` and, given the third record, `associative`.
//!
//! Each round builds three records of the device map (step 5 of the CRDT
//! tutorial in the UDF book) the way an application does, through the
//! module's write UDFs: a shared history first, then changes of each record's
//! own, as three sites would make them. One record is the current one, the
//! other two are the incoming record and the third site's. Every round whose
//! answer breaks a law, or whose merge fails, is reported with its three
//! records.
//!
//! Settings, from the environment:
//!
//! - `AEROSPIKE_HOSTS` (default `127.0.0.1:3000`): the cluster. Without the
//!   event system on it, the example skips.
//! - `AEROSPIKE_XDR_NAMESPACE`: the namespace, `test` by default.
//! - `XDR_LAWS_ROUNDS` (default 40) and `XDR_LAWS_SEED`, printed by every
//!   run, to run one again.
//! - `XDR_LAWS_MODULE`: a `.wasm` file to register instead of the device map
//!   built from `aerospike-udf-sdk/wasm-rust/examples/crdt`, such as the Java,
//!   Go or C++ build of it; `XDR_LAWS_FUNCTION` (default `xdr_merge`) its
//!   handler. The inputs are made by the device map's write UDFs, so the
//!   module must have them.
//! - `XDR_LAWS_SET`: check the handler already bound to this set, through the
//!   bound module's write UDFs, and leave the binding as it is. By default a
//!   new set is bound, and unbound at the end.
//! - `AEROSPIKE_USE_SERVICES_ALTERNATE`, as for every example.
//!
//! For a merge of another shape, keep `check_laws` and replace `Change`
//! with your own write UDFs.
//!
//! ```bash
//! AEROSPIKE_HOSTS=127.0.0.1:3000 cargo run --example xdr_merge_laws
//! XDR_LAWS_MODULE=tests/fixtures/wasm/crdt_go.wasm cargo run --example xdr_merge_laws
//! ```

use std::collections::HashMap;
use std::env;

use aerospike::{Bins, Client, Error, Key, ReadPolicy, Result, Value, WritePolicy};

#[path = "xdr/cluster.rs"]
mod cluster;

use cluster::Rng;

/// The device map, built from `aerospike-udf-sdk/wasm-rust/examples/crdt`.
const CRDT: &[u8] = include_bytes!("../tests/fixtures/wasm/crdt.wasm");

const DEVICES: &[&str] = &["device-A", "device-B", "device-C", "device-D", "device-E"];
const STATES: &[&str] = &["ACTIVE", "SUSPENDED", "LOST"];
const SITES: [&str; 3] = ["current", "incoming", "other"];

/// The application's clock, which moves on by up to two days a change, so that purges drop markers
/// older than the module's week.
const START_MS: i64 = 1_790_000_000_000;
const DAYS_2_MS: u64 = 2 * 24 * 60 * 60 * 1000;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

/// One change an application makes to a subscriber's devices, through the module's write UDFs.
enum Change {
    Update { device: &'static str, state: &'static str },
    Remove { device: &'static str },
    Purge,
}

impl Change {
    fn random(rng: &mut Rng) -> Self {
        match rng.below(10) {
            0..=5 => Change::Update { device: rng.pick(DEVICES), state: rng.pick(STATES) },
            6..=8 => Change::Remove { device: rng.pick(DEVICES) },
            _ => Change::Purge,
        }
    }

    async fn apply(&self, client: &Client, key: &Key, package: &str, now: i64) -> Result<()> {
        let (function, args) = match self {
            Change::Update { device, state } => {
                let changes = HashMap::from([(Value::from("state"), Value::from(*state))]);

                ("update_device", vec![Value::from(*device), Value::from(changes)])
            }
            Change::Remove { device } => ("remove_device", vec![Value::from(*device), Value::from(now)]),
            Change::Purge => ("purge_devices", vec![Value::from(now)]),
        };

        client.execute_udf(&WritePolicy::default(), key, package, function, &args).await?;
        Ok(())
    }
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`). An error says an
/// input broke a law, or that a step could not be done.
pub async fn run() -> Result<()> {
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = cluster::connect(&hosts).await?;

    if let Some(reason) = cluster::events_unavailable(&client).await? {
        println!("skipped: {reason}");
        return client.close().await;
    }

    let namespace = cluster::namespace();
    let suffix = cluster::unique_suffix();

    // The handler to check: the one already bound to XDR_LAWS_SET, or a module bound here to a new set.
    let (set, package, ours) = match env::var("XDR_LAWS_SET") {
        Ok(set) => match cluster::binding(&client, &namespace, &set).await? {
            Some(row) => {
                let package = row.get("module").cloned().unwrap_or_default();

                println!("checking {package}.{} as bound to {namespace}.{set}", row.get("function").map_or("", String::as_str));
                (set, package, false)
            }
            None => return Err(Error::invalid_argument(format!("XDR_LAWS_SET: {namespace}.{set} has no xdr-merge binding"))),
        },
        Err(_) => {
            let set = format!("laws_{suffix}");
            let package = format!("laws_{suffix}");
            let function = env::var("XDR_LAWS_FUNCTION").unwrap_or_else(|_| String::from("xdr_merge"));
            let module = match env::var("XDR_LAWS_MODULE") {
                Ok(path) => std::fs::read(&path).map_err(|e| Error::invalid_argument(format!("XDR_LAWS_MODULE {path}: {e}")))?,
                Err(_) => CRDT.to_vec(),
            };

            cluster::register(&client, &module, &package).await?;

            if let Err(e) = cluster::bind(&client, &namespace, &set, &package, &function).await {
                let _ = cluster::remove(&client, &package).await;

                // A binding needs this cluster's XDR src-id; a node without one cannot merge at all.
                if e.to_string().contains("src-id") {
                    println!("skipped: {e}");
                    return client.close().await;
                }
                return Err(e);
            }

            println!("checking {package}.{function}, bound to {namespace}.{set}");
            (set, package, true)
        }
    };

    let outcome = check_rounds(&client, &namespace, &set, &package, &suffix).await;

    if ours {
        let _ = cluster::unbind(&client, &namespace, &set).await;
        let _ = cluster::remove(&client, &package).await;
    }

    client.close().await?;
    outcome
}

/// The rounds: three records with a shared history, the dry run, the laws.
async fn check_rounds(client: &Client, namespace: &str, set: &str, package: &str, suffix: &str) -> Result<()> {
    let mut rng = Rng::seeded("XDR_LAWS_SEED");
    let rounds = cluster::number("XDR_LAWS_ROUNDS", 40);
    let mut broken = 0;

    for round in 0..rounds {
        let keys: Vec<Key> = SITES
            .iter()
            .map(|site| Key::new(namespace, set, Value::from(format!("laws-{suffix}-{round}-{site}"))))
            .collect::<Result<_>>()?;

        let outcome = round_of(client, &keys, package, &mut rng).await;

        for key in &keys {
            let _ = client.delete(&WritePolicy::default(), key).await;
        }

        if let Some(report) = outcome? {
            println!("round {round}: {report}");
            broken += 1;
        }
    }

    if broken > 0 {
        return Err(Error::client_error(format!("{broken} of {rounds} inputs broke a law")));
    }

    println!("{rounds} inputs: every merge symmetric, idempotent and associative");
    Ok(())
}

/// One round: what breaks a law, or None when nothing does.
async fn round_of(client: &Client, keys: &[Key], package: &str, rng: &mut Rng) -> Result<Option<String>> {
    // A history the three sites share, then each site's own changes, made after they parted.
    let mut now = START_MS;

    for _ in 0..=rng.below(8) {
        let change = Change::random(rng);

        now += rng.below(DAYS_2_MS) as i64;

        for key in keys {
            change.apply(client, key, package, now).await?;
        }
    }

    for key in keys {
        let mut own = now;

        for _ in 0..rng.below(6) {
            own += rng.below(DAYS_2_MS) as i64;
            Change::random(rng).apply(client, key, package, own).await?;
        }
    }

    let mut records = Vec::new();

    for key in keys {
        let record = client.get(&ReadPolicy::default(), key, Bins::All).await?;

        records.push(record.bins.into_iter().map(|(name, value)| (Value::from(name), value)).collect::<HashMap<_, _>>());
    }

    check_laws(client, &keys[0], &records[1], &records[2]).await.map(|broken| {
        broken.map(|why| {
            let shown: Vec<String> = SITES.iter().zip(&records).map(|(site, bins)| format!("\n  {site}: {}", Value::from(bins.clone()))).collect();

            format!("{why}{}", shown.concat())
        })
    })
}

/// The dry run of the handler bound to the key's set, merging `incoming` (from src-id 2) into the
/// record at `key`, with `other` (src-id 3) as the third site. Answers which law the merge broke, or
/// None when it keeps all three.
async fn check_laws(
    client: &Client,
    key: &Key,
    incoming: &HashMap<Value, Value>,
    other: &HashMap<Value, Value>,
) -> Result<Option<String>> {
    let metadata = HashMap::from([(Value::from("src-id"), Value::from(2))]);
    let args = [Value::from(incoming.clone()), Value::from(metadata), Value::from(other.clone())];

    let answer = match client.execute_udf(&WritePolicy::default(), key, "__event__", "xdr-merge", &args).await {
        Ok(Some(answer)) => answer,
        Ok(None) => return Ok(Some(String::from("the dry run answered nothing"))),
        // The handler failed: on a live merge, that write would be dropped.
        Err(e) => return Ok(Some(format!("the merge failed: {e}"))),
    };

    let law = |name: &str| match &answer {
        Value::HashMap(m) => m.get(&Value::from(name)).cloned(),
        Value::OrderedMap(m) => m.get(&Value::from(name)).cloned(),
        Value::SortedMap(m) => m.get(&Value::from(name)).cloned(),
        _ => None,
    };
    let broken: Vec<&str> = ["symmetric", "idempotent", "associative"]
        .into_iter()
        .filter(|name| law(name) != Some(Value::Bool(true)))
        .collect();

    if broken.is_empty() {
        Ok(None)
    } else {
        Ok(Some(format!("not {} (the dry run answered {answer})", broken.join(", not "))))
    }
}
