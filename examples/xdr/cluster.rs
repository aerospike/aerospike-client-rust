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

//! What the `xdr_*` examples share: connecting, info commands, the
//! `xdr-merge` binding, a seeded generator, and waiting for XDR to ship.
//!
//! Not an example of its own: each `xdr_*` example includes this file with
//! `#[path = "xdr/cluster.rs"]`, and uses the part it needs.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use aerospike::{AdminPolicy, Client, ClientPolicy, Error, Result, Task, UdfLang};

/// The two clusters, from `AEROSPIKE_XDR_DC1` and `AEROSPIKE_XDR_DC2` (`host:port` each), or None when
/// either is not set.
pub fn two_clusters() -> Option<(String, String)> {
    Some((env::var("AEROSPIKE_XDR_DC1").ok()?, env::var("AEROSPIKE_XDR_DC2").ok()?))
}

/// The namespace to work in: `AEROSPIKE_XDR_NAMESPACE`, `test` by default.
pub fn namespace() -> String {
    env::var("AEROSPIKE_XDR_NAMESPACE").unwrap_or_else(|_| String::from("test"))
}

/// A number from the environment, or `default`.
pub fn number(variable: &str, default: u64) -> u64 {
    env::var(variable).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// A client for `hosts`, honouring `AEROSPIKE_USE_SERVICES_ALTERNATE` as every example does.
pub async fn connect(hosts: &str) -> Result<Client> {
    let mut policy = ClientPolicy::default();

    policy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    Client::new(&policy, &hosts).await
}

/// One info command, answered by one node of the cluster.
pub async fn info(client: &Client, command: &str) -> Result<String> {
    let answers = client.info(&AdminPolicy::default(), &[command]).await?;

    Ok(answers.get(command).cloned().unwrap_or_default())
}

/// The value of `name` in an info answer of `name=value` fields.
fn field<'a>(answer: &'a str, separator: char, name: &str) -> Option<&'a str> {
    answer.split(separator).find_map(|f| f.strip_prefix(name)?.strip_prefix('='))
}

/// Why this cluster cannot run an `xdr-merge` handler, or None when it can. `event-kinds` lists the
/// event kinds a server serves; a server without the event system does not know the command.
pub async fn events_unavailable(client: &Client) -> Result<Option<String>> {
    let kinds = info(client, "event-kinds").await?;

    if kinds.split(';').any(|kind| kind.starts_with("event=xdr-merge:")) {
        Ok(None)
    } else {
        Ok(Some(format!("the server serves no xdr-merge event (event-kinds answered {kinds:?})")))
    }
}

/// Registers a WASM module as `<package>.wasm` and waits until every node has it.
pub async fn register(client: &Client, module: &[u8], package: &str) -> Result<()> {
    let task = client
        .register_udf(&AdminPolicy::default(), module, &format!("{package}.wasm"), UdfLang::Wasm)
        .await?;

    task.wait_till_complete(None).await?;
    Ok(())
}

/// Removes the module registered as `<package>.wasm`.
pub async fn remove(client: &Client, package: &str) -> Result<()> {
    let task = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await?;

    task.wait_till_complete(None).await?;
    Ok(())
}

/// Binds `function` of `package` to the set as its `xdr-merge` handler. A failing handler drops the
/// incoming write (`on-failure=abandon`), the default, spelled out.
pub async fn bind(client: &Client, namespace: &str, set: &str, package: &str, function: &str) -> Result<()> {
    let command = format!(
        "event-set:event=xdr-merge;namespace={namespace};set={set};module={package};function={function};on-failure=abandon"
    );

    match info(client, &command).await?.as_str() {
        "ok" => Ok(()),
        answer => Err(Error::client_error(format!("{command}: {answer}"))),
    }
}

/// Removes the set's binding.
pub async fn unbind(client: &Client, namespace: &str, set: &str) -> Result<()> {
    let command = format!("event-remove:event=xdr-merge;namespace={namespace};set={set}");

    match info(client, &command).await?.as_str() {
        "ok" => Ok(()),
        answer => Err(Error::client_error(format!("{command}: {answer}"))),
    }
}

/// The set's binding as `event-list` shows it, one `name=value` per field, or None when the set has
/// none. The counters in it are this node's; [`counters`] adds up every node's.
pub async fn binding(client: &Client, namespace: &str, set: &str) -> Result<Option<BTreeMap<String, String>>> {
    let rows = info(client, &format!("event-list:namespace={namespace}")).await?;

    Ok(rows
        .split(';')
        .map(|row| -> BTreeMap<String, String> {
            row.split(':')
                .filter_map(|f| f.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        })
        .find(|fields| fields.get("set").map(String::as_str) == Some(set)))
}

/// The set binding's counters, added up over every node of the cluster.
pub async fn counters(client: &Client, namespace: &str, set: &str) -> Result<BTreeMap<String, u64>> {
    let command = format!("event-list:namespace={namespace}");
    let mut total = BTreeMap::new();

    for node in client.nodes() {
        let answers = node.info(&AdminPolicy::default(), &[&command]).await?;
        let rows = answers.get(&command).map_or("", String::as_str);
        let Some(row) = rows.split(';').find(|r| r.contains(&format!(":set={set}:"))) else { continue };

        for (name, value) in row.split(':').filter_map(|f| f.split_once('=')) {
            if let Ok(n) = value.parse::<u64>() {
                *total.entry(name.to_string()).or_insert(0) += n;
            }
        }
    }

    Ok(total)
}

/// Whether the cluster has nothing left to ship in the namespace: on every node, every DC it ships to
/// has nothing queued and nothing in progress.
pub async fn shipped(client: &Client, namespace: &str) -> Result<bool> {
    let policy = AdminPolicy::default();

    for node in client.nodes() {
        let config = node.info(&policy, &["get-config:context=xdr"]).await?;
        let dcs = config.values().find_map(|a| field(a, ';', "dcs")).unwrap_or("").to_string();

        for dc in dcs.split(',').filter(|dc| !dc.is_empty()) {
            let command = format!("get-stats:context=xdr;dc={dc};namespace={namespace}");
            let stats = node.info(&policy, &[&command]).await?;
            let stats = stats.get(&command).map_or("", String::as_str);

            for name in ["in_queue", "in_progress"] {
                if field(stats, ';', name).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) > 0 {
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

/// A suffix for set and module names, so that two runs never share them.
pub fn unique_suffix() -> String {
    let mut rng = Rng(clock_nanos());

    (0..6).map(|_| char::from(b'a' + rng.below(26) as u8)).collect()
}

fn clock_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64)
}

/// A small seeded generator, so that a run that finds something can be run again exactly.
pub struct Rng(u64);

impl Rng {
    /// Seeded from the variable when it is set, from the clock otherwise. Prints the seed, and how to
    /// run with it again.
    pub fn seeded(variable: &str) -> Self {
        let seed = env::var(variable).ok().and_then(|s| s.parse().ok()).unwrap_or_else(clock_nanos);

        println!("seed {seed} ({variable}={seed} runs it again)");
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    /// A number in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    pub fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }
}
