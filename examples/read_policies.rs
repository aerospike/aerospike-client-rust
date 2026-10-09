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

//! Read policies: where a read goes and what consistency it gets.
//!
//! Three knobs on every policy's `base_policy` decide this:
//!
//! - `replica`: which node answers. `Master` (the default) always asks the
//!   partition's master; `MasterProles` and `Random` spread reads across
//!   replicas; `Sequence` walks the replicas on retry; `PreferRack` picks a
//!   replica in the client's own rack when `ClientPolicy.rack_ids` is set,
//!   which keeps reads off cross-zone links.
//! - `read_mode_ap`: in an AP namespace, whether one copy (`One`) or every
//!   copy (`All`) is consulted, trading latency for duplicate resolution.
//! - `read_mode_sc`: in a strong-consistency namespace, `Session`
//!   (monotonic for this client), `Linearize` (monotonic for everyone) or
//!   `AllowReplica` / `AllowUnavailable` (relaxed, may read a replica).
//!
//! Timeouts live beside them: `socket_timeout`, `total_timeout`,
//! `max_retries` and `sleep_between_retries`.
//!
//! ```bash
//! cargo run --example read_policies
//! ```

use std::env;

use aerospike::{
    as_bin, as_key, Bins, Client, ClientPolicy, ReadModeAp, ReadModeSc, ReadPolicy, Replica,
    WritePolicy,
};

const SET: &str = "read_policy_demo";

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    // Rack awareness: the client says which rack(s) it lives in, in order of
    // preference, and `Replica::PreferRack` reads then stay local when a
    // replica is there. `None` turns rack awareness off.
    cpolicy.rack_ids = Some(vec![1]);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let wpolicy = WritePolicy::default();
    let key = as_key!("test", SET, "profile");
    client
        .put(&wpolicy, &key, &[as_bin!("name", "Ada"), as_bin!("visits", 7)])
        .await
        .unwrap();

    let sc = client.is_strong_consistency("test").unwrap_or(false);
    println!("namespace `test` strong consistency: {sc}");

    // ---- Replica placement ----
    for replica in [
        Replica::Master,
        Replica::MasterProles,
        Replica::Sequence,
        Replica::Random,
        Replica::PreferRack,
    ] {
        let mut policy = ReadPolicy::default();
        policy.base_policy.replica = replica;
        let rec = client.get(&policy, &key, Bins::from(["name"])).await.unwrap();
        println!("replica {replica:?}: name = {:?}", rec.bins.get("name"));
    }

    // ---- AP read mode: one copy or all of them ----
    // `All` asks every replica and reconciles duplicates after a partition
    // heals. It costs a round trip per replica, so keep it for reads that
    // must see the latest write in an AP namespace.
    let mut consult_all = ReadPolicy::default();
    consult_all.base_policy.read_mode_ap = ReadModeAp::All;
    let rec = client.get(&consult_all, &key, Bins::from(["visits"])).await.unwrap();
    println!("read_mode_ap = All: visits = {:?}", rec.bins.get("visits"));

    // ---- SC read mode: how strict the ordering guarantee is ----
    // These only take effect in a strong-consistency namespace; in an AP
    // namespace the server ignores them.
    for mode in [ReadModeSc::Session, ReadModeSc::Linearize, ReadModeSc::AllowReplica] {
        let mut policy = ReadPolicy::default();
        policy.base_policy.read_mode_sc = mode;
        let rec = client.get(&policy, &key, Bins::from(["visits"])).await.unwrap();
        println!("read_mode_sc = {mode:?}: visits = {:?}", rec.bins.get("visits"));
    }

    // ---- Deadlines and retries ----
    // socket_timeout bounds one attempt; total_timeout bounds the whole call
    // including retries. 0 means "no limit" for either.
    let mut tight = ReadPolicy::default();
    tight.base_policy.socket_timeout = 500;
    tight.base_policy.total_timeout = 2_000;
    tight.base_policy.max_retries = 3;
    tight.base_policy.sleep_between_retries = 50;
    let rec = client.get(&tight, &key, Bins::All).await.unwrap();
    println!(
        "with a 500 ms socket / 2 s total deadline: {} bins read, generation {}",
        rec.bins.len(),
        rec.generation
    );

    let _ = client.delete(&wpolicy, &key).await;
    client.close().await.unwrap();
}
