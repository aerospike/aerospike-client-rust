// Copyright 2015-2018 Aerospike, Inc.
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

use std::time::Instant;

use aerospike::{as_bin, as_key, AdminPolicy, Bins, Client, Key, ReadPolicy, WritePolicy};
use aerospike_rt::time::Duration;

use crate::common;

/// Reproduce: single-key WRITE operations (put/remove) fail with
/// "Command timed out after ~N tries" after a node is killed and
/// the client evicts it via tend, because the partition map still
/// references the dead node as the master for some partitions.
///
/// TOPOLOGY: Start with a 4-node cluster (RF=2), kill one → 3 surviving.
/// The 4-node starting state ensures some partitions have their
/// ONLY master on the killed node in the stale partition map.
///
/// ─── CLUSTER LIFECYCLE COMMANDS ───────────────────────────────────────
///
/// TEARDOWN (stop & remove all 4 nodes):
///   aerolab cluster destroy -n mydc -f
///
///   If aerolab is unavailable, manual Docker commands:
///     docker kill aerolab-mydc_1 aerolab-mydc_2 aerolab-mydc_3 aerolab-mydc_4
///     docker rm   aerolab-mydc_1 aerolab-mydc_2 aerolab-mydc_3 aerolab-mydc_4
///
/// SETUP (create fresh 4-node cluster, ubuntu 22.04):
///   aerolab cluster create -n mydc -c 4 -i 22.04
///
/// VERIFY (all 4 nodes up, same cluster key):
///   docker ps --format '{{.Names}} {{.Status}}' | grep mydc
///   for i in 1 2 3 4; do
///     echo "node $i: $(docker exec aerolab-mydc_$i asinfo -v 'cluster-stable')";
///   done
///
/// MANUAL STOP ALL 4 (without destroying containers):
///   docker kill aerolab-mydc_1 aerolab-mydc_2 aerolab-mydc_3 aerolab-mydc_4
///
/// MANUAL START ALL 4 (restart containers + asd):
///   for i in 1 2 3 4; do docker start aerolab-mydc_$i; done
///   sleep 2
///   for i in 1 2 3 4; do
///     docker exec aerolab-mydc_$i /usr/bin/asd --config-file /etc/aerospike/aerospike.conf;
///   done
///   sleep 5   # wait for cluster to form
///
/// ─── RUN THE TEST ─────────────────────────────────────────────────────
///
/// RUN:
///   RUN_WRITE_BUG=1 \
///     AEROSPIKE_HOSTS="127.0.0.1:3100" \
///     AEROSPIKE_USE_SERVICES_ALTERNATE=true \
///     STOP_CMD="docker kill aerolab-mydc_3" \
///     START_CMD="docker start aerolab-mydc_3" \
///     RESTART_ASD_CMD="docker exec aerolab-mydc_3 /usr/bin/asd --config-file /etc/aerospike/aerospike.conf" \
///     cargo test --features rt-tokio -- test_seed_connection_and_partition_map_staleness --nocapture
///
/// RUN (with debug logging):
///   RUST_LOG=debug RUN_WRITE_BUG=1 \
///     AEROSPIKE_HOSTS="127.0.0.1:3100" \
///     AEROSPIKE_USE_SERVICES_ALTERNATE=true \
///     STOP_CMD="docker kill aerolab-mydc_3" \
///     START_CMD="docker start aerolab-mydc_3" \
///     RESTART_ASD_CMD="docker exec aerolab-mydc_3 /usr/bin/asd --config-file /etc/aerospike/aerospike.conf" \
///     cargo test --features rt-tokio -- test_seed_connection_and_partition_map_staleness --nocapture 2>&1 | tee /tmp/write_bug.log
#[aerospike_macro::test]
#[ignore = "manual repro: needs aerolab cluster; run with cargo test -- --ignored and RUN_WRITE_BUG=1"]
async fn test_seed_connection_and_partition_map_staleness() {
    use std::process::Command as ShellCmd;

    let run_flag = std::env::var("RUN_WRITE_BUG").unwrap_or_default();
    if !["1", "true", "yes"].contains(&run_flag.as_str()) {
        println!("Skipping: set RUN_WRITE_BUG=1 to run");
        return;
    }

    let stop_cmd =
        std::env::var("STOP_CMD").expect("Set STOP_CMD (e.g. 'docker kill aerolab-mydc_3')");
    let start_cmd =
        std::env::var("START_CMD").expect("Set START_CMD (e.g. 'docker start aerolab-mydc_3')");
    let restart_asd_cmd = std::env::var("RESTART_ASD_CMD").unwrap_or_default();

    let namespace = common::namespace();
    let set_name = "write_bug_repro";
    let bin_name = "v";
    let num_records: i64 = 10000;

    let client = common::client().await;
    let wpolicy = WritePolicy::default();

    let run_cmd = |cmd_str: &str, label: &str| -> bool {
        println!("  [{}] Running: {}", label, cmd_str);
        let parts: Vec<&str> = cmd_str.split_whitespace().collect();
        let output = ShellCmd::new(parts[0])
            .args(&parts[1..])
            .output()
            .unwrap_or_else(|e| panic!("[{}] Failed to execute: {}", label, e));
        if !output.status.success() {
            println!(
                "  [{}] Command failed (rc={}): stderr={:?}",
                label,
                output.status,
                String::from_utf8_lossy(&output.stderr),
            );
            return false;
        }
        println!("  [{}] OK", label);
        true
    };

    // ── Phase 1: Verify starting state (need 4+ nodes) ──────────────────
    let initial_nodes = client.nodes().len();
    println!("\n[PHASE 1] Client sees {} nodes", initial_nodes);
    if initial_nodes < 4 {
        println!(
            "  Need 4+ nodes to reproduce. Got {}.\n\
             Create a 4-node cluster first:\n\
               aerolab cluster create -n mydc -c 4 -i 22.04\n\
             Skipping test.",
            initial_nodes
        );
        return;
    }

    // ── Phase 2: Load data + verify baseline ─────────────────────────────
    println!(
        "\n[PHASE 2] Loading {} records into {}.{}...",
        num_records, namespace, set_name
    );
    for i in 0..num_records {
        let key = as_key!(namespace, set_name, i);
        let bins = vec![as_bin!(bin_name, i)];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }
    println!("  Done loading {} records", num_records);

    let (_, baseline_fail) = probe_get(&client, namespace, set_name, bin_name, num_records).await;
    assert_eq!(baseline_fail, 0, "Baseline get should have 0 failures");
    println!("  Baseline get: all {} passed", num_records);

    // ── Phase 3: Kill one node ───────────────────────────────────────────
    println!(
        "\n[PHASE 3] Killing one node (cluster {}→{})...",
        initial_nodes,
        initial_nodes - 1
    );
    assert!(run_cmd(&stop_cmd, "STOP-NODE"), "Stop command failed");

    println!("  Waiting for tend to evict dead node...");
    let evict_start = Instant::now();
    loop {
        let n = client.nodes().len();
        if n < initial_nodes {
            println!(
                "  Node evicted after {:.1}s (now {} nodes)",
                evict_start.elapsed().as_secs_f64(),
                n
            );
            break;
        }
        if evict_start.elapsed() > Duration::from_secs(30) {
            println!(
                "  WARNING: Dead node not evicted after 30s (still {} nodes). Proceeding anyway.",
                n
            );
            break;
        }
        aerospike_rt::sleep(Duration::from_millis(500)).await;
    }

    // ── Phase 4: Probe immediately — this is where the bug shows ─────────
    println!("\n[PHASE 4] Probing single-key ops immediately after tend eviction");
    println!("  (partition map likely stale — dead node still listed as master)");
    println!("  Nodes visible: {}", client.nodes().len());

    let start = Instant::now();
    let (get_ok, get_fail) = probe_get(&client, namespace, set_name, bin_name, num_records).await;
    let get_elapsed = start.elapsed().as_secs_f64();
    println!(
        "  get:    ok={:<5} fail={:<5} ({:.1}s)",
        get_ok, get_fail, get_elapsed
    );

    let (put_ok, put_fail) = (0, 0);
    let put_elapsed = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let (put_ok, put_fail) = probe_put(&client, namespace, set_name, bin_name, num_records).await;
    let put_elapsed = start.elapsed().as_secs_f64();
    println!(
        "  put:    ok={:<5} fail={:<5} ({:.1}s)",
        put_ok, put_fail, put_elapsed
    );

    let start = Instant::now();
    let (rm_ok, rm_fail) = probe_remove(&client, namespace, set_name, bin_name, num_records).await;
    let rm_elapsed = start.elapsed().as_secs_f64();
    println!(
        "  remove: ok={:<5} fail={:<5} ({:.1}s)",
        rm_ok, rm_fail, rm_elapsed
    );

    // ── Cleanup: restart killed node ─────────────────────────────────────
    // aerolab containers use a keep-alive loop as CMD, so `docker start`
    // only restarts the container — asd must be re-launched separately.
    println!("\n[CLEANUP]");
    let _ = std::panic::catch_unwind(|| {
        run_cmd(&start_cmd, "START-NODE");
    });
    if !restart_asd_cmd.is_empty() {
        let _ = std::panic::catch_unwind(|| {
            run_cmd(&restart_asd_cmd, "RESTART-ASD");
        });
    }
    // Give the restarted node time to rejoin the cluster
    aerospike_rt::sleep(Duration::from_secs(5)).await;
    let ap = AdminPolicy::default();
    let _ = client.truncate(&ap, namespace, set_name, 0).await;
    let _ = client.close().await;

    // ── Verdict ──────────────────────────────────────────────────────────
    println!("\n{}", "=".repeat(60));
    println!(
        "RESULTS (topology: {} nodes → kill 1 → {})",
        initial_nodes,
        initial_nodes - 1
    );
    println!("{}", "=".repeat(60));
    println!(
        "  get:    {}/{} failures in {:.1}s",
        get_fail, num_records, get_elapsed
    );
    println!(
        "  put:    {}/{} failures in {:.1}s",
        put_fail, num_records, put_elapsed
    );
    println!(
        "  remove: {}/{} failures in {:.1}s",
        rm_fail, num_records, rm_elapsed
    );

    let total_fail = get_fail + put_fail + rm_fail;
    if total_fail > 0 {
        panic!(
            "\nSTALE PARTITION MAP WRITE BUG CONFIRMED\n\
             {}/{} total single-key operations failed after node eviction.\n\
             get={} put={} remove={}\n\n\
             Root cause: partition map still references dead node as master.\n\
             is_active() filter rejects it, but no fallback for writes.\n\
             Client spin-retries until total_timeout expires.",
            total_fail,
            num_records * 3,
            get_fail,
            put_fail,
            rm_fail,
        );
    } else {
        println!("\n  No stale partition map write bug detected this run.");
        println!("  (partition map may have refreshed before probes ran)");
    }
}

async fn probe_get(client: &Client, ns: &str, set: &str, _bin: &str, n: i64) -> (i64, i64) {
    let mut rp = ReadPolicy::default();
    rp.base_policy.total_timeout = 3000;
    rp.base_policy.max_retries = 0;

    let (mut ok, mut fail) = (0i64, 0i64);
    for i in 0..n {
        let key = as_key!(ns, set, i);
        match client.get(&rp, &key, Bins::All).await {
            Ok(_) => ok += 1,
            Err(e) => {
                if fail < 5 {
                    println!("    get key={}: {:?}", i, e);
                }
                fail += 1;
            }
        }
    }
    (ok, fail)
}

async fn probe_put(client: &Client, ns: &str, set: &str, bin: &str, n: i64) -> (i64, i64) {
    let mut wp = WritePolicy::default();
    wp.base_policy.total_timeout = 3000;
    wp.base_policy.max_retries = 0;

    let (mut ok, mut fail) = (0i64, 0i64);
    for i in 0..n {
        let key = as_key!(ns, set, i);
        let bins = vec![as_bin!(bin, i + 1)];
        match client.put(&wp, &key, &bins).await {
            Ok(_) => ok += 1,
            Err(e) => {
                if fail < 5 {
                    println!("    put key={}: {:?}", i, e);
                }
                fail += 1;
            }
        }
    }
    (ok, fail)
}

/// Matches `Partition::new_by_key` in aerospike-core (digest → partition id, 0..4096).
fn partition_id_for_key(key: &Key) -> usize {
    const PARTITIONS: usize = 4096;
    (u16::from_le_bytes([key.digest[0], key.digest[1]]) as usize) & (PARTITIONS - 1)
}

async fn probe_remove(client: &Client, ns: &str, set: &str, _bin: &str, n: i64) -> (i64, i64) {
    let mut wp = WritePolicy::default();
    wp.base_policy.total_timeout = 3000;
    wp.base_policy.max_retries = 0;

    let (mut ok, mut fail) = (0i64, 0i64);
    for i in 0..n {
        let key = as_key!(ns, set, i);
        if i == 0i64 || i == 2i64 || i == 6i64 {
            let pid = partition_id_for_key(&key);
            println!(
                "  [probe_remove] sample key i={} partition_id={} ns={} set={}",
                i, pid, ns, set
            );
        }

        match client.delete(&wp, &key).await {
            Ok(_) => ok += 1,
            Err(e) => {
                if fail < 5 {
                    println!("    remove key={}: {:?}", i, e);
                }
                fail += 1;
            }
        }
    }
    (ok, fail)
}
