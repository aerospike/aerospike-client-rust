use crate::common;
use aerospike::*;
use std::process::Command as ShellCmd;
use std::time::{Duration, Instant};

/// Reproduces CLIENT-4405: stale partition map after node replacement
/// on a Docker bridge network with real IP address changes.
///
/// This test MUST run inside a Docker container on the same bridge
/// network as the Aerospike nodes. It connects via bridge IPs
/// (172.x.x.x), NOT localhost port mappings. When a node container
/// is killed, its IP genuinely becomes unreachable (TCP blackhole),
/// which is what exposes the partition map staleness bug.
///
/// The test mirrors the Python test_node_replacement_ip_change flow:
///   1. Start with 4 Aerospike nodes on a bridge network
///   2. Load 10000 records
///   3. Loop 3 times: kill one node, wait for eviction + migrations,
///      probe get/put/remove
///   4. Final cleanup probe (put/remove all records)
///   5. Report: Rust client shows ~25% put/remove failures because
///      its partition map still references the dead node's IP.
///      Java/C#/Go clients show 0 failures in the same scenario.
///
/// ─── SETUP (via docker-compose) ─────────────────────────────────
///   cd tools/docker-bridge-test
///   docker compose up --build
///
/// ─── MANUAL RUN (if already on bridge network) ──────────────────
///   RUN_BRIDGE_REPLACEMENT=1 \
///     AEROSPIKE_HOSTS="aerospike-1:3000" \
///     cargo test --features rt-tokio \
///     -- test_bridge_replacement_loop --nocapture
///
/// ─── ENV VARS ───────────────────────────────────────────────────
///   RUN_BRIDGE_REPLACEMENT=1        # gate: skip unless set
///   AEROSPIKE_HOSTS                 # seed node(s) on bridge
///   AS_CONTAINER_PREFIX             # container name prefix (default: "as-bridge-")
///   AS_NODE_COUNT                   # total nodes (default: 4)
///   RECORD_COUNT                    # records to load (default: 10000)
#[aerospike_macro::test]
async fn test_bridge_replacement_loop() {
    let run_flag = std::env::var("RUN_BRIDGE_REPLACEMENT").unwrap_or_default();
    if !["1", "true", "yes"].contains(&run_flag.as_str()) {
        println!("Skipping: set RUN_BRIDGE_REPLACEMENT=1 to run");
        return;
    }

    let namespace = common::namespace();
    let set_name = "bridge_replace";
    let bin_name = "v";
    let num_records: i64 = std::env::var("RECORD_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10000);

    let container_prefix = std::env::var("AS_CONTAINER_PREFIX")
        .unwrap_or_else(|_| "as-bridge-".to_string());
    let node_count: usize = std::env::var("AS_NODE_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);

    let client = common::client().await;
    let wpolicy = WritePolicy::default();

    // ── Phase 1: Verify starting state ───────────────────────────────
    let initial_nodes = client.nodes().len();
    println!("\n[PHASE 1] Client sees {} nodes (expected {})", initial_nodes, node_count);
    if initial_nodes < node_count {
        println!(
            "  ERROR: Need {} nodes, got {}. Check docker compose.",
            node_count, initial_nodes,
        );
        return;
    }

    // Print node addresses so we can verify bridge IPs
    println!("  Node addresses:");
    for node in client.nodes() {
        println!("    {}", node.name());
    }

    // Keep node 1 alive as the seed/asinfo target
    let asinfo_container = format!("{}1", container_prefix);
    let nodes_to_kill: Vec<String> = (2..=node_count)
        .map(|i| format!("{}{}", container_prefix, i))
        .collect();
    let last_idx = nodes_to_kill.len() - 1;

    // ── Phase 2: Load data ───────────────────────────────────────────
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

    let baseline_rp = ReadPolicy::default();
    let (_, baseline_fail) = probe_get(&client, namespace, set_name, bin_name, num_records, &baseline_rp).await;
    assert_eq!(baseline_fail, 0, "Baseline get should have 0 failures");
    println!("  Baseline get: all {} passed", num_records);

    // ── Phase 3: Replacement loop ────────────────────────────────────
    for (iteration, container) in nodes_to_kill.iter().enumerate() {
        let iter_num = iteration + 1;
        let is_last = iteration == last_idx;
        println!(
            "\n{}\n── Replacement {}/{}: killing {}{} ──\n{}",
            "═".repeat(60), iter_num, nodes_to_kill.len(), container,
            if is_last { " (FINAL — will NOT restart)" } else { "" },
            "═".repeat(60)
        );

        let current_nodes = client.nodes().len();
        println!("  Nodes before kill: {}", current_nodes);
        println!("  Node addresses:");
        for node in client.nodes() {
            println!("    {}", node.name());
        }

        // ── Kill the node ────────────────────────────────────────────
        println!("  Killing {}...", container);
        let ok = run_cmd(&format!("docker kill {}", container), "KILL");
        if !ok {
            println!("  WARNING: docker kill failed. Trying docker stop...");
            run_cmd(&format!("docker stop -t 0 {}", container), "STOP");
        }

        // ── Wait for tend to evict ───────────────────────────────────
        println!("  Waiting for tend to evict dead node...");
        let evict_start = Instant::now();
        loop {
            let n = client.nodes().len();
            if n < current_nodes {
                println!(
                    "  Node evicted after {:.1}s (now {} nodes)",
                    evict_start.elapsed().as_secs_f64(), n
                );
                break;
            }
            if evict_start.elapsed() > Duration::from_secs(60) {
                println!("  WARNING: not evicted after 60s ({} nodes)", n);
                break;
            }
            aerospike_rt::sleep(Duration::from_millis(500)).await;
        }

        println!("  Node addresses after eviction:");
        for node in client.nodes() {
            println!("    {}", node.name());
        }

        // ── Wait for migrations ──────────────────────────────────────
        wait_migrations_bridge(&asinfo_container, 120).await;

        // ── Verify: get/put/remove ───────────────────────────────────
        println!("  Post-kill verification (get+put+remove {} records)...", num_records);

        let rp = ReadPolicy::default();
        let start = Instant::now();
        let (get_ok, get_fail) = probe_get(&client, namespace, set_name, bin_name, num_records, &rp).await;
        let get_elapsed = start.elapsed().as_secs_f64();

        let start = Instant::now();
        let (put_ok, put_fail) = probe_put(&client, namespace, set_name, bin_name, num_records).await;
        let put_elapsed = start.elapsed().as_secs_f64();

        let start = Instant::now();
        let (rm_ok, rm_fail) = probe_remove(&client, namespace, set_name, num_records).await;
        let rm_elapsed = start.elapsed().as_secs_f64();

        println!(
            "  Replacement {}:\n    get:    ok={:<6} fail={:<6} ({:.1}s)\n    put:    ok={:<6} fail={:<6} ({:.1}s)\n    remove: ok={:<6} fail={:<6} ({:.1}s)",
            iter_num,
            get_ok, get_fail, get_elapsed,
            put_ok, put_fail, put_elapsed,
            rm_ok, rm_fail, rm_elapsed,
        );

        if rm_ok > 0 {
            println!("  Re-loading records that were successfully removed...");
            for i in 0..num_records {
                let key = as_key!(namespace, set_name, i);
                let bins = vec![as_bin!(bin_name, i)];
                let _ = client.put(&wpolicy, &key, &bins).await;
            }
        }

        if is_last {
            println!("  FINAL node — leaving it dead for Phase 4");
        } else {
            println!("  Restarting {}...", container);
            run_cmd(&format!("docker start {}", container), "START");

            println!("  Waiting for node to rejoin cluster...");
            let rejoin_start = Instant::now();
            loop {
                let n = client.nodes().len();
                if n >= current_nodes {
                    println!(
                        "  Node rejoined after {:.1}s (now {} nodes)",
                        rejoin_start.elapsed().as_secs_f64(), n
                    );
                    break;
                }
                if rejoin_start.elapsed() > Duration::from_secs(60) {
                    println!("  WARNING: node not rejoined after 60s ({} nodes)", n);
                    break;
                }
                aerospike_rt::sleep(Duration::from_secs(1)).await;
            }

            wait_migrations_bridge(&asinfo_container, 120).await;
        }

        println!("  Replacement {}/{} complete", iter_num, nodes_to_kill.len());
    }

    // ── Phase 4: Final cleanup probe ─────────────────────────────────
    println!("\n{}", "═".repeat(60));
    println!("PHASE 4: Post-replacement cleanup probe");
    println!("  Nodes visible: {}", client.nodes().len());
    println!("  Node addresses:");
    for node in client.nodes() {
        println!("    {}", node.name());
    }
    println!("{}", "═".repeat(60));

    let rp_final = ReadPolicy::default();
    let start = Instant::now();
    let (get_ok, get_fail) = probe_get(&client, namespace, set_name, bin_name, num_records, &rp_final).await;
    let get_elapsed = start.elapsed().as_secs_f64();
    println!("  get:    ok={:<6} fail={:<6} ({:.1}s)", get_ok, get_fail, get_elapsed);

    let start = Instant::now();
    let (put_ok, put_fail) = probe_put(&client, namespace, set_name, bin_name, num_records).await;
    let put_elapsed = start.elapsed().as_secs_f64();
    println!("  put:    ok={:<6} fail={:<6} ({:.1}s)", put_ok, put_fail, put_elapsed);

    let start = Instant::now();
    let (rm_ok, rm_fail) = probe_remove(&client, namespace, set_name, num_records).await;
    let rm_elapsed = start.elapsed().as_secs_f64();
    println!("  remove: ok={:<6} fail={:<6} ({:.1}s)", rm_ok, rm_fail, rm_elapsed);

    // ── Cleanup: restart dead nodes ──────────────────────────────────
    println!("\n  Restoring killed nodes for cleanup...");
    for container in &nodes_to_kill {
        let _ = run_cmd(&format!("docker start {}", container), "CLEANUP-START");
    }
    aerospike_rt::sleep(Duration::from_secs(5)).await;

    let ap = AdminPolicy::default();
    let _ = client.truncate(&ap, namespace, set_name, 0).await;
    let _ = client.close().await;

    // ── Verdict ──────────────────────────────────────────────────────
    println!("\n{}", "=".repeat(60));
    println!("RESULTS ({} replacement cycles, then cleanup probe)", nodes_to_kill.len());
    println!("{}", "=".repeat(60));
    println!("  get:    {}/{} failures in {:.1}s", get_fail, num_records, get_elapsed);
    println!("  put:    {}/{} failures in {:.1}s", put_fail, num_records, put_elapsed);
    println!("  remove: {}/{} failures in {:.1}s", rm_fail, num_records, rm_elapsed);

    let total_fail = get_fail + put_fail + rm_fail;
    if total_fail > 0 {
        panic!(
            "\nCLIENT-4405 REPRODUCED ON BRIDGE NETWORK (no ASX, pure Rust client)\n\
             {}/{} operations failed after {} node-replacement cycles + migrations.\n\
             get={} put={} remove={}\n\n\
             This is the same ~25% put/remove failure rate seen in the Python\n\
             ASX test, confirming the bug is in the Rust client partition map\n\
             management, not the test framework.\n\n\
             Comparison (same scenario, same bridge network):\n\
             - Java client:  0 failures\n\
             - C# client:    0 failures\n\
             - Go client:    0-2 transient failures, full recovery\n\
             - Rust client:  ~25% put/remove failures (THIS TEST)",
            total_fail,
            num_records * 3,
            nodes_to_kill.len(),
            get_fail, put_fail, rm_fail,
        );
    } else {
        println!("\n  All operations passed. Bug may be fixed in this version.");
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn run_cmd(cmd_str: &str, label: &str) -> bool {
    println!("  [{}] Running: {}", label, cmd_str);
    let parts: Vec<&str> = cmd_str.split_whitespace().collect();
    let output = ShellCmd::new(parts[0])
        .args(&parts[1..])
        .output()
        .unwrap_or_else(|e| panic!("[{}] Failed to execute: {}", label, e));
    if !output.status.success() {
        println!(
            "  [{}] Command failed (rc={}): stderr={:?}",
            label, output.status,
            String::from_utf8_lossy(&output.stderr),
        );
        return false;
    }
    println!("  [{}] OK", label);
    true
}

async fn wait_migrations_bridge(container: &str, timeout_secs: u64) {
    println!("  Waiting for server-side migrations (via {})...", container);
    let start = Instant::now();
    loop {
        let output = ShellCmd::new("docker")
            .args(&["exec", container, "asinfo", "-v", "statistics"])
            .output();
        match output {
            Ok(o) if o.status.success() => {
                let stats = String::from_utf8_lossy(&o.stdout);
                let remaining = stats
                    .split(';')
                    .find(|s| s.starts_with("migrate_partitions_remaining="))
                    .and_then(|s| s.split('=').nth(1))
                    .and_then(|v| v.trim().parse::<i64>().ok())
                    .unwrap_or(-1);
                if remaining == 0 {
                    println!(
                        "  Migrations complete after {:.1}s",
                        start.elapsed().as_secs_f64()
                    );
                    return;
                }
                if start.elapsed().as_secs() % 10 < 2 {
                    println!(
                        "  migrate_partitions_remaining = {} ({:.1}s)",
                        remaining, start.elapsed().as_secs_f64()
                    );
                }
            }
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr).trim().to_string();
                println!(
                    "  asinfo returned rc={}: stderr={:?} ({:.1}s)",
                    o.status, stderr, start.elapsed().as_secs_f64()
                );
            }
            Err(e) => {
                println!("  docker exec failed: {} ({:.1}s)", e, start.elapsed().as_secs_f64());
            }
        }
        if start.elapsed() > Duration::from_secs(timeout_secs) {
            println!("  WARNING: migrations not complete after {}s. Proceeding.", timeout_secs);
            return;
        }
        aerospike_rt::sleep(Duration::from_secs(2)).await;
    }
}

async fn probe_get(
    client: &Client,
    ns: &str,
    set: &str,
    _bin: &str,
    n: i64,
    rp: &ReadPolicy,
) -> (i64, i64) {
    let (mut ok, mut fail) = (0i64, 0i64);
    for i in 0..n {
        let key = as_key!(ns, set, i);
        match client.get(rp, &key, Bins::All).await {
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

async fn probe_put(
    client: &Client,
    ns: &str,
    set: &str,
    bin: &str,
    n: i64,
) -> (i64, i64) {
    let mut wp = WritePolicy::default();
    wp.base_policy.total_timeout = 5000;
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

async fn probe_remove(
    client: &Client,
    ns: &str,
    set: &str,
    n: i64,
) -> (i64, i64) {
    let mut wp = WritePolicy::default();
    wp.base_policy.total_timeout = 5000;
    wp.base_policy.max_retries = 0;

    let (mut ok, mut fail) = (0i64, 0i64);
    for i in 0..n {
        let key = as_key!(ns, set, i);
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
