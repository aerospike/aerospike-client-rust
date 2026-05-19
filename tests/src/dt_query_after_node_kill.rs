use crate::common;
use aerospike::query::{Filter, PartitionFilter};
use aerospike::*;
use futures::stream::StreamExt;
use std::process::Command as ShellCmd;
use std::time::{Duration, Instant};

/// Reproduce CLIENT-4405 regression: stale partition map after node kill.
///
/// Runs 4 independent rounds — each round tests ONE operation type with
/// a fresh kill/restart cycle so that timing cannot mask the bug:
///
///   Round 1: SI query  (Filter::range + PartitionFilter::all)
///   Round 2: PF query  (no filter, PartitionFilter::all, Long duration)
///   Round 3: Single-key get
///   Round 4: Single-key put
///
/// Each round:
///   1. Verify 4 nodes + data present
///   2. Kill node 3
///   3. Wait for tend eviction + server migrations
///   4. Probe ONE operation type (immediately — no other ops first)
///   5. Restart node 3, wait for rejoin + migrations
///   6. Record pass/fail
///
/// ─── SETUP (aerolab) ───────────────────────────────────────────────
///
///   aerolab cluster destroy -n mydc -f
///   aerolab cluster create -n mydc -c 4 -i 22.04
///
/// ─── RUN (aerolab) ─────────────────────────────────────────────────
///
///   RUN_QUERY_AFTER_KILL=1 \
///     AEROSPIKE_HOSTS="127.0.0.1:3100" \
///     AEROSPIKE_USE_SERVICES_ALTERNATE=true \
///     STOP_CMD="docker kill aerolab-mydc_3" \
///     START_CMD="docker start aerolab-mydc_3" \
///     RESTART_ASD_CMD="docker exec aerolab-mydc_3 /usr/bin/asd --config-file /etc/aerospike/aerospike.conf" \
///     WAIT_MIGRATION_NODE="aerolab-mydc_1" \
///     cargo test --features rt-tokio -- test_query_after_node_kill --nocapture
///
/// ─── RUN (docker bridge) ───────────────────────────────────────────
///
///   cd tools/docker-bridge-test && docker compose up --build
///
/// ─── EXPECTED ───────────────────────────────────────────────────────
///
///   BUG PRESENT:  one or more rounds show failures/missing records
///   BUG FIXED:    all 4 rounds pass (partition map cleaned on eviction)
#[aerospike_macro::test]
async fn test_query_after_node_kill() {
    let run_flag = std::env::var("RUN_QUERY_AFTER_KILL").unwrap_or_default();
    if !["1", "true", "yes"].contains(&run_flag.as_str()) {
        println!("Skipping: set RUN_QUERY_AFTER_KILL=1 to run");
        return;
    }

    let container_prefix = std::env::var("AS_CONTAINER_PREFIX").ok();
    let (stop_cmd, start_cmd, restart_asd_cmd, wait_migration_node) =
        if let Some(ref pfx) = container_prefix {
            let kill_target = format!("{}3", pfx);
            (
                format!("docker kill {}", kill_target),
                format!("docker start {}", kill_target),
                String::new(),
                format!("{}1", pfx),
            )
        } else {
            (
                std::env::var("STOP_CMD").expect("Set STOP_CMD or AS_CONTAINER_PREFIX"),
                std::env::var("START_CMD").expect("Set START_CMD or AS_CONTAINER_PREFIX"),
                std::env::var("RESTART_ASD_CMD").unwrap_or_default(),
                std::env::var("WAIT_MIGRATION_NODE")
                    .unwrap_or_else(|_| "aerolab-mydc_1".to_string()),
            )
        };

    let namespace = common::namespace();
    let set_name = "query_kill_repro";
    let bin_name = "v";
    let idx_name = format!("idx_{}_{}_{}", namespace, set_name, bin_name);
    let is_bridge = container_prefix.is_some();
    let num_records: i64 = std::env::var("RECORD_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(if is_bridge { 500 } else { 5000 });

    let client = common::client().await;
    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();

    // ── Initial setup: verify cluster, create index, load data ───────
    let initial_nodes = client.nodes().len();
    println!("\n[SETUP] Client sees {} nodes", initial_nodes);
    if initial_nodes < 4 {
        println!(
            "  Need 4+ nodes. Got {}.\n  Skipping test.",
            initial_nodes
        );
        return;
    }
    for node in client.nodes() {
        println!("  {}", node.name());
    }

    let _ = client
        .drop_index(&apolicy, namespace, set_name, &idx_name)
        .await;
    aerospike_rt::sleep(Duration::from_secs(1)).await;

    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            set_name,
            bin_name,
            &idx_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create secondary index");
    task.wait_till_complete(None).await.unwrap();

    for i in 0..num_records {
        let key = as_key!(namespace, set_name, i);
        let bins = vec![as_bin!(bin_name, i)];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }
    println!(
        "[SETUP] Loaded {} records, sindex '{}' created\n",
        num_records, idx_name
    );

    // ── Define the 4 rounds ──────────────────────────────────────────
    let op_names = ["SI query", "PF query", "Single-key GET", "Single-key PUT"];
    let mut results: Vec<RoundResult> = Vec::new();

    for (round_idx, op_name) in op_names.iter().enumerate() {
        let round_num = round_idx + 1;
        println!(
            "{}",
            "═".repeat(60)
        );
        println!(
            "  ROUND {}/{}:  {}",
            round_num,
            op_names.len(),
            op_name
        );
        println!(
            "{}",
            "═".repeat(60)
        );

        // 1. Verify cluster is healthy (4 nodes, data intact)
        let n = client.nodes().len();
        println!("  [pre-check] nodes={}", n);
        if n < initial_nodes {
            println!(
                "  ERROR: Expected {} nodes but only {}. Skipping round.",
                initial_nodes, n
            );
            results.push(RoundResult {
                op: op_name.to_string(),
                status: "SKIP".into(),
                detail: format!("only {} nodes", n),
                elapsed: 0.0,
            });
            continue;
        }

        // 2. Kill node
        println!("  [kill] Killing node ({}→{})...", initial_nodes, initial_nodes - 1);
        assert!(run_cmd(&stop_cmd, "STOP"), "Stop command failed");

        // 3. Wait for eviction
        println!("  [evict] Waiting for tend to evict dead node...");
        let evict_start = Instant::now();
        let mut evicted = false;
        loop {
            let cnt = client.nodes().len();
            if cnt < initial_nodes {
                println!(
                    "  [evict] Evicted after {:.1}s (now {} nodes)",
                    evict_start.elapsed().as_secs_f64(),
                    cnt
                );
                evicted = true;
                break;
            }
            // change to 30 for time being
            if evict_start.elapsed() > Duration::from_secs(30) {
                println!(
                    "  [evict] WARNING: not evicted after 30s (still {} nodes). Proceeding.",
                    cnt
                );
                break;
            }
            aerospike_rt::sleep(Duration::from_millis(500)).await;
        }

        // 4. Wait for server-side migrations
        wait_migrations(&wait_migration_node, 120).await;

        let visible = client.nodes().len();
        println!(
            "  [probe] Probing {} immediately (nodes visible: {}, evicted: {})",
            op_name, visible, evicted
        );

        // 5. Probe — ONE operation type only
        let start = Instant::now();
        let (status, detail) = match round_idx {
            0 => {
                let (_, fail, count) =
                    probe_si_query(&client, namespace, set_name, bin_name, 0, num_records).await;
                let ok = fail == 0 && count == num_records;
                (
                    if ok { "OK" } else { "FAIL" },
                    format!("records={}/{}  stream_errors={}", count, num_records, fail),
                )
            }
            1 => {
                let (_, fail, count) =
                    probe_pf_query(&client, namespace, set_name).await;
                let ok = fail == 0 && count == num_records;
                (
                    if ok { "OK" } else { "FAIL" },
                    format!("records={}/{}  stream_errors={}", count, num_records, fail),
                )
            }
            2 => {
                let (ok_cnt, fail_cnt) =
                    probe_get(&client, namespace, set_name, num_records).await;
                (
                    if fail_cnt == 0 { "OK" } else { "FAIL" },
                    format!("ok={}  fail={}", ok_cnt, fail_cnt),
                )
            }
            3 => {
                let (ok_cnt, fail_cnt) =
                    probe_put(&client, namespace, set_name, bin_name, num_records).await;
                (
                    if fail_cnt == 0 { "OK" } else { "FAIL" },
                    format!("ok={}  fail={}", ok_cnt, fail_cnt),
                )
            }
            _ => unreachable!(),
        };
        let elapsed = start.elapsed().as_secs_f64();
        println!(
            "  [result] {}  {}  ({:.1}s)  [{}]",
            op_name, detail, elapsed, status
        );

        results.push(RoundResult {
            op: op_name.to_string(),
            status: status.into(),
            detail,
            elapsed,
        });

        // 6. Restart killed node and wait for rejoin
        println!("  [restart] Restarting killed node...");
        let _ = run_cmd(&start_cmd, "START");
        if !restart_asd_cmd.is_empty() {
            aerospike_rt::sleep(Duration::from_secs(2)).await;
            let _ = run_cmd(&restart_asd_cmd, "RESTART-ASD");
        }

        // Wait for the node to rejoin the cluster
        let rejoin_start = Instant::now();
        loop {
            let cnt = client.nodes().len();
            if cnt >= initial_nodes {
                println!(
                    "  [restart] Node rejoined after {:.1}s ({} nodes)",
                    rejoin_start.elapsed().as_secs_f64(),
                    cnt
                );
                break;
            }
            if rejoin_start.elapsed() > Duration::from_secs(60) {
                println!(
                    "  [restart] WARNING: node did not rejoin after 60s (still {} nodes)",
                    cnt
                );
                break;
            }
            aerospike_rt::sleep(Duration::from_secs(1)).await;
        }

        // Wait for migrations to finish before next round
        wait_migrations(&wait_migration_node, 120).await;
        // Extra settle time for partition map to propagate
        aerospike_rt::sleep(Duration::from_secs(3)).await;

        println!();
    }

    // ── Cleanup ──────────────────────────────────────────────────────
    let _ = client
        .drop_index(&apolicy, namespace, set_name, &idx_name)
        .await;
    let _ = client.truncate(&apolicy, namespace, set_name, 0).await;
    let _ = client.close().await;

    // ── Final verdict ────────────────────────────────────────────────
    println!("\n{}", "═".repeat(60));
    println!("  FINAL RESULTS");
    println!("{}", "═".repeat(60));
    let mut any_fail = false;
    for (i, r) in results.iter().enumerate() {
        let marker = match r.status.as_str() {
            "FAIL" => {
                any_fail = true;
                "FAIL"
            }
            "OK" => " OK ",
            _ => "SKIP",
        };
        println!(
            "  Round {}: [{}]  {:<16}  {}  ({:.1}s)",
            i + 1,
            marker,
            r.op,
            r.detail,
            r.elapsed
        );
    }
    println!("{}", "═".repeat(60));

    if any_fail {
        let mut msg = String::from(
            "\nSTALE PARTITION MAP BUG CONFIRMED (CLIENT-4405 regression)\n\n\
             Each round had a fresh kill/restart cycle.\n\
             Failed operations could not reach partitions owned by the dead node.\n\n\
             Failing rounds:\n",
        );
        for (i, r) in results.iter().enumerate() {
            if r.status == "FAIL" {
                msg.push_str(&format!("  Round {}: {}  {}\n", i + 1, r.op, r.detail));
            }
        }
        panic!("{}", msg);
    } else {
        println!("\n  All 4 rounds passed. Partition map correctly cleaned up after each kill.");
    }
}

struct RoundResult {
    op: String,
    status: String,
    detail: String,
    elapsed: f64,
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn run_cmd(cmd_str: &str, label: &str) -> bool {
    println!("  [{}] Running: {}", label, cmd_str);
    let parts: Vec<&str> = cmd_str.split_whitespace().collect();
    let output = ShellCmd::new(parts[0])
        .args(&parts[1..])
        .output()
        .unwrap_or_else(|e| panic!("[{}] Failed to execute: {}", label, e));
    if !output.status.success() {
        println!(
            "  [{}] Failed (rc={}): {:?}",
            label,
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
        return false;
    }
    println!("  [{}] OK", label);
    true
}

async fn wait_migrations(container: &str, timeout_secs: u64) {
    println!("  [migrate] Waiting for migrations (via {})...", container);
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
                        "  [migrate] Complete after {:.1}s",
                        start.elapsed().as_secs_f64()
                    );
                    return;
                }
                if start.elapsed().as_secs() % 10 < 2 {
                    println!(
                        "  [migrate] remaining={} ({:.1}s)",
                        remaining,
                        start.elapsed().as_secs_f64()
                    );
                }
            }
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr).trim().to_string();
                println!(
                    "  [migrate] asinfo rc={}: {:?} ({:.1}s)",
                    o.status, stderr, start.elapsed().as_secs_f64()
                );
            }
            Err(e) => {
                println!(
                    "  [migrate] docker exec failed: {} ({:.1}s)",
                    e,
                    start.elapsed().as_secs_f64()
                );
            }
        }
        if start.elapsed() > Duration::from_secs(timeout_secs) {
            println!(
                "  [migrate] WARNING: not complete after {}s. Proceeding.",
                timeout_secs
            );
            return;
        }
        aerospike_rt::sleep(Duration::from_secs(2)).await;
    }
}

async fn probe_si_query(
    client: &Client,
    ns: &str,
    set: &str,
    bin: &str,
    range_lo: i64,
    range_hi: i64,
) -> (i64, i64, i64) {
    let mut qp = QueryPolicy::default();
    qp.base_policy.total_timeout = 10_000;
    qp.base_policy.socket_timeout = 5_000;
    qp.base_policy.max_retries = 1;

    let mut stmt = Statement::new(ns, set, Bins::All);
    stmt.add_filter(Filter::range(bin, range_lo, range_hi - 1));
    let pf = PartitionFilter::all();

    match client.query(&qp, pf, stmt).await {
        Ok(rs) => {
            let mut stream = rs.into_stream();
            let (ok, mut fail, mut count) = (1i64, 0i64, 0i64);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(_) => count += 1,
                    Err(e) => {
                        if fail < 3 {
                            println!("    SI stream error: {:?}", e);
                        }
                        fail += 1;
                    }
                }
            }
            (ok, fail, count)
        }
        Err(e) => {
            println!("    SI query failed to start: {:?}", e);
            (0, 1, 0)
        }
    }
}

async fn probe_pf_query(client: &Client, ns: &str, set: &str) -> (i64, i64, i64) {
    let mut qp = QueryPolicy::default();
    qp.base_policy.total_timeout = 10_000;
    qp.base_policy.socket_timeout = 5_000;
    qp.base_policy.max_retries = 1;
    qp.expected_duration = QueryDuration::Long;

    let stmt = Statement::new(ns, set, Bins::All);
    let pf = PartitionFilter::all();

    match client.query(&qp, pf, stmt).await {
        Ok(rs) => {
            let mut stream = rs.into_stream();
            let (ok, mut fail, mut count) = (1i64, 0i64, 0i64);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(_) => count += 1,
                    Err(e) => {
                        if fail < 3 {
                            println!("    PF stream error: {:?}", e);
                        }
                        fail += 1;
                    }
                }
            }
            (ok, fail, count)
        }
        Err(e) => {
            println!("    PF query failed to start: {:?}", e);
            (0, 1, 0)
        }
    }
}

async fn probe_get(client: &Client, ns: &str, set: &str, n: i64) -> (i64, i64) {
    let mut rp = ReadPolicy::default();
    rp.base_policy.total_timeout = 1000;
    rp.base_policy.socket_timeout = 500;
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
                if fail >= 10 {
                    println!(
                        "    (stopping early after {} failures in {} ops)",
                        fail,
                        ok + fail
                    );
                    break;
                }
            }
        }
    }
    (ok, fail)
}

async fn probe_put(client: &Client, ns: &str, set: &str, bin: &str, n: i64) -> (i64, i64) {
    let mut wp = WritePolicy::default();
    wp.base_policy.total_timeout = 1000;
    wp.base_policy.socket_timeout = 500;
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
                if fail >= 10 {
                    println!(
                        "    (stopping early after {} failures in {} ops)",
                        fail,
                        ok + fail
                    );
                    break;
                }
            }
        }
    }
    (ok, fail)
}
