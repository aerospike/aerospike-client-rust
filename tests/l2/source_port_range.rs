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

//! Testing strategy row F-3 against the `port-range` profile (Java
//! `TestL2SourcePortRange`), standing in for NLB source NAT port exhaustion.
//! Toxiproxy runs with a narrow local port range, which caps the upstream
//! connections it can open to each node. In its own profile so that a
//! failing F-3 does not fail the other rows.

use std::sync::Arc;
use std::time::Instant;

use aerospike::{Bin, Bins, Client, ClientPolicy, ReadPolicy, Value, WritePolicy};

use super::load::{kinds, Load};
use super::rows;
use super::topology::{self as topo};
use super::{run, skip_unless};

const BLOCKER: &str = "F-3 needs DISCOVERY_PROXY_PORT_RANGE, Toxiproxy with a narrow \
                       net.ipv4.ip_local_port_range (run_l2.sh L2_PROFILE=port-range)";
const SET: &str = "l2ports";
const KEY_COUNT: usize = 1000;
const RECOVERY_KEY_COUNT: usize = 100;
const SOCKET_TIMEOUT: u32 = 2000;
const TOTAL_TIMEOUT: u32 = 4000;

/// The process's resident set in MiB: from /proc, or from `ps` where there
/// is no /proc; 0 when neither answers.
fn resident_mb() -> u64 {
    let from_proc = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<u64>().ok())
        });
    let from_ps = || {
        std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()
            .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<u64>().ok())
    };
    from_proc.or_else(from_ps).map_or(0, |kb| kb / 1024)
}

async fn round_succeeds(client: &Client, wp: &WritePolicy, rp: &ReadPolicy, count: usize) -> bool {
    for i in 0..count {
        let key = rows::key(SET, format!("f3-round-{i}"));
        if client.put(wp, &key, &[Bin::new("v", Value::from(i as i64))]).await.is_err() {
            return false;
        }
        if client.get(rp, &key, Bins::All).await.is_err() {
            return false;
        }
    }
    true
}

/// F-3: with more concurrent commands than the proxy has source ports for,
/// and max_conns_per_node raised above that, the client surfaces a clear
/// error within a bound from socket_timeout and total_timeout rather than
/// hanging, the error types and counts are observable from the client, and
/// commands succeed again once the load drops.
#[test]
fn f3_source_port_exhaustion_surfaces_error_and_recovers() {
    run("F-3", || async {
        if !skip_unless(!topo::t().proxy_port_range.is_empty(), BLOCKER) {
            return;
        }
        let range: Vec<u32> = topo::t()
            .proxy_port_range
            .split_whitespace()
            .map(|p| p.parse().expect("port"))
            .collect();
        let ports = (range[1] - range[0] + 1) as usize;
        let nodes = topo::nodes().await;
        let tasks = 2 * ports * nodes.len();
        let mut policy = topo::policy();
        policy.address_translator = Some(rows::port_map(topo::port_map_by_endpoint(&nodes)));
        policy.max_conns_per_node = 4 * tasks;
        println!(
            "L2-F3-RANGE proxyLocalPortRange={} ports={ports} nodes={} tasks={tasks} maxConnsPerNode={} defaultMaxConnsPerNode={}",
            topo::t().proxy_port_range,
            nodes.len(),
            policy.max_conns_per_node,
            ClientPolicy::default().max_conns_per_node
        );

        let client = rows::connect(&policy, &[topo::seed().await]).await;
        client.enable_metrics(topo::metrics_policy());
        topo::wait_nodes(&client, &topo::node_names().await).await;
        topo::assert_all_partitions_owned("F-3-before", &client).await;

        let mut wp = WritePolicy::default();
        wp.base_policy.socket_timeout = SOCKET_TIMEOUT;
        wp.base_policy.total_timeout = TOTAL_TIMEOUT;
        let bound = u64::from(TOTAL_TIMEOUT + SOCKET_TIMEOUT);
        let start = topo::connection_stats(&client);
        let begin = Instant::now();

        let mut load = {
            let client = client.clone();
            let wp = wp.clone();
            Load::new("l2-f3", tasks, move |task, iteration| {
                let client = client.clone();
                let wp = wp.clone();
                async move {
                    let key = rows::key(SET, format!("f3-{}", (task * 31 + iteration) % KEY_COUNT));
                    client.put(&wp, &key, &[Bin::new("v", Value::from(iteration as i64))]).await
                }
            })
        };
        // Trace the client while the load runs, from a plain thread so a
        // saturated runtime cannot starve the sampler: the resident set, the
        // pool gauges and the connection counters, every 100 ms.
        let tracing = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let tracer = {
            let client = client.clone();
            let tracing = tracing.clone();
            std::thread::spawn(move || {
                let started = Instant::now();
                while tracing.load(std::sync::atomic::Ordering::Relaxed) {
                    let metrics = client.metrics();
                    let mut pools = Vec::new();
                    for node in client.nodes() {
                        let g = node.pool_gauges();
                        let c = metrics
                            .nodes
                            .get(&node.host().to_string())
                            .map(|s| s.counters.clone())
                            .unwrap_or_default();
                        pools.push(format!(
                            "{}{{total={},inPool={},recovering={},attempts={},ok={},failed={},poolEmpty={},closed={}}}",
                            node.name(),
                            g.total,
                            g.in_pool,
                            g.recovering,
                            c.connections_attempts,
                            c.connections_successful,
                            c.connections_failed,
                            c.connections_pool_empty,
                            c.connections_closed
                        ));
                    }
                    eprintln!(
                        "L2-F3-TRACE t={}ms rss={}MB {}",
                        started.elapsed().as_millis(),
                        resident_mb(),
                        pools.join(" ")
                    );
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            })
        };
        let trace_deadline = Instant::now() + std::time::Duration::from_millis(topo::WAIT_MS);
        while load.error_count() < tasks && Instant::now() < trace_deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        println!(
            "L2-F3-WAITED t={}ms outcomes={} errors={}",
            begin.elapsed().as_millis(),
            load.total(),
            load.error_count()
        );
        let finished = load.stop(bound).await;
        tracing.store(false, std::sync::atomic::Ordering::Relaxed);
        let _ = tracer.join();

        let errors = load.errors();
        let mut first_error_ms: Option<u64> = None;
        let mut max_elapsed = 0;
        let mut timeouts = Vec::new();
        for outcome in &errors {
            let at = outcome.start.saturating_duration_since(begin).as_millis() as u64 + outcome.elapsed_ms;
            first_error_ms = Some(first_error_ms.map_or(at, |f| f.min(at)));
            max_elapsed = max_elapsed.max(outcome.elapsed_ms);
            if outcome.is_socket_timeout() {
                timeouts.push(outcome.clone());
            }
        }

        let end = topo::connection_stats(&client);
        let mut error_delta = 0;
        let mut pooled = 0;
        for (name, s) in &end {
            error_delta += s.errors.saturating_sub(start.get(name).map_or(0, |s| s.errors));
            pooled = pooled.max(s.in_use + s.in_pool);
        }
        println!(
            "L2-F3-LOAD commands={} successes={} errors={:?} firstErrorMs={first_error_ms:?} maxErrorElapsedMs={max_elapsed} boundMs={bound} tasksReturned={finished} clientErrorDelta={error_delta} maxConnsPerNodeAfterLoad={pooled}",
            load.outcomes().len(),
            load.successes(),
            kinds(&errors)
        );
        println!("L2-F3-STATS {}", topo::format_stats(&end));
        if let Some(first) = errors.first() {
            println!(
                "L2-F3-ERROR {} {}",
                first.kind(),
                first.error.as_ref().map(topo::messages).unwrap_or_default()
            );
        }

        assert!(finished, "F-3: command tasks still blocked {bound}ms after stop");
        assert!(
            !errors.is_empty(),
            "F-3: {tasks} tasks never exceeded the proxy's {ports} source ports per node"
        );
        assert!(max_elapsed <= bound, "F-3: an error took {max_elapsed}ms, bound {bound}ms");
        assert!(error_delta > 0, "F-3: client counted no error: {}", topo::format_stats(&end));
        assert!(
            timeouts.is_empty(),
            "F-3: source port exhaustion surfaced as a client timeout: {:?}",
            kinds(&timeouts)
        );

        let rp = ReadPolicy::default();
        let dropped = Instant::now();
        let mut recovered = false;
        let deadline = dropped + std::time::Duration::from_millis(topo::WAIT_MS);
        while Instant::now() < deadline {
            if client.nodes().iter().all(|n| n.is_active()) && round_succeeds(&client, &wp, &rp, 4 * nodes.len()).await
            {
                recovered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let recovered_ms = dropped.elapsed().as_millis();
        let mut failures = Vec::new();
        for i in 0..RECOVERY_KEY_COUNT {
            let key = rows::key(SET, format!("f3-{i}"));
            let result = async {
                client.put(&wp, &key, &[Bin::new("v", Value::from(i as i64))]).await?;
                let record = client.get(&rp, &key, Bins::All).await?;
                assert_eq!(Some(&Value::from(i as i64)), record.bins.get("v"));
                Ok::<(), aerospike::Error>(())
            }
            .await;
            if let Err(e) = result {
                failures.push(format!("{i}: {} {}", e.kind().name(), e));
            }
        }
        println!(
            "L2-F3-RECOVERY recovered={recovered} ms={recovered_ms} keys={RECOVERY_KEY_COUNT} failed={} nodes={:?}",
            failures.len(),
            topo::client_node_names(&client)
        );
        assert!(recovered, "F-3: no successful round of commands after the load dropped");
        assert!(failures.is_empty(), "F-3: commands failed after the load dropped: {failures:?}");
        let _ = Arc::new(());
        client.close().await.expect("close");
    });
}
