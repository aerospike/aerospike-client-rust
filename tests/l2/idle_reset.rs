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

//! Testing strategy row F-1 against the default topology, in its own profile
//! (Java `TestL2IdleReset`). Every toxic the case adds is removed after the
//! row.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use aerospike::{Bin, Bins, ClientPolicy, ReadPolicy, Value, WritePolicy};

use super::load::{kinds, Load};
use super::rows;
use super::topology::{self as topo, L2Node, Restore};
use super::{run, skip_unless};

const BLOCKER: &str = "F-1 needs DISCOVERY_IDLE_RESET (run_l2.sh L2_PROFILE=idle-reset)";
const SET: &str = "l2f";
const WARM_TASKS: usize = 4;
const WARM_ITERATIONS: usize = 50;
const KEY_COUNT: usize = 100;
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Opens a connection through a listener and completes one info request on
/// it, so Toxiproxy has linked it to the node.
async fn probe(node: &L2Node) -> TcpStream {
    let mut stream = tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect((topo::t().host.as_str(), node.port)))
        .await
        .expect("probe connect in time")
        .expect("probe connect");
    let command = b"node\n";
    let mut buffer = Vec::with_capacity(8 + command.len());
    buffer.push(2);
    buffer.push(1);
    buffer.extend_from_slice(&(command.len() as u64).to_be_bytes()[2..8]);
    buffer.extend_from_slice(command);
    stream.write_all(&buffer).await.expect("probe request");
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await.expect("probe header");
    let mut len = [0u8; 8];
    len[2..].copy_from_slice(&header[2..]);
    let mut reply = vec![0u8; u64::from_be_bytes(len) as usize];
    stream.read_exact(&mut reply).await.expect("probe reply");
    stream
}

/// Reads the next byte of a probe connection: RST when the peer reset it,
/// FIN when it closed it.
async fn probe_result(stream: &mut TcpStream) -> String {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(PROBE_TIMEOUT, stream.read(&mut byte)).await {
        Ok(Ok(0)) => "FIN".to_string(),
        Ok(Ok(_)) => format!("DATA {}", byte[0]),
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => format!("RST {e}"),
        Ok(Err(e)) => format!("ERROR {e}"),
        Err(_) => "TIMEOUT".to_string(),
    }
}

/// F-1: idle flows torn down with RST, as an NLB tears down an idle flow.
/// Every listener's connections are made to close with RST rather than FIN,
/// and the listeners are then cycled while the client's connections sit idle
/// in the pool. With the default policy the next commands succeed: the pool
/// drops the dead sockets before use, or the policy's retry absorbs the
/// error, and no command fails to the caller. A raw probe connection on
/// every listener and the client's connection and error counters prove the
/// resets happened.
#[test]
fn f1_idle_reset_absorbed_by_pool() {
    run("F-1", || async {
        if !skip_unless(!topo::t().idle_reset.is_empty(), BLOCKER) {
            return;
        }
        let nodes = topo::nodes().await;
        let mut policy = topo::policy();
        policy.address_translator = Some(rows::port_map(topo::port_map_by_endpoint(&nodes)));
        assert_eq!(ClientPolicy::default().idle_timeout, policy.idle_timeout);

        for node in &nodes {
            topo::add_toxic(&node.proxy, "f1-rst", "reset_peer", "upstream", 0.0, "{\"timeout\":0}").await;
            topo::restore_later(Restore::RemoveToxic(node.proxy.clone(), "f1-rst".to_string()));
        }

        let client = rows::connect(&policy, &[topo::seed().await]).await;
        client.enable_metrics(topo::metrics_policy());
        topo::wait_nodes(&client, &topo::node_names().await).await;
        topo::assert_all_partitions_owned("F-1", &client).await;

        let wp = WritePolicy::default();
        let rp = ReadPolicy::default();
        let mut warm = {
            let client = client.clone();
            let wp = wp.clone();
            let rp = rp.clone();
            Load::new("l2-f1-warm", WARM_TASKS, move |task, iteration| {
                let client = client.clone();
                let wp = wp.clone();
                let rp = rp.clone();
                async move {
                    let key = rows::key(SET, format!("f1-{}", (task * WARM_ITERATIONS + iteration) % KEY_COUNT));
                    client.put(&wp, &key, &[Bin::new("v", Value::from(iteration as i64))]).await?;
                    client.get(&rp, &key, Bins::All).await?;
                    Ok(())
                }
            })
        };
        topo::wait_for(|| warm.outcomes().len() >= WARM_TASKS * WARM_ITERATIONS).await;
        assert!(warm.stop(topo::WAIT_MS).await, "Warm-up tasks blocked");
        assert!(
            warm.errors().is_empty(),
            "Warm-up errors: {:?}",
            kinds(&warm.errors())
        );

        let mut probes = Vec::new();
        for node in &nodes {
            probes.push(probe(node).await);
        }
        let idle = topo::connection_stats(&client);
        let pooled: u64 = idle.values().map(|s| s.in_pool).sum();
        println!(
            "L2-F1-IDLE idleTimeout={} pooled={pooled} {}",
            policy.idle_timeout,
            topo::format_stats(&idle)
        );
        assert!(pooled >= nodes.len() as u64, "No idle pooled connection to reset");

        for node in &nodes {
            topo::set_listener(&node.proxy, false).await;
            topo::set_listener(&node.proxy, true).await;
        }
        for (i, probe) in probes.iter_mut().enumerate() {
            let result = probe_result(probe).await;
            println!("L2-F1-PROBE listener={} result={result}", nodes[i].port);
            assert!(
                result.starts_with("RST"),
                "F-1: listener {} did not reset an idle flow with RST: {result}",
                nodes[i].port
            );
        }
        drop(probes);

        let start = topo::connection_stats(&client);
        let mut failures = Vec::new();
        let mut commands = 0;
        for i in 0..KEY_COUNT {
            let key = rows::key(SET, format!("f1-{i}"));
            commands += 1;
            if let Err(e) = client.put(&wp, &key, &[Bin::new("v", Value::from(i as i64))]).await {
                failures.push(format!("put {i}: {} {}", e.kind().name(), topo::messages(&e)));
            }
            commands += 1;
            if let Err(e) = client.get(&rp, &key, Bins::All).await {
                failures.push(format!("get {i}: {} {}", e.kind().name(), topo::messages(&e)));
            }
        }

        let end = topo::connection_stats(&client);
        let mut closed = 0;
        let mut errors = 0;
        for (name, s) in &end {
            let before = start.get(name).copied().unwrap_or_default();
            closed += s.closed.saturating_sub(before.closed);
            errors += s.errors.saturating_sub(before.errors);
        }
        println!(
            "L2-F1-RESULT commands={commands} failedToCaller={} pooledReset={pooled} closedDelta={closed} errorDelta={errors} writeMaxRetries={} readMaxRetries={}",
            failures.len(),
            wp.base_policy.max_retries,
            rp.base_policy.max_retries
        );
        println!("L2-F1-STATS {}", topo::format_stats(&end));
        for failure in &failures {
            println!("L2-F1-FAILED {failure}");
        }
        assert!(
            closed >= pooled,
            "F-1: {pooled} reset pooled connections but only {closed} closed by the client"
        );
        assert!(
            failures.is_empty(),
            "F-1: commands failed to the caller after idle RST: {failures:?}"
        );
        client.close().await.expect("close");
    });
}
