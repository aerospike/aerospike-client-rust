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

//! Testing strategy row F-4 against the default topology (Java
//! `TestL2FaultInjection`): a Toxiproxy timeout toxic on one node's listener
//! holds data with no reply and no close, as a conntrack mismatch does. No
//! client marks down a node the cluster still lists, so F-4 asserts bounded
//! timeouts, no blocked tasks, recovery, and the error-rate circuit breaker.
//! Every toxic a case adds is removed after the row.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aerospike::{Bin, Client, ClientPolicy, Key, Value, WritePolicy};

use super::load::{kinds, Load, Outcome};
use super::rows::{self, writes};
use super::topology::{self as topo, L2Node, Restore};
use super::{capture, run};

const SET: &str = "l2f";
const TOXIC: &str = "f4-hold";
const TASKS: usize = 4;
const KEY_COUNT: usize = 20;
const SOCKET_TIMEOUT: u32 = 500;
const TOTAL_TIMEOUT: u32 = 1500;
const MAX_RETRIES: usize = 2;
const BREAKER_MAX_ERROR_RATE: usize = 2;
const BREAKER_WINDOW: usize = 4;

/// The victim under load with its traffic held.
struct Drop {
    client: Client,
    victim: L2Node,
    wp: WritePolicy,
    keys: Arc<Vec<Key>>,
    load: Load,
    injected: Instant,
    timeout_bound: u64,
}

/// Loads the victim node with writes and drops its traffic silently.
async fn inject(policy: ClientPolicy) -> Drop {
    let nodes = topo::nodes().await;
    let victim = nodes[nodes.len() - 1].clone();
    let mut policy = policy;
    policy.address_translator = Some(rows::port_map(topo::port_map_by_endpoint(&nodes)));

    let client = rows::connect(&policy, &[topo::seed().await]).await;
    // Operational metrics count the breaker's rejections.
    client.enable_metrics(topo::metrics_policy());
    topo::wait_nodes(&client, &topo::node_names().await).await;
    let masters = topo::assert_all_partitions_owned("F-4-before", &client).await;
    let node = client.get_node(&victim.name).expect("victim node");
    let mut keys = Vec::new();
    let mut i = 0;
    while keys.len() < KEY_COUNT {
        let key = rows::key(SET, format!("f4-{i}"));
        if masters[key.partition_id()].as_ref().is_some_and(|m| Arc::ptr_eq(m, &node)) {
            keys.push(key);
        }
        i += 1;
    }
    let keys = Arc::new(keys);

    let mut wp = WritePolicy::default();
    wp.base_policy.socket_timeout = SOCKET_TIMEOUT;
    wp.base_policy.total_timeout = TOTAL_TIMEOUT;
    wp.base_policy.max_retries = MAX_RETRIES;
    let timeout_bound = u64::from(TOTAL_TIMEOUT + SOCKET_TIMEOUT);
    let load = {
        let client = client.clone();
        let wp = wp.clone();
        let keys = keys.clone();
        Load::new("l2-f4", TASKS, move |task, iteration| {
            let client = client.clone();
            let wp = wp.clone();
            let keys = keys.clone();
            async move {
                client
                    .put(&wp, &keys[(task + iteration * TASKS) % keys.len()], &[Bin::new("v", Value::from(iteration as i64))])
                    .await
            }
        })
    };
    assert!(
        topo::wait_for(|| load.successes() >= TASKS).await,
        "No command succeeded before the drop"
    );
    let injected = Instant::now();
    topo::add_toxic(&victim.proxy, TOXIC, "timeout", "upstream", 1.0, "{\"timeout\":0}").await;
    topo::restore_later(Restore::RemoveToxic(victim.proxy.clone(), TOXIC.to_string()));
    Drop {
        client,
        victim,
        wp,
        keys,
        load,
        injected,
        timeout_bound,
    }
}

/// Removes the toxic and asserts the node serves again and every partition
/// is owned.
async fn recover(mut drop: Drop) {
    drop.load.stop(drop.timeout_bound).await;
    topo::remove_toxic(&drop.victim.proxy, TOXIC).await;
    topo::restored(|r| matches!(r, Restore::RemoveToxic(_, _)));
    let removed = Instant::now();
    let key = drop.keys[0].clone();
    let client = &drop.client;
    let victim = &drop.victim;
    let wp = &drop.wp;
    let deadline = removed + Duration::from_millis(topo::WAIT_MS);
    let mut back = false;
    while Instant::now() < deadline {
        let listed = topo::client_node_names(client).contains(&victim.name);
        let active = client.get_node(&victim.name).is_ok_and(|n| n.is_active());
        if listed && active && topo::owners_of(client).contains_key(&victim.name) && writes(client, wp, &key).await {
            back = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!(
        "L2-F4-RECOVERED back={back} ms={} nodes={:?} owners={:?}",
        removed.elapsed().as_millis(),
        topo::client_node_names(client),
        topo::owners_of(client)
    );
    assert!(back, "F-4: {} did not come back after the toxic was removed", victim.name);
    topo::assert_all_partitions_owned("F-4-after", client).await;
    drop.client.close().await.expect("close");
}

fn count_timeouts(outcomes: &[Outcome]) -> usize {
    outcomes.iter().filter(|o| o.is_timeout()).count()
}

/// F-4: commands to the node fail with a timeout within a bound from
/// socket_timeout and total_timeout, every error is a timeout, no command
/// task stays blocked past the bound, and the node serves again once the
/// toxic is removed. Whether the client marks the node down is recorded, not
/// asserted.
#[test]
fn f4_silent_drop_times_out_without_blocking() {
    run("F-4", || async {
        capture::clear();
        let mut drop = inject(topo::policy()).await;
        assert!(
            topo::wait_for(|| count_timeouts(&drop.load.errors()) >= TASKS).await,
            "F-4: fewer than {TASKS} timeouts after the drop"
        );
        let finished = drop.load.stop(drop.timeout_bound).await;
        let errors = drop.load.errors();
        let node = drop.client.get_node(&drop.victim.name).ok();
        let mut first_timeout_ms: Option<u64> = None;
        let mut max_elapsed = 0;
        for outcome in &errors {
            max_elapsed = max_elapsed.max(outcome.elapsed_ms);
            assert!(
                outcome.is_timeout(),
                "F-4: error is not a timeout: {} {}",
                outcome.kind(),
                outcome.error.as_ref().map(topo::messages).unwrap_or_default()
            );
            let at = outcome.start.saturating_duration_since(drop.injected).as_millis() as u64 + outcome.elapsed_ms;
            first_timeout_ms = Some(first_timeout_ms.map_or(at, |f| f.min(at)));
        }
        let refresh_failures = capture::messages_containing(&[&drop.victim.name, "refresh failed"]).len();
        let first_timeout_ms = first_timeout_ms.unwrap_or_default();
        println!(
            "L2-F4-TIMEOUT victim={} listener={} firstTimeoutMs={first_timeout_ms} maxErrorElapsedMs={max_elapsed} timeoutBoundMs={} errors={:?} successes={} tasksReturned={finished}",
            drop.victim.name,
            drop.victim.port,
            drop.timeout_bound,
            kinds(&errors),
            drop.load.successes()
        );
        println!(
            "L2-F4-NODE active={} nodes={:?} owners={:?} refreshFailedWarnings={refresh_failures}",
            node.as_ref().is_some_and(|n| n.is_active()),
            topo::client_node_names(&drop.client),
            topo::owners_of(&drop.client)
        );
        assert!(finished, "F-4: command tasks still blocked {}ms after stop", drop.timeout_bound);
        assert!(
            first_timeout_ms <= drop.timeout_bound,
            "F-4: first timeout {first_timeout_ms}ms after the drop, bound {}ms",
            drop.timeout_bound
        );
        assert!(
            max_elapsed <= drop.timeout_bound,
            "F-4: an error took {max_elapsed}ms, bound {}ms",
            drop.timeout_bound
        );
        recover(drop).await;
    });
}

/// The breaker rejections the client recorded against `victim`, from the
/// operational metrics (aggregated once per tend).
fn breaker_hits(client: &Client, victim: &L2Node) -> u64 {
    let Ok(node) = client.get_node(&victim.name) else {
        return 0;
    };
    client
        .metrics()
        .nodes
        .get(&node.host().to_string())
        .map_or(0, |s| s.counters.circuit_breaker_hits)
}

/// F-4 with a low max_error_rate: the timeouts trip the node's error-rate
/// circuit breaker within a bound from error_rate_window and tend_interval.
/// A command started after the trip no longer waits out socket_timeout: the
/// breaker refuses the node at once, and the retry is served by the next
/// replica (a breaker rejection is not a client timeout, so the replica
/// sequence advances), so the caller sees a fast success. A write that may
/// not retry sees the rejection itself, as `MaxErrorRate`.
#[test]
fn f4_silent_drop_trips_circuit_breaker() {
    run("F-4 breaker", || async {
        let mut policy = topo::policy();
        policy.max_error_rate = BREAKER_MAX_ERROR_RATE;
        policy.error_rate_window = BREAKER_WINDOW;
        let tend_interval = u64::from(policy.tend_interval);
        let mut drop = inject(policy).await;
        let trip_bound = (BREAKER_WINDOW as u64 + 1) * tend_interval + drop.timeout_bound;
        let socket_timeout = u64::from(drop.wp.base_policy.socket_timeout);

        // The trip: the first breaker rejection the metrics show, or the
        // first MaxErrorRate error a task saw.
        let mut tripped_at: Option<Instant> = None;
        let deadline = Instant::now() + Duration::from_millis(topo::WAIT_MS);
        while Instant::now() < deadline {
            if breaker_hits(&drop.client, &drop.victim) > 0
                || drop.load.errors().iter().any(Outcome::is_max_error_rate)
            {
                tripped_at = Some(Instant::now());
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let tripped_ms = tripped_at.map(|at| at.saturating_duration_since(drop.injected).as_millis() as u64);

        // A write that may not retry meets the breaker itself. The window
        // resets every error_rate_window tends and the load re-trips it, so
        // a few tries may be needed to land inside a tripped window.
        let mut no_retry = drop.wp.clone();
        no_retry.base_policy.max_retries = 0;
        let mut refused: Option<(u64, String)> = None;
        if tripped_at.is_some() {
            for _ in 0..20 {
                let begin = Instant::now();
                match drop.client.put(&no_retry, &drop.keys[0], &[Bin::new("v", Value::from(-1_i64))]).await {
                    // With no retry left the rejection comes back wrapped in
                    // MaxRetriesExceeded; the cause chain carries MaxErrorRate.
                    Err(e) if e.matches_client(&[aerospike::ClientResultCode::MaxErrorRate]) => {
                        refused = Some((begin.elapsed().as_millis() as u64, e.to_string()));
                        break;
                    }
                    _ => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        }

        // Let the load run on past the trip, then measure what started after it.
        tokio::time::sleep(Duration::from_millis(2 * socket_timeout)).await;
        let finished = drop.load.stop(drop.timeout_bound).await;
        let outcomes = drop.load.outcomes();
        let errors = drop.load.errors();
        let margin = Duration::from_millis(50);
        let after_trip: Vec<&Outcome> = outcomes
            .iter()
            .filter(|o| tripped_at.is_some_and(|at| o.start >= at + margin))
            .collect();
        let max_after_trip_ms = after_trip.iter().map(|o| o.elapsed_ms).max().unwrap_or(0);
        // The window resets every error_rate_window tends, so until the load
        // re-trips the breaker a few commands pay one socket_timeout again.
        let fast_after_trip = after_trip.iter().filter(|o| o.elapsed_ms < socket_timeout).count();
        let after_kinds = kinds(&after_trip.iter().map(|o| (*o).clone()).collect::<Vec<_>>());
        println!(
            "L2-F4-BREAKER victim={} maxErrorRate={BREAKER_MAX_ERROR_RATE} errorRateWindow={BREAKER_WINDOW} tendInterval={tend_interval} trippedMs={tripped_ms:?} tripBoundMs={trip_bound} breakerHits={} commandsAfterTrip={} afterTrip={after_kinds:?} fastAfterTrip={fast_after_trip} maxAfterTripElapsedMs={max_after_trip_ms} socketTimeout={socket_timeout} noRetryRefused={refused:?} errors={:?} successes={} tasksReturned={finished}",
            drop.victim.name,
            breaker_hits(&drop.client, &drop.victim),
            after_trip.len(),
            kinds(&errors),
            drop.load.successes()
        );
        assert!(tripped_at.is_some(), "F-4: circuit breaker never tripped: {:?}", kinds(&errors));
        assert!(finished, "F-4: command tasks still blocked {}ms after stop", drop.timeout_bound);
        let tripped_ms = tripped_ms.unwrap_or(u64::MAX);
        assert!(
            tripped_ms <= trip_bound,
            "F-4: breaker tripped {tripped_ms}ms after the drop, bound {trip_bound}ms"
        );
        assert!(!after_trip.is_empty(), "F-4: no command started after the breaker tripped");
        assert!(
            fast_after_trip * 10 >= after_trip.len() * 9,
            "F-4: only {fast_after_trip} of {} commands started after the trip finished under socket_timeout {socket_timeout}ms (max {max_after_trip_ms}ms)",
            after_trip.len()
        );
        let (refused_ms, message) = refused.expect("F-4: no write with max_retries 0 was refused with MaxErrorRate");
        assert!(
            refused_ms < socket_timeout,
            "F-4: the refused write took {refused_ms}ms, socket_timeout {socket_timeout}ms: {message}"
        );
        recover(drop).await;
    });
}
