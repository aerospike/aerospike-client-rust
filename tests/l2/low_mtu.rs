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

//! Testing strategy row F-2 against the `low-mtu` profile (Java
//! `TestL2LowMtu`), standing in for PrivateLink's MTU of 8500 with no path
//! MTU discovery. The client network is created with a small MTU and the
//! client writes and reads records well above it. In its own profile so that
//! a failing F-2 does not fail the other rows.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng};

use aerospike::{Bin, Bins, Client, ErrorKind, Key, Node, ReadPolicy, Value, WritePolicy};

use super::rows;
use super::topology::{self as topo};
use super::{run, skip_unless};

const BLOCKER: &str = "F-2 needs DISCOVERY_CLIENT_NETWORK_MTU, a client network created with a small \
                       MTU (run_l2.sh L2_PROFILE=low-mtu)";
const SET: &str = "l2mtu";
const RECORD_SIZES: [usize; 2] = [64 * 1024, 1024 * 1024];
const SOCKET_TIMEOUT: u32 = 5000;
const TOTAL_TIMEOUT: u32 = 10000;

fn key_on(masters: &[Option<Arc<Node>>], node: &Arc<Node>) -> Key {
    for i in 0.. {
        let key = rows::key(SET, format!("f2-{i}"));
        if masters[key.partition_id()].as_ref().is_some_and(|m| Arc::ptr_eq(m, node)) {
            return key;
        }
    }
    unreachable!()
}

/// Runs a command with a bound and returns `ok <ms>`, or `error <ms> <message>`
/// for a diagnosable error. A hang past the bound or a client timeout fails
/// the row.
async fn bounded<F>(bound: u64, command: F) -> String
where
    F: std::future::Future<Output = aerospike::Result<()>>,
{
    let begin = Instant::now();
    match tokio::time::timeout(Duration::from_millis(bound), command).await {
        Err(_) => panic!("F-2: command still running after {bound}ms"),
        Ok(Ok(())) => format!("ok {}ms", begin.elapsed().as_millis()),
        Ok(Err(e)) => {
            let ms = begin.elapsed().as_millis();
            let message = topo::messages(&e);
            println!("L2-F2-ERROR {ms}ms {} {message}", e.kind().name());
            assert!(
                !matches!(e.kind(), ErrorKind::Timeout),
                "F-2: client timeout instead of a diagnosable error after {ms}ms: {message}"
            );
            format!("error {ms}ms {message}")
        }
    }
}

/// F-2: with the client network's MTU well below the record size, a write and
/// a read of a 1 MiB value through every node's listener succeed, or fail
/// with a diagnosable error, within a bound from socket_timeout and
/// total_timeout. A hang or a client timeout fails the row.
#[test]
fn f2_large_record_over_low_mtu_never_hangs() {
    run("F-2", || async {
        if !skip_unless(!topo::t().client_network_mtu.is_empty(), BLOCKER) {
            return;
        }
        let mtu: u64 = std::fs::read_to_string("/sys/class/net/eth0/mtu")
            .expect("/sys/class/net/eth0/mtu")
            .trim()
            .parse()
            .expect("mtu");
        println!("L2-F2-MTU eth0={mtu} network={}", topo::t().client_network_mtu);
        assert_eq!(topo::t().client_network_mtu.parse::<u64>().expect("mtu"), mtu);

        let nodes = topo::nodes().await;
        let mut policy = topo::policy();
        policy.address_translator = Some(rows::port_map(topo::port_map_by_endpoint(&nodes)));

        let client: Client = rows::connect(&policy, &[topo::seed().await]).await;
        topo::wait_nodes(&client, &topo::node_names().await).await;
        let masters = topo::assert_all_partitions_owned("F-2", &client).await;
        let mut wp = WritePolicy::default();
        wp.base_policy.socket_timeout = SOCKET_TIMEOUT;
        wp.base_policy.total_timeout = TOTAL_TIMEOUT;
        let mut rp = ReadPolicy::default();
        rp.base_policy.socket_timeout = SOCKET_TIMEOUT;
        rp.base_policy.total_timeout = TOTAL_TIMEOUT;
        let bound = u64::from(TOTAL_TIMEOUT + SOCKET_TIMEOUT);
        let mut random = rand::rngs::StdRng::seed_from_u64(mtu);

        for node in client.nodes() {
            let key = key_on(&masters, &node);
            for size in RECORD_SIZES {
                let mut value = vec![0u8; size];
                random.fill_bytes(&mut value);
                let write = bounded(bound, client.put(&wp, &key, &[Bin::new("v", Value::from(value.clone()))])).await;
                let read = bounded(bound, async {
                    let record = client.get(&rp, &key, Bins::All).await?;
                    assert_eq!(Some(&Value::from(value.clone())), record.bins.get("v"), "{key}");
                    Ok(())
                })
                .await;
                println!(
                    "L2-F2-RECORD node={} listener={} mtu={mtu} bytes={size} write={write} read={read} boundMs={bound}",
                    node.name(),
                    node.host().port
                );
            }
        }
        client.close().await.expect("close");
    });
}
