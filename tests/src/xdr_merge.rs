// Copyright 2015-2026 Aerospike, Inc.
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

//! The `xdr-merge` event across two clusters that ship to each other.
//!
//! Everything else about the event is covered on one node through the server's dry run
//! (`tests/src/events.rs`); this is the one thing the dry run cannot show: a real XDR write arriving at a
//! record that exists on the other side, the handler running inside that write, and both data centres
//! landing on the same record. It needs two Enterprise clusters, each an XDR source and destination of the
//! other with its own `src-id` and `ship-src-id true`, named by `AEROSPIKE_XDR_DC1` and `AEROSPIKE_XDR_DC2`
//! (`host:port` each); without them it skips. `_temp/asd-image-wasm/xdr/` has the two configurations.
//!
//! The module is the CRDT example (`aerospike-udf-sdk/wasm-rust/examples/crdt`): a merge that decides on the
//! data alone, so the two sides must converge whatever the order their writes arrive in.

use std::collections::HashMap;
use std::env;
use std::time::Duration;

use aerospike::*;

const CRDT: &[u8] = include_bytes!("../fixtures/wasm/crdt.wasm");
const COUNTER: &[u8] = include_bytes!("../fixtures/wasm/counter.wasm");
const COUNTER_JAVA: &[u8] = include_bytes!("../fixtures/wasm/counter_java.wasm");
const COUNTER_GO: &[u8] = include_bytes!("../fixtures/wasm/counter_go.wasm");
const COUNTER_CPP: &[u8] = include_bytes!("../fixtures/wasm/counter_cpp.wasm");
const PNCOUNTER: &[u8] = include_bytes!("../fixtures/wasm/pncounter.wasm");
const PNCOUNTER_JAVA: &[u8] = include_bytes!("../fixtures/wasm/pncounter_java.wasm");
const PNCOUNTER_GO: &[u8] = include_bytes!("../fixtures/wasm/pncounter_go.wasm");
const PNCOUNTER_CPP: &[u8] = include_bytes!("../fixtures/wasm/pncounter_cpp.wasm");
const TWOPSET: &[u8] = include_bytes!("../fixtures/wasm/twopset.wasm");
const TWOPSET_JAVA: &[u8] = include_bytes!("../fixtures/wasm/twopset_java.wasm");
const TWOPSET_GO: &[u8] = include_bytes!("../fixtures/wasm/twopset_go.wasm");
const TWOPSET_CPP: &[u8] = include_bytes!("../fixtures/wasm/twopset_cpp.wasm");
const LWWSET: &[u8] = include_bytes!("../fixtures/wasm/lwwset.wasm");
const LWWSET_JAVA: &[u8] = include_bytes!("../fixtures/wasm/lwwset_java.wasm");
const LWWSET_GO: &[u8] = include_bytes!("../fixtures/wasm/lwwset_go.wasm");
const LWWSET_CPP: &[u8] = include_bytes!("../fixtures/wasm/lwwset_cpp.wasm");
const REGISTERS: &[u8] = include_bytes!("../fixtures/wasm/registers.wasm");
const REGISTERS_JAVA: &[u8] = include_bytes!("../fixtures/wasm/registers_java.wasm");
const REGISTERS_GO: &[u8] = include_bytes!("../fixtures/wasm/registers_go.wasm");
const REGISTERS_CPP: &[u8] = include_bytes!("../fixtures/wasm/registers_cpp.wasm");
const RESOLVER: &[u8] = include_bytes!("../fixtures/wasm/resolver.wasm");
const RESOLVER_JAVA: &[u8] = include_bytes!("../fixtures/wasm/resolver_java.wasm");
const RESOLVER_GO: &[u8] = include_bytes!("../fixtures/wasm/resolver_go.wasm");
const RESOLVER_C: &[u8] = include_bytes!("../fixtures/wasm/resolver_c.wasm");
const RESOLVER_CPP: &[u8] = include_bytes!("../fixtures/wasm/resolver_cpp.wasm");
const APITEST: &[u8] = include_bytes!("../fixtures/wasm/apitest.wasm");
const APITEST_JAVA: &[u8] = include_bytes!("../fixtures/wasm/apitest_java.wasm");
const APITEST_GO: &[u8] = include_bytes!("../fixtures/wasm/apitest_go.wasm");
const APITEST_C: &[u8] = include_bytes!("../fixtures/wasm/apitest_c.wasm");
const APITEST_CPP: &[u8] = include_bytes!("../fixtures/wasm/apitest_cpp.wasm");

const SHIP_LAG: Duration = Duration::from_secs(20);

/// The namespace both clusters keep with bin convergence on, so that every bin has its own last-update time.
const BIN_CONVERGENCE_NS: &str = "testbc";

fn dcs() -> Option<(String, String)> {
    Some((env::var("AEROSPIKE_XDR_DC1").ok()?, env::var("AEROSPIKE_XDR_DC2").ok()?))
}

async fn info(client: &Client, command: &str) -> String {
    let answers = client.info(&AdminPolicy::default(), &[command]).await.unwrap();

    answers
        .get(command)
        .cloned()
        .unwrap_or_else(|| panic!("no answer to {command:?}: {answers:?}"))
}

async fn register(client: &Client, package: &str) {
    register_module(client, CRDT, package).await;
}

async fn register_module(client: &Client, module: &[u8], package: &str) {
    client
        .register_udf(&AdminPolicy::default(), module, &format!("{package}.wasm"), UdfLang::Wasm)
        .await
        .unwrap()
        .wait_till_complete(None)
        .await
        .unwrap();
}

fn map_of(v: &Value) -> HashMap<Value, Value> {
    match v {
        Value::HashMap(m) => m.clone(),
        Value::SortedMap(m) => m.clone().into_iter().collect(),
        Value::OrderedMap(m) => m.clone().into_iter().collect(),
        other => panic!("not a map: {other:?}"),
    }
}

/// The record's devices as (id, version, state), in stored order; None when the record is not there.
async fn devices(client: &Client, key: &Key) -> Option<Vec<(String, i64, String)>> {
    let record = match client.get(&ReadPolicy::default(), key, Bins::All).await {
        Ok(r) => r,
        Err(e) if e.matches(&[ResultCode::KeyNotFoundError]) => return None,
        Err(e) => panic!("{e:?}"),
    };
    let Some(Value::List(list)) = record.bins.get("devices") else { return Some(Vec::new()) };

    Some(
        list.iter()
            .map(|e| {
                let m = map_of(e);
                let text = |k: &str| match m.get(&Value::from(k)) {
                    Some(Value::String(s)) => s.clone(),
                    None => String::new(),
                    other => panic!("{k}: {other:?}"),
                };
                let version = match m.get(&Value::from("version")) {
                    Some(Value::Int(n)) => *n,
                    other => panic!("version: {other:?}"),
                };

                (text("deviceId"), version, text("state"))
            })
            .collect(),
    )
}

/// A counter of the xdr-merge binding on this cluster's set, from `event-list`.
async fn counter(client: &Client, set_name: &str, name: &str) -> u64 {
    let rows = info(client, "event-list").await;
    let row = rows
        .split(';')
        .find(|r| r.contains(&format!(":set={set_name}:")))
        .unwrap_or_else(|| panic!("no binding on {set_name} in {rows}"));

    row.split(':')
        .find_map(|f| f.strip_prefix(&format!("{name}=")))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no {name} in {row}"))
}

/// How many records this cluster has shipped to `dc` successfully (a kept-local merge counts: the source
/// hears `lost-conflict`, which XDR treats as success).
async fn xdr_success(client: &Client, dc: &str) -> u64 {
    let stats = info(client, &format!("get-stats:context=xdr;dc={dc};namespace=test")).await;

    stats
        .split(';')
        .find_map(|f| f.strip_prefix("success="))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no success in {stats}"))
}

/// Poll until `check` is true on both clusters, or fail with what each side holds.
async fn converge<F>(c1: &Client, c2: &Client, key: &Key, what: &str, check: F)
where
    F: Fn(&Option<Vec<(String, i64, String)>>) -> bool,
{
    let deadline = std::time::Instant::now() + SHIP_LAG;

    loop {
        let d1 = devices(c1, key).await;
        let d2 = devices(c2, key).await;

        if check(&d1) && check(&d2) {
            return;
        }

        assert!(std::time::Instant::now() < deadline, "{what}: dc1 holds {d1:?}, dc2 holds {d2:?}");
        aerospike_rt::sleep(Duration::from_millis(250)).await;
    }
}

/// Two data centres write the same subscriber at the same time; each receives the other's write as an
/// incoming XDR write to an existing record, the bound CRDT merge runs inside it, and both end up holding
/// the same devices. Then a write that merges to an unchanged record is elided on the receiving side and
/// counted as kept.
#[aerospike_macro::test]
async fn xdr_merge_converges_two_clusters() {
    let Some((dc1, dc2)) = dcs() else {
        eprintln!("skipped xdr_merge_converges_two_clusters: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2");
        return;
    };

    let c1 = Client::new(&ClientPolicy::default(), &dc1).await.expect("dc1");
    let c2 = Client::new(&ClientPolicy::default(), &dc2).await.expect("dc2");
    let namespace = "test";
    let set_name = &format!("subs_{}", crate::common::rand_str(6));
    let package = &format!("crdt_{}", crate::common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, "1001");
    let changes = |pairs: &[(&str, &str)]| -> Value {
        Value::from(pairs.iter().map(|(k, v)| (Value::from(*k), Value::from(*v))).collect::<HashMap<_, _>>())
    };

    for c in [&c1, &c2] {
        register(c, package).await;
        assert_eq!(
            info(
                c,
                &format!(
                    "event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function=xdr_merge;on-failure=abandon"
                )
            )
            .await,
            "ok"
        );
    }

    // 1. A record born on dc1 reaches dc2 as a create, which bypasses the merge.
    c1.execute_udf(&write, &key, package, "update_device", &[Value::from("device-A"), changes(&[("state", "ACTIVE")])])
        .await
        .unwrap();
    converge(&c1, &c2, &key, "the record should reach dc2", |d| {
        d.as_ref().is_some_and(|d| d == &[("device-A".to_string(), 1, "ACTIVE".to_string())])
    })
    .await;

    let fired_before = (counter(&c1, set_name, "fired").await, counter(&c2, set_name, "fired").await);

    // 2. One side at a time first. dc2 adds B: dc1 receives (A@1, B@1) as an XDR write to its existing
    //    record and must merge, not replace.
    c2.execute_udf(&write, &key, package, "update_device", &[Value::from("device-B"), changes(&[("state", "ACTIVE")])])
        .await
        .unwrap();

    let step2 = vec![("device-A".to_string(), 1, "ACTIVE".to_string()), ("device-B".to_string(), 1, "ACTIVE".to_string())];

    converge(&c1, &c2, &key, "dc1 should merge dc2's added device", |d| d.as_ref() == Some(&step2)).await;

    // Then dc1 changes A: dc2 receives (A@2, B@1) and merges to the same.
    c1.execute_udf(&write, &key, package, "update_device", &[Value::from("device-A"), changes(&[("state", "SUSPENDED")])])
        .await
        .unwrap();

    let expected = vec![
        ("device-A".to_string(), 2, "SUSPENDED".to_string()),
        ("device-B".to_string(), 1, "ACTIVE".to_string()),
    ];

    converge(&c1, &c2, &key, "dc2 should merge dc1's changed device", |d| d.as_ref() == Some(&expected)).await;

    // 3. Both sides write at once: dc1 changes A again, dc2 adds C. Each receives the other's write as an
    //    XDR write to an existing record, merges, and both land on A@3 + B@1 + C@1.
    let args_a = [Value::from("device-A"), changes(&[("state", "ACTIVE")])];
    let args_c = [Value::from("device-C"), changes(&[("state", "ACTIVE")])];
    let (r1, r2) = futures::join!(
        c1.execute_udf(&write, &key, package, "update_device", &args_a),
        c2.execute_udf(&write, &key, package, "update_device", &args_c),
    );
    r1.unwrap();
    r2.unwrap();

    let expected = vec![
        ("device-A".to_string(), 3, "ACTIVE".to_string()),
        ("device-B".to_string(), 1, "ACTIVE".to_string()),
        ("device-C".to_string(), 1, "ACTIVE".to_string()),
    ];

    converge(&c1, &c2, &key, "both data centres should hold the merged devices", |d| d.as_ref() == Some(&expected)).await;

    // The merge ran on each side, once per incoming write, and each produced a new record.
    let fired_after = (counter(&c1, set_name, "fired").await, counter(&c2, set_name, "fired").await);

    assert!(fired_after.0 > fired_before.0, "dc1 merge should have fired: {fired_before:?} -> {fired_after:?}");
    assert!(fired_after.1 > fired_before.1, "dc2 merge should have fired: {fired_before:?} -> {fired_after:?}");
    // A merged record is either the incoming write applied, or the two combined; the collision was
    // combined on at least one side.
    let combined = (counter(&c1, set_name, "combined").await, counter(&c2, set_name, "combined").await);
    let took = (counter(&c1, set_name, "took-incoming").await, counter(&c2, set_name, "took-incoming").await);

    assert!(combined.0 + took.0 >= 1, "dc1 should have written a merged record");
    assert!(combined.1 + took.1 >= 1, "dc2 should have written a merged record");
    assert!(combined.0 + combined.1 >= 1, "the collision should have been combined");
    assert_eq!(counter(&c1, set_name, "fallback").await, 0);
    assert_eq!(counter(&c2, set_name, "fallback").await, 0);

    // 4. A write that changes nothing: dc1 rewrites the same devices (a purge at time zero, which moves no
    //    horizon and drops no marker, so every bin is stored again as it was). The record still ships. dc2
    //    merges it to a record equal to its own, writes nothing, and counts it as kept; the source counts
    //    the shipment as a success.
    let kept_before = counter(&c2, set_name, "keep-local").await;
    let fired_before = counter(&c2, set_name, "fired").await;
    let shipped_before = xdr_success(&c1, "dc2").await;

    c1.execute_udf(&write, &key, package, "purge_devices", &[Value::from(0)]).await.unwrap();

    let deadline = std::time::Instant::now() + SHIP_LAG;

    loop {
        let fired = counter(&c2, set_name, "fired").await;
        let kept = counter(&c2, set_name, "keep-local").await;

        if fired > fired_before && kept > kept_before {
            break;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "dc2 should merge the unchanged record and keep its own: fired {fired_before} -> {fired}, keep-local {kept_before} -> {kept}"
        );
        aerospike_rt::sleep(Duration::from_millis(250)).await;
    }

    assert_eq!(devices(&c2, &key).await.as_ref(), Some(&expected), "dc2's record is unchanged");
    assert_eq!(devices(&c1, &key).await.as_ref(), Some(&expected), "dc1's record is unchanged");
    assert!(xdr_success(&c1, "dc2").await > shipped_before, "dc1 should count the elided shipment as a success");

    for c in [&c1, &c2] {
        assert_eq!(info(c, &format!("event-remove:event=xdr-merge;namespace={namespace};set={set_name}")).await, "ok");
        let _ = c.delete(&write, &key).await;
        let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
    }
}

/// The counter's value on one cluster, or None while the record is not there.
async fn counter_value(client: &Client, key: &Key, package: &str) -> Option<i64> {
    match client.execute_udf(&WritePolicy::default(), key, package, "value", &[]).await {
        Ok(Some(Value::Int(n))) => Some(n),
        _ => None,
    }
}

/// The grow-only counter across the two clusters, in Rust, Java and Go: each site increments its own entry,
/// the other side merges by keeping the larger count per site, and both end up with the same counts and
/// the same value.
#[aerospike_macro::test]
async fn counter_converges_two_clusters() {
    let Some((dc1, dc2)) = dcs() else {
        eprintln!("skipped counter_converges_two_clusters: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2");
        return;
    };

    let c1 = Client::new(&ClientPolicy::default(), &dc1).await.expect("dc1");
    let c2 = Client::new(&ClientPolicy::default(), &dc2).await.expect("dc2");
    let namespace = "test";
    let write = WritePolicy::default();

    for (lang, module) in [("rust", COUNTER), ("java", COUNTER_JAVA), ("go", COUNTER_GO), ("cpp", COUNTER_CPP)] {
        let set_name = &format!("hits_{}", crate::common::rand_str(6));
        let package = &format!("counter_{lang}_{}", crate::common::rand_str(6));
        let key = as_key!(namespace, set_name, "page");

        for c in [&c1, &c2] {
            register_module(c, module, package).await;
            assert_eq!(
                info(c, &format!("event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function=merge_counter;on-failure=abandon")).await,
                "ok",
                "{lang}"
            );
        }

        // Born on dc1, reaches dc2 as a create.
        c1.execute_udf(&write, &key, package, "increment", &[Value::from("dc1"), Value::from(3)]).await.unwrap();

        let deadline = std::time::Instant::now() + SHIP_LAG;

        while counter_value(&c2, &key, package).await != Some(3) {
            assert!(std::time::Instant::now() < deadline, "{lang}: the counter should reach dc2");
            aerospike_rt::sleep(Duration::from_millis(250)).await;
        }

        // Both sites count at once; each merges the other's write by keeping the larger count per site.
        let a = [Value::from("dc1"), Value::from(2)];
        let b = [Value::from("dc2"), Value::from(5)];
        let (r1, r2) = futures::join!(
            c1.execute_udf(&write, &key, package, "increment", &a),
            c2.execute_udf(&write, &key, package, "increment", &b),
        );
        r1.unwrap();
        r2.unwrap();

        let deadline = std::time::Instant::now() + SHIP_LAG;

        loop {
            let (v1, v2) = (counter_value(&c1, &key, package).await, counter_value(&c2, &key, package).await);

            if v1 == Some(10) && v2 == Some(10) {
                break;
            }

            assert!(std::time::Instant::now() < deadline, "{lang}: both sides should reach 10: dc1 {v1:?}, dc2 {v2:?}");
            aerospike_rt::sleep(Duration::from_millis(250)).await;
        }

        for c in [&c1, &c2] {
            assert_eq!(info(c, &format!("event-remove:event=xdr-merge;namespace={namespace};set={set_name}")).await, "ok");
            let _ = c.delete(&write, &key).await;
            let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
        }
    }
}

/// The PN-Counter across the two clusters, in Rust, Java, Go and C++: increments and decrements are two
/// grow-only maps merged by the larger amount per site, so a decrement at one site and an increment at the
/// other, at once, land both sides on the same value, and the value goes below zero when more is taken
/// than was ever added.
#[aerospike_macro::test]
async fn pncounter_converges_two_clusters() {
    let Some((dc1, dc2)) = dcs() else {
        eprintln!("skipped pncounter_converges_two_clusters: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2");
        return;
    };

    let c1 = Client::new(&ClientPolicy::default(), &dc1).await.expect("dc1");
    let c2 = Client::new(&ClientPolicy::default(), &dc2).await.expect("dc2");
    let namespace = "test";
    let write = WritePolicy::default();

    for (lang, module) in [("rust", PNCOUNTER), ("java", PNCOUNTER_JAVA), ("go", PNCOUNTER_GO), ("cpp", PNCOUNTER_CPP)] {
        let set_name = &format!("stock_{}", crate::common::rand_str(6));
        let package = &format!("pncounter_{lang}_{}", crate::common::rand_str(6));
        let key = as_key!(namespace, set_name, "widget");

        for c in [&c1, &c2] {
            register_module(c, module, package).await;
            assert_eq!(
                info(c, &format!("event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function=merge_pncounter;on-failure=abandon")).await,
                "ok",
                "{lang}"
            );
        }

        // Born on dc1 with three in stock, reaches dc2 as a create.
        c1.execute_udf(&write, &key, package, "increment", &[Value::from("dc1"), Value::from(3)]).await.unwrap();

        let deadline = std::time::Instant::now() + SHIP_LAG;

        while counter_value(&c2, &key, package).await != Some(3) {
            assert!(std::time::Instant::now() < deadline, "{lang}: the counter should reach dc2");
            aerospike_rt::sleep(Duration::from_millis(250)).await;
        }

        // dc1 takes two while dc2 adds five; each merges the other's write map by map.
        let a = [Value::from("dc1"), Value::from(2)];
        let b = [Value::from("dc2"), Value::from(5)];
        let (r1, r2) = futures::join!(
            c1.execute_udf(&write, &key, package, "decrement", &a),
            c2.execute_udf(&write, &key, package, "increment", &b),
        );
        r1.unwrap();
        r2.unwrap();

        let deadline = std::time::Instant::now() + SHIP_LAG;

        loop {
            let (v1, v2) = (counter_value(&c1, &key, package).await, counter_value(&c2, &key, package).await);

            if v1 == Some(6) && v2 == Some(6) {
                break;
            }

            assert!(std::time::Instant::now() < deadline, "{lang}: both sides should reach 6: dc1 {v1:?}, dc2 {v2:?}");
            aerospike_rt::sleep(Duration::from_millis(250)).await;
        }

        // No floor: both sides take four at once from a stock of six, and both end at minus two.
        let a = [Value::from("dc1"), Value::from(4)];
        let b = [Value::from("dc2"), Value::from(4)];
        let (r1, r2) = futures::join!(
            c1.execute_udf(&write, &key, package, "decrement", &a),
            c2.execute_udf(&write, &key, package, "decrement", &b),
        );
        r1.unwrap();
        r2.unwrap();

        let deadline = std::time::Instant::now() + SHIP_LAG;

        loop {
            let (v1, v2) = (counter_value(&c1, &key, package).await, counter_value(&c2, &key, package).await);

            if v1 == Some(-2) && v2 == Some(-2) {
                break;
            }

            assert!(std::time::Instant::now() < deadline, "{lang}: both sides should reach -2: dc1 {v1:?}, dc2 {v2:?}");
            aerospike_rt::sleep(Duration::from_millis(250)).await;
        }

        for c in [&c1, &c2] {
            assert_eq!(info(c, &format!("event-remove:event=xdr-merge;namespace={namespace};set={set_name}")).await, "ok");
            let _ = c.delete(&write, &key).await;
            let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
        }
    }
}

/// The members a set module answers on one cluster, or None while the record is not there.
async fn set_members(client: &Client, key: &Key, package: &str) -> Option<Vec<String>> {
    match client.execute_udf(&WritePolicy::default(), key, package, "members", &[]).await {
        Ok(Some(Value::List(items))) => Some(
            items
                .iter()
                .map(|i| match i {
                    Value::String(s) => s.clone(),
                    other => panic!("{other:?}"),
                })
                .collect(),
        ),
        _ => None,
    }
}

/// Waits until both clusters answer `expected` for the set, or fails naming the sides.
async fn converge_on(c1: &Client, c2: &Client, key: &Key, package: &str, expected: &[&str], what: &str) {
    let expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
    let deadline = std::time::Instant::now() + SHIP_LAG;

    loop {
        let (m1, m2) = (set_members(c1, key, package).await, set_members(c2, key, package).await);

        if m1.as_ref() == Some(&expected) && m2.as_ref() == Some(&expected) {
            return;
        }

        assert!(std::time::Instant::now() < deadline, "{what}: both sides should reach {expected:?}: dc1 {m1:?}, dc2 {m2:?}");
        aerospike_rt::sleep(Duration::from_millis(250)).await;
    }
}

/// One set module on both clusters: registered, bound and then removed again.
async fn with_set_bound(c1: &Client, c2: &Client, module: &[u8], package: &str, set_name: &str, function: &str, lang: &str) {
    for c in [c1, c2] {
        register_module(c, module, package).await;
        assert_eq!(
            info(c, &format!("event-set:event=xdr-merge;namespace=test;set={set_name};module={package};function={function};on-failure=abandon")).await,
            "ok",
            "{lang}"
        );
    }
}

async fn unbind_set(c1: &Client, c2: &Client, key: &Key, package: &str, set_name: &str) {
    for c in [c1, c2] {
        assert_eq!(info(c, &format!("event-remove:event=xdr-merge;namespace=test;set={set_name}")).await, "ok");
        let _ = c.delete(&WritePolicy::default(), key).await;
        let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
    }
}

/// The 2P-Set across the two clusters, in Rust, Java, Go and C++: a concurrent add on one side and a
/// removal on the other both survive, since the two lists merge by union, and a tag removed once never
/// comes back, on either side.
#[aerospike_macro::test]
async fn twopset_converges_two_clusters() {
    let Some((dc1, dc2)) = dcs() else {
        eprintln!("skipped twopset_converges_two_clusters: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2");
        return;
    };

    let c1 = Client::new(&ClientPolicy::default(), &dc1).await.expect("dc1");
    let c2 = Client::new(&ClientPolicy::default(), &dc2).await.expect("dc2");
    let write = WritePolicy::default();

    for (lang, module) in [("rust", TWOPSET), ("java", TWOPSET_JAVA), ("go", TWOPSET_GO), ("cpp", TWOPSET_CPP)] {
        let set_name = &format!("tags_{}", crate::common::rand_str(6));
        let package = &format!("twopset_{lang}_{}", crate::common::rand_str(6));
        let key = as_key!("test", set_name, "post");

        with_set_bound(&c1, &c2, module, package, set_name, "merge_twopset", lang).await;

        // Born on dc1, reaches dc2 as a create.
        c1.execute_udf(&write, &key, package, "add", &[Value::from("news")]).await.unwrap();
        converge_on(&c1, &c2, &key, package, &["news"], &format!("{lang}: first add")).await;

        // dc1 adds `art` while dc2 removes `news`: the union keeps both changes.
        let a = [Value::from("art")];
        let b = [Value::from("news")];
        let (r1, r2) = futures::join!(c1.execute_udf(&write, &key, package, "add", &a), c2.execute_udf(&write, &key, package, "remove", &b));
        r1.unwrap();
        r2.unwrap();
        converge_on(&c1, &c2, &key, package, &["art"], &format!("{lang}: concurrent add and remove")).await;

        // The limit: `news` is gone for good, whichever side adds it back.
        c1.execute_udf(&write, &key, package, "add", &[Value::from("news")]).await.unwrap();
        c2.execute_udf(&write, &key, package, "add", &[Value::from("music")]).await.unwrap();
        converge_on(&c1, &c2, &key, package, &["art", "music"], &format!("{lang}: re-add after removal")).await;

        unbind_set(&c1, &c2, &key, package, set_name).await;
    }
}

/// The LWW-Element-Set across the two clusters, in Rust, Java, Go and C++: the same concurrent add and
/// removal converge, and then a later add on either side brings the removed tag back, which the 2P-Set
/// could not do.
#[aerospike_macro::test]
async fn lwwset_converges_two_clusters() {
    let Some((dc1, dc2)) = dcs() else {
        eprintln!("skipped lwwset_converges_two_clusters: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2");
        return;
    };

    let c1 = Client::new(&ClientPolicy::default(), &dc1).await.expect("dc1");
    let c2 = Client::new(&ClientPolicy::default(), &dc2).await.expect("dc2");
    let write = WritePolicy::default();

    for (lang, module) in [("rust", LWWSET), ("java", LWWSET_JAVA), ("go", LWWSET_GO), ("cpp", LWWSET_CPP)] {
        let set_name = &format!("tags_{}", crate::common::rand_str(6));
        let package = &format!("lwwset_{lang}_{}", crate::common::rand_str(6));
        let key = as_key!("test", set_name, "post");

        with_set_bound(&c1, &c2, module, package, set_name, "merge_lwwset", lang).await;

        c1.execute_udf(&write, &key, package, "add", &[Value::from("news")]).await.unwrap();
        converge_on(&c1, &c2, &key, package, &["news"], &format!("{lang}: first add")).await;

        // dc1 adds `art` while dc2 removes `news`: each map keeps the later stamp per tag.
        let a = [Value::from("art")];
        let b = [Value::from("news")];
        let (r1, r2) = futures::join!(c1.execute_udf(&write, &key, package, "add", &a), c2.execute_udf(&write, &key, package, "remove", &b));
        r1.unwrap();
        r2.unwrap();
        converge_on(&c1, &c2, &key, package, &["art"], &format!("{lang}: concurrent add and remove")).await;

        // The re-add: a later stamp on dc1 beats dc2's removal, on both sides.
        c1.execute_udf(&write, &key, package, "add", &[Value::from("news")]).await.unwrap();
        converge_on(&c1, &c2, &key, package, &["art", "news"], &format!("{lang}: re-add after removal")).await;

        // And the removal of the re-added tag, from the other side this time.
        c2.execute_udf(&write, &key, package, "remove", &[Value::from("news")]).await.unwrap();
        converge_on(&c1, &c2, &key, package, &["art"], &format!("{lang}: second removal")).await;

        unbind_set(&c1, &c2, &key, package, set_name).await;
    }
}

/// The bins of a record as plain pairs, sorted by name; None when the record is not there.
async fn bins_of(client: &Client, key: &Key) -> Option<Vec<(String, Value)>> {
    let record = match client.get(&ReadPolicy::default(), key, Bins::All).await {
        Ok(r) => r,
        Err(e) if e.matches(&[ResultCode::KeyNotFoundError]) => return None,
        Err(e) => panic!("{e:?}"),
    };
    let mut bins: Vec<(String, Value)> = record.bins.into_iter().collect();

    bins.sort_by(|a, b| a.0.cmp(&b.0));

    Some(bins)
}

/// Waits until both clusters hold exactly `expected`, or fails naming the sides.
async fn converge_bins(c1: &Client, c2: &Client, key: &Key, expected: &[(&str, Value)], what: &str) {
    let expected: Vec<(String, Value)> = expected.iter().map(|(n, v)| (n.to_string(), v.clone())).collect();
    let deadline = std::time::Instant::now() + SHIP_LAG;

    loop {
        let (b1, b2) = (bins_of(c1, key).await, bins_of(c2, key).await);

        if b1.as_ref() == Some(&expected) && b2.as_ref() == Some(&expected) {
            return;
        }

        assert!(std::time::Instant::now() < deadline, "{what}: both sides should reach {expected:?}: dc1 {b1:?}, dc2 {b2:?}");
        aerospike_rt::sleep(Duration::from_millis(250)).await;
    }
}

/// The record of registers across the two clusters, on the namespace that keeps a last-update time per
/// bin: writes to different bins both survive, the later write to one bin wins on both sides, a bin that
/// is not a register (`seen`, a maximum) keeps the larger number although the smaller was written later,
/// and a delete that is the later write stays a delete.
#[aerospike_macro::test]
async fn registers_converge_two_clusters() {
    let Some((dc1, dc2)) = dcs() else {
        eprintln!("skipped registers_converge_two_clusters: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2");
        return;
    };

    let c1 = Client::new(&ClientPolicy::default(), &dc1).await.expect("dc1");
    let c2 = Client::new(&ClientPolicy::default(), &dc2).await.expect("dc2");
    let write = WritePolicy::default();

    if !info(&c1, "namespaces").await.split(';').any(|n| n == BIN_CONVERGENCE_NS) {
        eprintln!("skipped registers_converge_two_clusters: the clusters have no {BIN_CONVERGENCE_NS} namespace");
        return;
    }

    // A port that is not checked in yet can be tried alone: point this at its compiled module.
    let trial = env::var("AEROSPIKE_REGISTERS_MODULE").ok().map(|path| std::fs::read(&path).expect(&path));
    let modules: Vec<(&str, &[u8])> = match &trial {
        Some(module) => vec![("trial", module.as_slice())],
        None => vec![("rust", REGISTERS), ("java", REGISTERS_JAVA), ("go", REGISTERS_GO), ("cpp", REGISTERS_CPP)],
    };

    for (lang, module) in modules {
        let set_name = &format!("settings_{}", crate::common::rand_str(6));
        let package = &format!("registers_{lang}_{}", crate::common::rand_str(6));
        let key = as_key!(BIN_CONVERGENCE_NS, set_name, "ada");
        let bind = format!(
            "event-set:event=xdr-merge;namespace={BIN_CONVERGENCE_NS};set={set_name};module={package};function=merge_settings;on-failure=abandon"
        );

        for c in [&c1, &c2] {
            register_module(c, module, package).await;
            assert_eq!(info(c, &bind).await, "ok", "{lang}");
        }

        let put = |c: &Client, bins: Vec<Bin>| {
            let (c, key, write) = (c.clone(), key.clone(), write.clone());

            async move { c.put(&write, &key, &bins).await.unwrap() }
        };

        let saw = |c: &Client, n: i64| {
            let (c, key, write, package) = (c.clone(), key.clone(), write.clone(), package.clone());

            async move { c.execute_udf(&write, &key, &package, "saw", &[Value::from(n)]).await.unwrap() }
        };

        put(&c1, vec![as_bin!("theme", "dark"), as_bin!("lang", "en")]).await;
        saw(&c1, 3).await;
        converge_bins(
            &c1,
            &c2,
            &key,
            &[("lang", Value::from("en")), ("seen", Value::from(3)), ("theme", Value::from("dark"))],
            &format!("{lang}: the first write"),
        )
        .await;

        // Different bins at the same time: each side's write survives the other's.
        futures::join!(put(&c1, vec![as_bin!("theme", "light")]), put(&c2, vec![as_bin!("lang", "fr")]));
        converge_bins(
            &c1,
            &c2,
            &key,
            &[("lang", Value::from("fr")), ("seen", Value::from(3)), ("theme", Value::from("light"))],
            &format!("{lang}: different bins at once"),
        )
        .await;

        // The same bin on both sides, dc2's write a little later: the later stamp wins on both.
        futures::join!(put(&c1, vec![as_bin!("theme", "solar")]), async {
            aerospike_rt::sleep(Duration::from_millis(5)).await;
            put(&c2, vec![as_bin!("theme", "mono")]).await
        });
        converge_bins(
            &c1,
            &c2,
            &key,
            &[("lang", Value::from("fr")), ("seen", Value::from(3)), ("theme", Value::from("mono"))],
            &format!("{lang}: one bin, the later write"),
        )
        .await;

        // `seen` is not a register: the larger number stays, though the smaller one was written later.
        futures::join!(saw(&c1, 9), async {
            aerospike_rt::sleep(Duration::from_millis(5)).await;
            saw(&c2, 5).await
        });
        converge_bins(
            &c1,
            &c2,
            &key,
            &[("lang", Value::from("fr")), ("seen", Value::from(9)), ("theme", Value::from("mono"))],
            &format!("{lang}: seen keeps the maximum"),
        )
        .await;

        // A delete that is the later write stays a delete, and the other side's write to another bin
        // does not bring the bin back.
        futures::join!(put(&c1, vec![as_bin!("theme", "dark")]), async {
            aerospike_rt::sleep(Duration::from_millis(5)).await;
            put(&c2, vec![as_bin!("lang", Value::Nil)]).await
        });
        converge_bins(
            &c1,
            &c2,
            &key,
            &[("seen", Value::from(9)), ("theme", Value::from("dark"))],
            &format!("{lang}: a later delete"),
        )
        .await;

        // And a register written after its delete is back.
        put(&c1, vec![as_bin!("lang", "de")]).await;
        converge_bins(
            &c1,
            &c2,
            &key,
            &[("lang", Value::from("de")), ("seen", Value::from(9)), ("theme", Value::from("dark"))],
            &format!("{lang}: written again after the delete"),
        )
        .await;

        // Every merge answered with a record: none failed, and none fell back to the policy.
        for c in [&c1, &c2] {
            assert_eq!(
                counter(c, set_name, "fired").await,
                counter(c, set_name, "took-incoming").await
                    + counter(c, set_name, "combined").await
                    + counter(c, set_name, "keep-local").await,
                "{lang}"
            );
        }

        for c in [&c1, &c2] {
            assert_eq!(info(c, &format!("event-remove:event=xdr-merge;namespace={BIN_CONVERGENCE_NS};set={set_name}")).await, "ok");
            let _ = c.delete(&write, &key).await;
            let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
        }
    }
}

/// A value in a form two values that mean the same compare equal in: map entries in one order, whatever
/// map type the client chose, and a map's nil entries dropped (nil is "no such bin").
fn canonical(v: &Value) -> String {
    fn entries<'a>(m: impl Iterator<Item = (&'a Value, &'a Value)>) -> String {
        let mut e: Vec<String> = m
            .filter(|(_, v)| !matches!(v, Value::Nil))
            .map(|(k, v)| format!("{}={}", canonical(k), canonical(v)))
            .collect();

        e.sort();
        format!("{{{}}}", e.join(","))
    }

    match v {
        Value::HashMap(m) => entries(m.iter()),
        Value::OrderedMap(m) => entries(m.iter()),
        Value::SortedMap(m) => entries(m.iter()),
        Value::List(l) => format!("[{}]", l.iter().map(canonical).collect::<Vec<_>>().join(",")),
        other => format!("{other:?}"),
    }
}

/// A record's bins as a map value, for `canonical`.
fn bins_value(bins: impl IntoIterator<Item = (String, Value)>) -> Value {
    Value::from(bins.into_iter().map(|(k, v)| (Value::from(k), v)).collect::<HashMap<Value, Value>>())
}

fn hmap(entries: &[(&str, Value)]) -> Value {
    Value::from(entries.iter().map(|(k, v)| (Value::from(*k), v.clone())).collect::<HashMap<Value, Value>>())
}

fn strs(items: &[&str]) -> Value {
    Value::from(items.iter().map(|s| Value::from(*s)).collect::<Vec<Value>>())
}

/// **The dry run answers what the live merge does.** For each handler, in every language it is written
/// in: a record is made the same on both clusters, the handler is bound on dc1 only, and dc2 writes to the
/// record, so dc1 merges dc2's record into its own for real. Then dc1's dry run is asked to merge the same
/// incoming record into a second record that holds what the first one held before. The record the live
/// merge left and the record the dry run answers must be the same.
///
/// Every handler runs on the namespace without bin convergence, where the merge sees no per-bin time, so
/// the two records, written a moment apart, are the same input. The handlers that merge on the data alone
/// (the counters and the sets) run again on the namespace with bin convergence, where only the changed
/// bins travel and the handler is told bin convergence is on. A handler that reads per-bin times (the
/// resolver, the registers, `merge_api`) is left out there: each bin's time differs between the two
/// records, and it is entitled to answer differently. Those are checked by
/// `registers_converge_two_clusters` and, in the dry run, by `registers_merge_converges`.
#[aerospike_macro::test]
async fn the_dry_run_answers_what_the_live_merge_does() {
    let Some((dc1, dc2)) = dcs() else {
        eprintln!("skipped the_dry_run_answers_what_the_live_merge_does: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2");
        return;
    };

    let c1 = Client::new(&ClientPolicy::default(), &dc1).await.expect("dc1");
    let c2 = Client::new(&ClientPolicy::default(), &dc2).await.expect("dc2");
    let write = WritePolicy::default();
    let read = ReadPolicy::default();

    struct Case {
        name: &'static str,
        function: &'static str,
        modules: Vec<(&'static str, &'static [u8])>,
        current: Vec<(&'static str, Value)>,
        written: Vec<(&'static str, Value)>,
    }

    let four = |rust, java, go, cpp| vec![("rust", rust), ("java", java), ("go", go), ("cpp", cpp)];
    let cases = vec![
        Case {
            name: "counter",
            function: "merge_counter",
            modules: four(COUNTER, COUNTER_JAVA, COUNTER_GO, COUNTER_CPP),
            current: vec![("counts", hmap(&[("dc1", Value::from(3)), ("dc2", Value::from(1))]))],
            written: vec![("counts", hmap(&[("dc2", Value::from(5))]))],
        },
        Case {
            name: "pncounter",
            function: "merge_pncounter",
            modules: four(PNCOUNTER, PNCOUNTER_JAVA, PNCOUNTER_GO, PNCOUNTER_CPP),
            current: vec![
                ("inc", hmap(&[("dc1", Value::from(7))])),
                ("dec", hmap(&[("dc1", Value::from(2))])),
            ],
            written: vec![("inc", hmap(&[("dc2", Value::from(4))])), ("dec", hmap(&[("dc2", Value::from(1))]))],
        },
        Case {
            name: "twopset",
            function: "merge_twopset",
            modules: four(TWOPSET, TWOPSET_JAVA, TWOPSET_GO, TWOPSET_CPP),
            current: vec![("added", strs(&["art", "news"])), ("removed", strs(&["news"]))],
            written: vec![("added", strs(&["sport"])), ("removed", strs(&[]))],
        },
        Case {
            name: "lwwset",
            function: "merge_lwwset",
            modules: four(LWWSET, LWWSET_JAVA, LWWSET_GO, LWWSET_CPP),
            current: vec![
                ("addedAt", hmap(&[("art", Value::from(100)), ("news", Value::from(300))])),
                ("removedAt", hmap(&[("news", Value::from(200))])),
            ],
            written: vec![
                ("addedAt", hmap(&[("news", Value::from(150)), ("sport", Value::from(400))])),
                ("removedAt", hmap(&[("art", Value::from(500))])),
            ],
        },
        Case {
            name: "resolver",
            function: "merge_account",
            modules: vec![
                ("rust", RESOLVER),
                ("java", RESOLVER_JAVA),
                ("go", RESOLVER_GO),
                ("c", RESOLVER_C),
                ("cpp", RESOLVER_CPP),
            ],
            current: vec![
                ("visits", Value::from(7)),
                ("tags", strs(&["a", "b"])),
                ("profile", hmap(&[("name", Value::from("ada"))])),
                ("balance", Value::from(100)),
            ],
            written: vec![("visits", Value::from(5)), ("tags", strs(&["c"])), ("balance", Value::from(50))],
        },
        Case {
            name: "apitest",
            function: "merge_api",
            modules: vec![
                ("rust", APITEST),
                ("java", APITEST_JAVA),
                ("go", APITEST_GO),
                ("c", APITEST_C),
                ("cpp", APITEST_CPP),
            ],
            current: vec![("n", Value::from(4)), ("s", Value::from("here")), ("only_c", Value::from(1))],
            written: vec![("n", Value::from(9)), ("s", Value::from("there")), ("only_i", Value::from(2))],
        },
    ];

    let mut compared = 0;
    let data_only = ["counter", "pncounter", "twopset", "lwwset"];
    let with_bc = info(&c1, "namespaces").await.split(';').any(|n| n == BIN_CONVERGENCE_NS);
    let runs: Vec<(&str, &Case)> = cases
        .iter()
        .map(|c| ("test", c))
        .chain(cases.iter().filter(|c| with_bc && data_only.contains(&c.name)).map(|c| (BIN_CONVERGENCE_NS, c)))
        .collect();

    for (ns, case) in runs {
        for (lang, module) in &case.modules {
            let what = format!("{} ({lang}, {ns})", case.name);
            let set_name = &format!("dry_{}", crate::common::rand_str(6));
            let package = &format!("dry_{}_{lang}_{}", case.name, crate::common::rand_str(6));
            let live = as_key!(ns, set_name, "live");
            let dry = as_key!(ns, set_name, "dry");
            let current: Vec<Bin> = case.current.iter().map(|(n, v)| Bin::new(*n, v.clone())).collect();
            let written: Vec<Bin> = case.written.iter().map(|(n, v)| Bin::new(*n, v.clone())).collect();

            // The same record on both clusters, before anything is bound.
            c1.put(&write, &live, &current).await.unwrap();

            let want = canonical(&bins_value(c1.get(&read, &live, Bins::All).await.unwrap().bins));
            let deadline = std::time::Instant::now() + SHIP_LAG;

            loop {
                if let Ok(r) = c2.get(&read, &live, Bins::All).await {
                    if canonical(&bins_value(r.bins)) == want {
                        break;
                    }
                }

                assert!(std::time::Instant::now() < deadline, "{what}: dc2 never had the record");
                aerospike_rt::sleep(Duration::from_millis(100)).await;
            }

            // Bound on dc1 only, so dc2 takes dc1's later writes as they are.
            register_module(&c1, module, package).await;
            assert_eq!(
                info(&c1, &format!("event-set:event=xdr-merge;namespace={ns};set={set_name};module={package};function={}", case.function)).await,
                "ok",
                "{what}"
            );

            // dc2 writes; what it then holds is what reaches dc1, and dc1 merges it live.
            let fired = counter(&c1, set_name, "fired").await;

            c2.put(&write, &live, &written).await.unwrap();

            let incoming = c2.get(&read, &live, Bins::All).await.unwrap().bins;
            let deadline = std::time::Instant::now() + SHIP_LAG;

            while counter(&c1, set_name, "fired").await == fired {
                assert!(std::time::Instant::now() < deadline, "{what}: the merge never ran on dc1");
                aerospike_rt::sleep(Duration::from_millis(100)).await;
            }

            aerospike_rt::sleep(Duration::from_millis(300)).await;

            let merged = canonical(&bins_value(c1.get(&read, &live, Bins::All).await.unwrap().bins));

            // The dry run: the same incoming record against a record holding what the first one held.
            c1.put(&write, &dry, &current).await.unwrap();

            // Under bin convergence only the bins dc2's write changed travel.
            let incoming_bins: HashMap<Value, Value> = incoming
                .into_iter()
                .filter(|(k, _)| ns == "test" || case.written.iter().any(|(n, _)| n == k))
                .map(|(k, v)| (Value::from(k), v))
                .collect();
            let incoming_meta = HashMap::from([(Value::from("src-id"), Value::from(2))]);
            let answer = c1
                .execute_udf(&write, &dry, "__event__", "xdr-merge", &[Value::from(incoming_bins), Value::from(incoming_meta)])
                .await
                .unwrap_or_else(|e| panic!("{what}: the dry run failed: {e}"));
            let answer = match answer {
                Some(Value::HashMap(m)) => m,
                Some(Value::OrderedMap(m)) => m.into_iter().collect(),
                Some(Value::SortedMap(m)) => m.into_iter().collect(),
                other => panic!("{what}: {other:?}"),
            };
            let predicted = canonical(answer.get(&Value::from("bins")).unwrap_or_else(|| panic!("{what}: no bins in {answer:?}")));

            assert_eq!(predicted, merged, "{what}: the dry run predicted one record, the live merge made another");
            compared += 1;

            assert_eq!(info(&c1, &format!("event-remove:event=xdr-merge;namespace={ns};set={set_name}")).await, "ok");

            for c in [&c1, &c2] {
                let _ = c.delete(&write, &live).await;
                let _ = c.delete(&write, &dry).await;
            }

            let _ = c1.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
        }
    }

    assert_eq!(compared, if with_bc { 26 + 16 } else { 26 });
}

/// How long a rewound namespace may take to catch up: recovery scans the namespace before it ships.
const RECOVERY_LAG: Duration = Duration::from_secs(60);

/// Waits until both clusters count `total` and hold the same bins, or fails naming the sides. The bins are
/// compared as the client reads them, so maps compare by content: a merge may store its entries in a
/// different order on each side.
async fn converge_counter(c1: &Client, c2: &Client, key: &Key, package: &str, total: i64, lag: Duration, what: &str) {
    let deadline = std::time::Instant::now() + lag;

    loop {
        let (v1, v2) = (counter_value(c1, key, package).await, counter_value(c2, key, package).await);
        let (b1, b2) = (bins_of(c1, key).await, bins_of(c2, key).await);

        if v1 == Some(total) && v2 == Some(total) && b1 == b2 {
            return;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "{what}: both sides should count {total}: dc1 {v1:?} {b1:?}, dc2 {v2:?} {b2:?}"
        );
        aerospike_rt::sleep(Duration::from_millis(250)).await;
    }
}

/// An info command that may be refused for a while (a namespace still leaving its DC), retried until "ok".
async fn info_ok(client: &Client, command: &str) {
    let deadline = std::time::Instant::now() + SHIP_LAG;

    loop {
        let answer = info(client, command).await;

        if answer == "ok" {
            return;
        }

        assert!(std::time::Instant::now() < deadline, "{command}: {answer}");
        aerospike_rt::sleep(Duration::from_millis(250)).await;
    }
}

/// The races a merge has to survive, on one namespace. dc1 holds its ships for three seconds (`delay-ms`
/// in `_temp/asd-image-wasm/xdr/aerospike-dc1.conf`); dc2 ships at once.
///
/// - **The hold.** dc1 writes, and before that write leaves, dc2's write arrives on dc1 and is merged
///   there. The merged record holds dc1's change, which dc2 does not have yet, so dc1 must still ship it.
/// - **The outage.** dc1's link for the namespace goes. Both sides write; dc2's write is merged on dc1.
///   When the namespace comes back with a rewind, recovery reads every record and must ship the merged one.
///
/// Both end with the same bins on both sides.
async fn counter_survives_the_races(namespace: &str) {
    let Some((dc1, dc2)) = dcs() else {
        eprintln!("skipped the {namespace} races: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2");
        return;
    };

    let c1 = Client::new(&ClientPolicy::default(), &dc1).await.expect("dc1");
    let c2 = Client::new(&ClientPolicy::default(), &dc2).await.expect("dc2");
    let write = WritePolicy::default();
    let set_name = &format!("race_{}", crate::common::rand_str(6));
    let package = &format!("counter_race_{}", crate::common::rand_str(6));
    let key = as_key!(namespace, set_name, "page");
    let increment = |site: &str, n: i64| [Value::from(site), Value::from(n)];

    for c in [&c1, &c2] {
        register_module(c, COUNTER, package).await;
        assert_eq!(
            info(c, &format!("event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function=merge_counter;on-failure=abandon")).await,
            "ok"
        );
    }

    // Born on dc1, reaches dc2 as a create once the hold is over.
    c1.execute_udf(&write, &key, package, "increment", &increment("dc1", 3)).await.unwrap();
    converge_counter(&c1, &c2, &key, package, 3, SHIP_LAG, &format!("{namespace} create")).await;

    // The hold: dc1's write waits; dc2's is merged on dc1 first.
    c1.execute_udf(&write, &key, package, "increment", &increment("dc1", 2)).await.unwrap();
    c2.execute_udf(&write, &key, package, "increment", &increment("dc2", 5)).await.unwrap();
    converge_counter(&c1, &c2, &key, package, 10, SHIP_LAG, &format!("{namespace} hold")).await;

    let shipped_back = counter(&c1, set_name, "shipped-back").await;

    assert!(shipped_back >= 1, "{namespace}: the merge on dc1 held dc1's change, so it ships back");

    // The outage: dc1 stops shipping the namespace; both write; dc2's write is merged on dc1.
    let dc_ns = format!("set-config:context=xdr;dc=dc2;namespace={namespace}");

    info_ok(&c1, &format!("{dc_ns};action=remove")).await;
    c1.execute_udf(&write, &key, package, "increment", &increment("dc1", 1)).await.unwrap();
    c2.execute_udf(&write, &key, package, "increment", &increment("dc2", 1)).await.unwrap();

    let deadline = std::time::Instant::now() + SHIP_LAG;

    while counter_value(&c1, &key, package).await != Some(12) {
        assert!(std::time::Instant::now() < deadline, "{namespace}: dc2's write should be merged on dc1");
        aerospike_rt::sleep(Duration::from_millis(250)).await;
    }

    assert_eq!(counter_value(&c2, &key, package).await, Some(11), "{namespace}: dc2 lacks dc1's write until dc1 ships again");

    // Back, with a rewind: recovery reads the merged record and ships it.
    info_ok(&c1, &format!("{dc_ns};action=add;rewind=all")).await;
    converge_counter(&c1, &c2, &key, package, 12, RECOVERY_LAG, &format!("{namespace} outage")).await;

    for c in [&c1, &c2] {
        assert_eq!(info(c, &format!("event-remove:event=xdr-merge;namespace={namespace};set={set_name}")).await, "ok");
        let _ = c.delete(&write, &key).await;
        let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
    }
}

/// The races with whole records shipped and no bin convergence: the record's own flag is all XDR reads, so
/// a merge that holds anything dc2 lacks must not be marked an XDR write.
#[aerospike_macro::test]
async fn xdr_merge_survives_the_races_whole_records() {
    counter_survives_the_races("holdrec").await;
}

/// The races with only the changed bins shipped and no bin convergence: each bin's stamp decides whether
/// it ships, so a bin the merge computed must be stamped as a local write.
#[aerospike_macro::test]
async fn xdr_merge_survives_the_races_changed_bins() {
    counter_survives_the_races("holdch").await;
}

/// The races under bin convergence, which stamped bins correctly before; here so the three modes are held
/// to the same outcome.
#[aerospike_macro::test]
async fn xdr_merge_survives_the_races_bin_convergence() {
    counter_survives_the_races("holdbc").await;
}
