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
//! The module is the CRDT example (`aerospike-udf-sdk-wasm/examples/crdt`): a merge that decides on the
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

const SHIP_LAG: Duration = Duration::from_secs(20);

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
    assert!(counter(&c1, set_name, "record").await >= 1, "dc1 should have written a merged record");
    assert!(counter(&c2, set_name, "record").await >= 1, "dc2 should have written a merged record");
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
