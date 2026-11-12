// Copyright 2026 Aerospike, Inc.
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

//! Server events: a WASM function bound to the `xdr-merge` event, managed through the `event-set` /
//! `event-remove` / `event-list` info commands and exercised through the server's dry run.
//!
//! The event itself fires inside an XDR write, which needs two clusters. What a single node can verify is
//! everything around it: that a binding registers only when the module declares the right contract, that
//! the dry run hands the bound function the real local record and a remote one described by the caller and
//! returns its answer, that the answer is what the resolver was written to give, and that the server's
//! symmetry check agrees. The resolver is `aerospike-udf-sdk-wasm/examples/resolver`, checked in as a
//! fixture beside the others.
//!
//! # They self-skip
//!
//! Like the WASM UDF tests: a server without WASM, or without the event commands, is reported on stderr
//! and the test returns.

use std::collections::HashMap;

use crate::common;

use aerospike::Task;
use aerospike::*;

const RESOLVER: &[u8] = include_bytes!("../fixtures/wasm/resolver.wasm");

/// The same merge written against the C and Go SDKs.
const RESOLVER_C: &[u8] = include_bytes!("../fixtures/wasm/resolver_c.wasm");
const RESOLVER_GO: &[u8] = include_bytes!("../fixtures/wasm/resolver_go.wasm");
const RESOLVER_JAVA: &[u8] = include_bytes!("../fixtures/wasm/resolver_java.wasm");
const RESOLVER_CPP: &[u8] = include_bytes!("../fixtures/wasm/resolver_cpp.wasm");

/// The ordinary UDF fixture: it has functions, but none of them is an event handler, so binding one must be
/// refused.
const PLAIN: &[u8] = include_bytes!("../fixtures/wasm/udf_test.wasm");

/// The smallest CRDT, a grow-only counter: `aerospike-udf-sdk-wasm/examples/counter`, and the same in Java
/// and Go.
const COUNTER: &[u8] = include_bytes!("../fixtures/wasm/counter.wasm");
const COUNTER_JAVA: &[u8] = include_bytes!("../fixtures/wasm/counter_java.wasm");
const COUNTER_GO: &[u8] = include_bytes!("../fixtures/wasm/counter_go.wasm");
const COUNTER_CPP: &[u8] = include_bytes!("../fixtures/wasm/counter_cpp.wasm");

/// The counter that can go down, two grow-only counters glued together:
/// `aerospike-udf-sdk-wasm/examples/pncounter`, and the same in Java, Go and C++.
const PNCOUNTER: &[u8] = include_bytes!("../fixtures/wasm/pncounter.wasm");
const PNCOUNTER_JAVA: &[u8] = include_bytes!("../fixtures/wasm/pncounter_java.wasm");
const PNCOUNTER_GO: &[u8] = include_bytes!("../fixtures/wasm/pncounter_go.wasm");
const PNCOUNTER_CPP: &[u8] = include_bytes!("../fixtures/wasm/pncounter_cpp.wasm");

/// The 2P-Set, a set with removals: `aerospike-udf-sdk-wasm/examples/twopset`, and the same in Java, Go and C++.
const TWOPSET: &[u8] = include_bytes!("../fixtures/wasm/twopset.wasm");
const TWOPSET_JAVA: &[u8] = include_bytes!("../fixtures/wasm/twopset_java.wasm");
const TWOPSET_GO: &[u8] = include_bytes!("../fixtures/wasm/twopset_go.wasm");
const TWOPSET_CPP: &[u8] = include_bytes!("../fixtures/wasm/twopset_cpp.wasm");

/// The LWW-Element-Set, a set with re-adds on a clock: `aerospike-udf-sdk-wasm/examples/lwwset`, and the
/// same in Java, Go and C++.
const LWWSET: &[u8] = include_bytes!("../fixtures/wasm/lwwset.wasm");
const LWWSET_JAVA: &[u8] = include_bytes!("../fixtures/wasm/lwwset_java.wasm");
const LWWSET_GO: &[u8] = include_bytes!("../fixtures/wasm/lwwset_go.wasm");
const LWWSET_CPP: &[u8] = include_bytes!("../fixtures/wasm/lwwset_cpp.wasm");

/// The active-active CRDT of the design note: `aerospike-udf-sdk-wasm/examples/crdt`, and the same in Java
/// and Go.
const CRDT: &[u8] = include_bytes!("../fixtures/wasm/crdt.wasm");
const CRDT_JAVA: &[u8] = include_bytes!("../fixtures/wasm/crdt_java.wasm");
const CRDT_GO: &[u8] = include_bytes!("../fixtures/wasm/crdt_go.wasm");
const CRDT_CPP: &[u8] = include_bytes!("../fixtures/wasm/crdt_cpp.wasm");

/// The write-path handlers: `aerospike-udf-sdk-wasm/examples/guard`, and the same in C and Go.
const GUARD: &[u8] = include_bytes!("../fixtures/wasm/guard.wasm");
const GUARD_C: &[u8] = include_bytes!("../fixtures/wasm/guard_c.wasm");
const GUARD_GO: &[u8] = include_bytes!("../fixtures/wasm/guard_go.wasm");
const GUARD_JAVA: &[u8] = include_bytes!("../fixtures/wasm/guard_java.wasm");
const GUARD_CPP: &[u8] = include_bytes!("../fixtures/wasm/guard_cpp.wasm");

fn skipped(test: &str, reason: &str) {
    eprintln!("skipped {test}: {reason}");
}

/// Register a module, or say why this server cannot.
async fn register(client: &Client, module: &[u8], package: &str) -> std::result::Result<(), String> {
    match client
        .register_udf(&AdminPolicy::default(), module, &format!("{package}.wasm"), UdfLang::Wasm)
        .await
    {
        Ok(task) => task
            .wait_till_complete(None)
            .await
            .map(|_| ())
            .map_err(|e| format!("the module did not become available: {e}")),
        Err(e) => {
            let detail = format!("{e:?}");

            if detail.contains("invalid_udf_type") {
                Err("this server does not support WASM UDFs".to_string())
            } else {
                panic!("registering the module failed for a reason other than WASM support: {e:?}");
            }
        }
    }
}

/// One info command against every node; the answers must agree, so the first one is returned.
async fn info(client: &Client, command: &str) -> String {
    let answers = client.info(&AdminPolicy::default(), &[command]).await.unwrap();

    answers
        .get(command)
        .cloned()
        .unwrap_or_else(|| panic!("no answer to {command:?}: {answers:?}"))
}

/// Whether this server has the event system: the commands exist, and the feature key allows them.
async fn supported(client: &Client) -> bool {
    let answer = info(client, "event-list").await;

    !(answer.contains("not found") || answer.contains("feature key") || answer.contains("enterprise only"))
}

fn bind_command(namespace: &str, set_name: &str, package: &str, function: &str) -> String {
    format!("event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function={function}")
}

fn unbind_command(namespace: &str, set_name: &str) -> String {
    format!("event-remove:event=xdr-merge;namespace={namespace};set={set_name}")
}

/// `event-set` for any kind, with extra `;key=value` parameters appended as given.
fn set_command(kind: &str, namespace: &str, set_name: &str, package: &str, function: &str, extra: &str) -> String {
    let set = if set_name.is_empty() { String::new() } else { format!(";set={set_name}") };

    format!("event-set:event={kind};namespace={namespace}{set};module={package};function={function}{extra}")
}

fn remove_command(kind: &str, namespace: &str, set_name: &str) -> String {
    let set = if set_name.is_empty() { String::new() } else { format!(";set={set_name}") };

    format!("event-remove:event={kind};namespace={namespace}{set}")
}

/// This set's row of `event-list` for a kind and binding name.
async fn row(client: &Client, kind: &str, set_name: &str, name: &str) -> String {
    let listed = info(client, "event-list").await;

    listed
        .split(';')
        .find(|r| r.starts_with(&format!("event={kind}:")) && r.contains(&format!(":set={set_name}:")) && r.contains(&format!(":name={name}:")))
        .map(str::to_string)
        .unwrap_or_else(|| panic!("no {kind} row for set {set_name} name {name}: {listed}"))
}

fn counter(row: &str, name: &str) -> u64 {
    row.split(':')
        .find_map(|f| f.strip_prefix(&format!("{name}=")))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no {name} in {row}"))
}

async fn record_bins(client: &Client, key: &Key) -> HashMap<String, Value> {
    client
        .get(&ReadPolicy::default(), key, Bins::All)
        .await
        .unwrap()
        .bins
        .into_iter()
        .collect()
}

async fn exists(client: &Client, key: &Key) -> bool {
    client.exists(&ReadPolicy::default(), key).await.unwrap()
}

async fn remove_module(client: &Client, package: &str) {
    client
        .remove_udf(&AdminPolicy::default(), &format!("{package}.wasm"))
        .await
        .unwrap()
        .wait_till_complete(None)
        .await
        .unwrap();
}

/// The dry run: `__event__` is the reserved package, `xdr-merge` the function, and the arguments describe
/// the remote side - its bins, its metadata and, optionally, which bins would lose.
async fn dry_run(
    client: &Client,
    key: &Key,
    remote_bins: HashMap<Value, Value>,
    remote_meta: HashMap<Value, Value>,
) -> HashMap<Value, Value> {
    let result = client
        .execute_udf(
            &WritePolicy::default(),
            key,
            "__event__",
            "xdr-merge",
            &[Value::from(remote_bins), Value::from(remote_meta)],
        )
        .await
        .unwrap();

    match result {
        Some(Value::HashMap(m)) => m,
        Some(Value::SortedMap(m)) => m.into_iter().collect(),
        Some(Value::OrderedMap(m)) => m.into_iter().collect(),
        other => panic!("the dry run should answer with a map, got {other:?}"),
    }
}

fn bins_of(answer: &HashMap<Value, Value>) -> HashMap<Value, Value> {
    match answer.get(&Value::from("bins")) {
        Some(Value::HashMap(m)) => m.clone(),
        Some(Value::SortedMap(m)) => m.clone().into_iter().collect(),
        Some(Value::OrderedMap(m)) => m.clone().into_iter().collect(),
        other => panic!("the resolution should carry bins, got {other:?}"),
    }
}

fn meta(generation: i64, lut: i64, ttl: i64, src_id: i64) -> HashMap<Value, Value> {
    HashMap::from([
        (Value::from("gen"), Value::from(generation)),
        (Value::from("lut"), Value::from(lut)),
        (Value::from("ttl"), Value::from(ttl)),
        (Value::from("src-id"), Value::from(src_id)),
    ])
}

fn strings(items: &[&str]) -> Value {
    Value::List(items.iter().map(|s| Value::from(*s)).collect())
}

fn bins_map(entries: &[(&str, Value)]) -> HashMap<Value, Value> {
    entries.iter().map(|(k, v)| (Value::from(*k), v.clone())).collect()
}

/// The account the tests merge: a local record with visits, tags, a balance and a profile.
async fn put_account(client: &Client, key: &Key) {
    client
        .put(
            &WritePolicy::default(),
            key,
            &[
                as_bin!("visits", 7),
                as_bin!("tags", strings(&["a", "b"])),
                as_bin!("balance", 100),
                as_bin!(
                    "profile",
                    Value::from(HashMap::from([(Value::from("name"), Value::from("Al"))]))
                ),
            ],
        )
        .await
        .unwrap();
}

/// The whole management lifecycle: bind, list, dry-run, unbind.
#[aerospike_macro::test]
async fn an_xdr_merge_resolver_is_bound_and_dry_run() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("resolver_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("an_xdr_merge_resolver_is_bound_and_dry_run", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, RESOLVER, package).await {
        skipped("an_xdr_merge_resolver_is_bound_and_dry_run", &reason);
        return;
    }

    // Nothing is bound yet: the dry run says so rather than guessing.
    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, "alice");
    client
        .put(
            &write,
            &key,
            &[
                as_bin!("visits", 7),
                as_bin!("tags", strings(&["a", "b"])),
                as_bin!("balance", 100),
            ],
        )
        .await
        .unwrap();

    let unbound = client
        .execute_udf(&write, &key, "__event__", "xdr-merge", &[Value::from(HashMap::<Value, Value>::new())])
        .await;
    let detail = format!("{unbound:?}");
    assert!(
        detail.contains("no xdr-merge event is bound"),
        "a dry run with no binding must say so: {detail}"
    );

    // Bind the resolver to this set.
    let answer = info(&client, &bind_command(namespace, set_name, package, "merge_account")).await;
    assert_eq!(answer, "ok", "event-set should answer ok");

    let listed = info(&client, "event-list").await;
    assert!(
        listed.contains(&format!("namespace={namespace}:set={set_name}:name=merge_account:order=0:module={package}:function=merge_account:version=1")),
        "event-list should show the binding: {listed}"
    );

    // A remote record that saw fewer visits, other tags, and a profile the local one lacks; its LUT is
    // later than the local record's, so it is the newer side.
    let remote_bins = HashMap::from([
        (Value::from("visits"), Value::from(5)),
        (Value::from("tags"), strings(&["b", "c"])),
        (
            Value::from("profile"),
            Value::from(HashMap::from([(Value::from("name"), Value::from("Alice"))])),
        ),
        (Value::from("balance"), Value::from(50)),
    ]);
    let answer = dry_run(&client, &key, remote_bins, meta(3, 1_900_000_000_000, 0, 2)).await;

    assert_eq!(
        answer.get(&Value::from("verdict")),
        Some(&Value::from("record")),
        "the resolver returns a record: {answer:?}"
    );

    let bins = bins_of(&answer);

    assert_eq!(bins.get(&Value::from("visits")), Some(&Value::from(7)), "max of the two counts");
    assert_eq!(
        bins.get(&Value::from("tags")),
        Some(&strings(&["a", "b", "c"])),
        "the union of the tags, local first"
    );
    assert!(
        matches!(bins.get(&Value::from("profile")), Some(Value::HashMap(_) | Value::SortedMap(_) | Value::OrderedMap(_))),
        "the profile only the remote side has: {bins:?}"
    );
    assert_eq!(
        bins.get(&Value::from("balance")),
        Some(&Value::from(50)),
        "a bin the resolver has no opinion on is the newer side's - the remote, whose LUT is in 2030"
    );
    assert_eq!(
        answer.get(&Value::from("symmetric")),
        Some(&Value::from(true)),
        "max, union and newer-by-LUT are symmetric, and the server's check must agree: {answer:?}"
    );
    assert_eq!(answer.get(&Value::from("idempotent")), Some(&Value::from(true)), "{answer:?}");

    // The counters count conflicts, and a dry run is not one: the binding's row still reads zero. (A node
    // may list bindings left by other runs, so the check is on this test's own row.)
    let listed = info(&client, "event-list").await;
    let row = listed
        .split(';')
        .find(|row| row.contains(&format!(":set={set_name}:")))
        .unwrap_or_else(|| panic!("the binding should still be listed: {listed}"));
    assert!(row.contains(":fired=0:"), "a dry run must not count as a firing: {row}");

    // The module is bound, so it cannot be removed...
    let refused = client
        .remove_udf(&AdminPolicy::default(), &format!("{package}.wasm"))
        .await;
    assert!(refused.is_err(), "udf-remove must refuse a module an event binding refers to");

    // ...until the binding goes.
    let answer = info(&client, &unbind_command(namespace, set_name)).await;
    assert_eq!(answer, "ok", "event-remove should answer ok");

    let listed = info(&client, "event-list").await;
    assert!(!listed.contains(&format!("set={set_name}:")), "the binding should be gone: {listed}");

    client
        .remove_udf(&AdminPolicy::default(), &format!("{package}.wasm"))
        .await
        .unwrap()
        .wait_till_complete(None)
        .await
        .unwrap();
}

/// The payload's finer metadata: a bin's own LUT decides it, not the record's, and a delete by the later
/// writer is honoured rather than undone by the other side's copy.
#[aerospike_macro::test]
async fn per_bin_luts_and_deletes_decide_bins() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("resolver_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("per_bin_luts_and_deletes_decide_bins", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, RESOLVER, package).await {
        skipped("per_bin_luts_and_deletes_decide_bins", &reason);
        return;
    }

    let key = as_key!(namespace, set_name, "bob");
    put_account(&client, &key).await;
    assert_eq!(info(&client, &bind_command(namespace, set_name, package, "merge_account")).await, "ok");

    // The remote record is newer as a whole (its LUT is in 2030), but its `balance` was last written in
    // 1970 by its own account (`bin-luts`), and it deleted `profile`.
    let remote_bins = bins_map(&[
        ("visits", Value::from(5)),
        ("tags", strings(&["c"])),
        ("balance", Value::from(50)),
    ]);
    let mut remote_meta = meta(3, 1_900_000_000_000, 0, 2);
    remote_meta.insert(
        Value::from("bin-luts"),
        Value::from(bins_map(&[("balance", Value::from(1_000))])),
    );
    remote_meta.insert(Value::from("deleted"), strings(&["profile"]));

    let answer = dry_run(&client, &key, remote_bins, remote_meta).await;
    let bins = bins_of(&answer);

    assert_eq!(bins.get(&Value::from("visits")), Some(&Value::from(7)));
    assert_eq!(bins.get(&Value::from("tags")), Some(&strings(&["a", "b", "c"])));
    // Without bin convergence the current side has no per-bin LUTs and, by design, no record LUT either,
    // so the sides tie on time and the source id decides: the incoming write (source 2) over this cluster
    // (source 1). A resolver that must decide on data compares versions or bytes instead of LUTs.
    assert_eq!(bins.get(&Value::from("balance")), Some(&Value::from(50)), "{bins:?}");
    assert_eq!(
        bins.get(&Value::from("profile")),
        Some(&Value::Nil),
        "deleted by the later writer: a nil, which the server applies as a delete: {bins:?}"
    );
    assert_eq!(answer.get(&Value::from("symmetric")), Some(&Value::from(true)), "{answer:?}");

    assert_eq!(info(&client, &unbind_command(namespace, set_name)).await, "ok");
    client
        .remove_udf(&AdminPolicy::default(), &format!("{package}.wasm"))
        .await
        .unwrap()
        .wait_till_complete(None)
        .await
        .unwrap();
}

/// The merge written three times, against the Rust, C and Go SDKs, resolves the same pair to the same
/// record. Each language's event binding is exercised end to end: decoding the description, reading both
/// sides' bins by position, per-bin LUT ordering, deletes, and returning a record.
#[aerospike_macro::test]
async fn resolvers_in_three_languages_agree() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let tag = common::rand_str(6);

    if !supported(&client).await {
        skipped("resolvers_in_three_languages_agree", "no event commands on this server");
        return;
    }

    let key = as_key!(namespace, set_name, "carol");
    put_account(&client, &key).await;

    let modules = [("rust", RESOLVER), ("c", RESOLVER_C), ("go", RESOLVER_GO), ("java", RESOLVER_JAVA), ("cpp", RESOLVER_CPP)];
    let mut answers = Vec::new();

    for (lang, module) in modules {
        let package = &format!("resolver_{lang}_{tag}");

        if let Err(reason) = register(&client, module, package).await {
            skipped("resolvers_in_three_languages_agree", &reason);
            return;
        }

        // Rebinding the same set replaces the binding.
        assert_eq!(
            info(&client, &bind_command(namespace, set_name, package, "merge_account")).await,
            "ok",
            "{lang}"
        );

        let remote_bins = bins_map(&[
            ("visits", Value::from(5)),
            ("tags", strings(&["b", "c"])),
            ("balance", Value::from(50)),
            ("city", Value::from("Oslo")),
        ]);
        let mut remote_meta = meta(3, 1_900_000_000_000, 0, 2);
        remote_meta.insert(
            Value::from("bin-luts"),
            Value::from(bins_map(&[("balance", Value::from(1_000))])),
        );
        remote_meta.insert(Value::from("deleted"), strings(&["profile"]));

        let answer = dry_run(&client, &key, remote_bins, remote_meta).await;

        assert_eq!(answer.get(&Value::from("verdict")), Some(&Value::from("record")), "{lang}: {answer:?}");
        assert_eq!(answer.get(&Value::from("symmetric")), Some(&Value::from(true)), "{lang}: {answer:?}");
        answers.push((lang, bins_of(&answer)));
    }

    let (_, expected) = &answers[0];
    assert_eq!(expected.get(&Value::from("visits")), Some(&Value::from(7)));
    assert_eq!(expected.get(&Value::from("tags")), Some(&strings(&["a", "b", "c"])));
    // No current-side bin LUTs and no record LUT on either side: the sides tie on time, and the source id
    // decides for the incoming write (source 2 over this cluster's 1).
    assert_eq!(expected.get(&Value::from("balance")), Some(&Value::from(50)));
    assert_eq!(expected.get(&Value::from("profile")), Some(&Value::Nil));
    assert_eq!(expected.get(&Value::from("city")), Some(&Value::from("Oslo")), "a bin only the remote has");

    for (lang, bins) in &answers[1..] {
        assert_eq!(bins, expected, "the {lang} resolver disagrees with the Rust one");
    }

    assert_eq!(info(&client, &unbind_command(namespace, set_name)).await, "ok");

    for (lang, _) in modules {
        client
            .remove_udf(&AdminPolicy::default(), &format!("resolver_{lang}_{tag}.wasm"))
            .await
            .unwrap()
            .wait_till_complete(None)
            .await
            .unwrap();
    }
}

/// The `event-admin` privilege: `event-set` needs it, a role can carry it (code 5 round-trips through the
/// client's `PrivilegeCode::EventAdmin`), and a user without it is refused. Runs only against a server with
/// security enabled, which the harness signals through `AEROSPIKE_USER`.
#[aerospike_macro::test]
async fn event_admin_privilege_gates_binding() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let tag = common::rand_str(6);
    let package = &format!("resolver_{tag}");

    if std::env::var("AEROSPIKE_USER").is_err() {
        skipped("event_admin_privilege_gates_binding", "no AEROSPIKE_USER: security is not enabled");
        return;
    }

    if !supported(&client).await {
        skipped("event_admin_privilege_gates_binding", "no event commands on this server");
        return;
    }

    let admin = AdminPolicy::default();
    let hosts = common::hosts();

    // A fresh Enterprise node's bootstrap user has `user-admin` and nothing else. Give it what the rest of
    // this test needs, then log in again: a session's permissions refresh every few minutes, a login reads
    // them at once.
    let user = std::env::var("AEROSPIKE_USER").unwrap();
    let _ = client
        .grant_roles(&admin, &user, &["sys-admin", "data-admin", "read-write-udf"])
        .await;
    let client = Client::new(common::client_policy(), &hosts).await.unwrap();

    if let Err(reason) = register(&client, RESOLVER, package).await {
        skipped("event_admin_privilege_gates_binding", &reason);
        return;
    }
    let binder_role = format!("event_binder_{tag}");
    let binder = format!("binder_{tag}");
    let reader = format!("reader_{tag}");

    client
        .create_role(
            &admin,
            &binder_role,
            &[Privilege::new(PrivilegeCode::EventAdmin, None, None)],
            &[],
            0,
            0,
        )
        .await
        .unwrap();

    // Roles travel through SMD; give the read-back a moment.
    let mut roles = Vec::new();

    for _ in 0..20 {
        if let Ok(found) = client.query_roles(&admin, Some(&binder_role)).await {
            roles = found;
            break;
        }

        aerospike_rt::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(
        roles.iter().any(|r| r.privileges.iter().any(|p| p.code == PrivilegeCode::EventAdmin)),
        "the role should read back with event-admin: {roles:?}"
    );

    client.create_user(&admin, &binder, "pw", &[&binder_role]).await.unwrap();
    client.create_user(&admin, &reader, "pw", &["read"]).await.unwrap();

    // A client for each user, on the same cluster.
    let as_user = |user: &str| {
        let mut policy = common::client_policy().clone();
        policy.set_auth_mode(AuthMode::Internal(user.to_string(), "pw".to_string()));
        policy
    };
    let binder_client = Client::new(&as_user(&binder), &hosts).await.unwrap();
    let reader_client = Client::new(&as_user(&reader), &hosts).await.unwrap();

    let bind = bind_command(namespace, set_name, package, "merge_account");

    let refused = info(&reader_client, &bind).await;
    assert_ne!(refused, "ok", "a user without event-admin must be refused: {refused}");

    assert_eq!(info(&binder_client, &bind).await, "ok", "event-admin suffices to bind");
    assert!(info(&reader_client, "event-list").await.contains(&format!(":set={set_name}:")), "listing needs no privilege");

    let refused = info(&reader_client, &unbind_command(namespace, set_name)).await;
    assert_ne!(refused, "ok", "nor may that user unbind: {refused}");
    assert_eq!(info(&binder_client, &unbind_command(namespace, set_name)).await, "ok");

    binder_client.close().await.unwrap();
    reader_client.close().await.unwrap();
    client.drop_user(&admin, &binder).await.unwrap();
    client.drop_user(&admin, &reader).await.unwrap();
    client.drop_role(&admin, &binder_role).await.unwrap();
    client
        .remove_udf(&admin, &format!("{package}.wasm"))
        .await
        .unwrap()
        .wait_till_complete(None)
        .await
        .unwrap();
}

/// `event-kinds` says what this server serves and at which version.
#[aerospike_macro::test]
async fn event_kinds_are_listed_with_versions() {
    let client = common::client().await;

    if !supported(&client).await {
        skipped("event_kinds_are_listed_with_versions", "no event commands on this server");
        return;
    }

    let kinds = info(&client, "event-kinds").await;

    for kind in ["xdr-merge", "before-write", "before-delete", "before-truncate", "after-truncate"] {
        assert!(kinds.contains(&format!("event={kind}:version=1:")), "{kind} missing: {kinds}");
    }

    assert!(kinds.contains("event=before-write:version=1:scope=namespace:single=false:filter=true;"), "{kinds}");
    assert!(kinds.contains("event=xdr-merge:version=1:scope=namespace:single=true:filter=false;"), "{kinds}");
}

/// `before-write`: a handler rejects a write that breaks a rule and rewrites the ones it accepts.
#[aerospike_macro::test]
async fn before_write_rejects_and_stamps() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("before_write_rejects_and_stamps", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("before_write_rejects_and_stamps", &reason);
        return;
    }

    assert_eq!(info(&client, &set_command("before-write", namespace, set_name, package, "guard_account", "")).await, "ok");

    let mut write = WritePolicy::default();
    // The handler's message travels as server error detail, which a policy has to ask for.
    write.base_policy.error_detail_verbosity = 2;
    let key = as_key!(namespace, set_name, "dave");

    // Rejected: the record must not exist afterwards, and the client sees the handler's code and message.
    let refused = client.put(&write, &key, &[as_bin!("balance", -5)]).await;
    let e = refused.expect_err("a negative balance must be rejected");
    assert_eq!(e.result_code(), 22, "the handler's code, forbidden: {e:?}");
    assert_eq!(e.server_message(), Some("balance may not go negative"), "{e:?}");
    assert!(!exists(&client, &key).await, "a rejected create must leave nothing behind");

    // Accepted and stamped: the handler's bins ride on the write's.
    client.put(&write, &key, &[as_bin!("balance", 10)]).await.unwrap();
    let bins = record_bins(&client, &key).await;
    assert_eq!(bins.get("balance"), Some(&Value::from(10)));
    assert_eq!(bins.get("stamped"), Some(&Value::from("client")), "{bins:?}");
    assert_eq!(bins.get("writes"), Some(&Value::from(1)), "{bins:?}");

    // The stamp counts across writes: `before` carried the previous count.
    client.put(&write, &key, &[as_bin!("balance", 12)]).await.unwrap();
    let bins = record_bins(&client, &key).await;
    assert_eq!(bins.get("writes"), Some(&Value::from(2)), "{bins:?}");
    assert_eq!(bins.get("balance"), Some(&Value::from(12)));

    // An update that would go negative is refused and the record keeps its state.
    let e = client.put(&write, &key, &[as_bin!("balance", -1)]).await.expect_err("rejected");
    assert_eq!(e.result_code(), 22);
    assert_eq!(record_bins(&client, &key).await.get("balance"), Some(&Value::from(12)));

    let r = row(&client, "before-write", set_name, "guard_account").await;
    assert_eq!(counter(&r, "fired"), 4, "{r}");
    assert_eq!(counter(&r, "rejected"), 2, "{r}");
    assert_eq!(counter(&r, "record"), 2, "{r}");

    assert_eq!(info(&client, &remove_command("before-write", namespace, set_name)).await, "ok");
    remove_module(&client, package).await;
}

/// A filter expression on the binding: the handler runs only on records that match it.
#[aerospike_macro::test]
async fn before_write_filter_gates_the_handler() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("before_write_filter_gates_the_handler", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("before_write_filter_gates_the_handler", &reason);
        return;
    }

    // Only records with a `vip` bin, as the client's expression builder packs it.
    let exp = aerospike::expressions::bin_exists("vip").base64().unwrap();
    let bound = info(&client, &set_command("before-write", namespace, set_name, package, "guard_account", &format!(";exp={exp}"))).await;
    assert_eq!(bound, "ok", "{bound}");

    let r = row(&client, "before-write", set_name, "guard_account").await;
    assert!(r.contains(&format!(":filter=exp:{exp}:")), "the filter is listed as given: {r}");

    let write = WritePolicy::default();
    let plain = as_key!(namespace, set_name, "plain");
    let vip = as_key!(namespace, set_name, "vip");

    client.put(&write, &plain, &[as_bin!("balance", -5)]).await.unwrap();
    assert!(!record_bins(&client, &plain).await.contains_key("stamped"), "filtered out: not stamped, not rejected");

    let e = client.put(&write, &vip, &[as_bin!("balance", -5), as_bin!("vip", true)]).await.expect_err("matched: rejected");
    assert_eq!(e.result_code(), 22);
    client.put(&write, &vip, &[as_bin!("balance", 5), as_bin!("vip", true)]).await.unwrap();
    assert_eq!(record_bins(&client, &vip).await.get("stamped"), Some(&Value::from("client")));

    let r = row(&client, "before-write", set_name, "guard_account").await;
    assert_eq!(counter(&r, "filtered"), 1, "{r}");
    assert_eq!(counter(&r, "fired"), 2, "{r}");

    // A filter that does not build is refused at event-set.
    let bad = info(&client, &set_command("before-write", namespace, set_name, package, "guard_account", ";name=other;exp=AAAA")).await;
    assert!(bad.starts_with("ERROR:"), "{bad}");

    assert_eq!(info(&client, &remove_command("before-write", namespace, set_name)).await, "ok");
    remove_module(&client, package).await;
}

/// Two bindings of one kind on one scope run as a chain, in `order`, each seeing the previous one's record.
#[aerospike_macro::test]
async fn before_write_bindings_chain_in_order() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("before_write_bindings_chain_in_order", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("before_write_bindings_chain_in_order", &reason);
        return;
    }

    assert_eq!(info(&client, &set_command("before-write", namespace, set_name, package, "guard_account", ";order=2")).await, "ok");
    assert_eq!(info(&client, &set_command("before-write", namespace, set_name, package, "stamp_region", ";order=1")).await, "ok");

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, "erin");

    client.put(&write, &key, &[as_bin!("balance", 1)]).await.unwrap();
    let bins = record_bins(&client, &key).await;
    assert_eq!(bins.get("region"), Some(&Value::from("eu")), "the first handler's bin survives the second: {bins:?}");
    assert_eq!(bins.get("stamped"), Some(&Value::from("client")), "{bins:?}");
    assert_eq!(bins.get("writes"), Some(&Value::from(1)), "{bins:?}");

    let listed = info(&client, "event-list").await;
    assert!(listed.contains(&format!(":set={set_name}:name=stamp_region:order=1:")), "{listed}");
    assert!(listed.contains(&format!(":set={set_name}:name=guard_account:order=2:")), "{listed}");

    // Removing by name leaves the other; removing without a name takes the rest.
    assert_eq!(info(&client, &format!("{};name=stamp_region", remove_command("before-write", namespace, set_name))).await, "ok");
    client.put(&write, &key, &[as_bin!("balance", 2), as_bin!("region", "us")]).await.unwrap();
    assert_eq!(record_bins(&client, &key).await.get("region"), Some(&Value::from("us")), "stamp_region is gone");
    assert_eq!(info(&client, &remove_command("before-write", namespace, set_name)).await, "ok");
    assert!(!info(&client, "event-list").await.contains(&format!(":set={set_name}:")));

    remove_module(&client, package).await;
}

/// `before-delete`: a locked record cannot be deleted; an unlocked one can.
#[aerospike_macro::test]
async fn before_delete_refuses_locked_records() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("before_delete_refuses_locked_records", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("before_delete_refuses_locked_records", &reason);
        return;
    }

    assert_eq!(info(&client, &set_command("before-delete", namespace, set_name, package, "guard_delete", "")).await, "ok");

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, "frank");

    client.put(&write, &key, &[as_bin!("locked", true), as_bin!("n", 1)]).await.unwrap();
    let e = client.delete(&write, &key).await.expect_err("locked");
    assert_eq!(e.result_code(), 22, "{e:?}");
    assert!(exists(&client, &key).await, "still there");

    client.put(&write, &key, &[as_bin!("locked", false)]).await.unwrap();
    assert!(client.delete(&write, &key).await.unwrap(), "unlocked: deleted");
    assert!(!exists(&client, &key).await);

    let r = row(&client, "before-delete", set_name, "guard_delete").await;
    assert_eq!(counter(&r, "fired"), 2, "{r}");
    assert_eq!(counter(&r, "rejected"), 1, "{r}");

    assert_eq!(info(&client, &remove_command("before-delete", namespace, set_name)).await, "ok");
    remove_module(&client, package).await;
}

/// `before-truncate` fences the command; `after-truncate` is told when one went through.
#[aerospike_macro::test]
async fn truncate_is_fenced_and_announced() {
    let client = common::client().await;
    let namespace = common::namespace();
    let tag = common::rand_str(8);
    let package = &format!("guard_{tag}");

    if !supported(&client).await {
        skipped("truncate_is_fenced_and_announced", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("truncate_is_fenced_and_announced", &reason);
        return;
    }

    // Namespace-level bindings: every set of the namespace is covered.
    assert_eq!(info(&client, &set_command("before-truncate", namespace, "", package, "guard_truncate", "")).await, "ok");
    assert_eq!(info(&client, &set_command("after-truncate", namespace, "", package, "note_truncate", "")).await, "ok");

    let write = WritePolicy::default();
    let keep = format!("keep_{tag}");
    let tmp = format!("tmp_{tag}");

    client.put(&write, &as_key!(namespace, &keep, 1), &[as_bin!("n", 1)]).await.unwrap();
    client.put(&write, &as_key!(namespace, &tmp, 1), &[as_bin!("n", 1)]).await.unwrap();

    let refused = info(&client, &format!("truncate:namespace={namespace};set={keep}")).await;
    assert!(refused.starts_with("ERROR:") && refused.contains("protected"), "{refused}");
    assert!(exists(&client, &as_key!(namespace, &keep, 1)).await, "the protected set is intact");

    let allowed = info(&client, &format!("truncate:namespace={namespace};set={tmp}")).await;
    assert_eq!(allowed, "ok", "{allowed}");

    // The after event fires on the SMD accept, which is immediate on one node but not synchronous with the
    // command's answer.
    let mut after = 0;

    for _ in 0..30 {
        after = counter(&row(&client, "after-truncate", "", "note_truncate").await, "fired");

        if after >= 1 {
            break;
        }

        aerospike_rt::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(after, 1, "after-truncate fired once for the one truncate that went through");
    assert_eq!(counter(&row(&client, "before-truncate", "", "guard_truncate").await, "rejected"), 1);

    assert_eq!(info(&client, &remove_command("before-truncate", namespace, "")).await, "ok");
    assert_eq!(info(&client, &remove_command("after-truncate", namespace, "")).await, "ok");
    remove_module(&client, package).await;
}

/// `before-expire`: a handler keeps the records it wants past their TTL and lets the rest go. The
/// supervisor runs every `nsup-period`, which this test lowers to a second and restores.
#[aerospike_macro::test]
async fn before_expire_keeps_marked_records() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("before_expire_keeps_marked_records", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("before_expire_keeps_marked_records", &reason);
        return;
    }

    assert_eq!(info(&client, &set_command("before-expire", namespace, set_name, package, "keep_marked", "")).await, "ok");

    let period = info(&client, &format!("set-config:context=namespace;id={namespace};nsup-period=1")).await;
    assert_eq!(period, "ok", "{period}");

    let mut write = WritePolicy::default();
    write.expiration = Expiration::Seconds(1);
    let kept = as_key!(namespace, set_name, "kept");
    let gone = as_key!(namespace, set_name, "gone");

    client.put(&write, &kept, &[as_bin!("keep", true), as_bin!("n", 1)]).await.unwrap();
    client.put(&write, &gone, &[as_bin!("keep", false), as_bin!("n", 2)]).await.unwrap();

    // Both are due within two seconds. A read treats a past void time as "not found" on its own, so the
    // thing to wait for is the supervisor's visit: the binding's `fired` counter says when it has offered
    // both records to the handler.
    let mut visited = 0;

    for _ in 0..60 {
        aerospike_rt::sleep(std::time::Duration::from_millis(500)).await;
        visited = counter(&row(&client, "before-expire", set_name, "keep_marked").await, "fired");

        if visited >= 2 {
            break;
        }
    }

    let _ = info(&client, &format!("set-config:context=namespace;id={namespace};nsup-period=120")).await;

    assert!(visited >= 2, "the supervisor offered both records to the handler");
    assert!(!exists(&client, &gone).await, "the unmarked record expired");
    assert!(exists(&client, &kept).await, "the marked record was kept");

    let ttl = client.get(&ReadPolicy::default(), &kept, Bins::None).await.unwrap().time_to_live();
    assert!(ttl.is_some_and(|t| t.as_secs() > 600), "kept for an hour: {ttl:?}");

    let r = row(&client, "before-expire", set_name, "keep_marked").await;
    assert!(counter(&r, "fired") >= 2, "{r}");
    assert!(counter(&r, "rejected") >= 1, "a keep counts as a refused expiry: {r}");

    assert_eq!(info(&client, &remove_command("before-expire", namespace, set_name)).await, "ok");
    client.delete(&WritePolicy::default(), &kept).await.unwrap();
    remove_module(&client, package).await;
}

/// `before-evict`: a handler protects the records it wants from the eviction pass and lets the rest go.
/// The pass is forced with `eviction-reset` and a TTL, which makes every record due within that many
/// seconds a candidate (so, on a shared node, a sibling's short-lived records too), and run by the
/// supervisor, lowered to a second for the duration and restored, as is the eviction cut-off.
#[aerospike_macro::test]
async fn before_evict_protects_marked_records() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("before_evict_protects_marked_records", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("before_evict_protects_marked_records", &reason);
        return;
    }

    assert_eq!(info(&client, &set_command("before-evict", namespace, set_name, package, "protect_marked", "")).await, "ok");

    let mut write = WritePolicy::default();
    write.expiration = Expiration::Seconds(300);
    let kept = as_key!(namespace, set_name, "kept");
    let gone = as_key!(namespace, set_name, "gone");

    client.put(&write, &kept, &[as_bin!("keep", true), as_bin!("n", 1)]).await.unwrap();
    client.put(&write, &gone, &[as_bin!("keep", false), as_bin!("n", 2)]).await.unwrap();

    let reset = info(&client, &format!("eviction-reset:namespace={namespace};ttl=3600")).await;
    assert_eq!(reset, "ok", "{reset}");
    let period = info(&client, &format!("set-config:context=namespace;id={namespace};nsup-period=1")).await;
    assert_eq!(period, "ok", "{period}");

    let mut visited = 0;

    for _ in 0..60 {
        aerospike_rt::sleep(std::time::Duration::from_millis(500)).await;
        visited = counter(&row(&client, "before-evict", set_name, "protect_marked").await, "fired");

        if visited >= 2 {
            break;
        }
    }

    let _ = info(&client, &format!("set-config:context=namespace;id={namespace};nsup-period=120")).await;
    let _ = info(&client, &format!("eviction-reset:namespace={namespace}")).await;

    assert!(visited >= 2, "the eviction pass offered both records to the handler: {visited}");
    assert!(!exists(&client, &gone).await, "the unmarked record was evicted");
    assert!(exists(&client, &kept).await, "the marked record was protected");

    let r = row(&client, "before-evict", set_name, "protect_marked").await;
    assert!(counter(&r, "policy") >= 1, "an evict answer counts as policy: {r}");
    assert!(counter(&r, "rejected") >= 1, "a protect counts as a refused eviction: {r}");

    assert_eq!(info(&client, &remove_command("before-evict", namespace, set_name)).await, "ok");
    client.delete(&WritePolicy::default(), &kept).await.unwrap();
    remove_module(&client, package).await;
}

/// `after-read`: a handler answers a read with a record of its own; the stored record is untouched.
#[aerospike_macro::test]
async fn after_read_rewrites_the_response() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("after_read_rewrites_the_response", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("after_read_rewrites_the_response", &reason);
        return;
    }

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, "grace");

    client.put(&write, &key, &[as_bin!("secret", "s3cr3t"), as_bin!("a", 1)]).await.unwrap();
    assert_eq!(record_bins(&client, &key).await.get("secret"), Some(&Value::from("s3cr3t")), "before binding");

    assert_eq!(info(&client, &set_command("after-read", namespace, set_name, package, "hide_secret", "")).await, "ok");

    let bins = record_bins(&client, &key).await;
    assert_eq!(bins.get("secret"), None, "hidden: {bins:?}");
    assert_eq!(bins.get("a"), Some(&Value::from(1)), "{bins:?}");
    assert_eq!(bins.get("read_by"), Some(&Value::from("client")), "{bins:?}");

    // A read of named bins is answered with the handler's record too, whole.
    let named = client.get(&ReadPolicy::default(), &key, ["secret"]).await.unwrap().bins;
    assert_eq!(named.get("secret"), None, "{named:?}");
    assert_eq!(named.get("read_by"), Some(&Value::from("client")), "{named:?}");

    assert!(exists(&client, &key).await, "existence is not a read of bins");

    assert_eq!(info(&client, &remove_command("after-read", namespace, set_name)).await, "ok");
    assert_eq!(record_bins(&client, &key).await.get("secret"), Some(&Value::from("s3cr3t")), "stored untouched");

    let r_listed = info(&client, "event-list").await;
    assert!(!r_listed.contains(&format!(":set={set_name}:")), "{r_listed}");
    remove_module(&client, package).await;
}

/// The once-per-node events. On one node without a restart only `binding-deployed` can be seen to fire,
/// which it does for the lifecycle binding's own deploy; the others are bound and listed, and the
/// counters they would move are read.
#[aerospike_macro::test]
async fn lifecycle_events_bind_and_the_deploy_phase_fires() {
    let client = common::client().await;
    let namespace = common::namespace();
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("lifecycle_events_bind_and_the_deploy_phase_fires", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("lifecycle_events_bind_and_the_deploy_phase_fires", &reason);
        return;
    }

    // Global kinds take no namespace.
    let bound = info(&client, &format!("event-set:event=node-lifecycle;module={package};function=note_lifecycle")).await;
    assert_eq!(bound, "ok", "{bound}");

    // Every binding made anywhere on the node is a deploy, and the other tests in this file bind things
    // at the same time, so the count is monotone rather than exact.
    let after_own = counter(&row(&client, "node-lifecycle", "", "note_lifecycle").await, "fired");
    assert!(after_own >= 1, "its own deploy: {after_own}");

    let bound = info(&client, &format!("event-set:event=cluster-change;module={package};function=note_cluster")).await;
    assert_eq!(bound, "ok", "{bound}");
    let after_next = counter(&row(&client, "node-lifecycle", "", "note_lifecycle").await, "fired");
    assert!(after_next > after_own, "another deploy: {after_own} -> {after_next}");
    assert_eq!(counter(&row(&client, "cluster-change", "", "note_cluster").await, "fired"), 0, "no rebalance on one node");

    assert_eq!(info(&client, &set_command("partition-transfer", namespace, "", package, "note_transfer", "")).await, "ok");
    assert_eq!(counter(&row(&client, "partition-transfer", "", "note_transfer").await, "fired"), 0, "no migration on one node");

    // A namespace on a global kind, or none on a namespace kind, is refused.
    let bad = info(&client, &format!("event-set:event=partition-transfer;module={package};function=note_transfer")).await;
    assert!(bad.starts_with("ERROR:"), "{bad}");

    assert_eq!(info(&client, "event-remove:event=node-lifecycle").await, "ok");
    assert_eq!(info(&client, "event-remove:event=cluster-change").await, "ok");
    assert_eq!(info(&client, &remove_command("partition-transfer", namespace, "")).await, "ok");
    remove_module(&client, package).await;
}

/// `after-write` and `after-delete` run on the server's queue, off the transaction path: the write returns
/// before the handler has run, and the counters catch up.
#[aerospike_macro::test]
async fn after_events_run_off_the_write_path() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("after_events_run_off_the_write_path", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("after_events_run_off_the_write_path", &reason);
        return;
    }

    assert_eq!(info(&client, &set_command("after-write", namespace, set_name, package, "note_write", "")).await, "ok");
    assert_eq!(info(&client, &set_command("after-delete", namespace, set_name, package, "note_delete", "")).await, "ok");

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, "heidi");

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();
    client.put(&write, &key, &[as_bin!("n", 2)]).await.unwrap();
    assert!(client.delete(&write, &key).await.unwrap());

    let mut writes = 0;
    let mut deletes = 0;

    for _ in 0..50 {
        writes = counter(&row(&client, "after-write", set_name, "note_write").await, "fired");
        deletes = counter(&row(&client, "after-delete", set_name, "note_delete").await, "fired");

        if writes >= 2 && deletes >= 1 {
            break;
        }

        aerospike_rt::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(writes, 2, "one after-write per write");
    assert_eq!(deletes, 1, "one after-delete per delete");

    let r = row(&client, "after-write", set_name, "note_write").await;
    assert_eq!(counter(&r, "policy"), 2, "both ran to completion: {r}");
    assert_eq!(counter(&r, "dropped"), 0, "{r}");

    assert_eq!(info(&client, &remove_command("after-write", namespace, set_name)).await, "ok");
    assert_eq!(info(&client, &remove_command("after-delete", namespace, set_name)).await, "ok");
    remove_module(&client, package).await;
}

/// `timer`: a handler bound to the set runs on a record when a timer on it comes due, as a record UDF,
/// and can re-arm itself. The first timer is set through `event-timer-set`.
#[aerospike_macro::test]
async fn timers_run_the_handler_on_the_record() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("guard_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("timers_run_the_handler_on_the_record", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, GUARD, package).await {
        skipped("timers_run_the_handler_on_the_record", &reason);
        return;
    }

    assert_eq!(info(&client, &set_command("timer", namespace, set_name, package, "tick", "")).await, "ok");

    let write = WritePolicy::default();
    let key = as_key!(namespace, set_name, "ivan");

    client.put(&write, &key, &[as_bin!("ticks", 0)]).await.unwrap();

    let set = info(&client, &format!("event-timer-set:namespace={namespace};set={set_name};key=ivan;name=tick;in=200")).await;
    assert_eq!(set, "ok", "{set}");

    let listed = info(&client, "event-timer-list").await;
    assert!(listed.contains(&format!("namespace={namespace}:set={set_name}:")) && listed.contains(":name=tick:due="), "{listed}");

    // Three ticks, 200 ms apart, the handler re-arming itself twice.
    let mut ticks = 0;

    for _ in 0..50 {
        aerospike_rt::sleep(std::time::Duration::from_millis(200)).await;
        ticks = match record_bins(&client, &key).await.get("ticks") {
            Some(Value::Int(n)) => *n,
            _ => 0,
        };

        if ticks >= 3 {
            break;
        }
    }

    assert_eq!(ticks, 3, "the handler ran three times");
    assert!(!info(&client, "event-timer-list").await.contains(&format!(":set={set_name}:")), "no timer left");
    assert_eq!(counter(&row(&client, "timer", set_name, "tick").await, "fired"), 3);

    // A timer can be removed before it fires.
    assert_eq!(info(&client, &format!("event-timer-set:namespace={namespace};set={set_name};key=ivan;name=later;in=60000")).await, "ok");
    let listed = info(&client, "event-timer-list").await;
    let digest = listed
        .split(';')
        .find(|r| r.contains(":name=later:"))
        .and_then(|r| r.split(':').find_map(|f| f.strip_prefix("digest=")))
        .map(str::to_string)
        .unwrap_or_else(|| panic!("the timer should be listed: {listed}"));
    assert_eq!(info(&client, &format!("event-timer-remove:namespace={namespace};digest={digest};name=later")).await, "ok");
    assert!(!info(&client, "event-timer-list").await.contains(":name=later:"));

    assert_eq!(info(&client, &remove_command("timer", namespace, set_name)).await, "ok");
    remove_module(&client, package).await;
}

/// The write-path handlers written against the C and Go SDKs behave as the Rust ones: the generic event
/// bindings of all three languages, exercised on before-write and on a timer.
#[aerospike_macro::test]
async fn guards_in_three_languages_agree() {
    let client = common::client().await;
    let namespace = common::namespace();
    let tag = common::rand_str(6);

    if !supported(&client).await {
        skipped("guards_in_three_languages_agree", "no event commands on this server");
        return;
    }

    for (lang, module) in [("rust", GUARD), ("c", GUARD_C), ("go", GUARD_GO), ("java", GUARD_JAVA), ("cpp", GUARD_CPP)] {
        let set_name = &format!("g{lang}_{tag}");
        let package = &format!("guard_{lang}_{tag}");

        if let Err(reason) = register(&client, module, package).await {
            skipped("guards_in_three_languages_agree", &reason);
            return;
        }

        assert_eq!(info(&client, &set_command("before-write", namespace, set_name, package, "guard_account", "")).await, "ok", "{lang}");
        assert_eq!(info(&client, &set_command("timer", namespace, set_name, package, "tick", "")).await, "ok", "{lang}");

        let write = WritePolicy::default();
        let key = as_key!(namespace, set_name, "judy");

        let e = client.put(&write, &key, &[as_bin!("balance", -5)]).await.expect_err("rejected");
        assert_eq!(e.result_code(), 22, "{lang}: {e:?}");

        client.put(&write, &key, &[as_bin!("balance", 10), as_bin!("ticks", 0)]).await.unwrap();
        let bins = record_bins(&client, &key).await;
        assert_eq!(bins.get("stamped"), Some(&Value::from("client")), "{lang}: {bins:?}");
        assert_eq!(bins.get("writes"), Some(&Value::from(1)), "{lang}: {bins:?}");
        assert_eq!(bins.get("balance"), Some(&Value::from(10)), "{lang}: {bins:?}");

        assert_eq!(info(&client, &format!("event-timer-set:namespace={namespace};set={set_name};key=judy;name=tick;in=100")).await, "ok");

        let mut ticks = 0;

        for _ in 0..50 {
            aerospike_rt::sleep(std::time::Duration::from_millis(200)).await;
            ticks = match record_bins(&client, &key).await.get("ticks") {
                Some(Value::Int(n)) => *n,
                _ => 0,
            };

            if ticks >= 3 {
                break;
            }
        }

        assert_eq!(ticks, 3, "{lang}: the timer handler ran three times");

        assert_eq!(info(&client, &remove_command("before-write", namespace, set_name)).await, "ok");
        assert_eq!(info(&client, &remove_command("timer", namespace, set_name)).await, "ok");
        remove_module(&client, package).await;
    }
}

/// A device entry, as the application stores it.
fn device(id: &str, version: i64, extra: &[(&str, Value)]) -> Value {
    let mut m = HashMap::from([(Value::from("deviceId"), Value::from(id)), (Value::from("version"), Value::from(version))]);

    for (k, v) in extra {
        m.insert(Value::from(*k), v.clone());
    }

    Value::from(m)
}

fn devices(entries: &[Value]) -> Value {
    Value::List(entries.to_vec())
}

fn marker(off_at: i64, version: i64) -> Value {
    Value::from(HashMap::from([(Value::from("offAt"), Value::from(off_at)), (Value::from("version"), Value::from(version))]))
}

fn removed(markers: &[(&str, Value)], purged_before: i64, floor: Option<i64>) -> Value {
    let mut m = HashMap::from([
        (Value::from("markers"), Value::from(markers.iter().map(|(k, v)| (Value::from(*k), v.clone())).collect::<HashMap<_, _>>())),
        (Value::from("purgedBefore"), Value::from(purged_before)),
    ]);

    if let Some(f) = floor {
        m.insert(Value::from("versionFloor"), Value::from(f));
    }

    Value::from(m)
}

fn map_of(v: &Value) -> HashMap<Value, Value> {
    match v {
        Value::HashMap(m) => m.clone(),
        Value::SortedMap(m) => m.clone().into_iter().collect(),
        Value::OrderedMap(m) => m.clone().into_iter().collect(),
        other => panic!("not a map: {other:?}"),
    }
}

/// The merged devices as (id, version) pairs, in stored order.
fn merged_devices(bins: &HashMap<Value, Value>) -> Vec<(String, i64)> {
    let Some(Value::List(list)) = bins.get(&Value::from("devices")) else { return Vec::new() };

    list.iter()
        .map(|e| {
            let m = map_of(e);
            let id = match m.get(&Value::from("deviceId")) { Some(Value::String(s)) => s.clone(), other => panic!("{other:?}") };
            let v = match m.get(&Value::from("version")) { Some(Value::Int(n)) => *n, other => panic!("{other:?}") };
            (id, v)
        })
        .collect()
}

fn merged_markers(bins: &HashMap<Value, Value>) -> Vec<(String, i64)> {
    let Some(r) = bins.get(&Value::from("devicesRemoved")) else { return Vec::new() };
    let r = map_of(r);
    let Some(markers) = r.get(&Value::from("markers")) else { return Vec::new() };
    let mut out: Vec<(String, i64)> = map_of(markers)
        .iter()
        .map(|(k, v)| {
            let id = match k { Value::String(s) => s.clone(), other => panic!("{other:?}") };
            let ver = match map_of(v).get(&Value::from("version")) { Some(Value::Int(n)) => *n, other => panic!("{other:?}") };
            (id, ver)
        })
        .collect();
    out.sort();
    out
}

/// The record as DC1 holds it: devices A (v101) and B (v201), written through the module's UDFs.
async fn seed_subscriber(client: &Client, key: &Key, package: &str) {
    let write = WritePolicy::default();
    let changes = Value::from(HashMap::from([(Value::from("state"), Value::from("ACTIVE"))]));
    let _ = client.delete(&write, key).await;

    for _ in 0..101 {
        client.execute_udf(&write, key, package, "update_device", &[Value::from("device-A"), changes.clone()]).await.unwrap();
    }

    for _ in 0..201 {
        client.execute_udf(&write, key, package, "update_device", &[Value::from("device-B"), changes.clone()]).await.unwrap();
    }
}

/// The dry run of the CRDT merge, with `other` for the associativity check; asserts the three laws.
async fn crdt_merge(client: &Client, key: &Key, incoming: HashMap<Value, Value>, other: Option<HashMap<Value, Value>>, scenario: &str) -> HashMap<Value, Value> {
    let mut args = vec![Value::from(incoming), Value::from(meta(1, 0, 0, 2))];

    if let Some(o) = other {
        args.push(Value::from(o));
    }

    let result = client
        .execute_udf(&WritePolicy::default(), key, "__event__", "xdr-merge", &args)
        .await
        .unwrap_or_else(|e| panic!("{scenario}: {e:?}"));
    let answer = match result {
        Some(v) => map_of(&v),
        None => panic!("{scenario}: no answer"),
    };

    assert_eq!(answer.get(&Value::from("symmetric")), Some(&Value::from(true)), "{scenario}: commutative: {answer:?}");
    assert_eq!(answer.get(&Value::from("idempotent")), Some(&Value::from(true)), "{scenario}: idempotent: {answer:?}");

    if args.len() == 3 {
        assert_eq!(answer.get(&Value::from("associative")), Some(&Value::from(true)), "{scenario}: associative: {answer:?}");
    }

    bins_of(&answer)
}

/// The design note's active-active CRDT, its eight scenarios and one of our own driven through the server's dry run, which
/// checks commutativity, idempotence and associativity on the real bytes of every merge. The local
/// record is written by the module's own write UDFs; the incoming one is what the other data centre
/// would ship.
#[aerospike_macro::test]
async fn crdt_merge_converges_in_the_eight_scenarios() {
    let client = common::client().await;

    if !supported(&client).await {
        skipped("crdt_merge_converges_in_the_eight_scenarios", "no event commands on this server");
        return;
    }

    // The Rust module, the Java one and the Go one, rule for rule the same, through the same scenarios.
    for (lang, module) in [("rust", CRDT), ("java", CRDT_JAVA), ("go", CRDT_GO), ("cpp", CRDT_CPP)] {
        if !crdt_scenarios(&client, module, lang).await {
            return;
        }
    }
}

/// The grow-only counter, the smallest CRDT: each site adds to its own entry of a `counts` map and the merge
/// keeps the larger count per site, so the three laws hold by construction. Through the dry run, for the
/// Rust module, the Java one and the Go one.
#[aerospike_macro::test]
async fn counter_merge_converges() {
    let client = common::client().await;
    let namespace = common::namespace();

    if !supported(&client).await {
        skipped("counter_merge_converges", "no event commands on this server");
        return;
    }

    for (lang, module) in [("rust", COUNTER), ("java", COUNTER_JAVA), ("go", COUNTER_GO), ("cpp", COUNTER_CPP)] {
        let set_name = &common::rand_str(10);
        let package = &format!("counter_{lang}_{}", common::rand_str(6));

        if let Err(reason) = register(&client, module, package).await {
            skipped("counter_merge_converges", &reason);
            return;
        }

        assert_eq!(info(&client, &set_command("xdr-merge", namespace, set_name, package, "merge_counter", ";on-failure=abandon")).await, "ok");

        let write = WritePolicy::default();
        let key = as_key!(namespace, set_name, "hits");
        let counts = |pairs: &[(&str, i64)]| -> HashMap<Value, Value> {
            bins_map(&[("counts", Value::from(pairs.iter().map(|(k, v)| (Value::from(*k), Value::from(*v))).collect::<HashMap<_, _>>()))])
        };
        let merged_counts = |bins: &HashMap<Value, Value>| -> Vec<(String, i64)> {
            let mut out: Vec<(String, i64)> = map_of(bins.get(&Value::from("counts")).expect("counts"))
                .iter()
                .map(|(k, v)| match (k, v) {
                    (Value::String(s), Value::Int(n)) => (s.clone(), *n),
                    other => panic!("{lang}: {other:?}"),
                })
                .collect();
            out.sort();
            out
        };

        // This site counted three; the other site's copy of this site is stale (1) and it counted five itself.
        assert_eq!(client.execute_udf(&write, &key, package, "increment", &[Value::from("dc1"), Value::from(3)]).await.unwrap(), Some(Value::Int(3)), "{lang}");
        assert_eq!(client.execute_udf(&write, &key, package, "value", &[]).await.unwrap(), Some(Value::Int(3)), "{lang}");

        let bins = crdt_merge(&client, &key, counts(&[("dc1", 1), ("dc2", 5)]), None, "counter: concurrent").await;
        assert_eq!(merged_counts(&bins), vec![("dc1".into(), 3), ("dc2".into(), 5)], "{lang}");

        // A third site: the dry run checks associativity with it (asserted inside crdt_merge) and still
        // answers the merge of current and incoming.
        let bins = crdt_merge(&client, &key, counts(&[("dc1", 1), ("dc2", 5)]), Some(counts(&[("dc1", 4), ("dc3", 2)])), "counter: three sites").await;
        assert_eq!(merged_counts(&bins), vec![("dc1".into(), 3), ("dc2".into(), 5)], "{lang}");

        assert_eq!(info(&client, &remove_command("xdr-merge", namespace, set_name)).await, "ok");
        let _ = client.delete(&write, &key).await;
        remove_module(&client, package).await;
    }
}

/// The PN-Counter: two grow-only maps, `inc` and `dec`, the merge the counter's over each, the value
/// their difference. Through the dry run, for the Rust, Java, Go and C++ modules: a negative amount is
/// refused, the value goes below zero without complaint, and the three laws hold on both maps at once.
#[aerospike_macro::test]
async fn pncounter_merge_converges() {
    let client = common::client().await;
    let namespace = common::namespace();

    if !supported(&client).await {
        skipped("pncounter_merge_converges", "no event commands on this server");
        return;
    }

    for (lang, module) in [("rust", PNCOUNTER), ("java", PNCOUNTER_JAVA), ("go", PNCOUNTER_GO), ("cpp", PNCOUNTER_CPP)] {
        let set_name = &common::rand_str(10);
        let package = &format!("pncounter_{lang}_{}", common::rand_str(6));

        if let Err(reason) = register(&client, module, package).await {
            skipped("pncounter_merge_converges", &reason);
            return;
        }

        assert_eq!(info(&client, &set_command("xdr-merge", namespace, set_name, package, "merge_pncounter", ";on-failure=abandon")).await, "ok");

        let write = WritePolicy::default();
        let key = as_key!(namespace, set_name, "stock");
        let amounts = |pairs: &[(&str, i64)]| -> Value {
            Value::from(pairs.iter().map(|(k, v)| (Value::from(*k), Value::from(*v))).collect::<HashMap<_, _>>())
        };
        let record = |inc: &[(&str, i64)], dec: &[(&str, i64)]| -> HashMap<Value, Value> { bins_map(&[("inc", amounts(inc)), ("dec", amounts(dec))]) };
        let merged = |bins: &HashMap<Value, Value>, bin: &str| -> Vec<(String, i64)> {
            let mut out: Vec<(String, i64)> = map_of(bins.get(&Value::from(bin)).expect(bin))
                .iter()
                .map(|(k, v)| match (k, v) {
                    (Value::String(s), Value::Int(n)) => (s.clone(), *n),
                    other => panic!("{lang}: {other:?}"),
                })
                .collect();
            out.sort();
            out
        };

        // This site added three and took one; an entry only grows, so a negative amount is refused.
        assert_eq!(client.execute_udf(&write, &key, package, "increment", &[Value::from("dc1"), Value::from(3)]).await.unwrap(), Some(Value::Int(3)), "{lang}");
        assert_eq!(client.execute_udf(&write, &key, package, "decrement", &[Value::from("dc1"), Value::from(1)]).await.unwrap(), Some(Value::Int(2)), "{lang}");
        assert_eq!(client.execute_udf(&write, &key, package, "value", &[]).await.unwrap(), Some(Value::Int(2)), "{lang}");

        let err = client.execute_udf(&write, &key, package, "increment", &[Value::from("dc1"), Value::from(-2)]).await.unwrap_err();
        assert!(err.to_string().contains("must not be negative"), "{lang}: {err:?}");
        assert_eq!(client.execute_udf(&write, &key, package, "value", &[]).await.unwrap(), Some(Value::Int(2)), "{lang}: a refused write changes nothing");

        // The other site's copy of this site is stale (1 added, nothing taken); it added five and took two
        // itself. Each map merges on its own, so the merged value is 8 - 3.
        let bins = crdt_merge(&client, &key, record(&[("dc1", 1), ("dc2", 5)], &[("dc2", 2)]), None, "pncounter: concurrent").await;
        assert_eq!(merged(&bins, "inc"), vec![("dc1".into(), 3), ("dc2".into(), 5)], "{lang}");
        assert_eq!(merged(&bins, "dec"), vec![("dc1".into(), 1), ("dc2".into(), 2)], "{lang}");

        // A third site, for associativity.
        let bins = crdt_merge(
            &client,
            &key,
            record(&[("dc1", 1), ("dc2", 5)], &[("dc2", 2)]),
            Some(record(&[("dc1", 4), ("dc3", 2)], &[("dc3", 1)])),
            "pncounter: three sites",
        )
        .await;
        assert_eq!(merged(&bins, "inc"), vec![("dc1".into(), 3), ("dc2".into(), 5)], "{lang}");
        assert_eq!(merged(&bins, "dec"), vec![("dc1".into(), 1), ("dc2".into(), 2)], "{lang}");

        // No floor: taking more than was ever added is a legitimate state, not an error.
        assert_eq!(client.execute_udf(&write, &key, package, "decrement", &[Value::from("dc1"), Value::from(5)]).await.unwrap(), Some(Value::Int(-3)), "{lang}");

        assert_eq!(info(&client, &remove_command("xdr-merge", namespace, set_name)).await, "ok");
        let _ = client.delete(&write, &key).await;
        remove_module(&client, package).await;
    }
}

/// The strings of a list value, in its order.
fn strings_of(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::List(items)) => items
            .iter()
            .map(|i| match i {
                Value::String(s) => s.clone(),
                other => panic!("{other:?}"),
            })
            .collect(),
        other => panic!("not a list: {other:?}"),
    }
}

/// The 2P-Set: two grow-only lists, `added` and `removed`, the merge the union of each, a member when
/// added and not removed. Through the dry run, for the Rust, Java, Go and C++ modules: the lists stay
/// sorted, a removal is forever (the re-add is a no-op), removing a non-member is refused, and the three
/// laws hold on both lists at once.
#[aerospike_macro::test]
async fn twopset_merge_converges() {
    let client = common::client().await;
    let namespace = common::namespace();

    if !supported(&client).await {
        skipped("twopset_merge_converges", "no event commands on this server");
        return;
    }

    for (lang, module) in [("rust", TWOPSET), ("java", TWOPSET_JAVA), ("go", TWOPSET_GO), ("cpp", TWOPSET_CPP)] {
        let set_name = &common::rand_str(10);
        let package = &format!("twopset_{lang}_{}", common::rand_str(6));

        if let Err(reason) = register(&client, module, package).await {
            skipped("twopset_merge_converges", &reason);
            return;
        }

        assert_eq!(info(&client, &set_command("xdr-merge", namespace, set_name, package, "merge_twopset", ";on-failure=abandon")).await, "ok");

        let write = WritePolicy::default();
        let key = as_key!(namespace, set_name, "post");
        let record = |added: &[&str], removed: &[&str]| -> HashMap<Value, Value> { bins_map(&[("added", strings(added)), ("removed", strings(removed))]) };
        let members = |v: Option<Value>| strings_of(v.as_ref());

        // Adds come out sorted whatever order they go in; a second add of the same tag changes nothing.
        assert_eq!(members(client.execute_udf(&write, &key, package, "add", &[Value::from("news")]).await.unwrap()), ["news"], "{lang}");
        assert_eq!(members(client.execute_udf(&write, &key, package, "add", &[Value::from("art")]).await.unwrap()), ["art", "news"], "{lang}");
        assert_eq!(members(client.execute_udf(&write, &key, package, "add", &[Value::from("art")]).await.unwrap()), ["art", "news"], "{lang}");

        // A removal is forever: adding the tag back is a no-op, since `added` still has it.
        assert_eq!(members(client.execute_udf(&write, &key, package, "remove", &[Value::from("news")]).await.unwrap()), ["art"], "{lang}");
        assert_eq!(members(client.execute_udf(&write, &key, package, "add", &[Value::from("news")]).await.unwrap()), ["art"], "{lang}");
        assert_eq!(members(client.execute_udf(&write, &key, package, "members", &[]).await.unwrap()), ["art"], "{lang}");

        for tag in ["news", "nothing"] {
            let err = client.execute_udf(&write, &key, package, "remove", &[Value::from(tag)]).await.unwrap_err();
            assert!(err.to_string().contains("not a member"), "{lang}: {err:?}");
        }

        // The other side added `art` and `sport`, and removed `sport`; this side has `art` and `news`
        // with `news` removed. Each list merges by union.
        let bins = crdt_merge(&client, &key, record(&["art", "sport"], &["sport"]), None, "twopset: concurrent").await;
        assert_eq!(strings_of(bins.get(&Value::from("added"))), ["art", "news", "sport"], "{lang}");
        assert_eq!(strings_of(bins.get(&Value::from("removed"))), ["news", "sport"], "{lang}");

        // A third site, for associativity: it has `art` removed, so after the three merge only `art`'s
        // tombstone decides, on every site.
        let bins = crdt_merge(&client, &key, record(&["art", "sport"], &["sport"]), Some(record(&["art", "music"], &["art"])), "twopset: three sites").await;
        assert_eq!(strings_of(bins.get(&Value::from("added"))), ["art", "news", "sport"], "{lang}");
        assert_eq!(strings_of(bins.get(&Value::from("removed"))), ["news", "sport"], "{lang}");

        assert_eq!(info(&client, &remove_command("xdr-merge", namespace, set_name)).await, "ok");
        let _ = client.delete(&write, &key).await;
        remove_module(&client, package).await;
    }
}

/// The LWW-Element-Set: a stamp per tag in `addedAt` and `removedAt`, the merge the later stamp per tag of
/// each map, a member when its add is later than its removal, a tie to the removal. Through the dry run,
/// for the Rust, Java, Go and C++ modules: a removed tag comes back with a later add (the 2P-Set's limit
/// lifted), a tie written straight into the record resolves to the removal, the merge keeps the later
/// stamps, and the three laws hold on both maps at once.
#[aerospike_macro::test]
async fn lwwset_merge_converges() {
    let client = common::client().await;
    let namespace = common::namespace();

    if !supported(&client).await {
        skipped("lwwset_merge_converges", "no event commands on this server");
        return;
    }

    for (lang, module) in [("rust", LWWSET), ("java", LWWSET_JAVA), ("go", LWWSET_GO), ("cpp", LWWSET_CPP)] {
        let set_name = &common::rand_str(10);
        let package = &format!("lwwset_{lang}_{}", common::rand_str(6));

        if let Err(reason) = register(&client, module, package).await {
            skipped("lwwset_merge_converges", &reason);
            return;
        }

        assert_eq!(info(&client, &set_command("xdr-merge", namespace, set_name, package, "merge_lwwset", ";on-failure=abandon")).await, "ok");

        let write = WritePolicy::default();
        let key = as_key!(namespace, set_name, "post");
        let stamps = |pairs: &[(&str, i64)]| -> Value { Value::from(pairs.iter().map(|(k, v)| (Value::from(*k), Value::from(*v))).collect::<HashMap<_, _>>()) };
        let record = |added: &[(&str, i64)], removed: &[(&str, i64)]| -> HashMap<Value, Value> { bins_map(&[("addedAt", stamps(added)), ("removedAt", stamps(removed))]) };
        let stamps_of = |bins: &HashMap<Value, Value>, bin: &str| -> Vec<(String, i64)> {
            let mut out: Vec<(String, i64)> = map_of(bins.get(&Value::from(bin)).expect(bin))
                .iter()
                .map(|(k, v)| match (k, v) {
                    (Value::String(s), Value::Int(n)) => (s.clone(), *n),
                    other => panic!("{lang}: {other:?}"),
                })
                .collect();
            out.sort();
            out
        };
        let members = |v: Option<Value>| strings_of(v.as_ref());

        // Add, remove, add back: the later stamp wins, so the tag returns.
        assert_eq!(members(client.execute_udf(&write, &key, package, "add", &[Value::from("news")]).await.unwrap()), ["news"], "{lang}");
        assert_eq!(members(client.execute_udf(&write, &key, package, "add", &[Value::from("art")]).await.unwrap()), ["art", "news"], "{lang}");
        assert_eq!(members(client.execute_udf(&write, &key, package, "remove", &[Value::from("news")]).await.unwrap()), ["art"], "{lang}");
        assert_eq!(members(client.execute_udf(&write, &key, package, "add", &[Value::from("news")]).await.unwrap()), ["art", "news"], "{lang}");

        let err = client.execute_udf(&write, &key, package, "remove", &[Value::from("nothing")]).await.unwrap_err();
        assert!(err.to_string().contains("not a member"), "{lang}: {err:?}");

        // The stamps are the server's clock, so every one is past the epoch, and the re-add is later than
        // the removal.
        let rec = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();
        let added = stamps_of(&rec.bins.iter().map(|(k, v)| (Value::from(k.as_str()), v.clone())).collect(), "addedAt");
        let removed = stamps_of(&rec.bins.iter().map(|(k, v)| (Value::from(k.as_str()), v.clone())).collect(), "removedAt");
        assert_eq!(added.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>(), ["art", "news"], "{lang}");
        assert_eq!(removed.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>(), ["news"], "{lang}");
        assert!(added[1].1 > removed[0].1 && removed[0].1 > 1_600_000_000_000, "{lang}: {added:?} {removed:?}");
        let news_added = added[1].1;

        // The other side added `sport` long ago and removed `art` just after this side added it; its
        // copy of `news` is stale. Each map merges by the later stamp.
        let art_added = added[0].1;
        let bins = crdt_merge(&client, &key, record(&[("art", art_added), ("news", 1), ("sport", 1_700_000_000_000)], &[("art", art_added + 1)]), None, "lwwset: concurrent").await;
        assert_eq!(stamps_of(&bins, "addedAt"), vec![("art".into(), art_added), ("news".into(), news_added), ("sport".into(), 1_700_000_000_000)], "{lang}");
        assert_eq!(stamps_of(&bins, "removedAt"), vec![("art".into(), art_added + 1), ("news".into(), removed[0].1)], "{lang}");

        // A third site, for associativity.
        let bins = crdt_merge(
            &client,
            &key,
            record(&[("art", art_added), ("news", 1), ("sport", 1_700_000_000_000)], &[("art", art_added + 1)]),
            Some(record(&[("music", 1_700_000_000_001)], &[("sport", 1_700_000_000_002)])),
            "lwwset: three sites",
        )
        .await;
        assert_eq!(stamps_of(&bins, "addedAt"), vec![("art".into(), art_added), ("news".into(), news_added), ("sport".into(), 1_700_000_000_000)], "{lang}");

        // The bias: a tag added and removed at one stamp is not a member, and a fresh add, later by the
        // clock, brings it back.
        client.put(&write, &key, &[as_bin!("addedAt", stamps(&[("tie", 50)])), as_bin!("removedAt", stamps(&[("tie", 50)]))]).await.unwrap();
        assert_eq!(members(client.execute_udf(&write, &key, package, "members", &[]).await.unwrap()), Vec::<String>::new(), "{lang}");
        assert_eq!(members(client.execute_udf(&write, &key, package, "add", &[Value::from("tie")]).await.unwrap()), ["tie"], "{lang}");

        assert_eq!(info(&client, &remove_command("xdr-merge", namespace, set_name)).await, "ok");
        let _ = client.delete(&write, &key).await;
        remove_module(&client, package).await;
    }
}

/// The nine scenarios against one CRDT module; false when the module could not be registered (skipped).
async fn crdt_scenarios(client: &Client, module: &[u8], lang: &str) -> bool {
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("crdt_{lang}_{}", common::rand_str(6));

    if let Err(reason) = register(client, module, package).await {
        skipped("crdt_merge_converges_in_the_eight_scenarios", &reason);
        return false;
    }

    eprintln!("crdt scenarios: {lang}");

    assert_eq!(info(client, &set_command("xdr-merge", namespace, set_name, package, "xdr_merge", ";on-failure=abandon")).await, "ok");

    let write = WritePolicy::default();
    let changes = |pairs: &[(&str, &str)]| -> Value {
        Value::from(pairs.iter().map(|(k, v)| (Value::from(*k), Value::from(*v))).collect::<HashMap<_, _>>())
    };
    let week: i64 = 7 * 24 * 60 * 60 * 1000;
    let t0: i64 = 1_790_000_000_000;

    // The record as DC1 holds it: devices A (v101) and B (v201), written through the UDFs.
    let key = as_key!(namespace, set_name, "1001");

    // 1. Concurrent updates to different devices: DC1 raised A to 102, DC2 raised B to 202; both survive.
    seed_subscriber(client, &key, package).await;
    client.execute_udf(&write, &key, package, "update_device", &[Value::from("device-A"), changes(&[("state", "SUSPENDED")])]).await.unwrap();
    let bins = crdt_merge(client, &key, bins_map(&[("devices", devices(&[device("device-A", 101, &[]), device("device-B", 202, &[("name", Value::from("phone"))])]))]), None, "1 concurrent").await;
    assert_eq!(merged_devices(&bins), vec![("device-A".into(), 102), ("device-B".into(), 202)]);

    // 2. A tie: both sides changed A at version 102 differently; the bytes decide, the same on both sides.
    seed_subscriber(client, &key, package).await;
    client.execute_udf(&write, &key, package, "update_device", &[Value::from("device-A"), changes(&[("state", "SUSPENDED")])]).await.unwrap();
    let bins = crdt_merge(client, &key, bins_map(&[("devices", devices(&[device("device-A", 102, &[("name", Value::from("phone"))]), device("device-B", 201, &[])]))]), None, "2 tie").await;
    assert_eq!(merged_devices(&bins), vec![("device-A".into(), 102), ("device-B".into(), 201)]);

    // 3. Adds on both sides: C here, D there; the merge holds all four, sorted.
    seed_subscriber(client, &key, package).await;
    client.execute_udf(&write, &key, package, "update_device", &[Value::from("device-C"), changes(&[])]).await.unwrap();
    let bins = crdt_merge(client, &key, bins_map(&[("devices", devices(&[device("device-A", 101, &[]), device("device-B", 201, &[]), device("device-D", 1, &[])]))]), None, "3 adds").await;
    assert_eq!(merged_devices(&bins), vec![("device-A".into(), 101), ("device-B".into(), 201), ("device-C".into(), 1), ("device-D".into(), 1)]);

    // 4. A removal racing an update, both at version 102: the removal wins and leaves its marker.
    seed_subscriber(client, &key, package).await;
    client.execute_udf(&write, &key, package, "remove_device", &[Value::from("device-A"), Value::from(t0)]).await.unwrap();
    let bins = crdt_merge(client, &key, bins_map(&[("devices", devices(&[device("device-A", 102, &[("state", Value::from("SUSPENDED"))]), device("device-B", 201, &[])]))]), None, "4 removal vs update").await;
    assert_eq!(merged_devices(&bins), vec![("device-B".into(), 201)]);
    assert_eq!(merged_markers(&bins), vec![("device-A".into(), 102)]);

    // 5. A re-add: the other side removed A at 102, this side added it back at 103; the live entry wins.
    seed_subscriber(client, &key, package).await;
    client.execute_udf(&write, &key, package, "remove_device", &[Value::from("device-A"), Value::from(t0)]).await.unwrap();
    client.execute_udf(&write, &key, package, "update_device", &[Value::from("device-A"), changes(&[("state", "ACTIVE")])]).await.unwrap();
    let bins = crdt_merge(client, &key, bins_map(&[
        ("devices", devices(&[device("device-B", 201, &[])])),
        ("devicesRemoved", removed(&[("device-A", marker(t0, 102))], 0, None)),
    ]), None, "5 re-add").await;
    assert_eq!(merged_devices(&bins), vec![("device-A".into(), 103), ("device-B".into(), 201)]);
    assert_eq!(merged_markers(&bins), Vec::<(String, i64)>::new());

    // 6. A purge: a marker older than the purge age is dropped by both sides and its version becomes the
    //    floor, which the merge keeps as the higher of the two.
    seed_subscriber(client, &key, package).await;
    client.execute_udf(&write, &key, package, "remove_device", &[Value::from("device-A"), Value::from(t0)]).await.unwrap();
    client.execute_udf(&write, &key, package, "purge_devices", &[Value::from(t0 + week + 1)]).await.unwrap();
    let bins = crdt_merge(client, &key, bins_map(&[
        ("devices", devices(&[device("device-B", 201, &[])])),
        ("devicesRemoved", removed(&[("device-A", marker(t0, 102))], 0, None)),
    ]), None, "6 purge").await;
    assert_eq!(merged_devices(&bins), vec![("device-B".into(), 201)]);
    assert_eq!(merged_markers(&bins), Vec::<(String, i64)>::new(), "the stale marker from the other side is dropped by the horizon");
    assert_eq!(map_of(bins.get(&Value::from("devicesRemoved")).unwrap()).get(&Value::from("versionFloor")), Some(&Value::from(102)));

    // 7. A record written before this model: no devicesRemoved bin and an unsorted list on the incoming
    //    side; the merge sorts and the laws still hold.
    seed_subscriber(client, &key, package).await;
    let bins = crdt_merge(client, &key, bins_map(&[("devices", devices(&[device("device-B", 201, &[]), device("device-A", 101, &[])]))]), None, "7 pre-model").await;
    assert_eq!(merged_devices(&bins), vec![("device-A".into(), 101), ("device-B".into(), 201)]);

    // 8. A device written again right after a purge: the floor makes the new version 103, which beats
    //    an old copy of the marker (102) still on its way from the other side.
    seed_subscriber(client, &key, package).await;
    client.execute_udf(&write, &key, package, "remove_device", &[Value::from("device-A"), Value::from(t0)]).await.unwrap();
    client.execute_udf(&write, &key, package, "purge_devices", &[Value::from(t0 + week + 1)]).await.unwrap();
    client.execute_udf(&write, &key, package, "update_device", &[Value::from("device-A"), changes(&[("state", "ACTIVE")])]).await.unwrap();
    let bins = crdt_merge(client, &key, bins_map(&[
        ("devices", devices(&[device("device-B", 201, &[])])),
        ("devicesRemoved", removed(&[("device-A", marker(t0, 102))], 0, None)),
    ]), Some(bins_map(&[("devices", devices(&[device("device-A", 101, &[]), device("device-B", 201, &[]), device("device-E", 1, &[])]))])), "8 write after purge").await;
    assert_eq!(merged_devices(&bins), vec![("device-A".into(), 103), ("device-B".into(), 201)]);

    // 9. The other side purged first: its horizon drops the marker this side still holds, and the merge
    //    must carry the marker's version into the floor, or a re-add here could start below a copy of the
    //    marker still in flight.
    seed_subscriber(client, &key, package).await;
    client.execute_udf(&write, &key, package, "remove_device", &[Value::from("device-A"), Value::from(t0)]).await.unwrap();
    let bins = crdt_merge(client, &key, bins_map(&[
        ("devices", devices(&[device("device-B", 201, &[])])),
        ("devicesRemoved", removed(&[], t0 + 1, None)),
    ]), None, "9 purged elsewhere").await;
    assert_eq!(merged_devices(&bins), vec![("device-B".into(), 201)]);
    assert_eq!(merged_markers(&bins), Vec::<(String, i64)>::new(), "the horizon drops the marker");
    assert_eq!(map_of(bins.get(&Value::from("devicesRemoved")).unwrap()).get(&Value::from("versionFloor")), Some(&Value::from(102)), "but its version becomes the floor");

    assert_eq!(info(client, &remove_command("xdr-merge", namespace, set_name)).await, "ok");
    remove_module(client, package).await;

    true
}

/// The contract check at registration: a function that is not an event handler, or a module that is not
/// there, is refused with a reason.
#[aerospike_macro::test]
async fn event_set_refuses_what_is_not_a_handler() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("plain_{}", common::rand_str(6));

    if !supported(&client).await {
        skipped("event_set_refuses_what_is_not_a_handler", "no event commands on this server");
        return;
    }

    if let Err(reason) = register(&client, PLAIN, package).await {
        skipped("event_set_refuses_what_is_not_a_handler", &reason);
        return;
    }

    let cases = [
        (
            bind_command(namespace, set_name, package, "increment"),
            "a UDF that is not an event handler",
        ),
        (
            bind_command(namespace, set_name, package, "no_such_function"),
            "a function the module does not export",
        ),
        (
            bind_command(namespace, set_name, "no_such_module", "merge_account"),
            "a module that is not registered",
        ),
        (
            bind_command("no_such_namespace", set_name, package, "increment"),
            "a namespace that does not exist",
        ),
        (
            format!("event-set:event=no-such-event;namespace={namespace};module={package};function=increment"),
            "an event kind the server does not have",
        ),
    ];

    for (command, what) in cases {
        let answer = info(&client, &command).await;
        // The server's refusal format: `ERROR:<code>:<reason>`.
        assert!(answer.starts_with("ERROR:"), "{what} must be refused, got {answer:?} for {command}");
    }

    let listed = info(&client, "event-list").await;
    assert!(!listed.contains(package), "nothing was bound: {listed}");

    client
        .remove_udf(&AdminPolicy::default(), &format!("{package}.wasm"))
        .await
        .unwrap()
        .wait_till_complete(None)
        .await
        .unwrap();
}
