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

//! What the server does with an `xdr-merge` handler's answer, when the answer is not a good one.
//!
//! `tests/src/xdr_merge.rs` shows well-behaved merges converging. This is the other half: the handler is
//! code the server did not write, and its answer decides what is stored, so every answer a module can give
//! is input the server has to survive and act on predictably. The module is `merge_probe`
//! (`aerospike-udf-sdk/wasm-rust/examples/merge_probe`), which answers whatever the incoming record's `do` bin
//! names: a trap, a loop, a failure, a verdict that does not exist, a record whose bins are not bins, bin
//! names no bin can have, more bins than a record holds, a time to live out of range.
//!
//! Each answer is given under each `on-failure` policy, inside a real XDR write between two clusters
//! (`AEROSPIKE_XDR_DC1`, `AEROSPIKE_XDR_DC2`; without them every test here skips), and held to three
//! things: what the receiving cluster stores, that neither cluster stops, and that shipping is not left
//! wedged behind a write that can never succeed.

use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant};

use aerospike::*;

use crate::common;

const PROBE: &[u8] = include_bytes!("../fixtures/wasm/merge_probe.wasm");
const COUNTER: &[u8] = include_bytes!("../fixtures/wasm/counter.wasm");
const PNCOUNTER: &[u8] = include_bytes!("../fixtures/wasm/pncounter.wasm");
const TWOPSET: &[u8] = include_bytes!("../fixtures/wasm/twopset.wasm");

const SHIP_LAG: Duration = Duration::from_secs(30);

fn dcs() -> Option<(String, String)> {
    Some((env::var("AEROSPIKE_XDR_DC1").ok()?, env::var("AEROSPIKE_XDR_DC2").ok()?))
}

async fn clients(test: &str) -> Option<(Client, Client)> {
    let Some((dc1, dc2)) = dcs() else {
        eprintln!("skipped {test}: AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2 name no clusters");
        return None;
    };
    let policy = ClientPolicy::default();

    Some((Client::new(&policy, &dc1).await.unwrap(), Client::new(&policy, &dc2).await.unwrap()))
}

async fn info(client: &Client, command: &str) -> String {
    let answers = client.info(&AdminPolicy::default(), &[command]).await.unwrap();

    answers
        .get(command)
        .cloned()
        .unwrap_or_else(|| panic!("no answer to {command:?}: {answers:?}"))
}

async fn register(client: &Client, module: &[u8], package: &str) {
    let task = client
        .register_udf(&AdminPolicy::default(), module, &format!("{package}.wasm"), UdfLang::Wasm)
        .await
        .unwrap();

    task.wait_till_complete(None).await.unwrap();
}

async fn bind(client: &Client, set_name: &str, package: &str, function: &str, extra: &str) {
    let command = format!("event-set:event=xdr-merge;namespace=test;set={set_name};module={package};function={function}{extra}");

    assert_eq!(info(client, &command).await, "ok", "{command}");
}

async fn unbind(client: &Client, set_name: &str) -> String {
    info(client, &format!("event-remove:event=xdr-merge;namespace=test;set={set_name}")).await
}

/// One of a binding's counters, from `event-list`; 0 when the set has no binding.
async fn counter(client: &Client, set_name: &str, name: &str) -> u64 {
    let rows = info(client, "event-list").await;

    rows.split(';')
        .find(|r| r.contains(&format!(":set={set_name}:")))
        .and_then(|row| row.split(':').find_map(|f| f.strip_prefix(&format!("{name}="))))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// One of dc1's counters for what it ships to dc2.
async fn xdr_stat(client: &Client, name: &str) -> u64 {
    let stats = info(client, "get-stats:context=xdr;dc=dc2;namespace=test").await;

    stats
        .split(';')
        .find_map(|f| f.strip_prefix(&format!("{name}=")))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no {name} in {stats}"))
}

async fn bins(client: &Client, key: &Key) -> Option<HashMap<String, Value>> {
    // The suite's handlers spin to their deadline on purpose, and a merged record that goes back to the
    // other side runs its handler there too: a read can wait behind them longer than the default second.
    let mut policy = ReadPolicy::default();

    policy.base_policy.total_timeout = 5000;
    policy.base_policy.socket_timeout = 5000;

    match client.get(&policy, key, Bins::All).await {
        Ok(r) => Some(r.bins.into_iter().collect()),
        Err(e) if e.matches(&[ResultCode::KeyNotFoundError]) => None,
        Err(e) => panic!("{e:?}"),
    }
}

/// Poll until `check` holds for what `client` has under `key`, and answer what it has.
async fn wait_for<F>(client: &Client, key: &Key, lag: Duration, check: F) -> Option<HashMap<String, Value>>
where
    F: Fn(&Option<HashMap<String, Value>>) -> bool,
{
    let deadline = Instant::now() + lag;

    loop {
        let found = bins(client, key).await;

        if check(&found) || Instant::now() >= deadline {
            return found;
        }

        aerospike_rt::sleep(Duration::from_millis(100)).await;
    }
}

/// Both clusters answer, which is all "did not stop" can mean from here.
async fn alive(c1: &Client, c2: &Client, when: &str) {
    for (c, name) in [(c1, "dc1"), (c2, "dc2")] {
        let build = c.info(&AdminPolicy::default(), &["build"]).await;

        assert!(build.is_ok(), "{name} does not answer {when}: {build:?}");
    }
}

/// What the receiving cluster should hold once the handler has answered.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Want {
    /// The record as it was: the incoming write was dropped.
    Kept,
    /// The incoming write applied over the record, as with no handler at all.
    Taken,
    /// The handler's own record: exactly these bin names.
    Record(&'static [&'static str]),
    /// The handler failed, so the binding's `on-failure` decides: `policy` takes, and `abandon` keeps and
    /// the source counts a success. There is no third way: failing the write would not get it sent again.
    Failed,
    /// The handler answered `delete`: the record is removed on the receiving side.
    Gone,
}

/// Every answer the probe can give, the number it is given, and what should come of it.
const CASES: &[(&str, i64, Want)] = &[
    // No answer.
    ("trap", 0, Want::Failed),
    ("spin", 0, Want::Failed),
    ("fail", 0, Want::Failed),
    ("nonzero", 0, Want::Failed),
    ("silent", 0, Want::Failed),
    // An answer of the wrong shape.
    ("int", 0, Want::Failed),
    ("nil", 0, Want::Failed),
    ("list", 0, Want::Failed),
    ("m-empty", 0, Want::Failed),
    ("m-verdict-int", 0, Want::Failed),
    ("m-verdict-nul", 0, Want::Failed),
    ("banana", 0, Want::Failed),
    ("KEEP-LOCAL", 0, Want::Failed),
    ("record", 0, Want::Failed),
    ("r-nobins", 0, Want::Failed),
    ("r-bins-int", 0, Want::Failed),
    ("r-bins-list", 0, Want::Failed),
    ("r-bins-nil", 0, Want::Failed),
    // The three verdicts.
    ("policy", 0, Want::Taken),
    ("keep-local", 0, Want::Kept),
    ("take-remote", 0, Want::Taken),
    ("m-keep", 0, Want::Kept),
    ("m-take", 0, Want::Taken),
    ("m-take-ttl", 100, Want::Taken),
    ("delete", 0, Want::Gone),
    ("m-delete", 0, Want::Gone),
    // A record.
    ("r-ok", 7, Want::Record(&["merged"])),
    ("r-kinds", 0, Want::Record(&["i", "d", "t", "s", "e", "b", "g", "l", "m"])),
    ("r-15", 0, Want::Record(&["0123456789abcde"])),
    ("r-ttl", 100, Want::Record(&["merged"])),
    ("r-ttl", 0, Want::Record(&["merged"])),
    ("r-ttl", 0xffff_ffff, Want::Record(&["merged"])),
    ("r-deep", 30, Want::Record(&["merged"])),
    ("r-deep", 60, Want::Record(&["merged"])),
    ("r-big", 64, Want::Record(&["merged"])),
    ("r-many", 100, Want::Record(&[])),
    // A record that cannot be stored as it is given.
    ("r-ttl", -1, Want::Failed),
    ("r-ttl", 0x1_0000_0000, Want::Failed),
    ("r-ttl", i64::MIN, Want::Failed),
    ("m-take-ttl", -5, Want::Failed),
    ("r-ttl-str", 0, Want::Failed),
    ("r-long", 0, Want::Failed),
    ("r-nul", 0, Want::Failed),
    ("r-noname", 0, Want::Failed),
    ("r-intkey", 0, Want::Failed),
    ("r-many", 40000, Want::Failed),
    // Larger than the namespace stores as a record, and a document the server would not take as a write.
    ("r-big", 20000, Want::Failed),
    ("r-geo-bad", 0, Want::Failed),
    // A record of nothing is not a deletion: a merge that built nothing by mistake must not remove what
    // it merged. `delete` is how a handler says it.
    ("r-empty", 0, Want::Failed),
    ("r-nil", 0, Want::Failed),
];

fn describe(found: &Option<HashMap<String, Value>>) -> String {
    match found {
        None => "no record".into(),
        Some(b) => {
            let mut names: Vec<&String> = b.keys().collect();

            names.sort();

            if names.len() > 12 {
                format!("{} bins", names.len())
            } else {
                format!("{names:?}")
            }
        }
    }
}

/// What a record that holds `found` says the handler's answer came to.
fn outcome(found: &Option<HashMap<String, Value>>) -> &'static str {
    match found {
        None => "gone",
        Some(b) if b.contains_key("do") => "taken",
        Some(b) if b.len() == 1 && b.contains_key("base") => "kept",
        Some(_) => "record",
    }
}

/// Every answer, under one `on-failure` policy, inside real XDR writes from dc1 to dc2.
async fn answers_under(policy: &str) {
    let test = format!("answers_under({policy})");
    let Some((c1, c2)) = clients(&test).await else { return };
    let set_name = &format!("probe_{policy}_{}", common::rand_str(6));
    let package = &format!("merge_probe_{}", common::rand_str(6));
    let write = WritePolicy::default();

    register(&c2, PROBE, package).await;

    // The record exists on both sides before the handler is bound, so that making it is not a merge.
    let keys: Vec<Key> = (0..CASES.len()).map(|i| as_key!("test", set_name.as_str(), i as i64)).collect();

    for key in &keys {
        c2.put(&write, key, &[as_bin!("base", 1)]).await.unwrap();
    }

    for key in &keys {
        let there = wait_for(&c1, key, SHIP_LAG, |b| b.is_some()).await;

        assert!(there.is_some(), "the record did not ship to dc1 to begin with");
    }

    let abandoned_before = xdr_stat(&c1, "abandoned").await;

    // Short, so that the handler that never returns costs a fraction of a second a try.
    bind(&c2, set_name, package, "probe", &format!(";on-failure={policy};timeout=300")).await;

    for (i, (what, n, _)) in CASES.iter().enumerate() {
        c1.put(&write, &keys[i], &[as_bin!("do", *what), as_bin!("n", *n), as_bin!("stamp", i as i64)])
            .await
            .unwrap();
    }

    // Each answer is given once.
    let deadline = Instant::now() + SHIP_LAG;

    while counter(&c2, set_name, "fired").await < CASES.len() as u64 && Instant::now() < deadline {
        aerospike_rt::sleep(Duration::from_millis(200)).await;
    }

    // What a failed handler comes to under this policy.
    let failed = match policy {
        "policy" => "taken",
        _ => "kept",
    };
    let mut wrong = Vec::new();
    let mut table = String::new();

    for (i, (what, n, want)) in CASES.iter().enumerate() {
        let expected = match want {
            Want::Kept => "kept",
            Want::Taken => "taken",
            Want::Record(_) => "record",
            Want::Failed => failed,
            Want::Gone => "gone",
        };
        // A write that is still on its way reads as "kept"; give each the time to be anything else.
        let found = wait_for(&c2, &keys[i], Duration::from_secs(if expected == "kept" { 0 } else { 5 }), |b| {
            outcome(b) != "kept"
        })
        .await;
        let got = outcome(&found);
        let mut ok = got == expected;

        if let (Want::Record(names), Some(b)) = (want, &found) {
            ok = ok && (names.is_empty() || (b.len() == names.len() && names.iter().all(|n| b.contains_key(*n))));
        }

        table.push_str(&format!("  {policy:8} {what:14} {n:12} -> {got:7} {}\n", describe(&found)));

        if !ok {
            wrong.push(format!("{what}({n}): wanted {expected} ({want:?}), dc2 holds {}", describe(&found)));
        }
    }

    eprintln!("{table}  fired {} fallback {}", counter(&c2, set_name, "fired").await, counter(&c2, set_name, "fallback").await);
    alive(&c1, &c2, "after every answer").await;

    assert_eq!(unbind(&c2, set_name).await, "ok");

    // What the source was told: nothing it had to give up on. A kept record and an abandoned merge are
    // successes to it, and no answer a handler can give fails the write.
    let abandoned = xdr_stat(&c1, "abandoned").await - abandoned_before;

    if abandoned != 0 {
        wrong.push(format!("the source abandoned {abandoned} writes"));
    }

    // And shipping goes on: a record written now arrives.
    let sentinel = as_key!("test", set_name.as_str(), "sentinel");

    c1.put(&write, &sentinel, &[as_bin!("after", 1)]).await.unwrap();
    assert!(
        wait_for(&c2, &sentinel, SHIP_LAG, |b| b.is_some()).await.is_some(),
        "shipping from dc1 to dc2 has stopped"
    );

    alive(&c1, &c2, "at the end").await;

    for key in keys.iter().chain([&sentinel]) {
        let _ = c1.delete(&write, key).await;
        let _ = c2.delete(&write, key).await;
    }

    let _ = c2.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;

    assert!(wrong.is_empty(), "on-failure={policy}:\n  {}", wrong.join("\n  "));
}

#[aerospike_macro::test]
async fn every_answer_under_on_failure_policy() {
    answers_under("policy").await;
}

#[aerospike_macro::test]
async fn every_answer_under_on_failure_abandon() {
    answers_under("abandon").await;
}

/// `on-failure=fail` is not offered for this event. A merge that fails the write does not get the write
/// another try - the cluster that sent it gives up - so it would keep the local record exactly as
/// `abandon` does, and say something else.
#[aerospike_macro::test]
async fn on_failure_fail_is_not_offered() {
    let Some((c1, c2)) = clients("on_failure_fail_is_not_offered").await else { return };
    let set_name = &format!("nofail_{}", common::rand_str(6));
    let package = &format!("merge_probe_{}", common::rand_str(6));

    register(&c2, PROBE, package).await;

    let command = format!("event-set:event=xdr-merge;namespace=test;set={set_name};module={package};function=probe;on-failure=fail");
    let answer = format!("{:?}", c2.info(&AdminPolicy::default(), &[command.as_str()]).await);

    assert!(
        answer.contains("on-failure=fail is not for xdr-merge") && !answer.contains("\"ok\""),
        "binding with on-failure=fail: {answer}"
    );
    assert!(!info(&c2, "event-list").await.contains(&format!(":set={set_name}:")), "and nothing is bound");

    for policy in ["policy", "abandon"] {
        bind(&c2, set_name, package, "probe", &format!(";on-failure={policy}")).await;
        assert_eq!(unbind(&c2, set_name).await, "ok");
    }

    alive(&c1, &c2, "at the end").await;

    let _ = c2.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

//==========================================================
// A signature that is not one.
//

/// The server asks a function what it is before binding it: its signature export answers the address of
/// a string. The probe has exports that answer everything else, and binding any of them is refused, in
/// words that are the server's own - a module does not get to write part of the server's answer.
#[aerospike_macro::test]
async fn a_signature_that_is_not_one_is_refused() {
    let Some((c1, c2)) = clients("a_signature_that_is_not_one_is_refused").await else { return };
    let set_name = &format!("sig_{}", common::rand_str(6));
    let package = &format!("merge_probe_{}", common::rand_str(6));

    register(&c2, PROBE, package).await;

    for function in [
        "s_zero", "s_wild", "s_negative", "s_edge", "s_control", "s_long", "s_empty", "s_other", "s_version",
        "s_noversion", "s_bigversion", "s_plain", "s_spin", "s_trap", "no_such_function",
    ] {
        let started = Instant::now();
        let command = format!("event-set:event=xdr-merge;namespace=test;set={set_name};module={package};function={function}");
        let answers = c2.info(&AdminPolicy::default(), &[command.as_str()]).await;
        let answer = format!("{answers:?}");

        assert!(!answer.contains("\"ok\""), "{function} was bound: {answer}");
        assert!(
            !answer.contains("forged") || !answer.contains('\u{1b}'),
            "{function}: the module wrote into the server's answer: {answer}"
        );
        assert!(!answer.contains("\\n") && !answer.contains("\\u{1b}"), "{function}: control characters in {answer}");
        assert!(started.elapsed() < Duration::from_secs(10), "{function} took {:?} to be refused", started.elapsed());
        assert_eq!(counter(&c2, set_name, "fired").await, 0, "{function} left a binding behind");
    }

    // A right signature on a function that is not there binds, since the signature is all that is asked;
    // the merge it is then given fails as any handler's failure does, and the binding's policy decides.
    let key = as_key!("test", set_name.as_str(), 1);
    let write = WritePolicy::default();

    c2.put(&write, &key, &[as_bin!("base", 1)]).await.unwrap();
    assert!(wait_for(&c1, &key, SHIP_LAG, |b| b.is_some()).await.is_some());

    let command = format!("event-set:event=xdr-merge;namespace=test;set={set_name};module={package};function=s_ghost;on-failure=abandon");

    if info(&c2, &command).await == "ok" {
        c1.put(&write, &key, &[as_bin!("x", 1)]).await.unwrap();

        let deadline = Instant::now() + SHIP_LAG;

        while counter(&c2, set_name, "fallback").await == 0 && Instant::now() < deadline {
            aerospike_rt::sleep(Duration::from_millis(200)).await;
        }

        assert_eq!(counter(&c2, set_name, "fallback").await, 1, "a handler that is not there fails once");
        assert_eq!(outcome(&bins(&c2, &key).await), "kept", "and under abandon the record is kept");
        assert_eq!(unbind(&c2, set_name).await, "ok");
    }

    alive(&c1, &c2, "after every signature").await;

    let _ = c1.delete(&write, &key).await;
    let _ = c2.delete(&write, &key).await;
    let _ = c2.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

//==========================================================
// What fires the handler, and what a merged record does next.
//

/// The handler runs for an incoming XDR write to a record that exists, and for an incoming XDR delete:
/// not for a local write, and not for an XDR write that creates the record. And a merged record goes
/// back only when it holds something the other cluster lacks: with handlers on both sides, it goes back
/// once, the other side lands on the same record, and the exchange stops there.
#[aerospike_macro::test]
async fn a_merge_fires_once_and_settles() {
    let Some((c1, c2)) = clients("a_merge_fires_once_and_settles").await else { return };
    let set_name = &format!("once_{}", common::rand_str(6));
    let package = &format!("merge_probe_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!("test", set_name.as_str(), 1);

    for c in [&c1, &c2] {
        register(c, PROBE, package).await;
        bind(c, set_name, package, "probe", ";on-failure=abandon").await;
    }

    // A local write, and the XDR write that creates the record on the other side: neither is a merge.
    c2.put(&write, &key, &[as_bin!("base", 1)]).await.unwrap();
    assert!(wait_for(&c1, &key, SHIP_LAG, |b| b.is_some()).await.is_some(), "created on dc1 by shipping");
    c2.put(&write, &key, &[as_bin!("base", 1)]).await.unwrap();
    aerospike_rt::sleep(Duration::from_secs(2)).await;
    assert_eq!(counter(&c2, set_name, "fired").await, 0, "a local write is not a merge");

    // The second local write on dc2 did reach dc1 as a write to a record that exists: one merge there,
    // answered `policy` since the record says nothing else.
    let fired_1 = counter(&c1, set_name, "fired").await;

    assert!(fired_1 <= 1, "dc1 merged {fired_1} times for one incoming write");

    // A write on dc1 that the handler on dc2 turns into a record of its own.
    c1.put(&write, &key, &[as_bin!("do", "r-ok"), as_bin!("n", 5)]).await.unwrap();

    let merged = wait_for(&c2, &key, SHIP_LAG, |b| outcome(b) == "record").await;

    assert_eq!(outcome(&merged), "record", "dc2 merged to {}", describe(&merged));

    // dc2's record is not dc1's write, so it goes back to dc1 once. dc1's handler, given it, answers the
    // same record, which is then the incoming one: nothing is sent again, and both clusters hold it.
    let settled = wait_for(&c1, &key, SHIP_LAG, |b| outcome(b) == "record").await;

    assert_eq!(outcome(&settled), "record", "dc1 holds {}", describe(&settled));
    assert_eq!(bins(&c1, &key).await, bins(&c2, &key).await, "both clusters hold the merged record");
    assert_eq!(counter(&c2, set_name, "shipped-back").await, 1, "dc2's merge went back once");

    // Left alone, nothing more happens.
    let (f1, f2) = (counter(&c1, set_name, "fired").await, counter(&c2, set_name, "fired").await);

    aerospike_rt::sleep(Duration::from_secs(4)).await;

    assert_eq!(
        (counter(&c1, set_name, "fired").await, counter(&c2, set_name, "fired").await),
        (f1, f2),
        "merges went on with nothing being written"
    );
    assert_eq!(f2, 1, "one incoming write on dc2 is one merge");
    assert_eq!(counter(&c1, set_name, "shipped-back").await, 0, "dc1's merge brought nothing dc2 lacks");

    // A delete that arrives over XDR is offered to the handler too, and with `policy` it deletes.
    c1.delete(&write, &key).await.unwrap();
    assert!(wait_for(&c2, &key, SHIP_LAG, |b| b.is_none()).await.is_none(), "the delete did not reach dc2");
    assert_eq!(counter(&c2, set_name, "fired").await, 2, "an incoming delete is a merge");

    // And a handler that answers `keep-local` to a delete keeps the record.
    let kept = as_key!("test", set_name.as_str(), 2);

    c2.put(&write, &kept, &[as_bin!("do", "keep-local")]).await.unwrap();
    assert!(wait_for(&c1, &kept, SHIP_LAG, |b| b.is_some()).await.is_some(), "created on dc1 by shipping");
    c1.delete(&write, &kept).await.unwrap();

    let deadline = Instant::now() + SHIP_LAG;

    while counter(&c2, set_name, "keep-local").await == 0 && Instant::now() < deadline {
        aerospike_rt::sleep(Duration::from_millis(200)).await;
    }

    assert_eq!(counter(&c2, set_name, "keep-local").await, 1, "the delete was offered, and refused");
    assert!(bins(&c2, &kept).await.is_some(), "a delete the handler refused deleted the record");

    let _ = c2.delete(&write, &kept).await;

    // A handler that answers `delete` to a write removes the record where it merged, and is counted.
    let doomed = as_key!("test", set_name.as_str(), 3);

    c2.put(&write, &doomed, &[as_bin!("base", 1)]).await.unwrap();
    assert!(wait_for(&c1, &doomed, SHIP_LAG, |b| b.is_some()).await.is_some(), "created on dc1 by shipping");
    c1.put(&write, &doomed, &[as_bin!("do", "delete")]).await.unwrap();
    assert!(wait_for(&c2, &doomed, SHIP_LAG, |b| b.is_none()).await.is_none(), "`delete` did not remove the record");
    assert_eq!(counter(&c2, set_name, "delete").await, 1, "and it is counted as a delete");
    assert!(bins(&c1, &doomed).await.is_some(), "on the cluster that merged, and no other");

    let _ = c1.delete(&write, &doomed).await;

    alive(&c1, &c2, "at the end").await;

    for c in [&c1, &c2] {
        assert_eq!(unbind(c, set_name).await, "ok");
        let _ = c.delete(&write, &key).await;
        let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
    }
}

//==========================================================
// The binding, changed under the merges it serves.
//

/// Writes ship and merge while the binding they merge under is removed, put back, pointed at another
/// module, and its module replaced and removed. Each merge runs under whatever was bound when it began,
/// or under none; what must hold is that both clusters keep answering and that shipping is whole again
/// once the binding is settled.
#[aerospike_macro::test]
async fn the_binding_changes_while_merges_run() {
    let Some((c1, c2)) = clients("the_binding_changes_while_merges_run").await else { return };
    let set_name = &format!("churn_{}", common::rand_str(6));
    let package = &format!("merge_probe_{}", common::rand_str(6));
    let other = &format!("counter_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let keys: Vec<Key> = (0..20).map(|i| as_key!("test", set_name.as_str(), i as i64)).collect();

    register(&c2, PROBE, package).await;
    register(&c2, COUNTER, other).await;

    for key in &keys {
        c2.put(&write, key, &[as_bin!("base", 1)]).await.unwrap();
    }

    for key in &keys {
        assert!(wait_for(&c1, key, SHIP_LAG, |b| b.is_some()).await.is_some());
    }

    let until = Instant::now() + Duration::from_secs(10);

    let writer = async {
        let answers = ["r-ok", "take-remote", "keep-local", "trap", "r-kinds", "banana", "spin"];
        let mut i = 0usize;

        while Instant::now() < until {
            let key = &keys[i % keys.len()];
            let _ = c1
                .put(&write, key, &[as_bin!("do", answers[i % answers.len()]), as_bin!("n", i as i64)])
                .await;

            i += 1;
            aerospike_rt::sleep(Duration::from_millis(5)).await;
        }

        i
    };

    let changer = async {
        let admin = AdminPolicy::default();
        let mut i = 0usize;

        while Instant::now() < until {
            let command = match i % 8 {
                0 => format!("event-set:event=xdr-merge;namespace=test;set={set_name};module={package};function=probe;on-failure=abandon;timeout=100"),
                1 => format!("event-remove:event=xdr-merge;namespace=test;set={set_name}"),
                2 => format!("event-set:event=xdr-merge;namespace=test;set={set_name};module={package};function=probe;on-failure=policy;timeout=100"),
                3 => format!("event-set:event=xdr-merge;namespace=test;set={set_name};module={other};function=merge_counter;on-failure=abandon"),
                4 => format!("event-set:event=xdr-merge;namespace=test;set={set_name};module={package};function=probe;on-failure=policy"),
                _ => String::new(),
            };

            match i % 8 {
                // The bound module replaced, removed while bound, and put back.
                5 => {
                    let _ = c2.register_udf(&admin, PROBE, &format!("{package}.wasm"), UdfLang::Wasm).await;
                }
                6 => {
                    let _ = c2.remove_udf(&admin, &format!("{package}.wasm")).await;
                }
                7 => {
                    let _ = c2.register_udf(&admin, PROBE, &format!("{package}.wasm"), UdfLang::Wasm).await;
                }
                _ => {
                    let _ = c2.info(&admin, &[command.as_str()]).await;
                }
            }

            i += 1;
            aerospike_rt::sleep(Duration::from_millis(40)).await;
        }

        i
    };

    let (written, changed) = futures::join!(writer, changer);

    eprintln!("  {written} writes shipped under {changed} changes of the binding");
    alive(&c1, &c2, "after the binding was changed under load").await;

    // Settled on no binding, shipping is whole: a record written now arrives as it is.
    let _ = unbind(&c2, set_name).await;
    register(&c2, PROBE, package).await;

    let sentinel = as_key!("test", set_name.as_str(), "sentinel");

    c1.put(&write, &sentinel, &[as_bin!("after", 1)]).await.unwrap();
    assert!(
        wait_for(&c2, &sentinel, SHIP_LAG, |b| b.is_some()).await.is_some(),
        "shipping from dc1 to dc2 has stopped"
    );

    // And settled on a binding again, a merge is a merge.
    bind(&c2, set_name, package, "probe", ";on-failure=abandon").await;
    c1.put(&write, &sentinel, &[as_bin!("do", "r-ok"), as_bin!("n", 1)]).await.unwrap();

    let merged = wait_for(&c2, &sentinel, SHIP_LAG, |b| outcome(b) == "record").await;

    assert_eq!(outcome(&merged), "record", "after the churn a merge gives {}", describe(&merged));
    assert_eq!(unbind(&c2, set_name).await, "ok");

    for key in keys.iter().chain([&sentinel]) {
        let _ = c1.delete(&write, key).await;
        let _ = c2.delete(&write, key).await;
    }

    for package in [package, other] {
        let _ = c2.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
    }
}

//==========================================================
// Convergence, on operations nobody chose.
//

/// A small generator, so that a failure names the seed that reproduces it.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

async fn bind_both(c1: &Client, c2: &Client, module: &[u8], package: &str, set_name: &str, function: &str) {
    for c in [c1, c2] {
        register(c, module, package).await;
        bind(c, set_name, package, function, ";on-failure=abandon").await;
    }
}

async fn unbind_both(c1: &Client, c2: &Client, package: &str, set_name: &str, keys: &[Key]) {
    for c in [c1, c2] {
        assert_eq!(unbind(c, set_name).await, "ok");

        for key in keys {
            let _ = c.delete(&WritePolicy::default(), key).await;
        }

        let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
    }
}

async fn int_of(client: &Client, key: &Key, package: &str, function: &str) -> Option<i64> {
    match client.execute_udf(&WritePolicy::default(), key, package, function, &[]).await {
        Ok(Some(Value::Int(n))) => Some(n),
        _ => None,
    }
}

async fn members_of(client: &Client, key: &Key, package: &str) -> Option<Vec<String>> {
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

/// The three merges whose result can be said in advance, each given a random run of operations on both
/// clusters at once, on several records. Whatever order the writes ship and merge in, both clusters must
/// end on the same value, and it must be the one the operations add up to.
///
/// A counter's entry for a site is only ever written on that site, which is the rule the counters rest
/// on, so dc1 writes as `dc1` and dc2 as `dc2`.
#[aerospike_macro::test]
async fn random_operations_converge_on_the_value_they_add_up_to() {
    let Some((c1, c2)) = clients("random_operations_converge_on_the_value_they_add_up_to").await else { return };
    let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let seed = env::var("XDR_MERGE_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(seed);
    let mut rng = Rng(seed);
    let write = WritePolicy::default();
    const KEYS: usize = 3;
    const ROUNDS: usize = 30;

    eprintln!("  seed {seed} (XDR_MERGE_SEED={seed} runs it again)");

    // --- the grow-only counter and the counter that goes down ---

    for (name, module, function, down) in
        [("counter", COUNTER, "merge_counter", false), ("pncounter", PNCOUNTER, "merge_pncounter", true)]
    {
        let set_name = &format!("{name}_{}", common::rand_str(6));
        let package = &format!("{name}_{}", common::rand_str(6));
        let keys: Vec<Key> = (0..KEYS).map(|i| as_key!("test", set_name.as_str(), i as i64)).collect();
        let mut expected = [0i64; KEYS];

        bind_both(&c1, &c2, module, package, set_name, function).await;

        for _ in 0..ROUNDS {
            // One operation on each cluster, at the same moment, perhaps on the same record.
            let mut ops = Vec::new();

            for (client, site) in [(&c1, "dc1"), (&c2, "dc2")] {
                let k = rng.below(KEYS as u64) as usize;
                let n = rng.below(10) as i64;
                let verb = if down && rng.below(3) == 0 { "decrement" } else { "increment" };

                expected[k] += if verb == "decrement" { -n } else { n };
                ops.push((client, k, verb, site, n));
            }

            let run = |(client, k, verb, site, n): (&Client, usize, &'static str, &'static str, i64)| {
                let key = keys[k].clone();
                let package = package.clone();
                let write = write.clone();
                let client = client.clone();

                async move {
                    client
                        .execute_udf(&write, &key, &package, verb, &[Value::from(site), Value::from(n)])
                        .await
                        .unwrap_or_else(|e| panic!("seed {seed}: {verb}({site}, {n}): {e:?}"));
                }
            };

            futures::join!(run(ops[0]), run(ops[1]));

            // Now and then, let shipping catch up: a merge of two long-diverged records and a merge of two
            // records one write apart are different merges.
            if rng.below(4) == 0 {
                aerospike_rt::sleep(Duration::from_millis(300)).await;
            }
        }

        for (k, key) in keys.iter().enumerate() {
            let deadline = Instant::now() + SHIP_LAG;

            loop {
                let (v1, v2) = (int_of(&c1, key, package, "value").await, int_of(&c2, key, package, "value").await);

                // A record nothing was added to may not exist at all.
                let both = |v: Option<i64>| v.unwrap_or(0) == expected[k];

                if both(v1) && both(v2) {
                    break;
                }

                assert!(
                    Instant::now() < deadline,
                    "seed {seed}: {name} record {k} should be {}: dc1 says {v1:?}, dc2 says {v2:?}",
                    expected[k]
                );
                aerospike_rt::sleep(Duration::from_millis(250)).await;
            }
        }

        unbind_both(&c1, &c2, package, set_name, &keys).await;
    }

    // --- the set with removals: a tag removed once is gone for good, on both sides ---

    let set_name = &format!("twopset_{}", common::rand_str(6));
    let package = &format!("twopset_{}", common::rand_str(6));
    let keys: Vec<Key> = (0..KEYS).map(|i| as_key!("test", set_name.as_str(), i as i64)).collect();
    let tags = ["a", "b", "c", "d", "e", "f"];
    let mut added: Vec<Vec<&str>> = vec![Vec::new(); KEYS];
    let mut removed: Vec<Vec<&str>> = vec![Vec::new(); KEYS];

    bind_both(&c1, &c2, TWOPSET, package, set_name, "merge_twopset").await;

    for _ in 0..ROUNDS {
        for client in [&c1, &c2] {
            let k = rng.below(KEYS as u64) as usize;
            let tag = tags[rng.below(tags.len() as u64) as usize];

            if rng.below(3) == 0 {
                // A removal is refused for a tag this side does not hold; one that goes through is final.
                if client.execute_udf(&write, &keys[k], package, "remove", &[Value::from(tag)]).await.is_ok() {
                    removed[k].push(tag);
                }
            } else {
                client
                    .execute_udf(&write, &keys[k], package, "add", &[Value::from(tag)])
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed}: add({tag}): {e:?}"));
                added[k].push(tag);
            }
        }

        if rng.below(4) == 0 {
            aerospike_rt::sleep(Duration::from_millis(300)).await;
        }
    }

    for (k, key) in keys.iter().enumerate() {
        let mut expected: Vec<String> =
            tags.iter().filter(|t| added[k].contains(t) && !removed[k].contains(t)).map(|t| t.to_string()).collect();

        expected.sort();

        let deadline = Instant::now() + SHIP_LAG;

        loop {
            let (m1, m2) = (members_of(&c1, key, package).await, members_of(&c2, key, package).await);
            let same = |m: &Option<Vec<String>>| {
                let mut m = m.clone().unwrap_or_default();

                m.sort();
                m == expected
            };

            if same(&m1) && same(&m2) {
                break;
            }

            assert!(
                Instant::now() < deadline,
                "seed {seed}: set {k} should hold {expected:?}: dc1 says {m1:?}, dc2 says {m2:?}"
            );
            aerospike_rt::sleep(Duration::from_millis(250)).await;
        }
    }

    unbind_both(&c1, &c2, package, set_name, &keys).await;
    alive(&c1, &c2, "at the end").await;
}

//==========================================================
// The binding, across a restart.
//

/// A binding is the cluster's, not the process's: after the receiving node restarts it is still bound,
/// to the same function, and the next incoming write is merged by it.
///
/// Restarting a node is not something a client can do, so this needs to be told how:
/// `AEROSPIKE_XDR_DC2_RESTART` is a shell command that restarts dc2 (`podman restart asd-dc2`). Without
/// it the test skips.
#[aerospike_macro::test]
async fn a_binding_survives_a_restart() {
    let Ok(restart) = env::var("AEROSPIKE_XDR_DC2_RESTART") else {
        eprintln!("skipped a_binding_survives_a_restart: AEROSPIKE_XDR_DC2_RESTART names no way to restart dc2");
        return;
    };
    let Some((c1, c2)) = clients("a_binding_survives_a_restart").await else { return };
    let set_name = &format!("restart_{}", common::rand_str(6));
    let package = &format!("merge_probe_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let key = as_key!("test", set_name.as_str(), 1);

    register(&c2, PROBE, package).await;
    bind(&c2, set_name, package, "probe", ";on-failure=abandon;timeout=250").await;
    c2.put(&write, &key, &[as_bin!("base", 1)]).await.unwrap();
    assert!(wait_for(&c1, &key, SHIP_LAG, |b| b.is_some()).await.is_some(), "created on dc1 by shipping");
    drop(c2);

    // The record has to be on dc2's device before dc2 goes down, or it comes back without it and the
    // incoming write creates the record, which is no merge. A write reaches the device within a second.
    aerospike_rt::sleep(Duration::from_secs(3)).await;

    let status = std::process::Command::new("sh").arg("-c").arg(&restart).status().expect("the restart command");

    assert!(status.success(), "{restart} failed: {status:?}");

    // A write made while dc2 is coming back waits for it at the source.
    c1.put(&write, &key, &[as_bin!("do", "r-ok"), as_bin!("n", 9)]).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(90);
    let c2 = loop {
        match Client::new(&ClientPolicy::default(), &dcs().unwrap().1).await {
            Ok(c) => break c,
            Err(e) => assert!(Instant::now() < deadline, "dc2 did not come back: {e:?}"),
        }

        aerospike_rt::sleep(Duration::from_millis(500)).await;
    };

    let listed = info(&c2, "event-list").await;

    assert!(
        listed.contains(&format!(":set={set_name}:name=probe:order=0:module={package}:function=probe:"))
            && listed.contains(":on-failure=abandon:timeout=250:"),
        "the binding did not come back as it was: {listed}"
    );

    let merged = wait_for(&c2, &key, Duration::from_secs(90), |b| outcome(b) == "record").await;

    let row = info(&c2, "event-list").await;
    let row = row.split(';').find(|r| r.contains(&format!(":set={set_name}:"))).unwrap_or("no binding").to_string();

    assert_eq!(
        outcome(&merged),
        "record",
        "after the restart the write was not merged: dc2 holds {}, and its binding says {row}",
        describe(&merged)
    );

    assert_eq!(unbind(&c2, set_name).await, "ok");
    let _ = c1.delete(&write, &key).await;
    let _ = c2.delete(&write, &key).await;
    let _ = c2.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}
