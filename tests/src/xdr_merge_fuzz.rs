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

//! The `xdr-merge` review items (`XDR_MERGE_TODO.md`), each pinned by a test, then all of them fuzzed.
//!
//! The pinned tests: `abandon` is the default on-failure (item 8); an abandoned merge is counted apart at
//! the source (9); the counters tell a take-remote, a write taken as it came and a collision apart, and
//! failures by cause, with the time spent (10); a record answer to a delete fails rather than deletes (14);
//! a take-remote that changes nothing is a lost conflict (15); a set opts out of its namespace's binding
//! (16); a client cannot call a handler (18).
//!
//! The fuzzing: random binding commands checked against a model of the bindings (8, 16, 17, 19); random
//! answers checked against the counters' identities and against what the source counts (9, 10, 14, 15);
//! random counter writes on both clusters while ships are held, the link goes and comes back, and the
//! bindings are replaced under the writes (1-7, 19), checked for convergence; and random client calls to
//! the exports of bound modules (18). Each prints its seed; `XDR_FUZZ_SEED` runs one again and
//! `XDR_FUZZ_ROUNDS` scales them.
//!
//! Every test here binds in the `fuzz` namespace, which nothing else binds, and takes one lock, so that the
//! counters a test reads are its own.

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use aerospike::*;
use futures::lock::Mutex;

use crate::common;

const PROBE: &[u8] = include_bytes!("../fixtures/wasm/merge_probe.wasm");
const COUNTER: &[u8] = include_bytes!("../fixtures/wasm/counter.wasm");

const NS: &str = "fuzz";
const SHIP_LAG: Duration = Duration::from_secs(30);
const RECOVERY_LAG: Duration = Duration::from_secs(60);

static FUZZ_NS: OnceLock<Mutex<()>> = OnceLock::new();

fn ns_lock() -> &'static Mutex<()> {
    FUZZ_NS.get_or_init(|| Mutex::new(()))
}

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

fn seed_for(test: &str) -> u64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let seed = env::var("XDR_FUZZ_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(now);

    eprintln!("  {test}: seed {seed} (XDR_FUZZ_SEED={seed} runs it again)");
    seed
}

fn rounds(default: usize) -> usize {
    env::var("XDR_FUZZ_ROUNDS").ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

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

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

fn write_policy() -> WritePolicy {
    // A merge that spins to its deadline holds the record; a write behind it waits longer than a second.
    let mut policy = WritePolicy::default();

    policy.base_policy.total_timeout = 5000;
    policy.base_policy.socket_timeout = 5000;
    policy
}

async fn info(client: &Client, command: &str) -> String {
    let answers = client.info(&AdminPolicy::default(), &[command]).await.unwrap();

    answers
        .get(command)
        .cloned()
        .unwrap_or_else(|| panic!("no answer to {command:?}: {answers:?}"))
}

async fn info_ok(client: &Client, command: &str) {
    let deadline = Instant::now() + SHIP_LAG;

    loop {
        let answer = info(client, command).await;

        if answer == "ok" {
            return;
        }

        assert!(Instant::now() < deadline, "{command}: {answer}");
        aerospike_rt::sleep(Duration::from_millis(250)).await;
    }
}

async fn register(client: &Client, module: &[u8], package: &str) {
    let task = client
        .register_udf(&AdminPolicy::default(), module, &format!("{package}.wasm"), UdfLang::Wasm)
        .await
        .unwrap();

    task.wait_till_complete(None).await.unwrap();
}

fn set_command(set_name: &str, package: &str, function: &str, extra: &str) -> String {
    format!("event-set:event=xdr-merge;namespace={NS};set={set_name};module={package};function={function}{extra}")
}

async fn bind(client: &Client, set_name: &str, package: &str, function: &str, extra: &str) {
    let command = set_command(set_name, package, function, extra);

    assert_eq!(info(client, &command).await, "ok", "{command}");
}

async fn unbind(client: &Client, set_name: &str) -> String {
    info(client, &format!("event-remove:event=xdr-merge;namespace={NS};set={set_name}")).await
}

/// Every binding `event-list` shows in the namespace, as its fields, keyed by set and name.
async fn rows(client: &Client) -> BTreeMap<(String, String), HashMap<String, String>> {
    let listed = info(client, &format!("event-list:namespace={NS}")).await;
    let mut out = BTreeMap::new();

    for row in listed.split(';').filter(|r| !r.is_empty()) {
        let fields: HashMap<String, String> = row
            .split(':')
            .filter_map(|f| f.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        out.insert((fields["set"].clone(), fields["name"].clone()), fields);
    }

    out
}

/// One binding's counters; all zero when the set has none.
async fn counters(client: &Client, set_name: &str) -> HashMap<String, u64> {
    rows(client)
        .await
        .into_iter()
        .find(|((set, _), _)| set == set_name)
        .map(|(_, fields)| fields.into_iter().filter_map(|(k, v)| Some((k, v.parse().ok()?))).collect())
        .unwrap_or_default()
}

async fn counter(client: &Client, set_name: &str, name: &str) -> u64 {
    counters(client, set_name).await.get(name).copied().unwrap_or(0)
}

/// One of dc1's counters for what it ships to dc2 in `namespace`.
async fn xdr_stat(c1: &Client, namespace: &str, name: &str) -> u64 {
    let stats = info(c1, &format!("get-stats:context=xdr;dc=dc2;namespace={namespace}")).await;

    stats
        .split(';')
        .find_map(|f| f.strip_prefix(&format!("{name}=")))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no {name} in {stats}"))
}

/// dc1 has nothing left to ship to dc2 in `namespace`.
async fn shipped(c1: &Client, namespace: &str) {
    let deadline = Instant::now() + SHIP_LAG;

    loop {
        if xdr_stat(c1, namespace, "in_queue").await == 0 && xdr_stat(c1, namespace, "in_progress").await == 0 {
            // A ship that just completed may still be merging on the other side.
            aerospike_rt::sleep(Duration::from_millis(500)).await;

            if xdr_stat(c1, namespace, "in_queue").await == 0 && xdr_stat(c1, namespace, "in_progress").await == 0 {
                return;
            }
        }

        assert!(Instant::now() < deadline, "dc1 did not finish shipping {namespace}");
        aerospike_rt::sleep(Duration::from_millis(200)).await;
    }
}

async fn bins(client: &Client, key: &Key) -> Option<HashMap<String, Value>> {
    let mut policy = ReadPolicy::default();

    policy.base_policy.total_timeout = 5000;
    policy.base_policy.socket_timeout = 5000;

    match client.get(&policy, key, Bins::All).await {
        Ok(r) => Some(r.bins.into_iter().collect()),
        Err(e) if e.matches(&[ResultCode::KeyNotFoundError]) => None,
        Err(e) => panic!("{e:?}"),
    }
}

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

async fn wait_counter(client: &Client, set_name: &str, name: &str, at_least: u64) -> u64 {
    let deadline = Instant::now() + SHIP_LAG;

    loop {
        let n = counter(client, set_name, name).await;

        if n >= at_least || Instant::now() >= deadline {
            return n;
        }

        aerospike_rt::sleep(Duration::from_millis(100)).await;
    }
}

/// Both clusters hold the same record and dc1 has nothing left to ship.
async fn settled(c1: &Client, c2: &Client, key: &Key) {
    let deadline = Instant::now() + SHIP_LAG;

    loop {
        shipped(c1, NS).await;

        let (b1, b2) = (bins(c1, key).await, bins(c2, key).await);

        if b1 == b2 {
            return;
        }

        assert!(Instant::now() < deadline, "{key:?} never settled: dc1 {b1:?}, dc2 {b2:?}");
        aerospike_rt::sleep(Duration::from_millis(200)).await;
    }
}

async fn alive(c1: &Client, c2: &Client, when: &str) {
    for (c, name) in [(c1, "dc1"), (c2, "dc2")] {
        let build = c.info(&AdminPolicy::default(), &["build"]).await;

        assert!(build.is_ok(), "{name} does not answer {when}: {build:?}");
    }
}

/// A record on both clusters, created on dc1 before anything is bound, so the next write is a merge.
async fn seed_record(c1: &Client, c2: &Client, key: &Key, bins: &[Bin]) {
    c1.put(&write_policy(), key, bins).await.unwrap();
    assert!(wait_for(c2, key, SHIP_LAG, |b| b.is_some()).await.is_some(), "{key:?} never reached dc2");
}

async fn cleanup(c1: &Client, c2: &Client, set_name: &str, keys: &[Key], packages: &[&str]) {
    for c in [c1, c2] {
        let _ = unbind(c, set_name).await;

        for key in keys {
            let _ = c.delete(&write_policy(), key).await;
        }

        for package in packages {
            let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
        }
    }
}

//==========================================================
// The items, one by one.
//

/// Item 8: a binding made without `on-failure` abandons on failure, and says so in `event-list`; one
/// made with `policy` keeps it.
#[aerospike_macro::test]
async fn abandon_is_the_default_on_failure() {
    let Some((_c1, c2)) = clients("abandon_is_the_default_on_failure").await else { return };
    let _guard = ns_lock().lock().await;
    let package = &format!("probe_{}", common::rand_str(6));
    let (s1, s2) = (&format!("dflt_{}", common::rand_str(6)), &format!("pol_{}", common::rand_str(6)));

    register(&c2, PROBE, package).await;
    bind(&c2, s1, package, "probe", "").await;
    bind(&c2, s2, package, "probe", ";on-failure=policy").await;

    let listed = rows(&c2).await;

    assert_eq!(listed[&(s1.to_string(), "probe".to_string())]["on-failure"], "abandon");
    assert_eq!(listed[&(s2.to_string(), "probe".to_string())]["on-failure"], "policy");

    for bad in ["", "x", "Abandon"] {
        let answer = info(&c2, &set_command(s1, package, "probe", &format!(";on-failure={bad}"))).await;

        assert!(answer.contains("on-failure must be"), "on-failure={bad:?}: {answer}");
    }

    let _ = unbind(&c2, s1).await;
    let _ = unbind(&c2, s2).await;
    let _ = c2.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// Item 9: a merge that fails under `abandon` drops the incoming write, and the source counts it as
/// `merge_abandoned`, not as a success. Under `policy` the same failure lets the write in and the source
/// counts a success.
#[aerospike_macro::test]
async fn an_abandoned_merge_is_counted_apart_at_the_source() {
    let Some((c1, c2)) = clients("an_abandoned_merge_is_counted_apart_at_the_source").await else { return };
    let _guard = ns_lock().lock().await;
    let package = &format!("probe_{}", common::rand_str(6));
    let set_name = &format!("abandon_{}", common::rand_str(6));
    let key = as_key!(NS, set_name.as_str(), 1);

    register(&c2, PROBE, package).await;
    seed_record(&c1, &c2, &key, &[as_bin!("base", 1)]).await;
    bind(&c2, set_name, package, "probe", ";on-failure=abandon").await;
    shipped(&c1, NS).await;

    let (abandoned_0, success_0) = (xdr_stat(&c1, NS, "merge_abandoned").await, xdr_stat(&c1, NS, "success").await);

    c1.put(&write_policy(), &key, &[as_bin!("do", "trap")]).await.unwrap();
    assert_eq!(wait_counter(&c2, set_name, "abandoned", 1).await, 1, "dc2 abandoned the write");
    shipped(&c1, NS).await;

    assert_eq!(xdr_stat(&c1, NS, "merge_abandoned").await, abandoned_0 + 1, "dc1 counts the abandoned merge");
    assert_eq!(xdr_stat(&c1, NS, "success").await, success_0, "and not as a success");
    assert!(!bins(&c2, &key).await.unwrap().contains_key("do"), "dc2 kept its record");

    // The same failure under policy: the write goes in, and it is a success.
    bind(&c2, set_name, package, "probe", ";on-failure=policy").await;
    c1.put(&write_policy(), &key, &[as_bin!("do", "trap"), as_bin!("n", 2)]).await.unwrap();
    assert!(wait_for(&c2, &key, SHIP_LAG, |b| b.as_ref().is_some_and(|b| b.contains_key("n"))).await.is_some());
    shipped(&c1, NS).await;

    assert_eq!(xdr_stat(&c1, NS, "merge_abandoned").await, abandoned_0 + 1, "policy abandons nothing");
    assert_eq!(xdr_stat(&c1, NS, "success").await, success_0 + 1, "the write that went in is a success");

    cleanup(&c1, &c2, set_name, &[key], &[package]).await;
}

/// Item 10: the counters say what each merge came to. A take-remote, a record that is the incoming write
/// applied, one that combines the two, and failures by cause are each counted on their own, and the time
/// spent in the handler is shown.
#[aerospike_macro::test]
async fn the_counters_tell_the_outcomes_apart() {
    let Some((c1, c2)) = clients("the_counters_tell_the_outcomes_apart").await else { return };
    let _guard = ns_lock().lock().await;
    let probe = &format!("probe_{}", common::rand_str(6));
    let counter_pkg = &format!("counter_{}", common::rand_str(6));
    let set_name = &format!("count_{}", common::rand_str(6));
    let set_counter = &format!("countc_{}", common::rand_str(6));
    let key = as_key!(NS, set_name.as_str(), 1);
    let ckey = as_key!(NS, set_counter.as_str(), 1);
    let write = write_policy();

    register(&c2, PROBE, probe).await;
    seed_record(&c1, &c2, &key, &[as_bin!("base", 1)]).await;
    bind(&c2, set_name, probe, "probe", ";on-failure=abandon;timeout=100").await;

    // One of each, one at a time.
    let steps: &[(&str, &str)] = &[
        ("take-remote", "take-remote"),
        ("r-ok", "combined"),
        ("trap", "fail-error"),
        ("spin", "fail-timeout"),
        ("banana", "fail-answer"),
    ];

    for (i, (answer, counted)) in steps.iter().enumerate() {
        let before = counter(&c2, set_name, counted).await;

        c1.put(&write, &key, &[as_bin!("do", *answer), as_bin!("n", i as i64 + 10)]).await.unwrap();
        assert_eq!(wait_counter(&c2, set_name, counted, before + 1).await, before + 1, "{answer} counts as {counted}");

        // A merged record ships back to dc1, which has no binding and takes it whole. Landing after dc1's
        // next write, it would replace that write before it left, and the next step would never reach dc2.
        // A failure under abandon keeps dc2's record, so the two differ from then on, as they should.
        if ["take-remote", "combined"].contains(counted) {
            settled(&c1, &c2, &key).await;
        } else {
            shipped(&c1, NS).await;
        }
    }

    let c = counters(&c2, set_name).await;

    assert_eq!(c["fallback"], 3, "{c:?}");
    assert_eq!(c["abandoned"], 3, "{c:?}");
    assert!(!c.contains_key("record"), "the old record counter is gone: {c:?}");
    assert!(c["max-us"] >= c["avg-us"] && c["max-us"] >= 90_000, "a spin to 100 ms is the longest run: {c:?}");

    // A record answer that is the incoming write applied: the counter, written only on dc1, so that the
    // merge on dc2 takes dc1's count as it comes.
    register(&c1, COUNTER, counter_pkg).await;
    register(&c2, COUNTER, counter_pkg).await;
    c1.execute_udf(&write, &ckey, counter_pkg, "increment", &[Value::from("dc1"), Value::from(1)]).await.unwrap();
    assert!(wait_for(&c2, &ckey, SHIP_LAG, |b| b.is_some()).await.is_some());
    bind(&c2, set_counter, counter_pkg, "merge_counter", "").await;
    c1.execute_udf(&write, &ckey, counter_pkg, "increment", &[Value::from("dc1"), Value::from(2)]).await.unwrap();
    assert_eq!(wait_counter(&c2, set_counter, "took-incoming", 1).await, 1, "dc1's write taken as it came");
    assert_eq!(counter(&c2, set_counter, "combined").await, 0);

    cleanup(&c1, &c2, set_name, &[key], &[probe]).await;
    cleanup(&c1, &c2, set_counter, &[ckey], &[counter_pkg]).await;
}

/// Item 14: a record answer to an incoming delete cannot be applied in its place. It is the handler's
/// failure: under `abandon` the record is kept, under `policy` the delete goes ahead - and either way it
/// is counted, never a silent delete.
#[aerospike_macro::test]
async fn a_record_answer_to_a_delete_is_a_failure() {
    let Some((c1, c2)) = clients("a_record_answer_to_a_delete_is_a_failure").await else { return };
    let _guard = ns_lock().lock().await;
    let package = &format!("probe_{}", common::rand_str(6));
    let set_name = &format!("delrec_{}", common::rand_str(6));
    let write = write_policy();

    register(&c2, PROBE, package).await;

    for (on_failure, kept) in [("abandon", true), ("policy", false)] {
        let key = as_key!(NS, set_name.as_str(), on_failure);

        // The incoming delete has no bins, so the probe reads `do` from the current record.
        seed_record(&c1, &c2, &key, &[as_bin!("do", "r-ok"), as_bin!("n", 4)]).await;
        bind(&c2, set_name, package, "probe", &format!(";on-failure={on_failure}")).await;

        let failed = counter(&c2, set_name, "fail-answer").await;

        c1.delete(&write, &key).await.unwrap();
        assert_eq!(wait_counter(&c2, set_name, "fail-answer", failed + 1).await, failed + 1, "{on_failure}");
        shipped(&c1, NS).await;
        assert_eq!(bins(&c2, &key).await.is_some(), kept, "{on_failure}: the record on dc2");
        assert_eq!(counter(&c2, set_name, "delete").await, 0, "{on_failure}: nothing answered delete");

        let _ = c2.delete(&write, &key).await;
    }

    cleanup(&c1, &c2, set_name, &[], &[package]).await;
}

/// Item 15: a take-remote whose result is the record as it stands changes nothing, and the source hears
/// "lost conflict" - it is counted as keep-local, not as a take-remote.
#[aerospike_macro::test]
async fn a_take_remote_that_changes_nothing_is_a_lost_conflict() {
    let Some((c1, c2)) = clients("a_take_remote_that_changes_nothing_is_a_lost_conflict").await else { return };
    let _guard = ns_lock().lock().await;
    let package = &format!("probe_{}", common::rand_str(6));
    let set_name = &format!("same_{}", common::rand_str(6));
    let key = as_key!(NS, set_name.as_str(), 1);
    let same = [as_bin!("do", "take-remote"), as_bin!("n", 1)];

    register(&c2, PROBE, package).await;
    seed_record(&c1, &c2, &key, &same).await;
    bind(&c2, set_name, package, "probe", "").await;

    c1.put(&write_policy(), &key, &same).await.unwrap();
    assert_eq!(wait_counter(&c2, set_name, "keep-local", 1).await, 1);
    assert_eq!(counter(&c2, set_name, "take-remote").await, 0);

    // And one that does change the record is a take-remote.
    c1.put(&write_policy(), &key, &[as_bin!("do", "take-remote"), as_bin!("n", 2)]).await.unwrap();
    assert_eq!(wait_counter(&c2, set_name, "take-remote", 1).await, 1);

    cleanup(&c1, &c2, set_name, &[key], &[package]).await;
}

/// Item 16: `module=none` on a set opts it out of the namespace's binding. Its writes arrive as written;
/// another set's are merged. Removing the opt-out puts the set back under the namespace's binding.
#[aerospike_macro::test]
async fn a_set_opts_out_of_the_namespace_binding() {
    let Some((c1, c2)) = clients("a_set_opts_out_of_the_namespace_binding").await else { return };
    let _guard = ns_lock().lock().await;
    let package = &format!("probe_{}", common::rand_str(6));
    let (out_set, in_set) = (&format!("out_{}", common::rand_str(6)), &format!("in_{}", common::rand_str(6)));
    let (out_key, in_key) = (as_key!(NS, out_set.as_str(), 1), as_key!(NS, in_set.as_str(), 1));
    let write = write_policy();

    register(&c2, PROBE, package).await;
    seed_record(&c1, &c2, &out_key, &[as_bin!("base", 1)]).await;
    seed_record(&c1, &c2, &in_key, &[as_bin!("base", 1)]).await;

    // The checks on the opt-out itself.
    let ns_wide = format!("event-set:event=xdr-merge;namespace={NS};module={package};function=probe");

    for (command, refusal) in [
        (format!("event-set:event=xdr-merge;namespace={NS};module=none"), "needs a set"),
        (set_command(out_set, "none", "probe", ""), "takes no function"),
        (format!("event-set:event=xdr-merge;namespace={NS};set={out_set};module=none;on-failure=policy"), "no on-failure"),
        (format!("event-set:event=xdr-merge;namespace={NS};set={out_set};module=none;timeout=5"), "no timeout"),
    ] {
        let answer = info(&c2, &command).await;

        assert!(answer.contains(refusal), "{command}: {answer}");
    }

    assert_eq!(info(&c2, &ns_wide).await, "ok");
    assert_eq!(info(&c2, &format!("event-set:event=xdr-merge;namespace={NS};set={out_set};module=none")).await, "ok");

    let listed = rows(&c2).await;

    assert_eq!(listed[&(out_set.to_string(), "none".to_string())]["module"], "none");

    for key in [&out_key, &in_key] {
        c1.put(&write, key, &[as_bin!("do", "r-ok"), as_bin!("n", 5)]).await.unwrap();
    }

    let merged = wait_for(&c2, &in_key, SHIP_LAG, |b| b.as_ref().is_some_and(|b| b.contains_key("merged"))).await;
    let taken = wait_for(&c2, &out_key, SHIP_LAG, |b| b.as_ref().is_some_and(|b| b.contains_key("do"))).await;

    assert!(merged.unwrap().contains_key("merged"), "the bound set was merged");
    assert!(!taken.as_ref().unwrap().contains_key("merged"), "the opted-out set was not: {taken:?}");
    assert_eq!(counter(&c2, "", "fired").await, 1, "only the bound set's write was offered");

    // Opted back in.
    assert_eq!(unbind(&c2, out_set).await, "ok");
    c1.put(&write, &out_key, &[as_bin!("do", "r-ok"), as_bin!("n", 6)]).await.unwrap();

    let merged = wait_for(&c2, &out_key, SHIP_LAG, |b| b.as_ref().is_some_and(|b| b.contains_key("merged"))).await;

    assert!(merged.unwrap().contains_key("merged"), "without the opt-out the namespace's binding merges it");

    let _ = info(&c2, &format!("event-remove:event=xdr-merge;namespace={NS};set=")).await;
    let _ = info(&c2, &format!("event-remove:event=xdr-merge;namespace={NS}")).await;
    cleanup(&c1, &c2, in_set, &[in_key], &[]).await;
    cleanup(&c1, &c2, out_set, &[out_key], &[package]).await;
}

/// Item 18: a handler is the server's to call. A client calling it is refused before it runs; the
/// module's other functions answer as usual.
#[aerospike_macro::test]
async fn a_client_cannot_call_a_handler() {
    let Some((c1, _c2)) = clients("a_client_cannot_call_a_handler").await else { return };
    let _guard = ns_lock().lock().await;
    let package = &format!("counter_{}", common::rand_str(6));
    let key = as_key!(NS, format!("call_{}", common::rand_str(6)).as_str(), 1);
    let write = write_policy();

    register(&c1, COUNTER, package).await;

    let refused = c1.execute_udf(&write, &key, package, "merge_counter", &[]).await;
    let text = format!("{refused:?}");

    assert!(text.contains("is an event handler"), "{text}");

    // Asked twice, from the module's remembered answer.
    let again = format!("{:?}", c1.execute_udf(&write, &key, package, "merge_counter", &[Value::from(1)]).await);

    assert!(again.contains("is an event handler"), "{again}");

    c1.execute_udf(&write, &key, package, "increment", &[Value::from("dc1"), Value::from(2)]).await.unwrap();
    assert!(matches!(c1.execute_udf(&write, &key, package, "value", &[]).await, Ok(Some(Value::Int(2)))));

    let _ = c1.delete(&write, &key).await;
    let _ = c1.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

//==========================================================
// Fuzzing.
//

/// What a binding should look like in `event-list`, by the commands that were answered `ok`.
#[derive(Clone, Debug, PartialEq)]
struct Bound {
    module: String,
    function: String,
    on_failure: String,
    timeout: String,
    order: String,
}

/// Random `event-set` and `event-remove` commands, well-formed and not, on a few sets of the namespace
/// and on the namespace itself. After each one, `event-list` must show exactly the bindings the `ok`
/// answers made, as they were asked for, with the defaults filled in; and a refused command changes
/// nothing. Writes ship to the sets throughout, so the bindings change under merges.
#[aerospike_macro::test]
async fn fuzz_binding_commands() {
    let Some((c1, c2)) = clients("fuzz_binding_commands").await else { return };
    let _guard = ns_lock().lock().await;
    let mut rng = Rng(seed_for("fuzz_binding_commands"));
    let probe = &format!("probe_{}", common::rand_str(6));
    let counter_pkg = &format!("counter_{}", common::rand_str(6));
    let tag = common::rand_str(4);
    let sets: Vec<String> = (0..3).map(|i| format!("cmd{i}_{tag}")).collect();
    let keys: Vec<Key> = sets.iter().map(|s| as_key!(NS, s.as_str(), 1)).collect();
    let mut model: BTreeMap<(String, String), Bound>;

    register(&c2, PROBE, probe).await;
    register(&c2, COUNTER, counter_pkg).await;

    for key in &keys {
        seed_record(&c1, &c2, key, &[as_bin!("base", 1)]).await;
    }

    let long_set = "s".repeat(70);
    let set_choices: Vec<&str> = sets.iter().map(String::as_str).chain(["", long_set.as_str()]).collect();
    let modules = [probe.as_str(), counter_pkg.as_str(), "none", "nope", ""];
    let functions = ["probe", "merge_counter", "increment", "none", "", "__udf_sig_probe"];
    let on_failures = ["", "policy", "abandon", "fail", "x"];
    let timeouts = ["", "0", "100", "60000", "60001", "-1", "abc"];
    let orders = ["", "0", "7", "1000001", "x"];
    let extras = ["", ";trigger=conflict", ";exp=AAA", ";name=alt", ";version=2"];
    let n_rounds = rounds(200);
    let writes = std::sync::atomic::AtomicBool::new(true);
    let as_bound = |rows: &BTreeMap<(String, String), HashMap<String, String>>| -> BTreeMap<(String, String), Bound> {
        rows.iter()
            .map(|(k, f)| {
                (
                    k.clone(),
                    Bound {
                        module: f["module"].clone(),
                        function: f["function"].clone(),
                        on_failure: f["on-failure"].clone(),
                        timeout: f["timeout"].clone(),
                        order: f["order"].clone(),
                    },
                )
            })
            .collect()
    };

    // Whatever an earlier run left bound in the namespace is where the model starts.
    model = as_bound(&rows(&c2).await);

    // Writes that ship to the sets throughout, so every binding change happens under merges.
    let writer = async {
        let mut i = 0i64;

        while writes.load(std::sync::atomic::Ordering::Relaxed) {
            let key = &keys[(i as usize) % keys.len()];
            let answer = ["r-ok", "take-remote", "keep-local", "trap", "policy"][(i as usize) % 5];

            let _ = c1.put(&write_policy(), key, &[as_bin!("do", answer), as_bin!("n", i)]).await;
            i += 1;
            aerospike_rt::sleep(Duration::from_millis(5)).await;
        }

        i
    };

    let commander = async {
        for round in 0..n_rounds {
            let set = *rng.pick(&set_choices);
            let remove = rng.below(5) == 0;
            let mut command;
            let mut asked: Option<(String, Bound)> = None;

            if remove {
                command = format!("event-remove:event=xdr-merge;namespace={NS};set={set}");

                if rng.below(3) == 0 {
                    command.push_str(";name=alt");
                }
            } else {
                let module = *rng.pick(&modules);
                let function = *rng.pick(&functions);
                let on_failure = *rng.pick(&on_failures);
                let timeout = *rng.pick(&timeouts);
                let order = *rng.pick(&orders);
                let extra = *rng.pick(&extras);

                command = format!("event-set:event=xdr-merge;namespace={NS};set={set};module={module}");

                if !function.is_empty() || rng.below(2) == 0 {
                    command.push_str(&format!(";function={function}"));
                }

                for (k, v) in [("on-failure", on_failure), ("timeout", timeout), ("order", order)] {
                    if !v.is_empty() {
                        command.push_str(&format!(";{k}={v}"));
                    }
                }

                command.push_str(extra);

                let name = if extra == ";name=alt" { "alt".to_string() } else if module == "none" { "none".to_string() } else { function.to_string() };

                asked = Some((
                    name,
                    Bound {
                        module: module.to_string(),
                        function: if module == "none" { "none".to_string() } else { function.to_string() },
                        on_failure: if on_failure.is_empty() { "abandon".to_string() } else { on_failure.to_string() },
                        timeout: if timeout.is_empty() { "0".to_string() } else { timeout.to_string() },
                        order: if order.is_empty() { "0".to_string() } else { order.to_string() },
                    },
                ));
            }

            let before = as_bound(&rows(&c2).await);
            let answer = info(&c2, &command).await;
            let after = as_bound(&rows(&c2).await);

            if answer == "ok" {
                match &asked {
                    Some((name, bound)) => {
                        // One binding per scope: an ok replaces the scope's binding of this name.
                        model.insert((set.to_string(), name.clone()), bound.clone());

                        assert!(bound.module != "nope" && !bound.module.is_empty(), "round {round}: {command} was taken");
                        assert!(!command.contains("trigger=") && !command.contains("exp="), "round {round}: {command} was taken");
                        assert!(["policy", "abandon"].contains(&bound.on_failure.as_str()), "round {round}: {command} was taken");
                    }
                    None => {
                        let by_name = command.contains(";name=alt");

                        model.retain(|(s, n), _| s != set || (by_name && n != "alt"));
                    }
                }
            } else {
                assert!(answer.starts_with("ERROR") || answer.starts_with("error"), "round {round}: {command}: {answer}");
                assert_eq!(before, after, "round {round}: a refused command changed the bindings: {command}: {answer}");
            }

            assert_eq!(after, model, "round {round}: after {command} answered {answer}");

            // Room for writes to ship and merge under this binding before the next change.
            aerospike_rt::sleep(Duration::from_millis(rng.below(40))).await;
        }

        writes.store(false, std::sync::atomic::Ordering::Relaxed);
    };

    let ((), written) = futures::join!(commander, writer);

    eprintln!("  {n_rounds} commands under {written} writes");
    alive(&c1, &c2, "after the commands").await;

    let _ = info(&c2, &format!("event-remove:event=xdr-merge;namespace={NS};set=")).await;

    for set in &sets {
        cleanup(&c1, &c2, set, &keys, &[]).await;
    }

    for package in [probe, counter_pkg] {
        let _ = c2.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
    }
}

/// Every answer the probe gives, for the fuzzing to choose from.
const ANSWERS: &[&str] = &[
    "trap", "fail", "nonzero", "silent", "int", "nil", "list", "m-empty", "m-verdict-int", "m-verdict-nul",
    "banana", "KEEP-LOCAL", "record", "r-nobins", "r-bins-int", "r-bins-list", "r-bins-nil", "policy",
    "keep-local", "take-remote", "m-keep", "m-take", "m-take-ttl", "delete", "m-delete", "r-ok", "r-kinds",
    "r-15", "r-ttl", "r-deep", "r-big", "r-many", "r-ttl-str", "r-long", "r-nul", "r-noname", "r-intkey",
    "r-geo-bad", "r-empty", "r-nil", "spin",
];

/// Random answers to random writes and deletes, under each on-failure. Whatever the answers, the counters
/// must add up - every run is one outcome or one failure, and every failure one cause - and what the
/// source counts as abandoned must be exactly what the destination abandoned.
#[aerospike_macro::test]
async fn fuzz_answers_against_the_counters() {
    let Some((c1, c2)) = clients("fuzz_answers_against_the_counters").await else { return };
    let _guard = ns_lock().lock().await;
    let mut rng = Rng(seed_for("fuzz_answers_against_the_counters"));
    let package = &format!("probe_{}", common::rand_str(6));
    let write = write_policy();

    register(&c2, PROBE, package).await;

    for on_failure in ["abandon", "policy"] {
        let set_name = &format!("ans_{on_failure}_{}", common::rand_str(4));
        let keys: Vec<Key> = (0..6).map(|i| as_key!(NS, set_name.as_str(), i as i64)).collect();

        for key in &keys {
            seed_record(&c1, &c2, key, &[as_bin!("base", 1)]).await;
        }

        bind(&c2, set_name, package, "probe", &format!(";on-failure={on_failure};timeout=150")).await;
        shipped(&c1, NS).await;

        let abandoned_0 = xdr_stat(&c1, NS, "merge_abandoned").await;
        let n_rounds = rounds(80);
        let mut spins = 0;

        for _ in 0..n_rounds {
            let key = rng.pick(&keys);

            match rng.below(10) {
                // A delete, then the record back, so the next write is a merge again.
                0 => {
                    let _ = c1.delete(&write, key).await;
                }
                1 => {
                    let _ = c1.put(&write, key, &[as_bin!("base", 1)]).await;
                }
                _ => {
                    // A spin now and then on top of the menu's own, so that deadlines are reached.
                    let answer = if rng.below(12) == 0 { "spin" } else { *rng.pick(ANSWERS) };
                    let n = [0i64, 1, 7, 30, 64, 100, -1, 40000][rng.below(8) as usize];

                    if answer == "spin" {
                        spins += 1;
                        // Shipped alone, not folded into the next write to the record.
                        aerospike_rt::sleep(Duration::from_millis(50)).await;
                    }

                    let _ = c1.put(&write, key, &[as_bin!("do", answer), as_bin!("n", n)]).await;

                    if answer == "spin" {
                        aerospike_rt::sleep(Duration::from_millis(300)).await;
                    }
                }
            }

            if rng.below(6) == 0 {
                aerospike_rt::sleep(Duration::from_millis(200)).await;
            }
        }

        shipped(&c1, NS).await;
        alive(&c1, &c2, &format!("after the {on_failure} answers")).await;

        let c = counters(&c2, set_name).await;
        let outcomes = ["keep-local", "take-remote", "delete", "took-incoming", "combined", "policy", "fallback"];
        let sum: u64 = outcomes.iter().map(|o| c[*o]).sum();

        eprintln!("  {on_failure}: {c:?}");
        assert_eq!(c["fired"], sum, "{on_failure}: every run is one outcome: {c:?}");
        assert_eq!(c["fallback"], c["fail-timeout"] + c["fail-error"] + c["fail-answer"], "{on_failure}: {c:?}");
        assert!(c["fallback"] > 0 && c["combined"] > 0, "{on_failure}: the fuzz reached failures and merges: {c:?}");
        assert!(spins == 0 || c["fail-timeout"] > 0, "{on_failure}: {spins} spins, and no timeout counted: {c:?}");

        let abandoned = xdr_stat(&c1, NS, "merge_abandoned").await - abandoned_0;

        if on_failure == "abandon" {
            assert_eq!(c["abandoned"], c["fallback"], "{c:?}");
        } else {
            assert_eq!(c["abandoned"], 0, "{c:?}");
        }

        assert_eq!(abandoned, c["abandoned"], "{on_failure}: dc1 counts exactly what dc2 abandoned");

        cleanup(&c1, &c2, set_name, &keys, &[]).await;
    }

    let _ = c2.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

async fn counter_value(client: &Client, key: &Key, package: &str) -> Option<i64> {
    match client.execute_udf(&write_policy(), key, package, "value", &[]).await {
        Ok(Some(Value::Int(n))) => Some(n),
        _ => None,
    }
}

/// The races, at random. Both clusters count on several records of the three hold namespaces (dc1 holds
/// its ships for three seconds there), while dc1's link for a namespace goes and comes back with a rewind
/// and the bindings are replaced under the writes. Every record must end on the sum of what was added to
/// it, on both sides, with the same bins.
#[aerospike_macro::test]
async fn fuzz_counters_through_holds_outages_and_rebinding() {
    let Some((c1, c2)) = clients("fuzz_counters_through_holds_outages_and_rebinding").await else { return };
    let _guard = ns_lock().lock().await;
    let seed = seed_for("fuzz_counters_through_holds_outages_and_rebinding");
    let mut rng = Rng(seed);
    let package = &format!("counter_{}", common::rand_str(6));
    let set_name = &format!("race_{}", common::rand_str(6));
    let write = write_policy();
    let namespaces = ["holdrec", "holdch", "holdbc"];
    let keys: Vec<(usize, Key)> =
        (0..9).map(|i| (i % 3, as_key!(namespaces[i % 3], set_name.as_str(), i as i64))).collect();
    let mut expected = vec![0i64; keys.len()];
    let mut down = [false; 3];
    let bind_in = |namespace: &str| {
        format!("event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function=merge_counter")
    };

    for c in [&c1, &c2] {
        register(c, COUNTER, package).await;

        for namespace in namespaces {
            assert_eq!(info(c, &bind_in(namespace)).await, "ok");
        }
    }

    let n_rounds = rounds(60);

    for round in 0..n_rounds {
        let (ns_ix, key) = &keys[rng.below(keys.len() as u64) as usize];
        let k = keys.iter().position(|(_, kk)| kk == key).unwrap();

        match rng.below(12) {
            // The link for a namespace goes, or comes back with a rewind.
            0 => {
                let ns = namespaces[*ns_ix];
                let dc_ns = format!("set-config:context=xdr;dc=dc2;namespace={ns}");

                if down[*ns_ix] {
                    info_ok(&c1, &format!("{dc_ns};action=add;rewind=all")).await;
                } else {
                    info_ok(&c1, &format!("{dc_ns};action=remove")).await;
                }

                down[*ns_ix] = !down[*ns_ix];
            }
            // A binding replaced by itself under the writes: a new binding each time, counters carried.
            1 => {
                let c = if rng.below(2) == 0 { &c1 } else { &c2 };

                assert_eq!(info(c, &bind_in(namespaces[*ns_ix])).await, "ok", "round {round}");
            }
            // One increment on each side at once, perhaps on the same record.
            _ => {
                let n1 = rng.below(9) as i64 + 1;
                let n2 = rng.below(9) as i64 + 1;
                let (_, other) = &keys[rng.below(keys.len() as u64) as usize];
                let k2 = keys.iter().position(|(_, kk)| kk == other).unwrap();

                expected[k] += n1;
                expected[k2] += n2;

                let (a1, a2) = ([Value::from("dc1"), Value::from(n1)], [Value::from("dc2"), Value::from(n2)]);
                let (r1, r2) = futures::join!(
                    c1.execute_udf(&write, key, package, "increment", &a1),
                    c2.execute_udf(&write, other, package, "increment", &a2)
                );

                r1.unwrap_or_else(|e| panic!("seed {seed} round {round}: dc1 increment: {e:?}"));
                r2.unwrap_or_else(|e| panic!("seed {seed} round {round}: dc2 increment: {e:?}"));
            }
        }

        if rng.below(5) == 0 {
            aerospike_rt::sleep(Duration::from_millis(rng.below(3500))).await;
        }
    }

    // Every link back, with a rewind, so recovery ships what the outage held.
    for (i, ns) in namespaces.iter().enumerate() {
        if down[i] {
            info_ok(&c1, &format!("set-config:context=xdr;dc=dc2;namespace={ns};action=add;rewind=all")).await;
        }
    }

    for (k, (_, key)) in keys.iter().enumerate() {
        let deadline = Instant::now() + RECOVERY_LAG;

        loop {
            let (v1, v2) = (counter_value(&c1, key, package).await, counter_value(&c2, key, package).await);

            if v1.unwrap_or(0) == expected[k] && v2.unwrap_or(0) == expected[k] {
                break;
            }

            assert!(
                Instant::now() < deadline,
                "seed {seed}: {key:?} should count {}: dc1 says {v1:?}, dc2 says {v2:?}",
                expected[k]
            );
            aerospike_rt::sleep(Duration::from_millis(250)).await;
        }

        let (b1, b2) = (bins(&c1, key).await, bins(&c2, key).await);

        assert_eq!(b1, b2, "seed {seed}: {key:?} holds different bins on the two clusters");
    }

    alive(&c1, &c2, "after the races").await;

    for c in [&c1, &c2] {
        for namespace in namespaces {
            let _ = info(c, &format!("event-remove:event=xdr-merge;namespace={namespace};set={set_name}")).await;
        }

        for (_, key) in &keys {
            let _ = c.delete(&write, key).await;
        }

        let _ = c.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
    }
}

/// Random client calls to the exports of a module that is bound: every handler is refused before it
/// runs, whatever the arguments, and every other function answers as it would anywhere.
#[aerospike_macro::test]
async fn fuzz_client_calls_to_handlers() {
    let Some((c1, _c2)) = clients("fuzz_client_calls_to_handlers").await else { return };
    let _guard = ns_lock().lock().await;
    let mut rng = Rng(seed_for("fuzz_client_calls_to_handlers"));
    let package = &format!("counter_{}", common::rand_str(6));
    let set_name = &format!("calls_{}", common::rand_str(6));
    let key = as_key!(NS, set_name.as_str(), 1);
    let write = write_policy();
    let args: Vec<Vec<Value>> = vec![
        vec![],
        vec![Value::from(1)],
        vec![Value::from("dc1"), Value::from(3)],
        vec![Value::HashMap(HashMap::new()), Value::HashMap(HashMap::new())],
        vec![Value::Nil, Value::Nil, Value::Nil],
    ];
    let mut total = 0i64;

    register(&c1, COUNTER, package).await;
    bind(&c1, set_name, package, "merge_counter", "").await;

    for round in 0..rounds(60) {
        let function = *rng.pick(&["merge_counter", "merge_counter", "increment", "value", "__udf_sig_merge_counter"]);
        let arguments = rng.pick(&args);
        let answer = c1.execute_udf(&write, &key, package, function, arguments).await;
        let text = format!("{answer:?}");

        match function {
            "merge_counter" => assert!(text.contains("is an event handler"), "round {round}: {arguments:?}: {text}"),
            "__udf_sig_merge_counter" => assert!(text.contains("event xdr-merge/"), "round {round}: {text}"),
            "increment" if arguments.len() == 2 && arguments[0] == Value::from("dc1") => {
                assert!(answer.is_ok(), "round {round}: {text}");
                total += 3;
            }
            "value" => {
                assert!(!text.contains("is an event handler"), "round {round}: {text}");

                // `value` takes no arguments; asked with some, it refuses them as any UDF would.
                if total > 0 && arguments.is_empty() {
                    assert!(matches!(answer, Ok(Some(Value::Int(n))) if n == total), "round {round}: {text}");
                }
            }
            _ => assert!(!text.contains("is an event handler"), "round {round}: {function}: {text}"),
        }
    }

    let _ = unbind(&c1, set_name).await;
    let _ = c1.delete(&write, &key).await;
    let _ = c1.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}
