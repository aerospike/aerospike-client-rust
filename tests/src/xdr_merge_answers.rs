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

//! Every answer an `xdr-merge` handler can give, through the server's dry run, on one node.
//!
//! `tests/src/xdr_merge_limits.rs` gives the same answers inside real XDR writes, which needs two
//! clusters. This file needs one node: the dry run (`execute_udf(key, "__event__", "xdr-merge", ...)`)
//! runs the bound handler against the stored record and a described incoming one, takes its answer
//! through the same checks a live merge does (`interpret`, `bins_are_bins`, the verdict and the time to
//! live), and merges again with the sides swapped, with the incoming record again and, given a third
//! record, both ways round with that one. So it reaches the server's handling of a hostile answer
//! without XDR - which is what makes it the test to run under a sanitizer build of the server.
//!
//! The handler is `merge_probe` (`aerospike-udf-sdk-wasm/examples/merge_probe`), which answers whatever
//! the incoming record's `do` bin names, using its `n` bin as a number. Every name it knows is given, on
//! `test` and on the bin-convergence namespace `testbc`, with and without a third record. The test holds
//! the server to an outcome for each, and to answering at all after each.
//!
//! Without the event system (or WASM) on the node, the tests report it on stderr and return.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use aerospike::Task;
use aerospike::*;

use crate::common;

const PROBE: &[u8] = include_bytes!("../fixtures/wasm/merge_probe.wasm");

/// The namespace the node keeps with bin convergence on.
const BIN_CONVERGENCE_NS: &str = "testbc";

/// What the dry run should come to for an answer.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Dry {
    /// The handler failed, or its answer is not one the server takes: the dry run fails with the reason.
    Failed,
    /// The handler deferred to the namespace's policy, which the dry run reports as a failure of its own.
    Policy,
    /// `{verdict: keep-local}`.
    KeepLocal,
    /// `{verdict: record}`, the bins a write would store.
    Record,
    /// `{verdict: delete}`.
    Delete,
}

impl Dry {
    fn admits(self, got: Option<Dry>) -> bool {
        got == Some(self)
    }
}

/// Every name `merge_probe` answers to, the number it is given, and what the dry run should come to.
/// `None` is a record with no `do` bin, which the probe answers with `policy`.
const CASES: &[(Option<&str>, i64, Dry)] = &[
    // No answer.
    (Some("trap"), 0, Dry::Failed),
    (Some("spin"), 0, Dry::Failed),
    (Some("fail"), 0, Dry::Failed),
    (Some("nonzero"), 0, Dry::Failed),
    (Some("silent"), 0, Dry::Failed),
    // An answer of the wrong shape.
    (Some("int"), 0, Dry::Failed),
    (Some("nil"), 0, Dry::Failed),
    (Some("list"), 0, Dry::Failed),
    (Some("m-empty"), 0, Dry::Failed),
    (Some("m-verdict-int"), 0, Dry::Failed),
    (Some("m-verdict-nul"), 0, Dry::Failed),
    (Some("banana"), 0, Dry::Failed),
    (Some(""), 0, Dry::Failed),
    (Some("KEEP-LOCAL"), 0, Dry::Failed),
    (Some("record"), 0, Dry::Failed),
    (Some("r-nobins"), 0, Dry::Failed),
    (Some("r-bins-int"), 0, Dry::Failed),
    (Some("r-bins-list"), 0, Dry::Failed),
    (Some("r-bins-nil"), 0, Dry::Failed),
    // The verdicts.
    (Some("policy"), 0, Dry::Policy),
    (None, 0, Dry::Policy),
    (Some("not-a-probe-answer"), 0, Dry::Policy),
    (Some("keep-local"), 0, Dry::KeepLocal),
    (Some("m-keep"), 0, Dry::KeepLocal),
    (Some("take-remote"), 0, Dry::Record),
    (Some("m-take"), 0, Dry::Record),
    (Some("m-take-ttl"), 100, Dry::Record),
    (Some("delete"), 0, Dry::Delete),
    (Some("m-delete"), 0, Dry::Delete),
    // A record.
    (Some("r-ok"), 7, Dry::Record),
    (Some("r-kinds"), 0, Dry::Record),
    (Some("r-15"), 0, Dry::Record),
    (Some("r-ttl"), 100, Dry::Record),
    (Some("r-ttl"), 0, Dry::Record),
    (Some("r-ttl"), 0xffff_ffff, Dry::Record),
    (Some("r-deep"), 30, Dry::Record),
    (Some("r-deep"), 60, Dry::Record),
    (Some("r-big"), 64, Dry::Record),
    (Some("r-many"), 100, Dry::Record),
    // More bins than a record holds, but each one small: `bins_are_bins` counts bytes, not bins, so the
    // dry run takes it, and only a write would find out - if the handler builds it within its timeout.
    // The probe makes a handle for every key, so the call's 256 run out early and the rest of its puts are
    // refused; it answers nothing, and the server says why ("answered nothing after the server refused
    // ... requests, the first for want of the call's 256 handles").
    (Some("r-many"), 40000, Dry::Failed),
    // A record that cannot be stored as it is given.
    (Some("r-ttl"), -1, Dry::Failed),
    (Some("r-ttl"), 0x1_0000_0000, Dry::Failed),
    (Some("r-ttl"), i64::MIN, Dry::Failed),
    (Some("m-take-ttl"), -5, Dry::Failed),
    (Some("r-ttl-str"), 0, Dry::Failed),
    (Some("r-long"), 0, Dry::Failed),
    (Some("r-nul"), 0, Dry::Failed),
    (Some("r-noname"), 0, Dry::Failed),
    (Some("r-intkey"), 0, Dry::Failed),
    // Larger than max-record-size (1 MiB on these nodes), refused by `bins_are_bins` before a write; and
    // 20 MiB, which the handler does not finish building within the binding's timeout.
    (Some("r-big"), 1100, Dry::Failed),
    (Some("r-big"), 20000, Dry::Failed),
    (Some("r-geo-bad"), 0, Dry::Failed),
    (Some("r-empty"), 0, Dry::Failed),
    (Some("r-nil"), 0, Dry::Failed),
];

/// Register a module, or say why this server cannot.
async fn register(client: &Client, package: &str) -> std::result::Result<(), String> {
    match client
        .register_udf(&AdminPolicy::default(), PROBE, &format!("{package}.wasm"), UdfLang::Wasm)
        .await
    {
        Ok(task) => task
            .wait_till_complete(None)
            .await
            .map(|_| ())
            .map_err(|e| format!("the module did not become available: {e}")),
        Err(e) => Err(format!("this server does not take WASM modules: {e}")),
    }
}

async fn info(client: &Client, command: &str) -> String {
    let answers = client.info(&AdminPolicy::default(), &[command]).await.unwrap();

    answers
        .get(command)
        .cloned()
        .unwrap_or_else(|| panic!("no answer to {command:?}: {answers:?}"))
}

/// Whether this node has the event system and lets `xdr-merge` be bound.
async fn supported(client: &Client) -> std::result::Result<(), String> {
    let answer = info(client, "event-list").await;

    if answer.contains("not found") || answer.contains("feature key") || answer.contains("enterprise only") {
        return Err(format!("no event system on this node: {answer}"));
    }

    Ok(())
}

/// The node answers, which is all "did not stop" can mean from here.
async fn alive(client: &Client, when: &str) {
    let build = client.info(&AdminPolicy::default(), &["build"]).await;

    assert!(build.is_ok(), "the node does not answer {when}: {build:?}");
}

fn bins_of(what: Option<&str>, n: i64, extra: &str) -> HashMap<Value, Value> {
    let mut bins = HashMap::from([(Value::from("n"), Value::from(n)), (Value::from(extra), Value::from(1))]);

    if let Some(what) = what {
        bins.insert(Value::from("do"), Value::from(what));
    }

    bins
}

/// A side's metadata. On the bin-convergence namespace it carries per-bin times and sources too, as an
/// incoming write there does.
fn meta_of(bins: &HashMap<Value, Value>, src_id: i64, lut: i64, per_bin: bool) -> HashMap<Value, Value> {
    let mut meta = HashMap::from([(Value::from("ttl"), Value::from(0)), (Value::from("src-id"), Value::from(src_id))]);

    if per_bin {
        let luts: HashMap<Value, Value> = bins.keys().map(|k| (k.clone(), Value::from(lut))).collect();
        let srcs: HashMap<Value, Value> = bins.keys().map(|k| (k.clone(), Value::from(src_id))).collect();

        meta.insert(Value::from("bin-luts"), Value::from(luts));
        meta.insert(Value::from("bin-srcs"), Value::from(srcs));
    }

    meta
}

fn map_of(v: &Value) -> HashMap<Value, Value> {
    match v {
        Value::HashMap(m) => m.clone(),
        Value::SortedMap(m) => m.clone().into_iter().collect(),
        Value::OrderedMap(m) => m.clone().into_iter().collect(),
        other => panic!("not a map: {other:?}"),
    }
}

/// What one dry run came to, and the line the table shows for it.
fn classify(result: &aerospike::Result<Option<Value>>) -> (Option<Dry>, String) {
    match result {
        Ok(Some(v)) => {
            let answer = map_of(v);
            let verdict = match answer.get(&Value::from("verdict")) {
                Some(Value::String(s)) => s.clone(),
                other => return (None, format!("no verdict: {other:?}")),
            };
            let laws: Vec<String> = ["symmetric", "idempotent", "associative"]
                .iter()
                .filter_map(|law| answer.get(&Value::from(*law)).map(|v| format!("{law}={v}")))
                .collect();
            let n_bins = answer.get(&Value::from("bins")).map_or(0, |b| map_of(b).len());
            let dry = match verdict.as_str() {
                "keep-local" => Some(Dry::KeepLocal),
                "record" => Some(Dry::Record),
                "delete" => Some(Dry::Delete),
                _ => None,
            };

            (dry, format!("{verdict} ({n_bins} bins) {}", laws.join(" ")))
        }
        Ok(None) => (None, "no answer".into()),
        Err(e) => {
            let detail = format!("{e}");

            if detail.contains("deferred to the policy") {
                (Some(Dry::Policy), "policy".into())
            } else if matches!(e.kind(), ErrorKind::UdfBadResponse) {
                let short: String = e.base_message().chars().take(200).collect();

                (Some(Dry::Failed), format!("failed: {short}"))
            } else {
                (None, format!("{e:?}"))
            }
        }
    }
}

/// Every answer, on one namespace, with or without a third record for the associativity check.
async fn answers_on(client: &Client, namespace: &str, package: &str, third: bool) -> Vec<String> {
    let set_name = &format!("answers_{}", common::rand_str(6));
    let per_bin = namespace == BIN_CONVERGENCE_NS;
    let write = WritePolicy::default();
    let mut wrong = Vec::new();
    let mut table = String::new();

    // The handler that never answers costs this much a merge.
    let bind = format!(
        "event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function=probe;on-failure=policy;timeout=300"
    );

    assert_eq!(info(client, &bind).await, "ok", "{bind}");

    for (i, (what, n, want)) in CASES.iter().enumerate() {
        let key = as_key!(namespace, set_name.as_str(), i as i64);

        client.put(&write, &key, &[as_bin!("base", 1)]).await.unwrap();

        let incoming = bins_of(*what, *n, "inc");
        let mut args = vec![Value::from(incoming.clone()), Value::from(meta_of(&incoming, 2, 1_900_000_000_000, per_bin))];

        if third {
            let other = bins_of(*what, *n, "oth");
            let other_meta = meta_of(&other, 3, 1_900_000_000_500, per_bin);

            args.push(Value::from(other));
            args.push(Value::from(other_meta));
        }

        let started = Instant::now();
        let result = client.execute_udf(&write, &key, "__event__", "xdr-merge", &args).await;
        let took = started.elapsed();
        let (got, line) = classify(&result);

        table.push_str(&format!("  {namespace:6} {third:5} {:20} {n:20} -> {line}\n", format!("{what:?}")));

        if !want.admits(got) {
            wrong.push(format!("{namespace} third={third} {what:?}({n}): wanted {want:?}, got {line}"));
        }

        // A handler that answers nothing because the server refused it room is told so: the failure names
        // the ceiling, not "neither a verdict nor a map".
        if *what == Some("r-many") && *n > 1000 && !line.contains("the first for want of the call's 256 handles") {
            wrong.push(format!("{namespace} third={third} {what:?}({n}): the failure does not say why: {line}"));
        }

        // A handler that never answers is stopped by the binding's timeout, not by the client's.
        if took > Duration::from_secs(5) {
            wrong.push(format!("{namespace} third={third} {what:?}({n}): took {took:?}"));
        }

        alive(client, &format!("after {what:?}({n}) on {namespace}")).await;

        let _ = client.delete(&write, &key).await;
    }

    eprintln!("{table}");

    // A dry run is not a merge: the binding's counters still read zero.
    let rows = info(client, "event-list").await;
    let row = rows.split(';').find(|r| r.contains(&format!(":set={set_name}:"))).unwrap_or("").to_string();

    if !row.contains(":fired=0:") {
        wrong.push(format!("{namespace}: a dry run counted as a firing: {row}"));
    }

    let unbind = format!("event-remove:event=xdr-merge;namespace={namespace};set={set_name}");

    assert_eq!(info(client, &unbind).await, "ok");

    wrong
}

#[aerospike_macro::test]
async fn every_answer_through_the_dry_run() {
    let client = common::client().await;

    if let Err(reason) = supported(&client).await {
        eprintln!("skipped every_answer_through_the_dry_run: {reason}");
        return;
    }

    let package = &format!("merge_probe_{}", common::rand_str(6));

    if let Err(reason) = register(&client, package).await {
        eprintln!("skipped every_answer_through_the_dry_run: {reason}");
        return;
    }

    let namespaces = info(&client, "namespaces").await;
    let mut wrong = Vec::new();

    for namespace in [common::namespace(), BIN_CONVERGENCE_NS] {
        if !namespaces.split(';').any(|n| n == namespace) {
            eprintln!("every_answer_through_the_dry_run: no namespace {namespace} on this node, not run there");
            continue;
        }

        for third in [false, true] {
            wrong.extend(answers_on(&client, namespace, package, third).await);
        }
    }

    alive(&client, "at the end").await;

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;

    assert!(wrong.is_empty(), "\n  {}", wrong.join("\n  "));
}

/// The server asks a function what it is before binding it: its signature export answers the address of
/// a string in the module's memory. The probe has exports that answer everything else - no address, one
/// outside the memory, the last byte of it, a string far too long, control characters, a loop, a trap -
/// and binding any of them is refused, quickly, in the server's own words.
#[aerospike_macro::test]
async fn a_signature_that_is_not_one_is_refused_on_one_node() {
    let client = common::client().await;

    if let Err(reason) = supported(&client).await {
        eprintln!("skipped a_signature_that_is_not_one_is_refused_on_one_node: {reason}");
        return;
    }

    let package = &format!("merge_probe_{}", common::rand_str(6));

    if let Err(reason) = register(&client, package).await {
        eprintln!("skipped a_signature_that_is_not_one_is_refused_on_one_node: {reason}");
        return;
    }

    let namespace = common::namespace();
    let set_name = &format!("sig_{}", common::rand_str(6));

    for function in [
        "s_zero", "s_wild", "s_negative", "s_edge", "s_control", "s_long", "s_empty", "s_other", "s_version",
        "s_noversion", "s_bigversion", "s_plain", "s_spin", "s_trap", "no_such_function",
    ] {
        let started = Instant::now();
        let command =
            format!("event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function={function}");
        let answer = format!("{:?}", client.info(&AdminPolicy::default(), &[command.as_str()]).await);

        assert!(!answer.contains("\"ok\""), "{function} was bound: {answer}");
        assert!(!answer.contains("\\n") && !answer.contains("\\u{1b}"), "{function}: control characters in {answer}");
        assert!(started.elapsed() < Duration::from_secs(10), "{function} took {:?} to be refused", started.elapsed());
        alive(&client, &format!("after binding {function}")).await;
    }

    assert!(!info(&client, "event-list").await.contains(&format!(":set={set_name}:")), "a refused binding left a row");

    // A right signature on a function the module does not have binds, since the signature is all that is
    // asked; the dry run then fails as any handler's failure does.
    let command =
        format!("event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function=s_ghost");

    if info(&client, &command).await == "ok" {
        let key = as_key!(namespace, set_name.as_str(), 1);
        let write = WritePolicy::default();

        client.put(&write, &key, &[as_bin!("base", 1)]).await.unwrap();

        let incoming = HashMap::from([(Value::from("x"), Value::from(1))]);
        let meta = HashMap::from([(Value::from("src-id"), Value::from(2))]);
        let result = client
            .execute_udf(&write, &key, "__event__", "xdr-merge", &[Value::from(incoming), Value::from(meta)])
            .await;

        assert!(result.is_err(), "a handler that is not there answered: {result:?}");

        let _ = client.delete(&write, &key).await;
        let _ = info(&client, &format!("event-remove:event=xdr-merge;namespace={namespace};set={set_name}")).await;
    }

    alive(&client, "after every signature").await;

    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}

/// A stored boolean bin through a merge that keeps it: `take-remote` overlays the incoming bins on the
/// current ones, and `keep-local` copies the current ones, so the answer holds the current record's own
/// values. The common library's `as_val_reserve` does nothing for a boolean - it takes every boolean for
/// one of the two static ones - but the server read a stored boolean into a fresh heap one
/// (`bool_to_asval`), so the copy and the current record each freed the one value: a use-after-free,
/// and on a normal build a double free that could stop the node at the dry run's reply. The merge code
/// now shares booleans as the two static values. Run it against a sanitizer build to see it stay clean.
#[aerospike_macro::test]
async fn a_boolean_bin_survives_the_merge() {
    let client = common::client().await;

    if let Err(reason) = supported(&client).await {
        eprintln!("skipped a_boolean_bin_survives_the_merge: {reason}");
        return;
    }

    let package = &format!("merge_probe_{}", common::rand_str(6));

    if let Err(reason) = register(&client, package).await {
        eprintln!("skipped a_boolean_bin_survives_the_merge: {reason}");
        return;
    }

    let namespace = common::namespace();
    let set_name = &format!("bool_{}", common::rand_str(6));
    let write = WritePolicy::default();
    let bind = format!("event-set:event=xdr-merge;namespace={namespace};set={set_name};module={package};function=probe");

    assert_eq!(info(&client, &bind).await, "ok", "{bind}");

    for (i, what) in ["take-remote", "keep-local", "m-take"].iter().enumerate() {
        let key = as_key!(namespace, set_name.as_str(), i as i64);

        client.put(&write, &key, &[as_bin!("base", 1), as_bin!("flag", Value::Bool(true))]).await.unwrap();

        let incoming = bins_of(Some(what), 0, "inc");
        let meta = meta_of(&incoming, 2, 1_900_000_000_000, false);
        let result = client
            .execute_udf(&write, &key, "__event__", "xdr-merge", &[Value::from(incoming), Value::from(meta)])
            .await;
        let bins = match &result {
            Ok(Some(v)) => map_of(&map_of(v).get(&Value::from("bins")).cloned().unwrap_or(Value::Nil)),
            other => panic!("{what}: {other:?}"),
        };

        assert_eq!(bins.get(&Value::from("flag")), Some(&Value::Bool(true)), "{what}: {bins:?}");
        alive(&client, &format!("after {what} over a boolean bin")).await;

        let _ = client.delete(&write, &key).await;
    }

    let _ = info(&client, &format!("event-remove:event=xdr-merge;namespace={namespace};set={set_name}")).await;
    let _ = client.remove_udf(&AdminPolicy::default(), &format!("{package}.wasm")).await;
}
