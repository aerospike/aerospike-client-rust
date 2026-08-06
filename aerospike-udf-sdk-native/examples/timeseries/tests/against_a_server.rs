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

//! The module against a real server, where the blob actually crosses the wire.
//!
//! What these reach that the unit tests cannot: the ring being written into a particle and read back out of one, the
//! result maps being built by the host, and the rollup's partials being merged across query threads.

use std::collections::HashMap;
use std::env;

use aerospike_core::{
    as_bin, as_key, as_val, Bins, Client, ClientPolicy, PartitionFilter, QueryPolicy, ReadPolicy,
    Statement, Value, WritePolicy,
};
use futures::StreamExt;

use aerospike_udf_example_timeseries::Rollup;
use aerospike_udf_sdk_native::client::Combiner;

const STEP: i64 = 60_000;

fn module() -> String {
    env::var("AEROSPIKE_TIMESERIES_MODULE").unwrap_or_else(|_| "timeseries".to_string())
}

fn namespace() -> String {
    env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

/// **If `AEROSPIKE_HOSTS` is set, failing to reach it is a failure**, not a skip. The alternative is a suite that
/// reports a pass for tests that never ran, which has happened in this project and hid a real problem for a while.
async fn connect() -> Option<Client> {
    let asked = env::var("AEROSPIKE_HOSTS").is_ok();
    let policy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    match Client::new(&policy, &hosts).await {
        Ok(client) => Some(client),
        Err(e) if asked => {
            panic!("AEROSPIKE_HOSTS is set to {hosts:?} but the client could not reach it: {e}")
        }
        Err(_) => None,
    }
}

/// Call a record UDF and return its map, or the error text.
async fn call(
    client: &Client,
    key: &aerospike_core::Key,
    function: &str,
    args: &[Value],
) -> Result<HashMap<String, Value>, String> {
    let write = WritePolicy::default();

    match client
        .execute_udf(&write, key, &module(), function, Some(args))
        .await
    {
        Ok(Some(value)) => as_string_map(&value)
            .ok_or_else(|| format!("{function} did not return a map: {value:?}")),
        Ok(None) => Err(format!("{function} returned nothing")),
        Err(e) => Err(format!("{e}")),
    }
}

/// Flatten any of the three map representations into something keyed by `String`.
///
/// **The host builds a `SortedMap`, not a `HashMap`.** Matching only `HashMap` made every one of these tests report
/// "did not return a map" while the UDF had in fact returned exactly the right one — the same mistake the summary
/// example's benchmark made, where it silently found no record counts and every row read as zero.
fn as_string_map(value: &Value) -> Option<HashMap<String, Value>> {
    let pairs: Vec<(Value, Value)> = match value {
        Value::HashMap(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        Value::SortedMap(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        Value::OrderedMap(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        _ => return None,
    };

    Some(
        pairs
            .into_iter()
            .filter_map(|(k, v)| match k {
                Value::String(s) => Some((s, v)),
                _ => None,
            })
            .collect(),
    )
}

async fn must(
    client: &Client,
    key: &aerospike_core::Key,
    function: &str,
    args: &[Value],
) -> HashMap<String, Value> {
    call(client, key, function, args)
        .await
        .unwrap_or_else(|e| panic!("{function} should have succeeded: {e}"))
}

fn i64_of(map: &HashMap<String, Value>, key: &str) -> Option<i64> {
    match map.get(key) {
        Some(Value::Int(i)) => Some(*i),
        _ => None,
    }
}

fn f64_of(map: &HashMap<String, Value>, key: &str) -> Option<f64> {
    match map.get(key) {
        Some(Value::Float(f)) => Some(f64::from(f.clone())),
        Some(Value::Int(i)) => Some(*i as f64),
        _ => None,
    }
}

fn str_of(map: &HashMap<String, Value>, key: &str) -> Option<String> {
    match map.get(key) {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// The `values` list, with nil preserved as `None` — which is the whole point of it being nil.
fn values_of(map: &HashMap<String, Value>) -> Vec<Option<f64>> {
    match map.get("values") {
        Some(Value::List(items)) => items
            .iter()
            .map(|v| match v {
                Value::Nil => None,
                Value::Float(f) => Some(f64::from(f.clone())),
                Value::Int(i) => Some(*i as f64),
                other => panic!("a series value should be a number or nil, got {other:?}"),
            })
            .collect(),
        other => panic!("no values list: {other:?}"),
    }
}

async fn fresh(client: &Client, set: &str, id: i64, slots: i64, opts: Option<&str>) -> aerospike_core::Key {
    let ns = namespace();
    let key = as_key!(ns.as_str(), set, id);

    let _ = client.delete(&WritePolicy::default(), &key).await;

    // `created_ms` is pinned rather than taken from the clock. It is part of the config, so it is part of the config's
    // fingerprint — which means two records initialised a millisecond apart carry different fingerprints. That is
    // harmless for the guard, since a ring is only ever checked against its *own* record's config, but it does make two
    // records' rings differ in those four bytes. `a_batch_append_matches_appending_one_at_a_time` compares rings byte
    // for byte and needs them to agree.
    let args = vec![
        as_val!("v"),
        as_val!(STEP),
        as_val!(slots),
        opts.map_or(Value::Nil, |o| as_val!(o)),
        Value::Nil,
        as_val!(0),
    ];

    let created = must(client, &key, "ts_init", &args).await;

    assert_eq!(str_of(&created, "result").as_deref(), Some("created"), "ts_init");

    key
}

fn set_name(what: &str) -> String {
    format!("ts_{what}_{}", std::process::id())
}

//==========================================================

/// The round trip: create, append, read back — through a real particle.
#[tokio::test]
async fn a_series_is_created_appended_to_and_read_back() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let set = set_name("roundtrip");
    let key = fresh(&client, &set, 1, 10, None).await;

    for i in 0..5i64 {
        let applied = must(
            &client,
            &key,
            "ts_append",
            &[as_val!("v"), as_val!(i * STEP), Value::from(10.0 + i as f64)],
        )
        .await;

        assert_eq!(str_of(&applied, "result").as_deref(), Some("placed"), "sample {i}");
        assert_eq!(i64_of(&applied, "accepted"), Some(1));
        assert_eq!(i64_of(&applied, "bucket"), Some(i));
    }

    let report = must(&client, &key, "ts_report", &[as_val!("v")]).await;

    assert_eq!(str_of(&report, "result").as_deref(), Some("ok"));
    assert_eq!(i64_of(&report, "step_ms"), Some(STEP));
    assert_eq!(i64_of(&report, "slots"), Some(10));
    assert_eq!(i64_of(&report, "bucket_ms"), Some(STEP), "not downsampled");
    assert_eq!(i64_of(&report, "appends"), Some(5));

    // The window is the whole ring, so five known values and five slots that were never written.
    let values = values_of(&report);

    assert_eq!(values.len(), 10, "points");
    assert_eq!(i64_of(&report, "known"), Some(5));
    assert_eq!(i64_of(&report, "unknown"), Some(5));
    assert_eq!(&values[5..], &[Some(10.0), Some(11.0), Some(12.0), Some(13.0), Some(14.0)]);

    // **Unknown is nil, not zero.** A slot never written and a slot holding 0.0 are different facts.
    assert!(
        values[..5].iter().all(Option::is_none),
        "the buckets before the series began must be nil, got {:?}",
        &values[..5]
    );

    let stats = must(&client, &key, "ts_stats", &[as_val!("v")]).await;

    assert_eq!(i64_of(&stats, "count"), Some(5));
    assert_eq!(f64_of(&stats, "sum"), Some(60.0), "10+11+12+13+14");
    assert_eq!(f64_of(&stats, "min"), Some(10.0));
    assert_eq!(f64_of(&stats, "max"), Some(14.0));
    assert_eq!(f64_of(&stats, "first"), Some(10.0));
    assert_eq!(f64_of(&stats, "last"), Some(14.0));
    assert_eq!(f64_of(&stats, "mean"), Some(12.0));
    assert_eq!(i64_of(&stats, "unknown"), Some(5));
    assert!(stats.get("values").is_none(), "ts_stats returns no list at all");

    let _ = client.delete(&WritePolicy::default(), &key).await;
}

/// A batch of samples has to produce the identical ring to the same samples one at a time.
#[tokio::test]
async fn a_batch_append_matches_appending_one_at_a_time() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let set = set_name("batch");
    let n = 500i64;

    let one_at_a_time = fresh(&client, &set, 1, 1_000, None).await;
    let batched = fresh(&client, &set, 2, 1_000, None).await;

    let times: Vec<Value> = (0..n).map(|i| as_val!(i * STEP)).collect();
    let values: Vec<Value> = (0..n).map(|i| Value::from((i * 3) as f64)).collect();

    for i in 0..n {
        must(
            &client,
            &one_at_a_time,
            "ts_append",
            &[as_val!("v"), as_val!(i * STEP), Value::from((i * 3) as f64)],
        )
        .await;
    }

    let bulk = must(
        &client,
        &batched,
        "ts_append_many",
        &[as_val!("v"), Value::from(times), Value::from(values)],
    )
    .await;

    assert_eq!(i64_of(&bulk, "offered"), Some(n));
    assert_eq!(i64_of(&bulk, "accepted"), Some(n));
    assert_eq!(i64_of(&bulk, "placed"), Some(n));
    assert_eq!(i64_of(&bulk, "too_old"), Some(0));

    // Compare the raw bins, not the reports: the ring is the thing that has to match, byte for byte, including its
    // header — `last_bucket`, `appends` and the config fingerprint alike. `fresh` pins `created_ms` so the fingerprints
    // agree; see the note there.
    let a = client.get(&ReadPolicy::default(), &one_at_a_time, Bins::All).await.unwrap();
    let b = client.get(&ReadPolicy::default(), &batched, Bins::All).await.unwrap();

    let (Some(Value::Blob(ring_a)), Some(Value::Blob(ring_b))) = (a.bins.get("v"), b.bins.get("v"))
    else {
        panic!("both rings should be blobs");
    };

    assert_eq!(
        ring_a, ring_b,
        "a batch and a stream of singles must produce the same bytes"
    );

    let _ = client.delete(&WritePolicy::default(), &one_at_a_time).await;
    let _ = client.delete(&WritePolicy::default(), &batched).await;
}

/// Downsampling runs next to the data: a thousand buckets come back as ten numbers.
#[tokio::test]
async fn the_server_downsamples_before_anything_is_sent() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let set = set_name("downsample");
    let key = fresh(&client, &set, 1, 1_000, Some("agg=max")).await;
    let n = 1_000i64;

    let times: Vec<Value> = (0..n).map(|i| as_val!(i * STEP)).collect();
    let values: Vec<Value> = (0..n).map(|i| Value::from(i as f64)).collect();

    must(
        &client,
        &key,
        "ts_append_many",
        &[as_val!("v"), Value::from(times), Value::from(values)],
    )
    .await;

    let full = must(&client, &key, "ts_report", &[as_val!("v")]).await;

    assert_eq!(i64_of(&full, "points"), Some(1_000));
    assert_eq!(i64_of(&full, "bucket_ms"), Some(STEP));

    let ten = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("v"), Value::Nil, Value::Nil, as_val!(10)],
    )
    .await;

    assert_eq!(i64_of(&ten, "points"), Some(10));
    assert_eq!(i64_of(&ten, "bucket_ms"), Some(100 * STEP), "100 buckets per point");
    assert_eq!(str_of(&ten, "downsampled_with").as_deref(), Some("max"));

    // agg=max, and the values are 0..999 in order, so each group of 100 maxes to its last member.
    let values = values_of(&ten);

    assert_eq!(values.len(), 10);

    for (i, got) in values.iter().enumerate() {
        let expected = (i as f64 + 1.0) * 100.0 - 1.0;

        assert_eq!(*got, Some(expected), "point {i}");
    }

    // A window, rather than the whole ring.
    let window = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("v"), as_val!(10 * STEP), as_val!(19 * STEP)],
    )
    .await;

    assert_eq!(i64_of(&window, "points"), Some(10));
    assert_eq!(i64_of(&window, "from_ms"), Some(10 * STEP));
    assert_eq!(values_of(&window)[0], Some(10.0));

    let _ = client.delete(&WritePolicy::default(), &key).await;
}

/// **The refusals, and the fact that they change nothing.**
#[tokio::test]
async fn a_refused_append_leaves_the_ring_byte_identical() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let set = set_name("refuse");
    let key = fresh(&client, &set, 1, 10, None).await;

    for i in 0..10i64 {
        must(
            &client,
            &key,
            "ts_append",
            &[as_val!("v"), as_val!(i * STEP), Value::from(i as f64)],
        )
        .await;
    }

    let before = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();
    let Some(Value::Blob(ring_before)) = before.bins.get("v") else { panic!("blob") };
    let ring_before = ring_before.clone();

    // Far in the past: outside the window.
    let old = must(
        &client,
        &key,
        "ts_append",
        &[as_val!("v"), as_val!(-100 * STEP), Value::from(1.0)],
    )
    .await;

    assert_eq!(str_of(&old, "result").as_deref(), Some("too_old"));
    assert_eq!(i64_of(&old, "accepted"), Some(0));

    // A timestamp in microseconds when the series is stepped in milliseconds: the classic unit mistake, and it would
    // erase every slot.
    let wipe = must(
        &client,
        &key,
        "ts_append",
        &[as_val!("v"), as_val!(9 * STEP * 1000), Value::from(1.0)],
    )
    .await;

    assert_eq!(str_of(&wipe, "result").as_deref(), Some("would_wipe"));
    assert_eq!(i64_of(&wipe, "accepted"), Some(0));

    let after = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();
    let Some(Value::Blob(ring_after)) = after.bins.get("v") else { panic!("blob") };

    assert_eq!(
        &ring_before, ring_after,
        "two refused appends must leave the data exactly as it was, so a bad timestamp is recoverable"
    );

    let _ = client.delete(&WritePolicy::default(), &key).await;
}

/// A counter ring stores deltas, and says so when the counter restarted.
#[tokio::test]
async fn a_counter_ring_reports_deltas_resets_and_wraps() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let set = set_name("counter");
    let key = fresh(&client, &set, 1, 20, Some("kind=i64,mode=counter,agg=sum,wrap=32")).await;

    let baseline = must(
        &client,
        &key,
        "ts_append",
        &[as_val!("v"), as_val!(0), as_val!(1_000)],
    )
    .await;

    assert_eq!(str_of(&baseline, "counter").as_deref(), Some("baseline"));
    assert!(baseline.get("stored").is_none(), "one reading is not a rate");

    let delta = must(
        &client,
        &key,
        "ts_append",
        &[as_val!("v"), as_val!(STEP), as_val!(1_250)],
    )
    .await;

    assert_eq!(str_of(&delta, "counter").as_deref(), Some("delta"));
    assert_eq!(i64_of(&delta, "stored"), Some(250));

    // The counter restarted.
    let reset = must(
        &client,
        &key,
        "ts_append",
        &[as_val!("v"), as_val!(2 * STEP), as_val!(7)],
    )
    .await;

    assert_eq!(str_of(&reset, "counter").as_deref(), Some("reset"));
    assert_eq!(i64_of(&reset, "counter_resets"), Some(1));

    let report = must(&client, &key, "ts_report", &[as_val!("v")]).await;

    assert_eq!(i64_of(&report, "counter_resets"), Some(1));

    let values = values_of(&report);

    // Bucket 1 holds the delta; buckets 0 and 2 are unknown because a baseline and a reset are not rates.
    assert_eq!(values[values.len() - 3], None, "the baseline bucket");
    assert_eq!(values[values.len() - 2], Some(250.0));
    assert_eq!(values[values.len() - 1], None, "the reset bucket, not a spurious spike");

    let _ = client.delete(&WritePolicy::default(), &key).await;
}

/// An `i64` ring keeps values a double could not, all the way through a particle and back.
#[tokio::test]
async fn an_integer_ring_stays_exact_past_two_to_the_53() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let set = set_name("exact");
    let key = fresh(&client, &set, 1, 10, Some("kind=i64,agg=sum")).await;
    let base = (1i64 << 53) + 1;

    assert_ne!(base as f64 as i64, base, "the premise: an f64 cannot hold this");

    must(&client, &key, "ts_append", &[as_val!("v"), as_val!(0), as_val!(base)]).await;
    must(&client, &key, "ts_append", &[as_val!("v"), as_val!(STEP), as_val!(base)]).await;

    let stats = must(&client, &key, "ts_stats", &[as_val!("v")]).await;

    assert_eq!(
        i64_of(&stats, "sum"),
        Some(2 * base),
        "the sum has to come back as an exact integer"
    );
    assert_eq!(i64_of(&stats, "min"), Some(base));
    assert_eq!(i64_of(&stats, "count"), Some(2));

    // And a double offered to an integer ring is refused rather than truncated.
    let refused = call(
        &client,
        &key,
        "ts_append",
        &[as_val!("v"), as_val!(2 * STEP), Value::from(1.5)],
    )
    .await;

    assert!(refused.is_err(), "truncating 1.5 silently is what must not happen");

    let _ = client.delete(&WritePolicy::default(), &key).await;
}

/// **Editing the config bin behind a ring's back is caught, not honoured.**
#[tokio::test]
async fn a_config_edited_underneath_a_ring_is_detected() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let set = set_name("fingerprint");
    let key = fresh(&client, &set, 1, 10, None).await;

    must(&client, &key, "ts_append", &[as_val!("v"), as_val!(0), Value::from(1.0)]).await;

    // Read the config, edit a byte, write it straight back as a client would. Same length, valid magic, valid version -
    // every byte still decodes and every slot now means something else.
    let record = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();
    let Some(Value::Blob(cfg)) = record.bins.get("rrcfg") else { panic!("no config bin") };

    // Two edits, because the two are caught by different checks and both matter.
    //
    // `step_ms` is **not** mirrored in the ring's header, so nothing but the fingerprint can notice it - which is the
    // whole reason the fingerprint is stored. `agg` is mirrored, so the ring itself catches that one and says something
    // more specific than "a byte changed".
    let cases: [(usize, u8, &str, &str); 2] = [
        (8, 1, "step_ms", "different config"),
        (5, 3, "agg", "aggregates with"),
    ];

    for (offset, byte, what, expected) in cases {
        let mut edited = cfg.clone();

        edited[offset] = byte;

        client
            .put(&WritePolicy::default(), &key, &[as_bin!("rrcfg", edited)])
            .await
            .unwrap();

        for verb in ["ts_append", "ts_report", "ts_stats"] {
            let args: Vec<Value> = if verb == "ts_append" {
                vec![as_val!("v"), as_val!(STEP), Value::from(2.0)]
            } else {
                vec![as_val!("v")]
            };

            let err = call(&client, &key, verb, &args)
                .await
                .expect_err(&format!("{verb} must refuse a ring whose {what} changed"));

            assert!(
                err.contains(expected),
                "{verb} should say the {what} is wrong, got: {err}"
            );
        }
    }

    let _ = client.delete(&WritePolicy::default(), &key).await;
}

/// `ts_init` will not quietly destroy a series, and will not quietly reshape one.
#[tokio::test]
async fn init_refuses_to_reshape_or_erase() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let set = set_name("init");
    let key = fresh(&client, &set, 1, 10, None).await;

    must(&client, &key, "ts_append", &[as_val!("v"), as_val!(0), Value::from(42.0)]).await;

    // Same shape again: reports that it exists and leaves the data alone.
    let again = must(&client, &key, "ts_init", &[as_val!("v"), as_val!(STEP), as_val!(10)]).await;

    assert_eq!(str_of(&again, "result").as_deref(), Some("exists"));

    let report = must(&client, &key, "ts_report", &[as_val!("v")]).await;

    assert_eq!(i64_of(&report, "known"), Some(1), "re-running init must not erase anything");

    // A different shape: refused, because every ring in the record was built against the old one.
    let reshaped = call(&client, &key, "ts_init", &[as_val!("v"), as_val!(1_000), as_val!(10)]).await;

    assert!(reshaped.is_err(), "a different step must be refused");

    // A second ring sharing the same config is fine, and that is the point of the split.
    let second = must(&client, &key, "ts_init", &[as_val!("w"), as_val!(STEP), as_val!(10)]).await;

    assert_eq!(str_of(&second, "result").as_deref(), Some("created"));

    must(&client, &key, "ts_append", &[as_val!("w"), as_val!(0), Value::from(7.0)]).await;

    let w = must(&client, &key, "ts_report", &[as_val!("w")]).await;

    assert_eq!(i64_of(&w, "known"), Some(1));

    let v = must(&client, &key, "ts_report", &[as_val!("v")]).await;

    assert_eq!(f64_of(&v, "last_ms"), Some(0.0));
    assert_eq!(i64_of(&v, "known"), Some(1), "the two rings are independent");

    // An unknown option is an error rather than a shrug.
    let key2 = as_key!(namespace().as_str(), set.as_str(), 2);

    assert!(
        call(
            &client,
            &key2,
            "ts_init",
            &[as_val!("v"), as_val!(STEP), as_val!(10), as_val!("agg=maxx")]
        )
        .await
        .is_err(),
        "a typo in the options must not produce a series with a shape nobody asked for"
    );

    let _ = client.delete(&WritePolicy::default(), &key).await;
}

/// Many series into one, merged across query threads by the plugin's own `merge`.
#[tokio::test]
async fn the_rollup_aligns_and_sums_many_records() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let set = set_name("rollup");
    let ns = namespace();
    let devices = 200i64;
    let buckets = 20i64;

    // Every device reports the same value in the same buckets, so the rollup's answer is arithmetic: each bucket sums
    // to `devices * bucket_index` and has `devices` contributors.
    futures::stream::iter(0..devices)
        .for_each_concurrent(32, |d| {
            let client = &client;
            let set = set.as_str();
            let ns = ns.as_str();

            async move {
                let key = as_key!(ns, set, d);
                let _ = client.delete(&WritePolicy::default(), &key).await;

                must(client, &key, "ts_init", &[as_val!("v"), as_val!(STEP), as_val!(buckets)]).await;

                let times: Vec<Value> = (0..buckets).map(|b| as_val!(b * STEP)).collect();
                let values: Vec<Value> = (0..buckets).map(|b| Value::from(b as f64)).collect();

                must(
                    client,
                    &key,
                    "ts_append_many",
                    &[as_val!("v"), Value::from(times), Value::from(values)],
                )
                .await;
            }
        })
        .await;

    let args = [as_val!("v")];
    let mut statement = Statement::new(&ns, &set, Bins::All);

    statement.set_aggregate_function(&module(), "ts_rollup", Some(&args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut combiner = Combiner::<Rollup>::new(&args).expect("combiner");
    let mut stream = records.into_stream();
    let mut partials = 0;

    while let Some(record) = stream.next().await {
        for value in record.unwrap().bins.values() {
            combiner.absorb(value).expect("absorb a partial");
            partials += 1;
        }
    }

    assert!(partials > 0, "no partials - is the '{}' module configured?", module());

    let finished = combiner.finish().expect("finalize");
    let map = as_string_map(&finished).expect("the rollup should finalize to a map");

    assert_eq!(str_of(&map, "result").as_deref(), Some("ok"));
    assert_eq!(i64_of(&map, "records"), Some(devices));
    assert_eq!(i64_of(&map, "samples"), Some(devices * buckets));
    assert_eq!(i64_of(&map, "skipped_shape"), Some(0));
    assert_eq!(i64_of(&map, "skipped_unreadable"), Some(0));
    assert_eq!(i64_of(&map, "points"), Some(buckets));
    assert_eq!(str_of(&map, "op").as_deref(), Some("sum"));

    let values = values_of(&map);

    for b in 0..buckets as usize {
        assert_eq!(
            values[b],
            Some((devices * b as i64) as f64),
            "bucket {b}: every device reported {b}, so the sum is {devices} x {b}"
        );
    }

    // The count ring is what makes a mean across records possible without doubling every stored ring.
    match map.get("counts") {
        Some(Value::List(counts)) => {
            for (b, c) in counts.iter().enumerate() {
                assert_eq!(
                    c,
                    &Value::Int(devices),
                    "bucket {b} should have {devices} contributors"
                );
            }
        }
        other => panic!("no counts list: {other:?}"),
    }

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}


/// **A rollup over a mixed-shape set must not call its answer `ok`.**
///
/// The aggregation adopts the shape of the first record it reads, and across query threads there is no single first —
/// so an undeclared rollup over a mixed set returns some subset, and which subset varies between runs. A tutorial run
/// caught it reporting `result=ok` over a single record out of four. Now it says `shape_conflict`, and pinning the
/// shape makes it deterministic.
#[tokio::test]
async fn a_mixed_shape_rollup_is_flagged_and_pinning_fixes_it() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("shapes");
    let buckets = 6i64;

    // Three hosts at a one-minute step...
    for d in 0..3i64 {
        let key = as_key!(ns.as_str(), set.as_str(), d);
        let _ = client.delete(&WritePolicy::default(), &key).await;

        must(&client, &key, "ts_init", &[as_val!("v"), as_val!(STEP), as_val!(buckets), Value::Nil, Value::Nil, as_val!(0)]).await;

        let times: Vec<Value> = (0..buckets).map(|b| as_val!(b * STEP)).collect();
        let values: Vec<Value> = (0..buckets).map(|_| Value::from(1.0)).collect();

        must(&client, &key, "ts_append_many", &[as_val!("v"), Value::from(times), Value::from(values)]).await;
    }

    // ...and one at a one-second step, which cannot be combined with them.
    let odd = as_key!(ns.as_str(), set.as_str(), 99);
    let _ = client.delete(&WritePolicy::default(), &odd).await;

    must(&client, &odd, "ts_init", &[as_val!("v"), as_val!(1_000), as_val!(buckets), Value::Nil, Value::Nil, as_val!(0)]).await;
    must(&client, &odd, "ts_append", &[as_val!("v"), as_val!(0), Value::from(999.0)]).await;

    let loose = rollup(&client, &ns, &set, &[as_val!("v")]).await;

    assert_eq!(
        str_of(&loose, "result").as_deref(),
        Some("shape_conflict"),
        "a subset answer must not be labelled ok"
    );
    assert_eq!(i64_of(&loose, "shape_declared"), Some(0));
    assert!(i64_of(&loose, "skipped_shape").unwrap_or(0) > 0);

    // Pinned: deterministic, and every matching record contributes.
    let args = [
        as_val!("v"),
        as_val!("sum"),
        Value::Nil,
        Value::Nil,
        as_val!(STEP),
        as_val!(buckets),
    ];
    let pinned = rollup(&client, &ns, &set, &args).await;

    assert_eq!(str_of(&pinned, "result").as_deref(), Some("ok"));
    assert_eq!(i64_of(&pinned, "shape_declared"), Some(1));
    assert_eq!(i64_of(&pinned, "records"), Some(3), "all three matching hosts");
    assert_eq!(i64_of(&pinned, "skipped_shape"), Some(1), "and the odd one is counted");
    assert_eq!(i64_of(&pinned, "step_ms"), Some(STEP));

    // Every host reported 1.0 in every bucket, so every point is 3.
    for (b, v) in values_of(&pinned).iter().enumerate() {
        assert_eq!(*v, Some(3.0), "bucket {b}");
    }

    // Half a shape is refused rather than half applied.
    let half = [as_val!("v"), as_val!("sum"), Value::Nil, Value::Nil, as_val!(STEP)];

    assert!(
        Combiner::<Rollup>::new(&half).is_err(),
        "declaring step_ms without slots must be refused"
    );

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

//==========================================================
// Archives.
//

/// 2026-08-06T00:00:00Z, so every bucket boundary below is one a reader can check by hand.
const NOW: i64 = 20_671 * 86_400_000;

/// A record holding an archive hierarchy rather than a single ring.
async fn fresh_archived(
    client: &Client,
    set: &str,
    id: i64,
    bin: &str,
    opts: &str,
) -> aerospike_core::Key {
    let ns = namespace();
    let key = as_key!(ns.as_str(), set, id);

    let _ = client.delete(&WritePolicy::default(), &key).await;

    let created = must(
        client,
        &key,
        "ts_init",
        &[
            as_val!(bin),
            as_val!(STEP),
            as_val!(10),
            as_val!(opts),
            Value::Nil,
            as_val!(0),
        ],
    )
    .await;

    assert_eq!(str_of(&created, "result").as_deref(), Some("created"), "ts_init: {created:?}");

    key
}

/// The archive `table` entries, keyed by resolution.
fn table_of(map: &HashMap<String, Value>) -> HashMap<String, HashMap<String, Value>> {
    let Some(Value::List(rows)) = map.get("table") else {
        panic!("no archive table: {map:?}")
    };

    rows.iter()
        .map(|row| {
            let entry = as_string_map(row).expect("each table row is a map");
            let res = str_of(&entry, "resolution").unwrap_or_else(|| "ring".to_string());

            (res, entry)
        })
        .collect()
}

/// `ts_init` with archives makes one ring per resolution, named after it, and **no base ring**.
#[tokio::test]
async fn an_archived_series_gets_one_ring_per_resolution() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("archinit");
    let key = fresh_archived(&client, &set, 1, "v", "archives=default").await;

    let info = must(&client, &key, "ts_info", &[as_val!("v")]).await;

    assert_eq!(i64_of(&info, "archives"), Some(7));
    assert_eq!(i64_of(&info, "rings_present"), Some(7));
    // The figure the design notes claim, measured through a real record rather than computed twice.
    assert_eq!(i64_of(&info, "ring_bytes"), Some(62_232), "60.8 KiB per metric");

    let table = table_of(&info);
    let expected: [(&str, &str, i64); 7] = [
        ("second", "v~s", 3_600),
        ("minute", "v~m", 1_440),
        ("hour", "v~h", 336),
        ("day", "v~d", 100),
        ("week", "v~w", 104),
        ("month", "v~mo", 60),
        ("year", "v~y", 25),
    ];

    for (i, (res, bin, slots)) in expected.iter().enumerate() {
        let entry = table.get(*res).unwrap_or_else(|| panic!("no {res} archive: {table:?}"));

        assert_eq!(str_of(entry, "bin").as_deref(), Some(*bin), "{res} bin");
        assert_eq!(i64_of(entry, "slots"), Some(*slots), "{res} slots");
        assert_eq!(i64_of(entry, "index"), Some(i as i64), "{res} index");
        assert_eq!(str_of(entry, "state").as_deref(), Some("empty"), "{res} is new");
    }

    // **The base bin holds nothing.** The finest archive already is the ring it would hold, and a duplicate would cost
    // 28 KiB per metric to say the same thing twice.
    let record = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .expect("the record exists");

    assert!(
        !record.bins.contains_key("v"),
        "the base bin must be empty for an archived series, bins were {:?}",
        record.bins.keys().collect::<Vec<_>>()
    );

    for (_, bin, _) in expected {
        assert!(record.bins.contains_key(bin), "missing {bin}");
    }

    // Re-running init creates nothing and erases nothing.
    let again = must(
        &client,
        &key,
        "ts_init",
        &[as_val!("v"), as_val!(STEP), as_val!(10), as_val!("archives=default"), Value::Nil, as_val!(0)],
    )
    .await;

    assert_eq!(str_of(&again, "result").as_deref(), Some("exists"));
    assert_eq!(i64_of(&again, "rings_created"), Some(0));
    assert_eq!(i64_of(&again, "rings_existing"), Some(7));

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// One append reaches every archive, and each one aggregates it at its own resolution.
#[tokio::test]
async fn one_append_feeds_every_archive() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("archappend");
    let key = fresh_archived(&client, &set, 1, "v", "archives=default").await;

    // Ten samples, one per second, values 1 through 10.
    for i in 1..=10i64 {
        let applied = must(
            &client,
            &key,
            "ts_append",
            &[as_val!("v"), as_val!(NOW + (i - 1) * 1_000), Value::from(i as f64)],
        )
        .await;

        assert_eq!(i64_of(&applied, "accepted"), Some(1), "sample {i}");
        assert_eq!(i64_of(&applied, "archives"), Some(7));
        assert_eq!(i64_of(&applied, "archives_accepted"), Some(7), "every archive took it");

        let per = as_string_map(applied.get("per_archive").expect("per_archive")).expect("a map");

        // A new second every time, so the second archive places; every coarser archive is still in the same bucket, so
        // it combines. That pattern *is* consolidation happening.
        assert_eq!(str_of(&per, "second").as_deref(), Some("placed"), "sample {i}");

        let coarser = if i == 1 { "placed" } else { "combined" };

        for res in ["minute", "hour", "day", "week", "month", "year"] {
            assert_eq!(str_of(&per, res).as_deref(), Some(coarser), "sample {i}, {res}");
        }
    }

    // The per-second archive kept each sample separately: `last` over a bucket holding one sample.
    let report = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("v"), as_val!(NOW), as_val!(NOW + 9_000), Value::Nil, Value::Nil, as_val!("s")],
    )
    .await;

    assert_eq!(str_of(&report, "result").as_deref(), Some("ok"));
    assert_eq!(str_of(&report, "resolution").as_deref(), Some("second"));
    assert_eq!(str_of(&report, "archive_bin").as_deref(), Some("v~s"));
    assert_eq!(i64_of(&report, "bucket_ms"), Some(1_000));
    assert_eq!(
        values_of(&report),
        (1..=10).map(|i| Some(i as f64)).collect::<Vec<_>>()
    );

    // Every coarser archive holds one bucket, and its value is the **mean of all ten raw samples** — 5.5, not a mean of
    // means and not the last one.
    for (res, bin) in [
        ("m", "v~m"),
        ("h", "v~h"),
        ("d", "v~d"),
        ("w", "v~w"),
        ("mo", "v~mo"),
        ("y", "v~y"),
    ] {
        let report = must(
            &client,
            &key,
            "ts_report",
            &[as_val!("v"), as_val!(NOW), as_val!(NOW + 9_000), Value::Nil, Value::Nil, as_val!(res)],
        )
        .await;

        assert_eq!(str_of(&report, "archive_bin").as_deref(), Some(bin));
        assert_eq!(i64_of(&report, "points"), Some(1), "{res}");
        assert_eq!(values_of(&report), vec![Some(5.5)], "{res} should hold the true mean");
    }

    // The bucket boundaries, which is what the calendar arithmetic is for. NOW is a Thursday, so its week began Monday.
    let week = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("v"), as_val!(NOW), as_val!(NOW + 9_000), Value::Nil, Value::Nil, as_val!("w")],
    )
    .await;

    assert_eq!(
        i64_of(&week, "from_ms"),
        Some(20_668 * 86_400_000),
        "2026-08-06 is a Thursday; its week starts Monday 2026-08-03"
    );

    let month = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("v"), as_val!(NOW), as_val!(NOW + 9_000), Value::Nil, Value::Nil, as_val!("mo")],
    )
    .await;

    assert_eq!(
        i64_of(&month, "from_ms"),
        Some(20_666 * 86_400_000),
        "the month bucket starts on 2026-08-01"
    );

    // Asking for an archive the series does not keep is an error rather than a substitution.
    let err = call(
        &client,
        &key,
        "ts_report",
        &[as_val!("v"), Value::Nil, Value::Nil, Value::Nil, Value::Nil, as_val!("nanosecond")],
    )
    .await
    .expect_err("no such resolution");

    assert!(err.contains("resolution"), "{err}");

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// With no `res`, a report picks the finest archive that still reaches back to `from_ms`.
#[tokio::test]
async fn a_report_picks_the_archive_that_reaches_back() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("archpick");
    let key = fresh_archived(&client, &set, 1, "v", "archives=default").await;

    must(&client, &key, "ts_append", &[as_val!("v"), as_val!(NOW), Value::from(42.0)]).await;

    let minute = 60_000i64;
    let hour = 60 * minute;
    let day = 24 * hour;

    // (how far back the caller asks, which archive should answer)
    let cases: [(i64, &str, &str); 6] = [
        (10 * minute, "second", "v~s"),
        (2 * hour, "minute", "v~m"),
        (3 * day, "hour", "v~h"),
        (30 * day, "day", "v~d"),
        (200 * day, "week", "v~w"),
        (1_000 * day, "month", "v~mo"),
    ];

    for (back, res, bin) in cases {
        let report = must(
            &client,
            &key,
            "ts_report",
            &[
                as_val!("v"),
                as_val!(NOW - back),
                Value::Nil,
                as_val!(4),
                Value::Nil,
                Value::Nil,
                as_val!(NOW),
            ],
        )
        .await;

        assert_eq!(
            str_of(&report, "resolution").as_deref(),
            Some(res),
            "{back} ms back, chosen because {:?}",
            str_of(&report, "archive_chosen_because")
        );
        assert_eq!(str_of(&report, "archive_bin").as_deref(), Some(bin));
        assert_eq!(str_of(&report, "result").as_deref(), Some("ok"));
    }

    // No archive reaches back fifty years, so the coarsest answers and the result is **not** `ok`.
    let report = must(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("v"),
            as_val!(NOW - 50 * 365 * day),
            Value::Nil,
            as_val!(4),
            Value::Nil,
            Value::Nil,
            as_val!(NOW),
        ],
    )
    .await;

    assert_eq!(str_of(&report, "resolution").as_deref(), Some("year"));
    assert_eq!(
        str_of(&report, "result").as_deref(),
        Some("truncated"),
        "the numbers are real but the window is not the one asked for"
    );

    // And `ts_stats` selects the same way.
    let stats = must(
        &client,
        &key,
        "ts_stats",
        &[as_val!("v"), as_val!(NOW - 3 * day), Value::Nil, Value::Nil, Value::Nil, as_val!(NOW)],
    )
    .await;

    assert_eq!(str_of(&stats, "resolution").as_deref(), Some("hour"));
    assert_eq!(i64_of(&stats, "count"), Some(1));
    assert_eq!(f64_of(&stats, "last"), Some(42.0));

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// `ts_append_all` writes every metric in the record in **one** commit.
#[tokio::test]
async fn append_all_writes_every_metric_at_once() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("archall");
    let key = fresh_archived(&client, &set, 1, "cpu", "archives=m:60;h:24").await;

    // A second and third metric under the same config, which is what makes one commit possible.
    for bin in ["mem", "net"] {
        let created = must(
            &client,
            &key,
            "ts_init",
            &[as_val!(bin), as_val!(STEP), as_val!(10), as_val!("archives=m:60;h:24"), Value::Nil, as_val!(0)],
        )
        .await;

        assert_eq!(str_of(&created, "result").as_deref(), Some("created"), "{bin}");
    }

    let bins = Value::from(vec![as_val!("cpu"), as_val!("mem"), as_val!("net")]);
    let values = Value::from(vec![Value::from(1.0), Value::from(2.0), Value::from(3.0)]);

    let applied = must(&client, &key, "ts_append_all", &[bins, as_val!(NOW), values]).await;

    assert_eq!(str_of(&applied, "result").as_deref(), Some("ok"));
    assert_eq!(i64_of(&applied, "metrics"), Some(3));
    assert_eq!(i64_of(&applied, "rings_accepted"), Some(6), "three metrics, two archives each");
    assert_eq!(i64_of(&applied, "rings_refused"), Some(0));

    let per = as_string_map(applied.get("per_metric").expect("per_metric")).expect("a map");

    for bin in ["cpu", "mem", "net"] {
        assert_eq!(str_of(&per, bin).as_deref(), Some("placed"), "{bin}");
    }

    // Every metric's rings actually hold their own value.
    for (bin, expected) in [("cpu", 1.0), ("mem", 2.0), ("net", 3.0)] {
        for res in ["m", "h"] {
            let report = must(
                &client,
                &key,
                "ts_report",
                &[as_val!(bin), as_val!(NOW), as_val!(NOW), Value::Nil, Value::Nil, as_val!(res)],
            )
            .await;

            assert_eq!(values_of(&report), vec![Some(expected)], "{bin} at {res}");
        }
    }

    // Lists that do not line up are refused rather than half applied.
    let err = call(
        &client,
        &key,
        "ts_append_all",
        &[
            Value::from(vec![as_val!("cpu"), as_val!("mem")]),
            as_val!(NOW),
            Value::from(vec![Value::from(1.0)]),
        ],
    )
    .await
    .expect_err("mismatched lists");

    assert!(err.contains("line up"), "{err}");

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// The heartbeat and `xff`, through a real particle.
#[tokio::test]
async fn the_heartbeat_and_xff_apply_to_archive_rings() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("archxff");

    // A counter whose readings must not be more than two minutes apart to count as a rate.
    let key = fresh_archived(
        &client,
        &set,
        1,
        "b",
        "kind=i64,mode=counter,archives=m:60:sum;h:24:sum,heartbeat=120000",
    )
    .await;

    let minute = 60_000i64;

    let first = must(&client, &key, "ts_append", &[as_val!("b"), as_val!(NOW), as_val!(1_000)]).await;

    assert_eq!(str_of(&first, "counter").as_deref(), Some("baseline"));

    let second = must(
        &client,
        &key,
        "ts_append",
        &[as_val!("b"), as_val!(NOW + minute), as_val!(1_100)],
    )
    .await;

    assert_eq!(str_of(&second, "counter").as_deref(), Some("delta"));
    assert_eq!(i64_of(&second, "stored"), Some(100));

    // Five minutes later, past the heartbeat. Without one this would charge 3,900 to a single minute.
    let late = must(
        &client,
        &key,
        "ts_append",
        &[as_val!("b"), as_val!(NOW + 6 * minute), as_val!(5_000)],
    )
    .await;

    assert_eq!(str_of(&late, "counter").as_deref(), Some("stale"));
    assert!(late.get("stored").is_none(), "a stale reading stores unknown: {late:?}");

    let report = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("b"), as_val!(NOW), as_val!(NOW + 6 * minute), Value::Nil, Value::Nil, as_val!("m")],
    )
    .await;

    assert_eq!(
        values_of(&report),
        vec![None, Some(100.0), None, None, None, None, None],
        "one delta, and the gap the heartbeat refused to invent a number for"
    );

    // Now xff: an hourly archive that wants half of its 60 possible minutes before it reports.
    let key = fresh_archived(&client, &set, 2, "s", "archives=m:60;h:24:mean:50").await;

    let info = must(&client, &key, "ts_info", &[as_val!("s")]).await;
    let table = table_of(&info);
    let hour = table.get("hour").expect("an hour archive");

    assert_eq!(i64_of(hour, "xff"), Some(50));
    assert_eq!(
        i64_of(hour, "xff_needs_samples"),
        Some(30),
        "half of the 60 minute-buckets an hour could hold"
    );

    for i in 0..5i64 {
        must(
            &client,
            &key,
            "ts_append",
            &[as_val!("s"), as_val!(NOW + i * minute), Value::from(7.0)],
        )
        .await;
    }

    // The minute archive reports every one of them; the hour archive refuses to average five readings out of sixty.
    let minutes = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("s"), as_val!(NOW), as_val!(NOW + 4 * minute), Value::Nil, Value::Nil, as_val!("m")],
    )
    .await;

    assert_eq!(i64_of(&minutes, "known"), Some(5));

    let hours = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("s"), as_val!(NOW), as_val!(NOW + 4 * minute), Value::Nil, Value::Nil, as_val!("h")],
    )
    .await;

    assert_eq!(i64_of(&hours, "points"), Some(1));
    assert_eq!(i64_of(&hours, "known"), Some(0), "below xff");
    assert_eq!(
        i64_of(&hours, "xff_suppressed"),
        Some(1),
        "and it says the bucket was suppressed rather than empty - a different fact"
    );

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// A ring opened as the wrong archive is refused, through a real record.
#[tokio::test]
async fn one_archive_cannot_be_read_as_another() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("archswap");
    let key = fresh_archived(&client, &set, 1, "v", "archives=m:60;h:60").await;

    must(&client, &key, "ts_append", &[as_val!("v"), as_val!(NOW), Value::from(9.0)]).await;

    // Both archives have 60 slots and the same aggregation, so neither the length nor the agg distinguishes them, and
    // they share one config so they share one fingerprint. Only the resolution in the ring's own header is left.
    let swapped = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .expect("the record");

    let (Some(Value::Blob(m)), Some(Value::Blob(h))) =
        (swapped.bins.get("v~m"), swapped.bins.get("v~h"))
    else {
        panic!("both archive rings should be blobs: {:?}", swapped.bins.keys().collect::<Vec<_>>())
    };

    assert_eq!(m.len(), h.len(), "the premise: identical lengths");

    // Put the hourly ring in the minute archive's bin and the read must refuse rather than report values an hour away
    // from where they belong.
    client
        .put(&WritePolicy::default(), &key, &[as_bin!("v~m", Value::Blob(h.clone()))])
        .await
        .expect("swap the rings");

    let err = call(
        &client,
        &key,
        "ts_report",
        &[as_val!("v"), Value::Nil, Value::Nil, Value::Nil, Value::Nil, as_val!("m")],
    )
    .await
    .expect_err("the hourly ring must not read as the minute archive");

    assert!(
        err.contains("hour") && err.contains("minute"),
        "the error should name both resolutions: {err}"
    );

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// A rollup can target one archive of an archived series.
#[tokio::test]
async fn the_rollup_can_target_an_archive() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("archrollup");
    let minute = 60_000i64;

    // Three hosts, each reporting a different constant every minute for five minutes.
    for host in 1..=3i64 {
        let key = fresh_archived(&client, &set, host, "rps", "archives=m:60:mean;h:24:mean").await;

        for m in 0..5i64 {
            must(
                &client,
                &key,
                "ts_append",
                &[as_val!("rps"), as_val!(NOW + m * minute), Value::from((host * 10) as f64)],
            )
            .await;
        }
    }

    // The minute archive: five buckets, each summing 10 + 20 + 30 across the three records.
    let summed = rollup(
        &client,
        &ns,
        &set,
        &[as_val!("rps"), as_val!("sum"), Value::Nil, Value::Nil, Value::Nil, Value::Nil, as_val!("m")],
    )
    .await;

    assert_eq!(str_of(&summed, "result").as_deref(), Some("ok"), "{summed:?}");
    assert_eq!(i64_of(&summed, "records"), Some(3));
    assert_eq!(str_of(&summed, "resolution").as_deref(), Some("minute"));
    assert_eq!(str_of(&summed, "archive_bin").as_deref(), Some("rps~m"));
    assert_eq!(i64_of(&summed, "step_ms"), Some(minute));

    let values = values_of(&summed);
    let known: Vec<f64> = values.iter().flatten().copied().collect();

    assert_eq!(known, vec![60.0; 5], "10 + 20 + 30 in each of five minute buckets");

    // The hour archive: one bucket, and each record's mean over its five samples is its own constant, so the sum is
    // still 60. Same answer at a coarser resolution, which is the property that makes archives usable for a fleet.
    let hourly = rollup(
        &client,
        &ns,
        &set,
        &[as_val!("rps"), as_val!("sum"), Value::Nil, Value::Nil, Value::Nil, Value::Nil, as_val!("h")],
    )
    .await;

    assert_eq!(str_of(&hourly, "resolution").as_deref(), Some("hour"));
    assert_eq!(str_of(&hourly, "archive_bin").as_deref(), Some("rps~h"));
    assert_eq!(i64_of(&hourly, "records"), Some(3));
    assert_eq!(
        values_of(&hourly).iter().flatten().copied().collect::<Vec<f64>>(),
        vec![60.0],
        "one hourly bucket holding the three hosts' means"
    );

    // Bucket boundaries come from the resolution, not from `bucket * approx_step` — which is the arithmetic a calendar
    // resolution would get wrong, and the reason the accumulator carries the resolution it adopted.
    //
    // A rollup reports its **whole** window rather than only the populated part, so `from_ms` is 23 hours before the
    // newest bucket, not the bucket holding data. This assertion originally said `from_ms == NOW` and was simply wrong
    // about what the verb returns.
    assert_eq!(
        i64_of(&hourly, "to_ms"),
        Some(NOW),
        "NOW is midnight, so the hour containing it starts there"
    );
    assert_eq!(
        i64_of(&hourly, "from_ms"),
        Some(NOW - 23 * 3_600_000),
        "24 hourly slots, so the window opens 23 hours before the newest"
    );
    assert_eq!(i64_of(&hourly, "points"), Some(24));

    // With no `res`, the finest archive answers.
    let default = rollup(&client, &ns, &set, &[as_val!("rps"), as_val!("sum")]).await;

    assert_eq!(str_of(&default, "resolution").as_deref(), Some("minute"));

    // A resolution none of the records keep is a shape mismatch, counted rather than fatal - one odd config in a set
    // should not lose the answer for the rest.
    let absent = rollup(
        &client,
        &ns,
        &set,
        &[as_val!("rps"), as_val!("sum"), Value::Nil, Value::Nil, Value::Nil, Value::Nil, as_val!("y")],
    )
    .await;

    assert_eq!(i64_of(&absent, "records"), Some(0));
    assert_eq!(i64_of(&absent, "skipped_shape"), Some(3));
    assert_eq!(str_of(&absent, "result").as_deref(), Some("empty"));

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// Arguments at their awkward edges: empty lists, windows that miss, and timestamps at the ends of `i64`.
#[tokio::test]
async fn awkward_arguments_are_handled_rather_than_assumed_away() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("edges");
    let key = fresh_archived(&client, &set, 1, "v", "archives=m:60;h:24").await;
    let minute = 60_000i64;

    // --- an empty batch is a no-op, not an error and not a write ---

    let empty = must(
        &client,
        &key,
        "ts_append_many",
        &[as_val!("v"), Value::from(Vec::<Value>::new()), Value::from(Vec::<Value>::new())],
    )
    .await;

    assert_eq!(i64_of(&empty, "offered"), Some(0));
    assert_eq!(i64_of(&empty, "accepted"), Some(0));

    // --- a report on a series with nothing in it ---

    let virgin = must(&client, &key, "ts_report", &[as_val!("v")]).await;

    assert_eq!(str_of(&virgin, "result").as_deref(), Some("empty"));
    assert_eq!(i64_of(&virgin, "points"), Some(0));
    assert!(virgin.get("values").is_none(), "an empty series ships no list at all");

    let virgin_stats = must(&client, &key, "ts_stats", &[as_val!("v")]).await;

    assert_eq!(str_of(&virgin_stats, "result").as_deref(), Some("empty"));
    assert_eq!(i64_of(&virgin_stats, "count"), Some(0));

    // Now give it something.
    for i in 0..10i64 {
        must(
            &client,
            &key,
            "ts_append",
            &[as_val!("v"), as_val!(NOW + i * minute), Value::from(i as f64)],
        )
        .await;
    }

    // --- windows that do not overlap the data ---

    let ancient = must(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("v"),
            as_val!(NOW - 1_000 * minute),
            as_val!(NOW - 900 * minute),
            Value::Nil,
            Value::Nil,
            as_val!("m"),
        ],
    )
    .await;

    assert_eq!(
        str_of(&ancient, "result").as_deref(),
        Some("no_overlap"),
        "a window entirely before the ring is not an error and not an empty list of zeroes"
    );

    // Reversed bounds: `from` after `to`.
    let reversed = must(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("v"),
            as_val!(NOW + 9 * minute),
            as_val!(NOW),
            Value::Nil,
            Value::Nil,
            as_val!("m"),
        ],
    )
    .await;

    assert_eq!(str_of(&reversed, "result").as_deref(), Some("no_overlap"));

    // --- the ends of i64 as report bounds ---

    let widest = must(
        &client,
        &key,
        "ts_report",
        &[
            as_val!("v"),
            as_val!(i64::MIN),
            as_val!(i64::MAX),
            as_val!(4),
            Value::Nil,
            as_val!("m"),
        ],
    )
    .await;

    // Clamped to the live window rather than attempting sixty million years of buckets.
    assert_eq!(str_of(&widest, "result").as_deref(), Some("ok"));
    assert_eq!(i64_of(&widest, "points"), Some(4), "clamped, then downsampled to what was asked");
    assert_eq!(i64_of(&widest, "from_ms"), Some(NOW - 50 * minute), "the window's own start");

    // --- max_points at its edges ---

    for (max_points, expect_points) in [(1i64, 1i64), (60, 60), (600, 60)] {
        let report = must(
            &client,
            &key,
            "ts_report",
            &[as_val!("v"), Value::Nil, Value::Nil, as_val!(max_points), Value::Nil, as_val!("m")],
        )
        .await;

        assert_eq!(
            i64_of(&report, "points"),
            Some(expect_points),
            "max_points={max_points} on a 60-slot archive"
        );
    }

    // Zero and negative are ignored rather than dividing by zero or producing an empty answer.
    for max_points in [0i64, -1, i64::MIN] {
        let report = must(
            &client,
            &key,
            "ts_report",
            &[as_val!("v"), Value::Nil, Value::Nil, as_val!(max_points), Value::Nil, as_val!("m")],
        )
        .await;

        assert_eq!(
            i64_of(&report, "points"),
            Some(60),
            "max_points={max_points} should mean 'no limit', not 'no points'"
        );
    }

    // --- a batch longer than the window ---
    //
    // 200 contiguous samples into a 60-slot archive, starting where the data left off: every one is accepted, the early
    // ones are retired by the later ones **inside the same call**, and the window ends up holding the last 60.
    let times: Vec<Value> = (0..200).map(|i| as_val!(NOW + (10 + i) * minute)).collect();
    let values: Vec<Value> = (0..200).map(|i| Value::from(i as f64)).collect();
    let batch = must(
        &client,
        &key,
        "ts_append_many",
        &[as_val!("v"), Value::from(times), Value::from(values)],
    )
    .await;

    assert_eq!(i64_of(&batch, "offered"), Some(200));
    assert_eq!(i64_of(&batch, "accepted"), Some(200), "{batch:?}");
    assert_eq!(i64_of(&batch, "placed"), Some(200));

    let after = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("v"), Value::Nil, Value::Nil, Value::Nil, Value::Nil, as_val!("m")],
    )
    .await;

    let got = values_of(&after);

    assert_eq!(got.len(), 60);
    assert_eq!(
        got,
        (140..200).map(|i| Some(i as f64)).collect::<Vec<_>>(),
        "the newest sixty of the batch, and nothing older"
    );

    // --- a value of the wrong type ---

    let err = call(
        &client,
        &key,
        "ts_append",
        &[as_val!("v"), as_val!(NOW + 300 * minute), as_val!("not a number")],
    )
    .await
    .expect_err("a string is not a sample");

    assert!(err.contains("f64"), "{err}");

    // --- non-finite samples ---
    //
    // A NaN would be indistinguishable from the unknown marker, so it has to be refused rather than stored as a gap.
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let outcome = call(
            &client,
            &key,
            "ts_append",
            &[as_val!("v"), as_val!(NOW + 301 * minute), Value::from(bad)],
        )
        .await;

        match outcome {
            Err(e) => assert!(e.contains("finite") || e.contains("NaN"), "{bad}: {e}"),
            Ok(map) => panic!("{bad} was accepted: {map:?}"),
        }
    }

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// **An outage longer than the window wedges a series until someone says `wipe=1`.**
///
/// This is the wipe guard working as documented — a gap reaching `slots` erases every slot, so it is refused — but the
/// consequence is worth pinning rather than discovering: the refusal *cascades*. Because a rejected append does not move
/// `last_bucket`, every later sample is still a whole window away, so a series that goes quiet for longer than it can
/// hold never accepts anything again on its own.
///
/// A one-hour window and a two-hour outage is an ordinary Tuesday, not a misconfiguration, which is what makes this
/// sharper than the seconds-versus-milliseconds case the guard was built for.
#[tokio::test]
async fn an_outage_longer_than_the_window_needs_an_explicit_decision() {
    let Some(client) = connect().await else {
        eprintln!("skipping: no server");
        return;
    };

    let ns = namespace();
    let set = set_name("outage");
    let minute = 60_000i64;

    // A 60-slot minute archive: one hour of history.
    let key = fresh_archived(&client, &set, 1, "v", "archives=m:60").await;

    for i in 0..10i64 {
        must(
            &client,
            &key,
            "ts_append",
            &[as_val!("v"), as_val!(NOW + i * minute), Value::from(i as f64)],
        )
        .await;
    }

    // Two hours of silence, then the agent comes back with its buffer.
    let times: Vec<Value> = (0..30).map(|i| as_val!(NOW + (120 + i) * minute)).collect();
    let values: Vec<Value> = (0..30).map(|i| Value::from(100.0 + i as f64)).collect();
    let batch = must(
        &client,
        &key,
        "ts_append_many",
        &[as_val!("v"), Value::from(times.clone()), Value::from(values.clone())],
    )
    .await;

    assert_eq!(i64_of(&batch, "accepted"), Some(0), "every sample refused");
    assert_eq!(
        i64_of(&batch, "would_wipe"),
        Some(30),
        "and all thirty for the same reason, because the first refusal left last_bucket where it was"
    );

    // The old data is intact, which is what the guard is protecting.
    let intact = must(
        &client,
        &key,
        "ts_report",
        &[as_val!("v"), Value::Nil, Value::Nil, Value::Nil, Value::Nil, as_val!("m")],
    )
    .await;

    assert_eq!(i64_of(&intact, "known"), Some(10), "the pre-outage samples are still there");

    // **Recovery one: re-initialise with `wipe=1`.** The config is part of the shape, so this is a deliberate act on the
    // series rather than something an append can decide for itself.
    let key2 = fresh_archived(&client, &set, 2, "v", "archives=m:60,wipe=1").await;

    for i in 0..10i64 {
        must(
            &client,
            &key2,
            "ts_append",
            &[as_val!("v"), as_val!(NOW + i * minute), Value::from(i as f64)],
        )
        .await;
    }

    let recovered = must(
        &client,
        &key2,
        "ts_append_many",
        &[as_val!("v"), Value::from(times), Value::from(values)],
    )
    .await;

    assert_eq!(i64_of(&recovered, "accepted"), Some(30), "with wipe=1 the batch lands");

    let report = must(
        &client,
        &key2,
        "ts_report",
        &[as_val!("v"), Value::Nil, Value::Nil, Value::Nil, Value::Nil, as_val!("m")],
    )
    .await;

    assert_eq!(i64_of(&report, "known"), Some(30), "the new samples");
    assert_eq!(
        i64_of(&report, "unknown"),
        Some(30),
        "and the outage shows as a gap rather than as zeroes"
    );

    // **Recovery two: a sample inside the window still works.** The guard is about the *gap*, so a series that keeps
    // reporting at all is never wedged - which is why this only bites after a real outage.
    let key3 = fresh_archived(&client, &set, 3, "v", "archives=m:60").await;

    must(&client, &key3, "ts_append", &[as_val!("v"), as_val!(NOW), Value::from(1.0)]).await;

    let edge = must(
        &client,
        &key3,
        "ts_append",
        &[as_val!("v"), as_val!(NOW + 59 * minute), Value::from(2.0)],
    )
    .await;

    assert_eq!(
        str_of(&edge, "result").as_deref(),
        Some("placed"),
        "59 minutes into a 60-slot window is the last gap that is not a wipe"
    );

    let over = must(
        &client,
        &key3,
        "ts_append",
        &[as_val!("v"), as_val!(NOW + 59 * minute + 60 * minute), Value::from(3.0)],
    )
    .await;

    assert_eq!(str_of(&over, "result").as_deref(), Some("would_wipe"), "and 60 is the first that is");

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// Run `ts_rollup` and return its finalized map.
async fn rollup(
    client: &Client,
    ns: &str,
    set: &str,
    args: &[Value],
) -> HashMap<String, Value> {
    let mut statement = Statement::new(ns, set, Bins::All);

    statement.set_aggregate_function(&module(), "ts_rollup", Some(args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut combiner = Combiner::<Rollup>::new(args).expect("combiner");
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        for value in record.unwrap().bins.values() {
            combiner.absorb(value).expect("absorb");
        }
    }

    as_string_map(&combiner.finish().expect("finalize")).expect("a map")
}
