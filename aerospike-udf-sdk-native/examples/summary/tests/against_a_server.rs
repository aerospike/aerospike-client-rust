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

//! The aggregation against a real server, on data whose statistics are known independently.
//!
//! What these reach that the unit tests cannot: the partial actually crossing the wire and being merged. Every answer
//! here is a *combined* one — one partial per query thread, merged by the client through the plugin's own `merge`.
//!
//! Expected values are closed forms wherever possible. Comparing against a second implementation of the same formula
//! would only prove the two agree.

use std::collections::HashMap;
use std::env;

use aerospike_core::{
    as_bin, as_key, as_val, Bins, Client, ClientPolicy, PartitionFilter, QueryPolicy, Statement,
    Value, WritePolicy,
};
use futures::StreamExt;

const RECORDS: i64 = 2_000;

fn module() -> String {
    env::var("AEROSPIKE_SUMMARY_MODULE").unwrap_or_else(|_| "summary".to_string())
}

/// Connect, or decide whether not reaching a server is a skip or a failure.
///
/// **The rule: if `AEROSPIKE_HOSTS` is set, a connection failure is a failure.** Earlier in this project a suite
/// reported "11 passed" in 0.07 seconds because every test called `connect().await.ok()`, got `None`, and returned
/// early - which hid a real connectivity problem for a while and is the same silent-success shape as three server bugs
/// found here. Skipping is only defensible when nobody asked for a server.
async fn connect() -> Option<Client> {
    let asked_for_a_server = env::var("AEROSPIKE_HOSTS").is_ok();

    match connect_inner().await {
        Some(client) => Some(client),
        None if asked_for_a_server => panic!(
            "AEROSPIKE_HOSTS is set to {:?} but the client could not reach it - failing rather than \
             reporting a pass for a test that did not run",
            env::var("AEROSPIKE_HOSTS").unwrap_or_default()
        ),
        None => None,
    }
}

async fn connect_inner() -> Option<Client> {
    let mut policy = ClientPolicy::default();

    // False by default: the dev VM configures no alternate access address, and turning this on there leaves the client
    // with no address and a read timeout that looks like a network fault.
    policy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
        .unwrap_or(false);

    Client::new(
        &policy,
        &env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string()),
    )
    .await
    .ok()
}

/// Run the aggregation and return its finalized map, merged across every partial the query produced.
///
/// The combining is the plugin's own `merge`, run through the SDK's local host — the same code path a real client
/// uses, not a test-only reimplementation of it.
async fn summarise(
    client: &Client,
    namespace: &str,
    set: &str,
    args: &[Value],
) -> HashMap<String, Value> {
    use aerospike_udf_example_summary::Summary;
    use aerospike_udf_sdk_native::client::Combiner;

    let mut statement = Statement::new(namespace, set, Bins::All);

    statement.set_aggregate_function(&module(), "summary", Some(args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut combiner = Combiner::<Summary>::new(args).expect("combiner");
    let mut stream = records.into_stream();
    let mut partials = 0;

    while let Some(record) = stream.next().await {
        for value in record.unwrap().bins.values() {
            combiner.absorb(value).expect("absorb a partial");
            partials += 1;
        }
    }

    // No partials means the module is missing or the query matched nothing, and either way every assertion below
    // would be meaningless. Failing here beats a suite that passes because it tested nothing.
    assert!(partials > 0, "the query returned no partials - is the '{}' module configured?", module());

    match combiner.finish().expect("finalize") {
        Value::HashMap(map) => map
            .into_iter()
            .filter_map(|(k, v)| match k {
                Value::String(s) => Some((s, v)),
                _ => None,
            })
            .collect(),
        other => panic!("summary should finalize to a map, got {other:?}"),
    }
}

fn f64_of(map: &HashMap<String, Value>, key: &str) -> Option<f64> {
    match map.get(key) {
        Some(Value::Float(f)) => Some(f64::from(f.clone())),
        Some(Value::Int(i)) => Some(*i as f64),
        _ => None,
    }
}

fn i64_of(map: &HashMap<String, Value>, key: &str) -> Option<i64> {
    match map.get(key) {
        Some(Value::Int(i)) => Some(*i),
        _ => None,
    }
}

fn str_of(map: &HashMap<String, Value>, key: &str) -> Option<String> {
    match map.get(key) {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn close(got: f64, want: f64, tol: f64) -> bool {
    (got - want).abs() <= tol * want.abs().max(1.0)
}

async fn load<F>(client: &Client, namespace: &str, set: &str, n: i64, bin: F)
where
    F: Fn(i64) -> Value + Copy + Send + Sync,
{
    let write = WritePolicy::default();

    futures::stream::iter(0..n)
        .for_each_concurrent(64, |i| {
            let client = &client;
            let write = &write;

            async move {
                let key = as_key!(namespace, set, i);

                client.put(write, &key, &[as_bin!("v", bin(i))]).await.unwrap();
            }
        })
        .await;
}

async fn cleanup(client: &Client, namespace: &str, set: &str) {
    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), namespace, set, 0)
        .await;
}

/// **The exact statistics, against closed forms.**
///
/// Values 1..=n in an integer bin, so every exact statistic has an arithmetic answer: the sum is n(n+1)/2, the mean is
/// (n+1)/2, and the sample variance of 1..n is n(n+1)/12.
#[tokio::test]
async fn the_exact_statistics_are_exact() {
    let Some(client) = connect().await else {
        eprintln!("skipping the_exact_statistics_are_exact: no server");
        return;
    };

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("summary_exact_{}", std::process::id());

    load(&client, &namespace, &set, RECORDS, |i| Value::Int(i + 1)).await;

    let map = summarise(&client, &namespace, &set, &[as_val!("v")]).await;
    let n = RECORDS as f64;

    assert_eq!(i64_of(&map, "count"), Some(RECORDS), "count");
    assert_eq!(i64_of(&map, "read_int"), Some(RECORDS), "every value was an integer");
    assert_eq!(i64_of(&map, "read_double"), Some(0));

    assert!(close(f64_of(&map, "sum").unwrap(), n * (n + 1.0) / 2.0, 1e-12), "sum");
    assert!(close(f64_of(&map, "mean").unwrap(), (n + 1.0) / 2.0, 1e-12), "mean");

    // An integer bin, so the total crossed the wire as two halves of an `i128` and came back exact - not close.
    assert_eq!(
        i64_of(&map, "sum_exact"),
        Some(RECORDS * (RECORDS + 1) / 2),
        "an integer bin has an exactly reportable total"
    );
    assert_eq!(i64_of(&map, "sum_saturated"), None, "nothing saturated");
    assert_eq!(str_of(&map, "sum_exact_str"), None, "and it fitted an i64, so no string form");
    assert_eq!(f64_of(&map, "min"), Some(1.0));
    assert_eq!(f64_of(&map, "max"), Some(n));
    assert_eq!(f64_of(&map, "range"), Some(n - 1.0));

    // Sample variance of 1..n is n(n+1)/12.
    let variance = n * (n + 1.0) / 12.0;

    assert!(
        close(f64_of(&map, "variance").unwrap(), variance, 1e-9),
        "variance: got {:?}, want {variance}",
        f64_of(&map, "variance")
    );
    assert!(close(f64_of(&map, "stddev").unwrap(), variance.sqrt(), 1e-9), "stddev");

    // A uniform run is symmetric, and its excess kurtosis tends to -1.2.
    assert!(f64_of(&map, "skewness").unwrap().abs() < 1e-6, "a uniform run is not skewed");
    assert!(
        close(f64_of(&map, "kurtosis_excess").unwrap(), -1.2, 0.01),
        "uniform excess kurtosis"
    );

    // Harmonic mean of 1..n is n / H(n), and the geometric mean is below it - the AM-GM-HM ordering, which is a
    // property of the means rather than of this implementation.
    let (am, gm, hm) = (
        f64_of(&map, "mean").unwrap(),
        f64_of(&map, "geometric_mean").unwrap(),
        f64_of(&map, "harmonic_mean").unwrap(),
    );

    assert!(hm < gm && gm < am, "AM-GM-HM ordering: {hm} < {gm} < {am}");

    // Every value distinct, so the estimator should say so within its own error band.
    let distinct = f64_of(&map, "distinct_estimate").unwrap();
    let band = 5.0 * f64_of(&map, "distinct_error").unwrap() * n + 2.0;

    assert!(
        (distinct - n).abs() <= band,
        "distinct: got {distinct}, expected about {n} within {band}"
    );

    cleanup(&client, &namespace, &set).await;
}

/// **The percentiles, against the true order statistics and the error the sketch itself reports.**
#[tokio::test]
async fn percentiles_hold_their_stated_error() {
    let Some(client) = connect().await else {
        eprintln!("skipping percentiles_hold_their_stated_error: no server");
        return;
    };

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("summary_pct_{}", std::process::id());

    // Skewed, which is the shape percentiles get used on, and fully known so the true percentile is computable here.
    let values: Vec<f64> = (1..=RECORDS).map(|i| (i as f64).powf(1.5) / 10.0).collect();

    load(&client, &namespace, &set, RECORDS, |i| {
        Value::from(((i + 1) as f64).powf(1.5) / 10.0)
    })
    .await;

    let map = summarise(&client, &namespace, &set, &[as_val!("v")]).await;

    assert_eq!(i64_of(&map, "count"), Some(RECORDS));
    assert_eq!(i64_of(&map, "read_double"), Some(RECORDS), "every value was a double");

    let eps = f64_of(&map, "percentile_error").unwrap();

    assert!(close(eps, 0.01, 1e-12), "the default error should be 1%");
    assert_eq!(map.get("percentile_collapsed"), None, "no bucket should have collapsed");
    assert_eq!(map.get("percentile_epsilon_mismatch"), None, "no precision mismatch");

    let mut sorted = values.clone();
    sorted.sort_by(f64::total_cmp);

    for (key, q) in [
        ("p25", 0.25),
        ("median", 0.5),
        ("p75", 0.75),
        ("p90", 0.90),
        ("p95", 0.95),
        ("p99", 0.99),
    ] {
        let want = sorted[(((sorted.len() - 1) as f64) * q).round() as usize];
        let got = f64_of(&map, key).unwrap_or_else(|| panic!("{key} missing"));

        assert!(
            (got - want).abs() <= eps * want,
            "{key}: got {got}, true {want}, outside the promised {}%",
            eps * 100.0
        );
    }

    // IQR is p75 - p25, so it should agree with the two it is built from.
    let (p25, p75, iqr) = (
        f64_of(&map, "p25").unwrap(),
        f64_of(&map, "p75").unwrap(),
        f64_of(&map, "iqr").unwrap(),
    );

    assert!(close(iqr, p75 - p25, 1e-9), "iqr should be p75 - p25");

    cleanup(&client, &namespace, &set).await;
}

/// **The mode, and the flag that says whether to believe it.**
///
/// A low-cardinality bin — the only kind a mode is meaningful for — so the table never overflows and the counts are
/// exact. `mode_is_certain` is asserted alongside the value, because a mode without it is a number a caller cannot
/// check.
#[tokio::test]
async fn the_mode_is_found_and_reported_as_certain() {
    let Some(client) = connect().await else {
        eprintln!("skipping the_mode_is_found_and_reported_as_certain: no server");
        return;
    };

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("summary_mode_{}", std::process::id());

    // Deliberately unbalanced: 200 in seven of every ten records.
    const STATUSES: [i64; 10] = [200, 200, 200, 200, 200, 200, 200, 301, 404, 500];

    load(&client, &namespace, &set, RECORDS, |i| {
        Value::Int(STATUSES[(i as usize) % STATUSES.len()])
    })
    .await;

    let map = summarise(&client, &namespace, &set, &[as_val!("v")]).await;

    assert_eq!(i64_of(&map, "count"), Some(RECORDS));
    assert_eq!(f64_of(&map, "mode"), Some(200.0), "200 is the most frequent status");
    assert_eq!(
        i64_of(&map, "mode_count"),
        Some(RECORDS * 7 / 10),
        "seven in ten records carry it"
    );
    assert_eq!(i64_of(&map, "mode_is_certain"), Some(1), "nothing was evicted, so it is certain");
    assert_eq!(map.get("mode_count_error"), None, "an exact count carries no error term");
    assert_eq!(i64_of(&map, "distinct_values_tracked"), Some(4), "four distinct statuses");

    let distinct = f64_of(&map, "distinct_estimate").unwrap();

    assert!((distinct - 4.0).abs() < 1.0, "distinct of a tiny domain: got {distinct}");

    // The median of that multiset is 200, since 70% of it is 200 — but the sketch returns a *bucket
    // representative*, not the value, so it comes back as 198.4 rather than exactly 200.
    //
    // **Worth asserting as the guarantee rather than as equality.** The first version of this test demanded 200.0 and
    // failed, which looked like a bug and was the test contradicting the documented contract: an approximate
    // percentile is approximate even when the true answer is one overwhelmingly dominant value. If exactness matters
    // for a low-cardinality bin, `mode` is the statistic that gives it.
    let eps = f64_of(&map, "percentile_error").unwrap();
    let median = f64_of(&map, "median").unwrap();

    assert!(
        (median - 200.0).abs() <= eps * 200.0,
        "the median should be within {}% of the dominant value 200, got {median}",
        eps * 100.0
    );

    cleanup(&client, &namespace, &set).await;
}

/// A bin that is missing, non-numeric or a mixture of types is **counted**, not silently dropped.
#[tokio::test]
async fn unusable_records_are_counted_rather_than_ignored() {
    let Some(client) = connect().await else {
        eprintln!("skipping unusable_records_are_counted_rather_than_ignored: no server");
        return;
    };

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("summary_skip_{}", std::process::id());
    let write = WritePolicy::default();

    // 10 integers, 10 doubles, 5 strings, and 5 records without the bin at all.
    for i in 0..10i64 {
        let key = as_key!(namespace.as_str(), set.as_str(), format!("i{i}"));
        client.put(&write, &key, &[as_bin!("v", i + 1)]).await.unwrap();
    }

    for i in 0..10i64 {
        let key = as_key!(namespace.as_str(), set.as_str(), format!("d{i}"));
        client.put(&write, &key, &[as_bin!("v", i as f64 + 0.5)]).await.unwrap();
    }

    for i in 0..5i64 {
        let key = as_key!(namespace.as_str(), set.as_str(), format!("s{i}"));
        client.put(&write, &key, &[as_bin!("v", "not a number")]).await.unwrap();
    }

    for i in 0..5i64 {
        let key = as_key!(namespace.as_str(), set.as_str(), format!("m{i}"));
        client.put(&write, &key, &[as_bin!("other", i)]).await.unwrap();
    }

    let map = summarise(&client, &namespace, &set, &[as_val!("v")]).await;

    assert_eq!(i64_of(&map, "count"), Some(20), "only the numeric records count");
    assert_eq!(i64_of(&map, "read_int"), Some(10), "and the mix is visible");
    assert_eq!(i64_of(&map, "read_double"), Some(10));
    assert_eq!(i64_of(&map, "skipped_wrong_type"), Some(5), "strings were counted out");
    assert_eq!(i64_of(&map, "skipped_absent"), Some(5), "so were the records without the bin");

    cleanup(&client, &namespace, &set).await;
}

/// **A total no `f64` can hold, held anyway** — across the wire and through a merge.
///
/// The bin is `2⁵³ + i`, so every value is past the point where consecutive integers stay representable as doubles:
/// widening on the way in would lose the low bit of each one *before* any summing. The integer accumulator never
/// widens, so the answer is exact, and it is checked against a closed form computed in `i128`.
///
/// The total also exceeds `i64`, which is the case `sum_exact_str` exists for: there is no Aerospike integer wide
/// enough, and a double would defeat the purpose of having been exact.
#[tokio::test]
async fn a_sum_past_the_doubles_reach_is_still_exact() {
    let Some(client) = connect().await else {
        eprintln!("skipping a_sum_past_the_doubles_reach_is_still_exact: no server");
        return;
    };

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("summary_wide_{}", std::process::id());
    let base = 1i64 << 53;

    // The premise, asserted rather than assumed: these values do not survive an f64.
    assert_ne!((base + 1) as f64 as i64, base + 1);

    load(&client, &namespace, &set, RECORDS, |i| Value::Int(base + i)).await;

    let map = summarise(&client, &namespace, &set, &[as_val!("v")]).await;

    // Σ(2^53 + i) for i in 0..n.
    let n = RECORDS as i128;
    let truth = n * (base as i128) + n * (n - 1) / 2;

    assert_eq!(i64_of(&map, "count"), Some(RECORDS));
    assert_eq!(i64_of(&map, "read_int"), Some(RECORDS));
    assert!(i64::try_from(truth).is_err(), "the premise: this total is past i64");

    assert_eq!(
        str_of(&map, "sum_exact_str"),
        Some(truth.to_string()),
        "the exact total, as the only lossless shape Aerospike can carry it in"
    );
    assert_eq!(i64_of(&map, "sum_exact"), None, "and not as an i64, which cannot hold it");
    assert_eq!(i64_of(&map, "sum_saturated"), None);

    // And the double `sum` is a different number - which is the whole reason the exact key is there. Reported, useful,
    // and lossy, all three at once.
    let rounded = f64_of(&map, "sum").unwrap();

    assert_ne!(
        rounded as i128, truth,
        "if the double ever matches, this test has stopped demonstrating anything"
    );
    assert!(
        (rounded as i128 - truth).abs() < (1 << 16),
        "though it should still be the correctly rounded value: {rounded} against {truth}"
    );

    cleanup(&client, &namespace, &set).await;
}

/// A bin that is an integer in some records and a double in others has **no** exact total, and does not claim one.
///
/// The two accumulators are what make that distinction possible at all: the integer side is still exact for the
/// records it saw, and the module reports the pair of read counts, but the combined answer went through one rounding
/// so no exact key appears.
#[tokio::test]
async fn a_mixed_type_bin_offers_no_exact_total() {
    let Some(client) = connect().await else {
        eprintln!("skipping a_mixed_type_bin_offers_no_exact_total: no server");
        return;
    };

    let namespace = env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string());
    let set = format!("summary_mixed_{}", std::process::id());

    // Every fourth record stores a double. Halves, so the true total is still an exact binary fraction and the
    // assertion below can be an equality rather than a tolerance.
    load(&client, &namespace, &set, RECORDS, |i| {
        if i % 4 == 3 {
            Value::from((i + 1) as f64 / 2.0)
        } else {
            Value::Int(i + 1)
        }
    })
    .await;

    let map = summarise(&client, &namespace, &set, &[as_val!("v")]).await;

    let ints = (0..RECORDS).filter(|i| i % 4 != 3);
    let doubles = (0..RECORDS).filter(|i| i % 4 == 3);
    let int_total: i64 = ints.clone().map(|i| i + 1).sum();
    let double_total: f64 = doubles.clone().map(|i| (i + 1) as f64 / 2.0).sum();

    assert_eq!(i64_of(&map, "read_int"), Some(ints.count() as i64));
    assert_eq!(i64_of(&map, "read_double"), Some(doubles.count() as i64));
    assert_eq!(i64_of(&map, "count"), Some(RECORDS));

    assert_eq!(
        f64_of(&map, "sum"),
        Some(int_total as f64 + double_total),
        "the two sides combine once, at the end"
    );

    assert_eq!(
        i64_of(&map, "sum_exact"),
        None,
        "a double was seen, so there is no exact integer total to report"
    );
    assert_eq!(str_of(&map, "sum_exact_str"), None);

    cleanup(&client, &namespace, &set).await;
}
