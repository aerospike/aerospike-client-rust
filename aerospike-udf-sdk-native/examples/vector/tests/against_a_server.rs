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

//! Vector search against real records.
//!
//! ```text
//! mod-native { module vector { path …/libaerospike_udf_example_vector.so } }
//! ```
//!
//! The central assertion is **exactness**: the top-k a distributed scan returns must equal the top-k computed
//! here from the same vectors. Anything less and this would be an approximate index with extra steps.

use std::collections::HashMap;
use std::env;

use aerospike_core::{
    as_bin, as_key, as_val, Bins, Client, ClientPolicy, Key, PartitionFilter, QueryPolicy, Statement,
    Value, WritePolicy,
};
use aerospike_udf_example_vector::vec::Metric;
use futures::StreamExt;

const DIM: usize = 8;
const RECORDS: i64 = 500;

fn module() -> String {
    env::var("AEROSPIKE_VECTOR_MODULE").unwrap_or_else(|_| "vector".to_string())
}

fn namespace() -> String {
    env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

fn skipped(test: &str, why: &str) {
    eprintln!("skipping {test}: {why}");
}

async fn connect() -> Option<Client> {
    let mut policy = ClientPolicy::default();

    policy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
        .unwrap_or(false);

    Client::new(&policy, &env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".into()))
        .await
        .ok()
}

/// A deterministic pseudo-vector, so the expected answer can be computed without storing it.
fn vector_for(i: i64) -> Vec<f32> {
    (0..DIM)
        .map(|d| {
            let x = (i as f32 * 0.37 + d as f32 * 1.11).sin();

            x * 10.0
        })
        .collect()
}

fn pack(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn as_map_of(value: &Value) -> HashMap<Value, Value> {
    match value {
        Value::HashMap(m) => m.clone(),
        Value::OrderedMap(m) => m.clone().into_iter().collect(),
        Value::SortedMap(m) => m.clone().into_iter().collect(),
        other => panic!("wanted a map, got {other:?}"),
    }
}

/// Write `RECORDS` records, each with a packed-f32 vector and an integer `n`.
async fn seed(client: &Client, namespace: &str, set: &str) -> Key {
    let write = WritePolicy::default();

    for i in 1..=RECORDS {
        let key = as_key!(namespace, set, i);

        client
            .put(
                &write,
                &key,
                &[
                    as_bin!("v", Value::Blob(pack(&vector_for(i)))),
                    as_bin!("n", i),
                ],
            )
            .await
            .unwrap();
    }

    as_key!(namespace, set, 1i64)
}

async fn available(client: &Client, key: &Key) -> bool {
    client
        .execute_udf(
            &WritePolicy::default(),
            key,
            &module(),
            "vec_dim",
            Some(&[as_val!("v")]),
        )
        .await
        .is_ok()
}

/// Run `topk` and return (distance, digest) best-first, plus the skip counters.
///
/// **Reads the partial's shape, not the finalized one** — and the distinction is the design, not a detail. The
/// server calls `to_partial`, which carries the internal `score` (negated for a similarity metric so the heap has
/// one comparison). `finalize`, which converts that back to the `distance` a caller asked about, runs on the
/// *client*. So this helper un-inverts the score itself; the sibling test does it by handing the partials to
/// `Combiner`, which runs the plugin's own `finalize`.
async fn topk(
    client: &Client,
    namespace: &str,
    set: &str,
    query: &[f32],
    metric: &str,
    k: i64,
) -> (Vec<(f64, Vec<u8>)>, i64, i64) {
    let m = Metric::parse(metric).unwrap();
    let args = [
        as_val!("v"),
        Value::Blob(pack(query)),
        as_val!(metric),
        as_val!(k),
    ];

    let mut statement = Statement::new(namespace, set, Bins::All);

    statement.set_aggregate_function(&module(), "topk", Some(&args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    // One partial per query thread; the client merges them. Done by hand here rather than through `Combiner`,
    // because the point of this test is that the *merge* is what makes the answer exact - see the sibling test
    // for the `Combiner` path.
    let mut all: Vec<(f64, Vec<u8>)> = Vec::new();
    let (mut dim, mut none) = (0i64, 0i64);
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        for (_, value) in record.unwrap().bins.iter() {
            let map = as_map_of(value);

            if let Some(Value::Int(n)) = map.get(&as_val!("skipped_dim")) {
                dim += n;
            }

            if let Some(Value::Int(n)) = map.get(&as_val!("skipped_none")) {
                none += n;
            }

            let Some(Value::List(hits)) = map.get(&as_val!("hits")) else {
                continue;
            };

            for hit in hits {
                let h = as_map_of(hit);
                let score = match h.get(&as_val!("score")) {
                    Some(Value::Float(f)) => f64::from(f.clone()),
                    Some(Value::Int(i)) => *i as f64,
                    other => panic!("no score in {other:?} - a partial carries `score`, not `distance`"),
                };
                let Some(Value::Blob(digest)) = h.get(&as_val!("digest")) else {
                    panic!("no digest in {h:?}");
                };

                // Back to the number the metric means, which is what `finalize` would have done.
                all.push((m.unscore(score), digest.clone()));
            }
        }
    }

    // By score, so `dot` ranks the right way round - sorting by the reported distance would put the *worst*
    // inner products first.
    all.sort_by(|a, b| {
        m.score(a.0)
            .total_cmp(&m.score(b.0))
            .then_with(|| a.1.cmp(&b.1))
    });
    all.truncate(k as usize);

    (all, dim, none)
}

/// **The exactness test.** The server's top-k must equal the top-k of the same vectors computed here.
#[tokio::test]
async fn topk_is_exact() {
    let Some(client) = connect().await else {
        skipped("topk_is_exact", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("vec_topk_{}", std::process::id());
    let probe = seed(&client, &namespace, &set).await;

    if !available(&client, &probe).await {
        skipped("topk_is_exact", "no usable vector module");
        return;
    }

    let query = vector_for(7);

    for metric in ["cosine", "dot", "sqeuclidean", "euclidean"] {
        let m = Metric::parse(metric).unwrap();
        let k = 5;

        // What it should be: every vector measured here, sorted the way the metric means.
        let mut expected: Vec<(f64, i64)> = (1..=RECORDS)
            .map(|i| (m.score(m.measure(&query, &vector_for(i)).unwrap()), i))
            .collect();

        expected.sort_by(|a, b| a.0.total_cmp(&b.0));
        expected.truncate(k as usize);

        let (got, dim, none) = topk(&client, &namespace, &set, &query, metric, k).await;

        assert_eq!(dim, 0, "{metric}: no record should have the wrong dimension");
        assert_eq!(none, 0, "{metric}: every record has a vector");
        assert_eq!(got.len(), k as usize, "{metric}: k hits");

        // Compare the distances, which is what the caller sees. Comparing digests would need the key→digest
        // mapping; the distances are the ranking and are what has to match.
        let got_scores: Vec<f64> = got.iter().map(|(d, _)| m.score(*d)).collect();
        let want_scores: Vec<f64> = expected.iter().map(|(s, _)| *s).collect();

        for (i, (g, w)) in got_scores.iter().zip(want_scores.iter()).enumerate() {
            assert!(
                (g - w).abs() < 1e-4,
                "{metric}: rank {i} should be {w} but was {g} - a distributed top-k must equal the whole-set one"
            );
        }

        // The nearest vector to itself is itself, which is the sanity check the arithmetic cannot fake.
        if metric == "cosine" || metric == "sqeuclidean" || metric == "euclidean" {
            assert!(
                got[0].0.abs() < 1e-4,
                "{metric}: the query is in the set, so the best distance should be ~0, got {}",
                got[0].0
            );
        }
    }
}

/// The client combining the server's partials with the plugin's own heap, which is the point of `Combiner`.
#[tokio::test]
async fn a_client_combines_topk_partials() {
    use aerospike_udf_example_vector::TopK;
    use aerospike_udf_sdk_native::client::Combiner;

    let Some(client) = connect().await else {
        skipped("a_client_combines_topk_partials", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("vec_comb_{}", std::process::id());
    let probe = seed(&client, &namespace, &set).await;

    if !available(&client, &probe).await {
        skipped("a_client_combines_topk_partials", "no usable vector module");
        return;
    }

    let query = vector_for(7);
    let k = 5i64;
    let args = [
        as_val!("v"),
        Value::Blob(pack(&query)),
        as_val!("cosine"),
        as_val!(k),
    ];

    let mut statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

    statement.set_aggregate_function(&module(), "topk", Some(&args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut partials = Vec::new();
    let mut stream = records.into_stream();

    while let Some(record) = stream.next().await {
        for (_, value) in record.unwrap().bins.iter() {
            partials.push(value.clone());
        }
    }

    assert!(!partials.is_empty(), "the query produced no partial");

    // The plugin's own `merge`, run on the client, over partials a real server produced.
    let mut combiner = Combiner::<TopK>::new(&args).unwrap();

    for partial in &partials {
        combiner.absorb(partial).unwrap();
    }

    let answer = combiner.finish().unwrap();
    let map = as_map_of(&answer);

    let Some(Value::List(hits)) = map.get(&as_val!("hits")) else {
        panic!("no hits in {map:?}");
    };

    assert_eq!(hits.len(), k as usize, "the combined answer holds exactly k");

    // And it agrees with the by-hand merge in the other test.
    let (expected, _, _) = topk(&client, &namespace, &set, &query, "cosine", k).await;

    for (i, hit) in hits.iter().enumerate() {
        let h = as_map_of(hit);
        let d = match h.get(&as_val!("distance")) {
            Some(Value::Float(f)) => f64::from(f.clone()),
            other => panic!("no distance in {other:?}"),
        };

        assert!(
            (d - expected[i].0).abs() < 1e-9,
            "rank {i}: the Combiner and a by-hand merge must agree"
        );
    }

    eprintln!("combined {} partial(s) into a top-{k}", partials.len());
}

/// A record with a wrong-dimension vector, or none at all, is **skipped and counted**.
#[tokio::test]
async fn wrong_dimensions_are_counted_not_fatal() {
    let Some(client) = connect().await else {
        skipped("wrong_dimensions_are_counted_not_fatal", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("vec_skip_{}", std::process::id());
    let write = WritePolicy::default();
    let probe = seed(&client, &namespace, &set).await;

    if !available(&client, &probe).await {
        skipped("wrong_dimensions_are_counted_not_fatal", "no usable vector module");
        return;
    }

    // Three records with a vector of the wrong length, and two with no vector bin at all.
    for i in 0..3i64 {
        let key = as_key!(namespace.as_str(), set.as_str(), 9000 + i);
        let short: Vec<f32> = vec![1.0; DIM - 2];

        client
            .put(&write, &key, &[as_bin!("v", Value::Blob(pack(&short)))])
            .await
            .unwrap();
    }

    for i in 0..2i64 {
        let key = as_key!(namespace.as_str(), set.as_str(), 9100 + i);

        client.put(&write, &key, &[as_bin!("n", i)]).await.unwrap();
    }

    let (hits, dim, none) = topk(&client, &namespace, &set, &vector_for(7), "cosine", 5).await;

    assert_eq!(hits.len(), 5, "the good records still produce a full top-k");
    assert_eq!(dim, 3, "the three short vectors must be counted, not ignored");
    assert_eq!(none, 2, "the two records with no vector must be counted too");
}

/// The record functions, on one record by key.
#[tokio::test]
async fn the_vector_api_on_a_single_record() {
    let Some(client) = connect().await else {
        skipped("the_vector_api_on_a_single_record", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("vec_api_{}", std::process::id());
    let write = WritePolicy::default();
    let key = as_key!(namespace.as_str(), set.as_str(), 1i64);
    let v = vector_for(3);

    client
        .put(&write, &key, &[as_bin!("v", Value::Blob(pack(&v)))])
        .await
        .unwrap();

    if !available(&client, &key).await {
        skipped("the_vector_api_on_a_single_record", "no usable vector module");
        return;
    }

    // Dimensionality.
    assert_eq!(
        client
            .execute_udf(&write, &key, &module(), "vec_dim", Some(&[as_val!("v")]))
            .await
            .unwrap(),
        Some(Value::Int(DIM as i64))
    );

    // The norm, against one computed here.
    let want_norm = v.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();
    let got = client
        .execute_udf(&write, &key, &module(), "vec_norm", Some(&[as_val!("v")]))
        .await
        .unwrap();

    let Some(Value::Float(n)) = got else {
        panic!("wanted a norm, got {got:?}");
    };

    assert!((f64::from(n) - want_norm).abs() < 1e-3, "norm should be {want_norm}");

    // The distance to itself is zero under cosine, and the vector's squared norm under dot.
    for (metric, expected) in [("cosine", 0.0), ("dot", want_norm * want_norm)] {
        let got = client
            .execute_udf(
                &write,
                &key,
                &module(),
                "vec_distance",
                Some(&[as_val!("v"), Value::Blob(pack(&v)), as_val!(metric)]),
            )
            .await
            .unwrap();

        let Some(Value::Float(d)) = got else {
            panic!("{metric}: wanted a distance, got {got:?}");
        };

        assert!(
            (f64::from(d.clone()) - expected).abs() < 1e-2,
            "{metric}: distance to itself should be {expected}, got {d:?}"
        );
    }

    // A bin that is not a vector answers nil rather than failing.
    assert_eq!(
        client
            .execute_udf(&write, &key, &module(), "vec_dim", Some(&[as_val!("nosuchbin")]))
            .await
            .unwrap(),
        None
    );

    // An unknown metric is an error - a typo must not become a silent ranking by something else.
    assert!(client
        .execute_udf(
            &write,
            &key,
            &module(),
            "vec_distance",
            Some(&[as_val!("v"), Value::Blob(pack(&v)), as_val!("manhattan")])
        )
        .await
        .is_err());
}

/// `orderby` — the same bounded heap, a scalar key.
#[tokio::test]
async fn orderby_takes_the_first_n() {
    let Some(client) = connect().await else {
        skipped("orderby_takes_the_first_n", "no server");
        return;
    };

    let namespace = namespace();
    let set = format!("vec_order_{}", std::process::id());
    let probe = seed(&client, &namespace, &set).await;

    if !available(&client, &probe).await {
        skipped("orderby_takes_the_first_n", "no usable vector module");
        return;
    }

    for (direction, want) in [("asc", vec![1, 2, 3]), ("desc", vec![RECORDS, RECORDS - 1, RECORDS - 2])] {
        let args = [as_val!("n"), as_val!(direction), as_val!(3)];
        let mut statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

        statement.set_aggregate_function(&module(), "orderby", Some(&args));

        let records = client
            .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
            .await
            .unwrap();

        let mut rows: Vec<f64> = Vec::new();
        let mut stream = records.into_stream();

        while let Some(record) = stream.next().await {
            for (_, value) in record.unwrap().bins.iter() {
                let map = as_map_of(value);

                // A partial, so `hits[].score` - `rows[].value` is what `finalize` produces, on the client.
                let Some(Value::List(list)) = map.get(&as_val!("hits")) else {
                    continue;
                };

                for row in list {
                    let score = match as_map_of(row).get(&as_val!("score")) {
                        Some(Value::Float(f)) => f64::from(f.clone()),
                        Some(Value::Int(i)) => *i as f64,
                        other => panic!("no score in {other:?}"),
                    };

                    // `desc` was negated on the way in, for the same one-comparison reason.
                    rows.push(if direction == "asc" { score } else { -score });
                }
            }
        }

        // One partial per thread; the client keeps the best three of all of them.
        if direction == "asc" {
            rows.sort_by(|a, b| a.total_cmp(b));
        } else {
            rows.sort_by(|a, b| b.total_cmp(a));
        }

        rows.truncate(3);

        assert_eq!(
            rows,
            want.iter().map(|v| *v as f64).collect::<Vec<f64>>(),
            "orderby {direction} should give {want:?}"
        );
    }

    // A direction that is not asc or desc is an error, not a default.
    let args = [as_val!("n"), as_val!("sideways"), as_val!(3)];
    let mut statement = Statement::new(namespace.as_str(), set.as_str(), Bins::All);

    statement.set_aggregate_function(&module(), "orderby", Some(&args));

    let records = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut stream = records.into_stream();
    let mut failed = false;

    while let Some(r) = stream.next().await {
        if r.is_err() {
            failed = true;
        }
    }

    assert!(failed, "an unknown direction must fail the query");
}
