// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration tests for two-phase server query selection (explain → execute).
//!
//! Ported from Java fluent `QuerySelectionIntegrationTest`,
//! `QuerySelectionHintFlagsTest`, and `QuerySelectionExplainScopeTest`.
//!
//! Requires Aerospike Server >= 8.1.3; tests self-skip when the connected
//! node's [`Version::supports_query_selection`] is false.
//!
//! Debug query-plan logs (matching Java `Loggers.QUERY`):
//! `RUST_LOG=query=debug cargo test --test lib query_selection -- --nocapture`
//! (or `RUST_LOG=debug` for all targets). Tests self-skip without explain when
//! [`Version::supports_query_selection`] is false — look for `Skipping:` on stderr.

mod hints;
mod scope;

use std::sync::Arc;

use futures::stream::StreamExt;

use crate::common;

use aerospike::query::{PartitionFilter, QueryPlan, QuerySelection, Statement};
use aerospike::{
    as_bin, as_key, AdminPolicy, Bins, Client, ClientPolicy, CollectionIndexType, IndexType,
    QueryPolicy, ReadPolicy, Recordset, Task, Value, WritePolicy,
};

pub(crate) const DATASET_SIZE: i64 = 50;
pub(crate) const AGE_BIN: &str = "age";
pub(crate) const SCORE_BIN: &str = "score";
pub(crate) const COUNTRY_BIN: &str = "country";
pub(crate) const BOGUS_INDEX_NAME: &str = "qsel_nonexistent_idx";

pub(crate) struct QuerySelectionFixture {
    pub set_name: String,
    pub age_index_name: String,
    pub score_index_name: String,
}

pub(crate) async fn supports_query_selection(client: &Client) -> bool {
    let ok = client
        .cluster
        .get_random_node()
        .map(|node| node.version().supports_query_selection())
        .unwrap_or(false);
    if !ok {
        eprintln!("Skipping: server does not support query selection (requires >= 8.1.3)");
    }
    ok
}

pub(crate) async fn prepare_fixture(client: &Client) -> QuerySelectionFixture {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let age_index_name = format!("{namespace}_{set_name}_age_idx");
    let score_index_name = format!("{namespace}_{set_name}_score_idx");
    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();

    for i in 1..=DATASET_SIZE {
        let country = if i % 2 == 0 { "US" } else { "CA" };
        let key = as_key!(namespace, &set_name, i);
        let bins = vec![
            as_bin!(AGE_BIN, i),
            as_bin!(SCORE_BIN, i),
            as_bin!(COUNTRY_BIN, country),
        ];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    let _index_guard = common::lock_index_ops().await;
    for (bin, index_name) in [
        (AGE_BIN, &age_index_name),
        (SCORE_BIN, &score_index_name),
    ] {
        let task = client
            .create_index_on_bin(
                &apolicy,
                namespace,
                &set_name,
                bin,
                index_name,
                IndexType::Numeric,
                CollectionIndexType::Default,
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("failed to create {bin} index: {e}"));
        task.wait_till_complete(None).await.unwrap();
    }

    QuerySelectionFixture {
        set_name,
        age_index_name,
        score_index_name,
    }
}

pub(crate) async fn explain_plan(
    client: &Client,
    set_name: &str,
    ael: &str,
    index_name_hint: Option<&str>,
    explain_where_flags: Option<u8>,
) -> QueryPlan {
    let namespace = common::namespace();
    client
        .query_explain(
            &QueryPolicy::default(),
            namespace,
            Some(set_name),
            ael,
            index_name_hint,
            explain_where_flags,
        )
        .await
        .expect("query explain failed")
}

pub(crate) async fn execute_plan(
    client: &Client,
    set_name: &str,
    plan: QueryPlan,
    bins: Bins,
) -> Arc<Recordset> {
    let namespace = common::namespace();
    let statement = Statement::new(namespace, set_name, bins);
    client
        .query_with_plan(
            &QueryPolicy::default(),
            PartitionFilter::all(),
            statement,
            plan,
        )
        .await
        .expect("query with plan failed")
}

pub(crate) async fn execute_ael(
    client: &Client,
    set_name: &str,
    ael: &str,
    bins: Bins,
    index_name_hint: Option<&str>,
    explain_where_flags: Option<u8>,
) -> Arc<Recordset> {
    let plan = explain_plan(client, set_name, ael, index_name_hint, explain_where_flags).await;
    execute_plan(client, set_name, plan, bins).await
}

pub(crate) async fn collect_int_bin(rs: Arc<Recordset>, bin: &str) -> Vec<i64> {
    let mut values = Vec::new();
    let mut stream = rs.into_stream();
    while let Some(res) = stream.next().await {
        let rec = res.expect("record read failed");
        match rec.bins.get(bin) {
            Some(Value::Int(v)) => values.push(*v),
            other => panic!("unexpected {bin} bin value: {other:?}"),
        }
    }
    values.sort_unstable();
    values
}

pub(crate) async fn collect_string_bin(rs: Arc<Recordset>, bin: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut stream = rs.into_stream();
    while let Some(res) = stream.next().await {
        let rec = res.expect("record read failed");
        match rec.bins.get(bin) {
            Some(Value::String(v)) => values.push(v.clone()),
            other => panic!("unexpected {bin} bin value: {other:?}"),
        }
    }
    values
}

pub(crate) async fn count_records(rs: Arc<Recordset>) -> usize {
    let mut count = 0;
    let mut stream = rs.into_stream();
    while let Some(res) = stream.next().await {
        res.expect("record read failed");
        count += 1;
    }
    count
}

/// Index on bin `age`. Range AEL → server selects the age secondary index.
#[aerospike_macro::test]
async fn query_selection_explain_selects_secondary_index_for_age_range() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let namespace = common::namespace();
    let fixture = prepare_fixture(&client).await;
    let ael = "$.age >= 14 and $.age <= 18";
    let plan = explain_plan(&client, &fixture.set_name, ael, None, None).await;

    assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
    assert!(plan.is_secondary_index());
    assert_eq!(plan.namespace(), namespace);
    assert_eq!(plan.set_name(), Some(fixture.set_name.as_str()));
    assert_eq!(plan.index_name(), Some(fixture.age_index_name.as_str()));
    assert!(plan.index_range_bytes().is_some());
    assert!(!plan.ael().unwrap().is_empty());

    client.close().await.unwrap();
}

/// No index on `country`. Equality AEL → server selects primary index.
#[aerospike_macro::test]
async fn query_selection_explain_selects_primary_index_for_non_indexed_predicate() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let namespace = common::namespace();
    let fixture = prepare_fixture(&client).await;
    let plan = explain_plan(&client, &fixture.set_name, "$.country == 'US'", None, None).await;

    assert_eq!(plan.selection(), QuerySelection::PrimaryIndex);
    assert!(plan.is_primary_index());
    assert_eq!(plan.namespace(), namespace);
    assert_eq!(plan.set_name(), Some(fixture.set_name.as_str()));
    assert!(plan.index_name().is_none());
    assert!(plan.index_range_bytes().is_none());
    assert!(!plan.ael().unwrap().is_empty());

    client.close().await.unwrap();
}

/// Contradictory age range → explain filtered out.
#[aerospike_macro::test]
async fn query_selection_explain_contradiction_predicate_filtered_out() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let plan = explain_plan(&client, &fixture.set_name, "$.age > 100 and $.age < 10", None, None).await;

    assert_eq!(plan.selection(), QuerySelection::FilteredOut);
    assert!(plan.is_filtered_out());
    assert!(plan.index_name().is_none());
    assert!(plan.index_range_bytes().is_none());
    assert!(!plan.ael().unwrap().is_empty());

    client.close().await.unwrap();
}

/// Explain then execute on the same age range → five matching records (14..=18).
#[aerospike_macro::test]
async fn query_selection_execute_returns_matching_records() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let rs = execute_ael(
        &client,
        &fixture.set_name,
        "$.age >= 14 and $.age <= 18",
        Bins::from([AGE_BIN]),
        None,
        None,
    )
    .await;
    let ages = collect_int_bin(rs, AGE_BIN).await;
    assert_eq!(ages, vec![14, 15, 16, 17, 18]);

    client.close().await.unwrap();
}

/// Equality on indexed bin → single matching record.
#[aerospike_macro::test]
async fn query_selection_execute_equality_returns_single_record() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let rs = execute_ael(
        &client,
        &fixture.set_name,
        "$.age == 25",
        Bins::from([AGE_BIN]),
        None,
        None,
    )
    .await;
    let ages = collect_int_bin(rs, AGE_BIN).await;
    assert_eq!(ages, vec![25]);

    client.close().await.unwrap();
}

/// Primary-index execute on non-indexed predicate → 25 US rows.
#[aerospike_macro::test]
async fn query_selection_execute_primary_index_returns_matching_records() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let rs = execute_ael(
        &client,
        &fixture.set_name,
        "$.country == 'US'",
        Bins::from([COUNTRY_BIN]),
        None,
        None,
    )
    .await;
    let countries = collect_string_bin(rs, COUNTRY_BIN).await;
    assert_eq!(countries.len(), 25);
    assert!(countries.iter().all(|c| c == "US"));

    client.close().await.unwrap();
}

/// Compound predicate → SI on age; rows satisfy both conjuncts.
#[aerospike_macro::test]
async fn query_selection_execute_compound_predicate_returns_matching_records() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let ael = "$.age > 30 and $.country == 'US'";
    let plan = explain_plan(&client, &fixture.set_name, ael, None, None).await;
    assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(plan.index_name(), Some(fixture.age_index_name.as_str()));

    let rs = execute_plan(
        &client,
        &fixture.set_name,
        plan,
        Bins::from([AGE_BIN, COUNTRY_BIN]),
    )
    .await;

    let mut ages = Vec::new();
    let mut stream = rs.into_stream();
    while let Some(res) = stream.next().await {
        let rec = res.expect("record read failed");
        assert_eq!(rec.bins.get(COUNTRY_BIN), Some(&Value::String("US".into())));
        match rec.bins.get(AGE_BIN) {
            Some(Value::Int(age)) => {
                assert!(*age > 30);
                ages.push(*age);
            }
            other => panic!("unexpected age bin value: {other:?}"),
        }
    }
    ages.sort_unstable();
    assert_eq!(ages, vec![32, 34, 36, 38, 40, 42, 44, 46, 48, 50]);

    client.close().await.unwrap();
}

/// Valid SI query with no matching data → empty stream, not an error.
#[aerospike_macro::test]
async fn query_selection_execute_no_matches_returns_empty_stream() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let ael = "$.age == 999";
    let plan = explain_plan(&client, &fixture.set_name, ael, None, None).await;
    assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(plan.index_name(), Some(fixture.age_index_name.as_str()));

    let rs = execute_ael(&client, &fixture.set_name, ael, Bins::from([AGE_BIN]), None, None).await;
    assert_eq!(count_records(rs).await, 0);

    client.close().await.unwrap();
}

/// Repeated explain on the same AEL → stable selection and wire bytes.
#[aerospike_macro::test]
async fn query_selection_explain_bytes_stable_across_repeated_probes() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let ael = "$.age >= 14 and $.age <= 18";
    let first = explain_plan(&client, &fixture.set_name, ael, None, None).await;
    let second = explain_plan(&client, &fixture.set_name, ael, None, None).await;

    assert_eq!(first.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(first.index_name(), Some(fixture.age_index_name.as_str()));
    assert_eq!(first.selection(), second.selection());
    assert_eq!(first.index_name(), second.index_name());
    assert_eq!(first.ael().unwrap(), second.ael().unwrap());
    assert_eq!(first.index_range_bytes(), second.index_range_bytes());

    client.close().await.unwrap();
}

/// Soft `forIndex` hint → server still selects the matching age index.
#[aerospike_macro::test]
async fn query_selection_explain_for_index_hint_uses_hinted_index() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let ael = "$.age >= 14 and $.age <= 18";
    let plan = explain_plan(
        &client,
        &fixture.set_name,
        ael,
        Some(&fixture.age_index_name),
        None,
    )
    .await;

    assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(plan.index_name(), Some(fixture.age_index_name.as_str()));
    assert!(plan.index_range_bytes().is_some());

    client.close().await.unwrap();
}

/// Soft hint naming a non-existent index → server selects the correct index anyway.
#[aerospike_macro::test]
async fn query_selection_explain_for_index_hint_on_nonexistent_index() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let plan = explain_plan(
        &client,
        &fixture.set_name,
        "$.age >= 14 and $.age <= 18",
        Some(BOGUS_INDEX_NAME),
        None,
    )
    .await;

    assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(plan.index_name(), Some(fixture.age_index_name.as_str()));
    assert_ne!(plan.index_name(), Some(BOGUS_INDEX_NAME));

    client.close().await.unwrap();
}

/// Age and score indexes → server auto-selects the index matching each predicate.
#[aerospike_macro::test]
async fn query_selection_multi_index_auto_selects_matching_index() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let age_where = "$.age >= 14 and $.age <= 18";
    let score_where = "$.score >= 40 and $.score <= 44";

    let age_plan = explain_plan(&client, &fixture.set_name, age_where, None, None).await;
    let score_plan = explain_plan(&client, &fixture.set_name, score_where, None, None).await;

    let age_rs = execute_ael(
        &client,
        &fixture.set_name,
        age_where,
        Bins::from([AGE_BIN]),
        None,
        None,
    )
    .await;
    let score_rs = execute_ael(
        &client,
        &fixture.set_name,
        score_where,
        Bins::from([SCORE_BIN]),
        None,
        None,
    )
    .await;

    assert_eq!(age_plan.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(age_plan.index_name(), Some(fixture.age_index_name.as_str()));
    assert_eq!(
        collect_int_bin(age_rs, AGE_BIN).await,
        vec![14, 15, 16, 17, 18]
    );

    assert_eq!(score_plan.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(score_plan.index_name(), Some(fixture.score_index_name.as_str()));
    assert_eq!(
        collect_int_bin(score_rs, SCORE_BIN).await,
        vec![40, 41, 42, 43, 44]
    );

    client.close().await.unwrap();
}

/// Age-range query with hint for score index → server selects age index.
#[aerospike_macro::test]
async fn query_selection_explain_for_index_hint_on_wrong_existing_index() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_fixture(&client).await;
    let ael = "$.age >= 14 and $.age <= 18";
    let plan = explain_plan(
        &client,
        &fixture.set_name,
        ael,
        Some(&fixture.score_index_name),
        None,
    )
    .await;
    let rs = execute_ael(
        &client,
        &fixture.set_name,
        ael,
        Bins::from([AGE_BIN]),
        Some(&fixture.score_index_name),
        None,
    )
    .await;

    assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
    assert_ne!(plan.index_name(), Some(fixture.score_index_name.as_str()));
    assert_eq!(plan.index_name(), Some(fixture.age_index_name.as_str()));
    assert_eq!(
        collect_int_bin(rs, AGE_BIN).await,
        vec![14, 15, 16, 17, 18]
    );

    client.close().await.unwrap();
}

/// Stress pooled-connection reuse: one connection, many explain → execute → get cycles.
///
/// Guards against stale bytes in the send buffer after explain parse (no post-parse
/// `data_buffer.clear()`); a bad buffer would corrupt the next command on this connection.
const POOL_REUSE_STRESS_ITERATIONS: usize = 300;

async fn single_connection_client() -> Client {
    let mut policy = ClientPolicy::default();
    policy.min_conns_per_node = 1;
    policy.max_conns_per_node = 1;
    Client::new(&policy, &common::hosts())
        .await
        .expect("single-connection client failed to connect")
}

#[aerospike_macro::test]
async fn query_selection_pooled_connection_reuse_stress() {
    let probe = common::client().await;
    if !supports_query_selection(&probe).await {
        probe.close().await.unwrap();
        return;
    }
    probe.close().await.unwrap();

    let client = single_connection_client().await;
    let fixture = prepare_fixture(&client).await;
    let namespace = common::namespace();
    let ael = "$.age >= 14 and $.age <= 18";
    let bins = Bins::from([AGE_BIN]);
    let probe_key = as_key!(namespace, &fixture.set_name, 25_i64);
    let read_policy = ReadPolicy::default();

    for i in 0..POOL_REUSE_STRESS_ITERATIONS {
        let mut query_policy = QueryPolicy::default();
        if i % 50 == 0 {
            query_policy.base_policy.use_compression = true;
            query_policy.base_policy.compression_threshold = 0;
        }

        let plan = client
            .query_explain(
                &query_policy,
                namespace,
                Some(&fixture.set_name),
                ael,
                None,
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("explain failed on iteration {i}: {e}"));
        assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);

        let rs = execute_plan(&client, &fixture.set_name, plan, bins.clone()).await;
        assert_eq!(
            count_records(rs).await,
            5,
            "execute record count mismatch on iteration {i}"
        );

        let record = client
            .get(&read_policy, &probe_key, Bins::from([AGE_BIN]))
            .await
            .unwrap_or_else(|e| panic!("get failed on iteration {i}: {e}"));
        assert_eq!(
            record.bins.get(AGE_BIN),
            Some(&Value::from(25_i64)),
            "get bin mismatch on iteration {i}"
        );
    }

    client.close().await.unwrap();
}
