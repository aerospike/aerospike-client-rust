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

//! Tier D hint-flag integration tests (field `44` EXPLAIN flags).

use super::{
    explain_plan, supports_query_selection, AGE_BIN, BOGUS_INDEX_NAME, COUNTRY_BIN, SCORE_BIN,
};
use crate::common;

use aerospike::query::QuerySelection;
use aerospike::{
    as_bin, as_key, AdminPolicy, Client, CollectionIndexType, IndexType, QueryPolicy,
    QueryWhereWire, ResultCode, Task, WritePolicy, FLAG_EXPLAIN, FLAG_HARD_HINT, FLAG_REQUIRE_INDEX,
};

struct HintFixture {
    set_name: String,
    age_index_name: String,
    score_index_name: String,
}

async fn prepare_hint_fixture(client: &Client) -> HintFixture {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let age_index_name = format!("{namespace}_{set_name}_age_idx");
    let score_index_name = format!("{namespace}_{set_name}_score_idx");
    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();

    for (key, age, score, country) in [(1, 25, 25, "US"), (2, 30, 30, "CA")] {
        let key = as_key!(namespace, &set_name, key);
        let bins = vec![
            as_bin!(AGE_BIN, age),
            as_bin!(SCORE_BIN, score),
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

    HintFixture {
        set_name,
        age_index_name,
        score_index_name,
    }
}

async fn explain_hint_raw(
    client: &Client,
    set_name: &str,
    ael: &str,
    index_name_hint: Option<&str>,
    explain_where_flags: Option<u8>,
) -> aerospike::Result<aerospike::QueryPlan> {
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
}

/// `REQUIRE_INDEX` on a PI-eligible WHERE rejects explain.
#[aerospike_macro::test]
async fn query_selection_hint_require_index_on_primary_fails() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_hint_fixture(&client).await;
    let err = explain_hint_raw(
        &client,
        &fixture.set_name,
        "$.country == 'US'",
        None,
        Some(FLAG_EXPLAIN | FLAG_REQUIRE_INDEX),
    )
    .await
    .unwrap_err();

    assert_eq!(err.server_result_code(), Some(ResultCode::IndexNotFound));

    client.close().await.unwrap();
}

/// `REQUIRE_INDEX` + soft `forIndex` still selects the matching secondary index.
#[aerospike_macro::test]
async fn query_selection_hint_require_index_with_soft_hint() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_hint_fixture(&client).await;
    let plan = explain_plan(
        &client,
        &fixture.set_name,
        "$.age == 25",
        Some(&fixture.score_index_name),
        Some(FLAG_EXPLAIN | FLAG_REQUIRE_INDEX),
    )
    .await;

    assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(plan.index_name(), Some(fixture.age_index_name.as_str()));
    assert!(plan.index_range_bytes().is_some());
    assert_eq!(
        QueryWhereWire::flags(plan.explain_where_bytes()).unwrap(),
        FLAG_EXPLAIN | FLAG_REQUIRE_INDEX
    );

    client.close().await.unwrap();
}

/// `HARD_HINT` + matching `forIndex` selects that index.
#[aerospike_macro::test]
async fn query_selection_hint_hard_hint_matching_index() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_hint_fixture(&client).await;
    let plan = explain_plan(
        &client,
        &fixture.set_name,
        "$.age == 25",
        Some(&fixture.age_index_name),
        Some(FLAG_EXPLAIN | FLAG_HARD_HINT),
    )
    .await;

    assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(plan.index_name(), Some(fixture.age_index_name.as_str()));
    assert_eq!(
        QueryWhereWire::flags(plan.explain_where_bytes()).unwrap(),
        FLAG_EXPLAIN | FLAG_HARD_HINT
    );

    client.close().await.unwrap();
}

/// Both hint flags with index hint.
#[aerospike_macro::test]
async fn query_selection_hint_require_index_and_hard_hint() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_hint_fixture(&client).await;
    let plan = explain_plan(
        &client,
        &fixture.set_name,
        "$.age == 25",
        Some(&fixture.age_index_name),
        Some(FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT),
    )
    .await;

    assert_eq!(plan.index_name(), Some(fixture.age_index_name.as_str()));
    assert_eq!(
        QueryWhereWire::flags(plan.explain_where_bytes()).unwrap(),
        FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT
    );

    client.close().await.unwrap();
}

/// `HARD_HINT` with wrong index name fails explain.
#[aerospike_macro::test]
async fn query_selection_hint_hard_hint_wrong_index_fails() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_hint_fixture(&client).await;
    let err = explain_hint_raw(
        &client,
        &fixture.set_name,
        "$.age == 25",
        Some(BOGUS_INDEX_NAME),
        Some(FLAG_EXPLAIN | FLAG_HARD_HINT),
    )
    .await
    .unwrap_err();

    assert_eq!(err.server_result_code(), Some(ResultCode::IndexNotFound));

    client.close().await.unwrap();
}

/// Syntactically invalid AEL fails explain with `PARAMETER`.
#[aerospike_macro::test]
async fn query_selection_hint_bad_ael_fails_parameter() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_hint_fixture(&client).await;
    let err = explain_hint_raw(&client, &fixture.set_name, "$.age > 30 and", None, None)
        .await
        .unwrap_err();

    assert_eq!(err.server_result_code(), Some(ResultCode::ParameterError));

    client.close().await.unwrap();
}
