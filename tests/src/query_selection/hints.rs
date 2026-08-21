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

use super::{explain_plan, supports_query_selection, AGE_BIN, COUNTRY_BIN, SCORE_BIN};
use crate::common;

use aerospike::query::QuerySelection;
use aerospike::{
    as_bin, as_key, AdminPolicy, Client, CollectionIndexType, IndexType, Task, WritePolicy,
    FLAG_EXPLAIN, FLAG_HARD_HINT, FLAG_REQUIRE_INDEX,
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
    assert_eq!(plan.ael().unwrap(), "$.age == 25");

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
    assert_eq!(plan.ael().unwrap(), "$.age == 25");

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
    assert_eq!(plan.ael().unwrap(), "$.age == 25");

    client.close().await.unwrap();
}
