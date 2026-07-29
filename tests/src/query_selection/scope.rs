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

//! Explain/execute scope across index shapes (integer, blob, MAPKEYS CDT).

use std::collections::HashMap;

use futures::stream::StreamExt;

use super::{count_records, execute_ael, explain_plan, supports_query_selection, AGE_BIN, COUNTRY_BIN};
use crate::common;

use aerospike::query::QuerySelection;
use aerospike::{
    as_bin, as_key, AdminPolicy, Bins, Client, CollectionIndexType, IndexType, Task, Value,
    WritePolicy,
};

const BLOB_BIN: &str = "bb";
const MAP_BIN: &str = "map_bin";
const MAP_KEY: &str = "mkey2";

fn map_contains_str_key(map_value: &Value, key: &str) -> bool {
    let key = Value::String(key.into());
    match map_value {
        Value::HashMap(m) => m.contains_key(&key),
        Value::OrderedMap(m) => m.contains_key(&key),
        Value::SortedMap(m) => m.contains_key(&key),
        _ => false,
    }
}

struct ScopeFixture {
    set_name: String,
    blob_index_name: String,
    blob_hex: String,
}

async fn prepare_scope_fixture(client: &Client) -> ScopeFixture {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let age_index_name = format!("{namespace}_{set_name}_age_idx");
    let blob_index_name = format!("{namespace}_{set_name}_bb_idx");
    let map_index_name = format!("{namespace}_{set_name}_map_idx");
    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();

    let mut blob_bytes = [0u8; 8];
    blob_bytes.copy_from_slice(&50001_i64.to_be_bytes());
    let blob_hex = hex::encode(blob_bytes);

    let mut map = HashMap::new();
    map.insert(
        Value::String(MAP_KEY.into()),
        Value::String("v1".into()),
    );

    let key1 = as_key!(namespace, &set_name, "k1");
    client
        .put(
            &wpolicy,
            &key1,
            &[
                as_bin!(AGE_BIN, 25),
                as_bin!(COUNTRY_BIN, "US"),
                as_bin!(BLOB_BIN, blob_bytes.to_vec()),
                as_bin!(MAP_BIN, Value::HashMap(map)),
            ],
        )
        .await
        .unwrap();

    let key2 = as_key!(namespace, &set_name, "k2");
    client
        .put(
            &wpolicy,
            &key2,
            &[as_bin!(AGE_BIN, 30), as_bin!(COUNTRY_BIN, "CA")],
        )
        .await
        .unwrap();

    let _index_guard = common::lock_index_ops().await;
    for (bin, index_name, index_type, cit) in [
        (
            AGE_BIN,
            &age_index_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
        ),
        (
            BLOB_BIN,
            &blob_index_name,
            IndexType::Blob,
            CollectionIndexType::Default,
        ),
        (
            MAP_BIN,
            &map_index_name,
            IndexType::String,
            CollectionIndexType::MapKeys,
        ),
    ] {
        let task = client
            .create_index_on_bin(
                &apolicy,
                namespace,
                &set_name,
                bin,
                index_name,
                index_type,
                cit,
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("failed to create {bin} index: {e}"));
        task.wait_till_complete(None).await.unwrap();
    }

    ScopeFixture {
        set_name,
        blob_index_name,
        blob_hex,
    }
}

/// BLOB scalar equality → secondary index on explain.
#[aerospike_macro::test]
async fn query_selection_scope_blob_explain_selects_secondary_index() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_scope_fixture(&client).await;
    let ael = format!("$.{BLOB_BIN} == x'{}'", fixture.blob_hex);
    let plan = explain_plan(&client, &fixture.set_name, &ael, None, None).await;

    assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
    assert_eq!(plan.index_name(), Some(fixture.blob_index_name.as_str()));
    assert!(plan.index_range_bytes().is_some());

    client.close().await.unwrap();
}

/// BLOB equality explain → execute returns the matching row.
#[aerospike_macro::test]
async fn query_selection_scope_blob_execute_returns_matching_row() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_scope_fixture(&client).await;
    let ael = format!("$.{BLOB_BIN} == x'{}'", fixture.blob_hex);
    let rs = execute_ael(
        &client,
        &fixture.set_name,
        &ael,
        Bins::from([BLOB_BIN]),
        None,
        None,
    )
    .await;
    assert_eq!(count_records(rs).await, 1);

    client.close().await.unwrap();
}

/// MAPKEYS + CDT `.exists()` → primary index fallback on explain.
#[aerospike_macro::test]
async fn query_selection_scope_mapkeys_exists_primary_index_fallback() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_scope_fixture(&client).await;
    let ael = format!("$.{MAP_BIN}.{MAP_KEY}.exists() == true");
    let plan = explain_plan(&client, &fixture.set_name, &ael, None, None).await;

    assert_eq!(plan.selection(), QuerySelection::PrimaryIndex);
    assert!(plan.index_name().is_none());
    assert!(plan.index_range_bytes().is_none());
    assert!(!plan.explain_where_bytes().is_empty());

    client.close().await.unwrap();
}

/// MAPKEYS EXISTS explain → execute returns rows containing the map key.
#[aerospike_macro::test]
async fn query_selection_scope_mapkeys_exists_execute_returns_matching_rows() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }

    let fixture = prepare_scope_fixture(&client).await;
    let ael = format!("$.{MAP_BIN}.{MAP_KEY}.exists() == true");
    let rs = execute_ael(
        &client,
        &fixture.set_name,
        &ael,
        Bins::from([MAP_BIN]),
        None,
        None,
    )
    .await;

    let mut count = 0;
    let mut stream = rs.into_stream();
    while let Some(res) = stream.next().await {
        let rec = res.expect("record read failed");
        let map_bin = rec.bins.get(MAP_BIN).expect("missing map bin");
        assert!(
            map_contains_str_key(map_bin, MAP_KEY),
            "unexpected map bin value: {map_bin:?}"
        );
        count += 1;
    }
    assert_ne!(count, 0);

    client.close().await.unwrap();
}
