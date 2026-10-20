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

use crate::common;

use aerospike::{QueryPolicy, ResultCode, FLAG_EXPLAIN, FLAG_HARD_HINT, FLAG_REQUIRE_INDEX};

use super::{prepare_fixture, supports_query_selection, BOGUS_INDEX_NAME, COUNTRY_BIN};

const BAD_AEL: &str = "$.age > 30 and";

fn policy(verbosity: u8) -> QueryPolicy {
    let mut policy = QueryPolicy::default();
    policy.base_policy.error_detail_verbosity = verbosity;
    policy
}

#[aerospike_macro::test]
async fn bad_ael_message_verbosity_fails_without_expression_trace() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }
    let fixture = prepare_fixture(&client).await;

    let err = client
        .query_explain(
            &policy(2),
            common::namespace(),
            Some(&fixture.set_name),
            BAD_AEL,
            None,
            None,
        )
        .await
        .expect_err("invalid AEL should fail during explain");

    assert_eq!(err.server_result_code(), Some(ResultCode::ParameterError));
    assert_eq!(err.sub_code(), 0);
    assert!(err
        .server_error_detail()
        .and_then(|detail| detail.exp_trace.as_ref())
        .is_none());
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn bad_ael_expression_trace_verbosity_fails_at_explain() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }
    let fixture = prepare_fixture(&client).await;

    let err = client
        .query_explain(
            &policy(3),
            common::namespace(),
            Some(&fixture.set_name),
            BAD_AEL,
            None,
            None,
        )
        .await
        .expect_err("invalid AEL should fail during explain");

    assert_eq!(err.server_result_code(), Some(ResultCode::ParameterError));
    assert_eq!(err.sub_code(), 0);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn require_index_rejection_carries_no_refining_subcode() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }
    let fixture = prepare_fixture(&client).await;

    let err = client
        .query_explain(
            &policy(2),
            common::namespace(),
            Some(&fixture.set_name),
            &format!("$.{COUNTRY_BIN} == 'US'"),
            None,
            Some(FLAG_EXPLAIN | FLAG_REQUIRE_INDEX),
        )
        .await
        .expect_err("primary-index fallback should be rejected");

    assert_eq!(err.server_result_code(), Some(ResultCode::IndexNotFound));
    assert_eq!(err.sub_code(), 0);
    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn hard_hint_rejection_carries_no_refining_subcode() {
    let client = common::client().await;
    if !supports_query_selection(&client).await {
        return;
    }
    let fixture = prepare_fixture(&client).await;

    let err = client
        .query_explain(
            &policy(2),
            common::namespace(),
            Some(&fixture.set_name),
            "$.age == 25",
            Some(BOGUS_INDEX_NAME),
            Some(FLAG_EXPLAIN | FLAG_HARD_HINT),
        )
        .await
        .expect_err("hard hint to a missing index should be rejected");

    assert_eq!(err.server_result_code(), Some(ResultCode::IndexNotFound));
    assert_eq!(err.sub_code(), 0);
    client.close().await.unwrap();
}
