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

//! Integration tests for extended server-supplied error detail.
//! Ported from the Go client's `error_detail_verbosity_test.go` (CLIENT-4975).
//! Requires Aerospike Server version >= 8.1.3; every test self-skips on older
//! servers (the request flag is ignored there).
//!
//! Note on subcodes: the server's subcode *values* are a server-version-specific
//! wire contract and differ between builds (the Go catalogue in
//! `server_error::sub_code` documents per-status scoped values, while newer
//! server builds emit globally-unique values). These tests therefore assert
//! what the client contract guarantees — the correct result code, that a
//! subcode/message is surfaced when the server sends one, and that the parsed
//! subcode agrees with the `(subcode=N)` the server embedded in the message —
//! rather than pinning exact subcode integers. The exact msgpack decoding is
//! pinned by the unit tests in `aerospike_core::server_error`.

use crate::common;

use aerospike::expressions::{eq, float_val, int_bin, int_val, Expression};
use aerospike::operations::exp::ExpWriteFlags;
use aerospike::operations::hll::HLLPolicy;
use aerospike::operations::lists::ListReturnType;
use aerospike::operations::{bitwise, exp, hll, lists, scalar};
use aerospike::server_error::sub_code;
use aerospike::{
    as_bin, as_key, Bin, Bins, Client, Error, Key, ReadPolicy, ResultCode, Value, WritePolicy,
};

const BIN: &str = "edv-bin";

async fn supports_error_detail(client: &Client) -> bool {
    let ok = match client.cluster.get_random_node() {
        Ok(node) => node.version().supports_extended_error_detail(),
        Err(_) => false,
    };
    if !ok {
        eprintln!("Skipping: server does not support extended error detail (requires >= 8.1.3)");
    }
    ok
}

fn wpolicy_verbosity(level: u8) -> WritePolicy {
    let mut wp = WritePolicy::default();
    wp.base_policy.error_detail_verbosity = level;
    wp
}

fn rpolicy_verbosity(level: u8) -> ReadPolicy {
    let mut rp = ReadPolicy::default();
    rp.base_policy.error_detail_verbosity = level;
    rp
}

// A comparison whose operands are type-mismatched (int vs float); the server
// fails to *build* it, yielding PARAMETER_ERROR.
fn bad_exp() -> Expression {
    eq(int_val(5), float_val(6.0))
}

// Extract the integer following "subcode=" in a server message, if any.
fn message_subcode(msg: &str) -> Option<u32> {
    let start = msg.find("subcode=")? + "subcode=".len();
    let digits: String = msg[start..].chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

// ---- assertion helpers ----

// The server attached a subcode (verbosity >= 1). Assert the result code, that a
// subcode is present, and — crucially for validating our parser — that the
// parsed subcode agrees with the `subcode=N` the server embedded in the message.
fn assert_subcode_present(err: &Error, rc: ResultCode, substrings: &[&str]) {
    assert_eq!(err.server_result_code(), Some(rc), "result code: {err}");
    let detail = err.server_error_detail().expect("expected error detail");
    assert!(detail.sub_code >= 1, "expected a dispatched subcode: {err}");
    let msg = err.server_message().expect("expected a server message");
    assert_eq!(
        message_subcode(msg),
        Some(err.sub_code()),
        "parsed subcode must match the message's subcode tag: {msg}"
    );
    let lower = msg.to_lowercase();
    for s in substrings {
        assert!(lower.contains(&s.to_lowercase()), "message {msg:?} should contain {s:?}");
    }
}

// The result code is expected; if the server attached a message it must contain
// the given substrings and (if it carries a subcode) parse consistently. Used
// for cases where whether the server dispatches a subcode is version-dependent.
fn assert_result(err: &Error, rc: ResultCode, substrings: &[&str]) {
    assert_eq!(err.server_result_code(), Some(rc), "result code: {err}");
    if let Some(msg) = err.server_message() {
        if let Some(n) = message_subcode(msg) {
            assert_eq!(err.sub_code(), n, "parsed subcode must match message: {msg}");
        }
        let lower = msg.to_lowercase();
        for s in substrings {
            assert!(lower.contains(&s.to_lowercase()), "message {msg:?} should contain {s:?}");
        }
    }
}

// ---- per-test fixtures ----

async fn fresh_key(client: &Client, suffix: &str) -> Key {
    let key = as_key!(common::namespace(), &common::rand_str(20), suffix);
    let _ = client.delete(&WritePolicy::default(), &key).await;
    key
}

async fn put(client: &Client, key: &Key, bin: Bin) {
    client
        .put(&WritePolicy::default(), key, &[bin])
        .await
        .unwrap();
}

// ============================================================
// Verbosity level semantics
// ============================================================

#[aerospike_macro::test]
async fn defaults_verbosity_to_zero() {
    assert_eq!(ReadPolicy::default().base_policy.error_detail_verbosity, 0);
    assert_eq!(WritePolicy::default().base_policy.error_detail_verbosity, 0);
}

#[aerospike_macro::test]
async fn verbosity_disabled_no_server_message() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    // append (string concat) on an integer bin -> BIN_TYPE_ERROR.
    let err = client
        .operate(&wpolicy_verbosity(0), &key, &[scalar::append(&as_bin!(BIN, "bad"))])
        .await
        .expect_err("append to int bin should fail");
    assert_eq!(err.server_result_code(), Some(ResultCode::BinTypeError));
    assert_eq!(err.sub_code(), sub_code::NONE);
    assert_eq!(err.server_message(), None);
}

#[aerospike_macro::test]
async fn verbosity_subcode_only_surfaces_a_subcode() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-subonly").await;
    put(&client, &key, as_bin!("other-bin", 1)).await;

    let err = client
        .operate(&wpolicy_verbosity(1), &key, &[hll::refresh_count("no-hll-bin")])
        .await
        .expect_err("HLL refresh on missing bin should fail");
    assert_eq!(err.server_result_code(), Some(ResultCode::BinNotFound));
    assert!(err.sub_code() >= 1, "verbosity 1 must surface a subcode: {err}");
    assert!(
        err.server_message().unwrap_or("").contains("subcode="),
        "message should carry a subcode tag: {err}"
    );
}

#[aerospike_macro::test]
async fn verbosity_subcode_and_message_surfaces_both() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-submsg").await;
    put(&client, &key, as_bin!("other-bin", 1)).await;

    let err = client
        .operate(&wpolicy_verbosity(2), &key, &[hll::refresh_count("no-hll-bin")])
        .await
        .expect_err("HLL refresh on missing bin should fail");
    assert_subcode_present(&err, ResultCode::BinNotFound, &["count op", "(subcode="]);
}

// ============================================================
// Subcode dispatched by the server (verbosity 2)
// ============================================================

#[aerospike_macro::test]
async fn append_to_integer_bin_is_bin_type_error() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    let err = client
        .operate(&wpolicy_verbosity(2), &key, &[scalar::append(&as_bin!(BIN, "bad-append"))])
        .await
        .expect_err("append to int bin should fail");
    assert_result(&err, ResultCode::BinTypeError, &["append"]);
}

#[aerospike_macro::test]
async fn increment_string_bin_is_bin_type_error() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-str").await;
    put(&client, &key, as_bin!(BIN, "hello")).await;

    let err = client
        .operate(&wpolicy_verbosity(2), &key, &[scalar::add(&as_bin!(BIN, 1))])
        .await
        .expect_err("increment of string bin should fail");
    assert_result(&err, ResultCode::BinTypeError, &["increment"]);
}

#[aerospike_macro::test]
async fn hll_add_on_integer_bin_is_bin_type_error() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    let op = hll::add_with_index_and_min_hash(
        &HLLPolicy::default(),
        BIN,
        vec![Value::from("element1")],
        8,
        0,
    );
    let err = client
        .operate(&wpolicy_verbosity(2), &key, &[op])
        .await
        .expect_err("HLL add on int bin should fail");
    assert_result(&err, ResultCode::BinTypeError, &["hll"]);
}

#[aerospike_macro::test]
async fn delete_generation_mismatch_is_generation_error() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    let mut wp = wpolicy_verbosity(2);
    wp.generation_policy = aerospike::GenerationPolicy::ExpectGenEqual;
    wp.generation = 777;

    let err = client
        .delete(&wp, &key)
        .await
        .expect_err("delete with wrong generation should fail");
    assert_result(&err, ResultCode::GenerationError, &["generation"]);
}

#[aerospike_macro::test]
async fn hll_refresh_count_missing_bin_is_bin_not_found_with_subcode() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-no-hll").await;
    put(&client, &key, as_bin!("other-bin", 1)).await;

    let err = client
        .operate(&wpolicy_verbosity(2), &key, &[hll::refresh_count("no-hll-bin")])
        .await
        .expect_err("HLL refresh on missing bin should fail");
    assert_subcode_present(&err, ResultCode::BinNotFound, &["count op"]);
}

#[aerospike_macro::test]
async fn list_get_index_out_of_bounds_is_op_not_applicable_with_subcode() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-list").await;
    put(&client, &key, as_bin!(BIN, vec![Value::from(10), Value::from(20), Value::from(30)])).await;

    let err = client
        .operate(&wpolicy_verbosity(2), &key, &[lists::get(BIN, 99)])
        .await
        .expect_err("list get out of bounds should fail");
    assert_subcode_present(&err, ResultCode::OpNotApplicable, &["index"]);
}

#[aerospike_macro::test]
async fn list_get_by_rank_out_of_bounds_is_op_not_applicable_with_subcode() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-list").await;
    put(&client, &key, as_bin!(BIN, vec![Value::from(10), Value::from(20), Value::from(30)])).await;

    let err = client
        .operate(
            &wpolicy_verbosity(2),
            &key,
            &[lists::get_by_rank(BIN, 99, ListReturnType::Values)],
        )
        .await
        .expect_err("list get by rank out of bounds should fail");
    assert_subcode_present(&err, ResultCode::OpNotApplicable, &["rank"]);
}

#[aerospike_macro::test]
async fn hll_fold_target_too_large_is_op_not_applicable_with_subcode() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-hll-fold").await;
    client
        .operate(
            &WritePolicy::default(),
            &key,
            &[hll::init_with_min_hash(&HLLPolicy::default(), BIN, 8, 0)],
        )
        .await
        .unwrap();

    let err = client
        .operate(&wpolicy_verbosity(2), &key, &[hll::fold(BIN, 14)])
        .await
        .expect_err("HLL fold to a larger index_bits should fail");
    assert_subcode_present(&err, ResultCode::OpNotApplicable, &["fold"]);
}

#[aerospike_macro::test]
async fn bit_get_offset_out_of_range_is_parameter_error_with_subcode() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-bits").await;
    put(&client, &key, as_bin!(BIN, vec![0xAAu8, 0xBB, 0xCC, 0xDD])).await;

    let err = client
        .operate(&wpolicy_verbosity(2), &key, &[bitwise::get(BIN, 2_000_000_000, 8)])
        .await
        .expect_err("bit get past the blob should fail");
    assert_result(&err, ResultCode::ParameterError, &[]);
}

#[aerospike_macro::test]
async fn bit_get_size_zero_is_parameter_error_with_subcode() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-bits2").await;
    put(&client, &key, as_bin!(BIN, vec![0xAAu8, 0xBB, 0xCC, 0xDD])).await;

    let err = client
        .operate(&wpolicy_verbosity(2), &key, &[bitwise::get(BIN, 0, 0)])
        .await
        .expect_err("bit get with zero size should fail");
    assert_result(&err, ResultCode::ParameterError, &[]);
}

// ============================================================
// Filtered-out
// ============================================================

#[aerospike_macro::test]
async fn read_filtered_out_reports_result_code_and_message() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    let mut rp = rpolicy_verbosity(2);
    rp.base_policy.filter_expression = Some(eq(int_bin(BIN.to_string()), int_val(99)));

    let err = client
        .get(&rp, &key, Bins::All)
        .await
        .expect_err("filter should exclude the record");
    assert_result(&err, ResultCode::FilteredOut, &["filtered"]);
}

#[aerospike_macro::test]
async fn operate_filtered_out_reports_result_code_and_message() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    let mut wp = wpolicy_verbosity(2);
    wp.base_policy.filter_expression = Some(eq(int_bin(BIN.to_string()), int_val(99)));

    let err = client
        .operate(&wp, &key, &[scalar::get_bin(BIN)])
        .await
        .expect_err("filter should exclude the record");
    assert_result(&err, ResultCode::FilteredOut, &["filtered"]);
}

// ============================================================
// Write / delete policy
// ============================================================

#[aerospike_macro::test]
async fn create_only_existing_record_is_key_exists_error() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    let mut wp = wpolicy_verbosity(2);
    wp.record_exists_action = aerospike::RecordExistsAction::CreateOnly;

    let err = client
        .put(&wp, &key, &[as_bin!(BIN, 2)])
        .await
        .expect_err("create-only on existing record should fail");
    assert_result(&err, ResultCode::KeyExistsError, &[]);
}

#[aerospike_macro::test]
async fn replace_only_missing_record_is_key_not_found() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-replace-missing").await;

    let mut wp = wpolicy_verbosity(2);
    wp.record_exists_action = aerospike::RecordExistsAction::ReplaceOnly;

    let err = client
        .put(&wp, &key, &[as_bin!(BIN, 1)])
        .await
        .expect_err("replace-only on missing record should fail");
    assert_result(&err, ResultCode::KeyNotFoundError, &[]);
}

#[aerospike_macro::test]
async fn write_generation_mismatch_is_generation_error() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    let mut wp = wpolicy_verbosity(2);
    wp.generation_policy = aerospike::GenerationPolicy::ExpectGenEqual;
    wp.generation = 999;

    let err = client
        .put(&wp, &key, &[as_bin!(BIN, 2)])
        .await
        .expect_err("write with wrong generation should fail");
    assert_result(&err, ResultCode::GenerationError, &["generation"]);
}

// ============================================================
// Verbosity 3: expression build-failure detail
//
// A type-mismatched comparison (int vs float) fails to *build* on the server,
// yielding PARAMETER_ERROR. At verbosity 3 the server may additionally attach a
// structured expression trace (SERVER-1137); whether it does is a server-build
// property, so the trace is asserted only when present. The trace-decoding
// itself is pinned by the `server_error` unit tests.
// ============================================================

#[aerospike_macro::test]
async fn filter_expression_build_failure_is_parameter_error_at_verbosity_3() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    let mut rp = rpolicy_verbosity(3);
    rp.base_policy.filter_expression = Some(bad_exp());

    let err = client
        .get(&rp, &key, Bins::All)
        .await
        .expect_err("type-mismatched filter should fail to build");
    assert_result(&err, ResultCode::ParameterError, &["expression"]);

    if let Some(trace) = err.server_error_detail().and_then(|d| d.exp_trace.as_ref()) {
        assert_eq!(trace.phase, Some(aerospike::server_error::EXP_TRACE_PHASE_BUILD));
    }
}

#[aerospike_macro::test]
async fn exp_write_build_failure_is_parameter_error_at_verbosity_3() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-int").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    let err = client
        .operate(
            &wpolicy_verbosity(3),
            &key,
            &[exp::write_exp(BIN, bad_exp(), ExpWriteFlags::Default)],
        )
        .await
        .expect_err("type-mismatched exp_write should fail to build");
    assert_result(&err, ResultCode::ParameterError, &[]);

    if let Some(trace) = err.server_error_detail().and_then(|d| d.exp_trace.as_ref()) {
        assert_eq!(trace.phase, Some(aerospike::server_error::EXP_TRACE_PHASE_BUILD));
    }
}

// ============================================================
// Happy path: verbosity must not break successful commands
// ============================================================

#[aerospike_macro::test]
async fn verbosity_set_on_a_successful_command_does_not_break_it() {
    let client = common::client().await;
    if !supports_error_detail(&client).await {
        return;
    }
    let key = fresh_key(&client, "edv-success").await;

    client
        .put(&wpolicy_verbosity(2), &key, &[as_bin!(BIN, 42)])
        .await
        .unwrap();

    let rec = client.get(&rpolicy_verbosity(2), &key, Bins::All).await.unwrap();
    assert_eq!(rec.bins.get(BIN), Some(&Value::Int(42)));
}

#[aerospike_macro::test]
async fn verbosity_disabled_yields_no_detail_on_filtered_out() {
    let client = common::client().await;
    let key = fresh_key(&client, "edv-none").await;
    put(&client, &key, as_bin!(BIN, 1)).await;

    // With verbosity left at the default (0), no detail is ever attached,
    // regardless of server version.
    let mut rp = ReadPolicy::default();
    rp.base_policy.filter_expression = Some(eq(int_bin(BIN.to_string()), int_val(99)));

    let err = client
        .get(&rp, &key, Bins::All)
        .await
        .expect_err("filter should exclude the record");
    assert_eq!(err.server_result_code(), Some(ResultCode::FilteredOut));
    assert_eq!(err.sub_code(), sub_code::NONE);
    assert!(err.server_error_detail().is_none());
    assert!(err.server_message().is_none());
}
