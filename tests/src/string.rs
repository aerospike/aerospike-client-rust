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

//! Integration tests for CDT string operations.
//! Requires Aerospike Server version >= 8.1.3.

use std::collections::HashMap;

use crate::common;

use aerospike::operations::cdt_context::{ctx_list_index, ctx_map_key};
use aerospike::operations::string as str_op;
use aerospike::operations::string::{StringPolicy, StringRegexFlags, StringWriteFlags};
use aerospike::{
    as_bin, as_key, as_list, as_map, as_val, Bins, ReadPolicy, ResultCode, Value, WritePolicy,
};

async fn server_supports_string_operations(client: &aerospike::Client) -> bool {
    let supported = match client.cluster.get_random_node() {
        Ok(node) => node.version().supports_string_operations(),
        Err(_) => false,
    };

    if !supported {
        eprintln!("Skipping: server does not support string operations (requires >= 8.1.3)");
    }

    supported
}

const BIN: &str = "sbin";

async fn put(client: &aerospike::Client, wpolicy: &WritePolicy, key: &aerospike::Key, s: &str) {
    let _ = common::delete_durably(client, wpolicy, key).await;
    client
        .put(wpolicy, key, &[as_bin!(BIN, s)])
        .await
        .expect("put failed");
}

async fn get_string(client: &aerospike::Client, key: &aerospike::Key) -> String {
    let rec = client
        .get(&ReadPolicy::default(), key, Bins::All)
        .await
        .expect("get failed");
    match rec.bins.get(BIN).expect("bin missing") {
        Value::String(s) => s.clone(),
        other => panic!("expected string, got {:?}", other),
    }
}

// ============================================================
// Read operations
// ============================================================

#[aerospike_macro::test]
async fn strlen_returns_codepoint_count() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "strlen1");
    let wpolicy = WritePolicy::default();
    put(&client, &wpolicy, &key, "hello world").await;

    let rec = client
        .operate(&wpolicy, &key, &[str_op::strlen(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Int(11));
}

#[aerospike_macro::test]
async fn strlen_empty_string_is_zero() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "strlen2");
    let wpolicy = WritePolicy::default();
    put(&client, &wpolicy, &key, "").await;

    let rec = client
        .operate(&wpolicy, &key, &[str_op::strlen(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Int(0));
}

#[aerospike_macro::test]
async fn byte_length_returns_utf8_bytes() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "bytelen");
    let wpolicy = WritePolicy::default();
    put(&client, &wpolicy, &key, "hello").await;

    let rec = client
        .operate(&wpolicy, &key, &[str_op::byte_length(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Int(5));
}

#[aerospike_macro::test]
async fn substr_from_and_range_and_negative() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "substr");
    let wpolicy = WritePolicy::default();

    put(&client, &wpolicy, &key, "hello world").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::substr_from(BIN, 6)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("world"));

    let rec = client
        .operate(&wpolicy, &key, &[str_op::substr(BIN, 0, 5)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("hello"));

    // The second argument is the exclusive END index (half-open range), not a
    // length: [2, 5) of "hello world" is "llo".
    let rec = client
        .operate(&wpolicy, &key, &[str_op::substr(BIN, 2, 5)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("llo"));

    let rec = client
        .operate(&wpolicy, &key, &[str_op::substr_from(BIN, -5)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("world"));
}

#[aerospike_macro::test]
async fn char_at_returns_single_codepoint() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "charat");
    let wpolicy = WritePolicy::default();
    put(&client, &wpolicy, &key, "Hello123World").await;

    let rec = client
        .operate(&wpolicy, &key, &[str_op::char_at(BIN, 5)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("1"));
}

#[aerospike_macro::test]
async fn find_first_match_and_miss_and_nth() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "find");
    let wpolicy = WritePolicy::default();

    put(&client, &wpolicy, &key, "hello world").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::find(BIN, "world")])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Int(6));

    let rec = client
        .operate(&wpolicy, &key, &[str_op::find(BIN, "xyz")])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Int(-1));

    put(&client, &wpolicy, &key, "ababab").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::find_nth(BIN, "ab", 2)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Int(2));
}

#[aerospike_macro::test]
async fn contains_starts_with_ends_with() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "match");
    let wpolicy = WritePolicy::default();
    put(&client, &wpolicy, &key, "Hello123World").await;

    let rec = client
        .operate(&wpolicy, &key, &[str_op::contains(BIN, "Hello")])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(true));

    let rec = client
        .operate(&wpolicy, &key, &[str_op::contains(BIN, "xyz")])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(false));

    let rec = client
        .operate(&wpolicy, &key, &[str_op::starts_with(BIN, "Hello")])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(true));

    let rec = client
        .operate(&wpolicy, &key, &[str_op::ends_with(BIN, "World")])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(true));
}

#[aerospike_macro::test]
async fn case_predicates_upper_lower() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "case");
    let wpolicy = WritePolicy::default();

    put(&client, &wpolicy, &key, "HELLO").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::is_upper(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(true));

    put(&client, &wpolicy, &key, "hello").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::is_lower(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(true));

    put(&client, &wpolicy, &key, "Mixed").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::is_upper(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(false));
}

#[aerospike_macro::test]
async fn is_numeric_to_integer_to_double() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "num");
    let wpolicy = WritePolicy::default();

    put(&client, &wpolicy, &key, "12345").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::is_numeric(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(true));

    let rec = client
        .operate(&wpolicy, &key, &[str_op::to_integer(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Int(12345));

    put(&client, &wpolicy, &key, "3.14").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::to_double(BIN)])
        .await
        .unwrap();
    match rec.bins.get(BIN).unwrap() {
        Value::Float(f) => {
            let n = f64::from(f);
            assert!((n - 3.14).abs() < 1e-3, "got {n}");
        }
        other => panic!("expected float, got {:?}", other),
    }
}

#[aerospike_macro::test]
async fn split_by_separator_and_singleton() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "split");
    let wpolicy = WritePolicy::default();

    put(&client, &wpolicy, &key, "one,two,three").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::split_by_separator(BIN, ",")])
        .await
        .unwrap();
    let want = Value::List(vec![
        Value::from("one"),
        Value::from("two"),
        Value::from("three"),
    ]);
    assert_eq!(rec.bins.get(BIN).unwrap(), &want);

    put(&client, &wpolicy, &key, "Hello123World").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::split_by_separator(BIN, "|")])
        .await
        .unwrap();
    assert_eq!(
        rec.bins.get(BIN).unwrap(),
        &Value::List(vec![Value::from("Hello123World")])
    );
}

#[aerospike_macro::test]
async fn regex_compare_default_and_case_insensitive() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "rxc");
    let wpolicy = WritePolicy::default();

    put(&client, &wpolicy, &key, "Hello123World").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::regex_compare(BIN, "[0-9]+")])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(true));

    put(&client, &wpolicy, &key, "HELLO").await;
    let rec = client
        .operate(
            &wpolicy,
            &key,
            &[str_op::regex_compare_with_flags(
                BIN,
                "hello",
                StringRegexFlags::CASE_INSENSITIVE,
            )],
        )
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(true));
}

#[aerospike_macro::test]
async fn to_blob_and_b64_decode() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "blob");
    let wpolicy = WritePolicy::default();

    put(&client, &wpolicy, &key, "hello").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::to_blob(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Blob(b"hello".to_vec()));

    put(&client, &wpolicy, &key, "aGVsbG8=").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::b64_decode(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Blob(b"hello".to_vec()));
}

// ============================================================
// Modify operations
// ============================================================

#[aerospike_macro::test]
async fn upper_lower_case_fold() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "upmod");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(&wpolicy, &key, &[str_op::upper(&policy, BIN)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "HELLO WORLD");

    put(&client, &wpolicy, &key, "HELLO WORLD").await;
    client
        .operate(&wpolicy, &key, &[str_op::lower(&policy, BIN)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world");

    put(&client, &wpolicy, &key, "HELLO World").await;
    client
        .operate(&wpolicy, &key, &[str_op::case_fold(&policy, BIN)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world");
}

#[aerospike_macro::test]
async fn normalize_nfc_identity() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "nfc");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "hello").await;
    client
        .operate(&wpolicy, &key, &[str_op::normalize_nfc(&policy, BIN)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello");
}

#[aerospike_macro::test]
async fn insert_at_positions() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "insert");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::insert(&policy, BIN, 5, " beautiful")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello beautiful world");

    put(&client, &wpolicy, &key, "world").await;
    client
        .operate(&wpolicy, &key, &[str_op::insert(&policy, BIN, 0, "hello ")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world");

    put(&client, &wpolicy, &key, "hello").await;
    client
        .operate(&wpolicy, &key, &[str_op::insert(&policy, BIN, 5, " world")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world");

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(&wpolicy, &key, &[str_op::insert(&policy, BIN, -5, "big ")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello big world");
}

#[aerospike_macro::test]
async fn overwrite_at_start_middle_extend() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "overwrite");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::overwrite(&policy, BIN, 6, "earth")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello earth");

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::overwrite(&policy, BIN, 0, "HELLO")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "HELLO world");

    put(&client, &wpolicy, &key, "hello").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::overwrite(&policy, BIN, 3, "ping!")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "helping!");
}

#[aerospike_macro::test]
async fn snip_range_prefix_suffix() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "snip");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "hello beautiful world").await;
    client
        .operate(&wpolicy, &key, &[str_op::snip(&policy, BIN, 5, 15)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world");

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(&wpolicy, &key, &[str_op::snip(&policy, BIN, 0, 6)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "world");

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(&wpolicy, &key, &[str_op::snip(&policy, BIN, 5, 11)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello");
}

#[aerospike_macro::test]
async fn replace_first_and_replace_all() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "replace");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "hello world world").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::replace(&policy, BIN, "world", "earth")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello earth world");

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::replace(&policy, BIN, "xyz", "abc")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world");

    put(&client, &wpolicy, &key, "hi world").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::replace(&policy, BIN, "hi", "hello")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world");

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::replace(&policy, BIN, " world", "")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello");

    put(&client, &wpolicy, &key, "aabaa").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::replace_all(&policy, BIN, "a", "x")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "xxbxx");

    put(&client, &wpolicy, &key, "hello").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::replace_all(&policy, BIN, "z", "!")],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello");
}

#[aerospike_macro::test]
async fn trim_variants() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "trim");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "  hello world  ").await;
    client
        .operate(&wpolicy, &key, &[str_op::trim(&policy, BIN)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world");

    put(&client, &wpolicy, &key, "  hello  ").await;
    client
        .operate(&wpolicy, &key, &[str_op::trim_start(&policy, BIN)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello  ");

    put(&client, &wpolicy, &key, "  hello  ").await;
    client
        .operate(&wpolicy, &key, &[str_op::trim_end(&policy, BIN)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "  hello");
}

#[aerospike_macro::test]
async fn pad_start_end_and_multi_char() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "pad");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "hello").await;
    client
        .operate(&wpolicy, &key, &[str_op::pad_start(&policy, BIN, 10, "*")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "*****hello");

    put(&client, &wpolicy, &key, "hello world").await;
    client
        .operate(&wpolicy, &key, &[str_op::pad_start(&policy, BIN, 5, "*")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world");

    put(&client, &wpolicy, &key, "hello").await;
    client
        .operate(&wpolicy, &key, &[str_op::pad_end(&policy, BIN, 10, ".")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello.....");

    put(&client, &wpolicy, &key, "hi").await;
    client
        .operate(&wpolicy, &key, &[str_op::pad_start(&policy, BIN, 8, "ab")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "abababhi");
}

#[aerospike_macro::test]
async fn repeat_contents() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "repeat");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "ab").await;
    client
        .operate(&wpolicy, &key, &[str_op::repeat(&policy, BIN, 3)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "ababab");

    put(&client, &wpolicy, &key, "hello").await;
    client
        .operate(&wpolicy, &key, &[str_op::repeat(&policy, BIN, 1)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello");
}

#[aerospike_macro::test]
async fn concat_single_and_list() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "concat");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "  hello world  ").await;
    client
        .operate(&wpolicy, &key, &[str_op::concat(&policy, BIN, "!")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "  hello world  !");

    put(&client, &wpolicy, &key, "hello").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::concat_list(&policy, BIN, &[" ", "big", " world"])],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello big world");
}

#[aerospike_macro::test]
async fn append_and_prepend() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "appendprep");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "world").await;
    client
        .operate(&wpolicy, &key, &[str_op::append(&policy, BIN, "!")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "world!");

    client
        .operate(&wpolicy, &key, &[str_op::prepend(&policy, BIN, "hello ")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello world!");
}

#[aerospike_macro::test]
async fn regex_replace_default_global_no_match() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "rxrep");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    put(&client, &wpolicy, &key, "abc123def456").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::regex_replace(
                &policy,
                BIN,
                "[0-9]+",
                "NUM",
                StringRegexFlags::DEFAULT,
            )],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "abcNUMdef456");

    put(&client, &wpolicy, &key, "abc123def456").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::regex_replace(
                &policy,
                BIN,
                "[0-9]+",
                "NUM",
                StringRegexFlags::GLOBAL,
            )],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "abcNUMdefNUM");

    put(&client, &wpolicy, &key, "hello").await;
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::regex_replace(
                &policy,
                BIN,
                "[0-9]+",
                "NUM",
                StringRegexFlags::GLOBAL,
            )],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hello");
}

// ============================================================
// Multi-op pipelines
// ============================================================

#[aerospike_macro::test]
async fn reads_across_multiple_bins_in_one_operate() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "multi");
    let wpolicy = WritePolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    client
        .put(
            &wpolicy,
            &key,
            &[
                as_bin!("text", "  hello world  "),
                as_bin!("number_str", "12345"),
                as_bin!("upper_str", "HELLO"),
            ],
        )
        .await
        .unwrap();

    let rec = client
        .operate(
            &wpolicy,
            &key,
            &[
                str_op::strlen("text"),
                str_op::to_integer("number_str"),
                str_op::is_upper("upper_str"),
            ],
        )
        .await
        .unwrap();

    assert_eq!(rec.bins.get("text").unwrap(), &Value::Int(15));
    assert_eq!(rec.bins.get("number_str").unwrap(), &Value::Int(12345));
    assert_eq!(rec.bins.get("upper_str").unwrap(), &Value::Bool(true));
}

#[aerospike_macro::test]
async fn same_bin_pipeline_returns_one_result_slot_per_op() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "slots");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put(&client, &wpolicy, &key, "  hello world  ").await;

    let rec = client
        .operate(
            &wpolicy,
            &key,
            &[
                str_op::trim(&policy, BIN),
                str_op::upper(&policy, BIN),
                str_op::strlen(BIN),
            ],
        )
        .await
        .unwrap();

    // CLIENT-5102: because the client auto-sets RESPOND_ALL_OPS for string
    // ops, every op contributes exactly one result slot — the two modify
    // ops emit nil and strlen emits its value at its submission index. The
    // positional index<->op mapping is preserved.
    let results = rec.results.as_ref().expect("positional results");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Nil); // trim (modify)
    assert_eq!(results[1], Value::Nil); // upper (modify)
    assert_eq!(results[2], Value::Int(11));
    assert_eq!(get_string(&client, &key).await, "HELLO WORLD");
}

#[aerospike_macro::test]
async fn modify_mixed_with_reads_preserves_positional_mapping() {
    // The exact regression from CLIENT-5102: with the default policy
    // (respond_per_each_op = false), a same-bin multi-op that mixes a modify
    // op with reads must return one slot per submitted op. Without the fix
    // the modify op's slot is dropped and every following read shifts down
    // one position (e.g. [5, "H"] instead of [nil, 5, "H"]) — a silent
    // mis-read with no error.
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "posmap");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put(&client, &wpolicy, &key, "hello").await;

    let rec = client
        .operate(
            &wpolicy,
            &key,
            &[
                str_op::upper(&policy, BIN), // index 0: modify -> nil slot
                str_op::strlen(BIN),         // index 1: strlen -> 5
                str_op::char_at(BIN, 0),     // index 2: char_at -> "H"
            ],
        )
        .await
        .unwrap();

    let results = rec.results.as_ref().expect("positional results");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Nil);
    assert_eq!(results[1], Value::Int(5));
    assert_eq!(results[2], Value::from("H"));
    assert_eq!(get_string(&client, &key).await, "HELLO");
}

#[aerospike_macro::test]
async fn chained_replace_all_and_pad_compose() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "chain");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put(&client, &wpolicy, &key, "aabaa").await;

    client
        .operate(
            &wpolicy,
            &key,
            &[
                str_op::replace_all(&policy, BIN, "a", "x"),
                str_op::pad_end(&policy, BIN, 10, "."),
            ],
        )
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "xxbxx.....");
}

// ============================================================
// CTX navigation — string nested in list/map bins
// ============================================================

#[aerospike_macro::test]
async fn read_on_string_nested_in_list() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "ctx-list");
    let wpolicy = WritePolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    let list = as_list!("alpha", "beta", "hello world");
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, list)])
        .await
        .unwrap();

    let op = str_op::strlen(BIN).context(vec![ctx_list_index(2)]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Int(11));
}

#[aerospike_macro::test]
async fn read_boolean_op_on_string_nested_in_map() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "ctx-map");
    let wpolicy = WritePolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    let map = as_map!("a" => "Hello", "b" => "World");
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, map)])
        .await
        .unwrap();

    let op = str_op::starts_with(BIN, "Wor").context(vec![ctx_map_key(Value::from("b"))]);
    let rec = client.operate(&wpolicy, &key, &[op]).await.unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::Bool(true));
}

#[aerospike_macro::test]
async fn modify_on_string_nested_in_list() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "ctx-mod-list");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    let list = as_list!("alpha", "beta", "gamma");
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, list)])
        .await
        .unwrap();

    let op = str_op::upper(&policy, BIN).context(vec![ctx_list_index(1)]);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(
        rec.bins.get(BIN).unwrap(),
        &Value::List(vec![
            Value::from("alpha"),
            Value::from("BETA"),
            Value::from("gamma"),
        ])
    );
}

#[aerospike_macro::test]
async fn modify_on_string_nested_in_map() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "ctx-mod-map");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    let map = as_map!("a" => "hello world", "b" => "foo");
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, map)])
        .await
        .unwrap();

    let op = str_op::replace(&policy, BIN, "world", "earth")
        .context(vec![ctx_map_key(Value::from("a"))]);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    let mut expected = HashMap::new();
    expected.insert(Value::from("a"), Value::from("hello earth"));
    expected.insert(Value::from("b"), Value::from("foo"));
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::HashMap(expected));
}

// A modify op under CTX carrying non-default write flags. The nested
// CONTEXT_EVAL envelope exists to make this trailing element unambiguous: in
// the older flat shape it sat at the outer level, where it was
// indistinguishable from an optional operand of the op.
#[aerospike_macro::test]
async fn modify_with_write_flags_on_string_nested_in_list() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "ctx-mod-flags-list");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::NO_FAIL);
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    let list = as_list!("alpha", "beta", "gamma");
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, list)])
        .await
        .unwrap();

    let op = str_op::append(&policy, BIN, "!").context(vec![ctx_list_index(1)]);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(
        rec.bins.get(BIN).unwrap(),
        &Value::List(vec![
            Value::from("alpha"),
            Value::from("beta!"),
            Value::from("gamma"),
        ])
    );
}

#[aerospike_macro::test]
async fn modify_with_write_flags_on_string_nested_in_map() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "ctx-mod-flags-map");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::NO_FAIL);
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    let map = as_map!("a" => "hello world", "b" => "foo");
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, map)])
        .await
        .unwrap();

    // Three arguments plus the flags, so the inner array carries four elements.
    let op = str_op::pad_end(&policy, BIN, 13, ".").context(vec![ctx_map_key(Value::from("a"))]);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    let mut expected = HashMap::new();
    expected.insert(Value::from("a"), Value::from("hello world.."));
    expected.insert(Value::from("b"), Value::from("foo"));
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::HashMap(expected));
}

#[aerospike_macro::test]
async fn modify_on_string_deeply_nested_list_in_map() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "ctx-deep");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    let inner = as_list!("one", "two", "three");
    let map = as_map!("items" => inner);
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, map)])
        .await
        .unwrap();

    let op = str_op::upper(&policy, BIN)
        .context(vec![ctx_map_key(Value::from("items")), ctx_list_index(1)]);
    client.operate(&wpolicy, &key, &[op]).await.unwrap();

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    let mut expected = HashMap::new();
    expected.insert(
        Value::from("items"),
        Value::List(vec![
            Value::from("one"),
            Value::from("TWO"),
            Value::from("three"),
        ]),
    );
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::HashMap(expected));
}

// ============================================================
// toString op — op-type 19, no payload, no sub-op id, no CTX
// ============================================================

#[aerospike_macro::test]
async fn to_string_from_integer_double_string_blob_and_bin_type_error() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "tostr");
    let wpolicy = WritePolicy::default();

    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, 42)])
        .await
        .unwrap();
    let rec = client
        .operate(&wpolicy, &key, &[str_op::to_string(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("42"));

    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, 3.14_f64)])
        .await
        .unwrap();
    let rec = client
        .operate(&wpolicy, &key, &[str_op::to_string(BIN)])
        .await
        .unwrap();
    match rec.bins.get(BIN).unwrap() {
        Value::String(s) => assert!(!s.is_empty()),
        other => panic!("expected string, got {:?}", other),
    }

    put(&client, &wpolicy, &key, "hello").await;
    let rec = client
        .operate(&wpolicy, &key, &[str_op::to_string(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("hello"));

    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, b"hi".to_vec())])
        .await
        .unwrap();
    let rec = client
        .operate(&wpolicy, &key, &[str_op::to_string(BIN)])
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("hi"));

    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, as_list!("a", "b"))])
        .await
        .unwrap();
    let err = client
        .operate(&wpolicy, &key, &[str_op::to_string(BIN)])
        .await
        .expect_err("to_string on list bin should fail");
    let msg = format!("{}", err);
    assert!(
        msg.contains("BinTypeError") || msg.contains("BIN_TYPE_ERROR"),
        "unexpected error: {msg}"
    );
}

// ============================================================
// Missing-bin path
//
// Behavior keys off the op, not the NO_FAIL flag. The additive create-ops
// {insert, overwrite, concat, append, prepend, pad_start, pad_end, repeat}
// create a missing bin from an empty string; transform/subtractive ops are
// a silent no-op (success, bin not created). There is no BIN_NOT_FOUND
// path. NO_FAIL does not govern this path — it only suppresses an in-op
// execution failure.
// ============================================================

async fn put_other_bin_only(
    client: &aerospike::Client,
    wpolicy: &WritePolicy,
    key: &aerospike::Key,
) {
    let _ = common::delete_durably(client, wpolicy, key).await;
    client
        .put(wpolicy, key, &[as_bin!("other", "untouched")])
        .await
        .unwrap();
}

#[aerospike_macro::test]
async fn modify_on_missing_bin_is_noop() {
    // A non-create modify op (upper) on a missing bin is a silent no-op
    // (success, bin not created) regardless of NO_FAIL — there is no
    // BIN_NOT_FOUND path. Record exists but the target bin does not.
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "noop");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::upper(&policy, BIN)])
        .await
        .unwrap();

    // BIN must not have been created; the existing bin must be intact.
    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert!(rec.bins.get(BIN).is_none());
    assert_eq!(rec.bins.get("other").unwrap(), &Value::from("untouched"));
}

#[aerospike_macro::test]
async fn no_fail_does_not_change_missing_bin_noop() {
    // The missing-bin no-op for non-create ops is flag-independent; NO_FAIL
    // neither creates the bin nor raises an error.
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "nofail");
    let wpolicy = WritePolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    let no_fail = StringPolicy::new(StringWriteFlags::NO_FAIL);
    client
        .operate(&wpolicy, &key, &[str_op::upper(&no_fail, BIN)])
        .await
        .unwrap();

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert!(rec.bins.get(BIN).is_none());
    assert_eq!(rec.bins.get("other").unwrap(), &Value::from("untouched"));
}

// All eight additive ops create a missing bin from empty in server 8.1.3.
// Transform/subtractive ops still no-op.

#[aerospike_macro::test]
async fn insert_on_missing_bin_creates_the_bin_from_empty() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "cr-ins");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::insert(&policy, BIN, 0, "hi")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hi");
}

#[aerospike_macro::test]
async fn concat_on_missing_bin_creates_the_bin_from_empty() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "cr-cat");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::concat(&policy, BIN, "hi")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hi");
}

#[aerospike_macro::test]
async fn append_on_missing_bin_creates_the_bin_from_empty() {
    // Create-ops bootstrap an empty string and create a missing bin.
    // NO_FAIL is irrelevant — the op always succeeds.
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "cr-app");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::append(&policy, BIN, "x")])
        .await
        .unwrap();

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("x"));
    assert_eq!(rec.bins.get("other").unwrap(), &Value::from("untouched"));
}

#[aerospike_macro::test]
async fn prepend_on_missing_bin_creates_the_bin_from_empty() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "cr-pre");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::prepend(&policy, BIN, "hi")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hi");
}

#[aerospike_macro::test]
async fn overwrite_on_missing_bin_creates_the_bin_from_empty() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "cr-ovr");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::overwrite(&policy, BIN, 0, "hi")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "hi");
}

#[aerospike_macro::test]
async fn pad_start_on_missing_bin_creates_the_bin_from_empty() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "cr-pds");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::pad_start(&policy, BIN, 5, "x")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "xxxxx");
}

#[aerospike_macro::test]
async fn pad_end_on_missing_bin_creates_the_bin_from_empty() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "cr-pde");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::pad_end(&policy, BIN, 5, "x")])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "xxxxx");
}

#[aerospike_macro::test]
async fn repeat_on_missing_bin_creates_an_empty_bin() {
    // repeat(n) on empty = "" — the bin is created holding an empty string.
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "cr-rep");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::repeat(&policy, BIN, 3)])
        .await
        .unwrap();
    assert_eq!(get_string(&client, &key).await, "");
}

// ============================================================
// Prepare / parameter errors
//
// These exercise the server's prepare-phase validation (find occurrence
// != 0, empty/negative pad arguments, repeat count >= 0, regex_replace
// pattern compile). All surface as PARAMETER_ERROR.
// ============================================================

async fn assert_param_error(
    client: &aerospike::Client,
    wpolicy: &WritePolicy,
    key: &aerospike::Key,
    op: aerospike::operations::Operation,
) {
    let err = client
        .operate(wpolicy, key, &[op])
        .await
        .expect_err("operation should fail with PARAMETER_ERROR");
    assert_eq!(
        err.server_result_code(),
        Some(aerospike::ResultCode::ParameterError),
        "unexpected error: {err}"
    );
}

#[aerospike_macro::test]
async fn find_with_zero_occurrence_raises_parameter() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "pe-find");
    let wpolicy = WritePolicy::default();
    put(&client, &wpolicy, &key, "hello").await;

    // 0 is reserved as "no occurrence"; the server's find prepare rejects it.
    assert_param_error(&client, &wpolicy, &key, str_op::find_nth(BIN, "x", 0)).await;
}

#[aerospike_macro::test]
async fn pad_with_empty_pad_string_raises_parameter() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "pe-pad");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put(&client, &wpolicy, &key, "hello").await;

    assert_param_error(
        &client,
        &wpolicy,
        &key,
        str_op::pad_start(&policy, BIN, 10, ""),
    )
    .await;
    assert_param_error(
        &client,
        &wpolicy,
        &key,
        str_op::pad_end(&policy, BIN, 10, ""),
    )
    .await;
}

#[aerospike_macro::test]
async fn pad_start_with_negative_target_raises_parameter() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "pe-neg");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put(&client, &wpolicy, &key, "hello").await;

    assert_param_error(
        &client,
        &wpolicy,
        &key,
        str_op::pad_start(&policy, BIN, -1, "*"),
    )
    .await;
}

#[aerospike_macro::test]
async fn repeat_with_negative_count_raises_parameter() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "pe-rep");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put(&client, &wpolicy, &key, "hello").await;

    assert_param_error(&client, &wpolicy, &key, str_op::repeat(&policy, BIN, -1)).await;
}

#[aerospike_macro::test]
async fn regex_replace_with_invalid_pattern_raises_parameter() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "pe-rxr");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put(&client, &wpolicy, &key, "hello").await;

    // Unclosed character class — the pattern compile fails inside the op.
    assert_param_error(
        &client,
        &wpolicy,
        &key,
        str_op::regex_replace(&policy, BIN, "[unclosed", "NUM", StringRegexFlags::DEFAULT),
    )
    .await;
}

// ============================================================
// Write flags — CREATE_ONLY and UPDATE_ONLY
// ============================================================

#[aerospike_macro::test]
async fn create_only_creates_a_missing_bin() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "co-create");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::CREATE_ONLY);
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::append(&policy, BIN, "hi")])
        .await
        .unwrap();

    assert_eq!(get_string(&client, &key).await, "hi");
}

#[aerospike_macro::test]
async fn create_only_on_a_live_bin_raises_bin_exists() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "co-live");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::CREATE_ONLY);
    put(&client, &wpolicy, &key, "hello").await;

    let err = client
        .operate(&wpolicy, &key, &[str_op::append(&policy, BIN, " there")])
        .await
        .expect_err("CREATE_ONLY on a live bin must fail");
    assert_eq!(
        err.server_result_code(),
        Some(ResultCode::BinExistsError),
        "unexpected error: {err}"
    );

    assert_eq!(get_string(&client, &key).await, "hello");
}

#[aerospike_macro::test]
async fn create_only_with_no_fail_on_a_live_bin_is_a_silent_noop() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "co-nofail");
    let wpolicy = WritePolicy::default();
    let policy =
        StringPolicy::new(StringWriteFlags::CREATE_ONLY | StringWriteFlags::NO_FAIL);
    put(&client, &wpolicy, &key, "hello").await;

    client
        .operate(&wpolicy, &key, &[str_op::append(&policy, BIN, " there")])
        .await
        .unwrap();

    assert_eq!(get_string(&client, &key).await, "hello");
}

/// Only the eight create-capable server ops accept CREATE_ONLY; the per-op
/// `bad_flags` mask rejects it everywhere else.
#[aerospike_macro::test]
async fn create_only_on_a_non_create_op_raises_parameter_error() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "co-noncreate");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::CREATE_ONLY);
    put(&client, &wpolicy, &key, "hello").await;

    let err = client
        .operate(&wpolicy, &key, &[str_op::upper(&policy, BIN)])
        .await
        .expect_err("CREATE_ONLY on upper must fail");
    assert_eq!(
        err.server_result_code(),
        Some(ResultCode::ParameterError),
        "unexpected error: {err}"
    );

    assert_eq!(get_string(&client, &key).await, "hello");
}

/// The flag validations run while the server parses arguments, upstream of
/// everything NO_FAIL covers — so NO_FAIL cannot mask them.
#[aerospike_macro::test]
async fn no_fail_does_not_mask_the_create_only_rejection() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "co-nofail-mask");
    let wpolicy = WritePolicy::default();
    let policy =
        StringPolicy::new(StringWriteFlags::CREATE_ONLY | StringWriteFlags::NO_FAIL);
    put(&client, &wpolicy, &key, "hello").await;

    let err = client
        .operate(&wpolicy, &key, &[str_op::upper(&policy, BIN)])
        .await
        .expect_err("NO_FAIL must not suppress the flag rejection");
    assert_eq!(
        err.server_result_code(),
        Some(ResultCode::ParameterError),
        "unexpected error: {err}"
    );

    assert_eq!(get_string(&client, &key).await, "hello");
}

#[aerospike_macro::test]
async fn update_only_on_a_missing_bin_is_a_noop() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "uo-missing");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::UPDATE_ONLY);
    put_other_bin_only(&client, &wpolicy, &key).await;

    client
        .operate(&wpolicy, &key, &[str_op::append(&policy, BIN, "hi")])
        .await
        .unwrap();

    // The additive op would have created the bin without this flag.
    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert!(rec.bins.get(BIN).is_none());
    assert_eq!(rec.bins.get("other").unwrap(), &Value::from("untouched"));
}

#[aerospike_macro::test]
async fn update_only_on_a_live_bin_applies() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "uo-live");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::UPDATE_ONLY);
    put(&client, &wpolicy, &key, "hello").await;

    client
        .operate(&wpolicy, &key, &[str_op::append(&policy, BIN, " there")])
        .await
        .unwrap();

    assert_eq!(get_string(&client, &key).await, "hello there");
}

/// UPDATE_ONLY is valid on every modify op, unlike CREATE_ONLY.
#[aerospike_macro::test]
async fn update_only_is_accepted_by_a_non_create_op() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "uo-noncreate");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::UPDATE_ONLY);
    put(&client, &wpolicy, &key, "hello").await;

    client
        .operate(&wpolicy, &key, &[str_op::upper(&policy, BIN)])
        .await
        .unwrap();

    assert_eq!(get_string(&client, &key).await, "HELLO");
}

#[aerospike_macro::test]
async fn create_only_with_update_only_raises_parameter_error() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "co-uo");
    let wpolicy = WritePolicy::default();
    let policy =
        StringPolicy::new(StringWriteFlags::CREATE_ONLY | StringWriteFlags::UPDATE_ONLY);
    put(&client, &wpolicy, &key, "hello").await;

    let err = client
        .operate(&wpolicy, &key, &[str_op::append(&policy, BIN, " there")])
        .await
        .expect_err("CREATE_ONLY and UPDATE_ONLY are mutually exclusive");
    assert_eq!(
        err.server_result_code(),
        Some(ResultCode::ParameterError),
        "unexpected error: {err}"
    );

    assert_eq!(get_string(&client, &key).await, "hello");
}

#[aerospike_macro::test]
async fn create_only_with_no_fail_still_raises_the_mutual_exclusion_error() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "co-uo-nofail");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(
        StringWriteFlags::CREATE_ONLY | StringWriteFlags::UPDATE_ONLY | StringWriteFlags::NO_FAIL,
    );
    put(&client, &wpolicy, &key, "hello").await;

    let err = client
        .operate(&wpolicy, &key, &[str_op::append(&policy, BIN, " there")])
        .await
        .expect_err("NO_FAIL must not suppress the mutual-exclusion error");
    assert_eq!(
        err.server_result_code(),
        Some(ResultCode::ParameterError),
        "unexpected error: {err}"
    );
}

/// CREATE_ONLY is refused on a context path: there is no bin to create when the
/// target is a leaf inside a collection.
///
/// The Go client carries this test with a note that it cannot discriminate,
/// because its flat CTX envelope failed with `PARAMETER_ERROR` whatever the
/// policy said. This client nests the envelope (CLIENT-5308), so the control
/// below is the point of the test: the identical operation *without*
/// CREATE_ONLY succeeds, which is what makes the failure attributable to the
/// flag.
#[aerospike_macro::test]
async fn create_only_on_a_ctx_path_raises_parameter_error() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "co-ctx");
    let wpolicy = WritePolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    client
        .put(&wpolicy, &key, &[as_bin!("lbin", as_list!("hello"))])
        .await
        .unwrap();

    let create_only = StringPolicy::new(StringWriteFlags::CREATE_ONLY);
    let err = client
        .operate(
            &wpolicy,
            &key,
            &[str_op::append(&create_only, "lbin", "hi").context(vec![ctx_list_index(0)])],
        )
        .await
        .expect_err("CREATE_ONLY under a context must fail");
    assert_eq!(
        err.server_result_code(),
        Some(ResultCode::ParameterError),
        "unexpected error: {err}"
    );

    // The control: same op, same path, no CREATE_ONLY.
    let default = StringPolicy::default();
    client
        .operate(
            &wpolicy,
            &key,
            &[str_op::append(&default, "lbin", "hi").context(vec![ctx_list_index(0)])],
        )
        .await
        .expect("the same op without CREATE_ONLY must succeed");

    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(
        rec.bins.get("lbin").unwrap(),
        &Value::List(vec![Value::from("hellohi")])
    );
}

// ============================================================
// snip_from — the one-argument snip
// ============================================================

#[aerospike_macro::test]
async fn snip_from_truncates_to_the_end() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "snip-from");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();

    for (start, expected) in [
        (5i64, "hello"),
        // Everything from 0 onward: the string empties.
        (0, ""),
        // Negative counts from the end.
        (-5, "hello "),
        // Past the end: nothing to remove.
        (99, "hello world"),
    ] {
        put(&client, &wpolicy, &key, "hello world").await;
        client
            .operate(&wpolicy, &key, &[str_op::snip_from(&policy, BIN, start)])
            .await
            .unwrap();

        assert_eq!(
            get_string(&client, &key).await,
            expected,
            "snip_from({start})"
        );
    }
}

/// Codepoints, not bytes: the accented characters are two bytes each.
#[aerospike_macro::test]
async fn snip_from_addresses_codepoints() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "snip-from-cp");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put(&client, &wpolicy, &key, "héllo wörld").await;

    client
        .operate(&wpolicy, &key, &[str_op::snip_from(&policy, BIN, 5)])
        .await
        .unwrap();

    assert_eq!(get_string(&client, &key).await, "héllo");
}

/// The reason the one-argument form drops the flags element: were the flags
/// packed, the server would read them as `end` and snip the empty range
/// `[5, 0)`, leaving the string untouched. A non-default policy is the case
/// where that would show, since `DEFAULT` is zero either way.
#[aerospike_macro::test]
async fn snip_from_with_a_non_default_policy_still_truncates() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "snip-from-flags");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::NO_FAIL);
    put(&client, &wpolicy, &key, "hello world").await;

    client
        .operate(&wpolicy, &key, &[str_op::snip_from(&policy, BIN, 5)])
        .await
        .unwrap();

    assert_eq!(get_string(&client, &key).await, "hello");
}
