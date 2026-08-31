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

//! Integration tests for CDT string filter expressions.
//! Requires Aerospike Server version >= 8.1.3.

use crate::common;

use aerospike::expressions::lists::get_by_index as list_get_by_index;
use aerospike::expressions::maps::get_by_key as map_get_by_key;
use aerospike::expressions::string as str_exp;
use aerospike::expressions::{
    eq, int_val, list_bin, list_val, map_bin, string_bin, string_val, ExpType, Expression,
};
use aerospike::operations::exp::{read_exp, ExpReadFlags};
use aerospike::operations::lists::ListReturnType;
use aerospike::operations::maps::MapReturnType;
use aerospike::operations::string::{
    StringNumericType, StringPolicy, StringRegexFlags, StringWriteFlags,
};
use aerospike::{
    as_bin, as_key, as_list, as_map, as_val, Bins, Key, ReadPolicy, Record, ResultCode, Value,
    WritePolicy,
};

const BIN: &str = "sbin";
const VAR: &str = "out";

async fn server_supports_string_operations(client: &aerospike::Client) -> bool {
    let supported = match client.cluster.get_random_node() {
        Ok(node) => node.version().supports_string_operations(),
        Err(_) => false,
    };

    if !supported {
        eprintln!("Skipping: server does not support string expressions (requires >= 8.1.3)");
    }

    supported
}

async fn put_str(client: &aerospike::Client, wpolicy: &WritePolicy, key: &Key, s: &str) {
    let _ = common::delete_durably(client, wpolicy, key).await;
    client
        .put(wpolicy, key, &[as_bin!(BIN, s)])
        .await
        .expect("put failed");
}

async fn eval(client: &aerospike::Client, key: &Key, exp: Expression) -> Record {
    let ops = &vec![read_exp(VAR, exp, ExpReadFlags::Default)];
    client
        .operate(&WritePolicy::default(), key, ops)
        .await
        .expect("operate failed")
}

#[aerospike_macro::test]
async fn strlen_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        eprintln!("Skipping: server does not support string operations (requires >= 8.1.3)");
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_strlen");
    put_str(&client, &WritePolicy::default(), &key, "hello world").await;
    let rec = eval(&client, &key, str_exp::strlen(string_bin(BIN.into()))).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Int(11));
}

#[aerospike_macro::test]
async fn substr_variants_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_substr");
    put_str(&client, &WritePolicy::default(), &key, "hello world").await;

    let rec = eval(
        &client,
        &key,
        str_exp::substr(string_bin(BIN.into()), int_val(6)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("world"));

    let rec = eval(
        &client,
        &key,
        str_exp::substr_range(string_bin(BIN.into()), int_val(0), int_val(5)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello"));

    // The second argument is the exclusive END index (half-open range), not a
    // length: [2, 5) of "hello world" is "llo".
    let rec = eval(
        &client,
        &key,
        str_exp::substr_range(string_bin(BIN.into()), int_val(2), int_val(5)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("llo"));
}

#[aerospike_macro::test]
async fn char_at_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_char_at");
    put_str(&client, &WritePolicy::default(), &key, "Hello123World").await;
    let rec = eval(
        &client,
        &key,
        str_exp::char_at(string_bin(BIN.into()), int_val(5)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("1"));
}

#[aerospike_macro::test]
async fn find_first_and_nth_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_find");
    let wpolicy = WritePolicy::default();

    put_str(&client, &wpolicy, &key, "hello world").await;
    let rec = eval(
        &client,
        &key,
        str_exp::find(string_bin(BIN.into()), string_val("world".into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Int(6));

    put_str(&client, &wpolicy, &key, "ababab").await;
    let rec = eval(
        &client,
        &key,
        str_exp::find_nth(string_bin(BIN.into()), string_val("ab".into()), int_val(2)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Int(2));
}

#[aerospike_macro::test]
async fn contains_starts_ends_with_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_match");
    put_str(&client, &WritePolicy::default(), &key, "Hello123World").await;

    let rec = eval(
        &client,
        &key,
        str_exp::contains(string_bin(BIN.into()), string_val("Hello".into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));

    let rec = eval(
        &client,
        &key,
        str_exp::starts_with(string_bin(BIN.into()), string_val("Hello".into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));

    let rec = eval(
        &client,
        &key,
        str_exp::ends_with(string_bin(BIN.into()), string_val("World".into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));
}

#[aerospike_macro::test]
async fn to_integer_and_double_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_num");
    let wpolicy = WritePolicy::default();

    put_str(&client, &wpolicy, &key, "12345").await;
    let rec = eval(&client, &key, str_exp::to_integer(string_bin(BIN.into()))).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Int(12345));

    put_str(&client, &wpolicy, &key, "3.14").await;
    let rec = eval(&client, &key, str_exp::to_double(string_bin(BIN.into()))).await;
    match rec.bins.get(VAR).unwrap() {
        Value::Float(f) => {
            let n = f64::from(f);
            assert!((n - 3.14).abs() < 1e-3, "got {n}");
        }
        other => panic!("expected float, got {:?}", other),
    }
}

#[aerospike_macro::test]
async fn byte_length_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_bytelen");
    put_str(&client, &WritePolicy::default(), &key, "hello").await;
    let rec = eval(&client, &key, str_exp::byte_length(string_bin(BIN.into()))).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Int(5));
}

#[aerospike_macro::test]
async fn is_numeric_default_and_typed_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_isnum");
    let wpolicy = WritePolicy::default();

    put_str(&client, &wpolicy, &key, "12345").await;
    // Default (Any): integer string passes.
    let rec = eval(&client, &key, str_exp::is_numeric(string_bin(BIN.into()))).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));
    // Int-only: still passes for pure-digit string.
    let rec = eval(
        &client,
        &key,
        str_exp::is_numeric_typed(string_bin(BIN.into()), StringNumericType::Int),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));

    put_str(&client, &wpolicy, &key, "3.14").await;
    // Int-only: fails for a float-shaped string.
    let rec = eval(
        &client,
        &key,
        str_exp::is_numeric_typed(string_bin(BIN.into()), StringNumericType::Int),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(false));

    put_str(&client, &wpolicy, &key, "hello").await;
    let rec = eval(&client, &key, str_exp::is_numeric(string_bin(BIN.into()))).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(false));
}

#[aerospike_macro::test]
async fn case_predicates_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_case");
    let wpolicy = WritePolicy::default();

    put_str(&client, &wpolicy, &key, "HELLO").await;
    let rec = eval(&client, &key, str_exp::is_upper(string_bin(BIN.into()))).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));

    put_str(&client, &wpolicy, &key, "hello").await;
    let rec = eval(&client, &key, str_exp::is_lower(string_bin(BIN.into()))).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));
}

#[aerospike_macro::test]
async fn to_blob_and_b64_decode_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_blob");
    let wpolicy = WritePolicy::default();

    put_str(&client, &wpolicy, &key, "hello").await;
    let rec = eval(&client, &key, str_exp::to_blob(string_bin(BIN.into()))).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Blob(b"hello".to_vec()));

    put_str(&client, &wpolicy, &key, "aGVsbG8=").await;
    let rec = eval(&client, &key, str_exp::b64_decode(string_bin(BIN.into()))).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Blob(b"hello".to_vec()));
}

#[aerospike_macro::test]
async fn split_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_split");
    put_str(&client, &WritePolicy::default(), &key, "one,two,three").await;

    let rec = eval(
        &client,
        &key,
        str_exp::split_by_separator(string_bin(BIN.into()), string_val(",".into())),
    )
    .await;
    assert_eq!(
        rec.bins.get(VAR).unwrap(),
        &Value::List(vec![
            Value::from("one"),
            Value::from("two"),
            Value::from("three"),
        ])
    );
}

#[aerospike_macro::test]
async fn regex_compare_default_and_case_insensitive_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_regex");
    let wpolicy = WritePolicy::default();

    put_str(&client, &wpolicy, &key, "Hello123World").await;
    let rec = eval(
        &client,
        &key,
        str_exp::regex_compare(string_bin(BIN.into()), string_val("[0-9]+".into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));

    put_str(&client, &wpolicy, &key, "HELLO").await;
    let rec = eval(
        &client,
        &key,
        str_exp::regex_compare_with_flags(
            string_bin(BIN.into()),
            string_val("hello".into()),
            StringRegexFlags::CASE_INSENSITIVE,
        ),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));
}

// -----------------------------------------------------------------
// Modify expressions
// -----------------------------------------------------------------

#[aerospike_macro::test]
async fn upper_lower_case_fold_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_upmod");
    let policy = StringPolicy::default();
    put_str(&client, &WritePolicy::default(), &key, "Hello World").await;

    let rec = eval(
        &client,
        &key,
        str_exp::upper(&policy, string_bin(BIN.into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("HELLO WORLD"));

    let rec = eval(
        &client,
        &key,
        str_exp::lower(&policy, string_bin(BIN.into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello world"));

    let rec = eval(
        &client,
        &key,
        str_exp::case_fold(&policy, string_bin(BIN.into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello world"));
}

#[aerospike_macro::test]
async fn insert_and_overwrite_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_insmod");
    let policy = StringPolicy::default();
    put_str(&client, &WritePolicy::default(), &key, "hello world").await;

    let rec = eval(
        &client,
        &key,
        str_exp::insert(
            &policy,
            string_bin(BIN.into()),
            int_val(5),
            string_val(" beautiful".into()),
        ),
    )
    .await;
    assert_eq!(
        rec.bins.get(VAR).unwrap(),
        &Value::from("hello beautiful world")
    );

    let rec = eval(
        &client,
        &key,
        str_exp::overwrite(
            &policy,
            string_bin(BIN.into()),
            int_val(6),
            string_val("earth".into()),
        ),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello earth"));
}

#[aerospike_macro::test]
async fn concat_list_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_concat");
    let policy = StringPolicy::default();
    put_str(&client, &WritePolicy::default(), &key, "hello").await;

    let parts = list_val(vec![
        Value::from(" "),
        Value::from("big"),
        Value::from(" world"),
    ]);
    let rec = eval(
        &client,
        &key,
        str_exp::concat(&policy, string_bin(BIN.into()), parts),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello big world"));
}

#[aerospike_macro::test]
async fn append_and_prepend_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_appre");
    let policy = StringPolicy::default();
    put_str(&client, &WritePolicy::default(), &key, "world").await;

    let rec = eval(
        &client,
        &key,
        str_exp::append(&policy, string_bin(BIN.into()), string_val("!".into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("world!"));

    let rec = eval(
        &client,
        &key,
        str_exp::prepend(&policy, string_bin(BIN.into()), string_val("hello ".into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello world"));
}

#[aerospike_macro::test]
async fn snip_range_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_snip");
    let policy = StringPolicy::default();
    let wpolicy = WritePolicy::default();

    // Only the two-arg form is exercised. The server's snip op table requires
    // (start, end[, flags]); the 1-arg client form `[SNIP, start, flags]` is
    // silently misparsed — the flags slot is read as `end`, producing a no-op
    // when flags == DEFAULT == 0. Java's TestStringExp documents the same
    // limitation and likewise skips the 1-arg form here.
    put_str(&client, &wpolicy, &key, "hello beautiful world").await;
    let rec = eval(
        &client,
        &key,
        str_exp::snip(&policy, string_bin(BIN.into()), int_val(5), int_val(15)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello world"));
}

#[aerospike_macro::test]
async fn replace_first_and_all_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_rep");
    let policy = StringPolicy::default();
    let wpolicy = WritePolicy::default();

    put_str(&client, &wpolicy, &key, "hello world world").await;
    let rec = eval(
        &client,
        &key,
        str_exp::replace(
            &policy,
            string_bin(BIN.into()),
            string_val("world".into()),
            string_val("earth".into()),
        ),
    )
    .await;
    assert_eq!(
        rec.bins.get(VAR).unwrap(),
        &Value::from("hello earth world")
    );

    put_str(&client, &wpolicy, &key, "aabaa").await;
    let rec = eval(
        &client,
        &key,
        str_exp::replace_all(
            &policy,
            string_bin(BIN.into()),
            string_val("a".into()),
            string_val("x".into()),
        ),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("xxbxx"));
}

#[aerospike_macro::test]
async fn regex_replace_default_and_global_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_rxrep");
    let policy = StringPolicy::default();
    let wpolicy = WritePolicy::default();

    put_str(&client, &wpolicy, &key, "abc123def456").await;
    let rec = eval(
        &client,
        &key,
        str_exp::regex_replace(
            &policy,
            string_bin(BIN.into()),
            string_val("[0-9]+".into()),
            string_val("NUM".into()),
            StringRegexFlags::DEFAULT,
        ),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("abcNUMdef456"));

    let rec = eval(
        &client,
        &key,
        str_exp::regex_replace(
            &policy,
            string_bin(BIN.into()),
            string_val("[0-9]+".into()),
            string_val("NUM".into()),
            StringRegexFlags::GLOBAL,
        ),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("abcNUMdefNUM"));
}

#[aerospike_macro::test]
async fn trim_pad_repeat_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_trim");
    let policy = StringPolicy::default();
    let wpolicy = WritePolicy::default();

    put_str(&client, &wpolicy, &key, "  hello world  ").await;
    let rec = eval(
        &client,
        &key,
        str_exp::trim(&policy, string_bin(BIN.into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello world"));

    put_str(&client, &wpolicy, &key, "hello").await;
    let rec = eval(
        &client,
        &key,
        str_exp::pad_start(
            &policy,
            string_bin(BIN.into()),
            int_val(10),
            string_val("*".into()),
        ),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("*****hello"));

    let rec = eval(
        &client,
        &key,
        str_exp::pad_end(
            &policy,
            string_bin(BIN.into()),
            int_val(10),
            string_val(".".into()),
        ),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello....."));

    put_str(&client, &wpolicy, &key, "ab").await;
    let rec = eval(
        &client,
        &key,
        str_exp::repeat(&policy, string_bin(BIN.into()), int_val(3)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("ababab"));
}

#[aerospike_macro::test]
async fn normalize_nfc_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_nfc");
    let policy = StringPolicy::default();
    put_str(&client, &WritePolicy::default(), &key, "hello").await;
    let rec = eval(
        &client,
        &key,
        str_exp::normalize_nfc(&policy, string_bin(BIN.into())),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello"));
}

// -----------------------------------------------------------------
// Type conversion: toString on a non-string source
// -----------------------------------------------------------------

#[aerospike_macro::test]
async fn to_string_converts_integer_bin_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_tostr");
    let wpolicy = WritePolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    client
        .put(&wpolicy, &key, &[as_bin!("n", 42_i64)])
        .await
        .unwrap();

    let ops = &[read_exp(
        VAR,
        str_exp::to_string(aerospike::expressions::int_bin("n".into())),
        ExpReadFlags::Default,
    )];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("42"));
}

// -----------------------------------------------------------------
// Chained expressions
// -----------------------------------------------------------------

#[aerospike_macro::test]
async fn chained_trim_then_upper_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_chain");
    let policy = StringPolicy::default();
    put_str(&client, &WritePolicy::default(), &key, "  hello world  ").await;

    let trimmed = str_exp::trim(&policy, string_bin(BIN.into()));
    let chained = str_exp::upper(&policy, trimmed);
    let rec = eval(&client, &key, chained).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("HELLO WORLD"));
}

// -----------------------------------------------------------------
// Use as a filter expression on a primary-key Get
// -----------------------------------------------------------------

#[aerospike_macro::test]
async fn starts_with_filter_gates_get() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_filter");
    put_str(&client, &WritePolicy::default(), &key, "hello world").await;

    // Matching filter — the record passes through.
    let mut rpolicy = ReadPolicy::default();
    rpolicy.base_policy.filter_expression = Some(str_exp::starts_with(
        string_bin(BIN.into()),
        string_val("hello".into()),
    ));
    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("hello world"));

    // Non-matching filter — get fails with FILTERED_OUT.
    let mut rpolicy2 = ReadPolicy::default();
    rpolicy2.base_policy.filter_expression = Some(str_exp::starts_with(
        string_bin(BIN.into()),
        string_val("world".into()),
    ));
    let err = client
        .get(&rpolicy2, &key, Bins::All)
        .await
        .expect_err("filter should have rejected the get");
    let msg = format!("{}", err);
    assert!(
        msg.contains("FilteredOut") || msg.contains("FILTERED_OUT"),
        "unexpected error: {msg}"
    );
}

// -----------------------------------------------------------------
// Nested source — string inside a list/map projected via ListExp/MapExp
// -----------------------------------------------------------------

#[aerospike_macro::test]
async fn strlen_on_string_nested_in_list_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_nlist");
    let wpolicy = WritePolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    let list = as_list!("alpha", "beta", "hello world");
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, list)])
        .await
        .unwrap();

    let nested = list_get_by_index(
        ListReturnType::Values,
        ExpType::STRING,
        int_val(2),
        list_bin(BIN.into()),
        &[],
    );
    let rec = eval(&client, &key, str_exp::strlen(nested)).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Int(11));
}

#[aerospike_macro::test]
async fn upper_on_string_nested_in_map_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_nmap");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    let _ = common::delete_durably(&client, &wpolicy, &key).await;
    let map = as_map!("a" => "hello", "b" => "World");
    client
        .put(&wpolicy, &key, &[as_bin!(BIN, map)])
        .await
        .unwrap();

    let nested = map_get_by_key(
        MapReturnType::Value,
        ExpType::STRING,
        string_val("a".into()),
        map_bin(BIN.into()),
        &[],
    );
    let rec = eval(&client, &key, str_exp::upper(&policy, nested)).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("HELLO"));
}

#[aerospike_macro::test]
async fn equality_comparison_with_string_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_eq");
    put_str(&client, &WritePolicy::default(), &key, "hello world").await;

    // Build: strlen("hello world") == 11
    let expr = eq(str_exp::strlen(string_bin(BIN.into())), int_val(11));
    let rec = eval(&client, &key, expr).await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::Bool(true));
}

// ============================================================
// Write flags — CREATE_ONLY and UPDATE_ONLY
// ============================================================

/// A suppressed modify restores the original particle, so the expression
/// evaluates to the *source* string rather than to nil.
#[aerospike_macro::test]
async fn create_only_with_no_fail_evaluates_to_the_source_string() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_co_nofail");
    put_str(&client, &WritePolicy::default(), &key, "hello").await;

    let policy = StringPolicy::new(StringWriteFlags::CREATE_ONLY | StringWriteFlags::NO_FAIL);
    let rec = eval(
        &client,
        &key,
        str_exp::append(
            &policy,
            string_bin(BIN.to_string()),
            string_val(" there".to_string()),
        ),
    )
    .await;

    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello"));
}

/// The same shape for a failure NO_FAIL *does* cover: an empty pad string is a
/// prepare-time failure, and the expression falls back to the source.
#[aerospike_macro::test]
async fn no_fail_suppressing_a_prepare_failure_evaluates_to_the_source() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_nofail_prep");
    put_str(&client, &WritePolicy::default(), &key, "hello").await;

    let policy = StringPolicy::new(StringWriteFlags::NO_FAIL);
    let rec = eval(
        &client,
        &key,
        str_exp::pad_start(
            &policy,
            string_bin(BIN.to_string()),
            int_val(10),
            string_val(String::new()),
        ),
    )
    .await;

    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello"));
}

#[aerospike_macro::test]
async fn update_only_applies_to_an_existing_source() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_uo");
    put_str(&client, &WritePolicy::default(), &key, "hello").await;

    let policy = StringPolicy::new(StringWriteFlags::UPDATE_ONLY);
    let rec = eval(
        &client,
        &key,
        str_exp::append(
            &policy,
            string_bin(BIN.to_string()),
            string_val(" there".to_string()),
        ),
    )
    .await;

    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello there"));
}

#[aerospike_macro::test]
async fn create_only_on_an_existing_source_fails() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_co_live");
    put_str(&client, &WritePolicy::default(), &key, "hello").await;

    let policy = StringPolicy::new(StringWriteFlags::CREATE_ONLY);
    let ops = &[read_exp(
        VAR,
        str_exp::append(
            &policy,
            string_bin(BIN.to_string()),
            string_val(" there".to_string()),
        ),
        ExpReadFlags::Default,
    )];

    // The operation path reports `BIN_EXISTS_ERROR` here; the expression VM
    // collapses a failed sub-expression into `OP_NOT_APPLICABLE`, so that is
    // what a caller sees. (The Go client asserts only that it fails.)
    let err = client
        .operate(&WritePolicy::default(), &key, ops)
        .await
        .expect_err("CREATE_ONLY on an existing source must fail");
    assert_eq!(
        err.server_result_code(),
        Some(ResultCode::OpNotApplicable),
        "unexpected error: {err}"
    );
}

#[aerospike_macro::test]
async fn create_only_with_update_only_fails_in_an_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_co_uo");
    put_str(&client, &WritePolicy::default(), &key, "hello").await;

    let policy = StringPolicy::new(StringWriteFlags::CREATE_ONLY | StringWriteFlags::UPDATE_ONLY);
    let ops = &[read_exp(
        VAR,
        str_exp::append(
            &policy,
            string_bin(BIN.to_string()),
            string_val(" there".to_string()),
        ),
        ExpReadFlags::Default,
    )];

    // Same collapse as above: the operation path reports `PARAMETER_ERROR` for
    // this, an expression reports `OP_NOT_APPLICABLE`.
    let err = client
        .operate(&WritePolicy::default(), &key, ops)
        .await
        .expect_err("CREATE_ONLY and UPDATE_ONLY are mutually exclusive");
    assert_eq!(
        err.server_result_code(),
        Some(ResultCode::OpNotApplicable),
        "unexpected error: {err}"
    );
}

// ============================================================
// snip_from — the one-argument snip
// ============================================================

#[aerospike_macro::test]
async fn snip_from_via_expression() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_snip_from");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::default();
    put_str(&client, &wpolicy, &key, "hello world").await;

    let rec = eval(
        &client,
        &key,
        str_exp::snip_from(&policy, string_bin(BIN.into()), int_val(5)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello"));

    // A modify expression evaluates to the new string and leaves the bin alone.
    let rec = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();
    assert_eq!(rec.bins.get(BIN).unwrap(), &Value::from("hello world"));

    // Negative start counts from the end.
    let rec = eval(
        &client,
        &key,
        str_exp::snip_from(&policy, string_bin(BIN.into()), int_val(-5)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello "));
}

/// Same reasoning as the operation-level test: a leaked flags element would be
/// read as `end`, and the expression would evaluate to the untouched source.
#[aerospike_macro::test]
async fn snip_from_via_expression_ignores_a_non_default_policy() {
    let client = common::client().await;
    if !server_supports_string_operations(&client).await {
        return;
    }
    let key = as_key!(common::namespace(), &common::rand_str(10), "exp_snip_flags");
    let wpolicy = WritePolicy::default();
    let policy = StringPolicy::new(StringWriteFlags::NO_FAIL);
    put_str(&client, &wpolicy, &key, "hello world").await;

    let rec = eval(
        &client,
        &key,
        str_exp::snip_from(&policy, string_bin(BIN.into()), int_val(5)),
    )
    .await;
    assert_eq!(rec.bins.get(VAR).unwrap(), &Value::from("hello"));
}
