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

//! Runs the `examples/` programs as part of the integration test suite.
//!
//! Each async example exposes a `pub async fn run()` that its `main`
//! delegates to; the example source is included here verbatim via `#[path]`
//! and `run()` is awaited on the test runtime. This keeps the examples
//! compiling AND working against a live server as the API evolves.
//!
//! `crud_sync` is intentionally absent: it requires the `sync` feature,
//! which is mutually exclusive with the `async` feature this test suite is
//! built with (see `src/lib.rs`). It is still compile-checked by
//! `cargo build --examples --no-default-features --features "rt-tokio,sync"`.
//!
//! The examples read `AEROSPIKE_HOSTS` themselves — the same variable the
//! test suite uses — and target the default `test` namespace.

use crate::common;

#[path = "../../examples/crud.rs"]
#[allow(dead_code)]
mod crud_example;

#[path = "../../examples/batch_operations.rs"]
#[allow(dead_code)]
mod batch_operations_example;

#[path = "../../examples/query.rs"]
#[allow(dead_code)]
mod query_example;

#[path = "../../examples/timeout_configuration.rs"]
#[allow(dead_code)]
mod timeout_configuration_example;

#[path = "../../examples/record_operations.rs"]
#[allow(dead_code)]
mod record_operations_example;

#[path = "../../examples/cdt_operations.rs"]
#[allow(dead_code)]
mod cdt_operations_example;

#[path = "../../examples/bit_operations.rs"]
#[allow(dead_code)]
mod bit_operations_example;

#[path = "../../examples/transaction.rs"]
#[allow(dead_code)]
mod transaction_example;

#[path = "../../examples/udf.rs"]
#[allow(dead_code)]
mod udf_example;

#[path = "../../examples/scan.rs"]
#[allow(dead_code)]
mod scan_example;

#[path = "../../examples/geo_query.rs"]
#[allow(dead_code)]
mod geo_query_example;

#[path = "../../examples/path_expression.rs"]
#[allow(dead_code)]
mod path_expression_example;

#[path = "../../examples/server_info.rs"]
#[allow(dead_code)]
mod server_info_example;

#[cfg(all(feature = "dynamic-config", feature = "rt-tokio"))]
#[path = "../../examples/config_pigeon.rs"]
#[allow(dead_code)]
mod config_pigeon_example;

#[path = "../../examples/hll_operations.rs"]
#[allow(dead_code)]
mod hll_operations_example;

#[path = "../../examples/string_operations.rs"]
#[allow(dead_code)]
mod string_operations_example;

#[path = "../../examples/expression_operations.rs"]
#[allow(dead_code)]
mod expression_operations_example;

#[path = "../../examples/metrics.rs"]
#[allow(dead_code)]
mod metrics_example;

#[path = "../../examples/query_streaming.rs"]
#[allow(dead_code)]
mod query_streaming_example;

#[path = "../../examples/index_management.rs"]
#[allow(dead_code)]
mod index_management_example;

#[path = "../../examples/error_handling.rs"]
#[allow(dead_code)]
mod error_handling_example;

#[path = "../../examples/read_policies.rs"]
#[allow(dead_code)]
mod read_policies_example;

#[path = "../../examples/security.rs"]
#[allow(dead_code)]
mod security_example;

#[cfg(feature = "tls")]
#[path = "../../examples/tls.rs"]
#[allow(dead_code)]
mod tls_example;

#[path = "../../examples/object_mapping.rs"]
#[allow(dead_code)]
mod object_mapping_example;

#[cfg(feature = "dynamic-config")]
#[path = "../../examples/config_file.rs"]
#[allow(dead_code)]
mod config_file_example;

#[aerospike_macro::test]
async fn example_crud() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    crud_example::run().await;
}

#[aerospike_macro::test]
async fn example_batch_operations() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    batch_operations_example::run().await;
}

#[aerospike_macro::test]
async fn example_query() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    query_example::run().await;
}

#[aerospike_macro::test]
async fn example_timeout_configuration() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    timeout_configuration_example::run().await;
}

#[aerospike_macro::test]
async fn example_record_operations() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    // The example writes an explicit record TTL, which SC namespaces commonly
    // reject (see ServerCapabilities' doc comment) -- keep the example itself
    // unguarded (it's customer-facing demo code) and skip here instead.
    let client = common::client().await;
    if !common::ServerCapabilities::detect(&client)
        .await
        .explicit_record_ttl_allowed
    {
        eprintln!(
            "example_record_operations: skipped — explicit client TTL not allowed on this namespace"
        );
        return;
    }
    record_operations_example::run().await;
}

#[aerospike_macro::test]
async fn example_cdt_operations() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    cdt_operations_example::run().await;
}

#[aerospike_macro::test]
async fn example_bit_operations() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    bit_operations_example::run().await;
}

#[aerospike_macro::test]
async fn example_transaction() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    transaction_example::run().await;
}

#[aerospike_macro::test]
async fn example_udf() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    udf_example::run().await;
}

#[aerospike_macro::test]
async fn example_scan() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    scan_example::run().await;
}

#[aerospike_macro::test]
async fn example_geo_query() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    geo_query_example::run().await;
}

#[aerospike_macro::test]
async fn example_path_expression() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    path_expression_example::run().await;
}

#[aerospike_macro::test]
async fn example_server_info() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    server_info_example::run().await;
}

#[cfg(feature = "lua")]
#[path = "../../examples/query_aggregate.rs"]
#[allow(dead_code)]
mod query_aggregate_example;

#[cfg(feature = "lua")]
#[aerospike_macro::test]
async fn example_query_aggregate() {
    // The example opens its own client, so wait for the suite's one-time
    // namespace cleanup here: it drops every index, including one the example
    // is creating at that moment.
    common::ensure_clean_namespace();
    query_aggregate_example::run().await;
}

#[cfg(all(feature = "dynamic-config", feature = "rt-tokio"))]
#[aerospike_macro::test]
async fn example_config_pigeon() {
    common::ensure_clean_namespace();
    config_pigeon_example::run().await;
}

#[aerospike_macro::test]
async fn example_hll_operations() {
    common::ensure_clean_namespace();
    hll_operations_example::run().await;
}

#[aerospike_macro::test]
async fn example_string_operations() {
    common::ensure_clean_namespace();
    string_operations_example::run().await;
}

#[aerospike_macro::test]
async fn example_expression_operations() {
    common::ensure_clean_namespace();
    expression_operations_example::run().await;
}

#[aerospike_macro::test]
async fn example_metrics() {
    common::ensure_clean_namespace();
    metrics_example::run().await;
}

#[aerospike_macro::test]
async fn example_query_streaming() {
    common::ensure_clean_namespace();
    query_streaming_example::run().await;
}

#[aerospike_macro::test]
async fn example_index_management() {
    common::ensure_clean_namespace();
    index_management_example::run().await;
}

#[aerospike_macro::test]
async fn example_error_handling() {
    common::ensure_clean_namespace();
    error_handling_example::run().await;
}

#[aerospike_macro::test]
async fn example_read_policies() {
    common::ensure_clean_namespace();
    read_policies_example::run().await;
}

#[aerospike_macro::test]
async fn example_security() {
    common::ensure_clean_namespace();
    security_example::run().await;
}

#[cfg(feature = "tls")]
#[aerospike_macro::test]
async fn example_tls() {
    common::ensure_clean_namespace();
    tls_example::run().await;
}

#[aerospike_macro::test]
async fn example_object_mapping() {
    common::ensure_clean_namespace();
    object_mapping_example::run().await;
}

#[cfg(feature = "dynamic-config")]
#[aerospike_macro::test]
async fn example_config_file() {
    common::ensure_clean_namespace();
    config_file_example::run().await;
}
