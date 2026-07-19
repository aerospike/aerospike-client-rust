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

#[aerospike_macro::test]
async fn example_crud() {
    crud_example::run().await;
}

#[aerospike_macro::test]
async fn example_batch_operations() {
    batch_operations_example::run().await;
}

#[aerospike_macro::test]
async fn example_query() {
    query_example::run().await;
}

#[aerospike_macro::test]
async fn example_timeout_configuration() {
    timeout_configuration_example::run().await;
}
