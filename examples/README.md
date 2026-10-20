# Examples

This directory includes several Rust examples that demonstrate how to use the Aerospike Rust Client to interact with the Aerospike Database Server. Each example is a standalone binary with its own `main` function.

Each async example exposes its body as `pub async fn run()` (with `main` delegating to it). The integration test suite includes the example sources directly and executes `run()` against a live server (`tests/src/examples.rs`, test names `example_*`), so the examples are exercised on every test run:

```bash
AEROSPIKE_HOSTS=localhost:3000 cargo test --features rt-tokio --test lib examples::
```

The `crud_sync` example is the exception — it requires the `sync` feature, which is mutually exclusive with the `async` feature the test suite is built with, so it runs standalone only.

## Available Examples

* `batch_operations` — batch reads, writes, deletes and UDFs
* `bit_operations` — bitwise operations on blob bins
* `cdt_operations` — list/map (CDT) operations, including nested documents
* `config_file` — dynamic configuration from a YAML file (`YamlFileProvider`): static and dynamic sections, and a change in the file reaching the running client; needs the `dynamic-config` feature (on by default)
* `config_pigeon` — a `ConfigProvider` that fetches the dynamic configuration from a REST service (JSON), with a tiny service of its own to show a change reaching the running client; needs the `dynamic-config` feature (on by default)
* `crud` — async client basics
* `crud_sync` — sync (blocking) client; see [How to run sync example](#sync-example-crud_sync) below
* `error_handling` — reading an `Error`: `kind`, `result_code`, `matches`, `in_doubt`, client vs server timeouts, and the server's sub-code and message with `error_detail_verbosity` (server 8.2.0+)
* `expression_operations` — `read_exp`/`write_exp` inside `operate`: computed values, conditional writes, list/bit/string expressions, path expressions on nested documents
* `geo_query` — geospatial queries (geo2dsphere index, region/radius/contains)
* `hll_operations` — HyperLogLog sketches: init/add, count, union/intersection/similarity, fold, and the HLL expressions
* `index_management` — expression indexes, list-element indexes, set indexes, `IndexType::Integer`, dropping indexes and truncating a set
* `metrics` — client metrics: enabling the two tiers, reading the snapshot, exporting it as JSON
* `object_mapping` — `#[derive(RecordMapper)]` entity and value structs, bin renames, generation and skipped fields, serde types through the `serialization` feature
* `path_expression` — JSONPath-style CDT path expressions (server 8.1.1+)
* `query` — secondary-index queries, pagination, expression filters
* `query_aggregate` — stream UDF aggregation (map/reduce in Lua); requires the `lua` feature, see below
* `query_streaming` — `query_foreach` callbacks and `QueryHandle` (wait/cancel/resume cursor), `batch_foreach`, background `query_operate`, `query_explain` + `query_with_plan` (server 8.2.0+)
* `read_policies` — `Replica` placement including rack-aware reads, AP and SC read modes, timeouts and retries
* `record_operations` — single-record ops and write policies (add/append/TTL/generation/replace/send-key)
* `scan` — full-set scans, paging, resume and parallel consumption
* `security` — users, roles, scoped privileges, allowlists, quotas, PKI users and an XDR filter (Enterprise with security enabled; skips otherwise)
* `server_info` — the info protocol (build, namespaces, statistics)
* `string_operations` — server-side string reads and edits with `StringPolicy` write flags, and the string expressions (server 8.2.0+)
* `timeout_configuration` — socket/total timeouts and recovery
* `tls` — a TLS connection with rustls (CA roots, optional client certificate) and `AuthMode` (internal, PKI); needs the `tls` feature and a TLS-enabled server, skips otherwise
* `transaction` — multi-record transactions: commit and abort (server 8.0+, strong-consistency namespace)
* `udf` — register a Lua UDF, execute per-record, and run background UDFs

Stream-UDF aggregations (average, sum) are covered through `query_aggregate`.
Every example except `crud_sync` is async; the client is async-native, so the
others have no separate synchronous variants.

## Configuration

The examples connect to Aerospike using the `AEROSPIKE_HOSTS` environment variable.

If the variable is not set, the examples default to:

```
127.0.0.1:3000
```

You can override this by setting the environment variable before running an example:

```bash
export AEROSPIKE_HOSTS="127.0.0.1:3000"
```

## How to Run

From the root of the project, use Cargo to run an example by name:

```bash
cargo run --example <example_name>
```

### Examples

```bash
cargo run --example batch_operations
cargo run --example crud
cargo run --example query
cargo run --example timeout_configuration
```

### Aggregation example (`query_aggregate`)

Client-side stream UDF aggregation embeds a Lua interpreter, which is
compiled in only when the `lua` feature is enabled:

```bash
cargo run --example query_aggregate --features lua
```

(The example test `example_query_aggregate` likewise only exists when the
test suite is built with `--features lua`.)

### Sync example (`crud_sync`)

The `crud_sync` example uses the blocking client and requires the `sync` feature.
The runtime it blocks on follows the `rt-tokio` / `rt-async-std` feature:

```bash
cargo run --example crud_sync --no-default-features --features "sync,rt-tokio"
cargo run --example crud_sync --no-default-features --features "sync,rt-async-std"
```

Cargo will compile and run the selected example binary.
