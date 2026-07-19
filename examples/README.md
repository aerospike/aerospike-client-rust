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
* `crud` — async client basics
* `crud_sync` — sync (blocking) client; see [How to run sync example](#sync-example-crud_sync) below
* `geo_query` — geospatial queries (geo2dsphere index, region/radius/contains)
* `path_expression` — JSONPath-style CDT path expressions (server 8.1.1+)
* `query` — secondary-index queries, pagination, expression filters
* `record_operations` — single-record ops and write policies (add/append/TTL/generation/replace/send-key)
* `scan` — full-set scans, paging, resume and parallel consumption
* `server_info` — the info protocol (build, namespaces, statistics)
* `timeout_configuration` — socket/total timeouts and recovery
* `transaction` — multi-record transactions: commit and abort (server 8.0+, strong-consistency namespace)
* `udf` — register a Lua UDF, execute per-record, and run background UDFs

These cover the feature areas of the Java client's examples. Not ported:
stream-UDF aggregations (`QueryAverage`/`QuerySum` — not supported by the
Rust client) and the Java GUI/console scaffolding. The Java `Async*`
variants need no counterpart: the Rust client is async-native.

## Configuration

The examples connect to Aerospike using the `AEROSPIKE_HOSTS` environment variable.

If the variable is not set, the examples default to:

```
127.0.0.1:3100
```

You can override this by setting the environment variable before running an example:

```bash
export AEROSPIKE_HOSTS="127.0.0.1:3100"
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

### Sync example (`crud_sync`)

The `crud_sync` example uses the blocking client and requires the `sync` feature:

```bash
cargo run --example crud_sync --no-default-features --features "rt-tokio,sync"
```

Cargo will compile and run the selected example binary.
