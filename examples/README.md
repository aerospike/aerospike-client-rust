# Examples

This directory includes several Rust examples that demonstrate how to use the Aerospike Rust Client to interact with the Aerospike Database Server. Each example is a standalone binary with its own `main` function.

## Available Examples

* `batch_operations`
* `crud` — async client
* `crud_sync` — sync (blocking) client; see [How to run sync example](#sync-example-crud_sync) below
* `query`
* `timeout_configuration`

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
