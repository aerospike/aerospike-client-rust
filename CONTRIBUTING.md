# Contributing

Thanks for helping with the Aerospike Rust client. This page is the short
version of how the repository is built, tested and reviewed.

## Layout

| Path | What it is |
|---|---|
| `aerospike-core/` | the client (lib name `aerospike`); all of the API lives here |
| `aerospike-sync/` | the blocking wrapper, method for method over the async client |
| `aerospike-rt/` | runtime shim over Tokio / async-std |
| `aerospike-macro/` | the `RecordMapper` and `Config` derives and the `#[aerospike_macro::test]` attribute |
| `src/lib.rs` | the `aerospike` facade: re-exports the async or the blocking client by feature |
| `examples/`, `tests/`, `benches/` | runnable examples, the integration suites, load generators |
| `tools/benchmark/` | the benchmark CLI (not published) |

## Prerequisites

- Rust 1.87 or later (the workspace `rust-version`).
- An Aerospike server 6.4 or later for the integration suites and examples.
  A single node is enough:

  ```bash
  docker run -d --name aerospike -p 3000-3002:3000-3002 aerospike/aerospike-server
  ```

  Transaction tests need a strong-consistency namespace; without one they
  skip themselves.

## Building

Feature flags decide what compiles, and the wrong set is a compile error.
`rt-tokio` and `rt-async-std` are mutually exclusive, `async` and `sync` are
mutually exclusive on the facade, and `tls` needs `rt-tokio`.

```bash
# the default set: async client, Tokio, TLS, serialization, dynamic-config
cargo build

# the other combinations a change must keep building
cargo clippy --workspace --all-targets --no-default-features --features async,serialization,rt-tokio,tls,dynamic-config
cargo clippy -p aerospike-core --all-targets --no-default-features --features serialization,rt-tokio,tls,dynamic-config,lua
cargo clippy -p aerospike-core --no-default-features --features serialization,rt-tokio
cargo clippy -p aerospike-core --no-default-features --features serialization,rt-async-std,dynamic-config
cargo clippy -p aerospike-sync --all-targets --features rt-tokio
cargo clippy -p aerospike-sync --all-targets --no-default-features --features rt-async-std
cargo clippy --no-default-features --features sync,rt-tokio,serialization --example crud_sync
```

`aerospike-core` builds clean under `clippy::pedantic` and `clippy::nursery`;
the other crates under default clippy. Keep it that way rather than adding
`allow`s. Documentation must build with `RUSTDOCFLAGS='-D warnings' cargo doc
--no-deps`.

## Testing

```bash
# unit tests (no server)
cargo test -p aerospike-core --lib --no-default-features --features serialization,rt-tokio,tls,dynamic-config

# doc-comment examples are real tests; they connect to AEROSPIKE_HOSTS (default 127.0.0.1:3000)
cargo test -p aerospike-core --doc --no-default-features --features serialization,rt-tokio,dynamic-config
cargo test -p aerospike-sync --doc --features rt-tokio

# the integration suite, examples included (tests/src/examples.rs runs every example)
AEROSPIKE_HOSTS=localhost:3000 AEROSPIKE_CLEANUP=1 \
  cargo test --no-default-features --features async,serialization,rt-tokio,tls -- --skip proptest

# transactions, against a strong-consistency namespace
AEROSPIKE_HOSTS=localhost:3000 AEROSPIKE_NAMESPACE=<sc namespace> \
  cargo test --no-default-features --features async,serialization,rt-tokio,tls --test lib -- src::txn

# the blocking client, on each runtime
cargo test -p aerospike-sync --features rt-tokio
cargo test -p aerospike-sync --no-default-features --features rt-async-std
```

Environment variables the suites read: `AEROSPIKE_HOSTS`, `AEROSPIKE_NAMESPACE`
(default `test`), `AEROSPIKE_CLEANUP` (drop the sets a run created),
`AEROSPIKE_USE_SERVICES_ALTERNATE`, and the TLS variables in
`tests/common/mod.rs`. Property-based tests live under `tests/proptests/` and
run without `--skip proptest`.

## Changes

- New behaviour comes with a test in the suite that owns the feature
  (`tests/src/<feature>.rs`) or a unit test next to the code.
- Public items carry doc comments, and the examples in them compile and run.
- Every change to the public API gets a line in `CHANGELOG.md`; a breaking one
  also gets a row in `MIGRATION.md`.
- New source files start with the licence header used throughout the tree.
- Do not commit `Cargo.lock`, formatter-only churn, or generated files.

## Pull requests

Open the pull request against the branch you based your work on (`v3` for the
3.x line). Describe what changed and why, name the server version you tested
against, and list the feature sets you built. CI runs the integration suites
against community and enterprise servers on both runtimes.

## Licence

By contributing you agree that your contribution is licensed under the
[Apache License, Version 2.0](LICENSE.md), like the rest of the project.
