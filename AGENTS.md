# Working in this repository as an AI coding agent

The Aerospike Rust client: crates.io package `aerospike`, async-first over Tokio
or async-std, with a blocking API behind the `sync` feature. Authoritative
version: `[workspace.package] version` in the root `Cargo.toml`. Requires Rust
1.87+. Read this file before generating or changing code here.

**Two things to get right before writing any code:**

1. **The API lives in `aerospike-core/`.** Root [`src/lib.rs`](src/lib.rs) is a
   thin re-export facade: open it for the feature-selection guard, then go to
   `aerospike-core/src/` for the implementation. `aerospike-core`'s library
   name is `aerospike`, which is why its doc-tests write `use aerospike::…`.
2. **Cargo features decide what compiles.** The wrong feature set is a compile
   error, not a runtime error; see *Selecting features*.

## Selecting features

| Goal | Cargo features |
| --- | --- |
| Async client, Tokio (default) | `default`, or explicitly `["rt-tokio"]` |
| Async client, async-std | `default-features = false`, `["async", "serialization", "rt-async-std"]`; maintenance only, async-std is discontinued upstream |
| Blocking client | `default-features = false`, `["sync", "serialization", "rt-tokio"]` or `["sync", "serialization", "rt-async-std"]`; the sync crate follows whichever runtime feature the root enables |
| TLS | add `"tls"` (on by default; **requires `rt-tokio`**, not available under async-std); rustls with the `ring` provider |
| Runtime config file | add `"dynamic-config"` (on by default) |
| `query_aggregate` / stream UDFs | add `"lua"` (off by default; compiles a vendored Lua 5.4) |

`rt-tokio` and `rt-async-std` are mutually exclusive, and so are `async` and
`sync` on the facade: enabling both (for example adding one without disabling
the defaults first) is a compile error.

## Where things live

- **`aerospike-core/src/`**: the client implementation. This is "the Rust client".
- **`aerospike-sync/`**: thin blocking wrapper that mirrors the async API method
  for method, so read `aerospike-core` first regardless of which you are
  generating for. Task-returning methods give `aerospike_sync::Task<T>` with
  blocking waits; `query_foreach` gives a blocking `QueryHandle`.
- **`aerospike-rt/`**: runtime shim (`spawn`, `sleep`, `TcpStream`, …) bound to
  Tokio or async-std by feature. Nothing in it is API.
- **`aerospike-macro/`**: the `RecordMapper` and `Config` derives and the
  `#[aerospike_macro::test]` attribute the suites use.
- **`examples/`**: one runnable program per feature area;
  [examples/README.md](examples/README.md) is the routing table, with server
  version gates. Read it before writing a new example.
- **`tests/src/`**: the integration suite, one file per feature area, run against
  a live server. `tests/proptests/` and
  [`tests/proptest_async/`](tests/proptest_async/README.md) are property-based
  tests, a different tier. `tests/common/mod.rs` is the shared harness, not a
  place for feature tests.
- **`benches/`** and **`tools/benchmark/`**: load generators for tuning
  connection settings, not API usage references.
- **`MIGRATION.md`** and **`CHANGELOG.md`**: every public change gets a changelog
  line; a breaking one also gets a migration row.

## API conventions of 3.0

These were applied across the whole crate; keep new code on them.

- **Lists**: a list the callee stores takes `impl Into<Vec<T>>`
  (`BatchOperation::write`, `Statement::set_operations`, `Operation::context`);
  a list it only reads takes `&[T]` (`operate`, UDF `args`, every builder
  `ctx`). No list is wrapped in `Option`: an empty slice means none.
- **Strings**: a function that stores a name takes `impl Into<String>`; one
  that forwards it keeps `&str`. `int_bin("a")`, `Bin::new("a", v)`,
  `Key::new(ns, set, key)` all work on literals.
- **Flags**: every flag set is a newtype with SCREAMING constants combined with
  `|`, plus `bits()` and `from_bits()` (`ListWriteFlags::ADD_UNIQUE |
  ListWriteFlags::NO_FAIL`). They are defined with the `bit_flags!` macro in
  `aerospike-core/src/flags.rs`. `ListReturnType` and `MapReturnType` are
  selectors with `.inverted()`, defined with `return_type!`; selectors are not
  bits, so there is no `|` on them.
- **Enum variants** are `UpperCamelCase`, acronyms included: `UdfLang`,
  `ReadModeAp`, `AuthMode::Pki`, `Value::GeoJson`, `ExpType::Hll`.
- **Policies** are plain structs built from `Default` by mutation or
  struct-update syntax; the `policy` module docs show the pattern. `replica`,
  `filter_expression`, timeouts and `read_touch_ttl` live on `base_policy`.
  Batches that write use `BatchPolicy::write_default()`.
- **Non-exhaustive enums**: `ResultCode`, `ClientResultCode`, `ErrorKind`,
  `PrivilegeCode` and `Value` need a `_` arm. The client-side sets (`AuthMode`,
  `Replica`, `QueryDuration`, `ReadTouchTtl`, the index, UDF, command-type,
  transaction and task status enums) are exhaustive.
- **Errors**: `Error` is an opaque struct; `kind()` gives the `ErrorKind`,
  `result_code()` the Java-numbered code (server codes as they are, client
  conditions negative), `matches(&[ResultCode])` searches the whole cause
  chain, `in_doubt()`, `base_message()`, `sub_code()` and
  `server_error_detail()` read the rest. User code builds errors only with
  `Error::client_error`, `invalid_argument` and `chain_error`.
- **Batch**: `Client::batch(&policy, &mut ops)` writes each row's outcome into
  the operations; read `record()`, `result_code()`, `error()` on each
  `BatchOperation`. A row answered `KeyNotFound` or `FilteredOut` is a failed
  row and the call still succeeds. An op reply with no bin name (`touch`,
  `get_header`) lands in `Record::results`, never in `bins`.
- **Keys, samplers, contexts** keep their invariants behind accessors:
  `key.namespace()`, `key.digest()`, `Key::with_digest(..)`, `Sampler::new`.
- **Hidden items**: `#[doc(hidden)]` marks test hooks and things kept reachable
  for language bindings (`query::PartitionStatus`, `Txn::set_state`,
  `Statement::set_aggregate_function`). They are not API; do not build on them
  from Rust code, and mark any new hook the same way with a comment saying why.
- Every expression and operation builder is `#[must_use]`.

## Server version gates

The client reports what a cluster supports through `Version::supports_*`
(`aerospike-core/src/cluster/version_parser.rs`). Tests and examples that need
a feature check it and skip otherwise; do the same.

| Feature | Server |
| --- | --- |
| Transactions (`Txn`, `commit`, `abort`); need a strong-consistency namespace | 8.0 |
| CDT path expressions (`select_by_path`, `modify_by_path`) | 8.1.1 |
| Set indexes (`create_set_index`), enhanced expression API | 8.1.2 |
| String operations and expressions, `IndexType::Integer`, extended error detail, server-compiled AEL, query selection | 8.2.0 |

## Building and linting

```bash
cargo build                                  # default set: async, tokio, tls, serialization, dynamic-config
cargo clippy --workspace --all-targets --no-default-features \
  --features async,serialization,rt-tokio,tls,dynamic-config -- -D warnings
```

`aerospike-core` builds under `clippy::pedantic` and `clippy::nursery` and the
crate root has `#![deny(warnings)]` in tests, so a warning is a failure. CI
lints on Rust 1.87, whose clippy raises lints the current release does not;
`rustup toolchain install 1.87 --component clippy` and `cargo +1.87 clippy …`
reproduces the gate locally. [CONTRIBUTING.md](CONTRIBUTING.md) lists every
feature set the gate runs.

The tree is not rustfmt-clean. Do not run `cargo fmt` over files you did not
change: a formatting pass drowns the review of the real change. Write new code
already formatted.

New Rust files start with the licence header used throughout the tree
(`// Copyright 2015-2026 Aerospike, Inc.` and the Apache 2.0 notice).

## Testing

A local server is required for everything but the unit tests. The suites read
`AEROSPIKE_HOSTS` (default `127.0.0.1:3000`), `AEROSPIKE_NAMESPACE` (default
`test`), `AEROSPIKE_CLEANUP` (drop every index and truncate the namespace once
before the first test), `AEROSPIKE_USE_SERVICES_ALTERNATE`, `AEROSPIKE_USER` /
`AEROSPIKE_PASSWORD` and the TLS variables in `tests/common/mod.rs`.

```bash
# unit tests, no server
cargo test -p aerospike-core --lib --no-default-features --features serialization,rt-tokio,tls,dynamic-config

# the feature you are changing: one file of tests/src, by module path
AEROSPIKE_CLEANUP=1 AEROSPIKE_HOSTS=localhost:3000 \
  cargo test --no-default-features --features async,serialization,rt-tokio,tls --test lib -- src::batch

# the whole integration suite (proptests are slow; run them on purpose)
AEROSPIKE_CLEANUP=1 AEROSPIKE_HOSTS=localhost:3000 \
  cargo test --no-default-features --features async,serialization,rt-tokio,tls -- --skip proptest

# transactions need a strong-consistency namespace, or they all skip themselves
AEROSPIKE_NAMESPACE=testsc AEROSPIKE_HOSTS=localhost:3000 \
  cargo test --no-default-features --features async,serialization,rt-tokio,tls --test lib -- src::txn

# doc-comment examples are real tests and connect to the server
cargo test -p aerospike-core --doc --no-default-features --features serialization,rt-tokio,dynamic-config
cargo test -p aerospike-sync --doc --features rt-tokio

# the blocking client, on each runtime
cargo test -p aerospike-sync --features rt-tokio
cargo test -p aerospike-sync --no-default-features --features rt-async-std
```

Iterate on the tests of the feature you touch; run the whole suite before you
hand the change over.

Harness facts worth knowing (`tests/common/mod.rs`):

- `common::client()` opens a client and, under `AEROSPIKE_CLEANUP`, blocks
  until the one-time cleanup is done; `singleton_client()` shares one client.
  Code that opens its own `Client` (the examples) must call
  `common::ensure_clean_namespace()` first or it races the cleanup.
- `delete_durably` deletes with `durable_delete` on SC namespaces,
  `namespace_sc!(&client)` tells whether the namespace is SC,
  `lock_index_ops()` serializes index creation (a reused node can hit the
  index limit), `skip_if_not_enterprise` and `server_capabilities_cached` gate
  on the server.
- Tests use `#[aerospike_macro::test]`, which runs the body on the shared
  runtime and boxes it, so a long test does not overflow the test thread.
- A long-lived local node accumulates set names; the harness stops the run
  when the node is near the 4095-set limit. Recreate the node to reset it.
- Proptest case count comes from `PROPTEST_CASES`; CI uses 25.

## Adding an example

Examples are part of the test suite. Each async example exposes its body as
`pub async fn run()` with `main` delegating to it, is listed in
`tests/src/examples.rs` through a `#[path]` module and an `example_*` test, has
an `[[example]]` entry with `required-features` in the root `Cargo.toml`, and
a line in `examples/README.md`. It reads `AEROSPIKE_HOSTS` with a default of
`127.0.0.1:3000` and honours `AEROSPIKE_USE_SERVICES_ALTERNATE`. Gate it on the
server version it needs rather than letting it fail on an older server.

## Which API reference wins

[docs.rs/aerospike](https://docs.rs/aerospike/) documents the latest published
release, and older releases stay reachable from its version picker. A checkout
can be ahead of the latest release, so when the page and the code in front of
you disagree, run `cargo doc --open`: it builds the reference from the exact
tree you are reading, and it wins. Doc comments are load-bearing: their
examples compile and run as tests.
