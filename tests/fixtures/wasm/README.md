# WASM UDF fixture

`udf_test.wasm` is a compiled WebAssembly UDF module, used by `tests/src/udf_wasm.rs`. `udf_test.rs` is the
exact source it was compiled from, kept beside it so the binary is not opaque.

## Why it is checked in, when its source now lives here too

Building it needs the `wasm32-unknown-unknown` target, which the test suite cannot assume is installed — and it
is a `cdylib` for a foreign target, so it cannot be a workspace member that `cargo test` would build. Checking
in the artifact keeps `cargo test` working on a plain toolchain.

Unlike before, though, **the source is in this repository**: it is
`aerospike-udf-sdk-wasm/examples/udf`, built against the SDK crate beside it. There is no longer a
cross-repository build dependency — the guest SDK moved here, because writing a UDF is a client-side activity;
only the host half of the ABI belongs with the server.

## Rebuilding

```sh
cd aerospike-udf-sdk-wasm/examples/udf
cargo build --release --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/release/aerospike_udf_example.wasm ../../../tests/fixtures/wasm/udf_test.wasm
cp src/lib.rs ../../../tests/fixtures/wasm/udf_test.rs
```

The example crate declares its own `[workspace]`, so it is deliberately **not** built by the workspace: it
targets wasm32 and uses `core::arch::wasm32`, which does not compile for the host.

The functions the tests call — `abi_probe`, `increment`, `add_all`, `describe`, `nested`, `sum_list`,
`geo_roundtrip`, `hog`, and the `sum`/`stats` aggregations — are what makes this fixture a fixture, so changing the
source means changing the tests with it.

`geo_roundtrip` is the newest and exists for one reason: `mod-wamr` had no GeoJSON kind, so a geojson bin was
unreadable as text and could only be written back as a *string*, which quietly stopped it being geospatially
indexable. It reads a document and rewrites it with `set_geojson`, and the test asserts the bin is still geojson
afterwards.

## Which engine runs it

`mod-wamr`, in the server, is the only WASM engine. There was briefly a second (`mod-wasm`, Rust over wasmtime)
with an incompatible guest ABI, which is why this directory once held two fixtures and the tests probed to work
out which one a server could run. Both are gone.
