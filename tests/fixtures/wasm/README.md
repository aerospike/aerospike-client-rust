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

Every SDK's fixture also has `save_nothing`, `delete_it`, `drop_bin`, `nil_bin` and `index_probe`, which
pin down what the record's verbs do (a save with nothing staged, removing a bin, a nil written to a bin,
deleting the record) and the two host calls added with them (one element of a stored list by index, and a
built map asked for a key). Each language's suite has the two tests that call them.

`udf_test.wasm`, `udf_go.wasm`, `udf_cpp.wasm`, `udf_java.wasm` and (outside the conformance
specification) `apitest_c.wasm` have `churn(count)`: it builds `count` maps each holding a list and gives
each back as soon as it is nested (drop in Rust and C++, `Close` / `close` / `as_udf_handle_close` in Go,
Java and C), then walks 2000 nested lists (the lists of bin `nested`; in Rust, the ones it built), one
cursor each, closed as each walk ends. The tests
call it with 2000, so a call takes about 6000 handles and holds a few at a time. The Go and C ones close
the inner list before the map that holds it, which only a host that reuses any slot (not just the newest)
gives back; the Rust, C++ and Java ones close in the reverse of the order they opened.

The Rust fixture alone has `staged_read`, `stale_read` and the `poke` aggregation, which call host calls
directly to do what the SDK's types do not allow: read a bin after staging a value over it, keep a value
across a write to its bin, and write from a stream. They test the server, not the SDK.

`merge_probe.wasm` is `aerospike-udf-sdk-wasm/examples/merge_probe`: an `xdr-merge` handler that gives
whatever answer the incoming record's `do` bin names, well-formed or not, and signature exports that are
not signatures. `tests/src/xdr_merge_limits.rs` uses it between two clusters; it is a test instrument, not
an example of a merge.

The functions the tests call — `abi_probe`, `increment`, `add_all`, `describe`, `nested`, `sum_list`,
`packed_probe`, `cursor_probe`, `write_lists`, `last_of_doc`, `copy_vals`, `relist`, `copy_hll`,
`geo_roundtrip`, `hog`, `echo`, `tag`, `bulk`, `copy_list`, and the `sum`/`stats` aggregations — are what makes
this fixture a fixture, so changing the source means changing the tests with it.

The fixture enables the SDK's `allocator` feature, because `echo` takes a `String` and `bulk` formats its
labels; that is what took the module from 39 KB to 62 KB. A UDF that only needs `&str` or `Str<N>` leaves
the feature off and stays at the smaller size.

`geo_roundtrip` is the newest and exists for one reason: `mod-wamr` had no GeoJSON kind, so a geojson bin was
unreadable as text and could only be written back as a *string*, which quietly stopped it being geospatially
indexable. It reads a document and rewrites it with `set_geojson`, and the test asserts the bin is still geojson
afterwards.

## The vector modules

`vector_rust.wasm`, `vector_c.wasm`, `vector_cpp.wasm` and `vector_go.wasm` are the exact top-k over packed
`f32` vectors written to one algorithm in four languages, built from `aerospike-udf-sdk-wasm/examples/vector`,
`aerospike-udf-sdk-wasm-c/examples/vector`, `aerospike-udf-sdk-wasm-cpp/examples/vector` and
`aerospike-udf-sdk-wasm-go/examples/vector`. `tests/src/udf_vector.rs` asserts they agree with each other,
with Lua and with a local computation, and times them. Rebuild the Rust one as above (crate
`aerospike-udf-vector-wasm`), the C and C++ ones with `make` in their SDK directories (clang + wasm-ld), the
Go one with `make` in its SDK directory (TinyGo). The fixtures here are the scalar builds, so they register
on every server tier; `make SIMD=1` (C, C++, Go) and `RUSTFLAGS="-C target-feature=+simd128"` (Rust) build
the vectorised variants, which a Fast JIT server refuses.

## The Java modules

`hello_java.wasm`, `vector_java.wasm`, `resolver_java.wasm` and `crdt_java.wasm` are the
Java SDK's examples (`aerospike-udf-sdk-wasm-java/examples/*`), compiled by TeaVM 0.13.1's linear-memory backend;
the SDK's README says why that version and how to build them (Maven, in the `asd-build-wasm` container
here). `hello` is the bare contract without the SDK, run by `tests/src/udf_java.rs`; the other three sit
in the same agreement tests as their Rust, C and Go twins, and the Java CRDT runs through the same nine
dry-run scenarios as the Rust one. Every module but `hello` is built from annotated methods
(`@Function`, `@Stream.*`, `@On.*`): its exports and signatures are generated by the SDK's annotation
processor, and `generated_exports_decode_arguments` in `udf_java.rs` checks that path through
`counter_java.wasm`. `udf_java.wasm` is the Java SDK's own fixture (`examples/udf`), the counterpart of
`udf_test.wasm`: `java_sdk_covers_the_rust_surface` drives the list and map views, typed readers, every
write kind, integer map keys, GeoJSON, metadata, TTL and deletion through it.

## The resolver

`resolver.wasm` is an event handler, not a UDF: `aerospike-udf-sdk-wasm/examples/resolver` binds to the
server's `xdr-merge` event through `event!` and merges an account record (visits: the larger count; tags:
the union in LUT order; profile: the newer side; everything else: the newer side, which is the policy's own
answer). `tests/src/events.rs` registers it, binds it with `event-set`, runs it through the server's dry run
(`execute_udf` on the reserved package `__event__`) and checks the merged bins and the server's
`symmetric` and `idempotent` verdicts. Rebuild it as above (crate `aerospike-udf-resolver`). The plain
fixture doubles as the negative case: binding one of its UDFs is refused because its signature is not
`event xdr-merge/1 ...`.

`resolver_c.wasm`, `resolver_cpp.wasm` and `resolver_go.wasm` are the same merge written against the C, C++
and Go SDKs (`examples/resolver` in each), rebuilt with `make` in each SDK directory like the vector
modules; `resolvers_in_three_languages_agree` holds them all, with the Java one, to one answer.

## The counter

`counter.wasm`, `counter_java.wasm`, `counter_go.wasm` and `counter_cpp.wasm` are the smallest CRDT, a
grow-only counter (`examples/counter` in the Rust, Java, Go and C++ SDKs): one entry per site, the merge
keeps the larger count per site. `counter_merge_converges` runs all four through the dry run;
`tests/src/xdr_merge.rs` runs them across two live clusters.

`pncounter.wasm`, `pncounter_java.wasm`, `pncounter_go.wasm` and `pncounter_cpp.wasm` are the counter that
can go down (`examples/pncounter` in the same four SDKs): two grow-only maps, `inc` and `dec`, merged by
the larger amount per site each, the value their difference. `pncounter_merge_converges` runs the four
through the dry run, including the refusal of a negative amount and a value below zero;
`pncounter_converges_two_clusters` runs them across two live clusters.

`twopset*.wasm` and `lwwset*.wasm`, in the same four languages, are steps 3 and 4 of the book's CRDT
tutorial: the 2P-Set (two sorted lists, `added` and `removed`, merged by union; a removal is forever) and
the LWW-Element-Set (two maps of millisecond stamps, `addedAt` and `removedAt`, merged by the later stamp per
tag; a tie goes to the removal; the write UDFs stamp with `now()`, which the handler may not read).
`twopset_merge_converges` and `lwwset_merge_converges` run them through the dry run,
`twopset_converges_two_clusters` and `lwwset_converges_two_clusters` across two live clusters.

`registers*.wasm`, in the same four languages, is step 6: a record whose bins are last-writer-wins
registers stamped by the server (`examples/registers`), with one bin, `seen`, kept as a maximum by the
UDF `saw`. Its merge reads each bin's last-update time and source id, which only a namespace with
`conflict-resolve-writes true` keeps: `registers_merge_converges` and `registers_converge_two_clusters`
use the namespace `testbc` and skip themselves where there is none. `AEROSPIKE_REGISTERS_MODULE` makes
the two-cluster test run one compiled module of your choice instead of the four.

## The CRDT

`crdt.wasm` is `aerospike-udf-sdk-wasm/examples/crdt`: the design note's last-writer-wins map of devices
with removal markers, a purge horizon and a version floor, merged by an `xdr-merge` handler that decides
on versions, removals and stored bytes alone, plus the three write UDFs that keep it well-formed.
`crdt_merge_converges_in_the_eight_scenarios` drives the note's scenarios through the server's dry run,
which checks the three CRDT laws on real bytes, for it and for `crdt_java.wasm`, `crdt_go.wasm` and
`crdt_cpp.wasm`, the same module in the Java, Go and C++ SDKs.

## The C++ modules

`resolver_cpp.wasm`, `vector_cpp.wasm`, `counter_cpp.wasm`, `crdt_cpp.wasm` and
`udf_cpp.wasm` are the C++ SDK's examples (`aerospike-udf-sdk-wasm-cpp/examples/*`), compiled freestanding
by clang over the C++ header (`make` in the SDK directory; `make fixtures` copies them here). Every
module's exports and signatures come from the header's `UDF_*` macros at compile time. `udf_cpp.wasm` is
the C++ SDK's own fixture, the counterpart of `udf_test.wasm`: `cpp_sdk_covers_the_rust_surface` in
`tests/src/udf_cpp.rs` drives range-for iteration, the typed bin readers, every write kind, integer map
keys, GeoJSON, metadata, TTL and deletion through it, and `cpp_generated_exports_decode_arguments` drives
the rejections (including a string longer than its `Text<32>` parameter) through `counter_cpp.wasm`.
`stop_probe`, in `udf_cpp.wasm` and (outside the conformance specification) in `apitest_c.wasm`, takes
every handle of the call, or ends the values read from the record with a save the server refuses, and then
does a read that cannot answer; `cpp_unreadable_collections_stop_the_call` and
`c_unreadable_bins_stop_the_call` assert that the call stops with the SDK's message.

## The Go modules

`resolver_go.wasm`, `vector_go.wasm`, `counter_go.wasm`, `crdt_go.wasm` and `udf_go.wasm`
are the Go SDK's examples (`aerospike-udf-sdk-wasm-go/examples/*`), built with TinyGo by `make` in the SDK
directory (`make fixtures` copies them here). Every module's exports are generated by the SDK's `udfgen`
from `udf:` directives (`udf_exports.go` beside each `main.go`, checked in). `udf_go.wasm` is the Go SDK's
own fixture, the counterpart of `udf_test.wasm` and `udf_java.wasm`: `go_sdk_covers_the_rust_surface` in
`tests/src/udf_go.rs` drives range-over-func iteration, the typed bin readers, every write kind, integer
map keys, GeoJSON, metadata, TTL and deletion through it, and `go_generated_exports_decode_arguments`
drives the generated rejections through `counter_go.wasm`.

## Which engine runs it

`mod-wamr`, in the server, is the only WASM engine. There was briefly a second (`mod-wasm`, Rust over wasmtime)
with an incompatible guest ABI, which is why this directory once held two fixtures and the tests probed to work
out which one a server could run. Both are gone.
