# The WASM UDF book

An [mdBook](https://rust-lang.github.io/mdBook/) covering the server's WebAssembly UDF engine: record UDFs,
stream UDFs and the event system, each with a Rust, a Go and a C chapter that ends in the full API
reference for that language.

```sh
cargo install mdbook       # once
python3 gen-api.py         # regenerate src/api/*.md from the SDK sources
python3 gen-diagrams.py    # regenerate src/diagrams/*.svg and src/crdt/diagrams/*.svg, the hand-placed inline SVGs
mdbook serve               # http://localhost:3000
```

The API reference chapters under `src/api/` are generated from `aerospike-udf-sdk-wasm/src/lib.rs`,
`aerospike-udf-sdk-wasm-go/udf.go` and `aerospike-udf-sdk-wasm-c/include/aerospike_udf.h`, every public
item with the comment above it, so they are complete by construction; the guide chapters include them. The
examples are the real ones under each SDK's `examples/`, included from their files.
