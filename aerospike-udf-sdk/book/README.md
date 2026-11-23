# The WASM UDF book

An [mdBook](https://rust-lang.github.io/mdBook/) covering the server's WebAssembly UDF engine: record UDFs,
stream UDFs and the event system, each with a Rust, a Go and a C chapter that ends in the full API
reference for that language.

```sh
cargo install mdbook       # once
python3 gen-api.py         # regenerate src/api/*.md from the SDK sources
python3 gen-tutorial.py    # regenerate the two tutorial.md pages from tutorial/
python3 gen-diagrams.py    # regenerate src/diagrams/*.svg and src/crdt/diagrams/*.svg, the hand-placed inline SVGs
mdbook serve               # http://localhost:3000
```

The API reference chapters under `src/api/` are generated from the SDKs beside this book (`../wasm-rust/src/lib.rs`,
`../wasm-go/udf.go`, `../wasm-c/include/aerospike_udf.h` and the C++ and Java ones), every public
item with the comment above it, so they are complete by construction; the guide chapters include them. The
examples are the real ones under each SDK's `examples/`, included from their files.

The two tutorials (`src/record-udfs/tutorial.md`, `src/events/tutorial.md`) are generated too. Their prose
is in `tutorial/record-udfs.md` and `tutorial/events.md`; their code is in one source file per language
under `tutorial/`, between `ANCHOR` comments, so every snippet in the book is code a compiler has seen.
`gen-tutorial.py` lists the command that checks each language at its top.
