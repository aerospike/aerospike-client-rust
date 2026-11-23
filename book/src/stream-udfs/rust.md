# Rust

`stream!` declares a stream UDF: a `record` function with typed parameters, called per record, and an
`end` function, called once per instance, both with the argument checking `export!` does. It exports
`<name>_record`, `<name>_end` and the signature `stream <name> { record(...) end() }`.

## The shape

```rust
#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, stream, Error, Map, Record};

static mut TOTAL: i64 = 0;
static mut COUNT: i64 = 0;

stream! {
    /// The sum of bin `n` over the records the query reaches, and how many there were.
    fn sum_n {
        record(rec: &Record) -> Result<(), Error> {
            if let Some(n) = rec.int("n")? {
                unsafe {
                    TOTAL += n;
                    COUNT += 1;
                }
            }

            Ok(())
        }

        end() -> Result<Map, Error> {
            let out = as_udf::map(2)?;

            unsafe {
                out.insert("total", TOTAL)?.insert("count", COUNT)?;
            }

            Ok(out)
        }
    }
}
```

The state is `static mut` because one instance sees every record a query thread reads and nothing else
touches it; the `unsafe` is the compiler's honesty, not a hazard here. `record` takes the record it is
called on as `rec: &Record`, read-only: an aggregation cannot write, and `&mut Record` there does not
compile. It returns `Result<(), Error>`: `Err` ends the stream with its message. `end` has no record, and
returns the partial.

Parameters work as in `export!`: `record(rec: &Record, bin: &str, query: &[u8], k: Option<i64>)` is decoded and checked
on every record, so a stream reads its configuration from the arguments on the first call and keeps it in
statics.

## The ledger's aggregation

`examples/ledger` ends with a stream over accounts:

```rust
{{#include ../../../aerospike-udf-sdk/wasm-rust/examples/ledger/src/lib.rs:118:145}}
```

## The vector top-k

`examples/vector/src/lib.rs` is the full-size example: a stream that reads a packed `f32` vector from each
record, scores it against a query vector from the arguments with one of three SIMD kernels, keeps the
best `k` in a bounded array with a digest tie-break, and returns `{hits, skipped_dim, skipped_none}` as its
partial. Built with `RUSTFLAGS="-C target-feature=+simd128"` its kernels vectorise (scalar otherwise, which
every server tier runs); `libm` gives the square root.
`tests/src/udf_vector.rs` holds it exact against the Go and C modules, Lua, and a local computation.

## API reference

The stream-specific items. Everything a `record` function reads with is the record UDF API of the
previous part.

{{#include ../api/rust-stream.md}}
