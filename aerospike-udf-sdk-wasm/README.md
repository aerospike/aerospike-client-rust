# aerospike-udf-sdk-wasm

Write Aerospike user-defined functions in Rust and run them as WebAssembly.

```rust
use aerospike_udf_sdk_wasm::{self as udf, export, Args};

export! {
    /// Read a bin, write it back incremented, and return the new value.
    fn increment(_args: Args) -> i32 {
        let next = udf::bin_int("n") + 1;

        udf::set_int("n", next);

        // Staged until committed — the mistake that looks like it worked.
        if udf::exists() { udf::update() } else { udf::create() };

        udf::result_int(next);

        0
    }
}
```

Build it as a `cdylib` for `wasm32-unknown-unknown` and register the `.wasm` with `UDFLang::Wasm`. See
`examples/udf` for a crate that exercises the whole surface — records, arguments, collections, metadata,
logging, and aggregation.

## What is unusual about it

**No `alloc`, and no allocator at all.** The host never pushes bytes into guest memory uninvited: anything
returning bytes takes a buffer you own, typically a `static mut` or a stack array. Nothing here allocates, so a
UDF can be `no_std` with no allocator — which is why a compiled UDF is a few KB rather than tens, and why the
largest per-call cost in an allocator-based design simply does not exist here.

**Typed access, not serialisation.** Reading an integer bin is one import call returning an `i64`. Collections
are read *in place* through handles rather than being serialised, copied and parsed. The practical consequence
is that **cost is per import call**, so the fast way to use this SDK is to read in bulk or by key:

| prefer | over | why |
|---|---|---|
| `map.get_int("k")`, `map.get_coll("k")` | `map.key_str_at(i, …)` in a loop | one hash lookup, no allocation, no scan |
| `list.ints_into(&mut buf)` | `list.int_at(j)` in a loop | one call instead of n |

Both alternatives are still there — a map keyed by integers needs the indexed form — but the keyed and bulk
paths are what the measurements favour, by enough to matter: the indexed-scan version of a map-heavy
aggregation ran 1.48× slower than the equivalent Lua, and the keyed version is level with it.

## Stream (aggregation) UDFs

A stream UDF is driven by the host one record at a time, and **the whole stream shares one instance** — so
unlike a record UDF it can accumulate in ordinary `static mut`. `stream!` generates the two exports the host
looks for, `<name>_record` and `<name>_end`.

`end` runs once **per node**, so a client receives one partial per node and combines them itself. Lua's
aggregation looks seamless across nodes only because the Aerospike clients run its final reduce for it; nothing
does that for WASM.

## Versioning

This SDK is paired with the server's `mod-wamr` host module: they are two halves of one ABI. A module built
against a newer SDK than the server may ask for imports the server does not provide, which surfaces at the
first apply as `failed to call unlinked import function` rather than at registration — registration only loads
a module, it does not resolve imports.
