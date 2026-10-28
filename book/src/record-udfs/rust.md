# Rust

The Rust SDK, `aerospike-udf-sdk-wasm`, is the reference SDK: its macros decode arguments against a typed
signature, export the signature, and turn the contract into three ideas, every read is a `Value`, every
failure is an `Error`, and arguments are typed in the signature.

## A module from nothing

A UDF module is a `cdylib` crate for the `wasm32-unknown-unknown` target, `no_std`, with its own workspace
root (a wasm crate cannot be a member of a host workspace):

```toml
[package]
name = "my-udfs"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
aerospike-udf-sdk-wasm = { path = "../aerospike-udf-sdk-wasm" }

[profile.release]
opt-level = 3
lto = true
panic = "abort"
strip = true

[workspace]
```

```rust
#![no_std]

use aerospike_udf_sdk_wasm::{self as as_udf, export, Error, Value};

export! {
    /// Read a bin, write it back incremented, and return the new value.
    fn increment() -> Result<Value, Error> {
        let n = match as_udf::bin("n") {
            Ok(v) => v.int()?,
            Err(Error::Absent) => 0,
            Err(e) => return Err(e),
        };

        as_udf::set("n", n + 1)?;

        if as_udf::exists() { as_udf::update()? } else { as_udf::create()? }

        Ok(Value::Int(n + 1))
    }
}

/// A `no_std` cdylib needs a panic handler. A trap, which the host reports with the function's name, is
/// more useful than anything a guest could say for itself.
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}
```

```sh
rustup target add wasm32-unknown-unknown
cargo build --release --target wasm32-unknown-unknown
# target/wasm32-unknown-unknown/release/my_udfs.wasm, a few tens of KB
```

Register the `.wasm` with `UdfLang::Wasm` and call `my_udfs.increment` on a key.

## Reading

`as_udf::bin("name")` returns a `Value` or `Err(Error::Absent)`. A `Value` is an enum over the kinds, with
accessors that return `Err(Error::WrongType)` when asked for the wrong one: `int()`, `double()`, `bool()`,
`text()`, `blob()`, `list()`, `map()`. A string is a `Text` that knows its length and where it sits;
`as_str(&mut buf)` fetches it into a buffer you own, `Str<N>` is an owned string of a declared capacity,
and with the `alloc` feature `to_string()` gives a `String`. The same for blobs with `Blob`, `Bytes<N>`
and `Vec<u8>`.

A list or map is a `Coll`: `iter()` yields every element (for a map, keys and values alternately),
`entries()` yields pairs, `get("key")` and `get_i(7)` look up a map entry in one import, `len()` and
`is_empty()` answer without a walk. Iteration crosses 128 values per import; a nested collection comes
out as a `Value::List` or `Value::Map` you can iterate in turn.

```rust
export! {
    /// Sum a list bin.
    fn sum_list() -> Result<Value, Error> {
        let mut total = 0;

        for item in as_udf::bin("l")?.list()?.iter() {
            total += item?.int()?;
        }

        Ok(Value::Int(total))
    }
}
```

## Writing

`as_udf::set("name", value)` stages a write of anything `Into<Value>`: an integer, a float, a bool, a
`&str`, a `&[u8]`, a `Str<N>`, a built `List` or `Map`, or a `Value` read from the record, which goes
back by position without being decoded. `remove("name")` stages a delete. `update()` commits to an
existing record and `create()` to a new one; `set_ttl(seconds)` changes the record's TTL with the commit.
`set_list_ints(&[..])` and `set_list_doubles(&[..])` write a slice straight into a list particle.

## Building collections

`as_udf::list(capacity)` and `as_udf::map(capacity)` start a collection on the host and return a handle
you push or put into: `push(value)`, `put("key", value)`, `put_i(7, value)`, each taking anything `set`
takes. `extend(iterator)` takes a whole iterator 128 values per import, including a stored collection's
own iterator, so `list.extend(stored.iter())` copies a list without the guest seeing an element. A built
collection converts into a `Value` with `into_value()` and can be returned, written to a bin, or nested
in another.

## Arguments

`export!` declares the parameters, and the macro decodes and checks them before the body runs:

| Parameter type | Takes | Refuses |
| --- | --- | --- |
| `i64`, `f64`, `bool` | the scalar | another kind |
| `&str`, `&[u8]` | a string or blob of up to 256 bytes, into a buffer the macro owns | a longer one, by reported length, before any copy |
| `Str<N>`, `Bytes<N>` | up to `N` bytes, owned | longer |
| `String`, `Vec<u8>` (`alloc` feature) | any length, allocated once | nothing |
| `Value` | anything | nothing |
| `Coll` | a list or map | a scalar |
| `Option<T>` | `T` or nothing | the wrong kind |
| `Args` (last) | the rest, as a cursor over the remaining values | nothing |

A refused call fails with `name: argument 2 (numbers): expected an integer, got a string` and never runs the
body.

## Errors and results

A UDF is `fn name(...) -> Result<Value, Error>`. `Ok(value)` is the result the client receives. `Err(e)`
fails the call with the error's message; `Error::Custom("text")` carries your own, and the SDK's own
errors (`Absent`, `WrongType`, `TooLong`, `Refused`, ...) say what went wrong where. The `?` operator is the
whole error handling.

## Metadata

`generation()`, `ttl()`, `last_update()`, `set_name(&mut buf)`, `key()` (an `Option<Key>` of the user
key when stored), `digest(&mut buf)`, `exists()`, `record_size()`, `current_time()`, and `log(level,
message)`.

## The allocator

Nothing allocates by default. The `alloc` feature adds `String` and `Vec<u8>` as parameter types and as
what `Text::to_string` and `Blob::to_vec` return; the `allocator` feature links dlmalloc as the global
allocator so the crate needs nothing else. Linear memory persists across calls in an instance, so this is
a real allocator that frees.

## A module to read: the ledger

`examples/ledger` is a payments module written to be read: a guarded withdrawal, a bounded posting history
trimmed on the host by position, a summary, and an aggregation. It uses no allocator and builds to about
22 KB.

```rust
{{#include ../../../aerospike-udf-sdk-wasm/examples/ledger/src/lib.rs}}
```

`examples/udf` exercises the whole surface and is the fixture `tests/src/udf_wasm.rs` asserts against;
`examples/vector` is the SIMD top-k.

## Calling it

```rust
let withdrawn = client
    .execute_udf(&WritePolicy::default(), &key, "ledger", "withdraw", &[Value::from(25), Value::from(false)])
    .await?;
```

## API reference

Generated from `aerospike-udf-sdk-wasm/src/lib.rs`: every public item of the record UDF surface, with its
documentation. Items marked `#[doc(hidden)]` in the source, the macro plumbing, are left out.

{{#include ../api/rust-record.md}}
