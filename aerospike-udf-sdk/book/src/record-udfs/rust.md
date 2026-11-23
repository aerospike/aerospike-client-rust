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
aerospike-udf-sdk-wasm = { path = "../aerospike-udf-sdk/wasm-rust" }

[profile.release]
opt-level = 3
lto = true
panic = "abort"
strip = true

[workspace]
```

```rust
#![no_std]

use aerospike_udf_sdk_wasm::{export, Error, Record, Value};

export! {
    /// Read a bin, write it back incremented, and return the new value.
    fn increment(rec: &mut Record) -> Result<Value, Error> {
        let n = rec.int("n")?.unwrap_or(0);

        rec.set("n", n + 1)?;
        rec.save()?;

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

## The record

The record a function was called on is its first parameter: `rec: &Record` to read it, `rec: &mut Record`
to write it too. A function that does neither leaves the parameter out. It is not one of the call's
arguments: a client does not pass it, the arity check does not count it, and the signature the module
exports does not list it. Write it exactly so, with `Record` imported under that name.

Everything about the record is a method on it. What is left on the crate itself is what is not about the
record: `now()`, `log`, `fail`, the `list` and `map` builders.

`Record` and the `EventRecord` an event handler is given both implement `ReadRecord`, so a helper that
takes `&impl ReadRecord` reads a bin the same way in a UDF and in a handler.

## Reading

`rec.bin("name")` returns a `Value` or `Err(Error::Absent)`: the form for a bin that must be there. The
typed readers are for one that may not be: `rec.int("name")`, `double`, `bool`, `string`, `blob`, `list`,
`map` and the untyped `get` each return `Ok(None)` for an absent or nil bin and `Err(Error::WrongType)`
for a bin of another kind; `int_or("name", d)`, `double_or` and `bool_or` give a default in place of the
`None` (a bin of another kind is still `Err(Error::WrongType)`), and `rec.has("name")` says whether the bin
is there at all. Nothing is converted: an integer is not a double, a GeoJSON bin is not a string, and a
string is not a blob. `rec.bytes("name")` is the one reader that takes several kinds, the raw bytes of a
string, blob, GeoJSON or HLL bin. A `Value` is an enum over the kinds, with accessors that return
`Err(Error::WrongType)` when asked for the wrong one: `int()`, `double()`, `bool()`, `string()`,
`geojson()`, `blob()`, `hll()`, `bytes()`, `list()`, `map()`, beside the tests `is_int()`, `is_string()`,
`is_geojson()`, `is_hll()` and their neighbours.
A string is a `Text` that knows its length and where it sits;
`as_str(&mut buf)` fetches it into a buffer you own, `Str<N>` is an owned string of a declared capacity,
and with the `alloc` feature `to_string()` gives a `String`. The same for blobs with `Blob`, `Bytes<N>`
and `Vec<u8>`.

A list or map is a `Coll`: `iter()` yields every element (for a map, keys and values alternately),
`entries()` yields pairs, `get("key")` and `get(7)` look up a map entry in one import, `at(3)` fetches one
element of a list by its index in one import, `len()` and `is_empty()` answer without a walk. A key the map
does not have and an index past the end are `Ok(None)`, not an error. Iteration crosses 128 values per import; a nested collection comes
out as a `Value::List` or `Value::Map` you can iterate in turn.

```rust
export! {
    /// Sum a list bin.
    fn sum_list(rec: &Record) -> Result<Value, Error> {
        let mut total = 0;

        for item in rec.bin("l")?.list()?.iter() {
            total += item?.int()?;
        }

        Ok(Value::Int(total))
    }
}
```

## Writing

`rec.set("name", value)` stages a write of anything `Into<Value>`: an integer, a float, a bool, a
`&str`, a `&[u8]`, a `Str<N>`, a built `List` or `Map`, or a `Value` read from the record, which goes
back by position without being decoded. `rec.remove("name")` stages the removal of a bin; writing
`Value::Nil` does the same, since a bin cannot hold nil. `rec.set_ttl(seconds)` sets the TTL of the write.

**Nothing staged reaches the server until `rec.save()`.** A function that returns without saving has
changed nothing, however plausible its result. `save()` creates the record when the key has none, so
there is one verb for both; a function that must not overwrite checks `rec.exists()` first, and since
the record is locked for the whole call nothing comes between the check and the save. Two details:

- A save with nothing staged is still a write. The server treats it as a touch: the generation and the
  last-update time move.
- A save with nothing staged on a key with no record makes nothing, since a record must have a bin, and
  the client is told "key not found".

`rec.delete()` deletes the record. It needs no save, and whatever was staged goes with the record. The
server carries it out when the call returns, so `rec.exists()` keeps its answer for the rest of the call.

`rec.set_ints("name", &[..])` and `rec.set_doubles("name", &[..])` write a slice straight into a list
particle. They take effect at once, without a save.

## Building collections

`as_udf::list(capacity)` and `as_udf::map(capacity)` start a collection on the host and return a handle
you push or insert into: `push(value)`, `insert("key", value)`, `insert(7, value)`, each taking anything
`set` takes. Each push or insert holds nothing on the host but the element, so a loop can build a
collection of any size the call's memory allows; the host refusing one is `Err(Error::Refused)`.
`contains_key("key")` asks a map you are building whether it has a key. `extend(iterator)` takes a whole iterator 128 values per import, including a stored collection's
own iterator, so `list.extend(stored.iter())` copies a list without the guest seeing an element. A built
collection is written to a bin or nested in another by reference (`rec.set("tags", &list)`,
`outer.push(&inner)`), and returned by value: a function declared `-> Result<List, Error>` (or `Map`)
returns `Ok(list)`, and one that sometimes returns something else declares `-> Result<Output, Error>` and
returns `Ok(list.into())` there and `Ok(value.into())` elsewhere.

A `List` or `Map` owns its handle and gives it back when it is dropped; what it was set into or pushed
into keeps the collection, because the host holds its own reference. So a loop that builds a collection
per turn and nests it by reference holds one at a time, and an iterator gives its cursor back when it is
dropped too. A call is bounded by its memory on the server, not by a count of handles. There is no way to
turn a `List` or `Map` into a `Value` that outlives it: a value of one is only borrowed (`&list`), so no
value names a handle that was given back, and none keeps one past its use. A collection returned from a UDF
is dropped by `export!` as soon as the host has taken the result, which it keeps by its own reference.

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

A refused call fails with `name: argument 2 (numbers) is not an int` (or `... is missing`, or `... is 300
bytes, at most 256`) and never runs the body. Arguments count from 1, and the messages are word for word
those of the other SDKs. A call with more arguments than the function declares fails before any is decoded
(`name takes at most 2 arguments, got 3`); a call takes at most 128 arguments in all (`MAX_ARGS`), and one
with more fails the same way (`name takes at most 128 arguments, got 129`). No argument is ever dropped.

## Errors and results

A UDF is `fn name(...) -> Result<Value, Error>`. `Ok(value)` is the result the client receives. `Err(e)`
fails the call with the error's message; `Error::Custom("text")` carries your own, and the SDK's own
errors (`Absent`, `WrongType`, `TooSmall`, `Refused`, `Malformed`) say what went wrong where;
`e.status()` gives the host's status code for one. A function that builds a string or blob returns it
owned, since a `Value` only borrows: `-> Result<String, Error>` or `Result<Vec<u8>, Error>` with the
`alloc` feature, `Str<N>` or `Bytes<N>` without. The `?` operator is the
whole error handling.

## Metadata

On the record: `rec.exists()`, `rec.generation()`, `rec.ttl()` (`None` for a record that never expires),
`rec.last_update_time()`, `rec.size()`, `rec.bin_count()`, `rec.set_name(&mut buf)`, `rec.key(&mut buf)`
(the user key when the record was written with `send_key`, `Err(Absent)` otherwise) and
`rec.digest(&mut buf)`. Not on the record: `as_udf::now()` and `as_udf::log(level, message)`.

## The allocator

Nothing allocates by default. The `alloc` feature adds `String` and `Vec<u8>` as parameter and return types and as
what `Text::to_string` and `Blob::to_vec` return; the `allocator` feature links dlmalloc as the global
allocator so the crate needs nothing else. Linear memory persists across calls in an instance, so this is
a real allocator that frees.

## A module to read: the ledger

`examples/ledger` is a payments module written to be read: a guarded withdrawal, a bounded posting history
trimmed on the host by position, a summary, and an aggregation. It uses no allocator and builds to about
22 KB.

```rust
{{#include ../../../wasm-rust/examples/ledger/src/lib.rs}}
```

`examples/udf` exercises the whole surface and is the fixture `tests/src/udf_wasm.rs` asserts against;
`examples/vector` is the SIMD top-k.

## Calling it

```rust
let withdrawn = client
    .execute_udf(&WritePolicy::default(), &key, "ledger", "withdraw", &[Value::from(25), Value::from(false)])
    .await?;
```

> **A GeoJSON bin is saved only if it is GeoJSON.** Staging takes any text; the save builds the
> bin, and fails if the text is not a document the server would accept as a write.
> And after any save that fails, a list, map or blob value read before it answers an error: read
> the bin again.

## API reference

Generated from `aerospike-udf-sdk/wasm-rust/src/lib.rs`: every public item of the record UDF surface, with its
documentation. Items marked `#[doc(hidden)]` in the source, the macro plumbing, are left out.

{{#include ../api/rust-record.md}}
