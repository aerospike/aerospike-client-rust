# Record UDFs

A record UDF is a function a client runs on one record, by key, inside the transaction that holds the
record's lock. It reads the record's bins, may stage writes to them, and returns one value to the client.
It is the kind of function Lua users have written for years; the WASM version does the same job with a
typed signature, no serialisation, and, on an x86-64 node, machine code the Fast JIT compiled.

To learn the API by doing, start with the [tutorial](tutorial.md): eleven tasks on one record, each in
all five languages side by side. This chapter is the model behind it, and the language chapters are the
reference.

## The shape of a call

A client executes `module.function(args...)` on a key. The server opens the record, instantiates the
module for this call, a fresh linear memory with nothing of any earlier call in it, creates an execution
context, runs the module's initializer if it has one, and calls the exported function with the
arguments; the instance is torn down when the function returns (see [How the engine runs a
module](../engine.md)). The function:

1. **reads** bins, metadata and arguments through the natives, each read a sixteen-byte value;
2. **stages** writes with `set_*` and `bin_remove`, which touch nothing until it commits them with
   `rec_update` (the record exists) or `rec_create` (it does not); a function that returns without one of
   those changes nothing, which is the one way a UDF looks right and does nothing;
3. **answers** with `result_*`, a scalar, bytes, a collection it built, or a value it read handed back by
   position, or fails with `fail(message)`, which the client sees as a UDF execution error carrying the
   message.

{{#include ../diagrams/record-call.svg}}

Everything the function read is borrowed from the record and valid until the call ends, even after the
bin it came from is written again, the record is deleted, or a save fails: the host keeps the value for
the call. A value handed to a `set_*`, a `result_*` or a collection builder moves to the host.

## Values

Every read produces the same sixteen bytes: a **kind** (nil, bool, int, double, string, blob, list, map,
GeoJSON, HLL, or absent), a **length** (bytes for a string or blob, elements for a list, entries for a
map), and a **payload** that is the scalar itself or, for anything with bytes, where those bytes sit in a
source the host holds for the call. The SDKs wrap this in their own type (`Value` in Rust, `udf.Value` in
Go, `as_udf_value` in C); the layout is the same.

A string's bytes are fetched on request into a buffer the function owns. A list or map is walked with a
cursor, 128 values per import, each element arriving as a value of its own, a nested collection as a
position to enter. A map entry is looked up by key in one import. Nothing is deserialised ahead of the
function asking for it.

## Arguments and the signature

Arguments arrive as values too, all of them in one import. The Rust SDK's `export!` macro declares them
as typed parameters and refuses a call whose arguments do not match before the body runs; Go and C read
them into an array and check what they use.

Beside each function the SDKs export a signature, `__udf_sig_<name>`, which the server serves to a client
as a string when it is executed like any function: `fn add_all(name: str[256], numbers: any...) ->
value`. A client can check a call against it or generate bindings from it. The Rust macros emit it
automatically; in Go and C it is one line.

## Metadata and the key

A function can read the record's generation, TTL, last-update time, set name, user key (when stored) and
digest, set the TTL, and tell whether the record exists. It can write to the server log at three levels.

## Limits

A record UDF runs under the transaction's deadline and is stopped by the watchdog when that expires. It
holds its cursors and built collections within its memory budget, not up to a fixed count (each SDK gives them back when it is done with them), 128 arguments at most (a call with more fails), and the module's
`max-memory` of linear memory. It cannot read or write another record.

## Calling from a client

With the Rust client:

```rust
let result = client
    .execute_udf(&WritePolicy::default(), &key, "ledger", "withdraw", &[Value::from(25)])
    .await?;
```

`result` is `Option<Value>`: the function's answer, or `None` for nil. A failed call is an error whose
message is what the function passed to `fail`. The signature is read the same way, by executing
`__udf_sig_withdraw` with no arguments.

The four chapters that follow each build a module from nothing, walk through the real examples under
the SDK's `examples/` directory, and end with the complete API reference for the language.
