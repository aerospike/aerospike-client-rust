# Introduction

Aerospike runs user-defined functions on the server, next to the data, under the record lock. For a long
time those functions were Lua. This book is about the second language the server runs: **WebAssembly**,
through a module called `mod-wamr`, and about the five SDKs that produce modules for it in **Rust**,
**Go**, **C**, **C++** and **Java**.

A WASM module is registered the way a Lua one is, with `udf-put` and a type of `WASM`, and a client calls
its functions by package and function name as it always has. What changes is everything behind that
call.

## What the engine is

`mod-wamr` embeds the [WebAssembly Micro Runtime](https://github.com/bytecodealliance/wasm-micro-runtime)
in the server, in C. A module is compiled **ahead of time** to native code by `wamrc` when it is
registered, on a background thread, and cached under a hash of its bytes so a restart loads it compiled
and compiles nothing. Until its build is ready a module runs in WAMR's interpreter, which is roughly Lua
speed; compiled, it runs at native speed. A module written to use 128-bit SIMD compiles to NEON or SSE,
and a stream UDF written that way measured level with a native plugin and fourteen to nineteen times faster
than Lua on a vector top-k.

The engine implements the same five hooks Lua does (`validate`, `apply_record`, `apply_stream`, `update`,
`destroy`), so from the rest of the server's point of view it is one more language. Dispatch is by the
type recorded at registration.

## What a module sees

The contract between the server and a module is the thing to understand, because every SDK is a thin face
on it.

**Nothing is serialised.** The host holds the record natively and hands the guest its data *by position*.
Every read is a sixteen-byte value: a kind, a length, and a payload that is the scalar itself or, for a
string, blob or collection, the handle of a source the host keeps for the call plus an offset into it. A
guest reads the bytes of a string into a buffer it owns when it wants them, walks a nested list or map
from where it sits, and can hand a value it read straight back, into a bin, into a collection it is
building, or as its result, without ever decoding it.

{{#include diagrams/module-contract.svg}}

**The import call is the unit of cost.** Decoding a value in place is nanoseconds; crossing into the host
is well under a microsecond but paid per call. So every native that can take a buffer does, collections
cross 128 values per call in both directions, and a guest decides what it needs.

**Nothing is allocated unless the module asks.** The Rust SDK is `no_std` with no allocator by default;
C is freestanding; Go brings TinyGo's runtime. A module is tens of kilobytes.

**No WASI.** The engine is built without the WebAssembly System Interface and without WAMR's builtin
libc: a module's only imports are the record natives, so it has no file system, no clock, no randomness,
no sockets and no stdio, whatever it was compiled from. That is the sandbox, and it is also what lets an
`xdr-merge` handler be trusted to compute the same answer on every cluster. The SDKs therefore target
`wasm32-unknown-unknown`, not `wasm32-wasi`. (WAMR itself implements WASI Preview 1 and could be built
with it; Preview 2 and the component model it does not implement.) The one exception is Java, whose
runtime asks the host for its own logging, clock and `Math` functions under `teavm` and `teavmMath`; the
engine provides those and nothing more.

**A module describes itself.** Beside every function the SDKs export a signature, `fn withdraw(amount:
int, dry_run?: bool) -> value` or `event before-write/1 guard(...)`, which the server serves to a client
as a string and checks when a function is bound to an event.

## Three kinds of function

| Kind | Called by | Runs on | Answers |
| --- | --- | --- | --- |
| **Record UDF** | a client, on one key | the transaction thread, under the record lock | a value |
| **Stream UDF** | a client, over a query | the query threads, one instance per thread | one partial per thread, which the client combines |
| **Event handler** | the server itself, at a point in its own processing | the thread that reached that point, or a service thread | a verdict, or a record |

The first two are the UDFs Lua users know, faster and typed. The third is new: a function bound with
`event-set` to a kind of event, `before-write`, `before-expire`, `xdr-merge`, `timer`, fourteen others,
and run when the server reaches that point, with the records involved and a description of what is
happening. That is the subject of the third part of this book, and the fourth part, the CRDT
tutorial, is what you can build with one of those events.

## The SDKs

| | Rust | Go | C | C++ | Java |
| --- | --- | --- | --- | --- | --- |
| Package | `aerospike-udf-sdk-wasm` | `aerospike-udf-sdk-wasm-go` | `aerospike-udf-sdk-wasm-c` | `aerospike-udf-sdk-wasm-cpp` | `aerospike-udf-sdk-wasm-java` |
| Toolchain | `cargo`, target `wasm32-unknown-unknown` | TinyGo, target `wasm-unknown` | clang + wasm-ld, freestanding | clang + wasm-ld, freestanding C++20, over the C header | Maven + TeaVM 0.13.1, target `WEBASSEMBLY` |
| Style | typed signatures through macros; `Result<Value, Error>` | typed signatures through `udf:` directives; `go generate` writes the exports; `error` returns | one header; exported functions over `as_udf_value` | typed signatures through `UDF_*` macros and templates, at compile time; `Result<T>` and `UDF_TRY` | typed signatures through annotations; a processor generates the exports |
| Allocator | none unless the `alloc`/`allocator` features | TinyGo's | none | none | TeaVM's, with its collector |
| Module size | 10 to 60 KB | 10 to 40 KB | 5 to 10 KB | 8 to 85 KB | 150 to 220 KB |

The Rust SDK is the reference: its macros do the argument decoding and signature export for you, and its
documentation is where the contract is spelled out. Java's annotations and processor, Go's directives and
generator, and C++'s macros and templates do the same at compile time. C exposes the natives one call at
a time, and is what the C++ header is built on. Java's chapter says why its compiler is pinned to an old
release.
Each part of this book has one chapter per language, and each chapter ends with the complete API
reference for that language, generated from the SDK's source.

## How to read this book

[Configuration](configuration.md) is the server side: the `mod-wamr` stanza and what the events need.
[Record UDFs](record-udfs/README.md) is where to start writing; its Rust chapter walks through a real
module. [Stream UDFs](stream-udfs/README.md) is short, because a stream UDF is a record UDF with two
entry points and one rule about state. [The event system](events/README.md) is the longest part: the
binding model, every event kind with its payload and answers, timers, and then the four language
chapters. The [CRDT tutorial](crdt/README.md) is the last part: five data structures that two clusters
can both write and still agree on, built step by step on the `xdr-merge` event, each step in all four
languages.
