# aerospike-udf-sdk-wasm-go

Write Aerospike WASM UDFs in Go and build them with [TinyGo](https://tinygo.org), for the server's
`mod-wamr` engine. Package `udf` is the Go face of the same ABI the Rust SDK (`aerospike-udf-sdk-wasm`),
the C header (`aerospike-udf-sdk-wasm-c`) and the Java SDK (`aerospike-udf-sdk-wasm-java`) speak, and the
Rust SDK's documentation is the reference for the contract.

```go
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"

var n = udf.NewBin("n")

//udf:function increment
func increment(by int64) (int64, error) {
	next := n.IntOr(0) + by

	if err := n.SetInt(next); err != nil {
		return 0, err
	}

	return next, udf.Commit()
}

func main() {}
```

```sh
go generate . && tinygo build -o increment.wasm -target=<this directory>/wasm-unknown.json \
    -opt=2 -no-debug -panic=trap -scheduler=none -gc=leaking .
```

The `//udf:function` directive is the whole export: `go generate` runs the SDK's `udfgen` over the
package and writes `udf_exports.go` beside it, with the `//export increment` trampoline that decodes the
arguments into the parameters and delivers the return as the result, and the `__udf_sig_increment` export
(`fn increment(by: int) -> value`, derived from the parameter types). The generated file is checked in with
the module, so it builds without the step too. `udf.NewBin("n")` is a bin of the record by name,
`udf.Commit()` writes the staged bins, and anything that fails returns an `error`, which becomes the
failure the client sees.

Register the `.wasm` with `UdfLang::Wasm`. `wasm-unknown.json` here inherits TinyGo's `wasm-unknown` target,
which imports nothing but the server's natives and exports `_initialize` (the server runs it once per
instance before any UDF), and adds `+simd128` so a kernel written as independent lanes vectorises. TinyGo
0.34 or later, for range-over-func; `go` itself only runs the generator.

## The directives

The three directives are the Go face of the Rust SDK's `export!`, `stream!` and `event!`; `udfgen`
(`cmd/udfgen`) turns them into the exports at generation time.

- **`//udf:function <name>`** on a function: a record UDF. The parameters are the arguments, decoded by
  type (`int64`/`int`/`int32` → `int`, `float64` → `double`, `bool`, `string` → `str`, `[]byte` → `bytes`,
  `udf.Value` → `any`, `[]udf.Value` last → `any...`; a pointer type for one that may be absent or nil, `nil`
  then), and the return is the result: nothing (nil), an `error`, a value, or a value and an `error`, the
  value being any of those types or `*udf.Map` / `*udf.List`. A missing, mismatched or surplus argument
  fails the call before the function runs, naming the argument; a returned error fails it with its
  message.
- **`//udf:stream <name>`** on a `var x T` whose type has a `Record` method and an `End` method: a stream
  UDF, exported as `name_record` and `name_end` with the signature `stream name { record(...) end(...) }`.
  The variable is the stream's state; `Record` returns nothing or an `error`, `End` the partial.
- **`//udf:on <kind> <name>`** on a function: an event handler (`before-write`, `before-delete`, ...,
  `timer`, `xdr-merge`), whose parameters are the records and the info the kind hands over and whose
  return is the kind's answer and an `error`: `udf.Decision` for a write (`Accept()`, `Reject(code, msg)`,
  `AcceptRecord(bins)`), `udf.Gate` for a delete or truncate, `Expiry`, `Eviction`, `ReadResult`,
  `ShipDecision`, `Resolution` for the merge (`KeepLocal()`, `TakeRemote()`, `Policy()`, `Resolve(bins)`), and
  only an `error` for the after- and once-per-node kinds and timers. A handler with other parameters or
  another return is a generation error naming the file and line.
- **The raw form.** A function declared `(args, argc int32) int32` is exported as it is under any of the
  three, and decodes its arguments itself with `udf.Args`; the directive's `params` (and `end` for a
  stream) gives the signature's parameter text. This is for a hot path that reads its arguments once and
  keeps them, as `examples/vector` does.

## Shape of the API

- **`udf.Bin`**: a bin of the record by name, made once (`udf.NewBin("n")`) and kept as a package
  variable. Typed getters (`Int()`, `Float64()`, `Bool()`, `Str()`, `Bytes()`, `Floats(dst)`, each `(T,
  error)`; `IntOr(0)` and the other `Or` forms for a default), `Get()` as a `udf.Value` with whether it
  exists, `List()` / `Map()` to range over; setters of every kind (`SetInt` ... `SetMap`, `SetGeoJSON`,
  `SetNull`, `SetInts([]int64)`, `SetFloat64s`, `Remove()`), staged until `udf.Commit()`. `udf.Delete()`
  deletes the record, `udf.SetTTL` sets its time to live.
- **`udf.Value`**: the 16-byte value every read is, with `Int()`, `Float64()`, `Bool()`, `Str()`,
  `Bytes()`, `Floats()`, `Lookup(key)`, the `Is*` tests, and **`List()` / `Map()`** as Go 1.23 iterators
  (`for v := range v.List()`, `for k, v := range v.Map()`, 32 per import, the cursor released when the loop
  ends or breaks). `Kind` is a typed enum: `udf.KindInt`, `udf.KindMap`, ...
- **`*udf.Map` and `*udf.List`**: collections built on the host, with chainable `Put(key, any)` / `Add(any)`
  over every kind, `PutI` for integer keys, `PutGeoJSON` / `AddGeoJSON`, `PutAll` / `AddAll` from a stored
  collection by position; the first refusal is kept and answered by `Err()`. Written to a bin, returned
  as the result, nested, or handed back from a handler.
- **Metadata**: `udf.Generation`, `TTL`, `LastUpdateTime`, `RecordSize`, `BinCount`, `SetName`, `Digest`,
  the user key, `Exists`, `Now`; `udf.Log` writes to the server log.
- **Events**: `*udf.EventRecord` for a record the event hands over (typed readers `Int`, `Str`, ... as
  `(T, bool)`, `List` / `Map` / `Names` iterators, `WithBins(out)` to keep the rest of a record under a few
  decided bins), the `udf.*Info` structs for the kind's own entry, the answer values; the merge through
  `*udf.Conflict` (`Pair()` into `udf.Pair` and `udf.Both[T]` with `Newer`, `Older`, `Ordered`, `Merge`,
  `WithNewer`, and `udf.Max` / `Min` / `Sum` over a `Both`; `InvolvesDelete`, `Lost`, ...). `examples/guard`
  and `examples/resolver` are the worked examples, held to the same behaviour as the other languages' by
  the client tests.
- **The low-level tier**, under all of that and mirroring the natives one call at a time: `udf.Bin_(name,
  &v)`, `udf.Enter` / `Next` / `Close` over a caller-owned window, `udf.NewMapHandle` and the `MapPut*` /
  `ListPush*` calls over `int32` handles, the `Set*` and `Result*` calls, `udf.Fail`, and status codes
  instead of errors. It allocates nothing; the vector example is written on it. `udf.WrapMap` / `WrapList`
  bridge a handle to the builders, and the `udf.Arg*` decoders the generated exports call are public.

## What is different about Go here

Standard Go's WebAssembly output needs WASI or a JavaScript host, neither of which the server provides;
TinyGo's `wasm-unknown` is freestanding. TinyGo brings a runtime and a garbage collector, so a Go module is
not allocation-free by construction the way a Rust or C one is: the typed tier allocates a little per call,
which a record UDF or a handler can afford, and a stream's hot path stays on the low-level tier and fields
it reuses; `-gc=leaking` is fine for an instance that lives for one call or one query. A panic traps the
module (`-panic=trap`), and the host reports only that it trapped: return an `error` instead.

## The examples

- `examples/vector`: the exact top-k over packed float vectors that the Rust, C and Java SDKs also carry,
  digest for digest and bit for bit, a `//udf:stream` over a `TopK` variable in the raw form on the
  low-level tier; `tests/src/udf_vector.rs` holds the five engines (Lua included) to one answer and times
  them.
- `examples/guard`: before-write, before-delete, before-truncate and timer handlers.
- `examples/resolver`: the account merge on `xdr-merge`, over `Pair` and `Both`.
- `examples/counter`: the smallest CRDT, a grow-only counter on `xdr-merge` over a `map[string]int64`; read
  it first. The client test `go_generated_exports_decode_arguments` drives its generated exports through
  every rejection.
- `examples/pncounter`: the counter that can go down, two grow-only maps (`inc`, `dec`) merged map by map
  with a fresh `table` each.
- `examples/twopset` and `examples/lwwset`: steps 3 and 4 of the book's CRDT tutorial, a set with
  removals (two lists merged by union, `sort.Strings` for canonical bytes) and a set with re-adds (two maps
  of `udf.Now()` stamps merged by the later stamp); both return a `*udf.List` of members.
- `examples/crdt`: the active-active CRDT of the design note, rule for rule the same as the Rust one, held
  to the same nine dry-run scenarios by `tests/src/events.rs`.
- `examples/udf`: the SDK's own fixture, one function per corner of the record API (range over lists and
  maps, typed readers, every write, integer keys, GeoJSON, metadata, TTL, deletion), which
  `go_sdk_covers_the_rust_surface` in `tests/src/udf_go.rs` holds to the Rust fixture's expectations. Not
  an example to copy.

`make` generates and builds every example; `make fixtures` copies them to `tests/fixtures/wasm/` as
`*_go.wasm`. In this repository the toolchain lives in the `asd-build-wasm` container, which sees only
the server trees: `podman cp` this directory to `/work/go`, build there, copy the `.wasm` files and the
regenerated `udf_exports.go` files back.
