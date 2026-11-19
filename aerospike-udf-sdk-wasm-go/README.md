# aerospike-udf-sdk-wasm-go

Write Aerospike WASM UDFs in Go and build them with [TinyGo](https://tinygo.org), for the server's
`mod-wamr` engine. Package `udf` is the Go face of the same ABI the Rust SDK (`aerospike-udf-sdk-wasm`),
the C header (`aerospike-udf-sdk-wasm-c`) and the Java SDK (`aerospike-udf-sdk-wasm-java`) speak, and the
Rust SDK's documentation is the reference for the contract.

```go
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"

//udf:function increment
func increment(rec *udf.Record, by int64) (int64, error) {
	n, err := rec.IntOr("n", 0) // 0 when the bin is absent; an error when it is not an integer
	if err != nil {
		return 0, err
	}
	next := n + by

	if err := rec.Set("n", next); err != nil {
		return 0, err
	}

	return next, rec.Save()
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
the module, so it builds without the step too. `rec` is the record the call is on: the export hands it
over, a client does not pass it, and it is in neither the arity check nor the signature. `rec.Set` stages a
bin, `rec.Save()` writes the staged bins, and anything that fails returns an `error`, which becomes the
failure the client sees.

Register the `.wasm` with `UdfLang::Wasm`. `wasm-unknown.json` here inherits TinyGo's `wasm-unknown` target,
which imports nothing but the server's natives and exports `_initialize` (the server runs it once per
instance before any UDF). `wasm-unknown-simd.json` is the same plus `+simd128`, so a kernel written as
independent lanes vectorises; `make SIMD=1` builds with it. Off by default, since a server whose WASM tier has
no SIMD (Fast JIT, the x86-64 default) refuses a module built with it, where the scalar build runs. TinyGo
0.34 or later, for range-over-func; `go` itself only runs the generator.

## The directives

The three directives are the Go face of the Rust SDK's `export!`, `stream!` and `event!`; `udfgen`
(`cmd/udfgen`) turns them into the exports at generation time.

- **`//udf:function <name>`** on a function: a record UDF. The parameters are the arguments, decoded by
  type (`int64`/`int`/`int32` → `int`, `float64` → `double`, `bool`, `string` → `str`, `[]byte` → `bytes`,
  `udf.Value` → `any`, `[]udf.Value` last → `any...`; a pointer type for one that may be absent or nil, `nil`
  then), and the return is the result: nothing (nil), an `error`, a value, or a value and an `error`, the
  value being any of those types or `*udf.Map` / `*udf.List`. A missing, mismatched or surplus argument
  fails the call before the function runs, naming the argument; a function whose last parameter takes
  the rest takes `udf.MaxArgs` (128) arguments in all, and a call with more fails, naming the limit and
  the count; a returned error fails it with its
  message. A first parameter of type `*udf.Record` is the record, not an argument: the export passes it,
  and a function that does not touch the record leaves it out.
- **`//udf:stream <name>`** on a `var x T` whose type has a `Record` method and an `End` method: a stream
  UDF, exported as `name_record` and `name_end` with the signature `stream name { record(...) end(...) }`.
  The variable is the stream's state; `Record` returns nothing or an `error`, `End` the partial. `Record`
  may take the `*udf.Record` first, to read: a stream's record refuses every write. `End` takes none.
- **`//udf:on xdr-merge <name>`** on a function: the handler of the one event, the merge of an incoming
  XDR write with the record it meets. It is `func(current, incoming *udf.EventRecord)
  (udf.Resolution, error)`, and the resolution is `KeepLocal()`, `TakeRemote()`, `Policy()`,
  `DeleteRecord()`, or the final record: `Resolve(bins)`, `ResolveTTL(bins, ttl)`. A handler with other
  parameters or another return, or another kind after `on`, is a generation error naming the file and
  line.
- **The raw form.** A function declared `(args, argc int32) int32` is exported as it is under any of the
  three, and decodes its arguments itself with `udf.Args`; the directive's `params` (and `end` for a
  stream) gives the signature's parameter text. It too may take the record first: `(rec *udf.Record, args,
  argc int32) int32`. This is for a hot path that reads its arguments once and keeps them, as
  `examples/vector` does.

## Shape of the API

- **`*udf.Record`**: the record the call is on, and everything about it. Reading: `Bin(name)` for a bin
  that must be there (`(Value, error)`, the error naming the bin), and for one that may not be, readers
  with three outcomes: `Get(name)` (`(Value, bool, error)`), the typed `Int`, `Double`, `Bool`, `String`,
  `Blob`, `Bytes` as `(T, bool, error)`, and `List(name)` / `Map(name)` as the stored `Value`, each `false`
  for an absent or nil bin and an error with `Status` `udf.TypeMismatch` for a bin of another kind;
  nothing is converted (an integer is not a double, a string is not a blob; `Bytes` reads the raw bytes
  of a string, blob, GeoJSON or HLL). `IntOr` / `DoubleOr` / `BoolOr` (`(T, error)`) take a default for an
  absent or nil bin only. `Has`, `BinCount`. Writing: `Set(name, value)` over
  every kind (a Go scalar, string, `[]byte`, a `udf.Value` read from a record, a built `*udf.Map` /
  `*udf.List`, `udf.GeoJSONValue(doc)`; `nil` removes the bin), `SetInts` / `SetDoubles` for a packed
  list in one import, `SetBlob` and `SetGeoJSON` by name, `Remove(name)`. Metadata: `Exists`, `Generation`, `TTL`, `SetTTL`,
  `LastUpdateTime`, `Size`, `SetName`, `Key(buf)` (`(Value, bool, error)`: an error, never a cut key,
  when `buf` is too small), `Digest(buf)`. And `Save()`, which writes the staged
  bins and creates the record when there is none, and `Delete()`. There is no bin object and no
  create/update/commit: a bin is a name, and `Save` is the one way a staged write takes effect.
- **One record type.** Go has no read-only reference, so the same `*udf.Record` is handed out where the
  record may be written and where it may not. A write the host does not take - any write to a stream's
  record - returns an error that says so and changes nothing.
- **`udf.RecordReader`**: the reading methods as an interface, implemented by `*udf.Record` and
  `*udf.EventRecord`, so one helper reads a bin from the record of a UDF or from a side of an event;
  `examples/counter` gathers its counts that way in both its write UDFs and its merge.
- **`udf.Value`**: the 16-byte value every read is, with `Int()`, `Double()`, `Bool()`, `String()`,
  `Blob()`, `Bytes()`, `Floats()` (each `(T, error)`, none converting), `Lookup(key)` / `LookupInt(key)`
  and `Index(i)` for one entry or element in one import (`(Value, bool, error)`: false when it is not
  there, an error when the value is not a map or list), the `Is*` tests (`IsGeoJSON` and `IsHLL`
  included), and **`List()` / `Map()`** as Go 1.23 iterators
  (`for v := range v.List()`, `for k, v := range v.Map()`, 32 per import, the cursor released when the loop
  ends or breaks). `Kind` is a typed enum: `udf.KindInt`, `udf.KindMap`, ...
- **`*udf.Map` and `*udf.List`**: collections built on the host, with chainable `Put(key, any)` / `Add(any)`
  over every kind, the key a string or an integer, `PutGeoJSON` / `AddGeoJSON`, `PutAll` / `AddAll` from a
  stored collection by position, `PutEntries` / `AddValues` for a batch in one import, `Len()`, and
  `Map.Contains(key)`, which the host answers without packing the map; no put or add takes a handle, so a
  loop builds thousands of entries; the first refusal is kept and answered by `Err()`. Written to a bin,
  returned as the result, nested, or handed back from a handler. **`Close()`** gives the collection's
  handle back once it has been handed over (the host keeps its own reference to what it is given);
  one not closed is given back when the call ends. A call is bounded by its memory on the server, not by
  a count of handles, so a loop that builds and closes a collection per iteration runs as long as the
  memory lasts. Every walk the SDK makes closes its cursor when it ends, early exit included.
- **Not about the record**, so on the package: `udf.Now`, `udf.Log`, `udf.Fail`, the `Result*` calls.
- **Errors carry their status.** Every error the SDK returns is a `*udf.Error` whose `Status` is the
  host's (`udf.NotFound`, `udf.TypeMismatch`, `udf.Refused`, `udf.Err`, or at or below `udf.NeededBase`
  for a buffer too small, with `Needed()` the size); `udf.StatusOf(err)` reads it from any error.
- **The merge**: `*udf.EventRecord` for each of the two records the event hands over, read-only (the
  `RecordReader` methods, `SourceID`, `BinLastUpdateTime(name)` and `BinSourceID(name)`, the `BinNames`
  and `DeletedBins` iterators, `Deleted(name)`, `WithBins(out)` to keep the rest of a record under a few decided bins). What the server says of the merge is on the record it is true of: `incoming.IsDelete()`,
  `incoming.IsPartial()`, `incoming.LostBins()`, `current.IsTombstone()`, and `UsesBinConvergence()` on
  either. `udf.PairOf(current, incoming)` is the two side by side, a `udf.Pair`, read into
  `udf.Both[T]` (its sides are `Current` and `Incoming`) with `IncomingIsNewer`, `Newer`, `Older`,
  `Ordered`, `CurrentOrIncoming`, `IncomingOrCurrent`, `Merge`, and `udf.Max` / `Min` / `Sum` over a
  `Both` (`Sum`, like `Max` and `Min`, answers false when neither side has the bin, and wraps on overflow); the `Pair` readers answer an
  error when a side holds another kind; `Pair.WithNewer(out)` keeps the rest of the record from whichever
  side wrote each bin later, and leaves out a bin that side deleted. `examples/resolver` is the worked example, held to the same behaviour as the other
  languages' by the client tests.
- **Under the typed tier**, for a hot path that must not allocate: `udf.Args` into a caller-owned
  window, `udf.Bytes(v, buf)` to copy a value's raw bytes into a buffer (a status, never a cut copy, when
  it does not fit), `Record.Get`, `Record.Digest(buf)`
  and `Record.Key(buf)`, the `udf.*Value` constructors for a batch, `udf.Fail` and the scalar `Result*`
  calls. The vector example's per-record path is written on these. The calls that took a bare `int32`
  handle (`NewMapHandle`, `MapPut*`, `ListPush*`, `Enter` / `Next` / `Close`, `PackHandle`, ...) are no
  longer exported: the builders and the iterators cover them, and a handle is not something a UDF needs
  to hold. The `udf.Arg*` decoders and `udf.CurrentRecord()` that the generated exports call are public.

## What is different about Go here

Standard Go's WebAssembly output needs WASI or a JavaScript host, neither of which the server provides;
TinyGo's `wasm-unknown` is freestanding. TinyGo brings a runtime and a garbage collector, so a Go module is
not allocation-free by construction the way a Rust or C one is: the typed tier allocates a little per call,
which a record UDF or a handler can afford, and a stream's hot path stays on the calls that take a
buffer and on fields it reuses; `-gc=leaking` is fine for an instance that lives for one call or one query. A panic traps the
module (`-panic=trap`), and the host reports only that it trapped: return an `error` instead.

## The examples

- `examples/vector`: the exact top-k over packed float vectors that the Rust, C and Java SDKs also carry,
  digest for digest and bit for bit, a `//udf:stream` over a `TopK` variable in the raw form, its
  per-record path allocation-free; `tests/src/udf_vector.rs` holds the five engines (Lua included) to one answer and times
  them.
- `examples/resolver`: the account merge on `xdr-merge`, over `Pair` and `Both`.
- `examples/counter`: the smallest CRDT, a grow-only counter on `xdr-merge` over a `map[string]int64`; read
  it first. The client test `go_generated_exports_decode_arguments` drives its generated exports through
  every rejection.
- `examples/pncounter`: the counter that can go down, two grow-only maps (`inc`, `dec`) merged map by map
  with a fresh `table` each.
- `examples/twopset` and `examples/lwwset`: steps 3 and 4 of the book's CRDT tutorial, a set with
  removals (two lists merged by union, `sort.Strings` for canonical bytes) and a set with re-adds (two maps
  of `udf.Now()` stamps merged by the later stamp); both return a `*udf.List` of members.
- `examples/registers`: a record whose every bin is a last-writer-wins register, stamped by the server
  under bin convergence; the `xdr-merge` handler reads `BinLastUpdateTime` and `BinSourceID`, walks
  `BinNames` and `DeletedBins`, and merges the one bin that is not a register, `seen`, by the larger number.
- `examples/crdt`: the active-active CRDT of the design note, rule for rule the same as the Rust one, held
  to the same nine dry-run scenarios by `tests/src/events.rs`.
- `examples/udf`: the SDK's own fixture, one function per corner of the record API (range over lists and
  maps, typed readers, every write, integer keys, GeoJSON, metadata, TTL, deletion, `Save` with nothing
  staged, `Remove`, a list element by index, `Map.Contains`), which the tests in `tests/src/udf_go.rs`
  hold to the Rust fixture's expectations. Not an example to copy.

`make` generates and builds every example; `make fixtures` copies them to `tests/fixtures/wasm/` as
`*_go.wasm`. In this repository the toolchain lives in the `asd-build-wasm` container, which sees only
the server trees: `podman cp` this directory to `/work/go`, build there, copy the `.wasm` files and the
regenerated `udf_exports.go` files back.
