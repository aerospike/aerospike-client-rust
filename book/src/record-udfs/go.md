# Go

The Go SDK, `aerospike-udf-sdk-wasm-go`, is a package for [TinyGo](https://tinygo.org)'s `wasm-unknown`
target. Standard Go's WebAssembly output needs WASI or a JavaScript host, neither of which the server
provides; TinyGo's is freestanding, imports nothing but the server's natives, and exports `_initialize`,
which the server runs once per instance before any UDF.

## A module from nothing

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

That is the whole module. The `//udf:function` directive is the Go face of the Rust SDK's `export!`:
`go generate` runs the SDK's `udfgen` over the package and writes `udf_exports.go` beside it, with the
exported `increment(args, argc int32) int32` that decodes the call's arguments, calls the function and
delivers its return as the result; the `__udf_sig_increment` export, whose text `fn increment(by: int)
-> value` it derives from the parameter types; and nothing else. A generator is how Go does what a Rust
macro or a Java annotation processor does, and the generated file is checked in with the module, so a
build without the step still works.

`udf.NewBin("n")` is a bin of the record by name, kept as a package-level variable; `IntOr(0)` reads it as
an integer, 0 when absent, and `SetInt` stages a write that `udf.Commit()` makes, creating the record if
it does not exist. Every call that can fail returns an `error`, and an error returned from the function
fails the call with its message.

```sh
go generate . && tinygo build -o increment.wasm -target=<sdk dir>/wasm-unknown.json \
    -opt=2 -no-debug -panic=trap -scheduler=none -gc=leaking .
```

`wasm-unknown.json` in the SDK directory inherits TinyGo's `wasm-unknown` target; `wasm-unknown-simd.json`
adds `+simd128`, so a kernel written as independent lanes vectorises, and `make SIMD=1` selects it (off by
default: a Fast JIT server, the x86-64 default, refuses a SIMD module). Register the `.wasm` with `UdfLang::Wasm`. TinyGo 0.34 or
later is needed, for range-over-func.

## Parameters and results

The function's parameters are the UDF's arguments, in order, and their Go types decide both how an
argument is decoded and what the signature says:

| Go | Signature | Takes |
| --- | --- | --- |
| `int64`, `int`, `int32` | `int` | an integer (`int32` must fit) |
| `float64` | `double` | a double, or an integer widened |
| `bool` | `bool` | a boolean |
| `string` | `str` | a string |
| `[]byte` | `bytes` | a string or a blob, as its bytes |
| `udf.Value` | `any` | anything, nil included, as the 16-byte value |
| `*int64`, `*float64`, `*bool`, `*string`, `*udf.Value` | `name?: type` | an argument that may be absent or nil, `nil` then |
| `[]udf.Value`, last | `any...` | every remaining argument |

A call that leaves a required argument out, passes one of another kind, or passes more arguments than
there are parameters, fails before the function runs, with a message that names the argument
(`increment: argument 2 (n) is not an int`, `increment takes at most 2 arguments, got 3`), the same way
the Rust macro rejects a call.

The function returns nothing (a nil result), an `error`, a value, or a value and an `error`; the value is
any of the types above, or `*udf.Map` / `*udf.List` for a built collection, or `udf.Value` for anything
(`udf.GeoJSONValue(doc)` is a GeoJSON result). A non-nil error fails the call with its message.

**The raw form.** A function declared `(args, argc int32) int32` is exported as it is: it takes the
handles and returns the status, exactly as the hand-written export of old did, and decodes its arguments
itself with `udf.Args`. Since nothing can be derived, the directive's `params` gives the signature's
parameter text: `//udf:function vec_distance params="(bin: str, query: bytes, metric: str)"`. This is the
form for a hot path that wants to read its arguments once and keep them, as the vector example does.

## Bins

A `udf.Bin` is a bin of the record in hand, by name. Its getters read the bin as the type the UDF
expects, `Int()`, `Float64()`, `Bool()`, `Str()`, `Bytes()` (a string or blob's bytes), `Floats(dst)` (a
blob of 32-bit floats straight into the slice), each returning an error naming the bin when it is absent
or of another kind; the `Or` forms, `IntOr(0)`, `StrOr("")`, answer the default for absent and nil.
`Get()` is the bin as a `udf.Value` with whether it exists, `Exists()` says so alone, and `List()` and
`Map()` range over a collection bin. The setters stage a write of any kind: `SetInt`, `SetFloat64`,
`SetBool`, `SetStr`, `SetBytes` (a blob), `SetValue` (a value read from the record, by position, keeping
its kind), `SetMap`, `SetList`, `SetGeoJSON` (kept as the geospatial kind, not a string), `SetNull`,
`SetInts([]int64)` and `SetFloat64s` (a list in one import, no value built per element), and `Remove()`.
`udf.Commit()` writes them; `udf.Delete()` deletes the record, and `udf.SetTTL(seconds)` sets the time to
live the commit carries.

## Values and ranging

Every read is a `udf.Value`: `Kind` (a `udf.Kind`: `KindNil`, `KindBool`, `KindInt`, `KindDouble`,
`KindStr`, `KindBlob`, `KindList`, `KindMap`, `KindGeoJSON`, `KindHLL`, `KindAbsent`), `Len`, `Payload`,
with the accessors `Int()`, `Float64()`, `Bool()`, `Str()`, `Bytes()`, each `(T, error)`, and the
`IsInt()`, `IsStr()`, `IsList()`, `IsMap()`, `IsNull()` tests. A stored string or collection is held by
position, never copied until asked. `Lookup(key)` and `LookupInt(key)` read one entry of a stored map.

`List()` and `Map()` on a `Value` or a `Bin` are Go 1.23 iterators, the Rust SDK's `iter()` and
`entries()`: a list ranges over its values, a map over key and value, 32 per import, and the cursor is
released when the loop ends or breaks. Lists nest: a value that is itself a list can be ranged over
inside the loop.

```go
var (
	l      = udf.NewBin("l")
	counts = udf.NewBin("counts")
)

//udf:function sum_list
func sumList() (int64, error) {
	var total int64

	for v := range l.List() {
		n, err := v.Int()   // fails the call on anything but an int
		if err != nil {
			return 0, err
		}
		total += n
	}

	return total, nil
}

//udf:function max_count
func maxCount() (int64, error) {
	var best int64

	for site, v := range counts.Map() {
		if n, err := v.Int(); err == nil && n > best {
			best = n   // site is the entry's key
		}
	}

	return best, nil
}
```

## Building collections

`udf.NewMap(capacity)` and `udf.NewList(capacity)` build a map or list on the host, to be written to a bin,
returned as the result, nested in another collection, or handed back from an event handler. `Put(key,
v)` and `Add(v)` take a value of any supported Go type (`int64`, `int`, `int32`, `float64`, `bool`,
`string`, `[]byte` as a blob, `udf.Value`, `*udf.Map`, `*udf.List`, or `nil` for a nil entry); `PutI` takes
an integer key; `PutGeoJSON` / `AddGeoJSON` a GeoJSON; `PutAll(stored)` and `AddAll(stored)` copy a
stored map or list by position. They chain, each is one import, and the first refusal is kept and
answered by `Err()`, the way `strings.Builder` keeps its error; a generated export checks it when a map
or list is returned.

```go
//udf:function describe
func describe() (*udf.Map, error) {
	out := udf.NewMap(4).
		Put("generation", udf.Generation()).
		Put("bins", udf.BinCount()).
		Put("tags", udf.NewList(2).Add("a").Add("b"))

	return out, out.Err()
}
```

## The record's metadata

`udf.Generation()`, `TTL()`, `LastUpdateTime()`, `RecordSize()`, `BinCount()`, `SetName()` ("" when the
record is in no set), `Exists()`, `Digest(buf)`, `KeyType()` / `KeyInt()` / `KeyBytes(buf)` for the user
key, and `Now()`, the server's clock, which an event handler may not read.

## The low-level tier

Under the typed tier is the layer that mirrors the natives one call at a time: `udf.Bin_(name, &v)` into
a value the caller owns, `udf.Enter` / `Next` / `Close` over a window the caller owns, `udf.NewMapHandle`
and the `MapPut*` / `ListPush*` calls over `int32` handles, the `Set*` calls and `Result*` calls, and
`udf.Fail`, every one returning a status rather than an error. It allocates nothing and never touches the
collector. The vector example is written on it, because it reads its arguments once per stream and scores
from a `[maxDim]float32` field; nothing else needs to be. `udf.WrapMap(handle)` and `udf.WrapList(handle)`
bridge a handle built down there to the typed collections, and the `udf.Arg*` decoders the generated
exports call are public, so a hand-written export can use them too.

## What is different about Go

- **Allocation.** TinyGo brings a runtime and a garbage collector, so a Go module is not allocation-free by
  construction. The typed tier allocates a little per call (a `*Map`, a string per `Str()`), which is
  fine for a record UDF or a handler; a stream's hot path stays on the low-level tier and fields it
  reuses. `-gc=leaking` is fine for an instance that lives for one call or one query.
- **Errors, not panics.** A panic traps the module (`-panic=trap`), and the host reports only that it
  trapped. Return an `error`; the generated export turns it into the failure message the client sees.
- **The generator.** `udfgen` runs with the host Go, inside `go generate`, and reads the package's
  directives through `go/ast`. It reports a function it cannot export (a parameter of another type, a
  handler with the wrong parameters, a stream variable whose type lacks `Record` or `End`) as an error
  with the file and line. `udf_exports.go` is ordinary Go that TinyGo compiles like anything else.
- **Coverage.** The SDK carries the Rust SDK's whole record surface; `examples/udf` is its fixture, one
  function per corner, and `go_sdk_covers_the_rust_surface` in `tests/src/udf_go.rs` holds it to the
  expectations of the Rust fixture's tests.

## What the generator writes

For the module above, `udf_exports.go` reads:

```go
//export increment
func udfExport_increment(args, argc int32) int32 {
	var a [1]udf.Value
	n := udf.Args(args, argc, a[:])
	if err := udf.Arity("increment", 1, argc); err != nil {
		return udf.FailErr(err)
	}
	p0, err := udf.ArgInt64(a[:], n, 0, "increment", "by")
	if err != nil {
		return udf.FailErr(err)
	}
	r, err := increment(p0)
	if err != nil {
		return udf.FailErr(err)
	}
	udf.ResultInt(r)
	return 0
}

var udfSig_increment = []byte("fn increment(by: int) -> value\x00")

//export __udf_sig_increment
func udfSigFn_increment() int32 { return udf.Signature(&udfSig_increment) }
```

## A module to read: the vector top-k

`examples/vector/main.go` is an exact top-k over packed `f32` vectors, the same algorithm as the Rust, C
and Java modules, bit for bit: a `//udf:stream topk` over a `TopK` variable whose `Record` and `End`
methods are the two halves, a `//udf:function vec_distance`, and SIMD lane kernels for the three metrics.
Its state is the struct's fields. It keeps the raw form on the low-level tier, since it reads its
arguments once per stream and keeps them, and gives the signatures through `params`. The record half:

```go
{{#include ../../../aerospike-udf-sdk-wasm-go/examples/vector/main.go:404:421}}
```

## API reference

Generated from the SDK's sources: every exported identifier of the record UDF surface, with its comment.
The `//go:wasmimport` declarations are the raw natives and are not part of the API.

{{#include ../api/go-record.md}}
