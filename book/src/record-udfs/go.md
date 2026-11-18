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

//udf:function increment
func increment(rec *udf.Record, by int64) (int64, error) {
	n, _ := rec.Int("n") // 0 when the bin is absent
	next := n + by

	if err := rec.Set("n", next); err != nil {
		return 0, err
	}

	return next, rec.Save()
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

`rec` is the record the call is on. `rec.Int("n")` reads the bin as an integer, with false, and so 0, when
it is absent. `rec.Set` stages a write, and `rec.Save()` makes it, creating the record if it does not
exist. Every call that can fail returns an `error`, and an error returned from the function fails the
call with its message.

```sh
go generate . && tinygo build -o increment.wasm -target=<sdk dir>/wasm-unknown.json \
    -opt=2 -no-debug -panic=trap -scheduler=none -gc=leaking .
```

`wasm-unknown.json` in the SDK directory inherits TinyGo's `wasm-unknown` target; `wasm-unknown-simd.json`
adds `+simd128`, so a kernel written as independent lanes vectorises, and `make SIMD=1` selects it (off by
default: a Fast JIT server, the x86-64 default, refuses a SIMD module). Register the `.wasm` with `UdfLang::Wasm`. TinyGo 0.34 or
later is needed, for range-over-func.

## Parameters and results

After the record, if the function takes one, the function's parameters are the UDF's arguments, in
order. Their Go types decide both how an argument is decoded and what the signature says:

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
handles and returns the status, and decodes its arguments itself with `udf.Args`. It may take the record
first as well: `(rec *udf.Record, args, argc int32) int32`. Since nothing can be derived, the directive's
`params` gives the signature's parameter text: `//udf:function vec_distance params="(bin: str, query:
bytes, metric: str)"`. This is the form for a hot path that wants to read its arguments once and keep
them, as the vector example does.

## The record

The record a function was called on is its first parameter, `rec *udf.Record`. A function that does not
touch the record leaves the parameter out. It is not one of the call's arguments. A client does not pass
it, the arity check does not count it, and the signature the module exports does not list it. Adding or
removing it changes nothing a client sees.

Everything about the record is a method on it. There is no bin object: a bin is addressed by its name.
What is left on the package is what is not about the record: `udf.Now()`, `udf.Log`, `udf.Fail`, the
`Result*` calls, the `NewMap` and `NewList` builders.

There is one `Record` type. Go has no read-only reference, so the same `*udf.Record` is handed out where
the record may be written and where it may only be read. The per-record method of a
[stream UDF](../stream-udfs/go.md) is the place it may only be read: every write there is refused at run
time, with an error that says so, and nothing is changed.

`*udf.Record` and the `*udf.EventRecord` an event handler is given both implement `udf.RecordReader`, the
reading methods as an interface. A helper that takes a `udf.RecordReader` reads a bin the same way in a
UDF and in a handler:

```go
// The balance of any record: the one a UDF is called on, or a side of an event.
func balanceOf(rec udf.RecordReader) int64 {
	n, _ := rec.Int("balance")

	return n
}

//udf:function balance
func balance(rec *udf.Record) int64 {
	return balanceOf(rec)
}

//udf:on xdr-merge keep_richer
func keepRicher(local, remote *udf.EventRecord, _ *udf.Conflict) (udf.Resolution, error) {
	if balanceOf(remote) > balanceOf(local) {
		return udf.TakeRemote(), nil
	}

	return udf.KeepLocal(), nil
}
```

## Reading

`rec.Bin("name")` returns the bin as a `udf.Value`, or an error naming the bin when the record has none.
It is the form for a bin that must be there. The others are for a bin that may not be. `rec.Get("name")`
returns the value and whether there is one, false for an absent or nil bin. The typed readers
`rec.Int("name")`, `Float64`, `Bool`, `Str` and `Bytes` (a blob's bytes) return the Go value and false
when the bin is absent, nil or of another kind. `rec.List("name")` and `rec.Map("name")` range over a
collection bin and yield nothing when it is absent. `rec.Has("name")` says whether the bin is there, and
`rec.BinCount()` how many there are.

So a bin with a default is one line, `n, _ := rec.Int("n")`, and a bin that must be an integer is two
steps, each with its own error:

```go
//udf:function withdraw
func withdraw(rec *udf.Record, amount int64) (int64, error) {
	balance, err := rec.Bin("balance") // an error naming the bin when it is absent
	if err != nil {
		return 0, err
	}

	have, err := balance.Int() // an error when it is not an integer
	if err != nil {
		return 0, err
	}

	if err := rec.Set("balance", have-amount); err != nil {
		return 0, err
	}

	return have - amount, rec.Save()
}
```

`Get` allocates nothing, where `Bin` builds an error for an absent bin. A stream's per-record method that
expects many records without the bin uses `Get`.

## Writing

`rec.Set("name", value)` stages a write of any supported Go type: an `int`, `int32` or `int64`, a
`float32` or `float64`, a `bool`, a `string`, a `[]byte` (a blob), a built `*udf.Map` or `*udf.List`, or a
`udf.Value` read from a record, which goes back by position without being decoded and keeps its kind.
`udf.GeoJSONValue(doc)` writes a GeoJSON, kept as the geospatial kind and not a string.
`rec.Remove("name")` stages the removal of a bin. Writing `nil` does the same, since a bin cannot hold
nil. `rec.SetTTL(seconds)` sets the TTL of the write.

**Nothing staged reaches the server until `rec.Save()`.** A function that returns without saving has
changed nothing, however plausible its result. `Save()` creates the record when the key has none, so
there is one verb for both. A function that must not overwrite checks `rec.Exists()` first, and since the
record is locked for the whole call nothing comes between the check and the save. Two details:

- A save with nothing staged is still a write. The server treats it as a touch: the generation and the
  last-update time move.
- A save with nothing staged on a key with no record makes nothing, since a record must have a bin, and
  the client is told "key not found".

`rec.Delete()` deletes the record. It needs no save, and whatever was staged goes with the record. The
server carries it out when the call returns, so `rec.Exists()` keeps its answer for the rest of the call.

`rec.SetInts("name", []int64{..})` and `rec.SetFloat64s("name", []float64{..})` write a slice straight
into a list, in one import, with no value built per element. They take effect at once, without a save.

Every one of these returns an `error`. It says the name is empty or longer than 15 bytes, or that the
host refused the write, which is what any write to a stream's record gets.

```go
//udf:function open_account
func openAccount(rec *udf.Record, owner string) (bool, error) {
	if rec.Exists() {
		return false, nil // must not overwrite; nothing comes between this check and the save
	}

	if err := rec.Set("owner", owner); err != nil {
		return false, err
	}

	if err := rec.Set("balance", 0); err != nil {
		return false, err
	}

	return true, rec.Save() // creates the record
}

//udf:function close_account
func closeAccount(rec *udf.Record) error {
	return rec.Delete() // no save: the delete is the write
}
```

## Values and ranging

Every read is a `udf.Value`: `Kind` (a `udf.Kind`: `KindNil`, `KindBool`, `KindInt`, `KindDouble`,
`KindStr`, `KindBlob`, `KindList`, `KindMap`, `KindGeoJSON`, `KindHLL`, `KindAbsent`), `Len`, `Payload`,
with the accessors `Int()`, `Float64()`, `Bool()`, `Str()`, `Bytes()`, each `(T, error)`, and the
`IsInt()`, `IsStr()`, `IsList()`, `IsMap()`, `IsNull()` tests. A stored string or collection is held by
position, never copied until asked. `Len` is a list's element count or a map's entry count, known without
a walk. `Lookup(key)` and `LookupInt(key)` read one entry of a stored map, and `Index(i)` one element of a
stored list, each in one import, with false when there is none.

`List()` and `Map()` on a `Value`, and `rec.List(name)` and `rec.Map(name)` on a record, are Go 1.23
iterators, the Rust SDK's `iter()` and `entries()`: a list ranges over its values, a map over key and
value, 32 per import, and the cursor is released when the loop ends or breaks. Lists nest: a value that is
itself a list can be ranged over inside the loop.

```go
//udf:function sum_list
func sumList(rec *udf.Record) (int64, error) {
	var total int64

	for v := range rec.List("l") {
		n, err := v.Int() // fails the call on anything but an int
		if err != nil {
			return 0, err
		}
		total += n
	}

	return total, nil
}

//udf:function busiest_site
func busiestSite(rec *udf.Record) (string, error) {
	var best int64
	var where string

	for site, v := range rec.Map("counts") {
		if n, err := v.Int(); err == nil && n > best {
			best = n
			where, _ = site.Str() // the entry's key
		}
	}

	return where, nil
}

//udf:function last_payment
func lastPayment(rec *udf.Record) (udf.Value, error) {
	history, err := rec.Bin("history")
	if err != nil {
		return udf.NilValue(), err
	}

	last, ok := history.Index(int(history.Len) - 1) // one import, whatever the length
	if !ok {
		return udf.NilValue(), nil // an empty list, or not a list
	}

	return last, nil
}
```

## Building collections

`udf.NewMap(capacity)` and `udf.NewList(capacity)` build a map or list on the host, to be written to a bin,
returned as the result, nested in another collection, or handed back from an event handler. `Put(key,
v)` and `Add(v)` take a value of any supported Go type (`int64`, `int`, `int32`, `float64`, `bool`,
`string`, `[]byte` as a blob, `udf.Value`, `*udf.Map`, `*udf.List`, or `nil` for a nil entry). The key of
`Put` is a string or an integer. `PutGeoJSON` / `AddGeoJSON` take a GeoJSON; `PutAll(stored)` and
`AddAll(stored)` copy a stored map or list by position. They chain, each is one import, and the first
refusal is kept and answered by `Err()`, the way `strings.Builder` keeps its error; a generated export
checks it when a map or list is returned.

`Len()` is how many entries or elements a built collection holds. `Contains(key)` asks a map being built
whether it has a key, a string or an integer. The host answers from the map as it stands, in one import,
and nothing is packed.

```go
//udf:function describe
func describe(rec *udf.Record) (*udf.Map, error) {
	out := udf.NewMap(4).
		Put("generation", rec.Generation()).
		Put("bins", rec.BinCount()).
		Put("tags", udf.NewList(2).Add("a").Add("b")).
		Put(7, "seven") // an integer key

	if !out.Contains("ttl") { // asked of the host, nothing packed
		out.Put("ttl", rec.TTL())
	}

	return out, out.Err()
}
```

## The record's metadata

All on the record: `rec.Exists()`, `rec.Generation()`, `rec.TTL()` in seconds, `rec.LastUpdateTime()` in
milliseconds since the Aerospike epoch, `rec.Size()` in bytes on device, `rec.BinCount()` and
`rec.SetName()` ("" when the record is in no set). `rec.Digest(buf)` copies the 20-byte digest into a
buffer you own and returns that part of it. `rec.Key(buf)` is the user key when the record was written
with one stored: an integer value, or a string or blob whose bytes go into `buf`, and false when there is
none.

`udf.Now()`, the server's clock, is not about the record and stays on the package. An event handler may
not read it.

## Under the typed tier

A few calls allocate nothing, for a path that runs once per record of a query. `udf.Args` decodes the
arguments into a window the caller owns. `udf.Bytes(v, buf)` copies a value's bytes into a buffer.
`rec.Get`, `rec.Digest(buf)` and `rec.Key(buf)` read the record without building an error or a string.
The `udf.IntValue`, `StrValue`, `BlobValue` and `DoubleValue` constructors make values for a batch, which
`PutEntries` and `AddValues` hand the host in one import. `udf.Fail` and the scalar `Result*` calls
finish a call. The vector example's per-record path is written on these, because it reads its arguments
once per stream and scores from a `[maxDim]float32` field; nothing else needs to be.

The calls that took a bare `int32` handle are no longer exported. The builders and the iterators cover
them, and a handle is not something a UDF needs to hold. The `udf.Arg*` decoders and
`udf.CurrentRecord()`, which the generated exports call, are public, so a hand-written export can use
them too.

## What is different about Go

- **Allocation.** TinyGo brings a runtime and a garbage collector, so a Go module is not allocation-free by
  construction. The typed tier allocates a little per call (a `*Map`, a string per `Str()`), which is
  fine for a record UDF or a handler; a stream's hot path stays on the calls that take a buffer and on
  fields it reuses. `-gc=leaking` is fine for an instance that lives for one call or one query.
- **Errors, not panics.** A panic traps the module (`-panic=trap`), and the host reports only that it
  trapped. Return an `error`; the generated export turns it into the failure message the client sees.
- **The generator.** `udfgen` runs with the host Go, inside `go generate`, and reads the package's
  directives through `go/ast`. It reports a function it cannot export (a parameter of another type, a
  handler with the wrong parameters, a stream variable whose type lacks `Record` or `End`) as an error
  with the file and line. `udf_exports.go` is ordinary Go that TinyGo compiles like anything else.
- **Coverage.** The SDK carries the Rust SDK's whole record surface; `examples/udf` is its fixture, one
  function per corner, and the tests in `tests/src/udf_go.rs` hold it to the expectations of the Rust
  fixture's tests.

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
	r, err := increment(udf.CurrentRecord(), p0)
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

The record is handed in ahead of the decoded arguments. It appears in neither the arity check nor the
signature text.

## A module to read: the vector top-k

`examples/vector/main.go` is an exact top-k over packed `f32` vectors, the same algorithm as the Rust, C
and Java modules, bit for bit: a `//udf:stream topk` over a `TopK` variable whose `Record` and `End`
methods are the two halves, a `//udf:function vec_distance`, and SIMD lane kernels for the three metrics.
Its state is the struct's fields. It keeps the raw form, since it reads its arguments once per stream
and keeps them, and gives the signatures through `params`. The record half, with the record first:

```go
{{#include ../../../aerospike-udf-sdk-wasm-go/examples/vector/main.go:401:418}}
```

> **A GeoJSON bin is saved only if it is GeoJSON.** Staging takes any text; the save builds the
> bin, and fails if the text is not a document the server would accept as a write.
> And after any save that fails, a list, map or blob value read before it answers an error: read
> the bin again.

## API reference

Generated from the SDK's sources: every exported identifier of the record UDF surface, with its comment.
The `//go:wasmimport` declarations are the raw natives and are not part of the API.

{{#include ../api/go-record.md}}
