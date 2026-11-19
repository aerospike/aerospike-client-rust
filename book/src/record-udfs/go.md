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

That is the whole module. The `//udf:function` directive is the Go face of the Rust SDK's `export!`:
`go generate` runs the SDK's `udfgen` over the package and writes `udf_exports.go` beside it, with the
exported `increment(args, argc int32) int32` that decodes the call's arguments, calls the function and
delivers its return as the result; the `__udf_sig_increment` export, whose text `fn increment(by: int)
-> value` it derives from the parameter types; and nothing else. A generator is how Go does what a Rust
macro or a Java annotation processor does, and the generated file is checked in with the module, so a
build without the step still works.

`rec` is the record the call is on. `rec.IntOr("n", 0)` reads the bin as an integer, with 0 when it is
absent, and an error when it holds anything else. `rec.Set` stages a write, and `rec.Save()` makes it, creating the record if it does not
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
| `float64` | `double` | a double; an integer is refused, not widened |
| `bool` | `bool` | a boolean |
| `string` | `str` | a string |
| `[]byte` | `bytes` | a string or a blob, as its bytes |
| `udf.Value` | `any` | anything, nil included, as the 16-byte value |
| `*int64`, `*float64`, `*bool`, `*string`, `*udf.Value` | `name?: type` | an argument that may be absent or nil, `nil` then |
| `[]udf.Value`, last | `any...` | every remaining argument, up to 128 arguments in all |

A call that leaves a required argument out, passes one of another kind, or passes more arguments than
there are parameters, fails before the function runs, with a message that names the argument
(`increment: argument 2 (n) is not an int`, `increment takes at most 2 arguments, got 3`), the same way
the Rust macro rejects a call. A UDF takes `udf.MaxArgs`, 128, arguments at most in total: a function
whose last parameter takes the rest fails a call with more (`ar takes at most 128 arguments, got 129`),
and no argument is ever dropped.

The function returns nothing (a nil result), an `error`, a value, or a value and an `error`; the value is
any of the types above, or `*udf.Map` / `*udf.List` for a built collection, or `udf.Value` for anything
(`udf.GeoJSONValue(doc)` is a GeoJSON result). A non-nil error fails the call with its message.

**The raw form.** A function declared `(args, argc int32) int32` is exported as it is: it takes the
handles and returns the status, and decodes its arguments itself with `udf.Args`, which reads no more
than the window it is given: `udf.Arity(name, len(window), argc)` is the check that fails a call that
passed more. It may take the record
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
func balanceOf(rec udf.RecordReader) (int64, error) {
	return rec.IntOr("balance", 0)
}

//udf:function balance
func balance(rec *udf.Record) (int64, error) {
	return balanceOf(rec)
}

//udf:on xdr-merge keep_richer
func keepRicher(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	theirs, err := balanceOf(incoming)
	if err != nil {
		return udf.Resolution{}, err
	}

	mine, err := balanceOf(current)
	if err != nil {
		return udf.Resolution{}, err
	}

	if theirs > mine {
		return udf.TakeRemote(), nil
	}

	return udf.KeepLocal(), nil
}
```

## Reading

`rec.Bin("name")` returns the bin as a `udf.Value`, or an error naming the bin when the record has none.
It is the form for a bin that must be there. The others are for a bin that may not be, and each has
three outcomes a caller tells apart without reading a message: the value and `true`; `false` with a nil
error when the bin is absent or nil; and an error with `Status` `udf.TypeMismatch` when the bin holds
another kind. `rec.Get("name")` is the untyped one. The typed readers `rec.Int("name")`, `Double`,
`Bool`, `String` and `Blob` return `(T, bool, error)` and convert nothing: an integer is not a double,
a GeoJSON is not a string, and a string, GeoJSON or HLL is not a blob. `rec.Bytes("name")` reads the raw
bytes of any of those four. `rec.IntOr("name", 0)`, `DoubleOr` and `BoolOr` return `(T, error)`: the
default stands in for an absent or nil bin only, and a bin of another kind is still the error.
`rec.List("name")` and `rec.Map("name")` return the stored collection as a `udf.Value` the same way, so
an absent bin, a bin of another kind and an empty collection are three different answers.
`rec.Has("name")` says whether the bin is there, and `rec.BinCount()` how many there are.
`udf.StatusOf(err)` reads the status of any error the SDK returns.

So a bin with a default is one call, `n, err := rec.IntOr("n", 0)`, and a bin that must be there is two
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

`rec.SetInts("name", []int64{..})` and `rec.SetDoubles("name", []float64{..})` write a slice straight
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
with the accessors `Int()`, `Double()`, `Bool()`, `String()`, `Blob()`, each `(T, error)`, and the
`IsInt()`, `IsString()`, `IsGeoJSON()`, `IsHLL()`, `IsList()`, `IsMap()`, `IsNull()` tests. No accessor
converts: `Double()` of an integer is an error, and `Blob()` reads a blob only, where `Bytes()` reads the
raw bytes of a string, blob, GeoJSON or HLL. A stored string or collection is held by
position, never copied until asked. `Len` is a list's element count or a map's entry count, known without
a walk. `Lookup(key)` and `LookupInt(key)` read one entry of a stored map, and `Index(i)` one element of a
stored list, each in one import, with the three outcomes of the readers: `false` when there is none (past
the end, no such key), an error when the value is not a list or map.

`List()` and `Map()` on a `Value` are Go 1.23 iterators, the Rust SDK's `iter()` and `entries()`: a list
ranges over its values, a map over key and value, 32 per import, and the cursor is released when the loop
ends, breaks or returns. Lists nest: a value that is itself a list can be ranged over inside the loop. A
walk that cannot go on (the call's memory on the server is spent) fails the call rather than end short in
silence.

```go
//udf:function sum_list
func sumList(rec *udf.Record) (int64, error) {
	list, _, err := rec.List("l") // an absent bin ranges over nothing
	if err != nil {
		return 0, err // a bin that is not a list
	}

	var total int64

	for v := range list.List() {
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
	counts, _, err := rec.Map("counts")
	if err != nil {
		return "", err
	}

	var best int64
	var where string

	for site, v := range counts.Map() {
		if n, err := v.Int(); err == nil && n > best {
			best = n
			where, _ = site.String() // the entry's key
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

	last, ok, err := history.Index(int(history.Len) - 1) // one import, whatever the length
	if err != nil {
		return udf.NilValue(), err // not a list
	}
	if !ok {
		return udf.NilValue(), nil // an empty list
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
checks it when a map or list is returned. No put or add takes a handle, whatever the kinds, so a loop may
build a collection of thousands of entries one call at a time; a refusal's status is `udf.Refused`.

Go has no destructors, so a `*udf.Map` or `*udf.List` holds its handle until `Close()` gives it back, or
the call ends. Close one once it has been put, added, set or returned, since the host keeps its own
reference to what it is handed; `Close` is safe twice and on nil. A call is bounded by its memory on the
server, not by a count of handles, so a loop that builds and closes a collection per turn runs as long as
that memory lasts:

```go
out := udf.NewList(len(sites))
for _, site := range sites {
	entry := udf.NewMap(2).Put("site", site).Put("seen", now)
	out.Add(entry)
	entry.Close() // out keeps the entry
}
```

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
with one stored: an integer value, or a string or blob whose bytes go into `buf`; false when there is
none, and an error, whose `Needed()` is the size to call again with, when `buf` is too small. A key is
never cut.

`udf.Now()`, the server's clock, is not about the record and stays on the package. An event handler may
not read it.

## Under the typed tier

A few calls allocate nothing, for a path that runs once per record of a query. `udf.Args` decodes the
arguments into a window the caller owns. `udf.Bytes(v, buf)` copies a value's raw bytes into a buffer,
and answers a status, never a cut copy, when it does not fit.
`rec.Get`, `rec.Digest(buf)` and `rec.Key(buf)` read the record without building an error or a string.
The `udf.IntValue`, `StringValue`, `BlobValue` and `DoubleValue` constructors make values for a batch, which
`PutEntries` and `AddValues` hand the host in one import. `udf.Fail` and the scalar `Result*` calls
finish a call. The vector example's per-record path is written on these, because it reads its arguments
once per stream and scores from a `[maxDim]float32` field; nothing else needs to be.

The calls that took a bare `int32` handle are no longer exported. The builders and the iterators cover
them, and a handle is not something a UDF needs to hold. The `udf.Arg*` decoders and
`udf.CurrentRecord()`, which the generated exports call, are public, so a hand-written export can use
them too.

## What is different about Go

- **Allocation.** TinyGo brings a runtime and a garbage collector, so a Go module is not allocation-free by
  construction. The typed tier allocates a little per call (a `*Map`, a string per `String()`), which is
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
	p0, err := udf.ArgInt(a[:], n, 0, "increment", "by")
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
{{#include ../../../aerospike-udf-sdk-wasm-go/examples/vector/main.go:412:429}}
```

> **A GeoJSON bin is saved only if it is GeoJSON.** Staging takes any text; the save builds the
> bin, and fails if the text is not a document the server would accept as a write.
> And after any save that fails, a list, map or blob value read before it answers an error: read
> the bin again.

## API reference

Generated from the SDK's sources: every exported identifier of the record UDF surface, with its comment.
The `//go:wasmimport` declarations are the raw natives and are not part of the API.

{{#include ../api/go-record.md}}
