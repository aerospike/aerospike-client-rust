# Go

A Go stream UDF is a type with a `Record` method, called with every record a query thread reads, and an
`End` method, called once afterwards, whose return is the partial, and a variable of that type under a
`//udf:stream name` directive. The variable is the stream's state, which is the "one instance sees the
whole stream" the contract promises; the generator exports `name_record` and `name_end` over it, with the
signature `stream name { record(...) end(...) }` derived from the two methods' parameters, decoded like a
record UDF's. `Record` takes the record it is called on as its first parameter, `rec *udf.Record`, which
is not an argument and is not in the signature.

## The shape

```go
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go/cmd/udfgen

import udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk/wasm-go"

type SumN struct {
	total, count int64
}

func (s *SumN) Record(rec *udf.Record) error {
	n, ok, err := rec.Int("n")
	if err != nil {
		return err // an n that is not an integer ends the stream
	}

	if ok {
		s.total += n
		s.count++
	}

	return nil
}

func (s *SumN) End() (*udf.Map, error) {
	out := udf.NewMap(2).Put("total", s.total).Put("count", s.count)

	return out, out.Err()
}

//udf:stream sum_n
var sumN SumN

func main() {}
```

`Record` returns nothing or an `error`; an error ends the stream with the message. `End` returns the
partial, here a built map. Nothing resets the fields between records; `-gc=leaking` is fine for an
instance that lives for one query thread's run.

The record a stream is shown is read-only: an aggregation cannot write. Go has one `Record` type, so this
is not caught when the module is built. A `Set`, `Remove`, `Save` or `Delete` in `Record` is refused by
the host at run time and returns an error that says so. `End` has no record, since none is in hand when
the stream ends, and declaring one there is a generation error.

Everything `Record` reads with is the record API of the [previous part](../record-udfs/go.md). On a path
that runs per record, prefer `rec.Get` and the typed readers, which answer false for a missing bin, to
`rec.Bin`, which builds an error for one. A collection bin is read with `rec.List("l")` or
`rec.Map("m")` and ranged over as in a record UDF.

A stream in the raw form has `Record(args, argc int32) int32`, or `Record(rec *udf.Record, args, argc
int32) int32` to take the record, and `End(args, argc int32) int32`. They are exported as they are, and
the directive's `params` and `end` give the signature's parameter text. That is the form for a stream that
reads its arguments once, on the first record, and keeps them: decoding typed parameters on every record
would cost an import and, for a `string`, an allocation per record.

## The vector top-k

`examples/vector/main.go` is the full-size example, in the raw form. Its stream half:

```go
{{#include ../../../wasm-go/examples/vector/main.go:stream}}
```

The record method configures itself from the arguments on the first call, reads the vector straight into
a `[maxDim]float32` field, scores it with a four-lane kernel, and offers it to a bounded top-k. It hands
its record on to the helpers that read the vector, the digest and the key. The end method builds the
partial with one `PutEntries` batch for each hit.

## API reference

{{#include ../api/go-stream.md}}
