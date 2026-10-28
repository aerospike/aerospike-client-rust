# Go

A Go stream UDF is a type with a `Record` method, called with every record a query thread reads, and an
`End` method, called once afterwards, whose return is the partial, and a variable of that type under a
`//udf:stream name` directive. The variable is the stream's state, which is the "one instance sees the
whole stream" the contract promises; the generator exports `name_record` and `name_end` over it, with the
signature `stream name { record(...) end(...) }` derived from the two methods' parameters, decoded like a
record UDF's.

## The shape

```go
package main

//go:generate go run github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go/cmd/udfgen

import udf "github.com/aerospike/aerospike-client-rust/aerospike-udf-sdk-wasm-go"

var n = udf.NewBin("n")

type SumN struct {
	total, count int64
}

func (s *SumN) Record() error {
	if v, ok := n.Get(); ok && v.IsInt() {
		s.total += v.Payload
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

A stream in the raw form has `Record(args, argc int32) int32` and `End(args, argc int32) int32`,
exported as they are, and the directive's `params` and `end` give the signature's parameter text. That is
the form for a stream that reads its arguments once, on the first record, and keeps them: decoding typed
parameters on every record would cost an import and, for a `string`, an allocation per record. A
collection bin is ranged over as in a record UDF, `for v := range bin.List()` or `for k, v := range
bin.Map()`.

## The vector top-k

`examples/vector/main.go` is the full-size example, in the raw form. Its stream half:

```go
{{#include ../../../aerospike-udf-sdk-wasm-go/examples/vector/main.go:344:402}}
```

The record method configures itself from the arguments on the first call, reads the vector straight into
a `[maxDim]float32` field, scores it with a four-lane kernel, and offers it to a bounded top-k; the end
method builds the partial with `MapPutEntries` for each hit.

## API reference

{{#include ../api/go-stream.md}}
