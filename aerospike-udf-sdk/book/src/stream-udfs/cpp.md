# C++

A C++ stream UDF is two functions under `UDF_STREAM_RECORD(name, ret, params...)` and
`UDF_STREAM_END(name, ret, params...)`, in that order, over the same header a record UDF uses, with state
in statics. The macros export `name_record` and `name_end` and derive the signature
`stream name { record(...) end(...) }` from the two parameter lists, decoded like a record UDF's.

## The shape

```cpp
#include "aerospike_udf.hpp"

static int64_t total;
static int64_t count;

UDF_STREAM_RECORD(sum_n, udf::Result<void>, const udf::Record& rec)
{
	// A record whose n is absent, nil or of another kind is not counted: the reader says which it was.
	if (udf::Result<udf::Optional<int64_t>> n = rec.as_int("n"); n && *n) {
		total += **n;
		count++;
	}

	return {};
}

UDF_STREAM_END(sum_n, udf::Result<udf::Map>)
{
	udf::Map out(2);

	out.put("total", total).put("count", count);
	UDF_TRY(out.result());

	return out;
}
```

The record half takes the record it is called on as `const udf::Record& rec`, read-only: an aggregation
cannot write, and `udf::Record&` there does not compile. It returns `void` or `udf::Result<void>`; an
error ends the stream with its message. The end half has no record, and a record parameter there does
not compile either. It returns the partial, here a built map. One instance sees every record a query
thread reads, so statics are the state and nothing resets them between records.

Parameters work as in `UDF_FUNCTION`, after the record:
`UDF_STREAM_RECORD(topk, void, const udf::Record& rec, udf::Text<16> bin, udf::Optional<int64_t> k)` is
decoded and checked on every record and gives the signature `record(bin: str[16], k?: int)`. The record
is not in it.

`UDF_STREAM_RAW(name, "(record params)", "(end params)")` declares both halves in the raw form,
`name_record_udf_impl(int32_t args, int32_t argc)` and `name_end_udf_impl`, which the module then defines,
and gives the signature text. That is the form for a stream that reads its arguments once, on the first
record, and keeps them: decoding typed parameters on every record would cost an import per record. A
raw half is not handed a `udf::Record`; it reads the record with the C header's `as_udf_rec_*` functions.
A collection bin is ranged over as in a record UDF.

## The vector top-k

`examples/vector/vector.cpp` is the full-size example, in the raw form. Its stream half:

```cpp
{{#include ../../../wasm-cpp/examples/vector/vector.cpp:stream}}
```

The record half configures itself from the arguments on the first call, reads the vector straight into
a static `float[]`, scores it with a four-lane kernel, and offers it to a bounded top-k; the end half
builds the partial with `as_udf_map_put_entries` for each hit.

## API reference

{{#include ../api/cpp-stream.md}}
