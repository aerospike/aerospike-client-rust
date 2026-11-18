# C

A C stream UDF is two exported functions, `<name>_record` and `<name>_end`, over the same header a record
UDF uses, with state in statics. There is no stream-specific API; the contract is in the names, and
`AS_UDF_SIGNATURE` describes it as `stream <name> { record(...) end() }`.

## The shape

```c
#include "aerospike_udf.h"

static int64_t total;
static int64_t count;

AS_UDF_EXPORT(sum_n_record) int32_t
sum_n_record(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	as_udf_value v;
	if (as_udf_rec_bin("n", &v) == AS_UDF_OK && v.kind == AS_UDF_INT) {
		total += v.payload;
		count++;
	}

	return 0;
}

AS_UDF_EXPORT(sum_n_end) int32_t
sum_n_end(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	int32_t out = as_udf_map_new(2);
	if (out <= 0) return as_udf_failf("sum_n: could not build the partial");

	as_udf_map_put_str_int(out, "total", 5, total);
	as_udf_map_put_str_int(out, "count", 5, count);

	return as_udf_result_handle(out) == AS_UDF_OK ? 0 : as_udf_failf("sum_n: could not return the partial");
}

AS_UDF_SIGNATURE(sum_n, "stream sum_n { record() end() }")
```

`_record` returning non-zero (after `as_udf_failf`) ends the stream with the message. `_end` returns the
partial. One instance sees every record a query thread reads, so statics are the state.

`_record` reads the record it is called on with the `as_udf_rec_` functions a record UDF uses:
`as_udf_rec_bin`, the metadata, the key and the digest. It cannot write. An aggregation's record takes no
write, so the `as_udf_rec_set_*` family, `as_udf_rec_remove`, `as_udf_rec_save` and `as_udf_rec_delete`
are all refused there. `_end` runs after the last record and has no record to read.

## The vector top-k

`examples/vector/vector.c` is the full-size example; `make SIMD=1` builds it with `-msimd128`. Its stream half:

```c
{{#include ../../../aerospike-udf-sdk-wasm-c/examples/vector/vector.c:335:384}}
```

## API reference

{{#include ../api/c-stream.md}}
