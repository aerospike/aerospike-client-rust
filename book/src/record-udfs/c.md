# C

The C SDK, `aerospike-udf-sdk-wasm-c`, is one header, `include/aerospike_udf.h`: the module's imports with
their wasm import names, the value layout and status codes of the contract, and conveniences. A module is a
freestanding C file compiled with clang for `wasm32` and no libc.

## A module from nothing

```c
#include "aerospike_udf.h"

AS_UDF_EXPORT(increment) int32_t
increment(int32_t args, int32_t argc)
{
	as_udf_value v;
	int64_t n = as_udf_bin("n", &v) == AS_UDF_OK && v.kind == AS_UDF_INT ? v.payload : 0;

	as_udf_set_int("n", 1, n + 1);
	as_udf_rec_exists() ? as_udf_rec_update() : as_udf_rec_create();
	as_udf_result_int(n + 1);

	return 0;
}

AS_UDF_SIGNATURE(increment, "fn increment() -> value")
```

```sh
clang --target=wasm32 -O3 -nostdlib -ffreestanding -Wl,--no-entry -Wl,--export-dynamic \
    -Iinclude -o increment.wasm increment.c
```

`clang` with `wasm-ld` (lld) is all it takes. Apple's clang targets wasm32 but ships no `wasm-ld`; on a
Mac, install `llvm` or `lld` from Homebrew, or build in a Linux container as this repository's tests do.
`make SIMD=1` adds `-msimd128` for a kernel that should vectorise (off by default: a Fast JIT server, the
x86-64 default, refuses a SIMD module). The header defines weak `memcpy` and `memset`, which
clang emits calls to even with `-nostdlib`; a module that brings its own wins.

A UDF is `int32_t name(int32_t args, int32_t argc)` returning 0, declared with `AS_UDF_EXPORT(name)` so it
is exported under that name; it sets its result with `as_udf_result_*` and fails with
`as_udf_failf("message")`, which returns the 1 to return. `AS_UDF_SIGNATURE(name, "text")` exports the
signature a client reads by executing `__udf_sig_name`.

## Values

Every read fills an `as_udf_value`: `kind`, `len`, `payload`, with `as_udf_value_src()` and
`as_udf_value_at()` for where a string or collection sits and `as_udf_value_double()` for a float's bits.
The kinds are `AS_UDF_NIL`, `AS_UDF_BOOL`, `AS_UDF_INT`, `AS_UDF_DOUBLE`, `AS_UDF_STR`, `AS_UDF_BLOB`,
`AS_UDF_LIST`, `AS_UDF_MAP`, `AS_UDF_GEOJSON`, `AS_UDF_HLL`, `AS_UDF_ABSENT`; the status codes
`AS_UDF_OK`, `AS_UDF_NOT_FOUND`, `AS_UDF_TYPE_MISMATCH`, `AS_UDF_REFUSED`, and anything at or below
`AS_UDF_NEEDED_BASE` means a buffer was too small.

## Reading

- `as_udf_bin("name", &v)` reads a bin of any kind.
- `as_udf_bytes(&v, buf, cap)` copies the bytes of a string, blob, GeoJSON or HLL value; returns the length.
- `as_udf_cursor_enter_at(src, at)` opens a cursor over a list or map at a value's position;
  `as_udf_cursor_values(cur, out, cap)` fills `out` with the next values, 128 per import;
  `as_udf_cursor_close(cur)` gives the slot back.
- `as_udf_coll_get(src, at, key, key_len, &v, sizeof v)` looks a string key up in a map; `as_udf_coll_get_i`
  an integer key.
- `as_udf_args(args, argc, out, cap)` decodes the call's arguments in one import.

```c
AS_UDF_EXPORT(sum_list) int32_t
sum_list(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	as_udf_value l;
	if (as_udf_bin("l", &l) != AS_UDF_OK || l.kind != AS_UDF_LIST) return as_udf_failf("sum_list: no list bin l");

	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(&l), as_udf_value_at(&l));
	if (cur <= 0) return as_udf_failf("sum_list: could not open the list");

	as_udf_value w[128];
	int64_t total = 0;
	for (int32_t n; (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 0; i < n; i++) {
			if (w[i].kind != AS_UDF_INT) { as_udf_cursor_close(cur); return as_udf_failf("sum_list: not an integer"); }
			total += w[i].payload;
		}
		if (n < 128) break;
	}
	as_udf_cursor_close(cur);

	as_udf_result_int(total);
	return 0;
}
```

## Writing

`as_udf_set_int`, `set_double`, `set_bool`, `set_str`, `set_blob`, `set_geojson`, `set_nil`, `set_handle`
(a built collection), `set_at` (a value read from the record, by position), `bin_remove`,
`set_list_ints` and `set_list_doubles` (an array packed straight into a list particle); then
`as_udf_rec_update()` or `as_udf_rec_create()`. Every name is passed with its length.

## Building collections

`as_udf_list_new(capacity)` and `as_udf_map_new(capacity)` return handles. `list_push_int`,
`list_push_str`, `list_push` (a value handle from `as_udf_val_*`, or a collection handle), `list_push_at`
(a stored value), `map_put_str_int`, `map_put_str_str`, `map_put_str_handle`, `map_put_str_at`,
`map_put_i_at`, and `as_udf_map_put_value` for any kind. `list_push_values` and `map_put_entries` take an
array of values in one import; build them with `as_udf_v_int`, `as_udf_v_double`, `as_udf_v_str`,
`as_udf_v_blob`, `as_udf_v_handle`. `as_udf_result_handle(h)` returns a built collection;
`as_udf_result_at(src, at)` returns a stored value by position.

## A module to read: the vector top-k

`examples/vector/vector.c` is the exact top-k over packed `f32` vectors, the same algorithm as the Rust and
Go modules, with SIMD lane kernels. Its record UDF and signatures:

```c
{{#include ../../../aerospike-udf-sdk-wasm-c/examples/vector/vector.c:386:409}}
```

## API reference

Generated from `aerospike-udf-sdk-wasm-c/include/aerospike_udf.h`: every import, function, macro, constant
and type of the record UDF surface, with its comment.

{{#include ../api/c-record.md}}
