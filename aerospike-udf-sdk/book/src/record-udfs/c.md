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
	(void)args; (void)argc;

	int64_t n = as_udf_rec_bin("n", &v) == AS_UDF_OK && v.kind == AS_UDF_INT ? v.payload : 0;

	as_udf_rec_set_int("n", 1, n + 1);
	as_udf_rec_save();
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
`as_udf_fail_str("message")`, which returns the 1 to return (the message is taken as it is: there is no
format). `AS_UDF_SIGNATURE(name, "text")` exports the signature a client reads by executing
`__udf_sig_name`.

A UDF takes at most 128 arguments (`AS_UDF_MAX_ARGS`), and a call with more than a function declares has
to fail rather than run on the first few. `as_udf_args` reads into an array the function sizes, so the
function checks `argc` itself: `if (argc > 2) return as_udf_fail_str("f takes at most 2 arguments");`.
The other SDKs check arguments for the function, with the same words in each: `f takes at most 2 arguments,
got 3`, `f: argument 1 (name) is missing`, `f: argument 1 (name) is not a string` (arguments count from 1),
`f: argument 2 (n) is out of range for a 32-bit int`, `f: argument 1 (name) is 300 bytes, at most 256`. A C
module that wants to answer as they do writes those messages itself; the `apitest` example does.

## The record

C has no objects, so the record of the call is a family of functions. Every function that acts on it
starts with `as_udf_rec_`: reading a bin, staging a write, removing a bin, the metadata, saving and
deleting. The records an event hands over have their own family, `as_udf_event_`. What carries neither
prefix is not about a record: `as_udf_now`, `as_udf_log`, `as_udf_fail`, `as_udf_result_*`, the
list and map builders.

A function that does not touch the record calls none of them. The record is not one of the call's
arguments: `args` and `argc` hold only what the client passed.

## Values

Every read fills an `as_udf_value`: `kind`, `len`, `payload`, with `as_udf_value_src()` and
`as_udf_value_at()` for where a string or collection sits and `as_udf_value_double()` for a float's bits.
The kinds are `AS_UDF_NIL`, `AS_UDF_BOOL`, `AS_UDF_INT`, `AS_UDF_DOUBLE`, `AS_UDF_STR`, `AS_UDF_BLOB`,
`AS_UDF_LIST`, `AS_UDF_MAP`, `AS_UDF_GEOJSON`, `AS_UDF_HLL`, `AS_UDF_ABSENT`; the status codes
`AS_UDF_OK`, `AS_UDF_NOT_FOUND`, `AS_UDF_TYPE_MISMATCH`, `AS_UDF_REFUSED`, and anything at or below
`AS_UDF_NEEDED_BASE` means a buffer was too small.

## Reading

- `as_udf_rec_bin("name", &v)` reads a bin of any kind. It returns `AS_UDF_NOT_FOUND` when the record has
  no such bin.
- A bin of a known kind has a reader of its own, and every one of them has the same three answers, told apart
  by the status: `AS_UDF_OK` with the value, `AS_UDF_NOT_FOUND` when the bin is absent, and
  `AS_UDF_TYPE_MISMATCH` when it holds another kind (an integer is not a double, a string is not a blob,
  GeoJSON is not a string). `as_udf_rec_int("name", &n)`, `as_udf_rec_double` and `as_udf_rec_bool` fill the
  variable; `as_udf_rec_string("name", buf, cap)` and `as_udf_rec_blob` copy the bytes out and return the
  length or the status; `as_udf_rec_list` and `as_udf_rec_map` fill the collection as a value to walk, which
  may be empty. `as_udf_rec_int_or("name", 0, &n)`, `as_udf_rec_double_or` and `as_udf_rec_bool_or` put the
  default in for an absent bin only, and answer `AS_UDF_TYPE_MISMATCH` for a bin of another kind.
  `as_udf_as_int(&v, &n)` and its family are the same readers on a value: an element, an entry, an argument.
  `as_udf_rec_has("name")` says whether the bin is there, and `as_udf_rec_get("name", &v)` is
  `as_udf_rec_bin` that also answers false for a nil.
- `as_udf_rec_blob` takes only a blob. `as_udf_bytes(&v, buf, cap)` copies the raw bytes of a string, blob,
  GeoJSON or HLL value, and `as_udf_rec_bytes("name", buf, cap)` of such a bin; both answer
  `AS_UDF_TYPE_MISMATCH` for any other kind.
- `as_udf_cursor_enter_at(src, at)` opens a cursor over a list or map at a value's position;
  `as_udf_cursor_values(cur, out, cap)` fills `out` with the next values, 128 per import;
  `as_udf_cursor_close(cur)` gives the cursor back.
- `as_udf_coll_get(src, at, key, key_len, &v, sizeof v)` looks a string key up in a map; `as_udf_coll_get_int`
  an integer key. `as_udf_map_get(&map, "key")` and `as_udf_map_get_int(&map, 1)` are the same lookups on a
  value; a key the map does not have gives a value of kind `AS_UDF_ABSENT`, not an error.
- `as_udf_list_at(&list, index)` returns one element of a stored list by its index, in one import and
  without a cursor. Its kind is `AS_UDF_ABSENT` past the end or when the value is not a list.
  `as_udf_list_get` is the import under it.
- `v.len` is the size of a stored list or map, so no walk is needed to count.
- `as_udf_args(args, argc, out, cap)` decodes the call's arguments in one import, as many as `out` holds;
  compare `argc` with what the function takes.

```c
AS_UDF_EXPORT(sum_list) int32_t
sum_list(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	as_udf_value l;
	if (as_udf_rec_bin("l", &l) != AS_UDF_OK || l.kind != AS_UDF_LIST) return as_udf_fail_str("sum_list: no list bin l");

	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(&l), as_udf_value_at(&l));
	if (cur <= 0) return as_udf_fail_str("sum_list: could not open the list");

	as_udf_value w[128];
	int64_t total = 0;
	for (int32_t n; (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 0; i < n; i++) {
			if (w[i].kind != AS_UDF_INT) { as_udf_cursor_close(cur); return as_udf_fail_str("sum_list: not an integer"); }
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

`as_udf_rec_set_int`, `_set_double`, `_set_bool`, `_set_string`, `_set_blob`, `_set_geojson`, `_set_handle`
(a built collection) and `_set_at` (a value read from a record, by position) each stage a write to a bin.
`as_udf_rec_remove(name, len)` stages the removal of a bin. There is no call that sets a bin to nil: a
bin cannot hold nil, and removing it is the same thing. `as_udf_rec_set_ttl(seconds)` sets the TTL of the
write. Every name is passed with its length.

**Nothing staged reaches the server until `as_udf_rec_save()`.** A function that returns without saving
has changed nothing. `as_udf_rec_save()` creates the record when the key has none, so there is one call
for both. A function that must not overwrite checks `as_udf_rec_exists()` first; the record is locked for
the whole call, so nothing comes between the check and the save. Two details:

- A save with nothing staged is still a write. The server treats it as a touch: the generation and the
  last-update time move.
- A save with nothing staged on a key with no record makes nothing, since a record must have a bin, and
  the client is told "key not found".

`as_udf_rec_delete()` deletes the record. It needs no save, and whatever was staged goes with the record.
The server carries it out when the call returns, so `as_udf_rec_exists()` keeps its answer for the rest
of the call.

`as_udf_rec_set_ints` and `as_udf_rec_set_doubles` pack an array straight into a list particle.
They take effect at once, without a save.

```c
AS_UDF_EXPORT(clear_note) int32_t
clear_note(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	as_udf_rec_remove("note", 4);                 /* staged, like a set */
	if (as_udf_rec_save() != AS_UDF_OK) return as_udf_fail_str("clear_note: could not save");

	as_udf_result_int(as_udf_rec_bin_count());
	return 0;
}

AS_UDF_EXPORT(close_account) int32_t
close_account(int32_t args, int32_t argc)
{
	(void)args; (void)argc;

	/* No save: the delete is the write. */
	if (as_udf_rec_delete() != AS_UDF_OK) return as_udf_fail_str("close_account: could not delete");

	as_udf_result_nil();
	return 0;
}
```

## Metadata

On the record: `as_udf_rec_exists()`, `as_udf_rec_generation()`, `as_udf_rec_ttl()` (0 for a record that does
not expire), `as_udf_rec_last_update_time()`, `as_udf_rec_size()`, `as_udf_rec_bin_count()`,
`as_udf_rec_set_name(buf, cap)`, which copies out the name of the record's set, `as_udf_rec_key_type()`
with `as_udf_rec_key_int()` and `as_udf_rec_key_bytes(buf, cap)` for the user key of a record written
with `send_key`, `as_udf_rec_key(buf, cap, &key)` for that key as one value (`AS_UDF_NOT_FOUND` when there is
none, a needed-length status when it does not fit), and `as_udf_rec_digest(buf, cap)`. Not on the record: `as_udf_now()` and
`as_udf_log(level, message, len)`.

## Building collections

`as_udf_list_new(capacity)` and `as_udf_map_new(capacity)` return handles. Elements go in with
`list_push_int`, `list_push_string`, `list_push_double`, `list_push_bool`, `list_push_nil`, `list_push_blob`,
`list_push_geojson`, and `as_udf_list_push_value` for a value of any kind; entries with `map_put_string_int`,
`map_put_string_string`, `as_udf_map_put_string_value` (or `as_udf_map_put_value` with a NUL-terminated key)
and `as_udf_map_put_int_value` for any kind. None of these takes a handle, so a loop builds as many elements as
the call's memory allows. `as_udf_val_*` makes a value of its own on the host, by handle, for `list_push`,
`map_put` and `map_put_string_handle`: each holds a handle until it is closed.

`as_udf_handle_close(h)` gives any handle back: a built list or map once it has been stored, nested or
returned (each of which takes its own reference on the host), an `as_udf_val_*` value once it is in, a
cursor once its walk is done (`as_udf_cursor_close` is the same import). What is not closed is released
when the call ends; closing twice, or a handle the host still needs (a bin a value reads from), does
nothing. A call is bounded by its memory on the server, not by a count of handles, so a loop that builds
and closes a collection per turn runs as long as that memory lasts. `list_push_values` and `map_put_entries` take an array of values in one import; build them with
`as_udf_v_int`, `as_udf_v_double`, `as_udf_v_bool`, `as_udf_v_nil`, `as_udf_v_string`, `as_udf_v_blob`,
`as_udf_v_geojson`, `as_udf_v_handle`. `as_udf_rec_set_value(name, len, &v)` stages a value of any kind into a
bin. `as_udf_result_handle(h)` returns a built collection; `as_udf_result_at(src, at)` returns a stored value
by position.

`as_udf_map_contains(handle, "key")` and `as_udf_map_contains_int(handle, key)` say whether a map the call
is building has a key. The host answers for the map it holds, so nothing is packed to ask.
`as_udf_map_has` and `as_udf_map_has_int` are the imports under them, with the key's length passed.
`as_udf_coll_size(handle)` is the number of entries of a built map or elements of a built list.

## A module to read: the vector top-k

`examples/vector/vector.c` is the exact top-k over packed `f32` vectors, the same algorithm as the Rust and
Go modules, with SIMD lane kernels. Its record UDF and signatures:

```c
{{#include ../../../wasm-c/examples/vector/vector.c:record}}
```

> **A GeoJSON bin is saved only if it is GeoJSON.** Staging takes any text; the save builds the
> bin, and fails if the text is not a document the server would accept as a write.
> And after any save that fails, a list, map or blob value read before it answers an error: read
> the bin again.

## API reference

Generated from `aerospike-udf-sdk/wasm-c/include/aerospike_udf.h`: every import, function, macro, constant
and type of the record UDF surface, with its comment.

{{#include ../api/c-record.md}}
