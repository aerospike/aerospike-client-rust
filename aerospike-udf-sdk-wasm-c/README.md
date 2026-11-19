# aerospike-udf-sdk-wasm-c

Write Aerospike WASM UDFs in C, for the server's `mod-wamr` engine. One header, `include/aerospike_udf.h`:
the module's imports with their wasm import names, the value layout and status codes of the guest contract,
and a few conveniences. It is the C face of the same ABI the Rust SDK (`aerospike-udf-sdk-wasm`) and the Go
SDK (`aerospike-udf-sdk-wasm-go`) speak, and the Rust SDK's documentation is the reference for the contract.

```c
#include "aerospike_udf.h"

AS_UDF_EXPORT(increment) int32_t
increment(int32_t args, int32_t argc)
{
    as_udf_value v;
    int64_t n = as_udf_rec_bin("n", &v) == AS_UDF_OK && v.kind == AS_UDF_INT ? v.payload : 0;

    as_udf_rec_set_int("n", 1, n + 1);
    as_udf_rec_save();
    as_udf_result_int(n + 1);

    return 0;
}
```

Build it freestanding and register the `.wasm` with `UdfLang::Wasm`:

```sh
clang --target=wasm32 -O3 -nostdlib -ffreestanding -Wl,--no-entry -Wl,--export-dynamic \
    -Iinclude -o increment.wasm increment.c
```

`clang` with `wasm-ld` (lld) is all it takes; Apple's clang targets wasm32 but ships no `wasm-ld`, so on a
Mac install `llvm` or `lld` from Homebrew, or build in a Linux container as this repository's tests do. Add
`make SIMD=1` (that is, `-msimd128`) for a kernel that should vectorise; the server compiles it to NEON or
SSE ahead of time. Off by default: a module built with it is refused by a server whose WASM tier has no SIMD
(Fast JIT, the x86-64 default), where the same source without it runs scalar.

## What a UDF looks like

- A UDF is `int32_t name(int32_t args, int32_t argc)` returning 0; it sets its result with `as_udf_result_*`
  and fails with `as_udf_fail_str("message")`, which returns the 1 to return (no format: the message is
  taken as it is; `as_udf_log_str` is the same for the log). A stream UDF exports
  `<name>_record` and `<name>_end`; the whole stream shares one instance, so state is plain statics.
- Every read is an `as_udf_value`: `as_udf_rec_bin("v", &v)` for a bin; `as_udf_args(args, argc, out, cap)` for
  the arguments in one import, as many as `out` holds: a UDF takes at most 128 (`AS_UDF_MAX_ARGS`), and a
  function checks `argc` against what it declares and fails for more; `as_udf_bytes(&v, buf, cap)` for the
  raw bytes of a string, blob, GeoJSON or HLL (`AS_UDF_TYPE_MISMATCH` for anything else);
  `as_udf_cursor_enter_at` / `as_udf_cursor_values` to walk a collection 128 values per import.
- A bin of a known kind has a reader of its own: `as_udf_rec_int("n", &n)`, `as_udf_rec_double`,
  `as_udf_rec_bool`, `as_udf_rec_string("name", buf, cap)`, `as_udf_rec_blob` (only a blob; the raw bytes of
  any byte kind are `as_udf_rec_bytes`), `as_udf_rec_list` and `as_udf_rec_map`. Each answers a status:
  `AS_UDF_OK` with the value, `AS_UDF_NOT_FOUND` for an absent bin, `AS_UDF_TYPE_MISMATCH` for a bin of
  another kind. `as_udf_rec_int_or("n", 0, &n)`, `as_udf_rec_double_or` and `as_udf_rec_bool_or` put a
  default in for an absent bin only. `as_udf_as_int(&v, &n)` and its family read a value the same way. `as_udf_rec_has("n")` says whether the bin is there, `as_udf_rec_get("n", &v)` gives it unless it
  is nil, and `as_udf_rec_key(buf, cap)` gives the record's user key. An event's record has the same readers
  under `as_udf_event_*`, taking the record first.
- Everything about the record of the call is `as_udf_rec_*`: `as_udf_rec_bin` reads a bin,
  `as_udf_rec_set_int` and its family stage one, `as_udf_rec_remove(name, len)` removes one,
  `as_udf_rec_generation`, `as_udf_rec_ttl`, `as_udf_rec_key_type` and the rest read its metadata.
  `as_udf_rec_save()` writes what was staged, creating the record when it does not exist, and
  `as_udf_rec_delete()` deletes the record. An event's records are read with `as_udf_event_*` instead.
- Collections are built on the host: `as_udf_map_new`, `as_udf_list_new`, filled one value at a time
  (`as_udf_list_push_int`, `_double`, `_bool`, `_nil`, `_string`, `_blob`, `_geojson`, `as_udf_list_push_value`
  for any kind; `as_udf_map_put_string_value` and `as_udf_map_put_int_value` for an entry of any kind), none
  of which takes a handle, or in a batch of guest values with `as_udf_map_put_entries` /
  `as_udf_list_push_values`. The `as_udf_val_*` values each take one of the call's 256 handles for good: not
  for a loop. `as_udf_rec_set_value` stages a value of any kind into a bin.
  `as_udf_map_contains(handle, "key")` asks the host whether a map being built has a key, and
  `as_udf_list_at(&list, index)` reads one element of a stored list without a cursor.
- `AS_UDF_SIGNATURE(name, "fn name(a: int) -> value")` exports the signature a client can read with an
  execute of `__udf_sig_name`, as the Rust macros do automatically.

## Events

`AS_UDF_EVENT_XDR_MERGE(name) { ... }` declares an `xdr-merge` handler and its signature export. Inside,
`as_udf_event_decode(args, argc, &current, &incoming)` fills the two `as_udf_event_record`s, the record as
it is on this cluster and the one that arrived over XDR, each with its metadata (`generation`,
`last_update_time`, `ttl`, `source_id`), its `bin_names` and `deleted_bins`, its per-bin last-update times
and the facts that are about it (`incoming.is_delete`, `incoming.is_partial`, `incoming.lost_bins`,
`current.is_tombstone`, and `uses_bin_convergence` on either). `deleted_bins` and `lost_bins` are a list of names when
there are some and of kind `AS_UDF_ABSENT` (not an empty list) when there are none;
`as_udf_list_count(&incoming.lost_bins)` answers the count for either shape. `as_udf_event_bin(&current, "visits", &v)`
reads a bin of one record,
`as_udf_event_incoming_is_newer(&current, &incoming, "visits")` orders the two the way the server's bin
convergence does,
`as_udf_cmp_stored(&a, &b, &order)` compares two values' stored bytes for a data-only tie-break (a status,
and the order apart from it), and
`as_udf_resolve_record(map, -1)` (or `_keep_local`, `_take_remote`, `_policy`, `_delete`) answers.
`examples/resolver/resolver.c` is the account merge the Rust and Go SDKs also carry; the client test holds
the three to the same answer. `xdr-merge` is the one event there is. The contract and the rules a handler
must keep (pure, convergent) are in the Rust SDK's README under "Events".

## The examples

`examples/vector/vector.c` is an exact top-k over packed `f32` vectors - the same module as the Rust and Go
ones, step for step, so the three can be timed against each other. `make` builds it;
`tests/src/udf_vector.rs` in the client asserts the three agree with Lua and with a local computation, and
times them. `make` also builds `examples/resolver/resolver.c`, the `xdr-merge` handler above.
