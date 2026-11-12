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
    int64_t n = as_udf_bin("n", &v) == AS_UDF_OK && v.kind == AS_UDF_INT ? v.payload : 0;

    as_udf_set_int("n", 1, n + 1);
    as_udf_rec_exists() ? as_udf_rec_update() : as_udf_rec_create();
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
  and fails with `as_udf_failf("message")`, which returns the 1 to return. A stream UDF exports
  `<name>_record` and `<name>_end`; the whole stream shares one instance, so state is plain statics.
- Every read is an `as_udf_value`: `as_udf_bin("v", &v)` for a bin; `as_udf_args(args, argc, out, cap)` for
  the arguments in one import; `as_udf_bytes(&v, buf, cap)` for the bytes of a string or blob;
  `as_udf_cursor_enter_at` / `as_udf_cursor_values` to walk a collection 128 values per import.
- Collections are built on the host: `as_udf_map_new`, `as_udf_list_new`, filled one value at a time or in a
  batch of guest values with `as_udf_map_put_entries` / `as_udf_list_push_values`.
- `AS_UDF_SIGNATURE(name, "fn name(a: int) -> value")` exports the signature a client can read with an
  execute of `__udf_sig_name`, as the Rust macros do automatically.

## Events

`AS_UDF_EVENT_XDR_MERGE(name) { ... }` declares an `xdr-merge` handler and its signature export. Inside,
`as_udf_event_decode(args, argc, &c)` fills an `as_udf_conflict` - both sides as `as_udf_event_record`s
with their metadata, per-bin LUTs and deletes, and `conflict_trigger` for a binding with
`trigger=conflict` - `as_udf_event_bin(&c.local, "visits", &v)` reads a bin of one side,
`as_udf_event_remote_newer(&c, "visits")` orders the two sides the way the server's bin convergence does,
`as_udf_cmp_stored(&a, &b)` compares two values' stored bytes for a data-only tie-break, and
`as_udf_resolve_record(map, -1)` (or `_keep_local`, `_take_remote`, `_policy`) answers.
`examples/resolver/resolver.c` is the account merge the Rust and Go SDKs also carry; the client test holds
the three to the same answer. The other kinds are declared with `AS_UDF_EVENT_<KIND>(name)` and read with
`as_udf_event_desc`, `as_udf_event_get_record` and the `as_udf_event_*_info` decoders; `examples/guard`
has one of each. The contract and the rules a handler must keep (pure, convergent) are in the Rust SDK's
README under "Events".

## The example

`examples/vector/vector.c` is an exact top-k over packed `f32` vectors - the same module as the Rust and Go
ones, step for step, so the three can be timed against each other. `make` builds it;
`tests/src/udf_vector.rs` in the client asserts the three agree with Lua and with a local computation, and
times them.
