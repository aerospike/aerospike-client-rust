# aerospike-udf-sdk-wasm-cpp

Write Aerospike WASM UDFs in C++ and compile them with clang for the server's `mod-wamr` engine. One
header, `include/aerospike_udf.hpp`, over the C SDK's `aerospike_udf.h`: the natives and the contract's
constants come from the C header, and this one adds the typed tier, `udf::Record`, `Value`, `Map`, `List`,
range-for over stored collections, `Result` and `Error` instead of status codes, the records and the
answer of an `xdr-merge`, `Pair` and `Both` for a merge, and the `UDF_*` macros that derive the exports and their
signatures from a function's parameter types, as the Rust SDK's macros do. The Rust SDK's documentation
is the reference for the contract.

```cpp
#include "aerospike_udf.hpp"

UDF_FUNCTION(increment, udf::Result<int64_t>, udf::Record& rec, int64_t by)
{
	int64_t next = UDF_TRY(rec.int_or("n", 0)) + by;

	UDF_TRY(rec.set("n", next));
	UDF_TRY(rec.save());

	return next;
}
```

The record the UDF was called on is its first parameter. It is not an argument of the call: a client
passes only `by`, and the signature the module exports is `fn increment(by: int) -> value`.

```sh
clang -x c++ -std=c++20 --target=wasm32 -O3 -nostdlib -nostdinc++ -ffreestanding -fno-exceptions \
    -fno-rtti -fvisibility=hidden -Wl,--no-entry \
    -I../wasm-c/include -Iinclude -o increment.wasm increment.cpp
```

Freestanding C++20: no standard library, no exceptions, no RTTI, no entry point, and nothing allocated.
The header brings its own `Result`, `Optional`, `StrView`, `Text<N>` and type traits, so a module needs
no `operator new`; the C header provides the `memcpy` and `memset` the compiler emits calls to.
`-fvisibility=hidden` keeps everything but the attributed exports out of the export table. Add
`make SIMD=1` (that is, `-msimd128`) for a kernel that should vectorise; off by default, since a server whose
WASM tier has no SIMD (Fast JIT, the x86-64 default) refuses a module built with it. The one extension used is the statement expression behind
`UDF_TRY`, which clang and gcc provide. Register the `.wasm` with `UdfLang::Wasm`.

**The stack.** `Text<N>` and `Bytes<N>` hold their bytes inline, so a parameter or local of one is N bytes of
the wasm stack, and the generated export holds every parameter at once plus a copy on the way into the
function. clang gives a module 64 KB of stack, which a single `Text<65536>` parameter already overflows; an
overflow is not caught at build time and traps the call (or, without a guard, overwrites the module's
statics). Add up the capacities of the largest function's parameters and locals, double it, leave about
16 KB for the SDK, and link with `-Wl,-z,stack-size=N` for that much: a function with two `Text<65536>`
parameters wants about 512 KB, and `examples/apitest`, which has several, links with 4 MiB (see the
Makefile). Keep large buffers in statics where the function allows it; a bigger stack makes every
instance's linear memory bigger.

**Errors.** Every failure is a `udf::Error` with the host's `status` (`AS_UDF_NOT_FOUND`,
`AS_UDF_TYPE_MISMATCH`, `AS_UDF_REFUSED`, `AS_UDF_ERR`, or a needed-length status for a buffer too small)
and a message. A message the SDK builds lives in one of eight static buffers used in turn, so it is not
overwritten by the next error: it stays readable across further SDK calls until eight more messages have been
built. Keep the status, or copy the text, to hold an error longer than that.

## The macros

- **`UDF_FUNCTION(name, ReturnType, params...)`** `{ body }`: a record UDF. The parameters are the
  arguments, decoded by type (`int64_t`/`int32_t` → `int`, `double`, `bool`, `udf::Text<N>` → `str[N]`,
  `udf::Bytes<N>` → `bytes[N]`, `udf::Value` → `any`, `udf::Optional<T>` for one that may be absent or nil,
  `udf::Args` last → `any...`), and the return, a `udf::Result<T>` or a plain `T` (`int64_t`, `double`, `bool`,
  `StrView`, `Text<N>`, `Value`, `Map`, `List`, `void`), is the result. The signature text is built at
  compile time from the types and the stringised parameter names. A missing, mismatched, too-long or
  surplus argument fails the call before the body runs, naming the argument; a returned error fails it
  with its message. Before the arguments the function may take **the record**: `udf::Record& rec` to write
  it, `const udf::Record& rec` to read it, nothing when it does not touch the record. The export hands it
  in; it is not decoded, not counted against the arguments and not in the signature.
- **`UDF_STREAM_RECORD(name, ret, params...)`** then **`UDF_STREAM_END(name, ret, params...)`**: a stream
  UDF, exported as `name_record` and `name_end` with the signature `stream name { record(...) end(...) }`;
  state in statics. The record half may take `const udf::Record& rec` first, and only const: a stream's
  record takes no write, and `udf::Record&` there does not compile. The end half has no record.
- **`UDF_ON_XDR_MERGE(name)`**: an `xdr-merge` event handler, with the event's two parameters under
  fixed names (`current`, the record as it is on this cluster, and `incoming`, the one that arrived over
  XDR) and returning a `udf::Result<udf::Resolution>`: `keep_local()`, `take_remote()`, `policy()`,
  `delete_record()`, `expunge()` or `record(bins)`. `delete_record()` leaves a durable tombstone, so a
  late write cannot bring the record back; `expunge()` removes it outright. Neither is shipped to the
  other cluster. No answer carries a TTL: the merged record takes the later of the two records'
  expirations. The two records are read-only `udf::EventRecord`s, whose C
  struct is private: everything about them is a method.
- **The raw form**: `UDF_FUNCTION_RAW(name, "(params)")` and `UDF_STREAM_RAW(name, "(record)", "(end)")`
  declare `(int32_t args, int32_t argc)` functions exported as they are, for a hot path that reads its
  arguments once and keeps them, as `examples/vector` does.

## Shape of the API

- **`udf::Record`**: the record the UDF was called on, with everything about it as a method. Bins are
  addressed by name; there is no bin object.
  - Reading (const): `get(name)` as an `Optional<Value>`, nothing when the bin is absent or nil;
    `bin(name)` for the bin that must be there, a `Result<Value>` whose error names the bin, chained with
    the value's own reader when the kind must hold too (`UDF_TRY(UDF_TRY(rec.bin("n")).as_int())`); the
    typed readers `as_int(name)`, `as_double`, `as_bool`, `as_string<N>`, `as_blob<N>` (only a blob) and
    `as_bytes<N>` (the raw bytes of a string, blob, GeoJSON or HLL), each a `Result<Optional<T>>`: empty
    when the bin is absent or nil, the value, or an error of status `AS_UDF_TYPE_MISMATCH` when it holds
    another kind (nothing is widened); `int_or(name, 0)`, `double_or`, `bool_or`, a `Result<T>` whose
    default stands in for an absent bin only; `udf::required(reader, "msg")` for a value that must be
    there; `has(name)`; `floats_into`; `list(name)` / `map(name)`, a `Result<Optional<ListRange>>`
    (`MapRange`) that tells absent, another kind and empty apart and gives the stored value back with
    `value()`.
  - Writing (not const): `set(name, value)` overloads for every kind, `set_blob`, `set_geojson`,
    `set_ints`, `set_doubles`, and `remove(name)` to remove a bin. There is no `set_nil`: a bin cannot hold
    nil, and `set(name, udf::Value::nil())` removes it as `remove` does. Writes are staged until `save()`;
    the two bulk list writes take effect at once.
  - Metadata (const): `exists`, `generation`, `ttl`, `last_update_time`, `size`, `bin_count`, `set_name`,
    `key(buf, cap)` (the user key as a `Value`, over `key_type` / `key_int` / `key_bytes`), `digest`; and
    `set_ttl`, which is a write.
  - Lifecycle: `save()` writes what was staged and creates the record when it does not exist;
    `delete_record()` deletes it, with no save. `delete` is a C++ keyword, and the name keeps the verb the
    other SDKs use while staying clear of `remove(name)`, which removes one bin.
  - `const udf::Record&` is the read-only record. A `Record` is made only by the SDK and cannot be copied,
    so a function handed a const one has no way to write.
- **`udf::ReadRecord<R>`**: the reading methods above, shared by `udf::Record` and `udf::EventRecord`, so
  a helper written as `template <class R> ... (const udf::ReadRecord<R>& rec)` reads from the record of a
  call and from either side of an event. `examples/counter` loads its table that way.
- **`udf::Value`**: the 16-byte value every read is, with `kind()`, the `is_*` tests, `as_int()`,
  `as_double()`, `as_bool()`, `as_string<N>()`, `as_blob<N>()`, `as_bytes<N>()` (the same three
  outcomes), `bytes_into()`, `floats_into()`, `cmp_stored()` (a `Result<int32_t>`: exactly -1, 0 or 1, or a
  type-mismatch error for a value that is not stored), `is_geojson()` and `is_hll()` among the tests
  (`is_null()` is not `is_nil()`: it is true of a nil and of an absent value alike); for a
  stored collection `size()`, `empty()`, `get(key)` and `contains(key)` on a map, `at(index)` on a list
  (one import, no walk); and **`list()` / `map()`** as ranges: `for (udf::Value v : v.list())`,
  `for (udf::Entry e : v.map())`, 32 per import, the cursor released when the loop ends or breaks.
- **`udf::Map` and `udf::List`**: collections built on the host, with chainable `put` / `add` overloads for
  every kind, string and integer keys of any length, `put_nil`, `put_geojson`, `put_all` / `add_all` by
  position; no put or add takes a handle, so a loop builds as many elements as the call's memory allows;
  the first refusal is kept and answered by `error()` / `result()`. Each owns its handle and gives it back
  when it is destroyed (or at `close()`); they move and do not copy, and what one was put into, added to,
  set as a bin or returned as keeps it, since the host holds its own reference. A call is bounded by its
  memory on the server, not by a count of handles, so a loop that builds a collection per iteration holds
  one at a time. `value()` names the handle while the collection lives; `udf::Value::handle(c.release())`
  is a Value that outlives it. `size()` and `empty()` on both, and
  `Map::contains(key)` for a string or an integer key, answered by the host for the map being built.
  Written to a bin, returned as the result, nested, or handed back from a handler.
- **Strings**: `udf::StrView` (a pointer and a length over bytes the caller keeps alive) and
  `udf::Text<N>` (an inline buffer), both comparing bytewise; nothing allocates.
- **What is not about the record** stays in the namespace: `udf::now`, `udf::log`, `udf::fail`,
  `udf::result`.
- **The `xdr-merge` event**: `udf::EventRecord` for each of the two records the event hands over (the
  reading methods of `ReadRecord`; `generation()`, `last_update_time()`, `source_id()`,
  `is_incoming()`;
  `bin_names()`, `deleted_bins()`, `deleted_any()`; `bin_last_update_time(name)` and `bin_source_id(name)`;
  `with_bins(out)` to keep the rest of a record under a few decided bins) and `udf::Resolution` for the
  answer. What the server says of the merge is asked of the
  record it is about: `incoming.is_delete()`, `incoming.is_partial()`,
  `current.is_tombstone()`. The server gives a time and a source for every bin of both sides. The merge
  bin by bin goes through
  `udf::Pair pair = udf::Pair::of(current, incoming);`: its typed readers (`as_int`, `as_double`,
  `as_bool`, `as_string<N>`, `as_blob<N>`, `list`, `map`) each give a `Result<Both<T>>`, an error when a
  side holds another kind, and `bin` a `Both<Value>`, with the two sides
  as `current` and `incoming` and `incoming_is_newer`, `newer`, `older`, `ordered`, `current_or_incoming`,
  `incoming_or_current`, `max`, `min`, `sum` (each an `Optional`, nothing when neither side has the bin; the
  sum takes a missing side as 0 and wraps on overflow), `merge`;
  `pair.incoming_is_newer(name)` and `pair.with_newer(out)` (which never brings back a bin the newer side
  deleted) are asked of the pair itself. A function taking `udf::Args` takes at most 128 arguments in all;
  a call with more fails. `examples/resolver` is the worked example, held to the same behaviour as the
  other languages' by the client tests.
- **The low-level tier**: the whole C header, under the C++ one. `udf::Map::wrap` / `List::wrap` bridge a
  handle to the typed collections, and `udf::Value` is layout-compatible with `as_udf_value`.

## The examples

- `examples/vector`: the exact top-k over packed float vectors that the Rust, C, Go and Java SDKs also
  carry, bit for bit: the C module's kernels under `UDF_STREAM_RAW` and `UDF_FUNCTION_RAW`.
- `examples/resolver`: the account merge on `xdr-merge`, over `Pair` and `Both`.
- `examples/counter`: the smallest CRDT, a grow-only counter on `xdr-merge`; read it first. The client test
  `cpp_generated_exports_decode_arguments` drives its exports through every rejection.
- `examples/pncounter`: the counter that can go down, two grow-only maps (`inc`, `dec`) merged map by map;
  its `Table` is `static` in every function so the 64 entries stay off the stack.
- `examples/twopset` and `examples/lwwset`: steps 3 and 4 of the book's CRDT tutorial, a set with
  removals (two sorted `Text<32>` arrays merged by union) and a set with re-adds (two maps of `udf::now()`
  stamps merged by the later stamp); both return `udf::Result<udf::List>` of members.
- `examples/registers`: a record whose bins are last-writer-wins registers stamped by the server: the
  handler asks each side for `bin_last_update_time(name)` and
  `bin_source_id(name)`, keeps the later write bin by bin (a delete included, from `deleted_bins()`), and
  merges the one bin that is not a register, `seen`, by the larger number.
- `examples/crdt`: the active-active CRDT of the design note, rule for rule the same as the Rust one, held
  to the same nine dry-run scenarios by `tests/src/events.rs`.
- `examples/udf`: the SDK's own fixture, one function per corner of the record API, which
  `cpp_sdk_covers_the_rust_surface`, `cpp_udf_saves_removes_and_deletes` and
  `cpp_udf_indexes_a_list_and_asks_a_built_map` in `tests/src/udf_cpp.rs` hold to the Rust fixture's
  expectations, and `stop_probe`, which `cpp_unreadable_collections_stop_the_call` uses to check that a walk
  or lookup the host cannot serve stops the call. Not an example to copy.

`make` builds every example (`make CLANG=clang-18` for a specific clang; `wasm-ld` from lld must be on the
path); `make fixtures FIXTURES=<dir>` copies them to `<dir>` as `*_cpp.wasm`. Apple's clang targets
wasm32 but ships no `wasm-ld`: on a Mac install `llvm` or `lld` from Homebrew, or build in a Linux
container as this repository's tests do (`podman cp` this directory and the C SDK's, `../wasm-c`, to the
`asd-build-wasm` container as siblings, which is where the Makefile looks for the C header, and run clang
there).
