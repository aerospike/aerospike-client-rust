# aerospike-udf-sdk-wasm-cpp

Write Aerospike WASM UDFs in C++ and compile them with clang for the server's `mod-wamr` engine. One
header, `include/aerospike_udf.hpp`, over the C SDK's `aerospike_udf.h`: the natives and the contract's
constants come from the C header, and this one adds the typed tier, `udf::Bin`, `Value`, `Map`, `List`,
range-for over stored collections, `Result` and `Error` instead of status codes, the event records and
answers, `Pair` and `Both` for a merge, and the `UDF_*` macros that derive the exports and their
signatures from a function's parameter types, as the Rust SDK's macros do. The Rust SDK's documentation
is the reference for the contract.

```cpp
#include "aerospike_udf.hpp"

static constexpr udf::Bin N{"n"};

UDF_FUNCTION(increment, udf::Result<int64_t>, int64_t by)
{
	int64_t next = N.int_or(0) + by;

	UDF_TRY(N.set(next));
	UDF_TRY(udf::commit());

	return next;
}
```

```sh
clang -x c++ -std=c++20 --target=wasm32 -O3 -nostdlib -nostdinc++ -ffreestanding -fno-exceptions \
    -fno-rtti -fvisibility=hidden -Wl,--no-entry \
    -I../aerospike-udf-sdk-wasm-c/include -Iinclude -o increment.wasm increment.cpp
```

Freestanding C++20: no standard library, no exceptions, no RTTI, no entry point, and nothing allocated.
The header brings its own `Result`, `Optional`, `StrView`, `Text<N>` and type traits, so a module needs
no `operator new`; the C header provides the `memcpy` and `memset` the compiler emits calls to.
`-fvisibility=hidden` keeps everything but the attributed exports out of the export table. Add
`-msimd128` for a kernel that should vectorise. The one extension used is the statement expression behind
`UDF_TRY`, which clang and gcc provide. Register the `.wasm` with `UdfLang::Wasm`.

## The macros

- **`UDF_FUNCTION(name, ReturnType, params...)`** `{ body }`: a record UDF. The parameters are the
  arguments, decoded by type (`int64_t`/`int32_t` → `int`, `double`, `bool`, `udf::Text<N>` → `str[N]`,
  `udf::Bytes<N>` → `bytes[N]`, `udf::Value` → `any`, `udf::Optional<T>` for one that may be absent or nil,
  `udf::Args` last → `any...`), and the return, a `udf::Result<T>` or a plain `T` (`int64_t`, `double`, `bool`,
  `StrView`, `Text<N>`, `Value`, `Map`, `List`, `void`), is the result. The signature text is built at
  compile time from the types and the stringised parameter names. A missing, mismatched, too-long or
  surplus argument fails the call before the body runs, naming the argument; a returned error fails it
  with its message.
- **`UDF_STREAM_RECORD(name, ret, params...)`** then **`UDF_STREAM_END(name, ret, params...)`**: a stream
  UDF, exported as `name_record` and `name_end` with the signature `stream name { record(...) end(...) }`;
  state in statics.
- **`UDF_ON_BEFORE_WRITE(name)`**, `UDF_ON_BEFORE_DELETE`, ..., `UDF_ON_TIMER`, `UDF_ON_XDR_MERGE`: an event
  handler of the kind, with the kind's parameters under fixed names (`before`, `after`, `write`; `record`;
  `current`, `incoming`, `conflict`; ...) and returning `udf::Result` of the kind's answer: `Decision`
  (`accept()`, `reject(code, msg)`, `record(bins)`), `Gate`, `Expiry`, `Eviction`, `ReadResult`,
  `ShipDecision`, `Resolution` (`keep_local()`, `take_remote()`, `policy()`, `record(bins)`), or
  `Result<void>` for the kinds that answer nothing.
- **The raw form**: `UDF_FUNCTION_RAW(name, "(params)")` and `UDF_STREAM_RAW(name, "(record)", "(end)")`
  declare `(int32_t args, int32_t argc)` functions exported as they are, for a hot path that reads its
  arguments once and keeps them, as `examples/vector` does.

## Shape of the API

- **`udf::Bin`**: a bin by name, `constexpr` (`static constexpr udf::Bin N{"n"}`). Typed getters
  (`as_int()`, `as_double()`, `as_bool()`, `as_text<N>()`, `as_bytes<N>()`, `floats_into()`, each a
  `Result`; `int_or(0)` and the other `_or` forms for a default), `get()` as an `Optional<Value>`,
  `list()` / `map()` to range over; setters of every kind (`set(...)` overloads, `set_blob`, `set_geojson`,
  `set_null`, `set_ints`, `set_doubles`, `remove()`), staged until `udf::commit()`. `udf::remove_record()`
  deletes the record, `udf::set_ttl` sets its time to live.
- **`udf::Value`**: the 16-byte value every read is, with `kind()`, the `is_*` tests, `as_int()`,
  `as_double()`, `as_bool()`, `as_text<N>()`, `as_bytes<N>()`, `floats_into()`, `get(key)`,
  `cmp_stored()`, and **`list()` / `map()`** as ranges: `for (udf::Value v : v.list())`, `for (udf::Entry e :
  v.map())`, 32 per import, the cursor released when the loop ends or breaks.
- **`udf::Map` and `udf::List`**: collections built on the host, with chainable `put` / `add` overloads for
  every kind, string and integer keys, `put_null`, `put_geojson`, `put_all` / `add_all` by position; the
  first refusal is kept and answered by `error()` / `result()`. Written to a bin, returned as the result,
  nested, or handed back from a handler.
- **Strings**: `udf::StrView` (a pointer and a length over bytes the caller keeps alive) and
  `udf::Text<N>` (an inline buffer), both comparing bytewise; nothing allocates.
- **Metadata**: `udf::generation`, `ttl`, `last_update_time`, `record_size`, `bin_count`, `set_name`,
  `digest`, the user key, `exists`, `now`; `udf::log` writes to the server log.
- **Events**: `udf::EventRecord` for a record the event hands over (`get`, `as_int`, ..., `list` / `map` /
  `names` ranges, `with_bins(out)` to keep the rest of a record under a few decided bins), the `*Info`
  structs for the kind's own entry, the answer types; the merge through `udf::Conflict` (`pair()` into
  `Pair` and `Both<T>` with `newer`, `older`, `max`, `min`, `sum`, `merge`, `with_newer`;
  `involves_delete`, `lost_bins`). `examples/guard` and `examples/resolver` are the worked examples, held
  to the same behaviour as the other languages' by the client tests.
- **The low-level tier**: the whole C header, under the C++ one. `udf::Map::wrap` / `List::wrap` bridge a
  handle to the typed collections, and `udf::Value` is layout-compatible with `as_udf_value`.

## The examples

- `examples/vector`: the exact top-k over packed float vectors that the Rust, C, Go and Java SDKs also
  carry, bit for bit: the C module's kernels under `UDF_STREAM_RAW` and `UDF_FUNCTION_RAW`.
- `examples/guard`: before-write, before-delete, before-truncate and timer handlers.
- `examples/resolver`: the account merge on `xdr-merge`, over `Pair` and `Both`.
- `examples/counter`: the smallest CRDT, a grow-only counter on `xdr-merge`; read it first. The client test
  `cpp_generated_exports_decode_arguments` drives its exports through every rejection.
- `examples/pncounter`: the counter that can go down, two grow-only maps (`inc`, `dec`) merged map by map;
  its `Table` is `static` in every function so the 64 entries stay off the stack.
- `examples/twopset` and `examples/lwwset`: steps 3 and 4 of the book's CRDT tutorial, a set with
  removals (two sorted `Text<32>` arrays merged by union) and a set with re-adds (two maps of `udf::now()`
  stamps merged by the later stamp); both return `udf::Result<udf::List>` of members.
- `examples/crdt`: the active-active CRDT of the design note, rule for rule the same as the Rust one, held
  to the same nine dry-run scenarios by `tests/src/events.rs`.
- `examples/udf`: the SDK's own fixture, one function per corner of the record API, which
  `cpp_sdk_covers_the_rust_surface` in `tests/src/udf_cpp.rs` holds to the Rust fixture's expectations. Not
  an example to copy.

`make` builds every example (`make CLANG=clang-18` for a specific clang; `wasm-ld` from lld must be on the
path); `make fixtures` copies them to `tests/fixtures/wasm/` as `*_cpp.wasm`. Apple's clang targets
wasm32 but ships no `wasm-ld`: on a Mac install `llvm` or `lld` from Homebrew, or build in a Linux
container as this repository's tests do (`podman cp` this directory and the C SDK's to the
`asd-build-wasm` container and run clang there).
