# C++

The C++ SDK, `aerospike-udf-sdk-wasm-cpp`, is one header, `include/aerospike_udf.hpp`, over the C one: the
natives and the contract's constants come from `aerospike_udf.h`, and the C++ header adds the typed tier,
`Bin`, `Value`, `Map`, `List`, range-for over stored collections, `Result` and `Error` instead of status
codes, the event records and answers, and the `UDF_*` macros that derive the exports and their signatures
from a function's parameter types, the way the Rust SDK's macros do. A module is a freestanding C++20
file compiled with clang for `wasm32`: no standard library, no exceptions, no RTTI, and nothing allocated.

## A module from nothing

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
    -fno-rtti -fvisibility=hidden -Wl,--no-entry -I<c sdk>/include -I<c++ sdk>/include \
    -o increment.wasm increment.cpp
```

That is the whole module. `UDF_FUNCTION(name, ReturnType, params...)` is the Rust `export!`: it defines the
function with the parameter list given, the exported `increment(args, argc)` that decodes the call's
arguments into those parameters and delivers the return as the result, and the `__udf_sig_increment`
export whose text, `fn increment(by: int) -> value`, is built at compile time from the parameter types and
names (the names are read out of the stringised parameter list by a `constexpr` parser). Nothing runs
before `main` would, since there is no `main`: every object the SDK makes is `constexpr`.

`udf::Bin` is a bin of the record by name, kept as a `constexpr` constant; `int_or(0)` reads it as an
integer, 0 when absent, and `set` stages a write that `udf::commit()` makes, creating the record if it
does not exist. Every call that can fail returns a `udf::Result<T>`, and `UDF_TRY(expr)` is the Rust `?`:
it yields the value or returns the error from the function, which the generated export delivers as the
failure the client sees. (`UDF_TRY` is a statement expression, a clang and gcc extension.)

`clang` with `wasm-ld` (lld) is all it takes; Apple's clang ships no `wasm-ld`, so on a Mac install `llvm`
or `lld` from Homebrew, or build in a Linux container as this repository's tests do. `make SIMD=1` adds `-msimd128` for a
kernel that should vectorise. `-fvisibility=hidden` keeps everything but the attributed exports out of
the module's export table.

## Parameters and results

The function's parameters are the UDF's arguments, in order, and their C++ types decide both how an
argument is decoded and what the signature says:

| C++ | Signature | Takes |
| --- | --- | --- |
| `int64_t`, `int32_t` | `int` | an integer (`int32_t` must fit) |
| `double` | `double` | a double, or an integer widened |
| `bool` | `bool` | a boolean |
| `udf::Text<N>` | `str[N]` | a string of at most N bytes, inline, rejected before the body when longer |
| `udf::Bytes<N>` | `bytes[N]` | a string or a blob of at most N bytes, as its bytes |
| `udf::Value` | `any` | anything, nil included, as the 16-byte value |
| `udf::Optional<T>` | `name?: type` | an argument that may be absent or nil, empty then |
| `udf::Args`, last | `any...` | every remaining argument |

A call that leaves a required argument out, passes one of another kind, passes a string longer than its
`Text<N>`, or passes more arguments than there are parameters, fails before the function runs, with a
message that names the argument (`increment: argument 1 (site) is 40 bytes, at most 32`, `increment takes
at most 2 arguments, got 3`), the same way the Rust macro rejects a call. A parameter list may not contain
a comma inside a template argument list, since the macro splits on commas.

The return type is the result: `udf::Result<T>` or a plain `T`, where `T` is `int64_t`, `double`, `bool`,
`udf::StrView` or `udf::Text<N>` (a string), `udf::Value` (anything; `udf::Value::geojson(doc)` is a GeoJSON
result), `udf::Map` or `udf::List` (a built collection), or `void` (nil). An error returned through the
`Result` fails the call with its message.

**The raw form.** `UDF_FUNCTION_RAW(name, "(bin: str, k?: int)") { ... }` declares `(int32_t args, int32_t
argc)` returning the status, exported as it is, with the signature text given; the body decodes its
arguments itself with the C header's `as_udf_args`. This is the form for a hot path that wants to read its
arguments once and keep them, as the vector example does.

## Bins

A `udf::Bin` is a bin of the record in hand, by name, `constexpr` so it costs nothing to make. Its getters
read the bin as the type expected: `as_int()`, `as_double()`, `as_bool()`, `as_text<N>()`, `as_bytes<N>()`,
`floats_into(float*, n)`, each a `Result` with an error naming the bin when it is absent or of another
kind; the `Or` forms, `int_or(0)`, `double_or`, `bool_or`, answer the default for absent and nil. `get()`
is the bin as an `Optional<Value>`, `exists()` says so alone, and `list()` and `map()` give a range over a
collection bin. The setters stage a write of any kind: `set(int64_t)`, `set(double)`, `set(bool)`,
`set(StrView)`, `set(const Value&)` (a value read from the record, by position, keeping its kind),
`set(const Map&)`, `set(const List&)`, `set_blob(ptr, n)`, `set_geojson(doc)` (kept as the geospatial kind,
not a string), `set_null()`, `set_ints(ptr, n)` and `set_doubles(ptr, n)` (a list in one import, no value
built per element), and `remove()`, each a `Result<void>`. `udf::commit()` writes them;
`udf::remove_record()` deletes the record, and `udf::set_ttl(seconds)` sets the time to live the commit
carries.

## Values and ranges

Every read is a `udf::Value`, the C struct with accessors: `kind()` (a `udf::Kind`: `Int`, `Str`, `List`,
`Map`, `GeoJson`, `Absent`, ...), the `is_int()` ... `is_null()` tests, `as_int()`, `as_double()`,
`as_bool()`, `as_text<N>()`, `as_bytes<N>()`, `floats_into()`, each a `Result`, and `get(key)` for an
entry of a stored map by string or integer key. A stored string or collection is held by position, never
copied until asked.

`list()` and `map()` on a `Value` or a `Bin` are ranges, the Rust SDK's `iter()` and `entries()`: a list
ranges over its values, a map over `udf::Entry{key, value}`, 32 per import, and the cursor is released
when the loop ends or breaks (the iterator owns it). Collections nest, as long as the inner walk ends
before the outer one steps on.

```cpp
static constexpr udf::Bin L{"l"};
static constexpr udf::Bin COUNTS{"counts"};

UDF_FUNCTION(sum_list, udf::Result<int64_t>)
{
	int64_t total = 0;

	for (udf::Value v : L.list()) {
		total += UDF_TRY(v.as_int());   // fails the call on anything but an int
	}

	return total;
}

UDF_FUNCTION(max_count, udf::Result<int64_t>)
{
	int64_t best = 0;

	for (udf::Entry e : COUNTS.map()) {
		if (udf::Result<int64_t> n = e.value.as_int(); n && *n > best) {
			best = *n;   // e.key is the entry's key
		}
	}

	return best;
}
```

Strings never allocate: `Text<N>` is an inline buffer with a length, `StrView` a pointer and a length over
bytes the caller keeps alive (a literal, a `Text`), and both compare with `==` and `<` bytewise. A UDF that
needs a string from the host reads it into a `Text<N>` of the size it allows.

## Building collections

`udf::Map m(capacity)` and `udf::List l(capacity)` build a map or list on the host, to be written to a
bin, returned as the result, nested, or handed back from an event handler. `put(key, v)` and `add(v)` are
overloaded for every kind (`int64_t`, `double`, `bool`, `StrView`, a literal, `const Value&`, `const Map&`,
`const List&`); `put` takes a `StrView` or an `int64_t` key; `put_null`, `add_null`, `put_blob`,
`put_geojson`, `add_geojson`; `put_all(stored)` and `add_all(stored)` copy a stored collection by position;
`put_entries` and `add_values` take an array of values in one import. They chain, each is one import, and
the first refusal is kept and answered by `error()` or `result()`, which a UDF returning the collection
should check with `UDF_TRY(out.result())`.

```cpp
UDF_FUNCTION(describe, udf::Result<udf::Map>)
{
	udf::Map out(4);

	out.put("generation", udf::generation())
	    .put("bins", udf::bin_count())
	    .put("tags", udf::List(2).add("a").add("b"));

	UDF_TRY(out.result());

	return out;
}
```

## The record's metadata

`udf::generation()`, `ttl()`, `last_update_time()`, `record_size()`, `bin_count()`, `set_name()` (a `Text<64>`,
empty when the record is in no set), `exists()`, `digest(buf, cap)`, `key_type()` / `key_int()` /
`key_bytes(buf, cap)` for the user key, and `now()`, the server's clock, which an event handler may not
read.

## The low-level tier

The C header is there, in full, under the C++ one: `as_udf_bin`, `as_udf_cursor_enter_at` and the window
calls, `as_udf_map_new` and the `as_udf_map_put_*` / `as_udf_list_push_*` calls over `int32_t` handles, the
`as_udf_set_*` and `as_udf_result_*` calls, every one returning a status. The vector example is written on
it, because it reads its arguments once per stream and scores from a static `float[]`; nothing else
needs to be. `udf::Map::wrap(handle)` and `udf::List::wrap(handle)` bridge a handle built down there to the
typed collections, and `Value` is layout-compatible with `as_udf_value`, so the two tiers mix freely.

## What is different about C++

- **No standard library.** The module is built with `-nostdlib -nostdinc++`; the header brings its own
  `Result`, `Optional`, `StrView` and type traits, and nothing in it allocates. There is no `std::string`;
  `Text<N>` and `StrView` are the strings.
- **Errors, not exceptions.** `-fno-exceptions` is the build; a `Result` carries the error and `UDF_TRY`
  propagates it. `fail(error)` is what the export calls at the end.
- **Compile-time signatures.** The parameter names in the signature come from the macro's stringised
  parameter list, parsed by a `constexpr` function; the types come from templates. A wrong parameter
  type is a compile error on the `UDF_FUNCTION` line.
- **Coverage.** The SDK carries the Rust SDK's whole record surface; `examples/udf` is its fixture, one
  function per corner, and `cpp_sdk_covers_the_rust_surface` in `tests/src/udf_cpp.rs` holds it to the
  expectations of the Rust fixture's tests.

## A module to read: the vector top-k

`examples/vector/vector.cpp` is the C module's kernels under the C++ macros: `UDF_STREAM_RAW(topk, ...)`
declares the two halves in the raw form, since the arguments are read once per stream and kept, and
`UDF_FUNCTION_RAW(vec_distance, ...)` the record UDF; the signatures are the macros' text. It is
bit-identical with the Rust, C, Go and Java modules. The record half:

```cpp
{{#include ../../../aerospike-udf-sdk-wasm-cpp/examples/vector/vector.cpp:388:407}}
```

## API reference

Generated from `aerospike-udf-sdk-wasm-cpp/include/aerospike_udf.hpp`: the public types, their methods and
the macros of the record UDF surface, with their comments. The C natives underneath are in the C chapter.

{{#include ../api/cpp-record.md}}
