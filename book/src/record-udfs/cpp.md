# C++

The C++ SDK, `aerospike-udf-sdk-wasm-cpp`, is one header, `include/aerospike_udf.hpp`, over the C one: the
natives and the contract's constants come from `aerospike_udf.h`, and the C++ header adds the typed tier,
`Record`, `Value`, `Map`, `List`, range-for over stored collections, `Result` and `Error` instead of status
codes, the event records and answers, and the `UDF_*` macros that derive the exports and their signatures
from a function's parameter types, the way the Rust SDK's macros do. A module is a freestanding C++20
file compiled with clang for `wasm32`: no standard library, no exceptions, no RTTI, and nothing allocated.

## A module from nothing

```cpp
#include "aerospike_udf.hpp"

UDF_FUNCTION(increment, udf::Result<int64_t>, udf::Record& rec, int64_t by)
{
	int64_t next = rec.int_or("n", 0) + by;

	UDF_TRY(rec.set("n", next));
	UDF_TRY(rec.save());

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

`rec` is the record the function was called on. `int_or("n", 0)` reads bin `n` as an integer, 0 when
absent, and `set` stages a write that `rec.save()` makes, creating the record if it does not exist. The
client passes only `by`. Every call that can fail returns a `udf::Result<T>`, and `UDF_TRY(expr)` is the Rust `?`:
it yields the value or returns the error from the function, which the generated export delivers as the
failure the client sees. (`UDF_TRY` is a statement expression, a clang and gcc extension.)

`clang` with `wasm-ld` (lld) is all it takes; Apple's clang ships no `wasm-ld`, so on a Mac install `llvm`
or `lld` from Homebrew, or build in a Linux container as this repository's tests do. `make SIMD=1` adds `-msimd128` for a
kernel that should vectorise. `-fvisibility=hidden` keeps everything but the attributed exports out of
the module's export table.

## The record

The record a function was called on is its first parameter: `const udf::Record& rec` to read it,
`udf::Record& rec` to write it too. A function that does neither leaves the parameter out. It is not one
of the call's arguments: a client does not pass it, the arity check does not count it, and the signature
the module exports does not list it. It comes first and by reference. Anything else is a compile error
on the `UDF_FUNCTION` line.

Everything about the record is a method on it. The reading methods are `const` and the writing ones are
not, so a function that takes `const udf::Record&` cannot change the record. Only the SDK makes a
`Record`, and it cannot be copied, so a read-only record stays read-only. What is left in the `udf`
namespace is what is not about the record: `now()`, `log`, `fail`, `result_of`, the `Map` and `List`
builders, timers.

`udf::Record` and the `udf::EventRecord` an event handler is given both derive from `udf::ReadRecord`,
which carries the reading methods. A helper written against it reads a bin the same way in a UDF and in
a handler:

```cpp
template <class R>
static int64_t visits_of(const udf::ReadRecord<R>& rec)
{
	return rec.int_or("visits", 0);
}
```

## Parameters and results

After the record, the function's parameters are the UDF's arguments, in order, and their C++ types
decide both how an argument is decoded and what the signature says:

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

## Reading

Bins are addressed by name; there is no bin object. `rec.bin("name")` returns a `Result<Value>` whose
error names the bin: the form for a bin that must be there. Chain the value's own reader when the kind
must hold too, `UDF_TRY(UDF_TRY(rec.bin("balance")).as_int())`. The typed readers are for a bin that may
not be there: `rec.as_int("name")`, `as_double`, `as_bool`, `as_text<N>`, `as_bytes<N>` and the untyped
`get` each return an `Optional`, empty for a bin that is absent or nil, and for the typed ones also for a
bin of another kind. The `Or` forms take a default instead: `int_or("name", 0)`, `double_or`, `bool_or`.
`has("name")` says whether the bin is there, `floats_into("name", ptr, n)` reads a blob of floats into
an array, and `list("name")` and `map("name")` give a range over a collection bin, empty when the bin is
missing or of another kind.

```cpp
UDF_FUNCTION(withdraw, udf::Result<int64_t>, udf::Record& rec, int64_t amount)
{
	// An error when the bin is absent, and another when it is not an integer.
	int64_t balance = UDF_TRY(UDF_TRY(rec.bin("balance")).as_int());

	if (amount > balance) return udf::error(AS_UDF_REFUSED, "insufficient funds");

	UDF_TRY(rec.set("balance", balance - amount));
	UDF_TRY(rec.save());

	return balance - amount;
}
```

## Writing

`rec.set("name", value)` stages a write. It is overloaded for every kind: `int64_t`, `double`, `bool`,
`StrView` or a literal, `const Map&` and `const List&` (a built collection), and `const Value&` (a value
read from a record, which goes back by position, keeping its kind). `set_blob("name", ptr, n)` stages
bytes and `set_geojson("name", doc)` a document kept as the geospatial kind, not a string.
`rec.remove("name")` stages the removal of a bin. Setting `udf::Value::nil()` does the same, since a bin
cannot hold nil, and there is no `set_null`. `rec.set_ttl(seconds)` sets the TTL of the write. Each
returns a `Result<void>`.

**Nothing staged reaches the server until `rec.save()`.** A function that returns without saving has
changed nothing, however plausible its result. `save()` creates the record when the key has none, so
there is one verb for both. A function that must not overwrite checks `rec.exists()` first; the record
is locked for the whole call, so nothing comes between the check and the save. Two details:

- A save with nothing staged is still a write. The server treats it as a touch: the generation and the
  last-update time move.
- A save with nothing staged on a key with no record makes nothing, since a record must have a bin, and
  the client is told "key not found".

`rec.delete_record()` deletes the record. It needs no save, and whatever was staged goes with the
record. The server carries it out when the call returns, so `rec.exists()` keeps its answer for the rest
of the call. The name is not `delete` because that is a C++ keyword, and not `remove` because
`remove("name")` removes one bin.

`rec.set_ints("name", ptr, n)` and `rec.set_doubles("name", ptr, n)` write an array straight into a list
particle, with no value built per element. They take effect at once, without a save.

```cpp
UDF_FUNCTION(clear_note, udf::Result<int64_t>, udf::Record& rec)
{
	UDF_TRY(rec.remove("note"));   // staged, like a set
	UDF_TRY(rec.save());

	return rec.bin_count();
}

UDF_FUNCTION(close_account, udf::Result<void>, udf::Record& rec)
{
	UDF_TRY(rec.delete_record());   // no save: the delete is the write

	return {};
}
```

## Values and ranges

Every read is a `udf::Value`, the C struct with accessors: `kind()` (a `udf::Kind`: `Int`, `Str`, `List`,
`Map`, `GeoJson`, `Absent`, ...), the `is_int()` ... `is_null()` tests, `as_int()`, `as_double()`,
`as_bool()`, `as_text<N>()`, `as_bytes<N>()`, `floats_into()`, each a `Result`. A stored string or
collection is held by position, never copied until asked.

A stored list or map answers without a walk: `size()` and `empty()`; on a map `get(key)` for an entry by
string or integer key and `contains(key)`; on a list `at(index)`, one element by its index in one import,
an empty `Optional` past the end.

`list()` and `map()` on a `Value`, and `rec.list("name")` and `rec.map("name")` on a record, are ranges,
the Rust SDK's `iter()` and `entries()`: a list ranges over its values, a map over
`udf::Entry{key, value}`, 32 per import, and the cursor is released when the loop ends or breaks (the
iterator owns it). Collections nest, as long as the inner walk ends before the outer one steps on. A
range has `size()` and `empty()` too, a list range `at(index)` and a map range `contains(key)`.

```cpp
UDF_FUNCTION(sum_list, udf::Result<int64_t>, const udf::Record& rec)
{
	int64_t total = 0;

	for (udf::Value v : rec.list("l")) {
		total += UDF_TRY(v.as_int());   // fails the call on anything but an int
	}

	return total;
}

UDF_FUNCTION(last_of, udf::Result<udf::Value>, const udf::Record& rec)
{
	udf::Value l = UDF_TRY(rec.bin("l"));

	if (l.empty()) return udf::Value::nil();

	return l.at(l.size() - 1).value_or(udf::Value::nil());   // one import, no walk
}

UDF_FUNCTION(max_count, udf::Result<int64_t>, const udf::Record& rec)
{
	int64_t best = 0;

	for (udf::Entry e : rec.map("counts")) {
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

Both have `size()` and `empty()`. `Map::contains(key)` says whether the map being built has a string or
an integer key. The host answers for the map it holds, so the check is one import and fit for a loop.

```cpp
UDF_FUNCTION(describe, udf::Result<udf::Map>, const udf::Record& rec)
{
	udf::Map out(4);

	out.put("generation", rec.generation())
	    .put("bins", rec.bin_count())
	    .put("tags", udf::List(2).add("a").add("b"));

	if (!out.contains("owner")) out.put("owner", rec.get("owner").value_or(udf::Value::nil()));

	UDF_TRY(out.result());

	return out;
}
```

## Metadata

On the record: `rec.exists()`, `rec.generation()`, `rec.ttl()` (0 for a record that does not expire),
`rec.last_update_time()`, `rec.size()` (the stored size in bytes), `rec.bin_count()`, `rec.set_name()` (a
`Text<64>`, empty when the record is in no set), `rec.key_type()` with `rec.key_int()` and
`rec.key_bytes(buf, cap)` for the user key of a record written with `send_key`, and
`rec.digest(buf, cap)`. Not on the record: `udf::now()`, the server's clock, which an event handler may
not read, and `udf::log(level, message)`.

## The low-level tier

The C header is there, in full, under the C++ one: `as_udf_rec_bin`, `as_udf_cursor_enter_at` and the window
calls, `as_udf_map_new` and the `as_udf_map_put_*` / `as_udf_list_push_*` calls over `int32_t` handles, the
`as_udf_rec_set_*` and `as_udf_result_*` calls, every one returning a status. A raw-form function is not
handed a `udf::Record`; it uses the `as_udf_rec_*` functions. The vector example is written on
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
  function per corner, and the tests in `tests/src/udf_cpp.rs` hold it to the expectations of the Rust
  fixture's tests.

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
