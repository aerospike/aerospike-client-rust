# C++

A C++ event handler is a function defined by the `UDF_ON_XDR_MERGE` macro, with the parameters the event
hands over and returning its answer in a `udf::Result`: the C++ face of the Rust SDK's `event!`. The macro
writes the export that decodes the event into those parameters and delivers the answer, and the
`__udf_sig_<name>` export reading `event xdr-merge/1 ...`, which `event-set` checks before binding. The
parameter names are fixed (`current`, `incoming`, `conflict`), so a handler body reads them by those
names. An error returned fails the call with its message.

```cpp
UDF_ON_XDR_MERGE(merge_account)
{
	udf::Pair pair = conflict.pair();
	udf::Map out(4);

	if (udf::Optional<int64_t> visits = pair.as_int("visits").max()) {   // both sides' increments are real
		out.put("visits", *visits);
	}

	return udf::Resolution::record(pair.with_newer(out));   // every other bin from the side that wrote it later
}
```

## Records

A `udf::EventRecord` is one record the event hands over, read-only, the C struct underneath. It reads its
bins with the methods of `udf::ReadRecord`, the same ones `udf::Record` has in a record UDF: `get(name)`
is the bin as an `Optional<Value>`, empty for absent or nil; `bin(name)` is the bin that must be there, a
`Result<Value>`; `as_int(name)`, `as_double`, `as_bool`, `as_text<N>` and `as_bytes<N>` answer an
`Optional` of the type, empty for absent, nil or another kind; `int_or(name, 0)`, `double_or` and
`bool_or` take a default; `has(name)` says whether the bin is there; `list(name)` and `map(name)` give a
range over a collection bin. A helper written as `template <class R> ... (const udf::ReadRecord<R>& rec)`
serves a handler and a UDF alike, as `load` does in `examples/counter`. An `EventRecord` has none of the
writing methods, and a handler is not handed a `udf::Record`. `names()` ranges over the record's bin
names. `bins`, `deleted`, `bin_luts`, `bin_srcs` are the lists and maps from the description,
`bin_lut(name)` and `bin_src(name)` fall back to the record's values, and `gen`, `lut`, `ttl`, `src_id` are
its metadata.

`rec.with_bins(out)` is the Rust SDK's `with_bins`: every bin of the record that the `udf::Map` `out` lacks
goes in by position, so a handler that decides two bins and keeps the rest of one record writes those two
and returns `udf::Resolution::record(incoming.with_bins(out))`. It asks `out.contains(name)` for each bin,
which the host answers for the map being built. It prefers one side, so a handler that has no opinion on
the other bins uses `Pair::with_newer` instead, which takes each from the side that wrote it later.

## The handler

| Macro | Parameters | Returns `udf::Result` of |
| --- | --- | --- |
| `UDF_ON_XDR_MERGE` | `current, incoming` (`EventRecord`), `conflict` (`Conflict`) | `Resolution`: `keep_local()`, `take_remote()`, `policy()`, `delete_record()`, `record(bins[, ttl])` |

`keep_local()` keeps the current record, `take_remote()` takes the incoming write, and `policy()` leaves
the outcome to the namespace's policy, as if there were no handler. `delete_record()` removes the record
on this cluster; the other cluster decides for itself, so a merge that deletes has to delete on both
sides for the two to agree. A `record` answer takes a `udf::Map` of the final bins; a bin absent from it,
or nil, is deleted, at least one bin must be left, and the map's own error, if a put was refused, fails
the call. Its TTL is in seconds, 0 for the namespace default, and when left out the incoming write's is
kept. `conflict.conflict_trigger` says the binding has `trigger=conflict`, `conflict.lost_bins()` names
the incoming bins that would lose, and `Value::cmp_stored` / `stored_bytes_into` compare stored bytes for
a data-only tie-break; the two records are also `conflict.current()` and `conflict.incoming()`.

## Merging bin by bin

`conflict.pair()` reads the two sides bin by bin into a `udf::Both<T>`, the Rust SDK's `Pair` and `Both`:
`pair.as_int(name)`, `as_double`, `as_bool`, `bin` and `list` give the bin on each side (`local`,
`remote`, each an `Optional`), and `Both` carries the merges a resolver reaches for: `remote_newer()`, by
the bin's LUT and then source id, the order the server's own bin convergence uses; `newer()` and
`older()`; `max()`, `min()`, `sum()`, `merge(f)`; `local_or_remote()`, `remote_or_local()`.
`pair.with_newer(out)` fills in every bin the handler had no opinion on from the side that wrote it
later, which is symmetric, and `pair.remote_newer(name)` orders one bin. `conflict.involves_delete()` says
a bin was deleted on either side, the mistake every merge has to guard against.

```cpp
if (udf::Optional<int64_t> visits = pair.as_int("visits").max()) {   // both sides' increments are real
	out.put("visits", *visits);
}
if (udf::Optional<udf::Value> profile = pair.bin("profile").newer()) {   // last writer wins, by LUT
	out.put("profile", *profile);
}
return udf::Resolution::record(pair.with_newer(out));   // the rest as the policy would have kept it
```

## The raw form

The C header's macro and helpers are there under the C++ ones: `AS_UDF_EVENT_XDR_MERGE(name)` declares
`int32_t name(int32_t args, int32_t argc)` with the signature, `as_udf_event_decode` decodes the conflict
by hand, and `udf::Resolution::keep_local().deliver()` (or the C `as_udf_resolve_keep_local()`) sets the
result and returns the status to return. The C chapter is the reference for that form.

## The modules

`examples/resolver`, the account merge over `Pair` and `Both`, line for line the Rust one:

```cpp
{{#include ../../../aerospike-udf-sdk-wasm-cpp/examples/resolver/resolver.cpp}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the
[CRDT tutorial](../crdt/README.md), each step with its C++ beside the Rust, Java and Go.

## API reference

Generated from `aerospike_udf.hpp`: the event surface.

{{#include ../api/cpp-events.md}}
