# C++

A C++ event handler is a function defined by the `UDF_ON_XDR_MERGE` macro, with the parameters the event
hands over and returning its answer in a `udf::Result`: the C++ face of the Rust SDK's `event!`. The macro
writes the export that decodes the event into those parameters and delivers the answer, and the
`__udf_sig_<name>` export reading
`event xdr-merge/1 name(current: record, incoming: record) -> resolution`, which `event-set` checks before
binding. The two parameter names are fixed, `current` for the record as it is on this cluster and
`incoming` for the one that arrived over XDR, so a handler body reads them by those names. An error
returned fails the call with its message.

```cpp
UDF_ON_XDR_MERGE(merge_account)
{
	udf::Pair pair = udf::Pair::of(current, incoming);
	udf::Map out(4);

	if (udf::Optional<int64_t> visits = UDF_TRY(pair.as_int("visits")).max()) {   // both sides' increments are real
		out.put("visits", *visits);
	}

	return udf::Resolution::record(pair.with_newer(out));   // every other bin from the side that wrote it later
}
```

## Records

A `udf::EventRecord` is one record the event hands over, read-only, over the C struct, which it keeps
private: none of the struct's fields can be reached from a handler, only methods. It reads its
bins with the methods of `udf::ReadRecord`, the same ones `udf::Record` has in a record UDF: `get(name)`
is the bin as an `Optional<Value>`, empty for absent or nil; `bin(name)` is the bin that must be there, a
`Result<Value>`; `as_int(name)`, `as_double`, `as_bool`, `as_string<N>`, `as_blob<N>` and `as_bytes<N>`
answer a `Result<Optional<T>>`: empty for absent or nil, the value, or an error of status
`AS_UDF_TYPE_MISMATCH` for another kind; `int_or(name, 0)`, `double_or` and `bool_or` take a default for an
absent bin only and answer a `Result<T>`; `has(name)` says whether the bin is there; `list(name)` and
`map(name)` give a range over a collection bin, nothing for an absent one and an error for another kind. A helper written as `template <class R> ... (const udf::ReadRecord<R>& rec)`
serves a handler and a UDF alike, as `load` does in `examples/counter`. An `EventRecord` has none of the
writing methods, and a handler is not handed a `udf::Record`. Everything else about the record is a
method too: `bin_names()` ranges over the record's bin names and `deleted_bins()` over the names of the
bins this side deleted (the server sends no list at all when there are none, and the range is then empty, so
`size()` is the count either way), `bin_last_update_time(name)` and `bin_source_id(name)` answer for one bin and fall
back to the record's values where the server keeps none per bin, `generation()`, `last_update_time()`,
`ttl()` and `source_id()` are its metadata, and `is_incoming()` says which of the two records it is.

What the server says of the merge is asked of the record it is about. A method asked of the other record
answers false:

| Method | What it says |
| --- | --- |
| `incoming.is_delete()` | the incoming write is a record delete, so `incoming` has no bins |
| `incoming.is_partial()` | the incoming write carries only the bins that changed, so a bin it lacks may well exist on the other cluster |
| `current.is_tombstone()` | the current record exists but has no bins |
| `uses_bin_convergence()` | on either record, the same answer: per-bin timestamps are meaningful, for a time-based merge to check before it trusts `bin_last_update_time` |

`rec.with_bins(out)` is the Rust SDK's `with_bins`: every bin of the record that the `udf::Map` `out` lacks
goes in by position, so a handler that decides two bins and keeps the rest of one record writes those two
and returns `udf::Resolution::record(incoming.with_bins(out))`. It asks `out.contains(name)` for each bin,
which the host answers for the map being built. It prefers one side, so a handler that has no opinion on
the other bins uses `Pair::with_newer` instead, which takes each from the side that wrote it later.

## The handler

| Macro | Parameters | Returns `udf::Result` of |
| --- | --- | --- |
| `UDF_ON_XDR_MERGE` | `current`, `incoming` (`const EventRecord&`) | `Resolution`: `keep_local()`, `take_remote()`, `policy()`, `delete_record()`, `record(bins[, ttl])` |

`keep_local()` keeps the current record, `take_remote()` takes the incoming write, and `policy()` leaves
the outcome to the namespace's policy, as if there were no handler. `delete_record()` removes the record
on this cluster; the other cluster decides for itself, so a merge that deletes has to delete on both
sides for the two to agree. A `record` answer takes a `udf::Map` of the final bins; a bin absent from it,
or nil, is deleted, at least one bin must be left, and the map's own error, if a put was refused, fails
the call. Its TTL is in seconds, 0 for the namespace default, and when left out the incoming write's is
kept. `Value::cmp_stored` / `stored_bytes_into` compare stored bytes for a data-only tie-break;
`cmp_stored` is a `Result<int32_t>`, an error for a value that is not stored and exactly -1, 0 or 1
otherwise.

## Merging bin by bin

`udf::Pair pair = udf::Pair::of(current, incoming);` puts the two records side by side and reads them bin
by bin into a `udf::Both<T>`, the Rust SDK's `Pair` and `Both`. Each record knows which one it is, so the
two may be given in either order. `pair.as_int(name)`, `as_double`, `as_bool`, `as_string<N>`,
`as_blob<N>`, `list` and `map` give the bin on each side as a `Result<Both<T>>` (`current`, `incoming`, each
an `Optional`, empty for absent or nil; the error when either side holds another kind), `bin` as a plain
`Both<Value>`, and `Both` carries the merges a resolver reaches for: `incoming_is_newer()`, by the bin's
last-update time and then source id, the order the server's own bin convergence uses; `newer()` and
`older()`, and `ordered()` for both at once (`auto [older, newer] = both.ordered();`); `max()`, `min()`,
`sum()` (the one value when only one side has the bin, nothing when neither has it, as for `max()` and
`min()`, and an integer sum wraps rather than overflowing), `merge(f)`;
`current_or_incoming()`, `incoming_or_current()`.
`pair.with_newer(out)` fills in every bin the handler had no opinion on from the side that wrote it
later, which is symmetric; a bin the newer side deleted stays deleted (it goes in as nil), so the other
side's copy does not come back. `pair.incoming_is_newer(name)` orders one bin, and `deleted_any()` on a
record says it deleted a bin. `with_bins(out)` on one record fills in that record's bins, never a bin it
deleted; a name or bin it cannot read is kept as the map's error, which fails the answer.

```cpp
if (udf::Optional<int64_t> visits = UDF_TRY(pair.as_int("visits")).max()) {   // both sides' increments are real
	out.put("visits", *visits);
}
if (udf::Optional<udf::Value> profile = pair.bin("profile").newer()) {   // last writer wins, by LUT
	out.put("profile", *profile);
}
return udf::Resolution::record(pair.with_newer(out));   // the rest as the policy would have kept it
```

## The raw form

The C header's macro and helpers are there under the C++ ones: `AS_UDF_EVENT_XDR_MERGE(name)` declares
`int32_t name(int32_t args, int32_t argc)` with the signature, `as_udf_event_decode(args, argc, &current,
&incoming)` fills two C `as_udf_event_record`s by hand (not `udf::EventRecord`s, whose struct is private;
the raw form reads the C fields and calls the C functions), and `udf::Resolution::keep_local().deliver()`
(or the C `as_udf_resolve_keep_local()`) sets the
result and returns the status to return. The C chapter is the reference for that form.

## The modules

`examples/resolver`, the account merge over `Pair` and `Both`, line for line the Rust one:

```cpp
{{#include ../../../wasm-cpp/examples/resolver/resolver.cpp}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the
[CRDT tutorial](../crdt/README.md), each step with its C++ beside the Rust, Java and Go.

## API reference

Generated from `aerospike_udf.hpp`: the event surface.

{{#include ../api/cpp-events.md}}
