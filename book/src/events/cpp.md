# C++

A C++ event handler is a function defined by one of the `UDF_ON_*` macros, one per kind, with the
parameters the kind hands over and returning the kind's answer in a `udf::Result`: the C++ face of the
Rust SDK's `event!`. The macro writes the export that decodes the event into those parameters and
delivers the answer, and the `__udf_sig_<name>` export reading `event <kind>/1 ...`, which `event-set`
checks before binding. The parameter names are the kind's (`before`, `after`, `write`; `record`;
`current`, `incoming`, `conflict`; ...), so a handler body reads them by those names. An error returned
fails the call with its message.

```cpp
static constexpr udf::Bin TICKS{"ticks"};

UDF_ON_BEFORE_WRITE(guard_account)
{
	if (udf::Optional<int64_t> balance = after.as_int("balance"); balance && *balance < 0) {
		return udf::Decision::reject(22, "balance may not go negative");
	}

	udf::Map out(4);

	out.put("stamped", write.source_view()).put("writes", before.as_int("writes").value_or(0) + 1);

	return udf::Decision::record(after.with_bins(out));   // every bin of `after` under the two decided
}

UDF_ON_TIMER(tick)
{
	int64_t n = TICKS.int_or(0) + 1;

	UDF_TRY(TICKS.set(n));
	UDF_TRY(udf::commit());

	if (n < 3) {
		udf::timer_set("tick", 200);
	}

	return {};
}
```

## Records

A `udf::EventRecord` is one record the event hands over, read-only, the C struct with typed reads: `get(name)`
is the bin as an `Optional<Value>`, empty for absent or nil; `as_int(name)`, `as_double`, `as_bool` and
`as_text<N>` answer an `Optional` of the type, empty for absent, nil or another kind, the Rust SDK's
`Record::int` and friends; `list(name)` and `map(name)` give a range over a collection bin, `names()` over
the record's bin names. `bins`, `deleted`, `bin_luts`, `bin_srcs` are the lists and maps from the
description, `bin_lut(name)` and `bin_src(name)` fall back to the record's values, and `gen`, `lut`, `ttl`,
`src_id` are its metadata.

`rec.with_bins(out)` is the Rust SDK's `with_bins`: every bin of the record that the `udf::Map` `out` lacks
goes in by position, so a handler that decides two bins and keeps the rest writes those two and returns
`udf::Decision::record(after.with_bins(out))`. It prefers one side, so an `xdr-merge` handler uses
`Pair::with_newer` instead.

## The kinds

| Macro | Parameters | Returns `udf::Result` of |
| --- | --- | --- |
| `UDF_ON_BEFORE_WRITE` | `before, after` (`EventRecord`), `write` (`WriteInfo`) | `Decision`: `accept()`, `reject(code, msg)`, `record(bins[, ttl])` |
| `UDF_ON_AFTER_WRITE` | the same | `void` |
| `UDF_ON_BEFORE_DELETE` | `record`, `del` (`DeleteInfo`) | `Gate`: `allow()`, `refuse(code, msg)` |
| `UDF_ON_AFTER_DELETE` | the same | `void` |
| `UDF_ON_BEFORE_EXPIRE` | `record`, `expire` (`ExpireInfo`) | `Expiry`: `expire()`, `keep(ttl)` |
| `UDF_ON_BEFORE_EVICT` | `record`, `evict` (`ExpireInfo`) | `Eviction`: `evict()`, `protect()` |
| `UDF_ON_AFTER_READ` | `record`, `read` (`ReadInfo`) | `ReadResult`: `unchanged()`, `record(bins)` |
| `UDF_ON_BEFORE_TRUNCATE` | `truncate` (`TruncateInfo`) | `Gate` |
| `UDF_ON_AFTER_TRUNCATE` | the same | `void` |
| `UDF_ON_BEFORE_SHIP` | `record`, `ship` (`ShipInfo`) | `ShipDecision`: `ship()`, `skip()`, `record(bins)` |
| `UDF_ON_PARTITION_TRANSFER` | `transfer` (`TransferInfo`) | `void` |
| `UDF_ON_CLUSTER_CHANGE` | `cluster` (`ClusterInfo`) | `void` |
| `UDF_ON_NODE_LIFECYCLE` | `lifecycle` (`LifecycleInfo`) | `void` |
| `UDF_ON_TIMER` | `timer` (`TimerInfo`); runs as a record UDF | `void` |
| `UDF_ON_XDR_MERGE` | `current, incoming` (`EventRecord`), `conflict` (`Conflict`) | `Resolution`: `keep_local()`, `take_remote()`, `policy()`, `record(bins[, ttl])` |

A `record` answer takes a `udf::Map`; a bin absent from it, or nil, is deleted, and the map's own error, if
a put was refused, fails the call. A TTL is in seconds, 0 for the namespace default. The info structs are
the C ones with the strings as views (`write.source_view()`, `truncate.set_view()`). For `xdr-merge`,
`conflict.conflict_trigger` says the binding has `trigger=conflict`, `conflict.lost_bins()` names the
incoming bins that would lose, and `Value::cmp_stored` / `stored_bytes_into` compare stored bytes for a
data-only tie-break; the two records are also `conflict.current()` and `conflict.incoming()`.

## Merging bin by bin

For `xdr-merge`, `conflict.pair()` reads the two sides bin by bin into a `udf::Both<T>`, the Rust SDK's
`Pair` and `Both`: `pair.as_int(name)`, `as_double`, `as_bool`, `bin` and `list` give the bin on each side
(`local`, `remote`, each an `Optional`), and `Both` carries the merges a resolver reaches for:
`remote_newer()`, by the bin's LUT and then source id, the order the server's own bin convergence uses;
`newer()` and `older()`; `max()`, `min()`, `sum()`, `merge(f)`; `local_or_remote()`, `remote_or_local()`.
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

## Timers

A timer handler runs as a record UDF: `udf::Bin` reads and writes and `udf::commit` work in it, and
`udf::timer_set("tick", 200)` re-arms it; `udf::timer_cancel` cancels. Both are refused in any other
handler.

## The raw form

The C header's macros and helpers are there under the C++ ones: `AS_UDF_EVENT_BEFORE_WRITE(name)` declares
`int32_t name(int32_t args, int32_t argc)` with the signature, `as_udf_event_desc`, `as_udf_event_get_record`
and the `as_udf_event_*_info` helpers decode by hand, and `udf::Decision::accept().deliver()` (or the C
`as_udf_answer_accept()`) sets the result and returns the status to return. The C chapter is the reference
for that form.

## The modules

`examples/guard`:

```cpp
{{#include ../../../aerospike-udf-sdk-wasm-cpp/examples/guard/guard.cpp}}
```

`examples/resolver`, the account merge over `Pair` and `Both`, line for line the Rust one:

```cpp
{{#include ../../../aerospike-udf-sdk-wasm-cpp/examples/resolver/resolver.cpp}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the
[CRDT tutorial](../crdt/README.md), each step with its C++ beside the Rust, Java and Go.

## API reference

Generated from `aerospike_udf.hpp`: the event surface.

{{#include ../api/cpp-events.md}}
