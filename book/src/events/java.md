# Java

A Java event handler is a static (or instance) method under `@On.XdrMerge`, taking what the event hands
over and returning its answer: the Java face of the Rust SDK's `event!`. The processor generates the
export that decodes the event into those parameters and delivers the answer, and the `__udf_sig_<name>`
export reading `event xdr-merge/1 ...`, which `event-set` checks before binding. The parameter and return
types are part of the kind, so a handler with another signature does not compile. An exception fails the
call with its message.

```java
@On.XdrMerge("merge_visits")
static Resolution mergeVisits(EventRecord current, EventRecord incoming, Conflict conflict) {
    Long visits = conflict.pair().int64("visits").max();   // both sides' increments are real

    if (visits == null) {
        return Resolution.policy();
    }

    return Resolution.record(conflict.pair().withNewer(Udf.map(4).put("visits", visits)));
}
```

## Records

An `EventRecord` is one side of the merge, the current record or the incoming write, read-only. It implements `RecordReader`, as the
`Record` of a record UDF does, so it reads the same way: `rec.get(name)` answers `null` for a bin that is
absent or nil, the typed readers `getLong`, `getDouble`, `getBoolean`, `getString`, `getBytes`, `getList`
and `getMap` throw for an absent bin unless given a default, and all of them throw for a bin of another
kind. A helper that takes a `RecordReader` works on both.

What an event adds: `rec.binNames()` is the list of the record's bin names, `rec.deletedBins()` the names
of the bins this side deleted, and `rec.deletedAny()` says whether there are any. `rec.binLut(name)` and
`rec.binSrc(name)` are when and by which data centre the bin was last written, falling back to the
record's own values. `rec.generation()`, `lastUpdateTime()`, `ttl()` and `sourceId()` are its metadata.

`rec.withBins(out)` is the Rust SDK's `with_bins`: every bin of the record that the `MapBuilder` `out`
lacks goes in by position, for an answer that decides two bins and keeps the rest of that record. It
prefers one side, so a handler that has to be symmetric uses `Pair.withNewer` instead.

## The handler and its answer

| Annotation | Parameters | Returns |
| --- | --- | --- |
| `@On.XdrMerge` | `EventRecord current, EventRecord incoming, Conflict conflict` | `Resolution`: `keepLocal()`, `takeRemote()`, `policy()`, `delete()`, `record(bins[, ttl])` |

A `record` answer takes a `MapBuilder`; a bin absent from it, or nil, is deleted. A TTL is in seconds, 0
for the namespace default. `delete()` removes the record on this cluster. `conflict.conflictTrigger` says
the binding has `trigger=conflict`, `conflict.lost` names the incoming bins that would lose, and
`Value.compareStored` compares two stored values by their bytes for a data-only tie-break; the two records
are also `conflict.local` and `conflict.remote`.

## Merging bin by bin

`conflict.pair()` reads the two sides bin by bin into a `Both<T>`, the Rust SDK's `Pair`
and `Both`: `pair.int64(name)`, `float64`, `bool`, `str`, `list`, `map` and `bin` give the bin on each
side (`local`, `remote`, either null when that side has no such bin or holds nil; `list` and `map` give a
`ListValue` and a `MapValue`), and `Both` carries the
merges a resolver reaches for: `remoteNewer()`, by the bin's LUT and then source id, the order the server's
own bin convergence uses; `newer()` and `older()`; `max()`, `min()`, `merge(f)`, `sumInt()`, `sumDouble()`;
`localOrRemote()`, `remoteOrLocal()`. `pair.withNewer(out)` fills in every bin the handler had no opinion
on from the side that wrote it later, which is symmetric, and `pair.remoteNewer(name)` orders one bin.
`conflict.involvesDelete()` says a bin was deleted on either side, the mistake every merge has to guard
against.

```java
@On.XdrMerge("merge_visits")
static Resolution mergeVisits(EventRecord local, EventRecord remote, Conflict conflict) {
    Pair pair = conflict.pair();
    MapBuilder out = Udf.map(4);

    Long visits = pair.int64("visits").max();          // both sides' increments are real
    Value profile = pair.bin("profile").newer();       // last writer wins, by LUT

    if (visits != null) {
        out.put("visits", visits);
    }

    if (profile != null) {
        out.put("profile", profile);
    }

    return Resolution.record(pair.withNewer(out));     // the rest as the policy would have kept it
}
```

## The raw form

A handler may also be `int name(int args, int argc)`, exported as it is with only the signature
generated. `Conflict.decode(args, argc)` reads the call into a `Conflict` and its two records, and the
answer methods `Conflict.keepLocal()`, `takeRemote()`, `policy()`, `delete()` and `record(handle, ttl)`
set the result and return the status to return.

## The modules

`examples/resolver`, the account merge over `Pair` and `Both`, line for line the Rust one:

```java
{{#include ../../../aerospike-udf-sdk-wasm-java/examples/resolver/src/main/java/com/aerospike/udf/examples/Resolver.java}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the
[CRDT tutorial](../crdt/README.md), each step with its Java beside the Rust, Go and C++.

## API reference

Generated from the SDK's `On`, `Resolution`, `Conflict`, `EventRecord`, `Pair` and `Both` classes.

{{#include ../api/java-events.md}}
