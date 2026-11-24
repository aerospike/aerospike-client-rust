# Java

A Java event handler is a static (or instance) method under `@On.XdrMerge`, taking what the event hands
over and returning its answer: the Java face of the Rust SDK's `event!`. The processor generates the
export that decodes the event into those parameters and delivers the answer, and the `__udf_sig_<name>`
export reading `event xdr-merge/1 ...`, which `event-set` checks before binding. The parameter and return
types are part of the kind, so a handler with another signature does not compile. An exception fails the
call with its message.

```java
@On.XdrMerge("merge_visits")
static Resolution mergeVisits(EventRecord current, EventRecord incoming) {
    Pair pair = Pair.of(current, incoming);
    Long visits = pair.getInt("visits").max();  // both sides' increments are real

    if (visits == null) {
        return Resolution.policy();
    }

    return Resolution.record(pair.withNewer(Udf.map(4).put("visits", visits)));
}
```

## Records

An `EventRecord` is one side of the merge, the current record or the incoming write, read-only. It implements `RecordReader`, as the
`Record` of a record UDF does, so it reads the same way: `rec.get(name)` answers `null` for a bin that is
absent or nil, `rec.bin(name)` throws for one that is absent, `rec.has(name)` asks whether it is there,
the typed readers `getInt`, `getDouble`, `getBool`, `getString`, `getBlob`, `getBytes`, `getList` and
`getMap` answer `null` for an absent bin (or the default, when given one), and all of them throw
`UdfException` with the status `LowLevel.TYPE_MISMATCH` for a bin of another kind. A helper that
takes a `RecordReader` works on both.

What an event adds: `rec.binNames()` is the list of the record's bin names, `rec.deletedBins()` the names
of the bins this side deleted (an empty list when the server sent none), `rec.deletedAny()` says whether
there are any, and `rec.deleted(name)` whether this side deleted that bin.
`rec.binLastUpdateTime(name)` and `rec.binSourceId(name)` are when and by which data centre the bin was
last written. The server gives them for every bin of both sides; a bin the side neither has nor deleted
falls back to the record's own values. `rec.generation()`, `lastUpdateTime()` and `sourceId()` are its
metadata.

What the merge says of one side is on that record too. `incoming.isDelete()` says the incoming write is a
record delete, so it has no bins; `incoming.isPartial()` that it carries only the bins that changed, so a
bin it lacks may well exist on the other cluster. `current.isTombstone()` says the current record exists but has no bins.
One asked of the side it is not about answers false.

`rec.withBins(out)` is the Rust SDK's `with_bins`: every bin of the record that the `MapBuilder` `out`
lacks goes in by position, for an answer that decides two bins and keeps the rest of that record. It
prefers one side, so a handler that has to be symmetric uses `Pair.withNewer` instead.

## The handler and its answer

| Annotation | Parameters | Returns |
| --- | --- | --- |
| `@On.XdrMerge` | `EventRecord current, EventRecord incoming` | `Resolution`: `keepLocal()`, `takeRemote()`, `policy()`, `delete()`, `expunge()`, `record(bins)` |

A `record` answer takes a `MapBuilder`; a bin absent from it, or nil, is deleted. No answer carries a TTL:
the merged record takes the later of the two records' expirations, and a record that never expires wins.
`delete()` removes the record on this cluster and leaves a durable tombstone, so a late write cannot bring
it back; `expunge()` removes it outright, with no tombstone. Neither is shipped to the other cluster.
`Value.cmpStored` compares
two stored values by their bytes for a data-only tie-break. A handler with other parameters or another
return is a compile error that states the expected shape.

## Merging bin by bin

`Pair.of(current, incoming)` reads the two records bin by bin into a `Both<T>`, the Rust SDK's `Pair`
and `Both`: `pair.getInt(name)`, `getDouble`, `getBool`, `getString`, `getBlob`, `getBytes`, `getList`,
`getMap` and `bin` give the bin on each side (`current`, `incoming`, either null when that side has no such bin or
holds nil; `getList` and `getMap` give a `ListValue` and a `MapValue`), and `Both` carries the merges a
resolver reaches for: `incomingIsNewer()`, by the bin's LUT and then source id; `newer()` and `older()`, and `ordered()`, the two as a list, older first, for a
merge whose result depends on the order; `max()`, `min()`, `merge(f)`, `sumInt()` and `sumDouble()` (null
when neither side has the bin, as `max()` and `min()`; `sumInt()` wraps on overflow);
`currentOrIncoming()`, `incomingOrCurrent()`; and for a map or list bin the data-only merges
`maxPerKey()`, `sortedUnion()`, `counterMerge()` and `lwwByVersion()`, also static over two `Value`s beside
`Both.counterTotal`. `pair.withNewer(out)` fills in every bin the handler had no
opinion on from the side that wrote it later, which is symmetric, and leaves out a bin that side deleted
(rather than bring the older side's value back); `pair.incomingIsNewer(name)` orders
one bin.
`current.deletedAny() || incoming.deletedAny()` says a bin was deleted on either side: resurrecting it
from the other is the mistake every merge has to guard against.

```java
@On.XdrMerge("merge_visits")
static Resolution mergeVisits(EventRecord current, EventRecord incoming) {
    Pair pair = Pair.of(current, incoming);
    MapBuilder out = Udf.map(4);

    Long visits = pair.getInt("visits").max();         // both sides' increments are real
    Value profile = pair.bin("profile").newer();       // last writer wins, by LUT

    if (visits != null) {
        out.put("visits", visits);
    }

    if (profile != null) {
        out.put("profile", profile);
    }

    return Resolution.record(pair.withNewer(out));     // the rest to the last writer of each
}
```

## The raw form

A handler may also be `int name(int args, int argc)`, exported as it is with only the signature
generated. `Pair.decode(args, argc)` reads the call into its two records, `pair.current` the current one
and `pair.incoming` the incoming write, or answers null when the call is not an `xdr-merge`. The answer is
the same `Resolution`, delivered by hand: `return Resolution.keepLocal().deliver();` sets the result and
gives the status to return, and a map built over a raw handle goes in as
`Resolution.record(LowLevel.map(handle))`.

## The modules

`examples/resolver`, the account merge over `Pair` and `Both`, line for line the Rust one:

```java
{{#include ../../../wasm-java/examples/resolver/src/main/java/com/aerospike/udf/examples/Resolver.java}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the
[CRDT tutorial](../crdt/README.md), each step with its Java beside the Rust, Go and C++.

## API reference

Generated from the SDK's `On`, `Resolution`, `EventRecord`, `Pair` and `Both` classes.

{{#include ../api/java-events.md}}
