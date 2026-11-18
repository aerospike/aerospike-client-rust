# Java

A Java event handler is a static (or instance) method under one of the `@On` annotations, one per kind,
with the parameters the kind hands over and returning the kind's answer: the Java face of the Rust SDK's
`event!`. The processor generates the export that decodes the event into those parameters and delivers
the answer, and the `__udf_sig_<name>` export reading `event <kind>/1 ...`, which `event-set` checks
before binding. The parameter and return types are part of the kind, so a handler with another signature
does not compile. An exception fails the call with its message.

```java
@On.BeforeWrite("guard_account")
static Decision guardAccount(EventRecord before, EventRecord after, Events.WriteInfo write) {
    if (after.getLong("balance", 0) < 0) {
        return Decision.reject(22, "balance may not go negative");
    }

    MapBuilder out = Udf.map(4)
            .put("stamped", write.source)
            .put("writes", before.getLong("writes", 0) + 1);

    return Decision.record(after.withBins(out));   // every bin of `after` under the two decided
}
```

## Records

An `EventRecord` is one record the event hands over, read-only. It implements `RecordReader`, as the
`Record` of a record UDF does, so it reads the same way: `rec.get(name)` answers `null` for a bin that is
absent or nil, the typed readers `getLong`, `getDouble`, `getBoolean`, `getString`, `getBytes`, `getList`
and `getMap` throw for an absent bin unless given a default, and all of them throw for a bin of another
kind. A helper that takes a `RecordReader` works on both.

What an event adds: `rec.binNames()` is the list of the record's bin names, `rec.deletedBins()` the names
of the bins this side deleted, and `rec.deletedAny()` says whether there are any. `rec.binLut(name)` and
`rec.binSrc(name)` are when and by which data centre the bin was last written, falling back to the
record's own values. `rec.generation()`, `lastUpdateTime()`, `ttl()` and `sourceId()` are its metadata.

`rec.withBins(out)` is the Rust SDK's `with_bins`: every bin of the record that the `MapBuilder` `out`
lacks goes in by position, so a handler that decides two bins and keeps the rest writes those two and
returns `Decision.record(after.withBins(out))`. It prefers one side, so an `xdr-merge` handler uses
`Pair.withNewer` instead.

## The kinds

| Annotation | Parameters | Returns |
| --- | --- | --- |
| `@On.BeforeWrite` | `EventRecord before, EventRecord after, Events.WriteInfo write` | `Decision`: `accept()`, `reject(code, message)`, `record(bins[, ttl])` |
| `@On.AfterWrite` | the same | `void` |
| `@On.BeforeDelete` | `EventRecord record, Events.DeleteInfo delete` | `Gate`: `allow()`, `refuse(code, message)` |
| `@On.AfterDelete` | the same | `void` |
| `@On.BeforeExpire` | `EventRecord record, Events.ExpireInfo expire` | `Expiry`: `expire()`, `keep(ttl)` |
| `@On.BeforeEvict` | `EventRecord record, Events.ExpireInfo evict` | `Eviction`: `evict()`, `protect()` |
| `@On.AfterRead` | `EventRecord record, Events.ReadInfo read` | `ReadResult`: `unchanged()`, `record(bins)` |
| `@On.BeforeTruncate` | `Events.TruncateInfo truncate` | `Gate` |
| `@On.AfterTruncate` | the same | `void` |
| `@On.BeforeShip` | `EventRecord record, Events.ShipInfo ship` | `ShipDecision`: `ship()`, `skip()`, `record(bins)` |
| `@On.PartitionTransfer` | `Events.TransferInfo transfer` | `void` |
| `@On.ClusterChange` | `Events.ClusterInfo cluster` | `void` |
| `@On.NodeLifecycle` | `Events.LifecycleInfo lifecycle` | `void` |
| `@On.Timer` | `Record rec, Events.TimerInfo timer`; runs as a record UDF | `void` |
| `@On.XdrMerge` | `EventRecord current, EventRecord incoming, Conflict conflict` | `Resolution`: `keepLocal()`, `takeRemote()`, `policy()`, `record(bins[, ttl])` |

A `record` answer takes a `MapBuilder`; a bin absent from it, or nil, is deleted. A TTL is in seconds, 0
for the namespace default. For `xdr-merge`, `conflict.conflictTrigger` says the binding has
`trigger=conflict`, `conflict.lost` names the incoming bins that would lose, and `Value.compareStored`
compares two stored values by their bytes for a data-only tie-break; the two records are also
`conflict.local` and `conflict.remote`.

## Merging bin by bin

For `xdr-merge`, `conflict.pair()` reads the two sides bin by bin into a `Both<T>`, the Rust SDK's `Pair`
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

## Timers

A `timer` handler runs as a record UDF, on the record the timer was set on. So it is the one handler that
takes a `Record` as its first parameter, before its `TimerInfo`, and reads, sets and saves like any record
UDF. `Udf.timerSet("tick", 200)` re-arms it and throws when the server refuses. `Udf.timerCancel("tick")`
cancels. Both are refused in any other handler.

```java
@On.Timer("tick")
static void tick(Record rec, Events.TimerInfo timer) {
    long ticks = rec.getLong("ticks", 0) + 1;

    rec.set("ticks", ticks);
    rec.save();

    if (ticks < 3) {
        Udf.timerSet("tick", 200);
    }
}
```

## The raw form

Every kind also takes `int name(int args, int argc)`, exported as it is with only the signature
generated. `Events.desc(args, argc)` reads the description, `Events.side(desc, key, which)` gives the
record under its entry (`before`, `after`, `record`) or null, `which` being the index the record is read by, the
`Events.*Info` classes' `decode(desc)` read the kind's own entry (`Conflict.decode(args, argc)` for the
merge), and the answer methods `Events.accept()`, `reject`, `record(handle, ttl)`, `allow`, `refuse`,
`expire`, `keep`, `evict`, `protect`, `unchanged`, `ship`, `skip`, `done`, and `Conflict.keepLocal()`,
`takeRemote`, `policy`, `record(handle, ttl)` set the result and return the status to return.

## The modules

`examples/guard`:

```java
{{#include ../../../aerospike-udf-sdk-wasm-java/examples/guard/src/main/java/com/aerospike/udf/examples/Guard.java}}
```

`examples/resolver`, the account merge over `Pair` and `Both`, line for line the Rust one:

```java
{{#include ../../../aerospike-udf-sdk-wasm-java/examples/resolver/src/main/java/com/aerospike/udf/examples/Resolver.java}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the
[CRDT tutorial](../crdt/README.md), each step with its Java beside the Rust, Go and C++.

## API reference

Generated from the SDK's `On`, `Events`, `Conflict`, `EventRecord`, `Pair`, `Both` and answer classes.

{{#include ../api/java-events.md}}
