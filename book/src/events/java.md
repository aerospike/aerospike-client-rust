# Java

A Java event handler is a static (or instance) method under one of the `@On` annotations, one per kind,
with the parameters the kind hands over and returning the kind's answer: the Java face of the Rust SDK's
`event!`. The processor generates the export that decodes the event into those parameters and delivers
the answer, and the `__udf_sig_<name>` export reading `event <kind>/1 ...`, which `event-set` checks
before binding. The parameter and return types are part of the kind, so a handler with another signature
does not compile. An exception fails the call with its message.

```java
private static final Bin TICKS = Bin.of("ticks");

@On.BeforeWrite("guard_account")
static Decision guardAccount(EventRecord before, EventRecord after, Events.WriteInfo write) {
    Long balance = after.binInt("balance");

    if (balance != null && balance < 0) {
        return Decision.reject(22, "balance may not go negative");
    }

    Long writes = before.binInt("writes");
    UdfMap out = UdfMap.create(4)
            .put("stamped", write.source)
            .put("writes", (writes == null ? 0 : writes) + 1);

    return Decision.record(after.withBins(out));   // every bin of `after` under the two decided
}

@On.Timer("tick")
static void tick(Events.TimerInfo t) {
    long n = TICKS.getLong(0) + 1;

    TICKS.set(n);
    Udf.commit();

    if (n < 3) {
        Udf.timerSet("tick", 200);
    }
}
```

## Records

An `EventRecord` is one record the event hands over, read-only. The typed readers `rec.binInt(name)`,
`binDouble`, `binBool`, `binStr`, `binList`, `binMap` and `binValue` answer `null` for a bin that is
absent or nil and throw for one of another kind, the Rust SDK's `Record::int` and friends; `rec.list(name)`
and `rec.map(name)` open a `Cursor` over a collection bin. `rec.bins`, `rec.deleted`, `rec.binLuts`,
`rec.binSrcs` are the lists and maps from the description, `rec.binLut(name)` and `rec.binSrc(name)` fall
back to the record's values, and `rec.gen`, `lut`, `ttl`, `srcId` are its metadata.

`rec.withBins(out)` is the Rust SDK's `with_bins`: every bin of the record that the `UdfMap` `out` lacks
goes in by position, so a handler that decides two bins and keeps the rest writes those two and returns
`Decision.record(after.withBins(out))`. It prefers one side, so an `xdr-merge` handler uses
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
| `@On.Timer` | `Events.TimerInfo timer`; runs as a record UDF | `void` |
| `@On.XdrMerge` | `EventRecord current, EventRecord incoming, Conflict conflict` | `Resolution`: `keepLocal()`, `takeRemote()`, `policy()`, `record(bins[, ttl])` |

A `record` answer takes a `UdfMap`; a bin absent from it, or nil, is deleted. A TTL is in seconds, 0 for
the namespace default. For `xdr-merge`, `conflict.conflictTrigger` says the binding has
`trigger=conflict`, `conflict.lost` names the incoming bins that would lose, and `Udf.cmpStored` /
`storedBytes` compare stored bytes for a data-only tie-break; the two records are also `conflict.local`
and `conflict.remote`.

## Merging bin by bin

For `xdr-merge`, `conflict.pair()` reads the two sides bin by bin into a `Both<T>`, the Rust SDK's `Pair`
and `Both`: `pair.int64(name)`, `float64`, `bool`, `str`, `list`, `map` and `bin` give the bin on each
side (`local`, `remote`, either null when that side has no such bin or holds nil), and `Both` carries the
merges a resolver reaches for: `remoteNewer()`, by the bin's LUT and then source id, the order the server's
own bin convergence uses; `newer()` and `older()`; `max()`, `min()`, `merge(f)`, `sumInt()`, `sumDouble()`;
`localOrRemote()`, `remoteOrLocal()`. `pair.withNewer(out)` fills in every bin the handler had no opinion
on from the side that wrote it later, which is symmetric, and `pair.remoteNewer(name)` orders one bin.
`conflict.involvesDelete()` says a bin was deleted on either side, the mistake every merge has to guard
against.

```java
Long visits = pair.int64("visits").max();          // both sides' increments are real
Value profile = pair.bin("profile").newer();       // last writer wins, by LUT
return Resolution.record(pair.withNewer(out));     // the rest as the policy would have kept it
```

## Timers

A timer handler runs as a record UDF: `Bin` reads and writes and `Udf.commit` work in it, and
`Udf.timerSet("tick", 200)` re-arms it; `Udf.timerCancel` cancels. Both are refused in any other handler.

## The raw form

Every kind also takes `int name(int args, int argc)`, exported as it is with only the signature
generated. `Events.desc(args, argc)` reads the description, `Events.side(desc, key, which, rec)` fills a
record from its entry (`before`, `after`, `record`), `which` being the index the record is read by, the
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
