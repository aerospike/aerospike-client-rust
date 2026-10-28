# Go

A Go event handler is a function under a `//udf:on <kind> <name>` directive, with the parameters the kind
hands over and returning the kind's answer and an `error`: the Go face of the Rust SDK's `event!`. The
generator writes the export that decodes the event into those parameters and delivers the answer, and
the `__udf_sig_<name>` export reading `event <kind>/1 ...`, which `event-set` checks before binding. The
parameter and return types are part of the kind, so a handler with another signature is a generation
error. A returned error fails the call with its message.

```go
var ticks = udf.NewBin("ticks")

//udf:on before-write guard_account
func guardAccount(before, after *udf.EventRecord, w udf.WriteInfo) (udf.Decision, error) {
	if balance, ok := after.Int("balance"); ok && balance < 0 {
		return udf.Reject(22, "balance may not go negative"), nil
	}

	writes, _ := before.Int("writes")
	out := udf.NewMap(4).
		Put("stamped", w.Source).
		Put("writes", writes+1)

	return udf.AcceptRecord(after.WithBins(out)), out.Err()   // every bin of `after` under the two decided
}

//udf:on timer tick
func tick(_ udf.TimerInfo) error {
	n := ticks.IntOr(0) + 1

	if err := ticks.SetInt(n); err != nil {
		return err
	}

	if err := udf.Commit(); err != nil {
		return err
	}

	if n < 3 {
		udf.TimerSet("tick", 200)
	}

	return nil
}
```

## Records

An `*udf.EventRecord` is one record the event hands over, read-only. The typed readers `Int(name)`,
`Float64`, `Bool`, `Str` and `Get` answer the value and whether the side has the bin (false for absent,
nil or another kind), the Rust SDK's `Record::int` and friends; `List(name)` and `Map(name)` range over a
collection bin, `Names()` over the record's bin names. `rec.Bins`, `rec.Deleted`, `rec.BinLUTs`,
`rec.BinSrcs` are the lists and maps from the description, `rec.BinLUT(name)` and `rec.BinSrc(name)` fall
back to the record's values, and `rec.Gen`, `LUT`, `TTL`, `SrcID` are its metadata.

`rec.WithBins(out)` is the Rust SDK's `with_bins`: every bin of the record that the `*udf.Map` `out`
lacks goes in by position, so a handler that decides two bins and keeps the rest writes those two and
returns `udf.AcceptRecord(after.WithBins(out))`. It prefers one side, so an `xdr-merge` handler uses
`Pair.WithNewer` instead.

## The kinds

| Directive | Parameters | Returns |
| --- | --- | --- |
| `//udf:on before-write` | `before, after *udf.EventRecord, w udf.WriteInfo` | `udf.Decision`: `Accept()`, `Reject(code, msg)`, `AcceptRecord(bins)`, `AcceptRecordTTL(bins, ttl)` |
| `//udf:on after-write` | the same | `error` |
| `//udf:on before-delete` | `rec *udf.EventRecord, d udf.DeleteInfo` | `udf.Gate`: `Allow()`, `Refuse(code, msg)` |
| `//udf:on after-delete` | the same | `error` |
| `//udf:on before-expire` | `rec *udf.EventRecord, e udf.ExpireInfo` | `udf.Expiry`: `Expire()`, `Keep(ttl)` |
| `//udf:on before-evict` | `rec *udf.EventRecord, e udf.ExpireInfo` | `udf.Eviction`: `Evict()`, `Protect()` |
| `//udf:on after-read` | `rec *udf.EventRecord, r udf.ReadInfo` | `udf.ReadResult`: `Unchanged()`, `ReadRecord(bins)` |
| `//udf:on before-truncate` | `t udf.TruncateInfo` | `udf.Gate` |
| `//udf:on after-truncate` | the same | `error` |
| `//udf:on before-ship` | `rec *udf.EventRecord, s udf.ShipInfo` | `udf.ShipDecision`: `Ship()`, `Skip()`, `ShipRecord(bins)` |
| `//udf:on partition-transfer` | `t udf.TransferInfo` | `error` |
| `//udf:on cluster-change` | `c udf.ClusterInfo` | `error` |
| `//udf:on node-lifecycle` | `l udf.LifecycleInfo` | `error` |
| `//udf:on timer` | `t udf.TimerInfo`; runs as a record UDF | `error` |
| `//udf:on xdr-merge` | `current, incoming *udf.EventRecord, c *udf.Conflict` | `udf.Resolution`: `KeepLocal()`, `TakeRemote()`, `Policy()`, `Resolve(bins)`, `ResolveTTL(bins, ttl)` |

Every answer-returning handler returns `(Answer, error)`; the others return `error`. A `record` answer
takes a `*udf.Map`; a bin absent from it, or nil, is deleted. A TTL is in seconds, 0 for the namespace
default. For `xdr-merge`, `c.ConflictTrigger` says the binding has `trigger=conflict`, `c.Lost` names the
incoming bins that would lose, and `udf.CmpStored` / `StoredBytes` compare stored bytes for a data-only
tie-break; the two records are also `c.Local` and `c.Remote`.

## Merging bin by bin

For `xdr-merge`, `c.Pair()` reads the two sides bin by bin into a `udf.Both[T]`, the Rust SDK's `Pair` and
`Both`: `pair.Int(name)`, `Float64`, `Bool`, `Str`, `List` and `Bin` give the bin on each side (`Local`,
`Remote`, with `HasLocal` and `HasRemote`), and `Both` carries the merges a resolver reaches for:
`RemoteNewer()`, by the bin's LUT and then source id, the order the server's own bin convergence uses;
`Newer()` and `Older()`; `Ordered()` as (older, newer); `Merge(f)`; `LocalOrRemote()`, `RemoteOrLocal()`.
`udf.Max`, `udf.Min` and `udf.Sum` are functions over a `Both`, since a method cannot add the ordering
constraint. `pair.WithNewer(out)` fills in every bin the handler had no opinion on from the side that
wrote it later, which is symmetric, and `pair.RemoteNewer(name)` orders one bin. `c.InvolvesDelete()` says
a bin was deleted on either side, the mistake every merge has to guard against.

```go
if visits, ok := udf.Max(pair.Int("visits")); ok {   // both sides' increments are real
	out.Put("visits", visits)
}
if profile, ok := pair.Bin("profile").Newer(); ok {   // last writer wins, by LUT
	out.Put("profile", profile)
}
return udf.Resolve(pair.WithNewer(out)), out.Err()    // the rest as the policy would have kept it
```

## Timers

A timer handler runs as a record UDF: `udf.Bin` reads and writes and `udf.Commit` work in it, and
`udf.TimerSet("tick", 200)` re-arms it; `udf.TimerCancel` cancels. Both are refused in any other handler.

## The raw form

Every kind also takes `func(args, argc int32) int32`, exported as it is with only the signature
generated. `udf.EventDesc(args, argc, &desc)` reads the description, `udf.EventSide(desc, key, which,
&rec)` fills a record from its entry (`before`, `after`, `record`), `which` being the index the record is
read by, the `udf.Event*Info(desc, &info)` decoders read the kind's own entry (`udf.DecodeConflict(args,
argc, &c)` for the merge), and an answer value's `Deliver()` sets the result and returns the status to
return: `return udf.Accept().Deliver()`.

## The modules

`examples/guard`:

```go
{{#include ../../../aerospike-udf-sdk-wasm-go/examples/guard/main.go}}
```

`examples/resolver`, the account merge over `Pair` and `Both`, line for line the Rust one:

```go
{{#include ../../../aerospike-udf-sdk-wasm-go/examples/resolver/main.go}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the
[CRDT tutorial](../crdt/README.md), each step with its Go beside the Rust, Java and C++.

## API reference

Generated from the SDK's sources: the event surface.

{{#include ../api/go-events.md}}
