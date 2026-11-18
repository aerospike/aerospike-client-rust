# Go

A Go event handler is a function under a `//udf:on xdr-merge <name>` directive, taking the two records,
the current one and the incoming one, and returning a `udf.Resolution` and an `error`: the Go face of the
Rust SDK's `event!`. The generator writes the export that decodes the event into those parameters and
delivers the answer, and the `__udf_sig_<name>` export reading `event xdr-merge/1 ...`, which `event-set`
checks before binding. The parameter and return types are part of the kind, so a handler with another
signature is a generation error that states the expected one. A returned error fails the call with its
message.

```go
//udf:on xdr-merge merge_visits
func mergeVisits(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	pair := udf.PairOf(current, incoming)
	out := udf.NewMap(4)

	if visits, ok := udf.Max(pair.Int("visits")); ok { // both sides' increments are real
		out.Put("visits", visits)
	}

	return udf.Resolve(pair.WithNewer(out)), out.Err() // every other bin from the side that wrote it later
}
```

## Records

An `*udf.EventRecord` is one of the two records the event hands over, read-only: `current` is the record
as it is on this cluster and `incoming` the one that arrived over XDR. It has no writing methods: an event changes a record through its
answer. It reads like the record of a UDF. `Bin(name)` returns the bin or an error naming it when the side
has none. `Get(name)` and the typed readers `Int(name)`, `Double`, `Bool`, `String` and `Blob` answer the
value and whether the side has it, false for absent, nil or another kind; `IntOr(name, dflt)`, `DoubleOr`
and `BoolOr` answer a default instead. `List(name)` and `Map(name)` range over a collection bin,
`BinNames()` over the record's bin names and `DeletedBins()` over the names of the bins that side
deleted. `rec.BinLastUpdateTime(name)` and `rec.BinSourceID(name)` are when the bin was last written and
by which cluster, and fall back to the record's values where the server keeps none per bin.
`rec.Generation()`, `LastUpdateTime()`, `TTL()` and `SourceID()` are its metadata.

What the server says of the merge itself is on the record it is true of:

| Method | Asked of | Says |
| --- | --- | --- |
| `IsDelete()` | `incoming` | the incoming write is a record delete, so `incoming` has no bins |
| `IsPartial()` | `incoming` | the incoming write carries only the bins that changed, so a bin it lacks may well exist on the other cluster |
| `LostBins()` | `incoming` | the names of the incoming bins that would lose under bin convergence, to range over |
| `IsTombstone()` | `current` | the current record exists but has no bins |
| `UsesBinConvergence()` | either | per-bin timestamps are meaningful: a time-based merge checks it before trusting `BinLastUpdateTime` |

Asked of the other record, the first four answer false, or range over nothing.

Those reading methods are the `udf.RecordReader` interface, which the `*udf.Record` of a record UDF
implements too. A helper written over it serves both:

```go
// The balance of any record: a side of a merge here, the record of a UDF elsewhere.
func balanceOf(rec udf.RecordReader) int64 {
	n, _ := rec.Int("balance")

	return n
}

//udf:on xdr-merge keep_richer
func keepRicher(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	if balanceOf(incoming) > balanceOf(current) {
		return udf.TakeRemote(), nil
	}

	return udf.KeepLocal(), nil
}
```

`rec.WithBins(out)` is the Rust SDK's `with_bins`: every bin of the record that the `*udf.Map` `out`
lacks goes in by position. It asks `out.Contains(name)` for each bin, one import each. It prefers one
side, and the other DC prefers the other side, so a handler that decides a few bins and keeps the rest
uses `Pair.WithNewer` instead.

## The handler

| Directive | Parameters | Returns |
| --- | --- | --- |
| `//udf:on xdr-merge` | `current, incoming *udf.EventRecord` | `udf.Resolution`: `KeepLocal()`, `TakeRemote()`, `Policy()`, `DeleteRecord()`, `Resolve(bins)`, `ResolveTTL(bins, ttl)` |

The handler returns `(udf.Resolution, error)`. `Resolve` takes a `*udf.Map`; a bin absent from it, or nil,
is deleted. A TTL is in seconds, 0 for the namespace default; `Resolve` keeps the one the incoming write
asked for. `DeleteRecord()` removes the record on this cluster, and a record resolution with no bins left
in it is not that: it is the handler's failure. `udf.CmpStored` / `StoredBytes` compare stored bytes for a data-only
tie-break.

## Merging bin by bin

`udf.PairOf(current, incoming)` is the two records side by side, a `udf.Pair`, which reads them bin by bin
into a `udf.Both[T]`, the Rust SDK's `Pair` and `Both`: `pair.Int(name)`, `Double`, `Bool`, `String`, `Blob`, `List`, `Map` and `Bin` give the bin on each side (`Current`,
`Incoming`, with `HasCurrent` and `HasIncoming`), and `Both` carries the merges a resolver reaches for:
`IncomingIsNewer()`, by the bin's last update time and then source id, the order the server's own bin convergence uses;
`Newer()` and `Older()`; `Ordered()` as (older, newer); `Merge(f)`; `CurrentOrIncoming()`, `IncomingOrCurrent()`.
`udf.Max`, `udf.Min` and `udf.Sum` are functions over a `Both`, since a method cannot add the ordering
constraint. `pair.WithNewer(out)` fills in every bin the handler had no opinion on from the side that
wrote it later, which is symmetric, and `pair.IncomingIsNewer(name)` orders one bin. `rec.DeletedAny()` says
that side deleted a bin, and `rec.DeletedBins()` names them: bringing such a bin back from the other side is the
mistake every merge has to guard against.

```go
if visits, ok := udf.Max(pair.Int("visits")); ok { // both sides' increments are real
	out.Put("visits", visits)
}
if profile, ok := pair.Bin("profile").Newer(); ok { // last writer wins, by last update time
	out.Put("profile", profile)
}
return udf.Resolve(pair.WithNewer(out)), out.Err() // the rest as the policy would have kept it
```

## The raw form

A handler may also be `func(args, argc int32) int32`, exported as it is with only the signature
generated. `udf.DecodeMerge(args, argc, &current, &incoming)` reads the event into two `udf.EventRecord` variables,
false when the call is not that event, and a resolution's `Deliver()` sets the result and returns the status to
return: `return udf.KeepLocal().Deliver()`. It takes no `*udf.Record`: an event reads its records as
`*udf.EventRecord`.

## The modules

`examples/resolver`, the account merge over `Pair` and `Both`, line for line the Rust one:

```go
{{#include ../../../aerospike-udf-sdk-wasm-go/examples/resolver/main.go}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the
[CRDT tutorial](../crdt/README.md), each step with its Go beside the Rust, Java and C++.

## API reference

Generated from the SDK's sources: the event surface.

{{#include ../api/go-events.md}}
