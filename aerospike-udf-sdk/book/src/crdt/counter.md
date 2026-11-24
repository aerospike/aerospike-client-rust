# Step 1: a counter that only grows

The first step is the smallest CRDT there is: a counter that two data centres both increment and that
converges without anyone ever losing an increment. It is a dozen lines of logic, and it shows the whole
pattern every later step follows: a merge that decides on the data alone, plus the write UDFs that keep
the data in the shape the merge expects. Read it even if a counter is not what you need, because the
four steps after it change the data and keep the pattern.

## The idea

A plain integer bin cannot be merged: if dc1 adds 2 and dc2 adds 5 to a counter at 3, the two records
say 5 and 8, and neither is the answer. So the record keeps **one count per site**, in a map bin
`counts`. A site is a cluster, named by its XDR `src-id` as a decimal string:

```text
counts: {"1": 5, "2": 5}
```

Each data centre adds only to its own entry, and the counter's value is the sum of the entries. The two
timelines on [the tutorial's opening page](README.md#the-picture) draw this failure and this fix. Now the
merge is obvious: for every site, keep the **larger** of the two counts. A site's own count only ever
grows, so the larger one is always the later one, and no clock, LUT or writer id is needed to know it.
Taking a maximum per entry is commutative, idempotent and associative by construction, which is what makes
this a CRDT (a grow-only counter, in the literature), and the server's dry run confirms the three laws
on real bytes.

## The merge

In Rust:

```rust
event! {
    on xdr_merge
    fn merge_counter(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let mut counts = Counts::new();

        counts.offer_all(current.map("counts")?)?;
        counts.offer_all(incoming.map("counts")?)?;

        let out = as_udf::map(1)?;

        out.insert("counts", &counts.into_map()?)?;

        Ok(Resolution::Record { bins: incoming.with_bins(out)? })
    }
}
```

`Counts` is a small table of `(site, count)` pairs whose `offer` keeps the larger count for a site;
`offer_all` walks a stored map's entries into it. Every other bin is the incoming one's
(`incoming.with_bins`), which is what XDR would have written on its own. Every SDK also has this merge
ready-made: `counter_merge` gives the merged map of two sides and `counter_total` its value. The example
writes it out, so the pattern is in view.

The same in Java:

```java
@On.XdrMerge("merge_counter")
static Resolution mergeCounter(EventRecord current, EventRecord incoming) {
    size = 0;
    offerAll(current.getMap("counts", null));
    offerAll(incoming.getMap("counts", null));

    return Resolution.record(incoming.withBins(Udf.map(2).put("counts", toMap())));
}
```

`offerAll` walks the stored map entry by entry (`for (MapValue.Entry e : map)`), the Java form of
the Rust `entries()` loop, `toMap` builds a `MapBuilder`, and `incoming.withBins(out)` is
`incoming.with_bins(out)`. The annotation is the export: the processor generates the `merge_counter` export
that decodes the event into the three parameters and delivers the `Resolution`, and its signature. The
two write UDFs are `@Function("increment")` on `static long increment(Record rec, long n)` and
`@Function("value")` on `static long value(Record rec)`, their arguments decoded and their returns delivered
the same way. The record is the first parameter: `rec.getMap("counts", null)` reads the `counts` bin,
`rec.set` stages the new map, and `rec.save()` writes it.

And in Go:

```go
//udf:on xdr-merge merge_counter
func mergeCounter(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	t := table{}

	for _, side := range []*udf.EventRecord{current, incoming} {
		if err := t.offerRecord(side); err != nil {
			return udf.Resolution{}, err
		}
	}

	out := udf.NewMap(2).Put("counts", t.toMap())

	return udf.Resolve(incoming.WithBins(out)), out.Err()
}
```

`table` is a `map[string]int64` whose `offer` keeps the larger count, `offerRecord` reads the `counts` bin
of either side, `offerAll` ranges over the stored map (`for k, v := range stored.Map()`), and the `//udf:on` directive is the export: `udfgen` writes the
`merge_counter` trampoline that decodes the event and delivers the `Resolution`, and its signature. The
write UDFs are `//udf:function increment` on `func increment(rec *udf.Record, n int64) (int64, error)`
and `//udf:function value`. The record is the first parameter: `offerRecord` reads its `counts` bin
too, since a `udf.RecordReader` is either kind of record, `rec.Set` stages the new map, and `rec.Save()`
writes it.

And in C++:

```cpp
UDF_ON_XDR_MERGE(merge_counter)
{
	Table t;

	const udf::EventRecord* sides[] = {&current, &incoming};

	for (const udf::EventRecord* side : sides) {
		UDF_TRY(load(*side, t));
	}

	udf::Map out(2);

	out.put("counts", t.to_map());

	return udf::Resolution::record(incoming.with_bins(out));
}
```

`Table` is a fixed array of 64 `(Text<32>, count)` pairs, since a module has no allocator, whose `offer`
keeps the larger count and whose `offer_all` ranges over the stored map (`for (udf::Entry e :
stored.map())`); `load` offers the `counts` bin of a record, either side of the merge or the record a UDF
runs on; `UDF_ON_XDR_MERGE` is the export, writing the `merge_counter` trampoline and its signature.
The write UDFs are `UDF_FUNCTION(increment, udf::Result<int64_t>, udf::Record& rec, int64_t n)`
and `UDF_FUNCTION(value, udf::Result<int64_t>, const udf::Record& rec)`. The record is the first parameter: `rec.set` stages the `counts` bin, and
`rec.save()` writes it.

## The write UDFs

`increment(n)` reads the map, adds `n` to this cluster's entry (creating it if the entry is new), stores
the map and saves the record, answering the new total; `value()` answers the sum. Every application write goes
through `increment`, which is the only invariant this CRDT needs: a site touches its own entry and never
lowers it.

The site is not an argument. `increment` asks the server for its cluster's `src-id` (`cluster_src_id()` in
every SDK) and counts under it, so no client can add to another cluster's entry by passing the wrong name.
A cluster with no `src-id` (Community Edition, or none configured) has no entry to count under, and
`increment` fails there with a message saying so. A merge handler may not ask: the answer depends on
which cluster runs it.

## Deploying and checking it

Bind the handler on both clusters, with the write abandoned rather than retried if the handler ever
fails, and make sure each source ships its id:

```text
event-set:event=xdr-merge;namespace=test;set=counters;module=counter;function=merge_counter;on-failure=abandon
```

The dry run on one cluster checks the laws: `execute_udf(key, "__event__", "xdr-merge", [{"counts":
{"1": 1, "2": 5}}, {"src-id": 2}])` against a record the cluster with src-id 1 counted to 3 answers
`{"1": 3, "2": 5}` with `symmetric` and `idempotent` true; with a third record it also answers
`associative`. The test suite does this for the Rust, Java, Go and C++ modules (`counter_merge_converges`),
and runs all four across two real clusters shipping to each other (`counter_converges_two_clusters`): dc1
adds 2 while dc2 adds 5, and both sides land on 10.

## The limit, and the next step

This counter cannot go down. A decrement would shrink a site's entry, and the per-site maximum would throw
the shrink away at the next merge with any replica that still has the larger value. [Step 2](pncounter.md)
lifts that without touching the merge: it adds a second map.

## The sources

Rust, `aerospike-udf-sdk/wasm-rust/examples/counter`:

```rust
{{#include ../../../wasm-rust/examples/counter/src/lib.rs}}
```

Java, `aerospike-udf-sdk/wasm-java/examples/counter`:

```java
{{#include ../../../wasm-java/examples/counter/src/main/java/com/aerospike/udf/examples/Counter.java}}
```

Go, `aerospike-udf-sdk/wasm-go/examples/counter`:

```go
{{#include ../../../wasm-go/examples/counter/main.go}}
```

C++, `aerospike-udf-sdk/wasm-cpp/examples/counter`:

```cpp
{{#include ../../../wasm-cpp/examples/counter/counter.cpp}}
```
