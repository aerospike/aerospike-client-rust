# Step 2: a counter that can go down

The grow-only counter of [step 1](counter.md) has one limit: it cannot subtract. This step lifts that
limit with the next CRDT up, the PN-Counter (positive-negative counter), which is two grow-only counters
glued together. It keeps the whole pattern of step 1, a merge that decides on the data alone and the
write UDFs that keep the data in shape, and adds exactly one idea: **never subtract, add to a second
structure instead**. That idea carries the rest of the tutorial.

## The idea

Why can the grow-only counter not decrement? Its merge keeps the larger count per site, and it is right to
do so only because a site's own count never gets smaller. A decrement would make it smaller, and the next
merge with a replica that still holds the old value would quietly put the old value back. The decrement
would be lost, every time.

So the record keeps **two** maps of `site -> amount`, one of increments and one of decrements, and both
only ever grow:

```text
inc: {"1": 7, "2": 4}
dec: {"1": 2, "2": 0}
```

The counter's value is the sum of `inc` minus the sum of `dec`, here 9. A site is a cluster's XDR `src-id`, as for
the counter: an increment on the cluster with src-id 1 adds to `inc["1"]`, a decrement there adds to
`dec["1"]`. The merge is the counter's, applied to each map
separately: for every site, keep the larger amount. Both maps are grow-only counters, so the merge
inherits their properties, and the pair converges. The server's dry run checks the three laws on the
real bytes of both maps at once.

**What it cannot do is hold a floor.** Two sites can each see a stock of one, each take one, and the
merged value is a legitimate minus one. A bound like "never below zero" is not a property a merge can
keep: it needs coordination, or an escrow scheme where each site is given a budget of decrements it may
spend without asking. The test suite asserts the value goes negative on purpose, because a module that
tried to clamp it would break convergence.

## The merge

In Rust, the counter's merge inside a loop over the two bins:

```rust
event! {
    on xdr_merge
    fn merge_pncounter(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let out = as_udf::map(2)?;

        for bin in ["inc", "dec"] {
            let mut counts = Counts::new();

            counts.offer_all(current.map(bin)?)?;
            counts.offer_all(incoming.map(bin)?)?;
            out.insert(bin, &counts.into_map()?)?;
        }

        Ok(Resolution::Record { bins: incoming.with_bins(out)? })
    }
}
```

`Counts` is the counter's table of `(site, amount)` pairs, unchanged: `offer` keeps the larger amount for
a site, `offer_all` walks a stored map into it. The handler builds a map of the two merged bins and
returns it over the incoming record's other bins.

The same in Java, where the table is a small class with one static instance reused for both bins:

```java
@On.XdrMerge("merge_pncounter")
static Resolution mergePnCounter(EventRecord current, EventRecord incoming) {
    MapBuilder out = Udf.map(2);

    for (String bin : new String[] {"inc", "dec"}) {
        TABLE.size = 0;
        TABLE.offerAll(current.getMap(bin, null));
        TABLE.offerAll(incoming.getMap(bin, null));
        out.put(bin, TABLE.toMap());
    }

    return Resolution.record(incoming.withBins(out));
}
```

In Go, with a fresh `table` map per bin:

```go
//udf:on xdr-merge merge_pncounter
func mergePnCounter(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	out := udf.NewMap(2)

	for _, bin := range []string{"inc", "dec"} {
		t := table{}

		for _, side := range []*udf.EventRecord{current, incoming} {
			// A side without the bin has no entries to offer; a bin that is not a map is an error.
			stored, _, err := side.Map(bin)
			if err != nil {
				return udf.Resolution{}, err
			}

			if err := t.offerAll(stored); err != nil {
				return udf.Resolution{}, err
			}
		}

		out.Put(bin, t.toMap())
	}

	return udf.Resolve(incoming.WithBins(out)), out.Err()
}
```

And in C++, where the fixed-size table is `static` so that its 64 entries do not live on the stack:

```cpp
UDF_ON_XDR_MERGE(merge_pncounter)
{
	static Table t;
	const char* bins[] = {"inc", "dec"};
	const udf::EventRecord* sides[] = {&current, &incoming};
	udf::Map out(2);

	for (const char* bin : bins) {
		t.n = 0;

		for (const udf::EventRecord* side : sides) {
			if (udf::Optional<udf::Value> stored = side->get(bin)) {
				UDF_TRY(t.offer_all(*stored));
			}
		}

		out.put(bin, t.to_map());
	}

	return udf::Resolution::record(incoming.with_bins(out));
}
```

## The write UDFs

Three exports instead of two. `increment(n)` adds `n` to this cluster's entry of `inc`,
`decrement(n)` adds `n` to its entry of `dec`, and `value()` answers the difference of the
two sums. Both writes go through one helper that loads the map, adds, stores it and saves the record, and both
answer the counter's new value. The entry is the cluster's `src-id`, which the helper asks the server for,
as the counter's `increment` does; a cluster with none refuses the write.

The helper refuses a negative `n`. That is the invariant of this CRDT: an entry only grows, and a caller
who wants to subtract must say so by calling `decrement`. Letting `increment(-2)` through would
shrink `inc["1"]` and reintroduce exactly the lost update the second map exists to prevent. The Rust
module says it in one line, and the other three say the same:

```rust
if n < 0 {
    return Err(Error::Custom("the amount must not be negative: an entry only grows"));
}
```

## Deploying and checking it

Bind the handler on both clusters, with the write abandoned if the handler ever fails:

```text
event-set:event=xdr-merge;namespace=test;set=stock;module=pncounter;function=merge_pncounter;on-failure=abandon
```

`pncounter_merge_converges` in `tests/src/events.rs` runs the four modules through the dry run on one
node: the cluster with src-id 1 added three and took one, so it holds `inc {"1": 3}, dec {"1": 1}`; an
incoming record with `inc {"1": 1, "dc2": 5}, dec {"dc2": 2}` merges to `inc {"1": 3, "dc2": 5}, dec {"1":
1, "dc2": 2}`, with `symmetric` and `idempotent` true, and `associative` too once a third site's record is
added. It also checks that `increment` refuses a negative amount and leaves the record alone, and that
taking five from a stock of two answers minus three.

`pncounter_converges_two_clusters` in `tests/src/xdr_merge.rs` runs the four across two real clusters
shipping to each other: dc1 stocks three, dc1 takes two while dc2 adds five, and both sides land on six;
then both sides take four at once, and both land on minus two.

## The limit, and the next step

Two steps on counters are enough to see the shape of every state-based CRDT: find a representation whose
merge is a maximum or a union, and push every operation into something that only grows. What the counter
cannot hold is a floor, and no amount of restructuring gives it one; that is a job for coordination.

The next thing an application wants is not a number but a collection, with elements that come and go.
[Step 3](twopset.md) takes the second-structure idea to a set: the merge becomes a union, and a removal
becomes data of its own.

## The sources

Rust, `aerospike-udf-sdk/wasm-rust/examples/pncounter`:

```rust
{{#include ../../../wasm-rust/examples/pncounter/src/lib.rs}}
```

Java, `aerospike-udf-sdk/wasm-java/examples/pncounter`:

```java
{{#include ../../../wasm-java/examples/pncounter/src/main/java/com/aerospike/udf/examples/PnCounter.java}}
```

Go, `aerospike-udf-sdk/wasm-go/examples/pncounter`:

```go
{{#include ../../../wasm-go/examples/pncounter/main.go}}
```

C++, `aerospike-udf-sdk/wasm-cpp/examples/pncounter`:

```cpp
{{#include ../../../wasm-cpp/examples/pncounter/pncounter.cpp}}
```
