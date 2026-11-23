# Step 4: a set with re-adds, on a clock

The 2P-Set of [step 3](twopset.md) remembers whether a tag was removed, and so can never take it back.
This step remembers **when** instead. Every add and every removal carries a timestamp, a tag is a member
when its add is later than its removal, and adding a removed tag back is nothing more than a later add.
The structure is the LWW-Element-Set (last-writer-wins element set), and its merge is the one from
[step 2](pncounter.md): for every tag of each map, the larger stamp.

It is also the first step where the two clusters can disagree about something, because it is the first
step that reads a clock. The chapter is as much about that as about the set.

## The idea

Two map bins, a stamp per tag in each, in milliseconds since the epoch:

```text
addedAt:   {"art": 1790000000100, "news": 1790000000300}
removedAt: {"news": 1790000000200}
```

`news` was added at 100, removed at 200 and added back at 300, so it is a member; `art` was never
removed. A tag is a member when `addedAt[tag] > removedAt[tag]`, with a missing removal counting as
never. The member test is a comparison instead of a lookup, and that one change lifts both of step 3's
limits at once: a re-add is a later stamp, and the removal map, while it still keeps a stamp per tag
ever removed, can at least be overridden rather than only appended to.

A stamp only ever grows for a tag on the site that writes it, so the later stamp is the later write,
and the merge is the PN-Counter's per-key maximum over each map. Both maps converge for the reason the
counters did, and so does the member set computed from them.

{{#include diagrams/lwwset-race.svg}}

**The tie.** If a tag's add and removal carry the same stamp, the comparison is undecided, and the module
must choose. This one chooses the removal: `>` rather than `>=`. Either choice converges, since both
sides apply the same rule to the same stamps; what matters is that the bias is stated, because it is
visible. A tie is rare between two clusters and impossible on one, as the next section shows, but a
rule that is never exercised is still a rule the merge needs.

## Where the stamps come from

The write UDFs stamp with the server's clock, `now()`, which is the node's wall-clock time in
milliseconds. The merge does not, and could not: the server refuses the clock import inside an event
handler, because a handler that read it would compute a different answer on each cluster. The clock is
read where the write happens, stored in the record, and shipped with it; the merge only compares what
is stored. That division is what keeps the merge a function of the data.

One site can write the same tag twice within a millisecond, which would tie, so the write UDF takes the
later of the clock and one past the tag's last stamp on either map. Two writes to one tag on one site
therefore never tie, and never go backwards, whatever the clock does:

```rust
fn stamp_for(rec: &Record, tag: &str) -> Result<i64, Error> {
    let last =
        stored(rec, "addedAt")?.get(tag).unwrap_or(0).max(stored(rec, "removedAt")?.get(tag).unwrap_or(0));

    Ok(as_udf::now()?.max(last + 1))
}
```

Between two sites the clock is the arbiter, and that is the honest limit of this design. If dc2's clock
runs a second behind dc1's, a removal on dc2 can lose to an add on dc1 that happened, by any observer's
watch, earlier. "Last writer wins" means last by the stamp, and the stamp is as good as the clock that
wrote it. The device map in step 5 replaces the clock with a version the application raises by one on
every change, which no clock can skew.

{{#include diagrams/lwwset-skew.svg}}

## The merge

In Rust, step 2's handler with the bins renamed. The table `Stamps` is the counter's `Counts`, keeping the
larger value per key:

```rust
event! {
    on xdr_merge
    fn merge_lwwset(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let out = as_udf::map(2)?;

        for bin in ["addedAt", "removedAt"] {
            let mut stamps = Stamps::new();

            stamps.offer_all(current.map(bin)?)?;
            stamps.offer_all(incoming.map(bin)?)?;
            out.insert(bin, stamps.into_map()?)?;
        }

        Ok(Resolution::Record { bins: incoming.with_bins(out)?, ttl: None })
    }
}
```

The same in Java:

```java
@On.XdrMerge("merge_lwwset")
static Resolution mergeLwwSet(EventRecord current, EventRecord incoming) {
    MapBuilder out = Udf.map(2);

    for (String bin : new String[] {"addedAt", "removedAt"}) {
        ADDED.size = 0;
        ADDED.offerAll(current.getMap(bin, null));
        ADDED.offerAll(incoming.getMap(bin, null));
        out.put(bin, ADDED.toMap());
    }

    return Resolution.record(incoming.withBins(out));
}
```

In Go:

```go
//udf:on xdr-merge merge_lwwset
func mergeLwwSet(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	out := udf.NewMap(2)

	for _, bin := range []string{"addedAt", "removedAt"} {
		s := stamps{}

		for _, side := range []*udf.EventRecord{current, incoming} {
			// A side without the bin has no stamps to offer; a bin that is not a map is an error.
			stored, _, err := side.Map(bin)
			if err != nil {
				return udf.Resolution{}, err
			}

			if err := s.offerAll(stored); err != nil {
				return udf.Resolution{}, err
			}
		}

		out.Put(bin, s.toMap())
	}

	return udf.Resolve(incoming.WithBins(out)), out.Err()
}
```

And in C++:

```cpp
UDF_ON_XDR_MERGE(merge_lwwset)
{
	static Stamps s;
	const char* bins[] = {"addedAt", "removedAt"};
	const udf::EventRecord* sides[] = {&current, &incoming};
	udf::Map out(2);

	for (const char* bin : bins) {
		s.n = 0;

		for (const udf::EventRecord* side : sides) {
			if (udf::Optional<udf::Value> stored = side->get(bin)) {
				UDF_TRY(s.offer_all(*stored));
			}
		}

		out.put(bin, s.to_map());
	}

	return udf::Resolution::record(incoming.with_bins(out));
}
```

A stored map comes out of the server key-ordered, so the merged maps are canonical bytes without any
sorting on the module's part. The members list is a different matter: a tag added on this site sits at
the end of the table, so `members()` picks the smallest remaining tag each time to answer in order.

## The write UDFs

`add(tag)` stamps the tag in `addedAt`, `remove(tag)` stamps it in `removedAt` and refuses a tag that
is not currently a member, and both answer the members. Compared with step 3, `add` of a removed tag now
does what the caller meant: the new stamp is later than the removal's, and the tag is back.

## Deploying and checking it

Bind the handler on both clusters, with the write abandoned if the handler ever fails:

```text
event-set:event=xdr-merge;namespace=test;set=tags;module=lwwset;function=merge_lwwset;on-failure=abandon
```

`lwwset_merge_converges` in `tests/src/events.rs` runs the four modules through the dry run on one node.
It adds `news` and `art`, removes `news`, adds it back and sees both members, which step 3 could not
do; it reads the record and checks that every stamp is a real clock reading and the re-add's is later
than the removal's. Then the dry run: an incoming record whose copy of `news` is stale, whose `sport`
was added long ago and whose `art` was removed one millisecond after this side added it merges to the
later stamp per tag in each map, `symmetric` and `idempotent`, and `associative` with a third record.
Last, the bias: the test writes a tag with equal add and removal stamps straight into the record with
the client, and `members()` does not list it, and a fresh `add` brings it back.

`lwwset_converges_two_clusters` in `tests/src/xdr_merge.rs` runs the four across two live clusters:
dc1 adds `art` while dc2 removes `news`, and both sides land on `[art]`; dc1 adds `news` back and both
sides land on `[art, news]`; dc2 removes it again and both land on `[art]`. The two test clusters share
a machine and so a clock, which is why the sequence is deterministic there; the chapter above says what
happens when they do not.

## The limit, and the next step

Two things remain. The clock, which this step trusts and which two real clusters do not share; and the
removal map, which can be overridden but never shrinks, one stamp per tag ever removed, for the life of
the record.

[Step 5](device-map.md) is the structure those four steps were building towards. Its elements are maps
rather than tags, so an entry can be updated, not only added and removed. Each carries a version the
application raises by one on every change, in place of the clock. Its removal markers carry a time and a
version, and a purge horizon drops them once they are old enough that no replica can still be carrying
the entry they cancel, with a version floor to guard the window in which that assumption is wrong. And
when two sides change one entry at one version, the merge breaks the tie on the stored bytes, the same
on both sides, in place of this step's bias.

## The sources

Rust, `aerospike-udf-sdk/wasm-rust/examples/lwwset`:

```rust
{{#include ../../../aerospike-udf-sdk/wasm-rust/examples/lwwset/src/lib.rs}}
```

Java, `aerospike-udf-sdk/wasm-java/examples/lwwset`:

```java
{{#include ../../../aerospike-udf-sdk/wasm-java/examples/lwwset/src/main/java/com/aerospike/udf/examples/LwwSet.java}}
```

Go, `aerospike-udf-sdk/wasm-go/examples/lwwset`:

```go
{{#include ../../../aerospike-udf-sdk/wasm-go/examples/lwwset/main.go}}
```

C++, `aerospike-udf-sdk/wasm-cpp/examples/lwwset`:

```cpp
{{#include ../../../aerospike-udf-sdk/wasm-cpp/examples/lwwset/lwwset.cpp}}
```
