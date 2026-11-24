# Step 3: a set with removals

The two counters of steps [1](counter.md) and [2](pncounter.md) merge with a maximum. This step changes
the data from numbers to a collection, a set of tags on a record, and the merge from a maximum to a
union. It also introduces the idea the rest of the tutorial turns on: **a removal is data**. The structure
is the 2P-Set (two-phase set), and it is step 2's second-structure trick applied to a set.

## The idea

A set that only grows is already a CRDT. Two sites add tags, the merge takes the union, and a union is
commutative, idempotent and associative with no metadata at all; the `tags` bin of the resolver example
in the events part merges exactly this way. What a union cannot do is remove: a replica that still holds
the tag puts it back at the next merge, and the removal is lost, every time. The same wall step 1 hit
with decrement.

Step 2's answer was to never subtract and add to a second map instead. Here it is to never remove and
add to a second list instead:

```text
added:   ["art", "news", "sport"]
removed: ["news"]
```

A tag is a member when it is in `added` and not in `removed`. Adding puts the tag in `added`; removing
puts it in `removed`; neither list ever loses an element. The merge takes the union of the two `added`
lists and the union of the two `removed` lists, and since each is a grow-only set, the pair converges.

A tag in `removed` is a **tombstone**. It carries the fact of the removal to every replica, and it beats
a concurrent add on the other side: if dc1 adds `news` while dc2 removes it, the merged record has
`news` in both lists, and it is not a member. That is the right answer for a removal that raced an add,
and it is the rule the device map's removal markers in step 5 follow.

{{#include diagrams/twopset-race.svg}}

Both lists are kept **sorted and without duplicates**. The server checks the three laws by comparing the
merged records' stored bytes, and decides that a merge changed nothing the same way, so the same set
must always be the same bytes. A list that depended on insertion order would fail the symmetry check
even though the sets were equal.

## The merge

In Rust, the counter's merge with the table changed from counts to a sorted set of tags and the
maximum changed to a union:

```rust
event! {
    on xdr_merge
    fn merge_twopset(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let out = as_udf::map(2)?;

        for bin in ["added", "removed"] {
            let mut tags = Tags::new();

            tags.offer_all(current.list(bin)?)?;
            tags.offer_all(incoming.list(bin)?)?;
            out.insert(bin, &tags.into_list(None)?)?;
        }

        Ok(Resolution::Record { bins: incoming.with_bins(out)? })
    }
}
```

`Tags` is a fixed array of `Str<32>` kept in order by `insert`, which finds the tag's position and
does nothing when the tag is already there: offering every item of both lists to it is the union.
`into_list` writes the tags out in order, skipping those in an `except` set, which is how the members
are computed from the same table.

The same in Java, with the sorted table as a small class:

```java
@On.XdrMerge("merge_twopset")
static Resolution mergeTwoPSet(EventRecord current, EventRecord incoming) {
    MapBuilder out = Udf.map(2);

    for (String bin : new String[] {"added", "removed"}) {
        ADDED_TAGS.size = 0;
        ADDED_TAGS.offerAll(current.getList(bin, null));
        ADDED_TAGS.offerAll(incoming.getList(bin, null));
        out.put(bin, ADDED_TAGS.toList(null));
    }

    return Resolution.record(incoming.withBins(out));
}
```

In Go, where a `map[string]bool` is the set and `sort.Strings` puts the output in order:

```go
//udf:on xdr-merge merge_twopset
func mergeTwoPSet(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	out := udf.NewMap(2)

	for _, bin := range []string{"added", "removed"} {
		t := tags{}

		for _, side := range []*udf.EventRecord{current, incoming} {
			// A side without the bin has no tags to offer; a bin that is not a list is an error.
			stored, _, err := side.List(bin)
			if err != nil {
				return udf.Resolution{}, err
			}

			if err := t.offerAll(stored); err != nil {
				return udf.Resolution{}, err
			}
		}

		out.Put(bin, t.toList(nil))
	}

	return udf.Resolve(incoming.WithBins(out)), out.Err()
}
```

And in C++, the sorted array of `Text<32>` with an insertion that shifts the tail:

```cpp
UDF_ON_XDR_MERGE(merge_twopset)
{
	static Tags t;
	const char* bins[] = {"added", "removed"};
	const udf::EventRecord* sides[] = {&current, &incoming};
	udf::Map out(2);

	for (const char* bin : bins) {
		t.n = 0;

		for (const udf::EventRecord* side : sides) {
			if (udf::Optional<udf::Value> stored = side->get(bin)) {
				UDF_TRY(t.offer_all(*stored));
			}
		}

		out.put(bin, t.to_list(nullptr));
	}

	return udf::Resolution::record(incoming.with_bins(out));
}
```

## The write UDFs

`add(tag)` puts the tag in `added` if it is not there, `remove(tag)` puts it in `removed`, and both
answer the members, as does `members()`. Two rules keep the record honest:

- `remove` refuses a tag that is not a member, so a tombstone is only ever written for something that
  was there. The merge would cope with a stray tombstone, but the application should not be told a
  removal succeeded when there was nothing to remove.
- `add` of a tag that is in `removed` **succeeds and changes nothing**. `added` already has the tag,
  `removed` still has it, and the members are what they were. The module does not refuse the call,
  because from the application's side the add is legitimate; it is the structure that cannot express it.

That second rule is the limit of this step, and it is worth seeing why it cannot be fixed inside the
2P-Set. Suppose `add` removed the tag from `removed` to let it back in. The next merge with any replica
that still has the tombstone would put it back, and the re-add would be lost; worse, the laws would
break, because the result would depend on which replica merged first. A grow-only list cannot forget,
and a list that forgets is not grow-only.

## Deploying and checking it

Bind the handler on both clusters, with the write abandoned if the handler ever fails:

```text
event-set:event=xdr-merge;namespace=test;set=tags;module=twopset;function=merge_twopset;on-failure=abandon
```

`twopset_merge_converges` in `tests/src/events.rs` runs the four modules through the dry run on one
node. Adds come out sorted whatever order they go in, a second add changes nothing, a removal makes the
re-add a no-op, and removing a non-member is refused. Then the dry run: a record with `added: [art,
news]` and `removed: [news]` merged with an incoming `added: [art, sport], removed: [sport]` gives
`added: [art, news, sport]` and `removed: [news, sport]`, `symmetric` and `idempotent`, and
`associative` with a third record that has `art` removed.

`twopset_converges_two_clusters` in `tests/src/xdr_merge.rs` runs the four across two live clusters:
dc1 adds `art` while dc2 removes `news`, and both sides land on `[art]`; then dc1 adds `news` back and
dc2 adds `music`, and both sides land on `[art, music]`, the re-add having done nothing on either.

## The limit, and the next step

Two limits, and they are the same fact seen twice. A removed tag can never come back, because `removed`
never forgets; and `removed` grows for as long as the record lives, one tombstone per tag ever removed,
because it never forgets. The 2P-Set remembers *whether* a tag was removed. To let a tag back in, the
record has to remember *when*, so that a later add can outrank an earlier removal. [Step 4](lwwset.md)
puts a timestamp on every add and every removal, and the merge becomes step 2's again.

## The sources

Rust, `aerospike-udf-sdk/wasm-rust/examples/twopset`:

```rust
{{#include ../../../wasm-rust/examples/twopset/src/lib.rs}}
```

Java, `aerospike-udf-sdk/wasm-java/examples/twopset`:

```java
{{#include ../../../wasm-java/examples/twopset/src/main/java/com/aerospike/udf/examples/TwoPSet.java}}
```

Go, `aerospike-udf-sdk/wasm-go/examples/twopset`:

```go
{{#include ../../../wasm-go/examples/twopset/main.go}}
```

C++, `aerospike-udf-sdk/wasm-cpp/examples/twopset`:

```cpp
{{#include ../../../wasm-cpp/examples/twopset/twopset.cpp}}
```
