# Step 6: registers, on the server's clock

[Step 4](lwwset.md) kept a timestamp for every element inside the record, written by its own UDFs, and
[step 5](device-map.md) replaced the clock with versions the application raises. This step goes the
other way and lets the server keep the stamps. The server already records, for every bin, when it was
last written and by which cluster, and gives both to an `xdr-merge` handler for every bin of both sides.
That pair is all a last-writer-wins register needs.

The module is the smallest in the tutorial that reads `bin_last_update_time` and `bin_source_id`, and it
is the one to read before writing a merge that mixes the server's stamps with data of your own.

## The idea

A user's settings, one bin per setting:

```text
theme: "dark"      last written 1790000000100 by cluster 1
lang:  "fr"        last written 1790000000300 by cluster 2
email: (deleted)   last written 1790000000200 by cluster 2
```

Every bin is a register: it holds one value, and a write replaces it. The stamp of a register is the
pair (last-update time, source id), compared by time first and then by id, so two writes in the same
millisecond on two clusters still have an order, and both clusters agree on it. The merge of two
registers is the one with the later stamp.

A deleted bin keeps its stamp. The server remembers a deleted bin as a bin tombstone, with the time and the cluster of the delete, so a delete is a write like any other: it wins
when it is the later one, and loses to a write that came after it. Nothing in the record has to say
"removed", and nothing accumulates as step 3's and step 4's removal structures did. The server drops a
bin tombstone once it is older than the namespace's `xdr-bin-tombstone-ttl`.

A side that has never had a bin has no stamp for it at all, which is less than any stamp. That case is
common: with `bin-policy only-changed`, an incoming write carries only the bins that changed, and a bin
it lacks is not a bin it deleted.

## What the server does alone, and what it cannot

Picking the later stamp per bin is what bin convergence does without any handler. If every bin of your
record is a register, you do not need this module: leave the set unbound, turn bin convergence on and
stop. A set bound to a handler never uses bin convergence.

The handler is for the bin that is not a register. Here it is `seen`, the highest message number this
user has read on any device. If one cluster holds `seen = 9` and the other writes `seen = 5` a moment
later, the later write is the wrong answer: the user has read message 9. Its merge is the larger number,
whichever side wrote last, and no rule that picks a side can say that.

Once a handler is bound it decides the whole record, so it has to do the registers too. That is the
part of the module that reads the stamps.

## The merge

In Rust. A stamp is an `Option` of a pair, and Rust orders `None` below every `Some` and pairs by their
first element, then their second, so the comparison is a plain `>`:

```rust
/// Whether this side deleted the bin and still remembers that it did.
fn deleted(side: &EventRecord, name: &str) -> Result<bool, Error> {
    let mut buf = [0u8; 32];

    for bin in side.deleted_bins().iter() {
        if bin?.as_str(&mut buf)? == name {
            return Ok(true);
        }
    }

    Ok(false)
}

/// A register's stamp on one side: when the bin was last written there, and by which cluster. None when
/// this side has nothing to say about the bin, which is less than any stamp.
fn stamp(side: &EventRecord, name: &str) -> Result<Option<(i64, i64)>, Error> {
    if side.has(name) || deleted(side, name)? {
        Ok(Some((side.bin_last_update_time(name), side.bin_source_id(name))))
    } else {
        Ok(None)
    }
}

event! {
    /// The merge: `seen` is the larger number; every other bin is the side that wrote it last.
    on xdr_merge
    fn merge_settings(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let out = as_udf::map(16)?;

        if let Some(seen) = current.int("seen")?.max(incoming.int("seen")?) {
            out.insert("seen", seen)?;
        }

        let mut buf = [0u8; 32];

        for (side, is_incoming) in [(&current, false), (&incoming, true)] {
            for names in [side.bin_names(), side.deleted_bins()] {
                for name in names.iter() {
                    let name = name?;
                    let name = name.as_str(&mut buf)?;

                    // The first pass decided every bin the current side knows of.
                    if name == "seen" || (is_incoming && stamp(&current, name)?.is_some()) {
                        continue;
                    }

                    let winner = if stamp(&incoming, name)? > stamp(&current, name)? { &incoming } else { &current };

                    // A nil in the answer is "no such bin": the winner's delete, kept.
                    out.insert(name, winner.get(name)?.unwrap_or(Value::Nil))?;
                }
            }
        }

        Ok(Resolution::Record { bins: out })
    }
}
```

Three things to notice.

**The names come from four lists.** A bin either side holds, and a bin either side has deleted: a
register that exists only as a delete on one side still has to be decided, or the other side's older
value would survive it.

**The incoming side wins only when its stamp is strictly later.** Equal stamps are the same write, seen
twice, and keeping the current side then changes nothing.

**The answer is the whole record.** It does not start from either side with `with_bins`, as the earlier
steps did, because here no side is the default: every bin is decided by name.

The same in Java, where the comparison is written out:

```java
private static boolean incomingWins(EventRecord current, EventRecord incoming, String name) {
    if (!stamped(incoming, name)) {
        return false;
    }

    if (!stamped(current, name)) {
        return true;
    }

    long ours = current.binLastUpdateTime(name);
    long theirs = incoming.binLastUpdateTime(name);

    if (theirs != ours) {
        return theirs > ours;
    }

    return incoming.binSourceId(name) > current.binSourceId(name);
}

@On.XdrMerge("merge_settings")
static Resolution mergeSettings(EventRecord current, EventRecord incoming) {
    MapBuilder out = Udf.map(16);

    if (current.has(SEEN) || incoming.has(SEEN)) {
        out.put(SEEN, Math.max(current.getInt(SEEN, Long.MIN_VALUE), incoming.getInt(SEEN, Long.MIN_VALUE)));
    }

    decide(current.binNames(), current, incoming, out);
    decide(current.deletedBins(), current, incoming, out);
    decide(incoming.binNames(), current, incoming, out);
    decide(incoming.deletedBins(), current, incoming, out);

    return Resolution.record(out);
}
```

In Go, with the stamp as a small struct whose zero value is "nothing":

```go
// The side's stamp for a bin it has or lists as deleted; the zero stamp otherwise.
func stampOf(side *udf.EventRecord, name string) stamp {
	if side.Has(name) || side.Deleted(name) {
		return stamp{known: true, at: side.BinLastUpdateTime(name), sourceID: side.BinSourceID(name)}
	}

	return stamp{}
}

//udf:on xdr-merge merge_settings
func mergeSettings(current, incoming *udf.EventRecord) (udf.Resolution, error) {
	out := udf.NewMap(16)

	a, hasA, err := current.Int("seen")
	if err != nil {
		return udf.Resolution{}, err
	}

	b, hasB, err := incoming.Int("seen")
	if err != nil {
		return udf.Resolution{}, err
	}

	if hasA && hasB {
		out.Put("seen", max(a, b))
	} else if hasA {
		out.Put("seen", a)
	} else if hasB {
		out.Put("seen", b)
	}

	for _, side := range []*udf.EventRecord{current, incoming} {
		for _, names := range []iter.Seq[string]{side.BinNames(), side.DeletedBins()} {
			for name := range names {
				// The first pass decided every bin the current side knows of.
				if name == "seen" || (side == incoming && stampOf(current, name).known) {
					continue
				}

				winner := current

				if stampOf(incoming, name).after(stampOf(current, name)) {
					winner = incoming
				}

				// A nil in the answer is "no such bin": the winner's delete, kept.
				v, ok, err := winner.Get(name)
				if err != nil {
					return udf.Resolution{}, err
				}

				if ok {
					out.Put(name, v)
				} else {
					out.Put(name, nil)
				}
			}
		}
	}

	return udf.Resolve(out), out.Err()
}
```

And in C++:

```cpp
// The side's stamp for a bin it has or has deleted; no stamp otherwise.
static udf::Result<Stamp> stamp(const udf::EventRecord& side, udf::StrView name)
{
	if (!side.has(name) && !UDF_TRY(deleted(side, name))) return Stamp{};

	return Stamp{true, side.bin_last_update_time(name), side.bin_source_id(name)};
}

UDF_ON_XDR_MERGE(merge_settings)
{
	udf::Map out(16);
	udf::Optional<int64_t> mine = UDF_TRY(current.as_int(SEEN));
	udf::Optional<int64_t> theirs = UDF_TRY(incoming.as_int(SEEN));

	if (mine || theirs) {
		out.put(SEEN, mine && (!theirs || *mine > *theirs) ? *mine : *theirs);
	}

	const udf::EventRecord* sides[] = {&current, &incoming};

	for (const udf::EventRecord* side : sides) {
		const udf::ListRange lists[] = {side->bin_names(), side->deleted_bins()};

		for (const udf::ListRange& names : lists) {
			for (udf::Value bin : names) {
				Name name = UDF_TRY(udf::required(bin.as_string<NAME_MAX>(), "registers: a nil bin name"));
				Stamp here = UDF_TRY(stamp(current, name.view()));

				// The first pass decided every bin the current side knows of.
				if (name == SEEN || (side == &incoming && here.known)) continue;

				Stamp there = UDF_TRY(stamp(incoming, name.view()));
				const udf::EventRecord& winner = there > here ? incoming : current;

				// A nil in the answer is "no such bin": the winner's delete, kept.
				if (udf::Optional<udf::Value> v = winner.get(name.view())) {
					out.put(name.view(), *v);
				}
				else {
					out.put_nil(name.view());
				}
			}
		}
	}

	return udf::Resolution::record(out);
}
```

## The write side

A register needs no write UDF, which is the point of letting the server stamp. The application writes
`theme` with an ordinary put and deletes `email` by writing nil to it, from any client, and the server
stamps the bin.

`seen` has one, for the reason step 1's counter did:

```rust
export! {
    /// Records that message `n` was read. Returns the highest number read so far.
    fn saw(rec: &mut Record, n: i64) -> Result<Value, Error> {
        let seen = rec.int_or("seen", 0)?;

        if n > seen {
            rec.set("seen", n)?;
            rec.save()?;
        }

        Ok(Value::Int(seen.max(n)))
    }
}
```

A merge can only keep a maximum that the writes kept too. If a client puts `seen = 5` over a 9 that has
already arrived from the other cluster, that is not a conflict: it is one cluster lowering its own
value, and there is nothing left for a merge to compare it with. The two-cluster test of this step found
exactly that, with a plain put, before `saw` existed.

## What the cluster needs

The server gives the handler a time and a source for every bin of both sides, whatever the namespace's
settings. What makes the incoming times worth ordering by is the other cluster shipping them:

```text
xdr {
    src-id 1
    dc dc2 {
        namespace settings {
            bin-policy only-changed
            ship-bin-luts true
        }
    }
}
```

`ship-bin-luts` sends each bin's time with it, and a `bin-policy` of the changed-bins family sends only
the bins that changed. Each cluster needs its own `src-id`. A set bound to a handler never uses bin
convergence, so `conflict-resolve-writes` plays no part.

Without `ship-bin-luts` the merge still runs, but an incoming bin's time is then the time the write
arrived here: the later arrival wins, rather than the later write.

## What is shipped after a merge

The merge itself ships nothing. What it decides is how each bin is stamped, and so what this cluster's
own local writes carry when they ship:

- **The incoming value** is the other cluster's. It keeps the incoming stamp, the same one the other
  cluster holds, and no local write ships it.
- **This cluster's value, kept** is left exactly as it was. If it had not been shipped yet, because the
  merge landed first, it still ships with its own write.
- **A value the handler computed**, which is neither side's, is stamped as a write made here. The other
  cluster's handler, given the same two inputs, computes the same value without it.

The second case is what the two-cluster test of this step exposed: a merge used to mark every bin as
arrived from the other cluster, so a local write that was still waiting to ship when a merge landed was
never shipped, and the clusters stayed apart.

## Deploying and checking it

Bind the handler on both clusters:

```text
event-set:event=xdr-merge;namespace=settings;set=users;module=registers;function=merge_settings;on-failure=abandon
```

`registers_merge_converges` in `tests/src/events.rs` runs the four modules through the dry run, which
tells the handler what a live merge would. The incoming record it describes is, as a whole, from 1970,
but its `theme` and its delete of `email` are stamped 2030: the merge takes those two and keeps this
side's `lang`, takes a bin only the incoming side has, and keeps the larger `seen` though its stamp is
the older. The server reports the merge `symmetric` and `idempotent`; for the second it stamps the
merged record as a live merge would, since a merge that reads stamps cannot be repeated on a record that
has none.

`registers_converge_two_clusters` in `tests/src/xdr_merge.rs` runs the four between two live clusters:
writes to different bins at the same time both survive; of two writes to one bin the later wins on both
sides; `seen` stays 9 on both when the other side reports 5 afterwards; a delete that is the later write
stays a delete while the other side writes another bin; and a register written after its delete is
back.

## The limit

The clock is back. A stamp is as good as the clock of the cluster that wrote it, and everything
[step 4](lwwset.md#where-the-stamps-come-from) says about a cluster whose clock runs behind applies
here, to every bin. What this step saves is the bookkeeping: no stamps in the record, no UDF on the
write path for a register, no removal structure that grows.

And a register is replaced whole. Two clusters that each add an entry to one map bin lose one of the two
entries, because the bin has one stamp. A value that has to be merged inside is not a register, and
needs its own merge, as `seen` has here and as the elements of [step 5](device-map.md) have there.

## The sources

Rust, `aerospike-udf-sdk/wasm-rust/examples/registers`:

```rust
{{#include ../../../wasm-rust/examples/registers/src/lib.rs}}
```

Java, `aerospike-udf-sdk/wasm-java/examples/registers`:

```java
{{#include ../../../wasm-java/examples/registers/src/main/java/com/aerospike/udf/examples/Registers.java}}
```

Go, `aerospike-udf-sdk/wasm-go/examples/registers`:

```go
{{#include ../../../wasm-go/examples/registers/main.go}}
```

C++, `aerospike-udf-sdk/wasm-cpp/examples/registers`:

```cpp
{{#include ../../../wasm-cpp/examples/registers/registers.cpp}}
```
