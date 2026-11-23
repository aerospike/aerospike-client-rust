# Step 5: the device map

The four steps so far each lifted one limit: step 2 gave the counter a decrement, step 3 gave a set a
removal, step 4 gave the removed element a way back. What none of them has is an element that can be
**changed**, a removal that does not stay in the record forever, or an ordering of writes that does not
depend on a clock. This step has all three. It is the structure the tutorial was building towards: an
active-active record that two data centres both write, merged by an `xdr-merge` handler that reads
nothing but the data, so that both sides compute the same record in any delivery order and however many
times a write is delivered. The module is `examples/crdt` in each SDK; the test suite drives all four
through the server's dry run, which checks the three laws on real bytes.

The scenario is a subscriber's devices. Each device is a small map in a list bin, and the application
changes devices one at a time: it adds one, updates one, removes one. Both data centres take those
writes, and XDR ships each to the other.

## The data model

Two bins carry the state.

**`devices`** is a list of maps, one per live device, sorted by `deviceId`. Each entry has at least
`deviceId` and `version`, an integer the write UDFs raise on every change to that device, plus whatever
the application stores:

```text
devices: [
  {deviceId: "device-A", version: 102, state: "SUSPENDED"},
  {deviceId: "device-B", version: 201, state: "ACTIVE", name: "phone"},
]
```

The version is what replaces step 4's clock. It is raised by the write UDF, on whichever side the change
is made, to one above the device's last version seen there, live or removed. Two sides that change one
device concurrently will both raise it to the same number, which is the tie the merge has to break; two
changes made one after the other, with a merge in between, carry different numbers, whatever the clocks
on the two sides say.

**`devicesRemoved`** remembers removals, because a merge that only sees live entries would resurrect a
removed device from the side that has not heard of the removal yet, exactly as step 3's union would
without its `removed` list. It holds a marker per removed device under `markers`, keyed by `deviceId`,
with the application's time of removal and the version the removal took; a purge horizon
`purgedBefore`, before which markers have been dropped; and, once a purge has dropped a marker, a version
floor `versionFloor`:

```text
devicesRemoved: {
  markers: {"device-C": {offAt: 1790000000000, version: 7}},
  purgedBefore: 1789395200000,
  versionFloor: 102,
}
```

The markers are step 3's tombstones with a version attached. The horizon is what steps 3 and 4 lacked: a
way to drop a tombstone once it has done its work. The bin is absent while there is nothing to
remember. Every map a guest builds is key-ordered when stored, so the same logical state is always the
same bytes, which the tie-break and the server's "unchanged" check both rely on.

## The merge

Every device on either side, live entry or removal marker, goes in as a candidate, and for each device
the higher candidate wins:

1. the higher **version**;
2. at equal versions, the **removal**;
3. at equal versions and kinds, the greater **stored bytes** of the two values.

The first rule carries the application's intent: a change the application made later has a higher
version, on whichever side it was made. The second is step 4's bias, removal over add at a tie, now at
equal versions. The third is new: the tie between two different changes made at one version on two
sides at once, which no rule can call right; what matters is that both sides call it the same way, and
comparing the two values' stored bytes with `Value::cmp_stored` does that, since each side compares the
same two byte strings.

Nothing in the three rules reads a clock, a last-update time, a generation or a source id. The server
withholds all four from an `xdr-merge` handler anyway, zeroing `gen` and `lut` in the payload and
failing the clock import, because a merge that reads any of them computes a different answer on each
cluster.

The result is the sorted live list, the markers still inside the purge horizon (the higher of the two
sides' horizons), and the higher of the two version floors. A marker the horizon drops carries its
version into the floor, so that a device re-added on this side after the other side purged its marker
still starts above any copy of that marker left in flight. Every other bin is the incoming one's, which
is what XDR would have written on its own.

```rust
event! {
    on xdr_merge
    fn xdr_merge(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        ...
        for side in [&current, &incoming] {
            if let Some(devices) = side.list("devices")? {
                offer_live(&mut best, devices)?;
            }
            offer_markers(&mut best, side.map("devicesRemoved")?, &mut horizon, &mut floor)?;
        }
        best.sort();
        ...
        Ok(Resolution::Record { bins: current.with_bins(out)?, ttl: None })
    }
}
```

A merge whose result equals the current record writes nothing: the server compares the answer with the
stored record and tells the source `lost-conflict`, which it counts as success. That is what makes
redelivery and the mirror pair on the other cluster free.

## The write UDFs

The merge is only correct for records the application writes through three UDFs, which keep the two
invariants it needs: the list is sorted by `deviceId`, and every new version is one above the device's
last version, live or removed, and never below the floor.

| UDF | Arguments | What it does |
| --- | --- | --- |
| `update_device` | `id`, `changes` (a map) | adds the device, updates it, or adds a removed one back: the entry is the old one with `changes` on top and the next version; a marker for the device is dropped |
| `remove_device` | `id`, `now` (ms, the application's clock) | removes the live entry and leaves a marker with `offAt = now` and the next version; purges markers older than the purge age |
| `purge_devices` | `now` | drops markers older than the purge age before `now`, folding their versions into the floor, and stores the list sorted |

The purge age (a week in the example) must exceed the longest outage between the clusters plus the
replication lag. A marker purged before the other side has seen the removal lets that side's live entry
come back; the version floor limits the damage to devices re-added in that window, but the age is the
real guard. This is the price of dropping tombstones, and it is why steps 3 and 4 kept theirs: a bounded
removal map needs an assumption about how far behind a replica can be.

{{#include diagrams/device-map-horizon.svg}}

`now` comes from the application, never from the server. Step 4 stamped with the server's clock and
accepted the skew; here the clock only decides when a marker is old enough to purge, a judgement with a
week of slack, and the ordering of writes is the version's job. The handler cannot read a clock, and the
write UDFs do not either, so both sides agree on every `offAt`.

## Deploying it

Bind the handler to the set on both clusters, with the write dropped rather than retried if the handler
ever fails, and give the source its id to ship:

```text
event-set:event=xdr-merge;namespace=test;set=subscribers;module=crdt;function=xdr_merge;on-failure=abandon
```

```text
xdr {
    src-id 1          # 2 on the other cluster; event-set refuses the binding while it is 0
    ship-src-id true  # unless bin convergence already ships bin LUTs
    ...
}
```

Bin convergence (`conflict-resolve-writes`) is not needed and not wanted: the merge decides every bin
itself. `trigger` stays at its default, `always`, so every incoming XDR write to an existing subscriber
runs the merge; a create bypasses it.

## Checking it with the dry run

Both clusters are needed to see the merge fire, but one is enough to check that it is a CRDT. The dry
run runs the bound handler against the real current record and an incoming record you describe, writes
nothing, and reports the three laws checked on the stored bytes of the results:

```rust
let answer = client
    .execute_udf(&WritePolicy::default(), &key, "__event__", "xdr-merge", &[
        Value::from(incoming_bins),
        Value::from(HashMap::from([(Value::from("ttl"), Value::from(0)), (Value::from("src-id"), Value::from(2))])),
        Value::from(other_bins),   // optional: the third record for associativity
    ])
    .await?;
// answer: {verdict, bins, ttl, symmetric, idempotent, associative}
```

- **symmetric**: merging (current, incoming) and (incoming, current) gives the same record, so the two
  clusters, each merging the mirror pair, agree;
- **idempotent**: merging the result with the incoming record again changes nothing, so redelivery is
  harmless;
- **associative**, given a third record: `(current ⊕ incoming) ⊕ other` equals
  `current ⊕ (incoming ⊕ other)`, so three or more sites agree whatever the order writes reach them in.

`crdt_merge_converges_in_the_eight_scenarios` in `tests/src/events.rs` seeds the record through the write
UDFs and runs nine scenarios through it for each of the four modules, asserting the laws on each:
concurrent updates to different devices, a same-version tie, adds on both sides, a removal racing an
update, a re-add, a purge, a record written before this model with an unsorted list, a write right after
a purge racing a stale marker, and a marker dropped by the other side's horizon. A handler that passes
these on real bytes has the contract. `tests/src/xdr_merge.rs` then runs the module between two live
clusters, including the case where a merge changes nothing and the server elides the write.

## The same module in Java

Everything the merge decides is the same. `higher()` is the three rules in the same order:

```java
private static Candidate higher(Candidate a, Candidate b) {
    if (a.version != b.version) {
        return a.version > b.version ? a : b;
    }

    if (a.removed != b.removed) {
        return a.removed ? a : b;
    }

    return a.value.cmpStored(b.value) >= 0 ? a : b;
}
```

`Value.cmpStored` compares the two candidates' stored bytes without copying either. The candidates are
collected the same way, sorted by `deviceId` bytes, and written out as the live list plus the markers
inside the higher horizon, a dropped marker's version folded into the floor. What differs is the
language:

- **Ids are strings, ordered as bytes.** A device id is a `String`, checked against the 64-byte limit when
  it is read. `compareIds` orders two ids by their UTF-8 bytes, the order the server keeps a key-ordered
  map's keys in, which `String.compareTo` does not give once a surrogate pair is involved.
- **For-each over the stored collections.** `for (Value device : devices)` over a `ListValue` and
  `for (MapValue.Entry e : markers)` over a `MapValue` are the Rust `iter()` and `entries()` loops, the
  form steps 1 to 4 use. `entry.get("version")` reads a field of a stored map, and a candidate keeps the
  `Value` of its entry or marker by position, so it goes back into the result uncopied.
- **Lists that grow.** `Best` holds an `ArrayList` of `Candidate` objects and sorts it with
  `Collections.sort`; the write UDFs collect the live entries in an `ArrayList` of `Entry` objects. Both
  stop at 128 devices, the limit of the Rust module's fixed arrays.
- **No `Option`, no `Result`.** A missing value is a `null` or a default (`intOr`, `mapOr`). A helper that
  meets a malformed record throws a private `Malformed` exception, and the exported method turns that into
  a thrown `UdfException` that names the function, which the generated export delivers as the failure
  message.
- **The handler's shape.** `@On.XdrMerge("xdr_merge")` on `static Resolution xdrMerge(EventRecord current,
  EventRecord incoming)`, returning `Resolution.record(current.withBins(out))` over the
  `MapBuilder` it filled, and `@Function` on the three write UDFs, each taking the `Record` first and then
  typed parameters, staging its bins with `rec.set` and writing them with `rec.save()`. The processor
  generates `CrdtExports`.
- **The rule this backend imposes.** An array's address is valid only until the next allocation, because
  the collector may move it and this backend cannot pin. The SDK takes every address inside the expression
  that hands it to the host, so the module never has to think about it.

## The same module in Go

`higher` is the same three rules:

```go
func higher(a, b candidate) (candidate, error) {
	if a.version != b.version {
		if a.version > b.version {
			return a, nil
		}
		return b, nil
	}

	if a.removed != b.removed {
		if a.removed {
			return a, nil
		}
		return b, nil
	}

	order, err := udf.CmpStored(a.value, b.value) // -1, 0 or 1; an error for a value not stored
	if err != nil {
		return candidate{}, err
	}
	if order >= 0 {
		return a, nil
	}
	return b, nil
}
```

- **Go maps do the bookkeeping.** `best` is a `map[string]candidate`, `offer` is a lookup and a `higher`,
  and `sorted()` sorts the ids with `sort.Slice`, which compares strings bytewise, the order the server
  keeps a key-ordered map's keys in. The Rust module keeps a fixed array of 128 candidates because it has
  no allocator; the Go module allocates per call, which an instance that lives for one call can afford.
- **Range over the stored collections.** `for item := range list.List()` and `for key, marker := range
  markers.Map()` are the Rust `iter()` and `entries()` loops; `entry.Lookup("version")` reads a field of a
  stored map, and the entry itself, a `udf.Value` by position, goes back into the result list uncopied.
- **Errors, not `Result`.** Every helper returns `(T, error)` and the handler returns the first one. A
  panic would trap the module with a message that says only that, so the module never relies on one.
- **The builders.** `udf.NewMap(3).Put("markers", markers).Put("purgedBefore", horizon)` chains, and
  `e.PutAll(old.value)` then `e.PutAll(changes)` is the "old entry with the changes on top" of
  `update_device`. The first refusal surfaces through `Err()`.
- **The handler's shape.** `//udf:on xdr-merge xdr_merge` on `func xdrMerge(current, incoming
  *udf.EventRecord) (udf.Resolution, error)`, returning
  `udf.Resolve(current.WithBins(out))`, and `//udf:function` on the three write UDFs. `udfgen` writes
  `udf_exports.go`.

## The same module in C++

`higher` again:

```cpp
static const Candidate& higher(const Candidate& a, const Candidate& b)
{
	if (a.version != b.version) return a.version > b.version ? a : b;
	if (a.removed != b.removed) return a.removed ? a : b;

	udf::Result<int32_t> order = a.value.cmp_stored(b.value);   // both are stored entries
	return !order || *order >= 0 ? a : b;
}
```

- **Fixed storage, like Rust.** There is no allocator, so `Best` is an array of 128 candidates with an
  insertion sort, and the write UDFs keep their entries in static arrays; a device id is a `Text<64>`, the
  Rust `Str<64>`. The big tables are static rather than on the 64 KB stack.
- **Range-for over the stored collections.** `for (udf::Value item : list.list())` and
  `for (udf::Entry e : markers->map())` are the Rust loops; `entry.get("version")` reads a field of a
  stored map, and the entry itself, a `udf::Value` by position, goes back into the result list uncopied.
- **`Result` and `UDF_TRY`.** Every helper returns a `udf::Result`, `UDF_TRY` propagates the first error out
  of the handler, and the export delivers it as the failure message. The code reads like the Rust with `?`.
- **The builders.** `udf::Map removed(3); removed.put("markers", markers).put("purgedBefore", horizon)`
  chains, and `e.put_all(entries[i].value)` then `e.put_all(changes)` is the "old entry with the changes on
  top"; `UDF_TRY(e.result())` surfaces a refused put.
- **The handler's shape.** `UDF_ON_XDR_MERGE(xdr_merge)` defines
  `Result<Resolution> xdr_merge(const EventRecord& current, const EventRecord& incoming)`,
  returning `udf::Resolution::record(current.with_bins(out))`, and `UDF_FUNCTION` the three write UDFs,
  `fn update_device(id: str[64], changes: any) -> value` among the signatures the macros derive.

## Where the tutorial leaves you

Five structures, one method. Find a representation in which every operation the application needs is an
addition to something that only grows, choose the merge that representation makes correct, a maximum or
a union per entry, and write the operations as UDFs that keep the representation's invariants. When the
data needs to forget, as the device map's markers do, name the assumption that makes forgetting safe
and bound it. When two sides can make the same change at once, decide the tie on bytes both sides have.
Then let the dry run tell you whether the laws hold, and the two-cluster test whether they hold in
practice.

One more step follows, for the case this one deliberately avoided: [step 6](registers.md) trusts the
clock again, and reads the per-bin stamps the server keeps on a namespace with bin convergence.

## The sources

Rust, `aerospike-udf-sdk/wasm-rust/examples/crdt`:

```rust
{{#include ../../../aerospike-udf-sdk/wasm-rust/examples/crdt/src/lib.rs}}
```

Java, `aerospike-udf-sdk/wasm-java/examples/crdt`:

```java
{{#include ../../../aerospike-udf-sdk/wasm-java/examples/crdt/src/main/java/com/aerospike/udf/examples/Crdt.java}}
```

Go, `aerospike-udf-sdk/wasm-go/examples/crdt`:

```go
{{#include ../../../aerospike-udf-sdk/wasm-go/examples/crdt/main.go}}
```

C++, `aerospike-udf-sdk/wasm-cpp/examples/crdt`:

```cpp
{{#include ../../../aerospike-udf-sdk/wasm-cpp/examples/crdt/crdt.cpp}}
```
