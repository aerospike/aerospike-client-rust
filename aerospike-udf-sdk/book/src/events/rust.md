# Rust

`event!` declares an event handler the way `export!` declares UDFs: the parameter types are part of the
arm's grammar, the inputs are decoded and the answer delivered by the macro, and the signature
`event xdr-merge/1 name(...)` is exported beside the handler. A handler with the wrong shape does not
compile. There is one event, `xdr-merge`, and so one arm.

```rust
use aerospike_udf_sdk_wasm::{self as as_udf, event, Error, Pair, Resolution};

event! {
    on xdr_merge
    fn merge_account(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error> {
        let pair = Pair::of(current, incoming);
        let out = as_udf::map(4)?;

        if let Some(visits) = pair.int("visits")?.max() {
            out.insert("visits", visits)?;
        }

        Ok(Resolution::Record { bins: pair.with_newer(out)?, ttl: None })
    }
}
```

The type name in the arm (`EventRecord`) is a token the macro matches, not a name resolved in
your module, so it is not imported; `Resolution` and `Pair`, which you construct, are.

## The arm

| Arm | Signature | Answer |
| --- | --- | --- |
| `on xdr_merge` | `fn f(current: EventRecord, incoming: EventRecord) -> Result<Resolution, Error>` | `Resolution::KeepLocal`, `TakeRemote`, `Policy`, `Delete`, `Record { bins, ttl }` |

## Records

An `EventRecord` is a read-only record with its metadata. Its bins come through the event's own import, never
the record natives (which answer "nothing" in a handler), as `Option`s, since on either side of a merge a
bin may not exist: `int("name")`, `double`, `bool`, `string`, `blob`, `list`, `map` return `Ok(None)` for an absent or nil
bin and `Err(WrongType)` for another kind; `bin("name")` returns the raw `Value` or `Err(Absent)`.
`bin_names()` is the list of names, `deleted_bins()` the names this side deleted (bin deletes, or
tombstones) and `deleted_any()` whether there are any, `ttl()`, `source_id()`, and per bin
`bin_last_update_time(name)` and `bin_source_id(name)` where the server keeps them.
`generation()` and `last_update_time()` are zero on both sides: each cluster sets its own, and a merge
must not read them.

`with_bins(overrides)` builds a new record from this one's bins with `overrides` on top, by position: the
way a handler returns "the record, plus these". It prefers one side, so in a time-based resolver use
`Pair::with_newer` instead.

## The merge

`xdr_merge` hands over two records and nothing else: `current` (this node's record) and `incoming` (the
write that arrived over XDR). What the server knows about the merge is asked of the record it is true of:

| Method | True of | Says |
| --- | --- | --- |
| `incoming.is_delete()` | incoming | the write is a record delete, so `incoming` has no bins |
| `incoming.is_partial()` | incoming | the write carries only the bins that changed, so a bin it lacks may exist on the other cluster |
| `current.is_tombstone()` | current | the record exists but has no bins |
| `uses_bin_convergence()` | both, the same answer | per-bin timestamps are meaningful: check before trusting `bin_last_update_time` |

They are ordinary methods of `EventRecord`; asked of the other side they answer `false`, or an empty list.

`Pair::of(current, incoming)` reads a bin from both
sides into a `Both<T>`, whose members are `current` and `incoming`, with the merges a resolver reaches for, `max`, `min`, `sum` (`None` when neither side has the bin, as `max` and `min`; it wraps on overflow), `merge(f)`, and the
orderings `newer`, `older`, `ordered()`, which compare the bin's own last update time on each side and then the source
id, the order the server's bin convergence uses, so they are symmetric. `pair.incoming_is_newer(name)` is the
same test for a bin by name; `pair.with_newer(overrides)` fills in every bin the handler did not decide
from the newer side, which is the policy's own answer: a bin the newer side deleted (it is in that side's
`deleted_bins()`) stays deleted, rather than coming back from the older side.

A data-only merge has no times to order by: it keeps versions in the record and breaks a tie between two
values with `Value::cmp_stored(&a, &b)`, the stored bytes of the two compared, which both clusters compute
alike (exactly `Less`, `Equal` or `Greater`; values that cannot be compared are `Err(WrongType)`); `Value::stored_bytes(buf)` and `Coll::bytes(buf)` hand over the bytes themselves. `now()` fails in a
handler, so a merge cannot read the clock by accident.

## The modules

`examples/resolver`, the time-based account merge:

```rust
{{#include ../../../wasm-rust/examples/resolver/src/lib.rs}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the [CRDT tutorial](../crdt/README.md).

## API reference

Generated from `aerospike-udf-sdk/wasm-rust/src/lib.rs`: the event surface. The macro's own plumbing
(`xdr_merge_inputs`, `deliver_resolution`) is hidden in the source and left out here.

{{#include ../api/rust-events.md}}
