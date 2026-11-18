# Rust

`event!` declares an event handler the way `export!` declares UDFs: the parameter types are part of the
arm's grammar, the inputs are decoded and the answer delivered by the macro, and the signature
`event xdr-merge/1 name(...)` is exported beside the handler. A handler with the wrong shape does not
compile. There is one event, `xdr-merge`, and so one arm.

```rust
use aerospike_udf_sdk_wasm::{self as as_udf, event, Error, Resolution};

event! {
    on xdr_merge
    fn merge_account(local: EventRecord, remote: EventRecord, conflict: Conflict) -> Result<Resolution, Error> {
        let pair = conflict.pair();
        let out = as_udf::map(4)?;

        if let Some(visits) = pair.int("visits")?.max() {
            out.insert("visits", visits)?;
        }

        Ok(Resolution::Record { bins: pair.with_newer(out)?, ttl: None })
    }
}
```

The type names in the arm (`EventRecord`, `Conflict`) are tokens the macro matches, not names resolved in
your module, so they are not imported; `Resolution`, which you construct, is.

## The arm

| Arm | Signature | Answer |
| --- | --- | --- |
| `on xdr_merge` | `fn f(local: EventRecord, remote: EventRecord, conflict: Conflict) -> Result<Resolution, Error>` | `Resolution::KeepLocal`, `TakeRemote`, `Policy`, `Delete`, `Record { bins, ttl }` |

## Records

An `EventRecord` is a read-only record with its metadata. Its bins come through the event's own import, never
the record natives (which answer "nothing" in a handler), as `Option`s, since on either side of a merge a
bin may not exist: `int("name")`, `double`, `text`, `list`, `map` return `Ok(None)` for an absent or nil
bin and `Err(WrongType)` for another kind; `bin("name")` returns the raw `Value` or `Err(Absent)`. `bins()`
is the list of names, `deleted()` the names this side deleted (bin deletes, or tombstones), `ttl()`,
`source_id()`, and per bin `bin_lut(name)` and `bin_source_id(name)` where the server keeps them.
`generation()` and `last_update_time()` are zero on both sides: each cluster sets its own, and a merge
must not read them.

`with_bins(overrides)` builds a new record from this one's bins with `overrides` on top, by position: the
way a handler returns "the record, plus these". It prefers one side, so in a time-based resolver use
`Pair::with_newer` instead.

## The merge

`xdr_merge` hands over `local` (this node's record), `remote` (the incoming write) and a `Conflict`:
`trigger()` says whether the binding fires on every incoming write or only on a bin-convergence conflict,
`uses_bin_convergence()`, `lost_bins()`, `remote_is_partial()`, `incoming_is_delete()`,
`current_is_tombstone()` and `is_dry_run()` describe the merge. `conflict.pair()` reads a bin from both
sides into a `Both<T>` with the merges a resolver reaches for, `max`, `min`, `sum`, `merge(f)`, and the
orderings `newer`, `older`, `ordered()`, which compare the bin's own LUT on each side and then the source
id, the order the server's bin convergence uses, so they are symmetric. `pair.remote_is_newer(name)` is the
same test for a bin by name; `pair.with_newer(overrides)` fills in every bin the handler did not decide
from the newer side, which is the policy's own answer.

A data-only merge has no LUTs to order by: it keeps versions in the record and breaks a tie between two
values with `Value::cmp_stored(&a, &b)`, the stored bytes of the two compared, which both clusters compute
alike; `Value::stored_bytes(buf)` and `Coll::bytes(buf)` hand over the bytes themselves. `now()` fails in a
handler, so a merge cannot read the clock by accident.

## The modules

`examples/resolver`, the time-based account merge:

```rust
{{#include ../../../aerospike-udf-sdk-wasm/examples/resolver/src/lib.rs}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the [CRDT tutorial](../crdt/README.md).

## API reference

Generated from `aerospike-udf-sdk-wasm/src/lib.rs`: the event surface. The macro's own plumbing
(`xdr_merge_inputs`, `deliver_resolution`) is hidden in the source and left out here.

{{#include ../api/rust-events.md}}
