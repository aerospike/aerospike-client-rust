# Rust

`event!` declares event handlers the way `export!` declares UDFs: one arm per kind, the parameter types
part of the arm's grammar, the inputs decoded and the answer delivered by the macro, and the signature
`event <kind>/1 name(...)` exported beside it. A handler with the wrong shape does not compile.

```rust
use aerospike_udf_sdk_wasm::{self as as_udf, event, Decision, Gate, Value};

event! {
    on before_write
    fn guard_account(before: Record, after: Record, write: Write) -> Result<Decision, Error> {
        if let Some(balance) = after.int("balance")? {
            if balance < 0 {
                return Ok(Decision::Reject { code: 22, message: "balance may not go negative" });
            }
        }

        let out = as_udf::map(2)?;

        out.put("stamped", write.source().as_str())?;
        out.put("writes", before.int("writes")?.unwrap_or(0) + 1)?;

        Ok(Decision::Record { bins: after.with_bins(out)?, ttl: None })
    }

    on before_delete
    fn guard_delete(record: Record, _delete: Delete) -> Result<Gate, Error> {
        if matches!(record.bin("locked"), Ok(Value::Bool(true))) {
            return Ok(Gate::Refuse { code: 22, message: "record is locked" });
        }

        Ok(Gate::Allow)
    }
}
```

The type names in an arm (`Record`, `Write`, `Decision`, ...) are tokens the macro matches, not names
resolved in your module, so they are not imported; the answer enums you construct are.

## The arms

| Arm | Signature | Answer |
| --- | --- | --- |
| `on before_write` | `fn f(before: Record, after: Record, write: Write) -> Result<Decision, Error>` | `Decision::Accept`, `Reject { code, message }`, `Record { bins, ttl }` |
| `on before_delete` | `fn f(record: Record, delete: Delete) -> Result<Gate, Error>` | `Gate::Allow`, `Refuse { code, message }` |
| `on after_write` | `fn f(before: Record, after: Record, write: Write) -> Result<(), Error>` | `Ok(())` |
| `on after_delete` | `fn f(record: Record, delete: Delete) -> Result<(), Error>` | `Ok(())` |
| `on before_expire` | `fn f(record: Record, expire: Expire) -> Result<Expiry, Error>` | `Expiry::Expire`, `Keep { ttl }` |
| `on before_evict` | `fn f(record: Record, evict: Evict) -> Result<Eviction, Error>` | `Eviction::Evict`, `Protect` |
| `on after_read` | `fn f(record: Record, read: Read) -> Result<ReadResult, Error>` | `ReadResult::Unchanged`, `Record { bins }` |
| `on before_truncate` | `fn f(truncate: Truncate) -> Result<Gate, Error>` | `Gate` |
| `on after_truncate` | `fn f(truncate: Truncate) -> Result<(), Error>` | `Ok(())` |
| `on before_ship` | `fn f(record: Record, ship: Ship) -> Result<ShipDecision, Error>` | `ShipDecision::Ship`, `Skip`, `Record { bins }` |
| `on partition_transfer` | `fn f(transfer: Transfer) -> Result<(), Error>` | `Ok(())` |
| `on cluster_change` | `fn f(cluster: Cluster) -> Result<(), Error>` | `Ok(())` |
| `on node_lifecycle` | `fn f(phase: Lifecycle) -> Result<(), Error>` | `Ok(())` |
| `on timer` | `fn f(timer: Timer) -> Result<(), Error>` | `Ok(())`; runs as a record UDF |
| `on xdr_merge` | `fn f(local: Record, remote: Record, conflict: Conflict) -> Result<Resolution, Error>` | `Resolution::KeepLocal`, `TakeRemote`, `Policy`, `Record { bins, ttl }` |

## Records

A `Record` is a read-only record with its metadata. Its bins come through the event's own import, never
the record natives (which answer "nothing" in a handler), as `Option`s, since on either side of an event a
bin may not exist: `int("name")`, `double`, `text`, `list`, `map` return `Ok(None)` for an absent or nil
bin and `Err(WrongType)` for another kind; `bin("name")` returns the raw `Value` or `Err(Absent)`. `bins()`
is the list of names, `deleted()` the names this side deleted (bin deletes, or tombstones), `generation()`,
`last_update_time()`, `ttl()`, `source_id()`, and per bin `bin_lut(name)` and `bin_source_id(name)`
where the server keeps them.

`with_bins(overrides)` builds a new record from this one's bins with `overrides` on top, by position: the
way a handler returns "the record, plus these". It prefers one side, so in a time-based `xdr-merge`
resolver use `Pair::with_newer` instead.

## Timers

A `timer` handler runs as a record UDF, so `as_udf::bin`, `set`, `update` and the rest work in it, and
`as_udf::timer_set("tick", 200)` re-arms it; `timer_cancel("tick")` cancels. Both are refused in any
other handler.

```rust
event! {
    on timer
    fn tick(timer: Timer) -> Result<(), Error> {
        let ticks = match as_udf::bin("ticks") { Ok(v) => v.int()?, Err(_) => 0 } + 1;

        as_udf::set("ticks", ticks)?;
        as_udf::update()?;

        if timer.is("tick") && ticks < 3 {
            as_udf::timer_set("tick", 200)?;
        }

        Ok(())
    }
}
```

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

`examples/guard`, every kind but the merge:

```rust
{{#include ../../../aerospike-udf-sdk-wasm/examples/guard/src/lib.rs}}
```

`examples/resolver`, the time-based account merge:

```rust
{{#include ../../../aerospike-udf-sdk-wasm/examples/resolver/src/lib.rs}}
```

The data-only merges, `examples/counter` through `examples/crdt`, are the [CRDT tutorial](../crdt/README.md).

## API reference

Generated from `aerospike-udf-sdk-wasm/src/lib.rs`: the event surface. The macro's own plumbing
(`event_record`, the `deliver_*` functions) is hidden in the source and left out here.

{{#include ../api/rust-events.md}}
