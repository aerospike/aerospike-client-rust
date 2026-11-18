# The `xdr-merge` event

A record UDF runs when a client asks. An **event handler** runs when the server reaches a point in its
own processing, and its answer changes what the server does next. There is one such point: **an XDR write
arriving for a record that already exists here**. The handler is given the record as it is on this
cluster and the one that arrived, and answers what the record should be. It is how two clusters that
ship to each other end up holding the same record without one of them simply overwriting the other.

A handler is a WASM function in an ordinary module, bound with an info command and kept in SMD, so the
binding is cluster-wide and survives restarts.

This chapter is the whole of it: the binding, what the handler sees, what it may answer, and how to check
a merge before trusting it. The chapters after it are the languages. To learn it by doing, the
[CRDT tutorial](../crdt/README.md) builds five merges in five steps, from a counter to a device map.

## Edition and feature key

The event system is an Enterprise feature, gated by the feature key `asdb-event-system`. A Community
server answers every `event-*` command with `enterprise only` and runs no hooks. An Enterprise server
whose feature key file does not carry `asdb-event-system true` logs once at startup that the event system
is inert, ignores any bindings it finds in SMD (with a warning), and answers the commands and
the dry run with `feature key does not allow the event system`; UDFs are
unaffected. The key is read once at startup, with the other keys.

> Until issued feature keys carry `asdb-event-system`, the check is commented out in `as_event_init`
> and every Enterprise server behaves as if the key were present. The key is already parsed and the
> rest of the gate is in place, so enforcing it is restoring one line.

## Bindings

```text
event-set:event=xdr-merge;namespace=test;set=subscribers;module=crdt;function=xdr_merge;on-failure=abandon
```

| Parameter | Meaning |
| --- | --- |
| `event` | `xdr-merge`, the one kind there is |
| `namespace`, `set` | the scope: a namespace, and optionally a set within it. A set-level binding wins over a namespace-level one. |
| `module`, `function` | a registered WASM module and an export of it whose signature declares this event |
| `name` | the binding's name; defaults to the function name. A scope takes one binding: a second name is refused. |
| `on-failure` | `policy` (default): a handler that fails is counted and the write proceeds as it would have; `abandon`: the write is dropped as a lost conflict. `fail` is not offered, see below. |
| `trigger` | `always` (default), every incoming XDR write to an existing record; `conflict`, only those in which a bin would lose under bin convergence |
| `timeout` | the handler's own budget in milliseconds, within the write's deadline; 0 (default) means the deadline alone |

`event-set` refuses a module that is not WASM, a function the module does not export, and a function
whose signature is not `event xdr-merge/<version>` at the version the server serves. The signature is what
the SDK writes beside the handler; a module built against an older contract is refused rather than handed
a payload it would misread. It also refuses the binding while this cluster's XDR `src-id` is unset.

```text
event-remove:event=xdr-merge;namespace=test;set=subscribers
event-list[:namespace=test]
event-kinds
```

`event-list` prints one row per binding with its parameters and counters. `event-kinds` prints the kind
with its contract version. `udf-remove` refuses a module a binding refers to.

A row of `event-list`:

```text
event=xdr-merge:namespace=test:set=subscribers:name=xdr_merge:order=0:module=crdt:function=xdr_merge:version=1:on-failure=abandon:trigger=always:timeout=0:fired=12:rejected=0:keep-local=2:take-remote=0:delete=0:record=10:policy=0:fallback=0
```

| Counter | Counts |
| --- | --- |
| `fired` | handler calls |
| `rejected` | incoming writes refused before the handler ran, for carrying no source id or this cluster's own |
| `record` | answers that replaced the record |
| `keep-local`, `take-remote`, `delete` | those verdicts; a `record` answer equal to the current record counts as `keep-local` |
| `policy` | answers that let the write proceed as it would have |
| `fallback` | handler failures and unrecognised answers |

A dry run does not count. Counters are per node and reset at restart.

## Versions

The event has a contract version, `1` today, printed by `event-kinds`. A handler's
signature names the version its SDK was built against. Additive changes to a payload do not bump the
version, since payloads are maps and a handler ignores keys it does not know; a change in a key's meaning,
a removed key or a new answer vocabulary does, and a module built against the old one is then refused at
`event-set` instead of misreading the payload.

## What a handler sees and answers

Fires on every incoming XDR write to a record that exists on this node, live or tombstone, before any of
the write's ops are applied and regardless of bin convergence, with both records in hand; a create (no
record here) bypasses it, and an incoming XDR delete reaches it from the delete path with
`deleted-record` set. With `trigger=conflict` on the binding it fires only when, under bin convergence
(`conflict-resolve-writes`), some incoming bin would lose, which is what the `lost` list names. Enterprise
only, since XDR is.

Before the handler runs, a write whose source id is 0 or this cluster's own is refused (`forbidden`) and
counted, and `event-set` refuses the binding while this cluster's `src-id` is unset. The source must ship
its id: under bin convergence it travels with the bin LUTs; otherwise `ship-src-id true` in the source's
`xdr` stanza (dynamic) sends it with every record.

| | |
| --- | --- |
| Records | `current` (this node's), `incoming` (as shipped) |
| Description | `trigger` (`always`, `conflict`), `policy` (`bin-lut` under bin convergence, else `replace`), `mode` (`live`, `dry-run`), `lost` (the incoming bin names that would lose under bin convergence); each side with `ttl`, `src-id`, `bins`, `deleted` (bin deletes or tombstones) and, where kept, `bin-luts` and `bin-srcs`; `current` adds `tombstone`, `incoming` adds `partial` (the write carries only changed bins) and `deleted-record` |
| Answers | `record {bins, ttl}`; `keep-local`; `take-remote` (the incoming bins over the current record); `delete` (the record is removed on this cluster); `policy` (what would have happened without the event) |
| On failure | `policy`, and for this kind only `abandon`: the write is dropped and the source told `lost-conflict`, which it counts as success. `fail` is refused for this kind: the source does not send a failed write again, so it would drop the write as `abandon` does and report it as lost |
| One binding per scope | yes |

The SDKs hand a handler the two records and nothing else. What it may need of the description is asked
of the record it is about: `incoming` says whether it is a delete, whether it is partial and which of its
bins would lose; `current` says whether it is a tombstone; either says whether bin convergence is on, and
its source id, deleted bins and per-bin LUTs and sources. The `trigger` and `mode` entries are not shown
to a handler: a merge is a function of the two records, and should answer the same in a dry run.

A `record` answer equal to the current record writes nothing and answers the source `lost-conflict`,
which it counts as success; so does `keep-local`. One that differs is committed whole as an XDR write, so
it is not shipped back, and under bin convergence is stamped bin by bin with the later of the two sides'
LUTs and the source id that wrote it, which is what lets the other DC, merging the mirror pair, land on
the same record. The record-level `gen` and `lut` are 0 on both sides and `now` fails in the handler:
each cluster sets its own, and a merge that reads them cannot converge.

**What makes an answer a failure.** The answer is checked before it is taken, and what is wrong with it
is the handler's failure, which `on-failure` decides. A `record` answer fails when its `bins` is not a
map; when a bin's name is not a string of 1 to 15 bytes with no zero byte in it; when its `ttl` is not an
integer in a write's range; when a GeoJSON value is not a document the server would accept as a write;
when its values alone are larger than the namespace's `max-record-size`; and **when it has no bins
left** - an empty map, or one whose values are all nil. A record of nothing is not a deletion: a merge
that built nothing by mistake must not remove what it merged. A handler that means to remove the record
answers `delete`. The removal happens on the cluster that merged and on no other, so a merge that
deletes has to reach the same answer on both sides.

**Masking.** A handler is given both records' bins as they are stored, with no masking rule applied,
whoever wrote the incoming record. It has to: it merges real values and what it returns is stored. A
handler bound to a masked set is therefore code that sees that set unmasked, and binding one is an
operator's decision to trust it with that.

A merge must be **pure** (no clock, no other records) and **convergent**: DC A merges (A's record, B's
write) while DC B merges (B's record, A's write), in any order and any number of times, and both must land
on the same record or the DCs diverge for good. Two ways keep it. The time-based way orders the two sides
of a bin by its own LUT and then by source id, which is symmetric and is what the SDKs' `newer`/`older`
helpers do; it needs bin convergence shipping bin LUTs, and `trigger=conflict` is its natural binding.
The data-only way keeps versions and removal markers in the record and breaks ties on the stored bytes
(`cmp_stored` in the SDKs), so the merge is a function of the data alone; `examples/crdt` is this, and
`trigger=always` without bin convergence is its binding.

**The dry run.** `execute_udf(key, "__event__", "xdr-merge", [incoming bins, incoming metadata, other
bins])` runs the binding of the record's set against the real current record and an incoming record
described by the arguments, writes nothing and counts nothing, and answers `{verdict, bins, ttl,
symmetric, idempotent, associative}`: the three CRDT laws checked on real bytes, the sides swapped, the
merge merged with the incoming again, and, given the third record, `(current ⊕ incoming) ⊕ other`
against `current ⊕ (incoming ⊕ other)`. The metadata is a map of `ttl`, `src-id`, and optionally
`bin-luts`, `bin-srcs` and `deleted`. It is the way to check a merge on one cluster;
`crdt_merge_converges_in_the_eight_scenarios` in the test suite is nine of them, and
[step 5 of the CRDT tutorial](../crdt/device-map.md) builds the module they exercise.

**Two clusters.** `tests/src/xdr_merge.rs` runs the same module between two clusters that ship to each
other (`AEROSPIKE_XDR_DC1` and `AEROSPIKE_XDR_DC2` name them; without them it skips): a record born on one
side reaches the other as a create, a write from either side merges on the other, both sides writing at
once converge on the union, and a write that changes nothing is kept on the receiver and counted as a
success by the source. It is the check that the dry run cannot make.

## Operations

- **Privilege.** On a server with security enabled, `event-set` and `event-remove` need `event-admin`,
  which `data-admin` includes. The client's `PrivilegeCode::EventAdmin` (code 5) creates roles with it.
- **Restart.** Bindings are replayed from SMD before the service opens.
- **Cost.** A merge is a WASM call on every incoming XDR write to an existing record of its scope, on the
  transaction thread, and a read of the record's bins that a replacing write would otherwise skip. Bind
  to the set, not the namespace.
- **Logs.** A handler's failure is a ticker warning naming the binding and the reason.
- **What a handler cannot do.** Read or write another record, read the clock, or reach anything outside
  the two records it was handed.

## Writing a handler

Each language chapter shows the handler's shape and builds `examples/resolver`, a small merge. The test
suite binds the five languages' modules in turn and holds them to the same answers. The data-only merges
are a part of their own: the [CRDT tutorial](../crdt/README.md) builds five of them in five steps, from a
counter to the device map, each in Rust, Java, Go and C++ on one page, each run through the dry run and
across two live clusters.
