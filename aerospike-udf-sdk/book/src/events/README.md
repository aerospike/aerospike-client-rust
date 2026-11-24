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
| `module`, `function` | a registered WASM module and an export of it whose signature declares this event. `module=none` on a set, with no function, opts the set out of its namespace's binding: nothing is offered for it. |
| `name` | the binding's name; defaults to the function name (`none` for an opt-out). A scope takes one binding: a second name is refused. |
| `on-failure` | `abandon` (default): a handler that fails drops the incoming write, and the source counts it as `merge_abandoned`; `policy`: the failure is counted and the write proceeds as it would have. `fail` is not offered, see below. |
| `timeout` | the handler's own budget in milliseconds, within the write's deadline; 0 (default) means the deadline alone |

`event-set` refuses a module that is not WASM, a function the module does not export, and a function
whose signature is not `event xdr-merge/<version>` at a version the server serves: from the oldest it
still accepts to the one it builds payloads for (both `1` today). The signature is what the SDK writes
beside the handler; a module built against a contract the server no longer serves is refused rather than
handed a payload it would misread. It also refuses the binding while this cluster's XDR `src-id` is
unset, and a `trigger` parameter: every incoming write to a bound record is offered (see below). An
opt-out takes no function, `on-failure`, `timeout` or `order`, and needs a set.

Why `abandon` is the default: a handler that fails has merged nothing. Under `policy` the write goes
ahead as if there were no handler, and the cluster that receives it takes the other's record whole, which
is the loss the handler was bound to prevent. Dropping the write keeps this cluster's record; the source
counts the drop apart from a lost conflict, so an operator sees writes that never arrived.

```text
event-remove:event=xdr-merge;namespace=test;set=subscribers
event-list[:namespace=test]
event-kinds
```

`event-list` prints one row per binding with its parameters and counters. `event-kinds` prints the kind
with its contract version. `udf-remove` refuses a module a binding refers to. A client cannot call a
handler as a record UDF: the call is refused before the handler runs.

A row of `event-list`:

```text
event=xdr-merge:namespace=test:set=subscribers:name=xdr_merge:order=0:module=crdt:function=xdr_merge:version=1:on-failure=abandon:timeout=0:fired=12:rejected=0:keep-local=2:take-remote=0:delete=0:took-incoming=4:combined=6:policy=0:fallback=0:fail-timeout=0:fail-error=0:fail-answer=0:abandoned=0:shipped-back=3:avg-us=41:max-us=180
```

| Counter | Counts |
| --- | --- |
| `fired` | handler calls. Every call is one of `keep-local`, `take-remote`, `delete`, `took-incoming`, `combined`, `policy` and `fallback` |
| `rejected` | incoming writes refused before the handler ran: no source id, this cluster's own, or this cluster's own is 0 |
| `keep-local` | `keep-local` answers, and any answer that leaves the record as it is |
| `take-remote` | `take-remote` answers that changed the record |
| `delete` | `delete` answers |
| `took-incoming` | `record` answers that are the incoming write applied over the record: no collision to merge |
| `combined` | `record` answers unlike either side: a collision the handler merged |
| `policy` | answers that let the write proceed as it would have |
| `fallback` | handler failures and answers that cannot be taken; each is one of `fail-timeout` (stopped at its deadline), `fail-error` (trapped, failed, or could not run) and `fail-answer` (answered what cannot be taken) |
| `abandoned` | failures that dropped the incoming write (`on-failure=abandon`) |
| `shipped-back` | merges whose result holds something the incoming write did not bring, so the record goes back to the other cluster. A merge that converges finds nothing to change there and stops; one that does not makes this climb with every round |
| `avg-us`, `max-us` | time in the handler, every call: the mean and the longest, in microseconds |

A dry run does not count. Counters are per node and reset at restart; a binding set again keeps them.

Each failure is logged with the namespace, set and digest of the record it failed on, at most ten lines
a second per binding; a line then says how many were not logged. On the source, `get-stats:context=xdr`
shows `merge_abandoned` beside `success`: writes a destination's handler failed on and dropped.

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
`deleted-record` set. Enterprise only, since XDR is.

There is no binding that fires only when bin convergence would refuse some incoming bin. One was tried,
and it lost updates: when one cluster was behind, the other's write won every bin, the handler was never
offered it, and the cluster that was behind took the bins whole, unmerged.

Before the handler runs, a write whose source id is 0 or this cluster's own is refused (`forbidden`) and
counted, and so is every write while this cluster's own `src-id` is 0: `event-set` refuses a binding then,
and a binding that reaches such a node through the cluster's metadata is kept but merges nothing until
`src-id` is set. Every source with an `src-id` sends it with every record and every delete; nothing has to
be configured for it.

| | |
| --- | --- |
| Records | `current` (this node's), `incoming` (as shipped) |
| Description | `policy` (`bin-lut` under bin convergence, else `replace`), `mode` (`live`, `dry-run`); each side with `ttl`, `src-id`, `bins`, `deleted` (bin deletes or tombstones) and, where kept, `bin-luts` and `bin-srcs`; `current` adds `tombstone`, `incoming` adds `partial` (the write carries only changed bins) and `deleted-record` |
| Answers | `record {bins, ttl}`; `keep-local`; `take-remote` (the incoming bins over the current record); `delete` (the record is removed on this cluster); `policy` (what would have happened without the event) |
| On failure | `abandon` (default, and for this kind only): the write is dropped and the source told `lost-conflict` with a sub-code of its own, which it counts as `merge_abandoned`, not as success; or `policy`. `fail` is refused for this kind: the source does not send a failed write again, so it would drop the write as `abandon` does and report it as lost |
| One binding per scope | yes |

The SDKs hand a handler the two records and nothing else. What it may need of the description is asked
of the record it is about: `incoming` says whether it is a delete and whether it is partial; `current` says whether it is a tombstone; either says whether bin convergence is on, and
its source id, deleted bins and per-bin LUTs and sources. The `mode` entry is not shown
to a handler: a merge is a function of the two records, and should answer the same in a dry run.

A `record` answer equal to the current record writes nothing and answers the source `lost-conflict`,
which it counts as success; so does `keep-local`. One that differs replaces the record, and what XDR does
with it depends on where each value came from, whatever the namespace ships and whether it converges
bins; `take-remote` is written the same way, and one that leaves the record as it is counts as
`keep-local` and answers `lost-conflict` too.

An incoming delete is answered the same way, with three differences. `take-remote` is the delete.
`keep-local` keeps the record, and the source hears `lost-conflict`. A `record` answer cannot be applied
in its place - the delete path has no write to make a record with - so one that is not the current record
is the handler's failure, and `on-failure` decides: under `abandon` the record is kept, under `policy` the
delete goes ahead; either way it is counted in `fail-answer` and logged, never a silent delete.

- **The record.** It is marked an XDR write, and so not shipped back, only when everything in it came
  with the incoming write. Anything else - a value of this cluster's that the other did not send, a value
  the handler computed, an incoming bin the handler dropped, a bin the handler deleted that the other
  cluster still has - means the other cluster lacks part of it, and it ships back. Under whole-record
  shipping that is the only flag XDR reads, on the first attempt, on a retry and in a recovery.
- **Each bin**, where the namespace keeps metadata per bin (bin convergence, or `bin-policy` shipping
  only changed bins): a bin whose value is the incoming one is stamped as the other cluster's and not
  shipped back; a bin whose value is this cluster's own is left as it was, so a local write that had not
  been shipped yet still is; a value the handler computed, which is neither side's, is stamped as a write
  made here and shipped once.

The other DC, merging the mirror pair, lands on the same record and answers "unchanged", which ends the
exchange. Before this, a merged record was always marked an XDR write: a local change that had not left
when the merge landed was kept but never shipped, and the two clusters differed until that bin was
written again. The record-level `gen` and `lut` are 0 on both sides, the description carries no time,
and `now` fails in the handler: each cluster has its own, and a merge that reads them cannot converge.

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
helpers do; it needs bin convergence shipping bin LUTs.
The data-only way keeps versions and removal markers in the record and breaks ties on the stored bytes
(`cmp_stored` in the SDKs), so the merge is a function of the data alone; `examples/crdt` is this, and
it needs no bin convergence.

**The dry run.** `execute_udf(key, "__event__", "xdr-merge", [incoming bins, incoming metadata, other
bins, other metadata])`, the last two optional, runs the binding of the record's set against the real current record and an incoming record
described by the arguments, writes nothing and counts nothing, and answers `{verdict, bins, ttl,
symmetric, idempotent, associative}`: the three CRDT laws checked on real bytes, the sides swapped, the
merge merged with the incoming again, and, given the third record, `(current ⊕ incoming) ⊕ other`
against `current ⊕ (incoming ⊕ other)`. The metadata is a map of `ttl`, `src-id`, and optionally
`bin-luts`, `bin-srcs` and `deleted`; the third record's metadata is the same map, its `src-id` 3 when
not given. The handler is told what a live merge on that namespace would tell it, bin convergence
included, and the merged record is stamped as a live merge would stamp it before it is merged again.
`the_dry_run_answers_what_the_live_merge_does` holds the dry run to the live merge between two
clusters, for every handler in every language. It is the way to check a merge on one cluster;
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
