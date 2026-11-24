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
[CRDT tutorial](../crdt/README.md) builds six merges in six steps, from a counter to a device map and a
record of registers.

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
| `on-failure` | `abandon` (default): a handler that fails drops the incoming write, and the source counts it as `merge_abandoned`; `policy`: the failure is counted and the incoming write is applied, as `take-remote` applies it. `fail` is not offered, see below. |
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
event=xdr-merge:namespace=test:set=subscribers:name=xdr_merge:order=0:module=crdt:function=xdr_merge:version=1:on-failure=abandon:timeout=0:settings-warnings=0:fired=12:skipped=3:no-times=0:keep-local=2:take-remote=0:delete=0:took-incoming=4:combined=6:policy=0:fallback=0:fail-timeout=0:fail-error=0:fail-memory=0:fail-answer=0:fail-source=0:abandoned=0:avg-us=41:max-us=180
```

| Counter | Counts |
| --- | --- |
| `settings-warnings` | the namespace's settings this node warns about (see [Deployment](#deployment)); 0 when there are none. Not a counter: worked out each time |
| `fired` | handler calls. Every call is one of `keep-local`, `take-remote`, `delete`, `took-incoming`, `combined`, `policy` and `fallback` |
| `skipped` | writes that could not conflict, applied without calling the handler (see below) |
| `no-times` | calls on a write that carried no time at all, neither the record's nor a bin's: a handler that decides by time had nothing to decide by |
| `keep-local` | `keep-local` answers, and any answer that leaves the record as it is |
| `take-remote` | `take-remote` answers that changed the record |
| `delete` | `delete` and `expunge` answers |
| `took-incoming` | `record` answers that are the incoming write applied over the record: no collision to merge |
| `combined` | `record` answers unlike either side: a collision the handler merged |
| `policy` | `policy` answers |
| `fallback` | failures, each counted once more by its cause: `fail-timeout` (stopped at its time limit), `fail-error` (trapped, failed, or could not run), `fail-memory` (refused memory it asked for), `fail-answer` (answered what cannot be taken), `fail-source` (the write's source id was refused, or this cluster's own is 0; the handler is not called, so this one is not in `fired`) |
| `abandoned` | failures that dropped the incoming write (`on-failure=abandon`) |
| `avg-us`, `max-us` | time in the handler, every call: the mean and the longest, in microseconds |

A dry run does not count. Counters are per node and reset at restart; a binding set again keeps them.

Each failure is logged with the namespace, set and digest of the record it failed on, the record's user
key when the write carries one, and the cause by name and sub-code, at most ten lines a second per
binding; a line then says how many were not logged. On the source, `get-stats:context=xdr` shows
`merge_abandoned` beside `success`: writes a destination's handler failed on and dropped.

## Versions

The event has a contract version, `1` today, printed by `event-kinds`. A handler's
signature names the version its SDK was built against. Additive changes to a payload do not bump the
version, since payloads are maps and a handler ignores keys it does not know; a change in a key's meaning,
a removed key or a new answer vocabulary does, and a module built against the old one is then refused at
`event-set` instead of misreading the payload.

## What a handler sees and answers

Fires on every incoming XDR write to a record that exists on this node, live or tombstone, before any of
the write's ops are applied, with both records in hand; a create (no record here) bypasses it, and an
incoming XDR delete reaches it from the delete path with `deleted-record` set. Enterprise only, since XDR
is.

**A bound set has no bin convergence.** Sets with a handler and sets without one share a namespace; a set
with one gets no bin convergence whatever `conflict-resolve-writes` says, and behaves the same with it
on, off, or changed while running. Nothing is refused by time before the handler sees it, a local write
is never refused because a bin arrived with a later time, and every bin keeps its own time and source
whatever XDR ships. A set without a handler in the same namespace converges bins as it always has.

There is no binding that fires only when bin convergence would refuse some incoming bin. One was tried,
and it lost updates: when one cluster was behind, the other's write won every bin, the handler was never
offered it, and the cluster that was behind took the bins whole, unmerged.

Every source with an `src-id` sends it with every record and every delete; nothing has to be configured
for it. A write whose source id is 0 or this cluster's own, and every write while this cluster's own
`src-id` is 0, is not offered to the handler: it is a failure, counted in `fail-source`, and `on-failure`
decides. `event-set` refuses a binding while `src-id` is 0, and a binding that reaches such a node through
the cluster's metadata is kept but merges nothing until `src-id` is set.

**A write that cannot conflict skips the handler.** When everything the current record holds came from
the same cluster as the incoming write, by XDR, at or before the incoming write's time, with no local
change since, the incoming write supersedes all of it: any merge that keeps the contract returns it. It
is applied without a call and counted in `skipped`. Only a write that carries a time can be taken for one
(`ship-bin-luts`, or `ship-record-lut`); without a time, a retry of an older write could not be told from
a newer one.

| | |
| --- | --- |
| Records | `current` (this node's), `incoming` (as shipped) |
| Description | `policy` (always `take-remote`: what a `policy` answer does), `mode` (`live`, `dry-run`); each side with `src-id`, `bins`, `deleted` (bin deletes or tombstones), `bin-luts` and `bin-srcs`; `current` adds `tombstone`, `incoming` adds `partial` (the write carries only changed bins) and `deleted-record`. No time to live |
| Answers | `record {bins}`; `keep-local`; `take-remote` (the incoming write applied: its bins over the current record, or the whole record when it ships whole); `delete` (removed on this cluster, leaving a durable tombstone); `expunge` (removed on this cluster, with no tombstone); `policy` (the same as `take-remote`, since a bound set has no bin convergence) |
| On failure | `abandon` (default, and for this kind only): the write is dropped and the source told `xdr-merge-abandoned` (result code 33) with a sub-code for the cause, which it counts as `merge_abandoned`, not as success; or `policy`. `fail` is refused for this kind: the source does not send a failed write again, so it would drop the write as `abandon` does and report it as lost |
| One binding per scope | yes |

The SDKs hand a handler the two records and nothing else. What it may need of the description is asked
of the record it is about: `incoming` says whether it is a delete and whether it is partial; `current`
says whether it is a tombstone; either gives its source id, deleted bins and per-bin times and sources.
A bin's time is the one its writer stamped: for a bin that came over XDR, the time the other cluster
shipped with it (the bin's own under `ship-bin-luts`, the record's under `ship-record-lut`), or, when
the source shipped no time, the time the write arrived here. The `mode` entry is not shown to a handler:
a merge is a function of the two records, and should answer the same in a dry run.

A `record` answer equal to the current record writes nothing and answers the source `lost-conflict`,
which it counts as success; so does `keep-local`. One that differs replaces the record, each bin stamped
by where its value came from; `take-remote` and `policy` are written the same way, and one that leaves
the record as it is counts as `keep-local` and answers `lost-conflict` too.

An incoming delete is answered the same way, with three differences. `take-remote` and `policy` are the
delete, as the incoming delete made it; `delete` and `expunge` say how the record goes. `keep-local` keeps
the record, and the source hears `lost-conflict`. A `record` answer cannot be applied in its place - the
delete path has no write to make a record with - so one that is not the current record is the handler's
failure, and `on-failure` decides: under `abandon` the record is kept, under `policy` the delete goes
ahead; either way it is counted in `fail-answer` and logged, never a silent delete.

**A merge ships nothing.** With `forward` off, as a binding requires, the merge is submitted to XDR as the
XDR write it is, and nothing goes back to the other cluster: two clusters that both combine a collision
each reach the same record on their own, without a ship each way. What this cluster had changed locally
reaches the other cluster with that local write's own ship, its retry, or a recovery, all of which read
the record as it stands, merge included. For that the merge stamps what it writes:

- **The record** is marked an XDR write only when everything in it came with the incoming write.
  Anything else - a value of this cluster's that the other did not send, a value the handler computed,
  an incoming bin the handler dropped, a bin the handler deleted that the other cluster still has -
  leaves it marked local, so a ship of it that is still waiting, a retry and a recovery all still send it.
- **Each bin**: a bin whose value is the incoming one takes the incoming time and source; a bin whose
  value is this cluster's own is left as it was, so a local write that had not been shipped yet still
  is; a value the handler computed, which is neither side's, is stamped as a write made here, at the
  merge's time.

A handler that keeps the contract computes the same record on both sides, so nothing more needs to
travel. One that does not - that appends the incoming list to the current one, say - leaves the two
clusters different rather than sending the record back and forth; the record comparison and the manual
repair below are for that.

**The contract, in one sentence more.** A merge returns the incoming record whenever the current record
holds nothing the incoming one lacks. A handler does not change data on its own: purges and other
cleanup happen in local writes, which ship like any other, as the device map's `purge_devices` does.

**Expiration.** A merged record expires at the later of the two records' expirations, and a record that
never expires wins. The handler neither sees nor sets it: a `ttl` in its answer is a failure. Each
cluster taking the other's expiration would swap them on every exchange, and the record would expire on
one side first, to be recreated there by the other side's next write with only the bins it changed.

**What makes an answer a failure.** The answer is checked before it is taken, and what is wrong with it
is the handler's failure, which `on-failure` decides. A `record` answer fails when its `bins` is not a
map; when a bin's name is not a string of 1 to 15 bytes with no zero byte in it; when it carries a `ttl`;
when a GeoJSON value is not a document the server would accept as a write; when its values alone are
larger than the namespace's `max-record-size`; and **when it has no bins left** - an empty map, or one
whose values are all nil. A record of nothing is not a deletion: a merge that built nothing by mistake
must not remove what it merged. A handler that means to remove the record answers `delete` or `expunge`;
`expunge` fails where the namespace forbids non-durable deletes. The removal happens on the cluster that
merged and on no other, so a merge that deletes has to reach the same answer on both sides.

**Masking.** A handler is given both records' bins as they are stored, with no masking rule applied,
whoever wrote the incoming record. It has to: it merges real values and what it returns is stored. A
handler bound to a masked set is therefore code that sees that set unmasked, and binding one is an
operator's decision to trust it with that.

A merge must be **pure** (no clock, no other records) and **convergent**: DC A merges (A's record, B's
write) while DC B merges (B's record, A's write), in any order and any number of times, and both must land
on the same record or the DCs diverge for good. Two ways keep it. The time-based way orders the two sides
of a bin by its own time and then by source id, which is symmetric and is what the SDKs' `newer`/`older`
helpers do; it needs the source to ship times (`bin-policy only-changed` with `ship-bin-luts`, or
`ship-record-lut`), and it inherits the clocks' skew (see [Clock skew](#clock-skew)). The data-only way
keeps versions and removal markers in the record and breaks ties on the stored bytes (`cmp_stored` in the
SDKs), so the merge is a function of the data alone; `examples/crdt` is this, and it needs no times.

**The dry run.** `execute_udf(key, "__event__", "xdr-merge", [incoming bins, incoming metadata, other
bins, other metadata])`, the last two optional, runs the binding of the record's set against the real
current record and an incoming record described by the arguments, writes nothing and counts nothing, and
answers `{verdict, bins, symmetric, idempotent, associative}`: the three CRDT laws checked on real bytes,
the sides swapped, the merge merged with the incoming again, and, given the third record,
`(current ⊕ incoming) ⊕ other` against `current ⊕ (incoming ⊕ other)`. The metadata is a map of
`src-id`, and optionally `lut` (the time of every bin given no time of its own, as a shipped record's
time is), `bin-luts`, `bin-srcs`, `deleted` and `partial`; the third record's metadata is the same map, its
`src-id` 3 when not given. The handler is told what a live merge would tell it, and the merged record is
stamped as a live merge would stamp it before it is merged again.
`the_dry_run_answers_what_the_live_merge_does` holds the dry run to the live merge between two
clusters, for every handler in every language. It is the way to check a merge on one cluster;
`crdt_merge_converges_in_the_eight_scenarios` in the test suite is nine of them, and
[step 5 of the CRDT tutorial](../crdt/device-map.md) builds the module they exercise.

**Two clusters.** `tests/src/xdr_merge.rs` runs the same module between two clusters that ship to each
other (`AEROSPIKE_XDR_DC1` and `AEROSPIKE_XDR_DC2` name them; without them it skips): a record born on one
side reaches the other as a create, a write from either side merges on the other, both sides writing at
once converge on the union, and a write that changes nothing is kept on the receiver and counted as a
success by the source. It is the check that the dry run cannot make.

## Deployment

The preview supports two clusters that ship to each other, set up new: bindings and XDR in place on both
before the first record is written. More clusters, relays and clusters that already hold data are not
supported yet.

**Required, on both clusters:**

- A distinct, non-zero `src-id` in each cluster's `xdr` stanza.
- `forward false` (the default) for the namespace. A merge ships nothing only with forward off.
- `bin-policy all` or `only-changed`. A time-based handler also needs times: `only-changed` with
  `ship-bin-luts true`, or `ship-record-lut true`.
- The same module, at the same version, bound on both clusters to the same scope.
- `disable-eviction true` on every bound set. Eviction removes a record on one cluster only, and the
  other cluster's next write then recreates it there with only the bins it changed.
- `xdr-bin-tombstone-ttl` longer than any outage the link may have. A deleted bin is remembered as a
  tombstone for that long; after it, the other side's copy of the bin comes back.

**Not allowed.** Each cluster warns about its own outbound settings for a namespace with a binding, when
the binding is made and whenever one of them changes, and `event-list` shows how many it warns about
(`settings-warnings`). Nothing is refused:

- `forward true`.
- `bin-policy changed-and-specified` or `no-bins`: the other side would merge against bins it never
  received.
- Any `ignore-bin`, for the same reason.
- `bin-policy all` with `write-policy auto` or `replace` while the namespace has
  `conflict-resolve-writes true`: every shipped write is then a replace, which the namespace refuses
  before the handler sees it.
- A bound set without `disable-eviction true`.

**Truncate on both clusters or on neither.** A truncate is not shipped; one cluster's records would come
back from the other's next writes, merged against nothing.

**No record deletes in a bound set** in the preview: a client delete, a durable delete, or a plain put
that replaces the record. The application marks a record ended in its data and gives it an expiration.

**Restores** into a live cluster are writes the other cluster has not seen and cannot be merged against
what it holds; restore both clusters, or neither, and before bindings and XDR start.

## Local writes

A merge reads what the record holds, so a record must say everything a merge needs: **a value and its
version change in one atomic write** - one `operate`, or one write UDF - never in two. A merge that runs
between the two writes sees one without the other.

These local writes break the contract and leave the clusters different:

- A plain put or a replace of a record whose bins a merge combines (a counter, a set, a map with
  versions): it overwrites the structure the merge reads instead of changing it.
- A write that clears a removal marker or lowers a version.
- A record delete, a truncate on one side, a restore.

What the device map's write UDFs do is the pattern: each change is one call that updates the value, its
version and its removal marker together, and `purge_devices`, the cleanup, is a local write of its own.

## Clock skew

The clusters' clocks never agree exactly. A handler that decides by time inherits the difference: the
cluster whose clock runs ahead wins every collision it is in, including ones where its write came second.
The server does not adjust for it; it is the handler's concern.

- **Handler A** keeps the newer bin by its time. Cluster X's clock is 2 seconds ahead of Y's. A user
  changes a setting on X, then a second later changes it again on Y: X's write is stamped a second later
  than Y's, so both clusters keep X's value, the one the user replaced.
- **Handler B** keeps a version in the data and increments it with every change, in the same write. The
  second change carries the higher version on whichever cluster it was made, and both clusters keep it,
  whatever the clocks say.

Use versions in the data where the order of two changes matters. Time is safe where any of two values
written close together is acceptable.

## When the clusters differ

A merge that fails, or a handler that breaks the contract, leaves the two clusters holding different
records. Nothing ships them back together by itself; each case is counted and logged, and repaired by
hand.

**The causes.** Under `abandon` the incoming write is dropped and the source is told
`xdr-merge-abandoned` (33) with one of these sub-codes:

| Sub-code | Cause | Counted in |
| --- | --- | --- |
| 1 `module-fault` | the handler trapped, failed, or could not be run | `fail-error` |
| 2 `time-limit` | the handler was stopped at its time limit | `fail-timeout` |
| 3 `memory-limit` | the handler was refused memory it asked for: `max-memory`, or the node's `max-total-memory` | `fail-memory` |
| 4 `invalid-answer` | the handler answered something that cannot be applied | `fail-answer` |
| 5 `source-refused` | the write's source id was missing, 0 or this cluster's own, or this cluster's own is 0 | `fail-source` |

The source counts these writes in `merge_abandoned`. The destination logs each one with the record's
namespace, set, digest and user key, and the cause.

**The repair.** Find the records: the failure log names them, and a comparison of the two clusters'
records, by value, finds the ones a contract-breaking handler left apart. For each, decide which
record is right, fix the cause, and then **touch the record on the cluster that holds the right one**. A
touch is a local write: it marks every bin changed and ships the record, and the other cluster's handler
merges it as any incoming write. A handler that keeps the contract then lands both clusters on the same
record.

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
are a part of their own: the [CRDT tutorial](../crdt/README.md) builds six in six steps, from a counter
to the device map and a record of registers, each in Rust, Java, Go and C++ on one page, each run through
the dry run and across two live clusters.
