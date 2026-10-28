# The event system

A record UDF runs when a client asks. An **event handler** runs when the server reaches a point in its
own processing, a write about to commit, a record about to expire, an XDR write arriving, a node coming up,
with the records involved and a description of what is happening, and its answer changes what the server
does next. Handlers are WASM functions in ordinary modules, bound to events with an info command, kept in
SMD so the binding is cluster-wide and survives restarts.

This chapter is the whole system: the binding model, every event kind with its payload and answers,
timers, operations. The four chapters after it are the languages.

## Edition and feature key

The event system is an Enterprise feature, gated by the feature key `asdb-event-system`. A Community
server answers every `event-*` command with `enterprise only` and runs no hooks. An Enterprise server
whose feature key file does not carry `asdb-event-system true` logs once at startup that the event system
is inert, ignores any bindings and timers it finds in SMD (with a warning), and answers the commands,
the dry run and the timer imports with `feature key does not allow the event system`; UDFs are
unaffected. The key is read once at startup, with the other keys.

> Until issued feature keys carry `asdb-event-system`, the check is commented out in `as_event_init`
> and every Enterprise server behaves as if the key were present. The key is already parsed and the
> rest of the gate is in place, so enforcing it is restoring one line.

## Bindings

```text
event-set:event=before-write;namespace=test;set=accounts;module=guard;function=guard_account
```

| Parameter | Meaning |
| --- | --- |
| `event` | the kind, one of the fifteen below |
| `namespace`, `set` | the scope: a namespace, and optionally a set within it. Set-level bindings win over namespace-level ones. The two global kinds take no namespace. |
| `module`, `function` | a registered WASM module and an export of it whose signature declares this kind |
| `name` | this binding among others of its kind on the scope; defaults to the function name |
| `order` | a chain of bindings runs ascending by `order`, then by name |
| `exp` or `ael` | a filter expression, see below |
| `on-failure` | `policy` (default): a handler that fails is counted and the operation proceeds as it would have; `fail`: the operation fails; `abandon` (`xdr-merge` only): the write is dropped as a lost conflict |
| `trigger` | `xdr-merge` only: `always` (default), every incoming XDR write to an existing record; `conflict`, only those in which a bin would lose under bin convergence |
| `timeout` | the handler's own budget in milliseconds, within the operation's deadline; 0 (default) means the deadline alone |

`event-set` refuses a module that is not WASM, a function the module does not export, and a function
whose signature is not `event <kind>/<version>` for this kind at a version the server serves. The
signature is what the SDK writes beside the handler; a module built against an older contract is refused
rather than handed a payload it would misread.

```text
event-remove:event=before-write;namespace=test;set=accounts[;name=guard_account]
event-list[:namespace=test]
event-kinds
```

`event-remove` without a name removes every binding of the kind on the scope. `event-list` prints one row
per binding with its parameters and counters. `event-kinds` prints the kinds this server serves, each with
its contract version, its scope (`namespace` or `global`), whether it allows one binding per scope
(`single`) and whether it takes a filter. `udf-remove` refuses a module an event binding refers to.

A row of `event-list`:

```text
event=before-write:namespace=test:set=accounts:name=guard_account:order=0:module=guard:function=guard_account:version=1:on-failure=policy:trigger=always:timeout=0:filter=-:fired=12:filtered=3:rejected=2:keep-local=0:take-remote=0:record=10:policy=0:fallback=0:dropped=0
```

| Counter | Counts |
| --- | --- |
| `fired` | handler calls |
| `filtered` | records the filter kept from the handler |
| `rejected` | refusals: a rejected write, a refused delete or truncate, a kept expiry, a protected eviction, a skipped shipment |
| `record` | answers that replaced the record |
| `policy` | answers that let the operation proceed as it would have (`accept`, `allow`, `expire`, ...), and successful runs of the events that answer nothing |
| `keep-local`, `take-remote` | the `xdr-merge` verdicts; a `record` answer equal to the current record counts as `keep-local` |
| `fallback` | handler failures and unrecognised answers |
| `dropped` | after-events the queue had no room for |

A dry run does not count. Counters are per node and reset at restart.

## Chains

Several bindings of one kind may share a scope, each under its own `name`, and run as a chain in `order`.
The rule is one sentence per answer: a `record` answer is the next handler's input (its bins and the
description's bin names are refreshed between handlers); a refusal (`reject`, `refuse`) stops the chain
and fails the operation; the once-per-node and after-events run every handler. The decision kinds
(`xdr-merge`, `before-expire`, `before-evict`, `before-ship`, `timer`) allow one binding per scope and
refuse a second name.

## Filters

A binding on a kind that fires per record may carry a filter expression, evaluated against the record
before the handler is called, so a binding on a busy set costs nothing on the records it is not about:

- `exp=<base64>`: the packed expression as every client's expression builder produces it, the same form
  policies and the XDR filter take; in the Rust client, `expressions::bin_exists("vip").base64()`.
- `ael=<text>`: the expression as text, compiled on the server.

The server evaluates the metadata part first (set, TTL, size, LUT, a bin's existence) and reads bins only
when that does not decide. For `before-write` the expression sees the record the write would leave, the
way a SQL `WHEN` sees `NEW`; for the others, the stored record. A filter that does not build is refused at
`event-set`; `event-list` shows it as given.

## Versions

Each kind has a contract version, `1` for all of them today, printed by `event-kinds`. A handler's
signature names the version its SDK was built against. Additive changes to a payload do not bump the
version, since payloads are maps and a handler ignores keys it does not know; a change in a key's meaning,
a removed key or a new answer vocabulary does, and a module built against the old one is then refused at
`event-set` instead of misreading the payload.

## What a handler sees and answers

Every handler receives a **description**, a map with `kind`, `namespace`, `set`, `now` (ms since the
epoch), `empty` (an empty list, for SDKs that need one), and the kind's own entries. The record kinds also
hand over one or two **records**, read by name through a dedicated import so the handler sees the right
side; each record's entry in the description carries its metadata (`gen`, `lut`, `ttl`, `src-id`;
`xdr-merge` zeroes `gen` and `lut`) and its bin names.

A handler answers a **verdict word**, or a map `{verdict, code, message, bins, ttl}` with every key but
`verdict` optional. Which words a kind accepts is in its row below. A `record` answer's `bins` is the
whole record: a bin not in the map, or set to nil, is deleted; `ttl` has a write's meaning (seconds, `0`
for the namespace default, never-expire) and is bounded like a write's. A `reject` or `refuse` carries a
server result code (22, forbidden, by default) and a message the client receives as server error detail
when its policy asks for it.

## The kinds

### `before-write`

Fires in the write path after the ops have produced the record the write would leave and before anything
is written, for every write that reaches the master: client, batch, UDF and XDR. Under the record lock, on
the transaction thread; on a strong-consistency namespace before replication, so replicas never see a
rejected or rewritten write.

| | |
| --- | --- |
| Records | `before` (the record as it was; empty on a create), `after` (as the write would leave it) |
| Description | `write`: `source` (`client`, `batch`, `udf`, `ops`, `xdr`), `generation` (the generation the client asked to match), `ttl` (requested), `create`, `durable-delete` |
| Answers | `accept`; `reject {code, message}` (the write fails, nothing is written); `record {bins, ttl}` (this record is committed instead) |
| Filter | yes, against `after` |
| Uses | constraints, stamping (`last_updated`, write counts), normalisation, PII stripping on import |

### `before-delete`

Fires in the delete path after the filter, key and masking checks, with the record opened and loaded.

| | |
| --- | --- |
| Records | `record` |
| Description | `delete`: `source`, `durable` |
| Answers | `allow`; `refuse {code, message}` |
| Filter | yes |

### `after-write`, `after-delete`

Queued when a write has been written and indexed, or just before a delete drops the index entry, and run
on the event service thread, off the transaction path. The queue holds 4096 events; beyond that an event
is dropped and counted on every binding of its chain. A handler cannot change anything; it observes.

| | |
| --- | --- |
| Records | `before`, `after` (write); `record` (delete) |
| Description | `write` or `delete` as above |
| Answers | nothing (return `ok`) |
| Filter | yes, evaluated at enqueue time on the transaction thread |

### `before-expire`

Fires from the namespace supervisor when a record's void time has passed, before it is removed, with the
record opened and loaded. The supervisor runs every `nsup-period`. Note that a client read of a record past
its void time answers "not found" on its own, before the supervisor ever visits.

| | |
| --- | --- |
| Records | `record` |
| Description | `expire`: `void-time` (seconds since the Aerospike epoch), `now` |
| Answers | `expire`; `keep {ttl}` (kept for `ttl` seconds, or the namespace's default TTL when absent, or for good if that is zero; the index's void time is moved, and the stored copy learns it on the record's next write) |
| Filter | yes |
| One binding per scope | yes |

### `before-evict`

Fires from the supervisor's eviction pass, same shape; `protect` leaves the record alone. Not exercised
by the test suite, since it needs memory pressure.

| | |
| --- | --- |
| Description | `evict`: `void-time`, `now`, `evict-void-time` (records due before this are being evicted) |
| Answers | `evict`; `protect` |

### `after-read`

Fires in the read path after the record's bins are gathered, with the record loaded in full, and may
answer the read with a record of its own, which then is the whole response, whatever bins the client
named. The stored record is untouched. `exists` is not a read of bins and does not fire it.

| | |
| --- | --- |
| Records | `record` |
| Description | `read`: `source`, `all` (every bin was asked for) |
| Answers | `unchanged`; `record {bins}` |
| Filter | yes |
| Uses | redaction by role, computed bins, format shims |

### `before-truncate`, `after-truncate`

Once per `truncate` command: before, on the node that received the command, where a handler may refuse it;
after, on every node when the truncate takes effect through SMD. Namespace-level bindings cover every set
of the namespace.

| | |
| --- | --- |
| Description | `truncate`: `namespace`, `set` (empty for the whole namespace), `lut` (records last updated before this go) |
| Answers | `allow`; `refuse {code, message}` (before); nothing (after) |

### `xdr-merge`

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
| Answers | `record {bins, ttl}`; `keep-local`; `take-remote` (the incoming bins over the current record); `policy` (what would have happened without the event) |
| On failure | `policy`, `fail`, and for this kind only `abandon`: the write is dropped and the source told `lost-conflict`, which it counts as success, so a deterministic failure is not retried forever |
| One binding per scope | yes |

A `record` answer equal to the current record writes nothing and answers the source `lost-conflict`,
which it counts as success; so does `keep-local`. One that differs is committed whole as an XDR write, so
it is not shipped back, and under bin convergence is stamped bin by bin with the later of the two sides'
LUTs and the source id that wrote it, which is what lets the other DC, merging the mirror pair, land on
the same record. The record-level `gen` and `lut` are 0 on both sides and `now` fails in the handler:
each cluster sets its own, and a merge that reads them cannot converge.

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

### `before-ship`

Fires on the XDR ship thread before a record's shipment to a DC is built, with the record loaded.
Enterprise only. One-directional, so the symmetry rule does not apply; a transformed shipment cannot
converge with a conflict resolver that expects the original bins. Not exercised by the test suite, which
has no DC.

| | |
| --- | --- |
| Records | `record` |
| Description | `ship`: `dc` |
| Answers | `ship`; `skip`; `record {bins}` (this record ships instead) |
| Filter | yes |
| One binding per scope | yes |

### `partition-transfer`

Fires on the migration thread when an emigration or immigration of a partition has completed on this
node, after the partition lock is released. Namespace scope, no set. Not exercised by the test suite, which
runs one node.

| | |
| --- | --- |
| Description | `transfer`: `namespace`, `partition`, `direction` (`arrived`, `departed`), `peer` (the other node, hex) |
| Answers | nothing |

### `cluster-change`

Fires at the end of a rebalance, once the cluster's partitions have their replicas. Global scope.

| | |
| --- | --- |
| Description | `cluster`: `key` (hex), `size`, `nodes` (hex ids in succession order), `self` |
| Answers | nothing |

### `node-lifecycle`

Global scope. Three phases: `service-ready`, after the node has opened its service port; `shutdown`, on
the main thread at the start of a clean shutdown; `binding-deployed`, whenever any binding is made or
replaced on the node (not during the SMD replay at startup, when modules are not yet loaded). The last
is where a module seeds state, and it fires for the lifecycle binding's own deploy, which is how a test
sees it.

| | |
| --- | --- |
| Description | `lifecycle`: `phase`, `node` (hex); for `binding-deployed`, `binding` (`event`, `namespace`, `set`, `name`, `module`, `function`) |
| Answers | nothing |

### `timer`

The handler bound to a set under this kind runs on a record when a timer set on that record comes due. It
runs as an **ordinary record UDF** on the record, through an internal transaction, so every record
function works in it and it may re-arm itself. Its argument is one map, `{name, due, now}`. One binding
per scope. See the next section.

## Timers

A timer is "run the `timer` handler bound to this set on this record at this time". Timers live in SMD, so
they survive restarts and are visible cluster-wide; a scheduler thread checks them every 100 ms, removes a
due timer from SMD first, so a restart in the middle cannot fire it twice, and then runs the handler. A
timer that comes due with no handler bound is dropped with a warning.

```text
event-timer-set:namespace=test;set=accounts;key=alice;name=tick;in=200
event-timer-set:namespace=test;set=accounts;digest=<40 hex>;name=later;at=<epoch ms>
event-timer-list[:namespace=test]
event-timer-remove:namespace=test;digest=<40 hex>;name=later
```

`key` is a string key, from which the server computes the digest as the clients do; `digest` names the
record directly. `in` is milliseconds from now, `at` an absolute time in milliseconds since the epoch. A
second timer with the same name on the same record replaces the first.

From inside a record UDF or a timer handler, the natives `timer_set(name, delay_ms)` and
`timer_cancel(name)` set and cancel a timer on the record in hand; they are refused in an event handler,
which has no record of its own. A recurring timer is a handler that sets the next one.

## Operations

- **Privilege.** On a server with security enabled, `event-set`, `event-remove`, `event-timer-set` and
  `event-timer-remove` need `event-admin`, which `data-admin` includes. The client's `PrivilegeCode::EventAdmin`
  (code 5) creates roles with it.
- **Restart.** Bindings and timers are replayed from SMD before the service opens; `service-ready` and the
  startup rebalance's `cluster-change` fire once the modules are loaded.
- **Cost.** A per-record event is a WASM call on every record of its scope that passes the filter, on the
  transaction thread. Bind to the set, not the namespace, and filter. The after-events cost a snapshot of
  the record's bins on the transaction thread and the call on the service thread.
- **Logs.** A handler's failure is a ticker warning naming the binding and the reason; a refused truncate
  is logged with its reason.
- **What a handler cannot do.** Read or write another record, or anything outside the records it was
  handed; a timer is the way to act later. There is no outbox yet for the after-events to write into.

## Writing a handler

Each language chapter builds the same module, `examples/guard`: a `before-write` that refuses a negative
balance and stamps every write, a `before-delete` lock, a truncate fence, an expiry keeper, an `after-read`
redaction, the once-per-node events, and a timer that counts ticks and re-arms itself; and
`examples/resolver`, an `xdr-merge` merge. The test suite binds the four languages' modules in turn and
holds them to the same behaviour. The data-only merges are a part of their own: the
[CRDT tutorial](../crdt/README.md) builds five of them in five steps, from a counter to the device map,
each in Rust, Java, Go and C++ on one page, each run through the dry run and across two live clusters.
