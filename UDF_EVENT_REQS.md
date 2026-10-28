# The UDF rewrite requirements against what is built

A comparison of the "A safe, high-performance alternative to Lua UDFs" requirements draft (updated
2026-10-17; P1 to P4 by MoSCoW, three preview accounts A, B, C) with the WASM UDF engine, the three SDKs
and the event system in this tree as of 2026-10-21. Kept here until the design discussion resumes; the
closing section is the suggested order of work if the draft is the target.

**The short version (re-reviewed 2026-10-21, after `xdr-merge`).** The engine, the SDKs, the performance,
the event framework and now the XDR merge handler as specified are ahead of the document; the UDF *model*
the document describes (read UDFs vs write UDFs that return a record, batches, privileges, error codes) is
not what was built, and that remains the largest gap.

## Covered

1. **Safe by construction, sandboxed to the record, bounded time and memory.** WASM with natives as the only
   path to data, no file or memory access, `max-memory`, the transaction deadline for record UDFs, `timeout=`
  per event binding. The unsandboxed option exists as mod-native and is config-only, off by default, as the
  document asks.
2. **Faster than Lua, no cost when unused.** Measured: level with native on the vector kernel, 14 to 19× Lua;
   per-kind counters make an unbound event free.
3. **Managed alongside Lua** under the `WASM` type; every function declares what it is through its signature
   export (`fn`, `stream`, `event <kind>/<n>`), parsed at `udf-put`.
4. **Level 1 data access**: scalar and whole-collection bins, metadata, TTL including conditionally.
5. **An event framework that can grow**: versioned contexts per kind, declared outcomes, a failure policy per
   binding, per-handler counters. Fifteen kinds exist where the document wants one now and one later.
6. **The XDR merge handler as specified** (`xdr-merge`, 2026-10-21): fires on every incoming XDR write to an
   existing record before its ops, independent of bin convergence (`trigger=always`), or only on a
  bin-convergence conflict (`trigger=conflict`); incoming and current records as their own views; source
  ids shipped (`ship-src-id`, or with bin LUTs); a write from a missing, zero or own source id refused, and
  the binding refused while the cluster's own id is unset; a record answer equal to the current record
  writes nothing and answers `LOST_CONFLICT`; `on-failure` = `policy` | `fail` | `abandon`; outcome
  counters; the clock refused in handlers and record-level LUT/generation withheld from the payload;
  the dry run checking commutativity, idempotence and associativity on stored bytes.
7. **CRDT examples**: the time-based `examples/resolver` in three languages, and the data-only
   `examples/crdt` (the note's module, below) with nine scenarios through the dry run.
8. **Stream UDFs without Lua in the client** (P3), with partials merged by the caller.
9. **SDK merge building blocks** (P2): `max`, `min`, `sum`, `merge`, `newer`, `older`, `ordered`,
   `with_newer` (a canonical order by a bin's LUT then source id), and for data-only merges
  `stored_bytes` / `cmp_stored` and `Coll::bytes`.

## Partially covered

10. **Level 2 access.** Reading nested elements without copying the bin is there, by position; *writing* one
   element without rewriting the whole bin is not.
11. **Actionable errors.** Rejections carry messages through the 8.2 error-detail channel and failures name
   module and function in the log; the codes are the existing ones (`UDF_EXECUTION`, `FORBIDDEN`,
  `LOST_CONFLICT`, `PARAMETER`), with no Aerospike-defined subcodes per failure class and no location
  inside the module.
12. **Observability.** Calls and outcomes yes; latency per handler no; the handler's cost is not measured.
13. **Registration checks.** Signatures are parsed at `udf-put`; an event function's kind and version are
   checked at `event-set`, not at registration.
14. **Testing a module outside the server.** The language test runners work for pure logic, but there is no
   host-side mock of the natives, which account B rates critical. The server's dry run covers the merge.
15. **Batches and query UDFs.** The engine sits behind the same hooks Lua uses, so batch UDF and background
   query UDF probably work today; neither is tested.
16. **Concurrency contract.** Documented in the design doc and the book's introduction table (one instance
   per registration, an execution context per call, one instance per query thread for streams), not as a
  user-facing section of its own.
17. **Deletes from a merge.** A record answer with no bins deletes every bin (bin tombstones under bin
   convergence), not a record tombstone with the namespace's delete settings; an incoming XDR delete reaches
  the handler, which can answer `keep-local` or `policy`, but a `record` answer to a delete is not applied
  (the delete path has nothing to write with).
18. **`LOST_CONFLICT` as success.** The source counts it as success (EE `dc.c`); the destination still counts
   it under `n_fail_xdr_lost_conflict`, so the statistic's name disagrees with the document.

## Missing

19. **Read UDFs that behave like `get`**, and write UDFs that *return the record* and let the server apply the
   diff. Ours stage writes and commit, the Lua model; only the event handlers return whole records. This is
  the document's central idea and the largest gap.
20. **Returning what the call asks for** (metadata only, or the bins the write produced).
21. **`read-udf` and `write-udf` privileges**, and error messages gated on read permission (P2). Only
   `event-admin` exists.
22. **A writer identity for CRDTs**: the cluster's source id readable from a *write UDF* (handlers get it in
   the payload; record UDFs have no native for it).
23. **Binding in the namespace configuration** with per-set opt-out and "must match on every node". Ours is
   SMD through info commands, dynamic and cluster-wide, but not what the document specifies.
24. **The migration-merge handler** (P3). `as_record_resolve_conflict` is exactly the hook point; the event
   kind would be a day's work on the existing framework, with `gen` and LUT allowed there.
25. **XDR Proxy** is not considered at all; only its default mode carries the source id.
26. **Enterprise-only gating, a feature key, a preview flag.** Done 2026-10-21 for the first two: the
    event system is Enterprise code (`event_ee.c`, Community stub `event_ce.c`) gated by the feature key
    `asdb-event-system`. No preview flag; the `--preview` mechanism exists if one is wanted.
27. **Two-cluster tests and the per-record comparison script** for the CRDT examples; mesh and relay with
   three or more sites.
28. **Declared reads for streams** (metadata only or named bins), and the client-side final reduce through the
   same `queryAggregate` call.
29. **Level 3 operations** (list, map, HLL, bitwise from module code); only HLL read and copy by position exist.

## Where our choices differ from the document

- **Scope.** The document's won't-haves include events beyond the two merges, a plugin system and Community
  Edition; this tree has thirteen more events, mod-native, and a CE core with EE separation. None of it
  conflicts, but it is more surface than the document plans to support.
- **Binding** is dynamic through info commands and SMD rather than static namespace configuration; the
  document's "must match on every node" is met by construction (SMD is cluster-wide) rather than by a
  startup check.
- **Clock and record metadata** no longer differ: handlers cannot read the clock, and the merge payload
  carries no record-level LUT or generation (per-bin LUTs and source ids stay).

## Suggested order, if the document is the target

The numbers are the items above.

1. Read and write UDFs that return a record (20, 21), on the existing engine: a read kind routed on the
   read path (replica policy, read modes, read-touch TTL, no write lock, read privilege), a write kind whose
   returned record the server diffs and applies with the usual write checks. The signature export already
   carries the kind; the routing and the diff are the work.
2. `read-udf` / `write-udf` privileges (22); error codes and subcodes per failure class (11); a source-id
   native for write UDFs (23).
3. A host-side mock of the natives (14), so a module runs under `cargo test` / `go test` / a C harness.
4. The migration merge (25) as one more event kind on `as_record_resolve_conflict`.
5. The two-cluster proof of `xdr-merge` and the mesh question (28), the comparison script, XDR Proxy (26).
6. Level 2 writes by path (10); declared reads for streams (29); per-handler latency (12); batch and
   background UDF tests (15); the rest (13, 16, 17, 18, 19, 24, 27, 30).

Everything built so far is recorded in `WASM_UDF_DESIGN.md` (§7 for the events), `SERVER_EVENTS_CATALOG.md`
and the book under `book/`.

---

# The active-active CRDT note against what is built

A reading of "Resolving active-active collisions with CRDTs" (design note, updated 2026-09-24; the
Nokia nested-devices scenario, a last-writer-wins map per device with removal markers, a purge horizon and
a version floor, checked in a Python simulation) against the system in this tree.

## What the note teaches

- **Decide on data, not time.** The merge reads a per-entry version, whether an entry is a removal, and the
  entry's encoded bytes, in that order: no LUT, no source id, no clock, no generation. `higher()` is a true
  maximum over a total order, which makes the merge commutative, associative and idempotent at once, so
  arrival order and redelivery cannot matter. Our resolver helpers order sides by LUT then source id,
  which is the policy's logic, not a CRDT's.
- **Equal states must be equal bytes.** The "unchanged, so `LOST_CONFLICT`" path and the byte tie-break
  depend on a canonical encoding: lists sorted by key, maps key-ordered. That is a requirement on the
  SDK's builders, not on the handler.
- **The merge runs on every incoming XDR write to an existing record**, with bin convergence off, strong
  consistency on, and nothing shipped but the changed bins and the source id. Removal markers with a purge
  horizon and a version floor handle deletes inside a bin; the note's simulation found two real bugs there,
  a kind of check our dry run does not make.

## What it needs, and what we have

| The note needs | We have | Gap |
| --- | --- | --- |
| Handler on every incoming XDR write to an existing record, bin convergence off | `xdr-conflict` fires only when bins lose under `conflict-resolve-writes`; `before-write` fires on every write, XDR included, with `before` = current and `after` = incoming applied | Implementable today as a `before-write` handler filtered on `source == xdr`; see the `xdr-merge` proposal below |
| `incoming.bins`, `current.bins`, `copy(current)`, `delete rec.bins[name]` | `Record::bins()`, `with_bins`, a nil entry deletes | none |
| Key-ordered maps, so content is byte-identical | `map_new` builds an `as_orderedmap`, constructed K-ordered and stored with that flag: every guest-built map is key-ordered already | covered (a map copied back by position keeps its stored order, as it should) |
| `encode(v)`: a value's exact stored bytes, for the tie-break | strings and blobs by position; collections only as cursors | **missing**: the packed bytes of a stored collection (`Coll::bytes`) |
| Unchanged result → write nothing, `LOST_CONFLICT`, counted as success | a `record` answer always commits | **missing**: compare the answer to the current record on the server and elide the write |
| Refuse a write whose source id is missing, 0 or the destination's own; refuse binding with own id unset | nothing | **missing**, small |
| Companion write UDFs (`update_device`, `remove_device`, `purge_devices`) | record UDFs: read, rewrite lists and maps, sort in the guest, commit | covered, except the ordered map |
| `offAt` from the application, no server clock | `current_time` exists; nothing stops a handler using it | refuse `current_time` in event contexts |
| Failure policy abandon | `on-failure=fail` | covered in effect; see the abandon/fail nuance below |
| Write-UDF privilege without plain write | none | missing |
| Counters per handler | fired, outcomes, failures | covered; no latency |
| A simulation or property harness; two-cluster scripts; a comparison script | the dry run's symmetry check (commutativity only) | missing: idempotence and associativity; running a module outside the server |
| XDR Proxy default mode; mesh and relay | untested | unknown |

## Coverage

The handler logic ports line for line to a `before-write` handler with a `source == xdr` guard, or to
`xdr-conflict` under bin convergence. The companion write UDFs are covered, and the maps they build are
already key-ordered. What is missing for the byte tie-break is `Coll::bytes`; the server-side guarantees
the note leans on, source-id validation and no-op elision, are both small and absent. The tooling and the
deployment proof are absent.

## Steps

1. ✔ `Coll::bytes` (a stored collection's exact bytes) in the engine and the three SDKs, plus
   `Value::stored_bytes`/`cmp_stored` (C `as_udf_stored_bytes`/`as_udf_cmp_stored`, Go
   `StoredBytes`/`CmpStored`). Guest-built maps are already K-ordered, so no ordered-map builder is needed.
2. ✔ Source-id validation on XDR writes (0 or own: forbidden; binding refused while `src-id` is unset;
   `ship-src-id` on the source); write elision when a `record` answer equals the current record,
   answering `LOST_CONFLICT`; `on-failure=abandon`.
3. ✔ `now()` fails in event handlers; `gen` and `lut` zeroed in the payload.
4. ✔ `examples/crdt`: the note's module in Rust on `xdr-merge` plus the three write UDFs;
   `crdt_merge_converges_in_the_eight_scenarios` runs the eight scenarios through the dry run, which now
   checks symmetry, idempotence and associativity on real bytes (2026-10-21).
5. ✔ The two-cluster proof (`tests/src/xdr_merge.rs`, 2026-10-21: create, one-sided merge, concurrent
   convergence, no-op elision counted as keep-local and as a source success). ☐ The mesh question (three
   or more sites) and XDR Proxy.

The proposal below was implemented as written on 2026-10-21: `xdr-merge` replaced `xdr-conflict` in the
server, the three SDKs, the examples, the tests and the book.

---

# Proposal: an `xdr-merge` event, and what becomes of `xdr-conflict`

`before-write` filtered on `source == xdr` covers the CRDT case by accident of its payload, and that is a
hack: it fires after XDR's own apply, so the handler reconstructs "incoming" from the difference between
`before` and `after`; it runs for every write kind and relies on a filter to narrow; and it carries none of
the XDR-specific guarantees (source-id check, no-op elision, the `LOST_CONFLICT` answer). The right shape
is a kind of its own.

## `xdr-merge`

**Fires** on every incoming XDR write to a record that exists on this node, live or tombstone, before any
of the write's ops are applied and regardless of bin convergence. A create (no record) bypasses it.
Incoming XDR deletes reach it from the delete path with `incoming.deleted = true`. Namespace or set scope,
one binding per scope.

**Guards, before the handler runs:** a write whose source id is missing, 0 or this cluster's own is
refused and counted; `event-set` refuses the binding while the cluster's own `src-id` is unset.

**Payload.** `incoming`: the bins as shipped (and only those: under a changed-bins `bin-policy` that is a
partial record, flagged `partial`), its bin deletes, the source id, and per-bin LUTs and source ids when
the source ships them. `current`: the stored bins, its bin tombstones, and per-bin LUTs and source ids
when bin convergence keeps them. **No record-level LUT or generation on either side, and `current_time` is
refused in the handler**, since each cluster sets its own and a merge that reads them cannot converge.
When bin convergence is on, a `conflict` section says which incoming bins would lose and why, so the
handler can defer to the policy knowingly.

**Answers.**

| Answer | Server does | Source sees |
| --- | --- | --- |
| `record {bins, ttl?}` equal to `current` | writes nothing | `LOST_CONFLICT`, counted as success |
| `record {bins, ttl?}` that differs | commits it whole, as a local-stamped XDR write (not shipped back unless `forward`); under bin convergence, stamped bin by bin as today | `OK` |
| `keep-local` | as an equal record | `LOST_CONFLICT` |
| `policy` | the ops loop runs as it does today: plain apply, or bin convergence | as today |
| failure or an invalid record | per `on-failure`: `policy` (apply as today), `abandon` (drop the write: `LOST_CONFLICT`, so the source does not retry), `fail` (an error the source retries) | per the policy |

The equality check compares stored bytes. Guest-built maps are K-ordered, so a handler's record encodes
deterministically and the check is exact for what the handler built; a map copied back by position keeps
its stored bytes, which compare equal to themselves.

**Trigger mode.** `trigger=always` (the default) is the CRDT case. `trigger=conflict` fires only when, under
bin convergence, some incoming bin would lose, which is `xdr-conflict` today.

## Does `xdr-conflict` survive?

As a separate kind, no. It is the same event with a narrower trigger and a richer `conflict` section, and
everything it does, its payload, its stamping, its dry run, its counters, is a subset of `xdr-merge` with
`trigger=conflict`. Two kinds would give operators two names for one decision and the SDKs two types for
one record pair. The things worth keeping from it are kept: the `lost` list and the per-bin metadata (in
the `conflict` section and the per-side maps), the stamping for convergence under bin convergence, and the
dry run, which becomes `xdr-merge`'s.

What changes for a handler written against `xdr-conflict` today: it binds with `trigger=conflict`, loses
`gen` and the record-level `lut` from its payload (the per-bin ones stay), and keeps its answers. The Rust
`Pair`/`Both` helpers stay as the time-based toolkit for that mode; the CRDT mode wants the data-only
helpers the note describes (`offer`, `higher`, a byte comparison, ordered maps), which belong beside them.

## Why one kind rather than two

- One contract: "an incoming XDR record meets the current one; you say what the record is". Whether the
  policy would have objected is information, not a different event.
- One cost model, stated honestly: `always` is a call and a record load per incoming XDR write to an
  existing record, which the CRDT design accepts; `conflict` is free until a conflict.
- The migration merge (P3) becomes the obvious sibling: the same shape, both versions from one cluster,
  so generation and LUT are allowed there. Two merge kinds with one payload family, not three.

## The dry run, extended

`execute_udf(key, "__event__", "xdr-merge", [incoming bins, meta, other?])` keeps the mirror check
(commutativity) and adds the two laws the note's simulation checked and ours does not: idempotence,
`merge(merged, incoming) == merged`, and with an optional third record, associativity,
`merge(merge(a, b), c) == merge(a, merge(b, c))`, all compared as stored bytes. A handler that passes the
three on real bytes has the CRDT contract, which is what the SDK tooling of P2 asks for.

## Open points

- `abandon` versus `fail`: the draft says "abandon the write"; a retrying source would loop on a
  deterministic failure, so `abandon` must answer `LOST_CONFLICT`, and `fail` stays as the retry form.
- Incoming deletes: the note keeps record deletes outside the merge; the event should still present them
  (`incoming.deleted`) so a handler can decide, with `policy` as the default answer.
- Mesh and relay: a merged record is marked as an XDR write and not forwarded, as today; the note relies
  on each site's local writes reaching every other site directly. Three or more sites need the test.
- XDR Proxy: only its default mode carries the source id; nothing here has run through it.
