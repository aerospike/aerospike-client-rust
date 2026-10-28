# A server event system, starting from XDR conflict resolution: research notes

The ask: when an XDR write conflicts with what a node holds, raise an event that runs a UDF with the old and
the new record, and let the UDF return the final record. Before designing that, this is what other databases
do for events and for replication conflicts specifically, what the server already has that an event would
hook into, and which privilege such a registration would sit under. Written 2026-10-20 for discussion; nothing
here is implemented.

---

## 1. What the server does today at the point the event would fire

> **Correction (2026-10-20, from the implementation):** an XDR write never reaches the record-level
> comparison below. `as_record_resolve_conflict` is called from replication and migration; a shipped XDR
> write enters the ordinary write path (`write_master_bin_ops_loop`) and is judged bin by bin there, or
> not at all without bin convergence. The *record-level* event kind sketched in §3 therefore has nothing to
> hook, and the implemented event (`WASM_UDF_DESIGN.md` §7) is the bin-level one, with the whole record on
> both sides.

There are two places an incoming record is judged against the local one, and they are different in kind.

**Record level** (`as/src/base/record.c`, `as_record_resolve_conflict`): a replicated or shipped record is
compared whole, by the namespace's `conflict-resolution-policy` - `generation` (generation, then last-update
time), `last-update-time` (LUT, then generation) or the strong-consistency rule - and the answer is left wins,
right wins or tie. For an XDR write that loses, the write fails with `AS_ERR_RECORD_EXISTS` or
`AS_ERR_GENERATION`; the incoming record is dropped and a counter moves. This is where a *record-level*
conflict event belongs: both versions are in hand (the local record is open under its lock, the incoming one
is in the message), and nothing has been written yet.

**Bin level** (EE `rw_utils_ee.c`, `resolve_bin`, when `ship-bin-luts` / bin convergence is on): each
incoming bin op carries its own LUT and source DC id; a bin whose LUT is older than the stored bin's loses,
an equal LUT breaks the tie on source id, and a write where every bin lost fails with `AS_ERR_LOST_CONFLICT`.
The decision is made op by op inside `write_master_bin_ops_loop`, with the record open. This is where a
*bin-level* conflict event belongs, and it is the finer and more frequent of the two.

Both run on a transaction thread, under the record lock, inside the write's deadline. Whatever an event does
there has those constraints: it must finish inside the deadline, it must not touch other records, and a
failure must fall back to the policy rather than leave the write undefined. Note also `forbid_resolve` and
`record_replace_check`: a record that is mid-resolution refuses other writes with
`AS_SUB_FORBID_REPLACE_CONFLICT_RESOLVING`, which is the server already treating "resolving" as a state.

**What already resembles an event hook:** `as_module_hooks` (destroy, update, validate, apply_record,
apply_stream) is the language-module interface, and `as_aggr_flush` is a second hook the query job calls into
a module. A conflict event would be a third `apply_*`-shaped entry: a module function taking two records.

---

## 2. Event systems in other databases

### 2.1 SQL

| system | events | pre / post | what the handler sees | what it can do | registration | privilege |
|---|---|---|---|---|---|---|
| **PostgreSQL** triggers | INSERT, UPDATE (of columns), DELETE, TRUNCATE; row or statement | BEFORE (can veto or modify), AFTER (observe, transition tables), INSTEAD OF on views | `NEW`, `OLD`, `TG_OP`, `TG_TABLE_NAME`, `TG_ARGV`, `TG_WHEN`, `TG_LEVEL`, optional `WHEN (condition)` | BEFORE row: return NULL to skip, return a modified row to change the write; raise to abort | `CREATE TRIGGER ... EXECUTE FUNCTION f()`, ordered by name; `ENABLE`/`DISABLE`, `ENABLE REPLICA`/`ALWAYS` (fires on replication apply or not) | `TRIGGER` on the table plus `EXECUTE` on the function |
| PostgreSQL event triggers | DDL: `ddl_command_start`, `ddl_command_end`, `sql_drop`, `table_rewrite`; filter by tag | before/after the command | `pg_event_trigger_ddl_commands()` | abort | `CREATE EVENT TRIGGER` | superuser only |
| **MySQL / MariaDB** | INSERT, UPDATE, DELETE, row level only | BEFORE, AFTER | `NEW`, `OLD` | BEFORE: modify `NEW`, `SIGNAL` to abort | `CREATE TRIGGER ... FOLLOWS/PRECEDES` | `TRIGGER` on the table |
| **SQL Server** | DML (AFTER, INSTEAD OF), DDL (server/database scope), LOGON | AFTER, INSTEAD OF | `inserted` / `deleted` pseudo-tables, `EVENTDATA()` for DDL | INSTEAD OF replaces the write; `ROLLBACK` aborts | `CREATE TRIGGER`, `DISABLE TRIGGER` | `ALTER` on the table; `ALTER ANY DATABASE DDL TRIGGER`, server-level for logon |
| **Oracle** | DML (row/statement, compound), INSTEAD OF, system (DDL, LOGON, STARTUP, SERVERERROR) | BEFORE, AFTER, INSTEAD OF, `FOLLOWS` ordering | `:NEW`, `:OLD`, `WHEN` clause, event attribute functions | modify `:NEW` in BEFORE, `RAISE_APPLICATION_ERROR` | `CREATE TRIGGER` | `CREATE TRIGGER`, `ADMINISTER DATABASE TRIGGER` for system triggers |
| **SQLite** | INSERT, UPDATE OF, DELETE, row level | BEFORE, AFTER, INSTEAD OF | `NEW`, `OLD`, `WHEN` | `RAISE(ABORT|FAIL|IGNORE|ROLLBACK)` | `CREATE TRIGGER` | none |
| **CockroachDB** | triggers (24.3+) as PostgreSQL's; changefeeds (CDC) | triggers before/after; changefeeds post-commit only | row before/after images | triggers as PostgreSQL; changefeeds observe | `CREATE TRIGGER`; `CREATE CHANGEFEED` | table owner; `CHANGEFEED` privilege |

### 2.2 SQL replication conflict handlers, the closest analogues

| system | when it runs | parameters | returns | logging / unresolved | registration | privilege |
|---|---|---|---|---|---|---|
| **SQL Server merge replication**, custom stored-procedure resolver | during synchronisation, on a row-change conflict for an article (not for PK or unique violations) | `@tableowner`, `@tablename`, `@rowguid`, `@subscriber`, `@subscriber_db`, output `@log_conflict` (whether and which side to log as loser), `@conflict_message` | **one row with the base table's structure: the winning values** | conflict tables per article, viewable in the conflict viewer | `sp_addmergearticle` / `sp_changemergearticle` with `article_resolver` and `resolver_info`; the procedure lives on the Publisher | `EXECUTE` for the logins Subscribers connect with; publication owner to register |
| **EDB Postgres Distributed (BDR)**, conflict triggers | on apply, when a change's replication origin differs or row versions conflict; per event INSERT/UPDATE/DELETE | an ordinary `RETURNS TRIGGER` function with `TG_*` plus the conflict type and both rows; `args text[]` through `TG_ARGV` | a row to apply, or NULL to skip; built-in resolvers otherwise (`update_if_newer`, `keep_local`, `apply_remote`, `skip`, `insert_or_skip`, ...) | `bdr.conflict_history`; false positives possible under origin-based detection | `bdr.create_conflict_trigger(name, events[], relation, function, args)`; table needs `REPLICA IDENTITY FULL` | `TRIGGER` on the relation, `EXECUTE` on the function; takes a global DML lock to create |
| **Oracle GoldenGate CDR** | at Replicat apply; conflict types `INSERTROWEXISTS`, `UPDATEROWEXISTS`, `UPDATEROWMISSING`, `DELETEROWEXISTS`, `DELETEROWMISSING` | before and after images of the row (`GETBEFORECOLS`, supplemental logging); per-column | a handler per conflict type: `USEMAX`, `USEMIN`, `USEDELTA`, `OVERWRITE`, `DISCARD`, `IGNORE`, or `SQLEXEC` to a user procedure | discard file, `REPERROR`, exceptions table via `EXCEPTIONSONLY` MAP | `MAP ... COMPARECOLS ... RESOLVECONFLICT (type, (group, handler))` in the Replicat parameter file; one `RESOLVECONFLICT` per conflict type | the Replicat's database user; GoldenGate admin |
| **MySQL NDB Cluster** replication | at apply, per row (`NDB$OLD`, `NDB$MAX`, `NDB$MAX_DELETE_WIN`, `NDB$MAX_INS`) or per transaction by epoch (`NDB$EPOCH`, `NDB$EPOCH_TRANS`, `NDB$EPOCH2`) | a user-chosen integer timestamp column, or the epoch | the function decides; no user code | an **exceptions table** per table (`<table>$EX`) receiving the losing rows, with the conflicting columns | a row in `mysql.ndb_replication` naming the table, server id and function | the replication user; `mysql` schema write |
| **CockroachDB LDR** | at apply | MVCC timestamps | last write wins only; no user code (UDFs on replicated tables are unsupported) | a **dead-letter queue** table per replicated table for rows that fail constraints or retries | `CREATE LOGICAL REPLICATION STREAM` | admin |
| **YugabyteDB xCluster**, **Cassandra**, **ScyllaDB**, **DynamoDB global tables** | at apply | hybrid time / write time | last write wins, per row or per cell; no hook | counters only | replication config | admin |

### 2.3 NoSQL event systems

| system | events | pre / post | what the handler sees | what it can do | registration | privilege |
|---|---|---|---|---|---|---|
| **MongoDB** change streams | insert, update, replace, delete, drop, rename, invalidate | post-commit only; resumable by token | the change document; `fullDocument`, and `fullDocumentBeforeChange` when pre-images are enabled on the collection | observe | a client opens `watch()`; Atlas Triggers bind a change stream to a serverless Function | `changeStream` / `find` on the collection |
| MongoDB schema validation | insert, update | pre (validation) | the document | reject (`validationAction`) | `collMod` / `createCollection` with `validator` | `collMod` |
| **Couchbase Eventing** | `OnUpdate(doc, meta)`, `OnDelete(meta, options)`, timers | post-mutation, at-least-once, via DCP | document and metadata; a bucket binding to write elsewhere | write to other buckets, call out; cannot veto the mutation | deploy a JavaScript function through the Eventing service (UI/REST) with bucket bindings and a feed boundary | Eventing admin role |
| Couchbase XDCR conflict resolution | at apply | n/a | sequence number or timestamp (CAS / HLC) per bucket; `custom` mode (7.1, developer preview) hands the two documents to a JavaScript merge function on the Eventing service | built-in: higher seqno or newer timestamp wins; custom: merged document | bucket setting `conflictResolutionType`; merge function per replication | cluster admin |
| **CouchDB** | `validate_doc_update(newDoc, oldDoc, userCtx, secObj)`; replication conflicts | validate: pre (every write and replicated write), throw to reject; conflicts: post, deterministic winner by revision tree, losers kept as `_conflicts` | new and old document, the caller; for conflicts, all revisions on read with `conflicts=true` | reject; the application resolves a conflict by writing the winner and deleting the losing revisions | design documents in the database | database admin |
| **Riak** | pre-commit and post-commit hooks per bucket; siblings with `allow_mult` | pre: `fn(object) -> object | fail | {fail, reason}` can modify or veto; post: observe | the Riak object (all siblings, vclock, metadata) | pre: transform or reject; sibling resolution is the reader's job (or a CRDT) | bucket / bucket-type properties `precommit`, `postcommit` naming an Erlang module or JavaScript function | bucket admin |
| **Cassandra** | `ITrigger.augment(partitionKey, update)` | before the mutation is applied (coordinator side) | the partition update | return additional mutations | jar in `conf/triggers`, `CREATE TRIGGER ... USING class` | superuser |
| **DynamoDB** | Streams: INSERT, MODIFY, REMOVE | post-commit, ordered per shard; Lambda consumer | `NewImage`, `OldImage`, `Keys` per stream view type | observe; the write is already done | enable the stream on the table, map a Lambda | IAM on the stream |
| DynamoDB condition expressions | put, update, delete | pre | the current item | reject the write | per request, not registered | n/a |
| **Redis** | keyspace notifications (post, fire-and-forget pub/sub); Triggers and Functions (7.2): keyspace triggers and stream triggers in JavaScript | keyspace triggers run synchronously after the command on the key | key name, event, the key's value through the function API | perform further commands; cannot veto | `TFUNCTION LOAD` of a library registering `redis.registerKeySpaceTrigger(name, prefix, fn)` | `@scripting`, admin for load |
| **Azure Cosmos DB** | pre-triggers and post-triggers (JavaScript) on create, replace, delete | pre: can modify or throw; post: runs in the same transaction | the request body, the item | modify, abort | registered per container, but a request must **name** the trigger to run it | control plane (contributor) |
| Cosmos DB custom conflict resolution | multi-region write conflicts | at conflict detection | `resolver(incomingItem, existingItem, isTombstone, conflictingItems)`: the incoming write (null for a delete), the committed version, whether it collides with a deletion, other committed items in unique-index conflict | the procedure **commits the result itself** (it has normal stored-procedure access within the partition); anything it does not resolve, or an error, goes to the **conflicts feed** for manual resolution | container conflict-resolution policy: `LastWriterWins` on a numeric path, or `Custom` naming the stored procedure (or none, feed only) | control plane; the procedure runs as the system |
| **Firestore** | Cloud Functions `onCreate`, `onUpdate` (`change.before`/`change.after`), `onDelete`, `onWrite` | post-commit, at-least-once | the document snapshots | observe, write back | deploy a function with a document path pattern | project IAM |
| Firestore Security Rules | read, write | pre | `request.resource`, `resource` | allow or deny | rules deployment | project IAM |
| **Neo4j** APOC triggers | node/relationship create, delete, property set | phases `before`, `after`, `rollback`, `afterAsync` | transaction data: created/deleted nodes, assigned properties | Cypher in the same transaction | `apoc.trigger.add(name, statement, {phase})` | admin |
| **Elasticsearch** ingest pipelines | index | pre | the document | transform or drop | `PUT _ingest/pipeline`, named per index | `manage_pipeline` |

### 2.4 The patterns worth taking

- **Two kinds, never confused.** A *pre* hook runs inside the operation with a veto and a chance to change the
  outcome; a *post* hook is a notification that cannot change anything. Every system above keeps them apart,
  and the conflict handlers are all pre hooks.
- **A conflict handler gets both versions and the conflict's kind.** Cosmos spells the kinds out
  (incoming-vs-committed, incoming-vs-tombstone, unique-index collisions); GoldenGate names five; BDR
  distinguishes insert, update and delete conflicts; SQL Server limits itself to row-change conflicts and says
  so. Each passes metadata that identifies the sides: the subscriber, the replication origin, the site.
- **The return is a whole row, or a verdict.** SQL Server returns the winning row; BDR returns a row or NULL to
  skip; Cosmos's procedure writes the result itself; GoldenGate and NDB choose among built-in verdicts. A
  verdict vocabulary (`keep-local`, `take-remote`, `skip`, `merged(record)`) covers all of them.
- **Unresolved is a first-class outcome.** Exceptions tables (NDB), conflict history (BDR), the conflicts feed
  (Cosmos), dead-letter tables (CockroachDB), conflict tables (SQL Server): a handler that fails or declines
  leaves the conflict somewhere a person can find it, and the built-in policy decides the write meanwhile.
- **Registration is declarative and scoped.** A binding of (event, scope, function, arguments, enabled) kept
  in the catalogue - DDL, a catalogue table, bucket properties, a container policy, a replication article
  property. The function is deployed separately from the binding, so the same function can serve several
  bindings and a binding can be switched without redeploying code.
- **Privilege is two-sided.** Registering requires authority over the scope (the table or bucket) *and* over
  the function; event triggers that affect everything need the top role. Running happens with the server's
  authority, which is why registration is the gate.
- **Determinism matters for convergence.** Riak and CouchDB leave resolution to the application and warn that
  the merge must be deterministic or the sites diverge; Cosmos runs the procedure in the region that detects
  the conflict and commits there. A resolver that two DCs run on the same pair of records in opposite order
  must produce the same record, or produce one both sides will then agree on by the ordinary policy.

---

## 3. What this maps to in Aerospike

A first cut of the event vocabulary, so the XDR case is the first of a family rather than a one-off:

| event | fires | pre / post | sees | returns |
|---|---|---|---|---|
| `xdr-conflict` (record) | an XDR write loses or ties at the record level, before the write fails | pre | local record (bins, generation, LUT, TTL), incoming record (bins, generation, LUT, source DC), namespace, set, key or digest, the policy in effect, kind: `record-lost`, `record-tie`, `write-over-tombstone`, `tombstone-over-write` | `keep-local`, `take-remote`, `skip`, or `merged(record)` |
| `xdr-conflict` (bin) | an incoming bin op loses in `resolve_bin` | pre | as above, plus the bin name and both bin values with their LUTs and source ids | `keep-local`, `take-remote`, `merged(value)` for that bin |
| `write` / `delete` (later) | before a client write or delete commits | pre | the record before and the ops | veto or modify; this is what makes "triggers" general |
| `written` / `deleted` / `expired` / `evicted` (later) | after the fact | post | key, digest, before and after images if retained | nothing; a notification for XDR-like consumers or UDFs |

Design constraints the server imposes, from §1:

- The handler runs on the transaction thread under the record lock, with the write's deadline. The WASM
  engine already enforces a deadline through its watchdog and runs a module in 0.3 µs per record at the
  vector workload's size; Lua has no deadline. A resolver that traps, times out or returns something invalid
  falls back to the configured policy, and the fallback is counted and logged, as every system in §2.2 keeps
  its unresolved conflicts somewhere visible.
- The resolver must be side-effect free: no `aerospike:create/update` on other records, no writes at all
  except its return value. A read-only context already exists: aggregations run their records read-only.
- The merged record's metadata is the hard part: a resolved record that takes a fresh LUT would be shipped
  back by XDR and may conflict again on the other side. Options are to write it with the larger of the two
  LUTs and the source id that would have won (so the other DC's policy accepts it without a further event),
  or to mark it as a resolution so XDR ships it with precedence. This is the question Riak and CouchDB push
  to the application and Cosmos avoids by resolving in one region; it deserves its own discussion.
- The UDF contract needs two records. Every existing UDF path (Lua, native, WASM) takes one. The WASM ABI
  is the cheapest to extend: a second record handle in the context and a `which` argument on the record
  natives, or a `other_bin_value` family; the SDK's `export!` could then type a parameter as `Record`. The
  return `Result<Record>` fits the WASM SDK's `Result<Value, Error>` shape already: `Ok(Value::Map)` with the
  bins, or a `Verdict` enum.

**Registration.** The precedent in this server is SMD: UDF files and secondary indexes are registered through
info commands, stored in SMD and replayed on every node. An event binding is the same kind of object:

```text
event-set:event=xdr-conflict;namespace=<ns>[;set=<set>];module=<udf file>;function=<fn>[;on-failure=policy|fail]
event-remove:event=xdr-conflict;namespace=<ns>[;set=<set>]
event-list[:namespace=<ns>]
```

with the function deployed beforehand by `udf-put`, exactly as BDR separates the trigger function from the
binding and SQL Server separates the resolver procedure from the article property. A namespace config knob
(`xdr-conflict-resolver`) would be the static form of the same binding; SMD gives it dynamically and
cluster-wide, which is what the index and UDF registries chose.

**Privilege.** What exists:

| permission bit | info commands | in privilege |
|---|---|---|
| `PERM_UDF_ADMIN` (`udf-admin`) | `udf-put`, `udf-remove` | `data-admin`, and therefore `sys-admin` |
| `PERM_XDR_SET_FILTER` (`xdr-set-filter`) | `xdr-set-filter` | `sys-admin` only (ops permissions) |
| `PERM_SET_CONFIG` | `set-config` | `sys-admin` only |
| `PERM_NONE` | `udf-list`, `xdr-dc-state`, `xdr-get-filter` | everyone |

So the nearest existing fit is `udf-admin`: it already lets its holder deploy code that runs with the
server's authority on every user's data, and binding that code to an event is the same trust. The argument
for `sys-admin` instead is that an XDR conflict resolver changes replication semantics, which is where
`xdr-set-filter` was placed. The argument for a **new** bit, `PERM_EVENT_ADMIN`, surfaced as a privilege
named `event-admin` and included in `data-admin`, is the one §2.4 makes: registration is two-sided
(authority over the scope and over the function), and an operator may well want to let someone manage event
bindings without granting UDF upload. Adding a privilege is cheap on both sides - the permission bits have
room, the EE privilege codes have room below 10 for a global one, and the client's `PrivilegeCode` enum is
non-exhaustive by design. `event-list` would be `PERM_NONE` like `udf-list`.

---

## 4. Questions to settle before any code

Settled 2026-10-20, in order: (1) bin-level, seeing the whole record on both sides - the record-level kind
has no hook (see the correction in §1); (2) both verdicts and a full record, stamped with the later side's
LUT and source id and marked as an XDR write, so the other DC converges; (3) WASM only; (4) fall back to the
policy and count it, `on-failure=fail` per binding opts into failing the write; (5) SMD-backed info commands,
per namespace with a per-set override; (6) a new `event-admin` privilege under `data-admin`. Implementation
notes are in `WASM_UDF_DESIGN.md` §7.

1. Record-level, bin-level, or both as the first event - and whether a bin-level event sees the whole record
   or only the bin.
2. The return vocabulary: verdicts only, a full record, or both - and what the merged record's generation,
   LUT and source id are, so the other DC converges rather than fires again.
3. Which language runs it: all three UDF engines through a new `as_module_hooks` entry, or WASM first because
   it is the one with a deadline and a sandbox.
4. The failure policy: fall back to the configured conflict-resolution policy, fail the XDR write (it will be
   retried), or park the conflict somewhere queryable - and which of those is the default.
5. Registration: SMD-backed info commands, a namespace config knob, or both; per namespace or per set.
6. Privilege: `udf-admin`, `sys-admin`, or a new `event-admin` under `data-admin`.

Sources consulted for §2 beyond the systems' general documentation:
[Cosmos DB conflict resolution policies](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-manage-conflicts),
[SQL Server custom merge conflict resolver](https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2012/ms147290(v=sql.110)),
[EDB PGD stream and conflict triggers](https://www.enterprisedb.com/docs/pgd/latest/reference/tables-views-functions/streamtriggers/interfaces/),
[Oracle GoldenGate CDR overview](https://docs.oracle.com/en/middleware/goldengate/core/21.3/ggcab/overview-oracle-goldengate-cdr-feature.html),
[MySQL NDB Cluster conflict resolution](https://dev.mysql.com/doc/refman/8.4/en/mysql-cluster-replication-conflict-resolution.html),
[CockroachDB logical data replication](https://www.cockroachlabs.com/docs/stable/manage-logical-data-replication),
[Couchbase conflict resolution types](https://docs.couchbase.com/sdk-api/couchbase-java-client/com/couchbase/client/java/manager/bucket/ConflictResolutionType.html).
