# Server events: the catalogue after `xdr-merge`

What other databases hang on their write, read, expiry and cluster paths, and which of those would fit
the event system that `xdr-merge` (then `xdr-conflict`) introduced (`WASM_UDF_DESIGN.md` §7): a WASM function bound per
namespace or set through SMD, a versioned signature `event <kind>/<n>`, a dry run, counters, a per-binding
time budget and the `event-admin` privilege. Each entry says where the hook would sit in the server, what
the handler would see and answer, and what it costs. Written 2026-10-20 from the sources at the end.

## 1. What the survey found, by system

| System | Mechanism | Shape | Worth taking |
| --- | --- | --- | --- |
| Riak KV | pre-commit / post-commit hooks per bucket type | pre: return the object, a modified object, or `fail` (403); post: notified, must not modify; deletes are writes with an `X-Riak-Deleted` flag; docs warn of feedback loops from hooks that write | the closest ancestor of ours: a KV store, hooks per bucket, allow/modify/reject |
| Apache Ignite | `CacheInterceptor` | `onBeforePut` returns the value to store or `null` to cancel; `onBeforeRemove` returns (veto, value); must not lock, network or touch other caches | the veto-or-rewrite contract, and the "no other cache access" rule we already have |
| Hazelcast | `MapInterceptor` | `interceptGet/Put/Remove` before, `afterGet/Put/Remove` after | the read-side interceptor |
| Oracle Coherence | live events + `EventInterceptor` | `EntryEvent` INSERTING/UPDATING/REMOVING (pre-commit, synchronous) and INSERTED/… (post-commit, async on a dispatcher thread); `TransferEvent` DEPARTING/ARRIVED raised under the partition lock; `LifecycleEvent` ACTIVATED/DISPOSING; `CacheLifecycleEvent` CREATED/DESTROYED/TRUNCATED | the full taxonomy: record, partition, lifecycle; sync-before vs async-after |
| Couchbase Eventing | JavaScript handlers per bucket | `OnUpdate(doc, meta)` (coalesces rapid edits, cannot tell create from update), `OnDelete(meta, options.expired)`, `OnDeploy(action)`, timers bound to a document with a reference id; a failing timer can block the timer queue | expiry as a delete with a flag; timers as "run this on that document at that time"; a deploy hook |
| Redis | keyspace notifications (pub/sub, lossy, `expired`/`evicted`), Triggers & Functions keyspace triggers (JS, by key prefix; now deprecated) | fire-and-forget notification; the trigger's worked example is "stamp `last_updated` on every HSET" | expiry and eviction as events; the stamping use case; a warning that an in-database scripting tier can be withdrawn |
| DynamoDB | TTL deletes appear in Streams with `userIdentity.principalId = dynamodb.amazonaws.com` | the expiry is a normal delete event, distinguishable by provenance; only in the region where TTL ran, not where it replicated | provenance on an event (who caused it) instead of a separate kind |
| HBase | coprocessor `RegionObserver` | pre/post for get, put, delete, scan, flush, compaction, region open/close | the widest hook list; the storage-side hooks (flush, compaction) are the ones nobody else has |
| RocksDB | compaction filter, merge operator | drop or rewrite entries lazily during compaction; merge operands combined at read or compaction | the lazy-garbage idea; the merge operator is what `xdr-conflict` already does for one case |
| Cassandra | `ITrigger.augment(Partition)` | a trigger adds mutations to the same partition, applied atomically with the write; stateless, superuser-deployed jar | "augment, do not replace": the handler's output is merged with the write |
| CockroachDB | `CREATE TRIGGER` (PL/pgSQL, row-level, BEFORE/AFTER, `WHEN` condition), changefeeds | a `WHEN` predicate gates the trigger | the predicate gate: an expression decides whether the function runs at all |
| PostgreSQL | event triggers `login`, `ddl_command_start/end`, `sql_drop`, `table_rewrite` | DDL and login as events; a buggy login trigger locks everyone out, so there is a config switch and single-user mode to disable them | admin and login events, and the escape hatch |

## 2. Candidate events

Each has the same spine as `xdr-merge`: binding `kind|ns|set`, signature `event <kind>/1`, dry run,
counters, `on-failure`, `timeout=`. What differs is the payload, the answer and where it fires.

### 2.1 Record write path, synchronous, under the record lock

**`write-before`** (Riak pre-commit, Ignite `onBeforePut`, Coherence INSERTING/UPDATING, SQL BEFORE trigger).
Fires in `write_master` after the ops loop has produced the new bins and before the record is committed to
index and storage, for every write that reaches the master: client, batch, UDF, and XDR (tagged
`source: xdr` with its source id, which is how DynamoDB marks provenance). The handler sees the record as it
was (`before: Record`), the record as the write would leave it (`after: Record`), and the write's metadata
(generation, requested TTL, user, client address, source). It answers `accept`, `reject(code, message)`, or
`record {bins, ttl}` (Cassandra-style augmentation: the handler's bins are merged over `after`). Uses:
constraints, derived or stamped bins (`last_updated`, the Redis example), normalisation, PII stripping on
import. Cost: a WASM call on every write to a bound set, so it needs a **filter expression on the binding**
(`filter=<base64 expression>`, like CockroachDB's `WHEN`): the server evaluates the expression against
`after` first and runs the function only when it matches. On an SC namespace it runs before replication, so
replicas never see a rejected or rewritten write.

**`delete-before`** (Ignite `onBeforeRemove`, Riak `X-Riak-Deleted`). Same hook, for client deletes:
`allow`, `refuse(code, message)`, or `record {bins, ttl}` to turn the delete into a soft delete. Durable
deletes and truncation stay outside it.

**`write-after` / `delete-after`** (Riak post-commit, Coherence INSERTED/REMOVED async, SQL AFTER trigger).
After commit, off the record lock, on a service thread fed by a bounded queue (Couchbase coalesces rapid
successive edits to one document; so should this). The handler sees `before`/`after` and metadata and
answers nothing. It cannot write other records, so its value is observation: counters, logging, and an
**outbox** - a bin-less record the handler may write into a designated set, which XDR then ships to a
connector. The Riak warning applies: writes made from inside a handler must carry a mark so they do not
re-fire the event (a `by-event` flag on the transaction).

### 2.2 Record lifecycle driven by the server

**`expire`** (DynamoDB TTL stream, Redis `expired`, Couchbase `OnDelete` with `expired`). Fires from the
namespace supervisor when a record's void time has passed, before it is removed. The handler sees the
record and answers `expire`, `keep {ttl}` (extend; a bounded number of times, so a handler cannot pin a
record forever), or `record {bins, ttl}` (archive in place: keep a summary bin, drop the rest). Cost:
nsup is a sweep; a filter expression gate is mandatory, and the function runs on the nsup thread with its
own budget.

**`evict`** (Redis `evicted`). Same shape at the eviction path, with a narrower answer: `evict` or
`protect` with a per-namespace quota of protected records, since protection fights the memory ceiling that
caused the eviction.

**`truncate-before` / `truncate-after`** (Coherence `CacheLifecycleEvent` TRUNCATED, PostgreSQL
`sql_drop`). Once per truncate command, not per record: the handler sees namespace, set, the LUT
threshold and the user, and may `allow` or `refuse` - a guard rail against the one command that empties
a set in a heartbeat. Cheap, and the same for `set-delete`.

### 2.3 Reads

**`read-after`** (Hazelcast `interceptGet`/`afterGet`, HBase `postGet`, SQL views). Transforms the
record before it is returned: redaction by the caller's roles, computed bins, format shims during a
migration. The Enterprise masking feature covers the static cases; this is the programmable one. Cost:
every read of a bound set - filter expression and a per-user cache of "does this user need it" are the
difference between usable and not. Applies to query and scan results through the same hook.

### 2.4 Partition and cluster lifecycle, once per event, no record

**`partition-transfer`** (Coherence `TransferEvent` ARRIVED/DEPARTED, HBase region open/close). After a
migration completes on this node: namespace, partition id, direction, record count. Answers nothing. Uses:
warming, metrics, telling an external system which node owns what. Note Coherence raises it under the
partition lock; ours should run after the lock is released.

**`cluster-change`** (Ignite node events, Coherence member events). After exchange completes: the new
cluster key, node list, roster, and what changed. Answers nothing.

**`node-lifecycle`** (Couchbase `OnDeploy`, Coherence ACTIVATED/DISPOSING, Oracle STARTUP/SHUTDOWN
triggers): `service-ready`, `namespace-loaded`, `shutdown-begun`, and `binding-deployed` (the handler's own
deploy, which is where a module seeds state or registers timers).

### 2.5 Timers

**`timer`** (Couchbase timers, bound to a document and a reference id). "Run function `f` on key `k` at
time `t`", stored in SMD or a system set, fired by a per-namespace scheduler thread. The handler runs as an
ordinary record UDF on `k` - the machinery `udf_apply_record` already provides - so it can read and write
that record. Recurrence is the handler scheduling the next timer. A failing timer must be dropped and
counted, not retried forever (the Couchbase pitfall). This is the one event that gives handlers a way to
act later, and it needs `node-lifecycle` for the scheduler to re-arm after a restart.

### 2.6 XDR, beyond the conflict

**`xdr-ship-before`**. In `ship.c` before a record is shipped to a DC: the handler sees the record and the
destination DC and answers `ship`, `skip`, or `record {bins}` (strip bins per destination). XDR filter
expressions already do `ship`/`skip`; the function adds the transform. One-directional by nature, so the
symmetry rule of `xdr-merge` does not apply, and the design doc should say that a transformed ship
cannot converge with a conflict resolver that expects the original bins.

Not a new kind: an incoming XDR write with no conflict is `write-before` with `source: xdr`.

### 2.7 Security and administration

**`login`** (PostgreSQL 17 login event trigger, Oracle LOGON, SQL Server logon triggers). After
authentication, before the session is granted: user, roles, auth mode, client address, TLS name. Answers
`allow` or `deny(reason)`. Uses: time windows, a second factor checked against a bin, audit with context the
allowlist cannot express. Needs the PostgreSQL escape hatch: a config switch that disables login events,
and the rule that `sys-admin` sessions are never subject to it.

**`admin-command-before`** (PostgreSQL `ddl_command_start`). Before an info command that changes state:
`set-config`, `truncate`, `udf-put`, `sindex-create`, `event-set`. The handler sees the command, its
parameters and the user, and may `allow` or `refuse`. Same escape hatch, and one more rule: a binding
cannot refuse the `event-remove` that would remove it.

### 2.8 Storage-level, from RocksDB and HBase, not recommended

A **defrag filter** (RocksDB compaction filter, HBase `preCompact`) would let a handler drop or rewrite
records lazily as the defragmenter moves them: lazy expiry by content, garbage by rule. It is subtle (a
record may be rewritten long after the rule changed, replicas disagree until their own defrag runs) and
nsup plus `expire` cover the uses. A **merge operator** (RocksDB) defers combining operands to read time;
Aerospike has no operands to defer, and the one merge it needs is the conflict resolver that exists.
A **change feed** (MongoDB change streams, CockroachDB changefeeds, DynamoDB Streams, Couchbase DCP) is an
output, not a hook, and XDR with connectors is already that; `write-after` with an outbox feeds it rather
than duplicating it.

## 3. Cross-cutting design points the first event did not need

- **A filter expression on the binding**, evaluated before the function runs, for every per-record event.
  Without it `write-before`, `read-after` and `expire` cost a WASM call per operation on the set.
- **Synchronous before, asynchronous after.** Before-events run under the lock, answer, and can veto; after-
  events run on a service thread from a bounded queue, answer nothing, and may be coalesced or dropped under
  pressure (counted when they are). Coherence draws exactly this line.
- **Provenance in the payload** (`source: client | batch | udf | xdr | nsup | event`), the DynamoDB lesson,
  so one kind serves several causes and a handler can tell its own writes apart.
- **No cascades.** A write a handler causes carries a mark that prevents re-firing (Riak's loop warning);
  `timer` is the sanctioned way to act later.
- **An escape hatch** for the events that can lock an operator out (`login`, `admin-command-before`): a
  config switch, exemption for `sys-admin`, and `event-remove` never refusable by the binding it removes.
- **Budgets.** Every event has `timeout=`; after-events also have a queue depth; `expire`/`evict`/`read-after`
  have per-namespace rate caps so a handler cannot stall the supervisor or the read path.
- **Dry run per kind.** `__event__` with the kind as the function name, the way `xdr-merge` has it.

## 4. Recommended order

1. **`write-before`** with the filter-expression gate and `source` provenance. The most asked-for hook in every
   system surveyed (validation, stamping, constraints), the same shape as `xdr-merge` (two records in,
   a record or verdict out), and it subsumes the inbound-XDR transform.
2. **`truncate-before`** and **`admin-command-before`**: once per command, trivial cost, large operational
   value, and they exercise the escape-hatch design before `login` needs it.
3. **`expire`** with the gate: archive-in-place and notify are the DynamoDB and Redis use cases, and it is
   the first event on a server thread rather than a transaction thread.
4. **`node-lifecycle`**, **`cluster-change`**, **`partition-transfer`**: cheap, and `timer` needs the first.
5. **`timer`**: the one that lets handlers act later, built on `udf_apply_record`.
6. **`write-after`** with the outbox, once there is a consumer for it.
7. **`login`**, **`read-after`**, **`xdr-ship-before`**, **`evict`**: each real, each narrower.

## Sources

- Couchbase Eventing: [terminology (OnUpdate, OnDelete, OnDeploy)](https://docs.couchbase.com/cloud/eventing/eventing-Terminologies.html), [timers](https://docs.couchbase.com/cloud/eventing/eventing-timers.html), [timers and handlers blog](https://www.couchbase.com/blog/eventing-improvements-timers-handlers-and-statistics/)
- Redis: [keyspace notifications](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/), [triggers and functions (deprecated)](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/deprecated-features/triggers-and-functions/examples/), [JavaScript API](https://redis.antirez.com/commands/content/operate/oss_and_stack/stack-with-enterprise/deprecated-features/triggers-and-functions/concepts/JavaScript_API.md)
- Apache Ignite: [CacheInterceptor](https://ignite.apache.org/releases/latest/javadoc/org/apache/ignite/cache/CacheInterceptor.html), [CacheInterceptorAdapter](https://ignite.apache.org/releases/latest/javadoc/org/apache/ignite/cache/CacheInterceptorAdapter.html)
- Hazelcast: [MapInterceptor](https://docs.hazelcast.org/docs/3.9/javadoc/com/hazelcast/map/MapInterceptor.html)
- Oracle Coherence: [Using Live Events (15.1.1)](https://docs.oracle.com/en/middleware/standalone/coherence/15.1.1/develop-applications/using-live-events.html), [com.tangosol.net.events](https://docs.oracle.com/cd/F83331_01/java-reference/com/tangosol/net/events/package-summary.html)
- RocksDB: [compaction_filter.h](https://git.nextgraph.org/NextGraph/rocksdb/src/commit/c7cf981a8565d0cb9278fa4c142cd3b5c44652df/include/rocksdb/compaction_filter.h), [merge operator](https://www.mintlify.com/facebook/rocksdb/api/merge-operator)
- HBase: [RegionObserver (2.2)](https://hbase.apache.org/2.2/devapidocs/org/apache/hadoop/hbase/coprocessor/RegionObserver.html), [coprocessor guide](https://docs.arenadata.io/en/ADH/current/how-to/hbase/use-coprocessors.html)
- Cassandra: [ITrigger (3.11)](https://code.yawk.at/org.apache.cassandra/cassandra-all/3.11.5/org/apache/cassandra/triggers/ITrigger.java), [triggers in 2.0](https://datastax.com/blog/whats-new-cassandra-20-prototype-triggers-support)
- CockroachDB: [triggers](https://www.cockroachlabs.com/docs/dev/triggers), [CREATE TRIGGER](https://docs.cockroachlabs.com/docs/stable/create-trigger.md), [changefeeds](https://docs.cockroachlabs.com/docs/stable/change-data-capture-overview.md)
- PostgreSQL: [event trigger behaviour (login, ddl_command_start, sql_drop, table_rewrite)](https://www.postgresql.org/docs/current/event-trigger-definition.html)
- Riak KV: [Using Commit Hooks](https://docs.riak.com/riak/kv/2.2.0/developing/usage/commit-hooks)
- DynamoDB: [Streams and Time to Live](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/time-to-live-ttl-streams.html)
