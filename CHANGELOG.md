# Changelog

## [3.0.0]

Upgrading from 2.x: see the [migration guide](https://github.com/aerospike/aerospike-client-rust/blob/v3/MIGRATION.md).

* **New Features**
  * [CLIENT-5387] `TlsPolicy` (Java parity): `ClientPolicy::tls_config: Option<rustls::ClientConfig>`
    is replaced by `ClientPolicy::tls_policy: Option<TlsPolicy>`, which wraps the `rustls::ClientConfig`
    (`TlsPolicy::new(config)`, or `config.into()`) and carries the settings that govern *when* TLS is
    used rather than how. **Breaking**: `policy.tls_config = Some(cfg)` becomes
    `policy.tls_policy = Some(TlsPolicy::new(cfg))`.
  * [CLIENT-5387] `TlsPolicy::for_login_only` (Java `TlsPolicy.forLoginOnly`): encrypt the
    authentication exchange and run the data plane in cleartext. The login rides TLS; the client then
    reads the node's non-TLS address (`service-clear-*`), closes the TLS connection and reconnects
    there, and every pooled/tend connection afterwards is plain TCP authenticated with the session
    token. Credentials never cross a cleartext socket: a missing, expired or rejected token is renewed
    over a short-lived TLS connection, and a cleartext `LOGIN` is refused outright. Peers are still
    discovered and validated by their TLS addresses (as in Java) and switched to cleartext one by
    one. Requires an `auth_mode` other than `AuthMode::None`. **This trades away data-plane
    encryption** and is off by default.
  * `PartitionFilter` implements `Serialize`/`Deserialize` under the `serialization` feature: the
    partition range and each partition's resume point (id, retry, bval, digest — the same fields the Go
    client persists) round-trip, so a paginated query can hand its cursor to another process and
    continue there. Deserialization rejects a cursor whose range or entries are inconsistent.
  * [CLIENT-5626] `BatchRecord` is built around one `Error`. A row is failed when `error()` is set, succeeded when
    `record` is (every answered row has one, bin-less for an operation that returns nothing), pending
    when neither is; everything about a failure — result code, in-doubt, node, server detail, cause
    chain — is read off that error: `result_code()`, `in_doubt()`, `node()`, `error_detail()`,
    `sub_code()`, `server_message()`, and `error()` itself (so `matches()` and the rest of `Error`
    apply per row). A row the server answered `KeyNotFound` or `FilteredOut` is a failed row in this
    sense, as the single-key `get` would be; the batch call still succeeds. `Error` is now
    `Clone`. **Breaking**: the public
    fields `result_code` and `in_doubt` are methods — `row.result_code` becomes `row.result_code()`.
    A client-side failure with no server code (a connection loss) reads as `result_code() == None`
    with the failure on `error()`; a client timeout reads as `Timeout`, as before. An unanswered
    row's in-doubt now follows the failure's own flag, so a timeout before anything reached the
    wire no longer marks rows in doubt.
  * [CLIENT-5626] `Error` serializes (under `serialization`) as a structured record — `kind` (the variant name,
    also `ErrorKind::name()`), `result_code`, `message`, `node`, `iteration`, `in_doubt`,
    `server_error_detail`, `sub_errors` and `source`, the last two recursively — so a serialized
    `BatchRecord` (`key`, `record`, `error`, `has_write`) carries the whole failure, not a projection.
  * [CLIENT-5626] `BatchRecord::node()` (and `BatchOperation::node()`): the node that answered, or whose failure
    stamped, a batch row, in the same `"<name>: <host:port>"` form as `Error::node()`.
  * [CLIENT-5626] `Error::matches(&[ResultCode])` and `Error::matches_client(&[ClientResultCode])` search the whole
    cause chain for any of the given codes (Go `Matches` parity); `server_result_code()` still reports
    only the first server code it meets.
  * [CLIENT-5626] `server_error::sub_code::name(rc, sub_code)` gives a subcode's constant name, scoped by its parent
    result code; `Error`'s `Display` shows it beside the number (`SubCode: 1 (OPNOT_CDT_INDEX_OUT_OF_BOUNDS)`).
    `sub_code::UNAVAIL_NODE_SHUTTING_DOWN` (3, server 8.2.1+) joins the table: the node is leaving the
    cluster, so fail over rather than retry it.
  * `ResultCode::InvalidEncoding` (29, "Invalid UTF-8 encoding", server 8.2.0+) and the
    `OpNotApplicable` subcode `OPNOT_STRING_REGEX_LIMIT_EXCEEDED` (12) are decoded instead of
    falling into `Unknown`.
  * [CLIENT-4586] `Client::create_set_index(policy, namespace, set, name)` and `CollectionIndexType::Set`: a set
    index (record presence per set) created through the sindex framework with no bin, type, context
    or expression, so the `sindex-admin` role suffices. Server 8.1.2+, reported by
    `Version::supports_set_index`. Parity with Go `CreateSetIndex` (CLIENT-4315) and Java's
    four-argument `createIndex` (CLIENT-4316). Also on the sync client.
  * [CLIENT-4390] `IndexType::Integer` (`INTEGER`) for secondary indexes on server 8.2.0+, reported by
    `Version::supports_integer_index`; `IndexType::Numeric` remains for older servers.
  * [CLIENT-5462] Query delivery reworked and an exactly-once callback API added. Records cross the
    channel in batches of up to 64 and the resume cursor now commits when a record reaches the
    consumer, not when it is parsed: a stream closed early no longer loses the records still in
    the channel buffer, and a resume from `partition_filter()` no longer skips them (measured
    before: 7.9% of a 200k scan lost on a consume-half-then-cancel loop). Scan throughput +35%
    with bin data, +56% without; paged queries -10% wall / -26% CPU. The new
    `Client::query_foreach(policy, partition_filter, statement, callback)` has the C client's
    `aerospike_query_foreach` shape: the callback runs inline on the node streams (serially per
    node, concurrently across nodes), returning `false` aborts, and the cursor commits as each
    invocation returns, so abort, cancel and resume neither lose nor repeat a record. The returned
    `QueryHandle` can `wait()`, `cancel()` and hand back a `partition_filter()` usable with
    `query()`.
  * [CLIENT-5461] The `query_foreach` callback is async: `Fn(Result<Record>) -> Fut` with
    `Fut: Future<Output = bool>`, the shape of the `batch_foreach` row hook. A pending callback
    stalls only its own node's stream; it may await freely but must not block the thread.
  * [CLIENT-5527] Metrics aligned with the metrics specification. Two tiers: `MetricsPolicy.operational`
    (default off, `with_operational`) gates every per-command instrument and the operational
    counters (connection failures by phase, close reasons, pool empty/overflow, circuit-breaker
    hits, transaction retry/error); the base tier keeps opened/closed, tend, node add/remove and the
    pool gauges. One sampler draw per call, so retries never re-roll. Histogram buckets are
    `(2^(i-1), 2^i]` spaced by `latency_shift` (default 1); `HistogramType`, `Linear` and
    `latency_base` are gone and the default policy is `millis()` (ms × 7). Connection opens tag a
    failure with its TCP/TLS/auth phase, closes carry a `CloseReason`, and the pool gauges
    (`open_connections`, `connections_in_use`, `connections_in_pool`, `connections_recovering`)
    are readable even while collection is disabled. Per-namespace `latency` spans connection
    acquire to response parsed; `bytes_sent` / `bytes_received` count exactly what crossed the
    wire on every attempt, whatever the outcome. **Breaking** for consumers of the snapshot JSON:
    every field is `snake_case` (`cluster_aggregated_metrics`, `detailed_metrics`,
    `connections_error_tls`, …) and the node label `app-id` is `app_id`.
  * [CLIENT-5498] `Client::server_cluster_name()` and `Node::cluster_name()` report the name the
    server announces, captured on every tend. The metrics `cluster` label falls back to it when
    `ClientPolicy.cluster_name` is unset, which stays a validation-only setting.
  * [CLIENT-4963] A commit that fails in doubt leaves the transaction in `TxnState::CommitFailed`,
    and `abort` is refused from there (`TxnFailed`): the server may still be rolling the
    transaction forward, so an abort could discard writes it is committing. Retry the commit.

* **Improvements**
  * `AuthMode` no longer prints the password in `{:?}`; `task::Status` compares with `==`; every
    expression and operation builder is `#[must_use]`, so a built-and-dropped expression warns.
  * The workspace builds clean under `clippy::pedantic` + `clippy::nursery` (core) and default clippy
    (every other crate, the examples, the benchmark tool and the integration tests) for every feature
    set, including `rt-async-std`. Visible side effects: `ToValue`/`FromValue` for `HashMap` accept
    any hasher; a secondary-index query plan missing its index name or range is an error instead of
    a panic; `Value::Unknown` reaches Lua as bytes through the same arm as a blob.
  * Packaging: `rt-async-std` together with `tls` fails with the one-line "TLS support is only
    available for the tokio runtime" guard instead of trait-bound errors in the connection layer.
    `aerospike-rt` defaults to `rt-tokio` so it builds, documents and publishes on its own; the
    client crates depend on it without default features and still choose the runtime through
    their `rt-*` feature. The published `aerospike` tarball carries only the sources, examples,
    benches, tests and the four documents (no CI, registers or agent notes); every crate ships the
    full Apache 2.0 licence text; the in-tree crate versions are spelled once in
    `[workspace.dependencies]`; unused dependencies (`lazy_static`, `bencher`, `ripemd`) and
    core's direct `tokio` dependency are gone.
  * Dependencies: `tls` builds rustls with the `ring` crypto provider instead of `aws-lc-rs`, so
    building the client needs no cmake or C toolchain and exactly one provider is compiled in (a
    crate that also enables rustls's `aws-lc-rs` must install a process-level `CryptoProvider`,
    as rustls requires). Password hashing uses the `bcrypt` crate instead of `pwhash`; the hashes
    are byte-for-byte the same (`$2a$`, cost 10, the fixed Aerospike salt). The YAML provider
    reads with `serde_norway` (serde_yaml's maintained continuation) instead of `serde_yml`.
    `rt-async-std` is maintenance only: async-std is discontinued upstream, so the runtime stays
    for existing users and may be removed in a later major release.
  * Repository: `CONTRIBUTING.md` and `SECURITY.md`; the README's sync section describes the
    self-driving blocking client (no Tokio runtime to set up).
  * Four internals stay reachable for language bindings, hidden from the documentation:
    `query::PartitionStatus` with `PartitionFilter::partitions` (rebuilding a cursor),
    `Statement::set_aggregate_function`, the `Txn::set_state` test hook and `Txn::set_timeout`
    (a settable timeout attribute on a transaction the binding already built).
  * `Client::is_strong_consistency(namespace)` is a documented API: a synchronous lookup in the
    cached partition map that bindings call per command to pick the AP or SC policy.
  * `BatchOperation::into_batch_record(self)` moves a finished row's `BatchRecord` out without
    cloning the key, for callers that hand rows on by value.
  * `PrivilegeCode::Unknown(u8)`: a privilege code the server reports that this client has no
    name for is kept with its raw value instead of failing the role query as a bad response.
  * [CLIENT-5351] Batch sub-requests check their connection out of the pool queue chosen by the
    group's first digest byte, as single-key commands do, instead of always queue 0, so batch load
    spreads across `conn_pools_per_node` (ported from 2.2.0).
  * [CLIENT-5409] Record streams park until the producer wakes them instead of busy-polling an
    empty channel; the per-node partition state is locked once per stream body rather than once
    per record, the shared tracker is released before a record is pushed (a full queue no longer
    stalls every other node's stream), and the buffered read cache grows from 4 KB to 64 KB.
  * [CLIENT-5414] `TCP_NODELAY` is set on every connection.
  * [CLIENT-4581] README: an "AI coding agent entry point" section (where the API lives, how to
    select features, how to verify generated code), referenced from `AGENTS.md`.
  * [CLIENT-5291] API documentation pass over the client, expressions, path operations, record
    sets and result sets, plus docs.rs metadata for the crates.
  * [CLIENT-5292] CI: a line-coverage gate for `aerospike-core` merged across the community,
    enterprise AP and enterprise security test legs, and a reusable nightly workflow.
  * CI: pull requests now gate on clippy with warnings denied across every documented feature
    set, rustdoc with warnings denied, a docs.rs-style nightly build, a packaging dry run, each
    feature compiled on its own (`cargo hack`), and a build on current stable beside the MSRV;
    the server legs test the default feature set (`tls`, `dynamic-config`) and `lua` on Tokio,
    `dynamic-config` on async-std, and the blocking client's own suite; one server version
    (8.2.0.0) everywhere; every action pinned to a commit; the legacy Travis/AppVeyor-era
    `build.yml` and `.appveyor.yml` removed. The tag-push release workflow runs the same gate
    before it packages anything.

* **Bug Fixes**
  * A single-key command on a namespace missing from the partition map (or before the map is
    populated) now fails at once with `InvalidNamespace` (20), matching the batch path and the Java
    client. It used to retry the routing failure until the budget ran out and report
    `MaxRetriesExceeded` (-11), with the real cause buried in `source` and `sub_errors`.
  * Background queries (`query_operate`, `query_execute_udf`) treat `KEY_NOT_FOUND`
    (result code 2) as "set absent on this node" and succeed, matching the Java
    client. A 3-node cluster returns that code from nodes that do not hold a
    fresh, empty set.
  * A batch whose `total_timeout` elapsed outside the retry loop's own deadline checks returned a
    bare timeout and **dropped every row of that node group**: the caller's `BatchOperation`s came
    back as placeholders. The whole-command deadline is now a terminal failure like any other — the
    error is a client timeout and in doubt, and every unanswered write row is stamped TIMEOUT and
    in doubt with its key intact.
  * Single-key retry exhaustion now reports `MaxRetriesExceeded` (-11) like the batch path and the Go
    client; it used to report the timeout code 9 (which Java keeps behind its `Timeout` exception type,
    mirrored here by `ErrorKind::Timeout`).
  * [CLIENT-5626] Batch rows the server answered `KeyNotFound` or `FilteredOut` lost the server's extended error
    detail (the server's "filtered out by ..." message): the multi-key parser read it and discarded it.
  * [CLIENT-5624] A batch row served by the single-key path (its key alone on its node) keeps the
    server's error detail (subcode, message) on the row, as the multi-record path does.
  * [CLIENT-5474] A batch row served by the single-key path is stamped on a client-side failure the
    way the multi-key path stamps unanswered rows: `TIMEOUT` for a client timeout, otherwise the
    server code when there is one, and in doubt when the failure is. The same failure used to be
    reported three ways in one batch depending on how the keys hashed across nodes.
  * [CLIENT-5533] A batch delete whose key is alone on its node returns a bin-less `Record` with the
    generation and expiration, as the grouped path and the Java client do; `None` means not found
    or failed on both paths. Rows that carry no key fields no longer echo an all-zero placeholder
    key, and rows without operations report `results: None`.
  * The `batch_operations`, `query` and `timeout_configuration` examples defaulted to port 3100 when
    `AEROSPIKE_HOSTS` is unset; every other example and the test harness use 3000.
  * **Breaking**: `ErrorKind::BatchFailed` and `Error::batch_failed` are removed. They carried a copy
    of the batch rows inside the error, which was needed when `Client::batch` returned results by
    value; since results land in the caller's own operations, a failed batch returns the failure that
    ended it and nothing had produced the variant. `ClientResultCode::BatchFailed` (-16) stays as a
    reserved, never-produced value.
  * Nine client-built server errors passed message text (or nothing) as the *node*, so `Display`
    printed `node=<message>` and `base_message()` lost the text.
  * `ResultCode` descriptions corrected from the server/Java strings: `AlwaysForbidden` ("Operation not
    allowed"), `PartitionUnavailable` ("Partition unavailable"), `FilteredOut` ("Command filtered
    out"), `LostConflict`, and the allowlist codes no longer say "whitelist".
  * Serializing `Value::Infinity`, `Value::Wildcard` or a `Value::MultiResult` is a serde error
    instead of a panic. **Breaking**: `From<Value> for i64` is now `TryFrom<Value>` (and
    `TryFrom<&Value>`) with the crate `Error`, like every other `Value` conversion —
    `let n: i64 = value.into()` becomes `let n = i64::try_from(&value)?`.
  * [CLIENT-5492] `Error::base_message` for a server failure is the result code's descriptive string (`Key already exists`),
    not the variant name. Info-command failures (`FAIL:<code>:<message>`) keep the server's text as the base
    message under the server's code (`Error::info_command_failure`) instead of filing the text as the node and
    wrapping the code in a client error; an out-of-range code no longer panics. `BinNameTooLong` reads
    "greater than 15 characters", the server's actual limit, and `FailForbidden` reads "Operation not
    allowed at this time" (a stray rename had produced "OperationType").
  * `Error::base_message` for a UDF failure (`ErrorKind::UdfBadResponse`) is the UDF's `FAILURE` text, bare,
    as for a server failure; it no longer carries a `UDF Bad Response: ` prefix. `Display` and the serialized
    `message` follow.
  * A query or scan that fails at start-up (a filter expression the server cannot build, say), a
    background query or UDF job that fails, and a failed transaction verify, roll, close,
    mark-roll-forward or add-keys reply all reported a bare result code. The server's extended error
    detail (subcode, message, expression trace) rides in those replies too and is now surfaced on the
    error, as it already was for single-record and batch commands.
  * **Breaking**: `sub_code::FILTERED_META` and `sub_code::FILTERED_BINS` are removed. The server
    defines no subcodes under `FilteredOut` and never sent them; a filtered-out row explains itself in
    its message only.
  * Malformed or truncated server replies are reported as `BadResponse` errors instead of
    panicking the calling task: every length the server declares (field, operation, particle,
    msgpack element and ext sizes, batch row index, compressed and decompressed sizes, the
    partition bitmap) is checked before it is trusted, a hostile element count no longer drives a
    multi-gigabyte allocation, a map key of a type the server never sends is rejected before it
    reaches `Hash`, and a non-numeric version string no longer panics the tend task. The tend loop
    also survives a panic in one cycle and logs it rather than silently freezing the cluster view.
  * A query whose producer task failed could leave `Recordset` consumers waiting forever; the sink
    is now closed from a drop guard however the task ends.
  * `Client::close` cancels the dynamic-config watcher and waits for the tend task, so cleanup is
    complete when it returns. A node's partition map is merged on a scratch copy, so a parse error
    part-way through no longer leaves the shared map half updated.
  * Host names are resolved through the async runtime instead of blocking a worker thread on the
    system resolver during tend and seeding; the YAML config provider reads its file the same way.
  * The retry backoff ignores non-finite multipliers and caps the sleep at 60 s; an empty `exp_let`
    or `def` is an `InvalidArgument` instead of an underflow; more than 65535 operations or bins in
    one request is an `InvalidArgument` instead of a corrupt request.
  * Per-attempt "Parse result error" and node-error log lines are `debug!` (they were `warn!` at
    full request rate during an outage); the YAML provider warns once per distinct problem instead
    of once per poll.
  * Lua `bytes` values grow to at most 128 MiB; every mutator reports failure past that instead
    of exhausting memory.
  * [CLIENT-5386] The retry context's `iteration` counts the attempts actually made: `max_retries = 0`
    reports one try, not two. The counter was bumped before the budget check, so the pass that only
    discovered the budget was spent counted as an attempt, one more than Java reports.
  * [CLIENT-5425] The per-namespace `bytes_received` metric always recorded 0: it read a counter the
    timeout-recovery code resets at every read-phase transition. Received bytes are now accumulated
    separately across a command's read phases.
  * [CLIENT-5457] `Client::commit` reports an abandoned roll-forward as an `ErrorKind::Commit` error
    with `CommitErrorType::RollForwardAbandoned`, so the cause is not dropped; it used to return
    `Ok(CommitStatus::RollForwardAbandoned)`. The `CommitStatus` variant stays so existing matches
    compile.
  * [CLIENT-5529] `Record::time_to_live` floors "now" to whole seconds before subtracting, as the
    Java, Go and C clients do. It reported one second less than them at every instant but the top
    of the second, and two seconds less when a second ticked between write and read.
  * [CLIENT-5581] A batch row whose key is alone on its node now carries the same bins as a row
    grouped with other keys on one node.

* **Breaking Change**
  * **Breaking**, API lockdown before 3.0.0 (see `MIGRATION.md`):
    `Client::cluster` is private; use `Client::nodes`, `node_names`, `get_node`, the new
    `random_node` and `cluster_name`. The `Node`, `NodeMetrics` and `Txn` mutators that drove the
    tend loop and the transaction state machine are crate-private (Rust code sets the timeout
    at construction with `Txn::with_timeout(self, d)`); `PartitionFilter`'s `done` and `retry` fields
    are private, and `partitions` and `PartitionStatus` are hidden from the documentation (see
    *Improvements*). `BatchPolicy::filter_expression` the field is gone: the batch-wide filter
    is `base_policy.filter_expression`, which is what the encoder always read. The server-defined
    enums `ResultCode`, `ClientResultCode`, `ErrorKind`, `PrivilegeCode` and `Value` are
    `#[non_exhaustive]`, so matches on them need a `_` arm; the client-side sets (`AuthMode`,
    `Replica`, `QueryDuration`, `ReadTouchTtl`, the index, UDF-language, command-type, transaction
    and task status enums) stay exhaustive, so adding a variant to one is a breaking change. `EqFilterValue`,
    `RangeFilterValue`, `MapLike` and `Task` are sealed. The wire-level
    query-plan types, `ParticleType` and the AEL packing helpers are hidden from the documentation;
    `CITRUSLEAF_EPOCH` is `citrusleaf_epoch()` and `CITRUSLEAF_EPOCH_UNIX_SECS`. `BatchOperation`
    is an opaque struct: its variants (`Read`, `Write`, `Delete`, `UDF` and the hidden transaction
    rows) can no longer be matched or built by hand; use the constructors and the result accessors.
    `ErrorKind` drops the `Base64`, `PwHash`, `Async` and `BatchRow` variants, and the error
    constructors other than `client_error`, `invalid_argument` and `chain_error`, plus the retry
    bookkeeping (`set_in_doubt`, `with_retry_context`, `wrap`, `chain_cause`, `keep_connection`,
    `is_pool_empty`), are crate-private. `Statement.filters`/`add_filter` are `filter`/`set_filter`
    and `Statement.aggregation` is private. The six `Filter::geo_*_cit` constructors are replaced by
    `Filter::collection_type(cit)`, which chains on any filter. The policies' `filter_expression()`
    getters return `Option<&Expression>`. Every builder that stores a name or string value takes
    `impl Into<String>` (the expression bin builders, `Bin::new`, the operation builders, `Filter`,
    `Statement::new`), so `int_bin("a")` and `Bin::new("a", v)` work; an argument spelled
    `"a".into()` no longer infers. `Key::new` takes its two strings independently and
    `Key::key_with_digest` is the infallible `Key::with_digest`.
    `Client::get` takes `bins: impl Into<Bins>` without the `Send + Sync + 'static` bounds. Lists
    have one shape: stored lists (`BatchOperation::write/read_ops/udf`, `Statement::set_operations`,
    `Operation::context`, `Filter::context`) take `impl Into<Vec<_>>`, borrowed lists (`operate`,
    the UDF `args`, every builder `ctx`) take `&[_]`, and no list is wrapped in `Option`;
    `batch_foreach` takes `&mut [BatchOperation]` like `batch`; the UDF module parameter is
    `package_name` on `execute_udf` and `BatchOperation::udf`. `query_operate` applies the
    statement's own operations (`Statement::set_operations`) instead of a second list;
    `Statement::set_aggregate_function` is hidden from the documentation (see *Improvements*), the
    client's aggregate methods take the UDF; `MapWriteMode` is removed in favour of `MapWriteFlags`
    (`MapPolicy::new(order, flags)`,
    `with_persisted_index`), and the expression `put`/`put_items` now send the policy's flags. The
    deprecated filter macros (`as_eq!` and friends) and the server-deprecated `device_size()` /
    `memory_size()` expressions are removed. `Key`, `Sampler`, `CdtContext` and `LoopVarPart` keep
    their invariants behind private fields and accessors (`key.digest()` and friends).
    `ClientPolicy::set_auth_mode` no longer returns a `Result`, `RecordMapper::id` returns one,
    the `TryFrom<Value>` impls fail with the crate `Error`, and a password bcrypt refuses fails
    `Client::new` instead of panicking on the first connection. `Recordset::partition_filter` and
    `QueryHandle::partition_filter` are plain methods. The blocking client returns
    `aerospike_sync::Task<T>` with blocking waits, takes a plain `bool` closure in `batch_foreach`,
    and gains `query_foreach` with a blocking `aerospike_sync::QueryHandle`. Both clients
    implement `Clone` (a clone shares the cluster) and no longer carry hand-written `unsafe impl
    Send/Sync`: a compile-time assertion checks the property from the fields instead. Derives filled
    in: `Key: Hash`, `PartitionFilter: Clone`, `Copy` on the small policy enums, `PartialEq` on every
    policy, `Record`, `Statement` and `Filter`, `UdfLang: Copy + Eq + Hash`, `Bins` from
    `Vec<String>`; `TlsPolicy.config` is an `Arc<rustls::ClientConfig>`, shared with the
    connector instead of cloned per connection. `Hash for Value` is total (a map with an invalid
    key type errors when encoded instead of panicking when hashed), `Replica` is exported at the
    root, `Record::expiration()` is new, the dynamic-config section types are public and
    documented, and `Value::particle_type` is crate-private. The `Policy` trait is no longer
    exported. Acronyms in identifiers are `UpperCamelCase`: `BatchUdfPolicy`, `UdfLang`, `ReadModeAp`/`ReadModeSc`,
    `ReadTouchTtl`, `HllPolicy`/`HllWriteFlags`, `Value::GeoJson`/`Value::Hll`, `AuthMode::Pki`,
    `PrivilegeCode::{UdfAdmin, SindexAdmin, ReadWriteUdf}`, `ResultCode::XdrKeyBusy`,
    `QueryDuration::LongRelaxAp`; enum variants are too: `ExpType::{Nil, Bool, Int, String, List,
    Map, Blob, Float, Geo, Hll}`. Every flag set is one shape, a newtype with SCREAMING constants
    combined with `|` (`ListWriteFlags::ADD_UNIQUE | ListWriteFlags::NO_FAIL`), `bits()` and an
    unchecked `from_bits()` for flags the server knows before the client does: `ListWriteFlags`,
    `ListSortFlags`, `MapWriteFlags`, `BitwiseWriteFlags`, `BitwiseResizeFlags`, `HllWriteFlags`,
    `ExpWriteFlags`, `ExpReadFlags`, `RegexFlags` (was `RegexFlag`, and `regex_compare` takes it),
    plus the existing string and path flags with a private field. Policy `flags` fields are typed,
    the `To*FlagsBitmask` traits and the flag-combining `new_with_flags` constructors are gone,
    and `bitwise::resize` takes `BitwiseResizeFlags` instead of an `Option`. `ListReturnType` and
    `MapReturnType` are newtypes with selector constants and `.inverted()`; the `Inverted`
    variant, the `Inverted*Return` wrappers and the `To*ReturnTypeBitmask` traits are gone. Policies
    stay plain structs with public fields, built from `Default` by mutation or struct-update
    syntax (see the `policy` module docs).
  * **Breaking**: `replica` moved from `ReadPolicy`, `QueryPolicy` and `BatchPolicy` to
    `BasePolicy`, so write policies carry it too. A write with `Sequence` or `PreferRack` now
    moves to the next replica when retried, as in the Java and Go clients, instead of always
    targeting the master. `BatchPolicy::write_default()` (`max_retries` 0) is the parent policy
    for batches with writes. A write command's client-side failure after the request was sent is
    always in doubt, not only on timeout and connection errors.
  * [CLIENT-5582] A single-key `operate` keeps every op's answer in `Record::bins`, a write's
    `Value::Nil` included, as the batch path already does. A bin written and then read in one call
    holds `Value::MultiResult([Nil, …, value])` in op order instead of the bare value, and a
    write-only operate reports each written bin as `Nil` instead of returning no bins. Read an op's
    answer at its index in `Record::results`, or take the last element of the bin's `MultiResult`.

## [3.0.0-alpha.2]

* **New Features**
  * [CLIENT-4201] Dynamic configuration from YAML, with live reload. On by default.
  * [CLIENT-4851][CLIENT-5032][CLIENT-5164] String operations, including `append` and `prepend`.
  * [CLIENT-5120] `query_aggregate` for Lua stream aggregations (`lua` feature).
  * [CLIENT-5119] Error system reworked for Java parity: `ErrorKind`, `result_code()`, `ClientResultCode`.
  * [CLIENT-5115][CLIENT-5174] Detailed server errors, with subcodes on batch records.
  * [CLIENT-5398] Error detail completed against the server contract: `ExpressionTrace` gains `outcome` and
    `operands` (wire keys 7 and 13), the `OPNOT_STRING_B64_INVALID` subcode, and the server message is
    now kept **verbatim** — the subcode is rendered beside the result code by `Display` instead of being
    folded into the message.
  * [CLIENT-5125] Tiered buffer pool.
  * [CLIENT-5202] Liveness check before a connection leaves the pool.
  * [CLIENT-5114] Connections opened outside the command life cycle.
  * [CLIENT-5079] Concurrent tend.
  * [CLIENT-4751] `ClientPolicy::connect_timeout` and `login_timeout`.
  * [CLIENT-4973] Positional `Record::results` for op-ordered access.
  * [CLIENT-5176] Blob secondary index type.
  * [CLIENT-5340] `lists::join` / `join_by_separator` and their expression forms (CDT list read op 28).
  * [CLIENT-5391] `StringWriteFlags::CREATE_ONLY` and `UPDATE_ONLY`.
  * [CLIENT-5392] `string::snip_from` and its expression form, restored without the flags element.
  * [CLIENT-5349] `bitwise::b64_encode` / `b64_encode_range` and their expression forms (BITS read op 55).
  * [CLIENT-5395] `expressions::from_ael(text)`: AEL source text as a standalone filter expression.
  * [CLIENT-5243] `string::regex_replace` and its expression form now send the write flags (`NO_FAIL`,
    `UPDATE_ONLY`) alongside the regex flags; needs the server-side slot from SERVER-1365.
  * [CLIENT-5128] `Value::Unknown` for uninterpreted particle types.
  * [CLIENT-4878][CLIENT-5011] AEL expression parsing and two-phase index selection.
  * [CLIENT-5228] Error detail on the query plan: the parser message and expression trace at explain,
    under `error_detail_verbosity`.
  * [CLIENT-5242] Configurable metrics resolution; microseconds by default.
  * [CLIENT-5121] `asbench` parity with the Java benchmark app.
  * Object mapping: `RecordMapper` derive, `ToValue`/`FromValue`, serde to `Value`.
  * `Client::info()`, `Expressions::exclusive`, `AuthMode::ExternalInsecure`, `WritePolicy::xdr`.
  * `WritePolicy::records_per_second` throttles background queries.

* **Improvements**
  * [CLIENT-5392] **Breaking:** string expression builders take `src` first, then the operands.
  * [CLIENT-5393] Document that the `is_numeric` FLOAT filter needs a fractional digit, and cover it.
  * [CLIENT-5394] Document canonical-equivalence matching on `starts_with`/`ends_with`; reference tests
    for canonical `find`/`contains`/`replace` and the modify result-size cap.
  * [CLIENT-4990] One reusable timer per connection, removing timer-wheel contention.
  * [CLIENT-5129][CLIENT-5131] Batch REPEAT for write/UDF rows, and compression parity.
  * [CLIENT-5130] Rack-aware routing improvements, plus `client_version()`.
  * [CLIENT-5126] `Record::bins` keeps the server's return order.
  * [CLIENT-4624][CLIENT-2185][CLIENT-2089] Ordered and sorted map variants.
  * [CLIENT-5118] Admin commands pace on an empty connection pool instead of failing.
  * `Client::new` returns on cluster convergence; `close()` stops tend at once.
  * [CLIENT-5081] `ClientPolicy` and `MetricsPolicy` defaults aligned with the Java client, with
    four deliberate exceptions: `max_conns_per_node` is 256 (Java 100); `idle_timeout` 0 disables
    idle reaping (Java trims to `min_conns_per_node` after 55 s); `AdminPolicy.timeout` 0 falls back
    to 3 s (Java: no timeout); `QueryPolicy.record_queue_size` is 1024 (Java 5000, Go 50).
  * [CLIENT-5265] `Concurrency::Sequential` no longer claims to be the default, which it is not.
  * Fewer per-query allocations in scan and query partition tracking.
  * TLS is required for External and PKI auth modes.
  * Wider field visibility, so other crates can build on the core.
  * [CLIENT-4979] CI workflows, [CLIENT-3858] more examples, `AEROSPIKE_CLEANUP` for tests.

* **Bug Fixes**
  * [CLIENT-4966] Connection churn with `min_conn_per_node` > 0.
  * [CLIENT-4989] Socket I/O errors are Connection errors, so commands retry.
  * [CLIENT-5268] TLS writes are flushed, not left in the session buffer.
  * [CLIENT-5033] Sync client hangs.
  * [CLIENT-5132][CLIENT-5172] Batch retries, and one bad namespace failing a whole batch.
  * [CLIENT-5251][CLIENT-4884] In-doubt on batch terminal errors and on client Timeout/Connection errors.
  * [CLIENT-5266] Wrong batch index field size when a filter is set.
  * [CLIENT-5329] Nest the inner op in the string CTX wire shape.
  * [CLIENT-4881] Per-record result codes on `BatchRecord` instead of failing the batch.
  * [CLIENT-4865] Duplicate query bin projections returned a list.
  * [CLIENT-5175] Every `operate` op gets its own result slot.
  * [CLIENT-5173] `PARAMETER_ERROR` for INF and wildcard values instead of aborting.
  * [CLIENT-5195] `ClientPolicy.rack_ids: Some(vec![])` enabled rack awareness with no rack to prefer, so
    every `Replica::PreferRack` read failed node selection and was reported as a client timeout. Rejected at
    validation now, and an empty list reaching node selection degrades to "not configured".
  * [CLIENT-5147] Wait for a populated partition map before declaring the cluster stable.
  * [CLIENT-5059] `tls_name` parsing.
  * [CLIENT-5005] Bogus final metrics report at shutdown.
  * MessagePack `str8` encoding, and other wire divergences from the Java client.
  * `sleep_multiplier` was ignored by scan and query.
  * Role allowlists in admin commands.

## [3.0.0-alpha.1]

* **New Features**
  * [CLIENT-3779] Distributed ACID transactions (multi-record transactions): `Txn`, `commit`, `abort`,
    and the per-command `txn` policy field.
  * [CLIENT-3780] Strong Consistency mode support: `ReadModeSC`, `SCMode`, and linearizable reads.
  * [CLIENT-3815] CDT path expressions: `select_by_path` / `modify_by_path`, expression-filtered
    contexts, and the loop-variable expressions they need.
  * [CLIENT-4857] Allow setting `custom-client-id` in `the user-agent-id`.
  * [CLIENT-4858] Expose `SCMode`.
  * [CLIENT-4821] Support `batch_stream` API.
  * [CLIENT-3609] Support `seed_only` rust client configuration for testing.
  * [CLIENT-2403] Convert batch calls with just a one key per node in sub-batches to equivalent single requests.
  * [CLIENT-3999] Ops Projection.
  * [CLIENT-4437] Implement the enhanced expression API of server 8.1.2.
  * [CLIENT-3621] Support `compression_threshold`.
  * [CLIENT-2127][CLIENT-2388] Add a circuit breaker (`max_error_rate`, `error_rate_window`)
  * [CLIENT-4716] Use a dedicated connection for tend.

* **Improvements**
  * Ported missing tests from other clients
  * Consolidate the query command wire protocol with scan in one buffer encoder.

## [2.1.0]

* **Bug Fixes**
  * [CLIENT-4711] `close()` does not stop the `tend_thread`.
  * [CLIENT-4685] Reject `operate` calls with empty ops list.
  * [CLIENT-4686] Fix unexpected behavior for partition-based query with `QueryDuration::Short`.
  * [CLIENT-4405] Execute query failing during node churn (#195)
    * Check node active status before selecting node for partition.
    * State to remember last tried node for a partition retry.
    * added drop trait for node, to close node eventually and removed all weak ref to Arc node for last tried node
    * Check for node active status before returning a connection. Drain the conn pool on Node drop.
    * Removed deprecated `try_next`.
    * Change default policy for `max-retries` to `0` for writes, honoring `max-retries=0` as no retries.
    * Change policy to sequence for write/delete commands.

* **Improvements**
  * Update all dependencies to the latest, and adapt the code to the deprecation and removals.
  * Adds a cleanup test that is ignored by default.
    Can be manually invoked to remove indexes and then truncate of the tested namespace

## [2.0.0]

* **Bug Fixes**
  * [CLIENT-4530] `lists::get_by_value_range` and `lists::remove_by_value_range` return empty results when end is `Value::Nil`.

## [2.0.0-alpha.11]

* **New Features**
  * [CLIENT-4413] Support background Execute UDF.
  * [CLIENT-4412] Support background query operations.
  * [Client-4113] Rust performance testing `asbench`.
  * [CLIENT-4342] `MapPolicy` missing `MapWriteFlags` support.
  * [CLIENT-2023] Add `to_base64` encoding methods to `operations::cdt_context`
  * [CLIENT-2128][CLIENT-3956] Add missing APIs for importing/exporting compiled expressions.
  * Adds new filters to the `Filter`, deprecates the old macros for filter instantiation.
  * Add a few missing map and list operations:
    `cdt_list_create_with_index`,
    `cdt_list_set_order_with_index`,
    `cdt_list_set_with_policy`,
    `cdt_list_increment_by_one`,
    `cdt_list_increment_by_one_with_policy`,
    `map_create_op`,
    `map_create_with_index_op`,
    `map_set_policy_op`,
    `set_policy`

* **Improvements**
  * Chain all errors in `command.execute`
  * [CLIENT-3815] Avoid preallocations, remove Result in path constructors.
  * [CLIENT-4156] Fix rust-doc examples at remaining places in client, errors, and expressions.
  * [CLIENT-4102] Update readme.
  * [CLIENT-4023] Adds tests for `exp_remove_results()`.
  * [CLIENT-4222] Update `map_remove_by_*` expression functions to accept a caller-specified `MapReturnType`.
  * [CLIENT-4222] Update `list_remove_by*` calls to handle `ListReturnType` params.
  * Add rust docs for enums.
  * Update rust docs for client APIs.
  * Updated the `IndexTask` with the latest logic.
  * Address linter issues.

* **Bug Fixes**
  * [CLIENT-4227] `expressions::geo_val()` creates `Value::String` instead of `Value::GeoJSON`
  * [CLIENT-4411] Fix sindex Query with Bin selection.

## [2.0.0-alpha.10]

* **New Features**
  * Support recovering connections in batch command errors.
  * Added `bool_bin()` function returning `ExpType::BOOL` expression. (#179).

* **Improvements**
  * [CLIENT-4200] Performance fix (#185). Replaces `RwLock` with `ArcLock`.

* **Bug Fixes**
  * [CLIENT-4177] Query during migration hangs for full `socket_timeout` after scale-down cluster.
  * Allow truncating the whole namespace.
  * Fix an issue where `max_retries` were not respected in Scan/Queries.

## [2.0.0-alpha.9]

* **Bug Fixes**
  * [CLIENT-4140] SIGSEGV/Panic with parallel batch operations and short timeouts
  * [CLIENT-4131] Dix an issue where `Client.create_pki_user` hashes the predefined password twice.

* **Breaking Change**
  * [CLIENT-4148] Convert `BasePolicy.sleep_between_retries` and `ClientPolicy.tend_interval` to u32. Also sync default policy values with other clients.

* **Improvements**
  * Turn some panics into errors.

## [2.0.0-alpha.8]

* **New Features**
  * [CLIENT-4050] Support Privilege / Permission Code Expansion Due to DataMasking Feature.

* **Bug Fixes**
  * [CLIENT-4099] Enforce `policy.total_timeout` on all commands.
  * Remove `PrivilegeCode` related panics from the codebase.
  * Fix an issue where batch commands were not retried.
  * Handle the UDF error cases in batch commands.

## [2.0.0-alpha.7]

* **New Features**
  * [CLIENT-2088][CLIENT-2089][CLIENT-2175][CLIENT-2390] Support Ordered maps.
  * [CLIENT-3963] Support `ClientPolicy.timeout_delay` to allow recovering timed out connections.
  * [CLIENT-3948] Support `ClientPolicy.min_conns_per_node`.
  * [CLIENT-3946] Add support for user agent-id. Supported by server `v8.1+`.
  * [CLIENT-3945] Add `UdfRemove` and `DropIndex` tasks to the relevant API.
  * [CLIENT-3130] Support new server 7.1 info command error response strings. Server 7.1 now returns error strings with "ERROR" instead of "FAIL".
  * [CLIENT-2151] Support `set_xdr_filter`.
  * [CLIENT-3597] Support `socket_timeout` on all policies.
  * [CLIENT-3580] Support creating a PKI user without a password.
  * [CLIENT-3593] Support secondary index on an expression.
  * [CLIENT-3781][CLIENT-3851] Add full TLS support + property testing.
  * [CLIENT-3832] Add support for Async Streams.
  * Add `MapLike` trait to support passing both `HashMap` and `BTreeMap` to some functions.
  * Adds new privileges from server `v8.1.1`.

* **Improvements**
  * [CLIENT-3627] Deprecation warning changes.
  * [CLIENT-3849] Improve connection churn issue.
  * Make all `PartitionStatus` and `PartitionFilter` fields public.
  * Fix logging in tests.
  * Fixed and updated documentation.
  * Support peers protocol and fix minor bug in TLS.
  * Remove `Iterator` and `next_record` for Recordset in the async build.
  * Brings v2 branch up to rustc v1.90.x language expectations.
  * Close the connection in Multi-part commands (batch, scan, query) on error.
  * Added "examples".

* **Bug Fixes**
  * [CLIENT-4015] Allow empty set names in Scan/Queries.
  * [CLIENT-4007] Fix create_role field calc & correct privilege serializations.
  * [CLIENT-3892] Geo queries w/ filters are broken.
  * [CLIENT-3795] Dropping tokio tasks returns stale data from other commands.
  * Fix map operations due to MultiResult changes.
  * Fix an issue where only the last operation results were returned in multi operation commands.
  * Fix reading the AEROSPIKE_USE_SERVICES_ALTERNATE in tests.
  * Fix feature selection issue.
  * Fix an issue with Query encoding.
  * Fix Batch encoding issue.
  * Log nodes after tend, change info command results at trace level to prevent noise in debug level.
  * Fixes an issue with clustering and a faulty test case.
  * Fix `NOSUB` `RegexFlag` enum value.

* **Breaking Change**
  * [CLIENT-4068] Remove the Scan API due to deprecation.
  * Remove `Value::Uint` due to lack of native support on the server.
  * Move hashed password out of the client policy.
  * Fix an issue where signed integers were unpacked as unsigned.
  * Rename `FilterExpression` to `Expression`.

## [2.0.0-alpha.6]

* **Bug Fixes**
  * Fixes an issue where the client could not connect to single node clusters.

## [2.0.0-alpha.5]

* **Bug Fixes**
  * [CLIENT-3776] Fix an issue where load balancers are not supported.
  * Increase `MAX_BUFFER_SIZE` to 120MiB.

## [2.0.0-alpha.4]

* **New Features**
  * [CLIENT-2446] Only string, integer, bytes map-key types.
  * [CLIENT-3559] Missing API to initialize Key from namespace, digest, optional set name and optional user key.
  * [CLIENT-2408] Support partition queries.
  * [CLIENT-2407] Support `QueryPolicy.max_records` in queries.
  * [CLIENT-2401] Support partition scans.
  * [CLIENT-2399] Support `ScanPolicy.max_records` in scans.
  * [CLIENT-2105] Support scan/query pagination with `PartitionFilter`.
  * [CLIENT-2396] Remove legacy client code for old servers.
  * [CLIENT-2101] Remove `Policy.priority`, `ScanPolicy.scan_percent` and `ScanPolicy.fail_on_cluster_change`.

## [2.0.0-alpha.3]

* **New Features**
  * [CLIENT-3105] Add newer error codes to the client.
  * [CLIENT-2052] Support new 6.0 `truncate`, `udf-admin`, and `sindex-admin` privileges.
  * [CLIENT-2100] Support user quotas and statistics and newer API.

## [2.0.0-alpha.2]

* **New Features**
  * [CLIENT-2046] Add `Exists`, `OrderedMap` and `UnorderedMap` return types for CDT read operations.
  * [CLIENT-2385] Add support for `Infinity` and `Wildcard` values.
  * [CLIENT-2309] Add support for `expressions::infinity()` and `expressions::wildcard()`.
  * [CLIENT-2576] Support `expressions::record_size()` and `expressions::memory_size()`.
  * [CLIENT-3491] Add `allow_inline_ssd`, `respond_all_keys` to `BatchPolicy`.
  * [CLIENT-2832] Add `read_touch_ttl` to policies.
  * [CLIENT-2825] Support `QueryDuration` enum in `QueryPolicy`.
  * [CLIENT-3488] Support `records_per_second` for Scan/Query.

* **Bug Fixes**
  * Fix build issue on crates.io

## [2.0.0-alpha.1]
We are pleased to release the first alpha version of the next gen v2 for the Rust client.
This version of the client comes with a major feature: `async`! This feature was started by [Jonas Breuer](https://github.com/jonas32), in his epic PR and fixed and extended by Aerospike. We would like to thank him for his amazing contribution. Others also opened PRs which we have accepted and merged into this release.

Please keep in mind that the API is still unstable and we *WILL* break it to enhance ergonomics, feature-set and the performance of the library. We invite the community to test drive the library and file tickets for bug reports or enhancement either on `Github` or with Aerospike support.

* **New Features**
  * Support `async` rust. You can use both `tokio` and `async-std` as features to enable the respective runtimes. `tokio` is the default.
  * Support `sync` through blocking in the `sync` sub-crate.
  * [CLIENT-2051] Support new batch protocol, allowing `read`, `write`, `delete` and `udf` operations. Use `BatchOperation` constructors.
  * [CLIENT-2321] Support queries and scans not sending a fresh message header per partition in server v6+.
  * [CLIENT-2320] Implement `std::convert::TryFrom<aerospike::Value>` for each variant.
  * [CLIENT-2099] Support `boolean` particle type.
  * Support New Scan/Query wire protocol.
  * Replace `error-chain` with a custom implementation. We still use `thiserror`'s macros internally (To be removed in the future.)
  * Support for `Replica` policies, including `PreferRack` policy.
  * Removes lifetimes that were due to `&str`, replacing most of them with `String`.

* **Bug Fixes**
  * Fixed various bugs in `messagepack` encoding.
  * Fixed large integers packing when encoding to `messagepack`.
  * Fixed `Float` serialization.

## [1.2.0] - 2021-10-22

* **New Features**
  * Support Aerospike server v5.6+ expressions in Operate API. Thanks to [Jonas Breuer](https://github.com/jonas32)

* **Bug Fixes**
  * Fix for buffer size when using CDT contexts. Thanks to [Jonas Breuer](https://github.com/jonas32)

## [1.1.0] - 2021-10-12
This version of the client drops support for the older server versions without changing the API. `ScanPolicy.fail_on_cluster_change`, `ScanPolicy.scan_percent` and `BasePolicy.priority` are deprecated for the Scan operations and will not be sent to the server. They remain in the API to avoid breaking the API.

* **New Features**
  * Support Aerospike server v5.6+ server authentication.
  * Support Aerospike server v5.6+ Scan protocol for simple cases.

## [1.0.0] - 2020-10-29

* **Bug Fixes**
  * Client.is_connected() returns true even after client.close() is called. [(#87)](https://github.com/aerospike/aerospike-client-rust/pull/87)

* **New Features**
  * BREAKING CHANGE: Replace predicate expressions with new Aerospike Expression filters. Aerospike Expression filters give access to the full data type APIs (List, Map, Bit, HyperLogLog, Geospatial) and expanded metadata based filtering, to increase the power of filters in selecting records. This feature requires server version 5.2.0.4 or later. See [API Changes](https://www.aerospike.com/docs/client/rust/usage/incompatible.html#version-1-0-0) for details. [(#80)](https://github.com/aerospike/aerospike-client-rust/issues/80) Thanks to [@jonas32](https://github.com/jonas32)!
  * Support operations for the HyperLogLog (HLL) data type. [(#89)](https://github.com/aerospike/aerospike-client-rust/issues/89) Thanks to [@jonas32](https://github.com/jonas32)!
  * Serde Serializers for Record and Value objects. [(#85)](https://github.com/aerospike/aerospike-client-rust/pull/85) Thanks to [@jonas32](https://github.com/jonas32)!

## [0.6.0] - 2020-09-11

* **Bug Fixes**
  * Shrink connection buffers to avoid unbounded memory allocation. [(#83)](https://github.com/aerospike/aerospike-client-rust/pull/83) Thanks to [@soro](https://github.com/soro)!

* **New Features**

  * Big update for operations: [(#79)](https://github.com/aerospike/aerospike-client-rust/pull/79) Thanks to [@jonas32](https://github.com/jonas32)!
    * Added operation contexts for nested operations.
    * Added missing list operations, list policies, and ordered lists.
    * Added missing map operations.
    * Added bitwise operations.
    * BREAKING CHANGE: The policy and return types for Lists require additional parameters for the cdt op functions.

* **Updates**
  * Restrict Travis CI tests to stable/beta/nightly. [(#84)](https://github.com/aerospike/aerospike-client-rust/pull/84)

## [0.5.0] - 2020-07-30

* **Bug Fixes**
  * Clear connection buffer on server error. [(#76)](https://github.com/aerospike/aerospike-client-rust/pull/76)

* **New Features**
  * Accept batch read response without key digest. [(#67)](https://github.com/aerospike/aerospike-client-rust/pull/67) Thanks to [@jlr52](https://github.com/jlr52)!
  * Add new Task interface to wait for long-running index & UDF tasks to complete. [(#69)](https://github.com/aerospike/aerospike-client-rust/pull/69) Thanks to [@jlr52](https://github.com/jlr52)!
  * Support for Predicate Filters for Queries. Requires server version v3.12 or later. [(#71)](https://github.com/aerospike/aerospike-client-rust/pull/71) Thanks to [@jonas32](https://github.com/jonas32)!

* **Updates**
  * Move to rust edition 2018. [(#65)](https://github.com/aerospike/aerospike-client-rust/pull/65) Thanks to [@nassor](https://github.com/nassor)!
  * Min. required Rust version is now v1.38.

## [0.4.0] - 2019-12-03

* **Bug Fixes**
  * CDT lists/maps size operation fails with ParameterError. [#57](https://github.com/aerospike/aerospike-client-rust/issues/57)

* **Updates**
  * Update all dependencies and remove multi-versions. [#55](https://github.com/aerospike/aerospike-client-rust/pull/55) Thanks to [@dnaka91](https://github.com/dnaka91)!
  * Fix warnings and errors [#61](https://github.com/aerospike/aerospike-client-rust/pull/61) Thanks to [@dnaka91](https://github.com/dnaka91)!
  * Client benchmark now measures latencies in whole microseconds rather than fractional milliseconds. [#62](https://github.com/aerospike/aerospike-client-rust/pull/62)
  * Min. required Rust version is now v1.34.

## [0.3.0] - 2018-09-11

* **New Features**
  * Use generics to make Client#put API more flexible. [#47](https://github.com/aerospike/aerospike-client-rust/issues/47) [#49](https://github.com/aerospike/aerospike-client-rust/pull/49)

* **Bug Fixes**
  * GeoJSON bins are returned as Value::String instead of Value::GeoJSON. [#48](https://github.com/aerospike/aerospike-client-rust/issues/48)
  * Fix client panic when reading ordered list/map from server. [#51](https://github.com/aerospike/aerospike-client-rust/issues/51)

* **Updates**
  * Min. required Rust version is now v1.26.
  * Update several package dependencies to latest version.
  * Update to rustfmt-preview and re-apply cargo fmt.

## [0.2.1] - 2018-01-16

* **Bug Fixes**
  * Secondary index queries fail with parameter error on Aerospike Server 3.15.1.x #44

## [0.2.0] - 2017-10-12

* **New Features**
  * Support configurable scan socket timeout #40
  * Support returning keys/digests without bins in query #39
  * Add list increment operation #38
  * Implement truncate command #37

* **Bug Fixes**
  * Make value::FloatValue public #36 - Thanks to tpukep!

* **Updates**
  * Replace rustc_serialize::base64 with base64 crate #42
  * Switch to bencher crate for benchmarks #41

## [0.1.0] - 2017-04-04

* **New Features**
  * Support batch read requests (#7)
  * Support durable delete write policy (#14)
  * Support cluster name verification (#11)
  * [Performance] (Optionally) split connection pool into multiple smaller pools to reduce lock contention on machines with high core counts (#19)
  * Add benchmark suite (#16)

* **Bug Fixes**
  * Add missing ElementNotFound and ElementExists result codes

* **Updates**
  * Combine client's get and get_header command into updated get command
  * as_geo! now accepts both String and &str
  * Use rustfmt to enforce consistent code formatting
  * [Performance] Replace std::sync::{Mutex, RwLock} primitives with equivalent constructs from parking_lot crate
  * Replace threadpool with scoped-pool library to support both scoped and unscoped task execution
## [0.0.1] - 2017-02-08

Initial release
