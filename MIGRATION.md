# Migration guide

This file describes the changes an application has to make when moving
between major versions of the `aerospike` crate. New features that need no
change are listed in [CHANGELOG.md](CHANGELOG.md).

## 2.x to 3.0

### Requirements

- Rust 1.87 or later, unchanged.
- Aerospike Database 6.4 or later for the client as a whole. Individual
  features have their own minimum server version, stated in their
  documentation (`Version::supports_*` reports what a connected cluster
  offers).

### Cargo features

| Feature | 2.x | 3.0 |
|---|---|---|
| `default` | `async`, `serialization`, `rt-tokio`, `tls` | adds `dynamic-config` |
| `dynamic-config` | not present | runtime configuration from a YAML file (`Client::new_with_config`); on by default |
| `lua` | not present | client-side Lua for `query_aggregate` and stream UDFs; off by default, compiles a vendored Lua 5.4 |
| `sync` | Tokio or async-std | unchanged: the blocking client follows the `rt-tokio` / `rt-async-std` feature enabled at the root |

`rt-tokio` and `rt-async-std` remain mutually exclusive. A crate that already
uses `default-features = false` keeps building without changes; add
`dynamic-config` or `lua` only if you use them.

### Errors

`Error` is no longer an enum. It is an opaque struct; the variant moved to
`ErrorKind`, reached through `Error::kind()`, and the data that used to sit in
tuple payloads is read through accessors.

| 2.x | 3.0 |
|---|---|
| `Error::ServerError(rc, in_doubt, msg)` | `ErrorKind::Server { rc, detail }`; `err.in_doubt()`, `err.base_message()`, `err.server_error_detail()` |
| `Error::BatchError(idx, rc, in_doubt, msg)`, `Error::BatchLastError(..)` | `ErrorKind::BatchRow { index, rc, .. }` for a row-level failure; per-row outcomes live on the `BatchOperation` (see Batch) |
| `Error::Timeout(msg)` | `ErrorKind::Timeout`; `err.is_client_timeout()`, `err.in_doubt()` |
| `Error::Connection(msg)`, `Error::ClientError(msg)`, `Error::InvalidArgument(msg)`, ... | the same names as unit variants of `ErrorKind`; the text is `err.base_message()` |
| `Error::Chain(outer, inner)` | the chain is a cause list: `err.cause()` or `std::error::Error::source` |
| `Error::StreamTerminatedError()` | `ErrorKind::StreamTerminated` |
| `Error::ParsePeersError(msg)` | `ErrorKind::ParsePeers` |

Code matching no longer needs a pattern on the variant:

```rust
// 2.x
match client.get(&policy, &key, Bins::All).await {
    Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => None,
    Err(e) => return Err(e),
    Ok(r) => Some(r),
}

// 3.0
match client.get(&policy, &key, Bins::All).await {
    Err(e) if e.matches(&[ResultCode::KeyNotFoundError]) => None,
    Err(e) => return Err(e),
    Ok(r) => Some(r),
}
```

- `err.server_result_code()` returns the first server code in the chain,
  `err.matches(&[..])` searches the whole chain.
- `err.result_code()` returns an `i32` in the Java numbering: server codes as
  they are, client-side conditions as negative `ClientResultCode` values.
- Retry exhaustion on a single-record command is reported as
  `ClientResultCode::MaxRetriesExceeded` (-11) instead of the server timeout
  code 9. Its kind is still `Timeout`.
- `Error` is `Clone`, and serializes as a structured record under
  `serialization`.
- The server's extended error detail (subcode, message, expression trace) is
  available through `err.sub_code()`, `err.server_message()` and
  `err.server_error_detail()` when `BasePolicy::error_detail_verbosity` is set.
  `ErrorKind`, `ClientResultCode`, `ServerErrorDetail` and `ExpressionTrace`
  are exported at the crate root.
- `ErrorKind` no longer wraps third-party error types. `Base64`, `PwHash` and
  `Async` are gone; those failures arrive as `BadResponse` and `Client` with the
  cause in the message. The internal `BatchRow` variant is gone too. The
  std-type variants (`Io`, `InvalidUtf8`, `ParseAddr`, `ParseInt`) remain.
- User code constructs errors with `Error::client_error`,
  `Error::invalid_argument` and `Error::chain_error`. The kind-specific
  constructors and the retry bookkeeping (`set_in_doubt`, `with_retry_context`,
  `wrap`, `chain_cause`, `keep_connection`, `is_pool_empty`) are crate-private.
  The accessors are unchanged.

### Batch

`Client::batch` writes each row's outcome into the caller's operations
instead of returning a new vector.

| 2.x | 3.0 |
|---|---|
| `batch(&self, &BatchPolicy, &[BatchOperation]) -> Result<Vec<BatchRecord>>` | `batch(&self, &BatchPolicy, &mut [BatchOperation]) -> Result<()>` |
| one `BatchPolicy::default()` for every batch | `BatchPolicy::default()` for reads, `BatchPolicy::write_default()` (`max_retries` 0) for batches with writes, deletes or UDF calls |
| `BatchOperation::batch_record(&self) -> BatchRecord` | `batch_record(&self) -> &BatchRecord`, plus `record()`, `take_record()`, `result_code()`, `in_doubt()`, `error()`, `node()` on the operation itself |
| `BatchRecord { key, record, result_code, in_doubt }`, all public fields | `key` and `record` stay fields; `result_code()`, `in_doubt()`, `node()`, `error()`, `error_detail()`, `sub_code()`, `server_message()` are methods |
| `match op { BatchOperation::Read { br, .. } => .. }` on the (hidden) enum variants | `BatchOperation` is a struct; the kind of operation is not inspectable after construction. Read the outcome through `batch_record()`, `record()`, `result_code()`, and keep your own index if you need to know which row was a read, write, delete or UDF |

```rust
// 2.x
let ops = vec![BatchOperation::read(&brp, key, Bins::All)];
let rows = client.batch(&bp, &ops).await?;
if rows[0].result_code == Some(ResultCode::Ok) { /* rows[0].record */ }

// 3.0
let mut ops = vec![BatchOperation::read(&brp, key, Bins::All)];
client.batch(&bp, &mut ops).await?;
if let Some(record) = ops[0].record() { /* .. */ }
```

A row the server answered `KeyNotFound` or `FilteredOut` is a failed row
(`result_code()` reports the code, `error()` is set, `record()` is `None`),
and the batch call itself still succeeds. An `Err` from `batch` is the first
whole-node failure; every row that was not answered carries that failure.
`batch_foreach` delivers rows to a callback as they arrive.

The sync client's `batch` has the same new signature.

### Client internals that were reachable

- `Client::cluster` is private. Use `Client::nodes()`, `node_names()`,
  `get_node(name)`, `random_node()` and `cluster_name()`; the cluster's own
  methods (`add_seeds`, `update_partitions`, `close`, ...) are not available.
- The `Node` methods that drive the tend loop (`refresh`, `update_partitions`,
  `get_connection`, `close`, ...) and the `Txn` state mutators (`set_state`,
  `on_write`, `clear`, ...) are crate-private. The getters stay.
- `PartitionFilter`'s `partitions`, `done` and `retry` fields are private;
  use `done()` and the constructors.
- `BatchPolicy.filter_expression` is gone. Set `base_policy.filter_expression`,
  as on every other policy; that is the field the batch encoder reads.

### Enums are non-exhaustive

`ResultCode`, `ClientResultCode`, `Value`, `AuthMode`, `Replica`, `IndexType`,
`CollectionIndexType`, `PrivilegeCode`, `CommandType`, `CommitStatus`,
`AbortStatus`, `TxnState`, `QueryDuration`, `ReadTouchTtl`, `UdfLang` and
`task::Status` carry `#[non_exhaustive]`. An exhaustive `match` on one of them
needs a `_` arm.

### Policies

`ClientPolicy`:

| 2.x | 3.0 |
|---|---|
| `tls_config: Option<rustls::ClientConfig>` | `tls_policy: Option<TlsPolicy>`; `TlsPolicy::new(config)` or `config.into()` |
| `rack_ids: Option<HashSet<usize>>` | `rack_ids: Option<Vec<usize>>`, in order of preference |
| `timeout` default 30 000 ms | default 1 000 ms; also the fallback for the new `connect_timeout` (default 0) |
| `idle_timeout` default 30 000 ms | default 0, which disables the idle check |
| — | `login_timeout` (default 5 000 ms), `connect_timeout`, `config_interval`, `seed_only_cluster`, `custom_client_id`, buffer-pool settings |

`BasePolicy`:

| 2.x | 3.0 |
|---|---|
| `consistency_level: ConsistencyLevel` (`ConsistencyOne`, `ConsistencyAll`) | `read_mode_ap: ReadModeAp` (`One`, `All`) for AP namespaces, plus `read_mode_sc: ReadModeSc` (`Session`, `Linearize`, `AllowReplica`) for strong-consistency namespaces |
| — | `txn`, `use_compression`, `compression_threshold`, `sleep_multiplier`, `error_detail_verbosity`, `populate_positional_results` |
| `replica` on `ReadPolicy`, `QueryPolicy`, `BatchPolicy` | `base_policy.replica` on every policy, writes included; a write with `Sequence` or `PreferRack` fails over to the next replica on retry |

`ConsistencyLevel` is removed from the crate root; `ReadModeAp`, `ReadModeSc`,
`TlsPolicy`, `TxnVerifyPolicy` and `TxnRollPolicy` are exported there. The
`Policy` trait is not: read the fields on `BasePolicy` instead of calling its
getters.

Policies remain plain structs with public fields and no `#[non_exhaustive]`.
Build them from `Default`, by mutation or with struct-update syntax
(`WritePolicy { expiration: Expiration::Seconds(60), ..WritePolicy::default() }`).
Fields are added in minor releases, so a literal that names every field is not
a supported way to build a policy.

### Queries

| 2.x | 3.0 |
|---|---|
| `Statement.filters: Option<Vec<Filter>>`, `set_filter(f)` | `Statement.filter: Option<Filter>`, `set_filter(f)`; the server accepts one filter per query, which is all the old list ever allowed |
| `Statement.aggregation` public field | private; `set_aggregate_function` is unchanged |
| `filter_expression()` on `BasePolicy`, `WritePolicy`, `QueryPolicy`, `BatchPolicy` returned `&Option<Expression>` | returns `Option<&Expression>` |
| `int_bin("a".to_string())`, `string_val(s.to_string())`, `Bin::new("a".to_string(), v)` | the name parameters are `impl Into<String>`: `int_bin("a")`, `Bin::new("a", v)`; the old spelling still compiles, but `int_bin("a".into())` no longer infers and becomes `int_bin("a")` |
| operation builders, `Filter` constructors and `Statement::new` took `&str` | `impl Into<String>`; a `String` can now be passed without borrowing |
| `exp_int_loop_var(part)`, `exp_map_loop_var(part)`, … | `int_loop_var(part)`, `map_loop_var(part)`, … |
| `Key::new<S>(ns: S, set: S, key)`, one string type for both | `Key::new(ns: impl Into<String>, set: impl Into<String>, key)`; still returns `Result` because unsupported user-key types are rejected |
| `Key::key_with_digest::<S>(ns: String, set: Option<String>, key: Option<Value>, digest) -> Result<Key>` | `Key::with_digest(ns, set, key, digest) -> Key`; an empty set name means no set |
| `Client::get<T: Into<Bins> + Send + Sync + 'static>` | `bins: impl Into<Bins>`; a borrowed slice of names no longer needs `Bins::from(..)` first |
| `execute_udf(.., server_path, function_name, args: Option<&[Value]>)`, `query_aggregate(.., function_args: Option<&[Value]>)`, `query_execute_udf(.., args: Option<&[Value]>)`, `Statement::set_aggregate_function(.., Option<&[Value]>)` | `args: &[Value]`; pass `&[]` for no arguments. The single-record parameter is `package_name` |
| `BatchOperation::udf(policy, key, udf_name, function_name, args: Option<Vec<Value>>)` | `BatchOperation::udf(policy, key, package_name, function_name, args: impl Into<Vec<Value>>)`; a `Vec`, an array or `[]` |
| `BatchOperation::write(.., ops: Vec<Operation>)`, `read_ops`, `Statement::set_operations(Vec<..>)`, `Operation::context(Vec<CdtContext>)`, `Filter::context(Vec<..>)` | `impl Into<Vec<_>>`: a `Vec`, an array or a slice of clonable items |
| `Client::batch_foreach(policy, ops: Vec<BatchOperation>, hook)` | `ops: &mut [BatchOperation]`, like `batch`; the rows carry their results after the call as well as being handed to the hook |
| path and expression builders took `ctx: impl AsRef<[CdtContext]>` | `ctx: &[CdtContext]`, like every other builder; `&path` still works because `Path` derefs to the slice |
| `query_operate(policy, statement, ops: &[Operation])` | `query_operate(policy, statement)` after `statement.set_operations(ops)`; a statement without operations is `ParameterError` |
| `Statement::set_aggregate_function(..)` | crate-private; pass the package, function and arguments to `query_aggregate` / `query_execute_udf` |
| `key.namespace`, `key.set_name`, `key.user_key`, `key.digest` (public fields) | `key.namespace()`, `key.set_name()`, `key.user_key()` (`Option<&Value>`), `key.digest()` (`[u8; 20]`); keys are built only through `Key::new` and `Key::with_digest` |
| `Sampler { range, threshold }` public fields | `range()` / `threshold()`; build with `Sampler::new`, `all`, `never`, `probability` |
| `CdtContext { id, flags, value }` public fields, `LoopVarPart(pub i64)` | private; use the `ctx_*` builders, and `LoopVarPart::{MAP_KEY, VALUE, INDEX}` or `from_bits` |
| `recordset.partition_filter().await`, `handle.partition_filter().await` | plain methods, no `.await` |
| `ClientPolicy::set_auth_mode(..) -> Result<()>` | returns nothing; it cannot fail. A password bcrypt refuses is reported by `Client::new` |
| `RecordMapper::id(&self) -> Value` (the derive panicked on an unconvertible key) | `-> Result<Value>` |
| `i64::try_from(value)` and the other `TryFrom<Value>` impls erred with a `String` | they err with the crate `Error` (`InvalidArgument`) |
| `as_eq!`, `as_range!`, `as_contains!`, `as_contains_range!`, `as_within_region!`, `as_within_radius!`, `as_regions_containing_point!` | removed (they could not compile outside the crate); `Filter::equal`, `range`, `contains`, `contains_range`, `geo_within_region`, `geo_within_radius`, `geo_contains` |
| `expressions::device_size()`, `expressions::memory_size()` | removed; `expressions::record_size()` (server 7.0+) |
| `MapPolicy::new(order, MapWriteMode::Update)`, `MapPolicy::new_with_flags(order, flags)`, `MapWriteMode::{UpdateOnly, CreateOnly}` | `MapPolicy::new(order, MapWriteFlags::DEFAULT)`, `MapPolicy::new(order, flags)`, `MapWriteFlags::{UPDATE_ONLY, CREATE_ONLY}`; `new_with_flags_and_persisted_index` is `with_persisted_index`. `MapWriteMode` is removed |
| `Filter::geo_within_region_cit(bin, region, cit)` and the other five `geo_*_cit` constructors | `Filter::geo_within_region(bin, region).collection_type(cit)`; `collection_type` chains on any filter |

### Acronyms in identifiers

Acronyms are `UpperCamelCase` words. Names that existed in 2.x or in the 3.0
alphas:

| before | 3.0 |
|---|---|
| `BatchUDFPolicy` | `BatchUdfPolicy` |
| `UDFLang` | `UdfLang` |
| `ReadModeAP`, `ReadModeSC` | `ReadModeAp`, `ReadModeSc` |
| `ReadTouchTTL` | `ReadTouchTtl` |
| `HLLPolicy`, `HLLWriteFlags`, `ToHLLWriteFlagsBitmask` | `HllPolicy`, `HllWriteFlags`, `ToHllWriteFlagsBitmask` |
| `Value::GeoJSON`, `Value::HLL` | `Value::GeoJson`, `Value::Hll` |
| `AuthMode::PKI` | `AuthMode::Pki` |
| `PrivilegeCode::UDFAdmin`, `SIndexAdmin`, `ReadWriteUDF` | `UdfAdmin`, `SindexAdmin`, `ReadWriteUdf` |
| `ResultCode::XDRKeyBusy` | `ResultCode::XdrKeyBusy` |
| `QueryDuration::LongRelaxAP` | `QueryDuration::LongRelaxAp` |
| `ExpType::{NIL, BOOL, INT, STRING, LIST, MAP, BLOB, FLOAT, GEO, HLL}` | `ExpType::{Nil, Bool, Int, String, List, Map, Blob, Float, Geo, Hll}` |
| `RegexFlag::{NONE, EXTENDED, ICASE, NOSUB, NEWLINE}` enum, `regex_compare(regex, flags: i64, bin)` with `RegexFlag::ICASE as i64` | `RegexFlags` with the same constants, combined with `\|`; `regex_compare(regex, flags: RegexFlags, bin)` |

### Flags and return types

Every flag set in the operation and expression builders is one shape: a
newtype with SCREAMING constants, combined with `|`. `bits()` gives the raw
value and `from_bits(raw)` accepts one, so a flag the server supports before
this client names it can still be sent.

| 2.x / 3.0 alphas | 3.0 |
|---|---|
| `ListPolicy::new_with_flags(order, vec![ListWriteFlags::AddUnique, ListWriteFlags::NoFail])` | `ListPolicy::new(order, ListWriteFlags::ADD_UNIQUE \| ListWriteFlags::NO_FAIL)` |
| `HllPolicy::new_with_flags([HllWriteFlags::CreateOnly, HllWriteFlags::NoFail])` | `HllPolicy::new(HllWriteFlags::CREATE_ONLY \| HllWriteFlags::NO_FAIL)` |
| `BitPolicy::new(BitwiseWriteFlags::CreateOnly as u8 \| BitwiseWriteFlags::NoFail as u8)` | `BitPolicy::new(BitwiseWriteFlags::CREATE_ONLY \| BitwiseWriteFlags::NO_FAIL)` |
| `MapPolicy::new_with_flags(order, flags: u8)`, `MapWriteFlags` a module of `u8` constants | same call, `flags: MapWriteFlags` |
| `write_exp(bin, exp, vec![ExpWriteFlags::AllowDelete, ExpWriteFlags::PolicyNoFail])` | `write_exp(bin, exp, ExpWriteFlags::ALLOW_DELETE \| ExpWriteFlags::POLICY_NO_FAIL)` |
| `read_exp(bin, exp, ExpReadFlags::EvalNoFail)` | `read_exp(bin, exp, ExpReadFlags::EVAL_NO_FAIL)` |
| `bitwise::resize(bin, size, Some(BitwiseResizeFlags::FromFront), &policy)` | `bitwise::resize(bin, size, BitwiseResizeFlags::FROM_FRONT, &policy)`; `None` is `DEFAULT` |
| `lists::sort(bin, ListSortFlags::DropDuplicates)` | `lists::sort(bin, ListSortFlags::DESCENDING \| ListSortFlags::DROP_DUPLICATES)` |
| `ListPolicy.flags: u8`, `HllPolicy.flags: i64`, `BitPolicy.flags: u8`, `MapPolicy.flags: u8` | the typed flag set |
| `StringWriteFlags(raw)`, `SelectFlag(raw)` (public field) | `StringWriteFlags::from_bits(raw)`; the field is private |
| `ToListWriteFlagsBitmask`, `ToHllWriteFlagsBitmask`, `ToExpWriteFlagBitmask`, `ToExpReadFlagBitmask` | removed |

`ListReturnType` and `MapReturnType` are newtypes with selector constants and
an `inverted()` method; the `Inverted` variant, the `InvertedListReturn` /
`InvertedMapReturn` wrappers and the `ToListReturnTypeBitmask` /
`ToMapReturnTypeBitmask` traits are gone, and the builders take the return
type by value. Selectors are numbers, not bits, so there is no `|` on them.

| 2.x / 3.0 alphas | 3.0 |
|---|---|
| `ListReturnType::Values`, `MapReturnType::KeyValue`, … | `ListReturnType::VALUES`, `MapReturnType::KEY_VALUE`, … |
| `InvertedListReturn(ListReturnType::Values)` | `ListReturnType::VALUES.inverted()` |
| `InvertedMapReturn(MapReturnType::KeyValue)` | `MapReturnType::KEY_VALUE.inverted()` |

### Values and records

- `Value::OrderedMap` holds an `IndexMap` (insertion order) instead of a
  `BTreeMap`. The `BTreeMap` variant is `Value::SortedMap`. `From<BTreeMap>`
  produces `SortedMap`, `From<IndexMap>` produces `OrderedMap`. `as_ord_map!`
  builds an `IndexMap`; use `as_sorted_map!` for key order. Every map-taking
  API accepts all three through `MapCollection`. `IndexMap` is re-exported at
  the crate root.
- `Value::Unknown(particle_type, bytes)` carries a particle type the client
  does not interpret. Exhaustive matches on `Value` need an arm for it and for
  `SortedMap`.
- `From<Value> for i64` is `TryFrom<Value>` and `TryFrom<&Value>` with a
  `String` error: `let n: i64 = value.into()` becomes
  `let n = i64::try_from(&value)?`.
- `From<u64> for Value` panics on a value above `i64::MAX`; 2.x cast it to
  `i64` silently. Serializing `Value::Infinity`, `Value::Wildcard` or
  `Value::MultiResult` is a serde error instead of a panic.
- `Record::bins` is an `IndexMap<String, Value>` in the server's return order,
  not a `HashMap`. `Record` has a new `results: Option<Vec<Value>>` field:
  the per-operation results of an `operate` call in request order, `None`
  on every other path.

### Sync client

- Sixteen methods that were declared `pub async fn` in 2.x are blocking
  `pub fn` in 3.0 and no longer take `.await`: `set_xdr_filter`,
  `create_index_on_bin`, `create_index_using_expression`, `create_user`,
  `drop_user`, `change_password`, `grant_roles`, `revoke_roles`,
  `query_users`, `create_role`, `query_roles`, `drop_role`,
  `grant_privileges`, `revoke_privileges`, `set_allowlist`, `set_quotas`.
- The wrapper now mirrors the async client method for method, including
  transactions, metrics and `new_with_config`.
- `tls` needs `rt-tokio` with the blocking client too; the async-std flavour
  has no TLS.

- The task-returning methods (`create_index*`, `drop_index`, `register_udf*`,
  `remove_udf`, `query_operate`, `query_execute_udf`) return
  `aerospike_sync::Task<T>`, whose `wait_till_complete(timeout)` and
  `query_status()` block; `into_inner()` gives the asynchronous task back.
- `batch_foreach` takes a plain `Fn(usize, &BatchRecord) -> bool` instead of a
  future-returning closure.
- `query_foreach` exists and returns `aerospike_sync::QueryHandle`, whose
  `wait()` blocks; `cancel`, `is_active` and `partition_filter` are as on the
  asynchronous handle.


### Behaviour changes without an API change

- `Client::new` returns once the cluster has converged and the partition map
  is populated; `close()` stops the tend loop immediately.
- `Client::query_operate` and `query_execute_udf` treat `KEY_NOT_FOUND` from
  a node as "set absent on this node" and succeed.
- A batch that times out before anything reached the wire no longer marks its
  rows in doubt; in-doubt follows the failure's own flag.
- `ClientPolicy.rack_ids = Some(vec![])` is rejected at validation instead of
  enabling rack awareness with no rack to prefer.
- Default policy values are aligned with the Java client (see the tables
  above for the ones that changed), with four deliberate exceptions:
  `max_conns_per_node` 256 (Java 100); `idle_timeout` 0 disables idle reaping
  (Java trims to `min_conns_per_node` after 55 s); `AdminPolicy.timeout` 0
  falls back to 3 s (Java: no timeout); `QueryPolicy.record_queue_size` 1024
  (Java 5000).
- A write command that fails on the client after its request was sent is
  always in doubt. 2.x marked only timeouts and connection failures.

### New in 3.0

No migration is needed for these; see the changelog for details.

- `Client` (and the blocking `aerospike_sync::Client`) implements `Clone`.
  A clone shares the cluster, its connection pools and its background tasks,
  so pass clones to tasks instead of wrapping the client in an `Arc`;
  `close()` shuts the shared cluster down for every clone.

- Multi-record transactions (`Txn`, `Client::commit`, `Client::abort`) and
  strong-consistency read modes.
- Metrics (`Client::enable_metrics`, `MetricsPolicy`), dynamic configuration,
  `Client::info`, `Client::server_cluster_name`.
- Object mapping (`RecordMapper` derive, `ToValue`/`FromValue`, serde to
  `Value`).
- CDT path expressions, string operations and expressions, AEL filter
  expressions, `query_explain`, `query_foreach`, `query_aggregate` (behind
  `lua`), set indexes, integer indexes.
- `TlsPolicy::for_login_only`, `PartitionFilter` serialization, extended
  server error detail.
