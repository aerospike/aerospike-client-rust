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

### Batch

`Client::batch` writes each row's outcome into the caller's operations
instead of returning a new vector.

| 2.x | 3.0 |
|---|---|
| `batch(&self, &BatchPolicy, &[BatchOperation]) -> Result<Vec<BatchRecord>>` | `batch(&self, &BatchPolicy, &mut [BatchOperation]) -> Result<()>` |
| `BatchOperation::batch_record(&self) -> BatchRecord` | `batch_record(&self) -> &BatchRecord`, plus `record()`, `take_record()`, `result_code()`, `in_doubt()`, `error()`, `node()` on the operation itself |
| `BatchRecord { key, record, result_code, in_doubt }`, all public fields | `key` and `record` stay fields; `result_code()`, `in_doubt()`, `node()`, `error()`, `error_detail()`, `sub_code()`, `server_message()` are methods |

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
| `consistency_level: ConsistencyLevel` (`ConsistencyOne`, `ConsistencyAll`) | `read_mode_ap: ReadModeAP` (`One`, `All`) for AP namespaces, plus `read_mode_sc: ReadModeSC` (`Session`, `Linearize`, `AllowReplica`) for strong-consistency namespaces |
| — | `txn`, `use_compression`, `compression_threshold`, `sleep_multiplier`, `error_detail_verbosity`, `populate_positional_results` |

`ConsistencyLevel` is removed from the crate root; `ReadModeAP`, `ReadModeSC`,
`TlsPolicy`, `TxnVerifyPolicy` and `TxnRollPolicy` are exported there.

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
  above for the ones that changed).

### New in 3.0

No migration is needed for these; see the changelog for details.

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
