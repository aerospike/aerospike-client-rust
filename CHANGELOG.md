# Changelog

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
  * [CLIENT-5081] `ClientPolicy` and `MetricsPolicy` defaults aligned with the Java client.
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
