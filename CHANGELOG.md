# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [1.2.0] - 2021-10-22

* **New Features**
  * Support Aerospike server v5.6+ expressions in Operate API. Thanks to [Jonas Breuer](https://github.com/jonas32)

* **Bug Fixes**
  * Fix for buffer size when using CDT contexts. Thanks to [Jonas Breuer](https://github.com/jonas32)

## [1.1.0] - 2021-10-12
This version of the client drops support for the older server versions without changing the API. `ScanPolicy.fail_on_cluster_change`, `ScanPolicy.scan_percent` and `BasePolicy.priority` are deprected for the Scan operations and will not be sent to the server. They remain in the API to avoid breaking the API.

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
