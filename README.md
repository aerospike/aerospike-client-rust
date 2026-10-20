# Aerospike Rust Client 

Welcome to Aerospike's official [Rust client](https://aerospike.com/docs/develop/client/rust).

## About Aerospike

[Aerospike](https://aerospike.com/) is a distributed database built for workloads that need predictable, sub-millisecond latency at high throughput: real-time bidding, fraud detection, user profiles, session stores, and other systems where a request has a tight time budget. A cluster is a set of identical nodes that share the data; records are spread over 4096 partitions that the cluster assigns to nodes automatically, so adding or losing a node rebalances the data without any routing configuration on the client. The Hybrid Memory Architecture keeps the primary index in memory and the records on flash or in memory, which is what lets a small cluster serve large data sets quickly.

Data is organised into namespaces (the unit of storage policy), sets (the loose equivalent of tables) and records addressed by a key. A record holds bins: typed values that can be integers, floats, strings, blobs, booleans, GeoJSON, HyperLogLog sketches, and nested lists and maps. The server operates on those values in place through its collection data types (CDTs, aka Lists and Maps), bitwise operations and string operations, so a client can modify one element of a map or append to a list without reading and rewriting the record.

Beyond single-record reads and writes, the server offers batch operations across many keys, secondary indexes with queries and scans, filter expressions evaluated on the server to select records or compute values, user-defined functions written in Lua, and multi-record transactions with ACID guarantees on strong-consistency namespaces. Namespaces can run in availability mode or strong-consistency mode, and the Enterprise Edition adds cross-datacenter replication, security (authentication, roles, TLS) and more.

## About this client

This crate is the Rust client for that server. It is async-first: every operation is a future driven by Tokio by default, with async-std as an alternative runtime and a blocking API behind the `sync` feature for code without an async runtime. It speaks the native wire protocol directly, keeps a connection pool per node, follows the cluster's partition map as nodes come and go, and retries and times out according to policies you set per call.

It covers the server's feature set: single-record and batch reads, writes and deletes; operations on lists, maps, bits, HyperLogLog and strings, path expressions; secondary indexes, queries, scans and pagination with resumable cursors; filter and read/write expressions; UDFs, including client-side stream aggregation behind the `lua` feature; multi-record transactions; TLS and authentication; cluster, user and role administration; client metrics; and a dynamic configuration file shared with the other Aerospike clients. The sections below show the main patterns, and the [examples](https://github.com/aerospike/aerospike-client-rust/blob/v3/examples/README.md) directory holds a runnable program for each feature area.

## For AI coding agents

[AGENTS.md](https://github.com/aerospike/aerospike-client-rust/blob/v3/AGENTS.md) is the entry point: which crate holds the API, how the feature flags select what compiles, the 3.0 API conventions, the server version gates, and how to build, lint and test generated code against a live server.

## Feature highlights

**Execution models:**

- **Async-First:** Built for non-blocking IO, powered by [Tokio](https://tokio.rs/) by default, with optional support for [async-std](https://async.rs/).
- **Sync Support:** Blocking APIs are available using a sync sub-crate for flexibility in legacy or mixed environments.

**Advanced data operations:**

- **Batch protocol:** full support for read, write, delete, and udf operations through the `BatchOperation` API.
- **Lists** (`operations::lists`): create, set order, append, insert, set, increment, trim, clear, sort, size, get and remove by index, index range, rank, rank range, value, value list, value range and relative rank, pop, and get range.
- **Maps** (`operations::maps`): create, set policy and order, put, increment and decrement, clear, size, get and remove by key, key list, key range, relative index, value, value list, value range, relative rank, index, index range, rank and rank range.
- **Bitwise** (`operations::bitwise`): resize, insert, remove, set, or, xor, and, not, left and right shift, add, subtract, set and get integer, get, count, left and right scan, and base64 encode.
- **HyperLogLog** (`operations::hll`): init, add, set union, refresh count, fold, get count, get union, get union count, get intersect count, get similarity, and describe.
- **Strings** (`operations::string`, server 8.2.0+): length and byte length, substring, char at, find, contains, starts and ends with, numeric checks and conversions, case checks and conversion, split, insert, overwrite, concat, append, prepend, snip, replace, trim, pad, repeat, Unicode normalisation, base64 decode, and regex compare and replace.
- **Path expressions** (`operations::path`, server 8.1.1+): JSONPath-style selection and modification of nested list and map elements in one server-side operation: select by path, values, map keys, map entries or matching tree, and modify or remove by path.
- Every CDT operation has an expression counterpart in `expressions::{lists, maps, bitwise, hll, string}`, so the same work can run inside a filter or a read/write expression.

**Policy and expression enhancements:**

- **Replica policies:** includes support for Replica, including PreferRack placement.
- **Policy additions:** new fields such as `allow_inline_ssd`, `respond_all_keys` in `BatchPolicy`, `read_touch_ttl`, and `QueryDuration` in `QueryPolicy`.
- **Rate limiting:** supports `records_per_second` for query throttling.

**Data model improvements:**

- **Type support:** adds support for boolean particle type.
- **New data constructs:** returns types such as `Exists`, `OrderedMap`, `UnorderedMap` now supported for [CDT](https://aerospike.com/docs/develop/data-types/collections/) reads.
- **Value conversions:** implements `TryFrom<Value>` for the common Rust types, for seamless type interoperability.
- **Infinity and wildcard:** supports `Infinity`, `Wildcard`, and corresponding expression builders `expressions::infinity()` and `expressions::wildcard()`.
- **Size expressions:** adds `expressions::record_size()`; the server-deprecated `device_size()` and `memory_size()` are not carried into 3.0.

Take a look at the [changelog](https://github.com/aerospike/aerospike-client-rust/blob/v3/CHANGELOG.md) for more details. Upgrading from 2.x: see [MIGRATION.md](https://github.com/aerospike/aerospike-client-rust/blob/v3/MIGRATION.md).

## Getting started

Prerequisites:

- [Aerospike Database](https://aerospike.com/download/server/community/) 6.4 or later.
- [Rust](https://www.rust-lang.org/) version 1.87 or later
- [Tokio runtime](https://tokio.rs/) or [async-std](https://async.rs/)

### Upgrading from 2.x

3.0 changes the batch, query, error and policy APIs. The [migration guide](https://github.com/aerospike/aerospike-client-rust/blob/v3/MIGRATION.md) lists every change with its 2.x and 3.0 spelling side by side, and the [changelog](https://github.com/aerospike/aerospike-client-rust/blob/v3/CHANGELOG.md) has the details behind each one.

### Examples

The [`examples/`](https://github.com/aerospike/aerospike-client-rust/blob/v3/examples/README.md) directory holds one runnable program per feature area: CRUD, batch, queries, scans, CDT and bitwise operations, path expressions, UDFs, transactions, server info and a blocking-client variant. Every example runs against a live server in the test suite, so they are kept working. Run one with:

```bash
AEROSPIKE_HOSTS=127.0.0.1:3000 cargo run --example crud
```

## Installation

### Build from source

1. Clone the repository and change into the project directory:

   ```
   git clone --single-branch --branch v3 https://github.com/aerospike/aerospike-client-rust.git
   cd aerospike-client-rust
   ```

2. Build the project:

   ```
   cargo build
   ```

### Use as a dependency

To use the client in your own project, add one of the following to your `Cargo.toml`:

   ```
   [dependencies]
   # Async API with tokio Runtime
   aerospike = { version = "<version>", features = ["rt-tokio"]}

   # OR

   # Async API with async-std runtime
   # (default-features = false is required: the default `rt-tokio` and
   # `rt-async-std` cannot both be enabled — that's a compile error, not a
   # runtime one)
   aerospike = { version = "<version>", default-features = false, features = ["async", "serialization", "rt-async-std"]}

   # Sync API; pick the runtime it blocks on with `rt-tokio` or `rt-async-std`
   aerospike = { version = "<version>", default-features = false, features = ["sync", "serialization", "rt-tokio"]}
   ```

   > **Note:** on Tokio the sync client owns a dedicated runtime and can be called from inside a caller's Tokio runtime. On async-std it uses `async_std::task::block_on`, which must not be called from inside an async-std task. `tls` needs `rt-tokio` with either client.

   Then run `cargo build` in your project.

## Core feature examples

The following code examples demonstrate some of the Rust client's new features.

### Client connection

The examples below use the **async** client (default). For a blocking API with no `.await`, see [Sync client](#sync-client) below.

#### Standard connection

Connect to an Aerospike cluster without TLS:

```rust
use std::env;
use aerospike::{Client, ClientPolicy};

let policy = ClientPolicy::default();
let hosts = env::var("AEROSPIKE_HOSTS")
    .unwrap_or_else(|_| "127.0.0.1:3000".to_string());
let client = Client::new(&policy, &hosts)
    .await
    .expect("Failed to connect to cluster");
```

#### Sync client

The `sync` feature exposes blocking APIs — no `async`/`.await` at call sites and no runtime to set up. The blocking client drives the async client itself: on `rt-tokio` it owns a dedicated Tokio runtime, on `rt-async-std` it blocks on async-std's global executor.

**Cargo.toml**
```toml
[dependencies]
aerospike = { version = "<version>", default-features = false, features = ["sync", "serialization", "rt-tokio"] }
```

> Swap `rt-tokio` for `rt-async-std` if your project uses async-std instead. `tls` needs `rt-tokio` with either client.

**Example:**
```rust
use std::env;
use aerospike::{as_bin, as_key, Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let policy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS")
        .unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    let client = Client::new(&policy, &hosts)?;

    let key = as_key!("test", "myset", "sync-key");
    let bins = [as_bin!("name", "Alice"), as_bin!("count", 42)];
    client.put(&WritePolicy::default(), &key, &bins)?;

    let record = client.get(&ReadPolicy::default(), &key, Bins::All)?;
    println!("Record: {:?}", record.bins);

    client.close()?;
    Ok(())
}
```

> **Calling it from async code.** On Tokio the blocking client may be called from inside a caller's Tokio runtime: it blocks on its own runtime, not the caller's. On async-std its methods must not be called from inside an async-std task, because `async_std::task::block_on` cannot nest. Task-returning methods (`create_index`, `register_udf`, …) return `aerospike::Task`, whose `wait_till_complete` blocks, and `query_foreach` returns `aerospike::QueryHandle`.

#### TLS connection without client authentication

Connect to an Aerospike cluster with TLS but without client certificate authentication:

```rust
use aerospike::{Client, ClientPolicy, TlsPolicy};
use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;

fn tls_config_no_client_auth(ca_cert_path: &str) -> rustls::ClientConfig {
    let mut root_store = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    };

    // Add custom CA certificate
    root_store.add_parsable_certificates(
        CertificateDer::pem_file_iter(ca_cert_path)
            .expect("Cannot open CA file")
            .map(|result| result.unwrap()),
    );

    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

let mut policy = ClientPolicy::default();
policy.tls_policy = Some(TlsPolicy::new(tls_config_no_client_auth("/path/to/ca-cert.pem")));

let hosts = "tls-cluster.example.com:4333";
let client = Client::new(&policy, hosts).await
    .expect("Failed to connect to cluster");
```

#### TLS connection with client authentication

Connect to an Aerospike cluster with TLS and mutual authentication using client certificates:

```rust
use aerospike::{Client, ClientPolicy, TlsPolicy};
use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

fn tls_config_with_client_auth(
    ca_cert_path: &str,
    client_cert_path: &str,
    client_key_path: &str,
) -> rustls::ClientConfig {
    let mut root_store = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    };

    // Add custom CA certificate
    root_store.add_parsable_certificates(
        CertificateDer::pem_file_iter(ca_cert_path)
            .expect("Cannot open CA file")
            .map(|result| result.unwrap()),
    );

    // Load client certificate and private key
    let client_cert = CertificateDer::from_pem_file(client_cert_path)
        .expect("Cannot open client certificate file");
    let client_key = PrivateKeyDer::from_pem_file(client_key_path)
        .expect("Cannot open client key file");

    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![client_cert], client_key)
        .expect("Failed to configure client authentication")
}

let mut policy = ClientPolicy::default();
policy.tls_policy = Some(TlsPolicy::new(tls_config_with_client_auth(
    "/path/to/ca-cert.pem",
    "/path/to/client-cert.pem",
    "/path/to/client-key.pem",
)));

let hosts = "tls-cluster.example.com:4333";
let client = Client::new(&policy, hosts).await
    .expect("Failed to connect to cluster");
```

Every connection is encrypted by default. To encrypt only the authentication exchange and run the data plane in cleartext, set `for_login_only`:

```rust
policy.tls_policy =
    Some(TlsPolicy::new(tls_config_no_client_auth("/path/to/ca-cert.pem")).with_login_only(true));
```

The login rides TLS; the client then reads the node's non-TLS address, closes the TLS connection and reconnects there, authenticating every later connection with the session token. Credentials never cross a cleartext socket. **This trades away data-plane encryption**: records, bin values and query results travel unencrypted. It requires an `auth_mode` other than `AuthMode::None`.

**Note**: To use TLS features, enable the `tls` feature in your `Cargo.toml`:

```toml
[dependencies]
aerospike = { version = "...", features = ["tls"] }
```

### CRUD operations

```rust
use std::env;
use std::time::Instant;

use aerospike::{as_bin, as_key, Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
use aerospike::operations;

#[tokio::main]
async fn main() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS")
        .unwrap_or(String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts).await
        .expect("Failed to connect to cluster");

    let now = Instant::now();
    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    let key = as_key!("test", "test", "test");

    let bins = [
        as_bin!("int", 999),
        as_bin!("str", "Hello, World!"),
    ];
    client.put(&wpolicy, &key, &bins).await.unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All).await;
    println!("Record: {}", rec.unwrap());

    client.touch(&wpolicy, &key).await.unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All).await;
    println!("Record: {}", rec.unwrap());

    let rec = client.get(&rpolicy, &key, Bins::None).await;
    println!("Record Header: {}", rec.unwrap());

    let exists = client.exists(&rpolicy, &key).await.unwrap();
    println!("exists: {}", exists);

    let bin = as_bin!("int", "123");
    let ops = &vec![operations::put(&bin), operations::get()];
    let op_rec = client.operate(&wpolicy, &key, ops).await;
    println!("operate: {}", op_rec.unwrap());

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    println!("existed (should be true): {}", existed);

    let existed = client.delete(&wpolicy, &key).await.unwrap();
    println!("existed (should be false): {}", existed);

    println!("total time: {:?}", now.elapsed());
}
```

### Batch operations

`Client::batch` writes each row's outcome into the operations you pass in; read it back through `record()`, `result_code()` and `error()` on each `BatchOperation`. Batches that write use `BatchPolicy::write_default()` (no retries), batches that only read use `BatchPolicy::default()`.

```rust
use aerospike::{
    as_bin, as_key, as_val, operations, AdminPolicy, BatchDeletePolicy, BatchOperation,
    BatchPolicy, BatchReadPolicy, BatchUdfPolicy, BatchWritePolicy, Bins, Task, UdfLang,
};

let apolicy = AdminPolicy::default();

let udf_body = r#"
function echo(rec, val)
    return val
end
"#;

let task = client
    .register_udf(&apolicy, udf_body.as_bytes(), "test_udf.lua", UdfLang::Lua)
    .await
    .unwrap();
task.wait_till_complete(None).await.unwrap();

let bin1 = as_bin!("a", "a value");
let bin2 = as_bin!("b", "another value");
let bin3 = as_bin!("c", 42);

let key1 = as_key!(namespace, set_name, 1);
let key2 = as_key!(namespace, set_name, 2);
let key3 = as_key!(namespace, set_name, 3);

let key4 = as_key!(namespace, set_name, -1);
// key does not exist

let selected = Bins::from(["a"]);
let all = Bins::All;
let none = Bins::None;

let wops = vec![
    operations::put(&bin1),
    operations::put(&bin2),
    operations::put(&bin3),
];

let rops = vec![
    operations::get_bin(&bin1.name),
    operations::get_bin(&bin2.name),
    operations::get_header(),
];

let bpr = BatchReadPolicy::default();
let bpw = BatchWritePolicy::default();
let bpd = BatchDeletePolicy::default();
let bpu = BatchUdfPolicy::default();

// Writes: every row carries its outcome after the call.
let mut batch = vec![
    BatchOperation::write(&bpw, key1.clone(), wops.clone()),
    BatchOperation::write(&bpw, key2.clone(), wops.clone()),
    BatchOperation::write(&bpw, key3.clone(), wops.clone()),
];
client.batch(&BatchPolicy::write_default(), &mut batch).await.unwrap();
for op in &batch {
    println!("write: {:?} -> {:?}", op.result_code(), op.record());
}

// Reads
let mut batch = vec![
    BatchOperation::read(&bpr, key1.clone(), selected),
    BatchOperation::read(&bpr, key2.clone(), all),
    BatchOperation::read(&bpr, key3.clone(), none.clone()),
    BatchOperation::read_ops(&bpr, key3.clone(), rops),
    BatchOperation::read(&bpr, key4.clone(), none),
];
client.batch(&BatchPolicy::default(), &mut batch).await.unwrap();
for op in &batch {
    match op.record() {
        Some(record) => println!("read: {:?}", record.bins),
        None => println!("read failed: {:?}", op.error()),
    }
}

// UDF calls
let mut batch = vec![
    BatchOperation::udf(&bpu, key1.clone(), "test_udf", "echo", vec![as_val!(1)]),
    BatchOperation::udf(&bpu, key2.clone(), "test_udf", "echo", vec![as_val!(2)]),
    BatchOperation::udf(&bpu, key3.clone(), "test_udf", "echo", vec![as_val!(3)]),
    BatchOperation::udf(&bpu, key4.clone(), "test_udf", "echo", vec![as_val!(4)]),
];
client.batch(&BatchPolicy::write_default(), &mut batch).await.unwrap();
for op in &batch {
    println!("udf: {:?} -> {:?}", op.result_code(), op.record());
}

// Deletes
let mut batch = vec![
    BatchOperation::delete(&bpd, key1.clone()),
    BatchOperation::delete(&bpd, key2.clone()),
    BatchOperation::delete(&bpd, key3.clone()),
    BatchOperation::delete(&bpd, key4.clone()),
];
client.batch(&BatchPolicy::write_default(), &mut batch).await.unwrap();
for op in &batch {
    println!("delete: {:?}", op.result_code());
}
```

A complete working example can be found in [examples/batch_operations.rs](https://github.com/aerospike/aerospike-client-rust/blob/v3/examples/batch_operations.rs).

### Query operations

The Rust client supports various query patterns for retrieving data from Aerospike. Below are examples demonstrating different query capabilities.

#### Simple equality query

Query records where a bin equals a specific value:

```rust
use aerospike::{QueryPolicy, Statement, Bins};
use aerospike::query::{Filter, PartitionFilter};
use futures::StreamExt;

let policy = QueryPolicy::default();
let mut stmt = Statement::new(namespace, set_name, Bins::All);
stmt.set_filter(Filter::equal("bin_name", 5));

let rs = client.query(&policy, PartitionFilter::all(), stmt).await.unwrap();
let mut rs = rs.into_stream();

while let Some(r) = rs.next().await {
    println!("Record: {:?}", r.unwrap());
}
```

#### Range query

Query records where a bin value falls within a range:

```rust
let policy = QueryPolicy::default();
let mut stmt = Statement::new(namespace, set_name, Bins::All);
stmt.set_filter(Filter::range("bin_name", 0, 100));

let rs = client.query(&policy, PartitionFilter::all(), stmt).await.unwrap();
let mut rs = rs.into_stream();

while let Some(r) = rs.next().await {
    println!("Record: {:?}", r.unwrap());
}
```

#### Metadata-only query

Query records but only retrieve metadata (no bin data):

```rust
let policy = QueryPolicy::default();
let mut stmt = Statement::new(namespace, set_name, Bins::None);
stmt.set_filter(Filter::range("bin_name", 0, 100));

let rs = client.query(&policy, PartitionFilter::all(), stmt).await.unwrap();
let mut rs = rs.into_stream();

while let Some(r) = rs.next().await {
    let rec = r.unwrap();
    println!("Generation: {}, TTL: {:?}", rec.generation, rec.time_to_live());
}
```

#### Cursor-based pagination

Query records in batches using partition cursors for pagination:

```rust
let policy = QueryPolicy::default();
let mut pf = PartitionFilter::all();

while !pf.done() {
    let stmt = Statement::new(namespace, set_name, Bins::All);
    let rs = client.query(&policy, pf, stmt).await.unwrap();
    let mut rs = rs.into_stream();
    
    while let Some(r) = rs.next().await {
        println!("Record: {:?}", r.unwrap());
    }
    
    // Get the next partition filter to continue pagination
    pf = rs.partition_filter().unwrap();
}
```

#### Parallel query with multiple consumers

Process query results in parallel using multiple async tasks:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

let policy = QueryPolicy::default();
let mut stmt = Statement::new(namespace, set_name, Bins::All);
stmt.set_filter(Filter::range("bin_name", 0, 100));

let rs = client.query(&policy, PartitionFilter::all(), stmt).await.unwrap();
let count = Arc::new(AtomicUsize::new(0));
let mut handles = vec![];

// Spawn 4 worker tasks to process results in parallel
for _ in 0..4 {
    let rs_clone = rs.clone();
    let count = count.clone();
    
    handles.push(tokio::spawn(async move {
        let mut rs_stream = rs_clone.into_stream();
        while let Some(record) = rs_stream.next().await {
            if record.is_ok() {
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }));
}

futures::future::join_all(handles).await;
println!("Total processed: {}", count.load(Ordering::Relaxed));
```

#### Query with expression filter

Use filter expressions for more complex filtering logic:

```rust
use aerospike::expressions::{eq, int_bin, int_val};

let mut policy = QueryPolicy::default();
policy.base_policy.filter_expression.replace(
    eq(int_bin("bin_name"), int_val(42))
);

let stmt = Statement::new(namespace, set_name, Bins::All);
let rs = client.query(&policy, PartitionFilter::all(), stmt).await.unwrap();
let mut rs = rs.into_stream();

while let Some(r) = rs.next().await {
    println!("Record: {:?}", r.unwrap());
}
```

#### Rate-limited query

Control query throughput by limiting records per second:

```rust
let mut policy = QueryPolicy::default();
policy.records_per_second = 100;  // Limit to 100 records/second

let mut stmt = Statement::new(namespace, set_name, Bins::All);
stmt.set_filter(Filter::range("bin_name", 0, 1000));

let rs = client.query(&policy, PartitionFilter::all(), stmt).await.unwrap();
let mut rs = rs.into_stream();

while let Some(r) = rs.next().await {
    match r {
        Ok(rec) => println!("Record: {:?}", rec),
        Err(err) => eprintln!("Error: {:?}", err),
    }
}
```

#### Prerequisites for queries

Before running queries, you need to create a secondary index on the bin you want to query:

```rust
use aerospike::{AdminPolicy, CollectionIndexType, IndexType, Task};

let policy = AdminPolicy::default();
let task = client
    .create_index_on_bin(
        &policy,
        namespace,
        set_name,
        "bin_name",
        "idx_bin_name",
        IndexType::Numeric,
        CollectionIndexType::Default,
        None,
    )
    .await
    .expect("Failed to create index");

// Wait for index creation to complete
task.wait_till_complete(None).await.unwrap();
```

For a complete working example with all query patterns, see [`examples/query.rs`](https://github.com/aerospike/aerospike-client-rust/blob/v3/examples/query.rs).

### Timeout configuration

The Rust client provides flexible timeout configuration through `socket_timeout` and `total_timeout` parameters in policies. Understanding how these interact is crucial for handling network issues and controlling command execution time.

#### Timeout parameters

- **`socket_timeout`**: Socket idle timeout when processing a database command (in milliseconds). Default value 30000 (30 seconds).
- **`total_timeout`**: Total command timeout, including retries (in milliseconds). Default value 1000 (1 second); `0` means no limit.

#### Timeout behavior rules

1. **Both zero (0, 0)**: No timeout limits - commands wait indefinitely
2. **Socket zero, total non-zero (0, N)**: `socket_timeout` inherits `total_timeout` value
3. **Socket non-zero, total zero (N, 0)**: Socket idle timeout of N ms, no total limit
4. **Both non-zero, socket > total (N, M where N > M)**: `socket_timeout` capped at `total_timeout`
5. **Both non-zero, socket ≤ total (N, M where N ≤ M)**: Both timeouts enforced independently

When a socket timeout occurs, the client checks `max_retries` and `total_timeout`. If neither is exceeded, the command is automatically retried.

Rust client exposes these parameters through the Read/Write policy, and can be tuned as below:

```rust
use aerospike::{ReadPolicy, Bins};

let mut policy = ReadPolicy::default();
policy.base_policy.socket_timeout = 0;
policy.base_policy.total_timeout = 0;

let rec = client.get(&policy, &key, Bins::All).await;
```

#### Socket recovery with timeout_delay

The `timeout_delay` parameter controls how the client handles sockets after a read timeout. This is particularly important for cloud deployments.

```rust
let mut policy = ReadPolicy::default();
policy.base_policy.socket_timeout = 2000;  // 2 second socket timeout
policy.base_policy.total_timeout = 10000;  // 10 second total timeout
policy.base_policy.timeout_delay = 3000;   // 3 second delay for socket recovery

let rec = client.get(&policy, &key, Bins::All).await;
```

**How `timeout_delay` works:**

1. **When `timeout_delay = 0` (default)**: Socket is immediately closed on timeout
2. **When `timeout_delay > 0`**: After a socket read timeout, the client attempts to drain remaining data from the socket in the background for up to `timeout_delay` milliseconds
    - If all data is drained within the delay: Socket returned to connection pool (reusable)
    - If delay expires before draining completes: Socket is closed

**Why use `timeout_delay`?**

Many cloud providers experience performance issues when clients close sockets while the server still has data to write (results in TCP RST packets). Draining the socket before closing avoids this penalty.

**Trade-offs:**

- ✓ Avoids TCP RST performance penalties on cloud platforms
- ✓ Allows socket reuse when recovery is successful
- ✗ Requires extra processing to drain sockets
- ✗ May need additional connections for command retries during recovery

**Recommended value:** If enabling `timeout_delay`, 3000ms (3 seconds) is a reasonable starting point.

For a complete working example demonstrating timeout scenarios, see [`examples/timeout_configuration.rs`](https://github.com/aerospike/aerospike-client-rust/blob/v3/examples/timeout_configuration.rs).

### Dynamic configuration

The client can load policy overrides from a YAML file and apply them at runtime, so timeouts, retries, read modes, metrics, and more can be tuned without restarting your application. This is gated behind the `dynamic-config` cargo feature and uses the same cross-client config file format as the other Aerospike clients, so one file can be shared across languages.

```toml
[dependencies]
aerospike = { version = "<version>", features = ["rt-tokio", "dynamic-config"] }
```

There are two ways to point the client at a config file.

**1. Environment variable** — set `AEROSPIKE_CLIENT_CONFIG_URL` and construct the client normally; it is picked up automatically:

```bash
export AEROSPIKE_CLIENT_CONFIG_URL="file:///etc/aerospike/config.yaml"
# a bare path (no scheme) is treated as file:// too:
export AEROSPIKE_CLIENT_CONFIG_URL="/etc/aerospike/config.yaml"
```

```rust
let client = Client::new(&ClientPolicy::default(), &"127.0.0.1:3000").await?;
```

**2. Explicit provider** — inject a `YamlFileProvider` (no environment variable needed):

```rust
use std::sync::Arc;
use aerospike::config::YamlFileProvider;

let provider = Arc::new(YamlFileProvider::new("/etc/aerospike/config.yaml"));
let client = Client::new_with_config(
    &ClientPolicy::default(),
    &"127.0.0.1:3000",
    provider,
).await?;
```

Example config file:

```yaml
version: "1.0.0"          # required, or the file is ignored
static:
  client:
    config_interval: 5     # seconds between reloads (min 1s)
dynamic:
  read:
    total_timeout: 1000
    max_retries: 3
  write:
    send_key: true
    durable_delete: true
  metrics:
    enabled: true             # Tier 0: pool gauges, opened/closed, tend counts
    labels:
      app_id: billing
    extended:
      operational:            # Tier 1: latency histograms, bytes, errors
        enabled: true
        latency_unit: ms      # ms (default) | us
        latency_columns: 7    # <=1, >1, >2, >4, >8, >16, >32
        latency_shift: 1      # boundary spacing 2^shift
```

Notes:

- The file **must** contain a top-level `version` key or it is ignored (no error).
- It is re-read on a background watcher only when its modification time changes.
- Unsupported sections or keys are ignored rather than causing errors, so a config file written for another client still works.
- Only the `file://` scheme is built in; register others with `aerospike::config::register_provider`.

**Any source will do.** A provider is anything that implements `ConfigProvider`: one async `load` that returns a `ConfigDocument`, or `None` when nothing changed. The document is a plain serde type, so it can come from JSON, a key-value store or a service as easily as from a YAML file. [`examples/config_pigeon.rs`](https://github.com/aerospike/aerospike-client-rust/blob/v3/examples/config_pigeon.rs) is a carrier pigeon for a REST service: it fetches the document over HTTP, hands it over only when the service's answer changed, registers its own `pigeon://` scheme (a scheme named after the provider cannot collide with another provider's) so `AEROSPIKE_CLIENT_CONFIG_URL=pigeon://host:port/config` works, and shows a change on the service side reaching a running client.

### Stream UDF aggregation (client-side Lua)

Aggregation queries (`Client::query_aggregate`) run a stream UDF's map/reduce pipeline split across the cluster and the client: each server node executes the server-scope operations and returns one partial result, and the client combines the partials by running the remaining operations (from the first `reduce` onward) in an embedded Lua 5.4 interpreter. The interpreter is only compiled in when the `lua` cargo feature is enabled:

```toml
[dependencies]
aerospike = { version = "<version>", features = ["rt-tokio", "lua"] }
```

The UDF must be registered on the server **and** its source must be available to the client — either as `<package>.lua` in the directory set via `aerospike::lua::set_lua_path` (default `"udf"`), or registered in-memory with `aerospike::lua::register_package`:

```rust
use aerospike::*;
use futures::StreamExt;

aerospike::lua::set_lua_path("udf/"); // where my_package.lua lives

let stmt = Statement::new("test", "test", Bins::All);
let rs = client
    .query_aggregate(
        &QueryPolicy::default(),
        stmt,
        "my_package",
        "sum_single_bin",
        &[as_val!("score")],
    )
    .await?;

let mut stream = rs.into_stream();
while let Some(value) = stream.next().await {
    println!("aggregation result: {:?}", value?);
}
```

See [`examples/query_aggregate.rs`](https://github.com/aerospike/aerospike-client-rust/blob/v3/examples/query_aggregate.rs) for a complete working example, including the Lua UDF source.

## Good to Know

These patterns appear to require custom application logic or missing library features, but are already handled natively by the client.

* **Single-key batch operations automatically fall back to single-record calls.** If routing assigns only one key to a given node within a batch, the client uses the single-record protocol for that node automatically—no manual routing checks or single-record fallbacks needed. See [`Client::batch`](https://docs.rs/aerospike/latest/aerospike/struct.Client.html#method.batch).
* **Use `Client::batch` for multi-key operations instead of sequential loops.** Multi-key reads and writes should always go through `Client::batch` rather than looping individual calls like `get` or `put`. See the cross-references on [`get`](https://docs.rs/aerospike/latest/aerospike/struct.Client.html#method.get), `put`, `delete`, and `operate`.
* **Path-based modifications support deletion out of the box.** You can delete path targets using `modify_by_path` or `exp_modify_by_path` by passing `exp_remove_result()`, or simply use the [`remove`](https://docs.rs/aerospike/latest/aerospike/operations/path/fn.modify_by_path.html) and [`exp_remove`](https://docs.rs/aerospike/latest/aerospike/expressions/fn.exp_modify_by_path.html) wrappers.

## Feedback wanted

We need your help with:

- Real-world async patterns in your codebase
- Ergonomic pain points in API design

You’re not just testing this new client - you’re shaping the future of Rust in databases!

You can reach us through [Github Issues](https://github.com/aerospike/aerospike-client-rust/issues) or schedule a meeting to speak directly with our product team using [this scheduling link](https://calendar.app.google/sDseJu6vUg8da5Kw5).