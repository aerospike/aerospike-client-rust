# Aerospike Rust Client 

Welcome to the preview of Aerospike's official [Rust client](https://aerospike.com/docs/develop/client/rust).
This is your opportunity to help shape the direction of the Rust
client's ongoing development.

This early-release library brings async-native database operations to Rust 
developers, with support for batch updates and partition queries. 
We welcome your feedback as we work toward production readiness.

## Feature highlights

**Execution models:**

- **Async-First:** Built for non-blocking IO, powered by 
  [Tokio](https://tokio.rs/) by default, with optional support for [async-std](https://async.rs/).
- **Sync Support:** Blocking APIs are available using a sync sub-crate for 
  flexibility in legacy or mixed environments.

**Advanced data operations:**

- **Batch protocol:** full support for read, write, delete, and udf operations through the 
  new `BatchOperationAPI`.
- **New query wire protocols:** implements updated query protocols for 
  improved consistency and performance.

**Policy and expression enhancements:**

- **Replica policies:** includes support for Replica, including PreferRack placement.
- **Policy additions:** new fields such as `allow_inline_ssd`, `respond_all_keys` 
  in `BatchPolicy`, `read_touch_ttl`, and `QueryDuration` in `QueryPolicy`.
- **Rate limiting:** supports `records_per_second` for query throttling.

**Data model improvements:**

- **Type support:** adds support for boolean particle type.
- **New data constructs:** returns types such as `Exists`, 
  `OrderedMap`, `UnorderedMap` now supported for 
  [CDT](https://aerospike.com/docs/develop/data-types/collections/) reads.
- **Value conversions:** implements `TryFromaerospike::Value` for seamless type interoperability.
- **Infinity and wildcard:** supports `Infinity`, `Wildcard`, and 
  corresponding expression builders `expressions::infinity()` and 
  `expressions::wildcard()`.
- **Size expressions:** adds `expressions::record_size()` and `expressions::memory_size()` 
  for granular control.

Take a look at the [changelog](https://github.com/aerospike/aerospike-client-rust/blob/v2/CHANGELOG.md) for more details.

## What’s coming next?
We are working toward full functional parity with our 
other officially supported clients. Features on the roadmap include:

- Partition queries
- Distributed ACID transactions
- Strong consistency
- Full TLS support for secure, production-ready deployments

## Getting started

Prerequisites:

- [Aerospike Database](https://aerospike.com/download/server/community/) 6.4 or later.
- [Rust](https://www.rust-lang.org/) version 1.87 or later 
- [Tokio runtime](https://tokio.rs/) or [async-std](https://async.rs/)

## Installation

### Build from source

1. Clone the repository and change into the project directory:

   ```
   git clone --single-branch --branch v2 https://github.com/aerospike/aerospike-client-rust.git
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
   aerospike = { version = "<version>", features = ["rt-async-std"]}

   # The library still supports the old sync interface, but it will be deprecated in the future.
   # This is only for compatibility reasons and will be removed in a later stage.

   # Sync API with tokio
   aerospike = { version = "<version>", default-features = false, features = ["rt-tokio", "sync"]}

   # OR

   # Sync API with async-std
   aerospike = { version = "<version>", default-features = false, features = ["rt-async-std", "sync"]}
   ```

   Then run `cargo build` in your project.

## Core feature examples

The following code examples demonstrate some of the Rust client's new
features.

### Client connection

#### Standard connection

Connect to an Aerospike cluster without TLS:

```rust
use aerospike::{Client, ClientPolicy};

let policy = ClientPolicy::default();
let hosts = env::var("AEROSPIKE_HOSTS")
    .unwrap_or(String::from("127.0.0.1:3000"));
let client = Client::new(&policy, &hosts).await
    .expect("Failed to connect to cluster");
```

#### TLS connection without client authentication

Connect to an Aerospike cluster with TLS but without client certificate authentication:

```rust
use aerospike::{Client, ClientPolicy};
use rustls::RootCertStore;
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
policy.tls_config = Some(tls_config_no_client_auth("/path/to/ca-cert.pem"));

let hosts = "tls-cluster.example.com:4333";
let client = Client::new(&policy, hosts).await
    .expect("Failed to connect to cluster");
```

#### TLS connection with client authentication

Connect to an Aerospike cluster with TLS and mutual authentication using client certificates:

```rust
use aerospike::{Client, ClientPolicy};
use rustls::RootCertStore;
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
policy.tls_config = Some(tls_config_with_client_auth(
    "/path/to/ca-cert.pem",
    "/path/to/client-cert.pem",
    "/path/to/client-key.pem",
));

let hosts = "tls-cluster.example.com:4333";
let client = Client::new(&policy, hosts).await
    .expect("Failed to connect to cluster");
```

**Note**: To use TLS features, enable the `tls` feature in your `Cargo.toml`:

```toml
[dependencies]
aerospike = { version = "...", features = ["tls"] }
```

### CRUD operations

```rust
#[macro_use]
extern crate aerospike;
extern crate tokio;

use std::env;
use std::time::Instant;

use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
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

```rust
    let mut bpolicy = BatchPolicy::default();
    let apolicy = AdminPolicy::default();

    let udf_body = r#"
	function echo(rec, val)
  		return val
	end
	"#;

    let task = client
        .register_udf(&apolicy, udf_body.as_bytes(), "test_udf.lua", UDFLang::Lua)
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
    let bpu = BatchUDFPolicy::default();

    let batch = vec![
        BatchOperation::write(&bpw, key1.clone(), wops.clone()),
        BatchOperation::write(&bpw, key2.clone(), wops.clone()),
        BatchOperation::write(&bpw, key3.clone(), wops.clone()),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    dbg!(&results);

    // READ Operations
    let batch = vec![
        BatchOperation::read(&bpr, key1.clone(), selected),
        BatchOperation::read(&bpr, key2.clone(), all),
        BatchOperation::read(&bpr, key3.clone(), none.clone()),
        BatchOperation::read_ops(&bpr, key3.clone(), rops),
        BatchOperation::read(&bpr, key4.clone(), none),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    dbg!(&results);

    // DELETE Operations
    let batch = vec![
        BatchOperation::delete(&bpd, key1.clone()),
        BatchOperation::delete(&bpd, key2.clone()),
        BatchOperation::delete(&bpd, key3.clone()),
        BatchOperation::delete(&bpd, key4.clone()),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    dbg!(&results);

    // Read
    let args1 = &[as_val!(1)];
    let args2 = &[as_val!(2)];
    let args3 = &[as_val!(3)];
    let args4 = &[as_val!(4)];
    let batch = vec![
        BatchOperation::udf(&bpu, key1.clone(), "test_udf", "echo", Some(args1)),
        BatchOperation::udf(&bpu, key2.clone(), "test_udf", "echo", Some(args2)),
        BatchOperation::udf(&bpu, key3.clone(), "test_udf", "echo", Some(args3)),
        BatchOperation::udf(&bpu, key4.clone(), "test_udf", "echo", Some(args4)),
    ];
    let mut results = client.batch(&bpolicy, &batch).await.unwrap();

    dbg!(&results);
```

A complete working example can be found in [examples/batch_operations.rs](./examples/batch_operations.rs).

### Query operations

The Rust client supports various query patterns for retrieving data from Aerospike. Below are examples demonstrating different query capabilities.

#### Simple equality query

Query records where a bin equals a specific value:

```rust
use aerospike::{QueryPolicy, Statement, Bins};
use aerospike::query::PartitionFilter;

let policy = QueryPolicy::default();
let mut stmt = Statement::new(namespace, set_name, Bins::All);
stmt.add_filter(as_eq!("bin_name", 5));

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
stmt.add_filter(as_range!("bin_name", 0, 100));

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
stmt.add_filter(as_range!("bin_name", 0, 100));

let rs = client.query(&policy, PartitionFilter::all(), stmt).await.unwrap();
let mut rs = rs.into_stream();

while let Some(r) = rs.next().await {
    let rec = r.unwrap();
    println!("Generation: {}, TTL: {}", rec.generation, rec.expiration);
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
    pf = rs.partition_filter().await.unwrap();
}
```

#### Parallel query with multiple consumers

Process query results in parallel using multiple async tasks:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

let policy = QueryPolicy::default();
let mut stmt = Statement::new(namespace, set_name, Bins::All);
stmt.add_filter(as_range!("bin_name", 0, 100));

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
use aerospike_core::expressions::{eq, int_bin, int_val};

let mut policy = QueryPolicy::default();
policy.base_policy.filter_expression.replace(
    eq(int_bin("bin_name".to_string()), int_val(42))
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
stmt.add_filter(as_range!("bin_name", 0, 1000));

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
use aerospike_core::{AdminPolicy, IndexType, CollectionIndexType};

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

For a complete working example with all query patterns, see [`examples/query.rs`](./examples/query.rs).

### Timeout configuration

The Rust client provides flexible timeout configuration through `socket_timeout` and `total_timeout` parameters in policies. Understanding how these interact is crucial for handling network issues and controlling command execution time.

#### Timeout parameters

- **`socket_timeout`**: Socket idle timeout when processing a database command (in milliseconds). Default value 5000 (5 seconds).
- **`total_timeout`**: Total command timeout, including retries (in milliseconds). Default value 0.

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

For a complete working example demonstrating timeout scenarios, see [`examples/timeout_configuration.rs`](./examples/timeout_configuration.rs).
## Feedback wanted

We need your help with:

- Real-world async patterns in your codebase
- Ergonomic pain points in API design

You’re not just testing this new client - you’re shaping the future of Rust in databases!

You can reach us through [Github Issues](https://github.com/aerospike/aerospike-client-rust/issues)
or schedule a meeting to speak directly with our product team using
[this scheduling link](https://calendar.app.google/sDseJu6vUg8da5Kw5).