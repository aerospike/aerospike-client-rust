# Aerospike Rust Client [![crates-io][crates-io-image]][crates-io-url] [![docs][docs-image]][docs-url] [![travis][travis-image]][travis-url] [![appveyor][appveyor-image]][appveyor-url]

[crates-io-image]: https://img.shields.io/crates/v/aerospike.svg
[crates-io-url]: https://crates.io/crates/aerospike
[docs-image]: https://docs.rs/aerospike/badge.svg
[docs-url]: https://docs.rs/aerospike/
[travis-image]: https://travis-ci.org/aerospike/aerospike-client-rust.svg?branch=master
[travis-url]: https://travis-ci.org/aerospike/aerospike-client-rust
[appveyor-image]: https://ci.appveyor.com/api/projects/status/e9gx1b5d1307hj2t/branch/master?svg=true
[appveyor-url]: https://ci.appveyor.com/project/aerospike/aerospike-client-rust/branch/master

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

- [Aerospike Database}(https://aerospike.com/download/server/community/) 6.4 or later.
- [Rust](https://www.rust-lang.org/) version 1.63 or later 
- [Tokio runtime](https://tokio.rs/) or [async-std](https://async.rs/)

## Installation

1. Build from source code:

   ```
   git clone --single-branch --branch v2 https://github.com/aerospike/aerospike-client-rust.git
   cd aerospike-client-rust
   ```

1. Add the following to your `cargo.toml` file:

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

1. Run the following command:

   ```
   cargo build
   ```

## Core feature examples

The following code examples demonstrate some of the Rust client's new
features.

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

    let exists = client.exists(&wpolicy, &key).await.unwrap();
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

### Batch updates

```rust
// update 50k user profiles

let keys = user_ids.iter().map(|id| as_key!("test", "users", id));  
let ops = vec![  
    operations::Write::new("last_login", DateTime::now().into()),  
    operations::Increment::new("login_count", 1)  
];  
client.batch_operate(  
    &BatchPolicy::default(),  
    keys,  
    &ops  
).await.expect("Batch update failed");
```

## Feedback wanted

We need your help with:

- Real-world async patterns in your codebase
- Ergonomic pain points in API design

You’re not just testing this new client - you’re shaping the future of Rust in databases!

You can reach us through [Github Issues](https://github.com/aerospike/aerospike-client-rust/issues)
or schedule a meeting to speak directly with our product team using
[this scheduling link](https://calendar.app.google/sDseJu6vUg8da5Kw5).