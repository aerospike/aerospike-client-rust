# Aerospike Rust Client [![crates-io][crates-io-image]][crates-io-url] [![docs][docs-image]][docs-url] [![travis][travis-image]][travis-url]

[crates-io-image]: https://img.shields.io/crates/v/aerospike.svg
[crates-io-url]: https://crates.io/crates/aerospike
[docs-image]: https://docs.rs/aerospike/badge.svg
[docs-url]: https://docs.rs/aerospike/
[travis-image]: https://travis-ci.org/aerospike/aerospike-client-rust.svg?branch=master
[travis-url]: https://travis-ci.org/aerospike/aerospike-client-rust

An [Aerospike](https://www.aerospike.com/) client library for Rust.

> Notice: This is a work in progress. Use with discretion. Feedback, bug reports and pull requests are welcome!

This library is compatible with Rust v1.0+ and supports the following operating systems: Linux, Mac OS X (Windows builds are possible, but untested)

- [Usage](#Usage)
- [Known Limitations](#Limitations)
- [Tests](#Tests)
- [Benchmarks](#Benchmarks)


<a name="Usage"></a>
## Usage:

The following is a very simple example of CRUD operations in an Aerospike database.

```rust
#[macro_use]
extern crate aerospike;

use std::env;
use std::time::Instant;

use aerospike::{Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
use aerospike::operations;

fn main() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS")
        .unwrap_or(String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .expect("Failed to connect to cluster");

    let now = Instant::now();
    let rpolicy = ReadPolicy::default();
    let wpolicy = WritePolicy::default();
    let key = as_key!("test", "test", "test");
    let wbin = as_bin!("int", 999);
    let bins = vec![&wbin];

    client.put(&wpolicy, &key, &bins).unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All);
    println!("Record: {}", rec.unwrap());

    client.touch(&wpolicy, &key).unwrap();
    let rec = client.get(&rpolicy, &key, Bins::All);
    println!("Record: {}", rec.unwrap());

    let rec = client.get(&rpolicy, &key, Bins::None);
    println!("Record Header: {}", rec.unwrap());

    let exists = client.exists(&wpolicy, &key).unwrap();
    println!("exists: {}", exists);

    let ops = &vec![operations::put(&wbin), operations::get()];
    let op_rec = client.operate(&wpolicy, &key, ops);
    println!("operate: {}", op_rec.unwrap());

    let existed = client.delete(&wpolicy, &key).unwrap();
    println!("existed (sould be true): {}", existed);

    let existed = client.delete(&wpolicy, &key).unwrap();
    println!("existed (should be false): {}", existed);

    println!("total time: {:?}", now.elapsed());
}
```

<a name="Limitations"></a>
## Known Limitations

The client currently supports all single-key operations supported by Aerospike,
incl. the operate command with full support of List and (Sorted) Map
operations. The client also supports scan and query operations incl. support
for User-Defined Functions in the Lua scripting language, as well as APIs
to manage secondary indexes. For Aerospike Enterprise edition deployments the
client supports managing users and roles.

However the following features are not yet supported in the Aerospike Rust
client:

- Query Aggregation using Lua User-Defined Functions (which requires
  integrating the Lua run-time environment into the client)
- Async Task operations (like execute UDF on scan/queries, index drop/create
  operations, etc.)
- Secure connections using TLS (requires AS 3.10+)
- IPv6 support

<a name="Tests"></a>
## Tests

This library is packaged with a number of tests. The tests assume that an
Aerospike cluster is running at `localhost:3000`. To test using a cluster at a
different address, set the `AEROSPIKE_HOSTS` environment variable to the list
of cluster hosts.

To run all the test cases:

```shell
$ export AEROSPIKE_HOSTS=127.0.0.1:3000
$ cargo test
``

To enable debug logging for the `aerospike` crate:

```shell
$ RUST_LOG=aerospike=debug cargo test
```

To enable backtraces set the `RUST_BACKTRACE` environment variable:

```shell
$ RUST_BACKTRACE=1 cargo test
```

<a name="Benchmarks"></a>
## Benchmarks

The micro-benchmarks in the `benches` directory use the
[`bencher`](https://crates.io/crates/bencher) crate and can be run on Rust
stable releases:

```shell
$ export AEROSPIKE_HOSTS=127.0.0.1:3000
$ cargo bench
```

There is a separate benchmark tool under the
[tools/benchmark](tools/benchmark) directory that is designed to
insert data into an Aerospike server cluster and generate load.
