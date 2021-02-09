# Aerospike Rust Client [![crates-io][crates-io-image]][crates-io-url] [![docs][docs-image]][docs-url] [![travis][travis-image]][travis-url] [![appveyor][appveyor-image]][appveyor-url]

[crates-io-image]: https://img.shields.io/crates/v/aerospike.svg
[crates-io-url]: https://crates.io/crates/aerospike
[docs-image]: https://docs.rs/aerospike/badge.svg
[docs-url]: https://docs.rs/aerospike/
[travis-image]: https://travis-ci.org/aerospike/aerospike-client-rust.svg?branch=master
[travis-url]: https://travis-ci.org/aerospike/aerospike-client-rust
[appveyor-image]: https://ci.appveyor.com/api/projects/status/e9gx1b5d1307hj2t/branch/master?svg=true
[appveyor-url]: https://ci.appveyor.com/project/aerospike/aerospike-client-rust/branch/master

An [Aerospike](https://www.aerospike.com/) client library for Rust.

This library is compatible with Rust 1.46+ and supports the following operating systems: Linux, Mac OS X, and Windows.

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

    let bins = [
        as_bin!("int", 999),
        as_bin!("str", "Hello, World!"),
    ];
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

    let bin = as_bin!("int", "123");
    let ops = &vec![operations::put(&bin), operations::get()];
    let op_rec = client.operate(&wpolicy, &key, ops);
    println!("operate: {}", op_rec.unwrap());

    let existed = client.delete(&wpolicy, &key).unwrap();
    println!("existed (should be true): {}", existed);

    let existed = client.delete(&wpolicy, &key).unwrap();
    println!("existed (should be false): {}", existed);

    println!("total time: {:?}", now.elapsed());
}
```

<a name="Limitations"></a>
## Known Limitations

The following features are not yet supported in the Aerospike Rust client:

- Query Aggregation using Lua User-Defined Functions (UDF).
- Secure connections using TLS.
- IPv6 support.

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
```

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
