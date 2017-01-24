# Aerospike Rust Client [![travis][travis-image]][travis-url]

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

use aerospike::*;
use std::sync::Arc;
use std::time::Instant;
use std::thread;

fn main() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS")
        .unwrap_or(String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .expect("Failed to connect to cluster");
    let client = Arc::new(client);

    let mut threads = vec![];
    let now = Instant::now();
    for i in 0..2 {
        let client = client.clone();
        let t = thread::spawn(move || {
            let rpolicy = ReadPolicy::default();
            let wpolicy = WritePolicy::default();
            let key = as_key!("test", "test", i);
            let wbin = as_bin!("bin999", 1);
            let bins = vec![&wbin];

            client.put(&wpolicy, &key, &bins).unwrap();
            let rec = client.get(&rpolicy, &key, None);
            println!("Record: {}", rec.unwrap());

            client.touch(&wpolicy, &key).unwrap();
            let rec = client.get(&rpolicy, &key, None);
            println!("Record: {}", rec.unwrap());

            let rec = client.get_header(&rpolicy, &key);
            println!("Record Header: {}", rec.unwrap());

            let exists = client.exists(&wpolicy, &key).unwrap();
            println!("exists: {}", exists);

            let ops = &vec![Operation::put(&wbin), Operation::get()];
            let op_rec = client.operate(&wpolicy, &key, ops);
            println!("operate: {}", op_rec.unwrap());

            let existed = client.delete(&wpolicy, &key).unwrap();
            println!("existed (sould be true): {}", existed);

            let existed = client.delete(&wpolicy, &key).unwrap();
            println!("existed (should be false): {}", existed);
        });

        threads.push(t);
    }

    for t in threads {
        t.join();
    }

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

- Batch requests
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

    $ export AEROSPIKE_HOSTS=127.0.0.1:3000
    $ cargo test

To enable debug logging for the `aerospike` crate:

    $ RUST_LOG=aerospike=debug cargo test

To enable backtraces:

    $ RUST_LOG=aerospike=debug RUST_BACKTRACE=1 cargo test

<a name="Benchmarks"></a>
## Benchmarks

The micro-benchmarks in the `benches` directory require nightly Rust builds to execute:

    $ export AEROSPIKE_HOSTS=127.0.0.1:3000
    $ rustup run nightly cargo bench
