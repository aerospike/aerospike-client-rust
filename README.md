# Aerospike Rust Client [![travis][travis-image]][travis-url]

[travis-image]: https://travis-ci.org/aerospike/aerospike-client-rust.svg?branch=master
[travis-url]: https://travis-ci.org/aerospike/aerospike-client-rust

An [Aerospike](https://www.aerospike.com/) client library for Rust.

> Notice: This is a work in progress. Use with discretion. Feedback, bug reports and pull requests are welcome!

This library is compatible with Rust v1.0+ and supports the following operating systems: Linux, Mac OS X (Windows builds are possible, but untested)

- [Usage](#Usage)
- [Tests](#Tests)


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

<a name="Tests"></a>
## Tests

This library is packaged with a number of tests.

To run all the test cases:

    $ AEROSPIKE_HOSTS=127.0.0.1:3000 RUST_LOG=debug:aerospike RUST_BACKTRACE=1 cargo test -- --nocapture
