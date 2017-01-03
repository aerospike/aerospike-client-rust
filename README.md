# Aerospike Rust Client

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
    let client: Arc<Client> = Arc::new(Client::new(&cpolicy, &vec![Host::new("127.0.0.1", 3000)]).unwrap());

    let mut threads = vec![];
    let now = Instant::now();
    for i in 0..2 {
        let client = client.clone();
        let t = thread::spawn(move || {
            let policy = ReadPolicy::default();
            let wpolicy = WritePolicy::default();
            let key = as_key!("test", "test", i);
            let wbin = as_bin!("bin999", 1);
            let bins = vec![&wbin];

            client.put(&wpolicy, &key, &bins).unwrap();
            let rec = client.get(&policy, &key, None);
            println!("Record: {}", rec.unwrap());

            client.touch(&wpolicy, &key).unwrap();
            let rec = client.get(&policy, &key, None);
            println!("Record: {}", rec.unwrap());

            let rec = client.get_header(&policy, &key);
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

`$ AEROSPIKE_HOST=host AEROSPIKE_PORT=3000 AEROSPIKE_NAMESPACE=test RUST_LOG=debug:aerospike RUST_BACKTRACE=1 cargo test -- --nocapture`
