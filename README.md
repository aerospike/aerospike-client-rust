# Aerospike Rust Client

An Aerospike library for Rust.

This library is compatible with Rust v1.0+ and supports the following operating systems: Linux, Mac OS X (Windows builds are possible, but untested)

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Usage](#Usage)
- [Prerequisites](#Prerequisites)
- [Installation](#Installation)
- [Tweaking Performance](#Performance)
- [Benchmarks](#Benchmarks)
- [API Documentaion](#API-Documentation)
- [Tests](#Tests)
- [Examples](#Examples)
  - [Tools](#Tools)


## Usage:

The following is a very simple example of CRUD operations in an Aerospike database.

```rust
    let client: Arc<Client> = Arc::new(Client::new(&cpolicy, &vec![Host::new("127.0.0.1", 3000)]).unwrap());

    let mut threads = vec![];
    let now = Instant::now();
    for _ in 0..2 {
        let client = client.clone();
        let t = thread::spawn(move || {
            let policy = ReadPolicy::default();

            let wpolicy = WritePolicy::default();
            let key = key!("test", "test", 1);

            let wbin = bin!("bin999", 1);
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

More examples illustrating the use of the API are located in the
[`examples`](examples) directory.

<a name="Tests"></a>
## Tests

This library is packaged with a number of tests.

To run all the test cases:

`$ RUST_LOG=debug:aerospike RUST_BACKTRACE=1 cargo test -- --nocapture`
