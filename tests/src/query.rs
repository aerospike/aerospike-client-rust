// Copyright 2015-2016 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

extern crate aerospike;
extern crate env_logger;

use std::sync::atomic::{AtomicUsize, Ordering};

use aerospike::*;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common1;

const EXPECTED: usize = 1000;

#[test]
fn query_single_consumer() {

    let _ = env_logger::init();

    let ref client = common1::GLOBAL_CLIENT;
    let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
    let set_name = &common1::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..EXPECTED as i64 {
        let key = as_key!(namespace, set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![&wbin];
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    // create an index
    client.create_index(&wpolicy,
                        namespace,
                        set_name,
                        "bin",
                        &format!("{}_{}_{}", namespace, set_name, "bin"),
                        IndexType::Numeric)
        .expect("Failed to create index");

    // FIXME: replace sleep with wait task
    thread::sleep(Duration::from_millis(3000));

    let qpolicy = QueryPolicy::default();
    // let node = client.cluster.get_random_node().unwrap();

    let mut statement = Statement::new(namespace, set_name, None).unwrap();
    statement.add_filter(as_eq!("bin", 1));

    let rs = client.query(&qpolicy, Arc::new(statement)).unwrap();
    let mut count = 0;
    for res in rs.iter() {
        match res {
            Ok(rec) => {
                assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(1));
                // println!("Record: {}", rec);
                count += 1;
            }
            Err(err) => panic!(format!("{:?}", err)),
        }
    }

    assert_eq!(count, 1);

    // Range Query
    let mut statement = Statement::new(namespace, set_name, None).unwrap();
    let f = as_range!("bin", 0, 9);
    statement.add_filter(f);

    let rs = client.query(&qpolicy, Arc::new(statement)).unwrap();
    let mut count = 0;
    for res in rs.iter() {
        match res {
            Ok(rec) => {
                // println!("Record: {}", rec);
                count += 1;
                let v = i64::from(rec.bins.get("bin").unwrap());

                assert!(v >= 0);
                assert!(v < 10);

            }
            Err(err) => panic!(format!("{:?}", err)),
        }
    }

    assert_eq!(count, 10);
}

#[test]
fn query_multi_consumer() {

let _ = env_logger::init();


let ref client = common1::GLOBAL_CLIENT;

    let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
    let set_name = &common1::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..EXPECTED as i64 {
        let key = as_key!(namespace, set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![&wbin];
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    // create an index
    client.create_index(&wpolicy,
                        namespace,
                        set_name,
                        "bin",
                        &format!("{}_{}_{}", namespace, set_name, "bin"),
                        IndexType::Numeric)
        .expect("Failed to create index");

    // FIXME: replace sleep with wait task
    thread::sleep(Duration::from_millis(3000));

    let qpolicy = QueryPolicy::default();

    // Range Query
    let mut statement = Statement::new(namespace, set_name, None).unwrap();
    let f = as_range!("bin", 0, 9);
    statement.add_filter(f);

    let rs = client.query(&qpolicy, Arc::new(statement)).unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for _ in 0..8 {
        let count = count.clone();
        let rs = rs.clone();
        threads.push(thread::spawn(move || {
            for res in rs.iter() {
                match res {
                    Ok(rec) => {
                        count.fetch_add(1, Ordering::Relaxed);
                        let v = i64::from(rec.bins.get("bin").unwrap());

                        assert!(v >= 0);
                        assert!(v < 10);
                    }
                    Err(err) => panic!(format!("{:?}", err)),
                }
            }
        }));
    }

    for t in threads {
        t.join()
            .expect("Failed to join threads");
    }

    assert_eq!(count.load(Ordering::Relaxed), 10);
}
