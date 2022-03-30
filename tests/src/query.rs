// Copyright 2015-2020 Aerospike, Inc.
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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use crate::common;
use env_logger;

use aerospike::Task;
use aerospike::*;

const EXPECTED: usize = 1000;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![wbin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    let task = client
        .create_index(
            namespace,
            &set_name,
            "bin",
            &format!("{}_{}_{}", namespace, set_name, "bin"),
            IndexType::Numeric,
        )
        .await
        .expect("Failed to create index");
    task.wait_till_complete(None).await.unwrap();

    set_name
}

#[aerospike_macro::test]
async fn query_single_consumer() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    // Filter Query
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(as_eq!("bin", 1));
    let rs = client.query(&qpolicy, statement).await.unwrap();
    let mut count = 0;
    for res in &*rs {
        match res {
            Ok(rec) => {
                assert_eq!(rec.bins["bin"], as_val!(1));
                count += 1;
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 1);

    // Range Query
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(as_range!("bin", 0, 9));
    let rs = client.query(&qpolicy, statement).await.unwrap();
    let mut count = 0;
    for res in &*rs {
        match res {
            Ok(rec) => {
                count += 1;
                let v: i64 = rec.bins["bin"].clone().into();
                assert!(v >= 0);
                assert!(v < 10);
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 10);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_nobins() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    let mut statement = Statement::new(namespace, &set_name, Bins::None);
    statement.add_filter(as_range!("bin", 0, 9));
    let rs = client.query(&qpolicy, statement).await.unwrap();
    let mut count = 0;
    for res in &*rs {
        match res {
            Ok(rec) => {
                count += 1;
                assert!(rec.generation > 0);
                assert_eq!(0, rec.bins.len());
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 10);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_multi_consumer() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    // Range Query
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    let f = as_range!("bin", 0, 9);
    statement.add_filter(f);
    let rs = client.query(&qpolicy, statement).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for _ in 0..8 {
        let count = count.clone();
        let rs = rs.clone();
        threads.push(thread::spawn(move || {
            for res in &*rs {
                match res {
                    Ok(rec) => {
                        count.fetch_add(1, Ordering::Relaxed);
                        let v: i64 = rec.bins["bin"].clone().into();
                        assert!(v >= 0);
                        assert!(v < 10);
                    }
                    Err(err) => panic!("{:?}", err),
                }
            }
        }));
    }

    for t in threads {
        t.join().expect("Failed to join threads");
    }

    assert_eq!(count.load(Ordering::Relaxed), 10);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_node() {
    let _ = env_logger::try_init();

    let client = Arc::new(common::client().await);
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for node in client.nodes().await {
        let client = client.clone();
        let count = count.clone();
        let set_name = set_name.clone();
        threads.push(aerospike_rt::spawn(async move {
            let qpolicy = QueryPolicy::default();
            let mut statement = Statement::new(namespace, &set_name, Bins::All);
            statement.add_filter(as_range!("bin", 0, 99));
            let rs = client.query_node(&qpolicy, node, statement).await.unwrap();
            let ok = (&*rs).filter(Result::is_ok).count();
            count.fetch_add(ok, Ordering::Relaxed);
        }));
    }

    for t in threads {
        t.await;
    }

    assert_eq!(count.load(Ordering::Relaxed), 100);

    client.close().await.unwrap();
}
