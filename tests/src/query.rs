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

use futures::stream::StreamExt;

use crate::common;

use aerospike::query::PartitionFilter;
use aerospike::Task;
use aerospike::*;
use aerospike_rt::time::{Duration, Instant};

const EXPECTED: usize = 1000;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin1 = as_bin!("bin", i);
        let wbin2 = as_bin!("bin2", "hello");
        let wbin3 = as_bin!("extra", "extra");
        let bins = vec![wbin1, wbin2, wbin3];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            &set_name,
            "bin",
            &format!("{}_{}_{}", namespace, set_name, "bin"),
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create index");
    task.wait_till_complete(None).await.unwrap();

    set_name
}

#[aerospike_macro::test]
async fn query_timeout() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let mut qpolicy = QueryPolicy::default();
    qpolicy.base_policy.total_timeout = 5;
    qpolicy.base_policy.socket_timeout = 5;

    // Filter Query
    let statement = Statement::new(namespace, &set_name, Bins::All);
    let pf = PartitionFilter::all();

    let start = Instant::now();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut rs = rs.into_stream();
    let mut timed_out = false;
    while let Some(res) = rs.next().await {
        match res {
            Ok(_) => (),
            Err(Error::Timeout(_)) => timed_out = true,
            Err(err) => panic!("{:?}", err),
        }
    }
    let duration = start.elapsed();

    let expected_duration = Duration::from_millis((qpolicy.total_timeout() * 2) as u64);
    assert!(duration < expected_duration);
    assert_eq!(timed_out, true);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_single_consumer_no_setname() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = "";
    let mut qpolicy = QueryPolicy::default();
    qpolicy.expected_duration = QueryDuration::Short;

    // Filter Query
    let statement = Statement::new(namespace, &set_name, Bins::All);
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(_) => {
                count += 1;
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert!(count > 0);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_single_consumer() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let mut qpolicy = QueryPolicy::default();
    qpolicy.expected_duration = QueryDuration::Short;

    // Filter Query
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(as_eq!("bin", 1));
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
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
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
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
async fn query_single_consumer_with_cursor() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let mut qpolicy = QueryPolicy::default();
    qpolicy.expected_duration = QueryDuration::Short;

    let mut pf = PartitionFilter::all();
    let mut count = 0;
    while !pf.done() {
        // Filter Query
        let mut statement = Statement::new(namespace, &set_name, Bins::All);
        statement.add_filter(as_eq!("bin", 1));
        let rs = client.query(&qpolicy, pf, statement).await.unwrap();
        let mut rs = rs.into_stream();
        while let Some(res) = rs.next().await {
            match res {
                Ok(rec) => {
                    assert_eq!(rec.bins["bin"], as_val!(1));
                    count += 1;
                }
                Err(err) => panic!("{:?}", err),
            }
        }
        pf = rs.partition_filter().await.unwrap();
    }
    assert_eq!(count, 1);

    let mut pf = PartitionFilter::all();
    let mut iter = 0;
    count = 0;
    qpolicy.max_records = 1;
    while !pf.done() {
        iter += 1;
        // Range Query
        let mut statement = Statement::new(namespace, &set_name, Bins::Some(vec!["bin".into()]));
        statement.add_filter(as_range!("bin", 0, 9));
        let rs = client.query(&qpolicy, pf, statement).await.unwrap();
        let mut rs = rs.into_stream();
        while let Some(res) = rs.next().await {
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
        pf = rs.partition_filter().await.unwrap();
    }
    assert_eq!(count, 10);
    assert_eq!(iter, 11);

    let mut pf = PartitionFilter::all();
    qpolicy.max_records = (EXPECTED / 3) as u64;
    iter = 0;
    count = 0;
    while !pf.done() {
        iter += 1;
        // Range Query
        let statement = Statement::new(namespace, &set_name, Bins::Some(vec!["bin".into()]));
        let rs = client.query(&qpolicy, pf, statement).await.unwrap();
        let mut rs = rs.into_stream();
        while let Some(res) = rs.next().await {
            match res {
                Ok(rec) => {
                    count += 1;
                    rec.bins.get("bin").unwrap(); // must exists
                }
                Err(err) => panic!("{:?}", err),
            }
        }
        pf = rs.partition_filter().await.unwrap();
    }
    assert_eq!(count, EXPECTED);
    assert_eq!(iter, 4);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_single_consumer_rps() {
    let client = common::client().await;

    // only run on single node clusters
    if client.nodes().len() != 1 {
        return;
    }

    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let mut qpolicy = QueryPolicy::default();

    // Range Query
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(as_range!("bin", 0, (EXPECTED / 3) as i64));

    qpolicy.records_per_second = 3;
    let start_time = Instant::now();
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                let v: i64 = rec.bins["bin"].clone().into();
                assert!(v >= 0);
                assert!(v <= (EXPECTED / 3) as i64);
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, EXPECTED / 3 + 1);

    // Should take at least 3 seconds due to rps
    let duration = Instant::now() - start_time;
    assert!(duration.as_millis() > 3000);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_nobins() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    let mut statement = Statement::new(namespace, &set_name, Bins::None);
    statement.add_filter(as_range!("bin", 0, 9));
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
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
async fn query_some_bins() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    let mut statement = Statement::new(namespace, &set_name, Bins::Some(vec!["bin".into()]));
    statement.add_filter(as_range!("bin", 0, 9));
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                assert!(rec.generation > 0);
                assert_eq!(1, rec.bins.len());
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 10);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_multi_consumer() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    // Range Query
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    let f = as_range!("bin", 0, 9);
    statement.add_filter(f);

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();

    let count = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    for _ in 0..8 {
        let count = count.clone();
        let rs = rs.clone();
        threads.push(aerospike_rt::spawn(async move {
            let mut rs = rs.into_stream();
            while let Some(res) = rs.next().await {
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

    futures::future::join_all(threads).await;

    assert_eq!(count.load(Ordering::Relaxed), 10);

    client.close().await.unwrap();
}

// https://github.com/aerospike/aerospike-client-rust/issues/115
#[aerospike_macro::test]
async fn query_large_i64() {
    const SET: &str = "large_i64";
    const BIN: &str = "val";

    let client = Arc::new(common::client().await);
    let value = Value::from(i64::max_value());
    let key = Key::new(common::namespace(), SET, value.clone()).unwrap();
    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();

    let res = client
        .put(&wpolicy, &key, &[aerospike::Bin::new(BIN.into(), value)])
        .await;

    assert!(res.is_ok());

    let mut qpolicy = aerospike::QueryPolicy::new();
    let bin_name = aerospike::expressions::int_bin(BIN.into());
    let bin_val = aerospike::expressions::int_val(i64::max_value());
    qpolicy
        .base_policy
        .filter_expression
        .replace(aerospike::expressions::eq(bin_name, bin_val));
    let stmt = aerospike::Statement::new(common::namespace(), SET, aerospike::Bins::All);
    let pf = PartitionFilter::all();
    let recordset = client.query(&qpolicy, pf, stmt).await.unwrap();

    let mut recordset = recordset.into_stream();
    while let Some(r) = recordset.next().await {
        assert!(r.is_ok());
        let int = r.unwrap().bins.remove(BIN).unwrap();
        assert_eq!(int, Value::Int(i64::max_value()));
    }

    let _ = client.truncate(&apolicy, common::namespace(), SET, 0).await;
}

#[aerospike_macro::test]
async fn test_query_geo_within_geojson_region() {
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let bin_name = "geo_bin";

    let client = Arc::new(common::client().await);
    let apolicy = AdminPolicy::default();

    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            set_name,
            bin_name,
            &format!("{}_{}_{}", namespace, set_name, bin_name),
            IndexType::Geo2DSphere,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create index");
    task.wait_till_complete(None).await.unwrap();

    let wp = WritePolicy::default();

    // Records inside the polygon
    let key1 = as_key!(namespace, set_name, "point1");
    client
        .put(
            &wp,
            &key1,
            &vec![as_bin!(
                bin_name,
                as_geo!(r#"{"type": "Point", "coordinates": [-122.0, 37.5]}"#)
            )],
        )
        .await
        .unwrap();

    let key2 = as_key!(namespace, set_name, "point2");
    client
        .put(
            &wp,
            &key2,
            &vec![as_bin!(
                bin_name,
                as_geo!(r#"{"type": "Point", "coordinates": [-121.5, 37.5]}"#)
            )],
        )
        .await
        .unwrap();

    // Record outside the polygon
    let key3 = as_key!(namespace, set_name, "point3");
    client
        .put(
            &wp,
            &key3,
            &vec![as_bin!(
                bin_name,
                as_geo!(r#"{"type": "Point", "coordinates": [-120.0, 37.5]}"#)
            )],
        )
        .await
        .unwrap();

    let region_str = r#"{
            "type": "Polygon",
            "coordinates": [[[-122.500000, 37.000000],
                             [-121.000000, 37.000000],
                             [-121.000000, 38.080000],
                             [-122.500000, 38.080000],
                             [-122.500000, 37.000000]]]
        }"#;

    let predicate = as_within_region!(bin_name, region_str);

    let qpolicy = QueryPolicy::default();
    let mut stmt = aerospike::Statement::new(namespace, set_name, aerospike::Bins::All);
    stmt.add_filter(predicate);
    let pf = PartitionFilter::all();
    let mut rs = client
        .query(&qpolicy, pf, stmt)
        .await
        .unwrap()
        .into_stream();

    let mut count = 0;
    while let Some(r) = rs.next().await {
        assert!(r.is_ok());
        count += 1;
    }

    assert!(count == 2);

    let _ = client.truncate(&apolicy, namespace, set_name, 0).await;
}

#[aerospike_macro::test]
async fn query_operate_write() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let wpolicy = WritePolicy::default();

    // Use query_operate to add 100 to every record's "bin" value in range [0, 99]
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(as_range!("bin", 0, 99));
    let ops = vec![operations::add(&as_bin!("bin", 100))];
    let task = client
        .query_operate(&wpolicy, statement, &ops)
        .await
        .expect("query_operate failed");
    task.wait_till_complete(Some(Duration::from_secs(30)))
        .await
        .expect("task did not complete");

    // Verify the records were updated
    let rpolicy = ReadPolicy::default();
    for i in 0..100_i64 {
        let key = as_key!(namespace, &set_name, i);
        let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
        let val: i64 = rec.bins["bin"].clone().into();
        assert_eq!(val, i + 100, "record {i} was not updated correctly");
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_operate_scan_all() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 50).await;

    let wpolicy = WritePolicy::default();

    // Use query_operate without filter (scan mode) to set a new bin on all records
    let statement = Statement::new(namespace, &set_name, Bins::All);
    let ops = vec![operations::put(&as_bin!("new_bin", 999))];
    let task = client
        .query_operate(&wpolicy, statement, &ops)
        .await
        .expect("query_operate scan failed");
    task.wait_till_complete(Some(Duration::from_secs(30)))
        .await
        .expect("task did not complete");

    // Verify all records have the new bin
    let rpolicy = ReadPolicy::default();
    for i in 0..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
        let val: i64 = rec.bins["new_bin"].clone().into();
        assert_eq!(val, 999, "record {i} missing new_bin");
    }

    client.close().await.unwrap();
}
