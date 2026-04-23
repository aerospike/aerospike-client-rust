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

// Prefer `singleton_client` in every test: parallel libtest + per-test `client()` caused transient
// seed / I/O failures against a healthy single-node cluster.

use aerospike::query::{Filter, PartitionFilter};
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
    let client = common::singleton_client().await;
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

}

#[aerospike_macro::test]
async fn query_single_consumer_no_setname() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = "";
    let mut qpolicy = QueryPolicy::default();
    qpolicy.expected_duration = QueryDuration::Long;

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

}

#[aerospike_macro::test]
async fn query_single_consumer() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let mut qpolicy = QueryPolicy::default();
    qpolicy.expected_duration = QueryDuration::Short;

    // Filter Query
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::equal("bin", 1));
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
    statement.add_filter(Filter::range("bin", 0, 9));
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

}

#[aerospike_macro::test]
async fn query_single_consumer_with_cursor() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let mut qpolicy = QueryPolicy::default();
    qpolicy.expected_duration = QueryDuration::Short;

    let mut pf = PartitionFilter::all();
    let mut count = 0;
    while !pf.done() {
        // Filter Query
        let mut statement = Statement::new(namespace, &set_name, Bins::All);
        statement.add_filter(Filter::equal("bin", 1));
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
        statement.add_filter(Filter::range("bin", 0, 9));
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

}

#[aerospike_macro::test]
async fn query_single_consumer_rps() {
    let client = common::singleton_client().await;

    // only run on single node clusters
    if client.nodes().len() != 1 {
        return;
    }

    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let mut qpolicy = QueryPolicy::default();

    // Range Query
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::range("bin", 0, (EXPECTED / 3) as i64));

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

}

#[aerospike_macro::test]
async fn query_nobins() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    let mut statement = Statement::new(namespace, &set_name, Bins::None);
    statement.add_filter(Filter::range("bin", 0, 9));
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

}

#[aerospike_macro::test]
async fn query_some_bins() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    let mut statement = Statement::new(namespace, &set_name, Bins::Some(vec!["bin".into()]));
    statement.add_filter(Filter::range("bin", 0, 9));
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

}

#[aerospike_macro::test]
async fn query_multi_consumer() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    // Range Query
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    let f = Filter::range("bin", 0, 9);
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

}

// https://github.com/aerospike/aerospike-client-rust/issues/115
#[aerospike_macro::test]
async fn query_large_i64() {
    const SET: &str = "large_i64";
    const BIN: &str = "val";

    let client = common::singleton_client().await;
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

    let client = common::singleton_client().await;
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

    let predicate = Filter::geo_within_region(bin_name, region_str);

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

/// Query with a secondary index filter and specific bin selection.
/// This exercises the QUERY_BINLIST wire-protocol field: when a filter is
/// present, bin names are sent as a compact QUERY_BINLIST field rather than
/// individual READ operations.
#[aerospike_macro::test]
async fn query_filter_with_specific_bins() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    // Request only "bin" and "bin2" out of three bins (bin, bin2, extra)
    let mut statement = Statement::new(
        namespace,
        &set_name,
        Bins::Some(vec!["bin".into(), "bin2".into()]),
    );
    statement.add_filter(Filter::range("bin", 0, 9));

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                // Exactly the two requested bins should be present
                assert_eq!(rec.bins.len(), 2, "expected 2 bins, got {:?}", rec.bins);
                assert!(rec.bins.contains_key("bin"), "missing 'bin'");
                assert!(rec.bins.contains_key("bin2"), "missing 'bin2'");
                assert!(
                    !rec.bins.contains_key("extra"),
                    "'extra' should not be returned"
                );
                let v: i64 = rec.bins["bin"].clone().into();
                assert!(v >= 0 && v < 10);
                assert_eq!(rec.bins["bin2"], as_val!("hello"));
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 10);

}

/// Query using `Filter::new_by_index` to target a specific secondary index by name.
/// The server should use the named index instead of performing an index lookup by bin name.
#[aerospike_macro::test]
async fn query_filter_with_index_name() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    // The index name created by create_test_set
    let index_name = format!("{}_{}_{}", namespace, set_name, "bin");

    let mut statement = Statement::new(
        namespace,
        &set_name,
        Bins::Some(vec!["bin".into(), "bin2".into()]),
    );
    // Use Filter::range_by_index to target the index by name
    let filter = Filter::range_by_index(&index_name, 0, 9);
    statement.add_filter(filter);

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                assert_eq!(rec.bins.len(), 2, "expected 2 bins, got {:?}", rec.bins);
                assert!(rec.bins.contains_key("bin"), "missing 'bin'");
                assert!(rec.bins.contains_key("bin2"), "missing 'bin2'");
                let v: i64 = rec.bins["bin"].clone().into();
                assert!(v >= 0 && v < 10);
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 10);

}

/// Query with `include_bin_data` set to false.
/// This exercises the `NOBINDATA` flag via the `QueryPolicy.include_bin_data` field.
/// Records should be returned with metadata but no bin data.
#[aerospike_macro::test]
async fn query_include_bin_data_false() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let mut qpolicy = QueryPolicy::default();
    qpolicy.include_bin_data = false;

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::range("bin", 0, 9));

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                assert!(rec.generation > 0);
                assert_eq!(rec.bins.len(), 0, "expected no bins, got {:?}", rec.bins);
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 10);

}

/// Scan (no filter) with specific bin selection.
/// Without a filter, bin names are encoded as READ operations rather than
/// QUERY_BINLIST. This verifies that path still works correctly.
#[aerospike_macro::test]
async fn query_scan_with_specific_bins() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 50).await;
    let qpolicy = QueryPolicy::default();

    // Scan (no filter) requesting only "bin" and "bin2"
    let statement = Statement::new(
        namespace,
        &set_name,
        Bins::Some(vec!["bin".into(), "bin2".into()]),
    );

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                assert_eq!(rec.bins.len(), 2, "expected 2 bins, got {:?}", rec.bins);
                assert!(rec.bins.contains_key("bin"), "missing 'bin'");
                assert!(rec.bins.contains_key("bin2"), "missing 'bin2'");
                assert!(
                    !rec.bins.contains_key("extra"),
                    "'extra' should not be returned"
                );
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 50);

}

/// Query using `QueryDuration::LongRelaxAP`.
/// This exercises the fix where `INFO2_RELAX_AP_LONG_QUERY` is correctly
/// written into the info2 byte instead of info1.
///
/// `LongRelaxAP` relaxes AP-only read consistency during long queries; the
/// server rejects it on strong-consistency namespaces with `ParameterError`,
/// so skip there.
#[aerospike_macro::test]
async fn query_long_relax_ap_duration() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    if common::namespace_is_sc(&client, namespace).await {
        eprintln!("Skipping query_long_relax_ap_duration: AP-only feature on SC namespace");
        return;
    }
    let set_name = create_test_set(&client, EXPECTED).await;
    let mut qpolicy = QueryPolicy::default();
    qpolicy.expected_duration = QueryDuration::LongRelaxAP;

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::range("bin", 0, 9));

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                let v: i64 = rec.bins["bin"].clone().into();
                assert!(v >= 0 && v < 10);
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 10);

}

#[aerospike_macro::test]
async fn query_operate_write() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;

    let wpolicy = WritePolicy::default();

    // Use query_operate to add 100 to every record's "bin" value in range [0, 99]
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::range("bin", 0, 99));
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

}

#[aerospike_macro::test]
async fn query_operate_scan_all() {
    let client = common::singleton_client().await;
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

}

#[aerospike_macro::test]
async fn query_operate_empty_ops_returns_parameter_error() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let statement = Statement::new(namespace, set_name, Bins::All);

    // Calling query_operate with no operations must be rejected client-side
    // with a ParameterError before any server fan-out. We verify the message
    // contains "no operations" so the assertion would not be satisfied by an
    // accidental server-returned ParameterError (server fills the node addr
    // in that field instead).
    let result = client.query_operate(&wpolicy, statement, &[]).await;

    match result {
        Err(Error::ServerError(ResultCode::ParameterError, _, ref msg))
            if msg.contains("no operations") => {}
        Err(other) => panic!(
            "expected client-side ParameterError ('query_operate called with no \
             operations'); got {:?}",
            other
        ),
        Ok(_) => panic!("expected ParameterError, got Ok"),
    }

    client.close().await.unwrap();
}

// ============================================================================
// Filter::equal_by_index — equality filter targeting a named secondary index
// ============================================================================

#[aerospike_macro::test]
async fn query_filter_equal_by_index() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, EXPECTED).await;
    let qpolicy = QueryPolicy::default();

    let index_name = format!("{}_{}_{}", namespace, set_name, "bin");

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::equal_by_index(&index_name, 5_i64));

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                let v: i64 = rec.bins["bin"].clone().into();
                assert_eq!(v, 5);
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 1);

}

// ============================================================================
// Filter::contains — list collection index
// ============================================================================

async fn create_list_test_set(client: &Client) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();

    // Each record has a "list_bin" containing a list of integers [i, i+1, i+2],
    // built via list_append_items so the server stores native CDT lists.
    let list_policy = aerospike::operations::lists::ListPolicy::default();
    for i in 0..20_i64 {
        let key = as_key!(namespace, &set_name, i);
        let ops = vec![operations::lists::append_items(
            &list_policy,
            "list_bin",
            vec![as_val!(i), as_val!(i + 1), as_val!(i + 2)],
        )];
        client.operate(&wpolicy, &key, &ops).await.unwrap();
    }

    // Create a secondary index on list elements
    let idx_name = format!("{}_{}_list_bin", namespace, set_name);
    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            &set_name,
            "list_bin",
            &idx_name,
            IndexType::Numeric,
            CollectionIndexType::List,
            None,
        )
        .await
        .expect("Failed to create list index");
    task.wait_till_complete(None).await.unwrap();

    set_name
}

#[aerospike_macro::test]
async fn query_filter_contains_list() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_list_test_set(&client).await;
    let qpolicy = QueryPolicy::default();

    // Value 1 is in record 0's list [0,1,2] and record 1's list [1,2,3]
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::contains(
        "list_bin",
        1_i64,
        CollectionIndexType::List,
    ));

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(_) => count += 1,
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 2);

}

// ============================================================================
// Filter::contains_range — list collection index with range
// ============================================================================

#[aerospike_macro::test]
async fn query_filter_contains_range_list() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_list_test_set(&client).await;
    let qpolicy = QueryPolicy::default();

    // Range [0, 1]: matches any record whose list contains a value in [0, 1]
    // record 0: [0,1,2] has 0 and 1 => match
    // record 1: [1,2,3] has 1 => match
    // record 2: [2,3,4] has neither 0 nor 1 => no match
    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::contains_range(
        "list_bin",
        0_i64,
        1_i64,
        CollectionIndexType::List,
    ));

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(_) => count += 1,
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 2);

}

// ============================================================================
// Filter::geo_within_radius — points within a radius
// ============================================================================

async fn create_geo_test_set(client: &Client) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let bin_name = "geo_bin";

    let apolicy = AdminPolicy::default();
    let wp = WritePolicy::default();

    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            &set_name,
            bin_name,
            &format!("{}_{}_{}", namespace, set_name, bin_name),
            IndexType::Geo2DSphere,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create geo index");
    task.wait_till_complete(None).await.unwrap();

    // Points near San Francisco
    let key1 = as_key!(namespace, &set_name, "close1");
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

    let key2 = as_key!(namespace, &set_name, "close2");
    client
        .put(
            &wp,
            &key2,
            &vec![as_bin!(
                bin_name,
                as_geo!(r#"{"type": "Point", "coordinates": [-122.1, 37.5]}"#)
            )],
        )
        .await
        .unwrap();

    // Point far away (New York)
    let key3 = as_key!(namespace, &set_name, "far");
    client
        .put(
            &wp,
            &key3,
            &vec![as_bin!(
                bin_name,
                as_geo!(r#"{"type": "Point", "coordinates": [-73.9, 40.7]}"#)
            )],
        )
        .await
        .unwrap();

    set_name
}

#[aerospike_macro::test]
async fn query_filter_geo_within_radius() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = create_geo_test_set(&client).await;
    let qpolicy = QueryPolicy::default();

    // 50km radius around [-122.0, 37.5] should include close1 and close2 but not far
    let mut stmt = Statement::new(namespace, &set_name, Bins::All);
    stmt.add_filter(Filter::geo_within_radius("geo_bin", -122.0, 37.5, 50000.0));

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
    assert_eq!(count, 2);

    let apolicy = AdminPolicy::default();
    let _ = client.truncate(&apolicy, namespace, &set_name, 0).await;
}

// ============================================================================
// Filter::geo_contains — regions containing a point
// ============================================================================

#[aerospike_macro::test]
async fn query_filter_geo_contains() {
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let bin_name = "region_bin";

    let client = common::singleton_client().await;
    let apolicy = AdminPolicy::default();
    let wp = WritePolicy::default();

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
        .expect("Failed to create geo index");
    task.wait_till_complete(None).await.unwrap();

    // Region that contains the test point [-122.0, 37.5]
    let key1 = as_key!(namespace, set_name, "region1");
    client
        .put(
            &wp,
            &key1,
            &vec![as_bin!(
                bin_name,
                as_geo!(
                    r#"{
                    "type": "Polygon",
                    "coordinates": [[[-123.0, 37.0], [-121.0, 37.0],
                                     [-121.0, 38.0], [-123.0, 38.0],
                                     [-123.0, 37.0]]]
                }"#
                )
            )],
        )
        .await
        .unwrap();

    // Region that does NOT contain the test point
    let key2 = as_key!(namespace, set_name, "region2");
    client
        .put(
            &wp,
            &key2,
            &vec![as_bin!(
                bin_name,
                as_geo!(
                    r#"{
                    "type": "Polygon",
                    "coordinates": [[[-74.0, 40.0], [-73.0, 40.0],
                                     [-73.0, 41.0], [-74.0, 41.0],
                                     [-74.0, 40.0]]]
                }"#
                )
            )],
        )
        .await
        .unwrap();

    // Query: which regions contain the point [-122.0, 37.5]?
    let point = r#"{"type": "Point", "coordinates": [-122.0, 37.5]}"#;
    let mut stmt = Statement::new(namespace, set_name, Bins::All);
    stmt.add_filter(Filter::geo_contains(bin_name, point));

    let pf = PartitionFilter::all();
    let mut rs = client
        .query(&QueryPolicy::default(), pf, stmt)
        .await
        .unwrap()
        .into_stream();

    let mut count = 0;
    while let Some(r) = rs.next().await {
        assert!(r.is_ok());
        count += 1;
    }
    assert_eq!(count, 1);

    let _ = client.truncate(&apolicy, namespace, set_name, 0).await;
}

// ============================================================================
// Filter::expression() builder — expression-based secondary index on filter
// ============================================================================

#[aerospike_macro::test]
async fn query_filter_with_expression_builder() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let apolicy = AdminPolicy::default();
    let wpolicy = WritePolicy::default();

    for i in 0..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        let bins = vec![as_bin!("a", i)];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    // Create an expression-based secondary index: int_bin("a")
    let exp = aerospike::expressions::int_bin("a".to_string());
    let idx_name = format!("{}_{}_exp_a", namespace, set_name);
    let task = client
        .create_index_using_expression(
            &apolicy,
            namespace,
            &set_name,
            &idx_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
            &exp,
        )
        .await
        .expect("Failed to create expression index");
    task.wait_till_complete(None).await.unwrap();

    // Query using Filter::range().expression() builder
    let mut stmt = Statement::new(namespace, &set_name, Bins::All);
    stmt.add_filter(Filter::range("a", 0_i64, 9_i64).expression(exp));

    let qpolicy = QueryPolicy::default();
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                let v: i64 = rec.bins["a"].clone().into();
                assert!(v >= 0 && v <= 9);
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    assert_eq!(count, 10);

}

// ============================================================================
// Filter::context() builder — CDT context on secondary index filter
// ============================================================================

#[aerospike_macro::test]
async fn query_filter_with_context_builder() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let apolicy = AdminPolicy::default();
    let wpolicy = WritePolicy::default();

    let bin_name = "nested";

    // Each record has a "nested" bin containing a list with a single integer: [i]
    for i in 0..20_i64 {
        let key = as_key!(namespace, &set_name, i);
        let list_val = as_list!(i);
        let bins = vec![as_bin!(bin_name, list_val)];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    // Create a secondary index on list element at index 0 using CDT context
    use aerospike::operations::cdt_context::ctx_list_index;
    let ctx = vec![ctx_list_index(0)];
    let idx_name = format!("{}_{}_nested_ctx", namespace, set_name);
    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            &set_name,
            bin_name,
            &idx_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
            Some(&ctx),
        )
        .await
        .expect("Failed to create context index");
    task.wait_till_complete(None).await.unwrap();

    // Query using Filter::range().context() builder
    let mut stmt = Statement::new(namespace, &set_name, Bins::All);
    stmt.add_filter(Filter::range(bin_name, 0_i64, 4_i64).context(vec![ctx_list_index(0)]));

    let qpolicy = QueryPolicy::default();
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(_) => count += 1,
            Err(err) => panic!("{:?}", err),
        }
    }
    // Records 0..=4 have list [0]..[4], all in range [0,4]
    assert_eq!(count, 5);

}

// ============================================================================
// Filter::expression() + QueryPolicy.filter_expression combined
// ============================================================================

/// Tests that Filter.expression (index selection) and QueryPolicy.filter_expression
/// (post-filter) work correctly together in the same query.
///
/// Filter.expression selects the expression-based secondary index for the lookup.
/// QueryPolicy.filter_expression further narrows down the returned records.
#[aerospike_macro::test]
async fn query_filter_expression_with_policy_filter() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let apolicy = AdminPolicy::default();
    let wpolicy = WritePolicy::default();

    // Write 50 records: bin "a" = i, bin "b" = i % 2 (0 or 1)
    for i in 0..50_i64 {
        let key = as_key!(namespace, &set_name, i);
        let bins = vec![as_bin!("a", i), as_bin!("b", i % 2)];
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    // Create an expression-based secondary index on int_bin("a")
    let idx_exp = aerospike::expressions::int_bin("a".to_string());
    let idx_name = format!("{}_{}_exp_ab", namespace, set_name);
    let task = client
        .create_index_using_expression(
            &apolicy,
            namespace,
            &set_name,
            &idx_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
            &idx_exp,
        )
        .await
        .expect("Failed to create expression index");
    task.wait_till_complete(None).await.unwrap();

    // Filter.expression: use the expression-based index to find records with a in [0, 9]
    // QueryPolicy.filter_expression: post-filter to only return records where b == 0 (even a)
    let mut qpolicy = QueryPolicy::default();
    qpolicy
        .base_policy
        .filter_expression
        .replace(aerospike::expressions::eq(
            aerospike::expressions::int_bin("b".to_string()),
            aerospike::expressions::int_val(0),
        ));

    let mut stmt = Statement::new(namespace, &set_name, Bins::All);
    stmt.add_filter(Filter::range("a", 0_i64, 9_i64).expression(idx_exp));

    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, stmt).await.unwrap();
    let mut count = 0;
    let mut rs = rs.into_stream();
    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                let a: i64 = rec.bins["a"].clone().into();
                let b: i64 = rec.bins["b"].clone().into();
                assert!(a >= 0 && a <= 9, "a={} out of index range", a);
                assert_eq!(b, 0, "post-filter should exclude odd records, a={}", a);
            }
            Err(err) => panic!("{:?}", err),
        }
    }
    // a in [0,9] => 10 records, but only even a values have b==0: {0,2,4,6,8} => 5
    assert_eq!(count, 5);

}

#[aerospike_macro::test]
async fn test_short_query_not_tracked() {
    let client = common::client().await;
    let ns = common::namespace();
    // Use a unique per-run set name so we can filter `query-show` to only
    // entries from this test, and avoid clashing with concurrent tests.
    let set_owned = format!("short_q_bug_{}", common::rand_str(10));
    let set: &str = &set_owned;
    let bin_name = "val";
    let num_records: i64 = 100;

    let wp = WritePolicy::default();
    let ap = AdminPolicy::default();

    // Load test data
    // println!(
    //     "\n[SETUP] Loading {} records into {}.{}",
    //     num_records, ns, set
    // );
    for i in 0..num_records {
        let key = as_key!(ns, set, i);
        let bins = vec![as_bin!(bin_name, i)];
        client.put(&wp, &key, &bins).await.unwrap();
    }

    // Set query-max-done to 100 so completed queries are tracked
    // println!("[SETUP] Setting query-max-done=100 on all nodes");
    let nodes = client.nodes();
    for node in &nodes {
        let set_cfg_cmd: &str = "set-config:context=service;query-max-done=100";
        let _ = node.info(&ap, &vec![set_cfg_cmd]).await;
    }
    aerospike_rt::sleep(Duration::from_secs(1)).await;

    // Clear any stale tracked queries: set to 0, wait, set back to 100
    for node in &nodes {
        let reset_cmd: &str = "set-config:context=service;query-max-done=0";
        let _ = node.info(&ap, &vec![reset_cmd]).await;
    }
    aerospike_rt::sleep(Duration::from_millis(500)).await;
    for node in &nodes {
        let set_cmd: &str = "set-config:context=service;query-max-done=100";
        let _ = node.info(&ap, &vec![set_cmd]).await;
    }
    aerospike_rt::sleep(Duration::from_millis(500)).await;

    // ──────────────────────────────────────────────────────────────
    // Test: PI query (no filter) with QueryDuration::Short
    //         Server should NOT track this in query-show
    // ──────────────────────────────────────────────────────────────
    // println!("\n[TEST 1] PI query with QueryDuration::Short (Rust partition-based)");
    {
        let mut qpolicy = QueryPolicy::default();
        qpolicy.expected_duration = QueryDuration::Short;

        let stmt = Statement::new(ns, set, Bins::All);
        let pf = PartitionFilter::all();

        let rs = client.query(&qpolicy, pf, stmt).await.unwrap();
        let mut rs = rs.into_stream();
        let mut count = 0;
        while let Some(res) = rs.next().await {
            match res {
                Ok(_) => count += 1,
                Err(err) => panic!("  Error: {:?}", err),
            }
        }
        // println!("  Returned {} records", count);
        assert_eq!(
            count, num_records,
            "Expected {} records, but got {}",
            num_records, count
        );
    }
    aerospike_rt::sleep(Duration::from_millis(500)).await;

    // Check query-show for tracked queries belonging to *this* test only.
    // `query-show` returns server-wide tracked queries; concurrent tests
    // running long queries (the default duration) would otherwise pollute
    // this count and make the assertion flaky in parallel runs.
    let set_marker = format!(":set={}", set);
    let mut total_tracked = 0;
    for node in &nodes {
        let result = node.info(&ap, &vec!["query-show"]).await;
        match result {
            Ok(info_map) => {
                for (_, value) in &info_map {
                    if !value.is_empty() && value != "ok" {
                        for entry in value.split(';').filter(|s| !s.is_empty()) {
                            if entry.contains(&set_marker) {
                                total_tracked += 1;
                            }
                        }
                    }
                }
            }
            Err(e) => panic!("  Info error: {:?}", e),
        }
    }

    // println!(
    //     "\n[RESULT 1] Total tracked queries after SHORT pi_query: {}",
    //     total_tracked
    // );

    client.close().await.unwrap();

    assert_eq!(
        total_tracked, 0,
        "QueryDuration::Short should not be tracked by the server's query monitor; \
         got {total_tracked} tracked entries — set_scan likely failed to set INFO1_SHORT_QUERY"
    );
}

// ===== Foreground ops projection (CLIENT-3998 / Go commit 8cbed7e) =====
//
// `Statement::operations` lets a foreground query return only the result
// of an explicit ops list per matching record, in place of the bin set.
// The basic `Read` op works on any server; CDT / expression / bit / HLL
// reads require server >= 8.1.2.

async fn server_supports_query_ops_projection_ext(client: &aerospike::Client) -> bool {
    match client.cluster.get_random_node() {
        Ok(node) => node.version().supports_query_ops_projection_ext(),
        Err(_) => false,
    }
}

#[aerospike_macro::test]
async fn query_ops_projection_basic_read() {
    // Basic `Read` op projection works on every server version that
    // supports query ops projection at all (pre- and post-8.1.2).
    // Uses a secondary-index filter so it routes through the
    // partition-query path (set_query) regardless of any future
    // dispatch refactor.
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 50).await;

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::range("bin", 0, 49));
    statement.set_operations(vec![operations::get_bin("bin2")]);

    let qpolicy = QueryPolicy::default();
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut rs = rs.into_stream();

    let mut count = 0;
    while let Some(res) = rs.next().await {
        let rec = res.expect("query record");
        // Only the projected bin is returned; `bin` and `extra` should
        // be absent because the projection narrowed the result.
        assert!(
            rec.bins.contains_key("bin2"),
            "projected bin2 missing from record: {:?}",
            rec.bins
        );
        assert!(
            !rec.bins.contains_key("bin"),
            "non-projected bin leaked: {:?}",
            rec.bins
        );
        assert!(
            !rec.bins.contains_key("extra"),
            "non-projected extra leaked: {:?}",
            rec.bins
        );
        count += 1;
    }
    assert_eq!(count, 50);

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_ops_projection_rejects_write_op() {
    // Foreground ops projection rejects write ops up front — caller is
    // pointed at `query_operate` for the background equivalent.
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 1).await;

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.set_operations(vec![operations::put(&as_bin!("bin", 99))]);

    let qpolicy = QueryPolicy::default();
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut rs = rs.into_stream();

    let mut got_invalid = false;
    while let Some(res) = rs.next().await {
        match res {
            Ok(_) => {}
            Err(err) => {
                // The buffer-build error surfaces wrapped in a Chain;
                // walk the chain looking for our InvalidArgument.
                let chain_text = format!("{err:?}");
                assert!(
                    chain_text.contains("read-only"),
                    "expected read-only error somewhere in the chain, got: {chain_text}"
                );
                got_invalid = true;
            }
        }
    }
    assert!(got_invalid, "write op in foreground query should fail");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_ops_projection_extended_cdt_read() {
    // Extended ops projection (CDT/expression/bit/HLL reads) only
    // works on 8.1.2+. We use a CDT list size op as the smoke test.
    let client = common::client().await;
    if !server_supports_query_ops_projection_ext(&client).await {
        eprintln!(
            "Skipping: server does not support extended query ops projection (requires >= 8.1.2)"
        );
        return;
    }

    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let wpolicy = WritePolicy::default();

    // Seed 10 records with a list bin of varying lengths.
    for i in 0..10_i64 {
        let key = as_key!(namespace, &set_name, i);
        let list: Vec<Value> = (0..=i).map(Value::from).collect();
        client.delete(&wpolicy, &key).await.unwrap();
        client
            .put(&wpolicy, &key, &[as_bin!("nums", Value::List(list))])
            .await
            .unwrap();
    }

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.set_operations(vec![aerospike::operations::lists::size("nums")]);

    let qpolicy = QueryPolicy::default();
    let pf = PartitionFilter::all();
    let rs = client.query(&qpolicy, pf, statement).await.unwrap();
    let mut rs = rs.into_stream();

    let mut count = 0;
    while let Some(res) = rs.next().await {
        let rec = res.expect("query record");
        // The CDT-size op returns under the same bin name with an int
        // particle — assert presence; lengths should be in [1, 10].
        let size = match rec.bins.get("nums") {
            Some(Value::Int(n)) => *n,
            other => panic!("expected Int size for 'nums', got {:?}", other),
        };
        assert!(
            (1..=10).contains(&size),
            "list size out of expected range: {size}"
        );
        count += 1;
    }
    assert_eq!(count, 10);

    client.close().await.unwrap();
}

// ===== Additional ops-projection rejection tests (Go parity) =====
//
// Mirrors `query_operations_test.go` from the Go client: every shape of
// "wrong-direction op" must be rejected client-side before the
// statement is sent. Each test asserts the error surfaces, regardless
// of whether it's wrapped by the buffer-prepare chain.

fn assert_chain_contains(err: &Error, needle: &str) {
    let text = format!("{err:?}");
    assert!(
        text.contains(needle),
        "expected '{needle}' somewhere in the error chain, got: {text}"
    );
}

#[aerospike_macro::test]
async fn query_ops_projection_rejects_touch_op() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 1).await;

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.set_operations(vec![operations::touch()]);

    let qpolicy = QueryPolicy::default();
    let rs = client
        .query(&qpolicy, PartitionFilter::all(), statement)
        .await
        .unwrap();
    let mut rs = rs.into_stream();
    let mut got_err = false;
    while let Some(res) = rs.next().await {
        if let Err(err) = res {
            assert_chain_contains(&err, "read-only");
            got_err = true;
        }
    }
    assert!(got_err, "touch op in foreground query should fail");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_ops_projection_rejects_delete_op() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 1).await;

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.set_operations(vec![operations::delete()]);

    let qpolicy = QueryPolicy::default();
    let rs = client
        .query(&qpolicy, PartitionFilter::all(), statement)
        .await
        .unwrap();
    let mut rs = rs.into_stream();
    let mut got_err = false;
    while let Some(res) = rs.next().await {
        if let Err(err) = res {
            assert_chain_contains(&err, "read-only");
            got_err = true;
        }
    }
    assert!(got_err, "delete op in foreground query should fail");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_ops_projection_rejects_mixed_ops() {
    // A read op alongside a write op must fail with the same
    // read-only validation as a single write — the validator scans
    // the whole list.
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 1).await;

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.set_operations(vec![
        operations::get_bin("bin"),
        operations::put(&as_bin!("foo", "bar")),
    ]);

    let qpolicy = QueryPolicy::default();
    let rs = client
        .query(&qpolicy, PartitionFilter::all(), statement)
        .await
        .unwrap();
    let mut rs = rs.into_stream();
    let mut got_err = false;
    while let Some(res) = rs.next().await {
        if let Err(err) = res {
            assert_chain_contains(&err, "read-only");
            got_err = true;
        }
    }
    assert!(
        got_err,
        "mixed read+write ops in foreground query should fail"
    );

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_operate_rejects_read_op() {
    // Background ops projection (`query_operate`) is the inverse: it
    // accepts only write ops. A pure read op must be rejected.
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 1).await;

    let statement = Statement::new(namespace, &set_name, Bins::All);
    let wpolicy = WritePolicy::default();
    let result = client
        .query_operate(&wpolicy, statement, &[operations::get_bin("bin")])
        .await;

    match result {
        Err(err) => assert_chain_contains(&err, "write-only"),
        Ok(_) => panic!("read op in background query should fail before submit"),
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_operate_rejects_get_op() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 1).await;

    let statement = Statement::new(namespace, &set_name, Bins::All);
    let wpolicy = WritePolicy::default();
    let result = client
        .query_operate(&wpolicy, statement, &[operations::get()])
        .await;

    match result {
        Err(err) => assert_chain_contains(&err, "write-only"),
        Ok(_) => panic!("get op in background query should fail before submit"),
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_operate_rejects_mixed_ops() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 1).await;

    let statement = Statement::new(namespace, &set_name, Bins::All);
    let wpolicy = WritePolicy::default();
    let result = client
        .query_operate(
            &wpolicy,
            statement,
            &[
                operations::get_bin("bin"),
                operations::put(&as_bin!("tag", "mixed")),
            ],
        )
        .await;

    match result {
        Err(err) => assert_chain_contains(&err, "write-only"),
        Ok(_) => panic!("mixed read+write in background query should fail before submit"),
    }

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_returns_user_key_when_send_key_set() {
    // Java TestQueryKey: when records are written with send_key=true,
    // the server stores the user key alongside each record and a
    // secondary-index query returns it on the matching records.
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let apolicy = AdminPolicy::default();

    let mut wpolicy = WritePolicy::default();
    wpolicy.send_key = true;
    let bin_name = "qkbin";
    for i in 1..=5_i64 {
        let key = as_key!(namespace, &set_name, i);
        let bin = as_bin!(bin_name, i);
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &[bin]).await.unwrap();
    }

    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            &set_name,
            bin_name,
            &format!("{}_{}_qk", namespace, set_name),
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("create_index");
    task.wait_till_complete(None).await.unwrap();

    let mut statement = Statement::new(namespace, &set_name, Bins::from([bin_name]));
    statement.add_filter(Filter::range(bin_name, 2, 5));

    let qpolicy = QueryPolicy::default();
    let rs = client
        .query(&qpolicy, PartitionFilter::all(), statement)
        .await
        .unwrap();
    let mut rs = rs.into_stream();

    let mut count = 0;
    while let Some(res) = rs.next().await {
        let rec = res.expect("query record");
        // Each returned record should carry the user-key set by the
        // original put (send_key=true).
        let key = rec.key.as_ref().expect("record key present");
        assert!(
            key.user_key.is_some(),
            "expected user_key on returned record, got: {:?}",
            key
        );
        count += 1;
    }
    assert_eq!(count, 4, "expected 4 records in [2,5] range");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn query_ops_projection_multiple_bins() {
    // Project multiple specific bins via several `get_bin` ops on a
    // foreground query — verifies the trailing READ-op writer handles
    // > 1 op correctly.
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = create_test_set(&client, 20).await;

    let mut statement = Statement::new(namespace, &set_name, Bins::All);
    statement.add_filter(Filter::range("bin", 0, 19));
    statement.set_operations(vec![
        operations::get_bin("bin"),
        operations::get_bin("bin2"),
    ]);

    let qpolicy = QueryPolicy::default();
    let rs = client
        .query(&qpolicy, PartitionFilter::all(), statement)
        .await
        .unwrap();
    let mut rs = rs.into_stream();

    let mut count = 0;
    while let Some(res) = rs.next().await {
        let rec = res.expect("query record");
        assert!(rec.bins.contains_key("bin"));
        assert!(rec.bins.contains_key("bin2"));
        assert!(
            !rec.bins.contains_key("extra"),
            "non-projected 'extra' leaked: {:?}",
            rec.bins
        );
        count += 1;
    }
    assert_eq!(count, 20);

    client.close().await.unwrap();
}
