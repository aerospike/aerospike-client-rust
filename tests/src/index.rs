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

use log::*;

use crate::common;
use aerospike::expressions::*;

use aerospike::Task;
use aerospike::*;

const EXPECTED: usize = 100;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let wpolicy = WritePolicy::default();

    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![wbin];
        common::delete_durably(client, &wpolicy, &key)
            .await
            .unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }

    set_name
}

#[aerospike_macro::test]
async fn create_index_on_bin() {
    let client = common::client().await;
    let ns = common::namespace();
    let set = create_test_set(&client, EXPECTED).await;
    let bin = "bin";
    let index = format!("{}_{}_{}", ns, set, bin);
    let apolicy = AdminPolicy::default();

    let task = client.drop_index(&apolicy, ns, &set, &index).await.unwrap();
    task.wait_till_complete(None).await.unwrap();

    let _index_guard = common::lock_index_ops().await;
    let task = client
        .create_index_on_bin(
            &apolicy,
            ns,
            &set,
            bin,
            &index,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    // redo to make sure it is supported
    let task = client
        .create_index_on_bin(
            &apolicy,
            ns,
            &set,
            bin,
            &index,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn create_index_using_expression() {
    let client = common::client().await;

    if client
        .cluster
        .get_random_node()
        .is_ok_and(|node| node.version() < &Version::new(8, 1, 0, 0))
    {
        info!("create_index_using_expression test is only supported in server versions 8.1.0.0+. Skipping.");
        return;
    }

    let ns = common::namespace();
    let set = create_test_set(&client, EXPECTED).await;
    let bin = "bin";
    let index = format!("{}_{}_{}", ns, set, bin);
    let apolicy = AdminPolicy::default();

    let task = client.drop_index(&apolicy, ns, &set, &index).await.unwrap();
    task.wait_till_complete(None).await.unwrap();

    let fe: Expression = num_add(vec![int_bin(common::rand_str(10)), int_val(0)]);

    let _index_guard = common::lock_index_ops().await;
    let task = client
        .create_index_using_expression(
            &apolicy,
            ns,
            &set,
            &index,
            IndexType::Numeric,
            CollectionIndexType::Default,
            &fe,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    // redo to see if it is supported
    let task = client
        .create_index_using_expression(
            &apolicy,
            ns,
            &set,
            &index,
            IndexType::Numeric,
            CollectionIndexType::Default,
            &fe,
        )
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn blob_index_serves_a_blob_equality_filter() {
    // A blob equality filter had no index that could serve it: `Filter::equal`
    // has always accepted `Vec<u8>`/`&[u8]`, but IndexType had no BLOB variant
    // (server 7.0+, and the Java client has had it).
    let client = common::client().await;
    let ns = common::namespace();
    let apolicy = AdminPolicy::default();

    let supported = match client.cluster.nodes().first() {
        Some(node) => node.version().supports_blob_index(),
        None => false,
    };
    if !supported {
        eprintln!("skipping blob index test: server predates blob indexes (7.0)");
        client.close().await.unwrap();
        return;
    }

    let set = common::rand_str(10);
    let bin = "bl";
    let index = format!("idx_blob_{set}");
    let wpolicy = WritePolicy::default();

    // Three distinct blobs; only one is the needle.
    use futures::StreamExt;
    let needle: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let blobs: Vec<Vec<u8>> = vec![
        vec![0x01, 0x02],
        needle.clone(),
        vec![0xFF],
        // A blob that shares a prefix with the needle, so a truncating
        // comparison would over-match.
        vec![0xDE, 0xAD],
    ];
    for (i, blob) in blobs.iter().enumerate() {
        let key = as_key!(ns, &set, i as i64);
        client
            .put(&wpolicy, &key, &[as_bin!(bin, blob.clone())])
            .await
            .unwrap();
    }

    let _index_guard = common::lock_index_ops().await;

    // Causation check: the same filter without an index has nothing to run on,
    // so success below is the index doing the work rather than a silent scan.
    let mut unindexed = Statement::new(ns, &set, Bins::All);
    unindexed.add_filter(aerospike::query::Filter::equal(bin, needle.clone()));
    let unindexed_result = client
        .query(&QueryPolicy::default(), PartitionFilter::all(), unindexed)
        .await;
    match unindexed_result {
        Err(err) => {
            debug!("blob filter without an index failed as expected: {err}");
        }
        Ok(rs) => {
            let mut stream = rs.into_stream();
            let mut rows = 0;
            let mut errored = false;
            while let Some(res) = stream.next().await {
                if res.is_err() {
                    errored = true;
                } else {
                    rows += 1;
                }
            }
            assert!(
                errored && rows == 0,
                "a blob filter with no index should not quietly return rows"
            );
        }
    }

    let task = client
        .create_index_on_bin(
            &apolicy,
            ns,
            &set,
            bin,
            &index,
            IndexType::Blob,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("the server must accept a BLOB index");
    task.wait_till_complete(None).await.unwrap();

    let mut statement = Statement::new(ns, &set, Bins::All);
    statement.add_filter(aerospike::query::Filter::equal(bin, needle.clone()));
    let qpolicy = QueryPolicy::default();
    let rs = client
        .query(&qpolicy, PartitionFilter::all(), statement)
        .await
        .unwrap();

    let mut matched = Vec::new();
    let mut stream = rs.into_stream();
    while let Some(res) = stream.next().await {
        let record = res.unwrap();
        matched.push(record.bins.get(bin).cloned());
    }

    assert_eq!(
        matched.len(),
        1,
        "blob equality filter should match exactly the one record: {matched:?}"
    );
    assert_eq!(matched[0], Some(Value::from(needle.clone())));

    let task = client.drop_index(&apolicy, ns, &set, &index).await.unwrap();
    task.wait_till_complete(None).await.unwrap();
    client.close().await.unwrap();
}
