// Copyright 2015-2026 Aerospike, Inc.
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

//! Streaming delivery and background work: callbacks, handles and plans.
//!
//! `query` hands records back through a stream you pull from. This example
//! shows the other delivery shapes:
//!
//! - `query_foreach`: an exactly-once callback invoked on the node streams,
//!   with a `QueryHandle` to wait on, cancel, or take a resumable cursor from.
//! - `batch_foreach`: batch rows delivered to a hook as they arrive, before
//!   the call returns.
//! - `query_operate`: a background job that applies operations to every
//!   matching record on the server, with an `ExecuteTask` to await.
//! - `query_explain` + `query_with_plan` (server 8.2.0+): ask the server how
//!   it would run a query written in AEL text, then run that plan.
//!
//! ```bash
//! cargo run --example query_streaming
//! ```

use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aerospike::query::{Filter, PartitionFilter};
use aerospike::{
    as_bin, as_key, operations, AdminPolicy, BatchOperation, BatchPolicy, BatchReadPolicy, Bins,
    Client, ClientPolicy, CollectionIndexType, IndexType, QueryPolicy, ReadPolicy, Statement, Task,
    WritePolicy,
};

const SET: &str = "stream_demo";
const BIN: &str = "age";
const RECORDS: i64 = 200;

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();

    // ---- Data and an index to query on ----
    for i in 0..RECORDS {
        let key = as_key!("test", SET, i);
        client
            .put(&wpolicy, &key, &[as_bin!(BIN, i % 100), as_bin!("visits", 0)])
            .await
            .unwrap();
    }
    let index_name = "stream_demo_age_idx";
    let _ = client.drop_index(&apolicy, "test", SET, index_name).await;
    let task = client
        .create_index_on_bin(
            &apolicy,
            "test",
            SET,
            BIN,
            index_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create index");
    task.wait_till_complete(None).await.unwrap();

    // ---- query_foreach: a callback per record, exactly once ----
    // The callback runs on the node streams (serially per node, concurrently
    // across nodes). Returning `false` aborts the query.
    let mut stmt = Statement::new("test", SET, Bins::All);
    stmt.set_filter(Filter::range(BIN, 18, 30));
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = seen.clone();
    let mut handle = client
        .query_foreach(&QueryPolicy::default(), PartitionFilter::all(), stmt, move |record| {
            let counter = counter.clone();
            async move {
                match record {
                    Ok(_) => {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => eprintln!("query error: {e}"),
                }
                true
            }
        })
        .await
        .unwrap();
    handle.wait().await.unwrap();
    println!("query_foreach delivered {} records with age in 18..=30", seen.load(Ordering::Relaxed));

    // ---- A handle can stop a query early and hand back where it got to ----
    let mut stmt = Statement::new("test", SET, Bins::All);
    stmt.set_filter(Filter::range(BIN, 0, 99));
    let delivered = Arc::new(AtomicUsize::new(0));
    let counter = delivered.clone();
    let mut handle = client
        .query_foreach(&QueryPolicy::default(), PartitionFilter::all(), stmt, move |record| {
            let counter = counter.clone();
            async move {
                if record.is_ok() {
                    // Stop after the first 25 records.
                    counter.fetch_add(1, Ordering::Relaxed) < 24
                } else {
                    false
                }
            }
        })
        .await
        .unwrap();
    let _ = handle.wait().await;
    let cursor = handle.partition_filter();
    println!(
        "aborted after {} records; cursor done: {:?}",
        delivered.load(Ordering::Relaxed),
        cursor.as_ref().map(PartitionFilter::done)
    );
    // `cursor` can be passed to `query` or `query_foreach` to resume exactly
    // after the last record the callback saw.

    // ---- batch_foreach: rows delivered as they arrive ----
    let bpr = BatchReadPolicy::default();
    let mut ops: Vec<BatchOperation> = (0..10)
        .map(|i| BatchOperation::read(&bpr, as_key!("test", SET, i), Bins::from([BIN])))
        .collect();
    let rows = Arc::new(AtomicUsize::new(0));
    let counter = rows.clone();
    client
        .batch_foreach(&BatchPolicy::default(), &mut ops, move |index, row| {
            let counter = counter.clone();
            let found = row.record.is_some();
            async move {
                counter.fetch_add(1, Ordering::Relaxed);
                if index == 0 {
                    println!("batch_foreach: first row answered, found = {found}");
                }
                true
            }
        })
        .await
        .unwrap();
    println!("batch_foreach saw {} rows; the ops carry the results too: {:?}", rows.load(Ordering::Relaxed), ops[3].record().map(|r| r.bins.get(BIN)));

    // ---- query_operate: a background job on the server ----
    // Every record with age < 10 gets its visits bin incremented, with no
    // records sent to the client. The task tells when the job is done.
    let mut stmt = Statement::new("test", SET, Bins::None);
    stmt.set_filter(Filter::range(BIN, 0, 9));
    stmt.set_operations(vec![operations::add(&as_bin!("visits", 1))]);
    let task = client.query_operate(&wpolicy, stmt).await.unwrap();
    task.wait_till_complete(None).await.unwrap();
    let rec = client
        .get(&ReadPolicy::default(), &as_key!("test", SET, 5), Bins::from(["visits"]))
        .await
        .unwrap();
    println!("query_operate incremented visits on matching records: {:?}", rec.bins.get("visits"));

    // ---- query_explain + query_with_plan (server 8.2.0+) ----
    let explain_ok = client
        .random_node()
        .map(|n| n.version().supports_query_selection())
        .unwrap_or(false);
    if explain_ok {
        // AEL is the cross-client filter text; the server picks an index.
        let plan = client
            .query_explain(&QueryPolicy::default(), "test", Some(SET), "$.age >= 90", None, None)
            .await
            .unwrap();
        println!(
            "explain: index = {:?}, secondary index: {}, filtered out: {}",
            plan.index_name(),
            plan.is_secondary_index(),
            plan.is_filtered_out()
        );
        let stmt = Statement::new("test", SET, Bins::All);
        let rs = client
            .query_with_plan(&QueryPolicy::default(), PartitionFilter::all(), stmt, plan)
            .await
            .unwrap();
        use futures::StreamExt;
        let count = rs.into_stream().filter(|r| std::future::ready(r.is_ok())).count().await;
        println!("query_with_plan returned {count} records with age >= 90");
    } else {
        println!("Server does not support query explain (requires 8.2.0+); skipping that part.");
    }

    let _ = client.drop_index(&apolicy, "test", SET, index_name).await;
    for i in 0..RECORDS {
        let _ = client.delete(&wpolicy, &as_key!("test", SET, i)).await;
    }
    client.close().await.unwrap();
}
