//! Scans: full-set reads without a secondary-index filter, including
//! pagination and resume via partition filters, and parallel consumption.
//!
//! Ports of the Java client's `AsyncScan`, `ScanPage`, `ScanParallel`,
//! `ScanResume` and `ScanSeries` examples. In the Rust client a scan is a
//! query with no filter: partition iteration, pagination and resume all go
//! through [`PartitionFilter`].

use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aerospike::query::PartitionFilter;
use aerospike::{as_bin, as_key};
use aerospike::{Bins, Client, ClientPolicy, QueryPolicy, Statement, WritePolicy};
use futures::StreamExt;

const SET: &str = "scan_demo";
const RECORDS: usize = 300;

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Arc::new(
        Client::new(&cpolicy, &hosts)
            .await
            .expect("Failed to connect to cluster"),
    );

    let wpolicy = WritePolicy::default();
    for i in 0..RECORDS as i64 {
        let key = as_key!("test", SET, i);
        client.put(&wpolicy, &key, &[as_bin!("n", i)]).await.unwrap();
    }

    // ---- Full scan (Java AsyncScan): no filter on the statement ----
    let qpolicy = QueryPolicy::default();
    let stmt = Statement::new("test", SET, Bins::All);
    let rs = client
        .query(&qpolicy, PartitionFilter::all(), stmt)
        .await
        .unwrap();
    let count = rs
        .into_stream()
        .filter(|r| futures::future::ready(r.is_ok()))
        .count()
        .await;
    println!("full scan: {count} records");

    // ---- Paged scan (Java ScanPage / ScanResume / ScanSeries) ----
    // max_records caps each page; the recordset hands back a PartitionFilter
    // cursor that resumes exactly where the previous page stopped. The
    // cursor is plain data — persist it to resume a scan across process
    // restarts, like Java's ScanResume.
    let mut paged = QueryPolicy::default();
    paged.max_records = 100;

    let mut pf = PartitionFilter::all();
    let mut page = 0;
    let mut total = 0;
    while !pf.done() {
        let stmt = Statement::new("test", SET, Bins::All);
        let rs = client.query(&paged, pf, stmt).await.unwrap();
        let n = rs
            .clone()
            .into_stream()
            .filter(|r| futures::future::ready(r.is_ok()))
            .count()
            .await;
        page += 1;
        total += n;
        println!("page {page}: {n} records");
        pf = rs.partition_filter().await.unwrap();
    }
    println!("paged scan: {total} records over {page} pages");

    // ---- Parallel consumption (Java ScanParallel) ----
    // One stream, many workers: the recordset stream is shared by handle.
    let qpolicy = QueryPolicy::default();
    let stmt = Statement::new("test", SET, Bins::None);
    let rs = client
        .query(&qpolicy, PartitionFilter::all(), stmt)
        .await
        .unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for _ in 0..4 {
        let mut stream = rs.clone().into_stream();
        let counter = counter.clone();
        workers.push(tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                if item.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for w in workers {
        w.await.unwrap();
    }
    println!(
        "parallel scan: {} records via 4 workers",
        counter.load(Ordering::Relaxed)
    );

    for i in 0..RECORDS as i64 {
        let _ = client.delete(&wpolicy, &as_key!("test", SET, i)).await;
    }
    client.close().await.unwrap();
}
