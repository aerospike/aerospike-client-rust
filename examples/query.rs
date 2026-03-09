#[macro_use]
extern crate aerospike;
extern crate tokio;

use aerospike::query::PartitionFilter;
use aerospike::{Bins, Client, ClientPolicy, QueryPolicy, Statement};
use aerospike_core::expressions::{eq, int_bin, int_val};
use aerospike_core::{AdminPolicy, CollectionIndexType, IndexType, Task, WritePolicy};
use futures::stream::StreamExt;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const DEFAULT_NAMESPACE: &str = "test";
const DEFAULT_NUM_RECORDS: usize = 100;
const BIN_NAME: &str = "bin";

#[tokio::main]
async fn main() {
    let client = connect_to_aerospike().await;
    println!("Connected to Aerospike!");

    let set_name = create_test_set(&client, DEFAULT_NAMESPACE, DEFAULT_NUM_RECORDS).await;

    simple_equality_query(&client, DEFAULT_NAMESPACE, &set_name).await;
    range_query(&client, DEFAULT_NAMESPACE, &set_name).await;
    metadata_only_query(&client, DEFAULT_NAMESPACE, &set_name).await;
    cursor_pagination(&client, DEFAULT_NAMESPACE, &set_name).await;
    parallel_query(&client, DEFAULT_NAMESPACE, &set_name).await;
    expression_filter(&client, DEFAULT_NAMESPACE, &set_name).await;
    rate_limited_query(&client, DEFAULT_NAMESPACE, &set_name).await;

    client.close().await.unwrap();
}

async fn connect_to_aerospike() -> Client {
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3100"));

    let policy = ClientPolicy::default();

    Client::new(&policy, &hosts)
        .await
        .expect("Failed to connect to cluster")
}

async fn simple_equality_query(client: &Client, namespace: &str, set_name: &str) {
    println!("\n--- 1. Simple equality query ---");

    let policy = QueryPolicy::default();
    let mut stmt = Statement::new(namespace, set_name, Bins::All);
    stmt.add_filter(as_eq!(BIN_NAME, 5));

    let rs = client
        .query(&policy, PartitionFilter::all(), stmt)
        .await
        .expect("Query failed");

    let mut rs = rs.into_stream();
    while let Some(r) = rs.next().await {
        println!("Equal query record: {:?}", r.unwrap());
    }
}

async fn range_query(client: &Client, namespace: &str, set_name: &str) {
    println!("\n--- 2. Range query ---");

    let policy = QueryPolicy::default();
    let mut stmt = Statement::new(namespace, set_name, Bins::All);
    stmt.add_filter(as_range!(BIN_NAME, 0, 9));

    let rs = client
        .query(&policy, PartitionFilter::all(), stmt)
        .await
        .expect("Query failed");

    let mut rs = rs.into_stream();
    while let Some(r) = rs.next().await {
        println!("Range query record: {:?}", r.unwrap());
    }
}

async fn metadata_only_query(client: &Client, namespace: &str, set_name: &str) {
    println!("\n--- 3. Query without bins (metadata only) ---");

    let policy = QueryPolicy::default();
    let mut stmt = Statement::new(namespace, set_name, Bins::None);
    stmt.add_filter(as_range!(BIN_NAME, 0, 4));

    let rs = client
        .query(&policy, PartitionFilter::all(), stmt)
        .await
        .expect("Query failed");

    let mut rs = rs.into_stream();
    while let Some(r) = rs.next().await {
        let rec = r.unwrap();
        println!(
            "Record header: gen={}, bins={}",
            rec.generation,
            rec.bins.len()
        );
    }
}

async fn cursor_pagination(client: &Client, namespace: &str, set_name: &str) {
    println!("\n--- 4. Cursor-based query (pagination) ---");

    let policy = QueryPolicy::default();
    let mut pf = PartitionFilter::all();

    while !pf.done() {
        let stmt = Statement::new(namespace, set_name, Bins::All);
        let rs = client.query(&policy, pf, stmt).await.expect("Query failed");

        let mut rs = rs.into_stream();
        while let Some(r) = rs.next().await {
            println!("Cursor record: {:?}", r.unwrap());
        }
        pf = rs.partition_filter().await.unwrap();
    }
}

async fn parallel_query(client: &Client, namespace: &str, set_name: &str) {
    println!("\n--- 5. Multi-consumer parallel query ---");

    const NUM_WORKERS: usize = 4;
    let policy = QueryPolicy::default();
    let mut stmt = Statement::new(namespace, set_name, Bins::All);
    stmt.add_filter(as_range!(BIN_NAME, 0, 9));

    let rs = client
        .query(&policy, PartitionFilter::all(), stmt)
        .await
        .expect("Query failed");

    let count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(NUM_WORKERS);

    for _ in 0..NUM_WORKERS {
        let rs_clone = rs.clone();
        let count = count.clone();
        handles.push(tokio::spawn(async move {
            let mut rs_stream = rs_clone.into_stream();
            while let Some(record) = rs_stream.next().await {
                if record.is_ok() {
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    futures::future::join_all(handles).await;
    println!(
        "Total records processed in parallel: {}",
        count.load(Ordering::Relaxed)
    );
}

async fn expression_filter(client: &Client, namespace: &str, set_name: &str) {
    println!("\n--- 6. Query with expression filter ---");

    let mut policy = QueryPolicy::default();
    policy
        .base_policy
        .filter_expression
        .replace(eq(int_bin(BIN_NAME.to_string()), int_val(10)));

    let stmt = Statement::new(namespace, set_name, Bins::All);
    let rs = client
        .query(&policy, PartitionFilter::all(), stmt)
        .await
        .expect("Query failed");

    let mut rs = rs.into_stream();
    while let Some(r) = rs.next().await {
        println!("Expression query record: {:?}", r.unwrap());
    }
}

async fn rate_limited_query(client: &Client, namespace: &str, _set_name: &str) {
    println!("\n--- 7. Rate-limited query ---");

    // Use 1000 records for more observable rate limiting effect
    let test_set_name = create_test_set(client, namespace, 1000).await;

    const RATE_LIMIT: u32 = 3;
    const TOTAL_RECORDS: i64 = 1000;
    let range_end = TOTAL_RECORDS / RATE_LIMIT as i64;

    let mut policy = QueryPolicy::default();
    policy.records_per_second = RATE_LIMIT;

    // Query only a subset of records, matching the test pattern
    let mut stmt = Statement::new(namespace, &test_set_name, Bins::All);
    stmt.add_filter(as_range!(BIN_NAME, 0, range_end));

    let expected_count = range_end + 1;
    println!("Rate limit set to {} records/second", RATE_LIMIT);
    println!(
        "Querying records with bin value 0-{} (expected: ~{} records)",
        range_end, expected_count
    );

    let start_time = tokio::time::Instant::now();
    let rs = client
        .query(&policy, PartitionFilter::all(), stmt)
        .await
        .expect("Query failed");

    let mut count = 0;
    let mut rs = rs.into_stream();
    let mut last_log = tokio::time::Instant::now();
    let mut batch_count = 0;

    while let Some(res) = rs.next().await {
        match res {
            Ok(rec) => {
                count += 1;
                let v: i64 = rec.bins[BIN_NAME].clone().into();
                assert!(v >= 0 && v <= range_end, "Unexpected bin value: {}", v);

                // Log progress more frequently to see the throttling
                if last_log.elapsed().as_millis() >= 500 {
                    batch_count += 1;
                    println!(
                        "Batch {}: {} records in {:?}",
                        batch_count,
                        count,
                        start_time.elapsed()
                    );
                    last_log = tokio::time::Instant::now();
                }
            }
            Err(err) => panic!("Query error: {:?}", err),
        }
    }

    let duration = tokio::time::Instant::now() - start_time;
    println!("\nResults:");
    println!("  Records returned: {}", count);
    println!("  Duration: {:?} ({}ms)", duration, duration.as_millis());
}

async fn create_test_set(client: &Client, namespace: &str, num_records: usize) -> String {
    let set_name = generate_random_set_name();
    println!(
        "Creating test set '{}' with {} records...",
        set_name, num_records
    );

    populate_test_data(client, namespace, &set_name, num_records).await;
    create_secondary_index(client, namespace, &set_name).await;

    println!("Test set created successfully!");
    set_name
}

fn generate_random_set_name() -> String {
    let rng = rand::thread_rng();
    rng.sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

async fn populate_test_data(client: &Client, namespace: &str, set_name: &str, num_records: usize) {
    let wpolicy = WritePolicy::default();

    for i in 0..num_records as i64 {
        let key = as_key!(namespace, set_name, i);
        let wbin = as_bin!(BIN_NAME, i);
        let bins = vec![wbin];

        client.delete(&wpolicy, &key).await.ok(); // Ignore if doesn't exist
        client
            .put(&wpolicy, &key, &bins)
            .await
            .expect("Failed to write record");
    }
}

async fn create_secondary_index(client: &Client, namespace: &str, set_name: &str) {
    let apolicy = AdminPolicy::default();
    let index_name = format!("{}_{}_idx", set_name, BIN_NAME);

    let task = client
        .create_index_on_bin(
            &apolicy,
            namespace,
            set_name,
            BIN_NAME,
            &index_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create index");

    task.wait_till_complete(None)
        .await
        .expect("Index creation timed out");
}
