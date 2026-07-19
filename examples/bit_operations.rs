//! Bitwise operations on blob bins.
//!
//! Port of the Java client's `OperateBit` example.

use std::env;

use aerospike::operations::bitwise;
use aerospike::operations::bitwise::BitPolicy;
use aerospike::{as_bin, as_key};
use aerospike::{Client, ClientPolicy, Value, WritePolicy};

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let wpolicy = WritePolicy::default();
    let bpolicy = BitPolicy::default();
    let key = as_key!("test", "bit_ops", "demo");
    let _ = client.delete(&wpolicy, &key).await;

    // Store a 5-byte blob.
    client
        .put(
            &wpolicy,
            &key,
            &[as_bin!("bits", vec![0b0000_0001u8, 0b0100_0010, 0b0000_0011, 0b0000_0100, 0b0000_0101])],
        )
        .await
        .unwrap();

    // Read 5 bits starting at bit offset 9 -> 0b10000...
    let ops = [bitwise::get("bits", 9, 5)];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("bit get(offset=9, size=5): {:?}", rec.bins.get("bits"));

    // Count set bits across the whole blob.
    let ops = [bitwise::count("bits", 0, 40)];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("bit count(all 40 bits): {:?}", rec.bins.get("bits"));

    // Set three bits (OR with a mask at offset 0), then read the first byte.
    let ops = [
        bitwise::or("bits", 0, 8, Value::from(vec![0b1010_0000u8]), &bpolicy),
        bitwise::get("bits", 0, 8),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("bit or + get(first byte): {:?}", rec.bins.get("bits"));

    // Left-shift the second byte by one.
    let ops = [
        bitwise::lshift("bits", 8, 8, 1, &bpolicy),
        bitwise::get("bits", 8, 8),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("bit lshift(byte 1 by 1): {:?}", rec.bins.get("bits"));

    // Integer view: read 8 bits at offset 16 as a signed integer.
    let ops = [bitwise::get_int("bits", 16, 8, false)];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("bit get_int(byte 2): {:?}", rec.bins.get("bits"));
    assert_eq!(rec.bins.get("bits"), Some(&Value::Int(3)));

    client.delete(&wpolicy, &key).await.unwrap();
    client.close().await.unwrap();
}
