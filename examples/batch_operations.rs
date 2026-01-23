#[macro_use]
extern crate aerospike;
extern crate tokio;

use aerospike::operations;
use aerospike::{BatchPolicy, Bins, Client, ClientPolicy, UDFLang};
use aerospike_core::{
    AdminPolicy, BatchDeletePolicy, BatchOperation, BatchReadPolicy, BatchUDFPolicy,
    BatchWritePolicy, Task,
};
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::env;

#[tokio::main]
async fn main() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("127.0.0.1:3100"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    println!("Connected to Aerospike!");

    let namespace = "test";
    let set_name = generate_random_set_name();

    println!("Using set: {}", set_name);

    // Register UDF
    let udf_body = r#"
function echo(rec, val)
    return val
end
"#;

    println!("Registering UDF...");
    let apolicy = AdminPolicy::default();
    let task = client
        .register_udf(&apolicy, udf_body.as_bytes(), "test_udf.lua", UDFLang::Lua)
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();
    println!("UDF registered successfully!");

    let bpolicy = BatchPolicy::default();

    let bin1 = as_bin!("a", "a value");
    let bin2 = as_bin!("b", "another value");
    let bin3 = as_bin!("c", 42);

    let key1 = as_key!(namespace, &set_name, 1);
    let key2 = as_key!(namespace, &set_name, 2);
    let key3 = as_key!(namespace, &set_name, 3);
    let key4 = as_key!(namespace, &set_name, -1); // key does not exist

    let selected = Bins::from(["a"]);
    let all = Bins::All;
    let none = Bins::None;

    let wops = vec![
        operations::put(&bin1),
        operations::put(&bin2),
        operations::put(&bin3),
    ];

    let rops = vec![
        operations::get_bin(&bin1.name),
        operations::get_bin(&bin2.name),
        operations::get_header(),
    ];

    let bpr = BatchReadPolicy::default();
    let bpw = BatchWritePolicy::default();
    let bpd = BatchDeletePolicy::default();
    let bpu = BatchUDFPolicy::default();

    // WRITE Operations
    println!("\n--- Batch WRITE operations ---");
    let batch = vec![
        BatchOperation::write(&bpw, key1.clone(), wops.clone()),
        BatchOperation::write(&bpw, key2.clone(), wops.clone()),
        BatchOperation::write(&bpw, key3.clone(), wops.clone()),
    ];
    let results = client.batch(&bpolicy, &batch).await.unwrap();
    println!("Write results:");
    dbg!(&results);

    // READ Operations
    println!("\n--- Batch READ operations ---");
    let batch = vec![
        BatchOperation::read(&bpr, key1.clone(), selected),
        BatchOperation::read(&bpr, key2.clone(), all),
        BatchOperation::read(&bpr, key3.clone(), none.clone()),
        BatchOperation::read_ops(&bpr, key3.clone(), rops),
        BatchOperation::read(&bpr, key4.clone(), none.clone()),
    ];
    let results = client.batch(&bpolicy, &batch).await.unwrap();
    println!("Read results:");
    dbg!(&results);

    // UDF Operations
    println!("\n--- Batch UDF operations ---");
    let args1 = &[as_val!(1)];
    let args2 = &[as_val!(2)];
    let args3 = &[as_val!(3)];
    let args4 = &[as_val!(4)];
    let batch = vec![
        BatchOperation::udf(&bpu, key1.clone(), "test_udf", "echo", Some(args1)),
        BatchOperation::udf(&bpu, key2.clone(), "test_udf", "echo", Some(args2)),
        BatchOperation::udf(&bpu, key3.clone(), "test_udf", "echo", Some(args3)),
        BatchOperation::udf(&bpu, key4.clone(), "test_udf", "echo", Some(args4)),
    ];
    let results = client.batch(&bpolicy, &batch).await.unwrap();
    println!("UDF results:");
    dbg!(&results);

    // DELETE Operations
    println!("\n--- Batch DELETE operations ---");
    let batch = vec![
        BatchOperation::delete(&bpd, key1.clone()),
        BatchOperation::delete(&bpd, key2.clone()),
        BatchOperation::delete(&bpd, key3.clone()),
        BatchOperation::delete(&bpd, key4.clone()),
    ];
    let results = client.batch(&bpolicy, &batch).await.unwrap();
    println!("Delete results:");
    dbg!(&results);

    println!("\n✓ All batch operations completed!");
    client.close().await.unwrap();
}

fn generate_random_set_name() -> String {
    let rng = rand::thread_rng();
    rng.sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}
