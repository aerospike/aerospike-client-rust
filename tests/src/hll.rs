use crate::common;
use env_logger;

use aerospike::operations::hll;
use aerospike::operations::hll::HLLPolicy;
use aerospike::{as_key, as_list, as_val, Bins, FloatValue, ReadPolicy, Value, WritePolicy};

#[aerospike_macro::test]
async fn hll() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let key = as_key!(namespace, set_name, "test");

    let hpolicy = HLLPolicy::default();
    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();

    let ops = &vec![hll::init(&hpolicy, "bin", 4)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let v = vec![Value::from("asd123")];
    let ops = &vec![hll::add(&hpolicy, "bin", &v)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Int(1),
        "Register update did not match"
    );

    let ops = &vec![hll::get_count("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Int(1),
        "HLL Count did not match"
    );

    let ops = &vec![hll::init_with_min_hash(&hpolicy, "bin2", 8, 0)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let ops = &vec![hll::fold("bin2", 6)];
    client.operate(&wpolicy, &key, ops).await.unwrap();

    let v2 = vec![Value::from("123asd")];
    let ops = &vec![hll::add(&hpolicy, "bin2", &v2)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin2").unwrap(),
        Value::Int(1),
        "Register update did not match"
    );

    let ops = &vec![hll::describe("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_list!(4, 0),
        "Index bits did not match"
    );

    let rec = client
        .get(&rpolicy, &key, Bins::from(["bin2"]))
        .await
        .unwrap();
    let bin2val = vec![rec.bins.get("bin2").unwrap().clone()];

    let ops = &vec![hll::get_intersect_count("bin", &bin2val)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::from(0),
        "Intersect Count is wrong"
    );

    let ops = &vec![hll::get_union_count("bin", &bin2val)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::from(2),
        "Union Count is wrong"
    );

    let ops = &vec![hll::get_union("bin", &bin2val)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    let val = Value::HLL(vec![
        0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);
    assert_eq!(*rec.bins.get("bin").unwrap(), val, "Union does not match");

    let ops = &vec![hll::refresh_count("bin")];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Int(1),
        "HLL Refresh Count did not match"
    );

    let ops = &vec![
        hll::set_union(&hpolicy, "bin", &bin2val),
        hll::get_count("bin"),
    ];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::from(2),
        "Written Union count does not match"
    );

    let ops = &vec![hll::get_similarity("bin", &bin2val)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Float(FloatValue::F64(4602678819172646912)),
        "Similarity failed"
    );

    client.close().await.unwrap();
}
