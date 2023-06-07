use crate::common;
use aerospike::as_key;
use aerospike::as_val;
use aerospike::derive::readable::ReadableBins;
use aerospike::derive::writable::{WritableBins, WritableValue};
use aerospike::WritePolicy;
use aerospike::{Bins, ReadPolicy, Record, Value};
use std::collections::HashMap;

#[aerospike_macro::test]
async fn derive_writable() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, "derive_struct");

    #[derive(WritableValue)]
    struct TestValue {
        string: String,
        no_val_cdt: Option<String>,
        has_val_cdt: Option<i32>,
        #[aerospike(default = 1.231f64)]
        default_val_cdt: Option<f64>,
        #[aerospike(rename = "int32")]
        int: i32,
        #[aerospike(skip)]
        skipped_bool: bool,
    }

    #[derive(WritableBins)]
    struct TestData<'a> {
        #[aerospike(rename = "renamed_int32")]
        int32: i32,
        string: String,
        refstr: &'a str,
        uint16: u16,
        test: TestValue,
        #[aerospike(skip)]
        no_write: i32,
        #[aerospike(default = "test")]
        default_value: Option<String>,
        no_value: Option<i64>,
        has_value: Option<i64>,
        list: Vec<String>,
    }

    let testv = TestValue {
        string: "asd".to_string(),
        no_val_cdt: None,
        has_val_cdt: Some(123456),
        default_val_cdt: None,
        int: 1234,
        skipped_bool: true,
    };

    let test = TestData {
        int32: 65521,
        string: "string".to_string(),
        refstr: "str",
        uint16: 7,
        test: testv,
        no_write: 123,
        default_value: None,
        no_value: None,
        has_value: Some(12345),
        list: Vec::from(["test1".to_string(), "test2".to_string()]),
    };

    let res = client.put(&WritePolicy::default(), &key, &test).await;
    assert_eq!(res.is_ok(), true, "Derive writer failed");
    let res: Record<HashMap<String, Value>> = client
        .get(&ReadPolicy::default(), &key, Bins::All)
        .await
        .unwrap();

    let bins = res.bins;

    assert_eq!(bins.get("int32"), None, "Derive Bin renaming failed");
    assert_eq!(
        bins.get("renamed_int32"),
        Some(&as_val!(65521)),
        "Derive Bin renaming failed"
    );

    assert_eq!(bins.get("no_value"), None, "Derive Bin empty Option failed");
    assert_eq!(
        bins.get("has_value"),
        Some(&as_val!(12345)),
        "Derive Bin filled Option failed"
    );

    assert_eq!(
        bins.get("default_value"),
        Some(&as_val!("test")),
        "Derive Bin default value failed"
    );

    assert_eq!(bins.get("no_write"), None, "Derive Bin skipping failed");

    assert_eq!(
        bins.get("uint16"),
        Some(&as_val!(7)),
        "Derive Bin encoding failed for uint16"
    );
    assert_eq!(
        bins.get("string"),
        Some(&as_val!("string")),
        "Derive Bin encoding failed for string"
    );
    assert_eq!(
        bins.get("refstr"),
        Some(&as_val!("str")),
        "Derive Bin encoding failed for refstr"
    );

    assert_eq!(
        bins.get("test").is_some(),
        true,
        "Derive Bin encoding failed for cdt map"
    );

    assert_eq!(
        bins.get("list"),
        Some(&Value::List(Vec::from([
            as_val!("test1"),
            as_val!("test2")
        ]))),
        "Derive Bin encoding for list failed"
    );

    if let Some(bin) = bins.get("test") {
        match bin {
            Value::HashMap(m) => {
                assert_eq!(
                    m.get(&as_val!("string")),
                    Some(&as_val!("asd")),
                    "Derive Value encoding failed for string"
                );
                assert_eq!(
                    m.get(&as_val!("no_val_cdt")),
                    None,
                    "Derive Value encoding failed for no_val_cdt"
                );
                assert_eq!(
                    m.get(&as_val!("default_val_cdt")),
                    Some(&as_val!(1.231f64)),
                    "Derive Value encoding failed for default_val_cdt"
                );
                assert_eq!(
                    m.get(&as_val!("int32")),
                    Some(&as_val!(1234)),
                    "Derive Value encoding failed for renamed int"
                );
                assert_eq!(
                    m.get(&as_val!("has_val_cdt")),
                    Some(&as_val!(123456)),
                    "Derive Value encoding failed for has_val_cdt"
                );
                assert_eq!(
                    m.get(&as_val!("skipped_bool")),
                    None,
                    "Derive Value encoding failed for skipped_bool"
                );
            }
            _ => panic!("Derive Bin encoding for map returned wrong type"),
        }
    } else {
        panic!("Derive Bin encoding for map undefined")
    }
}

#[aerospike_macro::test]
async fn derive_readable() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, "derive_struct");

    #[derive(ReadableBins, WritableBins, Clone, Debug)]
    struct TestData {
        string: String,
        int: i64,
        float: f64,
        option: Option<String>,
        list: Vec<String>,
        list_i: Vec<i64>,
        no_val: Option<String>,
        nested_list: Vec<Vec<i64>>,
    }

    let write_data = TestData {
        string: "asdfsd".to_string(),
        int: 1234,
        float: 123.456,
        option: Some("asd".to_string()),
        list_i: vec![1, 5, 8, 9, 15],
        list: vec!["asd".to_string(), "ase".to_string(), "asf".to_string()],
        no_val: None,
        nested_list: vec![vec![1, 2, 3], vec![4, 5, 6]],
    };

    let res = client.put(&WritePolicy::default(), &key, &write_data).await;
    let res: aerospike::errors::Result<Record<TestData>> =
        client.get(&ReadPolicy::default(), &key, Bins::All).await;
    assert_eq!(res.is_ok(), true, "Aerospike derive reader failed");
    let res = res.unwrap().bins;
    assert_eq!(
        res.string, "asdfsd",
        "Aerospike derive reader failed for String"
    );
    assert_eq!(res.int, 1234, "Aerospike derive reader failed for Int");
    assert_eq!(
        res.float, 123.456,
        "Aerospike derive reader failed for Float"
    );
    assert_eq!(
        res.option,
        Some("asd".to_string()),
        "Aerospike derive reader failed for Option Some"
    );
    assert_eq!(
        res.no_val, None,
        "Aerospike derive reader failed for Option None"
    );
    assert_eq!(
        res.list_i,
        vec![1, 5, 8, 9, 15],
        "Aerospike derive reader failed for Int List"
    );
    assert_eq!(
        res.nested_list,
        vec![vec![1, 2, 3], vec![4, 5, 6]],
        "Aerospike derive reader failed for Nested List"
    );
    assert_eq!(
        res.list,
        vec!["asd".to_string(), "ase".to_string(), "asf".to_string()],
        "Aerospike derive reader failed for String List"
    );
}
