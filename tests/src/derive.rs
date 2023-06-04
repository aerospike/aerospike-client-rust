use crate::common;
use aerospike::as_key;
use aerospike::as_val;
use aerospike::WritePolicy;
use aerospike::{Bins, ReadPolicy, Record, Value};
use aerospike::{WritableBins, WritableValue};
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
}
