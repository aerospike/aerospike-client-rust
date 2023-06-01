use std::collections::HashMap;
use aerospike::{WritableBins, WritableValue};
use aerospike::WritePolicy;
use aerospike::as_key;
use aerospike::{Bins, ReadPolicy, Value};
use aerospike::as_val;
use crate::common;

#[aerospike_macro::test]
async fn derive_writable() {
    let client = common::client().await;
    let namespace: &str = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, "derive_struct");

    #[derive(WritableValue)]
    struct TestValue {
        string: String,
        int: i32,
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
        int: 1234
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
    let res = client.get(&ReadPolicy::default(), &key, Bins::All).await;

    let bins = res.unwrap().bins;
    println!("{:?}", bins);
    assert_eq!(bins.get("int32"), None, "Derive Bin renaming failed");
    assert_eq!(bins.get("renamed_int32"), Some(&as_val!(65521)), "Derive Bin renaming failed");

    assert_eq!(bins.get("no_value"), None, "Derive Bin empty Option failed");
    assert_eq!(bins.get("has_value"), Some(&as_val!(12345)), "Derive Bin filled Option failed");

    assert_eq!(bins.get("default_value"), Some(&as_val!("test")), "Derive Bin default value failed");

    assert_eq!(bins.get("no_write"), None, "Derive Bin skipping failed");

    assert_eq!(bins.get("uint16"), Some(&as_val!(7)), "Derive Bin encoding failed for uint16");
    assert_eq!(bins.get("string"), Some(&as_val!("string")), "Derive Bin encoding failed for string");
    assert_eq!(bins.get("refstr"), Some(&as_val!("str")), "Derive Bin encoding failed for refstr");
}