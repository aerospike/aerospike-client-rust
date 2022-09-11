use std::collections::HashMap;
use aerospike::{ToBins, ToValue};
use aerospike_core::Value;

#[aerospike_macro::test]
async fn test_derive() {

    #[derive(ToValue, Debug)]
    struct TestStructB {
        pub string: String,
        pub int64: i64,
        pub uint64: u64,
        pub blob: Vec<u8>,
        pub hm: HashMap<String, i64>,
        pub boolean: bool
    }

    #[derive(ToBins, Debug)]
    struct TestStruct {
        pub name: String,
        pub age: i64,
        pub test: TestStructB
    }

    let s = TestStruct {
        name: "testasd".to_string(),
        age: 15,
        test: TestStructB {
            string: "asd123".to_string(),
            int64: 321513,
            uint64: 2623743,
            blob: vec![11, 22, 33, 44],
            hm: HashMap::from([("v1".to_string(), 123), ("v2".to_string(), 234)]),
            boolean: false
        }
    };

    println!("{:?}", s.to_bins())
}