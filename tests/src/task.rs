use crate::common;
use aerospike::*;
use aerospike::task::{Status, Task};
use aerospike::errors::{ErrorKind};
use std::{thread};
use std::time::{Duration};


// TODO: replace matches_override with matches when upgrade to 1.42.0
#[macro_export]
macro_rules! matches_override {
    ($expression:expr, $($pattern:tt)+) => {
        match $expression {
            $($pattern)+ => true,
            _ => false
        }
    }
}

// If registering udf is successful, querying RegisterTask will return Status::Complete 
// If udf does not exist, querying RegisterTask will return error
#[test]
fn register_task_test() {
    let client = common::client();

    let code = r#"
    local function putBin(r,name,value)
        if not aerospike:exists(r) then aerospike:create(r) end
        r[name] = value
        aerospike:update(r)
    end
    function writeBin(r,name,value)
        putBin(r,name,value)
    end
    "#;


    let udf_name = common::rand_str(10);
    let udf_file_name = udf_name.clone().to_owned() + ".LUA";

    let register_task = client.register_udf(
        &WritePolicy::default(),
        code.as_bytes(),
        &udf_file_name,
        UDFLang::Lua).unwrap();

    assert!(matches_override!(register_task.wait_till_complete().unwrap(), Status::Complete));

    client.remove_udf(
        &WritePolicy::default(),
        &udf_name,
        UDFLang::Lua
    ).unwrap();


    thread::sleep(Duration::from_secs(5));

    assert!(matches_override!(
        register_task.wait_till_complete(),
        Err(Error(ErrorKind::Connection(_), _))
    ));
}


// If creating index is successful, querying IndexTask will return Status::Complete 
#[test]
fn index_task_test() {
    let client = common::client();
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let bin_name = common::rand_str(10);
    let index_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..2 as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin = as_bin!(&bin_name, i);
        let bins = vec![&wbin];
        client.put(&wpolicy, &key, &bins).unwrap();
    }

    let index_task = client.create_index(
        &wpolicy,
        &namespace,
        &set_name,
        &bin_name,
        &index_name,
        IndexType::Numeric,
    ).unwrap();

    assert!(matches_override!(
        index_task.wait_till_complete(),
        Ok(Status::Complete)
    ));
}















