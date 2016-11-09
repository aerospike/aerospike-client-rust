// Copyright 2015-2016 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

extern crate aerospike;
extern crate env_logger;

use aerospike::WritePolicy;
use aerospike::{Key, Bin};
use aerospike::UDFLang;
use aerospike::value::*;

use std::thread;
use std::time::Duration;

use common1;

#[test]
fn udf() {
    let _ = env_logger::init();

    let ref client = common1::GLOBAL_CLIENT;
    let namespace: &str = &common1::AEROSPIKE_NAMESPACE;
    let set_name = &common1::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let wbin = as_bin!("bin", 10);
    let bins = vec![&wbin];
    client.put(&wpolicy, &key, &bins).unwrap();

    let udf_body1 = r#"
function func_div(rec, div)
  local ret = map()
  local x = rec['bin']
  rec['bin2'] = math.floor(x / div)
  aerospike:update(rec)
  ret['status'] = 'OK'
  ret['res'] = math.floor(x / div)
  return ret
end
"#;

    let udf_body2 = r#"
function echo(rec, val)
  return val
end
"#;

    client.register_udf(&wpolicy,
                        udf_body1.as_bytes(),
                        "test_udf1.lua",
                        UDFLang::Lua)
          .unwrap();
    client.register_udf(&wpolicy,
                        udf_body2.as_bytes(),
                        "test_udf2.lua",
                        UDFLang::Lua)
          .unwrap();

    // FIXME: replace sleep with wait task
    thread::sleep(Duration::from_millis(3000));

    let res = client.execute_udf(&wpolicy,
                                 &key,
                                 "test_udf2",
                                 "echo",
                                 Some(&[as_val!("ha ha...")]));
    assert_eq!(res, Ok(Some(as_val!("ha ha..."))));

    let res = client.execute_udf(&wpolicy, &key, "test_udf1", "func_div", Some(&[as_val!(2)]));
    if let Ok(Some(Value::HashMap(values))) = res {
        assert_eq!(values.get(&as_val!("status")), Some(&as_val!("OK")));
        assert_eq!(values.get(&as_val!("res")), Some(&as_val!(5)));
    } else {
        panic!("UDF function did not return expected value");
    }
}
