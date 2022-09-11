// Copyright 2015-2020 Aerospike, Inc.
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

use crate::common;
use env_logger;

use aerospike::*;

#[aerospike_macro::test]
async fn execute_udf() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, 1);
    let wbin = as_bin!("bin", 10);
    let bins = vec![wbin];
    client.put(&wpolicy, &key, &bins).await.unwrap();

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

    let task = client
        .register_udf(udf_body1.as_bytes(), "test_udf1.lua", UDFLang::Lua)
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    let task = client
        .register_udf(udf_body2.as_bytes(), "test_udf2.lua", UDFLang::Lua)
        .await
        .unwrap();
    task.wait_till_complete(None).await.unwrap();

    let res = client
        .execute_udf(
            &wpolicy,
            &key,
            "test_udf2",
            "echo",
            Some(&[as_val!("ha ha...")]),
        )
        .await;
    assert_eq!(Some(as_val!("ha ha...")), res.unwrap());

    let res = client
        .execute_udf(&wpolicy, &key, "test_udf1", "func_div", Some(&[as_val!(2)]))
        .await;
    if let Ok(Some(Value::HashMap(values))) = res {
        assert_eq!(values.get(&as_val!("status")), Some(&as_val!("OK")));
        assert_eq!(values.get(&as_val!("res")), Some(&as_val!(5)));
    } else {
        panic!("UDF function did not return expected value");
    }

    let res = client
        .execute_udf(&wpolicy, &key, "test_udf1", "no_such_function", None)
        .await;
    if let Err(Error(ErrorKind::UdfBadResponse(response), _)) = res {
        assert_eq!(response, "function not found".to_string());
    } else {
        panic!("UDF function did not return the expected error");
    }

    client.close().await.unwrap();
}
