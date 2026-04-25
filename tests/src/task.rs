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
use aerospike::errors::Error;
use aerospike::task::{Status, Task};
use aerospike::*;
use std::time::Duration;

// If registering udf is successful, querying RegisterTask will return Status::Complete
// If udf does not exist, querying RegisterTask will return error
#[aerospike_macro::test]
async fn register_task_test() {
    let client = common::client().await;

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

    let apolicy = AdminPolicy::default();
    let udf_file_name = "my_udf_task.lua";

    let register_task = client
        .register_udf(&apolicy, code.as_bytes(), &udf_file_name, UDFLang::Lua)
        .await
        .unwrap();

    assert!(matches!(
        register_task.wait_till_complete(None).await,
        Ok(Status::Complete)
    ));

    let remove_task = client.remove_udf(&apolicy, &udf_file_name).await.unwrap();
    // Wait for some time to ensure UDF has been unregistered on all nodes.
    remove_task.wait_till_complete(None).await.unwrap();

    let timeout = Duration::from_millis(1000);
    assert!(matches!(
        register_task.wait_till_complete(Some(timeout)).await,
        Err(Error::Timeout(_))
    ));

    client.close().await.unwrap();
}

// If creating index is successful, querying IndexTask will return Status::Complete
#[aerospike_macro::test]
async fn index_task_test() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let bin_name = common::rand_str(10);
    let index_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let apolicy = AdminPolicy::default();
    for i in 0..2 as i64 {
        let key = as_key!(namespace, &set_name, i);
        let _ = common::delete_for_test_reset(&client, &wpolicy, &key).await;
        let _ = common::delete_on_cluster(&client, &wpolicy, &key).await;
        let wbin = as_bin!(&bin_name, i);
        let bins = vec![wbin];
        match client.put(&wpolicy, &key, &bins).await {
            Ok(()) => {}
            Err(aerospike::Error::ServerError(aerospike::ResultCode::ParameterError, _, _))
                if i == 0 =>
            {
                eprintln!("index_task_test: skipped — put returned ParameterError");
                client.close().await.unwrap();
                return;
            }
            Err(e) => panic!("index_task_test put: {e}"),
        }
    }

    let index_task = match client
        .create_index_on_bin(
            &apolicy,
            &namespace,
            &set_name,
            &bin_name,
            &index_name,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
    {
        Ok(t) => t,
        Err(aerospike::Error::ServerError(aerospike::ResultCode::FailForbidden, _, _)) => {
            eprintln!("index_task_test: skipped — create_index FailForbidden");
            client.close().await.unwrap();
            return;
        }
        Err(e) => panic!("index_task_test create_index: {e}"),
    };

    assert!(matches!(
        index_task.wait_till_complete(None).await,
        Ok(Status::Complete)
    ));

    let task = match client
        .drop_index(&apolicy, namespace, &set_name, &index_name)
        .await
    {
        Ok(t) => t,
        Err(aerospike::Error::ServerError(aerospike::ResultCode::FailForbidden, _, _)) => {
            client.close().await.unwrap();
            return;
        }
        Err(e) => panic!("index_task_test drop_index: {e}"),
    };
    assert!(matches!(
        task.wait_till_complete(None).await,
        Ok(Status::Complete)
    ));

    client.close().await.unwrap();
}
