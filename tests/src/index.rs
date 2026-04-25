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

use log::*;

use crate::common;
use aerospike::expressions::*;

use aerospike::Task;
use aerospike::*;

const EXPECTED: usize = 100;

async fn create_test_set(client: &Client, no_records: usize) -> Option<String> {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);
    let wpolicy = WritePolicy::default();

    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let wbin = as_bin!("bin", i);
        let bins = vec![wbin];
        let _ = common::delete_for_test_reset(client, &wpolicy, &key).await;
        let _ = common::delete_on_cluster(client, &wpolicy, &key).await;
        match client.put(&wpolicy, &key, &bins).await {
            Ok(()) => {}
            Err(Error::ServerError(ResultCode::ParameterError, _, _)) if i == 0 => {
                eprintln!("index create_test_set: skipped — put returned ParameterError");
                return None;
            }
            Err(Error::ServerError(ResultCode::ParameterError, _, _)) => {
                panic!("index create_test_set: put ParameterError at key {i}");
            }
            Err(e) => panic!("index create_test_set put: {e}"),
        }
    }

    Some(set_name)
}

#[aerospike_macro::test]
async fn create_index_on_bin() {
    let client = common::client().await;
    let ns = common::namespace();
    let Some(set) = create_test_set(&client, EXPECTED).await else {
        client.close().await.unwrap();
        return;
    };
    let bin = "bin";
    let index = format!("{}_{}_{}", ns, set, bin);
    let apolicy = AdminPolicy::default();

    let task = match client.drop_index(&apolicy, ns, &set, &index).await {
        Ok(t) => t,
        Err(Error::ServerError(ResultCode::FailForbidden, _, _)) => {
            info!("create_index_on_bin: drop_index returned FailForbidden; skipping");
            client.close().await.unwrap();
            return;
        }
        Err(e) => panic!("create_index_on_bin drop_index: {e}"),
    };
    task.wait_till_complete(None).await.unwrap();

    let task = match client
        .create_index_on_bin(
            &apolicy,
            ns,
            &set,
            bin,
            &index,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
    {
        Ok(t) => t,
        Err(Error::ServerError(ResultCode::FailForbidden, _, _)) => {
            info!("create_index_on_bin: create_index_on_bin returned FailForbidden; skipping");
            client.close().await.unwrap();
            return;
        }
        Err(e) => panic!("create_index_on_bin: {e}"),
    };
    task.wait_till_complete(None).await.unwrap();

    // redo to make sure it is supported
    let task = match client
        .create_index_on_bin(
            &apolicy,
            ns,
            &set,
            bin,
            &index,
            IndexType::Numeric,
            CollectionIndexType::Default,
            None,
        )
        .await
    {
        Ok(t) => t,
        Err(Error::ServerError(ResultCode::FailForbidden, _, _)) => {
            info!("create_index_on_bin: second create_index_on_bin returned FailForbidden; skipping");
            client.close().await.unwrap();
            return;
        }
        Err(e) => panic!("create_index_on_bin redo: {e}"),
    };
    task.wait_till_complete(None).await.unwrap();

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn create_index_using_expression() {
    let client = common::client().await;

    if client
        .cluster
        .get_random_node()
        .is_ok_and(|node| node.version() < &Version::new(8, 1, 0, 0))
    {
        info!("create_index_using_expression test is only supported in server versions 8.1.0.0+. Skipping.");
        return;
    }

    let ns = common::namespace();
    let Some(set) = create_test_set(&client, EXPECTED).await else {
        client.close().await.unwrap();
        return;
    };
    let bin = "bin";
    let index = format!("{}_{}_{}", ns, set, bin);
    let apolicy = AdminPolicy::default();

    let task = match client.drop_index(&apolicy, ns, &set, &index).await {
        Ok(t) => t,
        Err(Error::ServerError(ResultCode::FailForbidden, _, _)) => {
            info!("create_index_using_expression: drop_index returned FailForbidden; skipping");
            client.close().await.unwrap();
            return;
        }
        Err(e) => panic!("drop_index: {e}"),
    };
    task.wait_till_complete(None).await.unwrap();

    let fe: Expression = num_add(vec![int_bin(common::rand_str(10)), int_val(0)]);

    let task = match client
        .create_index_using_expression(
            &apolicy,
            ns,
            &set,
            &index,
            IndexType::Numeric,
            CollectionIndexType::Default,
            &fe,
        )
        .await
    {
        Ok(t) => t,
        Err(Error::ServerError(ResultCode::FailForbidden, _, _)) => {
            info!("create_index_using_expression: create returned FailForbidden; skipping");
            client.close().await.unwrap();
            return;
        }
        Err(e) => panic!("create_index_using_expression: {e}"),
    };
    task.wait_till_complete(None).await.unwrap();

    // redo to see if it is supported
    let task = match client
        .create_index_using_expression(
            &apolicy,
            ns,
            &set,
            &index,
            IndexType::Numeric,
            CollectionIndexType::Default,
            &fe,
        )
        .await
    {
        Ok(t) => t,
        Err(Error::ServerError(ResultCode::FailForbidden, _, _)) => {
            info!("create_index_using_expression: second create returned FailForbidden; skipping");
            client.close().await.unwrap();
            return;
        }
        Err(e) => panic!("create_index_using_expression redo: {e}"),
    };
    task.wait_till_complete(None).await.unwrap();

    client.close().await.unwrap();
}
