// Copyright 2015-2026 Aerospike, Inc.
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
use crate::proptest_async;
use aerospike::*;

use crate::proptests::{key::*, operation::*, policy::*};

proptest_async::proptest! {
    #[test]
    async fn operate(
        write_policy in write_policy(1000, 5000),
        key in any_key(common::namespace().into(), common::prop_setname().into()),
        ops in many_operations(50),
    ) {
        let client = common::singleton_client().await;

        // let now = aerospike_rt::time::Instant::now();

        // let as_ops: Vec<aerospike::operations::Operation> = ops.into_iter().map(|op| op.to_op()).collect();
        let mut as_ops = vec![];
        for op in &ops {
            let as_op = op.to_op();
            as_ops.push(as_op);
        }

        let res = client.operate(&write_policy, &key, &as_ops).await;
        // println!("Operate succeeded in {:?}", now.elapsed());

        match res {
            Err(e) if e.server_result_code() == Some(ResultCode::ParameterError)
                && write_policy.respond_per_each_op && ops.into_iter().find(|op| *op == PropOperation::Get).is_some() => {
                    return;
                }, // it's fine
            Err(e) if e.server_result_code() == Some(ResultCode::BinTypeError) => {
            }
            Err(e) if e.server_result_code() == Some(ResultCode::KeyNotFoundError) => {
            },
            Err(e) if e.server_result_code() == Some(ResultCode::KeyExistsError)
                && write_policy.record_exists_action != RecordExistsAction::CreateOnly => {
                    panic!("{}",e);
                 },
            Err(e) if e.server_result_code() == Some(ResultCode::GenerationError) => {
                if write_policy.generation_policy != GenerationPolicy::None {
                    return; // it's fine
                }
                panic!("{}", e);
            },
            Err(e) => panic!("{}", e),
            _ => (),
        }
    }
}
