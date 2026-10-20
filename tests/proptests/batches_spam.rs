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
use crate::proptest::prelude::*;
use crate::proptest_async;
use crate::proptests::{batch_operation::*, policy::*};

use aerospike::*;

const STRING_DEFAULT: &str = "aerospike default value";

prop_compose! {
    pub fn bop_read_bins1()(
        _brp in batch_read_policy(),
    ) -> PropBatchOperation {
        PropBatchOperation::ReadBins(BatchReadPolicy::default(), Bins::All)
    }
}

proptest_async::proptest! {
    #[test]
    async fn batch_udf(
        i in 0..10_000,
        batch_policy in batch_policy(1000, 5000),
        ops in many_batch_udf_operations(5),
    ) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();

        let key = as_key!(namespace, set_name, i);

        let mut as_ops = vec![];
        for op in &ops {
            let as_op = op.to_op(key.clone());
            as_ops.push(as_op);
        }

        // Invoke the batch operation.

        let mut as_ops = as_ops;
        let res = client.batch(&batch_policy, &mut as_ops).await;

        match res {
            Err(e)
                if matches!(
                    e.server_result_code(),
                    Some(
                        ResultCode::FilteredOut
                            | ResultCode::KeyBusy
                            | ResultCode::BinTypeError
                            | ResultCode::BinNameTooLong
                    )
                ) => {}
            Err(e) => panic!("ERR: {}", e),
            Ok(_) => (),
        }
    }
}
