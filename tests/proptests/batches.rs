use crate::proptest::prelude::*;
use crate::proptest_async;
use crate::{common, proptests::key};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;

use crate::proptests::value::*;

use aerospike::query::*;
use aerospike::*;

use futures::stream::StreamExt;

use crate::proptests::{
    batch_operation::*, bins::*, key::*, operation::*, partition_filter::*, policy::*,
};

prop_compose! {
    pub fn bop_read_bins1()(
        _brp in batch_read_policy(),
    ) -> PropBatchOperation {
        PropBatchOperation::ReadBins(BatchReadPolicy::default(), Bins::All)
    }
}

proptest_async::proptest! {
    #[test]
    async fn batch_read(
        i in 0..10,
        batch_policy in batch_policy(30000),
        ops in many_batch_read_operations(8),
		expected_value in any::<String>(),
    ) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();

        let key = as_key!(namespace, set_name, i);

        let mut as_ops = vec![];
        for op in &ops {
            let as_op = op.to_op(key.clone());

            // Not all reads will have a valid key that leads to content.
            // Does this read have a corresponding write to initialize a bin?

            if i % 2 == 0 {
                let bins = [as_bin!("binName", expected_value.clone()),];
                let write_policy = WritePolicy::default();

                // SAFETY: This is just a test, not production code.
                // Use of unwrap() here is OK; if something goes wrong, we
                // WANT the Rust runtime to panic.

                client.put(&write_policy, &key, &bins).await.unwrap();

				// Make sure write went through using non-batch means.

                let res = client.get(&ReadPolicy::default(), &key, Bins::All).await;
				match res {
					Err(e) => panic!("{:?}", e),
					Ok(res) => {
						if let Some(actual_value) = res.bins.get("binName") {
							if expected_value != actual_value.as_string() {
								panic!("Manual Get: Value for bin 'binName' doesn't match; expected: {expected_value}, got: {actual_value}");
							}
						}
					}
				}
            }

            as_ops.push(as_op);
        }

        let res = client.batch(&batch_policy, &as_ops).await;

        match res {
            Err(e) => panic!("{}", e),
            Ok(res) => {
                // Data validation
                for op in res {
					op.record.map(|r| {
						if let Some(actual_value) = r.bins.get("binName") {
							if expected_value != actual_value.as_string() {
								panic!("Batch Read: Value for bin 'binName' doesn't match; expected: {expected_value}, got: {actual_value}");
							}
						}
					});
                }
            }
        }
    }

    #[test]
    async fn batch_write(
        i in 0..10,
        batch_policy in batch_policy(30000),
        ops in many_batch_write_operations(2),
    ) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();

        // let as_ops: Vec<aerospike::operations::Operation> = ops.into_iter().map(|op| op.to_op()).collect();
        let mut as_ops = vec![];
        for op in &ops {
            let key = as_key!(namespace, set_name, i);
            let as_op = op.to_op(key);
            as_ops.push(as_op);
        }

        let res = client.batch(&batch_policy, &as_ops).await;

        match res {
        //     Err(Error::ServerError(ResultCode::ParameterError, _, _)) => {
        //         if write_policy.respond_per_each_op && ops.into_iter().find(|op| *op == PropOperation::Get).is_some() {
        //             return;
        //         }
        //     }, // it's fine
        //     Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {
        //     },
        //     Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
        //         if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
        //             panic!("{}",e);
        //          }
        //     },
            Err(e @ Error::BatchError(_, ResultCode::GenerationError, _, _)) => {
                // NOTE: there is no way to gain access to the generation_policy
                // from any field accessible to this scope.
                //
                // if batch_policy.generation_policy != GenerationPolicy::None {
                //     return; // it's fine
                // }
                // panic!("{}", e);
            },
            Err(Error::BatchError(_, ResultCode::BinTypeError, _, _)) => {}
            Err(e) => panic!("ERR: {}", e),
            Ok(res) => {}, // println!("OK: {:?}", res),
        }
    }

    #[test]
    async fn batch_delete(
        i in 0..10,
        batch_policy in batch_policy(30000),
        ops in many_batch_delete_operations(2),
    ) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();

        let mut as_ops = vec![];
        for op in &ops {
            let key = as_key!(namespace, set_name, i);
            let as_op = op.to_op(key);
            as_ops.push(as_op);
        }

        let res = client.batch(&batch_policy, &as_ops).await;

        match res {
        //     Err(Error::ServerError(ResultCode::ParameterError, _, _)) => {
        //         if write_policy.respond_per_each_op && ops.into_iter().find(|op| *op == PropOperation::Get).is_some() {
        //             return;
        //         }
        //     }, // it's fine
        //     Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {
        //     },
        //     Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
        //         if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
        //             panic!("{}",e);
        //          }
        //     },
            Err(e @ Error::BatchError(_, ResultCode::GenerationError, _, _)) => {
                // NOTE: there is no way to gain access to the generation_policy
                // from any field accessible to this scope.
                //
                // if batch_policy.generation_policy != GenerationPolicy::None {
                //     return; // it's fine
                // }
                // panic!("{}", e);
            },
            Err(Error::BatchError(_, ResultCode::BinTypeError, _, _)) => {}
            Err(e) => panic!("ERR: {}", e),
            Ok(res) => {}, // println!("OK: {:?}", res),
        }
    }
}
