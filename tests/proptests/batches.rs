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
        _batch_policy in batch_policy(30000),
        ops in bop_read_bins1(),
    ) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();

        println!("{}",i);
        let key = as_key!(namespace, set_name, i);

        // let as_ops: Vec<aerospike::operations::Operation> = ops.into_iter().map(|op| op.to_op()).collect();
        let mut as_ops = vec![];
        // for op in &ops {
            let as_op = ops.to_op(key.clone());

            // Not all reads will have a valid key that leads to content.
            // Does this read have a corresponding write to initialize a bin?

            if i % 2 == 0 {
                let bin = as_bin!("binName", "binValue");
                let bins = [bin];
                let write_policy = WritePolicy::default();

                // TO ensure maximum chance of successfully reading from the record,
                // we set filter expressions to None.  Here, we set it to None for
                // the Put operation explicitly.  For the reads, we rely on the
                // "false" parameter of many_batch_read_operations(), elsewhere.
                // The idea is that if both read and write policies have equal
                // filter expressions (namely, none at all), there should be no
                // reason for a filter operation to exclude any records.

                // write_policy.base_policy.filter_expression = None;

                // SAFETY: This is just a test, not production code.
                // Use of unwrap() here is OK; if something goes wrong, we
                // WANT the Rust runtime to panic.

                println!("WRITING: {}", i);
                client.put(&write_policy, &key, &bins).await.unwrap();
                let res = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();
                println!("THE RECORD IS: {:?}", res);
            }

            as_ops.push(as_op);
        // }

        println!("{:?}", ops);
        let batch_policy = BatchPolicy::default();
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
        //     Err(e @ Error::ServerError(ResultCode::GenerationError, _, _)) => {
        //         if write_policy.generation_policy != GenerationPolicy::None {
        //             return; // it's fine
        //         }
        //         panic!("{}", e);
        //     },
            Err(e @ Error::BatchError(index, ResultCode::BinTypeError, _, _)) => {
                panic!("Batch result ERROR: index={} {:?}", index, e)
            }
            Err(e) => panic!("{}", e),
            Ok(res) => {
                // Data validation
                for op in res {
                    eprintln!("OP: {:?}", op.record.map(|r| r.bins.len()));
                }
            }, //println!("OK"),
        }

        eprintln!("--------------------");
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
