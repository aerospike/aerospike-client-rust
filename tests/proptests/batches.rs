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

proptest_async::proptest! {
    #[test]
    async fn batch_read(
        i in 0..10,
        batch_policy in batch_policy(30000),
        ops in many_batch_read_operations(2),
    ) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();

        // let now = aerospike_rt::time::Instant::now();
        // eprintln!("PRPBAT001 It is now {:?}", now.elapsed());

        // let as_ops: Vec<aerospike::operations::Operation> = ops.into_iter().map(|op| op.to_op()).collect();
        let mut as_ops = vec![];
        for op in &ops {
            let key = as_key!(namespace, set_name, i);
            let as_op = op.to_op(key);
            as_ops.push(as_op);
        }

        // eprintln!("PRPBAT002 Submitting batch operation at {:?}", now.elapsed());
        let res = client.batch(&batch_policy, &as_ops).await;
        // eprintln!("PRPBAT003 Batch returned in {:?}", now.elapsed());

        match res {
        //     Err(Error::ServerError(ResultCode::ParameterError, _, _)) => {
        //         if write_policy.respond_per_each_op && ops.into_iter().find(|op| *op == PropOperation::Get).is_some() {
        //             return;
        //         }
        //     }, // it's fine
        //     Err(Error::ServerError(ResultCode::BinTypeError, _, _)) => {
        //     }
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
            Err(e) => panic!("{}", e),
            Ok(res) => (), //println!("OK"),
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

        let now = aerospike_rt::time::Instant::now();
        eprintln!("PRPBAT001 It is now {:?}", now.elapsed());

        // let as_ops: Vec<aerospike::operations::Operation> = ops.into_iter().map(|op| op.to_op()).collect();
        let mut as_ops = vec![];
        for op in &ops {
            let key = as_key!(namespace, set_name, i);
            let as_op = op.to_op(key);
            as_ops.push(as_op);
        }

        eprintln!("PRPBAT002 Submitting batch operation at {:?}", now.elapsed());
        let res = client.batch(&batch_policy, &as_ops).await;
        eprintln!("PRPBAT003 Batch returned in {:?}", now.elapsed());

        match res {
        //     Err(Error::ServerError(ResultCode::ParameterError, _, _)) => {
        //         if write_policy.respond_per_each_op && ops.into_iter().find(|op| *op == PropOperation::Get).is_some() {
        //             return;
        //         }
        //     }, // it's fine
        //     Err(Error::ServerError(ResultCode::BinTypeError, _, _)) => {
        //     }
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
            Err(e) => panic!("{}", e),
            Ok(res) => (), //println!("OK"),
        }
    }
}
