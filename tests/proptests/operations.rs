use crate::common;
use crate::proptest_async;
use aerospike::*;

use crate::proptests::{key::*, operation::*, policy::*};

proptest_async::proptest! {
    #[test]
    async fn operate(
        write_policy in write_policy(30000),
        key in any_key(common::namespace().into(), "test".into()),
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
            Err(Error::ServerError(ResultCode::ParameterError, _, _)) => {
                if write_policy.respond_per_each_op && ops.into_iter().find(|op| *op == PropOperation::Get).is_some() {
                    return;
                }
            }, // it's fine
            Err(Error::ServerError(ResultCode::BinTypeError, _, _)) => {
            }
            Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {
            },
            Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
                if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::GenerationError, _, _)) => {
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
