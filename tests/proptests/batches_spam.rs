use crate::common;
use crate::proptest::prelude::*;
use crate::proptest_async;
use crate::proptests::{batch_operation::*, policy::*};

use aerospike::*;

use crate::proptests::{batch_operation::*, policy::*};

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

        let res = client.batch(&batch_policy, &as_ops).await;

        match res {
            Err(Error::BatchError(_, ResultCode::FilteredOut, _, _)) => {}
            Err(Error::BatchError(_, ResultCode::KeyBusy, _, _)) => {}
            Err(Error::BatchError(_, ResultCode::BinTypeError, _, _)) => {}
            Err(e) => panic!("ERR: {}", e),
            Ok(_) => (),
        }
    }
}
