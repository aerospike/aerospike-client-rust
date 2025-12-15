use crate::common;
use crate::proptest::prelude::*;
use crate::proptest_async;

use aerospike::*;

use crate::proptests::{ batch_operation::*, policy::*, };

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
    async fn batch_read(
        i in 0..10,
        batch_policy in batch_policy(1000, 5000),
        ops in many_batch_read_operations(8),
        expected_value in any::<String>(),
    ) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();

        let key = as_key!(namespace, set_name, i);

        // Randomize the bin value to test for even further per iteration.

        let mut expected_value = expected_value.clone();
        expected_value.push_str(&format!("{}", i));

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
                                panic!("Manual Get: Value for bin 'binName' doesn't match; expected: {}, got: {}", expected_value, actual_value);
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
                                panic!("Batch Read: Value for bin 'binName' doesn't match; expected: {}, got: {}", expected_value, actual_value);
                            }
                        }
                    });
                }
            }
        }
    }

    #[test]
    async fn batch_write(
        i in 0..10_000,
        batch_policy in batch_policy(1000, 5000),
        ops in many_batch_write_operations(25),
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
            Err(Error::BatchError(_, ResultCode::KeyBusy, _, _)) => {}
            Err(Error::ClientError(_)) => {}
            Err(Error::BatchError(_, ResultCode::BinTypeError, _, _)) => {}
            Err(e) => panic!("ERR: {:#?}", e),
            Ok(_) => (),
        }
    }

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
