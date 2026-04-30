use std::collections::HashMap;

use crate::common;
use crate::proptest::prelude::*;
use crate::proptest_async;

use aerospike::*;

use crate::proptests::{batch_operation::*, operation::*, policy::*};

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
        let set_name = &format!("{}-batch-read", common::prop_setname());

        let write_policy = WritePolicy::default();
        let key = as_key!(namespace, set_name, i);
        client.delete(&write_policy, &key).await.expect("initial deletion of key should succeed");

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

                client.put(&write_policy, &key, &bins).await.expect("initial put should have succeeded");

                std::thread::sleep(std::time::Duration::from_millis(500));

                // Make sure write went through using non-batch means.

                let res = client.get(&ReadPolicy::default(), &key, Bins::All).await;
                match res {
                    Err(e) => panic!("Unexpected Err after get-after-put: {:?}", e),
                    Ok(res) => {
                        if let Some(actual_value) = res.bins.get("binName") {
                            if expected_value != actual_value.as_string() {
                                panic!("get-after-put: Value for bin 'binName' doesn't match; expected: {:?}, got: {:?}", expected_value, actual_value);
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
                                panic!("Batch Read: Value for bin 'binName' doesn't match; expected: {:?}, got: {:?}", expected_value, actual_value);
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
        batch_policy in batch_policy(1000, 5000),
        ops in many_batch_write_operations(2),
    ) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name = &format!("{}-batch-write", common::prop_setname());

        // Delete any previously existing record(s) associated with the key assigned to this test run.
        let write_policy = WritePolicy::default();
        let key = as_key!(namespace, set_name, i);
        client.delete(&write_policy, &key).await.expect("initial deletion of key should succeed");

        let mut puts_to_bins: HashMap<String, Value> = HashMap::new();
        let mut as_ops = vec![];
        for op in &ops {
            // Before validating our write operations, we want to initialize the
            // bins that have been randomly chosen for the property test.
            //
            // First, we need to discover the names of the bins that proptest-rs
            // has chosen for this run.

            match op {
                PropBatchOperation::Write(_, ref ops) => {
                    for sub_op in ops {
                        match sub_op {
                            PropOperation::Put(ref bin) => {
                                puts_to_bins.insert(bin.name.clone(), bin.value.clone());
                            }
                           _ => (),
                        }
                    }
                }
                _ => (),
            }

            // Now that we have a list of bins to initialize, let's actually
            // set a default value for each of these bins.	If a batch write
            // succeeds, the bin value (and type) will be changed according to
            // the write in the operation list.

            for (bin_name, _) in &puts_to_bins {
                let bins = [as_bin!(bin_name.clone(), 1)];
                client.put(&write_policy, &key, &bins).await.expect("initializing put should work");
            }

            // Finally, build the batch operation list.

            let as_op = op.to_op(key.clone());
            as_ops.push(as_op);
        }

        // Invoke the batch operation.

        let res = client.batch(&batch_policy, &as_ops).await;

        match res {
            Err(e) => panic!("ERR: {}", e),
            Ok(results) => {
                // `res` is a list of BatchRecord structures.
                // For each result,
                for result in results {
                    // Check the result code to filter out "errors" that aren't
                    // really errors.
                    match result.result_code {
                        // If a result has been filtered out,
                        Some(ResultCode::FilteredOut) => {
                            // skip the check; it's been filtered out.
                        }

                        // If a key has not been found,
                        Some(ResultCode::KeyNotFoundError) => {
                            // then skip this record too.
                        }

                        // Otherwise
                        Some(ResultCode::Ok) => {
                            // The results of the corresponding batch operation
                            // must have succeeded, somehow.
                            match result.record {
                                Some(record) => {
                                    // This record will have one or more bins associated with it.
                                    // For each bin in the hashmap of bins,
                                    // grab the bin name; ignore the value though,
                                    // as it will always be nil/empty.
                                    for (updated_name, _) in record.bins {
                                        // Get current contents of the bin from the database.
                                        // This cannot fail, since we just confirmed at least one bin.
                                        let read_result = client
                                                .get(
                                                    &ReadPolicy::default(),
                                                    &key,
                                                    Bins::Some(vec![updated_name.clone()])
                                                )
                                                .await
                                                .expect("validation get should work");
                                        // If the contents of the bin does not match the expected value, panic.
                                        for (candidate_name, candidate_value) in &read_result.bins {
                                            let maybe_expected_value = puts_to_bins.get(&String::from(candidate_name));
                                            match maybe_expected_value {
                                                // If the bin name was stored in puts_to_bins, then it must have been
                                                // provided by proptests-rs.  That means we must have previously
                                                // client.put() to establish a default record.  Check to make sure the
                                                // bin's current contents matches proptest-rs's expected value.
                                                Some(expected_value) => {
                                                    if *candidate_name == updated_name {
                                                        if *candidate_value != *expected_value {
                                                            panic!("For bin \"{:?}\"\n  Expected: {:?}\n  Actual: {:?}\n", updated_name, expected_value, candidate_value);
                                                        }
                                                    }
                                                }

                                                // If we found a bin in the reply that does not exist in the puts_to_bins map,
                                                // it is because it was created by the server in response to an alternative operation
                                                // that we're not checking for here (e.g., APPEND, ADD, etc.).  Although it looks like
                                                // it, these are _not_ errors.  But we will skip validation here, because properly validating
                                                // depends on the precise context and data type involved.
                                                _ => { }
                                            }
                                        }
                                    }
                                }

                                _ => panic!("I'd expect at least one bin to verify against")
                            }
                        }

                        _ => {
                            eprintln!("WARNING: Unknown result code {:?}", result.result_code);
                        }
                    }
                }
            }
        }
    }

    #[test]
    async fn batch_delete(
        i in 0..10,
        ops in many_batch_delete_operations(2),
    ) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name = &format!("{}-batch-delete", common::prop_setname());

        let write_policy = WritePolicy::default();
        let key = as_key!(namespace, set_name, i);

        client.delete(&write_policy, &key).await.expect("initial records deletion should work");
        if i % 2 == 0 {
            let bins = [as_bin!("bin2delete", 100)];
            client.put(&write_policy, &key, &bins).await.expect("planting a record to delete expected to work");
        }

        let mut as_ops = vec![];
        for op in &ops {
            let as_op = op.to_op(key.clone());
            as_ops.push(as_op);
        }

        let res = client.batch(&BatchPolicy::default(), &as_ops).await;

        match res {
            Err(e) => panic!("ERR: {}", e),
            Ok(results) => {
                if results.len() != ops.len() {
                    panic!("Batch results length differ: expected {} got {}", ops.len(), results.len());
                }

                for rec_index in 0..results.len() {
                    let record = results[rec_index].clone();
                    let ops_record = ops[rec_index].clone();
                    let bdp = match ops_record {
                        PropBatchOperation::Delete(ref bdp) => bdp.clone(),
                        _ => panic!("Unexpected batch operation"),
                    };

                    match record.result_code {
                        Some(ResultCode::Ok) => (),
                        Some(ResultCode::FilteredOut) => (),

                        Some(ResultCode::KeyNotFoundError) => {
                            // Key should not be found for i % 2 != 0, since we
                            // did not create one.  Otherwise, the key should
                            // have existed and should have been deleted.
                            if i % 2 == 0 {
                                panic!("Target record deleted was never written first, but should have been.");
                            }
                        }

                        Some(ResultCode::GenerationError) => {
                            let db_record = client.get(&ReadPolicy::default(), &key, Bins::All).await.expect("diagnostic read should work");
                            match bdp.generation_policy {
                                GenerationPolicy::None => { }

                                GenerationPolicy::ExpectGenGreater => {
                                    // The logic on this seems backwards, but it makes sense when you consider things from a database
                                    // restore operation's point of view.  The idea is that if a backup restore's record has a _greater_
                                    // generation than what already sits on the server, then it must be _newer_ data, and thus, eligible
                                    // for restoration.  This means that the write/delete will succeed if the current record's generation
                                    // is *less than or equal* to the provided generation, since the *provided* generation is the backup's
                                    // generation.
                                    if db_record.generation <= bdp.generation {
                                        panic!("ERROR: server record generation is {}, policy is looking for <= {}, but still got GenerationError.",
                                                db_record.generation, bdp.generation);
                                    }
                                }

                                GenerationPolicy::ExpectGenEqual => {
                                    if db_record.generation == bdp.generation {
                                        panic!("ERROR: record generations match, policy looking for equality, but still got GenerationError.");
                                    }
                                }
                            }
                        }

                        _ => {
                            panic!("Unexpected result code: {:?}", record.result_code);
                        }
                    }
                }
            }
        }
    }
}
