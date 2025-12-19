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
        let set_name: &str = common::prop_setname();

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

                // SAFETY: This is just a test, not production code.
                // Use of unwrap() here is OK; if something goes wrong, we
                // WANT the Rust runtime to panic.

                client.put(&write_policy, &key, &bins).await.expect("initial put should have succeeded");

                // Make sure write went through using non-batch means.

                let res = client.get(&ReadPolicy::default(), &key, Bins::All).await;
                match res {
                    Err(e) => panic!("{:?}", e),
                    Ok(res) => {
                        if let Some(actual_value) = res.bins.get("binName") {
                            if expected_value != actual_value.as_string() {
                                panic!("Manual Get: Value for bin 'binName' doesn't match; expected: {:?}, got: {:?}", expected_value, actual_value);
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
        let set_name: &str = common::prop_setname();

        // Delete any previously existing record(s) associated with the key assigned to this test run.
        let write_policy = WritePolicy::default();
        let key = as_key!(namespace, set_name, i);
        eprintln!(" DELETE: key {:?}", key.clone());
        client.delete(&write_policy, &key).await.expect("initial deletion of key should succeed");

        let mut puts_to_bins: HashMap<String, Value> = HashMap::new();
        let mut as_ops = vec![];
        for op in &ops {
            eprintln!("  AS_OP: {:?}\n.", op.clone());
            // Before validating our write operations, we want to initialize the
            // bins that have been randomly chosen for the property test.
            //
            // First, we need to discover the names of the bins that proptest-rs
            // has chosen for this run.

            match op {
                PropBatchOperation::Write(_, ref ops) => {
                    for sub_op in ops {
                        eprintln!(" SUB_OP: ..{:?}", sub_op.clone());
                        match sub_op {
                            PropOperation::Put(ref bin) => {
                                eprintln!(" INSERT: into puts_to_bins: bin \"{:?}\"", bin.name.clone());
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
                eprintln!("    PUT: bin \"{:?}\"", bin_name.clone());
                client.put(&write_policy, &key, &bins).await.expect("initializing put should work");
            }

            // Finally, build the batch operation list.

            let as_op = op.to_op(key.clone());
            as_ops.push(as_op);
        }

        eprintln!("   NOTE: puts_to_bins initialized.  Key initialized.");

        // Invoke the batch operation.

        let res = client.batch(&batch_policy, &as_ops).await;

        eprintln!("   NOTE: Batch operation completed.");

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
                                    eprintln!("   INFO: client.batch(_, _) -> {} bins returned", record.bins.len());
                                    for (updated_name, _) in record.bins {
                                        // Get current contents of the bin from the database.
                                        // This cannot fail, since we just confirmed at least one bin.
                                        eprintln!("   INFO: client.get(_, _, [\"{:?}\"])", updated_name);
                                        let read_result = client
                                                .get(
                                                    &ReadPolicy::default(),
                                                    &key,
                                                    Bins::Some(vec![updated_name.clone()])
                                                )
                                                .await
                                                .expect("validation get should work");
                                        eprintln!("   INFO: get() results length = {}", read_result.bins.len());
                                        // If the contents of the bin does not match the expected value, panic.
                                        for (candidate_name, candidate_value) in &read_result.bins {
                                            eprintln!("  FOUND: {:?}", candidate_name);
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
        batch_policy in batch_policy(1000, 5000),
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

        eprintln!("AS_OPS -> \n{:?}", as_ops);
        let res = client.batch(&batch_policy, &as_ops).await;

        match res {
        //	   Err(Error::ServerError(ResultCode::ParameterError, _, _)) => {
        //		   if write_policy.respond_per_each_op && ops.into_iter().find(|op| *op == PropOperation::Get).is_some() {
        //			   return;
        //		   }
        //	   }, // it's fine
        //	   Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {
        //	   },
        //	   Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
        //		   if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
        //			   panic!("{}",e);
        //			}
        //	   },
        //	   Err(e @ Error::BatchError(_, ResultCode::GenerationError, _, _)) => {
        //		   // NOTE: there is no way to gain access to the generation_policy
        //		   // from any field accessible to this scope.
        //		   //
        //		   // if batch_policy.generation_policy != GenerationPolicy::None {
        //		   //	  return; // it's fine
        //		   // }
        //		   // panic!("{}", e);
        //	   },
        //	   Err(Error::BatchError(_, ResultCode::BinTypeError, _, _)) => {} // ???!!!
            Err(e) => panic!("ERR: {}", e),
            Ok(_res) => {}, // println!("OK: {:?}", res),
        }
    }
}
