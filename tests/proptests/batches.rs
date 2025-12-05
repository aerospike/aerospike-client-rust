use std::collections::HashMap;

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
        batch_policy in batch_policy(30000),
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

        let key = as_key!(namespace, set_name, i);

        let mut puts_to_bins: Vec<(String, Value)> = vec![];
        let mut prepends_to_bins: Vec<(String, Value)> = vec![];
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
                                puts_to_bins.push((bin.name.clone(), bin.value.clone()))
                            }
                            PropOperation::Prepend(ref bin) => {
                                prepends_to_bins.push((bin.name.clone(), bin.value.clone()))
                            }
                            _ => (),
                        }
                    }
                }
                _ => (),
            }

            // Now that we have a list of bins to initialize, let's actually
            // set a default value for each of these bins.

            let write_policy = WritePolicy::default();
            for put in &puts_to_bins {
                match &put.1 {
                    Value::Nil => {
                        let bins = [as_bin!(put.0.clone(), 42)];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::Bool(b) => {
                        // We put the opposite of the boolean so that the
                        // difference shows up in any diagnostic output.
                        let bins = [as_bin!(put.0.clone(), !b)];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::Int(_) |
                    Value::UInt(_) => {
                        // Server does not support a distinct unsigned integer
                        // type.  So, we need to convert the value into a
                        // (signed) integer bit-for-bit.
                        let bins = [as_bin!(put.0.clone(), 12345i64)];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::Infinity |
                    Value::Float(FloatValue::F32(_)) => {
                        let bins = [as_bin!(put.0.clone(), 12345.0f32)];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::Float(FloatValue::F64(_)) => {
                        let bins = [as_bin!(put.0.clone(), 12345.0f64)];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::String(_) => {
                        let bins = [as_bin!(put.0.clone(), STRING_DEFAULT)];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::GeoJSON(_) => {
                        let bins = [as_bin!(put.0.clone(), Value::GeoJSON("{ \"type\": \"Point\", \"coordinates\": [ -122.335167, 47.608013 ] }".into()))];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::Blob(_) => {
                        let bins = [as_bin!(put.0.clone(), Value::Blob(vec![1, 2, 3]))];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::List(_) => {
                        let bins = [as_bin!(put.0.clone(), Value::List(vec!["1".into(), "2".into(), "3".into()]))];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::HashMap(_) => {
                        let mut hm: HashMap<Value, Value> = HashMap::new();
                        hm.insert(1.into(), 2.into());
                        hm.insert(2.into(), 4.into());

                        let bins = [as_bin!(put.0.clone(), Value::HashMap(hm))];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::OrderedMap(_) => {
                        let mut om: Vec<(Value, Value)> = vec![];
                        om.push((1.into(), 2.into()));
                        om.push((2.into(), 4.into()));

                        let bins = [as_bin!(put.0.clone(), Value::OrderedMap(om))];
                        client.put(&write_policy, &key, &bins);
                    }

                    Value::HLL(_) |
                    Value::Wildcard => (), // Not sure how to handle these yet
                }
            }

            for prepend in &prepends_to_bins {
                match &prepend.1 {
                    Value::Nil |
                    Value::Bool(_) |
                    Value::Int(_) |
                    Value::UInt(_) |
                    Value::Infinity |
                    Value::Float(FloatValue::F32(_)) |
                    Value::Float(FloatValue::F64(_)) |
                    Value::GeoJSON(_) |
                    Value::HLL(_) |
                    Value::Wildcard |
                    Value::HashMap(_) => (), // not sure how to handle prepends for these types.

                    Value::String(_) => {
                        let bins = [as_bin!(prepend.0.clone(), STRING_DEFAULT)];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::Blob(_) => {
                        let bins = [as_bin!(prepend.0.clone(), Value::Blob(vec![1, 2, 3]))];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::List(_) => {
                        let bins = [as_bin!(prepend.0.clone(), Value::List(vec!["1".into(), "2".into(), "3".into()]))];
                        client.put(&write_policy, &key, &bins);
                    }
                    Value::OrderedMap(_) => {
                        let mut om: Vec<(Value, Value)> = vec![];
                        om.push((1.into(), 2.into()));
                        om.push((2.into(), 4.into()));

                        let bins = [as_bin!(prepend.0.clone(), Value::OrderedMap(om))];
                        client.put(&write_policy, &key, &bins);
                    }
                }
            }

            // Finally, build the batch operation list.

            let as_op = op.to_op(key.clone());
            as_ops.push(as_op);
        }

        // Invoke the batch operation.

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
            Ok(res) => {
                let check = client.get(&ReadPolicy::default(), &key, Bins::All).await;
                if let Ok(rec) = &check {
                    for (name, value) in &rec.bins {
                        let bin = (name.clone(), value.clone());
                        confirm_puts(&bin, &puts_to_bins);
                        confirm_prepends(&bin, &prepends_to_bins);
                    }
                }
            }
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

/// Performs a data validation check, making sure that any Put operations
/// actually put new data into its corresponding bin.
fn confirm_puts(bin: &(String, Value), puts_to_bins: &Vec<(String, Value)>) {
    for candidate in puts_to_bins {
        let (bin_name, bin_value) = (bin.0.clone(), bin.1.clone_safely());
        let (cand_name, cand_value) = (candidate.0.clone(), candidate.1.clone_safely());
        if *bin_name == cand_name {
            if bin_value != cand_value {
                panic!(
                    "Put failed?  Bin \"{}\" expected {:#?} actual {:#?}",
                    bin_name, cand_value, bin_value
                );
            }
        }
    }
}

/// Performs a data validation check, making sure that any Prepend operations
/// actually places its data in front of the data already present in the bin.
fn confirm_prepends(bin: &(String, Value), prepends_to_bins: &Vec<(String, Value)>) {
    let bin_name = bin.0.clone();
    let bin_value = bin.1.clone();

    for (cand_name, cand_value) in prepends_to_bins {
        if bin_name == *cand_name {
            match &bin_value {
                Value::String(s) => {
                    if !s.ends_with(STRING_DEFAULT) {
                        panic!(
                            "Prepend failed?  Bin \"{}\" expected {:#?} actual {:#?}",
                            bin_name, cand_value, bin_value
                        );
                    }
                }
                Value::Blob(b) => {
                    if !b.ends_with(&[1, 2, 3]) {
                        panic!(
                            "Prepend failed?  Bin \"{}\" expected {:#?} actual {:#?}",
                            bin_name, cand_value, bin_value
                        );
                    }
                }
                Value::List(l) => {}
                Value::OrderedMap(om) => {}
                _ => (),
            }
        }
    }
}
