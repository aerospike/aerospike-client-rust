use crate::proptest::prelude::*;

use aerospike::*;

use crate::proptests::{bins::*, operation::*, policy::*, value::*};

#[derive(Clone, Debug, PartialEq)]
pub enum PropBatchOperation {
    ReadBins(BatchReadPolicy, Bins),
    ReadOps(BatchReadPolicy, Vec<PropOperation>),
    Write(BatchWritePolicy, Vec<PropOperation>),
    Delete(BatchDeletePolicy),
    UDF(BatchUDFPolicy, String, String, Option<Vec<Value>>),
}

impl PropBatchOperation {
    pub fn to_op(&self, key: Key) -> aerospike::BatchOperation {
        match self {
            PropBatchOperation::ReadBins(brp, bins) => BatchOperation::read(brp, key, bins.clone()),
            PropBatchOperation::ReadOps(brp, ops) => {
                BatchOperation::read_ops(brp, key, ops.iter().map(|op| op.to_op()).collect())
            }
            PropBatchOperation::Write(bwp, ops) => {
                BatchOperation::write(bwp, key, ops.iter().map(|op| op.to_op()).collect())
            }
            PropBatchOperation::Delete(bdp) => BatchOperation::delete(bdp, key),
            PropBatchOperation::UDF(bup, server_path, function_name, args) => {
                BatchOperation::udf(bup, key, server_path, function_name, args.clone())
            }
        }
    }
}

// select one batch operation and return a strategy for it.

pub fn any_batch_operation() -> impl Strategy<Value = PropBatchOperation> {
    prop_oneof![
        // bop_read_bins(),
        // bop_read_ops(),
        // bop_write(),
        bop_delete(),
        // bop_udf(),
    ]
}

pub fn any_batch_read_operation() -> impl Strategy<Value = PropBatchOperation> {
    prop_oneof![bop_read_bins(), bop_read_ops(),]
}

pub fn any_batch_write_operation() -> impl Strategy<Value = PropBatchOperation> {
    prop_oneof![bop_write()]
}

pub fn any_batch_delete_operation() -> impl Strategy<Value = PropBatchOperation> {
    prop_oneof![bop_delete()]
}

pub fn any_batch_udf_operation() -> impl Strategy<Value = PropBatchOperation> {
    prop_oneof![bop_udf()]
}

prop_compose! {
    pub fn many_batch_operations(n: usize)(ops in prop::collection::vec(any_batch_operation(), 1..n as usize)) -> Vec<PropBatchOperation> {
        ops
    }
}

prop_compose! {
    pub fn many_batch_read_operations(n: usize)(ops in prop::collection::vec(any_batch_read_operation(), 1..n as usize)) -> Vec<PropBatchOperation> {
        ops
    }
}

prop_compose! {
    pub fn many_batch_write_operations(n: usize)(ops in prop::collection::vec(any_batch_write_operation(), 1..n as usize)) -> Vec<PropBatchOperation> {
        ops
    }
}

prop_compose! {
    pub fn many_batch_delete_operations(n: usize)(ops in prop::collection::vec(any_batch_delete_operation(), 1..n as usize)) -> Vec<PropBatchOperation> {
        ops
    }
}

prop_compose! {
    pub fn many_batch_udf_operations(n: usize)(ops in prop::collection::vec(any_batch_udf_operation(), 1..n as usize)) -> Vec<PropBatchOperation> {
        ops
    }
}

// Given a randomly selected BatchReadPolicy, and
// given a random set of 10 bins, then
// construct an enumeration variant that implements a strategy.
//
// Later on, we'll 'match' on this variant (maybe not us directly, but one
// of aerospike's APIs) to invoke calls back to the server.

prop_compose! {
    pub fn bop_read_bins()(
        brp in batch_read_policy(),
        bs in bins(10),
    ) -> PropBatchOperation {
        PropBatchOperation::ReadBins(brp,  bs)
    }
}

prop_compose! {
    pub fn bop_read_ops()
    (n in 1usize..20, bin in bin())
    (
        brp in batch_read_policy(),
        ops in prop::collection::vec(operation_readish(bin), n)
    ) -> PropBatchOperation {
        PropBatchOperation::ReadOps(brp, ops)
    }
}

prop_compose! {
    pub fn bop_write()
    (n in 1usize..2, bin in bin())
    (
        bwp in batch_write_policy(),
        ops in prop::collection::vec(operation_writeish(bin), n)
    ) -> PropBatchOperation {
        PropBatchOperation::Write(bwp, ops)
    }
}

prop_compose! {
    pub fn bop_delete()
    (bdp in batch_delete_policy()) -> PropBatchOperation {
        PropBatchOperation::Delete(bdp)
    }
}

prop_compose! {
    pub fn bop_udf()
    (bup in batch_udf_policy(), v in value_any()) -> PropBatchOperation {
        PropBatchOperation::UDF(bup, "test_udf_proptests1".into(), "echo".into(), Some(vec![v]))
    }
}
