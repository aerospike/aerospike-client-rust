use crate::common;
use crate::proptest::prelude::*;
use crate::proptest_async;

use crate::proptests::key::*;
use crate::proptests::operation::*;
use crate::proptests::value::*;

use aerospike::policy::*;
use aerospike::BatchOperation;
use aerospike::*;

use futures::stream::StreamExt;

use crate::proptests::{bins::*, partition_filter::*, policy::*};
use crate::proptests::operation::any_operation_readish;

#[derive(Clone, Debug, PartialEq)]
pub enum PropBatchOperation {
    ReadBins(BatchReadPolicy, Bins),
    ReadOps(BatchReadPolicy, Vec<PropOperation>),
    // Write(BatchWritePolicy, Key, Vec<PropOperation>),
    // Delete(BatchDeletePolicy, Key),
    // UDF(BatchUDFPolicy, Key, String, String, Option<Vec<Value>>),
}

impl PropBatchOperation {
    pub fn to_op(&self, key: Key) -> aerospike::BatchOperation<'_> {
        match self {
            PropBatchOperation::ReadBins(brp, bins) => BatchOperation::read(brp, key, bins.clone()),
            PropBatchOperation::ReadOps(brp, ops) => {
                BatchOperation::read_ops(brp, key, ops.iter().map(|op| op.to_op()).collect())
            }
			// PropBatchOperation::Write(bwp, ops) => todo!(),
            // PropBatchOperation::Delete(bdp) => todo!(),
            // PropBatchOperation::UDF(bup, package, func, vals) => todo!(),
        }
    }
}

// select one batch operation and return a strategy for it.

pub fn batch_operation(bin: Bin) -> impl Strategy<Value = PropBatchOperation> {
    prop_oneof![
        bop_read_bins(),
        bop_read_ops(),
        // bop_write(),
        // bop_delete(),
        // bop_udf(),
    ]
}

pub fn any_batch_operation(bin: Bin) -> impl Strategy<Value = PropBatchOperation> {
    batch_operation(bin)
}

prop_compose! {
    pub fn many_batch_operations(n: usize)(bin in bin())(ops in prop::collection::vec(any_batch_operation(bin), 1..n as usize)) -> Vec<PropBatchOperation> {
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
		eprintln!("bop_read_bins() called");
        PropBatchOperation::ReadBins(brp,  bs)
    }
}

prop_compose! {
    pub fn bop_read_ops()
	(n in 1usize..20, bin in bin())
	(
        brp in batch_read_policy(),
        ops in prop::collection::vec(any_operation_readish(bin), n)
    ) -> PropBatchOperation {
		eprintln!("bop_read_ops() called");
        PropBatchOperation::ReadOps(brp, ops)
    }
}
