use crate::common;
use crate::proptest::prelude::*;
use crate::proptest_async;

use crate::proptests::key::*;
use crate::proptests::operation::*;
use crate::proptests::value::*;

use aerospike::policy::*;
use aerospike::*;

use futures::stream::StreamExt;

use crate::proptests::{bins::*, partition_filter::*, policy::*};

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
            } // PropBatchOperation::Write(bwp, ops) => ,
              // PropBatchOperation::Delete(bdp) => ,
              // PropBatchOperation::UDF(bup, package, func, vals) =>,
        }
    }
}

prop_compose! {
    pub fn many_batch_operations(n: usize)(bin in bin())(ops in prop::collection::vec(any_batch_operation(bin), 1..n as usize)) -> Vec<PropBatchOperation> {
        ops
    }
}

pub fn any_batch_operation(bin: Bin) -> impl Strategy<Value = PropBatchOperation> {
    batch_operation(bin)
}

pub fn batch_operation(bin: Bin) -> impl Strategy<Value = PropBatchOperation> {
    prop_oneof![
        bop_read_bins(),
        bop_read_ops(),
        // bop_write(),
        // bop_delete(),
        // bop_udf(),
    ]
}

prop_compose! {
    pub fn bop_read_bins()(
        brp in batch_read_policy(),
        bs in bins(10),
    ) -> PropBatchOperation {
        PropBatchOperation::ReadBins(brp,  bs)
    }
}

prop_compose! {
    pub fn bop_read_ops()(
        brp in batch_read_policy(),
        ops in ...,
    ) -> PropBatchOperation {
        PropBatchOperation::ReadBins(brp, ops)
    }
}
