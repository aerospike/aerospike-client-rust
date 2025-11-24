use crate::common;
use crate::proptest::prelude::*;
use crate::proptest_async;

use crate::proptests::value::*;

use aerospike::query::*;
use aerospike::*;

use futures::stream::StreamExt;

use crate::proptests::{bins::*, partition_filter::*, policy::*};

#[derive(Clone, Debug, PartialEq)]
pub enum PropOperation {
    Get,
    GetHeader,
    Touch,
    Delete,
    GetBin(String),
    Put(Bin),
    Append(Bin),
    Prepend(Bin),
    Add(Bin),
}

impl PropOperation {
    pub fn to_op(&self) -> aerospike::operations::Operation<'_> {
        match self {
            Self::Get => aerospike::operations::get(),
            Self::GetHeader => aerospike::operations::get_header(),
            Self::Touch => aerospike::operations::touch(),
            Self::Delete => aerospike::operations::delete(),
            Self::GetBin(name) => aerospike::operations::get_bin(name),
            Self::Put(bin) => aerospike::operations::put(bin),
            Self::Append(bin) => aerospike::operations::append(bin),
            Self::Prepend(bin) => aerospike::operations::prepend(bin),
            Self::Add(bin) => aerospike::operations::add(bin),
        }
    }
}

prop_compose! {
    pub fn many_operations(n: usize)(bin in bin())(ops in prop::collection::vec(any_operation(bin), 1..n as usize)) -> Vec<PropOperation> {
        ops
    }
}

pub fn any_operation(bin: Bin) -> impl Strategy<Value = PropOperation> {
    operation(bin)
}

pub fn any_operation_readish(bin: Bin) -> impl Strategy<Value = PropOperation> {
	operation_readish(bin)
}

// Selects an operation that is a readish or write-ish in nature.
pub fn operation(bin: Bin) -> impl Strategy<Value = PropOperation> {
    prop_oneof![
        // op_get(),
        op_get_header(),
        op_touch(),
        op_delete(),
        op_get_bin(bin.clone().name),
        op_put(bin.clone()),
        op_append(bin.clone()),
        op_prepend(bin.clone()),
        op_add(bin.clone()),
    ]
}

// Selects an operation that is strictly readish in nature.
pub fn operation_readish(bin: Bin) -> impl Strategy<Value = PropOperation> {
	prop_oneof![
        // op_get(),
        op_get_header(),
        op_get_bin(bin.clone().name),
	]
}

pub fn op_get() -> impl Strategy<Value = PropOperation> {
    Just(PropOperation::Get)
}

pub fn op_get_header() -> impl Strategy<Value = PropOperation> {
    Just(PropOperation::GetHeader)
}

pub fn op_touch() -> impl Strategy<Value = PropOperation> {
    Just(PropOperation::Touch)
}

pub fn op_delete() -> impl Strategy<Value = PropOperation> {
    Just(PropOperation::Delete)
}

pub fn op_get_bin(bin_name: String) -> impl Strategy<Value = PropOperation> {
    Just(PropOperation::GetBin(bin_name))
}

pub fn op_put(bin: Bin) -> impl Strategy<Value = PropOperation> {
    Just(PropOperation::Put(bin))
}

pub fn op_append(bin: Bin) -> impl Strategy<Value = PropOperation> {
    Just(PropOperation::Append(bin))
}

pub fn op_prepend(bin: Bin) -> impl Strategy<Value = PropOperation> {
    Just(PropOperation::Prepend(bin))
}

pub fn op_add(bin: Bin) -> impl Strategy<Value = PropOperation> {
    Just(PropOperation::Add(bin))
}
