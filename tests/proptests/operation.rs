// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use crate::proptest::prelude::*;
use aerospike::*;

use crate::proptests::bins::*;

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
    pub fn to_op(&self) -> aerospike::operations::Operation {
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
    pub fn many_operations(n: usize)(bin in bin())(ops in prop::collection::vec(any_operation(bin), 1..n)) -> Vec<PropOperation> {
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
    prop_oneof![op_get(), op_get_header(), op_get_bin(bin.clone().name),]
}

// Selects an operation that is strictly write-ish in nature.
pub fn operation_writeish(bin: Bin) -> impl Strategy<Value = PropOperation> {
    prop_oneof![
        op_touch(),
        op_put(bin.clone()),
        op_append(bin.clone()),
        op_prepend(bin.clone()),
        op_add(bin.clone()),
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
