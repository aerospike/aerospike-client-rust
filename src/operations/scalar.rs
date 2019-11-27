// Copyright 2015-2018 Aerospike, Inc.
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

//! String/number bin operations. Create operations used by the client's `operate()` method.

use operations::*;
use Bin;

/// Create read all record bins database operation.
pub const fn get<'a>() -> Operation<'a> {
    Operation {
        op: OperationType::Read,
        bin: OperationBin::All,
        data: OperationData::None,
    }
}

/// Create read record header database operation.
pub const fn get_header<'a>() -> Operation<'a> {
    Operation {
        op: OperationType::Read,
        bin: OperationBin::None,
        data: OperationData::None,
    }
}

/// Create read bin database operation.
pub fn get_bin(bin_name: &str) -> Operation {
    Operation {
        op: OperationType::Read,
        bin: OperationBin::Name(bin_name),
        data: OperationData::None,
    }
}

/// Create set database operation.
pub fn put<'a>(bin: &'a Bin) -> Operation<'a> {
    Operation {
        op: OperationType::Write,
        bin: OperationBin::Name(bin.name),
        data: OperationData::Value(&bin.value),
    }
}

/// Create string append database operation.
pub fn append<'a>(bin: &'a Bin) -> Operation<'a> {
    Operation {
        op: OperationType::Append,
        bin: OperationBin::Name(bin.name),
        data: OperationData::Value(&bin.value),
    }
}

/// Create string prepend database operation.
pub fn prepend<'a>(bin: &'a Bin) -> Operation<'a> {
    Operation {
        op: OperationType::Prepend,
        bin: OperationBin::Name(bin.name),
        data: OperationData::Value(&bin.value),
    }
}

/// Create integer add database operation.
pub fn add<'a>(bin: &'a Bin) -> Operation<'a> {
    Operation {
        op: OperationType::Incr,
        bin: OperationBin::Name(bin.name),
        data: OperationData::Value(&bin.value),
    }
}

/// Create touch database operation.
pub const fn touch<'a>() -> Operation<'a> {
    Operation {
        op: OperationType::Touch,
        bin: OperationBin::None,
        data: OperationData::None,
    }
}
