// Copyright 2015-2020 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! CDT Path Expression operations.
//! Requires Aerospike Server version >= 8.1.1.

use crate::expressions::{pack_path_modify_bytes, pack_path_select_bytes, Expression};
use crate::operations::cdt_context::CdtContext;
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::{Result, Value};

/// Flags for `select_by_path` operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectFlag(pub i64);

impl SelectFlag {
    /// Return a tree from root to the bottom, with only non-filtered nodes.
    pub const MATCHING_TREE: SelectFlag = SelectFlag(0);
    /// Return values of the finally-selected nodes.
    pub const VALUE: SelectFlag = SelectFlag(1);
    /// Synonym for VALUE — clarifies list element expectations.
    pub const LIST_VALUE: SelectFlag = SelectFlag(1);
    /// Synonym for VALUE — clarifies map value expectations.
    pub const MAP_VALUE: SelectFlag = SelectFlag(1);
    /// Return only map keys of the finally-selected nodes.
    pub const MAP_KEY: SelectFlag = SelectFlag(2);
    /// Return map key-value pairs of the finally-selected nodes.
    pub const MAP_KEY_VALUE: SelectFlag = SelectFlag(3);
    /// Ignore invalid type mismatches instead of failing.
    pub const NO_FAIL: SelectFlag = SelectFlag(0x10);
}

/// Flags for `modify_by_path` operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModifyFlag(pub i64);

impl ModifyFlag {
    /// Default behavior. Fails on type mismatches.
    pub const DEFAULT: ModifyFlag = ModifyFlag(0);
    /// Ignore type errors instead of failing.
    pub const NO_FAIL: ModifyFlag = ModifyFlag(0x10);
}

/// Creates a CDT operate read operation using a CDT path expression context.
/// Requires Aerospike Server version >= 8.1.1.
///
/// # Errors
///
/// Returns an error if the path bytes cannot be packed.
pub fn select_by_path(bin: &str, flag: SelectFlag, ctx: &[CdtContext]) -> Result<Operation> {
    let bytes = pack_path_select_bytes(ctx, flag.0)?;
    Ok(Operation {
        op: OperationType::CdtRead,
        ctx: vec![],
        bin: OperationBin::Name(bin.into()),
        data: OperationData::Value(Value::Blob(bytes)),
    })
}

/// Creates a CDT operate write operation using a CDT path expression context.
/// Requires Aerospike Server version >= 8.1.1.
///
/// # Errors
///
/// Returns an error if the path bytes cannot be packed.
pub fn modify_by_path(
    bin: &str,
    flag: ModifyFlag,
    exp: Expression,
    ctx: &[CdtContext],
) -> Result<Operation> {
    let bytes = pack_path_modify_bytes(ctx, flag.0, &exp)?;
    Ok(Operation {
        op: OperationType::CdtWrite,
        ctx: vec![],
        bin: OperationBin::Name(bin.into()),
        data: OperationData::Value(Value::Blob(bytes)),
    })
}
