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

use crate::expressions::Expression;
use crate::operations::cdt_context::{CdtContext, DEFAULT_CTX};
use crate::operations::{Operation, OperationBin, OperationData, OperationType};

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
/// Accepts any value convertible to `&[CdtContext]` — pass a
/// `&Vec<CdtContext>`, a slice, or a [`Path`](crate::operations::cdt_context::Path)
/// directly:
///
/// ```ignore
/// use aerospike::operations::cdt_context::Path;
/// use aerospike::operations::path::{select_by_path, SelectFlag};
///
/// let path = Path::new().map_key("book").all_children().map_key("price");
/// let op = select_by_path("myBin", SelectFlag::VALUE, &path);
/// ```
pub fn select_by_path(bin: &str, flag: SelectFlag, ctx: impl AsRef<[CdtContext]>) -> Operation {
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtSelectByPath(ctx.as_ref().to_vec(), flag),
    }
}

/// Creates a CDT operate write operation using a CDT path expression context.
/// Requires Aerospike Server version >= 8.1.1.
///
/// Like [`select_by_path`] this accepts anything convertible to
/// `&[CdtContext]`.
pub fn modify_by_path(
    bin: &str,
    flag: ModifyFlag,
    exp: Expression,
    ctx: impl AsRef<[CdtContext]>,
) -> Operation {
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtModifyByPath(ctx.as_ref().to_vec(), flag, exp),
    }
}

// ===== Convenience builders on top of `select_by_path` / `modify_by_path`
//
// These don't exist in the Java client. They package the most common
// flag combinations so callers don't have to memorize the bitmask
// constants for each shape of query. All wrappers inherit the same
// server-version requirement as their underlying operation
// (Aerospike Server >= 8.1.1).

/// Convenience wrapper: select the *values* at every path-resolved
/// location (`SelectFlag::VALUE`). Equivalent to
/// `select_by_path(bin, SelectFlag::VALUE, ctx)`.
/// Requires Aerospike Server version >= 8.1.1.
pub fn select_values(bin: &str, ctx: impl AsRef<[CdtContext]>) -> Operation {
    select_by_path(bin, SelectFlag::VALUE, ctx)
}

/// Convenience wrapper: select the matching *map keys* (`SelectFlag::MAP_KEY`).
/// Requires Aerospike Server version >= 8.1.1.
pub fn select_map_keys(bin: &str, ctx: impl AsRef<[CdtContext]>) -> Operation {
    select_by_path(bin, SelectFlag::MAP_KEY, ctx)
}

/// Convenience wrapper: select map *key/value pairs*
/// (`SelectFlag::MAP_KEY_VALUE`).
/// Requires Aerospike Server version >= 8.1.1.
pub fn select_map_entries(bin: &str, ctx: impl AsRef<[CdtContext]>) -> Operation {
    select_by_path(bin, SelectFlag::MAP_KEY_VALUE, ctx)
}

/// Convenience wrapper: select the *original tree shape* preserving only
/// matching nodes (`SelectFlag::MATCHING_TREE`).
/// Requires Aerospike Server version >= 8.1.1.
pub fn select_matching_tree(bin: &str, ctx: impl AsRef<[CdtContext]>) -> Operation {
    select_by_path(bin, SelectFlag::MATCHING_TREE, ctx)
}

/// Convenience wrapper: modify with default flags, failing on type
/// mismatches (`ModifyFlag::DEFAULT`). Equivalent to
/// `modify_by_path(bin, ModifyFlag::DEFAULT, exp, ctx)`.
/// Requires Aerospike Server version >= 8.1.1.
pub fn modify(bin: &str, exp: Expression, ctx: impl AsRef<[CdtContext]>) -> Operation {
    modify_by_path(bin, ModifyFlag::DEFAULT, exp, ctx)
}

/// Convenience wrapper: modify with `NO_FAIL` so type-mismatched leaves
/// are silently skipped instead of aborting the whole operation.
/// Requires Aerospike Server version >= 8.1.1.
pub fn modify_no_fail(bin: &str, exp: Expression, ctx: impl AsRef<[CdtContext]>) -> Operation {
    modify_by_path(bin, ModifyFlag::NO_FAIL, exp, ctx)
}

/// Convenience wrapper: remove the leaves resolved by a path. Equivalent
/// to `modify_by_path(bin, ModifyFlag::DEFAULT, exp_remove_result(), ctx)`.
/// Mirrors a common pattern (delete-by-filter / delete-by-key-set) that
/// would otherwise require importing `expressions::exp_remove_result`.
/// Requires Aerospike Server version >= 8.1.1.
pub fn remove(bin: &str, ctx: impl AsRef<[CdtContext]>) -> Operation {
    modify_by_path(
        bin,
        ModifyFlag::DEFAULT,
        crate::expressions::exp_remove_result(),
        ctx,
    )
}
