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

crate::flags::bit_flags! {
    /// Flags for [`select_by_path`]. Combine with `|`.
    pub struct SelectFlag(i64);
    /// Return a tree from root to the bottom, with only non-filtered nodes.
    const MATCHING_TREE = 0;
    /// Return values of the finally-selected nodes.
    const VALUE = 1;
    /// Synonym for `VALUE` — clarifies list element expectations.
    const LIST_VALUE = 1;
    /// Synonym for `VALUE` — clarifies map value expectations.
    const MAP_VALUE = 1;
    /// Return only map keys of the finally-selected nodes.
    const MAP_KEY = 2;
    /// Return map key-value pairs of the finally-selected nodes.
    const MAP_KEY_VALUE = 3;
    /// Ignore invalid type mismatches instead of failing.
    const NO_FAIL = 0x10;
}

crate::flags::bit_flags! {
    /// Flags for [`modify_by_path`]. Combine with `|`.
    pub struct ModifyFlag(i64);
    /// Default behavior. Fails on type mismatches.
    const DEFAULT = 0;
    /// Ignore type errors instead of failing.
    const NO_FAIL = 0x10;
}

/// Creates a CDT operate read operation using a CDT path expression context.
/// Requires Aerospike Server version >= 8.1.1.
///
/// Accepts any value convertible to `&[CdtContext]` — pass a
/// `&Vec<CdtContext>`, a slice, or a [`Path`](crate::operations::cdt_context::Path)
/// directly:
///
/// ```rust
/// use aerospike::operations::cdt_context::Path;
/// use aerospike::operations::path::{select_by_path, SelectFlag};
///
/// let path = Path::new().map_key("book").all_children().map_key("price");
/// let op = select_by_path("myBin", SelectFlag::VALUE, &path);
/// ```
#[must_use]
pub fn select_by_path(bin: impl Into<String>, flag: SelectFlag, ctx: &[CdtContext]) -> Operation {
    Operation {
        op: OperationType::CdtRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtSelectByPath(ctx.to_vec(), flag),
    }
}

/// Creates a CDT operate write operation using a CDT path expression context.
/// Requires Aerospike Server version >= 8.1.1.
///
/// Like [`select_by_path`] this accepts anything convertible to
/// `&[CdtContext]`.
///
/// While `exp` typically updates values, passing [`exp_remove_result`](crate::expressions::exp_remove_result) as `exp`
/// **removes** every leaf node matching the path instead of overwriting it.
/// You can also use the [`remove`] wrapper, which encapsulates this exact behavior:
/// ```rust
/// use aerospike::expressions::exp_remove_result;
/// use aerospike::operations::cdt_context::Path;
/// use aerospike::operations::path::{modify_by_path, remove, ModifyFlag};
///
/// let path = Path::new().map_key("book").all_children().map_key("price");
///
/// // Remove every matching "price" entry...
/// let op = modify_by_path("myBin", ModifyFlag::DEFAULT, exp_remove_result(), &path);
/// // ...equivalently, via the ready-made wrapper:
/// let op = remove("myBin", &path);
/// ```
#[must_use]
pub fn modify_by_path(
    bin: impl Into<String>,
    flag: ModifyFlag,
    exp: Expression,
    ctx: &[CdtContext],
) -> Operation {
    Operation {
        op: OperationType::CdtWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::CdtModifyByPath(ctx.to_vec(), flag, exp),
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
#[must_use]
pub fn select_values(bin: impl Into<String>, ctx: &[CdtContext]) -> Operation {
    select_by_path(bin, SelectFlag::VALUE, ctx)
}

/// Convenience wrapper: select the matching *map keys* (`SelectFlag::MAP_KEY`).
/// Requires Aerospike Server version >= 8.1.1.
#[must_use]
pub fn select_map_keys(bin: impl Into<String>, ctx: &[CdtContext]) -> Operation {
    select_by_path(bin, SelectFlag::MAP_KEY, ctx)
}

/// Convenience wrapper: select map *key/value pairs*
/// (`SelectFlag::MAP_KEY_VALUE`).
/// Requires Aerospike Server version >= 8.1.1.
#[must_use]
pub fn select_map_entries(bin: impl Into<String>, ctx: &[CdtContext]) -> Operation {
    select_by_path(bin, SelectFlag::MAP_KEY_VALUE, ctx)
}

/// Convenience wrapper: select the *original tree shape* preserving only
/// matching nodes (`SelectFlag::MATCHING_TREE`).
/// Requires Aerospike Server version >= 8.1.1.
#[must_use]
pub fn select_matching_tree(bin: impl Into<String>, ctx: &[CdtContext]) -> Operation {
    select_by_path(bin, SelectFlag::MATCHING_TREE, ctx)
}

/// Convenience wrapper: modify with default flags, failing on type
/// mismatches (`ModifyFlag::DEFAULT`).
///
/// Equivalent to
/// `modify_by_path(bin, ModifyFlag::DEFAULT, exp, ctx)`.
/// Requires Aerospike Server version >= 8.1.1.
#[must_use]
pub fn modify(bin: impl Into<String>, exp: Expression, ctx: &[CdtContext]) -> Operation {
    modify_by_path(bin, ModifyFlag::DEFAULT, exp, ctx)
}

/// Convenience wrapper: modify with `NO_FAIL` so type-mismatched leaves
/// are silently skipped instead of aborting the whole operation.
/// Requires Aerospike Server version >= 8.1.1.
#[must_use]
pub fn modify_no_fail(bin: impl Into<String>, exp: Expression, ctx: &[CdtContext]) -> Operation {
    modify_by_path(bin, ModifyFlag::NO_FAIL, exp, ctx)
}

/// Convenience wrapper: remove the leaves resolved by a path.
///
/// Equivalent
/// to `modify_by_path(bin, ModifyFlag::DEFAULT, exp_remove_result(), ctx)`.
/// Mirrors a common pattern (delete-by-filter / delete-by-key-set) that
/// would otherwise require importing `expressions::exp_remove_result`.
/// Requires Aerospike Server version >= 8.1.1.
#[must_use]
pub fn remove(bin: impl Into<String>, ctx: &[CdtContext]) -> Operation {
    modify_by_path(
        bin,
        ModifyFlag::DEFAULT,
        crate::expressions::exp_remove_result(),
        ctx,
    )
}
