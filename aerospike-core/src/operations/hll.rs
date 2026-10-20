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

//! `HyperLogLog` operations on HLL items nested in lists/maps are not currently
//! supported by the server.

use std::sync::Arc;

use crate::msgpack::encoder::pack_hll_op;
use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::DEFAULT_CTX;
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::Value;

crate::flags::bit_flags! {
    /// Write flags for HLL operations, carried by [`HllPolicy`]. Combine with `|`.
    pub struct HllWriteFlags(u8);
    /// Default. Allow create or update.
    const DEFAULT = 0;
    /// If the bin already exists, the operation will be denied.
    /// If the bin does not exist, a new bin will be created.
    const CREATE_ONLY = 1;
    /// If the bin already exists, the bin will be overwritten.
    /// If the bin does not exist, the operation will be denied.
    const UPDATE_ONLY = 2;
    /// Do not raise error if operation is denied.
    const NO_FAIL = 4;
    /// Allow the resulting set to be the minimum of provided index bits.
    /// Also, allow the usage of less precise HLL algorithms when minHash bits
    /// of all participating sets do not match.
    const ALLOW_FOLD = 8;
}

/// `HllPolicy` operation policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HllPolicy {
    /// The write flags.
    pub flags: HllWriteFlags,
}

impl HllPolicy {
    /// Use the given [`HllWriteFlags`] (combine them with `|`) when performing HLL operations.
    pub const fn new(write_flags: HllWriteFlags) -> Self {
        HllPolicy { flags: write_flags }
    }
}

impl Default for HllPolicy {
    /// Returns the default policy for HLL operations.
    fn default() -> Self {
        HllPolicy::new(HllWriteFlags::DEFAULT)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum HLLOpType {
    Init = 0,
    Add = 1,
    SetUnion = 2,
    SetCount = 3,
    Fold = 4,
    Count = 50,
    Union = 51,
    UnionCount = 52,
    IntersectCount = 53,
    Similarity = 54,
    Describe = 55,
}

/// Creates HLL init operation.
/// Server creates a new HLL or resets an existing HLL.
/// Server does not return a value.
#[must_use]
pub fn init(policy: &HllPolicy, bin: impl Into<String>, index_bit_count: i64) -> Operation {
    init_with_min_hash(policy, bin, index_bit_count, -1)
}

/// Creates HLL init operation with minhash bits.
/// Server creates a new HLL or resets an existing HLL.
/// Server does not return a value.
#[must_use]
pub fn init_with_min_hash(
    policy: &HllPolicy,
    bin: impl Into<String>,
    index_bit_count: i64,
    min_hash_bit_count: i64,
) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Init as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![
            CdtArgument::Int(index_bit_count),
            CdtArgument::Int(min_hash_bit_count),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL add operation. This operation assumes HLL bin already exists.
/// Server adds values to the HLL set.
/// Server returns number of entries that caused HLL to update a register.
#[must_use]
pub fn add(policy: &HllPolicy, bin: impl Into<String>, list: Vec<Value>) -> Operation {
    add_with_index_and_min_hash(policy, bin, list, -1, -1)
}

/// Creates HLL add operation.
/// Server adds values to HLL set. If HLL bin does not exist, use `indexBitCount` to create HLL bin.
/// Server returns number of entries that caused HLL to update a register.
#[must_use]
pub fn add_with_index(
    policy: &HllPolicy,
    bin: impl Into<String>,
    list: Vec<Value>,
    index_bit_count: i64,
) -> Operation {
    add_with_index_and_min_hash(policy, bin, list, index_bit_count, -1)
}

/// Creates HLL add operation with minhash bits.
///
/// Server adds values to HLL set. If HLL bin does not exist, use `indexBitCount` and `minHashBitCount`
/// to create HLL bin. Server returns number of entries that caused HLL to update a register.
#[must_use]
pub fn add_with_index_and_min_hash(
    policy: &HllPolicy,
    bin: impl Into<String>,
    list: Vec<Value>,
    index_bit_count: i64,
    min_hash_bit_count: i64,
) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Add as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![
            CdtArgument::List(list),
            CdtArgument::Int(index_bit_count),
            CdtArgument::Int(min_hash_bit_count),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL set union operation.
/// Server sets union of specified HLL objects with HLL bin.
/// Server does not return a value.
#[must_use]
pub fn set_union(policy: &HllPolicy, bin: impl Into<String>, list: Vec<Value>) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::SetUnion as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![
            CdtArgument::List(list),
            CdtArgument::Byte(policy.flags.bits()),
        ],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL refresh operation.
/// Server updates the cached count (if stale) and returns the count.
#[must_use]
pub fn refresh_count(bin: impl Into<String>) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::SetCount as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL fold operation.
/// Servers folds `indexBitCount` to the specified value.
/// This can only be applied when `minHashBitCount` on the HLL bin is 0.
/// Server does not return a value.
#[must_use]
pub fn fold(bin: impl Into<String>, index_bit_count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Fold as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![CdtArgument::Int(index_bit_count)],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL getCount operation.
/// Server returns estimated number of elements in the HLL bin.
#[must_use]
pub fn get_count(bin: impl Into<String>) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Count as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL getUnion operation.
/// Server returns an HLL object that is the union of all specified HLL objects in the list
/// with the HLL bin.
#[must_use]
pub fn get_union(bin: impl Into<String>, list: Vec<Value>) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Union as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL `get_union_count` operation.
/// Server returns estimated number of elements that would be contained by the union of these
/// HLL objects.
#[must_use]
pub fn get_union_count(bin: impl Into<String>, list: Vec<Value>) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::UnionCount as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL `get_intersect_count` operation.
/// Server returns estimated number of elements that would be contained by the intersection of
/// these HLL objects.
#[must_use]
pub fn get_intersect_count(bin: impl Into<String>, list: Vec<Value>) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::IntersectCount as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL getSimilarity operation.
/// Server returns estimated similarity of these HLL objects. Return type is a double.
#[must_use]
pub fn get_similarity(bin: impl Into<String>, list: Vec<Value>) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Similarity as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Creates HLL describe operation.
/// Server returns `indexBitCount` and `minHashBitCount` used to create HLL bin in a list of longs.
/// The list size is 2.
#[must_use]
pub fn describe(bin: impl Into<String>) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Describe as u8,
        encoder: Arc::new(pack_hll_op),
        args: vec![],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin.into()),
        data: OperationData::HLLOp(cdt_op),
    }
}
