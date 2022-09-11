// Copyright 2015-2020 Aerospike, Inc.
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

use crate::msgpack::encoder::pack_hll_op;
use crate::operations::cdt::{CdtArgument, CdtOperation};
use crate::operations::cdt_context::DEFAULT_CTX;
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::Value;

/// `HLLWriteFlags` determines write flags for HLL
#[derive(Debug, Clone, Copy)]
pub enum HLLWriteFlags {
    /// Default.  Allow create or update.
    Default = 0,
    /// If the bin already exists, the operation will be denied.
    /// If the bin does not exist, a new bin will be created.
    CreateOnly = 1,
    /// If the bin already exists, the bin will be overwritten.
    /// If the bin does not exist, the operation will be denied.
    UpdateOnly = 2,
    /// Do not raise error if operation is denied.
    NoFail = 4,
    /// Allow the resulting set to be the minimum of provided index bits.
    /// Also, allow the usage of less precise HLL algorithms when minHash bits
    /// of all participating sets do not match.
    AllowFold = 8,
}

/// `HLLPolicy` operation policy.
#[derive(Debug, Clone, Copy)]
pub struct HLLPolicy {
    /// CdtListWriteFlags
    pub flags: HLLWriteFlags,
}

impl HLLPolicy {
    /// Use specified `HLLWriteFlags` when performing `HLL` operations
    pub const fn new(write_flags: HLLWriteFlags) -> Self {
        HLLPolicy { flags: write_flags }
    }
}

impl Default for HLLPolicy {
    /// Returns the default policy for HLL operations.
    fn default() -> Self {
        HLLPolicy::new(HLLWriteFlags::Default)
    }
}

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum HLLOpType {
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

/// Create HLL init operation.
/// Server creates a new HLL or resets an existing HLL.
/// Server does not return a value.
pub fn init<'a>(policy: &HLLPolicy, bin: &'a str, index_bit_count: i64) -> Operation<'a> {
    init_with_min_hash(policy, bin, index_bit_count, -1)
}

/// Create HLL init operation with minhash bits.
/// Server creates a new HLL or resets an existing HLL.
/// Server does not return a value.
pub fn init_with_min_hash<'a>(
    policy: &HLLPolicy,
    bin: &'a str,
    index_bit_count: i64,
    min_hash_bit_count: i64,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HLLOpType::Init as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![
            CdtArgument::Int(index_bit_count),
            CdtArgument::Int(min_hash_bit_count),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL add operation. This operation assumes HLL bin already exists.
/// Server adds values to the HLL set.
/// Server returns number of entries that caused HLL to update a register.
pub fn add<'a>(policy: &HLLPolicy, bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    add_with_index_and_min_hash(policy, bin, list, -1, -1)
}

/// Create HLL add operation.
/// Server adds values to HLL set. If HLL bin does not exist, use `indexBitCount` to create HLL bin.
/// Server returns number of entries that caused HLL to update a register.
pub fn add_with_index<'a>(
    policy: &HLLPolicy,
    bin: &'a str,
    list: &'a [Value],
    index_bit_count: i64,
) -> Operation<'a> {
    add_with_index_and_min_hash(policy, bin, list, index_bit_count, -1)
}

/// Create HLL add operation with minhash bits.
/// Server adds values to HLL set. If HLL bin does not exist, use `indexBitCount` and `minHashBitCount`
/// to create HLL bin. Server returns number of entries that caused HLL to update a register.
pub fn add_with_index_and_min_hash<'a>(
    policy: &HLLPolicy,
    bin: &'a str,
    list: &'a [Value],
    index_bit_count: i64,
    min_hash_bit_count: i64,
) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HLLOpType::Add as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![
            CdtArgument::List(list),
            CdtArgument::Int(index_bit_count),
            CdtArgument::Int(min_hash_bit_count),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL set union operation.
/// Server sets union of specified HLL objects with HLL bin.
/// Server does not return a value.
pub fn set_union<'a>(policy: &HLLPolicy, bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HLLOpType::SetUnion as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![
            CdtArgument::List(list),
            CdtArgument::Byte(policy.flags as u8),
        ],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL refresh operation.
/// Server updates the cached count (if stale) and returns the count.
pub fn refresh_count(bin: &str) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::SetCount as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL fold operation.
/// Servers folds `indexBitCount` to the specified value.
/// This can only be applied when `minHashBitCount` on the HLL bin is 0.
/// Server does not return a value.
pub fn fold(bin: &str, index_bit_count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Fold as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![CdtArgument::Int(index_bit_count)],
    };
    Operation {
        op: OperationType::HllWrite,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL getCount operation.
/// Server returns estimated number of elements in the HLL bin.
pub fn get_count(bin: &str) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Count as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL getUnion operation.
/// Server returns an HLL object that is the union of all specified HLL objects in the list
/// with the HLL bin.
pub fn get_union<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HLLOpType::Union as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL `get_union_count` operation.
/// Server returns estimated number of elements that would be contained by the union of these
/// HLL objects.
pub fn get_union_count<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HLLOpType::UnionCount as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL `get_intersect_count` operation.
/// Server returns estimated number of elements that would be contained by the intersection of
/// these HLL objects.
pub fn get_intersect_count<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HLLOpType::IntersectCount as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL getSimilarity operation.
/// Server returns estimated similarity of these HLL objects. Return type is a double.
pub fn get_similarity<'a>(bin: &'a str, list: &'a [Value]) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: HLLOpType::Similarity as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![CdtArgument::List(list)],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}

/// Create HLL describe operation.
/// Server returns `indexBitCount` and `minHashBitCount` used to create HLL bin in a list of longs.
/// The list size is 2.
pub fn describe(bin: &str) -> Operation {
    let cdt_op = CdtOperation {
        op: HLLOpType::Describe as u8,
        encoder: Box::new(pack_hll_op),
        args: vec![],
    };
    Operation {
        op: OperationType::HllRead,
        ctx: DEFAULT_CTX,
        bin: OperationBin::Name(bin),
        data: OperationData::HLLOp(cdt_op),
    }
}
