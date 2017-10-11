// Copyright 2015-2017 Aerospike, Inc.
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

//! List bin operations. Create list operations used by the client's `operate()` method.
//!
//! List operations support negative indexing. If the index is negative, the resolved index starts
//! backwards from the end of the list.
//!
//! Index/Count examples:
//!
//! * Index 0: First item in list.
//! * Index 4: Fifth item in list.
//! * Index -1: Last item in list.
//! * Index -3: Third to last item in list.
//! * Index 1, Count 2: Second and third item in list.
//! * Index -3, Count 3: Last three items in list.
//! * Index -5, Count 4: Range between fifth to last item to second to last item inclusive.
//!
//! If an index is out of bounds, a paramter error will be returned. If a range is partially out of
//! bounds, the valid part of the range will be returned.

use Value;
use operations::{Operation, OperationType, OperationBin, OperationData};
use operations::cdt::{CdtOperation, CdtOpType, CdtArgument};

/// Create list append operation. Server appends value to the end of list bin. Server returns
/// list size.
pub fn append<'a>(bin: &'a str, value: &'a Value) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListAppend,
        args: vec![CdtArgument::Value(value)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list append items operation. Server appends each input list item to the end of list
/// bin. Server returns list size.
pub fn append_items<'a>(bin: &'a str, values: &'a [Value]) -> Operation<'a> {
    assert!(values.len() > 0);

    let cdt_op = CdtOperation {
        op: CdtOpType::ListAppendItems,
        args: vec![CdtArgument::List(values)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list insert operation. Server inserts value to the specified index of the list bin.
/// Server returns list size.
pub fn insert<'a>(bin: &'a str, index: i64, value: &'a Value) -> Operation<'a> {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListInsert,
        args: vec![CdtArgument::Int(index), CdtArgument::Value(value)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list insert items operation. Server inserts each input list item starting at the
/// specified index of the list bin. Server returns list size.
pub fn insert_items<'a>(bin: &'a str, index: i64, values: &'a [Value]) -> Operation<'a> {
    assert!(values.len() > 0);

    let cdt_op = CdtOperation {
        op: CdtOpType::ListInsertItems,
        args: vec![CdtArgument::Int(index), CdtArgument::List(values)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list pop operation. Server returns the item at the specified index and removes the
/// item from the list bin.
pub fn pop(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListPop,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list pop range operation. Server returns `count` items starting at the specified
/// index and removes the items from the list bin.
pub fn pop_range(bin: &str, index: i64, count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListPopRange,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list pop range operation. Server returns the items starting at the specified index
/// to the end of the list and removes those items from the list bin.
pub fn pop_range_from(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListPopRange,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove operation. Server removes the item at the specified index from the list
/// bin. Server returns the number of items removed.
pub fn remove(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListRemove,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove range operation. Server removes `count` items starting at the specified
/// index from the list bin. Server returns the number of items removed.
pub fn remove_range(bin: &str, index: i64, count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListRemoveRange,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list remove range operation. Server removes the items starting at the specified
/// index to the end of the list. Server returns the number of items removed.
pub fn remove_range_from(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListRemoveRange,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list set operation. Server sets the item value at the specified index in the list
/// bin. Server does not return a result by default.
pub fn set<'a>(bin: &'a str, index: i64, value: &'a Value) -> Operation<'a> {
    assert!(!value.is_nil());

    let cdt_op = CdtOperation {
        op: CdtOpType::ListSet,
        args: vec![CdtArgument::Int(index), CdtArgument::Value(value)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list trim operation. Server removes `count` items in the list bin that do not fall
/// into the range specified by `index` and `count`. If the range is out of bounds, then all
/// items will be removed. Server returns list size after trim.
pub fn trim(bin: &str, index: i64, count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListTrim,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list clear operation. Server removes all items in the list bin. Server does not
/// return a result by default.
pub fn clear(bin: &str) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListClear,
        args: vec![],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list increment operation. Server increments the item value at the specified index by the
/// given amount and returns the final result.
pub fn increment(bin: &str, index: i64, value: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListIncrement,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(value)],
    };
    Operation {
        op: OperationType::CdtWrite,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}


/// Create list size operation. Server returns size of the list.
pub fn size(bin: &str) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListSize,
        args: vec![],
    };
    Operation {
        op: OperationType::CdtRead,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list get operation. Server returns the item at the specified index in the list bin.
pub fn get(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListGet,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtRead,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list get range operation. Server returns `count` items starting at the specified
/// index in the list bin.
pub fn get_range(bin: &str, index: i64, count: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListGetRange,
        args: vec![CdtArgument::Int(index), CdtArgument::Int(count)],
    };
    Operation {
        op: OperationType::CdtRead,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}

/// Create list get range operation. Server returns items starting at the index to the end of
/// the list.
pub fn get_range_from(bin: &str, index: i64) -> Operation {
    let cdt_op = CdtOperation {
        op: CdtOpType::ListGetRange,
        args: vec![CdtArgument::Int(index)],
    };
    Operation {
        op: OperationType::CdtRead,
        bin: OperationBin::Name(bin),
        data: OperationData::CdtListOp(cdt_op),
    }
}
