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

use Value;
use operations::{Operation, OperationType, OperationBin, OperationData,
                 CdtOperation, CdtOpType, CdtArgument};

impl<'a> Operation<'a> {

    /// Create list append operation. Server appends value to the end of list bin. Server returns
    /// list size.
    pub fn list_append(bin: &'a str, value: &'a Value) -> Self {
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
    pub fn list_append_items(bin: &'a str, values: &'a [Value]) -> Self {
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
    pub fn list_insert(bin: &'a str, index: i64, value: &'a Value) -> Self {
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
    pub fn list_insert_items(bin: &'a str, index: i64, values: &'a [Value]) -> Self {
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
    pub fn list_pop(bin: &'a str, index: i64) -> Self {
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
    pub fn list_pop_range(bin: &'a str, index: i64, count: i64) -> Self {
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
    pub fn list_pop_range_from(bin: &'a str, index: i64) -> Self {
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
    pub fn list_remove(bin: &'a str, index: i64) -> Self {
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
    pub fn list_remove_range(bin: &'a str, index: i64, count: i64) -> Self {
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
    pub fn list_remove_range_from(bin: &'a str, index: i64) -> Self {
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
    pub fn list_set(bin: &'a str, index: i64, value: &'a Value) -> Self {
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
    pub fn list_trim(bin: &'a str, index: i64, count: i64) -> Self {
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
    pub fn list_clear(bin: &'a str) -> Self {
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

    /// Create list size operation. Server returns size of the list.
    pub fn list_size(bin: &'a str) -> Self {
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
    pub fn list_get(bin: &'a str, index: i64) -> Self {
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
    pub fn list_get_range(bin: &'a str, index: i64, count: i64) -> Self {
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
    pub fn list_get_range_from(bin: &'a str, index: i64) -> Self {
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
}
