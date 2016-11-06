// Copyright 2015-2016 Aerospike, Inc.
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

use value::Value;
use common::operation::*;

impl<'a> Operation<'a> {
    pub fn list_append(bin: &'a str, value: &'a Value) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListAppend,
            data: CdtOpData::Value(value),
            pre_args: None,
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_append_items(bin: &'a str, values: &'a [Value]) -> Self {
        assert!(values.len() > 0);

        let cdt_op = CdtOperation {
            op: CdtOpType::ListAppendItems,
            data: CdtOpData::List(values),
            pre_args: None,
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_insert(bin: &'a str, index: i64, value: &'a Value) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListInsert,
            data: CdtOpData::Value(value),
            pre_args: Some(vec![Value::from(index)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_insert_items(bin: &'a str, index: i64, values: &'a [Value]) -> Self {
        assert!(values.len() > 0);

        let cdt_op = CdtOperation {
            op: CdtOpType::ListInsertItems,
            data: CdtOpData::List(values),
            pre_args: Some(vec![Value::from(index)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_pop(bin: &'a str, index: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListPop,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }


    pub fn list_pop_range(bin: &'a str, index: i64, count: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListPopRange,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index), Value::from(count)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_pop_range_from(bin: &'a str, index: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListPopRange,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_remove(bin: &'a str, index: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListRemove,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_remove_range(bin: &'a str, index: i64, count: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListRemoveRange,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index), Value::from(count)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_remove_range_from(bin: &'a str, index: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListRemoveRange,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_set(bin: &'a str, index: i64, value: &'a Value) -> Self {
        assert!(!value.is_nil());

        let cdt_op = CdtOperation {
            op: CdtOpType::ListSet,
            data: CdtOpData::Value(value),
            pre_args: Some(vec![Value::from(index)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_trim(bin: &'a str, index: i64, count: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListTrim,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index), Value::from(count)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_clear(bin: &'a str) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListClear,
            data: CdtOpData::None,
            pre_args: None,
            post_args: None,
        };
        Operation {
            op: OperationType::CdtWrite,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_size(bin: &'a str) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListSize,
            data: CdtOpData::None,
            pre_args: None,
            post_args: None,
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_get(bin: &'a str, index: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListGet,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_get_range(bin: &'a str, index: i64, count: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListGetRange,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index), Value::from(count)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }

    pub fn list_get_range_from(bin: &'a str, index: i64) -> Self {
        let cdt_op = CdtOperation {
            op: CdtOpType::ListGetRange,
            data: CdtOpData::None,
            pre_args: Some(vec![Value::from(index)]),
            post_args: None,
        };
        Operation {
            op: OperationType::CdtRead,
            bin: OperationBin::Name(bin),
            data: OperationData::CdtListOp(cdt_op),
        }
    }
}
