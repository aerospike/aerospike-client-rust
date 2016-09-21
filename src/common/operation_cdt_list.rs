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
use common::Bin;
use common::operation;
use common::operation::*;

const CDT_LIST_APPEND: u8 = 1;
const CDT_LIST_APPEND_ITEMS: u8 = 2;
const CDT_LIST_INSERT: u8 = 3;
const CDT_LIST_INSERT_ITEMS: u8 = 4;
const CDT_LIST_POP: u8 = 5;
const CDT_LIST_POP_RANGE: u8 = 6;
const CDT_LIST_REMOVE: u8 = 7;
const CDT_LIST_REMOVE_RANGE: u8 = 8;
const CDT_LIST_SET: u8 = 9;
const CDT_LIST_TRIM: u8 = 10;
const CDT_LIST_CLEAR: u8 = 11;
const CDT_LIST_SIZE: u8 = 16;
const CDT_LIST_GET: u8 = 17;
const CDT_LIST_GET_RANGE: u8 = 18;

impl<'a> Operation<'a> {
    pub fn list_append(bin_name: &'a str, values: &'a [Value]) -> Self {
        assert!(values.len() > 0);

        let (bin_value, cdt_values, op) = match values.len() {
            1 => (&values[0], None, CDT_LIST_APPEND),
            _ => (operation::NIL_VALUE, Some(values), CDT_LIST_APPEND_ITEMS),
        };

        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(op),
            cdt_args: None,
            cdt_values: cdt_values,
            bin_name: bin_name,
            bin_value: bin_value,
            header_only: false,
        }
    }

    pub fn list_insert(bin_name: &'a str, index: i64, values: &'a [Value]) -> Self {
        assert!(values.len() > 0);

        let (bin_value, cdt_values, op) = match values.len() {
            1 => (&values[0], None, CDT_LIST_INSERT),
            _ => (operation::NIL_VALUE, Some(values), CDT_LIST_INSERT_ITEMS),
        };

        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(op),
            cdt_args: Some(vec![Value::from(index)]),
            cdt_values: cdt_values,
            bin_name: bin_name,
            bin_value: bin_value,
            header_only: false,
        }
    }

    pub fn list_pop(bin_name: &'a str, index: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(CDT_LIST_POP),
            cdt_args: Some(vec![Value::from(index)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_pop_range(bin_name: &'a str, index: i64, count: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(CDT_LIST_POP_RANGE),
            cdt_args: Some(vec![Value::from(index), Value::from(count)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_pop_range_from(bin_name: &'a str, index: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(CDT_LIST_POP_RANGE),
            cdt_args: Some(vec![Value::from(index)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_remove(bin_name: &'a str, index: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(CDT_LIST_REMOVE),
            cdt_args: Some(vec![Value::from(index)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_remove_range(bin_name: &'a str, index: i64, count: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(CDT_LIST_REMOVE_RANGE),
            cdt_args: Some(vec![Value::from(index), Value::from(count)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_remove_range_from(bin_name: &'a str, index: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(CDT_LIST_REMOVE_RANGE),
            cdt_args: Some(vec![Value::from(index)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_set(bin_name: &'a str, index: i64, value: &'a Value) -> Self {
        assert!(value != operation::NIL_VALUE);

        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(CDT_LIST_SET),
            cdt_args: Some(vec![Value::from(index)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: value,
            header_only: false,
        }
    }

    pub fn list_trim(bin_name: &'a str, index: i64, count: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(CDT_LIST_TRIM),
            cdt_args: Some(vec![Value::from(index), Value::from(count)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_clear(bin_name: &'a str) -> Self {
        Operation {
            op: operation::CDT_LIST_MODIFY,
            cdt_op: Some(CDT_LIST_CLEAR),
            cdt_args: None,
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_size(bin_name: &'a str) -> Self {
        Operation {
            op: operation::CDT_LIST_READ,
            cdt_op: Some(CDT_LIST_SIZE),
            cdt_args: None,
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_get(bin_name: &'a str, index: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_READ,
            cdt_op: Some(CDT_LIST_GET),
            cdt_args: Some(vec![Value::from(index)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_get_range(bin_name: &'a str, index: i64, count: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_READ,
            cdt_op: Some(CDT_LIST_GET_RANGE),
            cdt_args: Some(vec![Value::from(index), Value::from(count)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }

    pub fn list_get_range_from(bin_name: &'a str, index: i64) -> Self {
        Operation {
            op: operation::CDT_LIST_READ,
            cdt_op: Some(CDT_LIST_GET_RANGE),
            cdt_args: Some(vec![Value::from(index)]),
            cdt_values: None,
            bin_name: bin_name,
            bin_value: operation::NIL_VALUE,
            header_only: false,
        }
    }
}
