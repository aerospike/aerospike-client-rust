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

use value;
use common::{Bin};

#[derive(Debug,Clone)]
#[derive(PartialEq, Eq)]
pub struct OperationType {
    pub op: u8,
}

pub const READ: OperationType = OperationType { op: 1 };
pub const READ_HEADER: OperationType = OperationType { op: 1 };

pub const WRITE: OperationType = OperationType { op: 2 };
pub const CDT_READ: OperationType = OperationType { op: 3 };
pub const CDT_MODIFY: OperationType = OperationType { op: 4 };
pub const MAP_READ: OperationType = OperationType { op: 3 };
pub const MAP_MODIFY: OperationType = OperationType { op: 4 };
pub const ADD: OperationType = OperationType { op: 5 };
pub const APPEND: OperationType = OperationType { op: 9 };
pub const PREPEND: OperationType = OperationType { op: 10 };
pub const TOUCH: OperationType = OperationType { op: 11 };

// #[derive(Debug,Clone)]
pub struct Operation<'a> {
	// OpType determines type of operation.
	pub op: OperationType,

	// BinName (Optional) determines the name of bin used in operation.
	pub bin_name: Option<&'a str>,

	// BinValue (Optional) determines bin value used in operation.
	pub bin_value: &'a Option<Box<value::Value>>,

	// will be true ONLY for GetHeader() operation
	pub header_only: bool,
}

// impl<'a> Operation<'a> {
// 	pub fn get() -> Self {
// 		Operation {
// 			op: READ,
// 			bin_name: None,
// 			bin_value: &None,
// 			header_only: false,
// 		}
// 	}

// 	pub fn get_header() -> Self {
// 		Operation {
// 			op: READ,
// 			bin_name: None,
// 			bin_value: &None,
// 			header_only: true,
// 		}
// 	}

// 	pub fn get_bin(bin_name: &'a str) -> Self {
// 		Operation {
// 			op: READ,
// 			bin_name: Some(bin_name),
// 			bin_value: None,
// 			header_only: false,
// 		}
// 	}

// 	pub fn put(bin: &'a Bin) -> Self {
// 		Operation {
// 			op: WRITE,
// 			bin_name: Some(bin.name),
// 			bin_value: &bin.value,
// 			header_only: false,
// 		}
// 	}

// 	pub fn append(bin: &'a Bin) -> Self {
// 		Operation {
// 			op: APPEND,
// 			bin_name: Some(bin.name),
// 			bin_value: &bin.value,
// 			header_only: false,
// 		}
// 	}

// 	pub fn prepend(bin: &'a Bin) -> Self {
// 		Operation {
// 			op: PREPEND,
// 			bin_name: Some(bin.name),
// 			bin_value: &bin.value,
// 			header_only: false,
// 		}
// 	}

// 	pub fn add(bin: &'a Bin) -> Self {
// 		Operation {
// 			op: ADD,
// 			bin_name: Some(bin.name),
// 			bin_value: &bin.value,
// 			header_only: false,
// 		}
// 	}

// 	pub fn touch() -> Self {
// 		Operation {
// 			op: TOUCH,
// 			bin_name: None,
// 			bin_value: &None,
// 			header_only: false,
// 		}
// 	}
// }
