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

use value::{Value, IntValue, StringValue};

// #[derive(Debug)]
pub struct Bin<'a> {
    pub name: &'a str,
    pub value: &'a Value,
}


impl<'a> Bin<'a> {
	pub fn new(name: &'a str, val: &'a Value) -> Self {
		Bin {
			name: name,
			value: val,
		}
	}
}

// impl<'a> Bin<'a, IntValue> {
//     pub fn new(bin_name: &'a str, bin_value: i64) -> Self {
//         Bin {
//             name: bin_name,
//             value: IntValue::new(bin_value),
//         }
//     }
// }

// impl<'a> Bin<'a, StringValue> {
//     pub fn new(bin_name: &'a str, bin_value: &'a str) -> Self {
//         Bin {
//             name: bin_name,
//             value: StringValue::new(bin_value),
//         }
//     }
// }
