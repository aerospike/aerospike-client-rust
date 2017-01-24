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

use value::Value;

// #[derive(Debug)]
pub struct Bin<'a> {
    pub name: &'a str,
    pub value: Value,
}


impl<'a> Bin<'a> {
    pub fn new(name: &'a str, val: Value) -> Self {
        Bin {
            name: name,
            value: val,
        }
    }
}

#[macro_export]
macro_rules! as_bin {
    ($bin_name:expr, None) => {{
        $crate::Bin::new($bin_name, $crate::Value::Nil)
    }};
    ($bin_name:expr, $val:expr) => {{
        $crate::Bin::new($bin_name, $crate::Value::from($val))
    }};
}
