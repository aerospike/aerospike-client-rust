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

extern crate core;

#[derive(Debug)]
pub enum UDFLang {
    Lua,
}

impl<'a> core::fmt::Display for UDFLang {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        let s = match self {
            &UDFLang::Lua => "LUA",
        };

        write!(f, "{}", s)
    }
}

impl<'a> From<UDFLang> for &'a str {
    fn from(val: UDFLang) -> &'a str {
        match val {
            UDFLang::Lua => "LUA",
        }
    }
}