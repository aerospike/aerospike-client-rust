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

use std::fmt;

/// User-defined function (UDF) language
///
/// The language is chosen when the function is **registered**, not when it is called: the server
/// records it against the file name and looks it up on every apply. So [`Client::execute_udf`] takes
/// no language, and a registered file cannot change language without being re-registered.
///
/// [`Client::execute_udf`]: crate::Client::execute_udf
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UdfLang {
    /// Lua embedded programming language.
    Lua,

    /// WebAssembly, compiled and run by the server.
    ///
    /// The registered content is a **binary `.wasm` module**, not source text — so the bytes passed
    /// to [`Client::register_udf`] are the compiled module, and a file registered this way is named
    /// `*.wasm`.
    ///
    /// [`Client::register_udf`]: crate::Client::register_udf
    Wasm,
}

impl UdfLang {
    /// The name the server knows this language by, as sent in `udf-put`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            UdfLang::Lua => "LUA",
            UdfLang::Wasm => "WASM",
        }
    }
}

impl fmt::Display for UdfLang {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = match *self {
            UdfLang::Lua => "LUA",
            UdfLang::Wasm => "WASM",
        };

        write!(f, "{s}")
    }
}

impl<'a> From<UdfLang> for &'a str {
    fn from(val: UdfLang) -> &'a str {
        match val {
            UdfLang::Lua => "LUA",
            UdfLang::Wasm => "WASM",
        }
    }
}
