// Copyright 2015-2018 Aerospike, Inc.
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
use std::result::Result as StdResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Type of values in Aerospike wire-protocol.
#[doc(hidden)]
pub enum ParticleType {
    // Server particle types this client interprets. Any other wire code
    // (legacy language-specific serializations, retired server types)
    // decodes as `Value::Unknown(code, bytes)` — see `name_of` for the
    // textual names of those codes.
    NULL = 0,
    INTEGER = 1,
    FLOAT = 2,
    STRING = 3,
    BLOB = 4,
    DIGEST = 6,
    BOOL = 17,
    HLL = 18,
    MAP = 19,
    LIST = 20,
    LDT = 21,
    GEOJSON = 23,
}

impl ParticleType {
    /// Fallible conversion from the wire code. Returns `None` for codes
    /// this client does not interpret — callers must degrade gracefully
    /// (foreign data must never panic the decoder).
    pub(crate) const fn try_from_u8(val: u8) -> Option<ParticleType> {
        match val {
            0 => Some(ParticleType::NULL),
            1 => Some(ParticleType::INTEGER),
            2 => Some(ParticleType::FLOAT),
            3 => Some(ParticleType::STRING),
            4 => Some(ParticleType::BLOB),
            6 => Some(ParticleType::DIGEST),
            17 => Some(ParticleType::BOOL),
            18 => Some(ParticleType::HLL),
            19 => Some(ParticleType::MAP),
            20 => Some(ParticleType::LIST),
            21 => Some(ParticleType::LDT),
            23 => Some(ParticleType::GEOJSON),
            _ => None,
        }
    }

    /// Textual name for a wire particle-type code, including codes the
    /// client does not interpret (used to render `Value::Unknown` and
    /// error messages). Names follow the server's particle table.
    pub(crate) const fn name_of(code: u8) -> &'static str {
        match code {
            0 => "NULL",
            1 => "INTEGER",
            2 => "FLOAT",
            3 => "STRING",
            4 => "BLOB",
            6 => "DIGEST",
            7 => "JBLOB",
            8 => "CSHARP_BLOB",
            9 => "PYTHON_BLOB",
            10 => "RUBY_BLOB",
            11 => "PHP_BLOB",
            12 => "ERLANG_BLOB",
            17 => "BOOL",
            18 => "HLL",
            19 => "MAP",
            20 => "LIST",
            21 => "LDT",
            23 => "GEOJSON",
            _ => "UNKNOWN",
        }
    }
}

impl fmt::Display for ParticleType {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        write!(f, "{}", ParticleType::name_of(*self as u8))
    }
}
