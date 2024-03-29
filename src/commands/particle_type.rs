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

#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum ParticleType {
    // Server particle types. Unsupported types are commented out.
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

impl From<u8> for ParticleType {
    fn from(val: u8) -> ParticleType {
        match val {
            0 => ParticleType::NULL,
            1 => ParticleType::INTEGER,
            2 => ParticleType::FLOAT,
            3 => ParticleType::STRING,
            4 => ParticleType::BLOB,
            6 => ParticleType::DIGEST,
            17 => ParticleType::BOOL,
            18 => ParticleType::HLL,
            19 => ParticleType::MAP,
            20 => ParticleType::LIST,
            21 => ParticleType::LDT,
            23 => ParticleType::GEOJSON,
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for ParticleType {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        match self {
            ParticleType::NULL => write!(f, "NULL"),
            ParticleType::INTEGER => write!(f, "INTEGER"),
            ParticleType::FLOAT => write!(f, "FLOAT"),
            ParticleType::STRING => write!(f, "STRING"),
            ParticleType::BLOB => write!(f, "BLOB"),
            ParticleType::DIGEST => write!(f, "DIGEST"),
            ParticleType::BOOL => write!(f, "BOOL"),
            ParticleType::HLL => write!(f, "HLL"),
            ParticleType::MAP => write!(f, "MAP"),
            ParticleType::LIST => write!(f, "LIST"),
            ParticleType::LDT => write!(f, "LDT"),
            ParticleType::GEOJSON => write!(f, "GEOJSON"),
        }
    }
}
