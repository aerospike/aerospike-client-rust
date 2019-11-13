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

use std::collections::HashMap;

use errors::*;
use Value;
use msgpack::encoder;
use commands::ParticleType;
use commands::buffer::Buffer;

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum CdtOpType {
    ListAppend = 1,
    ListAppendItems = 2,
    ListInsert = 3,
    ListInsertItems = 4,
    ListPop = 5,
    ListPopRange = 6,
    ListRemove = 7,
    ListRemoveRange = 8,
    ListSet = 9,
    ListTrim = 10,
    ListClear = 11,
    ListIncrement = 12,
    ListSize = 16,
    ListGet = 17,
    ListGetRange = 18,
    MapSetType = 64,
    MapAdd = 65,
    MapAddItems = 66,
    MapPut = 67,
    MapPutItems = 68,
    MapReplace = 69,
    MapReplaceItems = 70,
    MapIncrement = 73,
    MapDecrement = 74,
    MapClear = 75,
    MapRemoveByKey = 76,
    MapRemoveByIndex = 77,
    MapRemoveByValue = 78,
    MapRemoveByRank = 79,
    MapRemoveByKeyList = 81,
    MapRemoveByValueList = 83,
    MapRemoveByKeyInterval = 84,
    MapRemoveByIndexRange = 85,
    MapRemoveByValueInterval = 86,
    MapRemoveByRankRange = 87,
    MapSize = 96,
    MapGetByKey = 97,
    MapGetByIndex = 98,
    MapGetByValue = 99,
    MapGetByRank = 100,
    MapGetByKeyInterval = 103,
    MapGetByIndexRange = 104,
    MapGetByValueInterval = 105,
    MapGetByRankRange = 106,
}

#[derive(Debug)]
#[doc(hidden)]
pub enum CdtArgument<'a> {
    Byte(u8),
    Int(i64),
    Value(&'a Value),
    List(&'a [Value]),
    Map(&'a HashMap<Value, Value>),
}

#[derive(Debug)]
#[doc(hidden)]
pub struct CdtOperation<'a> {
    pub op: CdtOpType,
    pub args: Vec<CdtArgument<'a>>,
}

impl<'a> CdtOperation<'a> {
    pub fn particle_type(&self) -> ParticleType {
        ParticleType::BLOB
    }

    pub fn estimate_size(&self) -> Result<usize> {
        let size: usize = encoder::pack_cdt_op(&mut None, self)?;
        Ok(size)
    }

    pub fn write_to(&self, buffer: &mut Buffer) -> Result<usize> {
        let size: usize = encoder::pack_cdt_op(&mut Some(buffer), self)?;
        Ok(size)
    }
}
