// Copyright 2015-2020 Aerospike, Inc.
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

//! Functions used to create database operations used in the client's `operate()` method.

pub mod bitwise;
#[doc(hidden)]
pub mod cdt;
pub mod cdt_context;
pub mod exp;
pub mod hll;
pub mod lists;
pub mod maps;
pub mod scalar;

use self::cdt::CdtOperation;
pub use self::maps::{MapOrder, MapPolicy, MapReturnType, MapWriteMode};
pub use self::scalar::*;

use crate::commands::buffer::Buffer;
use crate::commands::ParticleType;
use crate::operations::cdt_context::CdtContext;
use crate::operations::exp::ExpOperation;
use crate::Value;

#[derive(Clone, Copy)]
#[doc(hidden)]
pub enum OperationType {
    Read = 1,
    Write = 2,
    CdtRead = 3,
    CdtWrite = 4,
    Incr = 5,
    ExpRead = 7,
    ExpWrite = 8,
    Append = 9,
    Prepend = 10,
    Touch = 11,
    BitRead = 12,
    BitWrite = 13,
    Delete = 14,
    HllRead = 15,
    HllWrite = 16,
}

#[doc(hidden)]
pub enum OperationData<'a> {
    None,
    Value(&'a Value),
    CdtListOp(CdtOperation<'a>),
    CdtMapOp(CdtOperation<'a>),
    CdtBitOp(CdtOperation<'a>),
    HLLOp(CdtOperation<'a>),
    EXPOp(ExpOperation<'a>),
}

#[doc(hidden)]
pub enum OperationBin<'a> {
    None,
    All,
    Name(&'a str),
}

/// Database operation definition. This data type is used in the client's `operate()` method.
pub struct Operation<'a> {
    // OpType determines type of operation.
    #[doc(hidden)]
    pub op: OperationType,

    // CDT context for nested types
    #[doc(hidden)]
    pub ctx: &'a [CdtContext],

    // BinName (Optional) determines the name of bin used in operation.
    #[doc(hidden)]
    pub bin: OperationBin<'a>,

    // BinData determines bin value used in operation.
    #[doc(hidden)]
    pub data: OperationData<'a>,
}

impl<'a> Operation<'a> {
    #[doc(hidden)]
    pub fn estimate_size(&self) -> usize {
        let mut size: usize = 0;
        size += match self.bin {
            OperationBin::Name(bin) => bin.len(),
            OperationBin::None | OperationBin::All => 0,
        };
        size += match self.data {
            OperationData::None => 0,
            OperationData::Value(value) => value.estimate_size(),
            OperationData::EXPOp(ref exp_op) => exp_op.estimate_size(),
            OperationData::CdtListOp(ref cdt_op)
            | OperationData::CdtMapOp(ref cdt_op)
            | OperationData::CdtBitOp(ref cdt_op)
            | OperationData::HLLOp(ref cdt_op) => cdt_op.estimate_size(self.ctx),
        };

        size
    }

    #[doc(hidden)]
    pub fn write_to(&self, buffer: &mut Buffer) -> usize {
        let mut size: usize = 0;

        // remove the header size from the estimate
        let op_size = self.estimate_size();

        size += buffer.write_u32(op_size as u32 + 4);
        size += buffer.write_u8(self.op as u8);

        match self.data {
            OperationData::None => {
                size += self.write_op_header_to(buffer, ParticleType::NULL as u8);
            }
            OperationData::Value(value) => {
                size += self.write_op_header_to(buffer, value.particle_type() as u8);
                size += value.write_to(buffer);
            }
            OperationData::CdtListOp(ref cdt_op)
            | OperationData::CdtMapOp(ref cdt_op)
            | OperationData::CdtBitOp(ref cdt_op)
            | OperationData::HLLOp(ref cdt_op) => {
                size += self.write_op_header_to(buffer, cdt_op.particle_type() as u8);
                size += cdt_op.write_to(buffer, self.ctx);
            }
            OperationData::EXPOp(ref exp) => {
                size += self.write_op_header_to(buffer, ParticleType::BLOB as u8);
                size += exp.write_to(buffer);
            }
        };

        size
    }

    #[doc(hidden)]
    fn write_op_header_to(&self, buffer: &mut Buffer, particle_type: u8) -> usize {
        let mut size = buffer.write_u8(particle_type as u8);
        size += buffer.write_u8(0);
        match self.bin {
            OperationBin::Name(bin) => {
                size += buffer.write_u8(bin.len() as u8);
                size += buffer.write_str(bin);
            }
            OperationBin::None | OperationBin::All => {
                size += buffer.write_u8(0);
            }
        }
        size
    }

    /// Set the context of the operation. Required for nested structures
    pub const fn set_context(mut self, ctx: &'a [CdtContext]) -> Operation<'a> {
        self.ctx = ctx;
        self
    }
}
