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

//! Functions used to create database operations used in the client's `operate()` method.

#[doc(hidden)]
pub mod cdt;
pub mod scalar;
pub mod lists;
pub mod maps;

pub use self::maps::{MapOrder, MapPolicy, MapReturnType, MapWriteMode};
pub use self::scalar::*;
use self::cdt::CdtOperation;

use errors::*;
use Value;
use commands::ParticleType;
use commands::buffer::Buffer;

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum OperationType {
    Read = 1,
    Write = 2,
    CdtRead = 3,
    CdtWrite = 4,
    Incr = 5,
    Append = 9,
    Prepend = 10,
    Touch = 11,
}

#[derive(Debug)]
#[doc(hidden)]
pub enum OperationData<'a> {
    None,
    Value(&'a Value),
    CdtListOp(CdtOperation<'a>),
    CdtMapOp(CdtOperation<'a>),
}

#[derive(Debug)]
#[doc(hidden)]
pub enum OperationBin<'a> {
    None,
    All,
    Name(&'a str),
}

/// Database operation definition. This data type is used in the client's `operate()` method.
#[derive(Debug)]
pub struct Operation<'a> {
    // OpType determines type of operation.
    #[doc(hidden)]
    pub op: OperationType,

    // BinName (Optional) determines the name of bin used in operation.
    #[doc(hidden)]
    pub bin: OperationBin<'a>,

    // BinData determines bin value used in operation.
    #[doc(hidden)]
    pub data: OperationData<'a>,
}

impl<'a> Operation<'a> {
    #[doc(hidden)]
    pub fn estimate_size(&self) -> Result<usize> {
        let mut size: usize = 0;
        size += match self.bin {
            OperationBin::Name(bin) => bin.len(),
            OperationBin::None | OperationBin::All => 0,
        };
        size += match self.data {
            OperationData::None => 0,
            OperationData::Value(value) => try!(value.estimate_size()),
            OperationData::CdtListOp(ref cdt_op) | OperationData::CdtMapOp(ref cdt_op) => {
                try!(cdt_op.estimate_size())
            }
        };

        Ok(size)
    }

    #[doc(hidden)]
    pub fn write_to(&self, buffer: &mut Buffer) -> Result<usize> {
        let mut size: usize = 0;

        // remove the header size from the estimate
        let op_size = try!(self.estimate_size());

        size += try!(buffer.write_u32(op_size as u32 + 4));
        size += try!(buffer.write_u8(self.op as u8));

        match self.data {
            OperationData::None => {
                size += try!(self.write_op_header_to(buffer, ParticleType::NULL as u8));
            }
            OperationData::Value(value) => {
                size += try!(self.write_op_header_to(buffer, value.particle_type() as u8));
                size += try!(value.write_to(buffer));
            }
            OperationData::CdtListOp(ref cdt_op) | OperationData::CdtMapOp(ref cdt_op) => {
                size += try!(self.write_op_header_to(buffer, cdt_op.particle_type() as u8));
                size += try!(cdt_op.write_to(buffer));
            }
        };

        Ok(size)
    }

    #[doc(hidden)]
    fn write_op_header_to(&self, buffer: &mut Buffer, particle_type: u8) -> Result<usize> {
        let mut size = try!(buffer.write_u8(particle_type as u8));
        size += try!(buffer.write_u8(0));
        match self.bin {
            OperationBin::Name(bin) => {
                size += try!(buffer.write_u8(bin.len() as u8));
                size += try!(buffer.write_str(bin));
            }
            OperationBin::None | OperationBin::All => {
                size += try!(buffer.write_u8(0));
            }
        }
        Ok(size)
    }
}
