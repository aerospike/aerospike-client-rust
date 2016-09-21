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

use value::Value;
use common::Bin;

use msgpack::encoder;
use command::buffer;
use command::buffer::Buffer;
use error::{AerospikeError, ResultCode, AerospikeResult};
use common::ParticleType;

#[derive(Debug,Clone)]
#[derive(PartialEq, Eq)]
pub struct OperationType {
    pub op: u8,
}

pub const READ: OperationType = OperationType { op: 1 };
pub const READ_HEADER: OperationType = OperationType { op: 1 };

pub const WRITE: OperationType = OperationType { op: 2 };
pub const CDT_LIST_READ: OperationType = OperationType { op: 3 };
pub const CDT_LIST_MODIFY: OperationType = OperationType { op: 4 };
pub const CDT_MAP_READ: OperationType = OperationType { op: 3 };
pub const CDT_MAP_MODIFY: OperationType = OperationType { op: 4 };
pub const ADD: OperationType = OperationType { op: 5 };
pub const APPEND: OperationType = OperationType { op: 9 };
pub const PREPEND: OperationType = OperationType { op: 10 };
pub const TOUCH: OperationType = OperationType { op: 11 };

pub const NIL_VALUE: &'static Value = &Value::Nil;

#[derive(Debug)]
pub struct Operation<'a> {
    // OpType determines type of operation.
    pub op: OperationType,

    // BinName (Optional) determines the name of bin used in operation.
    pub bin_name: &'a str,

    // BinValue (Optional) determines bin value used in operation.
    pub bin_value: &'a Value,

    // will be true ONLY for GetHeader() operation
    pub header_only: bool,

    // CDT operation
    pub cdt_op: Option<u8>,
    // CDT operations aruments
    pub cdt_args: Option<Vec<Value>>,
    // values passed to the CDT operation
    // if they are an array of size more than ONE, they will be kept here.
    // Otherwise they first element will be kept in bin_value and this
    // attribute will be set to None.
    // This complication is for performance reasons to avoid allocating memory.
    pub cdt_values: Option<&'a [Value]>,
}

impl<'a> Operation<'a> {
    pub fn estimate_size(&self) -> AerospikeResult<usize> {
        let mut size: usize = 0;
        size += self.bin_name.len();
        size += match self.op {
            CDT_LIST_READ | CDT_LIST_MODIFY => {
                try!(encoder::pack_cdt_list_args(None,
                                                 self.cdt_op.unwrap(),
                                                 &self.cdt_args,
                                                 &self.cdt_values,
                                                 self.bin_value))
            }
            _ => try!(self.bin_value.estimate_size()),
        };

        Ok(size)
    }

    pub fn write_to(&self, buffer: &mut Buffer) -> AerospikeResult<usize> {
        let mut size: usize = 0;

        // remove the header size from the estimate
        let op_size = try!(self.estimate_size());

        size += try!(buffer.write_u32(op_size as u32 + 4));
        size += try!(buffer.write_u8(self.op.op));

        match self.op {
            CDT_LIST_READ | CDT_LIST_MODIFY => {
                size += try!(self.write_op_header_to(buffer, ParticleType::BLOB as u8));
                size += try!(encoder::pack_cdt_list_args(Some(buffer),
                                                         self.cdt_op.unwrap(),
                                                         &self.cdt_args,
                                                         &self.cdt_values,
                                                         self.bin_value));
            }
            _ => {
                size += try!(self.write_op_header_to(buffer, self.bin_value.particle_type() as u8));
                size += try!(self.bin_value.write_to(buffer));
            }
        };

        Ok(size)
    }

    fn write_op_header_to(&self, buffer: &mut Buffer, particle_type: u8) -> AerospikeResult<usize> {
        let mut size = try!(buffer.write_u8(particle_type as u8));
        size += try!(buffer.write_u8(0));
        size += try!(buffer.write_u8(self.bin_name.len() as u8));
        size += try!(buffer.write_str(self.bin_name));
        Ok(size)
    }


    pub fn get() -> Self {
        Operation {
            op: READ,
            cdt_op: None,
            cdt_args: None,
            cdt_values: None,
            bin_name: "",
            bin_value: NIL_VALUE,
            header_only: false,
        }
    }

    pub fn get_header() -> Self {
        Operation {
            op: READ,
            cdt_op: None,
            cdt_args: None,
            cdt_values: None,
            bin_name: "",
            bin_value: NIL_VALUE,
            header_only: true,
        }
    }

    pub fn get_bin(bin_name: &'a str) -> Self {
        Operation {
            op: READ,
            cdt_op: None,
            cdt_args: None,
            cdt_values: None,
            bin_name: bin_name,
            bin_value: NIL_VALUE,
            header_only: false,
        }
    }

    pub fn put(bin: &'a Bin) -> Self {
        Operation {
            op: WRITE,
            cdt_op: None,
            cdt_args: None,
            cdt_values: None,
            bin_name: bin.name,
            bin_value: &bin.value,
            header_only: false,
        }
    }

    pub fn append(bin: &'a Bin) -> Self {
        Operation {
            op: APPEND,
            cdt_op: None,
            cdt_args: None,
            cdt_values: None,
            bin_name: bin.name,
            bin_value: &bin.value,
            header_only: false,
        }
    }

    pub fn prepend(bin: &'a Bin) -> Self {
        Operation {
            op: PREPEND,
            cdt_op: None,
            cdt_args: None,
            cdt_values: None,
            bin_name: bin.name,
            bin_value: &bin.value,
            header_only: false,
        }
    }

    pub fn add(bin: &'a Bin) -> Self {
        Operation {
            op: ADD,
            cdt_op: None,
            cdt_args: None,
            cdt_values: None,
            bin_name: bin.name,
            bin_value: &bin.value,
            header_only: false,
        }
    }

    pub fn touch() -> Self {
        Operation {
            op: TOUCH,
            cdt_op: None,
            cdt_args: None,
            cdt_values: None,
            bin_name: "",
            bin_value: NIL_VALUE,
            header_only: false,
        }
    }
}
