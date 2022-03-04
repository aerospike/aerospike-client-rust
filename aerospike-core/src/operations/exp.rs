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

//! Expression Operations.
//! This functions allow users to run `FilterExpressions` as Operate commands.

use crate::commands::buffer::Buffer;
use crate::expressions::FilterExpression;
use crate::msgpack::encoder::{pack_array_begin, pack_integer};
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::ParticleType;

/// Expression write Flags
pub enum ExpWriteFlags {
    /// Default. Allow create or update.
    Default = 0,
    /// If bin does not exist, a new bin will be created.
    /// If bin exists, the operation will be denied.
    /// If bin exists, fail with Bin Exists
    CreateOnly = 1 << 0,
    /// If bin exists, the bin will be overwritten.
    /// If bin does not exist, the operation will be denied.
    /// If bin does not exist, fail with Bin Not Found
    UpdateOnly = 1 << 1,
    /// If expression results in nil value, then delete the bin.
    /// Otherwise, return OP Not Applicable when NoFail is not set
    AllowDelete = 1 << 2,
    /// Do not raise error if operation is denied.
    PolicyNoFail = 1 << 3,
    /// Ignore failures caused by the expression resolving to unknown or a non-bin type.
    EvalNoFail = 1 << 4,
}

#[doc(hidden)]
pub type ExpressionEncoder =
    Box<dyn Fn(&mut Option<&mut Buffer>, &ExpOperation) -> usize + Send + Sync + 'static>;

#[doc(hidden)]
pub struct ExpOperation<'a> {
    pub encoder: ExpressionEncoder,
    pub policy: i64,
    pub exp: &'a FilterExpression,
}

impl<'a> ExpOperation<'a> {
    #[doc(hidden)]
    pub const fn particle_type(&self) -> ParticleType {
        ParticleType::BLOB
    }
    #[doc(hidden)]
    pub fn estimate_size(&self) -> usize {
        let size: usize = (self.encoder)(&mut None, self);
        size
    }
    #[doc(hidden)]
    pub fn write_to(&self, buffer: &mut Buffer) -> usize {
        let size: usize = (self.encoder)(&mut Some(buffer), self);
        size
    }
}

/// Expression read Flags
pub enum ExpReadFlags {
    /// Default
    Default = 0,
    /// Ignore failures caused by the expression resolving to unknown or a non-bin type.
    EvalNoFail = 1 << 4,
}

/// Create operation that performs a expression that writes to record bin.
pub fn write_exp<'a>(
    bin: &'a str,
    exp: &'a FilterExpression,
    flags: ExpWriteFlags,
) -> Operation<'a> {
    let op = ExpOperation {
        encoder: Box::new(pack_write_exp),
        policy: flags as i64,
        exp,
    };
    Operation {
        op: OperationType::ExpWrite,
        ctx: &[],
        bin: OperationBin::Name(bin),
        data: OperationData::EXPOp(op),
    }
}

/// Create operation that performs a read expression.
pub fn read_exp<'a>(
    name: &'a str,
    exp: &'a FilterExpression,
    flags: ExpReadFlags,
) -> Operation<'a> {
    let op = ExpOperation {
        encoder: Box::new(pack_read_exp),
        policy: flags as i64,
        exp,
    };
    Operation {
        op: OperationType::ExpRead,
        ctx: &[],
        bin: OperationBin::Name(name),
        data: OperationData::EXPOp(op),
    }
}

fn pack_write_exp(buf: &mut Option<&mut Buffer>, exp_op: &ExpOperation) -> usize {
    let mut size = 0;
    size += pack_array_begin(buf, 2);
    size += exp_op.exp.pack(buf);
    size += pack_integer(buf, exp_op.policy);
    size
}

fn pack_read_exp(buf: &mut Option<&mut Buffer>, exp_op: &ExpOperation) -> usize {
    let mut size = 0;
    size += pack_array_begin(buf, 2);
    size += exp_op.exp.pack(buf);
    size += pack_integer(buf, exp_op.policy);
    size
}
