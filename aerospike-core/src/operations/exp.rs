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

use std::fmt;
use std::sync::Arc;

use crate::commands::buffer::Buffer;
use crate::expressions::Expression;
use crate::msgpack::encoder::{pack_array_begin, pack_integer};
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::Result;

/// Expression write Flags
#[derive(Clone, Copy)]
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
    /// Otherwise, return OP Not Applicable when `NoFail` is not set
    AllowDelete = 1 << 2,
    /// Do not raise error if operation is denied.
    PolicyNoFail = 1 << 3,
    /// Ignore failures caused by the expression resolving to unknown or a non-bin type.
    EvalNoFail = 1 << 4,
}

/// Something that can be resolved into a set of `ExpWriteFlags`. Either a single `ExpWriteFlag`, `Option<ExpWriteFlag>`, `ExpWriteFlag`, etc.
pub trait ToExpWriteFlagBitmask {
    /// Convert to an i64 bitmask
    fn to_bitmask(self) -> i64;
}

impl ToExpWriteFlagBitmask for ExpWriteFlags {
    fn to_bitmask(self) -> i64 {
        self as i64
    }
}

impl<T: IntoIterator<Item = ExpWriteFlags>> ToExpWriteFlagBitmask for T {
    fn to_bitmask(self) -> i64 {
        let mut out = 0;
        for val in self {
            out |= val.to_bitmask();
        }
        out
    }
}

pub(crate) type ExpressionEncoder =
    Arc<dyn Fn(&mut Option<&mut Buffer>, &ExpOperation) -> Result<usize> + Send + Sync + 'static>;

#[derive(Clone)]
pub(crate) struct ExpOperation {
    pub encoder: ExpressionEncoder,
    pub policy: i64,
    pub exp: Expression,
}

impl ExpOperation {
    // pub(crate) const fn particle_type(&self) -> ParticleType {
    //     ParticleType::BLOB
    // }
    pub(crate) fn estimate_size(&self) -> Result<usize> {
        let size: usize = (self.encoder)(&mut None, self)?;
        Ok(size)
    }
    pub(crate) fn write_to(&self, buffer: &mut Buffer) -> Result<usize> {
        let size: usize = (self.encoder)(&mut Some(buffer), self)?;
        Ok(size)
    }
}

impl fmt::Debug for ExpOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        #[derive(Debug)]
        #[allow(unused)]
        struct ExpOperation<'a> {
            policy: &'a i64,
            exp: &'a Expression,
        }

        let Self {
            encoder: _,
            policy,
            exp,
        } = self;

        fmt::Debug::fmt(&ExpOperation { policy, exp }, f)
    }
}

/// Expression read Flags
pub enum ExpReadFlags {
    /// Default
    Default = 0,
    /// Ignore failures caused by the expression resolving to unknown or a non-bin type.
    EvalNoFail = 1 << 4,
}

/// Something that can be resolved into a set of `ExpWriteFlags`. Either a single `ExpWriteFlag`, `Option<ExpWriteFlag>`, `ExpWriteFlag`, etc.
pub trait ToExpReadFlagBitmask {
    /// Convert to an i64 bitmask
    fn to_bitmask(self) -> i64;
}

impl ToExpReadFlagBitmask for ExpReadFlags {
    fn to_bitmask(self) -> i64 {
        self as i64
    }
}

impl<T: IntoIterator<Item = ExpReadFlags>> ToExpReadFlagBitmask for T {
    fn to_bitmask(self) -> i64 {
        let mut out = 0;
        for val in self {
            out |= val.to_bitmask();
        }
        out
    }
}

/// Create operation that performs a expression that writes to record bin.
pub fn write_exp<E: ToExpWriteFlagBitmask>(bin: &str, exp: Expression, flags: E) -> Operation {
    let op = ExpOperation {
        encoder: Arc::new(pack_write_exp),
        policy: flags.to_bitmask(),
        exp,
    };
    Operation {
        op: OperationType::ExpWrite,
        ctx: vec![],
        bin: OperationBin::Name(bin.into()),
        data: OperationData::EXPOp(op),
    }
}

/// Create operation that performs a read expression.
pub fn read_exp<E: ToExpReadFlagBitmask>(name: &str, exp: Expression, flags: E) -> Operation {
    let op = ExpOperation {
        encoder: Arc::new(pack_read_exp),
        policy: flags.to_bitmask(),
        exp,
    };
    Operation {
        op: OperationType::ExpRead,
        ctx: vec![],
        bin: OperationBin::Name(name.into()),
        data: OperationData::EXPOp(op),
    }
}

#[must_use]
fn pack_write_exp(buf: &mut Option<&mut Buffer>, exp_op: &ExpOperation) -> Result<usize> {
    let mut size = 0;
    size += pack_array_begin(buf, 2);
    size += exp_op.exp.pack(buf)?;
    size += pack_integer(buf, exp_op.policy);
    Ok(size)
}

#[must_use]
fn pack_read_exp(buf: &mut Option<&mut Buffer>, exp_op: &ExpOperation) -> Result<usize> {
    let mut size = 0;
    size += pack_array_begin(buf, 2);
    size += exp_op.exp.pack(buf)?;
    size += pack_integer(buf, exp_op.policy);
    Ok(size)
}
