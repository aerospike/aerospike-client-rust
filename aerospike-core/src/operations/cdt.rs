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

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use crate::commands::buffer::Buffer;
use crate::commands::ParticleType;
pub use crate::operations::cdt_context::CdtContext;
use crate::Result;
use crate::Value;

#[derive(Debug, Clone)]
pub enum CdtArgument {
    Byte(u8),
    Int(i64),
    Bool(bool),
    Value(Value),
    List(Vec<Value>),
    Map(HashMap<Value, Value>),
    OrderedMap(BTreeMap<Value, Value>),
}

pub type OperationEncoder = Arc<
    dyn Fn(&mut Option<&mut Buffer>, &CdtOperation, &[CdtContext]) -> Result<usize>
        + Send
        + Sync
        + 'static,
>;

#[derive(Clone)]
pub struct CdtOperation {
    pub op: u8,
    pub encoder: OperationEncoder,
    pub args: Vec<CdtArgument>,
}

impl CdtOperation {
    pub const fn particle_type(&self) -> ParticleType {
        ParticleType::BLOB
    }

    #[must_use]
    pub fn estimate_size(&self, ctx: &[CdtContext]) -> Result<usize> {
        let size: usize = (self.encoder)(&mut None, self, ctx)?;
        Ok(size)
    }

    #[must_use]
    pub fn write_to(&self, buffer: &mut Buffer, ctx: &[CdtContext]) -> Result<usize> {
        let size: usize = (self.encoder)(&mut Some(buffer), self, ctx)?;
        Ok(size)
    }
}

impl<'a> fmt::Debug for CdtOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        #[derive(Debug)]
        #[allow(unused)]
        struct CdtOperation<'a> {
            pub op: &'a u8,
            pub args: &'a Vec<CdtArgument>,
        }

        let Self {
            op,
            encoder: _,
            args,
        } = self;

        fmt::Debug::fmt(&CdtOperation { op, args }, f)
    }
}
