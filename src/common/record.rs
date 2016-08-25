// Copyright 2015-2016 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache Licenseersion 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

extern crate core;

use std::fmt;

use std::collections::HashMap;
use std::sync::Arc;

use value::{Value};
use cluster::node::Node;
use common::key::Key;
use error::{AerospikeResult, ResultCode, AerospikeError};

// #[derive(Debug)]
pub struct Record<'a> {
    pub key: &'a Key<'a>,
    pub bins: HashMap<String, Value>,
    pub generation: u32,
    pub expiration: u32,
}

impl<'a> Record<'a> {
    pub fn new(key: &'a Key<'a>,
           bins: HashMap<String, Value>,
           generation: u32,
           expiration: u32)
           -> AerospikeResult<Self> {
        Ok(Record {
            key: key,
            bins: bins,
            generation: generation,
            expiration: expiration,
        })
    }
}


impl<'a> core::fmt::Display for Record<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        try!("key: ".fmt(f));
        try!(self.key.fmt(f));
        try!(", bins: {".fmt(f));
        for (k, v) in &self.bins {
            try!(k.fmt(f));
            try!(": ".fmt(f));
            try!(v.fmt(f));
            try!(" ".fmt(f));
        }
        try!("}".fmt(f));
        try!(", generation: ".fmt(f));
        try!(self.generation.fmt(f));
        try!(", expiration: ".fmt(f));
        try!(self.expiration.fmt(f));
        Ok(())
    }
}
