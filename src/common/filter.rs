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

use std::sync::Arc;

use net::Connection;
use value::Value;
use common::particle_type::ParticleType;
use common::collection_index_type::CollectionIndexType;
use command::buffer::Buffer;
use error::{AerospikeError, ResultCode, AerospikeResult};

#[derive(Debug,Clone)]
pub struct Filter {
    pub bin_name: String,
    collection_index_type: CollectionIndexType,
    value_particle_type: ParticleType,
    pub begin: Arc<Value>,
    pub end: Arc<Value>,
}

impl Filter {
    pub fn new(bin_name: &str,
               collection_index_type: CollectionIndexType,
               value_particle_type: ParticleType,
               begin: Arc<Value>,
               end: Arc<Value>)
               -> AerospikeResult<Arc<Self>> {
        Ok(Arc::new(Filter {
            bin_name: bin_name.to_owned(),
            collection_index_type: collection_index_type,
            value_particle_type: value_particle_type,
            begin: begin,
            end: end,
        }))
    }

    pub fn collection_index_type(&self) -> CollectionIndexType {
        self.collection_index_type.clone()
    }

    pub fn estimate_size(&self) -> AerospikeResult<usize> {
        // bin name size(1) + particle type size(1) + begin particle size(4) + end particle size(4) = 10
        Ok(self.bin_name.len() + try!(self.begin.estimate_size()) +
           try!(self.end.estimate_size()) + 10)
    }

    pub fn write(&self, buffer: &mut Buffer) -> AerospikeResult<()> {
        try!(buffer.write_u8(self.bin_name.len() as u8));
        try!(buffer.write_str(&self.bin_name));
        try!(buffer.write_u8(self.value_particle_type.clone() as u8));

        try!(buffer.write_u32(try!(self.begin.estimate_size()) as u32));
        try!(self.begin.write_to(buffer));

        try!(buffer.write_u32(try!(self.end.estimate_size()) as u32));
        try!(self.end.write_to(buffer));

        Ok(())
    }
}

#[macro_export]
macro_rules! as_eq {
    ($bin_name:expr, $val:expr) => {{
        let val = Arc::new(value::Value::from($val));
        Filter::new($bin_name, common::collection_index_type::CollectionIndexType::Default, val.particle_type(), val.clone(), val.clone()).unwrap()
    }};
}

#[macro_export]
macro_rules! as_range {
    ($bin_name:expr, $begin:expr, $end:expr) => {{
        let begin = Arc::new(value::Value::from($begin));
        let end = Arc::new(value::Value::from($end));
        Filter::new($bin_name, common::collection_index_type::CollectionIndexType::Default, begin.particle_type(), begin, end).unwrap()
    }};
}

#[macro_export]
macro_rules! as_contains {
    ($bin_name:expr, $cit:expr, $val:expr) => {{
        let val = Arc::new(value::Value::from($val));
        Filter::new($bin_name, $cit, val.particle_type(), val, val).unwrap()
    }};
}

#[macro_export]
macro_rules! as_contains_range {
    ($bin_name:expr, $cit:expr, $begin:expr, $end:expr) => {{
        let begin = Arc::new(value::Value::from($begin));
        let end = Arc::new(value::Value::from($end));
        Filter::new($bin_name, $cit, begin.particle_type(), begin, end).unwrap()
    }};
}
