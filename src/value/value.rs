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

extern crate core;

use std::str;
use std::fmt;
use std::io::Write;

use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt, ByteOrder};

use std::vec::Vec;

use common::ParticleType;
use error::{AerospikeResult, ResultCode, AerospikeError};

pub trait Value {
    fn estimate_size(&self) -> usize;
    fn particle_type(&self) -> ParticleType;
    fn as_bytes(&self) -> AerospikeResult<Vec<u8>>;
    fn as_string(&self) -> String;
}

impl core::fmt::Display for Value {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        try!(self.as_string().fmt(f));
        Ok(())
    }
}


pub fn bytes_to_particle(ptype: u8, buf: &[u8], offset: usize, length: usize) -> AerospikeResult<Box<Value>> {
    match ptype {
        x if x == ParticleType::NULL as u8 => {
            let val = NullValue::new();
            Ok(Box::new(val))
        },
        x if x == ParticleType::INTEGER as u8 => {
            let val = IntValue::new(NetworkEndian::read_i64(&buf[offset..offset+length]));
            Ok(Box::new(val))
        },
        x if x == ParticleType::FLOAT as u8 => {
            let val = FloatValue::new(NetworkEndian::read_f64(&buf[offset..offset+length]));
            Ok(Box::new(val))
        },
        x if x == ParticleType::STRING as u8 => {
            let val = StringValue::new(&try!(str::from_utf8(&buf[offset..offset+length].to_owned())).to_string());
            Ok(Box::new(val))
        },
        _ => unreachable!(),
    }
}

// ----------------------------------------------------------------------
// NullValue
// ----------------------------------------------------------------------
// #[derive(Debug,Clone)]
pub struct NullValue {}

impl core::fmt::Display for NullValue {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        try!("Null".fmt(f));
        Ok(())
    }
}

impl NullValue {
    pub fn new() -> NullValue {
        NullValue {}
    }

    fn unwrap(&self) -> Option<NullValue> {
        None
    }
}

impl Value for NullValue {
    fn particle_type(&self) -> ParticleType {
        ParticleType::NULL
    }

    fn as_bytes(&self) -> AerospikeResult<Vec<u8>> {
        Ok(vec![])
    }

    fn estimate_size(&self) -> usize {
        0
    }

    fn as_string(&self) -> String {
        "Null".to_string()
    }
}

// ----------------------------------------------------------------------
// IntValue
// ----------------------------------------------------------------------
// #[derive(Debug,Clone)]
pub struct IntValue {
    val: i64,
}

impl core::fmt::Display for IntValue {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        try!(self.val.fmt(f));
        Ok(())
    }
}

impl IntValue {
    pub fn new(val: i64) -> IntValue {
        IntValue { val: val }
    }

    fn unwrap(&self) -> i64 {
        self.val
    }
}

impl Value for IntValue {
    fn particle_type(&self) -> ParticleType {
        ParticleType::INTEGER
    }

    fn as_bytes(&self) -> AerospikeResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(8);
        buf.resize(8, 0);
        NetworkEndian::write_i64(&mut buf, self.val);
        Ok(buf)
    }

    fn estimate_size(&self) -> usize {
        8
    }

    fn as_string(&self) -> String {
        format!("{}", self.val)
    }
}

// ----------------------------------------------------------------------
// FloatValue
// ----------------------------------------------------------------------
// #[derive(Debug,Clone)]
pub struct FloatValue {
    val: f64,
}

impl core::fmt::Display for FloatValue {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        try!(self.val.fmt(f));
        Ok(())
    }
}

impl FloatValue {
    pub fn new(val: f64) -> FloatValue {
        FloatValue { val: val }
    }

    fn unwrap(&self) -> f64 {
        self.val
    }
}

impl Value for FloatValue {
    fn particle_type(&self) -> ParticleType {
        ParticleType::FLOAT
    }

    fn as_bytes(&self) -> AerospikeResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(8);
        buf.resize(8, 0);
        NetworkEndian::write_f64(&mut buf, self.val);
        Ok(buf)
    }

    fn estimate_size(&self) -> usize {
        8
    }

    fn as_string(&self) -> String {
        format!("{}", self.val)
    }
}


// ----------------------------------------------------------------------
// StringValue
// ----------------------------------------------------------------------
// #[derive(Debug,Clone)]
pub struct StringValue {
    val: String,
}

impl core::fmt::Display for StringValue {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        try!(self.val.fmt(f));
        Ok(())
    }
}

impl StringValue {
    pub fn new(val: &str) -> StringValue {
        StringValue { val: val.to_string() }
    }


    fn unwrap(&self) -> &String {
        &self.val
    }
}

impl Value for StringValue {
    fn particle_type(&self) -> ParticleType {
        ParticleType::STRING
    }

    fn as_bytes(&self) -> AerospikeResult<Vec<u8>> {
        Ok(self.val.as_bytes().to_vec())
    }

    fn estimate_size(&self) -> usize {
        self.val.len()
    }

    fn as_string(&self) -> String {
        self.val.to_string()
    }
}
