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

//! Traits and Implementations for reading data into structs and variables

use crate::errors::Result;
use crate::{Buffer, ParticleType, Value};
use std::collections::HashMap;

use crate::value::bytes_to_particle;
pub use aerospike_macro::ReadableBins;

/// The ReadableBins Trait is used to convert Aerospike Wire Data to Objects
pub trait ReadableBins: Sync + Sized + Send + Clone {
    /// Convert the pre-parsed Bins to a compatible Object
    /// The String in `data_points` is the field name returned by the Server.
    /// This can vary from the actual name in the Object if the rename attribute is used.
    fn read_bins_from_bytes(data_points: &mut HashMap<String, PreParsedBin>) -> Result<Self>;
    /// Default Fallback for Empty Bins
    /// Should be implemented for Types like Options and Lists.
    /// Defaults to throwing an Error
    fn new_empty() -> Result<Self> {
        bail!("No empty implementation found")
    }
}

/// The ReadableValue Trait is used to convert Aerospike Wire Data into the Value of Objects
pub trait ReadableValue: Sync + Sized + Send + Clone {
    /// Read the data from the Wire Buffer.
    /// This method is primarily used for pre-parsing checks
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self>;
    /// Actual conversion of the bytes to the value
    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self>;
    /// CDT values are parsed differently from normal Values. This buffer is not a copy, so modifications can cause wrong behavior.
    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self>;
}

/// Before giving data to the Readable Traits, the client pre-parses the wire data into this format#[derive(Debug)]
#[derive(Debug)]
pub struct PreParsedBin {
    /// Value is always given for any datatype.
    pub value: PreParsedValue,
    /// Lists and Maps can have other sub-values for entries. In this case, they need to be appended to the parsed value
    pub sub_values: Vec<PreParsedValue>,
}

/// Includes the data for the Value part of a Bin.
#[derive(Debug)]
pub struct PreParsedValue {
    /// The Particle Type the Sever stored the Value as.
    pub particle_type: u8,
    /// Part of the wire Buffer with only the relevant Value Data inside. Value starts at offset 0 without meta around it.
    pub buffer: Buffer,
    /// Amount of bytes that should be parsed as Value in the buffer. Should be used instead of buffer length for safety reasons.
    pub byte_length: usize,
}

impl ReadableBins for HashMap<String, Value> {
    fn read_bins_from_bytes(data_points: &mut HashMap<String, PreParsedBin>) -> Result<Self> {
        let mut hm = HashMap::new();
        for (k, d) in data_points {
            let x = Value::read_value_from_bytes(d)?;
            hm.insert(k.to_string(), x);
        }

        Ok(hm)
    }

    fn new_empty() -> Result<Self> {
        Ok(HashMap::new())
    }
}

impl ReadableValue for Value {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        let mut val = Value::parse_value(&mut data_point.value)?;

        for sv in &mut data_point.sub_values {
            let sval = Value::parse_value(sv)?;
            match val {
                Value::List(ref mut list) => list.push(sval),
                ref mut prev => {
                    *prev = as_list!(prev.clone(), sval);
                }
            }
        }
        return Ok(val);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        bytes_to_particle(
            data_point.particle_type,
            &mut data_point.buffer,
            data_point.byte_length,
        )
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        crate::msgpack::decoder::unpack_value(buff)
    }
}

impl ReadableValue for i64 {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!("No Value received for Integer")
        }
        if !data_point.sub_values.is_empty() {
            bail!("Multiple Values received for Integer")
        }
        return Self::parse_value(&mut data_point.value);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        return Ok(data_point.buffer.read_i64(None));
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ptype = buff.read_u8(None);
        match ptype {
            0x00..=0x7f => Ok(i64::from(ptype) as i64),
            0xcc => Ok(buff.read_u8(None) as i64),
            0xcd => Ok(buff.read_u16(None) as i64),
            0xce => Ok(buff.read_u32(None) as i64),
            0xcf => Ok(buff.read_u64(None) as i64),
            0xd0 => Ok(buff.read_i8(None) as i64),
            0xd1 => Ok(buff.read_i16(None) as i64),
            0xd2 => Ok(buff.read_i32(None) as i64),
            0xd3 => Ok(buff.read_i64(None) as i64),
            _ => bail!("Invalid Data Type for derive i64 CDT Value"),
        }
    }
}

impl ReadableValue for isize {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!("No Value received for Integer")
        }
        if !data_point.sub_values.is_empty() {
            bail!("Multiple Values received for Integer")
        }
        return Self::parse_value(&mut data_point.value);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        return Ok(data_point.buffer.read_i64(None) as isize);
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ptype = buff.read_u8(None);
        match ptype {
            0x00..=0x7f => Ok(i64::from(ptype) as isize),
            0xcc => Ok(buff.read_u8(None) as isize),
            0xcd => Ok(buff.read_u16(None) as isize),
            0xce => Ok(buff.read_u32(None) as isize),
            0xcf => Ok(buff.read_u64(None) as isize),
            0xd0 => Ok(buff.read_i8(None) as isize),
            0xd1 => Ok(buff.read_i16(None) as isize),
            0xd2 => Ok(buff.read_i32(None) as isize),
            0xd3 => Ok(buff.read_i64(None) as isize),
            _ => bail!("Invalid Data Type for derive isize CDT Value"),
        }
    }
}


impl ReadableValue for f64 {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!("No Value received for Float")
        }
        if !data_point.sub_values.is_empty() {
            bail!("Multiple Values received for Float")
        }
        return Self::parse_value(&mut data_point.value);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        return Ok(data_point.buffer.read_f64(None));
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ptype = buff.read_u8(None);
        match ptype {
            0xca => Ok(buff.read_f32(None) as f64),
            0xcb => Ok(buff.read_f64(None)),
            _ => bail!("Invalid Data Type for derive float CDT Value"),
        }
    }
}

impl ReadableValue for String {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!("No Value received for String")
        }
        if !data_point.sub_values.is_empty() {
            bail!("Multiple Values received for string")
        }
        return Self::parse_value(&mut data_point.value);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        return data_point.buffer.read_str(data_point.byte_length);
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let len = (buff.read_u8(None) & 0x1f) as usize;
        let ptype = buff.read_u8(None);
        if ptype != ParticleType::STRING as u8 {
            bail!("Invalid Data Type for derive string CDT Value")
        }
        return buff.read_str(len - 1);
    }
}

impl<T: ReadableValue> ReadableValue for Option<T> {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            return Ok(None);
        }
        Ok(Some(T::read_value_from_bytes(data_point)?))
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        Ok(Some(T::parse_value(data_point)?))
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        Ok(Some(T::parse_cdt_value(buff)?))
    }
}

impl<T: ReadableValue> ReadableValue for Vec<T> {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        let mut v: Vec<T> = Vec::with_capacity(data_point.sub_values.len() + 1);
        v.extend(Self::parse_value(&mut data_point.value)?);
        for sv in &mut data_point.sub_values {
            v.extend(Self::parse_value(sv)?)
        }
        return Ok(v);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        if data_point.particle_type == ParticleType::LIST as u8 {
            Self::parse_cdt_value(&mut data_point.buffer)
        } else {
            bail!("Invalid Data Type for derive List CDT Type")
        }
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ltype = buff.read_u8(None);
        let count: usize = match ltype {
            0x90..=0x9f => (ltype & 0x0f) as usize,
            0xdc => buff.read_u16(None) as usize,
            0xdd => buff.read_u32(None) as usize,
            _ => {
                bail!("Invalid Data Type for derive List CDT Type")
            }
        };

        let mut list = Vec::with_capacity(count);
        for _ in 0..count {
            list.push(T::parse_cdt_value(buff)?);
        }
        return Ok(list);
    }
}
