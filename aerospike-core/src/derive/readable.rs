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

use crate::errors::{ErrorKind, Result};
use crate::{Buffer, ParticleType, Value};
use rand::distributions::weighted::alias_method::Weight;
use std::collections::HashMap;

use crate::value::bytes_to_particle;
pub use aerospike_macro::{ReadableBins, ReadableValue};

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
            bail!(ErrorKind::Derive(
                "No Value received for Integer".to_string()
            ))
        }
        if !data_point.sub_values.is_empty() {
            bail!(ErrorKind::Derive(
                "Multiple Values received for Integer".to_string()
            ))
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
            _ => Err(
                ErrorKind::Derive("Invalid Data Type for derive i64 CDT Value".to_string()).into(),
            ),
        }
    }
}

impl ReadableValue for i32 {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!(ErrorKind::Derive(
                "No Value received for Integer".to_string()
            ))
        }
        if !data_point.sub_values.is_empty() {
            bail!(ErrorKind::Derive(
                "Multiple Values received for Integer".to_string()
            ))
        }
        return Self::parse_value(&mut data_point.value);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        let data = data_point.buffer.read_i64(None);
        if data > i32::MAX as i64 {
            bail!(ErrorKind::Derive(format!(
                "Tried to cast an Integer > {} to i32",
                i32::MAX
            )))
        }
        Ok(data as i32)
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ptype = buff.read_u8(None);
        let data = match ptype {
            0x00..=0x7f => i64::from(ptype) as i64,
            0xcc => buff.read_u8(None) as i64,
            0xcd => buff.read_u16(None) as i64,
            0xce => buff.read_u32(None) as i64,
            0xcf => buff.read_u64(None) as i64,
            0xd0 => buff.read_i8(None) as i64,
            0xd1 => buff.read_i16(None) as i64,
            0xd2 => buff.read_i32(None) as i64,
            0xd3 => buff.read_i64(None) as i64,
            _ => {
                return Err(ErrorKind::Derive(
                    "Invalid Data Type for derive i32 CDT Value".to_string(),
                )
                .into())
            }
        };
        if data > i32::MAX as i64 {
            bail!(ErrorKind::Derive(format!(
                "Tried to cast an Integer > {} to i32",
                i32::MAX
            )))
        }
        Ok(data as i32)
    }
}

impl ReadableValue for i16 {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!(ErrorKind::Derive(
                "No Value received for Integer".to_string()
            ))
        }
        if !data_point.sub_values.is_empty() {
            bail!(ErrorKind::Derive(
                "Multiple Values received for Integer".to_string()
            ))
        }
        return Self::parse_value(&mut data_point.value);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        let data = data_point.buffer.read_i64(None);
        if data > i16::MAX as i64 {
            bail!(ErrorKind::Derive(format!(
                "Tried to cast an Integer > {} to i16",
                i16::MAX
            )))
        }
        Ok(data as i16)
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ptype = buff.read_u8(None);
        let data = match ptype {
            0x00..=0x7f => i64::from(ptype) as i64,
            0xcc => buff.read_u8(None) as i64,
            0xcd => buff.read_u16(None) as i64,
            0xce => buff.read_u32(None) as i64,
            0xcf => buff.read_u64(None) as i64,
            0xd0 => buff.read_i8(None) as i64,
            0xd1 => buff.read_i16(None) as i64,
            0xd2 => buff.read_i32(None) as i64,
            0xd3 => buff.read_i64(None) as i64,
            _ => {
                return Err(ErrorKind::Derive(
                    "Invalid Data Type for derive i16 CDT Value".to_string(),
                )
                .into())
            }
        };
        if data > i16::MAX as i64 {
            bail!(ErrorKind::Derive(format!(
                "Tried to cast an Integer > {} to i16",
                i16::MAX
            )))
        }
        Ok(data as i16)
    }
}

impl ReadableValue for i8 {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!(ErrorKind::Derive(
                "No Value received for Integer".to_string()
            ))
        }
        if !data_point.sub_values.is_empty() {
            bail!(ErrorKind::Derive(
                "Multiple Values received for Integer".to_string()
            ))
        }
        return Self::parse_value(&mut data_point.value);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        let data = data_point.buffer.read_i64(None);
        if data > i8::MAX as i64 {
            bail!(ErrorKind::Derive(format!(
                "Tried to cast an Integer > {} to i8",
                i8::MAX
            )))
        }
        Ok(data as i8)
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ptype = buff.read_u8(None);
        let data = match ptype {
            0x00..=0x7f => i64::from(ptype),
            0xcc => buff.read_u8(None) as i64,
            0xcd => buff.read_u16(None) as i64,
            0xce => buff.read_u32(None) as i64,
            0xcf => buff.read_u64(None) as i64,
            0xd0 => buff.read_i8(None) as i64,
            0xd1 => buff.read_i16(None) as i64,
            0xd2 => buff.read_i32(None) as i64,
            0xd3 => buff.read_i64(None) as i64,
            _ => {
                return Err(ErrorKind::Derive(
                    "Invalid Data Type for derive i8 CDT Value".to_string(),
                )
                .into())
            }
        };
        if data > i8::MAX as i64 {
            bail!(ErrorKind::Derive(format!(
                "Tried to cast an Integer > {} to i8",
                i8::MAX
            )))
        }
        Ok(data as i8)
    }
}

impl ReadableValue for isize {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!(ErrorKind::Derive(
                "No Value received for Integer".to_string()
            ))
        }
        if !data_point.sub_values.is_empty() {
            bail!(ErrorKind::Derive(
                "Multiple Values received for Integer".to_string()
            ))
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
            _ => Err(
                ErrorKind::Derive("Invalid Data Type for derive isize CDT Value".to_string())
                    .into(),
            ),
        }
    }
}

impl ReadableValue for f64 {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!(ErrorKind::Derive("No Value received for Float".to_string()))
        }
        if !data_point.sub_values.is_empty() {
            bail!(ErrorKind::Derive(
                "Multiple Values received for Float".to_string()
            ))
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
            _ => bail!(ErrorKind::Derive(
                "Invalid Data Type for derive float CDT Value".to_string()
            )),
        }
    }
}

impl ReadableValue for String {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        if data_point.value.particle_type == 0 {
            bail!(ErrorKind::Derive(
                "No Value received for String".to_string()
            ))
        }
        if !data_point.sub_values.is_empty() {
            bail!(ErrorKind::Derive(
                "Multiple Values received for string".to_string()
            ))
        }
        return Self::parse_value(&mut data_point.value);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        return data_point.buffer.read_str(data_point.byte_length);
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        // todo: fix this
        let len = (buff.read_u8(None) & 0x1f) as usize;
        let ptype = buff.read_u8(None);
        if ptype != ParticleType::STRING as u8 {
            bail!(ErrorKind::Derive(
                "Invalid Data Type for derive string CDT Value".to_string()
            ))
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
        if buff.data_buffer.len() > 0 {
            Ok(Some(T::parse_cdt_value(buff)?))
        } else {
            Ok(None)
        }
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
            bail!(ErrorKind::Derive(
                "Invalid Data Type for derive List CDT Type".to_string()
            ))
        }
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ltype = buff.read_u8(None);
        let count: usize = match ltype {
            0x90..=0x9f => (ltype & 0x0f) as usize,
            0xdc => buff.read_u16(None) as usize,
            0xdd => buff.read_u32(None) as usize,
            _ => {
                bail!(ErrorKind::Derive(
                    "Invalid Data Type for derive List CDT Type".to_string()
                ))
            }
        };

        let mut list = Vec::with_capacity(count);
        for _ in 0..count {
            list.push(T::parse_cdt_value(buff)?);
        }
        return Ok(list);
    }
}

impl<T: ReadableValue + Eq + std::hash::Hash, S: ReadableValue> ReadableValue for HashMap<T, S> {
    fn read_value_from_bytes(data_point: &mut PreParsedBin) -> Result<Self> {
        let mut v: HashMap<T, S> = HashMap::with_capacity(data_point.sub_values.len() + 1);
        let v1 = Self::parse_value(&mut data_point.value)?;
        v1.into_iter().for_each(|(key, val)| { v.insert(key, val); });
        for sv in &mut data_point.sub_values {
            let v2 = Self::parse_value(sv)?;
            v2.into_iter().for_each(|(key, val)| { v.insert(key, val); });
        }
        return Ok(v);
    }

    fn parse_value(data_point: &mut PreParsedValue) -> Result<Self> {
        if data_point.particle_type == ParticleType::MAP as u8 {
            Self::parse_cdt_value(&mut data_point.buffer)
        } else {
            bail!(ErrorKind::Derive(
                "Invalid Data Type for derive List CDT Type".to_string()
            ))
        }
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        if buff.data_buffer.is_empty() {
            return Ok(HashMap::new());
        }

        let ltype = buff.read_u8(None);

        let count: usize = match ltype {
            0x80..=0x8f => (ltype & 0x0f) as usize,
            0xde => buff.read_u16(None) as usize,
            0xdf => buff.read_u32(None) as usize,
            _ => {
                bail!(ErrorKind::Derive(
                    "Invalid Data Type for derive Map CDT Type".to_string()
                ))
            }
        };

        let mut map: HashMap<T, S> = HashMap::with_capacity(count);

        for _ in 0..count {
            let key = T::parse_cdt_value(buff)?;
            let val = S::parse_cdt_value(buff)?;
            map.insert(key, val);
        }
        return Ok(map);
    }
}

/// Used to skip values for struct derives
pub fn skip_map_value_bytes(buff: &mut Buffer) -> Result<()> {
    let vtype = buff.read_u8(None);
    match vtype as usize {
        0x00..=0x7f | 0xc2 | 0xc3 | 0xe0..=0xff => {}
        // todo: this is wrong, parse element lengths instead
        0x80..=0x8f => {
            let len = (vtype & 0x0f) as usize;
            for _ in 0..len {
                skip_map_value_bytes(buff)?;
                skip_map_value_bytes(buff)?;
            }
        },
        0x90..=0x9f => {
            let len = vtype & 0x0f;
            for _ in 0..len {
                skip_map_value_bytes(buff)?;
            }
        }
        0xa0..=0xbf => {
            let len = vtype & 0x1f;
            for _ in 0..len {
                skip_map_value_bytes(buff)?;
            }
        }
        0xdc => {
            let len = buff.read_u16(None);
            for _ in 0..len {
                skip_map_value_bytes(buff)?;
            }
        }
        0xdd => {
            let len = 0..buff.read_u32(None);
            for _ in len {
                skip_map_value_bytes(buff)?;
            }
        }
        0xde => {
            let len = 0..buff.read_u16(None);
            for _ in len {
                skip_map_value_bytes(buff)?;
                skip_map_value_bytes(buff)?;
            }
        }
        0xdf => {
            let len = 0..buff.read_u32(None);
            for _ in len {
                skip_map_value_bytes(buff)?;
                skip_map_value_bytes(buff)?;
            }
        }
        0xc4 | 0xd9 => {
            let l = buff.read_u8(None) as usize;
            buff.skip(l)
        }
        0xc4 | 0xd9 => {
            let l = buff.read_u16(None) as usize;
            buff.skip(l)
        }
        0xc4 | 0xd9 => {
            let l = buff.read_u32(None) as usize;
            buff.skip(l)
        }
        0xc7 => {
            let l = buff.read_u8(None);
            buff.skip(1 + l as usize)
        }
        0xc8 => {
            let l = buff.read_u16(None);
            buff.skip(1 + l as usize)
        }
        0xc9 => {
            let l = buff.read_u32(None);
            buff.skip(1 + l as usize)
        }
        0xcc | 0xd0 => buff.skip(1),
        0xcd | 0xd1 | 0xd4 => buff.skip(2),
        0xce | 0xd2 | 0xca => buff.skip(4),
        0xcf | 0xd3 | 0xcb => buff.skip(8),
        0xd5 => buff.skip(3),
        0xd6 => buff.skip(5),
        0xd7 => buff.skip(9),
        0xd8 => buff.skip(17),
        _ => {
            return Err(ErrorKind::BadResponse(format!(
                "Error unpacking value of type '{:x}'",
                vtype
            ))
            .into())
        }
    };
    Ok(())
}

/// Used to parse values for struct derives
pub fn read_map_value_bytes<T: ReadableValue>(buff: &mut Buffer, offset: Option<&usize>) -> Result<T> {
    println!("{:?}", offset);
    if let Some(offset) = offset {
        buff.data_offset = *offset;
        let v: T = ReadableValue::parse_cdt_value(buff)?;
        Ok(v)
    } else {
        let mut tmp = Buffer::new(0);
        let v: T = ReadableValue::parse_cdt_value(&mut tmp)?;
        Ok(v)
    }
}
