// Copyright 2015-2018 Aerospike, Inc.
//
// Commonly used Traits for Data input and output handling

use crate::commands::buffer::Buffer;
use crate::errors::Result;
use crate::{Bin, ParticleType, Value};
use std::collections::HashMap;

use crate::value::bytes_to_particle;
pub use aerospike_macro::{ReadableBins, WritableBins, WritableValue};

/// The WritableBins Trait is used to convert Objects to Aerospike Wire Data
pub trait WritableBins: Sync {
    /// Writes the Object as Bins to the Wire
    fn write_as_bins(&self, buffer: &mut Buffer, op_type: u8) -> Result<()>;
    /// The encoded size to size the buffer and set the offsets accordingly.
    /// Calculated by bin_name_bytes + value_bytes + 8
    /// Defaults to 0
    fn writable_bins_size(&self) -> usize {
        0
    }
    /// The amount of bins that will be processed. This is usually just the amount of struct members or list entries.
    /// Defaults to 0
    fn writable_bins_count(&self) -> usize {
        0
    }
}

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

/// The WritableValue Trait is used to convert Object Values to Aerospike Wire Data
pub trait WritableValue: Sync {
    /// Write the Object as Value of a Bin
    /// Requires `writable_value_size` and `writable_value_particle_type` to be overwritten to return the correct values
    /// Needs to return the byte size of the value
    fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize;
    /// Writes the Object as Value of a CDT
    /// Most CDT Objects (content of maps/lists etc.) are encoded differently from the normal Values
    fn write_as_cdt_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        self.write_as_value(buffer)
    }
    /// The particle Type of the value to write.
    /// This sets the Value Type for the Aerospike Server
    fn writable_value_particle_type(&self) -> ParticleType;
    /// Defines if the Object can be encoded
    /// For example empty Lists or Options should return false if no data is inside
    /// Defaults to true
    fn writable_value_encodable(&self) -> bool {
        true
    }
}

macro_rules! impl_writable_value_for_num {
    ($ty:ident) => {
        impl WritableValue for $ty {
            fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
                if let Some(ref mut buf) = *buffer {
                    buf.write_i64(*self as i64);
                }
                8
            }
            fn write_as_cdt_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
                crate::msgpack::encoder::pack_integer(buffer, *self as i64)
            }

            fn writable_value_particle_type(&self) -> ParticleType {
                ParticleType::INTEGER
            }
        }
    };
}

macro_rules! impl_writable_value_for_float {
    ($ty:ident) => {
        impl WritableValue for $ty {
            fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
                if let Some(ref mut buf) = *buffer {
                    buf.write_f64(f64::from(*self));
                }
                8
            }
            fn write_as_cdt_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
                crate::msgpack::encoder::pack_f64(buffer, f64::from(*self))
            }

            fn writable_value_particle_type(&self) -> ParticleType {
                ParticleType::FLOAT
            }
        }
    };
}

impl_writable_value_for_num!(u8);
impl_writable_value_for_num!(i8);
impl_writable_value_for_num!(u16);
impl_writable_value_for_num!(i16);
impl_writable_value_for_num!(u32);
impl_writable_value_for_num!(i32);
impl_writable_value_for_num!(u64);
impl_writable_value_for_num!(i64);
impl_writable_value_for_num!(usize);
impl_writable_value_for_num!(isize);
impl_writable_value_for_float!(f64);
impl_writable_value_for_float!(f32);

impl<T: WritableValue> WritableValue for Option<T> {
    fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        if let Some(v) = self {
            return v.write_as_value(buffer);
        }
        0
    }

    fn write_as_cdt_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        if let Some(v) = self {
            return v.write_as_cdt_value(buffer);
        }
        0
    }

    fn writable_value_particle_type(&self) -> ParticleType {
        if let Some(v) = self {
            return v.writable_value_particle_type();
        }
        ParticleType::NULL
    }

    fn writable_value_encodable(&self) -> bool {
        self.is_some()
    }
}

impl<T: WritableValue> WritableValue for Vec<T> {
    fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        let mut size = 0;
        size += crate::msgpack::encoder::pack_array_begin(buffer, self.len());
        for v in self {
            size += v.write_as_cdt_value(buffer)
        }
        size
    }

    fn writable_value_particle_type(&self) -> ParticleType {
        ParticleType::LIST
    }

    fn writable_value_encodable(&self) -> bool {
        !self.is_empty()
    }
}

impl WritableValue for String {
    fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        if let Some(ref mut buf) = *buffer {
            buf.write_str(&self);
        }
        self.len()
    }

    fn write_as_cdt_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        crate::msgpack::encoder::pack_string(buffer, &self)
    }

    fn writable_value_particle_type(&self) -> ParticleType {
        ParticleType::STRING
    }

    fn writable_value_encodable(&self) -> bool {
        !self.is_empty()
    }
}

impl WritableValue for bool {
    fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        if let Some(ref mut buf) = *buffer {
            buf.write_bool(*self);
        }
        1
    }

    fn write_as_cdt_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        crate::msgpack::encoder::pack_bool(buffer, *self)
    }

    fn writable_value_particle_type(&self) -> ParticleType {
        ParticleType::BOOL
    }
}

impl WritableValue for &str {
    fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        if let Some(ref mut buf) = *buffer {
            buf.write_str(self);
        }
        self.len()
    }

    fn write_as_cdt_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        crate::msgpack::encoder::pack_string(buffer, self)
    }

    fn writable_value_particle_type(&self) -> ParticleType {
        ParticleType::STRING
    }

    fn writable_value_encodable(&self) -> bool {
        !self.is_empty()
    }
}
impl<const COUNT: usize> WritableBins for [Bin; COUNT] {
    fn write_as_bins(&self, buffer: &mut Buffer, op_type: u8) -> Result<()> {
        legacy_bins_slice_write_wire(self.as_slice(), buffer, op_type)
    }
    fn writable_bins_size(&self) -> usize {
        legacy_bins_slice_writable_size(self.as_slice())
    }
    fn writable_bins_count(&self) -> usize {
        self.len()
    }
}

impl WritableBins for &[Bin] {
    fn write_as_bins(&self, buffer: &mut Buffer, op_type: u8) -> Result<()> {
        legacy_bins_slice_write_wire(&self, buffer, op_type)
    }
    fn writable_bins_size(&self) -> usize {
        legacy_bins_slice_writable_size(&self)
    }
    fn writable_bins_count(&self) -> usize {
        self.len()
    }
}

impl WritableBins for Vec<Bin> {
    fn write_as_bins(&self, buffer: &mut Buffer, op_type: u8) -> Result<()> {
        legacy_bins_slice_write_wire(self.as_slice(), buffer, op_type)
    }
    fn writable_bins_size(&self) -> usize {
        legacy_bins_slice_writable_size(self.as_slice())
    }
    fn writable_bins_count(&self) -> usize {
        self.len()
    }
}

fn legacy_bins_slice_write_wire(bins: &[Bin], buffer: &mut Buffer, op_type: u8) -> Result<()> {
    bins.iter().for_each(|b| {
        buffer.write_i32((b.name.len() + b.value.estimate_size() + 4) as i32);
        buffer.write_u8(op_type);
        buffer.write_u8(b.value.particle_type() as u8);
        buffer.write_u8(0);
        buffer.write_u8(b.name.len() as u8);
        buffer.write_str(&b.name);
        b.value.write_to(buffer);
    });
    Ok(())
}

fn legacy_bins_slice_writable_size(bins: &[Bin]) -> usize {
    let mut size: usize = 0;
    bins.iter().for_each(|b| {
        size += b.name.len() + b.value.estimate_size() + 8;
    });
    size
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

impl ReadableValue for usize {
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
        return Ok(data_point.buffer.read_i64(None) as usize);
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ptype = buff.read_u8(None);
        match ptype {
            0x00..=0x7f => Ok(i64::from(ptype) as usize),
            0xcc => Ok(buff.read_u8(None) as usize),
            0xcd => Ok(buff.read_u16(None) as usize),
            0xce => Ok(buff.read_u32(None) as usize),
            0xcf => Ok(buff.read_u64(None) as usize),
            0xd0 => Ok(buff.read_i8(None) as usize),
            0xd1 => Ok(buff.read_i16(None) as usize),
            0xd2 => Ok(buff.read_i32(None) as usize),
            0xd3 => Ok(buff.read_i64(None) as usize),
            _ => bail!("Invalid Data Type for derive usize CDT Value"),
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

impl ReadableValue for u64 {
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
        return Ok(data_point.buffer.read_i64(None) as u64);
    }

    fn parse_cdt_value(buff: &mut Buffer) -> Result<Self> {
        let ptype = buff.read_u8(None);
        match ptype {
            0x00..=0x7f => Ok(i64::from(ptype) as u64),
            0xcc => Ok(buff.read_u8(None) as u64),
            0xcd => Ok(buff.read_u16(None) as u64),
            0xce => Ok(buff.read_u32(None) as u64),
            0xcf => Ok(buff.read_u64(None)),
            0xd0 => Ok(buff.read_i8(None) as u64),
            0xd1 => Ok(buff.read_i16(None) as u64),
            0xd2 => Ok(buff.read_i32(None) as u64),
            0xd3 => Ok(buff.read_i64(None) as u64),
            _ => bail!("Invalid Data Type for derive u64 CDT Value"),
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
