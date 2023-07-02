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

//! Traits and Implementations for writing data from structs and variables

use std::collections::HashMap;
use crate::errors::{ErrorKind, Result};
use crate::{Bin, Buffer, ParticleType};

pub use aerospike_macro::{WritableBins, WritableValue};

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

impl<S: WritableValue, T: WritableValue> WritableValue for HashMap<S, T> {
    fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        let mut size = 0;
        size += crate::msgpack::encoder::pack_map_begin(buffer, self.len());
        for (k, v) in self {
            size += k.write_as_cdt_value(buffer);
            size += v.write_as_cdt_value(buffer);
        }
        size
    }
    fn writable_value_particle_type(&self) -> ParticleType {
        ParticleType::MAP
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
