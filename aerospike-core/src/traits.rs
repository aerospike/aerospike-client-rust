// Copyright 2015-2018 Aerospike, Inc.
//
// Commonly used Traits for Data input and output handling

use crate::errors::{Result};
use crate::commands::buffer::Buffer;
use crate::{Bin, ParticleType};

pub use aerospike_macro::{WritableBins, WritableValue};

/// The WritableBins Trait is used to convert normal Objects to Bins
pub trait WritableBins: Sync {
    // Writes the Object as Bins to the Wire
    // See the implementation of the Writable derive macro for Details on bin encoding
    fn write_as_bins(&self, buffer: &mut Buffer, op_type: u8) -> Result<()>;
    // The encoded size to size the buffer and set the offsets accordingly.
    // Calculated by bin_name_bytes + value_bytes + 8
    fn writable_bins_size(&self) -> usize { 0 }
    // The amount of bins that will be processed. This is usually just the amount of struct members or list entries.
    fn writable_bins_count(&self) -> usize { 0 }

}

/// The WritableValue Trait is used to convert normal Objects to Values
pub trait WritableValue: Sync {
    // Write the Object as Value of a Bin
    // Requires `writable_value_size` and `writable_value_particle_type` to be overwritten to return the correct values
    // Needs to return the byte size of the value
    fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize;
    // Writes the Object as Value of a CDT
    // Not always required, but some datatypes are packed differently. For example Strings.
    fn write_as_cdt_value(&self, buffer: &mut Option<&mut Buffer>) -> usize { self.write_as_value(buffer) }
    // The particle Type of the value to write.
    fn writable_value_particle_type(&self) -> ParticleType;
    // Defines if the Object can be encoded
    fn writable_value_encodable(&self) -> bool { true }
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

            fn writable_value_particle_type(&self) -> ParticleType { ParticleType::INTEGER }
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

impl<T: WritableValue> WritableValue for Option<T> {
    fn write_as_value(&self, buffer: &mut Option<&mut Buffer>) -> usize {
        if let Some(v) = self {
            return v.write_as_value(buffer)
        }
        0
    }

    fn writable_value_particle_type(&self) -> ParticleType {
        if let Some(v) = self {
            return v.writable_value_particle_type()
        }
        ParticleType::NULL
    }

    fn writable_value_encodable(&self) -> bool {
        self.is_some()
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
    fn write_as_bins(&self, buffer: &mut Buffer, op_type: u8) -> Result<()>{ legacy_bins_slice_write_wire(self.as_slice(), buffer, op_type) }
    fn writable_bins_size(&self) -> usize { legacy_bins_slice_writable_size(self.as_slice()) }
    fn writable_bins_count(&self) -> usize { self.len() }
}

impl WritableBins for &[Bin] {
    fn write_as_bins(&self, buffer: &mut Buffer, op_type: u8) -> Result<()>{ legacy_bins_slice_write_wire(&self, buffer, op_type) }
    fn writable_bins_size(&self) -> usize { legacy_bins_slice_writable_size(&self) }
    fn writable_bins_count(&self) -> usize { self.len() }
}

impl WritableBins for Vec<Bin> {
    fn write_as_bins(&self, buffer: &mut Buffer, op_type: u8) -> Result<()> { legacy_bins_slice_write_wire(self.as_slice(), buffer, op_type) }
    fn writable_bins_size(&self) -> usize { legacy_bins_slice_writable_size(self.as_slice()) }
    fn writable_bins_count(&self) -> usize { self.len() }
}

fn legacy_bins_slice_write_wire(bins: &[Bin], buffer: &mut Buffer, op_type: u8) -> Result<()>{
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