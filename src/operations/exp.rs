use crate::expressions::FilterExpression;
use crate::operations::{Operation, OperationType, OperationBin, OperationData};
use crate::commands::buffer::Buffer;
use crate::errors::Result;
use crate::msgpack::encoder::{pack_array_begin, pack_integer};
use crate::ParticleType;

/// Expression write Flags
pub enum ExpWriteFlags {
    /// Default. Allow create or update.
    Default = 0,
    /// If bin does not exist, a new bin will be created.
    /// If bin exists, the operation will be denied.
    /// If bin exists, fail with Bin Exists
    CreateOnly = 1,
    /// If bin exists, the bin will be overwritten.
    /// If bin does not exist, the operation will be denied.
    /// If bin does not exist, fail with Bin Not Found
    UpdateOnly = 2,
    /// If expression results in nil value, then delete the bin.
    /// Otherwise, return OP Not Applicable when NoFail is not set
    AllowDelete = 3,
    /// Do not raise error if operation is denied.
    PolicyNoFail = 8,
    /// Ignore failures caused by the expression resolving to unknown or a non-bin type.
    EvalNoFail = 16
}

pub type ExpressionEncoder = Box<dyn Fn(&mut Option<&mut Buffer>, &ExpOperation) -> Result<usize>>;

pub struct ExpOperation<'a>{
    pub encoder: ExpressionEncoder,
    pub policy: i64,
    pub exp: &'a FilterExpression,
}

impl<'a> ExpOperation<'a> {
    pub const fn particle_type(&self) -> ParticleType {
        ParticleType::BLOB
    }

    pub fn estimate_size(&self) -> Result<usize> {
        let size: usize = (self.encoder)(&mut None, self)?;
        Ok(size)
    }

    pub fn write_to(&self, buffer: &mut Buffer) -> Result<usize> {
        let size: usize = (self.encoder)(&mut Some(buffer), self)?;
        Ok(size)
    }
}

/// Expression read Flags
pub enum ExpReadFlags {
    /// Default
    Default = 0,
    /// Ignore failures caused by the expression resolving to unknown or a non-bin type.
    EvalNoFail = 16
}

pub fn write<'a>(flags: ExpWriteFlags, bin: &'a str, exp: &'a FilterExpression) -> Operation<'a> {
    let op = ExpOperation{
        encoder: Box::new(pack_write_exp),
        policy: flags as i64,
        exp
    };
    Operation {
        op: OperationType::ExpWrite,
        ctx: &[],
        bin: OperationBin::Name(bin),
        data: OperationData::EXPOp(op)
    }
}

pub fn read(exp: &FilterExpression) -> Operation {
    let op = ExpOperation {
        encoder: Box::new(pack_read_exp),
        policy: 0,
        exp
    };
    Operation {
        op: OperationType::ExpRead,
        ctx: &[],
        bin: OperationBin::None,
        data: OperationData::EXPOp(op)
    }
}

fn pack_write_exp(buf: &mut Option<&mut Buffer>, exp_op: &ExpOperation) -> Result<usize>{
    let mut size = 0;
    size += pack_array_begin(buf, 2)?;
    size += exp_op.exp.pack(buf)?;
    size += pack_integer(buf, exp_op.policy)?;
    Ok(size)
}

fn pack_read_exp(buf: &mut Option<&mut Buffer>, exp_op: &ExpOperation) -> Result<usize>{
    let mut size = 0;
    size += pack_array_begin(buf, 1)?;
    size += exp_op.exp.pack(buf)?;
    Ok(size)
}