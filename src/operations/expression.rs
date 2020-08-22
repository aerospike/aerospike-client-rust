use crate::operations::exp::Exp;

pub struct Expression {
    bytes: [u8]
}

impl Expression {
    pub fn new(exp: Exp) -> &Expression{
        // Implement Packer
        unimplemented!()
    }

    fn get_serial_version_uid() -> i64{
        1
    }
}