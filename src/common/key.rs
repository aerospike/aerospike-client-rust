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

use std::fmt;

use error::AerospikeResult;
use value::Value;

use crypto::ripemd160::Ripemd160;
use crypto::digest::Digest;

#[derive(Debug,Clone)]
pub struct Key {
    pub namespace: String,
    pub set_name: String,
    pub digest: [u8; 20],
    pub user_key: Option<Value>,
}

impl Key {
    pub fn new<S>(namespace: S, set_name: S, key: Value) -> AerospikeResult<Self>
        where S: Into<String>
    {
        let mut key = Key {
            namespace: namespace.into(),
            set_name: set_name.into(),
            digest: [0; 20],
            user_key: Some(key),
        };

        try!(key.compute_digest());
        Ok(key)
    }

    fn compute_digest(&mut self) -> AerospikeResult<()> {
        let mut hash = Ripemd160::new();
        hash.input(self.set_name.as_bytes());
        if let Some(ref user_key) = self.user_key {
            hash.input(&[user_key.particle_type() as u8]);
            try!(user_key.write_key_bytes(&mut hash));
        } else {
            unreachable!()
        }
        hash.result(&mut self.digest);

        Ok(())
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self.user_key {
            Some(ref value) => { 
                write!(f, "<Key: ns=\"{}\", set=\"{}\", key=\"{}\">", &self.namespace, &self.set_name, value)
            }
            None => {
                write!(f, "<Key: ns=\"{}\", set=\"{}\", digest=\"{:?}\">", &self.namespace, &self.set_name, &self.digest)
            }
        }
    }
}


#[macro_export]
macro_rules! as_key {
    ($ns:expr, $set:expr, $val:expr) => {{
        Key::new($ns, $set, Value::from($val)).unwrap()
    }};
}


#[cfg(test)]
mod tests {
    use super::*;
    use value::Value;

    static NS: &'static str = "namespace";
    static SET: &'static str = "set";

    #[test]
    fn int_keys() {
        let expected = vec![
            0x82, 0xd7, 0x21, 0x3b, 0x46, 0x98, 0x12, 0x94, 0x7c, 0x10,
            0x9a, 0x6d, 0x34, 0x1e, 0x3b, 0x5b, 0x1d, 0xed, 0xec, 0x1f
        ];
        assert_eq!(expected, as_key!(NS, SET, 1).digest);
        assert_eq!(expected, as_key!(NS, SET, &1).digest);
        assert_eq!(expected, as_key!(NS, SET, 1i8).digest);
        assert_eq!(expected, as_key!(NS, SET, &1i8).digest);
        assert_eq!(expected, as_key!(NS, SET, 1u8).digest);
        assert_eq!(expected, as_key!(NS, SET, &1u8).digest);
        assert_eq!(expected, as_key!(NS, SET, 1i16).digest);
        assert_eq!(expected, as_key!(NS, SET, &1i16).digest);
        assert_eq!(expected, as_key!(NS, SET, 1u16).digest);
        assert_eq!(expected, as_key!(NS, SET, &1u16).digest);
        assert_eq!(expected, as_key!(NS, SET, 1i32).digest);
        assert_eq!(expected, as_key!(NS, SET, &1i32).digest);
        assert_eq!(expected, as_key!(NS, SET, 1u32).digest);
        assert_eq!(expected, as_key!(NS, SET, &1u32).digest);
        assert_eq!(expected, as_key!(NS, SET, 1i64).digest);
        assert_eq!(expected, as_key!(NS, SET, &1i64).digest);
    }

    #[test]
    fn string_keys() {
        let expected = vec![
            0x36, 0xeb, 0x02, 0xa8, 0x07, 0xdb, 0xad, 0xe8, 0xcd, 0x78,
            0x4e, 0x78, 0x00, 0xd7, 0x63, 0x08, 0xb4, 0xe8, 0x92, 0x12
        ];
        assert_eq!(expected, as_key!(NS, SET, "haha").digest);
        assert_eq!(expected, as_key!(NS, SET, "haha".to_string()).digest);
        assert_eq!(expected, as_key!(NS, SET, &"haha".to_string()).digest);
    }

    #[test]
    fn blob_keys() {
        let expected = vec![
            0x81, 0xf0, 0xf1, 0xb8, 0xfb, 0x1e, 0x28, 0xcf, 0xfe, 0x37,
            0xd3, 0x5a, 0x4f, 0xd9, 0xaf, 0xbb, 0x76, 0x1d, 0x50, 0x12
        ];
        assert_eq!(expected, as_key!(NS, SET, vec![0x68, 0x61, 0x68, 0x61]).digest);
    }

    #[test]
    #[should_panic(expected = "Data type is not supported as Key value.")]
    fn unsupported_key_type() {
        as_key!(NS, SET, 3.1415);
    }
}
