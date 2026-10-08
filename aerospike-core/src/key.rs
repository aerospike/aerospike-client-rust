// Copyright 2015-2018 Aerospike, Inc.
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
use std::io::Cursor;
use std::result::Result as StdResult;

use crate::cluster::node;
use crate::errors::Result;
use crate::Value;

use byteorder::{LittleEndian, ReadBytesExt};
use ripemd::digest::Digest;
use ripemd::Ripemd160;

#[cfg(feature = "serialization")]
use serde::Serialize;
/// Unique record identifier.
///
/// Records can be identified using a specified namespace, an optional set name and a user defined
/// key which must be unique within a set. Records can also be identified by namespace/digest,
/// which is the combination used on the server.
///
/// The digest is derived from the set name and the user key when the key is
/// built, so the parts are read through accessors and cannot drift apart.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct Key {
    pub(crate) namespace: String,
    pub(crate) set_name: String,
    pub(crate) user_key: Option<Value>,
    pub(crate) digest: [u8; 20],
}

impl Key {
    /// The namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The set name; empty when the key has no set.
    #[must_use]
    pub fn set_name(&self) -> &str {
        &self.set_name
    }

    /// The user key, when the key was built from one.
    #[must_use]
    pub const fn user_key(&self) -> Option<&Value> {
        self.user_key.as_ref()
    }

    /// The 20-byte digest the server addresses the record by.
    #[must_use]
    pub const fn digest(&self) -> [u8; 20] {
        self.digest
    }
}

impl Key {
    /// Construct a new key given a namespace, a set name and a user key value.
    ///
    /// # Errors
    ///
    /// Only integers, strings and blobs (`Vec<u8>`) can be used as user keys;
    /// any other value type is rejected with `InvalidArgument`.
    pub fn new(
        namespace: impl Into<String>,
        set_name: impl Into<String>,
        key: Value,
    ) -> Result<Self> {
        let mut key = Key {
            namespace: namespace.into(),
            set_name: set_name.into(),
            digest: [0; 20],
            user_key: Some(key),
        };

        key.compute_digest()?;
        Ok(key)
    }

    /// Construct a key from a digest the caller already has, for example one
    /// read back from a record or stored elsewhere. The server addresses
    /// records by digest only, so the digest is taken as given and not
    /// validated; an empty `set_name` means no set, and `user_key` is sent to
    /// the server only when a write policy asks for it.
    #[must_use]
    pub fn with_digest(
        namespace: impl Into<String>,
        set_name: impl Into<String>,
        user_key: Option<Value>,
        digest: [u8; 20],
    ) -> Self {
        Self {
            namespace: namespace.into(),
            set_name: set_name.into(),
            user_key,
            digest,
        }
    }

    fn compute_digest(&mut self) -> Result<()> {
        let mut hash = Ripemd160::new();
        hash.update(self.set_name.as_bytes());
        if let Some(ref user_key) = self.user_key {
            hash.update([user_key.particle_type()?]);
            user_key.write_key_bytes(&mut hash)?;
        } else {
            unreachable!();
        }
        self.digest = hash.finalize().into();

        Ok(())
    }

    pub(crate) const fn has_value_to_send(&self) -> bool {
        self.user_key.is_some()
    }

    /// Returns the partitionId of the key.
    ///
    /// # Panics
    ///
    /// Panics if the digest slice cannot be read as a `u32` in little-endian byte order.
    pub fn partition_id(&self) -> usize {
        let mut rdr = Cursor::new(&self.digest[0..4]);

        // CAN'T USE MOD directly - mod will give negative numbers.
        // First AND makes positive and negative correctly, then mod.
        // For any x, y : x % 2^y = x & (2^y - 1); the second method is twice as fast
        rdr.read_u32::<LittleEndian>().unwrap() as usize & (node::PARTITIONS - 1)
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> StdResult<(), fmt::Error> {
        match self.user_key {
            Some(ref value) => write!(
                f,
                "<Key: ns=\"{}\", set=\"{}\", key=\"{}\">",
                &self.namespace, &self.set_name, value
            ),
            None => write!(
                f,
                "<Key: ns=\"{}\", set=\"{}\", digest=\"{:?}\">",
                &self.namespace, &self.set_name, &self.digest
            ),
        }
    }
}

/// Construct a new key given a namespace, a set name and a user key.
///
/// # Panics
///
/// Only integers, strings and blobs (`Vec<u8>`) can be used as user keys. The macro will
/// panic if any other value type is passed.
#[macro_export]
macro_rules! as_key {
    ($ns:expr, $set:expr, $val:expr) => {{
        $crate::Key::new($ns, $set, $crate::Value::from($val)).unwrap()
    }};
}

#[cfg(test)]
mod tests {

    macro_rules! digest {
        ($x:expr) => {
            hex::encode(as_key!("namespace", "set", $x).digest)
        };
    }
    macro_rules! str_repeat {
        ($c:expr, $n:expr) => {
            std::iter::repeat_n($c, $n).collect::<String>()
        };
    }

    #[test]
    fn mixed_string_types_and_unsupported_user_keys() {
        let ns = String::from("test");
        let key = crate::Key::new(ns, "set", crate::Value::from(1)).unwrap();
        assert_eq!(key.namespace, "test");
        assert!(crate::Key::new("test", "set", crate::Value::from(1.5)).is_err());
        assert!(crate::Key::new("test", "set", crate::Value::Nil).is_err());
    }

    #[test]
    fn with_digest_keeps_the_digest_as_given() {
        let digest = [7u8; 20];
        let key = crate::Key::with_digest("test", "", None, digest);
        assert_eq!(key.digest, digest);
        assert_eq!(key.set_name, "");
        assert!(key.user_key.is_none());
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn int_keys() {
        assert_eq!(digest!(0), "93d943aae37b017ad7e011b0c1d2e2143c2fb37d");
        assert_eq!(digest!(-1), "22116d253745e29fc63fdf760b6e26f7e197e01d");

        assert_eq!(digest!(1i8), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(&1i8), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1u8), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(&1u8), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1i16), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(&1i16), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1u16), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(&1u16), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1i32), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(&1i32), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1u32), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(&1u32), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1i64), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(&1i64), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(1isize), "82d7213b469812947c109a6d341e3b5b1dedec1f");
        assert_eq!(digest!(&1isize), "82d7213b469812947c109a6d341e3b5b1dedec1f");

        assert_eq!(
            digest!(i64::MIN),
            "7185c2a47fb02c996daed26b4e01b83240aee9d4"
        );
        assert_eq!(
            digest!(i64::MAX),
            "1698328974afa62c8e069860c1516f780d63dbb8"
        );
        assert_eq!(
            digest!(i32::MIN),
            "d635a867b755f8f54cdc6275e6fb437df82a728c"
        );
        assert_eq!(
            digest!(i32::MAX),
            "fa8c47b8b898af1bbcb20af0d729ca68359a2645"
        );
        assert_eq!(
            digest!(i16::MIN),
            "7f41e9dd1f3fe3694be0430e04c8bfc7d51ec2af"
        );
        assert_eq!(
            digest!(i16::MAX),
            "309fc9c2619c4f65ff7f4cd82085c3ee7a31fc7c"
        );
        assert_eq!(digest!(i8::MIN), "93191e549f8f3548d7e2cfc958ddc8c65bcbe4c6");
        assert_eq!(digest!(i8::MAX), "a58f7d98bf60e10fe369c82030b1c9dee053def9");

        assert_eq!(
            digest!(u32::MAX),
            "2cdf52bf5641027042b9cf9a499e509a58b330e2"
        );
        assert_eq!(
            digest!(u16::MAX),
            "3f0dd44352749a9fd5b7ec44213441ef54c46d57"
        );
        assert_eq!(digest!(u8::MAX), "5a7dd3ea237c30c8735b051524e66fd401a10f6a");
    }

    #[test]
    fn string_keys() {
        assert_eq!(digest!(""), "2819b1ff6e346a43b4f5f6b77a88bc3eaac22a83");
        assert_eq!(
            digest!(str_repeat!('s', 1)),
            "607cddba7cd111745ef0a3d783d57f0e83c8f311"
        );
        assert_eq!(
            digest!(str_repeat!('a', 10)),
            "5979fb32a80da070ff356f7695455592272e36c2"
        );
        assert_eq!(
            digest!(str_repeat!('m', 100)),
            "f00ad7dbcb4bd8122d9681bca49b8c2ffd4beeed"
        );
        assert_eq!(
            digest!(str_repeat!('t', 1000)),
            "07ac412d4c33b8628ab147b8db244ce44ae527f8"
        );
        assert_eq!(
            digest!(str_repeat!('-', 10000)),
            "b42e64afbfccb05912a609179228d9249ea1c1a0"
        );
        assert_eq!(
            digest!(str_repeat!('+', 100_000)),
            "0a3e888c20bb8958537ddd4ba835e4070bd51740"
        );

        assert_eq!(digest!("haha"), "36eb02a807dbade8cd784e7800d76308b4e89212");
        assert_eq!(
            digest!("haha".to_string()),
            "36eb02a807dbade8cd784e7800d76308b4e89212"
        );
        assert_eq!(
            digest!(&"haha".to_string()),
            "36eb02a807dbade8cd784e7800d76308b4e89212"
        );
    }

    #[test]
    fn blob_keys() {
        assert_eq!(
            digest!(vec![0u8; 0]),
            "327e2877b8815c7aeede0d5a8620d4ef8df4a4b4"
        );
        assert_eq!(
            digest!(vec![b's'; 1]),
            "ca2d96dc9a184d15a7fa2927565e844e9254e001"
        );
        assert_eq!(
            digest!(vec![b'a'; 10]),
            "d10982327b2b04c7360579f252e164a75f83cd99"
        );
        assert_eq!(
            digest!(vec![b'm'; 100]),
            "475786aa4ee664532a7d1ea69cb02e4695fcdeed"
        );
        assert_eq!(
            digest!(vec![b't'; 1000]),
            "5a32b507518a49bf47fdaa3deca53803f5b2e8c3"
        );
        assert_eq!(
            digest!(vec![b'-'; 10000]),
            "ed65c63f7a1f8c6697eb3894b6409a95461fd982"
        );
        assert_eq!(
            digest!(vec![b'+'; 100_000]),
            "fe19770c371774ba1a1532438d4851b8a773a9e6"
        );
    }

    #[test]
    #[should_panic(expected = "Data type is not supported as Key value.")]
    fn unsupported_float_key() {
        as_key!("namespace", "set", 4.1415);
    }

    #[test]
    #[should_panic(expected = "Aerospike does not support u64 natively on server-side.")]
    fn unsupported_u64_key() {
        as_key!("namespace", "set", u64::MAX);
    }
}
