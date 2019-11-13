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

use std::u32;

const NAMESPACE_DEFAULT: u32 = 0x0000_0000;
const NEVER_EXPIRE: u32 = 0xFFFF_FFFF; // -1 as i32
const DONT_UPDATE: u32 = 0xFFFF_FFFE; // -2 as i32

/// Record expiration, also known as time-to-live (TTL).
#[derive(Debug, Clone, Copy)]
pub enum Expiration {
    /// Set the record to expire X seconds from now
    Seconds(u32),

    /// Set the record's expiry time using the default time-to-live (TTL) value for the namespace
    NamespaceDefault,

    /// Set the record to never expire. Requires Aerospike 2 server version 2.7.2 or later or
    /// Aerospike 3 server version 3.1.4 or later. Do not use with older servers.
    Never,

    /// Do not change the record's expiry time when updating the record; requires Aerospike server
    /// version 3.10.1 or later.
    DontUpdate,
}

impl From<Expiration> for u32 {
    fn from(exp: Expiration) -> u32 {
        match exp {
            Expiration::Seconds(secs) => secs,
            Expiration::NamespaceDefault => NAMESPACE_DEFAULT,
            Expiration::Never => NEVER_EXPIRE,
            Expiration::DontUpdate => DONT_UPDATE,
        }
    }
}
