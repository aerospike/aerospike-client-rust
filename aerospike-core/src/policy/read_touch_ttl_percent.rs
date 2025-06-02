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

const SERVER_DEFAULT: u32 = 0x0000_0000;
const DONT_RESET: u32 = 0xFFFF_FFFF; // -1 as i32

/// ReadTouchTTLPercent determines how record TTL (time to live) is affected on reads. When enabled, the server can
/// efficiently operate as a read-based LRU cache where the least recently used records are expired.
/// The value is expressed as a percentage of the TTL sent on the most recent write such that a read
/// within this interval of the record’s end of life will generate a touch.
///
/// For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
/// 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
/// recent write) will result in a touch, resetting the TTL to another 10 hours.
/// Supported in server v8+.
#[derive(Debug, Clone, Copy)]
pub enum ReadTouchTTL {
    /// 1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL
    Percent(u8),

    /// Use server config `default-read-touch-ttl-pct` for the record's namespace/set
    ServerDefault,

    /// Do not reset record TTL on reads
    DontReset,
}

impl From<ReadTouchTTL> for u32 {
    fn from(exp: ReadTouchTTL) -> u32 {
        match exp {
            ReadTouchTTL::Percent(pct) => pct as u32,
            ReadTouchTTL::ServerDefault => SERVER_DEFAULT,
            ReadTouchTTL::DontReset => DONT_RESET,
        }
    }
}
