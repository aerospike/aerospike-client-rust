// Copyright 2015-2026 Aerospike, Inc.
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

//! Aerospike Expression Language (AEL) helpers for filter expressions — currently the
//! **server-compiled filter** wire form (protocol field 43): MessagePack `[128, "<utf-8 ael>"]`.

use crate::commands::buffer::Buffer;
use crate::msgpack::encoder::{
    pack_array_begin, pack_byte, pack_half_byte, pack_integer, pack_type_u16, pack_type_u32,
};
use crate::Result;

use super::from_packed_bytes;
use super::Expression;

#[cfg(test)]
fn pack_expression_to_vec(expression: &Expression) -> Result<Vec<u8>> {
    let n = expression.size()?;
    let mut buf = Buffer::new(0);
    buf.resize_buffer(n)?;
    expression.pack(&mut Some(&mut buf))?;
    Ok(buf.data_buffer[..buf.data_offset].to_vec())
}

/// First element of the root MessagePack array: server compiles the UTF-8 AEL string.
/// Matches the Java fluent client's `Expression.SERVER_COMPILED_AEL_EXPRESSION_OP`.
pub const SERVER_COMPILED_AEL_EXPRESSION_OP: i64 = 128;

const MSGPACK_STR8: u8 = 0xd9;

/// Pack UTF-8 `s` as MessagePack **string** (raw UTF-8 bytes, no Aerospike `ParticleType` prefix).
///
/// Encoding matches the Java fluent `Packer` layout used by `Expression.fromServerCompiledFilter`:
/// fixstr for length `< 32`, **str8** (`0xd9`) for lengths `32..=255` (so length `32` uses str8, not
/// str16), str16 / str32 beyond that.
fn pack_utf8_string_java_compatible(buf: &mut Option<&mut Buffer>, s: &str) -> usize {
    let payload = s.as_bytes();
    let n = payload.len();
    let mut size = 0;
    if n < 32 {
        size += pack_half_byte(buf, 0xa0 | (n as u8));
        if let Some(ref mut w) = *buf {
            size += w.write_bytes(payload);
        } else {
            size += n;
        }
    } else if n <= usize::from(u8::MAX) {
        size += pack_byte(buf, MSGPACK_STR8);
        size += pack_byte(buf, n as u8);
        if let Some(ref mut w) = *buf {
            size += w.write_bytes(payload);
        } else {
            size += n;
        }
    } else if n < (1 << 16) {
        size += pack_type_u16(buf, 0xda, n as u16);
        if let Some(ref mut w) = *buf {
            size += w.write_bytes(payload);
        } else {
            size += n;
        }
    } else {
        size += pack_type_u32(buf, 0xdb, n as u32);
        if let Some(ref mut w) = *buf {
            size += w.write_bytes(payload);
        } else {
            size += n;
        }
    }
    size
}

/// Build a filter [`Expression`] for the **server-compiled filter** wire form: a two-element
/// MessagePack array `[`[`SERVER_COMPILED_AEL_EXPRESSION_OP`]`, "<ael>"]`.
///
/// The Aerospike server (8.1.3+) parses and compiles the AEL text; the client does not expand it
/// into opcode trees. The packed bytes are stored verbatim via [`super::from_packed_bytes`].
///
/// # Errors
///
/// Returns [`crate::Error::InvalidArgument`] if the buffer size would exceed internal limits.
pub fn pack_ael_server_filter(ael: &str) -> Result<Expression> {
    let mut size = 0;
    size += pack_array_begin(&mut None, 2);
    size += pack_integer(&mut None, SERVER_COMPILED_AEL_EXPRESSION_OP);
    size += pack_utf8_string_java_compatible(&mut None, ael);

    let mut buf = Buffer::new(0);
    buf.resize_buffer(size)?;
    let mut opt = Some(&mut buf);
    pack_array_begin(&mut opt, 2);
    pack_integer(&mut opt, SERVER_COMPILED_AEL_EXPRESSION_OP);
    pack_utf8_string_java_compatible(&mut opt, ael);
    drop(opt);

    let bytes = buf.data_buffer[..buf.data_offset].to_vec();
    Ok(from_packed_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_ael_server_filter_encodes_two_element_root_array_with_opcode_128_then_utf8() {
        let e = pack_ael_server_filter("$.bin==1").unwrap();
        let b = pack_expression_to_vec(&e).unwrap();
        assert!(b.len() > 4);
        assert_eq!(b[0], 0x92);
        assert_eq!(b[1], 0xcc);
        assert_eq!(b[2], 0x80);
        assert_eq!(b[3], 0xa0_u8.wrapping_add("$.bin==1".len() as u8));
    }

    #[test]
    fn pack_ael_server_filter_empty_string_still_produces_two_element_root() {
        let e = pack_ael_server_filter("").unwrap();
        let b = pack_expression_to_vec(&e).unwrap();
        assert_eq!(b[0], 0x92);
        assert_eq!(b[1], 0xcc);
        assert_eq!(b[2], 0x80);
        assert_eq!(b[3], 0xa0);
    }

    #[test]
    fn pack_ael_server_filter_utf8_payload_verbatim_after_string_header() {
        let src = "$.café=='é'";
        let utf8 = src.as_bytes();
        let e = pack_ael_server_filter(src).unwrap();
        let payload = pack_expression_to_vec(&e).unwrap();
        assert_eq!(u32::from(payload[3]), 0xa0 + utf8.len() as u32);
        assert_eq!(&payload[4..4 + utf8.len()], utf8);
    }

    #[test]
    fn pack_ael_server_filter_ascii_length_32_uses_str8_not_fixstr() {
        let padded = "a".repeat(32);
        let b = pack_expression_to_vec(&pack_ael_server_filter(&padded).unwrap()).unwrap();
        assert_eq!(b[0], 0x92);
        assert_eq!(b[1], 0xcc);
        assert_eq!(b[2], 0x80);
        assert_eq!(u32::from(b[3]), 0xd9);
        assert_eq!(u32::from(b[4]), 32);
        assert_eq!(&b[5..37], padded.as_bytes());
    }

    #[test]
    fn pack_ael_server_filter_base64_round_trips_like_other_packed_exprs() {
        let e = pack_ael_server_filter("$.rank in (1,2,3)").unwrap();
        let b64 = e.base64().unwrap();
        let decoded = super::super::from_base64(&b64).unwrap();
        assert_eq!(b64, decoded.base64().unwrap());
    }
}
