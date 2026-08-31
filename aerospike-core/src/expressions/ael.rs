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

use crate::commands::buffer::Buffer;
use crate::msgpack::encoder::{
    pack_array_begin, pack_byte, pack_integer, pack_type_u16, pack_type_u32, pack_type_u8,
};
use crate::{Error, Result};

use super::from_packed_bytes;
use super::Expression;

/// First element of the root MessagePack array: server compiles the UTF-8 AEL string.
pub const SERVER_COMPILED_AEL_EXPRESSION_OP: i64 = 128;

/// Writes the AEL source as a `MessagePack` **bin** — `0xc4`/`0xc5`/`0xc6` by
/// length, then the bytes verbatim.
///
/// Not a `str`, and not [`pack_blob`](crate::msgpack::encoder::pack_blob) either:
/// the server reads this element with `msgpack_get_bin`, and a blob would carry
/// an Aerospike particle-type byte the AEL parser would see as source text. The
/// server's reader does accept the `str` headers as well, but `bin` is the form
/// the reference clients emit, so an expression exported as base64 by one client
/// is byte-identical to another's.
fn pack_ael_text(buf: &mut Option<&mut Buffer>, text: &str) -> usize {
    let bytes = text.as_bytes();
    let len = bytes.len();

    let mut size = if len < 1 << 8 {
        pack_type_u8(buf, 0xc4, len as u8)
    } else if len < 1 << 16 {
        pack_type_u16(buf, 0xc5, len as u16)
    } else {
        pack_type_u32(buf, 0xc6, len as u32)
    };

    for byte in bytes {
        size += pack_byte(buf, *byte);
    }

    size
}

/// Build a filter [`Expression`] from AEL (Aerospike Expression Language) source
/// text for the server to compile and evaluate.
///
/// The client does no parsing and no validation beyond refusing empty text: the
/// source is framed and sent as-is, and the server rejects invalid AEL with
/// `PARAMETER_ERROR`, reporting the parser message and the 1-based line and
/// column of the first error.
///
/// The result is a **complete, standalone filter expression**. The server accepts
/// the AEL opcode only as the top level of the payload, so assign it directly to
/// a policy's `filter_expression` and never nest it inside
/// [`and`](super::and), [`or`](super::or), [`not`](super::not) or any other
/// expression.
///
/// Requires a server that supports `EXP_AEL_COMPILE` — 8.1.3 or later, which
/// [`Version::supports_server_compiled_ael`](crate::cluster::version_parser::Version::supports_server_compiled_ael)
/// reports; older servers reject the payload.
///
/// # Errors
///
/// Returns [`Error::invalid_argument`] if `text` is empty, or if the buffer size
/// would exceed internal limits.
///
/// ```
/// use aerospike::expressions::from_ael;
/// let filter = from_ael("$.n + 1 == 2").unwrap();
/// assert!(from_ael("").is_err());
/// ```
pub fn from_ael(text: &str) -> Result<Expression> {
    if text.is_empty() {
        return Err(Error::invalid_argument(
            "AEL source text must not be empty",
        ));
    }

    let mut size = 0;
    size += pack_array_begin(&mut None, 2);
    size += pack_integer(&mut None, SERVER_COMPILED_AEL_EXPRESSION_OP);
    size += pack_ael_text(&mut None, text);

    let mut buf = Buffer::new(0);
    buf.resize_buffer(size)?;

    let mut opt = Some(&mut buf);
    pack_array_begin(&mut opt, 2);
    pack_integer(&mut opt, SERVER_COMPILED_AEL_EXPRESSION_OP);
    pack_ael_text(&mut opt, text);

    buf.data_buffer.truncate(buf.data_offset);
    Ok(from_packed_bytes(std::mem::take(&mut buf.data_buffer)))
}

/// Build a filter [`Expression`] for the AEL wire form: a two-element
/// MessagePack array `[`[`SERVER_COMPILED_AEL_EXPRESSION_OP`]`, <ael bin>]`.
///
/// The earlier name for [`from_ael`], kept working; new code should use
/// `from_ael`.
///
/// # Errors
///
/// As [`from_ael`].
pub fn pack_ael_server_filter(ael: &str) -> Result<Expression> {
    from_ael(ael)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The framing is a protocol contract with the server's `build_internal`,
    // not an internal detail, so these pin the exact bytes that reach the wire.

    #[test]
    fn frames_a_short_expression_with_a_bin8_header() {
        assert_eq!(
            packed(&from_ael("true").unwrap()),
            vec![
                0x92, // fixarray(2)
                0xcc, 0x80, // uint8(128): EXP_AEL_COMPILE
                0xc4, 0x04, // bin8(4)
                b't', b'r', b'u', b'e',
            ]
        );
    }

    #[test]
    fn frames_the_largest_bin8_payload_with_a_bin8_header() {
        let text = "a".repeat(255);
        let packed = packed(&from_ael(&text).unwrap());

        assert_eq!(&packed[..5], &[0x92, 0xcc, 0x80, 0xc4, 0xff]);
        assert_eq!(&packed[5..], text.as_bytes());
    }

    #[test]
    fn frames_a_256_byte_expression_with_a_bin16_header() {
        let text = "a".repeat(256);
        let packed = packed(&from_ael(&text).unwrap());

        assert_eq!(&packed[..6], &[0x92, 0xcc, 0x80, 0xc5, 0x01, 0x00]);
        assert_eq!(&packed[6..], text.as_bytes());
    }

    #[test]
    fn frames_a_64kib_expression_with_a_bin32_header() {
        let text = "a".repeat(1 << 16);
        let packed = packed(&from_ael(&text).unwrap());

        assert_eq!(
            &packed[..8],
            &[0x92, 0xcc, 0x80, 0xc6, 0x00, 0x01, 0x00, 0x00]
        );
        assert_eq!(&packed[8..], text.as_bytes());
    }

    /// The source is bytes, not codepoints: a multi-byte character contributes
    /// its UTF-8 length to the header and crosses verbatim.
    #[test]
    fn frames_utf8_source_by_byte_length() {
        let text = "$.café == 'é'";
        let packed = packed(&from_ael(text).unwrap());

        assert_eq!(&packed[..3], &[0x92, 0xcc, 0x80]);
        assert_eq!(packed[3], 0xc4);
        assert_eq!(usize::from(packed[4]), text.len());
        assert_eq!(&packed[5..], text.as_bytes());
    }

    /// The server requires a non-empty source, and refuses the envelope with a
    /// generic "invalid AEL compilation" rather than a parse position — so
    /// catching it here gives the caller a better error than the round trip
    /// would.
    #[test]
    fn rejects_empty_source_text() {
        let err = from_ael("").expect_err("empty AEL must be refused");

        assert_eq!(
            err.result_code(),
            i32::from(u8::from(crate::ResultCode::ParameterError))
        );
    }

    #[test]
    fn the_earlier_name_still_frames_the_same_bytes() {
        assert_eq!(
            packed(&pack_ael_server_filter("$.n == 1").unwrap()),
            packed(&from_ael("$.n == 1").unwrap())
        );
    }

    #[test]
    fn encodes_to_base64_for_transport_through_from_base64() {
        let expression = from_ael("$.n + 1 == 2").unwrap();
        let b64 = expression.base64().unwrap();
        let round_tripped = super::super::from_base64(&b64).unwrap();

        assert_eq!(packed(&round_tripped), packed(&expression));
    }

    fn packed(expression: &Expression) -> Vec<u8> {
        let n = expression.size().unwrap();
        let mut buf = Buffer::new(0);
        buf.resize_buffer(n).unwrap();
        expression.pack(&mut Some(&mut buf)).unwrap();

        buf.data_buffer[..buf.data_offset].to_vec()
    }
}
