// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use crate::commands::buffer::FIELD_HEADER_SIZE;
use crate::commands::field_type::FieldType;
use crate::errors::{Error, Result};

/// Parsed AS_MSG field TLVs from a message body slice.
pub struct AsMsgFields {
    fields: HashMap<u8, Vec<u8>>,
}

impl AsMsgFields {
    pub fn from_buffer(buffer: &[u8], offset: usize, field_count: usize) -> Result<Self> {
        Ok(Self {
            fields: parse_msg_fields(buffer, offset, field_count)?,
        })
    }

    pub fn field(&self, field_type: FieldType) -> Option<&[u8]> {
        self.fields.get(&(field_type as u8)).map(Vec::as_slice)
    }

    pub fn utf8_field(&self, field_type: FieldType) -> Option<String> {
        let data = self.field(field_type)?;
        std::str::from_utf8(data)
            .map(str::to_owned)
            .ok()
    }
}

fn parse_msg_fields(
    buffer: &[u8],
    offset: usize,
    field_count: usize,
) -> Result<HashMap<u8, Vec<u8>>> {
    let mut fields = HashMap::with_capacity(field_count);
    let mut pos = offset;

    for _ in 0..field_count {
        let field_header_size = FIELD_HEADER_SIZE as usize;
        if pos + field_header_size > buffer.len() {
            return Err(Error::BadResponse(
                "truncated message field header".into(),
            ));
        }
        let len = u32::from_be_bytes([
            buffer[pos],
            buffer[pos + 1],
            buffer[pos + 2],
            buffer[pos + 3],
        ]) as usize;
        pos += 4;
        let field_type = buffer[pos];
        pos += 1;
        let size = len.saturating_sub(1);
        if pos + size > buffer.len() {
            return Err(Error::BadResponse(
                "truncated message field body".into(),
            ));
        }
        let value = if size > 0 {
            buffer[pos..pos + size].to_vec()
        } else {
            Vec::new()
        };
        pos += size;
        fields.insert(field_type, value);
    }

    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_field(ftype: FieldType, value: &[u8]) -> Vec<u8> {
        let len = 1 + value.len();
        let mut out = Vec::with_capacity(4 + len);
        out.extend_from_slice(&(len as u32).to_be_bytes());
        out.push(ftype as u8);
        out.extend_from_slice(value);
        out
    }

    #[test]
    fn parses_field_tlvs_by_type() {
        let mut body = Vec::new();
        body.extend(encode_field(FieldType::IndexName, b"age_idx"));
        body.extend(encode_field(FieldType::IndexRange, &[1, 3, b'a']));

        let parsed = AsMsgFields::from_buffer(&body, 0, 2).unwrap();
        assert_eq!(
            parsed.utf8_field(FieldType::IndexName).as_deref(),
            Some("age_idx")
        );
        assert_eq!(
            parsed.field(FieldType::IndexRange),
            Some([1u8, 3, b'a'].as_slice())
        );
    }
}
