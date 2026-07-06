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

use crate::commands::field_type::FieldType;
use crate::errors::{Error, Result};
use crate::query::CollectionIndexType;

/// Parsed AS_MSG field TLVs from a single-message response.
pub struct ParsedMsgFields {
    fields: HashMap<u8, Vec<u8>>,
}

impl ParsedMsgFields {
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

    pub fn index_collection_type(&self) -> Result<CollectionIndexType> {
        let data = self.field(FieldType::IndexType);
        let Some(data) = data else {
            return Ok(CollectionIndexType::Default);
        };
        if data.is_empty() {
            return Ok(CollectionIndexType::Default);
        }
        collection_index_type_from_ordinal(data[0])
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
        if pos + 5 > buffer.len() {
            return Err(Error::InvalidArgument(
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
            return Err(Error::InvalidArgument(
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

fn collection_index_type_from_ordinal(ordinal: u8) -> Result<CollectionIndexType> {
    match ordinal {
        0 => Ok(CollectionIndexType::Default),
        1 => Ok(CollectionIndexType::List),
        2 => Ok(CollectionIndexType::MapKeys),
        3 => Ok(CollectionIndexType::MapValues),
        _ => Err(Error::InvalidArgument(format!(
            "Invalid INDEX_TYPE ordinal {ordinal}"
        ))),
    }
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
    fn parses_index_fields() {
        let mut body = Vec::new();
        body.extend(encode_field(FieldType::IndexName, b"age_idx"));
        body.extend(encode_field(
            FieldType::IndexType,
            &[CollectionIndexType::List as u8],
        ));
        body.extend(encode_field(FieldType::IndexRange, &[1, 3, b'a']));

        let parsed = ParsedMsgFields::from_buffer(&body, 0, 3).unwrap();
        assert_eq!(
            parsed.utf8_field(FieldType::IndexName).as_deref(),
            Some("age_idx")
        );
        assert_eq!(parsed.index_collection_type().unwrap(), CollectionIndexType::List);
        assert_eq!(
            parsed.field(FieldType::IndexRange),
            Some([1u8, 3, b'a'].as_slice())
        );
    }
}
