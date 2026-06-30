// Copyright 2015-2020 Aerospike, Inc.
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

/// `ReadModeAP` is the read policy for AP (availability) namespaces.
///
/// It indicates how duplicates should be consulted in a read operation.
/// Only makes a difference during migrations and only applicable in AP mode.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum ReadModeAP {
    /// A single node should be involved in the read operation.
    #[default]
    One = 0,

    /// All duplicates should be consulted in the read operation.
    All = 1,
}

// Case-insensitive YAML/config parsing (mirrors the Go client's `ToUpper`
// handling): accepts `ONE`/`ALL` in any case; anything else is an error.
#[cfg(feature = "dynamic-config")]
impl<'de> serde::Deserialize<'de> for ReadModeAP {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        match value.to_ascii_uppercase().as_str() {
            "ONE" => Ok(ReadModeAP::One),
            "ALL" => Ok(ReadModeAP::All),
            other => Err(serde::de::Error::custom(format!(
                "invalid ReadModeAp value: {other}"
            ))),
        }
    }
}
