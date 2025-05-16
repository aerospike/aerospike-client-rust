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

use crate::Key;
use crate::Record;
use crate::ResultCode;
#[cfg(feature = "serialization")]
use serde::Serialize;

/// Key and bin names used in batch read commands where variable bins are needed for each key.
#[cfg_attr(feature = "serialization", derive(Serialize))]
#[derive(Debug, Clone)]

/// Encapsulates the Batch key and record result.
pub struct BatchRecord {
    /// Key.
    pub key: Key,

    /// Record result after batch command has completed.  Will be nil if record was not found
    /// or an error occurred. See ResultCode.
    pub record: Option<Record>,

    /// ResultCode for this returned record. See ResultCode.
    /// If not OK, the record will be nil.
    pub result_code: Option<ResultCode>,

    /// InDoubt signifies the possibility that the write command may have completed even though an error
    /// occurred for this record. This may be the case when a client error occurs (like timeout)
    /// after the command was sent to the server.
    pub in_doubt: bool,

    /// Does this command contain a write operation. For internal use only.
    has_write: bool,
}

impl BatchRecord {
    pub(crate) fn new(key: Key, has_write: bool) -> Self {
        BatchRecord {
            key: key,
            record: None,
            result_code: None,
            in_doubt: false,
            has_write: has_write,
        }
    }
}
