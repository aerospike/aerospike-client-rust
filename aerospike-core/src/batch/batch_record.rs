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

/// Encapsulates the Batch key and record result.
#[cfg_attr(feature = "serialization", derive(Serialize))]
#[derive(Debug, Clone)]
pub struct BatchRecord {
    /// Key.
    pub key: Key,

    /// Record result after batch command has completed. Will be nil if record was not found
    /// or an error occurred. See `ResultCode`.
    pub record: Option<Record>,

    /// `ResultCode` for this returned record. See `ResultCode`.
    /// If not OK, the record will be nil.
    pub result_code: Option<ResultCode>,

    /// `InDoubt` signifies the possibility that the write command may have completed even though an error
    /// occurred for this record. This may be the case when a client error occurs (like timeout)
    /// after the command was sent to the server.
    pub in_doubt: bool,

    /// Does this command contain a write operation.
    has_write: bool,
}

impl BatchRecord {
    pub(crate) const fn new(key: Key, has_write: bool) -> Self {
        BatchRecord {
            key,
            record: None,
            result_code: None,
            in_doubt: false,
            has_write,
        }
    }

    /// True when this record's batch operation contains a write. Only write
    /// records can ever be [`in_doubt`](Self::in_doubt); useful to
    /// distinguish verify (read) from roll (write) records on
    /// [`Error::CommitFailed`](crate::errors::Error::CommitFailed).
    #[must_use]
    pub const fn has_write(&self) -> bool {
        self.has_write
    }

    /// Record a per-key failure. `in_doubt` is honored only for write
    /// records — reads can never be in-doubt.
    pub(crate) const fn set_error(&mut self, rc: crate::ResultCode, in_doubt: bool) {
        self.result_code = Some(rc);
        self.in_doubt = self.has_write && in_doubt;
    }
}
