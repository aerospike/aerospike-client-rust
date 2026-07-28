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

    /// Extended server-supplied error detail for this row. Boxed because it is
    /// 152 bytes against a `BatchRecord`'s 408 and is `None` for every row that
    /// succeeded; private so the boxing stays an implementation detail, as it is
    /// on [`Error`](crate::Error). Read it through
    /// [`error_detail`](Self::error_detail).
    error_detail: Option<Box<crate::ServerErrorDetail>>,

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
            error_detail: None,
            has_write,
        }
    }

    /// True when this record's batch operation contains a write. Only write
    /// records can ever be [`in_doubt`](Self::in_doubt); useful to
    /// distinguish verify (read) from roll (write) records on
    /// [`ErrorKind::Commit`](crate::ErrorKind::Commit).
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

    /// Attach the server's extended error detail for this row.
    pub(crate) fn set_error_detail(&mut self, detail: Option<Box<crate::ServerErrorDetail>>) {
        if detail.is_some() {
            self.error_detail = detail;
        }
    }

    /// Extended server-supplied error detail for this row — subcode, message,
    /// and expression trace — or `None` when the row succeeded or the server
    /// attached nothing.
    ///
    /// Populated on the same terms as the single-key commands: the request must
    /// ask for it via
    /// [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity)
    /// and the server must be 8.1.3+. [`sub_code`](Self::sub_code) and
    /// [`server_message`](Self::server_message) read the two fields callers
    /// usually want.
    #[must_use]
    pub fn error_detail(&self) -> Option<&crate::ServerErrorDetail> {
        self.error_detail.as_deref()
    }

    /// The server-supplied error subcode for this row, or
    /// [`sub_code::NONE`](crate::server_error::sub_code::NONE) when there is
    /// none.
    ///
    /// A subcode is only meaningful together with
    /// [`result_code`](Self::result_code): subcode values are scoped to their
    /// parent result code and are not globally unique, so dispatch on the pair.
    #[must_use]
    pub fn sub_code(&self) -> u32 {
        self.error_detail()
            .map_or(crate::server_error::sub_code::NONE, |d| d.sub_code)
    }

    /// The server's human-readable explanation for this row's failure, if it
    /// sent one.
    #[must_use]
    pub fn server_message(&self) -> Option<&str> {
        self.error_detail()
            .map(|d| d.message.as_str())
            .filter(|m| !m.is_empty())
    }
}
