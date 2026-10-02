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

use crate::{Error, Key, Record, ResultCode};
#[cfg(feature = "serialization")]
use serde::Serialize;

/// Encapsulates the Batch key and record result.
///
/// A row has three states, read off two fields: failed when [`error`](Self::error)
/// is set, succeeded when it is not and [`record`](Self::record) is — every
/// answered row has a record, a bin-less one for an operation that returns
/// nothing — and pending (never answered) when neither is. Every error
/// attribute a caller may want — the result code, whether a write is in doubt,
/// the node, the server's extended detail — is read off that one error through
/// the accessors here, so the row never keeps a parallel copy of what [`Error`]
/// already models. A row the server answered `KeyNotFound` or `FilteredOut` is
/// a *failed* row in this sense, exactly as the single-key `get` returns `Err`
/// for it; the batch call itself still succeeds, because per-key outcomes do
/// not fail the call.
///
/// Serializes field for field: `key`, `record`, `error` (see [`Error`]'s own
/// serialization), `has_write`.
#[cfg_attr(feature = "serialization", derive(Serialize))]
#[derive(Debug, Clone)]
pub struct BatchRecord {
    /// Key.
    pub key: Key,

    /// Record result after batch command has completed: `Some` for every
    /// answered row that did not fail (bin-less for an operation that returns
    /// nothing, such as a delete), `None` for a pending row and for most
    /// failures — a UDF failure keeps its `FAILURE` bin here.
    pub record: Option<Record>,

    /// The failure, when the row failed. Everything about it — result code,
    /// in-doubt, node, server detail, cause chain — lives here.
    error: Option<Error>,

    /// Does this command contain a write operation.
    has_write: bool,
}

impl BatchRecord {
    /// An empty row for `key`, before the server has answered for it.
    ///
    /// `has_write` says whether the command this row belongs to writes, and it is
    /// not cosmetic: only a write row can ever be
    /// [`in_doubt`](Self::in_doubt), and [`has_write`](Self::has_write) is what
    /// enforces that.
    ///
    /// A caller filling in a row it obtained elsewhere — a proxy, a cache, a
    /// test double — uses [`set_ok`](Self::set_ok) and
    /// [`set_error`](Self::set_error):
    ///
    /// ```
    /// use aerospike::{BatchRecord, Error, Key, ResultCode, Value};
    ///
    /// let mut row = BatchRecord::new(Key::new("test", "demo", Value::Int(1))?, false);
    /// row.set_error(Error::server_error_with_message(ResultCode::KeyNotFoundError, "no such key"));
    /// assert_eq!(row.result_code(), Some(ResultCode::KeyNotFoundError));
    /// assert!(row.record.is_none());
    /// # Ok::<(), aerospike::Error>(())
    /// ```
    #[must_use]
    pub const fn new(key: Key, has_write: bool) -> Self {
        BatchRecord {
            key,
            record: None,
            error: None,
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

    /// The row's result code: `Some(Ok)` on success, the failure's code
    /// otherwise, `None` while the row has no outcome (never sent, or the call
    /// died before the server answered it).
    ///
    /// A client-side timeout reads as [`ResultCode::Timeout`], as in the other
    /// clients; any other client-side failure has no server code and reads as
    /// `None` here — [`error`](Self::error) still has it.
    #[must_use]
    pub fn result_code(&self) -> Option<ResultCode> {
        match (&self.error, &self.record) {
            (Some(e), _) => e
                .server_result_code()
                .or_else(|| e.is_client_timeout().then_some(ResultCode::Timeout)),
            (None, Some(_)) => Some(ResultCode::Ok),
            (None, None) => None,
        }
    }

    /// Whether a write may have been applied even though the row failed — a
    /// client error (like a timeout) after the command reached the server.
    /// Never true for a read row, whatever its error says.
    #[must_use]
    pub fn in_doubt(&self) -> bool {
        self.has_write && self.error().is_some_and(Error::in_doubt)
    }

    /// The failure behind this row, with everything an [`Error`] carries:
    /// [`matches`](Error::matches), [`node`](Error::node),
    /// [`server_error_detail`](Error::server_error_detail), the cause chain.
    /// `None` for a pending or successful row.
    #[must_use]
    pub const fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }

    /// The node behind a failed row — the one that answered it, or the one
    /// whose failure stamped it — in the `"<name>: <host:port>"` form of
    /// [`Error::node`]. `None` for a pending or successful row.
    #[must_use]
    pub fn node(&self) -> Option<&str> {
        self.error().and_then(Error::node)
    }

    /// Extended server-supplied error detail for this row — subcode, message,
    /// and expression trace — or `None` when the row succeeded or the server
    /// attached nothing.
    ///
    /// Populated on the same terms as the single-key commands: the request must
    /// ask for it via
    /// [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity)
    /// and the server must be 8.2.0+. [`sub_code`](Self::sub_code) and
    /// [`server_message`](Self::server_message) read the two fields callers
    /// usually want.
    #[must_use]
    pub fn error_detail(&self) -> Option<&crate::ServerErrorDetail> {
        self.error().and_then(Error::server_error_detail)
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
        self.error()
            .map_or(crate::server_error::sub_code::NONE, Error::sub_code)
    }

    /// The server's human-readable explanation for this row's failure, if it
    /// sent one.
    #[must_use]
    pub fn server_message(&self) -> Option<&str> {
        self.error().and_then(Error::server_message)
    }

    /// Record a successful answer. An answered row always holds a record —
    /// `None` here stands for an operation that returns nothing and is stored
    /// as a bin-less record — so [`result_code`](Self::result_code) reads `Ok`.
    pub(crate) fn set_ok(&mut self, record: Option<Record>) {
        self.error = None;
        self.record =
            Some(record.unwrap_or_else(|| Record::new(None, crate::IndexMap::new(), None, 0, 0)));
    }

    /// Record a failure: the error is kept and any record is dropped, so a row
    /// that fails after an earlier answer does not carry stale data. The
    /// error's own in-doubt flag is honoured only for a write row. A failure
    /// that comes with a record of its own (a UDF's `FAILURE` bin) assigns
    /// [`record`](Self::record) after this call.
    pub(crate) fn set_error(&mut self, error: Error) {
        self.record = None;
        self.error = Some(error);
    }

    /// Back to pending: no record, no error. A reused operation starts a call
    /// with a clean row.
    pub(crate) fn clear(&mut self) {
        self.record = None;
        self.error = None;
    }

    /// Stamp a row the server never answered with the failure that ended its
    /// command. The row takes a copy of `cause`: for a write row the copy
    /// keeps the cause's in-doubt flag (the request may have been applied),
    /// and an attached transaction is told so a later commit degrades to
    /// abort correctly; a read row can never be in doubt, so its copy is
    /// cleared. A row that already has an outcome is left alone.
    pub(crate) fn stamp_unanswered(
        &mut self,
        cause: &Error,
        txn: Option<&std::sync::Arc<crate::txn::Txn>>,
    ) {
        if self.error.is_some() || self.record.is_some() {
            return;
        }
        let mut row_error = cause.clone();
        if self.has_write {
            if row_error.in_doubt() {
                if let Some(txn) = txn {
                    txn.on_write_in_doubt(&self.key);
                }
            }
        } else {
            row_error.clear_in_doubt();
        }
        self.error = Some(row_error);
    }
}
