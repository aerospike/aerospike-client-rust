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

//! Error and Result types for the Aerospike client.
//!
//! [`Error`] is an opaque struct carrying metadata common to every failure —
//! a Java-compatible [`result_code`](Error::result_code) (negative for
//! client-side failures, the server [`ResultCode`] value otherwise), the node,
//! iteration count, in-doubt flag, retry sub-errors, and an optional causal
//! [`source`](std::error::Error::source) — plus an [`ErrorKind`] describing
//! the specific failure. This mirrors the Java client's `AerospikeException`
//! base class + subclasses.
//!
//! # Examples
//!
//! Handling an error returned by the client.
//!
//! ```rust,edition2021
//! use aerospike::*;
//!
//! # async fn example() -> Result<()> {
//! let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
//! let policy = ClientPolicy::default();
//! let client = Client::new(&policy, &hosts).await?;
//! let key = as_key!("test", "test", "someKey");
//! match client.get(&ReadPolicy::default(), &key, Bins::None).await {
//!     Ok(record) => {
//!         match record.time_to_live() {
//!             None => println!("record never expires"),
//!             Some(duration) => println!("ttl: {} secs", duration.as_secs()),
//!         }
//!     },
//!     Err(err) if err.server_result_code() == Some(ResultCode::KeyNotFoundError) => {
//!         println!("No such record: {}", key);
//!     },
//!     Err(err) => {
//!         println!("Error fetching record: {}", err);
//!         let mut source = std::error::Error::source(&err);
//!         while let Some(e) = source {
//!             println!("Caused by: {}", e);
//!             source = e.source();
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```

#![allow(missing_docs)]

use std::fmt;

use crate::{ClientResultCode, ResultCode};
#[cfg(feature = "rt-tokio")]
use aerospike_rt::task;

/// The specific failure carried by an [`Error`].
///
/// The "subclass" half of the Java `AerospikeException` hierarchy; metadata
/// common to every failure (result code, node, iteration, in-doubt,
/// sub-errors, cause) lives on [`Error`] itself.
#[derive(Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Server responded with a non-OK result code. The detail carries the
    /// extended server-supplied error information (subcode, message,
    /// expression trace) when it was requested via
    /// [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity);
    /// see [`crate::ServerErrorDetail`] and the [`Error::sub_code`] /
    /// [`Error::server_message`] accessors.
    Server {
        /// The server result code.
        rc: ResultCode,
        /// Extended server-supplied error detail, when attached.
        detail: Option<Box<crate::ServerErrorDetail>>,
    },
    /// Per-row error inside a batch response. Internal to the batch parse
    /// flow; user-visible row outcomes ride on
    /// [`BatchRecord`](crate::BatchRecord) instead.
    BatchRow {
        /// Index of the failing row in the batch request.
        index: u32,
        /// The server result code for this row.
        rc: ResultCode,
        /// True when this row was the final record of the response stream.
        last: bool,
        /// Extended server-supplied detail for this row, when attached.
        detail: Option<Box<crate::ServerErrorDetail>>,
    },
    /// A batch command failed after per-key processing began. Carries every
    /// [`BatchRecord`](crate::BatchRecord) outcome known to the client
    /// (successes, per-key errors, and in-doubt marks for unanswered writes),
    /// mirroring Java's `AerospikeException.BatchRecordArray`.
    BatchFailed {
        /// Per-key outcomes in the original request order.
        records: Vec<crate::BatchRecord>,
    },
    /// Client-side timeout: the deadline elapsed or the retry budget was
    /// exhausted before a response arrived. Server-reported timeouts are
    /// [`ErrorKind::Server`] with [`ResultCode::Timeout`].
    Timeout,
    /// Network/connection failure while communicating with the cluster.
    Connection,
    /// The connection pool had no ready connection. A background task may
    /// have been spawned to open one; the operation should retry shortly.
    /// Command retry loops treat this as a pacing wait that does not consume
    /// the retry budget.
    ConnectionPoolEmpty,
    /// Exceeded max. number of connections per node.
    NoMoreConnections,
    /// Per-node circuit breaker has tripped: too many recent errors against
    /// the node within the configured `error_rate_window`. The command was
    /// *not* sent to the server.
    MaxErrorRate,
    /// Cluster node is invalid or not currently active.
    InvalidNode,
    /// Invalid or unknown namespace.
    InvalidNamespace,
    /// One or more of the arguments passed to the client are invalid.
    InvalidArgument,
    /// The client received a server response that it was not able to process.
    BadResponse,
    /// Parsing a peer string failed.
    ParsePeers,
    /// Executing a User-Defined Function (UDF) resulted in an error.
    UdfBadResponse,
    /// A scan/query record stream was terminated prematurely. The originating
    /// cause (parse error, socket error, consumer cancellation), when known,
    /// is on [`source`](std::error::Error::source).
    StreamTerminated,
    /// Transaction commit failed. Carries per-stage records so callers can
    /// implement selective recovery; the triggering error, if any, is on
    /// [`source`](std::error::Error::source). Returned by
    /// [`Client::commit`](crate::Client::commit).
    Commit {
        /// Which stage of the commit failed.
        error_type: crate::txn::CommitErrorType,
        /// Per-key outcomes of the verify phase. Empty when verify didn't run.
        verify_records: Vec<crate::BatchRecord>,
        /// Per-key outcomes of the roll phase. Empty when roll didn't run.
        roll_records: Vec<crate::BatchRecord>,
    },
    /// Untyped client-side error.
    Client,
    /// Error decoding a Base64-encoded value.
    Base64(::base64::DecodeError),
    /// Error interpreting a byte sequence as UTF-8.
    InvalidUtf8(::std::str::Utf8Error),
    /// Error during an I/O operation.
    Io(::std::io::Error),
    /// Error parsing an IP or socket address.
    ParseAddr(::std::net::AddrParseError),
    /// Error parsing a string as an integer.
    ParseInt(::std::num::ParseIntError),
    /// Error while hashing a password for user authentication.
    PwHash(::pwhash::error::Error),
    /// Async runtime error (e.g. task join failure).
    #[cfg(feature = "rt-tokio")]
    Async(task::JoinError),
}

/// Metadata common to every error — the "base class" half of the Java
/// `AerospikeException` hierarchy.
#[derive(Debug)]
struct ErrorInner {
    kind: ErrorKind,
    /// Java-compatible numeric code: server codes non-negative, client-side
    /// codes negative (see [`ClientResultCode`]).
    result_code: i32,
    /// Human-readable message. `None` falls back to a kind-derived default.
    message: Option<String>,
    /// Last node the command was attempted on, when known.
    node: Option<String>,
    /// Number of attempts before failing, when the retry loop recorded it.
    iteration: Option<u32>,
    /// Whether a write may have been applied despite the failure.
    in_doubt: bool,
    /// Errors from prior retry attempts of the same command.
    sub_errors: Vec<Error>,
    /// Underlying cause.
    source: Option<Box<Error>>,
}

/// Aerospike client and protocol errors.
///
/// See the [module docs](self) for the
/// overall shape; use [`kind`](Error::kind) to dispatch on the specific
/// failure and the accessors ([`result_code`](Error::result_code),
/// [`in_doubt`](Error::in_doubt), [`node`](Error::node),
/// [`iteration`](Error::iteration), [`sub_errors`](Error::sub_errors)) for
/// the metadata every error carries.
pub struct Error(Box<ErrorInner>);

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

impl Error {
    fn new(kind: ErrorKind, result_code: i32, message: Option<String>) -> Error {
        Error(Box::new(ErrorInner {
            kind,
            result_code,
            message,
            node: None,
            iteration: None,
            in_doubt: false,
            sub_errors: Vec::new(),
            source: None,
        }))
    }

    /// Server failure with the given result code, reporting node, and
    /// optional extended error detail.
    #[must_use]
    pub fn server_error(
        rc: ResultCode,
        node: impl Into<String>,
        detail: Option<Box<crate::ServerErrorDetail>>,
    ) -> Error {
        let mut e = Error::new(
            ErrorKind::Server { rc, detail },
            i32::from(u8::from(rc)),
            None,
        );
        e.0.node = Some(node.into());
        e
    }

    /// Per-row batch error (internal to the batch response parse flow).
    #[must_use]
    pub(crate) fn batch_row(
        index: u32,
        rc: ResultCode,
        last: bool,
        node: impl Into<String>,
        detail: Option<Box<crate::ServerErrorDetail>>,
    ) -> Error {
        let mut e = Error::new(
            ErrorKind::BatchRow {
                index,
                rc,
                last,
                detail,
            },
            i32::from(u8::from(rc)),
            None,
        );
        e.0.node = Some(node.into());
        e
    }

    /// Batch command failure carrying every per-key outcome known to the
    /// client. `source` is the failure that aborted the batch.
    #[must_use]
    pub fn batch_failed(records: Vec<crate::BatchRecord>, source: Error) -> Error {
        let mut e = Error::new(
            ErrorKind::BatchFailed { records },
            ClientResultCode::BatchFailed.into(),
            None,
        );
        e.0.in_doubt = source.in_doubt();
        e.0.source = Some(Box::new(source));
        e
    }

    /// Client-side timeout, not (yet) in-doubt. Write retry loops mark the
    /// error in-doubt via [`set_in_doubt`](Self::set_in_doubt) when at least
    /// one attempt reached the wire. Uses the Java-compatible `TIMEOUT` (9)
    /// result code; use [`max_retries_exceeded`](Self::max_retries_exceeded)
    /// when the retry budget (not the clock) ran out.
    #[must_use]
    pub fn timeout(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::Timeout,
            i32::from(u8::from(ResultCode::Timeout)),
            Some(msg.into()),
        )
    }

    /// Retry budget exhausted before the command completed
    /// (`MAX_RETRIES_EXCEEDED`, Java parity).
    #[must_use]
    pub fn max_retries_exceeded(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::Timeout,
            ClientResultCode::MaxRetriesExceeded.into(),
            Some(msg.into()),
        )
    }

    /// Network/connection failure, not (yet) in-doubt. Write retry loops mark
    /// the error in-doubt via [`set_in_doubt`](Self::set_in_doubt) when at
    /// least one attempt reached the wire.
    #[must_use]
    pub fn connection(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::Connection,
            ClientResultCode::ServerNotAvailable.into(),
            Some(msg.into()),
        )
    }

    /// The connection pool had no ready connection (pacing signal).
    #[must_use]
    pub fn pool_empty() -> Error {
        Error::new(
            ErrorKind::ConnectionPoolEmpty,
            ClientResultCode::NoMoreConnections.into(),
            None,
        )
    }

    /// Exceeded max. number of connections per node.
    #[must_use]
    pub fn no_more_connections() -> Error {
        Error::new(
            ErrorKind::NoMoreConnections,
            ClientResultCode::NoMoreConnections.into(),
            None,
        )
    }

    /// Per-node circuit breaker tripped for `node`.
    #[must_use]
    pub fn max_error_rate(node: impl Into<String>) -> Error {
        let mut e = Error::new(
            ErrorKind::MaxErrorRate,
            ClientResultCode::MaxErrorRate.into(),
            None,
        );
        e.0.node = Some(node.into());
        e
    }

    /// Cluster node is invalid or not currently active.
    #[must_use]
    pub fn invalid_node(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::InvalidNode,
            ClientResultCode::InvalidNodeError.into(),
            Some(msg.into()),
        )
    }

    /// Invalid or unknown namespace.
    #[must_use]
    pub fn invalid_namespace(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::InvalidNamespace,
            i32::from(u8::from(ResultCode::InvalidNamespace)),
            Some(msg.into()),
        )
    }

    /// Invalid argument passed to a client API (Java parity: the positive
    /// `PARAMETER_ERROR` code, like Java's client-side validation).
    #[must_use]
    pub fn invalid_argument(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::InvalidArgument,
            i32::from(u8::from(ResultCode::ParameterError)),
            Some(msg.into()),
        )
    }

    /// The client received a server response it could not process.
    #[must_use]
    pub fn bad_response(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::BadResponse,
            ClientResultCode::ParseError.into(),
            Some(msg.into()),
        )
    }

    /// Parsing a peer string failed.
    #[must_use]
    pub fn parse_peers(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::ParsePeers,
            ClientResultCode::ParseError.into(),
            Some(msg.into()),
        )
    }

    /// A UDF returned an error response.
    #[must_use]
    pub fn udf_bad_response(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::UdfBadResponse,
            i32::from(u8::from(ResultCode::UdfBadResponse)),
            Some(msg.into()),
        )
    }

    /// A record stream was terminated; `cause` carries the originating error
    /// when one is available.
    #[must_use]
    pub fn stream_terminated(cause: Option<Error>) -> Error {
        let mut e = Error::new(
            ErrorKind::StreamTerminated,
            ClientResultCode::ScanTerminated.into(),
            None,
        );
        e.0.source = cause.map(Box::new);
        e
    }

    /// Transaction commit failure with per-stage records.
    #[must_use]
    pub fn commit_failed(
        error_type: crate::txn::CommitErrorType,
        verify_records: Vec<crate::BatchRecord>,
        roll_records: Vec<crate::BatchRecord>,
        in_doubt: bool,
        source: Option<Error>,
    ) -> Error {
        let mut e = Error::new(
            ErrorKind::Commit {
                error_type,
                verify_records,
                roll_records,
            },
            ClientResultCode::TxnFailed.into(),
            None,
        );
        e.0.in_doubt = in_doubt;
        e.0.source = source.map(Box::new);
        e
    }

    /// Untyped client-side error.
    #[must_use]
    pub fn client_error(msg: impl Into<String>) -> Error {
        Error::new(
            ErrorKind::Client,
            ClientResultCode::ClientError.into(),
            Some(msg.into()),
        )
    }
}

macro_rules! impl_from {
    ($ty:ty, $kind:ident, $rc:expr) => {
        impl From<$ty> for Error {
            fn from(e: $ty) -> Error {
                Error::new(ErrorKind::$kind(e), $rc, None)
            }
        }
    };
}

impl_from!(
    ::base64::DecodeError,
    Base64,
    ClientResultCode::ParseError.into()
);
impl_from!(
    ::std::str::Utf8Error,
    InvalidUtf8,
    ClientResultCode::ParseError.into()
);
impl_from!(::std::io::Error, Io, ClientResultCode::ClientError.into());
impl_from!(
    ::std::net::AddrParseError,
    ParseAddr,
    ClientResultCode::ParseError.into()
);
impl_from!(
    ::std::num::ParseIntError,
    ParseInt,
    ClientResultCode::ParseError.into()
);
impl_from!(
    ::pwhash::error::Error,
    PwHash,
    ClientResultCode::SerializeError.into()
);
#[cfg(feature = "rt-tokio")]
impl_from!(task::JoinError, Async, ClientResultCode::ClientError.into());

// ---------------------------------------------------------------------------
// Accessors (the Java "base class" getters)
// ---------------------------------------------------------------------------

impl Error {
    /// The specific failure carried by this error.
    #[must_use]
    pub fn kind(&self) -> &ErrorKind {
        &self.0.kind
    }

    /// Java-compatible numeric result code: the server [`ResultCode`] wire
    /// value for server failures, a negative [`ClientResultCode`] value for
    /// client-side failures. Uniform across every error.
    #[must_use]
    pub fn result_code(&self) -> i32 {
        self.0.result_code
    }

    /// Returns the typed client-side result code carried by this error, if
    /// any — the counterpart of [`server_result_code`](Self::server_result_code)
    /// for failures generated on the client. Drills into the cause chain.
    ///
    /// `None` for server failures and for client-side timeouts (which use
    /// the server `TIMEOUT` code, matching the Java client — check for those
    /// via [`kind`](Self::kind) / [`ErrorKind::Timeout`]).
    #[must_use]
    pub fn client_result_code(&self) -> Option<ClientResultCode> {
        if self.0.result_code < 0 {
            Some(ClientResultCode::from(self.0.result_code))
        } else {
            self.0.source.as_ref().and_then(|s| s.client_result_code())
        }
    }

    /// Last node the command was attempted on, when known. Drills into the
    /// cause chain.
    #[must_use]
    pub fn node(&self) -> Option<&str> {
        self.0
            .node
            .as_deref()
            .or_else(|| self.0.source.as_ref().and_then(|s| s.node()))
    }

    /// Number of attempts before failing, when the retry loop recorded it.
    #[must_use]
    pub fn iteration(&self) -> Option<u32> {
        self.0.iteration
    }

    /// Errors from prior retry attempts of the same command. Empty when no
    /// retry occurred (Java: `getSubExceptions`).
    #[must_use]
    pub fn sub_errors(&self) -> &[Error] {
        &self.0.sub_errors
    }

    /// The failure that caused this one, as a typed [`Error`].
    ///
    /// [`std::error::Error::source`] also exposes the chain, but as
    /// `&dyn Error`, so reading the cause's [`kind`](Self::kind) or
    /// [`result_code`](Self::result_code) through it needs a downcast. This is
    /// the same link, typed: an aggregate
    /// [`ErrorKind::BatchFailed`](ErrorKind::BatchFailed) reports the
    /// `BATCH_FAILED` code (Java `AerospikeException.BatchRecordArray` parity)
    /// and this reaches the underlying timeout or server failure without
    /// parsing the message.
    ///
    /// Only the direct cause; walk it repeatedly for the whole chain. Returns
    /// `None` for causes that are not this crate's errors (an `io::Error`
    /// wrapped by [`ErrorKind::Io`], for instance) — those are reachable via
    /// `source`.
    #[must_use]
    pub fn cause(&self) -> Option<&Error> {
        self.0.source.as_deref()
    }

    /// The message without the metadata decoration that [`fmt::Display`]
    /// adds (Java: `getBaseMessage`).
    #[must_use]
    pub fn base_message(&self) -> String {
        let i = &*self.0;
        match &i.kind {
            ErrorKind::Server { rc, detail } => {
                let mut s = format!("Server error: {rc:?}");
                if let Some(d) = detail {
                    use std::fmt::Write as _;
                    let _ = write!(s, ", Detail: {d}");
                }
                s
            }
            ErrorKind::BatchRow { index, rc, .. } => {
                format!("Batch row error: index {index}, {rc:?}")
            }
            ErrorKind::BatchFailed { records } => {
                format!("Batch failed ({} records)", records.len())
            }
            ErrorKind::Timeout => format!(
                "Client Timeout: {}",
                i.message.as_deref().unwrap_or("Timeout")
            ),
            ErrorKind::Connection => format!(
                "Unable to communicate with server cluster: {}",
                i.message.as_deref().unwrap_or_default()
            ),
            ErrorKind::ConnectionPoolEmpty => {
                "Connection pool empty; a connection is being opened in the background".into()
            }
            ErrorKind::NoMoreConnections => "Too many connections".into(),
            ErrorKind::MaxErrorRate => format!(
                "Max error rate exceeded for node {}; backing off",
                i.node.as_deref().unwrap_or("<unknown>")
            ),
            ErrorKind::InvalidNode => format!(
                "Invalid cluster node: {}",
                i.message.as_deref().unwrap_or_default()
            ),
            ErrorKind::InvalidNamespace => format!(
                "Invalid namespace: {}",
                i.message.as_deref().unwrap_or_default()
            ),
            ErrorKind::InvalidArgument => format!(
                "Invalid argument: {}",
                i.message.as_deref().unwrap_or_default()
            ),
            ErrorKind::BadResponse => format!(
                "Bad Server Response: {}",
                i.message.as_deref().unwrap_or_default()
            ),
            ErrorKind::UdfBadResponse => format!(
                "UDF Bad Response: {}",
                i.message.as_deref().unwrap_or_default()
            ),
            ErrorKind::StreamTerminated => "Record stream was terminated".into(),
            ErrorKind::Commit { error_type, .. } => {
                format!("Commit failed: {error_type}")
            }
            ErrorKind::Base64(e) => format!("Error decoding Base64 encoded value: {e}"),
            ErrorKind::InvalidUtf8(e) => {
                format!("Error interpreting a sequence of u8 as a UTF-8 encoded string: {e}")
            }
            ErrorKind::Io(e) => format!("Error during an I/O operation: {e}"),
            ErrorKind::ParseAddr(e) => format!("Error parsing an IP or socket address: {e}"),
            ErrorKind::ParseInt(e) => format!("Error parsing an integer: {e}"),
            ErrorKind::PwHash(e) => {
                format!("Error returned while hashing a password for user authentication: {e}")
            }
            #[cfg(feature = "rt-tokio")]
            ErrorKind::Async(e) => format!("Async runtime error: {e}"),
            // Java `getBaseMessage` contract: the explicit message, else the
            // result code's descriptive string.
            _ => i.message.clone().unwrap_or_else(|| {
                if i.result_code < 0 {
                    ClientResultCode::from(i.result_code).into_string()
                } else {
                    ResultCode::from(i.result_code as u8).into_string()
                }
            }),
        }
    }

    /// Returns the server result code carried by this error, if any. Drills
    /// into the cause chain so checks still work after retry decoration.
    #[must_use]
    pub fn server_result_code(&self) -> Option<ResultCode> {
        match &self.0.kind {
            ErrorKind::Server { rc, .. } | ErrorKind::BatchRow { rc, .. } => Some(*rc),
            _ => self.0.source.as_ref().and_then(|s| s.server_result_code()),
        }
    }

    /// Returns the extended server-supplied error detail (subcode, message,
    /// expression trace), if the server attached one. Requires
    /// [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity)
    /// greater than zero and server version 8.1.3+. Drills into the cause
    /// chain so checks still work after retry decoration.
    #[must_use]
    pub fn server_error_detail(&self) -> Option<&crate::ServerErrorDetail> {
        match &self.0.kind {
            ErrorKind::Server {
                detail: Some(d), ..
            }
            | ErrorKind::BatchRow {
                detail: Some(d), ..
            } => Some(d),
            _ => self.0.source.as_ref().and_then(|s| s.server_error_detail()),
        }
    }

    /// Returns the server-supplied error subcode, or
    /// [`sub_code::NONE`](crate::server_error::sub_code::NONE) (0) when the
    /// server did not send one. A subcode is only meaningful when interpreted
    /// together with the result code: subcode values are scoped to their
    /// parent result code and are NOT globally unique. Dispatch on the
    /// ([`server_result_code`](Self::server_result_code), `sub_code`) pair.
    #[must_use]
    pub fn sub_code(&self) -> u32 {
        self.server_error_detail()
            .map_or(crate::server_error::sub_code::NONE, |d| d.sub_code)
    }

    /// The client-side message attached to this error, if any. For internal
    /// use (e.g. rebuilding per-row batch outcomes from command errors).
    pub(crate) fn message(&self) -> Option<&str> {
        self.0.message.as_deref()
    }

    /// Returns the formatted server-supplied error detail message, if any.
    #[must_use]
    pub fn server_message(&self) -> Option<&str> {
        self.server_error_detail()
            .map(|d| d.message.as_str())
            .filter(|m| !m.is_empty())
    }

    /// True when the outcome of a write is uncertain: the command may have
    /// been applied by the server even though an error was returned. Checks
    /// the cause chain so checks work after retry decoration.
    #[must_use]
    pub fn in_doubt(&self) -> bool {
        self.0.in_doubt || self.0.source.as_ref().is_some_and(|s| s.in_doubt())
    }

    /// Whether this error or any cause in its chain is a client-side
    /// timeout ([`ErrorKind::Timeout`]). Server-reported timeouts are
    /// `ErrorKind::Server` with [`ResultCode::Timeout`] and are not
    /// matched here.
    pub fn is_client_timeout(&self) -> bool {
        matches!(self.0.kind, ErrorKind::Timeout)
            || self
                .0
                .source
                .as_ref()
                .is_some_and(|s| s.is_client_timeout())
    }

    /// Whether the connection this error was produced on may be returned to
    /// the pool. Client-side errors and the `SCAN_ABORT` / `QUERY_ABORTED`
    /// server codes require the socket to be discarded (it may still have
    /// stream bytes pending); client timeouts keep the socket for background
    /// recovery (Java: `AerospikeException.keepConnection`).
    #[must_use]
    pub fn keep_connection(&self) -> bool {
        match &self.0.kind {
            ErrorKind::Server { rc, .. } | ErrorKind::BatchRow { rc, .. } => {
                !matches!(rc, ResultCode::ScanAbort | ResultCode::QueryAborted)
            }
            ErrorKind::Timeout => true,
            _ => false,
        }
    }

    /// True when this error is the connection-pool pacing signal.
    #[must_use]
    pub fn is_pool_empty(&self) -> bool {
        matches!(self.0.kind, ErrorKind::ConnectionPoolEmpty)
    }
}

// ---------------------------------------------------------------------------
// Mutators / composition (the Java "base class" setters)
// ---------------------------------------------------------------------------

impl Error {
    /// Append `cause` to the end of this error's cause chain.
    fn append_source(&mut self, cause: Error) {
        match &mut self.0.source {
            Some(s) => s.append_source(cause),
            None => self.0.source = Some(Box::new(cause)),
        }
    }

    /// Prefix this error with a client-side context message; `self` becomes
    /// the cause.
    #[must_use]
    pub fn chain_error(self, msg: &str) -> Error {
        let mut e = Error::client_error(msg);
        e.append_source(self);
        e
    }

    /// Make `outer` the primary error with `self` as its (deepest) cause.
    #[must_use]
    pub fn wrap(self, mut outer: Error) -> Error {
        outer.append_source(self);
        outer
    }

    /// Chain `cause` as the underlying cause of this error. If `cause` is
    /// `None`, returns `self` unchanged.
    #[must_use]
    pub fn chain_cause(mut self, cause: Option<Error>) -> Error {
        if let Some(c) = cause {
            self.append_source(c);
        }
        self
    }

    /// Set the reporting node, when not already known.
    #[must_use]
    pub fn with_node(mut self, node: impl Into<String>) -> Error {
        if self.0.node.is_none() {
            self.0.node = Some(node.into());
        }
        self
    }

    /// Attach retry context: iteration count, last node attempted, and the
    /// errors of prior attempts (Java: `setIteration` / `setSubExceptions`).
    #[must_use]
    pub fn with_retry_context(
        mut self,
        iterations: u32,
        node: Option<&str>,
        history: Vec<Error>,
    ) -> Self {
        self.0.iteration = Some(iterations);
        if self.0.node.is_none() {
            self.0.node = node.map(Into::into);
        }
        self.0.sub_errors.extend(history);
        self
    }

    /// Recompute the `in_doubt` flag:
    /// `in_doubt = true` when this is a write AND we sent more than one command OR
    /// sent exactly one and the failure was a client-side error or server TIMEOUT.
    /// No-op for non-write commands or kinds that cannot be in-doubt.
    ///
    /// Walks the cause chain, so it works whether it is applied to the naked
    /// terminal error or after retry context / exit-timeout wrapping.
    #[must_use]
    pub fn set_in_doubt(mut self, is_write: bool, commands_sent: u32) -> Self {
        self.mark_in_doubt(is_write, commands_sent);
        self
    }

    /// In-place recursive body of [`set_in_doubt`](Self::set_in_doubt).
    fn mark_in_doubt(&mut self, is_write: bool, commands_sent: u32) {
        if !is_write {
            return;
        }
        let eligible = match &self.0.kind {
            ErrorKind::Server { rc, .. } | ErrorKind::BatchRow { rc, .. } => {
                commands_sent > 1 || (commands_sent == 1 && matches!(rc, ResultCode::Timeout))
            }
            // Client-side timeouts / connection failures on a write command
            // where at least one command reached the wire are always
            // in-doubt: the request may have been applied without a response.
            ErrorKind::Timeout | ErrorKind::Connection => commands_sent >= 1,
            _ => false,
        };
        if eligible {
            self.0.in_doubt = true;
        }
        if let Some(s) = &mut self.0.source {
            s.mark_in_doubt(is_write, commands_sent);
        }
    }
}

// ---------------------------------------------------------------------------
// Display / std::error::Error
// ---------------------------------------------------------------------------

impl fmt::Display for Error {
    /// Uniform, Java-style format:
    /// `Error <code>[, iter=N][, In Doubt: true][, node=X]: <base message>`
    /// followed by one indented line per sub-error and the cause chain.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let i = &*self.0;
        write!(f, "Error {}", i.result_code)?;
        if let Some(it) = i.iteration {
            write!(f, ", iter={it}")?;
        }
        if i.in_doubt {
            f.write_str(", In Doubt: true")?;
        }
        if let Some(n) = &i.node {
            write!(f, ", node={n}")?;
        }
        write!(f, ": {}", self.base_message())?;
        if !i.sub_errors.is_empty() {
            f.write_str("\nsub-errors:")?;
            for s in &i.sub_errors {
                write!(f, "\n\t{s}")?;
            }
        }
        if let Some(src) = &i.source {
            write!(f, "\ncaused by: {src}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0.kind {
            ErrorKind::Base64(e) => Some(e),
            ErrorKind::InvalidUtf8(e) => Some(e),
            ErrorKind::Io(e) => Some(e),
            ErrorKind::ParseAddr(e) => Some(e),
            ErrorKind::ParseInt(e) => Some(e),
            ErrorKind::PwHash(e) => Some(e),
            #[cfg(feature = "rt-tokio")]
            ErrorKind::Async(e) => Some(e),
            _ => self
                .0
                .source
                .as_deref()
                .map(|e| e as &(dyn std::error::Error + 'static)),
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

macro_rules! log_error_chain {
    ($err:expr, $($arg:tt)*) => {
        error!($($arg)*);
        error!("Error: {}", $err);
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_error::{sub_code, ServerErrorDetail};

    fn detailed() -> Error {
        Error::server_error(
            ResultCode::ParameterError,
            "node",
            Some(Box::new(ServerErrorDetail {
                sub_code: sub_code::PARAM_TTL_INVALID,
                message: "ttl too long (subcode=1)".into(),
                exp_trace: None,
            })),
        )
    }

    // ---- uniform metadata (Java base-class parity) ----

    #[test]
    fn result_codes_are_java_compatible() {
        assert_eq!(detailed().result_code(), 4); // PARAMETER_ERROR
        assert_eq!(Error::timeout("t").result_code(), 9); // TIMEOUT
        assert_eq!(
            Error::max_retries_exceeded("t").result_code(),
            i32::from(ClientResultCode::MaxRetriesExceeded)
        );
        assert_eq!(
            Error::connection("c").result_code(),
            i32::from(ClientResultCode::ServerNotAvailable)
        );
        assert_eq!(
            Error::invalid_node("n").result_code(),
            i32::from(ClientResultCode::InvalidNodeError)
        );
        assert_eq!(
            Error::client_error("x").result_code(),
            i32::from(ClientResultCode::ClientError)
        );
        assert_eq!(
            Error::batch_failed(vec![], Error::timeout("t")).result_code(),
            i32::from(ClientResultCode::BatchFailed)
        );
    }

    #[test]
    fn client_result_code_accessor() {
        // Typed accessor, symmetric with server_result_code.
        assert_eq!(
            Error::max_retries_exceeded("t").client_result_code(),
            Some(ClientResultCode::MaxRetriesExceeded)
        );
        assert_eq!(
            Error::connection("c").client_result_code(),
            Some(ClientResultCode::ServerNotAvailable)
        );
        assert_eq!(
            Error::batch_failed(vec![], Error::timeout("t")).client_result_code(),
            Some(ClientResultCode::BatchFailed)
        );
        // Server failures and client timeouts (Java TIMEOUT=9) report None.
        assert_eq!(detailed().client_result_code(), None);
        assert_eq!(Error::timeout("t").client_result_code(), None);
        // Drills through the cause chain: a server error wrapping a client
        // parse failure still surfaces the client code.
        let wrapped =
            Error::bad_response("junk").wrap(Error::server_error(ResultCode::Ok, "n", None));
        assert_eq!(
            wrapped.client_result_code(),
            Some(ClientResultCode::ParseError)
        );
    }

    #[test]
    fn client_codes_format_like_server_codes() {
        // Display and String conversions mirror ResultCode's API.
        assert_eq!(
            ClientResultCode::MaxRetriesExceeded.to_string(),
            "Max retries exceeded"
        );
        let s: String = ClientResultCode::NoMoreConnections.into();
        assert_eq!(s, "No more available connections");
        // base_message falls back to the code's string when no explicit
        // message was attached (Java getBaseMessage contract).
        assert_eq!(
            Error::no_more_connections().base_message(),
            "Too many connections"
        );
        assert_eq!(
            Error::max_error_rate("n").base_message(),
            "Max error rate exceeded for node n; backing off"
        );
    }

    #[test]
    fn retry_context_is_typed() {
        let err = Error::timeout("Timeout after 2 tries").with_retry_context(
            2,
            Some("BB9051616AC4202: 172.22.22.5:3000"),
            vec![Error::connection("read: early eof")],
        );
        assert_eq!(err.iteration(), Some(2));
        assert_eq!(err.node(), Some("BB9051616AC4202: 172.22.22.5:3000"));
        assert_eq!(err.sub_errors().len(), 1);
        let s = err.to_string();
        assert!(s.contains("iter=2"), "{s}");
        assert!(s.contains("node=BB9051616AC4202"), "{s}");
        assert!(s.contains("sub-errors:"), "{s}");
    }

    #[test]
    fn display_uniform_format() {
        let s = Error::timeout("Timeout reading from the network connection")
            .set_in_doubt(true, 1)
            .to_string();
        // Error 9, In Doubt: true: Client Timeout: ...
        assert!(s.starts_with("Error 9"), "{s}");
        assert!(s.contains("In Doubt: true"), "{s}");
        assert!(s.contains("Client Timeout: Timeout reading"), "{s}");
    }

    // ---- extended server detail (CLIENT-4975) ----

    #[test]
    fn accessors_surface_detail() {
        let err = detailed();
        assert_eq!(err.server_result_code(), Some(ResultCode::ParameterError));
        assert_eq!(err.sub_code(), sub_code::PARAM_TTL_INVALID);
        assert_eq!(err.server_message(), Some("ttl too long (subcode=1)"));
        assert!(err.server_error_detail().is_some());
    }

    #[test]
    fn accessors_drill_through_cause_chain() {
        let wrapped = detailed().chain_error("retry context");
        assert_eq!(wrapped.sub_code(), sub_code::PARAM_TTL_INVALID);
        assert_eq!(
            wrapped.server_result_code(),
            Some(ResultCode::ParameterError)
        );
        assert!(wrapped.server_error_detail().is_some());
        assert_eq!(wrapped.node(), Some("node")); // node drills too
    }

    #[test]
    fn no_detail_reports_none_and_zero() {
        let err = Error::server_error(ResultCode::KeyNotFoundError, "node", None);
        assert_eq!(err.sub_code(), sub_code::NONE);
        assert_eq!(err.server_message(), None);
        assert!(err.server_error_detail().is_none());
    }

    #[test]
    fn display_includes_detail() {
        let s = detailed().to_string();
        assert!(s.contains("Detail: ttl too long (subcode=1)"), "{s}");
    }

    #[test]
    fn kind_dispatch_works() {
        let err = Error::server_error(ResultCode::KeyNotFoundError, "node", None);
        assert!(matches!(
            err.kind(),
            ErrorKind::Server {
                rc: ResultCode::KeyNotFoundError,
                ..
            }
        ));
        assert!(!matches!(
            err.kind(),
            ErrorKind::Server {
                rc: ResultCode::FilteredOut,
                ..
            }
        ));
    }

    // ---- typed in-doubt (QE SC-migration regression tests) ----

    #[test]
    fn naked_client_timeout_write_is_in_doubt() {
        let err =
            Error::timeout("Timeout reading from the network connection").set_in_doubt(true, 1);
        assert!(err.in_doubt());
        assert!(err.to_string().contains("In Doubt: true"), "{err}");
    }

    #[test]
    fn naked_connection_error_write_is_in_doubt() {
        let err = Error::connection("read: early eof").set_in_doubt(true, 1);
        assert!(err.in_doubt());
        assert!(err.to_string().contains("In Doubt: true"), "{err}");
    }

    #[test]
    fn retry_exhaustion_chain_is_in_doubt() {
        // The exact shape single_command builds on retry exit with
        // max_retries=0: last_err.wrap(exit_err) makes the exit error primary
        // with the last network error as its cause, THEN set_in_doubt runs.
        let last_err = Error::timeout("Timeout reading from the network connection");
        let exit_err = Error::max_retries_exceeded("Timeout after 2 tries");
        let tail = last_err.wrap(exit_err);

        let out = tail.set_in_doubt(true, 1).with_retry_context(
            2,
            Some("BB9051616AC4202: 172.22.22.5:3000"),
            vec![],
        );

        assert!(out.in_doubt(), "retry-exhaustion write must be in-doubt");
        assert!(out.to_string().contains("In Doubt: true"), "{out}");
        assert_eq!(
            out.result_code(),
            i32::from(ClientResultCode::MaxRetriesExceeded)
        );
        assert_eq!(out.iteration(), Some(2));
    }

    #[test]
    fn marking_after_retry_context_still_works() {
        let err = Error::timeout("Timeout reading from the network connection")
            .with_retry_context(2, Some("node"), vec![])
            .set_in_doubt(true, 1);
        assert!(err.in_doubt());
    }

    #[test]
    fn reads_are_never_in_doubt() {
        let err =
            Error::timeout("Timeout reading from the network connection").set_in_doubt(false, 1);
        assert!(!err.in_doubt());
        assert!(!err.to_string().contains("In Doubt"), "{err}");
    }

    #[test]
    fn unsent_writes_are_not_in_doubt() {
        let err = Error::timeout("Timeout").set_in_doubt(true, 0);
        assert!(!err.in_doubt());
    }

    #[test]
    fn server_error_in_doubt_semantics_preserved() {
        let e = Error::server_error(ResultCode::Timeout, "n", None).set_in_doubt(true, 1);
        assert!(e.in_doubt());
        let e = Error::server_error(ResultCode::KeyExistsError, "n", None).set_in_doubt(true, 1);
        assert!(!e.in_doubt());
        let e = Error::server_error(ResultCode::KeyExistsError, "n", None).set_in_doubt(true, 2);
        assert!(e.in_doubt());
    }

    #[test]
    fn set_in_doubt_preserves_detail() {
        let err = Error::server_error(
            ResultCode::Timeout,
            "node",
            Some(Box::new(ServerErrorDetail {
                sub_code: 3,
                message: "timed out (subcode=3)".into(),
                exp_trace: None,
            })),
        )
        .set_in_doubt(true, 2);
        assert_eq!(err.sub_code(), 3);
        assert_eq!(err.server_message(), Some("timed out (subcode=3)"));
    }

    // ---- keep_connection / predicates ----

    #[test]
    fn keep_connection_contract() {
        assert!(Error::server_error(ResultCode::KeyNotFoundError, "n", None).keep_connection());
        assert!(!Error::server_error(ResultCode::ScanAbort, "n", None).keep_connection());
        assert!(!Error::server_error(ResultCode::QueryAborted, "n", None).keep_connection());
        assert!(Error::timeout("t").keep_connection());
        assert!(!Error::connection("c").keep_connection());
        assert!(!Error::from(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof"
        ))
        .keep_connection());
    }

    // ---- batch partial results (Java BatchRecordArray parity) ----

    #[test]
    fn batch_failed_carries_records_and_cause() {
        let key = crate::Key::new("ns", "set", crate::Value::from("k")).unwrap();
        let mut rec = crate::BatchRecord::new(key, true);
        rec.in_doubt = true;
        let cause = Error::timeout("Timeout after 2 tries").set_in_doubt(true, 1);

        let err = Error::batch_failed(vec![rec], cause);
        assert_eq!(err.result_code(), i32::from(ClientResultCode::BatchFailed));
        assert!(err.in_doubt(), "batch failure inherits cause in-doubt");
        match err.kind() {
            ErrorKind::BatchFailed { records } => {
                assert_eq!(records.len(), 1);
                assert!(records[0].in_doubt);
            }
            other => panic!("expected BatchFailed, got {other:?}"),
        }
        // Cause chain reachable via std::error::Error.
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn cause_reaches_the_underlying_failure_typed() {
        // What a batch failure looks like: BATCH_FAILED on top (Java
        // BatchRecordArray parity), the real cause one link down.
        let inner = Error::timeout("Timeout reading from the network connection");
        let middle = Error::max_retries_exceeded("Timeout after 1 tries").chain_cause(Some(inner));
        let err = Error::batch_failed(vec![], middle.set_in_doubt(true, 1));

        assert_eq!(err.result_code(), i32::from(ClientResultCode::BatchFailed));
        assert!(err.in_doubt(), "in-doubt is inherited from the cause");

        // The timeout is reachable without parsing the message.
        let timeout_code = i32::from(u8::from(ResultCode::Timeout));
        let mut link = err.cause();
        let mut codes = Vec::new();
        while let Some(e) = link {
            codes.push(e.result_code());
            link = e.cause();
        }
        assert_eq!(
            codes,
            vec![
                i32::from(ClientResultCode::MaxRetriesExceeded),
                timeout_code
            ]
        );

        // A leaf error has no cause.
        assert!(Error::timeout("t").cause().is_none());
    }

    #[test]
    fn batch_read_timeout_is_not_in_doubt() {
        // is_write == false: nothing could have been applied.
        let err = Error::timeout("Timeout").set_in_doubt(false, 3);
        assert!(!err.in_doubt());
        assert!(!Error::batch_failed(vec![], err).in_doubt());
    }
}
