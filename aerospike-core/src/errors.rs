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
//!     Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _, _)) => {
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

use crate::ResultCode;
#[cfg(feature = "rt-tokio")]
use aerospike_rt::task;

/// Aerospike client and protocol errors.
#[derive(Error, Debug)]
pub enum Error {
    /// Error decoding a Base64-encoded value.
    #[error("Error decoding Base64 encoded value")]
    Base64(#[from] ::base64::DecodeError),
    /// Error interpreting a byte sequence as UTF-8.
    #[error("Error interpreting a sequence of u8 as a UTF-8 encoded string.")]
    InvalidUtf8(#[from] ::std::str::Utf8Error),
    /// Error during an I/O operation.
    #[error("Error during an I/O operation")]
    Io(#[from] ::std::io::Error),
    /// Error parsing an IP or socket address.
    #[error("Error parsing an IP or socket address")]
    ParseAddr(#[from] ::std::net::AddrParseError),
    /// Error parsing a string as an integer.
    #[error("Error parsing an integer")]
    ParseInt(#[from] ::std::num::ParseIntError),
    /// Error while hashing a password for user authentication.
    #[error("Error returned while hashing a password for user authentication")]
    PwHash(#[from] ::pwhash::error::Error),
    #[cfg(feature = "rt-tokio")]
    /// Async runtime error (e.g. task join failure).
    #[error("Async runtime error {0}")]
    Async(#[from] task::JoinError),
    /// The client received a server response that it was not able to process.
    #[error("Bad Server Response: {0}")]
    BadResponse(String),
    /// The client was not able to communicate with the cluster due to some issue with the
    /// network connection.
    #[error("Unable to communicate with server cluster: {0}")]
    Connection(String),
    /// One or more of the arguments passed to the client are invalid.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    /// Cluster node is invalid.
    #[error("Invalid cluster node: {0}")]
    InvalidNode(String),
    /// Invalid or unknown namespace.
    #[error("Invalid namespace: {0}")]
    InvalidNamespace(String),
    /// Exceeded max. number of connections per node.
    #[error("Too many connections")]
    NoMoreConnections,
    /// The connection pool had no ready connection. A background task may have
    /// been spawned to open one; the operation should retry shortly. Command
    /// retry loops treat this as a pacing wait that does not consume the
    /// retry budget.
    #[error("Connection pool empty; a connection is being opened in the background")]
    ConnectionPoolEmpty,
    /// Server responded with a response code indicating an error condition for batch.
    #[error("Batch error: Index: {0:?}, Result Code: {1:?}, In Doubt: {2}, Node: {3}")]
    BatchError(u32, ResultCode, bool, String),
    /// Server responded with a response code indicating an error condition for batch.
    #[error("Batch error: Index: {0:?}, Result Code: {1:?}, In Doubt: {2}, Node: {3}")]
    BatchLastError(u32, ResultCode, bool, String),
    /// Server responded with a response code indicating an error condition.
    /// The last element carries the extended server-supplied error detail
    /// (subcode, message, expression trace) when it was requested via
    /// [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity)
    /// and the server attached one; see [`crate::ServerErrorDetail`] and the
    /// [`Error::sub_code`] / [`Error::server_message`] accessors.
    #[error("Server error: {0:?}, In Doubt: {1}, Node: {2}{detail}",
        detail = .3.as_ref().map(|d| format!(", Detail: {d}")).unwrap_or_default()
    )]
    ServerError(
        ResultCode,
        bool,
        String,
        Option<Box<crate::ServerErrorDetail>>,
    ),
    /// Per-node circuit breaker has tripped: too many recent errors against
    /// the node within the configured `error_rate_window`. The command was
    /// *not* sent to the server.
    #[error("Max error rate exceeded for node {0}; backing off")]
    MaxErrorRate(String),
    /// Error returned when executing a User-Defined Function (UDF) resulted in an error.
    #[error("UDF Bad Response: {0}")]
    UdfBadResponse(String),
    /// Error returned when a task times out before it could be completed.
    #[error("Client Timeout: {0}")]
    Timeout(String), // TODO: Should have Node

    /// `ClientError` is an untyped Error happening on client-side
    #[error("{0}")]
    ClientError(String),
    /// `ParsePeersError` occurs when parsing a peer string fails.
    #[error("{0}")]
    ParsePeersError(String),

    /// `StreamSendError` is a client-side error that signifies the scan/query was terminated.
    /// Carries the originating cause (e.g. parse error, socket error) when one is available.
    #[error("Record stream was terminated{}",
        .0.as_ref().map(|e| format!(": {e}")).unwrap_or_default()
    )]
    StreamTerminatedError(Option<Box<Error>>),

    /// Error returned when a task timed out before it could be completed.
    #[error("{0}\n\t{1}")]
    Chain(Box<Error>, Box<Error>),

    /// Transaction commit failed. Carries per-stage records and an in-doubt flag
    /// so callers can implement selective recovery. Returned by `Client::commit`.
    #[error("Commit failed: {error_type} (in_doubt={in_doubt}){}",
        .source.as_ref().map(|e| format!("\n\t{e}")).unwrap_or_default()
    )]
    CommitFailed {
        /// Which stage of the commit failed.
        error_type: crate::txn::CommitErrorType,
        /// Per-key outcomes of the verify phase. Empty when verify didn't run.
        verify_records: Vec<crate::BatchRecord>,
        /// Per-key outcomes of the roll phase. Empty when roll didn't run.
        roll_records: Vec<crate::BatchRecord>,
        /// Whether the outcome is in doubt (client can't tell if the server
        /// committed or aborted the transaction).
        in_doubt: bool,
        /// Underlying error that triggered the failure, if any.
        source: Option<Box<Error>>,
    },
}

impl Error {
    #[must_use]
    pub fn chain_error(self, e: &str) -> Error {
        Error::Chain(Box::new(Error::ClientError(e.into())), Box::new(self))
    }

    #[must_use]
    pub fn wrap(self, e: Error) -> Error {
        Error::Chain(Box::new(e), Box::new(self))
    }

    /// Chain `cause` as context for this error. If `cause` is `None`, returns `self` unchanged.
    pub fn chain_cause(self, cause: Option<Error>) -> Error {
        match cause {
            Some(e) => Error::Chain(Box::new(self), Box::new(e)),
            None => self,
        }
    }

    /// Returns the server result code carried by this error, if any. Drills through
    /// `Chain` wrappers so pattern-style checks still work after retry decoration.
    #[must_use]
    pub fn server_result_code(&self) -> Option<ResultCode> {
        match self {
            Error::ServerError(rc, _, _, _)
            | Error::BatchError(_, rc, _, _)
            | Error::BatchLastError(_, rc, _, _) => Some(*rc),
            Error::Chain(a, b) => a.server_result_code().or_else(|| b.server_result_code()),
            _ => None,
        }
    }

    /// Returns the extended server-supplied error detail (subcode, message,
    /// expression trace), if the server attached one. Requires
    /// [`BasePolicy::error_detail_verbosity`](crate::policy::BasePolicy::error_detail_verbosity)
    /// > 0 and server version 8.1.3+. Drills through `Chain` wrappers so
    /// checks still work after retry decoration.
    #[must_use]
    pub fn server_error_detail(&self) -> Option<&crate::ServerErrorDetail> {
        match self {
            Error::ServerError(_, _, _, detail) => detail.as_deref(),
            Error::Chain(a, b) => a.server_error_detail().or_else(|| b.server_error_detail()),
            _ => None,
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

    /// Returns the formatted server-supplied error detail message, if any.
    #[must_use]
    pub fn server_message(&self) -> Option<&str> {
        self.server_error_detail()
            .map(|d| d.message.as_str())
            .filter(|m| !m.is_empty())
    }

    /// Recompute the `in_doubt` flag:
    /// `in_doubt = true` when this is a write AND we sent more than one command OR
    /// sent exactly one and the failure was a client-side error or server TIMEOUT.
    /// No-op for non-write commands or non-server error variants.
    #[must_use]
    pub fn set_in_doubt(mut self, is_write: bool, commands_sent: u32) -> Self {
        if !is_write {
            return self;
        }
        match &mut self {
            Error::ServerError(rc, in_doubt, _, _)
            | Error::BatchError(_, rc, in_doubt, _)
            | Error::BatchLastError(_, rc, in_doubt, _)
                if (commands_sent > 1
                    || (commands_sent == 1 && matches!(rc, ResultCode::Timeout))) =>
            {
                *in_doubt = true;
            }
            // Client-side timeouts / connection failures on a write command
            // where we sent at least one command are always in-doubt.
            Error::Timeout(_) | Error::Connection(_) if commands_sent >= 1 => {
                // No in_doubt field on these variants; wrap with context so
                // callers can observe via the Display chain.
                self = Error::Chain(
                    Box::new(Error::ClientError("in_doubt=true".into())),
                    Box::new(self),
                );
            }
            _ => (),
        }
        self
    }

    /// Attach retry context (iteration count, last node attempted, prior errors)
    /// as a leading `Chain` wrapper. Only wraps when there is something to report
    /// (iterations > 1 or a non-empty history), so happy-path errors are
    /// unaffected and existing pattern matches continue to work.
    #[must_use]
    pub fn with_retry_context(
        self,
        iterations: u32,
        node: Option<&str>,
        mut history: Vec<Error>,
    ) -> Self {
        if iterations <= 1 && history.is_empty() {
            return self;
        }
        use std::fmt::Write as _;
        let mut ctx = format!("iterations={iterations}");
        if let Some(n) = node {
            let _ = write!(ctx, " last_node={n}");
        }
        if !history.is_empty() {
            let _ = write!(ctx, " sub_errors={}", history.len());
        }
        // Build a nested chain so all prior sub-errors are reachable via Display.
        let mut chained = self;
        while let Some(prev) = history.pop() {
            chained = Error::Chain(Box::new(chained), Box::new(prev));
        }
        Error::Chain(Box::new(Error::ClientError(ctx)), Box::new(chained))
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[cfg(test)]
mod server_detail_tests {
    use super::*;
    use crate::server_error::{sub_code, ServerErrorDetail};

    fn detailed() -> Error {
        Error::ServerError(
            ResultCode::ParameterError,
            false,
            "node".into(),
            Some(Box::new(ServerErrorDetail {
                sub_code: sub_code::PARAM_TTL_INVALID,
                message: "ttl too long (subcode=1)".into(),
                exp_trace: None,
            })),
        )
    }

    #[test]
    fn accessors_surface_detail() {
        let err = detailed();
        assert_eq!(err.server_result_code(), Some(ResultCode::ParameterError));
        assert_eq!(err.sub_code(), sub_code::PARAM_TTL_INVALID);
        assert_eq!(err.server_message(), Some("ttl too long (subcode=1)"));
        assert!(err.server_error_detail().is_some());
    }

    #[test]
    fn accessors_drill_through_chain() {
        let wrapped = detailed().chain_error("retry context");
        assert_eq!(wrapped.sub_code(), sub_code::PARAM_TTL_INVALID);
        assert_eq!(wrapped.server_result_code(), Some(ResultCode::ParameterError));
        assert!(wrapped.server_error_detail().is_some());
    }

    #[test]
    fn no_detail_reports_none_and_zero() {
        let err = Error::ServerError(ResultCode::KeyNotFoundError, false, "node".into(), None);
        assert_eq!(err.sub_code(), sub_code::NONE);
        assert_eq!(err.server_message(), None);
        assert!(err.server_error_detail().is_none());
    }

    #[test]
    fn display_includes_detail() {
        let s = detailed().to_string();
        assert!(s.contains("Detail: ttl too long (subcode=1)"), "{s}");
    }

    // ---- const-sentinel contract (ported from Go error_test.go) ----
    //
    // Go's newServerError routes special-case result codes (KEY_NOT_FOUND,
    // FILTERED_OUT) through the same builder so the extended detail is never
    // dropped, while `errors.Is` still matches the sentinel. Rust has no
    // sentinel singletons; the equivalent contract is that the (result code,
    // detail) pair survives construction and remains matchable.

    fn server_error(rc: ResultCode, message: &str, sub_code: u32) -> Error {
        let detail = if message.is_empty() && sub_code == 0 {
            None
        } else {
            Some(Box::new(ServerErrorDetail {
                sub_code,
                message: message.into(),
                exp_trace: None,
            }))
        };
        Error::ServerError(rc, false, "node".into(), detail)
    }

    #[test]
    fn plain_key_not_found_matches_and_has_no_detail() {
        let err = server_error(ResultCode::KeyNotFoundError, "", 0);
        assert!(matches!(
            err,
            Error::ServerError(ResultCode::KeyNotFoundError, _, _, _)
        ));
        assert_eq!(err.server_result_code(), Some(ResultCode::KeyNotFoundError));
        assert_eq!(err.sub_code(), sub_code::NONE);
    }

    #[test]
    fn key_not_found_with_detail_still_matches_and_surfaces_detail() {
        let err = server_error(ResultCode::KeyNotFoundError, "record missing (subcode=7)", 7);
        assert_eq!(err.server_result_code(), Some(ResultCode::KeyNotFoundError));
        assert_eq!(err.server_message(), Some("record missing (subcode=7)"));
        assert_eq!(err.sub_code(), 7);
    }

    #[test]
    fn plain_filtered_out_matches() {
        let err = server_error(ResultCode::FilteredOut, "", 0);
        assert!(matches!(
            err,
            Error::ServerError(ResultCode::FilteredOut, _, _, _)
        ));
        assert_eq!(err.server_result_code(), Some(ResultCode::FilteredOut));
    }

    #[test]
    fn filtered_out_with_detail_carries_message_and_no_subcode() {
        // FILTERED_OUT carries no subcode (NONE) — only a contextual message.
        let err = server_error(
            ResultCode::FilteredOut,
            "filtered out by filter expression",
            sub_code::NONE,
        );
        assert_eq!(err.server_result_code(), Some(ResultCode::FilteredOut));
        assert_eq!(err.server_message(), Some("filtered out by filter expression"));
        assert_eq!(err.sub_code(), sub_code::NONE);
    }

    #[test]
    fn does_not_cross_match_unrelated_result_codes() {
        let err = server_error(ResultCode::KeyNotFoundError, "", 0);
        assert!(!matches!(
            err,
            Error::ServerError(ResultCode::FilteredOut, _, _, _)
        ));
    }

    #[test]
    fn set_in_doubt_preserves_detail() {
        // A write that failed in-doubt keeps its extended detail intact.
        let err = server_error(ResultCode::Timeout, "timed out (subcode=3)", 3).set_in_doubt(true, 2);
        assert_eq!(err.sub_code(), 3);
        assert_eq!(err.server_message(), Some("timed out (subcode=3)"));
    }
}

macro_rules! log_error_chain {
    ($err:expr, $($arg:tt)*) => {
        error!($($arg)*);
        error!("Error: {}", $err);
        // for e in $err.iter().skip(1) {
        //     error!("caused by: {}", e);
        // }
        // if let Some(backtrace) = $err.provide() {
        //     error!("backtrace: {:?}", backtrace);
        // }
    };
}
