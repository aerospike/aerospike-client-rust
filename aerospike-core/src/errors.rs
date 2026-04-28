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
//!     Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {
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
    /// Server responded with a response code indicating an error condition for batch.
    #[error("Batch error: Index: {0:?}, Result Code: {1:?}, In Doubt: {2}, Node: {3}")]
    BatchError(u32, ResultCode, bool, String),
    /// Server responded with a response code indicating an error condition for batch.
    #[error("Batch error: Index: {0:?}, Result Code: {1:?}, In Doubt: {2}, Node: {3}")]
    BatchLastError(u32, ResultCode, bool, String),
    /// Server responded with a response code indicating an error condition.
    #[error("Server error: {0:?}, In Doubt: {1}, Node: {2}")]
    ServerError(ResultCode, bool, String),
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
            Error::ServerError(rc, _, _)
            | Error::BatchError(_, rc, _, _)
            | Error::BatchLastError(_, rc, _, _) => Some(*rc),
            Error::Chain(a, b) => a.server_result_code().or_else(|| b.server_result_code()),
            _ => None,
        }
    }

    /// Recompute the `in_doubt` flag per Java's rule
    /// (`AerospikeException.setInDoubt(isWrite, commandSentCounter)`):
    /// `in_doubt = true` when this is a write AND we sent more than one command OR
    /// sent exactly one and the failure was a client-side error or server TIMEOUT.
    /// No-op for non-write commands or non-server error variants.
    #[must_use]
    pub fn set_in_doubt(mut self, is_write: bool, commands_sent: u32) -> Self {
        if !is_write {
            return self;
        }
        match &mut self {
            Error::ServerError(rc, in_doubt, _)
            | Error::BatchError(_, rc, in_doubt, _)
            | Error::BatchLastError(_, rc, in_doubt, _) => {
                if commands_sent > 1
                    || (commands_sent == 1 && matches!(rc, ResultCode::Timeout))
                {
                    *in_doubt = true;
                }
            }
            // Client-side timeouts / connection failures on a write command
            // where we sent at least one command are always in-doubt.
            Error::Timeout(_) | Error::Connection(_) => {
                if commands_sent >= 1 {
                    // No in_doubt field on these variants; wrap with context so
                    // callers can observe via the Display chain.
                    self = Error::Chain(
                        Box::new(Error::ClientError("in_doubt=true".into())),
                        Box::new(self),
                    );
                }
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
