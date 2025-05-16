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
//! ```rust,edition2018
//! use aerospike::*;
//!
//! let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
//! let policy = ClientPolicy::default();
//! let client = Client::new(&policy, &hosts).expect("Failed to connect to cluster").await;
//! let key = as_key!("test", "test", "someKey");
//! match client.get(&ReadPolicy::default(), &key, Bins::None).await {
//!     Ok(record) => {
//!         match record.time_to_live() {
//!             None => println!("record never expires"),
//!             Some(duration) => println!("ttl: {} secs", duration.as_secs()),
//!         }
//!     },
//!     Err(Error::ServerError(ResultCode::KeyNotFoundError)) => {
//!         println!("No such record: {}", key);
//!     },
//!     Err(err) => {
//!         println!("Error fetching record: {}", err);
//!         for err in err.iter().skip(1) {
//!             println!("Caused by: {}", err);
//!         }
//!         // The backtrace is not always generated. Try to run this example
//!         // with `RUST_BACKTRACE=1`.
//!         if let Some(backtrace) = err.backtrace() {
//!             println!("Backtrace: {:?}", backtrace);
//!         }
//!     }
//! }
//! ```

#![allow(missing_docs)]

use crate::ResultCode;
use aerospike_rt::task;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error decoding Base64 encoded value")]
    Base64(#[from] ::base64::DecodeError),
    #[error("Error interpreting a sequence of u8 as a UTF-8 encoded string.")]
    InvalidUtf8(#[from] ::std::str::Utf8Error),
    #[error("Error during an I/O operation")]
    Io(#[from] ::std::io::Error),
    #[error("Error returned from the `recv` function on an MPSC `Receiver`")]
    MpscRecv(#[from] ::std::sync::mpsc::RecvError),
    #[error("Error parsing an IP or socket address")]
    ParseAddr(#[from] ::std::net::AddrParseError),
    #[error("Error parsing an integer")]
    ParseInt(#[from] ::std::num::ParseIntError),
    #[error("Error returned while hashing a password for user authentication")]
    PwHash(#[from] ::pwhash::error::Error),
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
    /// Exceeded max. number of connections per node.
    #[error("Too many connections")]
    NoMoreConnections,
    /// Server responded with a response code indicating an error condition for batch.
    #[error("BatchIndex error: Index: {0:?}, Result Code: {1:?}, In Doubt: {2}, Node: {3}")]
    BatchError(u32, ResultCode, bool, String),
    /// Server responded with a response code indicating an error condition.
    #[error("Server error: {0:?}, In Doubt: {1}, Node: {2}")]
    ServerError(ResultCode, bool, String),
    /// Error returned when executing a User-Defined Function (UDF) resulted in an error.
    #[error("UDF Bad Response: {0}")]
    UdfBadResponse(String),
    /// Error returned when a task times out before it could be completed.
    #[error("Timeout: {0}, Client-Side: {1}")]
    Timeout(String, bool),

    /// ClientError is an untyped Error happening on client-side
    #[error("{0}")]
    ClientError(String),

    /// Error returned when a tasked timeed out before it could be completed.
    #[error("{0}\n\t{1}")]
    Chain(Box<Error>, Box<Error>),
}

impl Error {
    pub fn chain_error(self, e: &str) -> Error {
        Error::Chain(Box::new(Error::ClientError(e.into())), Box::new(self))
    }

    pub fn wrap(self, e: Error) -> Error {
        Error::Chain(Box::new(e), Box::new(self))
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
