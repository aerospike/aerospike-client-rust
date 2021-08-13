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
//!     Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _)) => {
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

error_chain! {

// Automatic conversions between this error chain and other error types not defined by the
// `error_chain!`.
    foreign_links {
        Base64(::base64::DecodeError)
            #[doc = "Error decoding Base64 encoded value"];
        InvalidUtf8(::std::str::Utf8Error)
            #[doc = "Error interpreting a sequence of u8 as a UTF-8 encoded string."];
        Io(::std::io::Error)
            #[doc = "Error during an I/O operation"];
        MpscRecv(::std::sync::mpsc::RecvError)
            #[doc = "Error returned from the `recv` function on an MPSC `Receiver`"];
        ParseAddr(::std::net::AddrParseError)
            #[doc = "Error parsing an IP or socket address"];
        ParseInt(::std::num::ParseIntError)
            #[doc = "Error parsing an integer"];
        PwHash(::pwhash::error::Error)
            #[doc = "Error returned while hashing a password for user authentication"];
    }

// Additional `ErrorKind` variants.
    errors {

/// The client received a server response that it was not able to process.
        BadResponse(details: String) {
            description("Bad Server Response")
            display("Bad Server Response: {}", details)
        }

/// The client was not able to communicate with the cluster due to some issue with the
/// network connection.
        Connection(details: String) {
            description("Network Connection Issue")
            display("Unable to communicate with server cluster: {}", details)
        }

/// One or more of the arguments passed to the client are invalid.
        InvalidArgument(details: String) {
            description("Invalid Argument")
            display("Invalid argument: {}", details)
        }

/// Cluster node is invalid.
        InvalidNode(details: String) {
            description("Invalid cluster node")
            display("Invalid cluster node: {}", details)
        }

/// Exceeded max. number of connections per node.
        NoMoreConnections {
            description("Too many connections")
            display("Too many connections")
        }

/// Server responded with a response code indicating an error condition.
        ServerError(rc: ResultCode) {
            description("Server Error")
            display("Server error: {}", rc.into_string())
        }

/// Error returned when executing a User-Defined Function (UDF) resulted in an error.
        UdfBadResponse(details: String) {
            description("UDF Bad Response")
            display("UDF Bad Response: {}", details)
        }

/// Error returned when a tasked timeed out before it could be completed.
        Timeout(details: String) {
            description("Timeout")
            display("Timeout: {}", details)
        }
    }
}

macro_rules! log_error_chain {
    ($err:expr, $($arg:tt)*) => {
        error!($($arg)*);
        error!("Error: {}", $err);
        for e in $err.iter().skip(1) {
            error!("caused by: {}", e);
        }
        if let Some(backtrace) = $err.backtrace() {
            error!("backtrace: {:?}", backtrace);
        }
    };
}
